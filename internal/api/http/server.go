// Package httpapi exposes the KV service without depending on the Raft protocol.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"

	httpcontract "github.com/kestfor/raft/api/http"
	"github.com/kestfor/raft/internal/kv"
)

type Store interface {
	Put(context.Context, string, []byte) (uint64, error)
	Get(context.Context, string) ([]byte, error)
	Delete(context.Context, string) (uint64, error)
	Status() kv.Status
}

type Handler struct {
	store   Store
	leaders map[string]string
	timeout time.Duration
}

func New(store Store, leaders map[string]string, timeout time.Duration) http.Handler {
	return &Handler{store: store, leaders: maps.Clone(leaders), timeout: timeout}
}

// ServeHTTP deliberately avoids ServeMux path cleaning: keys can contain slash,
// dot segments or repeated slashes, and must survive redirects byte for byte.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		switch r.URL.Path {
		case "/openapi.yaml":
			w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
			_, _ = io.WriteString(w, httpcontract.OpenAPI)
			return
		case "/swagger", "/swagger/":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.WriteString(w, swaggerPage)
			return
		}
	}

	if r.URL.Path == "/v1/status" && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, h.store.Status())
		return
	}

	if !strings.HasPrefix(r.URL.Path, "/v1/kv/") {
		writeError(w, http.StatusNotFound, "unknown endpoint")
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
		w.Header().Set("Allow", "GET, PUT, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.store.Status().Role != "leader" {
		h.redirect(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	key := strings.TrimPrefix(r.URL.Path, "/v1/kv/")
	var index uint64
	var err error

	switch r.Method {
	case http.MethodGet:
		var value []byte
		value, err = h.store.Get(ctx, key)
		if err == nil {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Cache-Control", "no-store")
			_, _ = w.Write(value)
			return
		}
	case http.MethodPut:
		r.Body = http.MaxBytesReader(w, r.Body, kv.MaxValueSize)
		var value []byte
		value, err = io.ReadAll(r.Body)
		if err == nil {
			index, err = h.store.Put(ctx, key, value)
		}
	case http.MethodDelete:
		index, err = h.store.Delete(ctx, key)
	}

	if err != nil {
		h.respondError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, struct {
		Index uint64 `json:"index"`
	}{index})
}

func (h *Handler) redirect(w http.ResponseWriter, r *http.Request) {
	state := h.store.Status()
	base := h.leaders[state.LeaderID]
	if state.LeaderID == "" || state.LeaderID == state.ID || base == "" {
		writeError(w, http.StatusServiceUnavailable, "leader unavailable; try again later")
		return
	}

	target, err := url.Parse(base)
	if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
		writeError(w, http.StatusServiceUnavailable, "leader address unavailable")
		return
	}

	target.Path, target.RawPath, target.RawQuery = r.URL.Path, r.URL.RawPath, r.URL.RawQuery
	w.Header().Set("Location", target.String())
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusTemporaryRedirect)
}

func (h *Handler) respondError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	switch {
	case errors.Is(err, kv.ErrNotLeader):
		h.redirect(w, r)
	case errors.As(err, &tooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "value exceeds 512 KiB")
	case errors.Is(err, kv.ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, kv.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "request timed out; a write may still commit")
	case errors.Is(err, context.Canceled), errors.Is(err, kv.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, "store unavailable; a write may still commit")
	default:
		writeError(w, http.StatusInternalServerError, "request failed; a write may still commit")
	}
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, struct {
		Error string `json:"error"`
	}{message})
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
