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
	"time"

	httpcontract "github.com/kestfor/raft/api"
	"github.com/kestfor/raft/internal/kv"
	"github.com/kestfor/raft/internal/limits"
)

type Store interface {
	Put(context.Context, string, []byte) (uint64, error)
	Get(context.Context, string) ([]byte, error)
	Log() ([]kv.Record, error)
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

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		switch r.URL.Path {
		case "/openapi.json", "/openapi.yaml":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, httpcontract.ClientOpenAPI)
			return
		case "/swagger", "/swagger/":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.WriteString(w, swaggerPage)
			return
		case "/v1/status":
			writeJSON(w, http.StatusOK, h.store.Status())
			return
		case "/log":
			records, err := h.store.Log()
			if err != nil {
				h.respondError(w, r, err)
				return
			}
			writeJSON(w, http.StatusOK, records)
			return
		}
	}
	if r.URL.Path != "/query" {
		if r.URL.Path == "/log" || r.URL.Path == "/v1/status" {
			w.Header().Set("Allow", "GET")
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		} else {
			writeError(w, http.StatusNotFound, "unknown endpoint")
		}
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.store.Status().Role != "leader" {
		h.redirect(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	if r.Method == http.MethodGet {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			validationError(w, []any{"query"}, "Invalid query string", "value_error")
			return
		}
		keys, ok := values["key"]
		if !ok {
			validationError(w, []any{"query", "key"}, "Field required", "missing")
			return
		}
		key := keys[len(keys)-1]
		if key == "" {
			validationError(w, []any{"query", "key"}, "String should have at least 1 character", "string_too_short")
			return
		}
		value, err := h.store.Get(ctx, key)
		if err != nil {
			h.respondError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, kv.Record{Key: key, Value: value})
		return
	}
	if r.ContentLength > limits.Command {
		writeError(w, http.StatusRequestEntityTooLarge, "request too large")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, limits.Command)
	decoder := json.NewDecoder(r.Body)
	var input map[string]json.RawMessage
	if err := decoder.Decode(&input); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.respondError(w, r, err)
		} else {
			validationError(w, []any{"body"}, "Invalid JSON object", "json_invalid")
		}
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.respondError(w, r, err)
		} else {
			validationError(w, []any{"body"}, "Unexpected trailing data", "json_invalid")
		}
		return
	}
	keyData, ok := input["key"]
	if !ok {
		validationError(w, []any{"body", "key"}, "Field required", "missing")
		return
	}
	var key string
	if len(keyData) == 0 || keyData[0] != '"' || json.Unmarshal(keyData, &key) != nil {
		validationError(w, []any{"body", "key"}, "Input should be a valid string", "string_type")
		return
	}
	value, ok := input["value"]
	if !ok {
		validationError(w, []any{"body", "value"}, "Field required", "missing")
		return
	}
	if !kv.ValidValue(value) {
		validationError(w, []any{"body", "value"}, "Expected string, number, array, object or null", "value_error")
		return
	}
	index, err := h.store.Put(ctx, key, value)
	if err != nil {
		h.respondError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Index uint64 `json:"index"`
	}{index})
}

func validationError(w http.ResponseWriter, loc []any, message, kind string) {
	type detail struct {
		Loc     []any  `json:"loc"`
		Message string `json:"msg"`
		Type    string `json:"type"`
	}
	writeJSON(w, http.StatusUnprocessableEntity, struct {
		Detail []detail `json:"detail"`
	}{[]detail{{loc, message, kind}}})
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
	case errors.Is(err, kv.ErrTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
	case errors.As(err, &tooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "request too large")
	case errors.Is(err, kv.ErrInvalid):
		validationError(w, []any{"body"}, err.Error(), "value_error")
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
