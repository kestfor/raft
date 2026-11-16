package httpapi

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/kestfor/raft/pkg/raft"
)

const proposeTimeout = 5 * time.Second

type Node interface {
	Status() raft.Status
	Log(from uint64, limit int) (raft.LogSnapshot, error)
	Propose(ctx context.Context, data []byte) (uint64, any, error)
}

var _ Node = (*raft.Node)(nil)

type handler struct{ node Node }

func NewHandler(node Node) http.Handler { return &handler{node: node} }

type statusResponse struct {
	ID           raft.NodeID `json:"id"`
	Role         raft.Role   `json:"role"`
	Term         uint64      `json:"term"`
	LeaderID     raft.NodeID `json:"leader_id"`
	LastLogIndex uint64      `json:"last_log_index"`
	CommitIndex  uint64      `json:"commit_index"`
}

type logEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  string `json:"data"`
	NoOp  bool   `json:"no_op"`
}

type logResponse struct {
	Entries      []logEntry `json:"entries"`
	LastLogIndex uint64     `json:"last_log_index"`
	CommitIndex  uint64     `json:"commit_index"`
}

type errorResponse struct {
	Error    string      `json:"error"`
	LeaderID raft.NodeID `json:"leader_id,omitempty"`
	Index    uint64      `json:"index,omitempty"`
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	var method string
	var serve http.HandlerFunc
	switch r.URL.Path {
	case "/v1/status":
		method, serve = http.MethodGet, h.status
	case "/v1/log":
		method, serve = http.MethodGet, h.log
	case "/v1/propose":
		method, serve = http.MethodPost, h.propose
	default:
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "not found"})
		return
	}

	if r.Method != method {
		w.Header().Set("Allow", method)
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	serve(w, r)
}

func (h *handler) status(w http.ResponseWriter, _ *http.Request) {
	s := h.node.Status()
	writeJSON(w, http.StatusOK, statusResponse{
		ID: s.ID, Role: s.Role, Term: s.Term, LeaderID: s.LeaderID,
		LastLogIndex: s.LastLogIndex, CommitIndex: s.CommitIndex,
	})
}

func (h *handler) log(w http.ResponseWriter, r *http.Request) {
	query, err := parseLogQuery(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	page, err := h.node.Log(query.from, query.limit)
	if err != nil {
		h.writeError(w, err, 0)
		return
	}

	response := logResponse{
		Entries:      make([]logEntry, 0, len(page.Entries)),
		LastLogIndex: page.LastLogIndex, CommitIndex: page.CommitIndex,
	}
	for _, entry := range page.Entries {
		response.Entries = append(response.Entries, logEntry{
			Index: entry.Index, Term: entry.Term, Data: base64.StdEncoding.EncodeToString(entry.Data), NoOp: entry.NoOp,
		})
	}

	writeJSON(w, http.StatusOK, response)
}

type logQuery struct {
	from  uint64
	limit int
}

func parseLogQuery(r *http.Request) (logQuery, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return logQuery{}, err
	}

	query := logQuery{from: 1, limit: 100}
	for name, args := range values {
		if (name != "from" && name != "limit") || len(args) != 1 {
			return logQuery{}, errors.New("expected single from and limit parameters")
		}

		value, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil || value == 0 {
			return logQuery{}, errors.New("from and limit must be positive integers")
		}

		if name == "from" {
			query.from = value
		} else {
			if value > 1000 {
				return logQuery{}, errors.New("limit must not exceed 1000")
			}

			query.limit = int(value)
		}
	}

	return query, nil
}

func (h *handler) propose(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, raft.MaxCommandSize)
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, errorResponse{Error: "command exceeds 1 MiB"})
		} else {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "could not read request body"})
		}

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), proposeTimeout)
	defer cancel()
	index, _, err := h.node.Propose(ctx, data)
	if err != nil {
		h.writeError(w, err, index)
		return
	}

	writeJSON(w, http.StatusOK, struct {
		Index uint64 `json:"index"`
	}{Index: index})
}

func (h *handler) writeError(w http.ResponseWriter, err error, index uint64) {
	code := http.StatusInternalServerError
	response := errorResponse{Error: "raft request failed", Index: index}
	switch {
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost):
		code, response.Error = http.StatusConflict, err.Error()
		response.LeaderID = h.node.Status().LeaderID
	case errors.Is(err, raft.ErrNotRunning), errors.Is(err, context.Canceled):
		code, response.Error = http.StatusServiceUnavailable, err.Error()
	case errors.Is(err, context.DeadlineExceeded):
		code, response.Error = http.StatusGatewayTimeout, err.Error()
	case errors.Is(err, raft.ErrInvalidRequest):
		code, response.Error = http.StatusBadRequest, err.Error()
	}

	writeJSON(w, code, response)
}
