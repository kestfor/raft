package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kestfor/raft/pkg/raft"
)

type stubNode struct {
	state   raft.Status
	page    raft.LogSnapshot
	from    uint64
	limit   int
	propose func(context.Context, []byte) (uint64, error)
}

func (s *stubNode) Status() raft.Status { return s.state }
func (s *stubNode) Log(from uint64, limit int) (raft.LogSnapshot, error) {
	s.from, s.limit = from, limit
	return s.page, nil
}

func (s *stubNode) Propose(ctx context.Context, data []byte) (uint64, any, error) {
	index, err := s.propose(ctx, data)
	return index, nil, err
}

func decodeResponse[T any](t *testing.T, response *httptest.ResponseRecorder) T {
	t.Helper()
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected content type: %v", response.Header())
	}
	var value T
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestStatusAndLog(t *testing.T) {
	node := &stubNode{
		state: raft.Status{ID: "b", Role: raft.Follower, Term: 3, LeaderID: "a", LastLogIndex: 2, CommitIndex: 1},
		page: raft.LogSnapshot{Entries: []raft.Entry{
			{Index: 1, Term: 3, NoOp: true}, {Index: 2, Term: 3, Data: []byte{0, 0xff, 1}},
		}, LastLogIndex: 2, CommitIndex: 1},
	}
	handler := NewHandler(node)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	status := decodeResponse[statusResponse](t, w)
	if w.Code != http.StatusOK || status.ID != "b" || status.Role != raft.Follower || status.Term != 3 ||
		status.LeaderID != "a" || status.LastLogIndex != 2 || status.CommitIndex != 1 {
		t.Fatalf("unexpected status: %d %+v", w.Code, status)
	}
	for _, path := range []string{"/v1/log", "/v1/log?from=2&limit=10"} {
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		page := decodeResponse[logResponse](t, w)
		if w.Code != http.StatusOK || page.CommitIndex != 1 || page.LastLogIndex != 2 || len(page.Entries) != 2 ||
			!page.Entries[0].NoOp || page.Entries[0].Data != "" ||
			page.Entries[1].Data != base64.StdEncoding.EncodeToString([]byte{0, 0xff, 1}) {
			t.Fatalf("unexpected page: %d %+v", w.Code, page)
		}
		if (path == "/v1/log" && (node.from != 1 || node.limit != 100)) ||
			(path != "/v1/log" && (node.from != 2 || node.limit != 10)) {
			t.Fatalf("wrong bounds: %d %d", node.from, node.limit)
		}
	}
	node.page.Entries = nil
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/log", nil))
	if page := decodeResponse[logResponse](t, w); page.Entries == nil || len(page.Entries) != 0 {
		t.Fatalf("expected JSON array: %s", w.Body)
	}
}

func TestRejectsInvalidQueriesAndRoutes(t *testing.T) {
	for _, query := range []string{
		"from=0", "from=-1", "from=", "from=x", "limit=0", "limit=1001", "limit=-1",
		"from=18446744073709551616", "from=1&from=2", "limit=1&limit=2", "unknown=1", "from=%ff%", "from=1;limit=2",
	} {
		w := httptest.NewRecorder()
		NewHandler(&stubNode{}).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/log?"+query, nil))
		if body := decodeResponse[errorResponse](t, w); w.Code != http.StatusBadRequest || body.Error == "" {
			t.Fatalf("query %q: %d %s", query, w.Code, w.Body)
		}
	}
	for _, tt := range []struct {
		method, path, allow string
		code                int
	}{
		{http.MethodPost, "/v1/status", "GET", 405},
		{http.MethodPost, "/v1/log", "GET", 405},
		{http.MethodGet, "/v1/propose", "POST", 405},
		{http.MethodGet, "/unknown", "", 404},
	} {
		w := httptest.NewRecorder()
		NewHandler(&stubNode{}).ServeHTTP(w, httptest.NewRequest(tt.method, tt.path, nil))
		if body := decodeResponse[errorResponse](
			t,
			w,
		); w.Code != tt.code || w.Header().Get("Allow") != tt.allow ||
			body.Error == "" {
			t.Fatalf("%s %s: %d %s", tt.method, tt.path, w.Code, w.Body)
		}
	}
}

func TestProposeBinaryBodyAndLimits(t *testing.T) {
	for _, size := range []int{0, 3, (1 << 20) + 1, raft.MaxCommandSize + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := bytes.Repeat([]byte{0xff}, min(size, (1<<20)+1))
			called := false
			node := &stubNode{propose: func(ctx context.Context, got []byte) (uint64, error) {
				called = true
				if !bytes.Equal(got, data) {
					t.Fatal("body changed")
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > proposeTimeout {
					t.Fatal("missing proposal deadline")
				}
				return 7, nil
			}}
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/propose", bytes.NewReader(data))
			req.ContentLength = int64(size)
			NewHandler(node).ServeHTTP(w, req)
			if size > raft.MaxCommandSize {
				if w.Code != http.StatusRequestEntityTooLarge || called {
					t.Fatalf("oversized proposal: %d, called=%t", w.Code, called)
				}
				return
			}
			body := decodeResponse[struct {
				Index uint64 `json:"index"`
			}](t, w)
			if w.Code != http.StatusOK || !called || body.Index != 7 {
				t.Fatalf("proposal: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestProposeErrorsPreserveIndex(t *testing.T) {
	for _, tt := range []struct {
		err   error
		index uint64
		code  int
	}{
		{raft.ErrNotLeader, 0, 409},
		{raft.ErrLeadershipLost, 7, 409},
		{context.DeadlineExceeded, 7, 504},
		{context.Canceled, 7, 503},
		{raft.ErrNotRunning, 0, 503},
		{raft.ErrInvalidRequest, 0, 400},
		{errors.New("private disk path"), 7, 500},
	} {
		node := &stubNode{state: raft.Status{LeaderID: "a"}, propose: func(context.Context, []byte) (uint64, error) {
			return tt.index, fmt.Errorf("wrapped: %w", tt.err)
		}}
		w := httptest.NewRecorder()
		NewHandler(node).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/propose", nil))
		body := decodeResponse[errorResponse](t, w)
		if w.Code != tt.code || body.Index != tt.index || body.Error == "" || (tt.code == 409 && body.LeaderID != "a") {
			t.Fatalf("error %v: %d %+v", tt.err, w.Code, body)
		}
		if tt.code == 500 && strings.Contains(body.Error, "private") {
			t.Fatal("exposed internal error")
		}
	}
}

func TestProposeDeadlineAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, canceled := range []bool{false, true} {
			node := &stubNode{propose: func(ctx context.Context, _ []byte) (uint64, error) {
				<-ctx.Done()
				return 9, ctx.Err()
			}}
			ctx, cancel := context.WithCancel(t.Context())
			if canceled {
				cancel()
			}
			request := httptest.NewRequest(http.MethodPost, "/v1/propose", nil).WithContext(ctx)
			w := httptest.NewRecorder()
			NewHandler(node).ServeHTTP(w, request)
			cancel()
			want := http.StatusGatewayTimeout
			if canceled {
				want = http.StatusServiceUnavailable
			}
			if body := decodeResponse[errorResponse](t, w); w.Code != want || body.Index != 9 {
				t.Fatalf("deadline/cancellation: %d %s", w.Code, w.Body)
			}
		}
	})
}
