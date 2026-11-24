package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	contract "github.com/kestfor/raft/api"
	"github.com/kestfor/raft/internal/kv"
	"github.com/kestfor/raft/internal/limits"
)

type stubStore struct {
	state  kv.Status
	err    error
	called bool
	key    string
	value  []byte
}

func (s *stubStore) Status() kv.Status { return s.state }
func (s *stubStore) Put(_ context.Context, key string, value []byte) (uint64, error) {
	s.called = true
	s.key = key
	s.value = bytes.Clone(value)
	return 1, s.err
}

func (s *stubStore) Get(context.Context, string) ([]byte, error) {
	s.called = true
	return s.value, s.err
}
func (s *stubStore) Log() ([]kv.Record, error) { return []kv.Record{}, s.err }

func TestOpenAPIAndLocalEndpoints(t *testing.T) {
	h := New(&stubStore{state: kv.Status{Role: "follower"}}, nil, time.Second)
	for _, path := range []string{"/openapi.json", "/openapi.yaml"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || w.Body.String() != contract.ClientOpenAPI {
			t.Fatalf("spec %s: %d", path, w.Code)
		}
	}
	for _, path := range []string{"/swagger", "/swagger/"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "/openapi.json") {
			t.Fatalf("swagger: %d", w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/log", nil))
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatalf("local log: %d %s", w.Code, w.Body)
	}
}

func TestQueryPreservesJSONValues(t *testing.T) {
	for _, value := range []string{`""`, `"text"`, `9007199254740993`, `-1.25e100`, `[]`, `[true,null,{"n":1}]`, `{"bool":false}`, `null`} {
		t.Run(value, func(t *testing.T) {
			s := &stubStore{state: kv.Status{Role: "leader"}}
			h := New(s, nil, time.Second)
			w := httptest.NewRecorder()
			h.ServeHTTP(
				w,
				httptest.NewRequest("POST", "/query", strings.NewReader(`{"key":"a/b ?#%","value":`+value+`}`)),
			)
			if w.Code != 200 || string(s.value) != value {
				t.Fatalf("post: %d %s", w.Code, w.Body)
			}
			w = httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/query?key=a%2Fb+%3F%23%25", nil))
			var record kv.Record
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &record) != nil || record.Key != "a/b ?#%" ||
				string(record.Value) != value {
				t.Fatalf("get: %d %s", w.Code, w.Body)
			}
		})
	}
	s := &stubStore{state: kv.Status{Role: "leader"}}
	w := httptest.NewRecorder()
	New(
		s,
		nil,
		time.Second,
	).ServeHTTP(w, httptest.NewRequest("POST", "/query", strings.NewReader(`{"key":"","value":null}`)))
	if w.Code != 200 {
		t.Fatalf("POST schema allows empty key: %d", w.Code)
	}
}

func TestQueryValidation(t *testing.T) {
	for _, tt := range []struct{ method, path, body string }{
		{"GET", "/query", ""},
		{"GET", "/query?key=", ""},
		{"POST", "/query", `{}`},
		{"POST", "/query", `{"key":"k"}`},
		{"POST", "/query", `{"value":1}`},
		{"POST", "/query", `{"key":1,"value":1}`},
		{"POST", "/query", `{"key":null,"value":1}`},
		{"POST", "/query", `{"key":"k","value":true}`},
		{"POST", "/query", `{"key":"k","value":false}`},
		{"POST", "/query", `null`},
		{"POST", "/query", `[]`},
		{"POST", "/query", `{"key":"k","value":1} {}`},
	} {
		s := &stubStore{state: kv.Status{Role: "leader"}}
		w := httptest.NewRecorder()
		New(s, nil, time.Second).ServeHTTP(w, httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body)))
		var response struct {
			Detail []struct {
				Loc  []any  `json:"loc"`
				Msg  string `json:"msg"`
				Type string `json:"type"`
			} `json:"detail"`
		}
		if w.Code != 422 || json.Unmarshal(w.Body.Bytes(), &response) != nil || len(response.Detail) == 0 ||
			response.Detail[0].Msg == "" ||
			response.Detail[0].Type == "" ||
			len(response.Detail[0].Loc) == 0 ||
			s.called {
			t.Fatalf("%s %s %s: %d %s", tt.method, tt.path, tt.body, w.Code, w.Body)
		}
	}
}

func TestRedirectErrorsAndSizeLimit(t *testing.T) {
	s := &stubStore{state: kv.Status{ID: "a", Role: "follower", LeaderID: "b"}}
	h := New(s, map[string]string{"b": "http://leader:18000"}, time.Second)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/query?trace=1", strings.NewReader(`{"key":"k","value":null}`)))
	if w.Code != 307 || w.Header().Get("Location") != "http://leader:18000/query?trace=1" || s.called {
		t.Fatalf("redirect: %d %v", w.Code, w.Header())
	}
	for _, tt := range []struct {
		err  error
		code int
	}{{kv.ErrTooLarge, 413}, {kv.ErrNotFound, 404}, {kv.ErrInvalid, 422}, {kv.ErrUnavailable, 503}, {context.DeadlineExceeded, 504}, {kv.ErrNotLeader, 503}, {errors.New("disk"), 500}} {
		s = &stubStore{state: kv.Status{Role: "leader"}, err: tt.err}
		w = httptest.NewRecorder()
		New(s, nil, time.Second).ServeHTTP(w, httptest.NewRequest("GET", "/query?key=k", nil))
		if w.Code != tt.code {
			t.Fatalf("%v: %d", tt.err, w.Code)
		}
	}
	s = &stubStore{state: kv.Status{Role: "leader"}}
	w = httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/query", nil)
	req.ContentLength = limits.Command + 1
	New(s, nil, time.Second).ServeHTTP(w, req)
	if w.Code != 413 || s.called {
		t.Fatalf("size: %d", w.Code)
	}
}
