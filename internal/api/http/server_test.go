package httpapi

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kestfor/raft/internal/kv"
	"go.yaml.in/yaml/v3"
)

type stubStore struct {
	state  kv.Status
	err    error
	called bool
}

func TestOpenAPIDocumentation(t *testing.T) {
	// Documentation is local even when a follower does not know the leader.
	h := New(&stubStore{state: kv.Status{Role: "follower"}}, nil, time.Second)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil))
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "yaml") {
		t.Fatalf("spec response: %d %v", w.Code, w.Header())
	}

	var spec map[string]any
	if err := yaml.Unmarshal(w.Body.Bytes(), &spec); err != nil {
		t.Fatal(err)
	}

	if spec["openapi"] != "3.0.3" {
		t.Fatal("unexpected OpenAPI version")
	}

	paths := spec["paths"].(map[string]any)
	for path, methods := range map[string][]string{"/v1/kv/{key}": {"get", "put", "delete"}, "/v1/status": {"get"}} {
		operations := paths[path].(map[string]any)
		for _, method := range methods {
			operation := operations[method].(map[string]any)
			responses := operation["responses"].(map[string]any)
			if responses["200"] == nil {
				t.Fatalf("missing success for %s %s", method, path)
			}

			if path != "/v1/status" && responses["307"] == nil {
				t.Fatal("missing redirect")
			}
		}
	}

	var checkRefs func(any)
	checkRefs = func(value any) {
		switch v := value.(type) {
		case map[string]any:
			if ref, ok := v["$ref"].(string); ok {
				var target any = spec
				for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
					object, ok := target.(map[string]any)
					if !ok {
						t.Fatalf("invalid reference %s", ref)
					}

					target = object[part]
				}

				if target == nil {
					t.Fatalf("missing reference %s", ref)
				}
			}

			for _, nested := range v {
				checkRefs(nested)
			}
		case []any:
			for _, nested := range v {
				checkRefs(nested)
			}
		}
	}

	checkRefs(spec)

	for _, path := range []string{"/swagger", "/swagger/"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "SwaggerUIBundle") ||
			!strings.Contains(w.Body.String(), "/openapi.yaml") {
			t.Fatalf("Swagger UI: %d %s", w.Code, w.Body.String())
		}
	}
}

func (s *stubStore) Status() kv.Status { return s.state }
func (s *stubStore) Put(context.Context, string, []byte) (uint64, error) {
	s.called = true
	return 1, s.err
}

func (s *stubStore) Delete(context.Context, string) (uint64, error) { s.called = true; return 1, s.err }

func (s *stubStore) Get(context.Context, string) ([]byte, error) { s.called = true; return nil, s.err }

func TestRedirectAndLocalStatus(t *testing.T) {
	s := &stubStore{state: kv.Status{ID: "a", Role: "follower", LeaderID: "b"}}
	h := New(s, map[string]string{"b": "http://leader:18000"}, time.Second)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/v1/kv/a%2Fb?trace=1", bytes.NewReader([]byte{0, 255})))
	if w.Code != 307 || w.Header().Get("Location") != "http://leader:18000/v1/kv/a%2Fb?trace=1" || s.called {
		t.Fatalf("redirect: %d %v called=%t", w.Code, w.Header(), s.called)
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}

	s.state.LeaderID = ""
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/kv/k", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}

func TestErrorsAndSizeLimit(t *testing.T) {
	for _, tt := range []struct {
		err  error
		code int
	}{
		{kv.ErrNotFound, 404},
		{kv.ErrInvalid, 400},
		{kv.ErrUnavailable, 503},
		{context.DeadlineExceeded, 504},
		{kv.ErrNotLeader, 503},
		{errors.New("disk"), 500},
	} {
		s := &stubStore{state: kv.Status{ID: "a", LeaderID: "a", Role: "leader"}, err: tt.err}
		w := httptest.NewRecorder()
		New(s, nil, time.Second).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/kv/k", nil))
		if w.Code != tt.code {
			t.Fatalf("%v: %d", tt.err, w.Code)
		}
	}

	s := &stubStore{state: kv.Status{Role: "leader"}}
	w := httptest.NewRecorder()
	New(
		s,
		nil,
		time.Second,
	).ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/v1/kv/k", bytes.NewReader(make([]byte, kv.MaxValueSize+1))))
	if w.Code != 413 || s.called {
		t.Fatalf("oversized request: %d called=%t", w.Code, s.called)
	}
}
