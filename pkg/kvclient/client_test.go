package kvclient

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRedirectLoopAndServerErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"no leader"}`))
			return
		}

		w.Header().Set("Location", r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	client, err := New(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.Put(t.Context(), "loop", []byte("value")); err == nil {
		t.Fatal("redirect loop succeeded")
	}

	_, err = client.Status(t.Context())
	var response *Error
	if !errors.As(err, &response) || response.StatusCode != 503 || response.Message != "no leader" {
		t.Fatalf("server error: %v", err)
	}
}

func TestEndpointValidation(t *testing.T) {
	for _, endpoint := range []string{"localhost:18001", "ftp://host", "http://host/path", "http://host?x=y", "http://user@host"} {
		if _, err := New(endpoint, time.Second); err == nil {
			t.Fatalf("accepted %q", endpoint)
		}
	}

	if _, err := New("http://localhost", 0); err == nil {
		t.Fatal("accepted zero timeout")
	}
}
