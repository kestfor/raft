package kvclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

	if _, err := client.Put(t.Context(), "loop", []byte(`"value"`)); err == nil {
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

func TestJSONContractAndLargeLog(t *testing.T) {
	key := "a/b ?#%ключ"
	value := json.RawMessage(`{"number":9007199254740993,"nested":[true,null]}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("wrong content type")
		}
		switch {
		case r.Method == "POST" && r.URL.Path == "/query":
			var record Record
			if json.NewDecoder(r.Body).Decode(&record) != nil || record.Key != key ||
				!bytes.Equal(record.Value, value) {
				t.Error("changed input")
			}
			_, _ = w.Write([]byte(`null`))
		case r.Method == "GET" && r.URL.Path == "/query":
			if r.URL.Query().Get("key") != key {
				t.Error("changed key")
			}
			_ = json.NewEncoder(w).Encode(Record{key, value})
		case r.Method == "GET" && r.URL.Path == "/log":
			_ = json.NewEncoder(w).Encode([]Record{{key, json.RawMessage(`"` + strings.Repeat("x", (1<<20)+1) + `"`)}})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if index, err := client.Put(t.Context(), key, value); err != nil || index != 0 {
		t.Fatalf("put: %d %v", index, err)
	}
	got, err := client.Get(t.Context(), key)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("get: %s %v", got, err)
	}
	records, err := client.Log(t.Context())
	if err != nil || len(records) != 1 || len(records[0].Value) < 1<<20 {
		t.Fatalf("large log: %v", err)
	}
}
