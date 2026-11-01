package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIBytesRedirectAndStatus(t *testing.T) {
	value := []byte{0, 255, '\n', 42}
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			_, _ = io.WriteString(w, `{"id":"a","role":"leader","term":2,"leader_id":"a","applied_index":3}`)
		case r.Method == http.MethodPut:
			got, _ := io.ReadAll(r.Body)
			if !bytes.Equal(got, value) {
				t.Errorf("body changed: %v", got)
			}

			_, _ = io.WriteString(w, `{"index":3}`)
		case r.Method == http.MethodGet:
			_, _ = w.Write(value)
		case r.Method == http.MethodDelete:
			_, _ = io.WriteString(w, `{"index":4}`)
		}
	}))
	defer leader.Close()
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", leader.URL+r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer follower.Close()
	execute := func(input []byte, args ...string) ([]byte, error) {
		cmd := newCommand()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(io.Discard)
		cmd.SetIn(bytes.NewReader(input))
		cmd.SetArgs(append([]string{"--endpoint", follower.URL}, args...))
		err := cmd.ExecuteContext(t.Context())
		return out.Bytes(), err
	}

	if out, err := execute(value, "put", "a/b", "--stdin"); err != nil || !strings.Contains(string(out), "index 3") {
		t.Fatalf("put: %s %v", out, err)
	}

	if out, err := execute(nil, "get", "a/b"); err != nil || !bytes.Equal(out, value) {
		t.Fatalf("get: %v %v", out, err)
	}

	path := filepath.Join(t.TempDir(), "value.bin")
	if _, err := execute(nil, "get", "a/b", "--output", path); err != nil {
		t.Fatal(err)
	}

	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, value) {
		t.Fatal("file differs", err)
	}

	if _, err := execute(nil, "put", "a/b", "--file", path); err != nil {
		t.Fatal(err)
	}

	out, err := execute(nil, "get", "a/b", "--json")
	var result struct {
		Value string `json:"value"`
	}

	if err != nil || json.Unmarshal(out, &result) != nil || result.Value != base64.StdEncoding.EncodeToString(value) {
		t.Fatalf("JSON: %s %v", out, err)
	}

	if out, err := execute(
		nil,
		"status",
	); err != nil || !strings.Contains(string(out), "Applied") ||
		bytes.Contains(out, []byte("\x1b")) {
		t.Fatalf("status: %s %v", out, err)
	}

	if _, err := execute(nil, "delete", "a/b"); err != nil {
		t.Fatal(err)
	}

	for _, args := range [][]string{{"put", "k"}, {"put", "k", "v", "--stdin"}, {"put", "k", "--stdin", "--file", path}, {"get", "k", "--json", "--output", path}} {
		if _, err := execute(nil, args...); err == nil {
			t.Fatalf("accepted invalid args %v", args)
		}
	}
}

func TestHelpAndCompletion(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"put", "--help"}, {"completion", "bash"}} {
		cmd := newCommand()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs(args)
		if err := cmd.ExecuteContext(t.Context()); err != nil || out.Len() == 0 {
			t.Fatalf("%v: %v", args, err)
		}
	}
}
