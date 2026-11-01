package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
)

type memoryClient struct {
	mu            sync.Mutex
	values        map[string][]byte
	reads, writes int
	wrong         bool
}

func (m *memoryClient) Put(_ context.Context, key string, value []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.values[key] = bytes.Clone(value)
	m.writes++
	return uint64(m.writes), nil
}

func (m *memoryClient) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.reads++
	if m.wrong {
		return []byte("wrong"), nil
	}

	return bytes.Clone(m.values[key]), nil
}

func TestExactPercentiles(t *testing.T) {
	samples := []time.Duration{
		5 * time.Millisecond,
		time.Millisecond,
		3 * time.Millisecond,
		2 * time.Millisecond,
		4 * time.Millisecond,
	}

	l := summarize(samples)
	if l.Min != 1 || l.Mean != 3 || l.P50 != 3 || l.P95 != 5 || l.P99 != 5 || l.Max != 5 {
		t.Fatalf("%+v", l)
	}

	if summarize(nil) != nil {
		t.Fatal("empty distribution must be null")
	}
}

func TestMixedWorkloadCountsAndIntegrity(t *testing.T) {
	m := &memoryClient{values: map[string][]byte{"bench/0": []byte("xx")}}
	cfg := settings{Requests: 100, Clients: 8, Size: 2, Keyspace: 1, ReadPercent: 80}
	r := benchmark(t.Context(), m, cfg, "mixed", "bench/", &progress{})
	if r.Completed != 100 || r.Success != 100 || r.Errors != 0 || m.reads != 80 || m.writes != 20 {
		t.Fatalf("%+v reads=%d writes=%d", r, m.reads, m.writes)
	}

	if r.Operations["get"].Success != 80 || r.Operations["put"].Success != 20 || r.Throughput <= 0 {
		t.Fatal(r)
	}

	m.wrong = true
	r = benchmark(t.Context(), m, cfg, "get", "bench/", &progress{})
	if r.Errors != 100 || r.Success != 0 || r.Latency != nil || r.ErrorCounts["value_mismatch"] != 100 {
		t.Fatal(r)
	}
}

func TestCanceledRunDoesNotLaunchRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	m := &memoryClient{values: make(map[string][]byte)}
	r := benchmark(ctx, m, settings{Requests: 100, Clients: 8, Keyspace: 1}, "get", "bench/", &progress{})
	if r.Completed != 0 || m.reads != 0 || r.Latency != nil {
		t.Fatal(r)
	}
}

func TestCLIJSONWarmupAndRedirect(t *testing.T) {
	m := &memoryClient{values: make(map[string][]byte)}
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_, _ = io.WriteString(w, `{"id":"a","role":"leader","term":1,"leader_id":"a"}`)
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/v1/kv/")
		if r.Method == http.MethodPut {
			value, _ := io.ReadAll(r.Body)
			index, _ := m.Put(r.Context(), key, value)
			_ = json.NewEncoder(w).Encode(map[string]uint64{"index": index})
			return
		}

		value, _ := m.Get(r.Context(), key)
		_, _ = w.Write(value)
	}))
	defer leader.Close()
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", leader.URL+r.URL.RequestURI())
		w.WriteHeader(307)
	}))
	defer follower.Close()
	cmd := newCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(
		[]string{
			"--endpoint",
			follower.URL,
			"-n",
			"100",
			"-c",
			"4",
			"-r",
			"2",
			"-d",
			"8",
			"-t",
			"mixed",
			"--warmup",
			"10",
			"--json",
			"--color", "always",
		},
	)
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}

	var got report
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err, out.String())
	}

	if len(got.Results) != 1 || got.Results[0].Success != 100 || got.Results[0].Operations["get"].Success != 80 {
		t.Fatalf("%+v", got)
	}

	if m.reads+m.writes != 112 {
		t.Fatalf("setup/warmup accounting: %d", m.reads+m.writes)
	}

	if errOut.Len() != 0 || strings.Contains(out.String(), "\x1b") {
		t.Fatal("JSON output contains terminal formatting")
	}
}

func TestCLIValidationAndHelp(t *testing.T) {
	for _, args := range [][]string{{"-n", "0"}, {"-c", "0"}, {"-d", "-1"}, {"-r", "0"}, {"-t", "invalid"}, {"--read-percent", "101"}, {"--warmup", "-1"}, {"--color", "invalid"}} {
		cmd := newCommand()
		cmd.SetArgs(args)
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		if err := cmd.ExecuteContext(t.Context()); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}

	cmd := newCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--help"})
	if err := cmd.ExecuteContext(t.Context()); err != nil || !strings.Contains(out.String(), "--clients") {
		t.Fatal(err, out.String())
	}
}

func TestResultColorsDoNotChangeMetricsOrAlignment(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	r := result{
		Workload: "put", Repeat: 1, Requested: 100, Success: 99, Errors: 1,
		Seconds: 1, Throughput: 99, PayloadMiBPerSecond: 0.1,
		Latency:     &latency{P50: 1.2, P95: 2.3, P99: 4.5, Max: 6.7},
		ErrorCounts: map[string]int{"timeout": 1},
	}
	var plain, colored, auto bytes.Buffer
	printResult(&plain, r, "never")
	printResult(&colored, r, "always")
	printResult(&auto, r, "auto")
	if !strings.Contains(colored.String(), "\x1b[") {
		t.Fatal("forced color produced no ANSI")
	}
	if ansi.Strip(colored.String()) != plain.String() || auto.String() != plain.String() {
		t.Fatal("color changed displayed metrics or column alignment")
	}
	if !strings.Contains(plain.String(), "99/100 successful") || !strings.Contains(plain.String(), "timeout") {
		t.Fatal("result omitted errors or counts")
	}

	t.Setenv("NO_COLOR", "1")
	colored.Reset()
	printResult(&colored, r, "always")
	if colored.String() != plain.String() {
		t.Fatal("NO_COLOR was ignored")
	}
}

func TestCLIReportsMeasuredErrorsAsJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			_, _ = io.WriteString(w, `{"id":"a","role":"leader","term":1}`)
		case r.Method == http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, `{"index":1}`)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"unavailable"}`)
		}
	}))
	defer server.Close()

	cmd := newCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(
		[]string{"--endpoint", server.URL, "-n", "8", "-c", "2", "-r", "1", "-t", "get", "--warmup", "0", "--json"},
	)
	if err := cmd.ExecuteContext(t.Context()); err == nil {
		t.Fatal("failed requests must produce a nonzero exit status")
	}

	var got report
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}

	r := got.Results[0]
	if r.Success != 0 || r.Errors != 8 || r.ErrorCounts["http_503"] != 8 || r.Latency != nil {
		t.Fatalf("errors hidden in metrics: %+v", r)
	}
}
