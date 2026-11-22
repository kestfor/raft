package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kestfor/raft/internal/config"
)

func TestRunServesHTTPAndStops(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Raft.NodeID, cfg.Raft.DataDir = "test", t.TempDir()
	cfg.Raft.ListenAddress, cfg.HTTP.ListenAddress = "127.0.0.1:0", address
	cfg.Raft.ElectionTimeoutMin, cfg.Raft.ElectionTimeoutMax = 20*time.Millisecond, 21*time.Millisecond
	cfg.Raft.HeartbeatInterval, cfg.Raft.RPCTimeout = 5*time.Millisecond, 5*time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var result error
	go func() {
		result = run(ctx, cfg)
		close(done)
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
			if result != nil {
				t.Errorf("run failed: %v", result)
			}
		case <-time.After(5 * time.Second):
			t.Error("run did not stop")
		}
	}
	t.Cleanup(stop)

	client := &http.Client{Timeout: time.Second, Transport: http.DefaultTransport.(*http.Transport).Clone()}
	defer client.CloseIdleConnections()
	base := "http://" + address
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.Get(base + "/v1/status")
		if err == nil {
			var status struct {
				Role string `json:"role"`
			}
			err = json.NewDecoder(response.Body).Decode(&status)
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if err == nil && status.Role == "leader" {
				break
			}
		}
		select {
		case <-done:
			t.Fatalf("run exited before election: %v", result)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("HTTP node did not elect itself")
		}
		time.Sleep(10 * time.Millisecond)
	}

	data := []byte(`{"answer":42}`)

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		base+"/query",
		bytes.NewReader([]byte(`{"key":"test","value":`+string(data)+`}`)),
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("put: %d", response.StatusCode)
	}
	response, err = client.Get(base + "/query?key=test")
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Value json.RawMessage `json:"value"`
	}
	err = json.NewDecoder(response.Body).Decode(&record)
	got := record.Value
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("get: %q, %v", got, err)
	}
	client.CloseIdleConnections()
	stop()
	if _, err := os.Stat(filepath.Join(cfg.Raft.DataDir, "raft.wal")); !os.IsNotExist(err) {
		t.Fatalf("expected an in-memory journal, stat raft.wal: %v", err)
	}
}

func TestRunClosesResourcesIfHTTPListenFails(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	cfg := config.Default()
	cfg.Raft.NodeID, cfg.Raft.DataDir = "test", t.TempDir()
	cfg.Raft.ListenAddress, cfg.HTTP.ListenAddress = "127.0.0.1:0", listener.Addr().String()
	if err := run(t.Context(), cfg); err == nil {
		t.Fatal("started HTTP on an occupied port")
	}
}
