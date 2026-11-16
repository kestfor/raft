package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/kestfor/raft/internal/config"
	"github.com/kestfor/raft/pkg/wal"
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
	cfg.ID, cfg.DataDir = "test", t.TempDir()
	cfg.ListenAddress, cfg.HTTPListenAddress = "127.0.0.1:0", address
	cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax = 20*time.Millisecond, 21*time.Millisecond
	cfg.HeartbeatInterval, cfg.RPCTimeout = 5*time.Millisecond, 5*time.Millisecond
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

	client := &http.Client{Timeout: time.Second}
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

	data := []byte{0, 0xff, 1}
	response, err := client.Post(base+"/v1/propose", "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var proposal struct {
		Index uint64 `json:"index"`
	}
	err = json.NewDecoder(response.Body).Decode(&proposal)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || proposal.Index != 2 {
		t.Fatalf("proposal: code=%d index=%d err=%v", response.StatusCode, proposal.Index, err)
	}
	response, err = client.Get(base + "/v1/log?from=2&limit=1")
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Entries []struct {
			Data string `json:"data"`
		} `json:"entries"`
		CommitIndex uint64 `json:"commit_index"`
	}
	err = json.NewDecoder(response.Body).Decode(&page)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || page.CommitIndex != 2 || len(page.Entries) != 1 ||
		page.Entries[0].Data != base64.StdEncoding.EncodeToString(data) {
		t.Fatalf("log: %+v, %v", page, err)
	}
	stop()
	journal, err := wal.Open(filepath.Join(cfg.DataDir, "raft.wal"))
	if err != nil {
		t.Fatalf("shutdown did not release WAL: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunClosesResourcesIfHTTPListenFails(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	cfg := config.Default()
	cfg.ID, cfg.DataDir = "test", t.TempDir()
	cfg.ListenAddress, cfg.HTTPListenAddress = "127.0.0.1:0", listener.Addr().String()
	if err := run(t.Context(), cfg); err == nil {
		t.Fatal("started HTTP on an occupied port")
	}
	journal, err := wal.Open(filepath.Join(cfg.DataDir, "raft.wal"))
	if err != nil {
		t.Fatalf("startup failure did not release WAL: %v", err)
	}
	defer func() { _ = journal.Close() }()
}
