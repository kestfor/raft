package raft

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type memoryNetwork struct {
	mu       sync.RWMutex
	nodes    map[NodeID]*Node
	isolated NodeID
}

type memoryTransport struct {
	network *memoryNetwork
	from    NodeID
}

func (m memoryTransport) target(peer NodeID) (*Node, error) {
	m.network.mu.RLock()
	defer m.network.mu.RUnlock()

	if m.network.isolated != "" && (m.from == m.network.isolated || peer == m.network.isolated) {
		return nil, errors.New("network partition")
	}

	return m.network.nodes[peer], nil
}

func (m memoryTransport) RequestVote(
	ctx context.Context,
	peer NodeID,
	req RequestVoteRequest,
) (RequestVoteResponse, error) {
	n, err := m.target(peer)
	if err != nil {
		return RequestVoteResponse{}, err
	}

	return n.RequestVote(ctx, req)
}

func (m memoryTransport) AppendEntries(
	ctx context.Context,
	peer NodeID,
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	n, err := m.target(peer)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	return n.AppendEntries(ctx, req)
}

func testConfig(id NodeID, peers []NodeID) Config {
	cfg := DefaultConfig(id, peers)
	cfg.ElectionTimeoutMin = 100 * time.Millisecond
	cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + time.Nanosecond
	cfg.HeartbeatInterval = 10 * time.Millisecond
	cfg.RPCTimeout = 20 * time.Millisecond
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	return cfg
}

func startMemoryCluster(t *testing.T) (*memoryNetwork, map[NodeID]context.CancelFunc, map[NodeID]<-chan error) {
	t.Helper()
	network := &memoryNetwork{nodes: make(map[NodeID]*Node)}
	cancels := make(map[NodeID]context.CancelFunc)
	done := make(map[NodeID]<-chan error)
	ids := []NodeID{"a", "b", "c"}
	for i, id := range ids {
		var peers []NodeID
		for _, peer := range ids {
			if peer != id {
				peers = append(peers, peer)
			}
		}

		cfg := testConfig(id, peers)
		cfg.ElectionTimeoutMin += time.Duration(i) * 100 * time.Millisecond
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + time.Nanosecond
		node, err := NewNode(cfg, memoryTransport{network: network, from: id})
		if err != nil {
			t.Fatal(err)
		}

		network.nodes[id] = node
	}

	for _, id := range ids {
		ctx, cancel := context.WithCancel(context.Background())
		cancels[id] = cancel
		result := make(chan error, 1)
		done[id] = result
		go func() { result <- network.nodes[id].Run(ctx) }()
	}

	synctest.Wait()

	return network, cancels, done
}

func stopMemoryCluster(cancels map[NodeID]context.CancelFunc) {
	for _, cancel := range cancels {
		cancel()
	}

	synctest.Wait()
}

func requireLeader(t *testing.T, network *memoryNetwork, leader NodeID, term uint64) {
	t.Helper()
	synctest.Wait()
	for id, node := range network.nodes {
		node.mu.Lock()
		running := node.runtime.running
		node.mu.Unlock()
		if !running {
			continue
		}

		status := node.Status()
		wantRole := Follower
		if id == leader {
			wantRole = Leader
		}

		if status.Role != wantRole || status.LeaderID != leader || status.Term != term {
			t.Fatalf("node %s: status %+v, want %s in term %d with leader %s", id, status, wantRole, term, leader)
		}
	}
}

func TestNodeClusterElectsAndReplacesLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network, cancels, done := startMemoryCluster(t)
		defer stopMemoryCluster(cancels)

		time.Sleep(150 * time.Millisecond)
		requireLeader(t, network, "a", 1)
		time.Sleep(time.Second)
		requireLeader(t, network, "a", 1)
		cancels["a"]()
		if err := <-done["a"]; err != nil {
			t.Fatal(err)
		}

		time.Sleep(250 * time.Millisecond)
		requireLeader(t, network, "b", 2)
	})
}

func TestNodeClusterPartitionAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network, cancels, _ := startMemoryCluster(t)
		defer stopMemoryCluster(cancels)

		time.Sleep(150 * time.Millisecond)
		requireLeader(t, network, "a", 1)
		network.mu.Lock()
		network.isolated = "a"
		network.mu.Unlock()
		time.Sleep(250 * time.Millisecond)
		synctest.Wait()
		if old, next := network.nodes["a"].Status(), network.nodes["b"].Status(); old.Role != Leader ||
			next.Role != Leader ||
			next.Term <= old.Term {
			t.Fatalf("partition should yield leaders of different terms: old=%+v new=%+v", old, next)
		}

		network.mu.Lock()
		network.isolated = ""
		network.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		requireLeader(t, network, "b", 2)
	})
}

type slowTransport struct {
	active atomic.Int32
	max    atomic.Int32
	calls  atomic.Int32
}

func (s *slowTransport) RequestVote(ctx context.Context, _ NodeID, _ RequestVoteRequest) (RequestVoteResponse, error) {
	active := s.active.Add(1)
	defer s.active.Add(-1)

	s.calls.Add(1)
	for old := s.max.Load(); active > old; old = s.max.Load() {
		if s.max.CompareAndSwap(old, active) {
			break
		}
	}

	<-ctx.Done()

	return RequestVoteResponse{}, ctx.Err()
}

func (s *slowTransport) AppendEntries(
	ctx context.Context,
	_ NodeID,
	_ AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	<-ctx.Done()
	return AppendEntriesResponse{}, ctx.Err()
}

func TestSlowPeerIsBoundedAndShutdownCancelsRPC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &slowTransport{}
		node, err := NewNode(testConfig("a", []NodeID{"b"}), transport)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- node.Run(ctx) }()
		time.Sleep(955 * time.Millisecond)
		synctest.Wait()
		if transport.calls.Load() < 2 || transport.max.Load() != 1 {
			t.Fatalf("calls=%d, max in flight=%d", transport.calls.Load(), transport.max.Load())
		}

		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}

		if transport.active.Load() != 0 {
			t.Fatal("Run returned with an RPC still active")
		}

		if err := node.Run(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("second Run = %v", err)
		}
	})
}

func TestNodeValidatesRequestsAndLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node, err := NewNode(testConfig("a", []NodeID{"b", "c"}), &slowTransport{})
		if err != nil {
			t.Fatal(err)
		}

		valid := RequestVoteRequest{Term: 1, CandidateID: "b"}
		if _, err := node.RequestVote(context.Background(), valid); !errors.Is(err, ErrNotRunning) {
			t.Fatalf("RPC before Run: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- node.Run(ctx) }()
		synctest.Wait()
		for _, request := range []RequestVoteRequest{
			{Term: 0, CandidateID: "b"}, {Term: 9, CandidateID: "stranger"}, {Term: 9, CandidateID: "a"},
		} {
			if _, err := node.RequestVote(ctx, request); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("accepted invalid request %+v: %v", request, err)
			}
		}

		canceled, cancelRequest := context.WithCancel(ctx)
		cancelRequest()
		if _, err := node.RequestVote(canceled, valid); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled RPC: %v", err)
		}

		response, err := node.RequestVote(ctx, RequestVoteRequest{Term: 1, CandidateID: "c"})
		if err != nil || !response.VoteGranted {
			t.Fatalf("rejected valid vote after invalid/canceled requests: %+v, %v", response, err)
		}

		cancel()
		<-done
		if _, err := node.AppendEntries(
			context.Background(),
			AppendEntriesRequest{Term: 1, LeaderID: "b"},
		); !errors.Is(
			err,
			ErrNotRunning,
		) {
			t.Fatalf("RPC after shutdown: %v", err)
		}
	})
}

func TestConfigValidation(t *testing.T) {
	tests := map[string]func(*Config){
		"empty ID":         func(c *Config) { c.ID = "" },
		"self peer":        func(c *Config) { c.Peers = []NodeID{"a"} },
		"duplicate peer":   func(c *Config) { c.Peers = []NodeID{"b", "b"} },
		"empty peer":       func(c *Config) { c.Peers = []NodeID{""} },
		"zero election":    func(c *Config) { c.ElectionTimeoutMin = 0 },
		"no randomization": func(c *Config) { c.ElectionTimeoutMax = c.ElectionTimeoutMin },
		"zero heartbeat":   func(c *Config) { c.HeartbeatInterval = 0 },
		"slow heartbeat":   func(c *Config) { c.HeartbeatInterval = c.ElectionTimeoutMin },
		"zero RPC timeout": func(c *Config) { c.RPCTimeout = 0 },
		"long RPC timeout": func(c *Config) { c.RPCTimeout = c.ElectionTimeoutMin },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig("a", []NodeID{"b"})
			mutate(&cfg)
			if _, err := NewNode(cfg, &slowTransport{}); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}

	if _, err := NewNode(DefaultConfig("a", nil), nil); err == nil {
		t.Fatal("nil transport accepted")
	}
}

func TestRejectedRequestsDoNotDelayElection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node, err := NewNode(testConfig("a", []NodeID{"b", "c"}), &slowTransport{})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			if err := node.Run(ctx); err != nil {
				t.Error(err)
			}
		}()
		synctest.Wait()
		time.Sleep(10 * time.Millisecond)
		if response, err := node.RequestVote(
			ctx,
			RequestVoteRequest{Term: 2, CandidateID: "b"},
		); err != nil ||
			!response.VoteGranted {
			t.Fatalf("initial vote: %+v, %v", response, err)
		}

		time.Sleep(40 * time.Millisecond)
		if response, err := node.RequestVote(
			ctx,
			RequestVoteRequest{Term: 2, CandidateID: "c"},
		); err != nil ||
			response.VoteGranted {
			t.Fatalf("second candidate: %+v, %v", response, err)
		}

		time.Sleep(40 * time.Millisecond)
		if response, err := node.AppendEntries(
			ctx,
			AppendEntriesRequest{Term: 1, LeaderID: "c"},
		); err != nil ||
			response.Success {
			t.Fatalf("stale heartbeat: %+v, %v", response, err)
		}

		time.Sleep(21 * time.Millisecond)
		synctest.Wait()
		if state := node.Status(); state.Role != Candidate || state.Term != 3 {
			t.Fatalf("rejected request delayed election: %+v", state)
		}
	})
}

func TestHeartbeatInvalidatesAnAlreadyReceivedTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node, err := NewNode(testConfig("a", []NodeID{"b"}), &slowTransport{})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			if err := node.Run(ctx); err != nil {
				t.Error(err)
			}
		}()
		synctest.Wait()
		time.Sleep(90 * time.Millisecond)
		if _, err := node.AppendEntries(ctx, AppendEntriesRequest{Term: 1, LeaderID: "b"}); err != nil {
			t.Fatal(err)
		}

		time.Sleep(10 * time.Millisecond)
		// Simulate a timeout already consumed by Run before the heartbeat acquired mu.
		if err := node.handleElectionTimeout(ctx); err != nil {
			t.Fatal(err)
		}

		if state := node.Status(); state.Role != Follower || state.Term != 1 {
			t.Fatalf("stale timeout started an election: %+v", state)
		}

		time.Sleep(91 * time.Millisecond)
		synctest.Wait()
		if state := node.Status(); state.Role != Candidate || state.Term != 2 {
			t.Fatalf("fresh timeout did not start election: %+v", state)
		}
	})
}
