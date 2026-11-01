package raft

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestReadIndexQuorumAndPartition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network, cancels, _ := startMemoryCluster(t)
		defer stopMemoryCluster(cancels)
		time.Sleep(150 * time.Millisecond)
		n := network.nodes["a"]
		before := n.Status().LastLogIndex
		for range 3 {
			index, err := n.ReadIndex(t.Context())
			if err != nil || index != before {
				t.Fatalf("read=%d %v", index, err)
			}
		}

		if n.Status().LastLogIndex != before {
			t.Fatal("read appended a log entry")
		}

		network.mu.Lock()
		network.isolated = "a"
		network.mu.Unlock()
		time.Sleep(250 * time.Millisecond)
		ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
		defer cancel()
		if _, err := n.ReadIndex(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("isolated leader read: %v", err)
		}

		if _, err := network.nodes["b"].ReadIndex(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

type readReplyTransport struct{}

func (readReplyTransport) RequestVote(context.Context, NodeID, RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, nil
}

func (readReplyTransport) AppendEntries(
	_ context.Context,
	_ NodeID,
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func TestReadIndexIgnoresOldRPCAndWaitsForCurrentTermCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, stoppedCtx := newElectionTestNode(t, "a", []NodeID{"b", "c"})
		n.transport = readReplyTransport{}
		n.election.currentTerm = 1
		if err := n.becomeLeader(stoppedCtx); err != nil {
			t.Fatal(err)
		}

		done := make(chan error, 1)
		go func() { _, err := n.ReadIndex(t.Context()); done <- err }()
		synctest.Wait()
		if n.log.readRound != 0 {
			t.Fatal("started read round before current-term commit")
		}

		n.mu.Lock()
		n.log.commitIndex = 1
		n.notifyLogChange()
		n.mu.Unlock()
		synctest.Wait()
		round := n.log.readRound
		if round == 0 {
			t.Fatal("no read round started")
		}

		req := AppendEntriesRequest{Term: 1, LeaderID: "a", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1}
		n.sendAppendEntries(stoppedCtx, "b", req, 0)
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("old RPC satisfied read: %v", err)
		default:
		}

		n.sendAppendEntries(stoppedCtx, "b", req, round)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestReadIndexWaitersCancelStepDownAndStop(t *testing.T) {
	for _, action := range []string{"cancel", "stepdown", "stop"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				network, cancels, _ := startMemoryCluster(t)
				defer stopMemoryCluster(cancels)
				time.Sleep(150 * time.Millisecond)
				n := network.nodes["a"]
				network.mu.Lock()
				network.isolated = "a"
				network.mu.Unlock()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 4)
				for range 4 {
					go func() { _, err := n.ReadIndex(ctx); done <- err }()
				}

				synctest.Wait()
				switch action {
				case "cancel":
					cancel()
				case "stepdown":
					appendEntries(t, n, AppendEntriesRequest{Term: 2, LeaderID: "b"})
				case "stop":
					cancels["a"]()
				}

				for range 4 {
					if err := <-done; err == nil {
						t.Fatal("read unexpectedly succeeded")
					}
				}
			})
		})
	}
}

func TestCommittedEntriesOwnershipAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network, cancels, _ := startMemoryCluster(t)
		defer stopMemoryCluster(cancels)
		time.Sleep(150 * time.Millisecond)
		index := propose(t, network.nodes["a"], "value")
		entries, err := network.nodes["b"].CommittedEntries(t.Context(), index-1)
		if err != nil || len(entries) != 1 {
			t.Fatalf("entries=%v err=%v", entries, err)
		}

		entries[0].Data[0] = 'X'
		again, err := network.nodes["b"].CommittedEntries(t.Context(), index-1)
		if err != nil || string(again[0].Data) != "value" {
			t.Fatal("caller mutated committed entries")
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := network.nodes["b"].CommittedEntries(ctx, index); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}
