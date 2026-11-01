package raft

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kestfor/raft/pkg/wal"
)

func TestAppendEntriesConsistencyAndCommit(t *testing.T) {
	for _, tt := range []struct {
		name    string
		req     AppendEntriesRequest
		terms   []uint64
		commit  uint64
		success bool
	}{
		{"missing prefix", AppendEntriesRequest{PrevLogIndex: 5, PrevLogTerm: 2}, []uint64{1, 1, 2, 2}, 0, false},
		{"wrong prefix term", AppendEntriesRequest{PrevLogIndex: 2, PrevLogTerm: 2}, []uint64{1, 1, 2, 2}, 0, false},
		{"heartbeat keeps suffix", AppendEntriesRequest{PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 4}, []uint64{1, 1, 2, 2}, 2, true},
		{"empty prefix commits nothing", AppendEntriesRequest{LeaderCommit: 4}, []uint64{1, 1, 2, 2}, 0, true},
		{"duplicate keeps suffix", AppendEntriesRequest{PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{{Index: 2, Term: 1}}}, []uint64{1, 1, 2, 2}, 0, true},
		{"replace conflict", AppendEntriesRequest{PrevLogIndex: 2, PrevLogTerm: 1, Entries: []Entry{{Index: 3, Term: 3}}, LeaderCommit: 9}, []uint64{1, 1, 3}, 3, true},
		{"extend matching log", AppendEntriesRequest{PrevLogIndex: 3, PrevLogTerm: 2, Entries: []Entry{{Index: 4, Term: 2}, {Index: 5, Term: 3}}, LeaderCommit: 4}, []uint64{1, 1, 2, 2, 3}, 4, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n, _ := newElectionTestNode(t, "a", []NodeID{"b", "c"})
			n.election.currentTerm = 3
			for i, term := range []uint64{1, 1, 2, 2} {
				n.log.entries = append(n.log.entries, Entry{Index: uint64(i + 1), Term: term})
			}
			tt.req.Term, tt.req.LeaderID = 3, "b"
			for range 2 { // Retransmission is idempotent, even after a tail replacement.
				response := appendEntries(t, n, tt.req)
				if response.Success != tt.success || n.log.commitIndex != tt.commit {
					t.Fatalf("response=%+v, commit=%d", response, n.log.commitIndex)
				}
				var terms []uint64
				for _, entry := range n.log.entries[1:] {
					terms = append(terms, entry.Term)
				}
				if !slices.Equal(terms, tt.terms) {
					t.Fatalf("terms=%v, want %v", terms, tt.terms)
				}
			}
		})
	}
}

func TestAppendEntriesRejectsInvalidAndCommittedConflicts(t *testing.T) {
	for _, req := range []AppendEntriesRequest{
		{PrevLogIndex: 1},
		{PrevLogTerm: 1},
		{PrevLogTerm: 4, PrevLogIndex: 1},
		{Entries: []Entry{{Index: 2, Term: 1}}},
		{Entries: []Entry{{Index: 1, Term: 4}}},
		{Entries: []Entry{{Index: 1, Term: 0}}},
		{Entries: []Entry{{Index: 1, Term: 2}, {Index: 2, Term: 1}}},
		{Entries: []Entry{{Index: 1, Term: 2, NoOp: true, Data: []byte("bad")}}},
		{Entries: []Entry{{Index: 1, Term: 2, Data: make([]byte, MaxCommandSize+1)}}},
		{Entries: []Entry{{Index: 1, Term: 2}}}, // Committed conflict.
	} {
		n, _ := newElectionTestNode(t, "a", []NodeID{"b"})
		n.election.currentTerm = 3
		n.log.entries = append(n.log.entries, Entry{Index: 1, Term: 1})
		n.log.commitIndex = 1
		req.Term, req.LeaderID = 3, "b"
		if response, err := n.AppendEntries(t.Context(), req); !errors.Is(err, ErrInvalidRequest) || response.Success {
			t.Fatalf("accepted invalid request: response=%+v, err=%v", response, err)
		}
		if n.lastIndex() != 1 || n.log.entries[1].Term != 1 || n.log.commitIndex != 1 {
			t.Fatal("invalid request changed log")
		}
	}
}

func TestRequestVoteComparesLastTermBeforeLength(t *testing.T) {
	for _, tt := range []struct {
		term, index uint64
		granted     bool
	}{
		{1, 100, false}, {2, 1, false}, {2, 2, true}, {2, 3, true}, {3, 1, true},
	} {
		n, _ := newElectionTestNode(t, "a", []NodeID{"b"})
		n.log.entries = append(n.log.entries, Entry{Index: 1, Term: 1}, Entry{Index: 2, Term: 2})
		n.election.currentTerm = 2
		response := requestVote(
			t,
			n,
			RequestVoteRequest{Term: 4, CandidateID: "b", LastLogIndex: tt.index, LastLogTerm: tt.term},
		)
		if response.VoteGranted != tt.granted || response.Term != 4 {
			t.Fatalf("term=%d index=%d: %+v", tt.term, tt.index, response)
		}
	}
}

func TestLeaderCommitsOnlyCurrentTermWithMajority(t *testing.T) {
	n, ctx := newElectionTestNode(t, "a", []NodeID{"b", "c", "d"})
	n.election.currentTerm = 2
	n.log.entries = append(n.log.entries, Entry{Index: 1, Term: 1})
	if err := n.becomeLeader(ctx); err != nil {
		t.Fatal(err)
	}
	if n.lastIndex() != 2 || !n.log.entries[2].NoOp {
		t.Fatal("leader did not append no-op")
	}
	n.log.matchIndex["b"], n.log.matchIndex["c"] = 1, 1
	n.advanceCommit()
	if n.log.commitIndex != 0 {
		t.Fatal("committed an old term by counting replicas")
	}
	n.log.matchIndex["b"] = 2
	n.advanceCommit()
	if n.log.commitIndex != 0 {
		t.Fatal("two out of four is not a majority")
	}
	n.log.matchIndex["c"] = 2
	n.advanceCommit()
	if n.log.commitIndex != 2 {
		t.Fatal("current-term majority did not commit the prefix")
	}
	old := AppendEntriesRequest{Term: 1}
	n.handleAppendResponse(ctx, "b", old, AppendEntriesResponse{Term: 2})
	if n.log.matchIndex["b"] != 2 {
		t.Fatal("old request changed replication progress")
	}
	n.handleAppendResponse(ctx, "b", old, AppendEntriesResponse{Term: 3})
	if n.election.role != Follower || n.election.currentTerm != 3 {
		t.Fatal("ignored higher term in stale response")
	}
}

func propose(t *testing.T, n *Node, data string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	index, err := n.Propose(ctx, []byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func TestReplicationPartitionConflictAndCatchUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network, cancels, _ := startMemoryCluster(t)
		defer stopMemoryCluster(cancels)
		time.Sleep(150 * time.Millisecond)
		index := propose(t, network.nodes["a"], "committed")
		synctest.Wait()
		network.mu.Lock()
		network.isolated = "a"
		network.mu.Unlock()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()
		uncommitted, err := network.nodes["a"].Propose(ctx, []byte("isolated"))
		if !errors.Is(err, context.DeadlineExceeded) || uncommitted <= index {
			t.Fatalf("isolated proposal: %d %v", uncommitted, err)
		}
		if network.nodes["a"].Status().CommitIndex != index {
			t.Fatal("isolated leader committed a command")
		}
		time.Sleep(250 * time.Millisecond)
		last := propose(t, network.nodes["b"], "new leader")
		network.mu.Lock()
		network.isolated = ""
		network.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		for id, n := range network.nodes {
			n.mu.Lock()
			if n.log.commitIndex != last || n.lastIndex() != last || string(n.log.entries[index].Data) != "committed" ||
				string(n.log.entries[last].Data) != "new leader" {
				t.Errorf("node %s: status=%+v log=%+v", id, n.status(), n.log.entries)
			}
			for _, entry := range n.log.entries {
				if string(entry.Data) == "isolated" {
					t.Errorf("node %s kept conflicting tail", id)
				}
			}
			n.mu.Unlock()
		}
	})
}

func TestLaggingFollowerCatchesUpAcrossBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network, cancels, _ := startMemoryCluster(t)
		defer stopMemoryCluster(cancels)
		time.Sleep(150 * time.Millisecond)
		network.mu.Lock()
		network.isolated = "c"
		network.mu.Unlock()
		var last uint64
		for range maxAppendEntries*2 + 3 {
			last = propose(t, network.nodes["a"], "batch")
		}
		// Also exercise the byte limit independently of the entry count.
		for range 3 {
			last = propose(t, network.nodes["a"], string(bytes.Repeat([]byte{0xff}, MaxCommandSize)))
		}
		network.mu.Lock()
		network.isolated = ""
		network.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		for id, n := range network.nodes {
			if s := n.Status(); s.CommitIndex != last || s.LastLogIndex != last {
				t.Fatalf("node %s: %+v", id, s)
			}
		}
		if !reflect.DeepEqual(network.nodes["a"].log.entries, network.nodes["c"].log.entries) {
			t.Fatal("follower log differs")
		}
	})
}

func TestProposeWakesAllWaiters(t *testing.T) {
	for _, stop := range []string{"commit", "stepdown", "shutdown"} {
		t.Run(stop, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				network, cancels, _ := startMemoryCluster(t)
				defer stopMemoryCluster(cancels)
				time.Sleep(150 * time.Millisecond)
				network.mu.Lock()
				network.isolated = "a"
				network.mu.Unlock()
				n := network.nodes["a"]
				const proposals = 8
				done := make(chan error, proposals)
				for range proposals {
					go func() { _, err := n.Propose(t.Context(), []byte("pending")); done <- err }()
				}
				synctest.Wait()
				var want error
				switch stop {
				case "commit":
					network.mu.Lock()
					network.isolated = ""
					network.mu.Unlock()
				case "shutdown":
					cancels["a"]()
					want = ErrNotRunning
				case "stepdown":
					appendEntries(t, n, AppendEntriesRequest{Term: 2, LeaderID: "b"})
					want = ErrLeadershipLost
				}
				for range proposals {
					if err := <-done; !errors.Is(err, want) {
						t.Fatalf("proposal error = %v, want %v", err, want)
					}
				}
			})
		})
	}
}

func TestWALRestoresVoteAndReplacedLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	open := func() (*Node, *wal.Log) {
		journal, err := wal.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = journal.Close() })
		cfg := testConfig("a", []NodeID{"b", "c"})
		cfg.Storage = journal
		n, err := NewNode(cfg, &slowTransport{})
		if err != nil {
			t.Fatal(err)
		}
		n.runtime.running = true
		n.runtime.electionTimer = time.NewTimer(time.Hour)
		t.Cleanup(func() { n.runtime.electionTimer.Stop() })
		return n, journal
	}
	n, journal := open()
	appendEntries(
		t,
		n,
		AppendEntriesRequest{
			Term:     1,
			LeaderID: "b",
			Entries:  []Entry{{Index: 1, Term: 1, Data: []byte("kept")}, {Index: 2, Term: 1, Data: []byte("removed")}},
		},
	)
	appendEntries(
		t,
		n,
		AppendEntriesRequest{
			Term:         2,
			LeaderID:     "b",
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries:      []Entry{{Index: 2, Term: 2, Data: []byte("replacement")}},
			LeaderCommit: 2,
		},
	)
	if !requestVote(t, n, RequestVoteRequest{Term: 3, CandidateID: "b", LastLogIndex: 2, LastLogTerm: 2}).VoteGranted {
		t.Fatal("vote rejected")
	}
	want := cloneEntries(n.log.entries)
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	n, journal = open()
	if n.election.currentTerm != 3 || n.election.votedFor != "b" || n.log.commitIndex != 0 ||
		!reflect.DeepEqual(n.log.entries, want) {
		t.Fatalf("restored state: %+v, vote=%s, log=%+v", n.status(), n.election.votedFor, n.log.entries)
	}
	if requestVote(t, n, RequestVoteRequest{Term: 3, CandidateID: "c", LastLogIndex: 2, LastLogTerm: 2}).VoteGranted {
		t.Fatal("voted twice after restart")
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	cfg := testConfig("other-node", nil)
	cfg.Storage = journal
	if _, err := NewNode(cfg, &slowTransport{}); err == nil {
		t.Fatal("loaded another node's WAL")
	}
}

type failingStorage struct{ err error }

func (s *failingStorage) Append([]byte) error           { return s.err }
func (*failingStorage) Replay(func([]byte) error) error { return nil }

func TestStorageFailureStopsNodeBeforeAcknowledgment(t *testing.T) {
	for _, operation := range []string{"vote", "append", "propose", "election"} {
		t.Run(operation, func(t *testing.T) {
			n, ctx := newElectionTestNode(t, "a", []NodeID{"b"})
			n.election.currentTerm = 1
			storageErr := errors.New("disk failure")
			n.config.Storage = &failingStorage{err: storageErr}
			var err error
			switch operation {
			case "vote":
				var response RequestVoteResponse
				response, err = n.RequestVote(t.Context(), RequestVoteRequest{Term: 1, CandidateID: "b"})
				if response.VoteGranted {
					t.Fatal("acknowledged failed vote")
				}
			case "append":
				var response AppendEntriesResponse
				response, err = n.AppendEntries(
					t.Context(),
					AppendEntriesRequest{Term: 1, LeaderID: "b", Entries: []Entry{{Index: 1, Term: 1}}},
				)
				if response.Success {
					t.Fatal("acknowledged failed append")
				}
			case "propose":
				n.election.role = Leader
				_, err = n.Propose(t.Context(), []byte("command"))
			case "election":
				err = n.startElection(ctx)
			}
			if !errors.Is(err, storageErr) || n.runtime.running || n.election.votedFor != "" || n.lastIndex() != 0 ||
				n.election.currentTerm != 1 {
				t.Fatalf("failure published state: err=%v status=%+v running=%v", err, n.status(), n.runtime.running)
			}
			if _, err := n.Propose(t.Context(), nil); !errors.Is(err, storageErr) {
				t.Fatal("failed node accepted new work")
			}
		})
	}
}

func TestSingleNodeCommitsAndRecoversOldEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	for attempt := range 2 {
		journal, err := wal.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		cfg := testConfig("a", nil)
		cfg.Storage = journal
		n, err := NewNode(cfg, &slowTransport{})
		if err != nil {
			t.Fatal(err)
		}
		n.runtime.running = true
		n.runtime.electionTimer = time.NewTimer(time.Hour)
		if err := n.startElection(t.Context()); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			if index := propose(t, n, "durable"); index != 2 {
				t.Fatalf("index=%d", index)
			}
		} else if n.log.commitIndex != 3 || string(n.log.entries[2].Data) != "durable" {
			t.Fatalf("no-op did not commit restored prefix: %+v", n.status())
		}
		n.runtime.electionTimer.Stop()
		if err := journal.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProposeValidationAndOwnership(t *testing.T) {
	n, ctx := newElectionTestNode(t, "a", nil)
	if _, err := n.Propose(t.Context(), nil); !errors.Is(err, ErrNotLeader) {
		t.Fatal(err)
	}
	if err := n.startElection(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Propose(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := n.Propose(t.Context(), make([]byte, MaxCommandSize+1)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if n.lastIndex() != 1 {
		t.Fatal("rejected proposal changed log")
	}
	data := []byte("command")
	index, err := n.Propose(t.Context(), data)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 'X'
	if string(n.log.entries[index].Data) != "command" {
		t.Fatal("caller can mutate the log")
	}
}

func TestStorageFailureReturnsFromRunAndUnblocksPropose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network, cancels, done := startMemoryCluster(t)
		defer stopMemoryCluster(cancels)
		time.Sleep(150 * time.Millisecond)
		network.mu.Lock()
		network.isolated = "a"
		network.mu.Unlock()
		n := network.nodes["a"]
		pending := make(chan error, 1)
		go func() { _, err := n.Propose(t.Context(), []byte("pending")); pending <- err }()
		synctest.Wait()
		storageErr := errors.New("sync failed")
		n.mu.Lock()
		n.config.Storage = &failingStorage{err: storageErr}
		n.mu.Unlock()
		if _, err := n.Propose(t.Context(), []byte("failure")); !errors.Is(err, storageErr) {
			t.Fatalf("propose error=%v", err)
		}
		if err := <-pending; !errors.Is(err, storageErr) {
			t.Fatalf("pending error=%v", err)
		}
		if err := <-done["a"]; !errors.Is(err, storageErr) {
			t.Fatalf("Run error=%v", err)
		}
	})
}

func TestConsistencyRejectionResetsElectionTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, ctx := newElectionTestNode(t, "a", []NodeID{"b"})
		n.election.currentTerm = 1
		n.runtime.electionDeadline = time.Now().Add(50 * time.Millisecond)
		time.Sleep(40 * time.Millisecond)
		response := appendEntries(t, n, AppendEntriesRequest{Term: 1, LeaderID: "b", PrevLogIndex: 3, PrevLogTerm: 1})
		if response.Success {
			t.Fatal("accepted missing prefix")
		}
		time.Sleep(20 * time.Millisecond)
		if err := n.handleElectionTimeout(ctx); err != nil {
			t.Fatal(err)
		}
		if n.election.role != Follower {
			t.Fatal("consistency rejection did not reset election timer")
		}
	})
}
