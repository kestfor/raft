package raft

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type batchStorage struct {
	mu      sync.Mutex
	records [][]byte
	err     error
	started chan struct{}
	release chan struct{}
}

func (s *batchStorage) Append(data []byte) error {
	if s.started != nil {
		s.started <- struct{}{}
		<-s.release
	}

	if s.err != nil {
		return s.err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.records = append(s.records, slices.Clone(data))
	return nil
}

func (s *batchStorage) Replay(visit func([]byte) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, data := range s.records {
		if err := visit(slices.Clone(data)); err != nil {
			return err
		}
	}

	return nil
}

func (s *batchStorage) commands(t *testing.T) [][]Entry {
	t.Helper()
	var batches [][]Entry
	if err := s.Replay(func(data []byte) error {
		var record diskRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return err
		}

		if len(record.Entries) > 0 && !record.Entries[0].NoOp {
			batches = append(batches, record.Entries)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	return batches
}

func batchLeader(t *testing.T) (*Node, *batchStorage) {
	t.Helper()
	n, ctx := newElectionTestNode(t, "a", nil)
	storage := &batchStorage{}
	n.config.Storage = storage
	if err := n.startElection(ctx); err != nil {
		t.Fatal(err)
	}

	return n, storage
}

func TestProposalBatchTimeoutAndReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, storage := batchLeader(t)
		done := make(chan proposalResult, 4)
		for range 4 {
			go func() {
				index, err := n.Propose(t.Context(), []byte("command"))
				done <- proposalResult{entry: Entry{Index: index}, err: err}
			}()
		}

		synctest.Wait()
		time.Sleep(proposalBatchDelay / 2)
		if len(storage.commands(t)) != 0 || n.Status().LastLogIndex != 1 {
			t.Fatal("batch flushed before deadline")
		}

		time.Sleep(proposalBatchDelay / 2)
		indexes := make([]uint64, 0, 4)
		for range 4 {
			result := <-done
			if result.err != nil {
				t.Fatal(result.err)
			}

			indexes = append(indexes, result.entry.Index)
		}

		slices.Sort(indexes)
		if !slices.Equal(indexes, []uint64{2, 3, 4, 5}) {
			t.Fatal(indexes)
		}

		batches := storage.commands(t)
		if len(batches) != 1 || len(batches[0]) != 4 {
			t.Fatalf("WAL batches: %v", batches)
		}

		restored, err := NewNode(n.config, &slowTransport{})
		if err != nil || restored.lastIndex() != 5 {
			t.Fatalf("replay: %v %v", restored, err)
		}

		for _, entry := range restored.log.entries[2:] {
			if string(entry.Data) != "command" {
				t.Fatal("lost batched command on replay")
			}
		}
	})
}

func TestProposalBatchCountLimitFlushesImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, storage := batchLeader(t)
		done := make(chan error, maxAppendEntries)
		start := time.Now()
		for range maxAppendEntries - 1 {
			go func() { _, err := n.Propose(t.Context(), nil); done <- err }()
		}

		synctest.Wait()
		if len(storage.commands(t)) != 0 {
			t.Fatal("early flush")
		}

		go func() { _, err := n.Propose(t.Context(), nil); done <- err }()
		for range maxAppendEntries {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}

		if time.Since(start) != 0 {
			t.Fatal("full batch waited for timer")
		}

		batches := storage.commands(t)
		if len(batches) != 1 || len(batches[0]) != maxAppendEntries {
			t.Fatalf("batches: %v", batches)
		}
	})
}

func TestProposalBatchByteLimit(t *testing.T) {
	for _, size := range []int{maxAppendBytes / 2, maxAppendBytes/2 + 1} {
		synctest.Test(t, func(t *testing.T) {
			n, storage := batchLeader(t)
			done := make(chan error, 2)
			for range 2 {
				go func() { _, err := n.Propose(t.Context(), make([]byte, size)); done <- err }()
			}

			for range 2 {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}

			batches := storage.commands(t)
			want := 1
			if size > maxAppendBytes/2 {
				want = 2
			}

			if len(batches) != want {
				t.Fatalf("size %d: %d batches, want %d", size, len(batches), want)
			}

			for _, batch := range batches {
				bytes := 0
				for _, entry := range batch {
					bytes += len(entry.Data)
				}

				if bytes > maxAppendBytes {
					t.Fatal("oversized WAL suffix")
				}
			}
		})
	}
}

func TestCanceledProposalIsExcludedBeforeFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, storage := batchLeader(t)
		ctx, cancel := context.WithCancel(t.Context())
		canceled := make(chan proposalResult, 1)
		go func() {
			index, err := n.Propose(ctx, []byte("canceled"))
			canceled <- proposalResult{entry: Entry{Index: index}, err: err}
		}()
		synctest.Wait()
		cancel()
		result := <-canceled
		if result.entry.Index != 0 || !errors.Is(result.err, context.Canceled) {
			t.Fatal(result)
		}

		index, err := n.Propose(t.Context(), []byte("kept"))
		if err != nil || index != 2 {
			t.Fatalf("index=%d err=%v", index, err)
		}

		batches := storage.commands(t)
		if len(batches) != 1 || len(batches[0]) != 1 || string(batches[0][0].Data) != "kept" {
			t.Fatal(batches)
		}
	})
}

func TestBatchWaitsForPersistenceAndPreservesIndexOnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, storage := batchLeader(t)
		storage.started = make(chan struct{})
		storage.release = make(chan struct{})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan proposalResult, 1)
		go func() {
			entry, err := n.appendCommand(ctx, []byte("command"))
			done <- proposalResult{entry: entry, err: err}
		}()
		<-storage.started
		select {
		case <-done:
			t.Fatal("acknowledged before fsync")
		default:
		}

		cancel()
		close(storage.release)
		result := <-done
		if result.err != nil || result.entry.Index != 2 {
			t.Fatalf("lost durable index: %+v", result)
		}
	})
}

func TestBatchStorageFailureRejectsAllWithoutPublishing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, storage := batchLeader(t)
		storage.err = errors.New("disk failure")
		done := make(chan proposalResult, 8)
		for range 8 {
			go func() {
				index, err := n.Propose(t.Context(), nil)
				done <- proposalResult{entry: Entry{Index: index}, err: err}
			}()
		}

		for range 8 {
			result := <-done
			if result.entry.Index != 0 || !errors.Is(result.err, storage.err) {
				t.Fatal(result)
			}
		}

		if n.lastIndex() != 1 || n.runtime.running || len(storage.commands(t)) != 0 {
			t.Fatal("failed batch published")
		}
	})
}

func TestPendingBatchRejectedOnStepdownAndOldTimerIsHarmless(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, storage := batchLeader(t)
		done := make(chan error, 1)
		go func() { _, err := n.Propose(t.Context(), []byte("old-term")); done <- err }()
		synctest.Wait()
		old := n.proposals
		n.mu.Lock()
		err := n.becomeFollower(t.Context(), 2, "")
		n.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		if err := <-done; !errors.Is(err, ErrLeadershipLost) {
			t.Fatal(err)
		}

		n.mu.Lock()
		err = n.startElection(t.Context())
		n.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		go func() { _, err := n.Propose(t.Context(), []byte("new-term")); done <- err }()
		synctest.Wait()
		n.mu.Lock()
		n.flushProposals(old)
		n.mu.Unlock()
		if len(storage.commands(t)) != 0 {
			t.Fatal("old timer flushed a new batch")
		}

		if err := <-done; err != nil {
			t.Fatal(err)
		}

		batches := storage.commands(t)
		if len(batches) != 1 || string(batches[0][0].Data) != "new-term" {
			t.Fatal(batches)
		}
	})
}

func TestShutdownRejectsPendingBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network, cancels, done := startMemoryCluster(t)
		defer stopMemoryCluster(cancels)
		time.Sleep(150 * time.Millisecond)
		n := network.nodes["a"]
		before := n.Status().LastLogIndex
		proposal := make(chan error, 1)
		go func() { _, err := n.Propose(t.Context(), nil); proposal <- err }()
		synctest.Wait()
		cancels["a"]()
		if err := <-done["a"]; err != nil {
			t.Fatal(err)
		}

		if err := <-proposal; !errors.Is(err, ErrNotRunning) {
			t.Fatal(err)
		}

		time.Sleep(2 * proposalBatchDelay)
		if n.Status().LastLogIndex != before {
			t.Fatal("batch wrote after shutdown")
		}
	})
}
