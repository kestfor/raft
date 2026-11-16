package raft

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kestfor/raft/pkg/wal"
)

type stateMachineFunc func(Entry) (any, error)

func (f stateMachineFunc) Do(entry Entry) (any, error) { return f(entry) }

func startStateMachineNode(t *testing.T, cfg Config) (*Node, context.CancelFunc, <-chan error) {
	t.Helper()
	n, err := NewNode(cfg, &slowTransport{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	synctest.Wait()
	t.Cleanup(cancel)
	return n, cancel, done
}

func stopStateMachineNode(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestStateMachineAppliesOnlyCommittedEntriesInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			n       *Node
			applied []uint64
		)
		cfg := testConfig("a", []NodeID{"b"})
		cfg.StateMachine = stateMachineFunc(func(entry Entry) (any, error) {
			// Status takes mu: callbacks must be able to call it without deadlock.
			if entry.Index > n.Status().CommitIndex {
				t.Errorf("applied uncommitted entry %d", entry.Index)
			}
			applied = append(applied, entry.Index)
			if len(entry.Data) > 0 {
				entry.Data[0] = 'X' // The callback must not alias the Raft log.
			}
			return nil, nil
		})
		n, cancel, done := startStateMachineNode(t, cfg)
		defer stopStateMachineNode(t, cancel, done)

		appendEntries(t, n, AppendEntriesRequest{
			Term: 1, LeaderID: "b",
			Entries: []Entry{
				{Index: 1, Term: 1, NoOp: true},
				{Index: 2, Term: 1, Data: []byte("first")},
				{Index: 3, Term: 1}, // Empty data is still an application command.
				{Index: 4, Term: 1, Data: []byte("uncommitted")},
			},
		})
		synctest.Wait()
		if len(applied) != 0 || n.Status().AppliedIndex != 0 {
			t.Fatal("applied entries before commit")
		}

		for range 2 { // Repeated commit notifications must not repeat Do.
			appendEntries(t, n, AppendEntriesRequest{
				Term: 1, LeaderID: "b", PrevLogIndex: 4, PrevLogTerm: 1, LeaderCommit: 3,
			})
			synctest.Wait()
		}
		if !reflect.DeepEqual(applied, []uint64{2, 3}) || n.Status().AppliedIndex != 3 {
			t.Fatalf("applied=%v, status=%+v", applied, n.Status())
		}
		n.mu.Lock()
		first := string(n.log.entries[2].Data)
		n.mu.Unlock()
		if first != "first" {
			t.Fatal("state machine modified the Raft log")
		}
	})
}

func TestStateMachineReplicatesAcrossLeadershipChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		applied := make(map[NodeID]*[]string)
		network, cancels, _ := startMemoryCluster(t, func(cfg *Config) {
			commands := new([]string)
			applied[cfg.ID] = commands
			cfg.StateMachine = stateMachineFunc(func(entry Entry) (any, error) {
				*commands = append(*commands, string(entry.Data))
				return len(*commands), nil
			})
		})
		defer stopMemoryCluster(cancels)
		time.Sleep(150 * time.Millisecond)
		index, result, err := network.nodes["a"].Propose(t.Context(), []byte("first"))
		if err != nil || result != 1 {
			t.Fatalf("first result: index=%d result=%v err=%v", index, result, err)
		}

		network.mu.Lock()
		network.isolated = "a"
		network.mu.Unlock()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()
		if _, _, err := network.nodes["a"].Propose(
			ctx,
			[]byte("discarded"),
		); !errors.Is(
			err,
			context.DeadlineExceeded,
		) {
			t.Fatalf("isolated proposal: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
		if _, result, err := network.nodes["b"].Propose(t.Context(), []byte("second")); err != nil || result != 2 {
			t.Fatalf("second result: %v, %v", result, err)
		}
		network.mu.Lock()
		network.isolated = ""
		network.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		for id, commands := range applied {
			if !reflect.DeepEqual(*commands, []string{"first", "second"}) {
				t.Errorf("node %s applied %v", id, *commands)
			}
			status := network.nodes[id].Status()
			if status.AppliedIndex != status.CommitIndex {
				t.Errorf("node %s: %+v", id, status)
			}
		}
	})
}

func TestProposeWaitsForApplicationAfterStepdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls int
		cfg := testConfig("a", nil)
		cfg.StateMachine = stateMachineFunc(func(entry Entry) (any, error) {
			calls++
			if string(entry.Data) == "first" {
				<-release
			}
			return calls, nil
		})
		n, cancel, done := startStateMachineNode(t, cfg)
		defer stopStateMachineNode(t, cancel, done)
		time.Sleep(150 * time.Millisecond)

		type response struct {
			value any
			err   error
		}
		first := make(chan response, 1)
		go func() {
			_, value, err := n.Propose(t.Context(), []byte("first"))
			first <- response{value, err}
		}()
		synctest.Wait()
		secondResult := make(chan response, 1)
		go func() {
			_, value, err := n.Propose(t.Context(), []byte("second"))
			secondResult <- response{value, err}
		}()
		synctest.Wait()
		second := n.Status().LastLogIndex
		if status := n.Status(); status.CommitIndex != second || status.AppliedIndex >= second-1 {
			t.Fatalf("unexpected progress while Do is blocked: %+v", status)
		}
		select {
		case got := <-first:
			t.Fatalf("returned before application: %+v", got)
		default:
		}

		n.mu.Lock()
		err := n.becomeFollower(t.Context(), 2, "")
		n.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		close(release)
		if got := <-first; got.err != nil || got.value != 1 {
			t.Fatalf("committed proposal lost its result after stepdown: %+v", got)
		}
		if got := <-secondResult; got.err != nil || got.value != 2 {
			t.Fatalf("second proposal result: %+v", got)
		}
		synctest.Wait()
		if calls != 2 || n.Status().AppliedIndex != second {
			t.Fatalf("calls=%d, status=%+v", calls, n.Status())
		}
	})
}

func TestStateMachineFailureStopsNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		applyErr := errors.New("cannot apply command")
		var calls int
		cfg := testConfig("a", nil)
		cfg.StateMachine = stateMachineFunc(func(Entry) (any, error) {
			calls++
			return nil, applyErr
		})
		n, cancel, done := startStateMachineNode(t, cfg)
		defer cancel()
		time.Sleep(150 * time.Millisecond)
		index, value, err := n.Propose(t.Context(), []byte("bad"))
		if index != 2 || value != nil || !errors.Is(err, applyErr) {
			t.Fatalf("result: index=%d value=%v err=%v", index, value, err)
		}
		if err := <-done; !errors.Is(err, applyErr) {
			t.Fatalf("Run: %v", err)
		}
		if calls != 1 || n.Status().AppliedIndex != 1 {
			t.Fatalf("calls=%d, status=%+v", calls, n.Status())
		}
		if _, _, err := n.Propose(t.Context(), nil); !errors.Is(err, applyErr) {
			t.Fatalf("failed node accepted a proposal: %v", err)
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if len(n.log.pending) != 0 {
			t.Fatal("retained completed proposal waiter")
		}
	})
}

func TestStateMachineContinuesAfterProposalCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls int
		cfg := testConfig("a", nil)
		cfg.StateMachine = stateMachineFunc(func(Entry) (any, error) {
			<-release
			calls++
			return calls, nil
		})
		n, cancel, done := startStateMachineNode(t, cfg)
		defer stopStateMachineNode(t, cancel, done)
		time.Sleep(150 * time.Millisecond)
		ctx, cancelRequest := context.WithTimeout(t.Context(), 5*time.Millisecond)
		defer cancelRequest()
		index, result, err := n.Propose(ctx, []byte("command"))
		if index != 2 || result != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("index=%d result=%v err=%v", index, result, err)
		}
		n.mu.Lock()
		pending := len(n.log.pending)
		n.mu.Unlock()
		if pending != 0 {
			t.Fatal("retained canceled proposal waiter")
		}
		close(release)
		synctest.Wait()
		if calls != 1 || n.Status().AppliedIndex != index {
			t.Fatalf("cancellation prevented application: calls=%d status=%+v", calls, n.Status())
		}
	})
}

func TestStateMachineShutdownWaitsForActiveCommand(t *testing.T) {
	for _, tt := range []struct {
		name    string
		err     error
		applied uint64
	}{
		{name: "success", applied: 2},
		{name: "failure", err: errors.New("apply failed during shutdown"), applied: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				var calls int
				cfg := testConfig("a", nil)
				cfg.StateMachine = stateMachineFunc(func(Entry) (any, error) {
					calls++
					<-release
					return nil, tt.err
				})
				n, cancel, done := startStateMachineNode(t, cfg)
				time.Sleep(150 * time.Millisecond)
				proposals := make(chan error, 2)
				go func() {
					_, _, err := n.Propose(t.Context(), []byte("first"))
					proposals <- err
				}()
				synctest.Wait()
				go func() {
					_, _, err := n.Propose(t.Context(), []byte("second"))
					proposals <- err
				}()
				synctest.Wait()
				cancel()
				synctest.Wait()
				for range 2 {
					if err := <-proposals; !errors.Is(err, ErrNotRunning) {
						t.Fatalf("pending proposal on shutdown: %v", err)
					}
				}
				select {
				case err := <-done:
					t.Fatalf("Run returned while Do was active: %v", err)
				default:
				}
				close(release)
				if err := <-done; !errors.Is(err, tt.err) {
					t.Fatalf("Run error=%v, want %v", err, tt.err)
				}
				if calls != 1 || n.Status().AppliedIndex != tt.applied {
					t.Fatalf("started another command during shutdown: calls=%d status=%+v", calls, n.Status())
				}
			})
		})
	}
}

func TestProposeWithDefaultNoOpStateMachine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, cancel, done := startStateMachineNode(t, testConfig("a", nil))
		defer stopStateMachineNode(t, cancel, done)
		time.Sleep(150 * time.Millisecond)
		if index, value, err := n.Propose(t.Context(), nil); index != 2 || value != nil || err != nil {
			t.Fatalf("index=%d value=%v err=%v", index, value, err)
		}
		if status := n.Status(); status.CommitIndex != 2 || status.AppliedIndex != 2 {
			t.Fatalf("unexpected status with default state machine: %+v", status)
		}
	})
}

func TestStateMachineRecoversFromWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	for attempt := range 2 {
		journal, err := wal.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		var applied []string
		cfg := testConfig("a", nil)
		cfg.Storage = journal
		cfg.StateMachine = stateMachineFunc(func(entry Entry) (any, error) {
			applied = append(applied, string(entry.Data))
			return len(applied), nil
		})
		synctest.Test(t, func(t *testing.T) {
			n, cancel, done := startStateMachineNode(t, cfg)
			defer stopStateMachineNode(t, cancel, done)
			if n.Status().AppliedIndex != 0 {
				t.Fatal("replayed WAL before commitment was reestablished")
			}
			time.Sleep(150 * time.Millisecond)
			if attempt == 0 {
				if _, value, err := n.Propose(t.Context(), []byte("durable")); err != nil || value != 1 {
					t.Fatalf("result=%v err=%v", value, err)
				}
			}
			synctest.Wait()
			if !reflect.DeepEqual(applied, []string{"durable"}) {
				t.Fatalf("attempt %d: applied=%v", attempt, applied)
			}
		})
		if err := journal.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
