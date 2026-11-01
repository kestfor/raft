package raft

import (
	"context"
	"fmt"
	"slices"
	"time"
)

const proposalBatchDelay = time.Millisecond

type proposal struct {
	ctx  context.Context
	data []byte
	done chan proposalResult
}

type proposalResult struct {
	entry Entry
	err   error
}

// Only one bounded batch is pending; all fields are protected by Node.mu.
type proposalBatch struct {
	term     uint64
	bytes    int
	requests []*proposal
	timer    *time.Timer
}

// Propose appends a command on the leader and waits until it is committed by a
// majority. Success does not mean that a state machine has executed the command.
// After an append, errors return its index: cancellation or lost leadership does
// not roll it back, and it may still commit. Retries are not deduplicated.
func (n *Node) Propose(ctx context.Context, data []byte) (uint64, error) {
	entry, err := n.appendCommand(ctx, data)
	if err != nil {
		return 0, err
	}

	return entry.Index, n.waitForCommit(ctx, entry)
}

func (n *Node) appendCommand(ctx context.Context, data []byte) (Entry, error) {
	n.mu.Lock()

	if err := n.canPropose(ctx); err != nil {
		n.mu.Unlock()
		return Entry{}, err
	}

	if len(data) > MaxCommandSize {
		n.mu.Unlock()
		return Entry{}, fmt.Errorf("%w: command exceeds %d bytes", ErrInvalidRequest, MaxCommandSize)
	}

	if batch := n.proposals; batch != nil && batch.bytes+len(data) > maxAppendBytes {
		n.flushProposals(batch)
		if err := n.canPropose(ctx); err != nil {
			n.mu.Unlock()
			return Entry{}, err
		}
	}

	request := &proposal{ctx: ctx, data: slices.Clone(data), done: make(chan proposalResult, 1)}
	if n.proposals == nil {
		batch := &proposalBatch{term: n.election.currentTerm}
		n.proposals = batch
		batch.timer = time.AfterFunc(proposalBatchDelay, func() {
			n.mu.Lock()
			defer n.mu.Unlock()

			n.flushProposals(batch)
		})
	}

	batch := n.proposals
	batch.requests = append(batch.requests, request)
	batch.bytes += len(data)
	if len(batch.requests) == maxAppendEntries || batch.bytes == maxAppendBytes {
		n.flushProposals(batch)
	}

	n.mu.Unlock()

	select {
	case result := <-request.done:
		return result.entry, result.err
	case <-ctx.Done():
		// Synchronize with a flush already in progress: if it persisted this
		// request, preserve its index even when the caller cancels concurrently.
		n.mu.Lock()
		defer n.mu.Unlock()

		select {
		case result := <-request.done:
			return result.entry, result.err
		default:
			return Entry{}, ctx.Err()
		}
	}
}

// flushProposals requires mu. A stopped/expired timer cannot flush a newer batch.
func (n *Node) flushProposals(batch *proposalBatch) {
	if n.proposals != batch {
		return
	}

	if err := n.canPropose(context.Background()); err != nil {
		n.rejectProposals(err)
		return
	}

	if batch.term != n.election.currentTerm {
		n.rejectProposals(ErrLeadershipLost)
		return
	}

	n.proposals = nil
	batch.timer.Stop()

	entries := make([]Entry, 0, len(batch.requests))
	active := make([]*proposal, 0, len(batch.requests))
	for _, request := range batch.requests {
		if err := request.ctx.Err(); err != nil {
			request.done <- proposalResult{err: err}
			continue
		}

		entries = append(
			entries,
			Entry{Index: n.lastIndex() + uint64(len(entries)) + 1, Term: batch.term, Data: request.data},
		)
		active = append(active, request)
	}

	if len(entries) == 0 {
		return
	}

	// One durable WAL record represents the entire suffix. Neither the log nor
	// any request's index is published until this single save succeeds.
	err := n.save(batch.term, n.election.votedFor, entries)
	if err == nil {
		n.log.entries = append(n.log.entries, entries...)
		n.advanceCommit()
		n.wakeRun()
	}

	for i, request := range active {
		result := proposalResult{err: err}
		if err == nil {
			result.entry = entries[i]
		}

		request.done <- result
	}
}

// rejectProposals requires mu. After it returns, a timer callback can only no-op;
// it cannot touch Storage, including after Run has returned and Storage closes.
func (n *Node) rejectProposals(err error) {
	batch := n.proposals
	if batch == nil {
		return
	}

	n.proposals = nil
	batch.timer.Stop()

	for _, request := range batch.requests {
		request.done <- proposalResult{err: err}
	}
}

func (n *Node) waitForCommit(ctx context.Context, entry Entry) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for {
		if entry.Index <= n.log.commitIndex && n.log.entries[entry.Index].Term == entry.Term {
			return nil
		}

		if err := n.canPropose(ctx); err != nil {
			if err == ErrNotLeader {
				return ErrLeadershipLost
			}

			return err
		}

		if n.election.currentTerm != entry.Term {
			return ErrLeadershipLost
		}

		changed := n.log.changed
		n.mu.Unlock()

		select {
		case <-ctx.Done():
		case <-changed:
		}

		n.mu.Lock()
	}
}

func (n *Node) canPropose(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if n.runtime.failure != nil {
		return n.runtime.failure
	}

	if !n.runtime.running {
		return ErrNotRunning
	}

	if n.election.role != Leader {
		return ErrNotLeader
	}

	return nil
}
