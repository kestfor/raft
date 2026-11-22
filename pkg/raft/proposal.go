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

type proposalID struct {
	index uint64
	term  uint64
}

// Propose waits for commit and local application, returning this command's result.
// Errors after append include its index; cancellation does not roll it back.
func (n *Node) Propose(ctx context.Context, data []byte) (uint64, any, error) {
	entry, err := n.appendCommand(ctx, data)
	if err != nil {
		return 0, nil, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	id := proposalID{index: entry.Index, term: entry.Term}
	defer delete(n.log.pending, id)

	if err := n.waitForCommit(ctx, id); err != nil {
		return entry.Index, nil, err
	}
	value, err := n.waitForApplied(ctx, id)
	return entry.Index, value, err
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
		for _, entry := range entries {
			n.log.pending[proposalID{index: entry.Index, term: entry.Term}] = nil
		}
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

// Both waits are called with mu held and release it while sleeping.
func (n *Node) waitForCommit(ctx context.Context, id proposalID) error {
	for id.index > n.log.commitIndex || n.log.entries[id.index].Term != id.term {
		if err := n.checkRunning(ctx); err != nil {
			return err
		}
		if n.election.role != Leader || n.election.currentTerm != id.term {
			return ErrLeadershipLost
		}
		n.waitForLogChange(ctx)
	}
	return nil
}

func (n *Node) waitForApplied(ctx context.Context, id proposalID) (any, error) {
	for n.log.lastApplied < id.index {
		if err := n.checkRunning(ctx); err != nil {
			return nil, err
		}
		n.waitForLogChange(ctx)
	}
	return n.log.pending[id], nil
}

func (n *Node) waitForLogChange(ctx context.Context) {
	changed := n.log.changed
	n.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-changed:
	}
	n.mu.Lock()
}

func (n *Node) checkRunning(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if n.runtime.failure != nil {
		return n.runtime.failure
	}

	if !n.runtime.running {
		return ErrNotRunning
	}
	return nil
}

func (n *Node) canPropose(ctx context.Context) error {
	if err := n.checkRunning(ctx); err != nil {
		return err
	}
	if n.election.role != Leader {
		return ErrNotLeader
	}

	return nil
}
