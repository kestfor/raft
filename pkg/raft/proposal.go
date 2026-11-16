package raft

import (
	"context"
	"fmt"
	"slices"
)

type proposalID struct {
	index uint64
	term  uint64
}

// Propose waits for commit and local application, returning this command's result.
// Errors after append include its index; cancellation does not roll it back.
func (n *Node) Propose(ctx context.Context, data []byte) (uint64, any, error) {
	n.mu.Lock()
	entry, err := n.appendCommand(ctx, data)
	if err != nil {
		n.mu.Unlock()
		return 0, nil, err
	}

	id := proposalID{index: entry.Index, term: entry.Term}
	n.log.pending[id] = nil
	n.mu.Unlock()

	value, err := n.waitForResult(ctx, id)
	return entry.Index, value, err
}

// appendCommand requires mu to be held.
func (n *Node) appendCommand(ctx context.Context, data []byte) (Entry, error) {
	if err := n.canPropose(ctx); err != nil {
		return Entry{}, err
	}

	if len(data) > MaxCommandSize {
		return Entry{}, fmt.Errorf("%w: command exceeds %d bytes", ErrInvalidRequest, MaxCommandSize)
	}

	entry := Entry{Index: n.lastIndex() + 1, Term: n.election.currentTerm, Data: slices.Clone(data)}
	if err := n.save(n.election.currentTerm, n.election.votedFor, []Entry{entry}); err != nil {
		return Entry{}, err
	}

	n.log.entries = append(n.log.entries, entry)
	n.advanceCommit()
	n.wakeRun()

	return entry, nil
}

func (n *Node) waitForResult(ctx context.Context, id proposalID) (any, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	defer delete(n.log.pending, id)

	for {
		committed := id.index <= n.log.commitIndex && n.log.entries[id.index].Term == id.term
		if committed && id.index <= n.log.lastApplied {
			return n.log.pending[id], nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if n.runtime.failure != nil {
			return nil, n.runtime.failure
		}
		if !n.runtime.running {
			return nil, ErrNotRunning
		}

		if !committed && (n.election.role != Leader || n.election.currentTerm != id.term) {
			return nil, ErrLeadershipLost
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
