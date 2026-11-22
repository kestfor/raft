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
	defer n.mu.Unlock()

	entry, err := n.appendCommand(ctx, data)
	if err != nil {
		return 0, nil, err
	}

	id := proposalID{index: entry.Index, term: entry.Term}
	n.log.pending[id] = nil
	defer delete(n.log.pending, id)

	if err := n.waitForCommit(ctx, id); err != nil {
		return entry.Index, nil, err
	}
	value, err := n.waitForApplied(ctx, id)
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
