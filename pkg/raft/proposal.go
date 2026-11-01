package raft

import (
	"context"
	"fmt"
	"slices"
)

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
	defer n.mu.Unlock()

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
