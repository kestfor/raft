package raft

import (
	"context"
	"fmt"
	"slices"
)

// StateMachine deterministically applies committed commands in log order.
// Start each node with an empty state machine; recovery replays the log.
type StateMachine interface {
	// Do receives a copy, runs outside mu, and must return promptly.
	// Errors stop the node. Returned values must remain immutable.
	Do(entry Entry) (any, error)
}

// NoOpStateMachine discards commands and returns nil.
type NoOpStateMachine struct{}

func (NoOpStateMachine) Do(Entry) (any, error) { return nil, nil }

// applyCommitted serializes application for all roles.
func (n *Node) applyCommitted(ctx context.Context) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.runtime.running && ctx.Err() == nil {
		if n.log.lastApplied == n.log.commitIndex {
			changed := n.log.changed
			n.mu.Unlock()
			select {
			case <-ctx.Done():
			case <-changed:
			}
			n.mu.Lock()
			continue
		}

		entry := n.log.entries[n.log.lastApplied+1]
		entry.Data = slices.Clone(entry.Data)
		n.mu.Unlock()
		var (
			value any
			err   error
		)
		if !entry.NoOp {
			value, err = n.config.StateMachine.Do(entry)
		}
		n.mu.Lock()
		if err != nil {
			_ = n.fail(fmt.Errorf("raft: apply entry %d: %w", entry.Index, err))
			return
		}

		n.log.lastApplied = entry.Index
		id := proposalID{index: entry.Index, term: entry.Term}
		if _, waiting := n.log.pending[id]; waiting {
			n.log.pending[id] = value
		}
		n.notifyLogChange()
	}
}
