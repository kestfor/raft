package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/kestfor/raft/internal/kv"
	"github.com/kestfor/raft/pkg/raft"
)

// raftLog adapts the protocol to the KV service at the composition boundary.
type raftLog struct{ node *raft.Node }

var _ kv.Log = raftLog{}

func (l raftLog) Propose(ctx context.Context, data []byte) (uint64, any, error) {
	index, value, err := l.node.Propose(ctx, data)
	return index, value, translate(err)
}

func (l raftLog) ReadIndex(ctx context.Context) (uint64, error) {
	index, err := l.node.ReadIndex(ctx)
	return index, translate(err)
}

func (l raftLog) LogEntries() []raft.Entry { return l.node.LogEntries() }

func (l raftLog) Status() kv.Status {
	s := l.node.Status()

	return kv.Status{
		ID:           string(s.ID),
		Role:         string(s.Role),
		Term:         s.Term,
		LeaderID:     string(s.LeaderID),
		LastLogIndex: s.LastLogIndex,
		CommitIndex:  s.CommitIndex,
		AppliedIndex: s.AppliedIndex,
		Available:    s.Running,
	}
}

func translate(err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost):
		return errors.Join(kv.ErrNotLeader, err)
	case errors.Is(err, raft.ErrInvalidRequest):
		return errors.Join(kv.ErrInvalid, err)
	case errors.Is(err, raft.ErrNotRunning):
		return errors.Join(kv.ErrUnavailable, err)
	default:
		return err
	}
}

// Keep election/replication RPCs, but never allow raw commands to bypass KV validation.
type protocolOnly struct{ *raft.Node }

func (protocolOnly) Propose(context.Context, []byte) (uint64, any, error) {
	return 0, nil, fmt.Errorf("%w: use the KV REST API", raft.ErrInvalidRequest)
}
