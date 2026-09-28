package grpctransport

import (
	"context"

	"github.com/kestfor/raft/pkg/raft"
)

type Handler interface {
	RequestVote(ctx context.Context, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error)
	Status() raft.Status
}

var _ Handler = (*raft.Node)(nil)
