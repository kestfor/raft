package raft

import "context"

type Transport interface {
	RequestVote(ctx context.Context, to NodeID, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to NodeID, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
