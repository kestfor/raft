package grpctransport

import (
	"context"
	"errors"

	"github.com/kestfor/raft/internal/transport/grpc/raftpb"
	"github.com/kestfor/raft/pkg/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Register(server grpc.ServiceRegistrar, handler Handler) {
	raftpb.RegisterRaftServiceServer(server, &service{handler: handler})
}

type service struct {
	raftpb.UnimplementedRaftServiceServer
	handler Handler
}

func (s *service) RequestVote(
	ctx context.Context,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	response, err := s.handler.RequestVote(
		ctx,
		raft.RequestVoteRequest{Term: req.GetTerm(), CandidateID: raft.NodeID(req.GetCandidateId())},
	)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &raftpb.RequestVoteResponse{Term: response.Term, VoteGranted: response.VoteGranted}, nil
}

func (s *service) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {
	response, err := s.handler.AppendEntries(
		ctx,
		raft.AppendEntriesRequest{Term: req.GetTerm(), LeaderID: raft.NodeID(req.GetLeaderId())},
	)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &raftpb.AppendEntriesResponse{Term: response.Term, Success: response.Success}, nil
}

func toGRPCError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, raft.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, raft.ErrNotRunning):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, "raft request failed")
	}
}
