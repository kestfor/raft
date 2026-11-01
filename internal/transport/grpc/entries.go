package grpctransport

import (
	"github.com/kestfor/raft/internal/transport/grpc/raftpb"
	"github.com/kestfor/raft/pkg/raft"
)

func entriesToProto(entries []raft.Entry) []*raftpb.Entry {
	result := make([]*raftpb.Entry, len(entries))
	for i, entry := range entries {
		result[i] = &raftpb.Entry{Index: entry.Index, Term: entry.Term, Data: entry.Data, NoOp: entry.NoOp}
	}
	return result
}

func entriesFromProto(entries []*raftpb.Entry) []raft.Entry {
	result := make([]raft.Entry, len(entries))
	for i, entry := range entries {
		result[i] = raft.Entry{
			Index: entry.GetIndex(),
			Term:  entry.GetTerm(),
			Data:  entry.GetData(),
			NoOp:  entry.GetNoOp(),
		}
	}
	return result
}
