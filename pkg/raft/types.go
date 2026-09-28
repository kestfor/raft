package raft

// NodeID identifies a member independently of its network address.
type NodeID string

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type RequestVoteRequest struct {
	Term        uint64
	CandidateID NodeID
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term     uint64
	LeaderID NodeID
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type Status struct {
	ID       NodeID
	Role     Role
	Term     uint64
	LeaderID NodeID // Empty if unknown.
}
