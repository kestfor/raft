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
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// Entry is immutable once appended. Indexes start at one; index zero represents
// the empty prefix. NoOp entries establish commitment in a new leader's term.
type Entry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data,omitempty"`
	NoOp  bool   `json:"no_op,omitempty"`
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type Status struct {
	ID           NodeID
	Role         Role
	Term         uint64
	LeaderID     NodeID // Empty if unknown.
	LastLogIndex uint64
	CommitIndex  uint64
}
