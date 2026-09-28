package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/kestfor/raft/pkg/set"
)

// Node's public methods are safe for concurrent use.
// A node has a single lifetime: call Run once, then discard it.
type Node struct {
	config Config
	logger *slog.Logger

	transport Transport

	mu       sync.Mutex // protects election and runtime
	election electionState
	runtime  runtimeState
}

type electionState struct {
	role        Role
	currentTerm uint64
	leaderID    NodeID
	votedFor    NodeID
	votes       set.Set[NodeID]
}

type runtimeState struct {
	started          bool
	running          bool
	electionTimer    *time.Timer
	electionDeadline time.Time
	rpcInFlight      set.Set[NodeID]
	rpcWG            sync.WaitGroup
}

// NewNode starts no goroutines or timers.
func NewNode(config Config, transport Transport) (*Node, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	if transport == nil {
		return nil, fmt.Errorf("raft: transport must not be nil")
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Node{
		config:    config,
		transport: transport,
		logger:    logger.With(slog.String("node", string(config.ID))),
		election:  electionState{role: Follower},
		runtime:   runtimeState{rpcInFlight: set.New[NodeID]()},
	}, nil
}

// Run blocks until ctx is canceled or the protocol cannot continue. Cancellation
// is a normal shutdown. All outgoing RPCs finish before Run returns.
func (n *Node) Run(ctx context.Context) error {
	n.mu.Lock()
	if n.runtime.started {
		n.mu.Unlock()
		return ErrAlreadyStarted
	}

	ctx, cancel := context.WithCancel(ctx)
	n.runtime.started, n.runtime.running = true, true
	timeout := n.randomElectionTimeout()
	n.runtime.electionDeadline = time.Now().Add(timeout)
	n.runtime.electionTimer = time.NewTimer(timeout)
	n.mu.Unlock()

	defer func() {
		n.mu.Lock()
		n.runtime.running = false
		n.runtime.electionTimer.Stop()
		n.mu.Unlock()

		cancel()
		n.runtime.rpcWG.Wait()

		n.logger.InfoContext(ctx, "node stopped")
	}()

	rpcTicker := time.NewTicker(n.config.HeartbeatInterval)
	defer rpcTicker.Stop()

	n.logger.InfoContext(ctx, "node started", slog.Any("peers", n.config.Peers))

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-n.runtime.electionTimer.C:
			if err := n.handleElectionTimeout(ctx); err != nil {
				return err
			}

		case <-rpcTicker.C:
			n.mu.Lock()
			switch n.election.role {
			case Candidate:
				n.requestVotes(ctx)
			case Leader:
				n.sendHeartbeats(ctx)
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.status()
}

func (n *Node) RequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.validateRequest(ctx, req.CandidateID, req.Term); err != nil {
		return RequestVoteResponse{}, err
	}

	n.advanceTerm(ctx, req.Term)

	granted := req.Term == n.election.currentTerm &&
		(n.election.votedFor == "" || n.election.votedFor == req.CandidateID)
	if granted {
		n.election.votedFor = req.CandidateID
		n.resetElectionTimer()
	}

	n.logger.DebugContext(ctx, "vote decided",
		slog.String("candidate", string(req.CandidateID)),
		slog.Uint64("request_term", req.Term),
		slog.Uint64("current_term", n.election.currentTerm),
		slog.Bool("granted", granted),
		slog.String("voted_for", string(n.election.votedFor)),
	)

	return RequestVoteResponse{Term: n.election.currentTerm, VoteGranted: granted}, nil
}

// AppendEntries currently handles heartbeats only, not log replication.
func (n *Node) AppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.validateRequest(ctx, req.LeaderID, req.Term); err != nil {
		return AppendEntriesResponse{}, err
	}

	accepted := req.Term >= n.election.currentTerm
	if accepted {
		n.becomeFollower(ctx, req.Term, req.LeaderID)
	}

	n.logger.DebugContext(ctx, "heartbeat handled",
		slog.String("leader", string(req.LeaderID)),
		slog.Uint64("request_term", req.Term),
		slog.Uint64("current_term", n.election.currentTerm),
		slog.Bool("accepted", accepted),
	)

	return AppendEntriesResponse{Term: n.election.currentTerm, Success: accepted}, nil
}

func (n *Node) handleElectionTimeout(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.election.role == Leader || time.Now().Before(n.runtime.electionDeadline) {
		return nil
	}

	return n.startElection(ctx)
}

// State transitions and RPC scheduling below require mu to be held.
func (n *Node) startElection(ctx context.Context) error {
	if n.election.role == Leader {
		return nil
	}

	if n.election.currentTerm == math.MaxUint64 {
		return ErrTermExhausted
	}

	previous := n.status()

	n.election.currentTerm++
	n.election.role = Candidate
	n.election.leaderID = ""
	n.election.votedFor = n.config.ID
	n.election.votes = set.New(n.config.ID)

	n.resetElectionTimer()
	n.logStateChange(ctx, previous, "election_timeout")

	if n.hasQuorum() {
		n.becomeLeader(ctx)
	} else {
		n.requestVotes(ctx)
	}

	return nil
}

func (n *Node) becomeFollower(ctx context.Context, term uint64, leader NodeID) {
	previous := n.status()

	if term > n.election.currentTerm {
		n.election.currentTerm = term
		n.election.votedFor = ""
	}

	// Stepping down in the same term must preserve the vote already cast.
	n.election.role = Follower
	n.election.leaderID = leader
	n.election.votes = nil

	if leader != "" || previous.Role == Leader {
		n.resetElectionTimer()
	}

	reason := "leader_heartbeat"
	if leader == "" {
		reason = "higher_term_observed"
	}
	n.logStateChange(ctx, previous, reason)
}

func (n *Node) becomeLeader(ctx context.Context) {
	previous := n.status()

	n.election.role = Leader
	n.election.leaderID = n.config.ID
	n.election.votes = nil

	n.runtime.electionTimer.Stop()
	n.logStateChange(ctx, previous, "majority_reached")

	n.sendHeartbeats(ctx)
}

func (n *Node) advanceTerm(ctx context.Context, term uint64) {
	if term > n.election.currentTerm {
		n.becomeFollower(ctx, term, "")
	}
}

func (n *Node) hasQuorum() bool {
	return len(n.election.votes) >= (len(n.config.Peers)+1)/2+1
}

func (n *Node) handleVoteResponse(ctx context.Context, peer NodeID, requestTerm uint64, response RequestVoteResponse) {
	// A higher term matters even when this reply belongs to an old election.
	n.advanceTerm(ctx, response.Term)

	if n.election.role != Candidate || requestTerm != n.election.currentTerm ||
		response.Term != n.election.currentTerm ||
		!response.VoteGranted {
		return
	}

	n.election.votes.Add(peer)
	if n.hasQuorum() {
		n.becomeLeader(ctx)
	}
}

func (n *Node) requestVotes(ctx context.Context) {
	req := RequestVoteRequest{Term: n.election.currentTerm, CandidateID: n.config.ID}

	for _, peer := range n.config.Peers {
		if !n.election.votes.Contains(peer) {
			n.startRPC(ctx, peer, func() {
				n.requestVote(ctx, peer, req)
			})
		}
	}
}

func (n *Node) sendHeartbeats(ctx context.Context) {
	req := AppendEntriesRequest{Term: n.election.currentTerm, LeaderID: n.config.ID}

	for _, peer := range n.config.Peers {
		n.startRPC(ctx, peer, func() { n.sendHeartbeat(ctx, peer, req) })
	}
}

// startRPC bounds outgoing work to one call per peer. Busy peers are retried
// on the next tick. Run marks the node stopped under mu before waiting for rpcWG.
func (n *Node) startRPC(ctx context.Context, peer NodeID, call func()) {
	if !n.runtime.running || ctx.Err() != nil || n.runtime.rpcInFlight.Contains(peer) {
		return
	}

	n.runtime.rpcInFlight.Add(peer)
	n.runtime.rpcWG.Go(call)
}

func (n *Node) requestVote(ctx context.Context, peer NodeID, req RequestVoteRequest) {
	rpcCtx, cancel := context.WithTimeout(ctx, n.config.RPCTimeout)
	defer cancel()

	n.logger.DebugContext(ctx, "requesting vote",
		slog.String("peer", string(peer)),
		slog.Uint64("request_term", req.Term),
	)
	response, err := n.transport.RequestVote(rpcCtx, peer, req)

	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.finishRPC(ctx, peer, "RequestVote", req.Term, err) {
		return
	}

	n.logger.DebugContext(ctx, "vote response received",
		slog.String("peer", string(peer)),
		slog.Uint64("request_term", req.Term),
		slog.Uint64("response_term", response.Term),
		slog.Bool("granted", response.VoteGranted),
	)
	n.handleVoteResponse(ctx, peer, req.Term, response)
}

func (n *Node) sendHeartbeat(ctx context.Context, peer NodeID, req AppendEntriesRequest) {
	rpcCtx, cancel := context.WithTimeout(ctx, n.config.RPCTimeout)
	defer cancel()

	n.logger.DebugContext(ctx, "sending heartbeat",
		slog.String("peer", string(peer)),
		slog.Uint64("request_term", req.Term),
	)
	response, err := n.transport.AppendEntries(rpcCtx, peer, req)

	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.finishRPC(ctx, peer, "AppendEntries", req.Term, err) {
		return
	}

	n.advanceTerm(ctx, response.Term)
}

// Helpers below require mu to be held.
func (n *Node) finishRPC(ctx context.Context, peer NodeID, rpc string, requestTerm uint64, err error) bool {
	n.runtime.rpcInFlight.Remove(peer)

	if !n.runtime.running {
		return false
	}

	if err != nil {
		n.logger.DebugContext(ctx, "RPC failed",
			slog.String("rpc", rpc),
			slog.String("peer", string(peer)),
			slog.Uint64("request_term", requestTerm),
			slog.Any("error", err),
		)
		return false
	}

	return true
}

func (n *Node) validateRequest(ctx context.Context, senderID NodeID, term uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if !n.runtime.running {
		return ErrNotRunning
	}

	if term == 0 || !slices.Contains(n.config.Peers, senderID) {
		return fmt.Errorf("%w: term must be positive and sender %q must be a peer", ErrInvalidRequest, senderID)
	}

	return nil
}

func (n *Node) resetElectionTimer() {
	timeout := n.randomElectionTimeout()
	n.runtime.electionDeadline = time.Now().Add(timeout)
	n.runtime.electionTimer.Reset(timeout)
}

func (n *Node) randomElectionTimeout() time.Duration {
	return n.config.ElectionTimeoutMin + time.Duration(
		rand.Int64N(int64(n.config.ElectionTimeoutMax-n.config.ElectionTimeoutMin)),
	)
}

func (n *Node) status() Status {
	return Status{ID: n.config.ID, Role: n.election.role, Term: n.election.currentTerm, LeaderID: n.election.leaderID}
}

func (n *Node) logStateChange(ctx context.Context, previous Status, reason string) {
	current := n.status()
	if previous == current {
		return
	}

	n.logger.InfoContext(ctx, "state changed",
		slog.String("from_role", string(previous.Role)),
		slog.String("to_role", string(current.Role)),
		slog.Uint64("from_term", previous.Term),
		slog.Uint64("to_term", current.Term),
		slog.String("from_leader", string(previous.LeaderID)),
		slog.String("leader", string(current.LeaderID)),
		slog.String("reason", reason),
	)
}
