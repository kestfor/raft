package raft

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

func TestElectionNeedsDistinctMajority(t *testing.T) {
	node, ctx := newElectionTestNode(t, "a", []NodeID{"b", "c", "d", "e"})
	if err := node.startElection(ctx); err != nil {
		t.Fatal(err)
	}

	if node.election.role != Candidate || node.election.currentTerm != 1 || node.election.votedFor != "a" {
		t.Fatalf("unexpected election state: %+v", node)
	}

	vote := RequestVoteResponse{Term: 1, VoteGranted: true}
	node.handleVoteResponse(ctx, "b", 1, vote)
	node.handleVoteResponse(ctx, "b", 1, vote)
	if node.election.role != Candidate {
		t.Fatal("duplicate vote counted toward quorum")
	}

	node.handleVoteResponse(ctx, "c", 1, vote)
	if node.election.role != Leader || node.election.leaderID != "a" {
		t.Fatalf("majority must elect leader: %+v", node)
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	node, ctx := newElectionTestNode(t, "a", nil)
	if err := node.startElection(ctx); err != nil {
		t.Fatal(err)
	}

	if node.election.role != Leader || node.election.currentTerm != 1 {
		t.Fatalf("single node did not elect itself: %+v", node.status())
	}

	if err := node.startElection(ctx); err != nil || node.election.currentTerm != 1 || node.election.role != Leader {
		t.Fatal("leader started another election")
	}
}

func TestVoteRules(t *testing.T) {
	node, _ := newElectionTestNode(t, "a", []NodeID{"b", "c"})
	tests := []struct {
		name    string
		request RequestVoteRequest
		granted bool
		term    uint64
	}{
		{"first vote", RequestVoteRequest{Term: 2, CandidateID: "b"}, true, 2},
		{"repeat vote", RequestVoteRequest{Term: 2, CandidateID: "b"}, true, 2},
		{"another candidate", RequestVoteRequest{Term: 2, CandidateID: "c"}, false, 2},
		{"old term", RequestVoteRequest{Term: 1, CandidateID: "b"}, false, 2},
		{"new term", RequestVoteRequest{Term: 3, CandidateID: "c"}, true, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := requestVote(t, node, tt.request)
			if response != (RequestVoteResponse{Term: tt.term, VoteGranted: tt.granted}) {
				t.Fatalf("unexpected reply: %+v", response)
			}
		})
	}
}

func TestHeartbeatStepsDownWithoutForgettingVote(t *testing.T) {
	node, ctx := newElectionTestNode(t, "a", []NodeID{"b", "c"})
	node.election.currentTerm = 1
	if err := node.startElection(ctx); err != nil {
		t.Fatal(err)
	}

	response := appendEntries(t, node, AppendEntriesRequest{Term: 2, LeaderID: "b"})
	if !response.Success || node.election.role != Follower || node.election.leaderID != "b" ||
		node.election.votedFor != "a" {
		t.Fatalf("incorrect same-term stepdown: %+v", node)
	}

	if requestVote(t, node, RequestVoteRequest{Term: 2, CandidateID: "c"}).VoteGranted {
		t.Fatal("voted twice in the same term after stepping down")
	}

	response = appendEntries(t, node, AppendEntriesRequest{Term: 1, LeaderID: "c"})
	if response.Success || node.election.leaderID != "b" {
		t.Fatal("stale heartbeat changed leader")
	}
}

func TestHigherTermInEveryRPCStepsDown(t *testing.T) {
	tests := []struct {
		name  string
		apply func(*testing.T, *Node, context.Context)
	}{
		{"vote request", func(t *testing.T, node *Node, ctx context.Context) {
			requestVote(t, node, RequestVoteRequest{Term: 5, CandidateID: "b"})
		}},
		{"append request", func(t *testing.T, node *Node, ctx context.Context) {
			appendEntries(t, node, AppendEntriesRequest{Term: 5, LeaderID: "b"})
		}},
		{"stale vote response", func(t *testing.T, node *Node, ctx context.Context) {
			node.handleVoteResponse(ctx, "b", 1, RequestVoteResponse{Term: 5})
		}},
		{"stale append response", func(t *testing.T, node *Node, ctx context.Context) { node.advanceTerm(ctx, 5) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, ctx := newElectionTestNode(t, "a", []NodeID{"b", "c"})
			if err := node.startElection(ctx); err != nil {
				t.Fatal(err)
			}

			node.handleVoteResponse(ctx, "b", 1, RequestVoteResponse{Term: 1, VoteGranted: true})
			tt.apply(t, node, ctx)
			if node.election.role != Follower || node.election.currentTerm != 5 {
				t.Fatalf("leader did not step down: %+v", node)
			}

			if tt.name != "vote request" && node.election.votedFor != "" {
				t.Fatalf("old vote survived a term change: %q", node.election.votedFor)
			}
		})
	}
}

func TestOldResponsesCannotWinNewElection(t *testing.T) {
	node, ctx := newElectionTestNode(t, "a", []NodeID{"b", "c"})
	for range 2 {
		if err := node.startElection(ctx); err != nil {
			t.Fatal(err)
		}
	}

	for _, reply := range []struct {
		requestTerm uint64
		response    RequestVoteResponse
	}{
		{1, RequestVoteResponse{Term: 1, VoteGranted: true}},
		{1, RequestVoteResponse{Term: 2, VoteGranted: true}},
		{2, RequestVoteResponse{Term: 1, VoteGranted: true}},
		{2, RequestVoteResponse{Term: 2, VoteGranted: false}},
	} {
		node.handleVoteResponse(ctx, "b", reply.requestTerm, reply.response)
		if node.election.role != Candidate || node.election.currentTerm != 2 {
			t.Fatalf("invalid response changed election outcome: %+v", reply)
		}
	}

	node.handleVoteResponse(ctx, "b", 2, RequestVoteResponse{Term: 2, VoteGranted: true})
	if node.election.role != Leader {
		t.Fatal("current-term majority did not elect leader")
	}
}

func TestCandidateTracksMissingVotes(t *testing.T) {
	node, ctx := newElectionTestNode(t, "a", []NodeID{"b", "c", "d", "e"})
	if err := node.startElection(ctx); err != nil {
		t.Fatal(err)
	}

	node.handleVoteResponse(ctx, "b", 1, RequestVoteResponse{Term: 1, VoteGranted: true})
	if node.election.role != Candidate || !node.election.votes.Contains("b") {
		t.Fatal("candidate did not record the granted vote")
	}

	for _, peer := range []NodeID{"c", "d", "e"} {
		if node.election.votes.Contains(peer) {
			t.Fatalf("candidate recorded a vote from %s before receiving it", peer)
		}
	}
}

func TestTermDoesNotWrap(t *testing.T) {
	node, ctx := newElectionTestNode(t, "a", nil)
	node.election.currentTerm = math.MaxUint64
	if err := node.startElection(
		ctx,
	); !errors.Is(err, ErrTermExhausted) ||
		node.election.currentTerm != math.MaxUint64 {
		t.Fatal("term overflow was not prevented")
	}
}

// Election safety must hold even when message loss prevents progress.
func TestElectionSafetyUnderMessageDisorder(t *testing.T) {
	const (
		electionTick = iota
		broadcastTick
		voteRequest
		voteResponse
		appendRequest
		appendResponse
	)
	type message struct {
		kind              int
		from, to          NodeID
		term, requestTerm uint64
		granted           bool
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // The simulation delivers outgoing messages explicitly.
	for seed := uint64(0); seed < 20; seed++ {
		rng := rand.New(rand.NewPCG(seed, 42))
		ids := []NodeID{"a", "b", "c", "d", "e"}
		nodes := make(map[NodeID]*Node)
		for _, id := range ids {
			var peers []NodeID
			for _, other := range ids {
				if id != other {
					peers = append(peers, other)
				}
			}

			nodes[id], _ = newElectionTestNode(t, id, peers)
		}

		var queue []message
		sendRPCs := func(node *Node) {
			for _, peer := range node.config.Peers {
				if node.election.role == Candidate && !node.election.votes.Contains(peer) {
					queue = append(
						queue,
						message{kind: voteRequest, from: node.config.ID, to: peer, term: node.election.currentTerm},
					)
				} else if node.election.role == Leader {
					queue = append(
						queue,
						message{kind: appendRequest, from: node.config.ID, to: peer, term: node.election.currentTerm},
					)
				}
			}
		}
		leaders := make(map[uint64]NodeID)
		for step := 0; step < 3000; step++ {
			msg := message{to: ids[rng.IntN(len(ids))], kind: broadcastTick}
			switch {
			case rng.IntN(5) == 0:
				msg.kind = electionTick
			case len(queue) > 0:
				i := rng.IntN(len(queue))
				msg = queue[i]
				queue[i] = queue[len(queue)-1]
				queue = queue[:len(queue)-1]
				if rng.IntN(10) == 0 {
					continue // drop
				}
				if rng.IntN(10) == 0 {
					queue = append(queue, msg) // duplicate
				}
			}

			node := nodes[msg.to]
			oldTerm, oldRole := node.election.currentTerm, node.election.role
			switch msg.kind {
			case electionTick:
				if err := node.startElection(ctx); err != nil {
					t.Fatal(err)
				}
				sendRPCs(node)
			case broadcastTick:
				sendRPCs(node)
			case voteRequest:
				response := requestVote(t, node, RequestVoteRequest{Term: msg.term, CandidateID: msg.from})
				queue = append(
					queue,
					message{
						kind:        voteResponse,
						from:        node.config.ID,
						to:          msg.from,
						term:        response.Term,
						requestTerm: msg.term,
						granted:     response.VoteGranted,
					},
				)
			case appendRequest:
				response := appendEntries(t, node, AppendEntriesRequest{Term: msg.term, LeaderID: msg.from})
				queue = append(
					queue,
					message{kind: appendResponse, from: node.config.ID, to: msg.from, term: response.Term},
				)
			case voteResponse:
				node.handleVoteResponse(
					ctx,
					msg.from,
					msg.requestTerm,
					RequestVoteResponse{Term: msg.term, VoteGranted: msg.granted},
				)
				if oldRole != Leader && node.election.role == Leader {
					sendRPCs(node)
				}
			case appendResponse:
				node.advanceTerm(ctx, msg.term)
			}

			if node.election.currentTerm < oldTerm {
				t.Fatalf("seed %d, step %d: term decreased", seed, step)
			}

			if node.election.role == Leader {
				if previous, ok := leaders[node.election.currentTerm]; ok && previous != node.config.ID {
					t.Fatalf(
						"seed %d, step %d: two leaders in term %d: %s and %s",
						seed,
						step,
						node.election.currentTerm,
						previous,
						node.config.ID,
					)
				}

				leaders[node.election.currentTerm] = node.config.ID
			}
		}
	}
}

// Tests drive Node without Run and cancel outbound RPCs to control message delivery.
func newElectionTestNode(t *testing.T, id NodeID, peers []NodeID) (*Node, context.Context) {
	t.Helper()
	node, err := NewNode(testConfig(id, peers), &slowTransport{})
	if err != nil {
		t.Fatal(err)
	}

	node.runtime.running = true
	node.runtime.electionTimer = time.NewTimer(time.Hour)
	t.Cleanup(func() { node.runtime.electionTimer.Stop() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	return node, ctx
}

func requestVote(t *testing.T, node *Node, req RequestVoteRequest) RequestVoteResponse {
	t.Helper()
	response, err := node.RequestVote(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}

	return response
}

func appendEntries(t *testing.T, node *Node, req AppendEntriesRequest) AppendEntriesResponse {
	t.Helper()
	response, err := node.AppendEntries(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}

	return response
}
