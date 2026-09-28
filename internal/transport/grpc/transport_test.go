package grpctransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/kestfor/raft/internal/transport/grpc/raftpb"
	"github.com/kestfor/raft/pkg/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type handlerStub struct {
	requestVote   func(context.Context, raft.RequestVoteRequest) (raft.RequestVoteResponse, error)
	appendEntries func(context.Context, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error)
	status        raft.Status
}

func (h handlerStub) Status() raft.Status { return h.status }

func (h handlerStub) RequestVote(ctx context.Context, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return h.requestVote(ctx, req)
}

func (h handlerStub) AppendEntries(
	ctx context.Context,
	req raft.AppendEntriesRequest,
) (raft.AppendEntriesResponse, error) {
	return h.appendEntries(ctx, req)
}

func testClient(t *testing.T, handler Handler) *Client {
	t.Helper()
	listener := bufconn.Listen(64 * 1024)
	server := grpc.NewServer()
	Register(server, handler)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	client, err := NewClient(map[raft.NodeID]string{"b": "passthrough:///b"},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})

	return client
}

func TestProtocolModelsRoundTrip(t *testing.T) {
	wantVote := raft.RequestVoteRequest{Term: 17, CandidateID: "a"}
	wantAppend := raft.AppendEntriesRequest{Term: 19, LeaderID: "c"}
	client := testClient(t, handlerStub{
		requestVote: func(_ context.Context, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
			if req != wantVote {
				t.Errorf("vote request = %+v, want %+v", req, wantVote)
			}

			return raft.RequestVoteResponse{Term: 18, VoteGranted: true}, nil
		},
		appendEntries: func(_ context.Context, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
			if req != wantAppend {
				t.Errorf("append request = %+v, want %+v", req, wantAppend)
			}

			return raft.AppendEntriesResponse{Term: 20, Success: false}, nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	voteResponse, err := client.RequestVote(ctx, "b", wantVote)
	if err != nil || voteResponse != (raft.RequestVoteResponse{Term: 18, VoteGranted: true}) {
		t.Fatalf("vote response = %+v, error = %v", voteResponse, err)
	}

	appendResponse, err := client.AppendEntries(ctx, "b", wantAppend)
	if err != nil || appendResponse != (raft.AppendEntriesResponse{Term: 20, Success: false}) {
		t.Fatalf("append response = %+v, error = %v", appendResponse, err)
	}

	if _, err := client.RequestVote(ctx, "unknown", wantVote); err == nil {
		t.Fatal("unknown peer accepted")
	}
}

func TestGetStatusReturnsCurrentNodeState(t *testing.T) {
	want := raft.Status{ID: "b", Role: raft.Leader, Term: 7, LeaderID: "b"}
	client := testClient(t, handlerStub{status: want})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	response, err := raftpb.NewRaftServiceClient(client.connections["b"]).GetStatus(ctx, &raftpb.GetStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetId() != string(want.ID) || response.GetRole() != string(want.Role) ||
		response.GetTerm() != want.Term || response.GetLeaderId() != string(want.LeaderID) {
		t.Fatalf("status = %+v, want %+v", response, want)
	}
}

func TestHandlerErrorsBecomeGRPCStatus(t *testing.T) {
	for _, tt := range []struct {
		err  error
		code codes.Code
	}{
		{fmt.Errorf("details: %w", raft.ErrInvalidRequest), codes.InvalidArgument},
		{raft.ErrNotRunning, codes.Unavailable},
		{context.Canceled, codes.Canceled},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{errors.New("internal details"), codes.Internal},
	} {
		t.Run(tt.code.String(), func(t *testing.T) {
			client := testClient(
				t,
				handlerStub{
					requestVote: func(context.Context, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
						return raft.RequestVoteResponse{}, tt.err
					},
				},
			)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, err := client.RequestVote(ctx, "b", raft.RequestVoteRequest{Term: 1, CandidateID: "a"})
			if status.Code(err) != tt.code {
				t.Fatalf("code = %s, want %s (error: %v)", status.Code(err), tt.code, err)
			}
		})
	}
}

func TestClientCancellation(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	client := testClient(
		t,
		handlerStub{
			requestVote: func(ctx context.Context, _ raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
				close(started)
				<-ctx.Done()
				close(finished)

				return raft.RequestVoteResponse{}, ctx.Err()
			},
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		_, err := client.RequestVote(ctx, "b", raft.RequestVoteRequest{Term: 1, CandidateID: "a"})
		result <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}

	cancel()
	if err := <-result; status.Code(err) != codes.Canceled {
		t.Fatalf("cancellation returned %v", err)
	}

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not receive cancellation")
	}
}

type clusterMember struct {
	node   *raft.Node
	server *grpc.Server
	cancel context.CancelFunc
	done   chan struct{}
	active bool
}

func TestThreeNodeGRPCElectionAndFailover(t *testing.T) {
	ids := []raft.NodeID{"a", "b", "c"}
	listeners := make(map[string]*bufconn.Listener)
	members := make(map[raft.NodeID]*clusterMember)
	for _, id := range ids {
		listener := bufconn.Listen(64 * 1024)
		listeners[string(id)] = listener
		t.Cleanup(func() { _ = listener.Close() })
	}

	for _, id := range ids {
		addresses := make(map[raft.NodeID]string)
		var peers []raft.NodeID
		for _, peer := range ids {
			if peer != id {
				addresses[peer] = "passthrough:///" + string(peer)
				peers = append(peers, peer)
			}
		}

		client, err := NewClient(addresses,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
				return listeners[address].DialContext(ctx)
			}),
		)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() { _ = client.Close() })
		cfg := raft.DefaultConfig(id, peers)
		cfg.ElectionTimeoutMin = 150 * time.Millisecond
		cfg.ElectionTimeoutMax = 300 * time.Millisecond
		cfg.HeartbeatInterval = 25 * time.Millisecond
		cfg.RPCTimeout = 50 * time.Millisecond
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		node, err := raft.NewNode(cfg, client)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		member := &clusterMember{
			node:   node,
			server: grpc.NewServer(),
			cancel: cancel,
			done:   make(chan struct{}),
			active: true,
		}
		members[id] = member
		Register(member.server, node)
		go func() {
			defer close(member.done)

			if err := node.Run(ctx); err != nil {
				t.Errorf("run %s: %v", id, err)
			}
		}()
		go func() {
			if err := member.server.Serve(listeners[string(id)]); err != nil {
				t.Errorf("serve %s: %v", id, err)
			}
		}()
		t.Cleanup(func() {
			cancel()
			member.server.Stop()
			<-member.done
		})
	}

	leader := waitForClusterLeader(t, members)
	oldTerm := members[leader].node.Status().Term
	members[leader].active = false
	members[leader].cancel()
	<-members[leader].done
	members[leader].server.Stop()
	replacement := waitForClusterLeader(t, members)
	if replacement == leader || members[replacement].node.Status().Term <= oldTerm {
		t.Fatalf("invalid replacement %s after leader %s in term %d", replacement, leader, oldTerm)
	}
}

func waitForClusterLeader(t *testing.T, members map[raft.NodeID]*clusterMember) raft.NodeID {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout.C:
			for id, member := range members {
				t.Logf("node %s: %+v, active=%v", id, member.node.Status(), member.active)
			}
			t.Fatal("cluster did not converge on one leader")
		case <-ticker.C:
			var leader raft.NodeID
			var term uint64
			count := 0
			for id, member := range members {
				if state := member.node.Status(); member.active && state.Role == raft.Leader {
					count++
					leader, term = id, state.Term
				}
			}
			if count != 1 {
				continue
			}
			consistent := true
			for _, member := range members {
				if state := member.node.Status(); member.active && (state.LeaderID != leader || state.Term != term) {
					consistent = false
				}
			}
			if consistent {
				return leader
			}
		}
	}
}
