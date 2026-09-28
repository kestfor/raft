package grpctransport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/kestfor/raft/internal/transport/grpc/raftpb"
	"github.com/kestfor/raft/pkg/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type Client struct {
	connections map[raft.NodeID]*grpc.ClientConn
}

var _ raft.Transport = (*Client)(nil)

// NewClient creates lazy connections; it does not wait for peers to start.
// The caller must supply transport credentials.
// Bare host:port addresses are resolved on each dial; explicit resolver URIs
// retain their own resolution behavior. RPC contexts should have deadlines.
func NewClient(addresses map[raft.NodeID]string, options ...grpc.DialOption) (*Client, error) {
	reconnect := backoff.DefaultConfig
	reconnect.BaseDelay = 50 * time.Millisecond
	reconnect.MaxDelay = 200 * time.Millisecond
	dialer := &net.Dialer{}
	options = append([]grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: reconnect, MinConnectTimeout: 200 * time.Millisecond}),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", address)
		}),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	}, options...)

	c := &Client{
		connections: make(map[raft.NodeID]*grpc.ClientConn, len(addresses)),
	}

	for id, address := range addresses {
		if id == "" || address == "" {
			return nil, errors.Join(fmt.Errorf("grpc transport: empty peer ID or address"), c.Close())
		}

		// Avoid the DNS resolver's independent retry delays for fixed peers.
		if !strings.Contains(address, "://") {
			address = "passthrough:///" + address
		}
		conn, err := grpc.NewClient(address, options...)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("grpc transport: connect to %q: %w", id, err), c.Close())
		}

		c.connections[id] = conn
	}

	return c, nil
}

// Close must be called after the node stops using the connections.
func (c *Client) Close() error {
	var errs []error
	for id, conn := range c.connections {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close peer %q: %w", id, err))
		}
	}

	return errors.Join(errs...)
}

func (c *Client) RequestVote(
	ctx context.Context,
	to raft.NodeID,
	req raft.RequestVoteRequest,
) (raft.RequestVoteResponse, error) {
	conn, ok := c.connections[to]
	if !ok {
		return raft.RequestVoteResponse{}, fmt.Errorf("grpc transport: unknown peer %q", to)
	}

	response, err := raftpb.NewRaftServiceClient(conn).
		RequestVote(ctx, &raftpb.RequestVoteRequest{Term: req.Term, CandidateId: string(req.CandidateID)})
	if err != nil {
		return raft.RequestVoteResponse{}, fmt.Errorf("request vote from %q: %w", to, err)
	}

	return raft.RequestVoteResponse{Term: response.GetTerm(), VoteGranted: response.GetVoteGranted()}, nil
}

func (c *Client) AppendEntries(
	ctx context.Context,
	to raft.NodeID,
	req raft.AppendEntriesRequest,
) (raft.AppendEntriesResponse, error) {
	conn, ok := c.connections[to]
	if !ok {
		return raft.AppendEntriesResponse{}, fmt.Errorf("grpc transport: unknown peer %q", to)
	}

	response, err := raftpb.NewRaftServiceClient(conn).
		AppendEntries(ctx, &raftpb.AppendEntriesRequest{Term: req.Term, LeaderId: string(req.LeaderID)})
	if err != nil {
		return raft.AppendEntriesResponse{}, fmt.Errorf("append entries to %q: %w", to, err)
	}

	return raft.AppendEntriesResponse{Term: response.GetTerm(), Success: response.GetSuccess()}, nil
}
