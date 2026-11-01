package grpctransport

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kestfor/raft/pkg/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func waitForConnectionState(t *testing.T, conn *grpc.ClientConn, want connectivity.State) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for state := conn.GetState(); state != want; state = conn.GetState() {
		if !conn.WaitForStateChange(ctx, state) {
			t.Fatalf("connection state = %s, want %s", conn.GetState(), want)
		}
	}
}

func TestClientReconnectsAfterDNSFailureAndRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A new listener simulates a peer restarting at a different address.
		var endpoint atomic.Pointer[bufconn.Listener]
		client, err := NewClient(map[raft.NodeID]string{"b": "node2:9000"},
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
				if address != "node2:9000" {
					t.Errorf("dial address = %q, want unresolved hostname", address)
				}
				if listener := endpoint.Load(); listener != nil {
					return listener.DialContext(ctx)
				}
				return nil, &net.DNSError{Name: address, Err: "no such host", IsNotFound: true}
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		conn := client.connections["b"]

		for range 2 {
			synctest.Wait()
			conn.Connect()
			waitForConnectionState(t, conn, connectivity.TransientFailure)
			// Stay offline long enough to exercise the maximum reconnect backoff.
			time.Sleep(3 * time.Second)
			listener := bufconn.Listen(64 * 1024)
			server := grpc.NewServer()
			Register(server, handlerStub{
				requestVote: func(_ context.Context, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
					return raft.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
				},
				appendEntries: func(_ context.Context, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
					return raft.AppendEntriesResponse{Term: req.Term, Success: true}, nil
				},
			})
			defer server.Stop()
			defer func() { _ = listener.Close() }()
			go func() {
				if err := server.Serve(listener); err != nil {
					t.Errorf("serve: %v", err)
				}
			}()
			endpoint.Store(listener)

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			vote, err := client.RequestVote(ctx, "b", raft.RequestVoteRequest{Term: 1, CandidateID: "a"})
			if err != nil || !vote.VoteGranted {
				t.Fatalf("vote after recovery = %+v, error = %v", vote, err)
			}
			heartbeat, err := client.AppendEntries(ctx, "b", raft.AppendEntriesRequest{Term: 1, LeaderID: "a"})
			cancel()
			if err != nil || !heartbeat.Success {
				t.Fatalf("heartbeat after recovery = %+v, error = %v", heartbeat, err)
			}

			endpoint.Store(nil)
			server.Stop()
		}
	})
}

func TestClientWaitingForConnectionHonorsContext(t *testing.T) {
	for _, method := range []string{"vote", "heartbeat"} {
		for _, cancelEarly := range []bool{false, true} {
			name := method + "/deadline"
			if cancelEarly {
				name = method + "/cancel"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					client, err := NewClient(map[raft.NodeID]string{"b": "node2:9000"},
						grpc.WithTransportCredentials(insecure.NewCredentials()),
						grpc.WithContextDialer(func(_ context.Context, address string) (net.Conn, error) {
							return nil, &net.DNSError{Name: address, Err: "no such host", IsNotFound: true}
						}),
					)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = client.Close() }()
					conn := client.connections["b"]
					conn.Connect()
					waitForConnectionState(t, conn, connectivity.TransientFailure)

					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer cancel()
					result := make(chan error, 1)
					go func() {
						var err error
						if method == "vote" {
							_, err = client.RequestVote(ctx, "b", raft.RequestVoteRequest{Term: 1, CandidateID: "a"})
						} else {
							_, err = client.AppendEntries(ctx, "b", raft.AppendEntriesRequest{Term: 1, LeaderID: "a"})
						}
						result <- err
					}()
					synctest.Wait()
					select {
					case err := <-result:
						t.Fatalf("RPC did not wait for connection: %v", err)
					default:
					}
					want := codes.DeadlineExceeded
					if cancelEarly {
						want = codes.Canceled
						cancel()
					}
					if err := <-result; status.Code(err) != want {
						t.Fatalf("RPC returned %v, want %s", err, want)
					}
				})
			})
		}
	}
}
