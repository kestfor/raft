package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	httpapi "github.com/kestfor/raft/internal/api/http"
	"github.com/kestfor/raft/internal/kv"
	"github.com/kestfor/raft/pkg/kvclient"
	"github.com/kestfor/raft/pkg/raft"
	"github.com/kestfor/raft/pkg/wal"
)

type network struct {
	mu       sync.RWMutex
	nodes    map[raft.NodeID]*raft.Node
	isolated raft.NodeID
}

type transport struct {
	net  *network
	from raft.NodeID
}

func (tr transport) target(to raft.NodeID) (*raft.Node, error) {
	tr.net.mu.RLock()
	defer tr.net.mu.RUnlock()
	if tr.net.isolated != "" && (tr.from == tr.net.isolated || to == tr.net.isolated) {
		return nil, errors.New("partition")
	}

	return tr.net.nodes[to], nil
}

func (tr transport) RequestVote(
	ctx context.Context,
	to raft.NodeID,
	req raft.RequestVoteRequest,
) (raft.RequestVoteResponse, error) {
	n, err := tr.target(to)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}

	return n.RequestVote(ctx, req)
}

func (tr transport) AppendEntries(
	ctx context.Context,
	to raft.NodeID,
	req raft.AppendEntriesRequest,
) (raft.AppendEntriesResponse, error) {
	n, err := tr.target(to)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}

	return n.AppendEntries(ctx, req)
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestClusterRESTFailoverAndRestart(t *testing.T) {
	ids := []raft.NodeID{"a", "b", "c"}
	paths := map[raft.NodeID]string{}
	for _, id := range ids {
		paths[id] = filepath.Join(t.TempDir(), "raft.wal")
	}

	key := "a/../ключ ?#%"
	value := []byte{0, 255, 1, '\n', 42}

	for attempt := range 2 {
		func() {
			net := &network{nodes: make(map[raft.NodeID]*raft.Node)}
			stores := make(map[raft.NodeID]*kv.Store)
			journals := make([]*wal.Log, 0, 3)
			for i, id := range ids {
				var peers []raft.NodeID
				for _, other := range ids {
					if id != other {
						peers = append(peers, other)
					}
				}

				cfg := raft.DefaultConfig(id, peers)
				cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
				cfg.ElectionTimeoutMin = time.Duration(600+i*200) * time.Millisecond
				cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + 150*time.Millisecond
				cfg.HeartbeatInterval = 20 * time.Millisecond
				cfg.RPCTimeout = 200 * time.Millisecond
				journal, err := wal.Open(paths[id])
				if err != nil {
					t.Fatal(err)
				}

				journals = append(journals, journal)
				cfg.Storage = journal
				node, err := raft.NewNode(cfg, transport{net, id})
				if err != nil {
					t.Fatal(err)
				}

				net.nodes[id] = node
				stores[id] = kv.New(raftLog{node: node})
			}

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 6)
			for _, id := range ids {
				go func() { done <- net.nodes[id].Run(ctx) }()
				go func() { done <- stores[id].Run(ctx) }()
			}

			defer func() {
				cancel()
				for range 6 {
					if err := <-done; err != nil {
						t.Error(err)
					}
				}

				for _, journal := range journals {
					if err := journal.Close(); err != nil {
						t.Error(err)
					}
				}
			}()

			// Bind all URLs before installing handlers, so every follower can redirect.
			servers := map[raft.NodeID]*httptest.Server{}
			origins := map[string]string{}
			for _, id := range ids {
				server := httptest.NewUnstartedServer(nil)
				servers[id] = server
				origins[string(id)] = "http://" + server.Listener.Addr().String()
			}

			clients := map[raft.NodeID]*kvclient.Client{}
			for _, id := range ids {
				servers[id].Config.Handler = httpapi.New(stores[id], origins, 2*time.Second)
				servers[id].Start()
				defer servers[id].Close()
				client, err := kvclient.New(origins[string(id)], 4*time.Second)
				if err != nil {
					t.Fatal(err)
				}

				clients[id] = client
			}

			var leader raft.NodeID
			eventually(t, func() bool {
				for _, id := range ids {
					if stores[id].Status().Role == "leader" {
						leader = id
						return true
					}
				}

				return false
			})
			eventually(t, func() bool {
				for _, id := range ids {
					if stores[id].Status().LeaderID != string(leader) {
						return false
					}
				}

				return true
			})
			var follower raft.NodeID
			for _, id := range ids {
				if id != leader {
					follower = id
					break
				}
			}

			if attempt == 0 {
				index, err := clients[follower].Put(t.Context(), key, value)
				if err != nil {
					t.Fatal(err)
				}

				if stores[leader].Status().AppliedIndex < index {
					t.Fatal("responded before apply")
				}

				eventually(t, func() bool {
					for _, id := range ids {
						if stores[id].Status().AppliedIndex < index {
							return false
						}
					}

					return true
				})
			}

			got, err := clients[follower].Get(t.Context(), key)
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("read after restart/redirect: %v %v", got, err)
			}

			before := stores[leader].Status().LastLogIndex
			if _, err := clients[leader].Get(t.Context(), key); err != nil {
				t.Fatal(err)
			}

			if stores[leader].Status().LastLogIndex != before {
				t.Fatal("GET wrote to WAL")
			}

			if attempt == 0 {
				net.mu.Lock()
				net.isolated = leader
				net.mu.Unlock()
				eventually(t, func() bool {
					for _, id := range ids {
						if id != leader && stores[id].Status().Role == "leader" {
							return true
						}
					}

					return false
				})
				if _, err := clients[leader].Get(t.Context(), key); err == nil {
					t.Fatal("isolated leader served GET")
				}

				net.mu.Lock()
				net.isolated = ""
				net.mu.Unlock()
				eventually(t, func() bool { return stores[leader].Status().Role == "follower" })
			} else {
				if _, err := clients[follower].Delete(t.Context(), key); err != nil {
					t.Fatal(err)
				}

				_, err := clients[follower].Get(t.Context(), key)
				var response *kvclient.Error
				if !errors.As(err, &response) || response.StatusCode != 404 {
					t.Fatalf("delete: %v", err)
				}
			}
		}()
	}
}
