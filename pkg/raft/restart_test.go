package raft

import (
	"context"
	"testing"
	"time"

	"github.com/kestfor/raft/pkg/wal"
)

func waitUntil(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for !check() {
		select {
		case <-deadline.C:
			t.Fatal("condition did not become true")
		case <-tick.C:
		}
	}
}

func TestWholeClusterRestartsFromWAL(t *testing.T) {
	ids := []NodeID{"a", "b", "c"}
	journals := make(map[NodeID]*wal.Memory)
	for _, id := range ids {
		journals[id] = &wal.Memory{}
	}

	var committed uint64
	for attempt := range 2 {
		func() {
			network := &memoryNetwork{nodes: make(map[NodeID]*Node)}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			for i, id := range ids {
				var peers []NodeID
				for _, other := range ids {
					if other != id {
						peers = append(peers, other)
					}
				}

				cfg := testConfig(id, peers)
				cfg.ElectionTimeoutMin = time.Duration(300+i*200) * time.Millisecond
				cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + 100*time.Millisecond
				cfg.RPCTimeout = 100 * time.Millisecond
				cfg.Storage = journals[id]
				n, err := NewNode(cfg, memoryTransport{network: network, from: id})
				if err != nil {
					t.Fatal(err)
				}

				network.nodes[id] = n
				if attempt > 0 &&
					(n.lastIndex() < committed || string(n.log.entries[committed].Data) != "survives restart") {
					t.Fatalf("node %s lost persisted command", id)
				}
			}

			done := make(chan error, len(ids))
			for _, n := range network.nodes {
				go func() { done <- n.Run(ctx) }()
			}

			defer func() {
				cancel()
				for range ids {
					if err := <-done; err != nil {
						t.Error(err)
					}
				}
			}()
			var leader *Node
			waitUntil(t, func() bool {
				for _, n := range network.nodes {
					if n.Status().Role == Leader {
						leader = n
						return true
					}
				}

				return false
			})
			if attempt == 0 {
				committed = propose(t, leader, "survives restart")
			}

			// On restart there are no new client commands: the leader's no-op
			// must restore commitment of the old prefix on every member.
			waitUntil(t, func() bool {
				for _, n := range network.nodes {
					if n.Status().CommitIndex < committed {
						return false
					}
				}

				return true
			})
		}()
	}
}
