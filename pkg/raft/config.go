package raft

import (
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/kestfor/raft/pkg/set"
)

type Config struct {
	ID                 NodeID
	Peers              []NodeID // Excludes this node.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RPCTimeout         time.Duration
	Logger             *slog.Logger
}

func DefaultConfig(id NodeID, peers []NodeID) Config {
	return Config{
		ID:                 id,
		Peers:              slices.Clone(peers),
		ElectionTimeoutMin: 5000 * time.Millisecond,
		ElectionTimeoutMax: 10 * time.Second,
		HeartbeatInterval:  1000 * time.Millisecond,
		RPCTimeout:         2000 * time.Millisecond,
	}
}

func (c Config) validate() error {
	if c.ID == "" {
		return fmt.Errorf("raft: node ID must not be empty")
	}

	seen := set.New(c.ID)
	for _, peer := range c.Peers {
		if peer == "" || seen.Contains(peer) {
			return fmt.Errorf("raft: empty, duplicate, or local peer ID %q", peer)
		}

		seen.Add(peer)
	}

	if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return fmt.Errorf("raft: election timeouts must satisfy 0 < min < max")
	}

	if c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return fmt.Errorf("raft: heartbeat interval must be positive and less than election timeout")
	}

	if c.RPCTimeout <= 0 || c.RPCTimeout >= c.ElectionTimeoutMin {
		return fmt.Errorf("raft: RPC timeout must be positive and less than election timeout")
	}

	return nil
}
