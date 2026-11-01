package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/kestfor/raft/pkg/raft"
	"go.yaml.in/yaml/v3"
)

type Config struct {
	ID                 string            `yaml:"id"           env:"RAFT_ID"`
	DataDir            string            `yaml:"data_dir"     env:"RAFT_DATA_DIR"`
	ListenAddress      string            `yaml:"listen"       env:"RAFT_LISTEN"`
	Peers              map[string]string `yaml:"peers"        env:"RAFT_PEERS"        envKeyValSeparator:"="`
	ElectionTimeoutMin time.Duration     `yaml:"election_min" env:"RAFT_ELECTION_MIN"`
	ElectionTimeoutMax time.Duration     `yaml:"election_max" env:"RAFT_ELECTION_MAX"`
	HeartbeatInterval  time.Duration     `yaml:"heartbeat"    env:"RAFT_HEARTBEAT"`
	RPCTimeout         time.Duration     `yaml:"rpc_timeout"  env:"RAFT_RPC_TIMEOUT"`
	LogLevel           slog.Level        `yaml:"log_level"    env:"RAFT_LOG_LEVEL"`
	LogFormat          string            `yaml:"log_format"   env:"RAFT_LOG_FORMAT"`
}

const (
	LogFormatJSON  = "json"
	LogFormatPlain = "plain"
)

func Default() Config {
	timings := raft.DefaultConfig("", nil)

	return Config{
		ListenAddress:      ":9000",
		DataDir:            "data",
		ElectionTimeoutMin: timings.ElectionTimeoutMin,
		ElectionTimeoutMax: timings.ElectionTimeoutMax,
		HeartbeatInterval:  timings.HeartbeatInterval,
		RPCTimeout:         timings.RPCTimeout,
		LogLevel:           slog.LevelInfo,
		LogFormat:          LogFormatJSON,
	}
}

// Load applies an optional YAML file and then environment variables to defaults.
// Call Validate after applying command-line overrides.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		if err := cfg.readYAML(path); err != nil {
			return Config{}, err
		}
	}

	if err := env.Parse(&cfg); err != nil {
		return Config{}, fmt.Errorf("read environment: %w", err)
	}

	return cfg, nil
}

func (c *Config) readYAML(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = file.Close() }()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(c); err != nil {
		return fmt.Errorf("decode config %q: %w", path, err)
	}

	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("decode config %q: %w", path, err)
		}

		return fmt.Errorf("config %q must contain one YAML document", path)
	}

	return nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("data directory must not be empty")
	}
	if c.LogFormat != LogFormatJSON && c.LogFormat != LogFormatPlain {
		return fmt.Errorf("invalid log format %q: expected json or plain", c.LogFormat)
	}

	if c.ID == "" || strings.TrimSpace(c.ID) != c.ID {
		return fmt.Errorf("id must be non-empty and have no surrounding whitespace")
	}

	if _, _, err := net.SplitHostPort(c.ListenAddress); err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}

	for id, address := range c.Peers {
		if id == "" || strings.TrimSpace(id) != id || id == c.ID {
			return fmt.Errorf("invalid peer ID %q: expected another node's ID", id)
		}

		host, _, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return fmt.Errorf("invalid address for peer %q: %q", id, address)
		}
	}

	return nil
}

func (c Config) RaftConfig() raft.Config {
	peers := make([]raft.NodeID, 0, len(c.Peers))
	for id := range c.Peers {
		peers = append(peers, raft.NodeID(id))
	}

	slices.Sort(peers)

	return raft.Config{
		ID: raft.NodeID(c.ID), Peers: peers,
		ElectionTimeoutMin: c.ElectionTimeoutMin,
		ElectionTimeoutMax: c.ElectionTimeoutMax,
		HeartbeatInterval:  c.HeartbeatInterval,
		RPCTimeout:         c.RPCTimeout,
	}
}

func (c Config) PeerAddresses() map[raft.NodeID]string {
	addresses := make(map[raft.NodeID]string, len(c.Peers))
	for id, address := range c.Peers {
		addresses[raft.NodeID(id)] = address
	}

	return addresses
}
