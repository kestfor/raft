package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/kestfor/raft/pkg/raft"
	"go.yaml.in/yaml/v3"
)

type Config struct {
	HTTP HTTPConfig `yaml:"http" envPrefix:"HTTP_"`
	Raft RaftConfig `yaml:"raft" envPrefix:"RAFT_"`
	Log  LogConfig  `yaml:"log"  envPrefix:"LOG_"`
}

func Default() Config {
	timings := raft.DefaultConfig("", nil)

	return Config{
		HTTP: HTTPConfig{ListenAddress: ":18000"},
		Raft: RaftConfig{
			ListenAddress:      ":9000",
			DataDir:            "data",
			ElectionTimeoutMin: timings.ElectionTimeoutMin,
			ElectionTimeoutMax: timings.ElectionTimeoutMax,
			HeartbeatInterval:  timings.HeartbeatInterval,
			RPCTimeout:         timings.RPCTimeout,
		},
		Log: LogConfig{Level: slog.LevelInfo, Format: LogFormatJSON},
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
	if err := c.HTTP.validate(); err != nil {
		return fmt.Errorf("http: %w", err)
	}

	if err := c.Raft.validate(); err != nil {
		return fmt.Errorf("raft: %w", err)
	}

	if err := c.Log.validate(); err != nil {
		return fmt.Errorf("log: %w", err)
	}

	return nil
}

// HTTPConfig describes the client-facing API. NodeURLs must be reachable by
// clients: they are redirect destinations, not inter-node transport addresses.
type HTTPConfig struct {
	ListenAddress string            `yaml:"listen_address" env:"LISTEN_ADDRESS"`
	NodeURLs      map[string]string `yaml:"node_urls"      env:"NODE_URLS"      envKeyValSeparator:"="`
}

func (c HTTPConfig) validate() error {
	if _, _, err := net.SplitHostPort(c.ListenAddress); err != nil {
		return fmt.Errorf("listen_address: %w", err)
	}

	for id, address := range c.NodeURLs {
		u, err := url.Parse(address)
		if id == "" || strings.TrimSpace(id) != id || err != nil || u.Host == "" ||
			(u.Scheme != "http" && u.Scheme != "https") || u.User != nil ||
			u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return fmt.Errorf("node_urls: invalid public HTTP origin for node %q: %q", id, address)
		}
	}

	return nil
}

// RaftConfig describes node identity, durable storage and inter-node transport.
type RaftConfig struct {
	NodeID        string            `yaml:"node_id"        env:"NODE_ID"`
	ListenAddress string            `yaml:"listen_address" env:"LISTEN_ADDRESS"`
	Peers         map[string]string `yaml:"peers"          env:"PEERS"          envKeyValSeparator:"="`
	DataDir       string            `yaml:"data_dir"       env:"DATA_DIR"`

	ElectionTimeoutMin time.Duration `yaml:"election_timeout_min" env:"ELECTION_TIMEOUT_MIN"`
	ElectionTimeoutMax time.Duration `yaml:"election_timeout_max" env:"ELECTION_TIMEOUT_MAX"`
	HeartbeatInterval  time.Duration `yaml:"heartbeat_interval"   env:"HEARTBEAT_INTERVAL"`
	RPCTimeout         time.Duration `yaml:"rpc_timeout"          env:"RPC_TIMEOUT"`
}

func (c RaftConfig) validate() error {
	if strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("data_dir must not be empty")
	}

	if c.NodeID == "" || strings.TrimSpace(c.NodeID) != c.NodeID {
		return fmt.Errorf("node_id must be non-empty and have no surrounding whitespace")
	}

	if _, _, err := net.SplitHostPort(c.ListenAddress); err != nil {
		return fmt.Errorf("listen_address: %w", err)
	}

	for id, address := range c.Peers {
		if id == "" || strings.TrimSpace(id) != id || id == c.NodeID {
			return fmt.Errorf("peers: invalid node ID %q: expected another node's ID", id)
		}

		host, _, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return fmt.Errorf("peers: invalid address for node %q: %q", id, address)
		}
	}

	return nil
}

func (c RaftConfig) NodeConfig() raft.Config {
	peers := make([]raft.NodeID, 0, len(c.Peers))
	for id := range c.Peers {
		peers = append(peers, raft.NodeID(id))
	}

	slices.Sort(peers)

	return raft.Config{
		ID:                 raft.NodeID(c.NodeID),
		Peers:              peers,
		ElectionTimeoutMin: c.ElectionTimeoutMin,
		ElectionTimeoutMax: c.ElectionTimeoutMax,
		HeartbeatInterval:  c.HeartbeatInterval,
		RPCTimeout:         c.RPCTimeout,
	}
}

func (c RaftConfig) PeerAddresses() map[raft.NodeID]string {
	addresses := make(map[raft.NodeID]string, len(c.Peers))
	for id, address := range c.Peers {
		addresses[raft.NodeID(id)] = address
	}

	return addresses
}

const (
	LogFormatJSON  = "json"
	LogFormatPlain = "plain"
)

type LogConfig struct {
	Level  slog.Level `yaml:"level"  env:"LEVEL"`
	Format string     `yaml:"format" env:"FORMAT"`
}

func (c LogConfig) validate() error {
	if c.Format != LogFormatJSON && c.Format != LogFormatPlain {
		return fmt.Errorf("invalid format %q: expected json or plain", c.Format)
	}

	return nil
}
