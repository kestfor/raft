package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kestfor/raft/pkg/raft"
)

func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"HTTP_LISTEN_ADDRESS", "HTTP_NODE_URLS", "RAFT_DATA_DIR", "RAFT_NODE_ID", "RAFT_LISTEN_ADDRESS", "RAFT_PEERS", "RAFT_ELECTION_TIMEOUT_MIN", "RAFT_ELECTION_TIMEOUT_MAX", "RAFT_HEARTBEAT_INTERVAL", "RAFT_RPC_TIMEOUT", "LOG_LEVEL", "LOG_FORMAT"} {
		t.Setenv(name, "")
	}
}

func configFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raft.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestLoadYAMLAndDefaults(t *testing.T) {
	clearEnvironment(t)
	cfg, err := Load(configFile(t, `raft:
  node_id: NodeA
  listen_address: 127.0.0.1:9000
  peers:
    NodeC: "[::1]:9002"
    NodeB: localhost:9001
  election_timeout_min: 800ms
log:
  level: debug
  format: plain
`))
	if err != nil {
		t.Fatal(err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	core := cfg.Raft.NodeConfig()
	if cfg.Raft.NodeID != "NodeA" || cfg.Log.Level != slog.LevelDebug || cfg.Log.Format != LogFormatPlain ||
		core.ElectionTimeoutMin != 800*time.Millisecond ||
		core.ElectionTimeoutMax != Default().Raft.ElectionTimeoutMax {
		t.Fatalf("unexpected config: %+v", cfg)
	}

	if !reflect.DeepEqual(core.Peers, []raft.NodeID{"NodeB", "NodeC"}) ||
		cfg.Raft.PeerAddresses()["NodeC"] != "[::1]:9002" {
		t.Fatalf("peer IDs or addresses changed: %+v", cfg)
	}
}

func TestEnvironmentOverridesYAML(t *testing.T) {
	clearEnvironment(t)
	values := map[string]string{
		"RAFT_DATA_DIR":             "env-data",
		"RAFT_NODE_ID":              "env-node",
		"RAFT_LISTEN_ADDRESS":       "127.0.0.1:9100",
		"RAFT_PEERS":                "NodeC=[::1]:9102,NodeB=localhost:9101",
		"RAFT_ELECTION_TIMEOUT_MIN": "900ms",
		"RAFT_ELECTION_TIMEOUT_MAX": "2s",
		"RAFT_HEARTBEAT_INTERVAL":   "150ms",
		"RAFT_RPC_TIMEOUT":          "300ms",
		"LOG_LEVEL":                 "info",
		"LOG_FORMAT":                "json",
	}

	for name, value := range values {
		t.Setenv(name, value)
	}

	cfg, err := Load(configFile(t, `raft:
  node_id: yaml-node
  peers:
    old-node: localhost:9001
log:
  level: debug
  format: plain
`))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		HTTP: HTTPConfig{ListenAddress: ":18000"},
		Raft: RaftConfig{
			DataDir: "env-data", NodeID: "env-node", ListenAddress: "127.0.0.1:9100",
			Peers:              map[string]string{"NodeB": "localhost:9101", "NodeC": "[::1]:9102"},
			ElectionTimeoutMin: 900 * time.Millisecond, ElectionTimeoutMax: 2 * time.Second,
			HeartbeatInterval: 150 * time.Millisecond, RPCTimeout: 300 * time.Millisecond,
		},
		Log: LogConfig{Level: slog.LevelInfo, Format: LogFormatJSON},
	}

	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("config = %+v, want %+v", cfg, want)
	}
}

func TestLoadWithoutFile(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("RAFT_NODE_ID", "a")
	cfg, err := Load("")
	if err != nil || cfg.Raft.NodeID != "a" || cfg.Raft.ListenAddress != ":9000" || cfg.Log.Format != LogFormatJSON ||
		cfg.Raft.ElectionTimeoutMin != Default().Raft.ElectionTimeoutMin {
		t.Fatalf("config = %+v, error = %v", cfg, err)
	}
}

func TestLoadRejectsInvalidYAML(t *testing.T) {
	clearEnvironment(t)
	for name, content := range map[string]string{
		"legacy flat config": "id: a\nlisten: :9000\nhttp_listen: :18000\n",
		"unknown HTTP field": "http:\n  listen: :18000\n",
		"unknown Raft field": "raft:\n  node_id: a\n  heartbeet: 100ms\n",
		"unknown field":      "heartbeet: 100ms\nraft:\n  node_id: a\n",
		"invalid duration":   "raft:\n  node_id: a\n  heartbeat_interval: soon\n",
		"invalid level":      "raft:\n  node_id: a\nlog:\n  level: nope\n",
		"duplicate peer":     "raft:\n  node_id: a\n  peers:\n    b: localhost:9001\n    b: localhost:9002\n",
		"multiple documents": "raft:\n  node_id: a\n---\nraft:\n  node_id: b\n",
		"syntax":             "raft:\n  peers: [\n",
		"empty file":         "",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(configFile(t, content)); err == nil {
				t.Fatal("accepted invalid YAML")
			}
		})
	}

	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("ignored missing explicit config file")
	}
}

func TestExampleConfigurations(t *testing.T) {
	clearEnvironment(t)
	paths, err := filepath.Glob("../../configs/*/*.yaml")
	if err != nil {
		t.Fatal(err)
	}

	paths = append(paths, "../../configs/raftnode.yaml")
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			cfg, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}

			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLoadRejectsInvalidEnvironment(t *testing.T) {
	for name, value := range map[string]string{"RAFT_HEARTBEAT_INTERVAL": "soon", "LOG_LEVEL": "nope", "RAFT_PEERS": "b"} {
		t.Run(name, func(t *testing.T) {
			clearEnvironment(t)
			t.Setenv(name, value)
			if _, err := Load(""); err == nil {
				t.Fatal("accepted invalid environment")
			}
		})
	}
}

func TestValidateDeploymentSettings(t *testing.T) {
	for name, change := range map[string]func(*Config){
		"empty data directory": func(c *Config) { c.Raft.DataDir = "" },
		"empty ID":             func(c *Config) { c.Raft.NodeID = "" },
		"whitespace ID":        func(c *Config) { c.Raft.NodeID = " a" },
		"bad listen":           func(c *Config) { c.Raft.ListenAddress = "invalid" },
		"self peer":            func(c *Config) { c.Raft.Peers = map[string]string{"a": "localhost:9001"} },
		"empty peer":           func(c *Config) { c.Raft.Peers = map[string]string{"": "localhost:9001"} },
		"missing peer host":    func(c *Config) { c.Raft.Peers = map[string]string{"b": ":9001"} },
		"bad peer address":     func(c *Config) { c.Raft.Peers = map[string]string{"b": "invalid"} },
		"bad log format":       func(c *Config) { c.Log.Format = "xml" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := Default()
			cfg.Raft.NodeID = "a"
			change(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}

func TestHTTPConfiguration(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("HTTP_LISTEN_ADDRESS", "127.0.0.1:18002")
	t.Setenv("HTTP_NODE_URLS", "a=http://localhost:18001,b=http://localhost:18002")
	cfg, err := Load(
		configFile(t, "http:\n  listen_address: :18003\n  node_urls:\n    b: http://old:18000\nraft:\n  node_id: a\n"),
	)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.HTTP.ListenAddress != "127.0.0.1:18002" || cfg.HTTP.NodeURLs["b"] != "http://localhost:18002" {
		t.Fatalf("HTTP overrides: %+v", cfg)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	for _, origin := range []string{"localhost:18001", "ftp://host", "http://host/path", "http://host?x=1", "http://user@host"} {
		cfg.HTTP.NodeURLs = map[string]string{"b": origin}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("accepted origin %q", origin)
		}
	}

	cfg.HTTP.NodeURLs = nil
	cfg.HTTP.ListenAddress = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Fatal("accepted invalid HTTP listen")
	}
}
