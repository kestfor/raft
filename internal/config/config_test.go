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
	for _, name := range []string{"RAFT_ID", "RAFT_LISTEN", "RAFT_PEERS", "RAFT_ELECTION_MIN", "RAFT_ELECTION_MAX", "RAFT_HEARTBEAT", "RAFT_RPC_TIMEOUT", "RAFT_LOG_LEVEL", "RAFT_LOG_FORMAT"} {
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
	cfg, err := Load(configFile(t, `id: NodeA
listen: 127.0.0.1:9000
peers:
  NodeC: "[::1]:9002"
  NodeB: localhost:9001
log_level: debug
log_format: plain
election_min: 800ms
`))
	if err != nil {
		t.Fatal(err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	core := cfg.RaftConfig()
	if cfg.ID != "NodeA" || cfg.LogLevel != slog.LevelDebug || cfg.LogFormat != LogFormatPlain || core.ElectionTimeoutMin != 800*time.Millisecond ||
		core.ElectionTimeoutMax != 10*time.Second {
		t.Fatalf("unexpected config: %+v", cfg)
	}

	if !reflect.DeepEqual(core.Peers, []raft.NodeID{"NodeB", "NodeC"}) || cfg.PeerAddresses()["NodeC"] != "[::1]:9002" {
		t.Fatalf("peer IDs or addresses changed: %+v", cfg)
	}
}

func TestEnvironmentOverridesYAML(t *testing.T) {
	clearEnvironment(t)
	values := map[string]string{
		"RAFT_ID":           "env-node",
		"RAFT_LISTEN":       "127.0.0.1:9100",
		"RAFT_PEERS":        "NodeC=[::1]:9102,NodeB=localhost:9101",
		"RAFT_ELECTION_MIN": "900ms",
		"RAFT_ELECTION_MAX": "2s",
		"RAFT_HEARTBEAT":    "150ms",
		"RAFT_RPC_TIMEOUT":  "300ms",
		"RAFT_LOG_LEVEL":    "info",
		"RAFT_LOG_FORMAT":   "json",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}

	cfg, err := Load(configFile(t, `id: yaml-node
peers:
  old-node: localhost:9001
log_level: debug
log_format: plain
`))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		ID: "env-node", ListenAddress: "127.0.0.1:9100",
		Peers:              map[string]string{"NodeB": "localhost:9101", "NodeC": "[::1]:9102"},
		ElectionTimeoutMin: 900 * time.Millisecond, ElectionTimeoutMax: 2 * time.Second,
		HeartbeatInterval: 150 * time.Millisecond, RPCTimeout: 300 * time.Millisecond, LogLevel: slog.LevelInfo,
		LogFormat: LogFormatJSON,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("config = %+v, want %+v", cfg, want)
	}
}

func TestLoadWithoutFile(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("RAFT_ID", "a")
	cfg, err := Load("")
	if err != nil || cfg.ID != "a" || cfg.ListenAddress != ":9000" || cfg.LogFormat != LogFormatJSON || cfg.ElectionTimeoutMin != 5*time.Second {
		t.Fatalf("config = %+v, error = %v", cfg, err)
	}
}

func TestLoadRejectsInvalidYAML(t *testing.T) {
	clearEnvironment(t)
	for name, content := range map[string]string{
		"unknown field":      "id: a\nheartbeet: 100ms\n",
		"invalid duration":   "id: a\nheartbeat: soon\n",
		"invalid level":      "id: a\nlog_level: nope\n",
		"duplicate peer":     "id: a\npeers:\n  b: localhost:9001\n  b: localhost:9002\n",
		"multiple documents": "id: a\n---\nid: b\n",
		"syntax":             "peers: [\n",
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

func TestLoadRejectsInvalidEnvironment(t *testing.T) {
	for name, value := range map[string]string{"RAFT_HEARTBEAT": "soon", "RAFT_LOG_LEVEL": "nope", "RAFT_PEERS": "b"} {
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
		"empty ID":          func(c *Config) { c.ID = "" },
		"whitespace ID":     func(c *Config) { c.ID = " a" },
		"bad listen":        func(c *Config) { c.ListenAddress = "invalid" },
		"self peer":         func(c *Config) { c.Peers = map[string]string{"a": "localhost:9001"} },
		"empty peer":        func(c *Config) { c.Peers = map[string]string{"": "localhost:9001"} },
		"missing peer host": func(c *Config) { c.Peers = map[string]string{"b": ":9001"} },
		"bad peer address":  func(c *Config) { c.Peers = map[string]string{"b": "invalid"} },
		"bad log format":    func(c *Config) { c.LogFormat = "xml" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := Default()
			cfg.ID = "a"
			change(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}
