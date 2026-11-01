package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kestfor/raft/internal/config"
	"github.com/kestfor/raft/pkg/raft"
)

func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"HTTP_LISTEN_ADDRESS", "HTTP_NODE_URLS", "RAFTNODE_CONFIG", "RAFT_DATA_DIR", "RAFT_NODE_ID", "RAFT_LISTEN_ADDRESS", "RAFT_PEERS", "RAFT_ELECTION_TIMEOUT_MIN", "RAFT_ELECTION_TIMEOUT_MAX", "RAFT_HEARTBEAT_INTERVAL", "RAFT_RPC_TIMEOUT", "LOG_LEVEL", "LOG_FORMAT"} {
		t.Setenv(name, "")
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raft.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestCommandConfigurationPrecedence(t *testing.T) {
	clearEnvironment(t)
	path := writeConfig(t, `raft:
  node_id: file-node
  listen_address: 127.0.0.1:9001
  peers:
    old-node: localhost:9002
  election_timeout_min: 800ms
log:
  level: debug
  format: plain
`)
	t.Setenv("RAFTNODE_CONFIG", path)
	t.Setenv("RAFT_NODE_ID", "env-node")
	t.Setenv("RAFT_DATA_DIR", "from-env")
	t.Setenv("RAFT_LISTEN_ADDRESS", "127.0.0.1:9101")
	t.Setenv("RAFT_HEARTBEAT_INTERVAL", "150ms")
	t.Setenv("HTTP_LISTEN_ADDRESS", ":18002")
	var got config.Config
	called := false
	ctx := t.Context()
	cmd := newCommand(func(received context.Context, cfg config.Config) error {
		called = true
		if received != ctx {
			t.Error("command did not forward its context")
		}

		got = cfg

		return nil
	})
	cmd.SetArgs(
		[]string{
			"--raft-node-id=flag-node",
			"--raft-peers=NodeB=[::1]:9202",
			"--log-level=info",
			"--log-format=json",
			"--raft-data-dir=from-flag",
			"--http-listen-address=:18003",
			"--http-node-urls=NodeB=http://localhost:18004",
		},
	)
	if err := cmd.ExecuteContext(ctx); err != nil {
		t.Fatal(err)
	}

	if !called || got.Raft.NodeID != "flag-node" || got.Raft.DataDir != "from-flag" ||
		got.Raft.ListenAddress != "127.0.0.1:9101" ||
		got.Log.Level != slog.LevelInfo ||
		got.Log.Format != config.LogFormatJSON ||
		got.Raft.ElectionTimeoutMin != 800*time.Millisecond ||
		got.Raft.ElectionTimeoutMax != config.Default().Raft.ElectionTimeoutMax ||
		got.Raft.HeartbeatInterval != 150*time.Millisecond {
		t.Fatalf("wrong precedence: %+v", got)
	}

	if len(got.Raft.Peers) != 1 || got.Raft.Peers["NodeB"] != "[::1]:9202" {
		t.Fatalf("peer override must replace the entire map: %v", got.Raft.Peers)
	}

	if got.HTTP.ListenAddress != ":18003" || got.HTTP.NodeURLs["NodeB"] != "http://localhost:18004" {
		t.Fatalf("HTTP flag overrides: %+v", got)
	}
}

func TestExplicitDefaultsOverrideConfiguration(t *testing.T) {
	clearEnvironment(t)
	path := writeConfig(t, `raft:
  node_id: a
  listen_address: 127.0.0.1:9001
  peers:
    b: localhost:9002
  election_timeout_min: 800ms
  election_timeout_max: 2s
  heartbeat_interval: 150ms
  rpc_timeout: 300ms
log:
  level: debug
  format: plain
`)
	cmd := newCommand(func(_ context.Context, cfg config.Config) error {
		defaults := config.Default()
		if cfg.Raft.ListenAddress != defaults.Raft.ListenAddress ||
			cfg.Raft.ElectionTimeoutMin != defaults.Raft.ElectionTimeoutMin ||
			cfg.Raft.ElectionTimeoutMax != defaults.Raft.ElectionTimeoutMax ||
			cfg.Raft.HeartbeatInterval != defaults.Raft.HeartbeatInterval ||
			cfg.Raft.RPCTimeout != defaults.Raft.RPCTimeout ||
			cfg.Log.Level != defaults.Log.Level ||
			cfg.Log.Format != defaults.Log.Format ||
			len(cfg.Raft.Peers) != 1 {
			t.Errorf("explicit flag defaults did not override YAML: %+v", cfg)
		}

		return nil
	})
	cmd.SetArgs(
		[]string{
			"--config",
			path,
			"--raft-listen-address=:9000",
			"--raft-election-timeout-min=500ms",
			"--raft-election-timeout-max=1s",
			"--raft-heartbeat-interval=100ms",
			"--raft-rpc-timeout=200ms",
			"--log-level=info",
			"--log-format=json",
		},
	)
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestConfigFlagOverridesEnvironmentPath(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("RAFTNODE_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
	cmd := newCommand(func(_ context.Context, cfg config.Config) error {
		if cfg.Raft.NodeID != "a" {
			t.Errorf("unexpected config: %+v", cfg)
		}

		return nil
	})
	cmd.SetArgs([]string{"--config", writeConfig(t, "raft:\n  node_id: a\n")})
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCommandHelpDoesNotStartNode(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("RAFTNODE_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
	cmd := newCommand(func(context.Context, config.Config) error {
		t.Fatal("help started the node")
		return nil
	})
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetArgs([]string{"--help"})
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(output.String(), "--config") {
		t.Fatal("help does not describe config flag")
	}
}

func TestCommandRejectsInvalidArguments(t *testing.T) {
	clearEnvironment(t)
	for _, args := range [][]string{
		{},
		{"--raft-node-id=a", "unexpected"},
		{"--unknown"},
		{"--raft-node-id=a", "--raft-peers=a=localhost:9001"},
		{"--raft-node-id=a", "--raft-peers=b"},
		{"--raft-node-id=a", "--raft-listen-address=invalid"},
		{"--raft-node-id=a", "--log-level=nope"},
		{"--raft-node-id=a", "--log-format=xml"},
		{"--raft-node-id=a", "--raft-heartbeat-interval=soon"},
	} {
		cmd := newCommand(func(context.Context, config.Config) error {
			t.Errorf("invalid arguments started the node: %v", args)
			return nil
		})
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs(args)
		if err := cmd.ExecuteContext(t.Context()); err == nil {
			t.Errorf("accepted invalid arguments: %v", args)
		}
	}
}

func TestLoggerFormat(t *testing.T) {
	for _, format := range []string{config.LogFormatJSON, config.LogFormatPlain} {
		t.Run(format, func(t *testing.T) {
			var output bytes.Buffer
			cfg := config.Default()
			cfg.Log.Format = format
			newLogger(cfg.Log, &output).Info("sample", "node", "a")

			if format == config.LogFormatJSON {
				var record map[string]any
				if err := json.Unmarshal(output.Bytes(), &record); err != nil {
					t.Fatalf("invalid JSON log: %v", err)
				}

				if record["msg"] != "sample" || record["node"] != "a" {
					t.Fatalf("unexpected JSON log: %v", record)
				}

				return
			}

			if !strings.Contains(output.String(), "INFO") || !strings.Contains(output.String(), "sample node=a") {
				t.Fatalf("unexpected plain log: %s", output.String())
			}
		})
	}
}

func TestLoggerLevel(t *testing.T) {
	var output bytes.Buffer
	cfg := config.Default()
	cfg.Log.Level = slog.LevelWarn
	logger := newLogger(cfg.Log, &output)
	logger.Info("hidden")
	logger.Warn("visible")

	if strings.Contains(output.String(), "hidden") || !strings.Contains(output.String(), "visible") {
		t.Fatalf("wrong level filtering: %s", output.String())
	}
}

func TestLoggerForcedColor(t *testing.T) {
	t.Setenv("CLICOLOR_FORCE", "1")
	t.Setenv("NO_COLOR", "")

	for _, tt := range []struct {
		format    string
		wantColor bool
	}{
		{config.LogFormatPlain, true},
		{config.LogFormatJSON, false},
	} {
		t.Run(tt.format, func(t *testing.T) {
			var output bytes.Buffer
			cfg := config.Default()
			cfg.Log.Format = tt.format
			newLogger(cfg.Log, &output).Info("sample", "node", "a")

			if gotColor := strings.Contains(output.String(), "\x1b["); gotColor != tt.wantColor {
				t.Fatalf("color = %t, want %t: %q", gotColor, tt.wantColor, output.String())
			}

			if tt.wantColor && !strings.Contains(output.String(), "\x1b[90mnode") {
				t.Fatalf("key is not explicit gray: %q", output.String())
			}
		})
	}
}

func TestRawProposeIsRejected(t *testing.T) {
	index, err := (protocolOnly{}).Propose(t.Context(), []byte("bypass KV"))
	if index != 0 || !errors.Is(err, raft.ErrInvalidRequest) {
		t.Fatalf("%d %v", index, err)
	}
}
