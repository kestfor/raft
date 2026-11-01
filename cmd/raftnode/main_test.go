package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kestfor/raft/internal/config"
)

func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"RAFT_CONFIG", "RAFT_DATA_DIR", "RAFT_ID", "RAFT_LISTEN", "RAFT_PEERS", "RAFT_ELECTION_MIN", "RAFT_ELECTION_MAX", "RAFT_HEARTBEAT", "RAFT_RPC_TIMEOUT", "RAFT_LOG_LEVEL", "RAFT_LOG_FORMAT"} {
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
	path := writeConfig(t, `id: file-node
listen: 127.0.0.1:9001
peers:
  old-node: localhost:9002
log_level: debug
log_format: plain
election_min: 800ms
`)
	t.Setenv("RAFT_CONFIG", path)
	t.Setenv("RAFT_ID", "env-node")
	t.Setenv("RAFT_DATA_DIR", "from-env")
	t.Setenv("RAFT_LISTEN", "127.0.0.1:9101")
	t.Setenv("RAFT_HEARTBEAT", "150ms")
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
			"--id=flag-node",
			"--peers=NodeB=[::1]:9202",
			"--log-level=info",
			"--log-format=json",
			"--data-dir=from-flag",
		},
	)
	if err := cmd.ExecuteContext(ctx); err != nil {
		t.Fatal(err)
	}

	if !called || got.ID != "flag-node" || got.DataDir != "from-flag" || got.ListenAddress != "127.0.0.1:9101" ||
		got.LogLevel != slog.LevelInfo ||
		got.LogFormat != config.LogFormatJSON ||
		got.ElectionTimeoutMin != 800*time.Millisecond ||
		got.ElectionTimeoutMax != config.Default().ElectionTimeoutMax ||
		got.HeartbeatInterval != 150*time.Millisecond {
		t.Fatalf("wrong precedence: %+v", got)
	}

	if len(got.Peers) != 1 || got.Peers["NodeB"] != "[::1]:9202" {
		t.Fatalf("peer override must replace the entire map: %v", got.Peers)
	}
}

func TestExplicitDefaultsOverrideConfiguration(t *testing.T) {
	clearEnvironment(t)
	path := writeConfig(t, `id: a
listen: 127.0.0.1:9001
peers:
  b: localhost:9002
election_min: 800ms
election_max: 2s
heartbeat: 150ms
rpc_timeout: 300ms
log_level: debug
log_format: plain
`)
	cmd := newCommand(func(_ context.Context, cfg config.Config) error {
		defaults := config.Default()
		if cfg.ListenAddress != defaults.ListenAddress || cfg.ElectionTimeoutMin != defaults.ElectionTimeoutMin ||
			cfg.ElectionTimeoutMax != defaults.ElectionTimeoutMax ||
			cfg.HeartbeatInterval != defaults.HeartbeatInterval ||
			cfg.RPCTimeout != defaults.RPCTimeout ||
			cfg.LogLevel != defaults.LogLevel || cfg.LogFormat != defaults.LogFormat ||
			len(cfg.Peers) != 1 {
			t.Errorf("explicit flag defaults did not override YAML: %+v", cfg)
		}

		return nil
	})
	cmd.SetArgs(
		[]string{
			"--config",
			path,
			"--listen=:9000",
			"--election-min=500ms",
			"--election-max=1s",
			"--heartbeat=100ms",
			"--rpc-timeout=200ms",
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
	t.Setenv("RAFT_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
	cmd := newCommand(func(_ context.Context, cfg config.Config) error {
		if cfg.ID != "a" {
			t.Errorf("unexpected config: %+v", cfg)
		}

		return nil
	})
	cmd.SetArgs([]string{"--config", writeConfig(t, "id: a\n")})
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCommandHelpDoesNotStartNode(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("RAFT_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
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
		{"--id=a", "unexpected"},
		{"--unknown"},
		{"--id=a", "--peers=a=localhost:9001"},
		{"--id=a", "--peers=b"},
		{"--id=a", "--listen=invalid"},
		{"--id=a", "--log-level=nope"},
		{"--id=a", "--log-format=xml"},
		{"--id=a", "--heartbeat=soon"},
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
			cfg.LogFormat = format
			newLogger(cfg, &output).Info("sample", "node", "a")

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
	cfg.LogLevel = slog.LevelWarn
	logger := newLogger(cfg, &output)
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
			cfg.LogFormat = tt.format
			newLogger(cfg, &output).Info("sample", "node", "a")

			if gotColor := strings.Contains(output.String(), "\x1b["); gotColor != tt.wantColor {
				t.Fatalf("color = %t, want %t: %q", gotColor, tt.wantColor, output.String())
			}
			if tt.wantColor && !strings.Contains(output.String(), "\x1b[90mnode") {
				t.Fatalf("key is not explicit gray: %q", output.String())
			}
		})
	}
}
