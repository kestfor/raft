package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"charm.land/lipgloss/v2"
	charmlog "charm.land/log/v2"
	"github.com/kestfor/raft/internal/config"
	grpctransport "github.com/kestfor/raft/internal/transport/grpc"
	"github.com/kestfor/raft/pkg/raft"
	"github.com/kestfor/raft/pkg/wal"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := newCommand(run).ExecuteContext(ctx); err != nil {
		newLogger(config.Default(), os.Stderr).ErrorContext(ctx, "raftnode failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func newCommand(start func(context.Context, config.Config) error) *cobra.Command {
	var path string
	overrides := config.Default()

	cmd := &cobra.Command{
		Use:           "raftnode",
		Short:         "Run a Raft node",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(path)
			if err != nil {
				return err
			}

			applyOverrides(&cfg, overrides, cmd.Flags())
			if err := cfg.Validate(); err != nil {
				return err
			}

			return start(cmd.Context(), cfg)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&path, "config", os.Getenv("RAFT_CONFIG"), "YAML configuration file (RAFT_CONFIG)")
	flags.StringVar(&overrides.ID, "id", overrides.ID, "unique node ID (RAFT_ID)")
	flags.StringVar(&overrides.DataDir, "data-dir", overrides.DataDir, "node storage directory (RAFT_DATA_DIR)")
	flags.StringVar(&overrides.ListenAddress, "listen", overrides.ListenAddress, "gRPC listen address (RAFT_LISTEN)")
	flags.StringToStringVar(&overrides.Peers, "peers", nil, "other members: id=host:port,id=host:port (RAFT_PEERS)")

	flags.DurationVar(
		&overrides.ElectionTimeoutMin,
		"election-min",
		overrides.ElectionTimeoutMin,
		"minimum election timeout (RAFT_ELECTION_MIN)",
	)
	flags.DurationVar(
		&overrides.ElectionTimeoutMax,
		"election-max",
		overrides.ElectionTimeoutMax,
		"maximum election timeout, exclusive (RAFT_ELECTION_MAX)",
	)
	flags.DurationVar(
		&overrides.HeartbeatInterval,
		"heartbeat",
		overrides.HeartbeatInterval,
		"heartbeat and vote retry interval (RAFT_HEARTBEAT)",
	)
	flags.DurationVar(
		&overrides.RPCTimeout,
		"rpc-timeout",
		overrides.RPCTimeout,
		"outgoing RPC timeout (RAFT_RPC_TIMEOUT)",
	)

	flags.TextVar(&overrides.LogLevel, "log-level", overrides.LogLevel, "debug, info, warn, error (RAFT_LOG_LEVEL)")
	flags.StringVar(&overrides.LogFormat, "log-format", overrides.LogFormat, "json or plain (RAFT_LOG_FORMAT)")

	return cmd
}

// Unspecified flags must not replace values loaded from YAML or the environment.
func applyOverrides(cfg *config.Config, overrides config.Config, flags *pflag.FlagSet) {
	flags.Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "id":
			cfg.ID = overrides.ID
		case "data-dir":
			cfg.DataDir = overrides.DataDir
		case "listen":
			cfg.ListenAddress = overrides.ListenAddress
		case "peers":
			cfg.Peers = overrides.Peers
		case "election-min":
			cfg.ElectionTimeoutMin = overrides.ElectionTimeoutMin
		case "election-max":
			cfg.ElectionTimeoutMax = overrides.ElectionTimeoutMax
		case "heartbeat":
			cfg.HeartbeatInterval = overrides.HeartbeatInterval
		case "rpc-timeout":
			cfg.RPCTimeout = overrides.RPCTimeout
		case "log-level":
			cfg.LogLevel = overrides.LogLevel
		case "log-format":
			cfg.LogFormat = overrides.LogFormat
		}
	})
}

func run(ctx context.Context, cfg config.Config) (result error) {
	logger := newLogger(cfg, os.Stdout)
	raftConfig := cfg.RaftConfig()
	raftConfig.Logger = logger
	if err := makeDataDir(cfg.DataDir); err != nil {
		return err
	}

	journal, err := wal.Open(filepath.Join(cfg.DataDir, "raft.wal"))
	if err != nil {
		return err
	}

	defer func() { result = errors.Join(result, journal.Close()) }()
	raftConfig.Storage = journal

	client, err := grpctransport.NewClient(
		cfg.PeerAddresses(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}

	defer func() {
		if err := client.Close(); err != nil {
			logger.ErrorContext(ctx, "close transport", slog.Any("error", err))
		}
	}()

	node, err := raft.NewNode(raftConfig, client)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	defer func() { _ = listener.Close() }()

	server := grpc.NewServer()
	grpctransport.Register(server, node)
	reflection.Register(server)
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	nodeDone := make(chan error, 1)
	serverDone := make(chan error, 1)
	go func() { nodeDone <- node.Run(ctx) }()
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, grpc.ErrServerStopped) {
			err = nil
		}

		serverDone <- err
	}()

	logger.InfoContext(ctx, "gRPC listening",
		slog.String("node", string(cfg.ID)),
		slog.String("address", listener.Addr().String()),
	)

	var nodeExited, serverExited bool
	select {
	case <-ctx.Done():
	case result = <-nodeDone:
		nodeExited = true
	case result = <-serverDone:
		serverExited = true
	}

	healthServer.Shutdown()
	cancel()
	stopServer(server)

	if !nodeExited {
		result = errors.Join(result, <-nodeDone)
	}

	if !serverExited {
		result = errors.Join(result, <-serverDone)
	}

	return result
}

func newLogger(cfg config.Config, output io.Writer) *slog.Logger {
	format := charmlog.JSONFormatter
	if cfg.LogFormat == config.LogFormatPlain {
		format = charmlog.TextFormatter
	}

	handler := charmlog.NewWithOptions(output, charmlog.Options{
		Level:           charmlog.Level(cfg.LogLevel),
		Formatter:       format,
		ReportTimestamp: true,
		TimeFormat:      time.Kitchen,
	})
	if cfg.LogFormat == config.LogFormatPlain {
		styles := charmlog.DefaultStyles()
		styles.Key = styles.Key.Faint(false).Foreground(lipgloss.Color("8"))
		handler.SetStyles(styles)
	}

	return slog.New(handler)
}

// Sync each newly created directory's parent so the WAL's path survives a crash.
func makeDataDir(path string) error {
	path = filepath.Clean(path)
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("data path %q is not a directory", path)
		}

		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	parent := filepath.Dir(path)
	if err := makeDataDir(parent); err != nil {
		return err
	}

	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create data directory: %w", err)
	}

	dir, err := os.Open(parent)
	if err != nil {
		return err
	}

	return errors.Join(dir.Sync(), dir.Close())
}

func stopServer(server *grpc.Server) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()

	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		server.Stop()
		<-done
	}
}
