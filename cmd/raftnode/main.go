package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"charm.land/lipgloss/v2"
	charmlog "charm.land/log/v2"
	httpapi "github.com/kestfor/raft/internal/api/http"
	"github.com/kestfor/raft/internal/config"
	"github.com/kestfor/raft/internal/kv"
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
		newLogger(config.Default().Log, os.Stderr).ErrorContext(ctx, "raftnode failed", slog.Any("error", err))
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
	flags.StringVar(
		&overrides.HTTP.ListenAddress,
		"http-listen-address",
		overrides.HTTP.ListenAddress,
		"REST listen address (HTTP_LISTEN_ADDRESS)",
	)
	flags.StringToStringVar(
		&overrides.HTTP.NodeURLs,
		"http-node-urls",
		nil,
		"public REST origins: id=http://host:port (HTTP_NODE_URLS)",
	)
	flags.StringVar(&path, "config", os.Getenv("RAFTNODE_CONFIG"), "YAML configuration file (RAFTNODE_CONFIG)")
	flags.StringVar(&overrides.Raft.NodeID, "raft-node-id", overrides.Raft.NodeID, "unique node ID (RAFT_NODE_ID)")
	flags.StringVar(
		&overrides.Raft.DataDir,
		"raft-data-dir",
		overrides.Raft.DataDir,
		"node storage directory (RAFT_DATA_DIR)",
	)
	flags.StringVar(
		&overrides.Raft.ListenAddress,
		"raft-listen-address",
		overrides.Raft.ListenAddress,
		"gRPC listen address (RAFT_LISTEN_ADDRESS)",
	)
	flags.StringToStringVar(
		&overrides.Raft.Peers,
		"raft-peers",
		nil,
		"other members: id=host:port,id=host:port (RAFT_PEERS)",
	)

	flags.DurationVar(
		&overrides.Raft.ElectionTimeoutMin,
		"raft-election-timeout-min",
		overrides.Raft.ElectionTimeoutMin,
		"minimum election timeout (RAFT_ELECTION_TIMEOUT_MIN)",
	)
	flags.DurationVar(
		&overrides.Raft.ElectionTimeoutMax,
		"raft-election-timeout-max",
		overrides.Raft.ElectionTimeoutMax,
		"maximum election timeout, exclusive (RAFT_ELECTION_TIMEOUT_MAX)",
	)
	flags.DurationVar(
		&overrides.Raft.HeartbeatInterval,
		"raft-heartbeat-interval",
		overrides.Raft.HeartbeatInterval,
		"heartbeat and vote retry interval (RAFT_HEARTBEAT_INTERVAL)",
	)
	flags.DurationVar(
		&overrides.Raft.RPCTimeout,
		"raft-rpc-timeout",
		overrides.Raft.RPCTimeout,
		"outgoing RPC timeout (RAFT_RPC_TIMEOUT)",
	)

	flags.TextVar(&overrides.Log.Level, "log-level", overrides.Log.Level, "debug, info, warn, error (LOG_LEVEL)")
	flags.StringVar(&overrides.Log.Format, "log-format", overrides.Log.Format, "json or plain (LOG_FORMAT)")

	return cmd
}

// Unspecified flags must not replace values loaded from YAML or the environment.
func applyOverrides(cfg *config.Config, overrides config.Config, flags *pflag.FlagSet) {
	flags.Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "http-listen-address":
			cfg.HTTP.ListenAddress = overrides.HTTP.ListenAddress
		case "http-node-urls":
			cfg.HTTP.NodeURLs = overrides.HTTP.NodeURLs
		case "raft-node-id":
			cfg.Raft.NodeID = overrides.Raft.NodeID
		case "raft-data-dir":
			cfg.Raft.DataDir = overrides.Raft.DataDir
		case "raft-listen-address":
			cfg.Raft.ListenAddress = overrides.Raft.ListenAddress
		case "raft-peers":
			cfg.Raft.Peers = overrides.Raft.Peers
		case "raft-election-timeout-min":
			cfg.Raft.ElectionTimeoutMin = overrides.Raft.ElectionTimeoutMin
		case "raft-election-timeout-max":
			cfg.Raft.ElectionTimeoutMax = overrides.Raft.ElectionTimeoutMax
		case "raft-heartbeat-interval":
			cfg.Raft.HeartbeatInterval = overrides.Raft.HeartbeatInterval
		case "raft-rpc-timeout":
			cfg.Raft.RPCTimeout = overrides.Raft.RPCTimeout
		case "log-level":
			cfg.Log.Level = overrides.Log.Level
		case "log-format":
			cfg.Log.Format = overrides.Log.Format
		}
	})
}

func run(ctx context.Context, cfg config.Config) (result error) {
	logger := newLogger(cfg.Log, os.Stdout)
	raftConfig := cfg.Raft.NodeConfig()
	raftConfig.Logger = logger
	if err := makeDataDir(cfg.Raft.DataDir); err != nil {
		return err
	}

	journal, err := wal.Open(filepath.Join(cfg.Raft.DataDir, "raft.wal"))
	if err != nil {
		return err
	}

	defer func() { result = errors.Join(result, journal.Close()) }()
	raftConfig.Storage = journal

	client, err := grpctransport.NewClient(
		cfg.Raft.PeerAddresses(),
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

	listener, err := net.Listen("tcp", cfg.Raft.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	defer func() { _ = listener.Close() }()

	server := grpc.NewServer()
	grpctransport.Register(server, protocolOnly{Node: node})
	reflection.Register(server)
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	store := kv.New(raftLog{node: node})
	httpListener, err := net.Listen("tcp", cfg.HTTP.ListenAddress)
	if err != nil {
		return fmt.Errorf("HTTP listen: %w", err)
	}

	defer func() { _ = httpListener.Close() }()

	httpServer := &http.Server{
		Handler:           httpapi.New(store, cfg.HTTP.NodeURLs, 5*time.Second),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	done := make(chan error, 4)
	go func() { done <- node.Run(ctx) }()
	go func() { done <- store.Run(ctx) }()
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, grpc.ErrServerStopped) {
			err = nil
		}

		done <- err
	}()
	go func() {
		err := httpServer.Serve(httpListener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}

		done <- err
	}()

	logger.InfoContext(ctx, "node listening", "grpc", listener.Addr().String(), "http", httpListener.Addr().String())
	remaining := 4
	select {
	case <-ctx.Done():
	case result = <-done:
		remaining--
	}

	healthServer.Shutdown()
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		_ = httpServer.Close()
		result = errors.Join(result, err)
	}

	stopServer(server)
	for range remaining {
		result = errors.Join(result, <-done)
	}

	return result
}

func newLogger(cfg config.LogConfig, output io.Writer) *slog.Logger {
	format := charmlog.JSONFormatter
	if cfg.Format == config.LogFormatPlain {
		format = charmlog.TextFormatter
	}

	handler := charmlog.NewWithOptions(output, charmlog.Options{
		Level:           charmlog.Level(cfg.Level),
		Formatter:       format,
		ReportTimestamp: true,
		TimeFormat:      time.Kitchen,
	})
	if cfg.Format == config.LogFormatPlain {
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
