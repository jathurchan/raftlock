package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/server"
	"github.com/jathurchan/raftlock/types"
)

const (
	AppName    = "RaftLock Server"
	AppVersion = "v1.0.0"
	AppDesc    = "A distributed lock service built on Raft consensus using Go"
)

// AppConfig holds application-level and nested server configuration.
type AppConfig struct {
	LogLevel    string
	EnablePprof bool
	ShowVersion bool

	ServerConfig server.RaftLockServerConfig
}

// main is the entry point of the application.
func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

// run handles initialization, startup, and shutdown logic.
func run() error {
	cfg, err := parseAndValidateFlags()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	if cfg.ShowVersion {
		fmt.Printf("%s %s\n%s\n", AppName, AppVersion, AppDesc)
		return nil
	}

	appLogger := createLogger(cfg.LogLevel)
	appLogger.Infow("Initializing RaftLock server",
		"nodeID", cfg.ServerConfig.NodeID,
		"dataDir", cfg.ServerConfig.DataDir,
		"version", AppVersion,
	)

	if cfg.EnablePprof {
		startPprofServer(appLogger)
	}

	raftServer, err := buildServer(cfg, appLogger)
	if err != nil {
		return fmt.Errorf("failed to build server: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := raftServer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	appLogger.Infow("RaftLock server started",
		"nodeID", raftServer.GetNodeID(),
		"clientAPIAddress", cfg.ServerConfig.ClientAPIAddress,
	)

	waitForShutdown(appLogger)
	return gracefulShutdown(raftServer, cfg.ServerConfig.ShutdownTimeout, appLogger)
}

// parseAndValidateFlags parses CLI flags into AppConfig and validates required fields.
func parseAndValidateFlags() (*AppConfig, error) {
	cfg := &AppConfig{
		ServerConfig: server.DefaultRaftLockServerConfig(),
	}

	var peersStr string

	flag.StringVar(
		(*string)(&cfg.ServerConfig.NodeID),
		"node-id",
		"",
		"Unique identifier for this node (required)",
	)
	flag.StringVar(
		&cfg.ServerConfig.ListenAddress,
		"listen",
		"0.0.0.0:8080",
		"gRPC listen address",
	)
	flag.StringVar(
		&cfg.ServerConfig.ClientAPIAddress,
		"client-api-listen",
		"127.0.0.1:9090",
		"gRPC listen address for client API requests",
	)
	flag.StringVar(
		&cfg.ServerConfig.DataDir,
		"data-dir",
		"./data",
		"Directory for data persistence",
	)
	flag.StringVar(
		&peersStr,
		"peers",
		"",
		"Comma-separated peers (e.g. node1=addr1,node2=addr2)",
	)
	flag.StringVar(
		&cfg.LogLevel,
		"log-level",
		"info",
		"Log level: debug, info, warn, error",
	)
	flag.BoolVar(
		&cfg.ShowVersion,
		"version",
		false,
		"Print version and exit",
	)
	flag.BoolVar(
		&cfg.EnablePprof,
		"pprof",
		false,
		"Enable pprof server on :6060",
	)

	flag.IntVar(
		&cfg.ServerConfig.MaxRequestSize,
		"max-request-size",
		4*1024*1024,
		"Max request size (bytes)",
	)
	flag.IntVar(
		&cfg.ServerConfig.MaxConcurrentReqs,
		"max-concurrent",
		1000,
		"Max concurrent requests",
	)
	flag.DurationVar(
		&cfg.ServerConfig.RequestTimeout,
		"request-timeout",
		30*time.Second,
		"Request timeout",
	)
	flag.DurationVar(
		&cfg.ServerConfig.ShutdownTimeout,
		"shutdown-timeout",
		30*time.Second,
		"Shutdown timeout",
	)

	flag.IntVar(
		&cfg.ServerConfig.RaftConfig.Options.HeartbeatTickCount,
		"heartbeat-ticks",
		1,
		"Heartbeat tick count",
	)
	flag.IntVar(
		&cfg.ServerConfig.RaftConfig.Options.ElectionTickCount,
		"election-ticks",
		24,
		"Election timeout ticks",
	)
	flag.IntVar(
		&cfg.ServerConfig.RaftConfig.Options.SnapshotThreshold,
		"snapshot-threshold",
		10000,
		"Snapshot threshold",
	)

	flag.BoolVar(
		&cfg.ServerConfig.EnableRateLimit,
		"rate-limit",
		false,
		"Enable rate limiting",
	)
	flag.IntVar(
		&cfg.ServerConfig.RateLimit,
		"rate-limit-rps",
		100,
		"Requests per second per client",
	)
	flag.IntVar(
		&cfg.ServerConfig.RateLimitBurst,
		"rate-limit-burst",
		200,
		"Rate limit burst",
	)

	flag.Parse()

	if cfg.ShowVersion {
		return cfg, nil
	}

	if cfg.ServerConfig.NodeID == "" {
		return nil, errors.New("--node-id is required")
	}
	if peersStr == "" {
		return nil, errors.New("--peers is required")
	}
	if cfg.ServerConfig.DataDir == "" {
		return nil, errors.New("--data-dir is required")
	}

	peers, err := parsePeers(peersStr)
	if err != nil {
		return nil, fmt.Errorf("invalid peer configuration: %w", err)
	}

	if _, exists := peers[cfg.ServerConfig.NodeID]; !exists {
		return nil, fmt.Errorf(
			"current node-id %q must be included in the peers list",
			cfg.ServerConfig.NodeID,
		)
	}

	cfg.ServerConfig.Peers = peers

	if err := cfg.ServerConfig.Validate(); err != nil {
		return nil, fmt.Errorf("server configuration validation failed: %w", err)
	}

	return cfg, nil
}

// createLogger returns a logger instance.
func createLogger(level string) logger.Logger {
	return logger.NewStdLogger(level)
}

// startPprofServer starts an HTTP server for pprof diagnostics on port 6060.
func startPprofServer(logger logger.Logger) {
	go func() {
		logger.Infow("pprof server started", "address", ":6060")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			logger.Errorw("pprof server error", "error", err)
		}
	}()
}

// buildServer constructs the RaftLockServer using a builder pattern from the application config.
func buildServer(cfg *AppConfig, appLogger logger.Logger) (server.RaftLockServer, error) {
	appLogger.Infow(
		"Cluster configuration validated",
		"totalNodes",
		len(cfg.ServerConfig.Peers),
		"peers",
		formatPeers(cfg.ServerConfig.Peers),
	)

	return server.NewRaftLockServerBuilder().
		WithNodeID(cfg.ServerConfig.NodeID).
		WithListenAddress(cfg.ServerConfig.ListenAddress). // Raft internal listen address
		WithClientAPIAddress(cfg.ServerConfig.ClientAPIAddress).
		WithPeers(cfg.ServerConfig.Peers).
		WithDataDir(cfg.ServerConfig.DataDir).
		WithRaftConfig(cfg.ServerConfig.RaftConfig).
		WithLimits(cfg.ServerConfig.MaxRequestSize, cfg.ServerConfig.MaxResponseSize, cfg.ServerConfig.MaxConcurrentReqs).
		WithTimeouts(cfg.ServerConfig.RequestTimeout, cfg.ServerConfig.ShutdownTimeout, cfg.ServerConfig.RedirectTimeout).
		WithLogger(appLogger).
		WithMetrics(server.NewNoOpServerMetrics()).
		WithLeaderRedirect(true).
		Build()
}

// parsePeers parses the peer string into a map of Raft peer configurations.
func parsePeers(peersStr string) (map[types.NodeID]raft.PeerConfig, error) {
	peers := make(map[types.NodeID]raft.PeerConfig)
	if peersStr == "" {
		return peers, nil
	}
	for _, entry := range strings.Split(peersStr, ",") {
		parts := strings.Split(strings.TrimSpace(entry), "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer format: %q", entry)
		}
		nodeID := types.NodeID(strings.TrimSpace(parts[0]))
		addr := strings.TrimSpace(parts[1])
		if nodeID == "" || addr == "" {
			return nil, fmt.Errorf("invalid peer entry: %q", entry)
		}
		peers[nodeID] = raft.PeerConfig{ID: nodeID, Address: addr}
	}
	return peers, nil
}

// formatPeers returns a slice of "nodeID=address" strings for logging.
func formatPeers(peers map[types.NodeID]raft.PeerConfig) []string {
	var result []string
	for nodeID, config := range peers {
		result = append(result, fmt.Sprintf("%s=%s", nodeID, config.Address))
	}
	return result
}

// waitForShutdown blocks until an interrupt or termination signal is received.
func waitForShutdown(logger logger.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	logger.Infow("Shutdown signal received", "signal", sig.String())
}

// gracefulShutdown stops the server with a timeout context.
func gracefulShutdown(
	raftServer server.RaftLockServer,
	timeout time.Duration,
	logger logger.Logger,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	logger.Infow("Shutting down", "timeout", timeout)
	if err := raftServer.Stop(ctx); err != nil {
		logger.Errorw("Shutdown failed", "error", err)
		return fmt.Errorf("shutdown error: %w", err)
	}
	logger.Infow("Shutdown complete")
	return nil
}
