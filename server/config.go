package server

import (
	"fmt"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

// RaftLockServerConfig holds the configuration settings for a RaftLock server instance.
type RaftLockServerConfig struct {
	// NodeID uniquely identifies this node in the Raft cluster.
	NodeID types.NodeID

	// ListenAddress is the gRPC server's bind address (e.g., "0.0.0.0:8080").
	ListenAddress string

	// Peers maps all known Raft nodes by NodeID, including this node itself.
	Peers map[types.NodeID]raft.PeerConfig

	// RaftConfig holds the Raft protocol-specific configuration.
	RaftConfig raft.Config

	// DataDir is the path to store Raft state (logs, snapshots, etc.).
	DataDir string

	RequestTimeout    time.Duration // Max time to handle a client request
	ShutdownTimeout   time.Duration // Max time allowed for graceful shutdown
	MaxRequestSize    int           // Maximum size of incoming requests (in bytes)
	MaxResponseSize   int           // Maximum size of outgoing responses (in bytes)
	MaxConcurrentReqs int           // Max number of requests processed in parallel

	EnableRateLimit bool          // Whether rate limiting is enforced
	RateLimit       int           // Requests per second allowed per client
	RateLimitBurst  int           // Burst capacity for client requests
	RateLimitWindow time.Duration // Time window used for rate calculation

	Logger  logger.Logger
	Metrics ServerMetrics

	HealthCheckInterval time.Duration // Frequency of internal health checks
	HealthCheckTimeout  time.Duration // Timeout for individual health checks

	EnableLeaderRedirect bool          // Redirect write requests to leader if not one
	RedirectTimeout      time.Duration // Timeout when attempting leader redirection
}

// DefaultRaftLockServerConfig returns a ServerConfig pre-populated with safe defaults.
// Callers must explicitly set NodeID, Peers, DataDir, and RaftConfig.ID.
func DefaultRaftLockServerConfig() RaftLockServerConfig {
	return RaftLockServerConfig{
		ListenAddress: "0.0.0.0:8080",
		Peers:         make(map[types.NodeID]raft.PeerConfig),
		RaftConfig: raft.Config{
			Options: raft.Options{
				ElectionTickCount:           raft.DefaultElectionTickCount,
				HeartbeatTickCount:          raft.DefaultHeartbeatTickCount,
				ElectionRandomizationFactor: raft.DefaultElectionRandomizationFactor,
				MaxLogEntriesPerRequest:     raft.DefaultMaxLogEntriesPerRequest,
				SnapshotThreshold:           raft.DefaultSnapshotThreshold,
				StorageSyncDelay:            raft.DefaultStorageSyncDelayTicks,
				LogCompactionMinEntries:     raft.DefaultLogCompactionMinEntries,
				ApplyTickCount:              raft.DefaultApplyTickCount,
				ApplyEntryTimeout:           raft.DefaultApplyEntryTimeout,
				FetchEntriesTimeout:         raft.DefaultFetchEntriesTimeout,
			},
			FeatureFlags: raft.FeatureFlags{
				EnableReadIndex:   raft.DefaultEnableReadIndex,
				EnableLeaderLease: raft.DefaultEnableLeaderLease,
				PreVoteEnabled:    raft.DefaultPreVoteEnabled,
			}.WithExplicitFlags(),
			TuningParams: raft.TuningParams{
				MaxApplyBatchSize:    raft.DefaultMaxApplyBatchSize,
				MaxSnapshotChunkSize: raft.DefaultMaxSnapshotChunkSize,
			},
		},
		RequestTimeout:       DefaultRequestTimeout,
		ShutdownTimeout:      DefaultShutdownTimeout,
		MaxRequestSize:       DefaultMaxRequestSize,
		MaxResponseSize:      DefaultMaxResponseSize,
		MaxConcurrentReqs:    DefaultMaxConcurrentRequests,
		EnableRateLimit:      false,
		RateLimit:            DefaultRateLimit,
		RateLimitBurst:       DefaultRateLimitBurst,
		RateLimitWindow:      DefaultRateLimitWindow,
		Logger:               logger.NewNoOpLogger(),
		Metrics:              NewNoOpServerMetrics(),
		HealthCheckInterval:  DefaultHealthCheckInterval,
		HealthCheckTimeout:   DefaultHealthCheckTimeout,
		EnableLeaderRedirect: true,
		RedirectTimeout:      DefaultRedirectTimeout,
	}
}

// Validate checks if the server configuration is valid.
func (c *RaftLockServerConfig) Validate() error {
	if c.NodeID == "" {
		return NewRaftLockServerConfigError("NodeID cannot be empty")
	}
	if c.ListenAddress == "" {
		return NewRaftLockServerConfigError("ListenAddress cannot be empty")
	}
	if c.DataDir == "" {
		return NewRaftLockServerConfigError("DataDir cannot be empty")
	}
	if c.Peers == nil {
		return NewRaftLockServerConfigError("Peers map cannot be nil")
	}
	if _, ok := c.Peers[c.NodeID]; !ok {
		return NewRaftLockServerConfigError(fmt.Sprintf("Peers must include an entry for NodeID %q", c.NodeID))
	}
	if c.RaftConfig.ID != "" && c.RaftConfig.ID != c.NodeID {
		return NewRaftLockServerConfigError(fmt.Sprintf("RaftConfig.ID (%s) must match NodeID (%s)", c.RaftConfig.ID, c.NodeID))
	}

	for peerID, cfg := range c.Peers {
		if peerID == "" {
			return NewRaftLockServerConfigError("Peer ID cannot be empty")
		}
		if cfg.Address == "" {
			return NewRaftLockServerConfigError(fmt.Sprintf("Peer %q must have a non-empty address", peerID))
		}
	}

	checkPositiveDuration := func(val time.Duration, name string) error {
		if val <= 0 {
			return NewRaftLockServerConfigError(fmt.Sprintf("%s must be positive", name))
		}
		return nil
	}

	checkPositiveInt := func(val int, name string) error {
		if val <= 0 {
			return NewRaftLockServerConfigError(fmt.Sprintf("%s must be positive", name))
		}
		return nil
	}

	if err := checkPositiveDuration(c.RequestTimeout, "RequestTimeout"); err != nil {
		return err
	}
	if err := checkPositiveDuration(c.ShutdownTimeout, "ShutdownTimeout"); err != nil {
		return err
	}
	if err := checkPositiveInt(c.MaxRequestSize, "MaxRequestSize"); err != nil {
		return err
	}
	if err := checkPositiveInt(c.MaxResponseSize, "MaxResponseSize"); err != nil {
		return err
	}
	if err := checkPositiveInt(c.MaxConcurrentReqs, "MaxConcurrentReqs"); err != nil {
		return err
	}

	if c.EnableRateLimit {
		if err := checkPositiveInt(c.RateLimit, "RateLimit"); err != nil {
			return err
		}
		if err := checkPositiveInt(c.RateLimitBurst, "RateLimitBurst"); err != nil {
			return err
		}
		if err := checkPositiveDuration(c.RateLimitWindow, "RateLimitWindow"); err != nil {
			return err
		}
	}

	if err := checkPositiveDuration(c.HealthCheckInterval, "HealthCheckInterval"); err != nil {
		return err
	}
	if err := checkPositiveDuration(c.HealthCheckTimeout, "HealthCheckTimeout"); err != nil {
		return err
	}
	if c.EnableLeaderRedirect {
		if err := checkPositiveDuration(c.RedirectTimeout, "RedirectTimeout"); err != nil {
			return err
		}
	}

	return nil
}

// RaftLockServerConfigError represents a validation error in ServerConfig.
type RaftLockServerConfigError struct {
	Message string
}

// NewRaftLockServerConfigError returns a new ConfigError instance.
func NewRaftLockServerConfigError(msg string) *RaftLockServerConfigError {
	return &RaftLockServerConfigError{Message: msg}
}

// Error implements the error interface.
func (e *RaftLockServerConfigError) Error() string {
	return "server config error: " + e.Message
}
