package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

// RaftLockServerBuilder helps construct a RaftLockServer with validated configuration
// and sane defaults.
type RaftLockServerBuilder struct {
	config RaftLockServerConfig

	hasNodeID     bool // True if NodeID was set.
	hasListenAddr bool // True if ListenAddr was set.
	hasPeers      bool // True if peers were set.
	hasDataDir    bool // True if DataDir was set.
}

// NewRaftLockServerBuilder returns a ServerBuilder preloaded with default configuration values.
func NewRaftLockServerBuilder() *RaftLockServerBuilder {
	return &RaftLockServerBuilder{
		config: DefaultRaftLockServerConfig(),
	}
}

// WithNodeID sets the Raft node's unique identifier. This must be set explicitly.
func (b *RaftLockServerBuilder) WithNodeID(nodeID types.NodeID) *RaftLockServerBuilder {
	b.config.NodeID = nodeID
	b.hasNodeID = true
	return b
}

// WithListenAddress sets the gRPC server's listening address.
// If not set, a default address is used.
func (b *RaftLockServerBuilder) WithListenAddress(address string) *RaftLockServerBuilder {
	b.config.ListenAddress = address
	b.hasListenAddr = true
	return b
}

// WithPeers sets the Raft cluster configuration.
// The map must include an entry for this node's NodeID. This must be set explicitly.
func (b *RaftLockServerBuilder) WithPeers(peers map[types.NodeID]raft.PeerConfig) *RaftLockServerBuilder {
	b.config.Peers = peers
	b.hasPeers = true
	return b
}

// WithDataDir sets the directory for storing persistent data. This must be set explicitly.
func (b *RaftLockServerBuilder) WithDataDir(dataDir string) *RaftLockServerBuilder {
	b.config.DataDir = dataDir
	b.hasDataDir = true
	return b
}

// WithRaftConfig overrides the default Raft configuration.
// RaftConfig.ID and RaftConfig.Peers should match NodeID and Peers in ServerConfig.
func (b *RaftLockServerBuilder) WithRaftConfig(raftConfig raft.Config) *RaftLockServerBuilder {
	b.config.RaftConfig = raftConfig
	return b
}

// WithTimeouts sets timeouts for request handling, shutdown, and leader redirection.
// Values <= 0 leave the defaults unchanged.
func (b *RaftLockServerBuilder) WithTimeouts(requestTimeout, shutdownTimeout, redirectTimeout time.Duration) *RaftLockServerBuilder {
	if requestTimeout > 0 {
		b.config.RequestTimeout = requestTimeout
	}
	if shutdownTimeout > 0 {
		b.config.ShutdownTimeout = shutdownTimeout
	}
	if redirectTimeout > 0 {
		b.config.RedirectTimeout = redirectTimeout
	}
	return b
}

// WithLimits sets request size and concurrency limits.
// Values <= 0 leave the defaults unchanged.
func (b *RaftLockServerBuilder) WithLimits(maxRequestSize, maxResponseSize, maxConcurrentReqs int) *RaftLockServerBuilder {
	if maxRequestSize > 0 {
		b.config.MaxRequestSize = maxRequestSize
	}
	if maxResponseSize > 0 {
		b.config.MaxResponseSize = maxResponseSize
	}
	if maxConcurrentReqs > 0 {
		b.config.MaxConcurrentReqs = maxConcurrentReqs
	}
	return b
}

// WithRateLimit configures rate limiting.
// Values <= 0 use the default if rate limiting is enabled.
func (b *RaftLockServerBuilder) WithRateLimit(enabled bool, rateLimit, burst int, window time.Duration) *RaftLockServerBuilder {
	b.config.EnableRateLimit = enabled
	if enabled {
		if rateLimit > 0 {
			b.config.RateLimit = rateLimit
		}
		if burst > 0 {
			b.config.RateLimitBurst = burst
		}
		if window > 0 {
			b.config.RateLimitWindow = window
		}
	}
	return b
}

// WithHealthCheck sets the health check interval and timeout.
// Values <= 0 use the default values.
func (b *RaftLockServerBuilder) WithHealthCheck(interval, timeout time.Duration) *RaftLockServerBuilder {
	if interval > 0 {
		b.config.HealthCheckInterval = interval
	}
	if timeout > 0 {
		b.config.HealthCheckTimeout = timeout
	}
	return b
}

// WithLogger sets the server logger.
// If nil, a no-op logger is used.
func (b *RaftLockServerBuilder) WithLogger(logger logger.Logger) *RaftLockServerBuilder {
	b.config.Logger = logger
	return b
}

// WithMetrics sets the metrics collector.
// If nil, a no-op implementation is used.
func (b *RaftLockServerBuilder) WithMetrics(metrics ServerMetrics) *RaftLockServerBuilder {
	b.config.Metrics = metrics
	return b
}

// WithLeaderRedirect enables or disables automatic leader redirection.
func (b *RaftLockServerBuilder) WithLeaderRedirect(enabled bool) *RaftLockServerBuilder {
	b.config.EnableLeaderRedirect = enabled
	return b
}

// prepareConfig finalizes internal configuration before validation.
// It ensures RaftConfig.ID and RaftConfig.Peers are aligned with NodeID and Peers,
// applies default Logger and Metrics if nil, and sets ListenAddress from Peers if not explicitly set.
func (b *RaftLockServerBuilder) prepareConfig() {
	if b.config.RaftConfig.ID == "" && b.config.NodeID != "" {
		b.config.RaftConfig.ID = b.config.NodeID
	}

	if (b.config.RaftConfig.Peers == nil && b.config.Peers != nil) ||
		(len(b.config.RaftConfig.Peers) == 0 && len(b.config.Peers) > 0) {
		peersCopy := make(map[types.NodeID]raft.PeerConfig, len(b.config.Peers))
		for k, v := range b.config.Peers {
			peersCopy[k] = v
		}
		b.config.RaftConfig.Peers = peersCopy
	}

	if b.config.Logger == nil {
		b.config.Logger = logger.NewNoOpLogger()
	}
	if b.config.Metrics == nil {
		b.config.Metrics = NewNoOpServerMetrics()
	}

	if !b.hasListenAddr && b.config.NodeID != "" && b.config.Peers != nil {
		if peerConfig, ok := b.config.Peers[b.config.NodeID]; ok && peerConfig.Address != "" {
			b.config.ListenAddress = peerConfig.Address
		}
	}
}

// Build constructs a RaftLockServer using the current builder state.
// Returns an error if required fields are missing or configuration is invalid.
func (b *RaftLockServerBuilder) Build() (RaftLockServer, error) {
	if !b.hasNodeID {
		return nil, errors.New("server builder: NodeID must be set using WithNodeID")
	}
	if !b.hasDataDir {
		return nil, errors.New("server builder: DataDir must be set using WithDataDir")
	}
	if !b.hasPeers || b.config.Peers == nil {
		return nil, errors.New("server builder: Peers must be set using WithPeers")
	}

	b.prepareConfig()

	if err := b.config.Validate(); err != nil {
		return nil, fmt.Errorf("server builder: configuration validation failed: %w", err)
	}

	return NewRaftLockServer(b.config)
}

// RaftLockServerQuickBuild is a convenience method for building a server with minimal configuration.
func RaftLockServerQuickBuild(nodeID types.NodeID, listenAddr string, peers map[types.NodeID]raft.PeerConfig, dataDir string) (RaftLockServer, error) {
	return NewRaftLockServerBuilder().
		WithNodeID(nodeID).
		WithListenAddress(listenAddr).
		WithPeers(peers).
		WithDataDir(dataDir).
		Build()
}
