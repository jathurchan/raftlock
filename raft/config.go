package raft

import "fmt"

// PeerConfig represents a static configuration entry for a peer node in the Raft cluster.
type PeerConfig struct {
	ID      NodeID // Unique identifier of the peer node.
	Address string // Network address for RPC communication.
}

// Config contains the full configuration for a Raft node instance.
// This is the primary configuration struct that brings together all aspects
// of a Raft node's configuration including identity, peer information,
// behavior options, and component dependencies.
type Config struct {
	ID    NodeID
	Peers []PeerConfig

	Options      Options
	FeatureFlags FeatureFlags
	TuningParams TuningParams

	Storage      Storage
	Transport    Transport
	StateMachine StateMachine
	Logger       Logger
	Metrics      Metrics
}

// Options defines timing and replication parameters that govern
// core Raft protocol behavior, including elections, heartbeats, log replication,
// snapshotting, and compaction. These options allow tuning for stability,
// failover responsiveness, and resource efficiency.
type Options struct {
	// Number of logical ticks that must pass without a heartbeat before triggering a new election.
	// A higher value increases failover latency but improves stability.
	// Default: 10
	ElectionTickCount int

	// Number of ticks between heartbeat messages sent by the leader.
	// Must be significantly lower than ElectionTickCount.
	// Default: 1
	HeartbeatTickCount int

	// Randomization factor added to the election timeout to reduce the chance of split votes.
	// A value of 0.2 results in timeouts between ElectionTickCount and ElectionTickCount * 1.2.
	// Default: 0.2
	ElectionRandomizationFactor float64

	// MaxLogEntriesPerRequest limits the number of log entries sent in a single
	// AppendEntries RPC. This helps with network efficiency and batching.
	// Lower values reduce latency spikes but increase RPC overhead.
	// Higher values improve throughput but may cause longer processing delays.
	// Default: 100 entries
	MaxLogEntriesPerRequest int

	// Number of new log entries that must be applied since the last snapshot before creating a new one.
	// Prevents log growth by periodically snapshotting state.
	// Default: 10,000
	SnapshotThreshold int

	// Number of ticks between storage sync operations.
	// A value of 1 means syncing every tick; higher values reduce disk I/O at the cost of durability risk.
	// Default: 5
	StorageSyncDelay int

	// Minimum number of log entries required before log compaction can be considered.
	// Helps prevent aggressive or premature compactions.
	// Default: 5,000
	LogCompactionMinEntries int

	// Number of ticks between checks to apply committed entries to the state machine.
	// Default: 1
	ApplyTickCount int
}

// FeatureFlags enables optional behavior extensions within the Raft consensus protocol.
// These flags allow fine-grained tuning of performance, availability, and safety
// without modifying the core Raft algorithm. They support incremental adoption
// of advanced features while preserving correctness.
type FeatureFlags struct {
	// EnableReadIndex activates the Read Index protocol for linearizable reads.
	// This ensures that read operations observe the most up-to-date committed state
	// by consulting a quorum of nodes before responding.
	EnableReadIndex bool

	// EnableLeaderLease allows the leader to serve local reads without quorum confirmation
	// during stable leadership periods. This improves read latency under low contention,
	// but requires loosely synchronized clocks and periodic heartbeat extensions.
	EnableLeaderLease bool

	// PreVoteEnabled introduces a PreVote phase prior to elections to minimize
	// disruption from partitioned or slow nodes. This prevents unnecessary term bumps
	// and helps maintain leadership stability in large or unreliable clusters.
	PreVoteEnabled bool
}

// TuningParams defines performance-oriented parameters that control how the Raft
// system applies logs and transfers snapshots. These knobs allow to balance
// throughput, latency, and resource usage based on workload characteristics.
type TuningParams struct {
	// MaxApplyBatchSize limits how many committed log entries are applied to the
	// state machine in a single batch (tick). This helps balance throughput and
	// responsiveness by applying entries in controlled bursts.
	//
	// A smaller value may improve responsiveness (less latency per tick),
	// while a larger value increases throughput (fewer apply loops).
	//
	// Default: 10
	MaxApplyBatchSize int

	// MaxSnapshotChunkSize limits the size (in bytes) of each snapshot chunk
	// sent during the InstallSnapshot RPC. This helps control the memory and
	// network overhead when transmitting large snapshots.
	//
	// If set to 0, snapshot chunking is disabled and the full snapshot is sent
	// in one RPC call. This is fine for small snapshots but may be problematic
	// for large clusters or slow networks.
	//
	// Use chunking to reduce the risk of timeouts or dropped connections.
	//
	// Default: 0 (no chunking)
	MaxSnapshotChunkSize int
}

// DefaultOptions returns an Options instance initialized with default values.
func DefaultOptions() Options {
	return Options{
		ElectionTickCount:           10,
		HeartbeatTickCount:          1,
		ElectionRandomizationFactor: 0.2,
		MaxLogEntriesPerRequest:     100,
		SnapshotThreshold:           10000,
		StorageSyncDelay:            5,
		LogCompactionMinEntries:     5000,
		ApplyTickCount:              1,
	}
}

// DefaultFeatureFlags returns the default feature flag settings.
func DefaultFeatureFlags() FeatureFlags {
	return FeatureFlags{
		EnableReadIndex:   true,
		EnableLeaderLease: true,
		PreVoteEnabled:    true,
	}
}

// DefaultTuningParams returns default tuning parameters.
func DefaultTuningParams() TuningParams {
	return TuningParams{
		MaxApplyBatchSize:    10,
		MaxSnapshotChunkSize: 0,
	}
}

// NewConfig creates and validates a new immutable Raft Config.
// This function performs comprehensive validation of all configuration parameters
// and their inter-dependencies, ensuring a valid and consistent configuration.
// It also provides defaults for optional components like Logger and Metrics.
func NewConfig(
	id NodeID,
	peers []PeerConfig,
	opts Options,
	flags FeatureFlags,
	tuning TuningParams,
	storage Storage,
	transport Transport,
	stateMachine StateMachine,
	logger Logger,
	metrics Metrics,
) (Config, error) {
	if err := validatePeerSet(id, peers); err != nil {
		return Config{}, err
	}
	if err := opts.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid options: %w", err)
	}
	if err := flags.Validate(); err != nil {
		return Config{}, err
	}
	if err := tuning.Validate(); err != nil {
		return Config{}, err
	}
	if storage == nil || transport == nil || stateMachine == nil {
		return Config{}, fmt.Errorf("raft: missing required components")
	}
	if logger == nil {
		logger = &NoOpLogger{}
	}
	if metrics == nil {
		metrics = &NoOpMetrics{}
	}

	return Config{
		ID:           id,
		Peers:        peers,
		Options:      opts,
		FeatureFlags: flags,
		TuningParams: tuning,
		Storage:      storage,
		Transport:    transport,
		StateMachine: stateMachine,
		Logger:       logger,
		Metrics:      metrics,
	}, nil
}

// Validate performs comprehensive validation of Options, checking that:
// - All values are within acceptable ranges
// - Inter-dependent values are compatible with each other
// - No values would lead to known problematic behavior
// Returns detailed error messages to help diagnose configuration issues.
func (o Options) Validate() error {
	if o.HeartbeatTickCount <= 0 {
		return fmt.Errorf("HeartbeatTickCount must be positive, got %d", o.HeartbeatTickCount)
	}
	if o.HeartbeatTickCount >= o.ElectionTickCount {
		return fmt.Errorf("HeartbeatTickCount (%d) must be less than ElectionTickCount (%d)",
			o.HeartbeatTickCount, o.ElectionTickCount)
	}
	minElectionTicks := o.HeartbeatTickCount * 3
	if o.ElectionTickCount < minElectionTicks {
		return fmt.Errorf("ElectionTickCount (%d) should be at least 3x HeartbeatTickCount (%d)",
			o.ElectionTickCount, o.HeartbeatTickCount)
	}
	if o.ElectionRandomizationFactor < 0 || o.ElectionRandomizationFactor > 1 {
		return fmt.Errorf("ElectionRandomizationFactor must be in [0.0, 1.0], got %f", o.ElectionRandomizationFactor)
	}
	if o.MaxLogEntriesPerRequest <= 0 {
		return fmt.Errorf("MaxLogEntriesPerRequest must be positive, got %d", o.MaxLogEntriesPerRequest)
	}
	if o.SnapshotThreshold < 1000 {
		return fmt.Errorf("SnapshotThreshold (%d) is very low; may lead to frequent snapshots", o.SnapshotThreshold)
	}
	if o.StorageSyncDelay <= 0 {
		return fmt.Errorf("StorageSyncDelay must be positive, got %d", o.StorageSyncDelay)
	}
	if o.StorageSyncDelay > 100 {
		return fmt.Errorf("StorageSyncDelay (%d) is unusually high; it may risk durability", o.StorageSyncDelay)
	}
	if o.LogCompactionMinEntries <= 0 {
		return fmt.Errorf("LogCompactionMinEntries must be positive, got %d", o.LogCompactionMinEntries)
	}
	if o.ApplyTickCount <= 0 {
		return fmt.Errorf("ApplyTickCount must be positive, got %d", o.ApplyTickCount)
	}

	return nil
}

// Validate checks logical compatibility between Raft feature toggles.
// For example, leader leases depend on the read index protocol.
func (f FeatureFlags) Validate() error {
	if !f.EnableReadIndex && f.EnableLeaderLease {
		return fmt.Errorf("raft: EnableLeaderLease requires EnableReadIndex to be true")
	}
	return nil
}

// Validate checks if tuning parameters are valid.
func (t TuningParams) Validate() error {
	if t.MaxApplyBatchSize <= 0 {
		return fmt.Errorf("raft: MaxApplyBatchSize must be > 0, got %d", t.MaxApplyBatchSize)
	}
	if t.MaxSnapshotChunkSize < 0 {
		return fmt.Errorf("raft: MaxSnapshotChunkSize must be >= 0, got %d", t.MaxSnapshotChunkSize)
	}
	return nil
}

// WithElectionTicks creates a copy of Options with the specified election tick count.
func (o Options) WithElectionTicks(ticks int) Options {
	if ticks <= 0 {
		panic("ElectionTickCount must be positive")
	}
	o.ElectionTickCount = ticks
	return o
}

// WithHeartbeatTicks creates a copy of Options with the specified heartbeat tick count.
func (o Options) WithHeartbeatTicks(ticks int) Options {
	if ticks <= 0 {
		panic("HeartbeatTickCount must be positive")
	}
	o.HeartbeatTickCount = ticks
	return o
}

// WithRandomizationFactor creates a copy of Options with the specified election randomization factor.
func (o Options) WithRandomizationFactor(factor float64) Options {
	if factor < 0 {
		panic("ElectionRandomizationFactor must be non-negative")
	}
	o.ElectionRandomizationFactor = factor
	return o
}

// WithMaxEntriesPerRequest creates a copy of Options with the specified max entries per request.
func (o Options) WithMaxEntriesPerRequest(max int) Options {
	if max <= 0 {
		panic("MaxLogEntriesPerRequest must be positive")
	}
	o.MaxLogEntriesPerRequest = max
	return o
}

// WithSnapshotThreshold creates a copy of Options with the specified snapshot threshold.
func (o Options) WithSnapshotThreshold(threshold int) Options {
	if threshold <= 0 {
		panic("SnapshotThreshold must be positive")
	}
	o.SnapshotThreshold = threshold
	return o
}

// WithStorageSyncDelay creates a copy of Options with the specified storage sync delay.
func (o Options) WithStorageSyncDelay(delay int) Options {
	if delay <= 0 {
		panic("StorageSyncDelay must be positive")
	}
	o.StorageSyncDelay = delay
	return o
}

// WithLogCompactionMinEntries creates a copy of Options with the specified log compaction minimum entries.
func (o Options) WithLogCompactionMinEntries(min int) Options {
	if min <= 0 {
		panic("LogCompactionMinEntries must be positive")
	}
	o.LogCompactionMinEntries = min
	return o
}

// WithApplyTickCount creates a copy of Options with the specified apply tick count.
func (o Options) WithApplyTickCount(ticks int) Options {
	if ticks <= 0 {
		panic("ApplyTickCount must be positive")
	}
	o.ApplyTickCount = ticks
	return o
}

// Helper functions

// validatePeerSet ensures the peer configuration is valid by:
// - Verifying the local node's ID is non-empty and exists in the peer list
// - Checking that all peer IDs and addresses are non-empty
// - Confirming there are no duplicate peer IDs
func validatePeerSet(id NodeID, peers []PeerConfig) error {
	if id == "" {
		return fmt.Errorf("raft: node ID cannot be empty")
	}
	if len(peers) == 0 {
		return fmt.Errorf("raft: peer list cannot be empty")
	}

	var idFound bool
	seen := make(map[NodeID]struct{})

	for _, peer := range peers {
		if peer.ID == "" {
			return fmt.Errorf("raft: found peer with empty ID")
		}
		if peer.Address == "" {
			return fmt.Errorf("raft: peer %q has an empty address", peer.ID)
		}
		if _, exists := seen[peer.ID]; exists {
			return fmt.Errorf("raft: duplicate peer ID %q found", peer.ID)
		}
		seen[peer.ID] = struct{}{}
		if peer.ID == id {
			idFound = true
		}
	}

	if !idFound {
		return fmt.Errorf("raft: node ID %q not found in peer list", id)
	}
	return nil
}
