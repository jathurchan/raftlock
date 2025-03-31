package raft

import "fmt"

// RaftID uniquely identifies a Raft node within the cluster.
type RaftID string

// PeerConfig represents a static configuration entry for a peer node in the Raft cluster.
type PeerConfig struct {
	ID      RaftID // Unique identifier of the peer node.
	Address string // Network address for RPC communication.
}

// Config contains the full configuration for a Raft node instance.
// Includes identity, peer information, runtime parameters, and injected dependencies.
type Config struct {
	ID    RaftID       // Unique identifier of the current node.
	Peers []PeerConfig // List of peer nodes in the cluster, including the current node.

	Options RaftOptions // Core Raft parameters.

	EnableReadIndex   bool // Feature Flag: Enables linearizable reads using the read index protocol.
	EnableLeaderLease bool // Feature Flag: Enables serving reads using leader leases when quorum is stable.
	PreVoteEnabled    bool // Feature Flag: Enables PreVote phase before elections to reduce unnecessary disruptions.

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

	Storage      Storage      // Persistent storage interface for logs and snapshots.
	Transport    Transport    // RPC transport layer for inter-node communication.
	StateMachine StateMachine // Application-defined state machine to apply committed commands.
	Logger       Logger       // Logger for debugging and observability.
	Metrics      Metrics      // Metrics collector for instrumentation.
}

// WithDefaults applies recommended default values to optional fields in the Config.
func (c Config) WithDefaults() Config {
	if c.MaxApplyBatchSize == 0 {
		c.MaxApplyBatchSize = 10
	}
	if c.MaxSnapshotChunkSize < 0 {
		c.MaxSnapshotChunkSize = 0
	}
	if !c.EnableReadIndex {
		c.EnableReadIndex = true
	}
	if !c.EnableLeaderLease {
		c.EnableLeaderLease = true
	}
	if !c.PreVoteEnabled {
		c.PreVoteEnabled = true
	}
	return c
}

// NewRaftConfig creates a Config using the provided RaftOptions,
// validates them, and applies recommended default values to optional fields.
func NewRaftConfig(options RaftOptions) (Config, error) {
	if err := options.Validate(); err != nil {
		return Config{}, err
	}
	return Config{
		Options: options,
	}.WithDefaults(), nil
}

// RaftOptions defines the internal control parameters that govern
// Raft's behavior around elections, heartbeats, log replication, and storage.
type RaftOptions struct {
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

// DefaultRaftOptions returns a RaftOptions instance initialized with default values.
func DefaultRaftOptions() RaftOptions {
	return RaftOptions{
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

// WithElectionTicks creates a copy of RaftOptions with the specified election tick count.
func (o RaftOptions) WithElectionTicks(ticks int) RaftOptions {
	if ticks <= 0 {
		panic("ElectionTickCount must be positive")
	}
	o.ElectionTickCount = ticks
	return o
}

// WithHeartbeatTicks creates a copy of RaftOptions with the specified heartbeat tick count.
func (o RaftOptions) WithHeartbeatTicks(ticks int) RaftOptions {
	if ticks <= 0 {
		panic("HeartbeatTickCount must be positive")
	}
	o.HeartbeatTickCount = ticks
	return o
}

// WithRandomizationFactor creates a copy of RaftOptions with the specified election randomization factor.
func (o RaftOptions) WithRandomizationFactor(factor float64) RaftOptions {
	if factor < 0 {
		panic("ElectionRandomizationFactor must be non-negative")
	}
	o.ElectionRandomizationFactor = factor
	return o
}

// WithMaxEntriesPerRequest creates a copy of RaftOptions with the specified max entries per request.
func (o RaftOptions) WithMaxEntriesPerRequest(max int) RaftOptions {
	if max <= 0 {
		panic("MaxLogEntriesPerRequest must be positive")
	}
	o.MaxLogEntriesPerRequest = max
	return o
}

// WithSnapshotThreshold creates a copy of RaftOptions with the specified snapshot threshold.
func (o RaftOptions) WithSnapshotThreshold(threshold int) RaftOptions {
	if threshold <= 0 {
		panic("SnapshotThreshold must be positive")
	}
	o.SnapshotThreshold = threshold
	return o
}

// WithStorageSyncDelay creates a copy of RaftOptions with the specified storage sync delay.
func (o RaftOptions) WithStorageSyncDelay(delay int) RaftOptions {
	if delay <= 0 {
		panic("StorageSyncDelay must be positive")
	}
	o.StorageSyncDelay = delay
	return o
}

// WithLogCompactionMinEntries creates a copy of RaftOptions with the specified log compaction minimum entries.
func (o RaftOptions) WithLogCompactionMinEntries(min int) RaftOptions {
	if min <= 0 {
		panic("LogCompactionMinEntries must be positive")
	}
	o.LogCompactionMinEntries = min
	return o
}

// WithApplyTickCount creates a copy of RaftOptions with the specified apply tick count.
func (o RaftOptions) WithApplyTickCount(ticks int) RaftOptions {
	if ticks <= 0 {
		panic("ApplyTickCount must be positive")
	}
	o.ApplyTickCount = ticks
	return o
}

// Validate checks if the options are valid and returns an error if not.
func (o RaftOptions) Validate() error {
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
