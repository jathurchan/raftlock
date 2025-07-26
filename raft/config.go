package raft

import (
	"time"

	"github.com/jathurchan/raftlock/types"
)

// PeerConfig holds the configuration for a single peer in the Raft cluster.
type PeerConfig struct {
	// ID is the unique identifier of the peer node.
	ID types.NodeID

	// Address is the network address (e.g., "host:port") for communication with this peer.
	Address string
}

// Config holds the complete configuration for initializing a Raft node.
// It aggregates node identity, peer info, Raft timing options, feature flags,
// tuning parameters, and external dependencies.
type Config struct {
	// ID is the unique identifier of this node.
	ID types.NodeID

	// Peers maps peer NodeIDs to their PeerConfig.
	Peers map[types.NodeID]PeerConfig

	// Options define Raft timing and behavior parameters.
	Options Options

	// FeatureFlags control optional Raft features and optimizations.
	FeatureFlags FeatureFlags

	// TuningParams configure advanced performance settings.
	TuningParams TuningParams
}

// Options define core Raft timing and behavior settings.
// Values are typically expressed in logical "ticks".
type Options struct {
	// ElectionTickCount is the number of ticks a follower waits before starting an election.
	ElectionTickCount int

	// HeartbeatTickCount is the number of ticks between leader heartbeats to followers.
	HeartbeatTickCount int

	// ElectionRandomizationFactor adds jitter to election timeouts to prevent split votes.
	// The timeout is randomized between ElectionTickCount and ElectionTickCount * (1 + ElectionRandomizationFactor).
	ElectionRandomizationFactor float64

	// MaxLogEntriesPerRequest limits the number of entries per AppendEntries RPC.
	MaxLogEntriesPerRequest int

	// SnapshotThreshold controls when to trigger snapshotting based on the log size.
	SnapshotThreshold int

	// StorageSyncDelay is the number of ticks a leader may wait before forcing a storage sync.
	// Higher values improve throughput but increase data loss risk on crash.
	StorageSyncDelay int

	// LogCompactionMinEntries is the minimum number of entries needed beyond a snapshot index to allow compaction.
	LogCompactionMinEntries int

	// ApplyTickCount controls how frequently committed entries are applied to the state machine.
	ApplyTickCount int

	// ApplyEntryTimeout sets a maximum duration for applying a single entry to the state machine.
	ApplyEntryTimeout time.Duration

	// FetchEntriesTimeout sets a timeout for fetching log entries from a leader or peer.
	FetchEntriesTimeout time.Duration
}

// FeatureFlags enables optional Raft features and optimizations.
type FeatureFlags struct {
	// EnableReadIndex enables the ReadIndex protocol for faster linearizable reads.
	EnableReadIndex bool

	// EnableLeaderLease enables leader leases for low-latency reads.
	// Requires EnableReadIndex to be true.
	EnableLeaderLease bool

	// explicitlySet is an internal flag to track if defaults should be overridden.
	explicitlySet bool
}

// TuningParams configures advanced performance-related parameters.
type TuningParams struct {
	// MaxApplyBatchSize limits the number of entries applied in a single batch.
	MaxApplyBatchSize int
	// MaxSnapshotChunkSize sets the maximum snapshot chunk size (bytes) during InstallSnapshot RPCs.
	// 0 disables chunking.
	MaxSnapshotChunkSize int
}

// WithExplicitFlags marks feature flags as explicitly set by the user
func (f FeatureFlags) WithExplicitFlags() FeatureFlags {
	f.explicitlySet = true
	return f
}
