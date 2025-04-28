package raft

import (
	"fmt"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

// PeerConfig holds the configuration for a single peer in the Raft cluster.
type PeerConfig struct {
	// ID is the unique identifier of the peer node.
	ID types.NodeID

	// Address is the network address (e.g., "host:port") for communication with this peer.
	Address string
}

// Dependencies bundles the external components required by a Raft node instance.
type Dependencies struct {
	// Storage persists Raft log entries, state, and snapshots.
	Storage storage.Storage

	// Network handles RPC communication between Raft peers.
	Network PeerNetwork

	// Applier applies committed entries to the user state machine and handles snapshots.
	Applier Applier

	// Logger provides structured logging.
	Logger logger.Logger

	// Metrics records operational metrics.
	Metrics Metrics
}

// Validate checks that all required dependencies are provided.
// Optional dependencies (Logger, Metrics) may be nil.
func (d *Dependencies) Validate() error {
	if d == nil {
		return fmt.Errorf("%w: dependencies struct cannot be nil", ErrMissingDependencies)
	}
	if d.Storage == nil {
		return fmt.Errorf("%w: Storage dependency cannot be nil", ErrMissingDependencies)
	}
	if d.Network == nil {
		return fmt.Errorf("%w: Network dependency cannot be nil", ErrMissingDependencies)
	}
	if d.Applier == nil {
		return fmt.Errorf("%w: Applier dependency cannot be nil", ErrMissingDependencies)
	}
	return nil
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

	// Deps contains external components (Storage, Network, etc.).
	Deps Dependencies

	// peerList stores the original list of peers, used for convenience methods like PeersAsSlice.
	peerList []PeerConfig
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
}

// FeatureFlags enables optional Raft features and optimizations.
type FeatureFlags struct {
	// EnableReadIndex enables the ReadIndex protocol for faster linearizable reads.
	EnableReadIndex bool

	// EnableLeaderLease enables leader leases for low-latency reads.
	// Requires EnableReadIndex to be true.
	EnableLeaderLease bool

	// PreVoteEnabled enables a PreVote phase before starting an election.
	PreVoteEnabled bool
}

// TuningParams configures advanced performance-related parameters.
type TuningParams struct {
	// MaxApplyBatchSize limits the number of entries applied in a single batch.
	MaxApplyBatchSize int
	// MaxSnapshotChunkSize sets the maximum snapshot chunk size (bytes) during InstallSnapshot RPCs.
	// 0 disables chunking.
	MaxSnapshotChunkSize int
}
