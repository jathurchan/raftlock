package raft

import (
	"time"

	"github.com/jathurchan/raftlock/types"
)

const (
	// unknownNodeID represents the absence of a node.
	unknownNodeID = types.NodeID("")

	// HeartbeatTickCount is the number of ticks between leader heartbeats (empty AppendEntries RPCs).
	DefaultHeartbeatTickCount = 1

	// ElectionTickCount is the number of ticks a follower waits without hearing from the leader before starting an election.
	DefaultElectionTickCount = 10

	// ElectionRandomizationFactor randomizes election timeouts to reduce split votes. Range: [0.0, 1.0].
	DefaultElectionRandomizationFact = 0.2

	// MaxLogEntriesPerRequest limits the number of log entries sent in one AppendEntries RPC.
	DefaultMaxLogEntriesPerRequest = 100

	// SnapshotThreshold is the number of new log entries after the last snapshot before triggering a new snapshot.
	DefaultSnapshotThreshold = 10000

	// LogCompactionMinEntries is the minimum number of entries beyond the last snapshot before allowing log compaction.
	DefaultLogCompactionMinEntries = 5000

	// StorageSyncDelayTicks is the number of ticks a leader may wait before fsyncing persisted log entries. 0 or 1 means immediate.
	DefaultStorageSyncDelayTicks = 5

	// ApplyTickCount is the number of ticks between checks for committed entries to apply to the state machine.
	DefaultApplyTickCount = 1

	// EnableReadIndex enables the ReadIndex optimization for linearizable reads.
	DefaultEnableReadIndex = true

	// EnableLeaderLease enables a leader lease mechanism for faster local reads under stable leadership.
	DefaultEnableLeaderLease = true

	// PreVoteEnabled enables the PreVote phase to avoid unnecessary elections caused by partitioned nodes.
	DefaultPreVoteEnabled = true

	// MaxApplyBatchSize limits the number of committed log entries applied per batch to the state machine.
	DefaultMaxApplyBatchSize = 10

	// MaxSnapshotChunkSize is the maximum size in bytes of a snapshot chunk. 0 disables chunking (sends entire snapshot).
	DefaultMaxSnapshotChunkSize = 0
)

// Internal thresholds for validation and tuning.
const (
	// minReasonableSnapshotThreshold is the recommended lower bound for SnapshotThreshold to avoid too-frequent snapshots.
	minReasonableSnapshotThreshold = 1000

	// maxReasonableStorageSyncDelay is the recommended upper bound for StorageSyncDelayTicks to avoid risking data durability.
	maxReasonableStorageSyncDelay = 100

	// minElectionTickMultiplier ensures ElectionTickCount is sufficiently larger than HeartbeatTickCount for cluster stability.
	minElectionTickMultiplier = 3
)

const (
	// logManagerInternalOpTimeout is the timeout used for internal log manager operations such as reading metadata
	// after a log mutation (e.g., fetching term after truncation). Keeps internal tasks bounded in duration.
	logManagerInternalOpTimeout = 500 * time.Millisecond
)
