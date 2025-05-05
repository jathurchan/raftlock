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
	// nominalTickInterval defines the base interval between Raft ticks.
	// Used as the time unit for election and heartbeat timeouts.
	nominalTickInterval = 100 * time.Millisecond

	// logManagerOpTimeout is the timeout used for internal log manager operations such as reading metadata
	// after a log mutation (e.g., fetching term after truncation). Keeps internal tasks bounded in duration.
	logManagerOpTimeout = 500 * time.Millisecond

	// electionManagerOpTimeout bounds the duration of electionManager operations
	// such as requesting votes or updating election state, ensuring they complete promptly.
	electionManagerOpTimeout = 1 * time.Second

	// defaultSnapshotCaptureTimeout is the timeout for capturing snapshot data from the applier.
	defaultSnapshotCaptureTimeout = 30 * time.Second

	// defaultSnapshotLogTermTimeout is the timeout for resolving the term of the log entry at the snapshot index.
	defaultSnapshotLogTermTimeout = 2 * time.Second

	// defaultSnapshotPersistTimeout is the timeout for persisting the snapshot data to stable storage.
	defaultSnapshotPersistTimeout = 30 * time.Second

	// defaultSnapshotLogTruncateTimeout is the timeout for truncating the Raft log prefix after snapshot creation or installation.
	defaultSnapshotLogTruncateTimeout = 10 * time.Second

	// defaultSnapshotRestoreTimeout is the timeout for restoring the state machine from snapshot data.
	defaultSnapshotRestoreTimeout = 5 * time.Minute

	// defaultSnapshotLoadTimeout is the timeout for loading snapshot data from storage in preparation for sending to a follower.
	defaultSnapshotLoadTimeout = 30 * time.Second

	// defaultSnapshotSendRPCTimeout is the timeout for sending the InstallSnapshot RPC to a follower.
	defaultSnapshotSendRPCTimeout = 2 * time.Minute

	// defaultSnapshotStopTimeout is the timeout for completing snapshot-related operations during shutdown.
	// Ensures the system can shut down gracefully without hanging on snapshot activity.
	defaultSnapshotStopTimeout = 10 * time.Second

	// defaultAppendEntriesTimeout is the maximum duration allowed for an AppendEntries RPC to complete.
	defaultAppendEntriesTimeout = 2 * time.Second

	// defaultLogFetchTimeout bounds the time spent fetching log entries from storage or memory.
	defaultLogFetchTimeout = 1 * time.Second

	// defaultReadIndexTimeout is the timeout for waiting on quorum confirmation in a ReadIndex operation.
	defaultReadIndexTimeout = 1 * time.Second

	// defaultTermFetchTimeout limits the time allowed for retrieving the term of a specific log index,
	defaultTermFetchTimeout = 500 * time.Millisecond
)
