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
	DefaultElectionRandomizationFactor = 0.2

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

	// DefaultApplyEntryTimeout is the timeout for applying a single committed log entry to the state machine.
	DefaultApplyEntryTimeout = 5 * time.Second

	// DefaultFetchEntriesTimeout is the timeout for fetching entries from the log manager.
	// This can be made configurable.
	DefaultFetchEntriesTimeout = 5 * time.Second
)

const (
	// DefaultMaxRecvMsgSize is the default maximum gRPC message size the server will receive. (16 MB)
	DefaultMaxRecvMsgSize = 16 * 1024 * 1024

	// DefaultMaxSendMsgSize is the default maximum gRPC message size the client/server will send. (16 MB)
	DefaultMaxSendMsgSize = 16 * 1024 * 1024

	// DefaultDialTimeout is the default timeout for establishing a gRPC connection to a peer. (2 seconds)
	DefaultDialTimeout = 2 * time.Second

	// DefaultServerStartTimeout is the default maximum time to wait for the gRPC server goroutine to start listening. (5 seconds)
	DefaultServerStartTimeout = 5 * time.Second

	// DefaultKeepaliveTime is the default interval for client/server gRPC keepalive pings when idle. (5 seconds)
	DefaultKeepaliveTime = 5 * time.Second

	// DefaultKeepaliveTimeout is the default timeout waiting for a keepalive ping acknowledgement. (1 second)
	DefaultKeepaliveTimeout = 1 * time.Second

	// DefaultServerMaxConnectionIdle is the default maximum duration a server-side gRPC connection can be idle before being closed. (15 seconds)
	DefaultServerMaxConnectionIdle = 15 * time.Second

	// DefaultServerMaxConnectionAge is the default maximum duration a server-side gRPC connection may exist before being gracefully closed. (30 minutes)
	DefaultServerMaxConnectionAge = 30 * time.Minute

	// DefaultServerMaxConnectionAgeGrace is the default time allowed for RPCs to complete on a server-side connection after a graceful close is initiated due to MaxConnectionAge. (5 seconds)
	DefaultServerMaxConnectionAgeGrace = 5 * time.Second
)

const (
	// NominalTickInterval defines the base interval between Raft ticks.
	// Used as the time unit for election and heartbeat timeouts.
	NominalTickInterval = 100 * time.Millisecond

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
