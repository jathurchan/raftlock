package raft

import (
	"time"

	"github.com/jathurchan/raftlock/types"
)

const (
	// Represents a non-existent or unknown node.
	unknownNodeID = types.NodeID("")

	// Maximum number of log entries per AppendEntries RPC.
	DefaultMaxLogEntriesPerRequest = 100

	// Number of new entries after the last snapshot before triggering a new snapshot.
	DefaultSnapshotThreshold = 10000

	// Minimum number of entries beyond the last snapshot required to allow log compaction.
	DefaultLogCompactionMinEntries = 5000

	// Number of ticks the leader can wait before fsyncing persisted entries.
	// A value of 0 or 1 means immediate sync.
	DefaultStorageSyncDelayTicks = 5

	// Ticks between checks for applying committed entries to the state machine.
	DefaultApplyTickCount = 1

	// Enables the ReadIndex optimization for linearizable reads.
	DefaultEnableReadIndex = true

	// Enables leader lease for low-latency local reads under stable leadership.
	DefaultEnableLeaderLease = true

	// Enables PreVote phase to reduce unnecessary elections due to network partitions.
	DefaultPreVoteEnabled = true

	// Maximum number of log entries applied to the state machine in one batch.
	DefaultMaxApplyBatchSize = 10

	// Maximum snapshot chunk size in bytes. A value of 0 disables chunking (entire snapshot sent at once).
	DefaultMaxSnapshotChunkSize = 0

	// Timeout for applying a single committed log entry.
	DefaultApplyEntryTimeout = 5 * time.Second

	// Timeout for fetching entries from the log manager.
	DefaultFetchEntriesTimeout = 5 * time.Second
)

const (
	// Default maximum size (in bytes) of gRPC messages the server will receive. (16 MB)
	DefaultMaxRecvMsgSize = 16 * 1024 * 1024

	// Default maximum size (in bytes) of gRPC messages sent by client/server. (16 MB)
	DefaultMaxSendMsgSize = 16 * 1024 * 1024

	// Timeout for establishing a gRPC connection to a peer. (2 seconds)
	DefaultDialTimeout = 2 * time.Second

	// Maximum time to wait for the gRPC server to start listening. (5 seconds)
	DefaultServerStartTimeout = 5 * time.Second

	// Interval for keepalive pings when gRPC connections are idle. (5 seconds)
	DefaultKeepaliveTime = 5 * time.Second

	// Timeout waiting for a keepalive ping acknowledgment. (1 second)
	DefaultKeepaliveTimeout = 1 * time.Second

	// Maximum idle time for a server-side gRPC connection before it is closed. (15 seconds)
	DefaultServerMaxConnectionIdle = 15 * time.Second

	// Maximum lifetime of a server-side gRPC connection before graceful shutdown. (30 minutes)
	DefaultServerMaxConnectionAge = 30 * time.Minute

	// Grace period after max connection age to finish ongoing RPCs. (5 seconds)
	DefaultServerMaxConnectionAgeGrace = 5 * time.Second
)

const (
	// Base interval between Raft ticks; used for election and heartbeat timeouts.
	NominalTickInterval = 100 * time.Millisecond

	// Timeout for internal log manager operations (e.g., fetching term after truncation).
	logManagerOpTimeout = 500 * time.Millisecond

	// Timeout for capturing snapshot data from the state machine.
	defaultSnapshotCaptureTimeout = 30 * time.Second

	// Timeout for fetching the term of the log entry at the snapshot index.
	defaultSnapshotLogTermTimeout = 2 * time.Second

	// Timeout for persisting a snapshot to stable storage.
	defaultSnapshotPersistTimeout = 30 * time.Second

	// Timeout for truncating the log after snapshot creation or installation.
	defaultSnapshotLogTruncateTimeout = 10 * time.Second

	// Timeout for restoring state from a snapshot.
	defaultSnapshotRestoreTimeout = 5 * time.Minute

	// Timeout for loading snapshot data before sending it to a follower.
	defaultSnapshotLoadTimeout = 30 * time.Second

	// Timeout for sending InstallSnapshot RPC to a follower.
	defaultSnapshotSendRPCTimeout = 2 * time.Minute
)

// state.go

const (
	// Timeouts for internal state manager operations to avoid deadlocks.
	stateManagerOpTimeout   = 5 * time.Second
	persistOperationTimeout = 3 * time.Second
	stateTransitionTimeout  = 2 * time.Second

	// Retry configuration for persistence failures.
	maxPersistRetries = 3
	basePersistDelay  = 10 * time.Millisecond
	maxPersistDelay   = 100 * time.Millisecond
)

// election.go

const (
	// Minimum number of election ticks per node to reduce split votes
	MinElectionTicksPerNode = 8

	// Minimum valid value for the randomization factor
	MinRandomizationFactor = 1.0

	// Number of ticks before a node starts an election.
	DefaultElectionTickCount = 50

	// Interval between heartbeat ticks sent by the leader.
	DefaultHeartbeatTickCount = 5

	// Factor to randomize election timeout (avoids collisions).
	DefaultElectionRandomizationFactor = 2.0

	// Maximum number of concurrent elections allowed.
	maxConcurrentElections = 3

	// Timeout for sending a vote request.
	voteRequestTimeout = 15 * time.Second

	// Timeout for election manager operations.
	electionManagerOpTimeout = 3 * time.Second

	// Minimum base interval between elections.
	minElectionIntervalBase = 50 * time.Millisecond

	// Maximum backoff duration between elections.
	maxElectionBackoff = 1 * time.Second

	// Multiplier for upper bound of randomized timeout.
	ElectionTimeoutMaxFactor = 2.0

	// Exponent applied to random float to skew toward lower values.
	ElectionTimeoutExponent = 2.0

	// Scaling factor for cluster-size jitter contribution.
	ClusterSizeJitterFactor = 2.0

	// Multiplier for deterministic node-specific timeout component.
	NodeComponentFactor = 2.0

	// ElectionRetryBackoffStepMS is the additional backoff (in ms) per failed election attempt.
	ElectionRetryBackoffStepMS = 25

	// MaxElectionAttemptsBeforeReset is the threshold of consecutive election attempts
	// before partially resetting the counter to prevent runaway retries.
	MaxElectionAttemptsBeforeReset = 10

	// ElectionAttemptResetValue is the value to reset electionAttempts to
	// after exceeding the maximum threshold.
	ElectionAttemptResetValue = 5

	// ElectionManagerStopTimeout is the maximum duration to wait for ongoing operations to finish during shutdown.
	ElectionManagerStopTimeout = 2 * time.Second

	// ElectionManagerStopPollInterval is the interval between checks for active operations during shutdown.
	ElectionManagerStopPollInterval = 10 * time.Millisecond
)

// replication.go

const (
	// Timeout for sending AppendEntries RPCs.
	defaultAppendEntriesTimeout = 10 * time.Second

	// Timeout for fetching log entries.
	defaultLogFetchTimeout = 5 * time.Second

	// Timeout for retrieving the current term.
	defaultTermFetchTimeout = 2 * time.Second

	// Timeout to stop an ongoing snapshot transfer.
	defaultSnapshotStopTimeout = 30 * time.Second

	// Buffer size for the commit notification channel to prevent blocking.
	commitNotifyChannelSize = 16
)
