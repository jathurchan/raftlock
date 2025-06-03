package types

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

// LockID is a unique identifier for a lock resource.
type LockID string

// ClientID is a unique identifier for a client interacting with the lock manager.
type ClientID string

// NodeID uniquely identifies a Raft node within a cluster.
// It should be globally unique and remain stable across restarts.
type NodeID string

// Term represents a Raft term, which is a monotonically increasing number
// used to determine leadership and maintain log consistency across nodes.
type Term uint64

// Index represents a position in the Raft log.
// Log indices start at 1 and increase with each appended entry.
type Index uint64

// ProposalID uniquely identifies a Raft proposal within the system.
// It must be unique across all active proposals to ensure correct tracking and resolution.
type ProposalID string

// LockOperation defines the type of action that can be performed on a lock.
type LockOperation string

const (
	// OperationAcquire represents an attempt to acquire a lock.
	OperationAcquire LockOperation = "acquire"

	// OperationRelease represents an attempt to release a previously acquired lock.
	OperationRelease LockOperation = "release"

	// OperationRenew represents an attempt to renew the TTL of an existing lock.
	OperationRenew LockOperation = "renew"

	// OperationEnqueueWaiter represents adding a client to the waiting queue for a lock.
	OperationEnqueueWaiter LockOperation = "enqueue_waiter"

	// OperationCancelWait represents removing a client from the waiting queue for a lock.
	OperationCancelWait LockOperation = "cancel_wait"
)

// SnapshotOperation represents a type of snapshot-related operation.
type SnapshotOperation string

const (
	// SnapshotCreate indicates a snapshot creation event.
	SnapshotCreate SnapshotOperation = "create"

	// SnapshotRestore indicates a snapshot restoration event.
	SnapshotRestore SnapshotOperation = "restore"
)

// Command represents a client operation submitted to the lock manager.
// These commands are serialized and applied to the state machine via Raft.
type Command struct {
	Op          LockOperation     `json:"op"`                     // Type of operation: acquire, release, renew, etc.
	LockID      LockID            `json:"lock_id"`                // Unique identifier for the target lock.
	ClientID    ClientID          `json:"client_id"`              // ID of the client issuing the command.
	TTL         int64             `json:"ttl,omitempty"`          // Time-to-live in ms (used for acquire/renew).
	Version     Index             `json:"version,omitempty"`      // Raft index used for fencing.
	Priority    int               `json:"priority,omitempty"`     // Priority when waiting for a lock.
	Timeout     int64             `json:"timeout,omitempty"`      // Optional general timeout in ms
	Wait        bool              `json:"wait,omitempty"`         // Whether to wait in queue if lock is held.
	WaitTimeout int64             `json:"wait_timeout,omitempty"` // Max time to wait in queue (in ms).
	RequestID   string            `json:"request_id,omitempty"`   // Optional request ID for idempotency.
	Metadata    map[string]string `json:"metadata,omitempty"`     // Optional metadata for the lock request.
}

// WaiterInfo provides information about a client currently waiting for a lock.
type WaiterInfo struct {
	ClientID   ClientID  `json:"client_id"`   // Unique identifier of the waiting client.
	EnqueuedAt time.Time `json:"enqueued_at"` // Time the client was added to the wait queue.
	TimeoutAt  time.Time `json:"timeout_at"`  // Time when the client's wait request will expire.
	Priority   int       `json:"priority"`    // Priority of the client in the wait queue.
	Position   int       `json:"position"`    // Current index in the wait queue (0 = front of the queue).
}

// LockInfo describes the current state of a lock, including ownership,
// timing, metadata, and information about waiting clients.
type LockInfo struct {
	LockID       LockID            `json:"lock_id"`                // Unique identifier for the lock.
	OwnerID      ClientID          `json:"owner_id"`               // ID of the client that currently holds the lock. Empty if unheld.
	Version      Index             `json:"version"`                // Raft log index when the lock was acquired (used for fencing).
	AcquiredAt   time.Time         `json:"acquired_at"`            // Timestamp when the current owner acquired the lock.
	ExpiresAt    time.Time         `json:"expires_at"`             // Timestamp when the lock will automatically expire unless renewed.
	WaiterCount  int               `json:"waiter_count"`           // Number of clients currently waiting to acquire the lock.
	WaitersInfo  []WaiterInfo      `json:"waiters_info,omitempty"` // Detailed info about each waiting client (omitted if empty).
	Metadata     map[string]string `json:"metadata,omitempty"`     // Optional user-defined metadata associated with the lock.
	LastModified time.Time         `json:"last_modified"`          // Timestamp of the most recent modification to the lock state.
}

// NodeRole represents the possible roles of a Raft node.
type NodeRole int

const (
	// RoleFollower is the default role of a Raft node at startup.
	// - Followers respond to requests from other nodes but do not initiate actions themselves.
	// - Receiving valid heartbeats (AppendEntries RPCs) from a leader resets their election timeout.
	// - Lack of heartbeats causes a follower to transition to Candidate and start an election.
	RoleFollower NodeRole = iota

	// RoleCandidate is the role a node assumes after an election timeout.
	// - Candidates initiate new elections by incrementing their term and sending RequestVote RPCs.
	// - A Candidate becomes Leader if it wins a majority of votes.
	// - It reverts to Follower upon discovering a current leader or higher term.
	// - If an election is inconclusive, it starts a new election cycle.
	RoleCandidate

	// RoleLeader is the role a node assumes after winning an election.
	// - Leaders send periodic heartbeats (AppendEntries RPCs) to maintain authority.
	// - They handle client requests and replicate log entries to followers.
	// - If a leader observes a higher term, it steps down to Follower.
	RoleLeader
)

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    Term   // Term when the entry was created
	Index   Index  // Position of the entry in the log
	Command []byte // Command to apply to the replicated state machine
}

// RaftStatus represents the current state of a Raft node for monitoring and debugging.
type RaftStatus struct {
	ID       NodeID   // This node's identifier
	Role     NodeRole // Current role (Leader, Follower, Candidate)
	Term     Term     // Current term
	LeaderID NodeID   // Current leader's ID (if known, empty if unknown)

	LastLogIndex Index // Highest log entry index in the node's log
	LastLogTerm  Term  // Term of the highest log entry
	CommitIndex  Index // Highest index known to be committed
	LastApplied  Index // Highest index applied to state machine

	SnapshotIndex Index // Index of the last snapshot
	SnapshotTerm  Term  // Term of the last snapshot

	Replication map[NodeID]PeerState
}

// PeerState tracks the replication state for a single peer.
type PeerState struct {
	NextIndex          Index     // Next index to send to this peer
	MatchIndex         Index     // Highest log entry known to be replicated on server
	IsActive           bool      // Whether the peer is responding
	LastActive         time.Time // When the peer was last known to be active
	SnapshotInProgress bool      // Whether snapshot transfer is in progress
	ReplicationLag     Index     // How far behind this peer is (leader's lastIndex - matchIndex)

}

// ApplyMsg is sent on the ApplyChannel after a log entry is committed and applied,
// or when a snapshot is installed. It represents either an applied command or a snapshot.
//
// Only one of CommandValid or SnapshotValid will be true.
type ApplyMsg struct {
	// CommandValid is true if this message contains a valid applied command.
	CommandValid bool

	// Command is the raw command that was applied.
	Command []byte

	// CommandIndex is the log index of the applied command.
	CommandIndex Index

	// CommandTerm is the term when the command was appended to the log.
	CommandTerm Term

	// CommandResultData is the result returned by Applier.Apply (e.g., *types.LockInfo).
	CommandResultData any

	// CommandResultError is the error returned by Applier.Apply (e.g., lock.ErrLockHeld).
	CommandResultError error

	// SnapshotValid is true if this message contains a snapshot instead of a command.
	SnapshotValid bool

	// Snapshot is the serialized snapshot data.
	Snapshot []byte

	// SnapshotIndex is the highest log index included in the snapshot.
	SnapshotIndex Index

	// SnapshotTerm is the term of the last log entry included in the snapshot.
	SnapshotTerm Term
}

// IndexOffsetPair maps a Raft log index to its file offset.
// Used for efficient log access and compaction.
type IndexOffsetPair struct {
	Index  Index // Log index
	Offset int64 // File offset
}

// PersistentState defines the state that must be saved to stable storage
// before responding to RPCs.
type PersistentState struct {
	// The latest term the current node has seen.
	// Starts at 0 and is incremented monotonically.
	CurrentTerm Term `json:"current_term"`
	// The Candidate the current node voted for in the current term.
	// Empty if the node hasn't voted.
	VotedFor NodeID `json:"voted_for"`
}

// SnapshotMetadata holds information about the most recent snapshot.
type SnapshotMetadata struct {
	LastIncludedIndex Index `json:"last_included_index"`
	LastIncludedTerm  Term  `json:"last_included_term"`
}

// RequestVoteArgs encapsulates the arguments for the RequestVote RPC.
type RequestVoteArgs struct {
	Term         Term   // Candidate's term
	CandidateID  NodeID // Candidate requesting vote (Empty if none)
	LastLogIndex Index  // Index of candidate's last log entry
	LastLogTerm  Term   // Term of candidate's last log entry
	IsPreVote    bool   // Flag for pre-vote phase
}

// RequestVoteReply encapsulates the reply for the RequestVote RPC.
type RequestVoteReply struct {
	Term        Term // Current term, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

// AppendEntriesArgs encapsulates the arguments for the AppendEntries RPC.
// Can also function as a heartbeat if Entries is empty.
type AppendEntriesArgs struct {
	Term         Term       // Leader's term
	LeaderID     NodeID     // So follower can redirect clients
	PrevLogIndex Index      // Index of log entry immediately preceding new ones
	PrevLogTerm  Term       // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit Index      // Leader's commitIndex
}

// AppendEntriesReply encapsulates the reply for the AppendEntries RPC.
type AppendEntriesReply struct {
	Term          Term  // Current term, for leader to update itself
	Success       bool  // True if follower contained matching entry
	ConflictIndex Index // Recommended nextIndex for leader if Success=false (optimization)
	ConflictTerm  Term  // Term of the conflicting entry if Success=false (optimization)
	MatchIndex    Index // Follower's last matching index (optimization for faster updates)
}

// InstallSnapshotArgs encapsulates the arguments for the InstallSnapshot RPC.
type InstallSnapshotArgs struct {
	Term              Term   // Leader's term
	LeaderID          NodeID // So follower can redirect clients (Empty if none)
	LastIncludedIndex Index  // Snapshot replaces entries up to this index
	LastIncludedTerm  Term   // Term of lastIncludedIndex
	Data              []byte // Snapshot chunk data
}

// InstallSnapshotReply encapsulates the reply for the InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term Term // Current term, for leader to update itself
}

// PeerConnectionStatus describes the current health of a peer connection.
type PeerConnectionStatus struct {
	Connected   bool      // True if connection likely exists.
	LastError   error     // Last significant error; nil if none.
	LastActive  time.Time // Last successful communication timestamp.
	PendingRPCs int       // Number of in-flight RPCs.
}

// PendingProposal represents a Raft proposal awaiting completion.
type PendingProposal struct {
	ID        ProposalID          // Unique identifier for the proposal (e.g., "{term}-{index}")
	Index     Index               // Raft log index of the proposal
	Term      Term                // Raft log term of the proposal
	Operation LockOperation       // Type of lock operation being proposed
	StartTime time.Time           // When the proposal was initiated by the client request
	ResultCh  chan ProposalResult // Buffered channel to send the result back to the requester
	Context   context.Context     // Client's context for cancellation and timeout tracking
	Command   []byte              // The marshalled command data submitted to Raft

	ClientID ClientID // Client that initiated the proposal
	LockID   LockID   // Lock being operated on
}

// ProposalResult contains the outcome of a Raft proposal after it has been
// processed (or has failed/timed out).
type ProposalResult struct {
	Success   bool          // True if the operation was successfully applied by the state machine
	Error     error         // Error if the operation failed, was cancelled, or invalidated
	Data      any           // Operation-specific result data (e.g., *types.LockInfo for acquire)
	AppliedAt time.Time     // Timestamp when the proposal was actually applied or finalized
	Duration  time.Duration // Total duration from submission to completion/failure
}

// ProposalStats provides aggregated statistics about proposal handling.
type ProposalStats struct {
	TotalProposals      int64
	PendingProposals    int64
	SuccessfulProposals int64
	FailedProposals     int64
	CancelledProposals  int64
	ExpiredProposals    int64
	AverageLatency      time.Duration
	MaxLatency          time.Duration
}

// BackoffAdvice provides guidance to clients on how to back off before retrying a lock request.
type BackoffAdvice struct {
	// InitialBackoff is the recommended starting backoff duration.
	InitialBackoff *durationpb.Duration

	// MaxBackoff is the maximum backoff duration the client should apply.
	MaxBackoff *durationpb.Duration

	// Multiplier determines how the backoff increases after each failed attempt.
	Multiplier float64

	// JitterFactor introduces randomness to reduce contention during retries.
	JitterFactor float64

	// EstimatedAvailabilityIn is the estimated time until the lock might become available.
	EstimatedAvailabilityIn *durationpb.Duration
}
