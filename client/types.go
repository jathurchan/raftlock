package client

import (
	"time"

	pb "github.com/jathurchan/raftlock/proto"
)

// AcquireRequest contains parameters for acquiring a distributed lock.
type AcquireRequest struct {
	// LockID is the unique identifier for the lock resource
	LockID string

	// ClientID is the unique identifier for the requesting client
	ClientID string

	// TTL is the time-to-live for the lock. If zero, server default is used.
	TTL time.Duration

	// Wait indicates whether to wait in queue if lock is currently held
	Wait bool

	// WaitTimeout is the maximum time to wait in queue. If zero, server default is used.
	WaitTimeout time.Duration

	// Priority in wait queue (higher numbers = higher priority, 0 = default)
	Priority int32

	// Metadata is optional key-value data to associate with the lock
	Metadata map[string]string

	// RequestID is an optional client-generated ID for idempotency
	RequestID string
}

// AcquireResult contains the outcome of a lock acquisition attempt.
type AcquireResult struct {
	// Acquired indicates whether the lock was successfully acquired
	Acquired bool

	// Lock contains lock information if acquired=true
	Lock *Lock

	// BackoffAdvice provides retry guidance if acquired=false and not waiting
	BackoffAdvice *BackoffAdvice

	// QueuePosition indicates position in wait queue if waiting
	QueuePosition int32

	// EstimatedWaitDuration provides estimated wait time if enqueued
	EstimatedWaitDuration time.Duration

	// Error contains detailed error information if the operation failed
	Error *ErrorDetail
}

// ReleaseRequest contains parameters for releasing a distributed lock.
type ReleaseRequest struct {
	// LockID is the unique identifier for the lock resource
	LockID string

	// ClientID is the unique identifier for the client releasing the lock
	ClientID string

	// Version is the fencing token from when the lock was acquired
	Version int64
}

// ReleaseResult contains the outcome of a lock release operation.
type ReleaseResult struct {
	// Released indicates whether the lock was successfully released
	Released bool

	// WaiterPromoted indicates whether a waiter was promoted to lock holder
	WaiterPromoted bool

	// Error contains detailed error information if the operation failed
	Error *ErrorDetail
}

// RenewRequest contains parameters for renewing a distributed lock.
type RenewRequest struct {
	// LockID is the unique identifier for the lock resource
	LockID string

	// ClientID is the unique identifier for the client renewing the lock
	ClientID string

	// Version is the fencing token from when the lock was acquired
	Version int64

	// NewTTL is the new time-to-live for the lock
	NewTTL time.Duration
}

// RenewResult contains the outcome of a lock renewal operation.
type RenewResult struct {
	// Renewed indicates whether the lock was successfully renewed
	Renewed bool

	// Lock contains updated lock information if renewed=true
	Lock *Lock

	// Error contains detailed error information if the operation failed
	Error *ErrorDetail
}

// GetLockInfoRequest contains parameters for retrieving lock information.
type GetLockInfoRequest struct {
	// LockID is the unique identifier for the lock resource
	LockID string

	// IncludeWaiters indicates whether to include detailed waiter information
	IncludeWaiters bool
}

// GetLocksRequest contains parameters for retrieving multiple locks.
type GetLocksRequest struct {
	// Filter specifies criteria for locks to return
	Filter *LockFilter

	// Limit is the maximum number of locks to return (0 = no limit)
	Limit int32

	// Offset is the number of locks to skip (for pagination)
	Offset int32

	// IncludeWaiters indicates whether to include detailed waiter information
	IncludeWaiters bool
}

// GetLocksResult contains the outcome of a multi-lock query.
type GetLocksResult struct {
	// Locks is the list of locks matching the filter
	Locks []*LockInfo

	// TotalMatching is the total number of locks matching the filter
	TotalMatching int32

	// HasMore indicates if there are more results available
	HasMore bool
}

// EnqueueWaiterRequest contains parameters for joining a lock's wait queue.
type EnqueueWaiterRequest struct {
	// LockID is the unique identifier for the lock resource
	LockID string

	// ClientID is the unique identifier for the client
	ClientID string

	// Timeout is the maximum time to wait in queue
	Timeout time.Duration

	// Priority in wait queue (higher numbers = higher priority)
	Priority int32

	// Version is the fencing token for this wait request
	Version int64
}

// EnqueueResult contains the outcome of joining a wait queue.
type EnqueueResult struct {
	// Enqueued indicates whether the client was successfully enqueued
	Enqueued bool

	// Position is the position in the wait queue (0 = front of queue)
	Position int32

	// EstimatedWaitDuration is the estimated wait time
	EstimatedWaitDuration time.Duration

	// Error contains detailed error information if the operation failed
	Error *ErrorDetail
}

// CancelWaitRequest contains parameters for leaving a lock's wait queue.
type CancelWaitRequest struct {
	// LockID is the unique identifier for the lock resource
	LockID string

	// ClientID is the unique identifier for the client
	ClientID string

	// Version is the fencing token for the wait request to cancel
	Version int64
}

// CancelWaitResult contains the outcome of leaving a wait queue.
type CancelWaitResult struct {
	// Cancelled indicates whether the wait was successfully cancelled
	Cancelled bool

	// Error contains detailed error information if the operation failed
	Error *ErrorDetail
}

// BackoffAdviceRequest contains parameters for requesting backoff guidance.
type BackoffAdviceRequest struct {
	// LockID is the unique identifier for the lock resource
	LockID string
}

// GetStatusRequest contains parameters for requesting cluster status.
type GetStatusRequest struct {
	// IncludeReplicationDetails indicates whether to include detailed replication info
	IncludeReplicationDetails bool
}

// ClusterStatus contains information about the RaftLock cluster state.
type ClusterStatus struct {
	// RaftStatus contains current Raft cluster status
	RaftStatus *RaftStatus

	// LockStats contains lock manager statistics
	LockStats *LockManagerStats

	// Health contains server health information
	Health *HealthStatus
}

// HealthRequest contains parameters for health check requests.
type HealthRequest struct {
	// ServiceName is an optional service name for specific health checks
	ServiceName string
}

// Lock represents an acquired distributed lock.
type Lock struct {
	// LockID is the unique identifier for the lock resource
	LockID string

	// OwnerID is the unique identifier for the lock owner
	OwnerID string

	// Version is the fencing token for this lock acquisition
	Version int64

	// AcquiredAt is when the lock was acquired
	AcquiredAt time.Time

	// ExpiresAt is when the lock will expire if not renewed
	ExpiresAt time.Time

	// Metadata contains optional key-value data associated with the lock
	Metadata map[string]string
}

// LockInfo contains detailed information about a lock.
type LockInfo struct {
	// LockID is the unique identifier for the lock resource
	LockID string

	// OwnerID is the unique identifier for the current owner (empty if not held)
	OwnerID string

	// Version is the fencing token for the current lock acquisition
	Version int64

	// AcquiredAt is when the lock was acquired
	AcquiredAt time.Time

	// ExpiresAt is when the lock will expire if not renewed
	ExpiresAt time.Time

	// WaiterCount is the number of clients waiting for this lock
	WaiterCount int32

	// WaitersInfo contains detailed information about waiters (if requested)
	WaitersInfo []*WaiterInfo

	// Metadata contains optional key-value data associated with the lock
	Metadata map[string]string

	// LastModifiedAt is when the lock state was last modified
	LastModifiedAt time.Time
}

// WaiterInfo contains information about a client waiting for a lock.
type WaiterInfo struct {
	// ClientID is the unique identifier for the waiting client
	ClientID string

	// EnqueuedAt is when the client was added to the queue
	EnqueuedAt time.Time

	// TimeoutAt is when the client's wait request will expire
	TimeoutAt time.Time

	// Priority is the client's priority in the wait queue
	Priority int32

	// Position is the current position in the queue (0 = front)
	Position int32
}

// LockFilter specifies criteria for filtering locks in queries.
type LockFilter struct {
	// LockIDPattern is a pattern to match lock IDs (supports wildcards)
	LockIDPattern string

	// OwnerIDPattern is a pattern to match owner IDs (supports wildcards)
	OwnerIDPattern string

	// OnlyHeld indicates to return only locks that are currently held
	OnlyHeld bool

	// OnlyContested indicates to return only locks with waiters
	OnlyContested bool

	// ExpiresBefore filters locks that expire before this time
	ExpiresBefore *time.Time

	// ExpiresAfter filters locks that expire after this time
	ExpiresAfter *time.Time

	// MetadataFilter contains key-value pairs that must be present
	MetadataFilter map[string]string
}

// BackoffAdvice provides guidance for handling lock contention.
type BackoffAdvice struct {
	// InitialBackoff is the recommended starting backoff duration
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration
	MaxBackoff time.Duration

	// Multiplier determines how backoff increases after each failed attempt
	Multiplier float64

	// JitterFactor introduces randomness to reduce contention
	JitterFactor float64

	// EstimatedAvailabilityIn is estimated time until lock might become available
	EstimatedAvailabilityIn time.Duration
}

// RaftStatus contains information about the Raft cluster state.
type RaftStatus struct {
	// NodeID is this node's identifier
	NodeID string

	// Role is the current node role (Leader, Follower, Candidate)
	Role string

	// Term is the current Raft term
	Term uint64

	// LeaderID is the current leader's identifier (empty if unknown)
	LeaderID string

	// LastLogIndex is the highest log entry index
	LastLogIndex uint64

	// LastLogTerm is the term of the highest log entry
	LastLogTerm uint64

	// CommitIndex is the highest index known to be committed
	CommitIndex uint64

	// LastApplied is the highest index applied to state machine
	LastApplied uint64

	// SnapshotIndex is the index of the last snapshot
	SnapshotIndex uint64

	// SnapshotTerm is the term of the last snapshot
	SnapshotTerm uint64

	// Replication contains replication state for each peer (leaders only)
	Replication map[string]*PeerState
}

// PeerState contains replication state for a single peer.
type PeerState struct {
	// NextIndex is the next index to send to this peer
	NextIndex uint64

	// MatchIndex is the highest log entry known to be replicated on this peer
	MatchIndex uint64

	// IsActive indicates whether the peer is currently responding
	IsActive bool

	// LastActiveAt is when the peer was last known to be active
	LastActiveAt time.Time

	// SnapshotInProgress indicates whether snapshot transfer is in progress
	SnapshotInProgress bool

	// ReplicationLag is how far behind this peer is
	ReplicationLag uint64
}

// LockManagerStats contains statistics about the lock manager.
type LockManagerStats struct {
	// ActiveLocksCount is the total number of locks currently held
	ActiveLocksCount int32

	// TotalWaitersCount is the total number of clients in all wait queues
	TotalWaitersCount int32

	// ContestedLocksCount is the number of locks that currently have waiters
	ContestedLocksCount int32

	// AverageHoldDuration is the average lock hold time
	AverageHoldDuration time.Duration

	// AcquisitionRatePerSecond is the rate of lock acquisitions
	AcquisitionRatePerSecond float64

	// ReleaseRatePerSecond is the rate of lock releases
	ReleaseRatePerSecond float64

	// ExpiredLocksLastPeriod is the number of locks that expired recently
	ExpiredLocksLastPeriod int32
}

// HealthStatus contains health information about the service.
type HealthStatus struct {
	// Status is the overall serving status
	Status HealthServingStatus

	// Message is a human-readable status message
	Message string

	// IsRaftLeader indicates whether this node believes it is the Raft leader
	IsRaftLeader bool

	// RaftLeaderAddress is the address of the current Raft leader (if known)
	RaftLeaderAddress string

	// RaftTerm is the current Raft term as seen by this node
	RaftTerm uint64

	// RaftLastApplied is the last log index applied by this node's state machine
	RaftLastApplied uint64

	// CurrentActiveLocks is the number of currently held locks on this node
	CurrentActiveLocks int32

	// CurrentTotalWaiters is the total number of clients in wait queues on this node
	CurrentTotalWaiters int32

	// Uptime is the server uptime duration
	Uptime time.Duration

	// LastHealthCheckAt is the timestamp of the last health check
	LastHealthCheckAt time.Time
}

// HealthServingStatus represents the health serving status.
type HealthServingStatus int

const (
	// HealthStatusUnknown indicates status cannot be determined
	HealthStatusUnknown HealthServingStatus = iota
	// HealthStatusServing indicates server is healthy and handling requests
	HealthStatusServing
	// HealthStatusNotServing indicates server is not handling requests or is unhealthy
	HealthStatusNotServing
)

// ErrorDetail contains structured error information.
type ErrorDetail struct {
	// Code is the machine-readable error code
	Code pb.ErrorCode

	// Message is a human-readable description of the error
	Message string

	// Details contains additional context for debugging
	Details map[string]string
}

// RetryPolicy defines how the client should retry failed operations.
type RetryPolicy struct {
	// MaxRetries is the maximum number of retry attempts (0 = no retries)
	MaxRetries int

	// InitialBackoff is the initial delay before the first retry
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between retries
	MaxBackoff time.Duration

	// BackoffMultiplier determines how backoff increases between retries
	BackoffMultiplier float64

	// JitterFactor adds randomness to backoff timing (0.0 to 1.0)
	JitterFactor float64

	// RetryableErrors specifies which error codes should trigger retries
	RetryableErrors []pb.ErrorCode
}
