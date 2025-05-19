package lock

import "time"

// Time
const (
	// DefaultLockTTL is the default time-to-live for a lock if not specified.
	DefaultLockTTL = 30 * time.Second

	// MaxLockTTL is the maximum allowed TTL for any lock.
	MaxLockTTL = 5 * time.Minute

	// MinLockTTL is the minimum allowed TTL for any lock.
	MinLockTTL = 1 * time.Second

	// DefaultTickInterval is how often Tick() should be called to process expirations.
	DefaultTickInterval = 1 * time.Second

	// DefaultCacheTTL is how long cached lock information remains valid.
	DefaultCacheTTL = 5 * time.Second

	// RenewalLeadTime is the recommended time before expiration to renew a lock.
	// Clients should attempt to renew locks when remaining TTL < RenewalLeadTime.
	RenewalLeadTime = 5 * time.Second

	// WaitQueueTimeout is the default timeout for waiting in a lock queue.
	DefaultWaitQueueTimeout = 60 * time.Second

	// MaxWaitQueueTimeout is the maximum allowed timeout for waiting in a lock queue.
	MaxWaitQueueTimeout = 10 * time.Minute
)

// Capacity
const (
	// DefaultMaxWaiters is the default maximum number of clients in a wait queue.
	DefaultMaxWaiters = 100

	// DefaultMaxLocks is the default maximum number of locks managed per node.
	DefaultMaxLocks = 10000

	// DefaultCacheSize is the default maximum number of locks in the read cache.
	DefaultCacheSize = 5000
)

// Lock modes
const (
	// LockModeExclusive represents an exclusive (write) lock.
	LockModeExclusive = "exclusive"

	// LockModeShared represents a shared (read) lock.
	LockModeShared = "shared"
)

// Wait queue
const (
	// WaitQueuePriorityDefault is the default priority for waiters.
	WaitQueuePriorityDefault = 0

	// WaitQueuePriorityHigh is a high priority level for waiters.
	WaitQueuePriorityHigh = 10

	// WaitQueuePriorityLow is a low priority level for waiters.
	WaitQueuePriorityLow = -10
)

// CommandType represents the type of a Raft command applied to the LockManager.
type CommandType string

const (
	// CommandTypeAcquire indicates a command to acquire a lock.
	CommandTypeAcquire CommandType = "acquire"

	// CommandTypeRelease indicates a command to release a lock.
	CommandTypeRelease CommandType = "release"

	// CommandTypeRenew indicates a command to renew the TTL of an existing lock.
	CommandTypeRenew CommandType = "renew"

	// CommandTypeEnqueueWaiter indicates a command to add a client to a lock’s wait queue.
	CommandTypeEnqueueWaiter CommandType = "enqueue_waiter"

	// CommandTypeCancelWait indicates a command to remove a client from a lock’s wait queue.
	CommandTypeCancelWait CommandType = "cancel_wait"
)

// SnapshotOperation represents a type of snapshot-related operation.
type SnapshotOperation string

const (
	// SnapshotCreate indicates a snapshot creation event.
	SnapshotCreate SnapshotOperation = "create"

	// SnapshotRestore indicates a snapshot restoration event.
	SnapshotRestore SnapshotOperation = "restore"
)
