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

	// MinWaitQueueTimeout is the minimum allowed timeout for waiting in a lock queue.
	MinWaitQueueTimeout = 1 * time.Second
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

// Wait queue
const (
	// WaitQueuePriorityDefault is the default priority for waiters.
	WaitQueuePriorityDefault = 0

	// WaitQueuePriorityHigh is a high priority level for waiters.
	WaitQueuePriorityHigh = 10

	// WaitQueuePriorityLow is a low priority level for waiters.
	WaitQueuePriorityLow = -10
)
