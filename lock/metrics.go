package lock

import (
	"time"

	"github.com/jathurchan/raftlock/types"
)

// Metrics defines the interface for recording metrics related to lock operations and system performance.
// All methods must be safe for concurrent use.
type Metrics interface {
	// IncrAcquireRequest increments counters for lock acquisition attempts.
	// `success` indicates whether the attempt succeeded.
	// `waitQueued` indicates whether the client entered a wait queue.
	IncrAcquireRequest(lockID types.LockID, success bool, waitQueued bool)

	// IncrReleaseRequest increments counters for lock releases.
	// `byExpiration` is true if the lock expired naturally.
	IncrReleaseRequest(lockID types.LockID, success bool, byExpiration bool)

	// IncrRenewRequest increments counters for lock renewals.
	IncrRenewRequest(lockID types.LockID, success bool)

	// IncrWaitQueueRequest increments counters for enqueue attempts.
	IncrWaitQueueRequest(lockID types.LockID, success bool)

	// IncrWaitCancelRequest increments counters for wait queue cancellations.
	IncrWaitCancelRequest(lockID types.LockID, success bool)

	// IncrPromotedWaiter increments counters when a waiter is promoted to lock holder.
	IncrPromotedWaiter(lockID types.LockID, waitTime time.Duration)

	// IncrExpiredLock increments counters for locks that expired due to TTL.
	IncrExpiredLock(lockID types.LockID)

	// IncrTimeoutWaiter increments counters for waiters removed due to timeout.
	IncrTimeoutWaiter(lockID types.LockID)

	// IncrRaftApply increments counters for Raft-applied commands.
	IncrRaftApply(cmdType types.LockOperation, success bool)

	// IncrSnapshotEvent increments counters for snapshot create/restore events.
	IncrSnapshotEvent(operation types.SnapshotOperation, success bool)

	// ObserveLockHoldDuration records how long a lock was held.
	// `byRelease` is true if the lock was explicitly released.
	ObserveLockHoldDuration(lockID types.LockID, holdTime time.Duration, byRelease bool)

	// ObserveAcquireLatency records time taken to acquire a lock.
	// `contested` is true if the lock was initially unavailable.
	ObserveAcquireLatency(lockID types.LockID, latency time.Duration, contested bool)

	// ObserveReleaseLatency records time taken to release a lock.
	ObserveReleaseLatency(lockID types.LockID, latency time.Duration)

	// ObserveRenewLatency records time taken to renew a lock.
	ObserveRenewLatency(lockID types.LockID, latency time.Duration)

	// ObserveWaitQueueLatency records the latency of enqueueing to a wait queue.
	ObserveWaitQueueLatency(lockID types.LockID, latency time.Duration)

	// ObserveWaitQueueSize records the current size of a lock's wait queue.
	ObserveWaitQueueSize(lockID types.LockID, size int)

	// ObserveTickDuration records how long a tick cycle took and how many locks expired.
	ObserveTickDuration(duration time.Duration, expiredCount int)

	// ObserveSnapshotSize records the byte size of a snapshot.
	ObserveSnapshotSize(bytes int)

	// ObserveSnapshotDuration records the duration of a snapshot operation.
	ObserveSnapshotDuration(operation types.SnapshotOperation, duration time.Duration)

	// SetActiveLocks sets the current number of held locks.
	SetActiveLocks(count int)

	// SetTotalWaiters sets the current number of clients across all wait queues.
	SetTotalWaiters(count int)

	// IncrCacheHit increments counters for successful cache hits.
	IncrCacheHit(lockID types.LockID)

	// IncrCacheMiss increments counters for failed cache lookups.
	IncrCacheMiss(lockID types.LockID)

	// IncrCacheExpired increments counters for expired cache entries.
	IncrCacheExpired(lockID types.LockID)

	// IncrCacheAdd increments counters when an entry is added to the cache.
	IncrCacheAdd(lockID types.LockID)

	// IncrCacheUpdate increments counters when an existing cache entry is updated.
	IncrCacheUpdate(lockID types.LockID)

	// IncrCacheEvict increments counters for cache evictions.
	IncrCacheEvict(lockID types.LockID)

	// IncrCacheInvalidate increments counters for explicit cache invalidation.
	IncrCacheInvalidate(lockID types.LockID)

	// IncrCacheInvalidateAll increments counters when the full cache is invalidated.
	IncrCacheInvalidateAll(count int)

	// IncrCacheCleanup increments counters when a cache cleanup run completes.
	IncrCacheCleanup(count int)

	// Reset clears all metrics.
	Reset()
}

type NoOpMetrics struct{}

func NewNoOpMetrics() Metrics {
	return &NoOpMetrics{}
}

func (n *NoOpMetrics) IncrAcquireRequest(lockID types.LockID, success bool, waitQueued bool)   {}
func (n *NoOpMetrics) IncrReleaseRequest(lockID types.LockID, success bool, byExpiration bool) {}
func (n *NoOpMetrics) IncrRenewRequest(lockID types.LockID, success bool)                      {}
func (n *NoOpMetrics) IncrWaitQueueRequest(lockID types.LockID, success bool)                  {}
func (n *NoOpMetrics) IncrWaitCancelRequest(lockID types.LockID, success bool)                 {}
func (n *NoOpMetrics) IncrPromotedWaiter(lockID types.LockID, waitTime time.Duration)          {}
func (n *NoOpMetrics) IncrExpiredLock(lockID types.LockID)                                     {}
func (n *NoOpMetrics) IncrTimeoutWaiter(lockID types.LockID)                                   {}
func (n *NoOpMetrics) IncrRaftApply(cmdType types.LockOperation, success bool)                 {}
func (n *NoOpMetrics) IncrSnapshotEvent(operation types.SnapshotOperation, success bool)       {}

func (n *NoOpMetrics) ObserveLockHoldDuration(
	lockID types.LockID,
	holdTime time.Duration,
	byRelease bool,
) {
}

func (n *NoOpMetrics) ObserveAcquireLatency(
	lockID types.LockID,
	latency time.Duration,
	contested bool,
) {
}
func (n *NoOpMetrics) ObserveReleaseLatency(lockID types.LockID, latency time.Duration)   {}
func (n *NoOpMetrics) ObserveRenewLatency(lockID types.LockID, latency time.Duration)     {}
func (n *NoOpMetrics) ObserveWaitQueueLatency(lockID types.LockID, latency time.Duration) {}
func (n *NoOpMetrics) ObserveWaitQueueSize(lockID types.LockID, size int)                 {}
func (n *NoOpMetrics) ObserveTickDuration(duration time.Duration, expiredCount int)       {}
func (n *NoOpMetrics) ObserveSnapshotSize(bytes int)                                      {}

func (n *NoOpMetrics) ObserveSnapshotDuration(
	operation types.SnapshotOperation,
	duration time.Duration,
) {
}

func (n *NoOpMetrics) SetActiveLocks(count int)  {}
func (n *NoOpMetrics) SetTotalWaiters(count int) {}

func (n *NoOpMetrics) IncrCacheHit(lockID types.LockID)        {}
func (n *NoOpMetrics) IncrCacheMiss(lockID types.LockID)       {}
func (n *NoOpMetrics) IncrCacheExpired(lockID types.LockID)    {}
func (n *NoOpMetrics) IncrCacheAdd(lockID types.LockID)        {}
func (n *NoOpMetrics) IncrCacheUpdate(lockID types.LockID)     {}
func (n *NoOpMetrics) IncrCacheEvict(lockID types.LockID)      {}
func (n *NoOpMetrics) IncrCacheInvalidate(lockID types.LockID) {}
func (n *NoOpMetrics) IncrCacheInvalidateAll(count int)        {}
func (n *NoOpMetrics) IncrCacheCleanup(count int)              {}

func (n *NoOpMetrics) Reset() {}
