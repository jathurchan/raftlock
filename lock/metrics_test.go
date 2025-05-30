package lock

import (
	"testing"
	"time"

	"github.com/jathurchan/raftlock/types"
)

func TestLockMetrics_NoOpMetrics(t *testing.T) {
	metrics := NewNoOpMetrics()

	lockID := types.LockID("test-lock-1")

	metrics.IncrAcquireRequest(lockID, true, false)
	metrics.IncrReleaseRequest(lockID, true, false)
	metrics.IncrRenewRequest(lockID, true)
	metrics.IncrWaitQueueRequest(lockID, true)
	metrics.IncrWaitCancelRequest(lockID, true)
	metrics.IncrPromotedWaiter(lockID, 10*time.Millisecond)
	metrics.IncrExpiredLock(lockID)
	metrics.IncrTimeoutWaiter(lockID)
	metrics.IncrRaftApply(types.OperationAcquire, true)
	metrics.IncrSnapshotEvent(types.SnapshotCreate, true)

	metrics.ObserveLockHoldDuration(lockID, 100*time.Millisecond, true)
	metrics.ObserveAcquireLatency(lockID, 10*time.Millisecond, false)
	metrics.ObserveReleaseLatency(lockID, 5*time.Millisecond)
	metrics.ObserveRenewLatency(lockID, 3*time.Millisecond)
	metrics.ObserveWaitQueueLatency(lockID, 50*time.Millisecond)
	metrics.ObserveWaitQueueSize(lockID, 5)
	metrics.ObserveTickDuration(15*time.Millisecond, 2)
	metrics.ObserveSnapshotSize(1024)
	metrics.ObserveSnapshotDuration(types.SnapshotRestore, 200*time.Millisecond)

	metrics.SetActiveLocks(10)
	metrics.SetTotalWaiters(20)

	metrics.IncrCacheHit(lockID)
	metrics.IncrCacheMiss(lockID)
	metrics.IncrCacheExpired(lockID)
	metrics.IncrCacheAdd(lockID)
	metrics.IncrCacheUpdate(lockID)
	metrics.IncrCacheEvict(lockID)
	metrics.IncrCacheInvalidate(lockID)
	metrics.IncrCacheInvalidateAll(5)
	metrics.IncrCacheCleanup(3)

	metrics.Reset()
}
