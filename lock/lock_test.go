package lock

import (
	"container/heap"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func createTestLockManager(t *testing.T) (*lockManager, *mockClock) {
	t.Helper()
	clock := newMockClock()

	config := LockManagerConfig{
		DefaultTTL:          30 * time.Second,
		MaxTTL:              5 * time.Minute,
		TickInterval:        1 * time.Second,
		MaxWaiters:          100,
		EnablePriorityQueue: false,
		EnableCache:         false,
		CacheTTL:            5 * time.Second,
		MaxLocks:            10000,
		CacheSize:           5000,
		Serializer:          &JSONSerializer{},
		Clock:               clock,
		Logger:              logger.NewNoOpLogger(),
		Metrics:             NewNoOpMetrics(),
	}

	expHeap := make(expirationHeap, 0)
	heap.Init(&expHeap)

	lm := &lockManager{
		locks:            make(map[types.LockID]*lockState),
		waiters:          make(map[types.LockID]*waitQueue),
		clientLocks:      make(map[types.ClientID]map[types.LockID]struct{}),
		pendingWaits:     make(map[types.LockID]map[types.ClientID]*waiter),
		expirationHeap:   &expHeap,
		lastAppliedIndex: 0,
		config:           config,
		serializer:       config.Serializer,
		clock:            clock,
		logger:           config.Logger.WithComponent("lockmanager"),
		metrics:          config.Metrics,
	}

	return lm, clock
}

func getInternal(t *testing.T, lm LockManager) *lockManager {
	internal, ok := lm.(*lockManager)
	testutil.AssertTrue(t, ok, "Expected *lockManager type")
	return internal
}

func assertInitialized(t *testing.T, internal *lockManager) {
	testutil.AssertNotNil(t, internal.locks)
	testutil.AssertNotNil(t, internal.waiters)
	testutil.AssertNotNil(t, internal.expirationHeap)
	testutil.AssertNotNil(t, internal.serializer)
	testutil.AssertNotNil(t, internal.logger)
	testutil.AssertNotNil(t, internal.metrics)
	testutil.AssertNotNil(t, internal.clock)
	testutil.AssertEqual(t, types.Index(0), internal.lastAppliedIndex)
}

func TestNewLockManager_Defaults(t *testing.T) {
	lm := NewLockManager()
	testutil.RequireNotNil(t, lm)

	internal := getInternal(t, lm)
	assertInitialized(t, internal)

	expected := DefaultLockManagerConfig()
	testutil.AssertEqual(t, expected.DefaultTTL, internal.config.DefaultTTL)
	testutil.AssertEqual(t, expected.MaxTTL, internal.config.MaxTTL)
	testutil.AssertFalse(t, internal.config.EnableCache)
	testutil.AssertNil(t, internal.cache)
}

func TestNewLockManager_WithCustomOptions(t *testing.T) {
	customTTL := 45 * time.Second
	mockClock := newMockClock()

	lm := NewLockManager(
		WithDefaultTTL(customTTL),
		WithClock(mockClock),
		WithCache(true),
		WithCacheSize(1000),
	)

	internal := getInternal(t, lm)
	assertInitialized(t, internal)

	testutil.AssertEqual(t, customTTL, internal.config.DefaultTTL)
	testutil.AssertTrue(t, internal.config.EnableCache)
	testutil.AssertEqual(t, 1000, internal.config.CacheSize)
	testutil.AssertEqual(t, mockClock, internal.clock)
	testutil.AssertNotNil(t, internal.cache)
}

func TestLockManager_Apply(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("test-apply-lock")
	clientID := types.ClientID("test-apply-client")

	t.Run("apply acquire command", func(t *testing.T) {
		cmd := types.Command{
			Op:       types.OperationAcquire,
			LockID:   lockID,
			ClientID: clientID,
			TTL:      30000, // 30 seconds in milliseconds
		}

		cmdData, err := json.Marshal(cmd)
		testutil.RequireNoError(t, err)

		err = lm.Apply(ctx, types.Index(1), cmdData)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, types.Index(1), lm.lastAppliedIndex)

		info, err := lm.GetLockInfo(ctx, lockID)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, clientID, info.OwnerID)
	})

	t.Run("apply with empty command", func(t *testing.T) {
		err := lm.Apply(ctx, types.Index(2), []byte{})
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "empty command")
	})

	t.Run("apply with invalid command data", func(t *testing.T) {
		invalidData := []byte("invalid json")
		err := lm.Apply(ctx, types.Index(3), invalidData)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to decode command")
	})

	t.Run("apply duplicate index (idempotency)", func(t *testing.T) {
		cmd := types.Command{
			Op:       types.OperationRelease,
			LockID:   lockID,
			ClientID: clientID,
			Version:  types.Index(1),
		}

		cmdData, err := json.Marshal(cmd)
		testutil.RequireNoError(t, err)

		err = lm.Apply(ctx, types.Index(1), cmdData)
		testutil.AssertNoError(t, err)

		info, err := lm.GetLockInfo(ctx, lockID)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, clientID, info.OwnerID)
	})
}

func TestLockManager_ApplyAcquire(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("test-lock-1")
	clientID := types.ClientID("client-1")
	ttl := 30 * time.Second
	version := types.Index(1)

	t.Run("successful acquisition of new lock", func(t *testing.T) {
		info, err := lm.ApplyAcquire(ctx, lockID, clientID, ttl, version)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, info)
		testutil.AssertEqual(t, lockID, info.LockID)
		testutil.AssertEqual(t, clientID, info.OwnerID)
		testutil.AssertEqual(t, version, info.Version)
		testutil.AssertFalse(t, info.AcquiredAt.IsZero())
		testutil.AssertFalse(t, info.ExpiresAt.IsZero())
	})

	t.Run("renewal by same client", func(t *testing.T) {
		newTTL := 60 * time.Second
		newVersion := types.Index(2)

		info, err := lm.ApplyAcquire(ctx, lockID, clientID, newTTL, newVersion)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, info)
		testutil.AssertEqual(t, clientID, info.OwnerID)
		testutil.AssertEqual(t, newVersion, info.Version)
	})

	t.Run("acquisition failure when held by different client", func(t *testing.T) {
		differentClient := types.ClientID("client-2")

		info, err := lm.ApplyAcquire(ctx, lockID, differentClient, ttl, version)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrLockHeld)
		testutil.AssertNil(t, info)
	})

	t.Run("invalid TTL rejection", func(t *testing.T) {
		invalidTTL := 10 * time.Hour // Exceeds MaxTTL
		newLockID := types.LockID("test-lock-2")

		info, err := lm.ApplyAcquire(ctx, newLockID, clientID, invalidTTL, version)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrInvalidTTL)
		testutil.AssertNil(t, info)
	})
}

func TestLockManager_ApplyRelease(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("test-lock-release")
	clientID := types.ClientID("client-release")
	ttl := 30 * time.Second
	version := types.Index(1)

	_, err := lm.ApplyAcquire(ctx, lockID, clientID, ttl, version)
	testutil.RequireNoError(t, err)

	t.Run("successful release by owner", func(t *testing.T) {
		released, err := lm.ApplyRelease(ctx, lockID, clientID, version)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, released)

		lm.mu.RLock()
		lock, exists := lm.locks[lockID]
		testutil.AssertTrue(t, exists)
		testutil.AssertEqual(t, types.ClientID(""), lock.Owner)
		lm.mu.RUnlock()
	})

	t.Run("release failure when lock not held", func(t *testing.T) {
		nonExistentLock := types.LockID("non-existent")

		released, err := lm.ApplyRelease(ctx, nonExistentLock, clientID, version)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrLockNotHeld)
		testutil.AssertFalse(t, released)
	})

	t.Run("release failure by non-owner", func(t *testing.T) {
		_, err := lm.ApplyAcquire(ctx, lockID, clientID, ttl, version)
		testutil.RequireNoError(t, err)

		differentClient := types.ClientID("different-client")
		released, err := lm.ApplyRelease(ctx, lockID, differentClient, version)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotLockOwner)
		testutil.AssertFalse(t, released)
	})

	t.Run("release failure with version mismatch", func(t *testing.T) {
		wrongVersion := types.Index(999)
		released, err := lm.ApplyRelease(ctx, lockID, clientID, wrongVersion)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrVersionMismatch)
		testutil.AssertFalse(t, released)
	})
}

func TestLockManager_ApplyRenew(t *testing.T) {
	lm, clock := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("test-lock-renew")
	clientID := types.ClientID("client-renew")
	ttl := 30 * time.Second
	version := types.Index(1)

	_, err := lm.ApplyAcquire(ctx, lockID, clientID, ttl, version)
	testutil.RequireNoError(t, err)

	t.Run("successful renewal by owner", func(t *testing.T) {
		newTTL := 60 * time.Second
		newVersion := types.Index(2)

		err := lm.ApplyRenew(ctx, lockID, clientID, newVersion, newTTL)

		testutil.AssertNoError(t, err)

		lm.mu.RLock()
		lock := lm.locks[lockID]
		testutil.AssertEqual(t, newVersion, lock.Version)
		expectedExpiry := clock.Now().Add(newTTL)
		testutil.AssertTrue(t, lock.ExpiresAt.Equal(expectedExpiry) || lock.ExpiresAt.After(expectedExpiry.Add(-time.Second)))
		lm.mu.RUnlock()
	})

	t.Run("renewal failure when lock not held", func(t *testing.T) {
		nonExistentLock := types.LockID("non-existent-renew")

		err := lm.ApplyRenew(ctx, nonExistentLock, clientID, version, ttl)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrLockNotHeld)
	})

	t.Run("renewal failure by non-owner", func(t *testing.T) {
		differentClient := types.ClientID("different-client-renew")

		err := lm.ApplyRenew(ctx, lockID, differentClient, version, ttl)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotLockOwner)
	})

	t.Run("renewal failure with invalid TTL", func(t *testing.T) {
		invalidTTL := 10 * time.Hour // Exceeds MaxTTL

		err := lm.ApplyRenew(ctx, lockID, clientID, version, invalidTTL)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrInvalidTTL)
	})
}

func TestLockManager_ApplyWaitQueue(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("test-lock-wait")
	ownerClient := types.ClientID("owner-client")
	waiterClient := types.ClientID("waiter-client")
	ttl := 30 * time.Second
	timeout := 60 * time.Second
	version := types.Index(1)
	priority := 0

	_, err := lm.ApplyAcquire(ctx, lockID, ownerClient, ttl, version)
	testutil.RequireNoError(t, err)

	t.Run("successful enqueue to wait queue", func(t *testing.T) {
		position, err := lm.ApplyWaitQueue(ctx, lockID, waiterClient, timeout, version, priority)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 0, position) // First waiter should be at position 0

		lm.mu.RLock()
		wq, exists := lm.waiters[lockID]
		testutil.AssertTrue(t, exists)
		testutil.AssertEqual(t, 1, wq.Len())
		lm.mu.RUnlock()
	})

	t.Run("update existing waiter", func(t *testing.T) {
		newTimeout := 120 * time.Second
		newPriority := 5

		position, err := lm.ApplyWaitQueue(ctx, lockID, waiterClient, newTimeout, version, newPriority)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 0, position) // Still at same position

		lm.mu.RLock()
		wq := lm.waiters[lockID]
		testutil.AssertEqual(t, 1, wq.Len())
		lm.mu.RUnlock()
	})

	t.Run("enqueue failure with invalid timeout", func(t *testing.T) {
		invalidTimeout := -1 * time.Second
		newClient := types.ClientID("new-waiter")

		position, err := lm.ApplyWaitQueue(ctx, lockID, newClient, invalidTimeout, version, priority)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrInvalidTimeout)
		testutil.AssertEqual(t, 0, position)
	})

	t.Run("enqueue failure when queue is full", func(t *testing.T) {
		lm.config.MaxWaiters = 1

		newClient := types.ClientID("queue-full-client")
		position, err := lm.ApplyWaitQueue(ctx, lockID, newClient, timeout, version, priority)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrWaitQueueFull)
		testutil.AssertEqual(t, 0, position)
	})
}

func TestLockManager_ApplyCancelWait(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("test-lock-cancel")
	ownerClient := types.ClientID("owner-cancel")
	waiterClient := types.ClientID("waiter-cancel")
	ttl := 30 * time.Second
	timeout := 60 * time.Second
	version := types.Index(1)
	priority := 0

	_, err := lm.ApplyAcquire(ctx, lockID, ownerClient, ttl, version)
	testutil.RequireNoError(t, err)

	_, err = lm.ApplyWaitQueue(ctx, lockID, waiterClient, timeout, version, priority)
	testutil.RequireNoError(t, err)

	t.Run("successful wait cancellation", func(t *testing.T) {
		cancelled, err := lm.ApplyCancelWait(ctx, lockID, waiterClient, version)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, cancelled)

		lm.mu.RLock()
		wq, exists := lm.waiters[lockID]
		if exists {
			testutil.AssertEqual(t, 0, wq.Len())
		}
		lm.mu.RUnlock()
	})

	t.Run("cancellation failure when not waiting", func(t *testing.T) {
		nonWaitingClient := types.ClientID("not-waiting")

		cancelled, err := lm.ApplyCancelWait(ctx, lockID, nonWaitingClient, version)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotWaiting)
		testutil.AssertFalse(t, cancelled)
	})

	t.Run("cancellation failure for non-existent lock", func(t *testing.T) {
		nonExistentLock := types.LockID("non-existent-cancel")

		cancelled, err := lm.ApplyCancelWait(ctx, nonExistentLock, waiterClient, version)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotWaiting)
		testutil.AssertFalse(t, cancelled)
	})
}

func TestLockManager_GetLockInfo(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("test-lock-info")
	clientID := types.ClientID("client-info")
	ttl := 30 * time.Second
	version := types.Index(1)

	t.Run("get info for non-existent lock", func(t *testing.T) {
		info, err := lm.GetLockInfo(ctx, lockID)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrLockNotFound)
		testutil.AssertNil(t, info)
	})

	_, err := lm.ApplyAcquire(ctx, lockID, clientID, ttl, version)
	testutil.RequireNoError(t, err)

	t.Run("get info for existing lock", func(t *testing.T) {
		info, err := lm.GetLockInfo(ctx, lockID)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, info)
		testutil.AssertEqual(t, lockID, info.LockID)
		testutil.AssertEqual(t, clientID, info.OwnerID)
		testutil.AssertEqual(t, version, info.Version)
		testutil.AssertFalse(t, info.AcquiredAt.IsZero())
		testutil.AssertFalse(t, info.ExpiresAt.IsZero())
		testutil.AssertEqual(t, 0, info.WaiterCount)
	})
}

func TestLockManager_GetLocks(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	client1 := types.ClientID("client-1")
	client2 := types.ClientID("client-2")
	ttl := 30 * time.Second

	locks := []types.LockID{"lock-1", "lock-2", "lock-3"}
	for i, lockID := range locks {
		var clientID types.ClientID
		if i%2 == 0 {
			clientID = client1
		} else {
			clientID = client2
		}
		_, err := lm.ApplyAcquire(ctx, lockID, clientID, ttl, types.Index(i+1))
		testutil.RequireNoError(t, err)
	}

	t.Run("get all locks", func(t *testing.T) {
		lockInfos, total, err := lm.GetLocks(ctx, FilterAll, 0, 0)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 3, total)
		testutil.AssertLen(t, lockInfos, 3)
	})

	t.Run("get locks with pagination", func(t *testing.T) {
		lockInfos, total, err := lm.GetLocks(ctx, FilterAll, 2, 0)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 3, total)
		testutil.AssertLen(t, lockInfos, 2)
	})

	t.Run("get locks with offset", func(t *testing.T) {
		lockInfos, total, err := lm.GetLocks(ctx, FilterAll, 0, 1)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 3, total)
		testutil.AssertLen(t, lockInfos, 2)
	})

	t.Run("get locks by owner filter", func(t *testing.T) {
		filterByClient1 := FilterByOwner(client1)
		lockInfos, total, err := lm.GetLocks(ctx, filterByClient1, 0, 0)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 2, total) // client1 owns locks at index 0 and 2
		testutil.AssertLen(t, lockInfos, 2)

		for _, lockInfo := range lockInfos {
			testutil.AssertEqual(t, client1, lockInfo.OwnerID)
		}
	})
}

func TestLockManager_Tick(t *testing.T) {
	lm, clock := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("test-lock-tick")
	clientID := types.ClientID("client-tick")
	ttl := 5 * time.Second
	version := types.Index(1)

	_, err := lm.ApplyAcquire(ctx, lockID, clientID, ttl, version)
	testutil.RequireNoError(t, err)

	t.Run("tick with no expirations", func(t *testing.T) {
		expiredCount := lm.Tick(ctx)
		testutil.AssertEqual(t, 0, expiredCount)

		info, err := lm.GetLockInfo(ctx, lockID)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, clientID, info.OwnerID)
	})

	t.Run("tick with lock expiration", func(t *testing.T) {
		clock.Advance(ttl + time.Second)

		expiredCount := lm.Tick(ctx)
		testutil.AssertEqual(t, 1, expiredCount)

		lm.mu.RLock()
		lock := lm.locks[lockID]
		testutil.AssertEqual(t, types.ClientID(""), lock.Owner)
		lm.mu.RUnlock()
	})
}

func TestLockManager_Snapshot(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("test-lock-snapshot")
	clientID := types.ClientID("client-snapshot")
	version := types.Index(1)

	// Use Apply method to properly set lastAppliedIndex
	cmd := types.Command{
		Op:       types.OperationAcquire,
		LockID:   lockID,
		ClientID: clientID,
		TTL:      30000, // 30 seconds in milliseconds
	}

	cmdData, err := json.Marshal(cmd)
	testutil.RequireNoError(t, err)

	err = lm.Apply(ctx, version, cmdData)
	testutil.RequireNoError(t, err)

	t.Run("create snapshot", func(t *testing.T) {
		index, data, err := lm.Snapshot(ctx)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, version, index)
		testutil.AssertTrue(t, len(data) > 0)

		// Verify we can deserialize the snapshot
		var snapshot lockSnapshot
		err = json.Unmarshal(data, &snapshot)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, version, snapshot.LastAppliedIndex)
		testutil.AssertEqual(t, 1, len(snapshot.Locks))
	})
}

func TestLockManager_RestoreSnapshot(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("restored-lock")
	clientID := types.ClientID("restored-client")
	version := types.Index(5)
	now := time.Now()

	testLock := &lockState{
		LockID:       lockID,
		Owner:        clientID,
		Version:      version,
		AcquiredAt:   now,
		ExpiresAt:    now.Add(30 * time.Second),
		Metadata:     make(map[string]string),
		LastModified: now,
	}

	snapshot := lockSnapshot{
		LastAppliedIndex: version,
		Locks:            map[types.LockID]*lockState{lockID: testLock},
		Waiters:          make(map[types.LockID][]*waiter),
		Version:          1,
	}

	data, err := json.Marshal(snapshot)
	testutil.RequireNoError(t, err)

	t.Run("restore snapshot", func(t *testing.T) {
		err := lm.RestoreSnapshot(ctx, version, types.Term(1), data)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, version, lm.lastAppliedIndex)

		info, err := lm.GetLockInfo(ctx, lockID)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, lockID, info.LockID)
		testutil.AssertEqual(t, clientID, info.OwnerID)
		testutil.AssertEqual(t, version, info.Version)
	})

	t.Run("ignore stale snapshot", func(t *testing.T) {
		staleIndex := types.Index(3) // Less than current lastAppliedIndex

		err := lm.RestoreSnapshot(ctx, staleIndex, types.Term(1), data)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, version, lm.lastAppliedIndex)
	})

	t.Run("restore snapshot with invalid data", func(t *testing.T) {
		invalidData := []byte("invalid json")

		err := lm.RestoreSnapshot(ctx, version+1, types.Term(1), invalidData)

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "unmarshal snapshot")
	})
}

func TestLockManager_WaiterPromotion(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("promotion-lock")
	ownerClient := types.ClientID("owner")
	waiterClient := types.ClientID("waiter")
	ttl := 30 * time.Second
	timeout := 60 * time.Second
	version := types.Index(1)

	_, err := lm.ApplyAcquire(ctx, lockID, ownerClient, ttl, version)
	testutil.RequireNoError(t, err)

	_, err = lm.ApplyWaitQueue(ctx, lockID, waiterClient, timeout, version, 0)
	testutil.RequireNoError(t, err)

	t.Run("waiter promotion on release", func(t *testing.T) {
		_, err := lm.ApplyRelease(ctx, lockID, ownerClient, version)
		testutil.RequireNoError(t, err)

		info, err := lm.GetLockInfo(ctx, lockID)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, waiterClient, info.OwnerID)
		testutil.AssertEqual(t, 0, info.WaiterCount) // Queue should be empty now
	})
}

func TestLockManager_TimeoutWaiters(t *testing.T) {
	lm, clock := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("timeout-lock")
	ownerClient := types.ClientID("timeout-owner")
	waiterClient := types.ClientID("timeout-waiter")
	ttl := 30 * time.Second
	shortTimeout := 5 * time.Second
	version := types.Index(1)

	_, err := lm.ApplyAcquire(ctx, lockID, ownerClient, ttl, version)
	testutil.RequireNoError(t, err)

	_, err = lm.ApplyWaitQueue(ctx, lockID, waiterClient, shortTimeout, version, 0)
	testutil.RequireNoError(t, err)

	t.Run("waiter timeout cleanup", func(t *testing.T) {
		lm.mu.RLock()
		wq := lm.waiters[lockID]
		testutil.AssertEqual(t, 1, wq.Len())
		lm.mu.RUnlock()

		clock.Advance(shortTimeout + time.Second)

		lm.Tick(ctx)

		lm.mu.RLock()
		wq, exists := lm.waiters[lockID]
		if exists {
			testutil.AssertEqual(t, 0, wq.Len())
		}
		lm.mu.RUnlock()
	})
}

func TestLockManager_Close(t *testing.T) {
	lm, _ := createTestLockManager(t)
	ctx := context.Background()

	lockID := types.LockID("close-lock")
	clientID := types.ClientID("close-client")
	ttl := 30 * time.Second

	_, err := lm.ApplyAcquire(ctx, lockID, clientID, ttl, types.Index(1))
	testutil.RequireNoError(t, err)

	t.Run("close cleans up resources", func(t *testing.T) {
		err := lm.Close()
		testutil.AssertNoError(t, err)

		lm.mu.RLock()
		testutil.AssertEmpty(t, lm.locks)
		testutil.AssertEmpty(t, lm.waiters)
		testutil.AssertEmpty(t, lm.clientLocks)
		testutil.AssertEmpty(t, lm.pendingWaits)
		testutil.AssertEqual(t, 0, lm.expirationHeap.Len())
		lm.mu.RUnlock()
	})
}
