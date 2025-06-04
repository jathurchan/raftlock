package lock

import (
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func createTestCache(size int, ttl time.Duration, clock *mockClock) LockCache {
	if clock == nil {
		clock = newMockClock()
	}
	config := LockCacheConfig{
		Size:       size,
		DefaultTTL: ttl,
		Clock:      clock,
		Logger:     logger.NewNoOpLogger(),
		Metrics:    NewNoOpMetrics(),
	}
	return NewLockCache(config)
}

func createTestLockInfo(lockID types.LockID, ownerID types.ClientID) *types.LockInfo {
	return &types.LockInfo{
		LockID:       lockID,
		OwnerID:      ownerID,
		Version:      1,
		AcquiredAt:   time.Now(),
		ExpiresAt:    time.Now().Add(30 * time.Second),
		LastModified: time.Now(),
		Metadata:     make(map[string]string),
	}
}

func TestLockCache_NewLockCache(t *testing.T) {
	cache := createTestCache(10, 5*time.Second, nil)
	testutil.AssertNotNil(t, cache, "Cache should not be nil")

	// Test with nil dependencies
	config := LockCacheConfig{Size: 10, DefaultTTL: 5 * time.Second}
	cache2 := NewLockCache(config)
	testutil.AssertNotNil(t, cache2, "Cache with defaults should not be nil")
}

func TestLockCache_Get_Miss(t *testing.T) {
	cache := createTestCache(10, 5*time.Second, nil)

	info, found := cache.Get("non-existent")
	testutil.AssertFalse(t, found, "Should not find non-existent entry")
	testutil.AssertNil(t, info, "Info should be nil")
}

func TestLockCache_Add_And_Get(t *testing.T) {
	cache := createTestCache(10, 5*time.Second, nil)
	lockInfo := createTestLockInfo("test-lock", "client-1")

	cache.Add("test-lock", lockInfo)

	info, found := cache.Get("test-lock")
	testutil.AssertTrue(t, found, "Should find added entry")
	testutil.AssertEqual(t, lockInfo.LockID, info.LockID, "Lock ID should match")
	testutil.AssertEqual(t, lockInfo.OwnerID, info.OwnerID, "Owner ID should match")
}

func TestLockCache_Add_Nil(t *testing.T) {
	cache := createTestCache(10, 5*time.Second, nil)

	cache.Add("test-lock", nil)

	_, found := cache.Get("test-lock")
	testutil.AssertFalse(t, found, "Should not add nil info")
}

func TestLockCache_Add_Update(t *testing.T) {
	cache := createTestCache(10, 5*time.Second, nil)

	cache.Add("test-lock", createTestLockInfo("test-lock", "client-1"))
	cache.Add("test-lock", createTestLockInfo("test-lock", "client-2"))

	info, found := cache.Get("test-lock")
	testutil.AssertTrue(t, found, "Should find updated entry")
	testutil.AssertEqual(t, types.ClientID("client-2"), info.OwnerID, "Should have updated owner")
}

func TestLockCache_Expiration(t *testing.T) {
	clock := newMockClock()
	cache := createTestCache(10, 5*time.Second, clock)

	cache.Add("test-lock", createTestLockInfo("test-lock", "client-1"))

	clock.Advance(10 * time.Second)

	info, found := cache.Get("test-lock")
	testutil.AssertFalse(t, found, "Should not find expired entry")
	testutil.AssertNil(t, info, "Info should be nil for expired entry")
	testutil.AssertEqual(t, 0, cache.Size(), "Cache should be empty after expiration")
}

func TestLockCache_Invalidate(t *testing.T) {
	cache := createTestCache(10, 5*time.Second, nil)

	cache.Add("test-lock", createTestLockInfo("test-lock", "client-1"))
	testutil.AssertEqual(t, 1, cache.Size(), "Should have one entry")

	cache.Invalidate("test-lock")

	_, found := cache.Get("test-lock")
	testutil.AssertFalse(t, found, "Should not find invalidated entry")
	testutil.AssertEqual(t, 0, cache.Size(), "Should be empty")
}

func TestLockCache_InvalidateAll(t *testing.T) {
	cache := createTestCache(10, 5*time.Second, nil)

	cache.Add("lock-1", createTestLockInfo("lock-1", "client-1"))
	cache.Add("lock-2", createTestLockInfo("lock-2", "client-2"))
	testutil.AssertEqual(t, 2, cache.Size(), "Should have 2 entries")

	cache.InvalidateAll()

	testutil.AssertEqual(t, 0, cache.Size(), "Should be empty after InvalidateAll")
	_, found := cache.Get("lock-1")
	testutil.AssertFalse(t, found, "lock-1 should be gone")
}

func TestLockCache_Cleanup(t *testing.T) {
	clock := newMockClock()
	cache := createTestCache(10, 5*time.Second, clock)

	cache.Add("lock-1", createTestLockInfo("lock-1", "client-1"))
	cache.Add("lock-2", createTestLockInfo("lock-2", "client-2"))

	// Advance time to expire first entry
	clock.Advance(3 * time.Second)
	cache.Add("lock-3", createTestLockInfo("lock-3", "client-3"))
	clock.Advance(3 * time.Second) // first two should expire

	removed := cache.Cleanup()

	testutil.AssertEqual(t, 2, removed, "Should remove 2 expired entries")
	testutil.AssertEqual(t, 1, cache.Size(), "Should have 1 remaining")

	_, found := cache.Get("lock-3")
	testutil.AssertTrue(t, found, "lock-3 should remain")
}

func TestLockCache_LRU_Eviction(t *testing.T) {
	cache := createTestCache(2, 5*time.Second, nil)

	cache.Add("lock-1", createTestLockInfo("lock-1", "client-1"))
	cache.Add("lock-2", createTestLockInfo("lock-2", "client-2"))

	cache.Get("lock-1")

	cache.Add("lock-3", createTestLockInfo("lock-3", "client-3"))

	testutil.AssertEqual(t, 2, cache.Size(), "Should remain at capacity")

	_, found := cache.Get("lock-1")
	testutil.AssertTrue(t, found, "lock-1 should remain (recently used)")

	_, found = cache.Get("lock-2")
	testutil.AssertFalse(t, found, "lock-2 should be evicted (LRU)")

	_, found = cache.Get("lock-3")
	testutil.AssertTrue(t, found, "lock-3 should exist (newly added)")
}

func TestLockCache_Size(t *testing.T) {
	cache := createTestCache(10, 5*time.Second, nil)

	testutil.AssertEqual(t, 0, cache.Size(), "Should start empty")

	cache.Add("lock-1", createTestLockInfo("lock-1", "client-1"))
	testutil.AssertEqual(t, 1, cache.Size(), "Should have 1 entry")

	cache.Add("lock-2", createTestLockInfo("lock-2", "client-2"))
	testutil.AssertEqual(t, 2, cache.Size(), "Should have 2 entries")

	cache.Invalidate("lock-1")
	testutil.AssertEqual(t, 1, cache.Size(), "Should have 1 entry after invalidation")
}

func TestLockCache_UnlimitedSize(t *testing.T) {
	cache := createTestCache(0, 5*time.Second, nil) // 0 = unlimited

	for i := range 100 {
		lockID := types.LockID("lock-" + string(rune('0'+i%10)))
		cache.Add(lockID, createTestLockInfo(lockID, "client-1"))
	}

	testutil.AssertEqual(t, 10, cache.Size(), "Should retain unique entries")
}
