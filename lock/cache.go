package lock

import (
	"container/list"
	"sync"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

// LockCache defines the interface for a cache that stores lock metadata
// to improve read performance for frequently accessed locks.
type LockCache interface {
	// Get retrieves the cached LockInfo for the given lock ID.
	// Returns the LockInfo and true if the entry exists and is not expired;
	// otherwise returns nil and false.
	Get(lockID types.LockID) (*types.LockInfo, bool)

	// Add inserts or updates the LockInfo for the specified lock ID,
	// using the default TTL configured for the cache.
	Add(lockID types.LockID, info *types.LockInfo)

	// Invalidate removes the cached entry for the specified lock ID, if present.
	Invalidate(lockID types.LockID)

	// InvalidateAll clears all entries from the cache immediately.
	// Useful during state resets or reinitialization.
	InvalidateAll()

	// Cleanup removes expired entries from the cache.
	// Should be invoked periodically by the LockManager (e.g., via Tick).
	// Returns the number of entries that were removed.
	Cleanup() int

	// Size returns the current number of active (non-expired) entries in the cache.
	Size() int
}

// LockCacheConfig holds configuration for the LockCache.
type LockCacheConfig struct {
	// Size is the maximum number of entries the cache can hold.
	// When the limit is reached, the least recently used entry is evicted.
	Size int

	// DefaultTTL defines the default time-to-live for each cache entry.
	// Entries are considered stale and eligible for removal after this duration.
	DefaultTTL time.Duration

	Clock   raft.Clock
	Logger  logger.Logger
	Metrics Metrics
}

// cacheEntry represents a single cached lock record stored in the LRU cache.
type cacheEntry struct {
	// info holds the lock metadata being cached.
	info *types.LockInfo

	// expiresAt indicates the absolute time at which this entry becomes invalid.
	expiresAt time.Time

	// element points to the entry's position in the LRU list,
	// enabling constant-time removal and reordering.
	element *list.Element
}

// lockCache is a thread-safe, LRU-based implementation of the LockCache interface.
// It supports entry expiration via TTL and evicts the least recently used items
// when the cache exceeds its configured capacity.
type lockCache struct {
	// mu protects access to the cache’s internal state.
	mu sync.RWMutex

	config LockCacheConfig

	// items maps lock IDs to their corresponding cache entries.
	items map[types.LockID]*cacheEntry

	// lruList maintains the order of item usage, with the most recently accessed
	// entries at the front and least recently used at the back.
	lruList *list.List

	clock   raft.Clock
	logger  logger.Logger
	metrics Metrics
}

// NewLockCache creates a new LRU-based lock cache with TTL support.
func NewLockCache(config LockCacheConfig) LockCache {
	if config.Clock == nil {
		config.Clock = raft.NewStandardClock()
	}
	if config.Logger == nil {
		config.Logger = logger.NewNoOpLogger()
	}
	if config.Metrics == nil {
		config.Metrics = newNoOpMetrics()
	}

	return &lockCache{
		config:  config,
		items:   make(map[types.LockID]*cacheEntry, config.Size),
		lruList: list.New(),
		clock:   config.Clock,
		logger:  config.Logger,
		metrics: config.Metrics,
	}
}

// Get retrieves the cached LockInfo for the given lock ID.
// Returns the LockInfo and true if the entry exists and is not expired;
// otherwise returns nil and false.
func (lc *lockCache) Get(lockID types.LockID) (*types.LockInfo, bool) {
	lc.mu.RLock()
	entry, exists := lc.items[lockID]
	lc.mu.RUnlock()

	if !exists {
		lc.metrics.IncrCacheMiss(lockID)
		return nil, false
	}

	if lc.clock.Now().After(entry.expiresAt) {
		lc.mu.Lock()
		if currentEntry, stillExists := lc.items[lockID]; stillExists {
			if lc.clock.Now().After(currentEntry.expiresAt) {
				lc.removeEntryLocked(lockID, currentEntry)
				lc.metrics.IncrCacheExpired(lockID)
				lc.logger.Debugw("Cache entry expired on access", "lockID", lockID)
			}
		}
		lc.mu.Unlock()
		return nil, false
	}

	lc.mu.Lock()
	if currentEntry, stillExists := lc.items[lockID]; stillExists {
		lc.lruList.MoveToFront(currentEntry.element)
		lc.mu.Unlock()
		lc.metrics.IncrCacheHit(lockID)
		return currentEntry.info, true
	}
	lc.mu.Unlock()
	return nil, false
}

// Add inserts or updates the LockInfo for the specified lock ID,
// using the default TTL configured for the cache.
func (sc *lockCache) Add(lockID types.LockID, info *types.LockInfo) {
	if info == nil {
		return
	}

	infoCopy := *info // To prevent mutation outside cache

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if existingEntry, exists := sc.items[lockID]; exists {
		existingEntry.info = &infoCopy
		existingEntry.expiresAt = sc.clock.Now().Add(sc.config.DefaultTTL)
		sc.lruList.MoveToFront(existingEntry.element)
		sc.metrics.IncrCacheUpdate(lockID)
		return
	}

	if sc.config.Size > 0 && sc.lruList.Len() >= sc.config.Size { // need to evict?
		sc.evictLRUEntryLocked()
	}

	element := sc.lruList.PushFront(lockID)
	entry := &cacheEntry{
		info:      &infoCopy,
		expiresAt: sc.clock.Now().Add(sc.config.DefaultTTL),
		element:   element,
	}
	sc.items[lockID] = entry
	sc.metrics.IncrCacheAdd(lockID)
}

// Invalidate removes the cached entry for the specified lock ID, if present.
func (sc *lockCache) Invalidate(lockID types.LockID) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if entry, exists := sc.items[lockID]; exists {
		sc.removeEntryLocked(lockID, entry)
		sc.metrics.IncrCacheInvalidate(lockID)
		sc.logger.Debugw("Cache entry invalidated", "lockID", lockID)
	}
}

// InvalidateAll clears all entries from the cache immediately.
// Useful during state resets or reinitialization.
func (sc *lockCache) InvalidateAll() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	oldSize := len(sc.items)
	sc.items = make(map[types.LockID]*cacheEntry, sc.config.Size)
	sc.lruList.Init()
	sc.metrics.IncrCacheInvalidateAll(oldSize)
	sc.logger.Debugw("Cache cleared", "entriesRemoved", oldSize)
}

// Cleanup removes expired entries from the cache.
// Should be invoked periodically by the LockManager (e.g., via Tick).
// Returns the number of entries that were removed.
func (sc *lockCache) Cleanup() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	now := sc.clock.Now()
	cleanedCount := 0

	expiredKeys := make([]types.LockID, 0) // to avoid mutation during iteration

	for lockID, entry := range sc.items {
		if now.After(entry.expiresAt) {
			expiredKeys = append(expiredKeys, lockID)
		}
	}

	for _, lockID := range expiredKeys {
		entry := sc.items[lockID]
		sc.removeEntryLocked(lockID, entry)
		cleanedCount++
	}

	if cleanedCount > 0 {
		sc.metrics.IncrCacheCleanup(cleanedCount)
		sc.logger.Debugw("Cache cleanup performed", "expiredEntriesRemoved", cleanedCount)
	}

	return cleanedCount
}

// Size returns the current number of active (non-expired) entries in the cache.
func (sc *lockCache) Size() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return len(sc.items)
}

// evictLRUEntryLocked removes the least recently used entry from the cache.
// Caller must hold the write lock.
func (sc *lockCache) evictLRUEntryLocked() {
	if sc.lruList.Len() == 0 {
		return
	}

	element := sc.lruList.Back()
	lockID := element.Value.(types.LockID)

	entry := sc.items[lockID]
	sc.removeEntryLocked(lockID, entry)

	sc.metrics.IncrCacheEvict(lockID)
	sc.logger.Debugw("Cache entry evicted", "lockID", lockID, "reason", "cache full")
}

// removeEntryLocked removes an entry from both the map and the LRU list.
// Caller must hold the write lock.
func (sc *lockCache) removeEntryLocked(lockID types.LockID, entry *cacheEntry) {
	delete(sc.items, lockID)
	sc.lruList.Remove(entry.element)
}
