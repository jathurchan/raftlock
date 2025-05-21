package lock

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

// lockState encapsulates the current state of a specific lock.
type lockState struct {
	// Unique identifier of the lock.
	lockID types.LockID

	// Identifier of the client currently holding the lock. Empty if unowned.
	owner types.ClientID

	// Raft log index when the lock was last acquired or renewed.
	version types.Index

	// Timestamp when the lock was acquired.
	acquiredAt time.Time

	// Timestamp when the lock is set to expire automatically.
	expiresAt time.Time

	// Optional key-value metadata associated with the lock.
	metadata map[string]string

	// Timestamp of the last modification to the lock's state.
	lastModified time.Time
}

// lockSnapshot represents a point-in-time view of the lock manager state.
type lockSnapshot struct {
	// LastAppliedIndex is the Raft log index up to which all entries have been applied.
	LastAppliedIndex types.Index `json:"last_applied_index"`

	// Locks holds the current state of all active locks, indexed by LockID.
	Locks map[types.LockID]*lockState `json:"locks"`

	// Waiters holds the list of clients waiting for each lock, indexed by LockID.
	Waiters map[types.LockID][]*waiter `json:"waiters"`

	// Version is a monotonically increasing number used to detect stale snapshots.
	Version int `json:"version"`
}

// lockManager provides a concrete implementation of the LockManager interface.
// It manages the state of all locks, their waiters, and their expirations.
type lockManager struct {
	// Protects all shared state within the LockManager.
	mu sync.RWMutex

	cache LockCache

	locks          map[types.LockID]*lockState                  // Tracks the current state of each lock by its ID.
	waiters        map[types.LockID]*waitQueue                  // Maps lock IDs to their respective wait queues.
	clientLocks    map[types.ClientID]map[types.LockID]struct{} // Maps each client to the set of locks it currently holds.
	expirationHeap *expirationHeap                              // Min-heap for managing lock expirations.

	// Index of the last Raft log entry applied; used for state consistency.
	lastAppliedIndex types.Index

	config     LockManagerConfig
	serializer serializer
	clock      raft.Clock
	logger     logger.Logger
	metrics    Metrics

	// pendingWaits maps LockID and ClientID to a waiter struct.
	// This is crucial for managing client-side blocking calls that wait for lock acquisition.
	// The waiter.notifyCh is used to signal the client when a lock is acquired,
	// the wait is cancelled, or it times out.
	pendingWaits map[types.LockID]map[types.ClientID]*waiter
}

// NewLockManager creates a new instance of lockManager with the provided options
func NewLockManager(opts ...LockManagerOption) LockManager {
	config := DefaultLockManagerConfig()
	for _, opt := range opts {
		opt(&config)
	}

	if config.Serializer == nil {
		config.Serializer = &jsonSerializer{}
	}
	if config.Logger == nil {
		config.Logger = &logger.NoOpLogger{}
	}
	if config.Metrics == nil {
		config.Metrics = &NoOpMetrics{}
	}
	if config.Clock == nil {
		config.Clock = raft.NewStandardClock()
	}

	expHeap := make(expirationHeap, 0)
	heap.Init(&expHeap)

	lm := &lockManager{
		mu:               sync.RWMutex{},
		locks:            make(map[types.LockID]*lockState),
		waiters:          make(map[types.LockID]*waitQueue),
		clientLocks:      make(map[types.ClientID]map[types.LockID]struct{}),
		pendingWaits:     make(map[types.LockID]map[types.ClientID]*waiter),
		expirationHeap:   &expHeap,
		lastAppliedIndex: 0,
		config:           config,
		serializer:       config.Serializer,
		clock:            config.Clock,
		logger:           config.Logger.WithComponent("lockmanager"),
		metrics:          config.Metrics,
	}

	if config.EnableCache {
		cacheConfig := LockCacheConfig{
			Size:       config.CacheSize,
			DefaultTTL: config.CacheTTL,
			Clock:      lm.clock,
			Logger:     lm.logger.WithComponent("cache"),
			Metrics:    lm.metrics,
		}
		lm.cache = NewLockCache(cacheConfig)
		lm.logger.Infow("LockInfo cache enabled", "size", config.CacheSize, "ttl", config.CacheTTL)
	}

	lm.logger.Infow("LockManager initialized", "config", fmt.Sprintf("%+v", config))
	return lm
}

// Apply routes a committed Raft command to the appropriate lock operation
func (lm *lockManager) Apply(ctx context.Context, index types.Index, cmdData []byte) error {
	if len(cmdData) == 0 {
		lm.logger.Errorw("Apply received an empty command", "index", index)
		return errors.New("empty command")
	}

	cmd, err := lm.config.Serializer.DecodeCommand(cmdData)
	if err != nil {
		lm.logger.Errorw("Failed to decode command", "index", index, "error", err, "commandData", string(cmdData))
		return fmt.Errorf("failed to decode command: %w", err)
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	if index <= lm.lastAppliedIndex { // Idempotency
		lm.logger.Debugw("Skipping already applied command", "commandIndex", index, "lastAppliedIndex", lm.lastAppliedIndex, "operation", cmd.Op)
		return nil
	}

	startTime := lm.clock.Now()
	var opErr error

	switch cmd.Op {
	case types.OperationAcquire:
		_, opErr = lm.applyAcquireLocked(cmd.LockID, cmd.ClientID, time.Duration(cmd.TTL)*time.Millisecond, index)
	case types.OperationRelease:
		_, opErr = lm.applyReleaseLocked(cmd.LockID, cmd.ClientID, cmd.Version)
	case types.OperationRenew:
		opErr = lm.applyRenewLocked(cmd.LockID, cmd.ClientID, cmd.Version, time.Duration(cmd.TTL)*time.Millisecond)
	case types.OperationEnqueueWaiter:
		_, opErr = lm.applyWaitQueueLocked(cmd.LockID, cmd.ClientID, time.Duration(cmd.Timeout)*time.Millisecond, cmd.Version, cmd.Priority)
	case types.OperationCancelWait:
		_, opErr = lm.applyCancelWaitLocked(cmd.LockID, cmd.ClientID, cmd.Version)
	default:
		opErr = fmt.Errorf("unknown operation type: %s", cmd.Op)
	}

	lm.metrics.IncrRaftApply(cmd.Op, opErr == nil)
	latency := lm.clock.Now().Sub(startTime)

	if opErr != nil {
		lm.logger.Errorw("Failed to apply command",
			"index", index, "operation", cmd.Op, "lockID", cmd.LockID, "clientID", cmd.ClientID, "error", opErr, "latency", latency)
	} else {
		lm.lastAppliedIndex = index
		lm.logger.Debugw("Successfully applied command",
			"index", index, "operation", cmd.Op, "lockID", cmd.LockID, "clientID", cmd.ClientID, "latency", latency)

		if lm.config.EnableCache && lm.cache != nil {
			lm.cache.Invalidate(cmd.LockID)
		}
	}

	return opErr
}

// ApplyAcquire attempts to acquire a lock for the specified client
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) ApplyAcquire(ctx context.Context, lockID types.LockID, clientID types.ClientID, ttl time.Duration, version types.Index) (*types.LockInfo, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.applyAcquireLocked(lockID, clientID, ttl, version)
}

// applyAcquireLocked attempts to acquire a lock for the specified client.
// This method must be called with the lockManager's internal mutex held.
func (lm *lockManager) applyAcquireLocked(lockID types.LockID, clientID types.ClientID, ttl time.Duration, version types.Index) (*types.LockInfo, error) {
	startTime := lm.clock.Now()
	var (
		success   bool
		contested bool
	)

	defer func() {
		lm.metrics.IncrAcquireRequest(lockID, success, false)
		lm.metrics.ObserveAcquireLatency(lockID, lm.clock.Now().Sub(startTime), contested)
	}()

	if err := lm.validateTTL(ttl); err != nil {
		return nil, err
	}

	now := lm.clock.Now()
	currentLock, exists := lm.locks[lockID]

	switch {
	case lm.isHeldByOther(currentLock, clientID):
		contested = true
		return lm.rejectAcquisitionDueToContention(lockID, currentLock.owner, clientID)

	case lm.isHeldBySame(currentLock, clientID):
		success = true
		return lm.renewLock(lockID, currentLock, ttl, version, now)

	case !exists:
		currentLock = lm.createNewLockState(lockID)
		lm.locks[lockID] = currentLock
	}

	success = true
	return lm.assignLockOwnership(lockID, currentLock, clientID, ttl, version, now)
}

// isHeldByOther returns true if the lock exists and is currently owned by a different client.
func (lm *lockManager) isHeldByOther(lock *lockState, clientID types.ClientID) bool {
	return lock != nil && lock.owner != "" && lock.owner != clientID
}

// isHeldBySame returns true if the lock exists and is currently held by the same client.
func (lm *lockManager) isHeldBySame(lock *lockState, clientID types.ClientID) bool {
	return lock != nil && lock.owner == clientID
}

// rejectAcquisitionDueToContention logs and returns a standardized error
// when a lock is already held by a different client.
func (lm *lockManager) rejectAcquisitionDueToContention(
	lockID types.LockID,
	currentOwner types.ClientID,
	requestingClient types.ClientID,
) (*types.LockInfo, error) {
	lm.logger.Infow("Lock acquisition failed - already held",
		"lockID", lockID,
		"currentOwner", currentOwner,
		"requestingClient", requestingClient,
	)
	return nil, ErrLockHeld
}

// renewLock extends the expiration of a lock currently held by the requesting client.
func (lm *lockManager) renewLock(
	lockID types.LockID,
	lock *lockState,
	ttl time.Duration,
	version types.Index,
	now time.Time,
) (*types.LockInfo, error) {
	newExpiresAt := now.Add(ttl)
	if newExpiresAt.After(lock.expiresAt) {
		lock.expiresAt = newExpiresAt
	}
	lock.version = version
	lock.lastModified = now

	lm.updateExpirationLocked(lockID, lock.expiresAt)

	lm.logger.Debugw("Lock renewed by current owner",
		"lockID", lockID,
		"clientID", lock.owner,
		"ttl", ttl,
		"version", version,
		"expiresAt", lock.expiresAt,
	)
	return lm.createLockInfoFromState(lock), nil
}

// createNewLockState initializes a new, empty lockState for a previously unseen lock.
func (lm *lockManager) createNewLockState(lockID types.LockID) *lockState {
	lm.logger.Infow("Created new lock state",
		"lockID", lockID,
	)
	return &lockState{
		lockID:   lockID,
		metadata: make(map[string]string),
	}
}

// assignLockOwnership sets the client as the owner of the lock and initializes
// all related metadata including TTL, version, and timestamps.
func (lm *lockManager) assignLockOwnership(
	lockID types.LockID,
	lock *lockState,
	clientID types.ClientID,
	ttl time.Duration,
	version types.Index,
	now time.Time,
) (*types.LockInfo, error) {
	lock.owner = clientID
	lock.version = version
	lock.acquiredAt = now
	lock.expiresAt = now.Add(ttl)
	lock.lastModified = now

	lm.addToExpirationHeapLocked(lockID, lock.expiresAt)
	lm.trackClientLockLocked(clientID, lockID)
	lm.metrics.SetActiveLocks(len(lm.locks))

	lm.logger.Infow("Lock acquired successfully",
		"lockID", lockID,
		"clientID", clientID,
		"ttl", ttl,
		"version", version,
		"expiresAt", lock.expiresAt,
	)
	return lm.createLockInfoFromState(lock), nil
}

// ApplyRelease releases a lock held by the specified client and version
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) ApplyRelease(ctx context.Context, lockID types.LockID, clientID types.ClientID, version types.Index) (bool, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.applyReleaseLocked(lockID, clientID, version)
}

// applyReleaseLocked releases a lock held by the specified client.
// This method must be invoked with the lockManager's internal mutex held.
func (lm *lockManager) applyReleaseLocked(lockID types.LockID, clientID types.ClientID, version types.Index) (bool, error) {
	startTime := lm.clock.Now()
	var success bool

	defer func() {
		lm.metrics.IncrReleaseRequest(lockID, success, false) // false = not expired
		lm.metrics.ObserveReleaseLatency(lockID, lm.clock.Now().Sub(startTime))
	}()

	lock, err := lm.validateRelease(lockID, clientID, version)
	if err != nil {
		return false, err
	}

	lm.performRelease(lockID, clientID, lock)

	lm.logger.Infow("Lock released",
		"lockID", lockID,
		"clientID", clientID,
		"version", version,
		"heldDuration", lm.clock.Now().Sub(lock.acquiredAt),
	)

	lm.tryPromoteWaiterLocked(lockID)

	success = true
	return true, nil
}

// validateRelease checks if the release request is valid and returns the lock if so.
func (lm *lockManager) validateRelease(lockID types.LockID, clientID types.ClientID, version types.Index) (*lockState, error) {
	lock, exists := lm.locks[lockID]
	if !exists || lock.owner == "" {
		lm.logger.Warnw("Release failed: lock not found or not held",
			"lockID", lockID, "clientID", clientID)
		return nil, ErrLockNotHeld
	}

	if lock.owner != clientID {
		lm.logger.Warnw("Release failed: client does not own lock",
			"lockID", lockID, "currentOwner", lock.owner, "clientID", clientID)
		return nil, ErrNotLockOwner
	}

	if version != 0 && lock.version != version {
		lm.logger.Warnw("Release failed: version mismatch",
			"lockID", lockID, "lockVersion", lock.version, "clientVersion", version)
		return nil, ErrVersionMismatch
	}

	return lock, nil
}

// performRelease clears the lock owner and updates internal state and metrics.
func (lm *lockManager) performRelease(lockID types.LockID, clientID types.ClientID, lock *lockState) {
	now := lm.clock.Now()
	lock.owner = ""
	lock.lastModified = now

	holdDuration := now.Sub(lock.acquiredAt)
	lm.metrics.ObserveLockHoldDuration(lockID, holdDuration, true)

	lm.removeFromClientLocksLocked(clientID, lockID)
	lm.metrics.SetActiveLocks(len(lm.locks))
}

// ApplyRenew extends the TTL of a held lock if the client and version match
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) ApplyRenew(ctx context.Context, lockID types.LockID, clientID types.ClientID, version types.Index, ttl time.Duration) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.applyRenewLocked(lockID, clientID, version, ttl)
}

// applyRenewLocked attempts to renew the TTL of a lock currently held by the specified client.
// This method must be called with the lockManager's internal mutex held.
func (lm *lockManager) applyRenewLocked(lockID types.LockID, clientID types.ClientID, commandVersion types.Index, ttl time.Duration) error {
	startTime := lm.clock.Now()
	var success bool

	defer func() {
		lm.metrics.IncrRenewRequest(lockID, success)
		lm.metrics.ObserveRenewLatency(lockID, lm.clock.Now().Sub(startTime))
	}()

	if err := lm.validateTTL(ttl); err != nil {
		return err
	}

	lock, err := lm.validateRenewRequest(lockID, clientID)
	if err != nil {
		return err
	}

	lm.renewLockTTL(lockID, lock, commandVersion, ttl)

	lm.logger.Infow("Lock renewed successfully",
		"lockID", lockID,
		"clientID", clientID,
		"newTTL", ttl,
		"newVersion", lock.version,
		"expiresAt", lock.expiresAt,
	)
	success = true
	return nil
}

// validateRenewRequest ensures the lock exists and is currently held by the requesting client.
func (lm *lockManager) validateRenewRequest(lockID types.LockID, clientID types.ClientID) (*lockState, error) {
	lock, exists := lm.locks[lockID]
	if !exists || lock.owner == "" {
		lm.logger.Warnw("Renew failed: lock not found or not currently held",
			"lockID", lockID, "clientID", clientID)
		return nil, ErrLockNotHeld
	}
	if lock.owner != clientID {
		lm.logger.Warnw("Renew failed: client is not the lock owner",
			"lockID", lockID, "currentOwner", lock.owner, "clientID", clientID)
		return nil, ErrNotLockOwner
	}
	return lock, nil
}

// renewLockTTL updates the TTL and version of the lock.
func (lm *lockManager) renewLockTTL(lockID types.LockID, lock *lockState, newVersion types.Index, ttl time.Duration) {
	now := lm.clock.Now()
	lock.expiresAt = now.Add(ttl)
	lock.version = newVersion
	lock.lastModified = now

	lm.updateExpirationLocked(lockID, lock.expiresAt)
}

// ApplyWaitQueue enqueues a client into the lock's wait queue, optionally with priority
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) ApplyWaitQueue(ctx context.Context, lockID types.LockID, clientID types.ClientID, timeout time.Duration, version types.Index, priority int) (int, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.applyWaitQueueLocked(lockID, clientID, timeout, version, priority)
}

// applyWaitQueueLocked enqueues a client into the wait queue for the specified lock.
// This method must be called with the lockManager's internal mutex held.
func (lm *lockManager) applyWaitQueueLocked(lockID types.LockID, clientID types.ClientID, timeout time.Duration, version types.Index, priority int) (int, error) {
	start := lm.clock.Now()
	var success bool

	defer func() {
		lm.metrics.IncrWaitQueueRequest(lockID, success)
		lm.metrics.ObserveWaitQueueLatency(lockID, lm.clock.Now().Sub(start))
	}()

	if err := lm.validateWaitTimeout(timeout); err != nil {
		return 0, err
	}

	lm.ensureLockStateExists(lockID)
	wq := lm.ensureWaitQueueExists(lockID)

	if wq.Len() >= lm.config.MaxWaiters {
		lm.logger.Warnw("Enqueue waiter failed: wait queue full",
			"lockID", lockID, "clientID", clientID,
			"queueSize", wq.Len(), "maxWaiters", lm.config.MaxWaiters)
		return 0, ErrWaitQueueFull
	}

	if index, updated := lm.updateExistingWaiterIfPresent(lockID, clientID, timeout, priority); updated {
		success = true
		return index, nil
	}

	newWaiter := lm.enqueueNewWaiter(lockID, clientID, timeout, version, priority, wq)

	lm.logger.Infow("Client enqueued to wait for lock",
		"lockID", lockID, "clientID", clientID,
		"priority", priority, "timeout", timeout, "queuePosition", newWaiter.index)

	success = true
	return newWaiter.index, nil
}

// validateWaitTimeout checks whether the provided timeout duration is within acceptable bounds.
// Returns ErrInvalidTimeout if the value is <= 0 or exceeds MaxWaitQueueTimeout.
func (lm *lockManager) validateWaitTimeout(timeout time.Duration) error {
	if timeout <= 0 || timeout > MaxWaitQueueTimeout {
		return ErrInvalidTimeout
	}
	return nil
}

// ensureLockStateExists guarantees that a lockState exists for the given lockID.
// If no lockState is found, a new one is created and initialized.
func (lm *lockManager) ensureLockStateExists(lockID types.LockID) {
	_, exists := lm.locks[lockID]
	if !exists {
		lock := &lockState{
			lockID:       lockID,
			metadata:     make(map[string]string),
			lastModified: lm.clock.Now(),
		}
		lm.locks[lockID] = lock
		lm.logger.Infow("Created lock state for waiting client", "lockID", lockID)
	}
}

// ensureWaitQueueExists guarantees that a waitQueue exists for the given lockID.
// If not present, it initializes a new heap-backed queue and returns a pointer to it.
func (lm *lockManager) ensureWaitQueueExists(lockID types.LockID) *waitQueue {
	wq, exists := lm.waiters[lockID]
	if !exists {
		q := make(waitQueue, 0)
		wq = &q
		heap.Init(wq)
		lm.waiters[lockID] = wq
	}
	return wq
}

// updateExistingWaiterIfPresent checks if the client is already waiting on the lock.
// If so, it updates the waiter's timeout and priority, re-heaps if needed, and returns the index.
// Returns (index, true) if updated, otherwise (0, false) if no existing waiter is found.
func (lm *lockManager) updateExistingWaiterIfPresent(
	lockID types.LockID,
	clientID types.ClientID,
	timeout time.Duration,
	priority int,
) (int, bool) {
	if pwMap, exists := lm.pendingWaits[lockID]; exists {
		if w, found := pwMap[clientID]; found {
			w.timeoutAt = lm.clock.Now().Add(timeout)
			w.priority = priority
			if lm.config.EnablePriorityQueue {
				heap.Fix(lm.waiters[lockID], w.index)
			}
			lm.logger.Infow("Updated existing waiter",
				"lockID", lockID, "clientID", clientID,
				"timeout", timeout, "priority", priority, "queuePosition", w.index)
			return w.index, true
		}
	}
	return 0, false
}

// enqueueNewWaiter adds a new waiter to the lock's wait queue and tracks it in pendingWaits.
// It initializes the waiter, pushes it onto the heap, and updates metrics accordingly.
func (lm *lockManager) enqueueNewWaiter(
	lockID types.LockID,
	clientID types.ClientID,
	timeout time.Duration,
	version types.Index,
	priority int,
	wq *waitQueue,
) *waiter {
	now := lm.clock.Now()
	w := &waiter{
		clientID:  clientID,
		enqueued:  now,
		timeoutAt: now.Add(timeout),
		priority:  priority,
		version:   version,
		notifyCh:  make(chan struct{}, 1),
	}
	heap.Push(wq, w)

	if _, ok := lm.pendingWaits[lockID]; !ok {
		lm.pendingWaits[lockID] = make(map[types.ClientID]*waiter)
	}
	lm.pendingWaits[lockID][clientID] = w

	lm.metrics.ObserveWaitQueueSize(lockID, wq.Len())
	lm.updateTotalWaitersMetric()
	return w
}

// ApplyCancelWait removes a client from a lock's wait queue
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) ApplyCancelWait(ctx context.Context, lockID types.LockID, clientID types.ClientID, version types.Index) (bool, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.applyCancelWaitLocked(lockID, clientID, version)
}

// applyCancelWaitLocked removes a client from the wait queue of the specified lock.
// This method must be called with the lockManager's internal mutex held.
func (lm *lockManager) applyCancelWaitLocked(lockID types.LockID, clientID types.ClientID, version types.Index) (bool, error) {
	success := false
	defer func() {
		lm.metrics.IncrWaitCancelRequest(lockID, success)
	}()

	waiterIndex := lm.removePendingWaiter(lockID, clientID)

	wq, exists := lm.waiters[lockID]
	if !exists {
		lm.logger.Infow("Cancel wait failed: no wait queue exists",
			"lockID", lockID, "clientID", clientID)
		return false, ErrNotWaiting
	}

	removed := lm.removeWaiterFromHeap(wq, clientID, waiterIndex)
	if !removed {
		lm.logger.Infow("Cancel wait failed: client not found in wait queue",
			"lockID", lockID, "clientID", clientID)
		return false, ErrNotWaiting
	}

	if wq.Len() == 0 {
		delete(lm.waiters, lockID)
	}

	lm.metrics.ObserveWaitQueueSize(lockID, wq.Len())
	lm.updateTotalWaitersMetric()

	lm.logger.Infow("Client successfully cancelled wait",
		"lockID", lockID, "clientID", clientID)
	success = true
	return true, nil
}

// removePendingWaiter removes the client from pendingWaits and closes their notify channel.
// Returns the waiter's index in the heap if known, otherwise -1.
func (lm *lockManager) removePendingWaiter(lockID types.LockID, clientID types.ClientID) int {
	if pwMap, exists := lm.pendingWaits[lockID]; exists {
		if waiter, found := pwMap[clientID]; found {
			if waiter.notifyCh != nil {
				close(waiter.notifyCh)
			}
			delete(pwMap, clientID)
			if len(pwMap) == 0 {
				delete(lm.pendingWaits, lockID)
			}
			lm.logger.Debugw("Removed waiter from pendingWaits",
				"lockID", lockID, "clientID", clientID)
			return waiter.index
		}
	}
	return -1
}

// removeWaiterFromHeap removes a client from the wait queue heap.
// If the waiter's index is known and valid, it is used; otherwise a linear search is performed.
// Returns true if the waiter was successfully removed.
func (lm *lockManager) removeWaiterFromHeap(wq *waitQueue, clientID types.ClientID, knownIndex int) bool {
	if knownIndex >= 0 && knownIndex < wq.Len() { // Fast path: known and correct index.
		if (*wq)[knownIndex].clientID == clientID {
			heap.Remove(wq, knownIndex)
			return true
		}
	}

	for i, w := range *wq { // Fallback: linear search.
		if w.clientID == clientID {
			if w.notifyCh != nil {
				select {
				case <-w.notifyCh: // Already closed
				default:
					close(w.notifyCh)
				}
			}
			heap.Remove(wq, i)
			return true
		}
	}
	return false
}

// GetLockInfo retrieves the current state of a lock
// If limit <= 0, all items from offset are returned.
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) GetLockInfo(ctx context.Context, lockID types.LockID) (*types.LockInfo, error) {
	if lm.config.EnableCache && lm.cache != nil {
		if info, found := lm.cache.Get(lockID); found {
			lm.metrics.IncrCacheHit(lockID)
			return info, nil
		}
		lm.metrics.IncrCacheMiss(lockID)
	}

	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lockState, exists := lm.locks[lockID]
	if !exists {
		return nil, ErrLockNotFound
	}

	info := lm.createLockInfoFromState(lockState)

	if lm.config.EnableCache && lm.cache != nil {
		lm.cache.Add(lockID, info)
	}
	return info, nil
}

// GetLocks returns a paginated list of locks matching an optional filter
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) GetLocks(ctx context.Context, filter LockFilter, limit int, offset int) ([]*types.LockInfo, int, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	allMatches := lm.filterLocks(filter)
	paged := lm.paginateLocks(allMatches, limit, offset)

	return paged, len(allMatches), nil
}

// filterLocks returns a slice of LockInfo objects that match the given filter.
func (lm *lockManager) filterLocks(filter LockFilter) []*types.LockInfo {
	estimated := len(lm.locks)
	matches := make([]*types.LockInfo, 0, estimated)

	for _, state := range lm.locks {
		info := lm.createLockInfoFromState(state)
		if filter(info) {
			matches = append(matches, info)
		}
	}

	return matches
}

// paginateLocks returns a sub-slice of lock info based on the provided limit and offset.
// If limit <= 0, all items from offset are returned.
// If offset is beyond the slice length, an empty slice is returned.
func (lm *lockManager) paginateLocks(locks []*types.LockInfo, limit int, offset int) []*types.LockInfo {
	total := len(locks)

	if offset >= total {
		return []*types.LockInfo{}
	}

	if limit <= 0 || offset+limit > total {
		return locks[offset:]
	}

	return locks[offset : offset+limit]
}

// Tick advances the internal clock to trigger TTL expirations and wait queue promotions
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) Tick(ctx context.Context) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	start := lm.clock.Now()
	now := start
	expiredCount := lm.processExpiredLocks(now)
	lm.cleanupTimedOutWaiters(now)
	lm.updateTotalWaitersMetric()
	lm.updateActiveLockMetrics()
	lm.metrics.ObserveTickDuration(lm.clock.Now().Sub(start), expiredCount)

	if lm.config.EnableCache && lm.cache != nil {
		removed := lm.cache.Cleanup()
		lm.logger.Debugw("Cache cleanup during tick", "removedExpired", removed)
	}

	return expiredCount
}

// expireLocks handles TTL-based expiration of held locks.
func (lm *lockManager) processExpiredLocks(now time.Time) int {
	count := 0
	for lm.expirationHeap.Len() > 0 && (*lm.expirationHeap)[0].expiresAt.Before(now) {
		item := heap.Pop(lm.expirationHeap).(*expirationItem)
		lock, exists := lm.locks[item.lockID]

		if !exists || lock.owner == "" || lock.expiresAt.After(now) {
			continue // Lock was deleted, not held, or renewed since scheduled expiration.
		}

		lm.logger.Infow("Lock expired by TTL",
			"lockID", lock.lockID, "owner", lock.owner, "expiresAt", lock.expiresAt)

		holdDuration := now.Sub(lock.acquiredAt)
		lm.metrics.ObserveLockHoldDuration(lock.lockID, holdDuration, false)
		lm.metrics.IncrExpiredLock(lock.lockID)

		lm.removeFromClientLocksLocked(lock.owner, lock.lockID)
		lock.owner = ""
		lock.lastModified = now

		lm.tryPromoteWaiterLocked(lock.lockID)
		count++
	}
	return count
}

// cleanupTimedOutWaiters removes waiters whose timeout has expired.
func (lm *lockManager) cleanupTimedOutWaiters(now time.Time) {
	for lockID, wq := range lm.waiters {
		i := 0
		for i < wq.Len() {
			waiter := (*wq)[i]
			if now.After(waiter.timeoutAt) {
				lm.logger.Infow("Waiter timed out", "lockID", lockID, "clientID", waiter.clientID, "timeoutAt", waiter.timeoutAt)
				lm.metrics.IncrTimeoutWaiter(lockID)

				lm.cleanupPendingWaiter(lockID, waiter)
				heap.Remove(wq, i)
			} else {
				i++
			}
		}

		if wq.Len() == 0 {
			delete(lm.waiters, lockID)
		}

		lm.metrics.ObserveWaitQueueSize(lockID, wq.Len())
	}
}

// updateActiveLockMetrics updates the count of actively held locks.
func (lm *lockManager) updateActiveLockMetrics() {
	active := 0
	for _, lock := range lm.locks {
		if lock.owner != "" {
			active++
		}
	}
	lm.metrics.SetActiveLocks(active)
}

// Snapshot serializes the LockManager's state for Raft snapshotting
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) Snapshot(ctx context.Context) (types.Index, []byte, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	start := lm.clock.Now()
	var success bool

	defer func() {
		lm.metrics.IncrSnapshotEvent(types.SnapshotCreate, success)
		lm.metrics.ObserveSnapshotDuration(types.SnapshotCreate, lm.clock.Now().Sub(start))
	}()

	snapshot := lockSnapshot{
		LastAppliedIndex: lm.lastAppliedIndex,
		Locks:            lm.locks,
		Waiters:          lm.snapshotWaiters(),
		Version:          1,
	}

	data, err := lm.serializer.EncodeSnapshot(snapshot)
	if err != nil {
		lm.logger.Errorw("Failed to encode snapshot", "error", err)
		return 0, nil, fmt.Errorf("snapshot encoding failed: %w", err)
	}

	lm.metrics.ObserveSnapshotSize(len(data))
	lm.logger.Infow("Snapshot created",
		"lastAppliedIndex", lm.lastAppliedIndex,
		"sizeBytes", len(data))
	success = true
	return lm.lastAppliedIndex, data, nil
}

// snapshotWaiters converts all waitQueue heaps into stable slices for serialization.
func (lm *lockManager) snapshotWaiters() map[types.LockID][]*waiter {
	result := make(map[types.LockID][]*waiter, len(lm.waiters))
	for lockID, wq := range lm.waiters {
		slice := make([]*waiter, wq.Len())
		for i := range wq.Len() {
			slice[i] = (*wq)[i]
		}
		result[lockID] = slice
	}
	return result
}

// RestoreSnapshot loads a LockManager state from a Raft snapshot
// Note: ctx is currently unused but included for future support.
func (lm *lockManager) RestoreSnapshot(ctx context.Context, lastIndex types.Index, lastTerm types.Term, data []byte) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	start := lm.clock.Now()
	var success bool

	defer func() {
		lm.metrics.IncrSnapshotEvent(types.SnapshotRestore, success)
		lm.metrics.ObserveSnapshotDuration(types.SnapshotRestore, lm.clock.Now().Sub(start))
	}()

	if lastIndex < lm.lastAppliedIndex {
		lm.logger.Warnw("Ignored stale snapshot", "snapshotIndex", lastIndex, "currentLastApplied", lm.lastAppliedIndex)
		return nil
	}

	var snapshot lockSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		lm.logger.Errorw("Failed to decode snapshot", "error", err)
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	if snapshot.Version != 1 {
		lm.logger.Errorw("Unsupported snapshot version", "version", snapshot.Version)
		return fmt.Errorf("unsupported snapshot version: %d", snapshot.Version)
	}

	lm.restoreLocksUnlocked(snapshot.Locks, snapshot.LastAppliedIndex)
	lm.restoreWaitersUnlocked(snapshot.Waiters)
	lm.rebuildExpirationHeapUnlocked()
	lm.rebuildClientLocksUnlocked()
	lm.resetPendingWaitsUnlocked()
	lm.invalidateCacheIfEnabled()
	lm.updateActiveLockMetrics()
	lm.updateTotalWaitersMetric()

	lm.logger.Infow("Snapshot restore completed",
		"restoredIndex", lm.lastAppliedIndex,
		"lockCount", len(lm.locks))

	success = true
	return nil
}

// restoreLocksUnlocked replaces the current lock map with the snapshot's lock state
// and updates the last applied index for Raft consistency.
// This method must be called with the lockManager's internal mutex held.
func (lm *lockManager) restoreLocksUnlocked(locks map[types.LockID]*lockState, lastIndex types.Index) {
	lm.locks = locks
	lm.lastAppliedIndex = lastIndex
}

// restoreWaitersUnlocked rebuilds wait queues from the serialized slice format used in snapshots.
// It reconstructs each heap and restores required runtime fields like notifyCh.
func (lm *lockManager) restoreWaitersUnlocked(snapshotWaiters map[types.LockID][]*waiter) {
	lm.waiters = make(map[types.LockID]*waitQueue)
	for lockID, waiters := range snapshotWaiters {
		if len(waiters) == 0 {
			continue
		}
		q := make(waitQueue, len(waiters))
		copy(q, waiters)
		for i := range q {
			q[i].index = i
			q[i].notifyCh = make(chan struct{}, 1)
		}
		lm.waiters[lockID] = &q
		heap.Init(lm.waiters[lockID])
	}
}

// rebuildExpirationHeapUnlocked reconstructs the expiration priority queue using the restored locks.
// Only active locks (with owners) are scheduled for expiration tracking.
func (lm *lockManager) rebuildExpirationHeapUnlocked() {
	expHeap := make(expirationHeap, 0, len(lm.locks))
	for lockID, lock := range lm.locks {
		if lock.owner != "" {
			expHeap = append(expHeap, &expirationItem{
				lockID:    lockID,
				expiresAt: lock.expiresAt,
				index:     -1,
			})
		}
	}
	lm.expirationHeap = &expHeap
	heap.Init(lm.expirationHeap)
}

// rebuildClientLocksUnlocked reconstructs the mapping of clients to the locks they currently hold.
// This is used for lock cleanup and client ownership tracking.
func (lm *lockManager) rebuildClientLocksUnlocked() {
	lm.clientLocks = make(map[types.ClientID]map[types.LockID]struct{})
	for lockID, lock := range lm.locks {
		if lock.owner != "" {
			lm.trackClientLockLocked(lock.owner, lockID)
		}
	}
}

// resetPendingWaitsUnlocked clears any live waiter references.
//
// Note: pendingWaits cannot be restored from a snapshot because notifyCh channels
// are not serializable. As a result, any clients waiting on a lock at the time
// of the snapshot will need to reissue their requests after restore.
func (lm *lockManager) resetPendingWaitsUnlocked() {
	lm.pendingWaits = make(map[types.LockID]map[types.ClientID]*waiter)
}

// invalidateCacheIfEnabled clears all lock cache entries
// after a snapshot restore to ensure consistency.
func (lm *lockManager) invalidateCacheIfEnabled() {
	if lm.config.EnableCache && lm.cache != nil {
		lm.cache.InvalidateAll()
	}
}

// Close shuts down background routines and cleans up resources
func (lm *lockManager) Close() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.logger.Infow("Closing LockManager...")

	// To help GC
	lm.locks = make(map[types.LockID]*lockState)
	lm.waiters = make(map[types.LockID]*waitQueue)
	lm.clientLocks = make(map[types.ClientID]map[types.LockID]struct{})
	lm.pendingWaits = make(map[types.LockID]map[types.ClientID]*waiter)

	emptyExpHeap := make(expirationHeap, 0)
	lm.expirationHeap = &emptyExpHeap

	if lm.config.EnableCache && lm.cache != nil {
		lm.cache.InvalidateAll()
	}

	lm.logger.Infow("LockManager closed and resources released.")
	return nil
}

// validateTTL ensures the provided TTL is within configured bounds.
func (lm *lockManager) validateTTL(ttl time.Duration) error {
	if ttl < MinLockTTL || ttl > lm.config.MaxTTL {
		lm.logger.Warnw("Invalid TTL specified for lock operation", "ttl", ttl, "minAllowed", MinLockTTL, "maxAllowed", lm.config.MaxTTL)
		return ErrInvalidTTL
	}
	return nil
}

// createLockInfoFromState converts internal lockState to public LockInfo.
// Assumes caller holds lm.mu (read or write).
func (lm *lockManager) createLockInfoFromState(state *lockState) *types.LockInfo {
	info := &types.LockInfo{
		LockID:       state.lockID,
		OwnerID:      state.owner,
		Version:      state.version,
		AcquiredAt:   state.acquiredAt,
		ExpiresAt:    state.expiresAt,
		LastModified: state.lastModified,
		Metadata:     make(map[string]string, len(state.metadata)),
	}
	for k, v := range state.metadata { // Deep copy metadata.
		info.Metadata[k] = v
	}

	if wq, exists := lm.waiters[state.lockID]; exists {
		info.WaiterCount = wq.Len()
	}
	return info
}

// addToExpirationHeapLocked adds a lock to the expiration heap.
// Assumes caller holds lm.mu.
func (lm *lockManager) addToExpirationHeapLocked(lockID types.LockID, expiresAt time.Time) {
	heap.Push(lm.expirationHeap, &expirationItem{lockID: lockID, expiresAt: expiresAt}) // no need to set index (heap.Interface)
}

// updateExpirationLocked updates a lock's expiration time in the heap.
// Assumes caller holds lm.mu.
func (lm *lockManager) updateExpirationLocked(lockID types.LockID, newExpiresAt time.Time) {
	for i, item := range *lm.expirationHeap {
		if item.lockID == lockID {
			item.expiresAt = newExpiresAt
			heap.Fix(lm.expirationHeap, i)
			return
		}
	}

	lm.addToExpirationHeapLocked(lockID, newExpiresAt)
}

// trackClientLockLocked records that a client holds a particular lock.
// Assumes caller holds lm.mu.
func (lm *lockManager) trackClientLockLocked(clientID types.ClientID, lockID types.LockID) {
	if _, exists := lm.clientLocks[clientID]; !exists {
		lm.clientLocks[clientID] = make(map[types.LockID]struct{})
	}
	lm.clientLocks[clientID][lockID] = struct{}{}
}

// removeFromClientLocksLocked stops tracking a lock for a client.
// Assumes caller holds lm.mu.
func (lm *lockManager) removeFromClientLocksLocked(clientID types.ClientID, lockID types.LockID) {
	if clientLocks, exists := lm.clientLocks[clientID]; exists {
		delete(clientLocks, lockID)
		if len(clientLocks) == 0 {
			delete(lm.clientLocks, clientID)
		}
	}
}

// tryPromoteWaiterLocked promotes the highest-priority waiter to lock ownership.
// Caller must hold lm.mu.
func (lm *lockManager) tryPromoteWaiterLocked(lockID types.LockID) {
	wq, hasWaiters := lm.waiters[lockID]
	if !hasWaiters || wq.Len() == 0 {
		return
	}

	lock, exists := lm.locks[lockID]
	if !exists || lock.owner != "" {
		return
	}

	nextWaiter := heap.Pop(wq).(*waiter)

	lm.cleanupPendingWaiter(lockID, nextWaiter)
	lm.notifyWaiter(nextWaiter, lockID)
	lm.grantLockToWaiter(lockID, lock, nextWaiter)

	if wq.Len() == 0 {
		delete(lm.waiters, lockID)
	}
	lm.metrics.ObserveWaitQueueSize(lockID, wq.Len())
	lm.updateTotalWaitersMetric()
	lm.logger.Infow("Promoted waiter to lock owner",
		"lockID", lockID,
		"promotedClient", nextWaiter.clientID,
		"newVersion", lock.version,
		"newExpiresAt", lock.expiresAt)
}

// cleanupPendingWaiter removes the promoted waiter from pendingWaits.
func (lm *lockManager) cleanupPendingWaiter(lockID types.LockID, w *waiter) {
	if pendingClients, ok := lm.pendingWaits[lockID]; ok {
		delete(pendingClients, w.clientID)
		if len(pendingClients) == 0 {
			delete(lm.pendingWaits, lockID)
		}
	}
}

// notifyWaiter closes the notification channel to unblock the client.
func (lm *lockManager) notifyWaiter(w *waiter, lockID types.LockID) {
	if w.notifyCh != nil {
		close(w.notifyCh)
	}
	waitDuration := lm.clock.Now().Sub(w.enqueued)
	lm.metrics.IncrPromotedWaiter(lockID, waitDuration)
}

// grantLockToWaiter assigns the lock to the given waiter and updates state.
func (lm *lockManager) grantLockToWaiter(lockID types.LockID, lock *lockState, w *waiter) {
	now := lm.clock.Now()
	lock.owner = w.clientID
	lock.version = lm.lastAppliedIndex
	lock.acquiredAt = now
	lock.expiresAt = now.Add(lm.config.DefaultTTL)
	lock.lastModified = now

	lm.trackClientLockLocked(w.clientID, lockID)
	lm.addToExpirationHeapLocked(lockID, lock.expiresAt)
}

// updateTotalWaitersMetric recalculates and sets the gauge for the total number of waiting clients.
// Assumes caller holds lm.mu.
func (lm *lockManager) updateTotalWaitersMetric() {
	totalWaiters := 0
	for _, wq := range lm.waiters {
		totalWaiters += wq.Len()
	}
	lm.metrics.SetTotalWaiters(totalWaiters)
}
