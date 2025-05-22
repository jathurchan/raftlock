package lock

import (
	"context"
	"time"

	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

// LockManager defines a distributed lock management interface,
// serving as the application-level state machine for Raft consensus.
//
// It extends the raft.Applier interface to handle lock-specific operations
// after commands are committed through the Raft consensus protocol.
//
// Notes:
//   - All methods must be deterministic, thread-safe, and idempotent.
//   - Clients waiting in the lock queue at the time of a snapshot cannot be restored.
//     After a snapshot restore, clients must re-issue their wait requests to resume.
type LockManager interface {
	raft.Applier

	// ApplyAcquire attempts to acquire a lock for the specified client.
	//
	// Returns:
	//   - *LockInfo if the lock is granted.
	//   - ErrLockHeld if the lock is already taken.
	//   - ErrInvalidTTL, ErrResourceNotFound, or other fatal errors.
	ApplyAcquire(ctx context.Context, lockID types.LockID, clientID types.ClientID, ttl time.Duration, version types.Index) (*types.LockInfo, error)

	// ApplyRelease releases a lock held by the specified client and version.
	//
	// Returns:
	//   - true if the release succeeds.
	//   - ErrLockNotHeld, ErrNotLockOwner, ErrVersionMismatch, or fatal errors.
	ApplyRelease(ctx context.Context, lockID types.LockID, clientID types.ClientID, version types.Index) (bool, error)

	// ApplyRenew extends the TTL of a held lock if the client and version match.
	//
	// Returns:
	//   - nil on success.
	//   - ErrLockNotHeld, ErrNotLockOwner, ErrVersionMismatch, ErrInvalidTTL, or fatal errors.
	ApplyRenew(ctx context.Context, lockID types.LockID, clientID types.ClientID, version types.Index, ttl time.Duration) error

	// ApplyWaitQueue enqueues a client into the lock's wait queue, optionally with priority.
	//
	// Returns:
	//   - The client's position in the queue.
	//   - ErrWaitQueueFull, ErrInvalidTimeout, or fatal errors.
	ApplyWaitQueue(ctx context.Context, lockID types.LockID, clientID types.ClientID, timeout time.Duration, version types.Index, priority int) (int, error)

	// ApplyCancelWait removes a client from a lock’s wait queue.
	//
	// Returns:
	//   - true if the client was removed.
	//   - ErrNotWaiting or a fatal error.
	ApplyCancelWait(ctx context.Context, lockID types.LockID, clientID types.ClientID, version types.Index) (bool, error)

	// GetLockInfo retrieves the current state of a lock.
	//
	// Returns:
	//   - *LockInfo if the lock exists.
	//   - ErrLockNotFound or other access-related errors.
	GetLockInfo(ctx context.Context, lockID types.LockID) (*types.LockInfo, error)

	// GetLocks returns a paginated list of locks matching an optional filter.
	// If limit <= 0, all items from offset are returned.
	//
	// Returns:
	//   - A slice of *LockInfo.
	//   - The total number of matching locks.
	//   - An error if retrieval fails.
	GetLocks(ctx context.Context, filter LockFilter, limit int, offset int) (locks []*types.LockInfo, total int, err error)

	// Tick advances the internal clock to trigger TTL expirations
	// and wait queue promotions.
	// This should be called periodically by an external ticker.
	//
	// Returns the number of expired locks.
	Tick(ctx context.Context) (expiredCount int)

	// Close shuts down background routines and cleans up resources.
	Close() error
}
