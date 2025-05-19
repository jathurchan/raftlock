package lock

import (
	"context"
	"time"

	"github.com/jathurchan/raftlock/types"
)

// LockManager defines a distributed lock management interface,
// serving as the application-level state machine for Raft consensus.
//
// All `Apply*` methods must be deterministic, thread-safe, and idempotent.
// These are invoked only after commands are committed by Raft.
type LockManager interface {
	// Apply routes a committed Raft command to the appropriate lock operation.
	//
	// Returns a fatal error only if the command cannot be processed.
	Apply(ctx context.Context, index types.Index, command []byte) error

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
	//
	// Returns:
	//   - A slice of *LockInfo.
	//   - The total number of matching locks.
	//   - An error if retrieval fails.
	GetLocks(ctx context.Context, filter LockFilter, limit int, offset int) (locks []*types.LockInfo, total int, err error)

	// Tick advances the internal clock to trigger TTL expirations
	// and wait queue promotions.
	//
	// Returns the number of expired locks.
	Tick(ctx context.Context) (expiredCount int)

	// Snapshot serializes the LockManager's state for Raft snapshotting.
	//
	// Returns:
	//   - The Raft index included in the snapshot.
	//   - A byte slice of the serialized state.
	//   - An error if the snapshot fails.
	Snapshot(ctx context.Context) (types.Index, []byte, error)

	// RestoreSnapshot loads a LockManager state from a Raft snapshot.
	//
	// Returns an error if the snapshot is invalid or restoration fails.
	RestoreSnapshot(ctx context.Context, lastIndex types.Index, lastTerm types.Term, data []byte) error

	// Close shuts down background routines and cleans up resources.
	Close() error
}
