package client

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"
)

const (
	// releaseTimeout is the duration allowed for a best-effort lock release during Close.
	releaseTimeout = 5 * time.Second
)

// LockHandle provides a convenient, stateful wrapper for managing the lifecycle of a single distributed lock.
// It simplifies the process of acquiring, renewing, and releasing a specific lock by maintaining
// the lock's state (like its version) internally. All methods are thread-safe.
type LockHandle interface {
	// Acquire attempts to acquire the lock with the specified TTL.
	// If `wait` is true, the client may be enqueued if the lock is held.
	// Returns an error if the lock cannot be acquired (e.g., ErrLockHeld, ErrClientClosed).
	Acquire(ctx context.Context, ttl time.Duration, wait bool) error

	// Release releases the lock if it is currently held by this handle.
	// It uses the fencing token from the last successful acquire or renew operation.
	// Returns an error if the release fails (e.g., ErrNotLockOwner, ErrClientClosed).
	Release(ctx context.Context) error

	// Renew extends the TTL of the currently held lock.
	// Returns an error if the lock is not held or the renewal fails.
	Renew(ctx context.Context, newTTL time.Duration) error

	// IsHeld returns true if the lock is currently considered held by this handle.
	IsHeld() bool

	// Lock returns a copy of the current lock information if held, or nil otherwise.
	// The returned Lock struct is a snapshot and should not be modified.
	Lock() *Lock

	// Close releases the lock if held and marks the handle as closed, preventing further operations.
	// It is safe to call Close multiple times.
	Close(ctx context.Context) error
}

// lockHandle implements the LockHandle interface.
type lockHandle struct {
	client RaftLockClient

	// The unique identifier of the distributed lock this handle manages.
	lockID string

	// The unique identifier of the client that owns this handle.
	clientID string

	mu     sync.RWMutex // Protects access to the mutable fields below.
	lock   *Lock        // Stores the details of the acquired lock. nil if the lock not held.
	closed bool         // Flag indicating if the handle has been closed. Once true, all operations will fail.
}

// NewLockHandle creates a new LockHandle that can be used to manage a single lock.
// It requires a RaftLockClient instance and identifiers for the lock and the client.
func NewLockHandle(client RaftLockClient, lockID, clientID string) (LockHandle, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if lockID == "" {
		return nil, errors.New("lockID cannot be empty")
	}
	if clientID == "" {
		return nil, errors.New("clientID cannot be empty")
	}
	return &lockHandle{
		client:   client,
		lockID:   lockID,
		clientID: clientID,
	}, nil
}

// Acquire attempts to acquire the lock with the specified TTL.
func (h *lockHandle) Acquire(ctx context.Context, ttl time.Duration, wait bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return ErrClientClosed
	}

	req := &AcquireRequest{
		LockID:   h.lockID,
		ClientID: h.clientID,
		TTL:      ttl,
		Wait:     wait,
	}

	result, err := h.client.Acquire(ctx, req)
	if err != nil {
		return err
	}

	if !result.Acquired {
		if result.Error != nil {
			return ErrorFromCode(result.Error.Code)
		}
		return ErrLockHeld
	}

	h.lock = result.Lock
	return nil
}

// Release releases the lock if it's currently held.
func (h *lockHandle) Release(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		// To prevent releasing a lock that might have been re-acquired after
		// this handle was closed, we check the closed flag first.
		return ErrClientClosed
	}

	if h.lock == nil {
		return ErrNotLockOwner
	}

	req := &ReleaseRequest{
		LockID:   h.lockID,
		ClientID: h.clientID,
		Version:  h.lock.Version,
	}

	result, err := h.client.Release(ctx, req)
	if err != nil {
		return err
	}

	if !result.Released {
		if result.Error != nil {
			return ErrorFromCode(result.Error.Code)
		}
		return fmt.Errorf("failed to release lock for client %s on resource %s", h.clientID, h.lockID)
	}

	h.lock = nil
	return nil
}

// Renew extends the TTL of the currently held lock.
func (h *lockHandle) Renew(ctx context.Context, newTTL time.Duration) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return ErrClientClosed
	}

	if h.lock == nil {
		return ErrNotLockOwner
	}

	req := &RenewRequest{
		LockID:   h.lockID,
		ClientID: h.clientID,
		Version:  h.lock.Version,
		NewTTL:   newTTL,
	}

	result, err := h.client.Renew(ctx, req)
	if err != nil {
		return err
	}

	if !result.Renewed {
		if result.Error != nil {
			return ErrorFromCode(result.Error.Code)
		}
		return fmt.Errorf("failed to renew lock for client %s on resource %s", h.clientID, h.lockID)
	}

	h.lock = result.Lock
	return nil
}

// IsHeld returns true if the lock is currently considered held by this handle.
func (h *lockHandle) IsHeld() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return !h.closed && h.lock != nil
}

// Lock returns a copy of the current lock information if held, or nil otherwise.
func (h *lockHandle) Lock() *Lock {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed || h.lock == nil {
		return nil
	}
	// Return a copy to prevent external modification of the internal state.
	lockCopy := *h.lock
	if lockCopy.Metadata != nil {
		// Also copy the metadata map to prevent modification of the map itself.
		newMeta := make(map[string]string, len(lockCopy.Metadata))
		maps.Copy(newMeta, lockCopy.Metadata)
		lockCopy.Metadata = newMeta
	}
	return &lockCopy
}

// Close releases the lock if held and marks the handle as closed.
func (h *lockHandle) Close(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil
	}
	h.closed = true

	if h.lock != nil {
		// Using a background context for this best-effort release, so it's not
		// cancelled if the calling context is already done.
		releaseCtx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		defer cancel()

		_, _ = h.client.Release(releaseCtx, &ReleaseRequest{
			LockID:   h.lockID,
			ClientID: h.clientID,
			Version:  h.lock.Version,
		})
		h.lock = nil
	}

	return nil
}
