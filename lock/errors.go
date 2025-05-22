package lock

import "errors"

var (
	// ErrLockHeld indicates an attempt to acquire a lock that is already held.
	ErrLockHeld = errors.New("lockmanager: lock is already held")

	// ErrLockNotHeld indicates an operation requiring a held lock was attempted on a free lock.
	ErrLockNotHeld = errors.New("lockmanager: lock is not currently held")

	// ErrNotLockOwner indicates an attempt to modify a lock by a client that does not own it.
	ErrNotLockOwner = errors.New("lockmanager: client is not the lock owner")

	// ErrVersionMismatch indicates that a provided fencing token (version) does not match the lock's current version.
	ErrVersionMismatch = errors.New("lockmanager: version mismatch")

	// ErrLockNotFound indicates that the requested lock does not exist.
	ErrLockNotFound = errors.New("lockmanager: lock not found")

	// ErrInvalidTTL indicates that an invalid Time-To-Live (TTL) value was provided.
	ErrInvalidTTL = errors.New("lockmanager: invalid TTL")

	// ErrWaitQueueFull indicates that the wait queue for a lock is full.
	ErrWaitQueueFull = errors.New("lockmanager: wait queue is full")

	// ErrNotWaiting indicates an attempt to cancel a wait for a client not in the wait queue.
	ErrNotWaiting = errors.New("lockmanager: client not in wait queue")

	// ErrResourceNotFound indicates that a lock resource could not be found or created.
	ErrResourceNotFound = errors.New("lockmanager: resource not found")

	// ErrInvalidTimeout indicates that an invalid timeout value was provided for a wait operation.
	ErrInvalidTimeout = errors.New("lockmanager: invalid timeout")
)
