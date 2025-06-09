package client

import "context"

// RaftLockClient defines a high-level client for interacting with a RaftLock cluster.
// It abstracts gRPC communication and provides methods for distributed lock operations
// with support for retries, leader redirection, and backoff.
//
// All operations are context-aware and honor cancellation and timeouts.
// The client handles topology changes and reconnects to new leaders automatically.
type RaftLockClient interface {
	// Acquire attempts to acquire a distributed lock using the given parameters.
	// If the lock is already held and wait=true, the client is enqueued.
	//
	// Returns:
	//   - AcquireResult with lock details and status
	//   - Error if the operation fails or times out
	//
	// Possible errors:
	//   - ErrLockHeld: lock is held by another client (wait=false)
	//   - ErrTimeout: operation exceeded timeout
	//   - ErrLeaderUnavailable: no available leader
	//   - ErrInvalidArgument: invalid input parameters
	Acquire(ctx context.Context, req *AcquireRequest) (*AcquireResult, error)

	// Release releases a lock previously acquired by the client, using the given version token.
	//
	// Returns:
	//   - ReleaseResult indicating success and whether a waiter was promoted
	//   - Error if the operation fails
	//
	// Possible errors:
	//   - ErrNotLockOwner: caller is not the lock owner
	//   - ErrVersionMismatch: version does not match current lock version
	//   - ErrLockNotFound: lock does not exist
	Release(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error)

	// Renew extends the TTL of a lock held by the client.
	//
	// Returns:
	//   - RenewResult with updated lock information
	//   - Error if the operation fails
	//
	// Possible errors:
	//   - ErrNotLockOwner: caller is not the lock owner
	//   - ErrVersionMismatch: version does not match current lock version
	//   - ErrInvalidTTL: TTL value is invalid
	Renew(ctx context.Context, req *RenewRequest) (*RenewResult, error)

	// GetLockInfo returns metadata and state for a given lock without acquiring it.
	//
	// Returns:
	//   - LockInfo with lock state and metadata
	//   - Error if the operation fails
	//
	// Uses linearizable reads for consistency.
	GetLockInfo(ctx context.Context, req *GetLockInfoRequest) (*LockInfo, error)

	// GetLocks returns a paginated list of locks matching the specified filter.
	//
	// Returns:
	//   - GetLocksResult with matching locks and pagination data
	//   - Error if the operation fails
	//
	GetLocks(ctx context.Context, req *GetLocksRequest) (*GetLocksResult, error)

	// Close shuts down the client, releasing all resources and closing connections.
	// The client must not be used after Close is called.
	Close() error
}
