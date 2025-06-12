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

// AdminClient provides administrative and monitoring capabilities for RaftLock clusters.
// Intended for use in tools, dashboards, and scripts—not for client applications performing locking.
type AdminClient interface {
	// GetStatus returns the current status of the RaftLock cluster node.
	//
	// Useful for monitoring and diagnostics.
	//
	// Returns:
	//   - ClusterStatus with Raft and lock manager state
	//   - Error if the request fails
	GetStatus(ctx context.Context, req *GetStatusRequest) (*ClusterStatus, error)

	// Health checks the health of the RaftLock service.
	//
	// Returns:
	//   - HealthStatus indicating current health
	//   - Error if the service is unhealthy or unreachable
	Health(ctx context.Context, req *HealthRequest) (*HealthStatus, error)

	// GetBackoffAdvice returns adaptive backoff parameters to guide retry behavior
	// during lock contention.
	//
	// Returns:
	//   - BackoffAdvice with recommended delay settings
	//   - Error if the request fails
	GetBackoffAdvice(ctx context.Context, req *BackoffAdviceRequest) (*BackoffAdvice, error)

	// Close shuts down the client and releases associated resources.
	Close() error
}

// AdvancedClient provides low-level operations for advanced RaftLock use cases.
// These methods expose finer control over lock queuing and client behavior.
type AdvancedClient interface {
	// EnqueueWaiter explicitly adds the client to a lock's wait queue.
	//
	// Most applications should use Acquire with wait=true instead.
	//
	// Returns:
	//   - EnqueueResult with queue position and estimated wait time
	//   - Error if the operation fails
	//
	// Possible errors:
	//   - ErrWaitQueueFull: the wait queue is at capacity
	EnqueueWaiter(ctx context.Context, req *EnqueueWaiterRequest) (*EnqueueResult, error)

	// CancelWait removes the client from a lock's wait queue.
	//
	// Most applications should rely on context cancellation instead.
	//
	// Returns:
	//   - CancelWaitResult indicating cancellation success
	//   - Error if the operation fails
	//
	// Possible errors:
	//   - ErrNotWaiting: client is not in the queue
	CancelWait(ctx context.Context, req *CancelWaitRequest) (*CancelWaitResult, error)

	// GetLeaderAddress returns the current leader's address, or an empty string if unknown.
	GetLeaderAddress() string

	// IsConnected reports whether the client has an active connection to the cluster.
	IsConnected() bool

	// SetRetryPolicy sets the client's retry behavior for failed operations.
	//
	// If not set, a default policy is used.
	SetRetryPolicy(policy RetryPolicy)

	// GetMetrics returns client-side metrics for observability.
	//
	// Returns nil if metrics collection is disabled.
	GetMetrics() Metrics

	// Close shuts down the client and releases resources.
	Close() error
}
