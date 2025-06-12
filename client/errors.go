package client

import (
	"errors"
	"fmt"

	pb "github.com/jathurchan/raftlock/proto"
)

// Common client errors
var (
	// ErrLockHeld is returned when a lock is already held by another client
	ErrLockHeld = errors.New("lock is already held by another client")

	// ErrNotLockOwner is returned when attempting to operate on a lock not owned by the client
	ErrNotLockOwner = errors.New("client does not own the lock")

	// ErrVersionMismatch is returned when the provided version doesn't match the current lock version
	ErrVersionMismatch = errors.New("lock version mismatch")

	// ErrLockNotFound is returned when the specified lock does not exist
	ErrLockNotFound = errors.New("lock not found")

	// ErrInvalidTTL is returned when an invalid TTL value is provided
	ErrInvalidTTL = errors.New("invalid TTL value")

	// ErrWaitQueueFull is returned when the wait queue has reached capacity
	ErrWaitQueueFull = errors.New("wait queue is full")

	// ErrNotWaiting is returned when trying to cancel a wait for a client not in the queue
	ErrNotWaiting = errors.New("client is not waiting in the queue")

	// ErrTimeout is returned when an operation times out
	ErrTimeout = errors.New("operation timed out")

	// ErrLeaderUnavailable is returned when no leader is available
	ErrLeaderUnavailable = errors.New("no leader available")

	// ErrInvalidArgument is returned when request parameters are invalid
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrUnavailable is returned when the service is unavailable
	ErrUnavailable = errors.New("service unavailable")

	// ErrRateLimit is returned when the request is rate limited
	ErrRateLimit = errors.New("request rate limited")

	// ErrClientClosed is returned when attempting to use a closed client
	ErrClientClosed = errors.New("client is closed")
)

// ErrorFromCode converts a protobuf error code to a Go error.
func ErrorFromCode(code pb.ErrorCode) error {
	switch code {
	case pb.ErrorCode_LOCK_HELD:
		return ErrLockHeld
	case pb.ErrorCode_LOCK_NOT_HELD:
		return ErrNotLockOwner
	case pb.ErrorCode_NOT_LOCK_OWNER:
		return ErrNotLockOwner
	case pb.ErrorCode_VERSION_MISMATCH:
		return ErrVersionMismatch
	case pb.ErrorCode_LOCK_NOT_FOUND:
		return ErrLockNotFound
	case pb.ErrorCode_INVALID_TTL:
		return ErrInvalidTTL
	case pb.ErrorCode_WAIT_QUEUE_FULL:
		return ErrWaitQueueFull
	case pb.ErrorCode_NOT_WAITING:
		return ErrNotWaiting
	case pb.ErrorCode_NOT_LEADER:
		return ErrLeaderUnavailable
	case pb.ErrorCode_NO_LEADER:
		return ErrLeaderUnavailable
	case pb.ErrorCode_TIMEOUT:
		return ErrTimeout
	case pb.ErrorCode_RATE_LIMITED:
		return ErrRateLimit
	case pb.ErrorCode_UNAVAILABLE:
		return ErrUnavailable
	case pb.ErrorCode_INVALID_ARGUMENT:
		return ErrInvalidArgument
	default:
		return fmt.Errorf("unknown error code: %v", code)
	}
}

// ClientError wraps an error with additional client context.
type ClientError struct {
	Op      string            // Operation that failed
	Err     error             // Underlying error
	Code    pb.ErrorCode      // Error code from server
	Details map[string]string // Additional error details
}

// Error implements the error interface.
func (e *ClientError) Error() string {
	if len(e.Details) > 0 {
		return fmt.Sprintf(
			"client %s failed: %v (code: %v, details: %v)",
			e.Op,
			e.Err,
			e.Code,
			e.Details,
		)
	}
	return fmt.Sprintf("client %s failed: %v (code: %v)", e.Op, e.Err, e.Code)
}

// Unwrap returns the underlying error.
func (e *ClientError) Unwrap() error {
	return e.Err
}

// Is checks if the error matches the target error.
func (e *ClientError) Is(target error) bool {
	return errors.Is(e.Err, target)
}

// NewClientError creates a new ClientError.
func NewClientError(
	op string,
	err error,
	code pb.ErrorCode,
	details map[string]string,
) *ClientError {
	return &ClientError{
		Op:      op,
		Err:     err,
		Code:    code,
		Details: details,
	}
}
