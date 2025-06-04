package server

import (
	"errors"
	"fmt"

	pb "github.com/jathurchan/raftlock/proto"
)

var (
	// ErrServerNotStarted indicates the server has not been started or is not yet ready.
	ErrServerNotStarted = errors.New("server: server not started or not ready")

	// ErrServerAlreadyStarted indicates an attempt to start an already running server.
	ErrServerAlreadyStarted = errors.New("server: server already started")

	// ErrServerStopped indicates the server has been stopped and cannot process requests.
	ErrServerStopped = errors.New("server: server stopped")

	// ErrNotLeader indicates this server is not the current Raft leader and cannot process the request.
	ErrNotLeader = errors.New("server: this node is not the Raft leader")

	// ErrNoLeader indicates that no leader is currently elected or known in the Raft cluster.
	ErrNoLeader = errors.New("server: no leader available in the cluster")

	// ErrRequestTooLarge indicates the client's request exceeds the configured maximum allowed size.
	ErrRequestTooLarge = errors.New("server: request too large")

	// ErrResponseTooLarge indicates an internally generated response exceeds the maximum allowed size.
	// This is typically an internal server issue.
	ErrResponseTooLarge = errors.New("server: response too large")

	// ErrRateLimited indicates the request was rejected due to rate limiting policies.
	ErrRateLimited = errors.New("server: request rate limited")

	// ErrShutdownTimeout indicates the server's graceful shutdown process timed out.
	ErrShutdownTimeout = errors.New("server: shutdown timed out")

	// ErrInvalidRequest indicates the request is malformed or contains invalid parameters
	// not caught by more specific validation errors.
	ErrInvalidRequest = errors.New("server: invalid request")

	// ErrRaftUnavailable indicates the Raft consensus system is not available or has errors.
	ErrRaftUnavailable = errors.New("server: raft consensus system unavailable")

	// ErrLockManagerUnavailable indicates the core lock management logic is unavailable or has errors.
	ErrLockManagerUnavailable = errors.New("server: lock manager unavailable")
)

// LeaderRedirectError contains information needed to redirect a client to the current leader.
// It is used when a write request is sent to a follower node.
type LeaderRedirectError struct {
	LeaderAddress string
	LeaderID      string
}

// NewLeaderRedirectError creates a new LeaderRedirectError.
func NewLeaderRedirectError(leaderAddress, leaderID string) *LeaderRedirectError {
	return &LeaderRedirectError{
		LeaderAddress: leaderAddress,
		LeaderID:      leaderID,
	}
}

// Error implements the error interface, providing a human-readable message.
func (e *LeaderRedirectError) Error() string {
	if e.LeaderAddress != "" && e.LeaderID != "" {
		return fmt.Sprintf("server: not leader, redirect to node %s at %s", e.LeaderID, e.LeaderAddress)
	}
	return "server: not leader, leader information unavailable for redirect"
}

// ValidationError represents a request validation error with details about the specific field.
type ValidationError struct {
	Field   string // The name of the field that failed validation.
	Value   any    // The value of the field that caused the error.
	Message string // A descriptive message explaining the validation failure.
}

// NewValidationError creates a new ValidationError.
func NewValidationError(field string, value any, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}

// Error implements the error interface, providing a structured validation error message.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("server: validation error for field '%s' (value: %v): %s", e.Field, e.Value, e.Message)
}

// ServerError represents a generic internal server error, potentially wrapping an underlying cause.
type ServerError struct {
	Operation string // The operation being performed when the error occurred.
	Cause     error  // The underlying error, if any.
	Message   string // A high-level message describing the server error.
}

// NewServerError creates a new ServerError.
func NewServerError(operation string, cause error, message string) *ServerError {
	return &ServerError{
		Operation: operation,
		Cause:     cause,
		Message:   message,
	}
}

// Error implements the error interface, providing context about the operation and cause.
func (e *ServerError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("server: error during %s: %s (cause: %v)", e.Operation, e.Message, e.Cause)
	}
	return fmt.Sprintf("server: error during %s: %s", e.Operation, e.Message)
}

// Unwrap provides compatibility with Go's errors.Is and errors.As by returning the cause.
func (e *ServerError) Unwrap() error {
	return e.Cause
}

// ErrorToProtoError converts Go errors to protobuf ErrorDetail messages.
// This function maps common server errors to appropriate ErrorCode values
// and provides structured error information for clients.
func ErrorToProtoError(err error) *pb.ErrorDetail {
	if err == nil {
		return nil
	}

	var leaderRedirectErr *LeaderRedirectError
	var validationErr *ValidationError
	var serverErr *ServerError

	if errors.As(err, &leaderRedirectErr) {
		details := map[string]string{}
		if leaderRedirectErr.LeaderAddress != "" {
			details["leader_address"] = leaderRedirectErr.LeaderAddress
		}
		if leaderRedirectErr.LeaderID != "" {
			details["leader_id"] = leaderRedirectErr.LeaderID
		}
		return &pb.ErrorDetail{
			Code:    pb.ErrorCode_NOT_LEADER,
			Message: leaderRedirectErr.Error(),
			Details: details,
		}
	}
	if errors.As(err, &validationErr) {
		return &pb.ErrorDetail{
			Code:    pb.ErrorCode_INVALID_ARGUMENT,
			Message: validationErr.Message,
			Details: map[string]string{
				"field": validationErr.Field,
				"value": fmt.Sprintf("%v", validationErr.Value),
			},
		}
	}
	if errors.As(err, &serverErr) {
		details := map[string]string{
			"operation": serverErr.Operation,
		}
		if serverErr.Cause != nil {
			details["cause"] = serverErr.Cause.Error()
		}
		return &pb.ErrorDetail{
			Code:    pb.ErrorCode_INTERNAL_ERROR,
			Message: serverErr.Message, // e.Error() too verbose or leak internal details
			Details: details,
		}
	}
	if errors.Is(err, lock.ErrLockNotFound) {
		return &pb.ErrorDetail{Code: pb.ErrorCode_LOCK_NOT_FOUND, Message: err.Error()}
	}
	if errors.Is(err, lock.ErrNotWaiting) {
		return &pb.ErrorDetail{Code: pb.ErrorCode_NOT_WAITING, Message: err.Error()}
	}

	switch {
	case errors.Is(err, ErrNotLeader):
		return &pb.ErrorDetail{Code: pb.ErrorCode_NOT_LEADER, Message: ErrNotLeader.Error()}
	case errors.Is(err, ErrNoLeader):
		return &pb.ErrorDetail{Code: pb.ErrorCode_NO_LEADER, Message: ErrNoLeader.Error()}
	case errors.Is(err, ErrRequestTooLarge):
		return &pb.ErrorDetail{Code: pb.ErrorCode_INVALID_ARGUMENT, Message: ErrRequestTooLarge.Error()}
	case errors.Is(err, ErrRateLimited):
		return &pb.ErrorDetail{Code: pb.ErrorCode_RATE_LIMITED, Message: ErrRateLimited.Error()}
	case errors.Is(err, ErrRaftUnavailable):
		return &pb.ErrorDetail{Code: pb.ErrorCode_UNAVAILABLE, Message: ErrRaftUnavailable.Error()}
	case errors.Is(err, ErrLockManagerUnavailable):
		return &pb.ErrorDetail{Code: pb.ErrorCode_UNAVAILABLE, Message: ErrLockManagerUnavailable.Error()}
	case errors.Is(err, ErrServerNotStarted), errors.Is(err, ErrServerStopped):
		return &pb.ErrorDetail{Code: pb.ErrorCode_UNAVAILABLE, Message: err.Error()}
	case errors.Is(err, ErrInvalidRequest):
		return &pb.ErrorDetail{Code: pb.ErrorCode_INVALID_ARGUMENT, Message: ErrInvalidRequest.Error()}
	case errors.Is(err, ErrServerAlreadyStarted):
		return &pb.ErrorDetail{Code: pb.ErrorCode_INTERNAL_ERROR, Message: ErrServerAlreadyStarted.Error()}
	case errors.Is(err, ErrResponseTooLarge):
		return &pb.ErrorDetail{Code: pb.ErrorCode_INTERNAL_ERROR, Message: ErrResponseTooLarge.Error()}
	case errors.Is(err, ErrShutdownTimeout):
		return &pb.ErrorDetail{Code: pb.ErrorCode_INTERNAL_ERROR, Message: ErrShutdownTimeout.Error()}
	default:
		return &pb.ErrorDetail{
			Code:    pb.ErrorCode_INTERNAL_ERROR,
			Message: "An unexpected internal error occurred.",
		}
	}
}
