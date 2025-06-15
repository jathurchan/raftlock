package main

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrClusterUnhealthy     = errors.New("cluster health check failed")
	ErrInsufficientNodes    = errors.New("insufficient healthy nodes for benchmark")
	ErrLeaderNotFound       = errors.New("no leader found in cluster")
	ErrOperationTimeout     = errors.New("operation timed out")
	ErrBenchmarkCanceled    = errors.New("benchmark was canceled")
	ErrInvalidConfiguration = errors.New("invalid benchmark configuration")
	ErrDockerNotAvailable   = errors.New("Docker is not available or accessible")
	ErrContainerNotFound    = errors.New("Docker container not found")
	ErrConnectionFailed     = errors.New("failed to establish connection")
	ErrHealthCheckFailed    = errors.New("health check failed")
)

// BenchmarkError describes an error that occurred during a benchmark phase.
type BenchmarkError struct {
	Phase     string         `json:"phase"`             // Phase of the benchmark where the error occurred
	Operation string         `json:"operation"`         // Specific operation being executed
	Err       error          `json:"error"`             // Underlying error
	Context   map[string]any `json:"context,omitempty"` // Optional key-value context for debugging
	Timestamp string         `json:"timestamp"`         // Time when the error occurred (UNIX format)
	Severity  string         `json:"severity"`          // Severity level: "info", "warning", or "error"
}

// Error implements the error interface for BenchmarkError.
func (e *BenchmarkError) Error() string {
	base := fmt.Sprintf("benchmark error in %s.%s: %v", e.Phase, e.Operation, e.Err)
	if len(e.Context) > 0 {
		return fmt.Sprintf("%s (context: %v)", base, e.Context)
	}
	return base
}

// Unwrap returns the underlying error.
func (e *BenchmarkError) Unwrap() error {
	return e.Err
}

// Is allows BenchmarkError to match wrapped errors using errors.Is.
func (e *BenchmarkError) Is(target error) bool {
	return errors.Is(e.Err, target)
}

// NewBenchmarkError constructs a BenchmarkError with full context and severity classification.
func NewBenchmarkError(phase, operation string, err error, context map[string]any) *BenchmarkError {
	severity := "error"
	switch {
	case errors.Is(err, ErrBenchmarkCanceled):
		severity = "info"
	case errors.Is(err, ErrOperationTimeout):
		severity = "warning"
	}

	return &BenchmarkError{
		Phase:     phase,
		Operation: operation,
		Err:       err,
		Context:   context,
		Timestamp: fmt.Sprintf("%d", time.Now().Unix()),
		Severity:  severity,
	}
}

// WrapError creates a BenchmarkError from an existing error without context.
func WrapError(phase, operation string, err error) error {
	if err == nil {
		return nil
	}
	return NewBenchmarkError(phase, operation, err, nil)
}

// WrapErrorWithContext creates a BenchmarkError and attaches additional context.
func WrapErrorWithContext(phase, operation string, err error, context map[string]any) error {
	if err == nil {
		return nil
	}
	return NewBenchmarkError(phase, operation, err, context)
}

// ValidationError represents a configuration validation failure.
type ValidationError struct {
	Field   string `json:"field"`   // The name of the invalid field
	Value   any    `json:"value"`   // The value that caused the validation failure
	Message string `json:"message"` // Human-readable error description
}

// Error implements the error interface for ValidationError.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s': %s (value: %v)", e.Field, e.Message, e.Value)
}

// NewValidationError creates a new ValidationError.
func NewValidationError(field string, value any, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}

// ConnectionError describes a failure to connect to a remote address.
type ConnectionError struct {
	Address   string `json:"address"`   // The address that was attempted
	Operation string `json:"operation"` // The connection-related operation (e.g. "dial", "ping")
	Err       error  `json:"error"`     // Underlying connection error
}

// Error implements the error interface for ConnectionError.
func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection error during %s to %s: %v", e.Operation, e.Address, e.Err)
}

// Unwrap returns the underlying error.
func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// NewConnectionError creates a new ConnectionError.
func NewConnectionError(address, operation string, err error) *ConnectionError {
	return &ConnectionError{
		Address:   address,
		Operation: operation,
		Err:       err,
	}
}
