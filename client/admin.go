package client

import (
	"context"
	"fmt"

	pb "github.com/jathurchan/raftlock/proto"
)

const (
	defaultMinAdminTimeout = 5000 // minimum timeout for admin ops (ms)
	maxLockIDLength        = 256  // max allowed length for lock IDs
)

// ValidationError describes a configuration or input validation error.
type ValidationError struct {
	Field   string
	Value   any
	Message string
}

// NewValidationError returns a structured validation error.
func NewValidationError(field string, value any, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}

// Error returns the error message for ValidationError.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s': %s", e.Field, e.Message)
}

// adminClient is the default AdminClient implementation.
// It embeds baseClient to reuse connection handling, retries, and leader resolution logic.
type adminClient struct {
	baseClient
}

// NewAdminClient creates a new AdminClient for administrative and monitoring tasks.
// It validates the provided config and initializes the underlying base client.
func NewAdminClient(config Config) (AdminClient, error) {
	if err := validateAdminConfig(config); err != nil {
		return nil, fmt.Errorf("invalid admin client config: %w", err)
	}

	base, err := newBaseClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create base client: %w", err)
	}

	return &adminClient{baseClient: base}, nil
}

// validateAdminConfig checks if the given config is suitable for admin operations.
func validateAdminConfig(config Config) error {
	if len(config.Endpoints) == 0 {
		return NewValidationError(
			"endpoints",
			config.Endpoints,
			"at least one endpoint must be provided",
		)
	}
	if config.RequestTimeout > 0 && config.RequestTimeout < defaultMinAdminTimeout {
		return NewValidationError("requestTimeout", config.RequestTimeout,
			fmt.Sprintf("timeout must be at least %v", defaultMinAdminTimeout))
	}
	return nil
}

// GetStatus retrieves cluster status, including Raft state, lock stats, and health info.
// If IncludeReplicationDetails is true, replication data is also included.
func (c *adminClient) GetStatus(
	ctx context.Context,
	req *GetStatusRequest,
) (*ClusterStatus, error) {
	if req == nil {
		return nil, NewValidationError("request", nil, "request cannot be nil")
	}

	var result *ClusterStatus
	err := c.executeWithRetry(
		ctx,
		"GetStatus",
		func(ctx context.Context, client pb.RaftLockClient) error {
			pbReq := &pb.GetStatusRequest{
				IncludeReplicationDetails: req.IncludeReplicationDetails,
			}

			resp, err := client.GetStatus(ctx, pbReq)
			if err != nil {
				return fmt.Errorf("GetStatus RPC failed: %w", err)
			}
			if resp == nil {
				return fmt.Errorf("nil GetStatus response")
			}

			result = protoToClusterStatus(resp)
			if result == nil {
				return fmt.Errorf("failed to convert GetStatus response")
			}
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("GetStatus failed: %w", err)
	}
	return result, nil
}

// Health performs a health check on the specified service.
// If ServiceName is empty, it checks the default service.
func (c *adminClient) Health(ctx context.Context, req *HealthRequest) (*HealthStatus, error) {
	if req == nil {
		return nil, NewValidationError("request", nil, "request cannot be nil")
	}

	var result *HealthStatus
	err := c.executeWithRetry(
		ctx,
		"Health",
		func(ctx context.Context, client pb.RaftLockClient) error {
			pbReq := &pb.HealthRequest{
				ServiceName: req.ServiceName,
			}

			resp, err := client.Health(ctx, pbReq)
			if err != nil {
				return fmt.Errorf("Health RPC failed: %w", err)
			}
			if resp == nil {
				return fmt.Errorf("nil Health response")
			}

			result = protoToHealthStatusFromHealthResponse(resp)
			if result == nil {
				return fmt.Errorf("failed to convert Health response")
			}
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("Health check failed: %w", err)
	}
	return result, nil
}

// GetBackoffAdvice returns backoff guidance for the given lock ID.
// Used by clients to implement smarter retries under contention.
func (c *adminClient) GetBackoffAdvice(
	ctx context.Context,
	req *BackoffAdviceRequest,
) (*BackoffAdvice, error) {
	if req == nil {
		return nil, NewValidationError("request", nil, "request cannot be nil")
	}
	if err := validateLockID(req.LockID); err != nil {
		return nil, fmt.Errorf("invalid lock ID: %w", err)
	}

	var result *BackoffAdvice
	err := c.executeWithRetry(
		ctx,
		"GetBackoffAdvice",
		func(ctx context.Context, client pb.RaftLockClient) error {
			pbReq := &pb.BackoffAdviceRequest{
				LockId: req.LockID,
			}

			resp, err := client.GetBackoffAdvice(ctx, pbReq)
			if err != nil {
				return fmt.Errorf("GetBackoffAdvice RPC failed: %w", err)
			}
			if resp == nil || resp.Advice == nil {
				return fmt.Errorf("invalid GetBackoffAdvice response")
			}

			result = protoToBackoffAdvice(resp.Advice)
			if result == nil {
				return fmt.Errorf("failed to convert GetBackoffAdvice response")
			}
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("GetBackoffAdvice failed: %w", err)
	}
	return result, nil
}

// validateLockID checks if the lock ID is within valid constraints.
func validateLockID(lockID string) error {
	if lockID == "" {
		return nil // empty ID is allowed for general advice
	}
	if len(lockID) > maxLockIDLength {
		return NewValidationError("lockID", lockID,
			fmt.Sprintf("lock ID exceeds max length of %d", maxLockIDLength))
	}
	return nil
}

// Close shuts down the client and releases underlying resources.
// Safe to call multiple times.
func (c *adminClient) Close() error {
	if err := c.baseClient.close(); err != nil {
		return fmt.Errorf("failed to close admin client: %w", err)
	}
	return nil
}
