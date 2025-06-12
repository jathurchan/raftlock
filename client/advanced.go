package client

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// advancedClient implements the AdvancedClient interface.
// It embeds baseClient to handle connection management, retries, and related logic.
type advancedClient struct {
	baseClient
}

// NewAdvancedClient creates a new AdvancedClient for low-level lock operations.
// It sets up connection handling, retry logic, and metrics using the provided config.
func NewAdvancedClient(config Config) (AdvancedClient, error) {
	if len(config.Endpoints) == 0 {
		return nil, errors.New("at least one endpoint must be provided")
	}

	base, err := newBaseClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create base client: %w", err)
	}
	return &advancedClient{baseClient: base}, nil
}

// EnqueueWaiter requests to enqueue the client as a waiter on a specific lock.
// The caller must provide a valid lock ID, client ID, and a positive timeout.
func (c *advancedClient) EnqueueWaiter(
	ctx context.Context,
	req *EnqueueWaiterRequest,
) (*EnqueueResult, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}
	if req.LockID == "" {
		return nil, errors.New("lock ID cannot be empty")
	}
	if req.ClientID == "" {
		return nil, errors.New("client ID cannot be empty")
	}
	if req.Timeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}

	var result *EnqueueResult
	err := c.executeWithRetry(
		ctx,
		"EnqueueWaiter",
		func(ctx context.Context, client pb.RaftLockClient) error {
			pbReq := &pb.EnqueueWaiterRequest{
				LockId:   req.LockID,
				ClientId: req.ClientID,
				Priority: req.Priority,
				Version:  req.Version,
				Timeout:  durationpb.New(req.Timeout),
			}
			resp, err := client.EnqueueWaiter(ctx, pbReq)
			if err != nil {
				return fmt.Errorf("enqueue waiter RPC failed: %w", err)
			}
			result = protoToEnqueueResult(resp)
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue waiter for lock %s: %w", req.LockID, err)
	}
	return result, nil
}

// CancelWait cancels a previously enqueued waiter for a given lock and client.
func (c *advancedClient) CancelWait(
	ctx context.Context,
	req *CancelWaitRequest,
) (*CancelWaitResult, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}
	if req.LockID == "" {
		return nil, errors.New("lock ID cannot be empty")
	}
	if req.ClientID == "" {
		return nil, errors.New("client ID cannot be empty")
	}

	var result *CancelWaitResult
	err := c.executeWithRetry(
		ctx,
		"CancelWait",
		func(ctx context.Context, client pb.RaftLockClient) error {
			pbReq := &pb.CancelWaitRequest{
				LockId:   req.LockID,
				ClientId: req.ClientID,
				Version:  req.Version,
			}
			resp, err := client.CancelWait(ctx, pbReq)
			if err != nil {
				return fmt.Errorf("cancel wait RPC failed: %w", err)
			}
			result = protoToCancelWaitResult(resp)
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to cancel wait for lock %s: %w", req.LockID, err)
	}
	return result, nil
}

// GetLeaderAddress returns the current address of the known Raft leader.
func (c *advancedClient) GetLeaderAddress() string {
	return c.baseClient.getCurrentLeader()
}

// IsConnected reports whether the client has at least one active gRPC connection.
func (c *advancedClient) IsConnected() bool {
	return c.baseClient.isConnected()
}

// SetRetryPolicy configures the retry policy for subsequent client operations.
func (c *advancedClient) SetRetryPolicy(policy RetryPolicy) {
	c.baseClient.setRetryPolicy(policy)
}

// GetMetrics returns the metrics collector associated with the client instance.
func (c *advancedClient) GetMetrics() Metrics {
	return c.baseClient.getMetrics()
}

// Close shuts down the client and releases all resources.
// This call is safe to invoke multiple times.
func (c *advancedClient) Close() error {
	if err := c.baseClient.close(); err != nil {
		return fmt.Errorf("failed to close advanced client: %w", err)
	}
	return nil
}
