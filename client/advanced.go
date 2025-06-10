package client

import (
	"context"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// advancedClient is the default implementation of the AdvancedClient interface.
// It delegates all logic to an embedded baseClient, including connection handling and retries.
type advancedClient struct {
	baseClient
}

// NewAdvancedClient returns a new AdvancedClient for issuing low-level lock operations.
// It sets up connection management, retry logic, and metrics collection using the provided configuration.
func NewAdvancedClient(config Config) (AdvancedClient, error) {
	base, err := newBaseClient(config)
	if err != nil {
		return nil, err
	}
	return &advancedClient{baseClient: base}, nil
}

// EnqueueWaiter sends a request to enqueue the client as a waiter on the given lock.
func (c *advancedClient) EnqueueWaiter(ctx context.Context, req *EnqueueWaiterRequest) (*EnqueueResult, error) {
	var result *EnqueueResult
	err := c.executeWithRetry(ctx, "EnqueueWaiter", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.EnqueueWaiterRequest{
			LockId:   req.LockID,
			ClientId: req.ClientID,
			Priority: req.Priority,
			Version:  req.Version,
			Timeout:  durationpb.New(req.Timeout),
		}
		resp, err := client.EnqueueWaiter(ctx, pbReq)
		if err != nil {
			return err
		}
		result = protoToEnqueueResult(resp)
		return nil
	})
	return result, err
}

// CancelWait cancels a previously enqueued wait request for the specified lock.
func (c *advancedClient) CancelWait(ctx context.Context, req *CancelWaitRequest) (*CancelWaitResult, error) {
	var result *CancelWaitResult
	err := c.executeWithRetry(ctx, "CancelWait", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.CancelWaitRequest{
			LockId:   req.LockID,
			ClientId: req.ClientID,
			Version:  req.Version,
		}
		resp, err := client.CancelWait(ctx, pbReq)
		if err != nil {
			return err
		}
		result = protoToCancelWaitResult(resp)
		return nil
	})
	return result, err
}

// GetLeaderAddress returns the current known address of the Raft cluster leader.
func (c *advancedClient) GetLeaderAddress() string {
	return c.baseClient.getCurrentLeader()
}

// IsConnected reports whether the client has at least one active gRPC connection.
func (c *advancedClient) IsConnected() bool {
	return c.baseClient.isConnected()
}

// SetRetryPolicy updates the retry policy used for all future client operations.
func (c *advancedClient) SetRetryPolicy(policy RetryPolicy) {
	c.baseClient.setRetryPolicy(policy)
}

// GetMetrics returns the metrics collector associated with the client.
func (c *advancedClient) GetMetrics() Metrics {
	return c.baseClient.getMetrics()
}

// Close shuts down the client and releases all associated resources.
func (c *advancedClient) Close() error {
	return c.baseClient.close()
}
