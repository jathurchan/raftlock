package client

import (
	"context"

	pb "github.com/jathurchan/raftlock/proto"
)

// adminClient is the default implementation of the AdminClient interface.
// It delegates connection management, retries, and leader discovery to baseClient.
type adminClient struct {
	baseClient
}

// NewAdminClient returns a new AdminClient for administrative and monitoring operations.
func NewAdminClient(config Config) (AdminClient, error) {
	base, err := newBaseClient(config)
	if err != nil {
		return nil, err
	}
	return &adminClient{baseClient: base}, nil
}

// GetStatus implements the AdminClient interface.
func (c *adminClient) GetStatus(ctx context.Context, req *GetStatusRequest) (*ClusterStatus, error) {
	var result *ClusterStatus
	err := c.executeWithRetry(ctx, "GetStatus", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.GetStatusRequest{
			IncludeReplicationDetails: req.IncludeReplicationDetails,
		}
		resp, err := client.GetStatus(ctx, pbReq)
		if err != nil {
			return err
		}
		result = protoToClusterStatus(resp)
		return nil
	})
	return result, err
}

// Health implements the AdminClient interface.
func (c *adminClient) Health(ctx context.Context, req *HealthRequest) (*HealthStatus, error) {
	var result *HealthStatus
	err := c.executeWithRetry(ctx, "Health", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.HealthRequest{
			ServiceName: req.ServiceName,
		}
		resp, err := client.Health(ctx, pbReq)
		if err != nil {
			return err
		}

		result = protoToHealthStatusFromHealthResponse(resp)
		return nil
	})
	return result, err
}

// GetBackoffAdvice implements the AdminClient interface.
func (c *adminClient) GetBackoffAdvice(ctx context.Context, req *BackoffAdviceRequest) (*BackoffAdvice, error) {
	var result *BackoffAdvice
	err := c.executeWithRetry(ctx, "GetBackoffAdvice", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.BackoffAdviceRequest{
			LockId: req.LockID,
		}
		resp, err := client.GetBackoffAdvice(ctx, pbReq)
		if err != nil {
			return err
		}
		result = protoToBackoffAdvice(resp.Advice)
		return nil
	})
	return result, err
}

// Close implements the AdminClient interface.
func (c *adminClient) Close() error {
	return c.baseClient.close()
}
