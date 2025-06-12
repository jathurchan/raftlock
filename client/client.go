package client

import (
	"context"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// raftLockClient is the default implementation of the RaftLockClient interface.
// It delegates core functionality such as retries and leader tracking to a baseClient.
type raftLockClient struct {
	baseClient
}

// NewRaftLockClient returns a new RaftLock client configured to interact with a cluster.
// It establishes internal connection handling, metrics, and retry logic.
func NewRaftLockClient(config Config) (RaftLockClient, error) {
	base, err := newBaseClient(config)
	if err != nil {
		return nil, err
	}
	return &raftLockClient{baseClient: base}, nil
}

// Acquire attempts to acquire a distributed lock with the given parameters.
func (c *raftLockClient) Acquire(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
	var result *AcquireResult
	err := c.executeWithRetry(ctx, "Acquire", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.AcquireRequest{
			LockId:      req.LockID,
			ClientId:    req.ClientID,
			Wait:        req.Wait,
			Priority:    req.Priority,
			Metadata:    req.Metadata,
			RequestId:   req.RequestID,
			Ttl:         durationpb.New(req.TTL),
			WaitTimeout: durationpb.New(req.WaitTimeout),
		}
		resp, err := client.Acquire(ctx, pbReq)
		if err != nil {
			return err
		}
		result = protoToAcquireResult(resp)
		return nil
	})
	return result, err
}

// Release releases a previously acquired lock.
func (c *raftLockClient) Release(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
	var result *ReleaseResult
	err := c.executeWithRetry(ctx, "Release", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.ReleaseRequest{
			LockId:   req.LockID,
			ClientId: req.ClientID,
			Version:  req.Version,
		}
		resp, err := client.Release(ctx, pbReq)
		if err != nil {
			return err
		}
		result = protoToReleaseResult(resp)
		return nil
	})
	return result, err
}

// Renew extends the TTL of an existing lock.
func (c *raftLockClient) Renew(ctx context.Context, req *RenewRequest) (*RenewResult, error) {
	var result *RenewResult
	err := c.executeWithRetry(ctx, "Renew", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.RenewRequest{
			LockId:   req.LockID,
			ClientId: req.ClientID,
			Version:  req.Version,
			NewTtl:   durationpb.New(req.NewTTL),
		}
		resp, err := client.Renew(ctx, pbReq)
		if err != nil {
			return err
		}
		result = protoToRenewResult(resp)
		return nil
	})
	return result, err
}

// GetLockInfo retrieves metadata and state for a specific lock.
func (c *raftLockClient) GetLockInfo(ctx context.Context, req *GetLockInfoRequest) (*LockInfo, error) {
	var result *LockInfo
	err := c.executeWithRetry(ctx, "GetLockInfo", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.GetLockInfoRequest{
			LockId:         req.LockID,
			IncludeWaiters: req.IncludeWaiters,
		}
		resp, err := client.GetLockInfo(ctx, pbReq)
		if err != nil {
			return err
		}
		if resp.Error != nil {
			return ErrorFromCode(resp.Error.Code)
		}
		result = protoToLockInfo(resp.LockInfo)
		return nil
	})
	return result, err
}

// GetLocks returns a paginated list of locks matching the specified filter.
func (c *raftLockClient) GetLocks(ctx context.Context, req *GetLocksRequest) (*GetLocksResult, error) {
	var result *GetLocksResult
	err := c.executeWithRetry(ctx, "GetLocks", func(ctx context.Context, client pb.RaftLockClient) error {
		pbReq := &pb.GetLocksRequest{
			Filter:         lockFilterToProto(req.Filter),
			Limit:          req.Limit,
			Offset:         req.Offset,
			IncludeWaiters: req.IncludeWaiters,
		}
		resp, err := client.GetLocks(ctx, pbReq)
		if err != nil {
			return err
		}
		result = protoToGetLocksResult(resp)
		return nil
	})
	return result, err
}

// Close shuts down the client and releases any associated resources.
func (c *raftLockClient) Close() error {
	return c.baseClient.close()
}
