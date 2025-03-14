package client

import (
	"context"
	"fmt"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RaftLockClient is a high-level client for interacting with the RaftLock service.
// It manages the underlying gRPC connection and provides methods for acquiring and releasing locks.
type RaftLockClient struct {
	conn     *grpc.ClientConn
	clientID string
	pb.RaftLockClient
}

// Creates a new RaftLock client connected to the specified server
func NewRaftLockClient(serverAddr string, clientID string) (*RaftLockClient, error) {
	conn, err := grpc.NewClient(
		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", serverAddr, err)
	}

	return &RaftLockClient{
		conn:           conn,
		clientID:       clientID,
		RaftLockClient: pb.NewRaftLockClient(conn),
	}, nil
}

// Closes the client connection
func (c *RaftLockClient) Close() error {
	return c.conn.Close()
}

// LockResponse represents the outcome of a lock operation
type LockResponse struct {
	Success bool
	Message string
}

// LockOptions contains optional parameters for the lock operation
type LockOptions struct {
	// TTLSeconds specifies how long the lock should be held (in seconds)
	// If set to 0 or negative, server default will be used
	TtlSeconds int64
}

// Returns the default options for Lock operations
func DefaultLockOptions() *LockOptions {
	return &LockOptions{
		TtlSeconds: 0, // No TTL specified, use server default
	}
}

// Attempts to acquire a lock on the specified resource
// The options parameter is optional; if nil, default options will be used
func (c *RaftLockClient) Lock(ctx context.Context, resourceID string, options *LockOptions) (*LockResponse, error) {
	if options == nil {
		options = DefaultLockOptions()
	}

	req := &pb.LockRequest{
		ResourceId: resourceID,
		ClientId:   c.clientID,
	}

	if options.TtlSeconds > 0 {
		req.TtlSeconds = options.TtlSeconds
	}

	res, err := c.RaftLockClient.Lock(ctx, req)
	if err != nil {
		return nil, err
	}

	return &LockResponse{
		Success: res.Success,
		Message: res.Message,
	}, nil
}

// UnlockResponse represents the outcome of an unlock operation
type UnlockResponse struct {
	Success bool
	Message string
}

// Attempts to release a lock on the specified resource
func (c *RaftLockClient) Unlock(ctx context.Context, resourceID string) (*UnlockResponse, error) {
	req := &pb.UnlockRequest{
		ResourceId: resourceID,
		ClientId:   c.clientID,
	}

	res, err := c.RaftLockClient.Unlock(ctx, req)
	if err != nil {
		return nil, err
	}

	return &UnlockResponse{
		Success: res.Success,
		Message: res.Message,
	}, nil
}
