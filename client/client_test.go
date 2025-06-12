package client

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewRaftLockClient(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		config := DefaultClientConfig()
		config.Endpoints = []string{"localhost:8080"}

		client, err := NewRaftLockClient(config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client == nil {
			t.Fatal("expected non-nil client")
		}

		// Clean up
		if err := client.Close(); err != nil {
			t.Errorf("error closing client: %v", err)
		}
	})

	t.Run("invalid config", func(t *testing.T) {
		config := DefaultClientConfig()
		config.Endpoints = []string{} // Empty endpoints should cause error

		client, err := NewRaftLockClient(config)
		if err == nil {
			t.Fatal("expected error for empty endpoints")
		}
		if client != nil {
			t.Fatal("expected nil client on error")
		}
	})
}

func TestRaftLockClient_Acquire(t *testing.T) {
	config := DefaultClientConfig()
	config.Endpoints = []string{"localhost:8080"}

	client, _, clock, _ := setupTestClient(config)
	raftClient := &raftLockClient{baseClient: client}

	ctx := context.Background()
	req := &AcquireRequest{
		LockID:      "test-lock",
		ClientID:    "test-client",
		TTL:         30 * time.Second,
		Wait:        true,
		Priority:    5,
		Metadata:    map[string]string{"key": "value"},
		RequestID:   "req-123",
		WaitTimeout: 60 * time.Second,
	}

	t.Run("successful acquire", func(t *testing.T) {
		expectedResp := &pb.AcquireResponse{
			Acquired: true,
			Lock: &pb.Lock{
				LockId:     "test-lock",
				OwnerId:    "test-client",
				Version:    1,
				AcquiredAt: timestamppb.New(clock.Now()),
				ExpiresAt:  timestamppb.New(clock.Now().Add(30 * time.Second)),
				Metadata:   map[string]string{"key": "value"},
			},
		}

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				acquireFunc: func(ctx context.Context, req *pb.AcquireRequest, opts ...grpc.CallOption) (*pb.AcquireResponse, error) {
					if req.LockId != "test-lock" {
						t.Errorf("expected LockId 'test-lock', got %s", req.LockId)
					}
					if req.ClientId != "test-client" {
						t.Errorf("expected ClientId 'test-client', got %s", req.ClientId)
					}
					if !req.Wait {
						t.Error("expected Wait to be true")
					}
					if req.Priority != 5 {
						t.Errorf("expected Priority 5, got %d", req.Priority)
					}
					if req.Ttl.AsDuration() != 30*time.Second {
						t.Errorf("expected TTL 30s, got %v", req.Ttl.AsDuration())
					}
					if req.WaitTimeout.AsDuration() != 60*time.Second {
						t.Errorf("expected WaitTimeout 60s, got %v", req.WaitTimeout.AsDuration())
					}
					if req.RequestId != "req-123" {
						t.Errorf("expected RequestId 'req-123', got %s", req.RequestId)
					}
					if req.Metadata["key"] != "value" {
						t.Errorf("expected metadata key=value, got %v", req.Metadata)
					}
					return expectedResp, nil
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.Acquire(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if !result.Acquired {
			t.Error("expected lock to be acquired")
		}
		if result.Lock == nil {
			t.Error("expected non-nil lock")
		}
		if result.Lock.LockID != "test-lock" {
			t.Errorf("expected LockID 'test-lock', got %s", result.Lock.LockID)
		}
		if result.Lock.OwnerID != "test-client" {
			t.Errorf("expected OwnerID 'test-client', got %s", result.Lock.OwnerID)
		}
	})

	t.Run("acquire with error", func(t *testing.T) {
		expectedError := errors.New("lock already held")

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				acquireFunc: func(ctx context.Context, req *pb.AcquireRequest, opts ...grpc.CallOption) (*pb.AcquireResponse, error) {
					return nil, expectedError
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.Acquire(ctx, req)
		if err == nil {
			t.Fatal("expected error")
		}
		if result != nil {
			t.Error("expected nil result on error")
		}
		if !errors.Is(err, expectedError) {
			t.Errorf("expected error %v, got %v", expectedError, err)
		}
	})

	t.Run("acquire with client closed", func(t *testing.T) {
		if err := client.close(); err != nil {
			t.Fatalf("error closing client: %v", err)
		}

		result, err := raftClient.Acquire(ctx, req)
		if err == nil {
			t.Fatal("expected error for closed client")
		}
		if result != nil {
			t.Error("expected nil result for closed client")
		}
		if !errors.Is(err, ErrClientClosed) {
			t.Errorf("expected ErrClientClosed, got %v", err)
		}
	})
}

func TestRaftLockClient_Release(t *testing.T) {
	config := DefaultClientConfig()
	config.Endpoints = []string{"localhost:8080"}

	client, _, _, _ := setupTestClient(config)
	raftClient := &raftLockClient{baseClient: client}

	ctx := context.Background()
	req := &ReleaseRequest{
		LockID:   "test-lock",
		ClientID: "test-client",
		Version:  1,
	}

	t.Run("successful release", func(t *testing.T) {
		expectedResp := &pb.ReleaseResponse{
			Released:       true,
			WaiterPromoted: false,
		}

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				releaseFunc: func(ctx context.Context, req *pb.ReleaseRequest, opts ...grpc.CallOption) (*pb.ReleaseResponse, error) {
					if req.LockId != "test-lock" {
						t.Errorf("expected LockId 'test-lock', got %s", req.LockId)
					}
					if req.ClientId != "test-client" {
						t.Errorf("expected ClientId 'test-client', got %s", req.ClientId)
					}
					if req.Version != 1 {
						t.Errorf("expected Version 1, got %d", req.Version)
					}
					return expectedResp, nil
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.Release(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if !result.Released {
			t.Error("expected lock to be released")
		}
		if result.WaiterPromoted {
			t.Error("expected no waiter to be promoted")
		}
	})

	t.Run("release with waiter promoted", func(t *testing.T) {
		expectedResp := &pb.ReleaseResponse{
			Released:       true,
			WaiterPromoted: true,
		}

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				releaseFunc: func(ctx context.Context, req *pb.ReleaseRequest, opts ...grpc.CallOption) (*pb.ReleaseResponse, error) {
					return expectedResp, nil
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.Release(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result.WaiterPromoted {
			t.Error("expected waiter to be promoted")
		}
	})

	t.Run("release with error", func(t *testing.T) {
		expectedError := errors.New("not lock owner")

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				releaseFunc: func(ctx context.Context, req *pb.ReleaseRequest, opts ...grpc.CallOption) (*pb.ReleaseResponse, error) {
					return nil, expectedError
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.Release(ctx, req)
		if err == nil {
			t.Fatal("expected error")
		}
		if result != nil {
			t.Error("expected nil result on error")
		}
		if !errors.Is(err, expectedError) {
			t.Errorf("expected error %v, got %v", expectedError, err)
		}
	})
}

func TestRaftLockClient_Renew(t *testing.T) {
	config := DefaultClientConfig()
	config.Endpoints = []string{"localhost:8080"}

	client, _, clock, _ := setupTestClient(config)
	raftClient := &raftLockClient{baseClient: client}

	ctx := context.Background()
	req := &RenewRequest{
		LockID:   "test-lock",
		ClientID: "test-client",
		Version:  1,
		NewTTL:   45 * time.Second,
	}

	t.Run("successful renew", func(t *testing.T) {
		expectedResp := &pb.RenewResponse{
			Renewed: true,
			Lock: &pb.Lock{
				LockId:     "test-lock",
				OwnerId:    "test-client",
				Version:    2, // Version should increment
				AcquiredAt: timestamppb.New(clock.Now().Add(-10 * time.Second)),
				ExpiresAt:  timestamppb.New(clock.Now().Add(45 * time.Second)),
				Metadata:   map[string]string{},
			},
		}

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				renewFunc: func(ctx context.Context, req *pb.RenewRequest, opts ...grpc.CallOption) (*pb.RenewResponse, error) {
					if req.LockId != "test-lock" {
						t.Errorf("expected LockId 'test-lock', got %s", req.LockId)
					}
					if req.ClientId != "test-client" {
						t.Errorf("expected ClientId 'test-client', got %s", req.ClientId)
					}
					if req.Version != 1 {
						t.Errorf("expected Version 1, got %d", req.Version)
					}
					if req.NewTtl.AsDuration() != 45*time.Second {
						t.Errorf("expected NewTTL 45s, got %v", req.NewTtl.AsDuration())
					}
					return expectedResp, nil
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.Renew(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if !result.Renewed {
			t.Error("expected lock to be renewed")
		}
		if result.Lock == nil {
			t.Error("expected non-nil lock")
		}
		if result.Lock.Version != 2 {
			t.Errorf("expected Version 2, got %d", result.Lock.Version)
		}
	})

	t.Run("renew with error", func(t *testing.T) {
		expectedError := errors.New("version mismatch")

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				renewFunc: func(ctx context.Context, req *pb.RenewRequest, opts ...grpc.CallOption) (*pb.RenewResponse, error) {
					return nil, expectedError
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.Renew(ctx, req)
		if err == nil {
			t.Fatal("expected error")
		}
		if result != nil {
			t.Error("expected nil result on error")
		}
		if !errors.Is(err, expectedError) {
			t.Errorf("expected error %v, got %v", expectedError, err)
		}
	})
}

func TestRaftLockClient_GetLockInfo(t *testing.T) {
	config := DefaultClientConfig()
	config.Endpoints = []string{"localhost:8080"}

	client, _, clock, _ := setupTestClient(config)
	raftClient := &raftLockClient{baseClient: client}

	ctx := context.Background()
	req := &GetLockInfoRequest{
		LockID:         "test-lock",
		IncludeWaiters: true,
	}

	t.Run("successful get lock info", func(t *testing.T) {
		expectedResp := &pb.GetLockInfoResponse{
			LockInfo: &pb.LockInfo{
				LockId:      "test-lock",
				OwnerId:     "test-client",
				Version:     1,
				AcquiredAt:  timestamppb.New(clock.Now().Add(-10 * time.Second)),
				ExpiresAt:   timestamppb.New(clock.Now().Add(20 * time.Second)),
				WaiterCount: 2,
				WaitersInfo: []*pb.WaiterInfo{
					{
						ClientId:   "waiter1",
						EnqueuedAt: timestamppb.New(clock.Now().Add(-5 * time.Second)),
						TimeoutAt:  timestamppb.New(clock.Now().Add(55 * time.Second)),
						Priority:   0,
						Position:   0,
					},
					{
						ClientId:   "waiter2",
						EnqueuedAt: timestamppb.New(clock.Now().Add(-3 * time.Second)),
						TimeoutAt:  timestamppb.New(clock.Now().Add(57 * time.Second)),
						Priority:   5,
						Position:   1,
					},
				},
				Metadata:       map[string]string{"env": "test"},
				LastModifiedAt: timestamppb.New(clock.Now().Add(-1 * time.Second)),
			},
		}

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				getLockInfoFunc: func(ctx context.Context, req *pb.GetLockInfoRequest, opts ...grpc.CallOption) (*pb.GetLockInfoResponse, error) {
					if req.LockId != "test-lock" {
						t.Errorf("expected LockId 'test-lock', got %s", req.LockId)
					}
					if !req.IncludeWaiters {
						t.Error("expected IncludeWaiters to be true")
					}
					return expectedResp, nil
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.GetLockInfo(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.LockID != "test-lock" {
			t.Errorf("expected LockID 'test-lock', got %s", result.LockID)
		}
		if result.OwnerID != "test-client" {
			t.Errorf("expected OwnerID 'test-client', got %s", result.OwnerID)
		}
		if result.WaiterCount != 2 {
			t.Errorf("expected WaiterCount 2, got %d", result.WaiterCount)
		}
		if len(result.WaitersInfo) != 2 {
			t.Errorf("expected 2 waiters, got %d", len(result.WaitersInfo))
		}
		if result.Metadata["env"] != "test" {
			t.Errorf("expected metadata env=test, got %v", result.Metadata)
		}
	})

	t.Run("get lock info with error response", func(t *testing.T) {
		expectedResp := &pb.GetLockInfoResponse{
			LockInfo: nil,
			Error: &pb.ErrorDetail{
				Code:    pb.ErrorCode_LOCK_NOT_FOUND,
				Message: "lock not found",
			},
		}

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				getLockInfoFunc: func(ctx context.Context, req *pb.GetLockInfoRequest, opts ...grpc.CallOption) (*pb.GetLockInfoResponse, error) {
					return expectedResp, nil
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.GetLockInfo(ctx, req)
		if err == nil {
			t.Fatal("expected error")
		}
		if result != nil {
			t.Error("expected nil result on error")
		}
		if !errors.Is(err, ErrLockNotFound) {
			t.Errorf("expected ErrLockNotFound, got %v", err)
		}
	})

	t.Run("get lock info with RPC error", func(t *testing.T) {
		expectedError := errors.New("network error")

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				getLockInfoFunc: func(ctx context.Context, req *pb.GetLockInfoRequest, opts ...grpc.CallOption) (*pb.GetLockInfoResponse, error) {
					return nil, expectedError
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.GetLockInfo(ctx, req)
		if err == nil {
			t.Fatal("expected error")
		}
		if result != nil {
			t.Error("expected nil result on error")
		}
		if !errors.Is(err, expectedError) {
			t.Errorf("expected error %v, got %v", expectedError, err)
		}
	})
}

func TestRaftLockClient_GetLocks(t *testing.T) {
	config := DefaultClientConfig()
	config.Endpoints = []string{"localhost:8080"}

	client, _, clock, _ := setupTestClient(config)
	raftClient := &raftLockClient{baseClient: client}

	ctx := context.Background()
	req := &GetLocksRequest{
		Filter: &LockFilter{
			LockIDPattern:  "test-*",
			OwnerIDPattern: "client-*",
			OnlyHeld:       true,
			OnlyContested:  false,
		},
		Limit:          10,
		Offset:         0,
		IncludeWaiters: false,
	}

	t.Run("successful get locks", func(t *testing.T) {
		expectedResp := &pb.GetLocksResponse{
			Locks: []*pb.LockInfo{
				{
					LockId:         "test-lock-1",
					OwnerId:        "client-1",
					Version:        1,
					AcquiredAt:     timestamppb.New(clock.Now().Add(-5 * time.Second)),
					ExpiresAt:      timestamppb.New(clock.Now().Add(25 * time.Second)),
					WaiterCount:    0,
					Metadata:       map[string]string{"type": "mutex"},
					LastModifiedAt: timestamppb.New(clock.Now().Add(-5 * time.Second)),
				},
				{
					LockId:         "test-lock-2",
					OwnerId:        "client-2",
					Version:        3,
					AcquiredAt:     timestamppb.New(clock.Now().Add(-10 * time.Second)),
					ExpiresAt:      timestamppb.New(clock.Now().Add(20 * time.Second)),
					WaiterCount:    1,
					Metadata:       map[string]string{"type": "semaphore"},
					LastModifiedAt: timestamppb.New(clock.Now().Add(-2 * time.Second)),
				},
			},
			TotalMatchingFilter: 2,
			HasMore:             false,
		}

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				getLocksFunc: func(ctx context.Context, req *pb.GetLocksRequest, opts ...grpc.CallOption) (*pb.GetLocksResponse, error) {
					if req.Limit != 10 {
						t.Errorf("expected Limit 10, got %d", req.Limit)
					}
					if req.Offset != 0 {
						t.Errorf("expected Offset 0, got %d", req.Offset)
					}
					if req.IncludeWaiters {
						t.Error("expected IncludeWaiters to be false")
					}
					if req.Filter == nil {
						t.Error("expected non-nil Filter")
					} else {
						if req.Filter.LockIdPattern != "test-*" {
							t.Errorf("expected LockIdPattern 'test-*', got %s", req.Filter.LockIdPattern)
						}
						if req.Filter.OwnerIdPattern != "client-*" {
							t.Errorf("expected OwnerIdPattern 'client-*', got %s", req.Filter.OwnerIdPattern)
						}
						if !req.Filter.OnlyHeld {
							t.Error("expected OnlyHeld to be true")
						}
						if req.Filter.OnlyContested {
							t.Error("expected OnlyContested to be false")
						}
					}
					return expectedResp, nil
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.GetLocks(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if len(result.Locks) != 2 {
			t.Errorf("expected 2 locks, got %d", len(result.Locks))
		}
		if result.TotalMatching != 2 {
			t.Errorf("expected TotalMatching 2, got %d", result.TotalMatching)
		}
		if result.HasMore {
			t.Error("expected HasMore to be false")
		}

		lock1 := result.Locks[0]
		if lock1.LockID != "test-lock-1" {
			t.Errorf("expected LockID 'test-lock-1', got %s", lock1.LockID)
		}
		if lock1.OwnerID != "client-1" {
			t.Errorf("expected OwnerID 'client-1', got %s", lock1.OwnerID)
		}
		if lock1.Metadata["type"] != "mutex" {
			t.Errorf("expected metadata type=mutex, got %v", lock1.Metadata)
		}
	})

	t.Run("get locks with error", func(t *testing.T) {
		expectedError := errors.New("database error")

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mockClient := &mockRaftLockClient{
				getLocksFunc: func(ctx context.Context, req *pb.GetLocksRequest, opts ...grpc.CallOption) (*pb.GetLocksResponse, error) {
					return nil, expectedError
				},
			}
			return fn(ctx, mockClient)
		}

		result, err := raftClient.GetLocks(ctx, req)
		if err == nil {
			t.Fatal("expected error")
		}
		if result != nil {
			t.Error("expected nil result on error")
		}
		if !errors.Is(err, expectedError) {
			t.Errorf("expected error %v, got %v", expectedError, err)
		}
	})
}

func TestRaftLockClient_Close(t *testing.T) {
	config := DefaultClientConfig()
	config.Endpoints = []string{"localhost:8080"}

	client, _, _, _ := setupTestClient(config)
	raftClient := &raftLockClient{baseClient: client}

	t.Run("successful close", func(t *testing.T) {
		err := raftClient.Close()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		ctx := context.Background()
		req := &AcquireRequest{
			LockID:   "test-lock",
			ClientID: "test-client",
			TTL:      30 * time.Second,
		}

		_, err = raftClient.Acquire(ctx, req)
		if err == nil {
			t.Error("expected error after close")
		}
		if !errors.Is(err, ErrClientClosed) {
			t.Errorf("expected ErrClientClosed, got %v", err)
		}
	})

	t.Run("multiple close calls", func(t *testing.T) {
		config2 := DefaultClientConfig()
		config2.Endpoints = []string{"localhost:8080"}
		client2, _, _, _ := setupTestClient(config2)
		raftClient2 := &raftLockClient{baseClient: client2}

		err1 := raftClient2.Close()
		if err1 != nil {
			t.Errorf("first close failed: %v", err1)
		}

		err2 := raftClient2.Close()
		if err2 == nil {
			t.Error("expected error on second close")
		}
		if !errors.Is(err2, ErrClientClosed) {
			t.Errorf("expected ErrClientClosed on second close, got %v", err2)
		}
	})
}
