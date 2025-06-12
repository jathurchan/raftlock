package client

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNewAdvancedClient(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: Config{
				Endpoints: []string{"localhost:8080"},
			},
			expectError: false,
		},
		{
			name: "empty endpoints",
			config: Config{
				Endpoints: []string{},
			},
			expectError: true,
			errorMsg:    "at least one endpoint must be provided",
		},
		{
			name: "nil endpoints",
			config: Config{
				Endpoints: nil,
			},
			expectError: true,
			errorMsg:    "at least one endpoint must be provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewAdvancedClient(tt.config)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("expected error message %q, got %q", tt.errorMsg, err.Error())
				}
				if client != nil {
					t.Errorf("expected nil client on error, got %v", client)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if client == nil {
					t.Errorf("expected non-nil client")
				}
				// Clean up
				if client != nil {
					_ = client.Close()
				}
			}
		})
	}
}

func TestAdvancedClient_EnqueueWaiter(t *testing.T) {
	tests := []struct {
		name        string
		req         *EnqueueWaiterRequest
		mockResp    *pb.EnqueueWaiterResponse
		mockErr     error
		expectError bool
		errorMsg    string
	}{
		{
			name: "successful enqueue",
			req: &EnqueueWaiterRequest{
				LockID:   "test-lock",
				ClientID: "test-client",
				Timeout:  30 * time.Second,
				Priority: 1,
				Version:  123,
			},
			mockResp: &pb.EnqueueWaiterResponse{
				Enqueued:              true,
				Position:              2,
				EstimatedWaitDuration: durationpb.New(10 * time.Second),
			},
			expectError: false,
		},
		{
			name:        "nil request",
			req:         nil,
			expectError: true,
			errorMsg:    "request cannot be nil",
		},
		{
			name: "empty lock ID",
			req: &EnqueueWaiterRequest{
				LockID:   "",
				ClientID: "test-client",
				Timeout:  30 * time.Second,
			},
			expectError: true,
			errorMsg:    "lock ID cannot be empty",
		},
		{
			name: "empty client ID",
			req: &EnqueueWaiterRequest{
				LockID:   "test-lock",
				ClientID: "",
				Timeout:  30 * time.Second,
			},
			expectError: true,
			errorMsg:    "client ID cannot be empty",
		},
		{
			name: "zero timeout",
			req: &EnqueueWaiterRequest{
				LockID:   "test-lock",
				ClientID: "test-client",
				Timeout:  0,
			},
			expectError: true,
			errorMsg:    "timeout must be positive",
		},
		{
			name: "negative timeout",
			req: &EnqueueWaiterRequest{
				LockID:   "test-lock",
				ClientID: "test-client",
				Timeout:  -1 * time.Second,
			},
			expectError: true,
			errorMsg:    "timeout must be positive",
		},
		{
			name: "RPC error",
			req: &EnqueueWaiterRequest{
				LockID:   "test-lock",
				ClientID: "test-client",
				Timeout:  30 * time.Second,
			},
			mockErr:     errors.New("network error"),
			expectError: true,
			errorMsg:    "failed to enqueue waiter for lock test-lock: operation \"EnqueueWaiter\" failed after 3 attempts: enqueue waiter RPC failed: network error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultClientConfig()
			config.Endpoints = []string{"localhost:8080"}
			config.RetryPolicy.MaxRetries = 3

			client, _, _, _ := setupTestClient(config)
			advClient := &advancedClient{baseClient: client}

			// Set up mock behavior
			if !tt.expectError || tt.mockErr != nil {
				client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
					mockClient := &mockRaftLockClient{
						enqueueWaiterFunc: func(ctx context.Context, req *pb.EnqueueWaiterRequest, opts ...grpc.CallOption) (*pb.EnqueueWaiterResponse, error) {
							if tt.mockErr != nil {
								return nil, tt.mockErr
							}
							return tt.mockResp, nil
						},
					}
					return fn(ctx, mockClient)
				}
			}

			ctx := context.Background()
			result, err := advClient.EnqueueWaiter(ctx, tt.req)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("expected error message %q, got %q", tt.errorMsg, err.Error())
				}
				if result != nil {
					t.Errorf("expected nil result on error, got %v", result)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result == nil {
					t.Errorf("expected non-nil result")
				} else {
					if result.Enqueued != tt.mockResp.Enqueued {
						t.Errorf("expected Enqueued %v, got %v", tt.mockResp.Enqueued, result.Enqueued)
					}
					if result.Position != tt.mockResp.Position {
						t.Errorf("expected Position %d, got %d", tt.mockResp.Position, result.Position)
					}
				}
			}
		})
	}
}

func TestAdvancedClient_CancelWait(t *testing.T) {
	tests := []struct {
		name        string
		req         *CancelWaitRequest
		mockResp    *pb.CancelWaitResponse
		mockErr     error
		expectError bool
		errorMsg    string
	}{
		{
			name: "successful cancel",
			req: &CancelWaitRequest{
				LockID:   "test-lock",
				ClientID: "test-client",
				Version:  123,
			},
			mockResp: &pb.CancelWaitResponse{
				Cancelled: true,
			},
			expectError: false,
		},
		{
			name:        "nil request",
			req:         nil,
			expectError: true,
			errorMsg:    "request cannot be nil",
		},
		{
			name: "empty lock ID",
			req: &CancelWaitRequest{
				LockID:   "",
				ClientID: "test-client",
				Version:  123,
			},
			expectError: true,
			errorMsg:    "lock ID cannot be empty",
		},
		{
			name: "empty client ID",
			req: &CancelWaitRequest{
				LockID:   "test-lock",
				ClientID: "",
				Version:  123,
			},
			expectError: true,
			errorMsg:    "client ID cannot be empty",
		},
		{
			name: "RPC error",
			req: &CancelWaitRequest{
				LockID:   "test-lock",
				ClientID: "test-client",
				Version:  123,
			},
			mockErr:     errors.New("network error"),
			expectError: true,
			errorMsg:    "failed to cancel wait for lock test-lock: operation \"CancelWait\" failed after 3 attempts: cancel wait RPC failed: network error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultClientConfig()
			config.Endpoints = []string{"localhost:8080"}
			config.RetryPolicy.MaxRetries = 3

			client, _, _, _ := setupTestClient(config)
			advClient := &advancedClient{baseClient: client}

			// Set up mock behavior
			if !tt.expectError || tt.mockErr != nil {
				client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
					mockClient := &mockRaftLockClient{
						cancelWaitFunc: func(ctx context.Context, req *pb.CancelWaitRequest, opts ...grpc.CallOption) (*pb.CancelWaitResponse, error) {
							if tt.mockErr != nil {
								return nil, tt.mockErr
							}
							return tt.mockResp, nil
						},
					}
					return fn(ctx, mockClient)
				}
			}

			ctx := context.Background()
			result, err := advClient.CancelWait(ctx, tt.req)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("expected error message %q, got %q", tt.errorMsg, err.Error())
				}
				if result != nil {
					t.Errorf("expected nil result on error, got %v", result)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result == nil {
					t.Errorf("expected non-nil result")
				} else {
					if result.Cancelled != tt.mockResp.Cancelled {
						t.Errorf("expected Cancelled %v, got %v", tt.mockResp.Cancelled, result.Cancelled)
					}
				}
			}
		})
	}
}

func TestAdvancedClient_GetLeaderAddress(t *testing.T) {
	config := DefaultClientConfig()
	config.Endpoints = []string{"localhost:8080"}

	client, _, _, _ := setupTestClient(config)
	advClient := &advancedClient{baseClient: client}

	// Initially should be empty
	leader := advClient.GetLeaderAddress()
	if leader != "" {
		t.Errorf("expected empty leader address initially, got %q", leader)
	}

	// Set a leader and verify
	client.setCurrentLeader("leader:8081")
	leader = advClient.GetLeaderAddress()
	if leader != "leader:8081" {
		t.Errorf("expected leader address 'leader:8081', got %q", leader)
	}
}

func TestAdvancedClient_IsConnected(t *testing.T) {
	config := DefaultClientConfig()
	config.Endpoints = []string{"localhost:8080"}

	client, _, _, _ := setupTestClient(config)
	advClient := &advancedClient{baseClient: client}

	// Initially should report as not connected (no connections established)
	connected := advClient.IsConnected()
	if connected {
		t.Errorf("expected not connected initially, got connected")
	}
}

func TestAdvancedClient_SetRetryPolicy(t *testing.T) {
	config := DefaultClientConfig()
	config.Endpoints = []string{"localhost:8080"}

	client, _, _, _ := setupTestClient(config)
	advClient := &advancedClient{baseClient: client}

	// Set a new retry policy
	newPolicy := RetryPolicy{
		MaxRetries:        5,
		InitialBackoff:    200 * time.Millisecond,
		MaxBackoff:        10 * time.Second,
		BackoffMultiplier: 3.0,
		JitterFactor:      0.2,
	}

	advClient.SetRetryPolicy(newPolicy)

	// Verify the policy was set (we can check this through the base client)
	client.mu.RLock()
	currentPolicy := client.config.RetryPolicy
	client.mu.RUnlock()

	if currentPolicy.MaxRetries != newPolicy.MaxRetries {
		t.Errorf("expected MaxRetries %d, got %d", newPolicy.MaxRetries, currentPolicy.MaxRetries)
	}
	if currentPolicy.InitialBackoff != newPolicy.InitialBackoff {
		t.Errorf("expected InitialBackoff %v, got %v", newPolicy.InitialBackoff, currentPolicy.InitialBackoff)
	}
	if currentPolicy.MaxBackoff != newPolicy.MaxBackoff {
		t.Errorf("expected MaxBackoff %v, got %v", newPolicy.MaxBackoff, currentPolicy.MaxBackoff)
	}
	if currentPolicy.BackoffMultiplier != newPolicy.BackoffMultiplier {
		t.Errorf("expected BackoffMultiplier %f, got %f", newPolicy.BackoffMultiplier, currentPolicy.BackoffMultiplier)
	}
	if currentPolicy.JitterFactor != newPolicy.JitterFactor {
		t.Errorf("expected JitterFactor %f, got %f", newPolicy.JitterFactor, currentPolicy.JitterFactor)
	}
}

func TestAdvancedClient_GetMetrics(t *testing.T) {
	tests := []struct {
		name          string
		enableMetrics bool
		expectedType  string
	}{
		{
			name:          "metrics enabled",
			enableMetrics: true,
			expectedType:  "*client.metrics",
		},
		{
			name:          "metrics disabled",
			enableMetrics: false,
			expectedType:  "*client.noOpMetrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultClientConfig()
			config.Endpoints = []string{"localhost:8080"}
			config.EnableMetrics = tt.enableMetrics

			client, _, _, _ := setupTestClient(config)
			advClient := &advancedClient{baseClient: client}

			metrics := advClient.GetMetrics()
			if metrics == nil {
				t.Errorf("expected non-nil metrics")
			}

			// Verify metrics interface works
			metrics.IncrSuccess("test")
			metrics.IncrFailure("test")
			metrics.ObserveLatency("test", time.Millisecond)
		})
	}
}

func TestAdvancedClient_Close(t *testing.T) {
	tests := []struct {
		name        string
		closeError  error
		expectError bool
	}{
		{
			name:        "successful close",
			closeError:  nil,
			expectError: false,
		},
		{
			name:        "close with error",
			closeError:  errors.New("close error"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBase := &mockBaseClient{
				closeFunc: func() error {
					return tt.closeError
				},
			}
			advClient := &advancedClient{baseClient: mockBase}

			err := advClient.Close()

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}
