package client

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidationError(t *testing.T) {
	tests := []struct {
		name          string
		field         string
		value         any
		message       string
		expectedError string
	}{
		{
			name:          "string field validation error",
			field:         "lockID",
			value:         "too-long-lock-id",
			message:       "exceeds maximum length",
			expectedError: "validation error for field 'lockID': exceeds maximum length",
		},
		{
			name:          "nil value validation error",
			field:         "request",
			value:         nil,
			message:       "request cannot be nil",
			expectedError: "validation error for field 'request': request cannot be nil",
		},
		{
			name:          "numeric field validation error",
			field:         "timeout",
			value:         1000,
			message:       "timeout too small",
			expectedError: "validation error for field 'timeout': timeout too small",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewValidationError(tt.field, tt.value, tt.message)

			if err == nil {
				t.Fatal("NewValidationError() returned nil")
			}

			if err.Field != tt.field {
				t.Errorf("Field = %v, want %v", err.Field, tt.field)
			}

			if err.Value != tt.value {
				t.Errorf("Value = %v, want %v", err.Value, tt.value)
			}

			if err.Message != tt.message {
				t.Errorf("Message = %v, want %v", err.Message, tt.message)
			}

			if err.Error() != tt.expectedError {
				t.Errorf("Error() = %v, want %v", err.Error(), tt.expectedError)
			}
		})
	}
}

func TestValidateAdminConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorField  string
		errorMsg    string
	}{
		{
			name: "valid config",
			config: Config{
				Endpoints:      []string{"localhost:8080"},
				RequestTimeout: 10000,
			},
			expectError: false,
		},
		{
			name: "valid config with default timeout",
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
			errorField:  "endpoints",
			errorMsg:    "at least one endpoint must be provided",
		},
		{
			name: "nil endpoints",
			config: Config{
				Endpoints: nil,
			},
			expectError: true,
			errorField:  "endpoints",
			errorMsg:    "at least one endpoint must be provided",
		},
		{
			name: "timeout too small",
			config: Config{
				Endpoints:      []string{"localhost:8080"},
				RequestTimeout: 1000, // Less than defaultMinAdminTimeout
			},
			expectError: true,
			errorField:  "requestTimeout",
			errorMsg:    "timeout must be at least 5000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAdminConfig(tt.config)

			if tt.expectError {
				if err == nil {
					t.Error("validateAdminConfig() expected error but got none")
					return
				}

				var validationErr *ValidationError
				if !errors.As(err, &validationErr) {
					t.Errorf("Expected ValidationError, got %T", err)
					return
				}

				if validationErr.Field != tt.errorField {
					t.Errorf("Error field = %v, want %v", validationErr.Field, tt.errorField)
				}

				if validationErr.Message != tt.errorMsg {
					t.Errorf("Error message = %v, want %v", validationErr.Message, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateAdminConfig() unexpected error: %v", err)
				}
			}
		})
	}
}

func TestValidateLockID(t *testing.T) {
	tests := []struct {
		name        string
		lockID      string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid lock ID",
			lockID:      "valid-lock-id",
			expectError: false,
		},
		{
			name:        "empty lock ID",
			lockID:      "",
			expectError: false, // Empty ID is allowed for general advice
		},
		{
			name:        "lock ID at max length",
			lockID:      string(make([]byte, maxLockIDLength)), // Exactly 256 characters
			expectError: false,
		},
		{
			name:        "lock ID too long",
			lockID:      string(make([]byte, maxLockIDLength+1)), // 257 characters
			expectError: true,
			errorMsg:    "lock ID exceeds max length of 256",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLockID(tt.lockID)

			if tt.expectError {
				if err == nil {
					t.Error("validateLockID() expected error but got none")
					return
				}

				var validationErr *ValidationError
				if !errors.As(err, &validationErr) {
					t.Errorf("Expected ValidationError, got %T", err)
					return
				}

				if validationErr.Field != "lockID" {
					t.Errorf("Error field = %v, want lockID", validationErr.Field)
				}

				if validationErr.Message != tt.errorMsg {
					t.Errorf("Error message = %v, want %v", validationErr.Message, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateLockID() unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNewAdminClient(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorType   string
	}{
		{
			name: "valid config",
			config: Config{
				Endpoints: []string{"localhost:8080"},
			},
			expectError: false,
		},
		{
			name: "invalid config - empty endpoints",
			config: Config{
				Endpoints: []string{},
			},
			expectError: true,
			errorType:   "validation",
		},
		{
			name: "invalid config - timeout too small",
			config: Config{
				Endpoints:      []string{"localhost:8080"},
				RequestTimeout: 1000,
			},
			expectError: true,
			errorType:   "validation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewAdminClient(tt.config)

			if tt.expectError {
				if err == nil {
					t.Error("NewAdminClient() expected error but got none")
					return
				}

				if tt.errorType == "validation" {
					var validationErr *ValidationError
					if !errors.As(err, &validationErr) {
						t.Errorf(
							"Expected ValidationError (possibly wrapped), got %T: %v",
							err,
							err,
						)
					}
				}

				if client != nil {
					t.Error("NewAdminClient() expected nil client on error")
				}
			} else {
				if err != nil {
					t.Errorf("NewAdminClient() unexpected error: %v", err)
				}

				if client == nil {
					t.Error("NewAdminClient() expected client but got nil")
				}
			}
		})
	}
}

func TestAdminClient_GetStatus(t *testing.T) {
	tests := []struct {
		name          string
		request       *GetStatusRequest
		mockResponse  *pb.GetStatusResponse
		mockError     error
		expectError   bool
		errorContains string
	}{
		{
			name: "successful get status",
			request: &GetStatusRequest{
				IncludeReplicationDetails: true,
			},
			mockResponse: &pb.GetStatusResponse{
				RaftStatus: &pb.RaftStatus{
					NodeId:   "node-1",
					Role:     "Leader",
					Term:     5,
					LeaderId: "node-1",
				},
				LockStats: &pb.LockManagerStats{
					ActiveLocksCount:  10,
					TotalWaitersCount: 5,
				},
			},
			expectError: false,
		},
		{
			name:          "nil request",
			request:       nil,
			expectError:   true,
			errorContains: "request cannot be nil",
		},
		{
			name:          "rpc error",
			request:       &GetStatusRequest{},
			mockError:     errors.New("connection failed"),
			expectError:   true,
			errorContains: "GetStatus failed",
		},
		{
			name:          "nil response",
			request:       &GetStatusRequest{},
			mockResponse:  nil,
			expectError:   true,
			errorContains: "GetStatus failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBase := &mockBaseClient{
				executeWithRetryFunc: func(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error {
					if tt.mockError != nil {
						return tt.mockError
					}

					mockClient := &adminMockClient{
						getStatusFunc: func(ctx context.Context, req *pb.GetStatusRequest, opts ...grpc.CallOption) (*pb.GetStatusResponse, error) {
							if tt.request != nil &&
								req.IncludeReplicationDetails != tt.request.IncludeReplicationDetails {
								t.Errorf(
									"Expected IncludeReplicationDetails %v, got %v",
									tt.request.IncludeReplicationDetails,
									req.IncludeReplicationDetails,
								)
							}
							return tt.mockResponse, nil
						},
					}
					return fn(ctx, mockClient)
				},
			}

			adminClient := &adminClient{baseClient: mockBase}

			ctx := context.Background()
			result, err := adminClient.GetStatus(ctx, tt.request)

			if tt.expectError {
				if err == nil {
					t.Error("GetStatus() expected error but got none")
					return
				}

				if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}

				if result != nil {
					t.Error("GetStatus() expected nil result on error")
				}
			} else {
				if err != nil {
					t.Errorf("GetStatus() unexpected error: %v", err)
				}

				if result == nil {
					t.Error("GetStatus() expected result but got nil")
				}
			}
		})
	}
}

func TestAdminClient_Health(t *testing.T) {
	tests := []struct {
		name          string
		request       *HealthRequest
		mockResponse  *pb.HealthResponse
		mockError     error
		expectError   bool
		errorContains string
	}{
		{
			name: "successful health check",
			request: &HealthRequest{
				ServiceName: "raftlock",
			},
			mockResponse: &pb.HealthResponse{
				Status:  pb.HealthStatus_SERVING,
				Message: "Service is healthy",
				HealthInfo: &pb.HealthStatus{
					Status:              pb.HealthStatus_SERVING,
					Message:             "All systems operational",
					IsRaftLeader:        true,
					RaftLeaderAddress:   "localhost:8080",
					RaftTerm:            10,
					RaftLastApplied:     100,
					CurrentActiveLocks:  5,
					CurrentTotalWaiters: 2,
					Uptime:              durationpb.New(time.Hour),
					LastHealthCheckAt:   timestamppb.Now(),
				},
			},
			expectError: false,
		},
		{
			name: "empty service name",
			request: &HealthRequest{
				ServiceName: "",
			},
			mockResponse: &pb.HealthResponse{
				Status:  pb.HealthStatus_SERVING,
				Message: "Default health check",
			},
			expectError: false,
		},
		{
			name:          "nil request",
			request:       nil,
			expectError:   true,
			errorContains: "request cannot be nil",
		},
		{
			name:          "rpc error",
			request:       &HealthRequest{ServiceName: "test"},
			mockError:     errors.New("network error"),
			expectError:   true,
			errorContains: "Health check failed",
		},
		{
			name:          "nil response",
			request:       &HealthRequest{ServiceName: "test"},
			mockResponse:  nil,
			expectError:   true,
			errorContains: "Health check failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBase := &mockBaseClient{
				executeWithRetryFunc: func(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error {
					if tt.mockError != nil {
						return tt.mockError
					}

					mockClient := &adminMockClient{
						healthFunc: func(ctx context.Context, req *pb.HealthRequest, opts ...grpc.CallOption) (*pb.HealthResponse, error) {
							if tt.request != nil && req.ServiceName != tt.request.ServiceName {
								t.Errorf("Expected ServiceName %v, got %v",
									tt.request.ServiceName, req.ServiceName)
							}
							return tt.mockResponse, nil
						},
					}
					return fn(ctx, mockClient)
				},
			}

			adminClient := &adminClient{baseClient: mockBase}

			ctx := context.Background()
			result, err := adminClient.Health(ctx, tt.request)

			if tt.expectError {
				if err == nil {
					t.Error("Health() expected error but got none")
					return
				}

				if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}

				if result != nil {
					t.Error("Health() expected nil result on error")
				}
			} else {
				if err != nil {
					t.Errorf("Health() unexpected error: %v", err)
				}

				if result == nil {
					t.Error("Health() expected result but got nil")
				}
			}
		})
	}
}

func TestAdminClient_GetBackoffAdvice(t *testing.T) {
	tests := []struct {
		name          string
		request       *BackoffAdviceRequest
		mockResponse  *pb.BackoffAdviceResponse
		mockError     error
		expectError   bool
		errorContains string
	}{
		{
			name: "successful get backoff advice",
			request: &BackoffAdviceRequest{
				LockID: "test-lock",
			},
			mockResponse: &pb.BackoffAdviceResponse{
				Advice: &pb.BackoffAdvice{
					InitialBackoff: durationpb.New(100 * time.Millisecond),
					MaxBackoff:     durationpb.New(5 * time.Second),
					Multiplier:     2.0,
					JitterFactor:   0.1,
				},
			},
			expectError: false,
		},
		{
			name: "empty lock id",
			request: &BackoffAdviceRequest{
				LockID: "",
			},
			mockResponse: &pb.BackoffAdviceResponse{
				Advice: &pb.BackoffAdvice{
					InitialBackoff: durationpb.New(50 * time.Millisecond),
					MaxBackoff:     durationpb.New(1 * time.Second),
					Multiplier:     1.5,
					JitterFactor:   0.2,
				},
			},
			expectError: false,
		},
		{
			name: "lock id too long",
			request: &BackoffAdviceRequest{
				LockID: string(make([]byte, maxLockIDLength+1)),
			},
			expectError:   true,
			errorContains: "invalid lock ID",
		},
		{
			name:          "nil request",
			request:       nil,
			expectError:   true,
			errorContains: "request cannot be nil",
		},
		{
			name:          "rpc error",
			request:       &BackoffAdviceRequest{LockID: "test"},
			mockError:     errors.New("server unavailable"),
			expectError:   true,
			errorContains: "GetBackoffAdvice failed",
		},
		{
			name:          "nil response",
			request:       &BackoffAdviceRequest{LockID: "test"},
			mockResponse:  nil,
			expectError:   true,
			errorContains: "GetBackoffAdvice failed",
		},
		{
			name:    "response with nil advice",
			request: &BackoffAdviceRequest{LockID: "test"},
			mockResponse: &pb.BackoffAdviceResponse{
				Advice: nil,
			},
			expectError:   true,
			errorContains: "GetBackoffAdvice failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBase := &mockBaseClient{
				executeWithRetryFunc: func(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error {
					if tt.mockError != nil {
						return tt.mockError
					}

					mockClient := &adminMockClient{
						getBackoffAdviceFunc: func(ctx context.Context, req *pb.BackoffAdviceRequest, opts ...grpc.CallOption) (*pb.BackoffAdviceResponse, error) {
							if tt.request != nil && req.LockId != tt.request.LockID {
								t.Errorf("Expected LockId %v, got %v",
									tt.request.LockID, req.LockId)
							}
							return tt.mockResponse, nil
						},
					}
					return fn(ctx, mockClient)
				},
			}

			adminClient := &adminClient{baseClient: mockBase}

			ctx := context.Background()
			result, err := adminClient.GetBackoffAdvice(ctx, tt.request)

			if tt.expectError {
				if err == nil {
					t.Error("GetBackoffAdvice() expected error but got none")
					return
				}

				if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}

				if result != nil {
					t.Error("GetBackoffAdvice() expected nil result on error")
				}
			} else {
				if err != nil {
					t.Errorf("GetBackoffAdvice() unexpected error: %v", err)
				}

				if result == nil {
					t.Error("GetBackoffAdvice() expected result but got nil")
				}
			}
		})
	}
}

func TestAdminClient_Close(t *testing.T) {
	tests := []struct {
		name        string
		expectError bool
		closeError  error
	}{
		{
			name:        "successful close",
			expectError: false,
		},
		{
			name:        "close with error",
			expectError: true,
			closeError:  errors.New("close failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBase := &mockBaseClient{
				closeFunc: func() error {
					return tt.closeError
				},
			}

			adminClient := &adminClient{baseClient: mockBase}

			err := adminClient.Close()

			if tt.expectError {
				if err == nil {
					t.Error("Close() expected error but got none")
				}

				if !contains(err.Error(), "failed to close admin client") {
					t.Errorf("Expected error to contain wrapping message, got %q", err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Close() unexpected error: %v", err)
				}
			}
		})
	}
}

func TestAdminClient_ContextHandling(t *testing.T) {
	tests := []struct {
		name      string
		setupCtx  func() context.Context
		operation func(ctx context.Context, client *adminClient) error
	}{
		{
			name: "cancelled context for GetStatus",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			operation: func(ctx context.Context, client *adminClient) error {
				_, err := client.GetStatus(ctx, &GetStatusRequest{})
				return err
			},
		},
		{
			name: "cancelled context for Health",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			operation: func(ctx context.Context, client *adminClient) error {
				_, err := client.Health(ctx, &HealthRequest{})
				return err
			},
		},
		{
			name: "cancelled context for GetBackoffAdvice",
			setupCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			operation: func(ctx context.Context, client *adminClient) error {
				_, err := client.GetBackoffAdvice(ctx, &BackoffAdviceRequest{LockID: "test"})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBase := &mockBaseClient{
				executeWithRetryFunc: func(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						return nil
					}
				},
			}

			adminClient := &adminClient{baseClient: mockBase}
			ctx := tt.setupCtx()

			err := tt.operation(ctx, adminClient)
			if err == nil {
				t.Error("Expected context cancellation error but got none")
			}
		})
	}
}

func TestAdminClient_Integration(t *testing.T) {
	config := Config{
		Endpoints:      []string{"localhost:8080", "localhost:8081"},
		RequestTimeout: 10000, // Above minimum threshold
		RetryPolicy: RetryPolicy{
			MaxRetries:     3,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     1 * time.Second,
		},
	}

	adminClient, err := NewAdminClient(config)
	if err != nil {
		t.Fatalf("NewAdminClient() unexpected error: %v", err)
	}

	if adminClient == nil {
		t.Fatal("NewAdminClient() returned nil client")
	}

	err = adminClient.Close()
	if err != nil {
		t.Errorf("Close() unexpected error: %v", err)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) &&
			(s[:len(substr)] == substr ||
				s[len(s)-len(substr):] == substr ||
				containsInMiddle(s, substr))))
}

func containsInMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
