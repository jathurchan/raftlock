package server

import (
	"strings"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewRequestValidator(t *testing.T) {
	logger := logger.NewNoOpLogger()
	validator := NewRequestValidator(logger)

	if validator == nil {
		t.Fatal("NewRequestValidator returned nil")
	}

	var _ RequestValidator = validator
}

func TestValidateAcquireRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name      string
		req       *pb.AcquireRequest
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid minimal request",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
			},
			wantError: false,
		},
		{
			name: "valid complete request",
			req: &pb.AcquireRequest{
				LockId:      "test-lock",
				ClientId:    "test-client",
				Ttl:         durationpb.New(30 * time.Second),
				Wait:        true,
				WaitTimeout: durationpb.New(5 * time.Minute),
				Priority:    100,
				Metadata: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
				RequestId: "req-123",
			},
			wantError: false,
		},
		{
			name: "empty lock_id",
			req: &pb.AcquireRequest{
				LockId:   "",
				ClientId: "test-client",
			},
			wantError: true,
			errorMsg:  "lock_id cannot be empty",
		},
		{
			name: "empty client_id",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "",
			},
			wantError: true,
			errorMsg:  "client_id cannot be empty",
		},
		{
			name: "lock_id too long",
			req: &pb.AcquireRequest{
				LockId:   strings.Repeat("a", MaxLockIDLength+1),
				ClientId: "test-client",
			},
			wantError: true,
			errorMsg:  "lock_id must be a non-empty string",
		},
		{
			name: "client_id too long",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: strings.Repeat("a", MaxClientIDLength+1),
			},
			wantError: true,
			errorMsg:  "client_id must be a non-empty string",
		},
		{
			name: "lock_id with invalid characters",
			req: &pb.AcquireRequest{
				LockId:   "test\x00lock",
				ClientId: "test-client",
			},
			wantError: true,
			errorMsg:  "invalid characters",
		},
		{
			name: "client_id with invalid characters",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test\nclient",
			},
			wantError: true,
			errorMsg:  "invalid characters",
		},
		{
			name: "TTL too short",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Ttl:      durationpb.New(500 * time.Millisecond),
			},
			wantError: true,
			errorMsg:  "ttl must be between",
		},
		{
			name: "TTL too long",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Ttl:      durationpb.New(25 * time.Hour),
			},
			wantError: true,
			errorMsg:  "ttl must be between",
		},
		{
			name: "wait timeout too short",
			req: &pb.AcquireRequest{
				LockId:      "test-lock",
				ClientId:    "test-client",
				WaitTimeout: durationpb.New(500 * time.Millisecond),
			},
			wantError: true,
			errorMsg:  "timeout must be between",
		},
		{
			name: "wait timeout too long",
			req: &pb.AcquireRequest{
				LockId:      "test-lock",
				ClientId:    "test-client",
				WaitTimeout: durationpb.New(15 * time.Minute),
			},
			wantError: true,
			errorMsg:  "timeout must be between",
		},
		{
			name: "priority too low",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Priority: MinPriority - 1,
			},
			wantError: true,
			errorMsg:  "priority must be between",
		},
		{
			name: "priority too high",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Priority: MaxPriority + 1,
			},
			wantError: true,
			errorMsg:  "priority must be between",
		},
		{
			name: "too many metadata entries",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Metadata: func() map[string]string {
					m := make(map[string]string)
					for i := 0; i <= MaxMetadataEntries; i++ {
						m[string(rune('a'+i))] = "value"
					}
					return m
				}(),
			},
			wantError: true,
			errorMsg:  "cannot have more than",
		},
		{
			name: "metadata key too long",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Metadata: map[string]string{
					strings.Repeat("a", MaxMetadataKeyLength+1): "value",
				},
			},
			wantError: true,
			errorMsg:  "key length cannot exceed",
		},
		{
			name: "metadata value too long",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Metadata: map[string]string{
					"key": strings.Repeat("a", MaxMetadataValueLength+1),
				},
			},
			wantError: true,
			errorMsg:  "value length cannot exceed",
		},
		{
			name: "empty metadata key",
			req: &pb.AcquireRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Metadata: map[string]string{
					"": "value",
				},
			},
			wantError: true,
			errorMsg:  "metadata key cannot be empty",
		},
		{
			name: "request_id too long",
			req: &pb.AcquireRequest{
				LockId:    "test-lock",
				ClientId:  "test-client",
				RequestId: strings.Repeat("a", MaxRequestIDLength+1),
			},
			wantError: true,
			errorMsg:  "request_id length cannot exceed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateAcquireRequest(tt.req)

			if tt.wantError {
				if err == nil {
					t.Errorf("ValidateAcquireRequest() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("ValidateAcquireRequest() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateAcquireRequest() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateReleaseRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name      string
		req       *pb.ReleaseRequest
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid request",
			req: &pb.ReleaseRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  123,
			},
			wantError: false,
		},
		{
			name: "empty lock_id",
			req: &pb.ReleaseRequest{
				LockId:   "",
				ClientId: "test-client",
				Version:  123,
			},
			wantError: true,
			errorMsg:  "lock_id cannot be empty",
		},
		{
			name: "empty client_id",
			req: &pb.ReleaseRequest{
				LockId:   "test-lock",
				ClientId: "",
				Version:  123,
			},
			wantError: true,
			errorMsg:  "client_id cannot be empty",
		},
		{
			name: "zero version",
			req: &pb.ReleaseRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  0,
			},
			wantError: true,
			errorMsg:  "version must be positive",
		},
		{
			name: "negative version",
			req: &pb.ReleaseRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  -1,
			},
			wantError: true,
			errorMsg:  "version must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateReleaseRequest(tt.req)

			if tt.wantError {
				if err == nil {
					t.Errorf("ValidateReleaseRequest() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("ValidateReleaseRequest() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateReleaseRequest() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateRenewRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name      string
		req       *pb.RenewRequest
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid request",
			req: &pb.RenewRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  123,
				NewTtl:   durationpb.New(30 * time.Second),
			},
			wantError: false,
		},
		{
			name: "empty lock_id",
			req: &pb.RenewRequest{
				LockId:   "",
				ClientId: "test-client",
				Version:  123,
				NewTtl:   durationpb.New(30 * time.Second),
			},
			wantError: true,
			errorMsg:  "lock_id cannot be empty",
		},
		{
			name: "empty client_id",
			req: &pb.RenewRequest{
				LockId:   "test-lock",
				ClientId: "",
				Version:  123,
				NewTtl:   durationpb.New(30 * time.Second),
			},
			wantError: true,
			errorMsg:  "client_id cannot be empty",
		},
		{
			name: "zero version",
			req: &pb.RenewRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  0,
				NewTtl:   durationpb.New(30 * time.Second),
			},
			wantError: true,
			errorMsg:  "version must be positive",
		},
		{
			name: "nil new_ttl",
			req: &pb.RenewRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  123,
				NewTtl:   nil,
			},
			wantError: true,
			errorMsg:  "new_ttl is required",
		},
		{
			name: "invalid TTL too short",
			req: &pb.RenewRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  123,
				NewTtl:   durationpb.New(500 * time.Millisecond),
			},
			wantError: true,
			errorMsg:  "ttl must be between",
		},
		{
			name: "invalid TTL too long",
			req: &pb.RenewRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  123,
				NewTtl:   durationpb.New(25 * time.Hour),
			},
			wantError: true,
			errorMsg:  "ttl must be between",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateRenewRequest(tt.req)

			if tt.wantError {
				if err == nil {
					t.Errorf("ValidateRenewRequest() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("ValidateRenewRequest() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateRenewRequest() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateGetLockInfoRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name      string
		req       *pb.GetLockInfoRequest
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid request",
			req: &pb.GetLockInfoRequest{
				LockId:         "test-lock",
				IncludeWaiters: true,
			},
			wantError: false,
		},
		{
			name: "empty lock_id",
			req: &pb.GetLockInfoRequest{
				LockId: "",
			},
			wantError: true,
			errorMsg:  "lock_id cannot be empty",
		},
		{
			name: "lock_id too long",
			req: &pb.GetLockInfoRequest{
				LockId: strings.Repeat("a", MaxLockIDLength+1),
			},
			wantError: true,
			errorMsg:  "lock_id must be a non-empty string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateGetLockInfoRequest(tt.req)

			if tt.wantError {
				if err == nil {
					t.Errorf("ValidateGetLockInfoRequest() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("ValidateGetLockInfoRequest() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateGetLockInfoRequest() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateGetLocksRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name      string
		req       *pb.GetLocksRequest
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid minimal request",
			req: &pb.GetLocksRequest{
				Limit:  100,
				Offset: 0,
			},
			wantError: false,
		},
		{
			name: "valid request with filter",
			req: &pb.GetLocksRequest{
				Filter: &pb.LockFilter{
					LockIdPattern:  "test-*",
					OwnerIdPattern: "client-*",
					OnlyHeld:       true,
					OnlyContested:  false,
				},
				Limit:          50,
				Offset:         10,
				IncludeWaiters: true,
			},
			wantError: false,
		},
		{
			name: "negative limit",
			req: &pb.GetLocksRequest{
				Limit: -1,
			},
			wantError: true,
			errorMsg:  "limit cannot be negative",
		},
		{
			name: "limit too high",
			req: &pb.GetLocksRequest{
				Limit: MaxPageLimit + 1,
			},
			wantError: true,
			errorMsg:  "limit cannot exceed",
		},
		{
			name: "negative offset",
			req: &pb.GetLocksRequest{
				Offset: -1,
			},
			wantError: true,
			errorMsg:  "offset cannot be negative",
		},
		{
			name: "invalid filter",
			req: &pb.GetLocksRequest{
				Filter: &pb.LockFilter{
					LockIdPattern: strings.Repeat("a", MaxLockIDLength+1),
				},
			},
			wantError: true,
			errorMsg:  "lock_id_pattern too long",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateGetLocksRequest(tt.req)

			if tt.wantError {
				if err == nil {
					t.Errorf("ValidateGetLocksRequest() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("ValidateGetLocksRequest() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateGetLocksRequest() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateEnqueueWaiterRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name      string
		req       *pb.EnqueueWaiterRequest
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid request",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Timeout:  durationpb.New(5 * time.Minute),
				Priority: 100,
				Version:  123,
			},
			wantError: false,
		},
		{
			name: "empty lock_id",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "",
				ClientId: "test-client",
				Timeout:  durationpb.New(5 * time.Minute),
			},
			wantError: true,
			errorMsg:  "lock_id cannot be empty",
		},
		{
			name: "empty client_id",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "",
				Timeout:  durationpb.New(5 * time.Minute),
			},
			wantError: true,
			errorMsg:  "client_id cannot be empty",
		},
		{
			name: "nil timeout",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Timeout:  nil,
			},
			wantError: true,
			errorMsg:  "timeout is required for enqueue waiter",
		},
		{
			name: "timeout too short",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Timeout:  durationpb.New(500 * time.Millisecond),
			},
			wantError: true,
			errorMsg:  "timeout must be between",
		},
		{
			name: "timeout too long",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Timeout:  durationpb.New(15 * time.Minute),
			},
			wantError: true,
			errorMsg:  "timeout must be between",
		},
		{
			name: "priority too low",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Timeout:  durationpb.New(5 * time.Minute),
				Priority: MinPriority - 1,
			},
			wantError: true,
			errorMsg:  "priority must be between",
		},
		{
			name: "priority too high",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Timeout:  durationpb.New(5 * time.Minute),
				Priority: MaxPriority + 1,
			},
			wantError: true,
			errorMsg:  "priority must be between",
		},
		{
			name: "negative version",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Timeout:  durationpb.New(5 * time.Minute),
				Version:  -1,
			},
			wantError: true,
			errorMsg:  "version cannot be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateEnqueueWaiterRequest(tt.req)

			if tt.wantError {
				if err == nil {
					t.Errorf("ValidateEnqueueWaiterRequest() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("ValidateEnqueueWaiterRequest() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateEnqueueWaiterRequest() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateCancelWaitRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name      string
		req       *pb.CancelWaitRequest
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid request",
			req: &pb.CancelWaitRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  123,
			},
			wantError: false,
		},
		{
			name: "empty lock_id",
			req: &pb.CancelWaitRequest{
				LockId:   "",
				ClientId: "test-client",
				Version:  123,
			},
			wantError: true,
			errorMsg:  "lock_id cannot be empty",
		},
		{
			name: "empty client_id",
			req: &pb.CancelWaitRequest{
				LockId:   "test-lock",
				ClientId: "",
				Version:  123,
			},
			wantError: true,
			errorMsg:  "client_id cannot be empty",
		},
		{
			name: "negative version",
			req: &pb.CancelWaitRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  -1,
			},
			wantError: true,
			errorMsg:  "version cannot be negative",
		},
		{
			name: "zero version (valid)",
			req: &pb.CancelWaitRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Version:  0,
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateCancelWaitRequest(tt.req)

			if tt.wantError {
				if err == nil {
					t.Errorf("ValidateCancelWaitRequest() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("ValidateCancelWaitRequest() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateCancelWaitRequest() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateBackoffAdviceRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name      string
		req       *pb.BackoffAdviceRequest
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid request",
			req: &pb.BackoffAdviceRequest{
				LockId: "test-lock",
			},
			wantError: false,
		},
		{
			name: "empty lock_id",
			req: &pb.BackoffAdviceRequest{
				LockId: "",
			},
			wantError: true,
			errorMsg:  "lock_id cannot be empty",
		},
		{
			name: "lock_id too long",
			req: &pb.BackoffAdviceRequest{
				LockId: strings.Repeat("a", MaxLockIDLength+1),
			},
			wantError: true,
			errorMsg:  "lock_id must be a non-empty string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateBackoffAdviceRequest(tt.req)

			if tt.wantError {
				if err == nil {
					t.Errorf("ValidateBackoffAdviceRequest() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("ValidateBackoffAdviceRequest() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateBackoffAdviceRequest() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateGetStatusRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name string
		req  *pb.GetStatusRequest
	}{
		{
			name: "valid empty request",
			req:  &pb.GetStatusRequest{},
		},
		{
			name: "valid request with fields",
			req: &pb.GetStatusRequest{
				IncludeReplicationDetails: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateGetStatusRequest(tt.req)
			if err != nil {
				t.Errorf("ValidateGetStatusRequest() unexpected error = %v", err)
			}
		})
	}
}

func TestValidateHealthRequest(t *testing.T) {
	validator := NewRequestValidator(logger.NewNoOpLogger())

	tests := []struct {
		name string
		req  *pb.HealthRequest
	}{
		{
			name: "valid empty request",
			req:  &pb.HealthRequest{},
		},
		{
			name: "valid request with service name",
			req: &pb.HealthRequest{
				ServiceName: "raftlock",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateHealthRequest(tt.req)
			if err != nil {
				t.Errorf("ValidateHealthRequest() unexpected error = %v", err)
			}
		})
	}
}

func TestValidateLockFilter(t *testing.T) {
	validator := &requestValidator{logger: logger.NewNoOpLogger()}

	tests := []struct {
		name      string
		filter    *pb.LockFilter
		wantError bool
		errorMsg  string
	}{
		{
			name:      "empty filter",
			filter:    &pb.LockFilter{},
			wantError: false,
		},
		{
			name: "valid filter",
			filter: &pb.LockFilter{
				LockIdPattern:  "test-*",
				OwnerIdPattern: "client-*",
				OnlyHeld:       true,
				OnlyContested:  false,
				ExpiresBefore:  timestamppb.Now(),
				ExpiresAfter:   timestamppb.New(time.Now().Add(-1 * time.Hour)),
				MetadataFilter: map[string]string{
					"key1": "value1",
				},
			},
			wantError: false,
		},
		{
			name: "lock_id_pattern too long",
			filter: &pb.LockFilter{
				LockIdPattern: strings.Repeat("a", MaxLockIDLength+1),
			},
			wantError: true,
			errorMsg:  "lock_id_pattern too long",
		},
		{
			name: "owner_id_pattern too long",
			filter: &pb.LockFilter{
				OwnerIdPattern: strings.Repeat("a", MaxClientIDLength+1),
			},
			wantError: true,
			errorMsg:  "owner_id_pattern too long",
		},
		{
			name: "invalid expires_before timestamp",
			filter: &pb.LockFilter{
				ExpiresBefore: &timestamppb.Timestamp{
					Seconds: -1,
					Nanos:   -1,
				},
			},
			wantError: true,
			errorMsg:  "invalid timestamp for expires_before",
		},
		{
			name: "invalid expires_after timestamp",
			filter: &pb.LockFilter{
				ExpiresAfter: &timestamppb.Timestamp{
					Seconds: -1,
					Nanos:   -1,
				},
			},
			wantError: true,
			errorMsg:  "invalid timestamp for expires_after",
		},
		{
			name: "invalid metadata filter",
			filter: &pb.LockFilter{
				MetadataFilter: map[string]string{
					"": "value", // empty key
				},
			},
			wantError: true,
			errorMsg:  "sub-validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validateLockFilter(tt.filter)

			if tt.wantError {
				if err == nil {
					t.Errorf("validateLockFilter() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateLockFilter() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateLockFilter() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateLockID(t *testing.T) {
	validator := &requestValidator{logger: logger.NewNoOpLogger()}

	tests := []struct {
		name      string
		lockID    string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "valid lock ID",
			lockID:    "test-lock-123",
			wantError: false,
		},
		{
			name:      "valid lock ID with special chars",
			lockID:    "test_lock.123-abc",
			wantError: false,
		},
		{
			name:      "empty lock ID",
			lockID:    "",
			wantError: true,
			errorMsg:  "lock_id cannot be empty",
		},
		{
			name:      "lock ID too long",
			lockID:    strings.Repeat("a", MaxLockIDLength+1),
			wantError: true,
			errorMsg:  "lock_id must be a non-empty string",
		},
		{
			name:      "lock ID with null character",
			lockID:    "test\x00lock",
			wantError: true,
			errorMsg:  "invalid characters",
		},
		{
			name:      "lock ID with newline",
			lockID:    "test\nlock",
			wantError: true,
			errorMsg:  "invalid characters",
		},
		{
			name:      "lock ID with carriage return",
			lockID:    "test\rlock",
			wantError: true,
			errorMsg:  "invalid characters",
		},
		{
			name:      "lock ID with tab",
			lockID:    "test\tlock",
			wantError: true,
			errorMsg:  "invalid characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validateLockID(tt.lockID)

			if tt.wantError {
				if err == nil {
					t.Errorf("validateLockID() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateLockID() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateLockID() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateClientID(t *testing.T) {
	validator := &requestValidator{logger: logger.NewNoOpLogger()}

	tests := []struct {
		name      string
		clientID  string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "valid client ID",
			clientID:  "test-client-123",
			wantError: false,
		},
		{
			name:      "valid client ID with special chars",
			clientID:  "test_client.123-abc",
			wantError: false,
		},
		{
			name:      "empty client ID",
			clientID:  "",
			wantError: true,
			errorMsg:  "client_id cannot be empty",
		},
		{
			name:      "client ID too long",
			clientID:  strings.Repeat("a", MaxClientIDLength+1),
			wantError: true,
			errorMsg:  "client_id must be a non-empty string",
		},
		{
			name:      "client ID with null character",
			clientID:  "test\x00client",
			wantError: true,
			errorMsg:  "invalid characters",
		},
		{
			name:      "client ID with newline",
			clientID:  "test\nclient",
			wantError: true,
			errorMsg:  "invalid characters",
		},
		{
			name:      "client ID with carriage return",
			clientID:  "test\rclient",
			wantError: true,
			errorMsg:  "invalid characters",
		},
		{
			name:      "client ID with tab",
			clientID:  "test\tclient",
			wantError: true,
			errorMsg:  "invalid characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validateClientID(tt.clientID)

			if tt.wantError {
				if err == nil {
					t.Errorf("validateClientID() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateClientID() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateClientID() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateTTL(t *testing.T) {
	validator := &requestValidator{logger: logger.NewNoOpLogger()}

	tests := []struct {
		name      string
		ttl       time.Duration
		wantError bool
		errorMsg  string
	}{
		{
			name:      "valid TTL minimum",
			ttl:       MinLockTTL,
			wantError: false,
		},
		{
			name:      "valid TTL maximum",
			ttl:       MaxLockTTL,
			wantError: false,
		},
		{
			name:      "valid TTL in range",
			ttl:       30 * time.Second,
			wantError: false,
		},
		{
			name:      "TTL too short",
			ttl:       500 * time.Millisecond,
			wantError: true,
			errorMsg:  "ttl must be between",
		},
		{
			name:      "TTL too long",
			ttl:       25 * time.Hour,
			wantError: true,
			errorMsg:  "ttl must be between",
		},
		{
			name:      "zero TTL",
			ttl:       0,
			wantError: true,
			errorMsg:  "ttl must be between",
		},
		{
			name:      "negative TTL",
			ttl:       -1 * time.Second,
			wantError: true,
			errorMsg:  "ttl must be between",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validateTTL(tt.ttl)

			if tt.wantError {
				if err == nil {
					t.Errorf("validateTTL() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateTTL() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateTTL() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateWaitTimeout(t *testing.T) {
	validator := &requestValidator{logger: logger.NewNoOpLogger()}

	tests := []struct {
		name      string
		timeout   time.Duration
		wantError bool
		errorMsg  string
	}{
		{
			name:      "valid timeout minimum",
			timeout:   MinWaitTimeout,
			wantError: false,
		},
		{
			name:      "valid timeout maximum",
			timeout:   MaxWaitTimeout,
			wantError: false,
		},
		{
			name:      "valid timeout in range",
			timeout:   5 * time.Minute,
			wantError: false,
		},
		{
			name:      "timeout too short",
			timeout:   500 * time.Millisecond,
			wantError: true,
			errorMsg:  "timeout must be between",
		},
		{
			name:      "timeout too long",
			timeout:   15 * time.Minute,
			wantError: true,
			errorMsg:  "timeout must be between",
		},
		{
			name:      "zero timeout",
			timeout:   0,
			wantError: true,
			errorMsg:  "timeout must be between",
		},
		{
			name:      "negative timeout",
			timeout:   -1 * time.Second,
			wantError: true,
			errorMsg:  "timeout must be between",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validateWaitTimeout(tt.timeout)

			if tt.wantError {
				if err == nil {
					t.Errorf("validateWaitTimeout() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateWaitTimeout() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateWaitTimeout() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidatePriority(t *testing.T) {
	validator := &requestValidator{logger: logger.NewNoOpLogger()}

	tests := []struct {
		name      string
		priority  int32
		wantError bool
		errorMsg  string
	}{
		{
			name:      "valid priority minimum",
			priority:  MinPriority,
			wantError: false,
		},
		{
			name:      "valid priority maximum",
			priority:  MaxPriority,
			wantError: false,
		},
		{
			name:      "valid priority zero",
			priority:  0,
			wantError: false,
		},
		{
			name:      "valid priority positive",
			priority:  100,
			wantError: false,
		},
		{
			name:      "valid priority negative",
			priority:  -100,
			wantError: false,
		},
		{
			name:      "priority too low",
			priority:  MinPriority - 1,
			wantError: true,
			errorMsg:  "priority must be between",
		},
		{
			name:      "priority too high",
			priority:  MaxPriority + 1,
			wantError: true,
			errorMsg:  "priority must be between",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validatePriority(tt.priority)

			if tt.wantError {
				if err == nil {
					t.Errorf("validatePriority() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("validatePriority() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validatePriority() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateMetadata(t *testing.T) {
	validator := &requestValidator{logger: logger.NewNoOpLogger()}

	tests := []struct {
		name      string
		metadata  map[string]string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "nil metadata",
			metadata:  nil,
			wantError: false,
		},
		{
			name:      "empty metadata",
			metadata:  map[string]string{},
			wantError: false,
		},
		{
			name: "valid metadata",
			metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			wantError: false,
		},
		{
			name: "metadata at max entries",
			metadata: func() map[string]string {
				m := make(map[string]string)
				for i := 0; i < MaxMetadataEntries; i++ {
					m[string(rune('a'+i))] = "value"
				}
				return m
			}(),
			wantError: false,
		},
		{
			name: "too many metadata entries",
			metadata: func() map[string]string {
				m := make(map[string]string)
				for i := 0; i <= MaxMetadataEntries; i++ {
					m[string(rune('a'+i))] = "value"
				}
				return m
			}(),
			wantError: true,
			errorMsg:  "cannot have more than",
		},
		{
			name: "empty metadata key",
			metadata: map[string]string{
				"": "value",
			},
			wantError: true,
			errorMsg:  "metadata key cannot be empty",
		},
		{
			name: "metadata key too long",
			metadata: map[string]string{
				strings.Repeat("a", MaxMetadataKeyLength+1): "value",
			},
			wantError: true,
			errorMsg:  "key length cannot exceed",
		},
		{
			name: "metadata value too long",
			metadata: map[string]string{
				"key": strings.Repeat("a", MaxMetadataValueLength+1),
			},
			wantError: true,
			errorMsg:  "value length cannot exceed",
		},
		{
			name: "metadata key at max length",
			metadata: map[string]string{
				strings.Repeat("a", MaxMetadataKeyLength): "value",
			},
			wantError: false,
		},
		{
			name: "metadata value at max length",
			metadata: map[string]string{
				"key": strings.Repeat("a", MaxMetadataValueLength),
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validateMetadata(tt.metadata)

			if tt.wantError {
				if err == nil {
					t.Errorf("validateMetadata() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateMetadata() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateMetadata() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateRequestID(t *testing.T) {
	validator := &requestValidator{logger: logger.NewNoOpLogger()}

	tests := []struct {
		name      string
		requestID string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "empty request ID",
			requestID: "",
			wantError: false,
		},
		{
			name:      "valid request ID",
			requestID: "req-123-abc",
			wantError: false,
		},
		{
			name:      "request ID at max length",
			requestID: strings.Repeat("a", MaxRequestIDLength),
			wantError: false,
		},
		{
			name:      "request ID too long",
			requestID: strings.Repeat("a", MaxRequestIDLength+1),
			wantError: true,
			errorMsg:  "request_id length cannot exceed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validateRequestID(tt.requestID)

			if tt.wantError {
				if err == nil {
					t.Errorf("validateRequestID() expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateRequestID() error = %v, want error containing %q", err, tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateRequestID() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidationError(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		value    any
		message  string
		expected string
	}{
		{
			name:     "string value",
			field:    "lock_id",
			value:    "test-lock",
			message:  "lock_id cannot be empty",
			expected: "server: validation error for field 'lock_id' (value: test-lock): lock_id cannot be empty",
		},
		{
			name:     "integer value",
			field:    "priority",
			value:    1500,
			message:  "priority out of range",
			expected: "server: validation error for field 'priority' (value: 1500): priority out of range",
		},
		{
			name:     "nil value",
			field:    "ttl",
			value:    nil,
			message:  "ttl is required",
			expected: "server: validation error for field 'ttl' (value: <nil>): ttl is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewValidationError(tt.field, tt.value, tt.message)

			if err.Field != tt.field {
				t.Errorf("ValidationError.Field = %v, want %v", err.Field, tt.field)
			}
			if err.Value != tt.value {
				t.Errorf("ValidationError.Value = %v, want %v", err.Value, tt.value)
			}
			if err.Message != tt.message {
				t.Errorf("ValidationError.Message = %v, want %v", err.Message, tt.message)
			}
			if err.Error() != tt.expected {
				t.Errorf("ValidationError.Error() = %v, want %v", err.Error(), tt.expected)
			}
		})
	}
}
