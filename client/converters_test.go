package client

import (
	"reflect"
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func mustTimestamp(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

func mustDuration(d time.Duration) *durationpb.Duration {
	return durationpb.New(d)
}

func assertTimeEqual(t *testing.T, expected, actual time.Time, fieldName string) {
	t.Helper()
	if !actual.Equal(expected) {
		t.Errorf("%s: expected %v, got %v", fieldName, expected, actual)
	}
}

func assertStringMapEqual(t *testing.T, expected, actual map[string]string, fieldName string) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("%s: expected %v, got %v", fieldName, expected, actual)
	}
}

func TestProtoToLock(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.Lock
		expected *Lock
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "complete lock with all fields",
			input: &pb.Lock{
				LockId:     "test-lock-123",
				OwnerId:    "client-456",
				Version:    42,
				AcquiredAt: mustTimestamp(baseTime),
				ExpiresAt:  mustTimestamp(baseTime.Add(30 * time.Second)),
				Metadata: map[string]string{
					"environment": "production",
					"service":     "api-gateway",
					"region":      "us-west-2",
				},
			},
			expected: &Lock{
				LockID:     "test-lock-123",
				OwnerID:    "client-456",
				Version:    42,
				AcquiredAt: baseTime,
				ExpiresAt:  baseTime.Add(30 * time.Second),
				Metadata: map[string]string{
					"environment": "production",
					"service":     "api-gateway",
					"region":      "us-west-2",
				},
			},
		},
		{
			name: "lock with nil metadata",
			input: &pb.Lock{
				LockId:     "test-lock",
				OwnerId:    "test-owner",
				Version:    1,
				AcquiredAt: mustTimestamp(baseTime),
				ExpiresAt:  mustTimestamp(baseTime.Add(time.Minute)),
				Metadata:   nil,
			},
			expected: &Lock{
				LockID:     "test-lock",
				OwnerID:    "test-owner",
				Version:    1,
				AcquiredAt: baseTime,
				ExpiresAt:  baseTime.Add(time.Minute),
				Metadata:   nil,
			},
		},
		{
			name: "lock with negative version (edge case)",
			input: &pb.Lock{
				LockId:     "negative-version-lock",
				OwnerId:    "test-client",
				Version:    -1,
				AcquiredAt: mustTimestamp(baseTime),
				ExpiresAt:  mustTimestamp(baseTime.Add(time.Hour)),
				Metadata:   map[string]string{"test": "value"},
			},
			expected: &Lock{
				LockID:     "negative-version-lock",
				OwnerID:    "test-client",
				Version:    -1,
				AcquiredAt: baseTime,
				ExpiresAt:  baseTime.Add(time.Hour),
				Metadata:   map[string]string{"test": "value"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToLock(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.LockID != tt.expected.LockID {
				t.Errorf("LockID: expected %q, got %q", tt.expected.LockID, result.LockID)
			}
			if result.OwnerID != tt.expected.OwnerID {
				t.Errorf("OwnerID: expected %q, got %q", tt.expected.OwnerID, result.OwnerID)
			}
			if result.Version != tt.expected.Version {
				t.Errorf("Version: expected %d, got %d", tt.expected.Version, result.Version)
			}

			assertTimeEqual(t, tt.expected.AcquiredAt, result.AcquiredAt, "AcquiredAt")
			assertTimeEqual(t, tt.expected.ExpiresAt, result.ExpiresAt, "ExpiresAt")
			assertStringMapEqual(t, tt.expected.Metadata, result.Metadata, "Metadata")
		})
	}
}

func TestProtoToLockInfo(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.LockInfo
		expected *LockInfo
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "lock info with multiple waiters",
			input: &pb.LockInfo{
				LockId:      "contested-lock",
				OwnerId:     "current-owner",
				Version:     15,
				AcquiredAt:  mustTimestamp(baseTime),
				ExpiresAt:   mustTimestamp(baseTime.Add(2 * time.Minute)),
				WaiterCount: 3,
				WaitersInfo: []*pb.WaiterInfo{
					{
						ClientId:   "waiter-1",
						EnqueuedAt: mustTimestamp(baseTime.Add(10 * time.Second)),
						TimeoutAt:  mustTimestamp(baseTime.Add(5 * time.Minute)),
						Priority:   5,
						Position:   0,
					},
					{
						ClientId:   "waiter-2",
						EnqueuedAt: mustTimestamp(baseTime.Add(20 * time.Second)),
						TimeoutAt:  mustTimestamp(baseTime.Add(3 * time.Minute)),
						Priority:   0,
						Position:   1,
					},
				},
				Metadata: map[string]string{
					"operation": "critical-update",
				},
				LastModifiedAt: mustTimestamp(baseTime.Add(30 * time.Second)),
			},
			expected: &LockInfo{
				LockID:      "contested-lock",
				OwnerID:     "current-owner",
				Version:     15,
				AcquiredAt:  baseTime,
				ExpiresAt:   baseTime.Add(2 * time.Minute),
				WaiterCount: 3,
				WaitersInfo: []*WaiterInfo{
					{
						ClientID:   "waiter-1",
						EnqueuedAt: baseTime.Add(10 * time.Second),
						TimeoutAt:  baseTime.Add(5 * time.Minute),
						Priority:   5,
						Position:   0,
					},
					{
						ClientID:   "waiter-2",
						EnqueuedAt: baseTime.Add(20 * time.Second),
						TimeoutAt:  baseTime.Add(3 * time.Minute),
						Priority:   0,
						Position:   1,
					},
				},
				Metadata: map[string]string{
					"operation": "critical-update",
				},
				LastModifiedAt: baseTime.Add(30 * time.Second),
			},
		},
		{
			name: "uncontested lock without waiters",
			input: &pb.LockInfo{
				LockId:         "simple-lock",
				OwnerId:        "sole-owner",
				Version:        1,
				AcquiredAt:     mustTimestamp(baseTime),
				ExpiresAt:      mustTimestamp(baseTime.Add(time.Hour)),
				WaiterCount:    0,
				WaitersInfo:    nil,
				Metadata:       nil,
				LastModifiedAt: mustTimestamp(baseTime),
			},
			expected: &LockInfo{
				LockID:         "simple-lock",
				OwnerID:        "sole-owner",
				Version:        1,
				AcquiredAt:     baseTime,
				ExpiresAt:      baseTime.Add(time.Hour),
				WaiterCount:    0,
				WaitersInfo:    []*WaiterInfo{}, // Note: converter creates empty slice for nil
				Metadata:       nil,
				LastModifiedAt: baseTime,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToLockInfo(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.LockID != tt.expected.LockID {
				t.Errorf("LockID: expected %q, got %q", tt.expected.LockID, result.LockID)
			}
			if result.OwnerID != tt.expected.OwnerID {
				t.Errorf("OwnerID: expected %q, got %q", tt.expected.OwnerID, result.OwnerID)
			}
			if result.WaiterCount != tt.expected.WaiterCount {
				t.Errorf("WaiterCount: expected %d, got %d", tt.expected.WaiterCount, result.WaiterCount)
			}

			assertTimeEqual(t, tt.expected.AcquiredAt, result.AcquiredAt, "AcquiredAt")
			assertTimeEqual(t, tt.expected.ExpiresAt, result.ExpiresAt, "ExpiresAt")
			assertTimeEqual(t, tt.expected.LastModifiedAt, result.LastModifiedAt, "LastModifiedAt")

			assertStringMapEqual(t, tt.expected.Metadata, result.Metadata, "Metadata")

			if len(result.WaitersInfo) != len(tt.expected.WaitersInfo) {
				t.Errorf("WaitersInfo length: expected %d, got %d",
					len(tt.expected.WaitersInfo), len(result.WaitersInfo))
			}
		})
	}
}

func TestProtoToAcquireResult(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.AcquireResponse
		expected *AcquireResult
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "successful immediate acquisition",
			input: &pb.AcquireResponse{
				Acquired: true,
				Lock: &pb.Lock{
					LockId:     "immediate-lock",
					OwnerId:    "client-123",
					Version:    1,
					AcquiredAt: mustTimestamp(baseTime),
					ExpiresAt:  mustTimestamp(baseTime.Add(30 * time.Second)),
				},
			},
			expected: &AcquireResult{
				Acquired: true,
				Lock: &Lock{
					LockID:     "immediate-lock",
					OwnerID:    "client-123",
					Version:    1,
					AcquiredAt: baseTime,
					ExpiresAt:  baseTime.Add(30 * time.Second),
				},
			},
		},
		{
			name: "failed acquisition with backoff advice",
			input: &pb.AcquireResponse{
				Acquired: false,
				BackoffAdvice: &pb.BackoffAdvice{
					InitialBackoff: mustDuration(200 * time.Millisecond),
					MaxBackoff:     mustDuration(10 * time.Second),
					Multiplier:     1.5,
					JitterFactor:   0.2,
				},
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_LOCK_HELD,
					Message: "Lock is currently held",
				},
			},
			expected: &AcquireResult{
				Acquired: false,
				BackoffAdvice: &BackoffAdvice{
					InitialBackoff: 200 * time.Millisecond,
					MaxBackoff:     10 * time.Second,
					Multiplier:     1.5,
					JitterFactor:   0.2,
				},
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_LOCK_HELD,
					Message: "Lock is currently held",
				},
			},
		},
		{
			name: "client enqueued in wait queue",
			input: &pb.AcquireResponse{
				Acquired:              false,
				QueuePosition:         3,
				EstimatedWaitDuration: mustDuration(2 * time.Minute),
			},
			expected: &AcquireResult{
				Acquired:              false,
				QueuePosition:         3,
				EstimatedWaitDuration: 2 * time.Minute,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToAcquireResult(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.Acquired != tt.expected.Acquired {
				t.Errorf("Acquired: expected %v, got %v", tt.expected.Acquired, result.Acquired)
			}
			if result.QueuePosition != tt.expected.QueuePosition {
				t.Errorf("QueuePosition: expected %d, got %d", tt.expected.QueuePosition, result.QueuePosition)
			}
			if result.EstimatedWaitDuration != tt.expected.EstimatedWaitDuration {
				t.Errorf("EstimatedWaitDuration: expected %v, got %v",
					tt.expected.EstimatedWaitDuration, result.EstimatedWaitDuration)
			}

			if (tt.expected.Lock == nil) != (result.Lock == nil) {
				t.Errorf("Lock presence mismatch: expected %v, got %v",
					tt.expected.Lock != nil, result.Lock != nil)
			}
			if (tt.expected.BackoffAdvice == nil) != (result.BackoffAdvice == nil) {
				t.Errorf("BackoffAdvice presence mismatch: expected %v, got %v",
					tt.expected.BackoffAdvice != nil, result.BackoffAdvice != nil)
			}
			if (tt.expected.Error == nil) != (result.Error == nil) {
				t.Errorf("Error presence mismatch: expected %v, got %v",
					tt.expected.Error != nil, result.Error != nil)
			}
		})
	}
}

func TestProtoToErrorDetail(t *testing.T) {
	tests := []struct {
		name     string
		input    *pb.ErrorDetail
		expected *ErrorDetail
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "error with detailed context",
			input: &pb.ErrorDetail{
				Code:    pb.ErrorCode_LOCK_HELD,
				Message: "Lock is currently held by another client",
				Details: map[string]string{
					"lock_id":       "user-session-123",
					"current_owner": "client-456",
				},
			},
			expected: &ErrorDetail{
				Code:    pb.ErrorCode_LOCK_HELD,
				Message: "Lock is currently held by another client",
				Details: map[string]string{
					"lock_id":       "user-session-123",
					"current_owner": "client-456",
				},
			},
		},
		{
			name: "leader redirect error",
			input: &pb.ErrorDetail{
				Code:    pb.ErrorCode_NOT_LEADER,
				Message: "This node is not the current leader",
				Details: map[string]string{
					"leader_address": "node-2.cluster.local:8080",
					"leader_id":      "node-2",
				},
			},
			expected: &ErrorDetail{
				Code:    pb.ErrorCode_NOT_LEADER,
				Message: "This node is not the current leader",
				Details: map[string]string{
					"leader_address": "node-2.cluster.local:8080",
					"leader_id":      "node-2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToErrorDetail(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.Code != tt.expected.Code {
				t.Errorf("Code: expected %v, got %v", tt.expected.Code, result.Code)
			}
			if result.Message != tt.expected.Message {
				t.Errorf("Message: expected %q, got %q", tt.expected.Message, result.Message)
			}

			assertStringMapEqual(t, tt.expected.Details, result.Details, "Details")
		})
	}
}

func TestLockFilterToProto(t *testing.T) {
	expiresBefore := time.Unix(2000, 0)
	expiresAfter := time.Unix(1000, 0)

	tests := []struct {
		name     string
		input    *LockFilter
		expected *pb.LockFilter
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "complete filter with all fields",
			input: &LockFilter{
				LockIDPattern:  "test-*",
				OwnerIDPattern: "owner-*",
				OnlyHeld:       true,
				OnlyContested:  false,
				ExpiresBefore:  &expiresBefore,
				ExpiresAfter:   &expiresAfter,
				MetadataFilter: map[string]string{
					"type": "test",
					"env":  "prod",
				},
			},
			expected: &pb.LockFilter{
				LockIdPattern:  "test-*",
				OwnerIdPattern: "owner-*",
				OnlyHeld:       true,
				OnlyContested:  false,
				ExpiresBefore:  timestamppb.New(expiresBefore),
				ExpiresAfter:   timestamppb.New(expiresAfter),
				MetadataFilter: map[string]string{
					"type": "test",
					"env":  "prod",
				},
			},
		},
		{
			name: "minimal filter with only pattern",
			input: &LockFilter{
				LockIDPattern: "simple-lock",
			},
			expected: &pb.LockFilter{
				LockIdPattern:  "simple-lock",
				OwnerIdPattern: "",
				OnlyHeld:       false,
				OnlyContested:  false,
				ExpiresBefore:  nil,
				ExpiresAfter:   nil,
				MetadataFilter: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := lockFilterToProto(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.LockIdPattern != tt.expected.LockIdPattern {
				t.Errorf("LockIdPattern: expected %q, got %q",
					tt.expected.LockIdPattern, result.LockIdPattern)
			}
			if result.OnlyHeld != tt.expected.OnlyHeld {
				t.Errorf("OnlyHeld: expected %v, got %v",
					tt.expected.OnlyHeld, result.OnlyHeld)
			}

			if (tt.expected.ExpiresBefore == nil) != (result.ExpiresBefore == nil) {
				t.Errorf("ExpiresBefore presence mismatch")
			}
			if (tt.expected.ExpiresAfter == nil) != (result.ExpiresAfter == nil) {
				t.Errorf("ExpiresAfter presence mismatch")
			}

			assertStringMapEqual(t, tt.expected.MetadataFilter, result.MetadataFilter, "MetadataFilter")
		})
	}
}

func TestProtoToReleaseResult(t *testing.T) {
	tests := []struct {
		name     string
		input    *pb.ReleaseResponse
		expected *ReleaseResult
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "successful release with waiter promoted",
			input: &pb.ReleaseResponse{
				Released:       true,
				WaiterPromoted: true,
				Error:          nil,
			},
			expected: &ReleaseResult{
				Released:       true,
				WaiterPromoted: true,
				Error:          nil,
			},
		},
		{
			name: "successful release without waiter promotion",
			input: &pb.ReleaseResponse{
				Released:       true,
				WaiterPromoted: false,
				Error:          nil,
			},
			expected: &ReleaseResult{
				Released:       true,
				WaiterPromoted: false,
				Error:          nil,
			},
		},
		{
			name: "failed release with error",
			input: &pb.ReleaseResponse{
				Released:       false,
				WaiterPromoted: false,
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_NOT_LOCK_OWNER,
					Message: "Client does not own the lock",
					Details: map[string]string{
						"lock_id":      "test-lock",
						"actual_owner": "other-client",
						"requested_by": "requesting-client",
					},
				},
			},
			expected: &ReleaseResult{
				Released:       false,
				WaiterPromoted: false,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_NOT_LOCK_OWNER,
					Message: "Client does not own the lock",
					Details: map[string]string{
						"lock_id":      "test-lock",
						"actual_owner": "other-client",
						"requested_by": "requesting-client",
					},
				},
			},
		},
		{
			name: "version mismatch error",
			input: &pb.ReleaseResponse{
				Released:       false,
				WaiterPromoted: false,
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_VERSION_MISMATCH,
					Message: "Lock version does not match",
					Details: map[string]string{
						"expected_version": "5",
						"provided_version": "3",
					},
				},
			},
			expected: &ReleaseResult{
				Released:       false,
				WaiterPromoted: false,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_VERSION_MISMATCH,
					Message: "Lock version does not match",
					Details: map[string]string{
						"expected_version": "5",
						"provided_version": "3",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToReleaseResult(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.Released != tt.expected.Released {
				t.Errorf("Released: expected %v, got %v", tt.expected.Released, result.Released)
			}
			if result.WaiterPromoted != tt.expected.WaiterPromoted {
				t.Errorf("WaiterPromoted: expected %v, got %v", tt.expected.WaiterPromoted, result.WaiterPromoted)
			}

			// Check error details
			if (tt.expected.Error == nil) != (result.Error == nil) {
				t.Errorf("Error presence mismatch: expected %v, got %v",
					tt.expected.Error != nil, result.Error != nil)
			}
			if tt.expected.Error != nil && result.Error != nil {
				if result.Error.Code != tt.expected.Error.Code {
					t.Errorf("Error.Code: expected %v, got %v", tt.expected.Error.Code, result.Error.Code)
				}
				if result.Error.Message != tt.expected.Error.Message {
					t.Errorf("Error.Message: expected %q, got %q", tt.expected.Error.Message, result.Error.Message)
				}
				assertStringMapEqual(t, tt.expected.Error.Details, result.Error.Details, "Error.Details")
			}
		})
	}
}

func TestProtoToRenewResult(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.RenewResponse
		expected *RenewResult
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "successful renewal with updated lock",
			input: &pb.RenewResponse{
				Renewed: true,
				Lock: &pb.Lock{
					LockId:     "renewable-lock",
					OwnerId:    "lock-owner",
					Version:    3,
					AcquiredAt: mustTimestamp(baseTime),
					ExpiresAt:  mustTimestamp(baseTime.Add(5 * time.Minute)), // Extended TTL
					Metadata: map[string]string{
						"operation": "data-processing",
						"renewed":   "true",
					},
				},
				Error: nil,
			},
			expected: &RenewResult{
				Renewed: true,
				Lock: &Lock{
					LockID:     "renewable-lock",
					OwnerID:    "lock-owner",
					Version:    3,
					AcquiredAt: baseTime,
					ExpiresAt:  baseTime.Add(5 * time.Minute),
					Metadata: map[string]string{
						"operation": "data-processing",
						"renewed":   "true",
					},
				},
				Error: nil,
			},
		},
		{
			name: "failed renewal due to version mismatch",
			input: &pb.RenewResponse{
				Renewed: false,
				Lock:    nil,
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_VERSION_MISMATCH,
					Message: "Lock version has changed",
					Details: map[string]string{
						"current_version":  "7",
						"provided_version": "5",
						"lock_id":          "stale-lock",
					},
				},
			},
			expected: &RenewResult{
				Renewed: false,
				Lock:    nil,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_VERSION_MISMATCH,
					Message: "Lock version has changed",
					Details: map[string]string{
						"current_version":  "7",
						"provided_version": "5",
						"lock_id":          "stale-lock",
					},
				},
			},
		},
		{
			name: "failed renewal - not lock owner",
			input: &pb.RenewResponse{
				Renewed: false,
				Lock:    nil,
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_NOT_LOCK_OWNER,
					Message: "Cannot renew lock owned by another client",
					Details: map[string]string{
						"actual_owner": "different-client",
						"requester":    "unauthorized-client",
					},
				},
			},
			expected: &RenewResult{
				Renewed: false,
				Lock:    nil,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_NOT_LOCK_OWNER,
					Message: "Cannot renew lock owned by another client",
					Details: map[string]string{
						"actual_owner": "different-client",
						"requester":    "unauthorized-client",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToRenewResult(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.Renewed != tt.expected.Renewed {
				t.Errorf("Renewed: expected %v, got %v", tt.expected.Renewed, result.Renewed)
			}

			if (tt.expected.Lock == nil) != (result.Lock == nil) {
				t.Errorf("Lock presence mismatch: expected %v, got %v",
					tt.expected.Lock != nil, result.Lock != nil)
			}
			if tt.expected.Lock != nil && result.Lock != nil {
				if result.Lock.LockID != tt.expected.Lock.LockID {
					t.Errorf("Lock.LockID: expected %q, got %q", tt.expected.Lock.LockID, result.Lock.LockID)
				}
				if result.Lock.Version != tt.expected.Lock.Version {
					t.Errorf("Lock.Version: expected %d, got %d", tt.expected.Lock.Version, result.Lock.Version)
				}
				assertTimeEqual(t, tt.expected.Lock.ExpiresAt, result.Lock.ExpiresAt, "Lock.ExpiresAt")
			}

			if (tt.expected.Error == nil) != (result.Error == nil) {
				t.Errorf("Error presence mismatch: expected %v, got %v",
					tt.expected.Error != nil, result.Error != nil)
			}
		})
	}
}

func TestProtoToGetLocksResult(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.GetLocksResponse
		expected *GetLocksResult
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "multiple locks with pagination",
			input: &pb.GetLocksResponse{
				Locks: []*pb.LockInfo{
					{
						LockId:         "lock-001",
						OwnerId:        "client-alpha",
						Version:        1,
						AcquiredAt:     mustTimestamp(baseTime),
						ExpiresAt:      mustTimestamp(baseTime.Add(time.Hour)),
						WaiterCount:    0,
						LastModifiedAt: mustTimestamp(baseTime),
					},
					{
						LockId:      "lock-002",
						OwnerId:     "client-beta",
						Version:     5,
						AcquiredAt:  mustTimestamp(baseTime.Add(10 * time.Minute)),
						ExpiresAt:   mustTimestamp(baseTime.Add(70 * time.Minute)),
						WaiterCount: 2,
						WaitersInfo: []*pb.WaiterInfo{
							{
								ClientId:   "waiter-001",
								EnqueuedAt: mustTimestamp(baseTime.Add(15 * time.Minute)),
								TimeoutAt:  mustTimestamp(baseTime.Add(25 * time.Minute)),
								Priority:   1,
								Position:   0,
							},
						},
						LastModifiedAt: mustTimestamp(baseTime.Add(15 * time.Minute)),
					},
				},
				TotalMatchingFilter: 50,
				HasMore:             true,
			},
			expected: &GetLocksResult{
				Locks: []*LockInfo{
					{
						LockID:         "lock-001",
						OwnerID:        "client-alpha",
						Version:        1,
						AcquiredAt:     baseTime,
						ExpiresAt:      baseTime.Add(time.Hour),
						WaiterCount:    0,
						WaitersInfo:    []*WaiterInfo{},
						LastModifiedAt: baseTime,
					},
					{
						LockID:      "lock-002",
						OwnerID:     "client-beta",
						Version:     5,
						AcquiredAt:  baseTime.Add(10 * time.Minute),
						ExpiresAt:   baseTime.Add(70 * time.Minute),
						WaiterCount: 2,
						WaitersInfo: []*WaiterInfo{
							{
								ClientID:   "waiter-001",
								EnqueuedAt: baseTime.Add(15 * time.Minute),
								TimeoutAt:  baseTime.Add(25 * time.Minute),
								Priority:   1,
								Position:   0,
							},
						},
						LastModifiedAt: baseTime.Add(15 * time.Minute),
					},
				},
				TotalMatching: 50,
				HasMore:       true,
			},
		},
		{
			name: "empty result set",
			input: &pb.GetLocksResponse{
				Locks:               []*pb.LockInfo{},
				TotalMatchingFilter: 0,
				HasMore:             false,
			},
			expected: &GetLocksResult{
				Locks:         []*LockInfo{},
				TotalMatching: 0,
				HasMore:       false,
			},
		},
		{
			name: "single lock result",
			input: &pb.GetLocksResponse{
				Locks: []*pb.LockInfo{
					{
						LockId:         "single-lock",
						OwnerId:        "sole-owner",
						Version:        1,
						AcquiredAt:     mustTimestamp(baseTime),
						ExpiresAt:      mustTimestamp(baseTime.Add(30 * time.Minute)),
						WaiterCount:    0,
						LastModifiedAt: mustTimestamp(baseTime),
						Metadata: map[string]string{
							"category": "exclusive",
						},
					},
				},
				TotalMatchingFilter: 1,
				HasMore:             false,
			},
			expected: &GetLocksResult{
				Locks: []*LockInfo{
					{
						LockID:         "single-lock",
						OwnerID:        "sole-owner",
						Version:        1,
						AcquiredAt:     baseTime,
						ExpiresAt:      baseTime.Add(30 * time.Minute),
						WaiterCount:    0,
						WaitersInfo:    []*WaiterInfo{},
						LastModifiedAt: baseTime,
						Metadata: map[string]string{
							"category": "exclusive",
						},
					},
				},
				TotalMatching: 1,
				HasMore:       false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToGetLocksResult(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.TotalMatching != tt.expected.TotalMatching {
				t.Errorf("TotalMatching: expected %d, got %d", tt.expected.TotalMatching, result.TotalMatching)
			}
			if result.HasMore != tt.expected.HasMore {
				t.Errorf("HasMore: expected %v, got %v", tt.expected.HasMore, result.HasMore)
			}

			if len(result.Locks) != len(tt.expected.Locks) {
				t.Errorf("Locks length: expected %d, got %d", len(tt.expected.Locks), len(result.Locks))
			}

			for i, expectedLock := range tt.expected.Locks {
				if i >= len(result.Locks) {
					t.Errorf("Missing lock at index %d", i)
					continue
				}
				resultLock := result.Locks[i]
				if resultLock.LockID != expectedLock.LockID {
					t.Errorf("Locks[%d].LockID: expected %q, got %q", i, expectedLock.LockID, resultLock.LockID)
				}
				if resultLock.OwnerID != expectedLock.OwnerID {
					t.Errorf("Locks[%d].OwnerID: expected %q, got %q", i, expectedLock.OwnerID, resultLock.OwnerID)
				}
				if resultLock.WaiterCount != expectedLock.WaiterCount {
					t.Errorf("Locks[%d].WaiterCount: expected %d, got %d", i, expectedLock.WaiterCount, resultLock.WaiterCount)
				}
			}
		})
	}
}

func TestProtoToRaftStatus(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.RaftStatus
		expected *RaftStatus
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "leader status with replication info",
			input: &pb.RaftStatus{
				NodeId:        "node-1",
				Role:          "Leader",
				Term:          5,
				LeaderId:      "node-1",
				LastLogIndex:  100,
				LastLogTerm:   5,
				CommitIndex:   95,
				LastApplied:   90,
				SnapshotIndex: 50,
				SnapshotTerm:  3,
				Replication: map[string]*pb.PeerState{
					"node-2": {
						NextIndex:          101,
						MatchIndex:         95,
						IsActive:           true,
						LastActiveAt:       mustTimestamp(baseTime),
						SnapshotInProgress: false,
						ReplicationLag:     5,
					},
					"node-3": {
						NextIndex:          98,
						MatchIndex:         92,
						IsActive:           false,
						LastActiveAt:       mustTimestamp(baseTime.Add(-2 * time.Minute)),
						SnapshotInProgress: true,
						ReplicationLag:     8,
					},
				},
			},
			expected: &RaftStatus{
				NodeID:        "node-1",
				Role:          "Leader",
				Term:          5,
				LeaderID:      "node-1",
				LastLogIndex:  100,
				LastLogTerm:   5,
				CommitIndex:   95,
				LastApplied:   90,
				SnapshotIndex: 50,
				SnapshotTerm:  3,
				Replication: map[string]*PeerState{
					"node-2": {
						NextIndex:          101,
						MatchIndex:         95,
						IsActive:           true,
						LastActiveAt:       baseTime,
						SnapshotInProgress: false,
						ReplicationLag:     5,
					},
					"node-3": {
						NextIndex:          98,
						MatchIndex:         92,
						IsActive:           false,
						LastActiveAt:       baseTime.Add(-2 * time.Minute),
						SnapshotInProgress: true,
						ReplicationLag:     8,
					},
				},
			},
		},
		{
			name: "follower status without replication info",
			input: &pb.RaftStatus{
				NodeId:        "node-2",
				Role:          "Follower",
				Term:          5,
				LeaderId:      "node-1",
				LastLogIndex:  95,
				LastLogTerm:   5,
				CommitIndex:   95,
				LastApplied:   90,
				SnapshotIndex: 50,
				SnapshotTerm:  3,
				Replication:   nil,
			},
			expected: &RaftStatus{
				NodeID:        "node-2",
				Role:          "Follower",
				Term:          5,
				LeaderID:      "node-1",
				LastLogIndex:  95,
				LastLogTerm:   5,
				CommitIndex:   95,
				LastApplied:   90,
				SnapshotIndex: 50,
				SnapshotTerm:  3,
				Replication:   map[string]*PeerState{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToRaftStatus(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.NodeID != tt.expected.NodeID {
				t.Errorf("NodeID: expected %q, got %q", tt.expected.NodeID, result.NodeID)
			}
			if result.Role != tt.expected.Role {
				t.Errorf("Role: expected %q, got %q", tt.expected.Role, result.Role)
			}
			if result.Term != tt.expected.Term {
				t.Errorf("Term: expected %d, got %d", tt.expected.Term, result.Term)
			}
			if result.LeaderID != tt.expected.LeaderID {
				t.Errorf("LeaderID: expected %q, got %q", tt.expected.LeaderID, result.LeaderID)
			}
			if result.CommitIndex != tt.expected.CommitIndex {
				t.Errorf("CommitIndex: expected %d, got %d", tt.expected.CommitIndex, result.CommitIndex)
			}

			if len(result.Replication) != len(tt.expected.Replication) {
				t.Errorf("Replication length: expected %d, got %d",
					len(tt.expected.Replication), len(result.Replication))
			}

			for nodeID, expectedPeer := range tt.expected.Replication {
				resultPeer, exists := result.Replication[nodeID]
				if !exists {
					t.Errorf("Missing replication info for node %q", nodeID)
					continue
				}
				if resultPeer.IsActive != expectedPeer.IsActive {
					t.Errorf("Replication[%s].IsActive: expected %v, got %v",
						nodeID, expectedPeer.IsActive, resultPeer.IsActive)
				}
				if resultPeer.ReplicationLag != expectedPeer.ReplicationLag {
					t.Errorf("Replication[%s].ReplicationLag: expected %d, got %d",
						nodeID, expectedPeer.ReplicationLag, resultPeer.ReplicationLag)
				}
			}
		})
	}
}

func TestProtoToEnqueueResult(t *testing.T) {
	tests := []struct {
		name     string
		input    *pb.EnqueueWaiterResponse
		expected *EnqueueResult
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "successful enqueue at front of queue",
			input: &pb.EnqueueWaiterResponse{
				Enqueued:              true,
				Position:              0,
				EstimatedWaitDuration: mustDuration(30 * time.Second),
				Error:                 nil,
			},
			expected: &EnqueueResult{
				Enqueued:              true,
				Position:              0,
				EstimatedWaitDuration: 30 * time.Second,
				Error:                 nil,
			},
		},
		{
			name: "successful enqueue with longer wait",
			input: &pb.EnqueueWaiterResponse{
				Enqueued:              true,
				Position:              5,
				EstimatedWaitDuration: mustDuration(3 * time.Minute),
				Error:                 nil,
			},
			expected: &EnqueueResult{
				Enqueued:              true,
				Position:              5,
				EstimatedWaitDuration: 3 * time.Minute,
				Error:                 nil,
			},
		},
		{
			name: "failed enqueue - queue full",
			input: &pb.EnqueueWaiterResponse{
				Enqueued:              false,
				Position:              0,
				EstimatedWaitDuration: mustDuration(0),
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_WAIT_QUEUE_FULL,
					Message: "Wait queue has reached maximum capacity",
					Details: map[string]string{
						"max_capacity":    "100",
						"current_waiters": "100",
						"lock_id":         "popular-lock",
					},
				},
			},
			expected: &EnqueueResult{
				Enqueued:              false,
				Position:              0,
				EstimatedWaitDuration: 0,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_WAIT_QUEUE_FULL,
					Message: "Wait queue has reached maximum capacity",
					Details: map[string]string{
						"max_capacity":    "100",
						"current_waiters": "100",
						"lock_id":         "popular-lock",
					},
				},
			},
		},
		{
			name: "failed enqueue - invalid timeout",
			input: &pb.EnqueueWaiterResponse{
				Enqueued:              false,
				Position:              0,
				EstimatedWaitDuration: mustDuration(0),
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_INVALID_ARGUMENT,
					Message: "Wait timeout exceeds maximum allowed duration",
					Details: map[string]string{
						"provided_timeout": "600s",
						"max_timeout":      "300s",
					},
				},
			},
			expected: &EnqueueResult{
				Enqueued:              false,
				Position:              0,
				EstimatedWaitDuration: 0,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_INVALID_ARGUMENT,
					Message: "Wait timeout exceeds maximum allowed duration",
					Details: map[string]string{
						"provided_timeout": "600s",
						"max_timeout":      "300s",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToEnqueueResult(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.Enqueued != tt.expected.Enqueued {
				t.Errorf("Enqueued: expected %v, got %v", tt.expected.Enqueued, result.Enqueued)
			}
			if result.Position != tt.expected.Position {
				t.Errorf("Position: expected %d, got %d", tt.expected.Position, result.Position)
			}
			if result.EstimatedWaitDuration != tt.expected.EstimatedWaitDuration {
				t.Errorf("EstimatedWaitDuration: expected %v, got %v",
					tt.expected.EstimatedWaitDuration, result.EstimatedWaitDuration)
			}

			// Check error details
			if (tt.expected.Error == nil) != (result.Error == nil) {
				t.Errorf("Error presence mismatch: expected %v, got %v",
					tt.expected.Error != nil, result.Error != nil)
			}
		})
	}
}

func TestProtoToLockManagerStats(t *testing.T) {
	tests := []struct {
		name     string
		input    *pb.LockManagerStats
		expected *LockManagerStats
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "complete lock manager statistics",
			input: &pb.LockManagerStats{
				ActiveLocksCount:         150,
				TotalWaitersCount:        45,
				ContestedLocksCount:      12,
				AverageHoldDuration:      mustDuration(2 * time.Minute),
				AcquisitionRatePerSecond: 5.7,
				ReleaseRatePerSecond:     5.2,
				ExpiredLocksLastPeriod:   8,
			},
			expected: &LockManagerStats{
				ActiveLocksCount:         150,
				TotalWaitersCount:        45,
				ContestedLocksCount:      12,
				AverageHoldDuration:      2 * time.Minute,
				AcquisitionRatePerSecond: 5.7,
				ReleaseRatePerSecond:     5.2,
				ExpiredLocksLastPeriod:   8,
			},
		},
		{
			name: "zero values statistics",
			input: &pb.LockManagerStats{
				ActiveLocksCount:         0,
				TotalWaitersCount:        0,
				ContestedLocksCount:      0,
				AverageHoldDuration:      mustDuration(0),
				AcquisitionRatePerSecond: 0.0,
				ReleaseRatePerSecond:     0.0,
				ExpiredLocksLastPeriod:   0,
			},
			expected: &LockManagerStats{
				ActiveLocksCount:         0,
				TotalWaitersCount:        0,
				ContestedLocksCount:      0,
				AverageHoldDuration:      0,
				AcquisitionRatePerSecond: 0.0,
				ReleaseRatePerSecond:     0.0,
				ExpiredLocksLastPeriod:   0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToLockManagerStats(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.ActiveLocksCount != tt.expected.ActiveLocksCount {
				t.Errorf("ActiveLocksCount: expected %d, got %d",
					tt.expected.ActiveLocksCount, result.ActiveLocksCount)
			}
			if result.TotalWaitersCount != tt.expected.TotalWaitersCount {
				t.Errorf("TotalWaitersCount: expected %d, got %d",
					tt.expected.TotalWaitersCount, result.TotalWaitersCount)
			}
			if result.ContestedLocksCount != tt.expected.ContestedLocksCount {
				t.Errorf("ContestedLocksCount: expected %d, got %d",
					tt.expected.ContestedLocksCount, result.ContestedLocksCount)
			}
			if result.AverageHoldDuration != tt.expected.AverageHoldDuration {
				t.Errorf("AverageHoldDuration: expected %v, got %v",
					tt.expected.AverageHoldDuration, result.AverageHoldDuration)
			}
			if result.AcquisitionRatePerSecond != tt.expected.AcquisitionRatePerSecond {
				t.Errorf("AcquisitionRatePerSecond: expected %f, got %f",
					tt.expected.AcquisitionRatePerSecond, result.AcquisitionRatePerSecond)
			}
			if result.ReleaseRatePerSecond != tt.expected.ReleaseRatePerSecond {
				t.Errorf("ReleaseRatePerSecond: expected %f, got %f",
					tt.expected.ReleaseRatePerSecond, result.ReleaseRatePerSecond)
			}
			if result.ExpiredLocksLastPeriod != tt.expected.ExpiredLocksLastPeriod {
				t.Errorf("ExpiredLocksLastPeriod: expected %d, got %d",
					tt.expected.ExpiredLocksLastPeriod, result.ExpiredLocksLastPeriod)
			}
		})
	}
}

func TestProtoToPeerState(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.PeerState
		expected *PeerState
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "active peer with current replication",
			input: &pb.PeerState{
				NextIndex:          105,
				MatchIndex:         100,
				IsActive:           true,
				LastActiveAt:       mustTimestamp(baseTime),
				SnapshotInProgress: false,
				ReplicationLag:     5,
			},
			expected: &PeerState{
				NextIndex:          105,
				MatchIndex:         100,
				IsActive:           true,
				LastActiveAt:       baseTime,
				SnapshotInProgress: false,
				ReplicationLag:     5,
			},
		},
		{
			name: "inactive peer with snapshot in progress",
			input: &pb.PeerState{
				NextIndex:          50,
				MatchIndex:         45,
				IsActive:           false,
				LastActiveAt:       mustTimestamp(baseTime.Add(-5 * time.Minute)),
				SnapshotInProgress: true,
				ReplicationLag:     55,
			},
			expected: &PeerState{
				NextIndex:          50,
				MatchIndex:         45,
				IsActive:           false,
				LastActiveAt:       baseTime.Add(-5 * time.Minute),
				SnapshotInProgress: true,
				ReplicationLag:     55,
			},
		},
		{
			name: "peer with zero lag (fully caught up)",
			input: &pb.PeerState{
				NextIndex:          200,
				MatchIndex:         199,
				IsActive:           true,
				LastActiveAt:       mustTimestamp(baseTime.Add(-30 * time.Second)),
				SnapshotInProgress: false,
				ReplicationLag:     0,
			},
			expected: &PeerState{
				NextIndex:          200,
				MatchIndex:         199,
				IsActive:           true,
				LastActiveAt:       baseTime.Add(-30 * time.Second),
				SnapshotInProgress: false,
				ReplicationLag:     0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToPeerState(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.NextIndex != tt.expected.NextIndex {
				t.Errorf("NextIndex: expected %d, got %d", tt.expected.NextIndex, result.NextIndex)
			}
			if result.MatchIndex != tt.expected.MatchIndex {
				t.Errorf("MatchIndex: expected %d, got %d", tt.expected.MatchIndex, result.MatchIndex)
			}
			if result.IsActive != tt.expected.IsActive {
				t.Errorf("IsActive: expected %v, got %v", tt.expected.IsActive, result.IsActive)
			}
			if result.SnapshotInProgress != tt.expected.SnapshotInProgress {
				t.Errorf("SnapshotInProgress: expected %v, got %v",
					tt.expected.SnapshotInProgress, result.SnapshotInProgress)
			}
			if result.ReplicationLag != tt.expected.ReplicationLag {
				t.Errorf("ReplicationLag: expected %d, got %d",
					tt.expected.ReplicationLag, result.ReplicationLag)
			}

			assertTimeEqual(t, tt.expected.LastActiveAt, result.LastActiveAt, "LastActiveAt")
		})
	}
}

func TestProtoToHealthStatus(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.HealthStatus
		expected *HealthStatus
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "healthy leader node",
			input: &pb.HealthStatus{
				Status:              pb.HealthStatus_SERVING,
				Message:             "All systems operational",
				IsRaftLeader:        true,
				RaftLeaderAddress:   "node-1.cluster.local:8080",
				RaftTerm:            10,
				RaftLastApplied:     1500,
				CurrentActiveLocks:  75,
				CurrentTotalWaiters: 25,
				Uptime:              mustDuration(2 * time.Hour),
				LastHealthCheckAt:   mustTimestamp(baseTime),
			},
			expected: &HealthStatus{
				Status:              HealthServingStatus(pb.HealthStatus_SERVING),
				Message:             "All systems operational",
				IsRaftLeader:        true,
				RaftLeaderAddress:   "node-1.cluster.local:8080",
				RaftTerm:            10,
				RaftLastApplied:     1500,
				CurrentActiveLocks:  75,
				CurrentTotalWaiters: 25,
				Uptime:              2 * time.Hour,
				LastHealthCheckAt:   baseTime,
			},
		},
		{
			name: "unhealthy follower node",
			input: &pb.HealthStatus{
				Status:              pb.HealthStatus_NOT_SERVING,
				Message:             "Network partition detected",
				IsRaftLeader:        false,
				RaftLeaderAddress:   "",
				RaftTerm:            8,
				RaftLastApplied:     1200,
				CurrentActiveLocks:  0,
				CurrentTotalWaiters: 0,
				Uptime:              mustDuration(30 * time.Minute),
				LastHealthCheckAt:   mustTimestamp(baseTime.Add(-2 * time.Minute)),
			},
			expected: &HealthStatus{
				Status:              HealthServingStatus(pb.HealthStatus_NOT_SERVING),
				Message:             "Network partition detected",
				IsRaftLeader:        false,
				RaftLeaderAddress:   "",
				RaftTerm:            8,
				RaftLastApplied:     1200,
				CurrentActiveLocks:  0,
				CurrentTotalWaiters: 0,
				Uptime:              30 * time.Minute,
				LastHealthCheckAt:   baseTime.Add(-2 * time.Minute),
			},
		},
		{
			name: "unknown status node",
			input: &pb.HealthStatus{
				Status:              pb.HealthStatus_UNKNOWN,
				Message:             "Status check in progress",
				IsRaftLeader:        false,
				RaftLeaderAddress:   "node-2.cluster.local:8080",
				RaftTerm:            5,
				RaftLastApplied:     800,
				CurrentActiveLocks:  10,
				CurrentTotalWaiters: 5,
				Uptime:              mustDuration(5 * time.Minute),
				LastHealthCheckAt:   mustTimestamp(baseTime.Add(-1 * time.Minute)),
			},
			expected: &HealthStatus{
				Status:              HealthServingStatus(pb.HealthStatus_UNKNOWN),
				Message:             "Status check in progress",
				IsRaftLeader:        false,
				RaftLeaderAddress:   "node-2.cluster.local:8080",
				RaftTerm:            5,
				RaftLastApplied:     800,
				CurrentActiveLocks:  10,
				CurrentTotalWaiters: 5,
				Uptime:              5 * time.Minute,
				LastHealthCheckAt:   baseTime.Add(-1 * time.Minute),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToHealthStatus(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.Status != tt.expected.Status {
				t.Errorf("Status: expected %v, got %v", tt.expected.Status, result.Status)
			}
			if result.Message != tt.expected.Message {
				t.Errorf("Message: expected %q, got %q", tt.expected.Message, result.Message)
			}
			if result.IsRaftLeader != tt.expected.IsRaftLeader {
				t.Errorf("IsRaftLeader: expected %v, got %v", tt.expected.IsRaftLeader, result.IsRaftLeader)
			}
			if result.RaftLeaderAddress != tt.expected.RaftLeaderAddress {
				t.Errorf("RaftLeaderAddress: expected %q, got %q",
					tt.expected.RaftLeaderAddress, result.RaftLeaderAddress)
			}
			if result.RaftTerm != tt.expected.RaftTerm {
				t.Errorf("RaftTerm: expected %d, got %d", tt.expected.RaftTerm, result.RaftTerm)
			}
			if result.RaftLastApplied != tt.expected.RaftLastApplied {
				t.Errorf("RaftLastApplied: expected %d, got %d",
					tt.expected.RaftLastApplied, result.RaftLastApplied)
			}
			if result.CurrentActiveLocks != tt.expected.CurrentActiveLocks {
				t.Errorf("CurrentActiveLocks: expected %d, got %d",
					tt.expected.CurrentActiveLocks, result.CurrentActiveLocks)
			}
			if result.CurrentTotalWaiters != tt.expected.CurrentTotalWaiters {
				t.Errorf("CurrentTotalWaiters: expected %d, got %d",
					tt.expected.CurrentTotalWaiters, result.CurrentTotalWaiters)
			}
			if result.Uptime != tt.expected.Uptime {
				t.Errorf("Uptime: expected %v, got %v", tt.expected.Uptime, result.Uptime)
			}

			assertTimeEqual(t, tt.expected.LastHealthCheckAt, result.LastHealthCheckAt, "LastHealthCheckAt")
		})
	}
}

func TestProtoToHealthStatusFromHealthResponse(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.HealthResponse
		expected *HealthStatus
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "response with complete health info",
			input: &pb.HealthResponse{
				Status:  pb.HealthStatus_SERVING,
				Message: "Service is healthy and ready",
				HealthInfo: &pb.HealthStatus{
					Status:              pb.HealthStatus_SERVING,
					Message:             "All subsystems operational",
					IsRaftLeader:        true,
					RaftLeaderAddress:   "leader.cluster.local:8080",
					RaftTerm:            15,
					RaftLastApplied:     2000,
					CurrentActiveLocks:  50,
					CurrentTotalWaiters: 15,
					Uptime:              mustDuration(4 * time.Hour),
					LastHealthCheckAt:   mustTimestamp(baseTime),
				},
			},
			expected: &HealthStatus{
				Status:              HealthServingStatus(pb.HealthStatus_SERVING),
				Message:             "Service is healthy and ready", // Top-level message takes precedence
				IsRaftLeader:        true,
				RaftLeaderAddress:   "leader.cluster.local:8080",
				RaftTerm:            15,
				RaftLastApplied:     2000,
				CurrentActiveLocks:  50,
				CurrentTotalWaiters: 15,
				Uptime:              4 * time.Hour,
				LastHealthCheckAt:   baseTime,
			},
		},
		{
			name: "response without health info (nil HealthInfo)",
			input: &pb.HealthResponse{
				Status:     pb.HealthStatus_NOT_SERVING,
				Message:    "Service temporarily unavailable",
				HealthInfo: nil,
			},
			expected: &HealthStatus{
				Status:              HealthServingStatus(pb.HealthStatus_NOT_SERVING),
				Message:             "Service temporarily unavailable",
				IsRaftLeader:        false,
				RaftLeaderAddress:   "",
				RaftTerm:            0,
				RaftLastApplied:     0,
				CurrentActiveLocks:  0,
				CurrentTotalWaiters: 0,
				Uptime:              0,
				LastHealthCheckAt:   time.Time{},
			},
		},
		{
			name: "conflicting status between response and health info",
			input: &pb.HealthResponse{
				Status:  pb.HealthStatus_NOT_SERVING, // Top-level says not serving
				Message: "Load balancer health check failed",
				HealthInfo: &pb.HealthStatus{
					Status:              pb.HealthStatus_SERVING, // But detailed info says serving
					Message:             "Internal systems are healthy",
					IsRaftLeader:        false,
					RaftLeaderAddress:   "other-node.cluster.local:8080",
					RaftTerm:            12,
					RaftLastApplied:     1800,
					CurrentActiveLocks:  30,
					CurrentTotalWaiters: 8,
					Uptime:              mustDuration(1 * time.Hour),
					LastHealthCheckAt:   mustTimestamp(baseTime.Add(-30 * time.Second)),
				},
			},
			expected: &HealthStatus{
				Status:              HealthServingStatus(pb.HealthStatus_NOT_SERVING), // Top-level status wins
				Message:             "Load balancer health check failed",              // Top-level message wins
				IsRaftLeader:        false,
				RaftLeaderAddress:   "other-node.cluster.local:8080",
				RaftTerm:            12,
				RaftLastApplied:     1800,
				CurrentActiveLocks:  30,
				CurrentTotalWaiters: 8,
				Uptime:              1 * time.Hour,
				LastHealthCheckAt:   baseTime.Add(-30 * time.Second),
			},
		},
		{
			name: "unknown status with partial info",
			input: &pb.HealthResponse{
				Status:  pb.HealthStatus_UNKNOWN,
				Message: "Initializing health checks",
				HealthInfo: &pb.HealthStatus{
					Status:              pb.HealthStatus_UNKNOWN,
					Message:             "Raft node starting up",
					IsRaftLeader:        false,
					RaftLeaderAddress:   "",
					RaftTerm:            1,
					RaftLastApplied:     0,
					CurrentActiveLocks:  0,
					CurrentTotalWaiters: 0,
					Uptime:              mustDuration(30 * time.Second),
					LastHealthCheckAt:   mustTimestamp(baseTime.Add(-10 * time.Second)),
				},
			},
			expected: &HealthStatus{
				Status:              HealthServingStatus(pb.HealthStatus_UNKNOWN),
				Message:             "Initializing health checks",
				IsRaftLeader:        false,
				RaftLeaderAddress:   "",
				RaftTerm:            1,
				RaftLastApplied:     0,
				CurrentActiveLocks:  0,
				CurrentTotalWaiters: 0,
				Uptime:              30 * time.Second,
				LastHealthCheckAt:   baseTime.Add(-10 * time.Second),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToHealthStatusFromHealthResponse(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.Status != tt.expected.Status {
				t.Errorf("Status: expected %v, got %v", tt.expected.Status, result.Status)
			}
			if result.Message != tt.expected.Message {
				t.Errorf("Message: expected %q, got %q", tt.expected.Message, result.Message)
			}
			if result.IsRaftLeader != tt.expected.IsRaftLeader {
				t.Errorf("IsRaftLeader: expected %v, got %v", tt.expected.IsRaftLeader, result.IsRaftLeader)
			}
			if result.RaftLeaderAddress != tt.expected.RaftLeaderAddress {
				t.Errorf("RaftLeaderAddress: expected %q, got %q",
					tt.expected.RaftLeaderAddress, result.RaftLeaderAddress)
			}
			if result.RaftTerm != tt.expected.RaftTerm {
				t.Errorf("RaftTerm: expected %d, got %d", tt.expected.RaftTerm, result.RaftTerm)
			}
			if result.RaftLastApplied != tt.expected.RaftLastApplied {
				t.Errorf("RaftLastApplied: expected %d, got %d",
					tt.expected.RaftLastApplied, result.RaftLastApplied)
			}
			if result.CurrentActiveLocks != tt.expected.CurrentActiveLocks {
				t.Errorf("CurrentActiveLocks: expected %d, got %d",
					tt.expected.CurrentActiveLocks, result.CurrentActiveLocks)
			}
			if result.CurrentTotalWaiters != tt.expected.CurrentTotalWaiters {
				t.Errorf("CurrentTotalWaiters: expected %d, got %d",
					tt.expected.CurrentTotalWaiters, result.CurrentTotalWaiters)
			}
			if result.Uptime != tt.expected.Uptime {
				t.Errorf("Uptime: expected %v, got %v", tt.expected.Uptime, result.Uptime)
			}

			assertTimeEqual(t, tt.expected.LastHealthCheckAt, result.LastHealthCheckAt, "LastHealthCheckAt")
		})
	}
}

func TestProtoToClusterStatus(t *testing.T) {
	baseTime := time.Unix(1749632059, 0) // Wed Jun 11 2025 08:54:19 GMT+0000

	tests := []struct {
		name     string
		input    *pb.GetStatusResponse
		expected *ClusterStatus
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "complete cluster status",
			input: &pb.GetStatusResponse{
				RaftStatus: &pb.RaftStatus{
					NodeId:        "primary-node",
					Role:          "Leader",
					Term:          8,
					LeaderId:      "primary-node",
					LastLogIndex:  500,
					LastLogTerm:   8,
					CommitIndex:   495,
					LastApplied:   490,
					SnapshotIndex: 400,
					SnapshotTerm:  7,
					Replication: map[string]*pb.PeerState{
						"backup-node": {
							NextIndex:          501,
							MatchIndex:         495,
							IsActive:           true,
							LastActiveAt:       mustTimestamp(baseTime),
							SnapshotInProgress: false,
							ReplicationLag:     5,
						},
					},
				},
				LockStats: &pb.LockManagerStats{
					ActiveLocksCount:         200,
					TotalWaitersCount:        60,
					ContestedLocksCount:      15,
					AverageHoldDuration:      mustDuration(90 * time.Second),
					AcquisitionRatePerSecond: 3.2,
					ReleaseRatePerSecond:     2.8,
					ExpiredLocksLastPeriod:   5,
				},
				Health: &pb.HealthStatus{
					Status:              pb.HealthStatus_SERVING,
					Message:             "Cluster is healthy",
					IsRaftLeader:        true,
					RaftLeaderAddress:   "primary-node.cluster.local:8080",
					RaftTerm:            8,
					RaftLastApplied:     490,
					CurrentActiveLocks:  200,
					CurrentTotalWaiters: 60,
					Uptime:              mustDuration(6 * time.Hour),
					LastHealthCheckAt:   mustTimestamp(baseTime),
				},
			},
			expected: &ClusterStatus{
				RaftStatus: &RaftStatus{
					NodeID:        "primary-node",
					Role:          "Leader",
					Term:          8,
					LeaderID:      "primary-node",
					LastLogIndex:  500,
					LastLogTerm:   8,
					CommitIndex:   495,
					LastApplied:   490,
					SnapshotIndex: 400,
					SnapshotTerm:  7,
					Replication: map[string]*PeerState{
						"backup-node": {
							NextIndex:          501,
							MatchIndex:         495,
							IsActive:           true,
							LastActiveAt:       baseTime,
							SnapshotInProgress: false,
							ReplicationLag:     5,
						},
					},
				},
				LockStats: &LockManagerStats{
					ActiveLocksCount:         200,
					TotalWaitersCount:        60,
					ContestedLocksCount:      15,
					AverageHoldDuration:      90 * time.Second,
					AcquisitionRatePerSecond: 3.2,
					ReleaseRatePerSecond:     2.8,
					ExpiredLocksLastPeriod:   5,
				},
				Health: &HealthStatus{
					Status:              HealthServingStatus(pb.HealthStatus_SERVING),
					Message:             "Cluster is healthy",
					IsRaftLeader:        true,
					RaftLeaderAddress:   "primary-node.cluster.local:8080",
					RaftTerm:            8,
					RaftLastApplied:     490,
					CurrentActiveLocks:  200,
					CurrentTotalWaiters: 60,
					Uptime:              6 * time.Hour,
					LastHealthCheckAt:   baseTime,
				},
			},
		},
		{
			name: "partial cluster status with nil components",
			input: &pb.GetStatusResponse{
				RaftStatus: &pb.RaftStatus{
					NodeId:      "follower-node",
					Role:        "Follower",
					Term:        5,
					LeaderId:    "some-leader",
					CommitIndex: 100,
					LastApplied: 95,
				},
				LockStats: nil, // Missing lock stats
				Health:    nil, // Missing health info
			},
			expected: &ClusterStatus{
				RaftStatus: &RaftStatus{
					NodeID:      "follower-node",
					Role:        "Follower",
					Term:        5,
					LeaderID:    "some-leader",
					CommitIndex: 100,
					LastApplied: 95,
					Replication: map[string]*PeerState{}, // Empty map for nil replication
				},
				LockStats: nil,
				Health:    nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToClusterStatus(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if (tt.expected.RaftStatus == nil) != (result.RaftStatus == nil) {
				t.Errorf("RaftStatus presence mismatch: expected %v, got %v",
					tt.expected.RaftStatus != nil, result.RaftStatus != nil)
			}
			if tt.expected.RaftStatus != nil && result.RaftStatus != nil {
				if result.RaftStatus.NodeID != tt.expected.RaftStatus.NodeID {
					t.Errorf("RaftStatus.NodeID: expected %q, got %q",
						tt.expected.RaftStatus.NodeID, result.RaftStatus.NodeID)
				}
				if result.RaftStatus.Role != tt.expected.RaftStatus.Role {
					t.Errorf("RaftStatus.Role: expected %q, got %q",
						tt.expected.RaftStatus.Role, result.RaftStatus.Role)
				}
				if result.RaftStatus.Term != tt.expected.RaftStatus.Term {
					t.Errorf("RaftStatus.Term: expected %d, got %d",
						tt.expected.RaftStatus.Term, result.RaftStatus.Term)
				}
			}

			if (tt.expected.LockStats == nil) != (result.LockStats == nil) {
				t.Errorf("LockStats presence mismatch: expected %v, got %v",
					tt.expected.LockStats != nil, result.LockStats != nil)
			}
			if tt.expected.LockStats != nil && result.LockStats != nil {
				if result.LockStats.ActiveLocksCount != tt.expected.LockStats.ActiveLocksCount {
					t.Errorf("LockStats.ActiveLocksCount: expected %d, got %d",
						tt.expected.LockStats.ActiveLocksCount, result.LockStats.ActiveLocksCount)
				}
			}

			if (tt.expected.Health == nil) != (result.Health == nil) {
				t.Errorf("Health presence mismatch: expected %v, got %v",
					tt.expected.Health != nil, result.Health != nil)
			}
			if tt.expected.Health != nil && result.Health != nil {
				if result.Health.Status != tt.expected.Health.Status {
					t.Errorf("Health.Status: expected %v, got %v",
						tt.expected.Health.Status, result.Health.Status)
				}
				if result.Health.IsRaftLeader != tt.expected.Health.IsRaftLeader {
					t.Errorf("Health.IsRaftLeader: expected %v, got %v",
						tt.expected.Health.IsRaftLeader, result.Health.IsRaftLeader)
				}
			}
		})
	}
}

func TestProtoToCancelWaitResult(t *testing.T) {
	tests := []struct {
		name     string
		input    *pb.CancelWaitResponse
		expected *CancelWaitResult
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name: "successful cancellation",
			input: &pb.CancelWaitResponse{
				Cancelled: true,
				Error:     nil,
			},
			expected: &CancelWaitResult{
				Cancelled: true,
				Error:     nil,
			},
		},
		{
			name: "failed cancellation - not waiting",
			input: &pb.CancelWaitResponse{
				Cancelled: false,
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_NOT_WAITING,
					Message: "Client is not in the wait queue",
					Details: map[string]string{
						"client_id": "absent-client-123",
						"lock_id":   "contested-lock-456",
						"reason":    "client_not_found",
					},
				},
			},
			expected: &CancelWaitResult{
				Cancelled: false,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_NOT_WAITING,
					Message: "Client is not in the wait queue",
					Details: map[string]string{
						"client_id": "absent-client-123",
						"lock_id":   "contested-lock-456",
						"reason":    "client_not_found",
					},
				},
			},
		},
		{
			name: "failed cancellation - version mismatch",
			input: &pb.CancelWaitResponse{
				Cancelled: false,
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_VERSION_MISMATCH,
					Message: "Wait request version does not match",
					Details: map[string]string{
						"expected_version": "5",
						"provided_version": "3",
						"lock_id":          "version-sensitive-lock",
					},
				},
			},
			expected: &CancelWaitResult{
				Cancelled: false,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_VERSION_MISMATCH,
					Message: "Wait request version does not match",
					Details: map[string]string{
						"expected_version": "5",
						"provided_version": "3",
						"lock_id":          "version-sensitive-lock",
					},
				},
			},
		},
		{
			name: "failed cancellation - timeout error",
			input: &pb.CancelWaitResponse{
				Cancelled: false,
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_TIMEOUT,
					Message: "Cancel wait request timed out",
					Details: map[string]string{
						"timeout_duration": "30s",
						"operation":        "cancel_wait",
					},
				},
			},
			expected: &CancelWaitResult{
				Cancelled: false,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_TIMEOUT,
					Message: "Cancel wait request timed out",
					Details: map[string]string{
						"timeout_duration": "30s",
						"operation":        "cancel_wait",
					},
				},
			},
		},
		{
			name: "failed cancellation with empty error details",
			input: &pb.CancelWaitResponse{
				Cancelled: false,
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_INTERNAL_ERROR,
					Message: "Internal server error during cancellation",
					Details: map[string]string{},
				},
			},
			expected: &CancelWaitResult{
				Cancelled: false,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_INTERNAL_ERROR,
					Message: "Internal server error during cancellation",
					Details: map[string]string{},
				},
			},
		},
		{
			name: "failed cancellation with nil error details",
			input: &pb.CancelWaitResponse{
				Cancelled: false,
				Error: &pb.ErrorDetail{
					Code:    pb.ErrorCode_UNAVAILABLE,
					Message: "Service temporarily unavailable",
					Details: nil,
				},
			},
			expected: &CancelWaitResult{
				Cancelled: false,
				Error: &ErrorDetail{
					Code:    pb.ErrorCode_UNAVAILABLE,
					Message: "Service temporarily unavailable",
					Details: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protoToCancelWaitResult(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected %+v, got nil", tt.expected)
			}

			if result.Cancelled != tt.expected.Cancelled {
				t.Errorf("Cancelled: expected %v, got %v", tt.expected.Cancelled, result.Cancelled)
			}

			// Check error details
			if (tt.expected.Error == nil) != (result.Error == nil) {
				t.Errorf("Error presence mismatch: expected %v, got %v",
					tt.expected.Error != nil, result.Error != nil)
			}

			if tt.expected.Error != nil && result.Error != nil {
				if result.Error.Code != tt.expected.Error.Code {
					t.Errorf("Error.Code: expected %v, got %v", tt.expected.Error.Code, result.Error.Code)
				}
				if result.Error.Message != tt.expected.Error.Message {
					t.Errorf("Error.Message: expected %q, got %q", tt.expected.Error.Message, result.Error.Message)
				}
				assertStringMapEqual(t, tt.expected.Error.Details, result.Error.Details, "Error.Details")
			}
		})
	}
}
