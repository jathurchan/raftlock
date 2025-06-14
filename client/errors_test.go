package client

import (
	"errors"
	"fmt"
	"testing"

	pb "github.com/jathurchan/raftlock/proto"
)

func TestErrorFromCode(t *testing.T) {
	tests := []struct {
		name        string
		code        pb.ErrorCode
		expectedErr error
		shouldMatch bool
	}{
		{
			name:        "LOCK_HELD maps to ErrLockHeld",
			code:        pb.ErrorCode_LOCK_HELD,
			expectedErr: ErrLockHeld,
			shouldMatch: true,
		},
		{
			name:        "LOCK_NOT_HELD maps to ErrNotLockOwner",
			code:        pb.ErrorCode_LOCK_NOT_HELD,
			expectedErr: ErrNotLockOwner,
			shouldMatch: true,
		},
		{
			name:        "NOT_LOCK_OWNER maps to ErrNotLockOwner",
			code:        pb.ErrorCode_NOT_LOCK_OWNER,
			expectedErr: ErrNotLockOwner,
			shouldMatch: true,
		},
		{
			name:        "VERSION_MISMATCH maps to ErrVersionMismatch",
			code:        pb.ErrorCode_VERSION_MISMATCH,
			expectedErr: ErrVersionMismatch,
			shouldMatch: true,
		},
		{
			name:        "LOCK_NOT_FOUND maps to ErrLockNotFound",
			code:        pb.ErrorCode_LOCK_NOT_FOUND,
			expectedErr: ErrLockNotFound,
			shouldMatch: true,
		},
		{
			name:        "INVALID_TTL maps to ErrInvalidTTL",
			code:        pb.ErrorCode_INVALID_TTL,
			expectedErr: ErrInvalidTTL,
			shouldMatch: true,
		},
		{
			name:        "WAIT_QUEUE_FULL maps to ErrWaitQueueFull",
			code:        pb.ErrorCode_WAIT_QUEUE_FULL,
			expectedErr: ErrWaitQueueFull,
			shouldMatch: true,
		},
		{
			name:        "NOT_WAITING maps to ErrNotWaiting",
			code:        pb.ErrorCode_NOT_WAITING,
			expectedErr: ErrNotWaiting,
			shouldMatch: true,
		},
		{
			name:        "NOT_LEADER maps to ErrLeaderUnavailable",
			code:        pb.ErrorCode_NOT_LEADER,
			expectedErr: ErrLeaderUnavailable,
			shouldMatch: true,
		},
		{
			name:        "NO_LEADER maps to ErrLeaderUnavailable",
			code:        pb.ErrorCode_NO_LEADER,
			expectedErr: ErrLeaderUnavailable,
			shouldMatch: true,
		},
		{
			name:        "TIMEOUT maps to ErrTimeout",
			code:        pb.ErrorCode_TIMEOUT,
			expectedErr: ErrTimeout,
			shouldMatch: true,
		},
		{
			name:        "RATE_LIMITED maps to ErrRateLimit",
			code:        pb.ErrorCode_RATE_LIMITED,
			expectedErr: ErrRateLimit,
			shouldMatch: true,
		},
		{
			name:        "UNAVAILABLE maps to ErrUnavailable",
			code:        pb.ErrorCode_UNAVAILABLE,
			expectedErr: ErrUnavailable,
			shouldMatch: true,
		},
		{
			name:        "INVALID_ARGUMENT maps to ErrInvalidArgument",
			code:        pb.ErrorCode_INVALID_ARGUMENT,
			expectedErr: ErrInvalidArgument,
			shouldMatch: true,
		},
		{
			name:        "Unknown error code returns formatted error",
			code:        pb.ErrorCode(999), // Non-existent error code
			expectedErr: nil,
			shouldMatch: false,
		},
		{
			name:        "ERROR_CODE_UNSPECIFIED returns formatted error",
			code:        pb.ErrorCode_ERROR_CODE_UNSPECIFIED,
			expectedErr: nil,
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ErrorFromCode(tt.code)

			if tt.shouldMatch {
				if !errors.Is(result, tt.expectedErr) {
					t.Errorf("ErrorFromCode(%v) = %v, want %v", tt.code, result, tt.expectedErr)
				}
			} else {
				expectedMsg := fmt.Sprintf("unknown error code: %v", tt.code)
				if result.Error() != expectedMsg {
					t.Errorf("ErrorFromCode(%v) = %v, want error with message %q", tt.code, result, expectedMsg)
				}
			}
		})
	}
}

func TestClientError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *ClientError
		expected string
	}{
		{
			name: "Error with details",
			err: &ClientError{
				Op:   "Acquire",
				Err:  ErrLockHeld,
				Code: pb.ErrorCode_LOCK_HELD,
				Details: map[string]string{
					"lock_id":  "test-lock",
					"owner_id": "client-123",
				},
			},
			expected: "client Acquire failed: lock is already held by another client (code: LOCK_HELD, details: map[lock_id:test-lock owner_id:client-123])",
		},
		{
			name: "Error without details",
			err: &ClientError{
				Op:      "Release",
				Err:     ErrNotLockOwner,
				Code:    pb.ErrorCode_NOT_LOCK_OWNER,
				Details: nil,
			},
			expected: "client Release failed: client does not own the lock (code: NOT_LOCK_OWNER)",
		},
		{
			name: "Error with empty details map",
			err: &ClientError{
				Op:      "Renew",
				Err:     ErrVersionMismatch,
				Code:    pb.ErrorCode_VERSION_MISMATCH,
				Details: map[string]string{},
			},
			expected: "client Renew failed: lock version mismatch (code: VERSION_MISMATCH)",
		},
		{
			name: "Error with single detail",
			err: &ClientError{
				Op:   "GetLockInfo",
				Err:  ErrLockNotFound,
				Code: pb.ErrorCode_LOCK_NOT_FOUND,
				Details: map[string]string{
					"lock_id": "non-existent-lock",
				},
			},
			expected: "client GetLockInfo failed: lock not found (code: LOCK_NOT_FOUND, details: map[lock_id:non-existent-lock])",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("ClientError.Error() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestClientError_Unwrap(t *testing.T) {
	originalErr := ErrLockHeld
	clientErr := &ClientError{
		Op:   "Acquire",
		Err:  originalErr,
		Code: pb.ErrorCode_LOCK_HELD,
	}

	unwrapped := clientErr.Unwrap()
	if unwrapped != originalErr {
		t.Errorf("ClientError.Unwrap() = %v, want %v", unwrapped, originalErr)
	}

	if !errors.Is(clientErr, ErrLockHeld) {
		t.Error("errors.Is(clientErr, ErrLockHeld) should return true")
	}

	if errors.Is(clientErr, ErrNotLockOwner) {
		t.Error("errors.Is(clientErr, ErrNotLockOwner) should return false")
	}
}

func TestNewClientError(t *testing.T) {
	op := "TestOperation"
	err := ErrTimeout
	code := pb.ErrorCode_TIMEOUT
	details := map[string]string{
		"timeout_duration": "30s",
		"operation_id":     "op-123",
	}

	clientErr := NewClientError(op, err, code, details)

	if clientErr.Op != op {
		t.Errorf("NewClientError().Op = %v, want %v", clientErr.Op, op)
	}

	if clientErr.Err != err {
		t.Errorf("NewClientError().Err = %v, want %v", clientErr.Err, err)
	}

	if clientErr.Code != code {
		t.Errorf("NewClientError().Code = %v, want %v", clientErr.Code, code)
	}

	if len(clientErr.Details) != len(details) {
		t.Errorf(
			"NewClientError().Details length = %v, want %v",
			len(clientErr.Details),
			len(details),
		)
	}

	for key, value := range details {
		if clientErr.Details[key] != value {
			t.Errorf(
				"NewClientError().Details[%q] = %v, want %v",
				key,
				clientErr.Details[key],
				value,
			)
		}
	}
}

func TestPredefinedErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		msg  string
	}{
		{"ErrLockHeld", ErrLockHeld, "lock is already held by another client"},
		{"ErrNotLockOwner", ErrNotLockOwner, "client does not own the lock"},
		{"ErrVersionMismatch", ErrVersionMismatch, "lock version mismatch"},
		{"ErrLockNotFound", ErrLockNotFound, "lock not found"},
		{"ErrInvalidTTL", ErrInvalidTTL, "invalid TTL value"},
		{"ErrWaitQueueFull", ErrWaitQueueFull, "wait queue is full"},
		{"ErrNotWaiting", ErrNotWaiting, "client is not waiting in the queue"},
		{"ErrTimeout", ErrTimeout, "operation timed out"},
		{"ErrLeaderUnavailable", ErrLeaderUnavailable, "no leader available"},
		{"ErrInvalidArgument", ErrInvalidArgument, "invalid argument"},
		{"ErrUnavailable", ErrUnavailable, "service unavailable"},
		{"ErrRateLimit", ErrRateLimit, "request rate limited"},
		{"ErrClientClosed", ErrClientClosed, "client is closed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Error() != tt.msg {
				t.Errorf("%s.Error() = %q, want %q", tt.name, tt.err.Error(), tt.msg)
			}
		})
	}

	allErrors := []error{
		ErrLockHeld, ErrNotLockOwner, ErrVersionMismatch, ErrLockNotFound,
		ErrInvalidTTL, ErrWaitQueueFull, ErrNotWaiting, ErrTimeout,
		ErrLeaderUnavailable, ErrInvalidArgument, ErrUnavailable,
		ErrRateLimit, ErrClientClosed,
	}

	for i, err1 := range allErrors {
		for j, err2 := range allErrors {
			if i != j && errors.Is(err1, err2) {
				t.Errorf("Error %v should not equal error %v", err1, err2)
			}
		}
	}
}
