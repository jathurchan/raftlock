package server

import (
	"errors"
	"fmt"
	"testing"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/testutil"
)

func TestPredefinedErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrServerNotStarted", ErrServerNotStarted, "server: server not started or not ready"},
		{"ErrServerAlreadyStarted", ErrServerAlreadyStarted, "server: server already started"},
		{"ErrServerStopped", ErrServerStopped, "server: server stopped"},
		{"ErrNotLeader", ErrNotLeader, "server: this node is not the Raft leader"},
		{"ErrNoLeader", ErrNoLeader, "server: no leader available in the cluster"},
		{"ErrRequestTooLarge", ErrRequestTooLarge, "server: request too large"},
		{"ErrResponseTooLarge", ErrResponseTooLarge, "server: response too large"},
		{"ErrRateLimited", ErrRateLimited, "server: request rate limited"},
		{"ErrShutdownTimeout", ErrShutdownTimeout, "server: shutdown timed out"},
		{"ErrInvalidRequest", ErrInvalidRequest, "server: invalid request"},
		{"ErrRaftUnavailable", ErrRaftUnavailable, "server: raft consensus system unavailable"},
		{
			"ErrLockManagerUnavailable",
			ErrLockManagerUnavailable,
			"server: lock manager unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testutil.AssertEqual(t, tt.want, tt.err.Error())
		})
	}
}

func TestLeaderRedirectError_NewLeaderRedirectError(t *testing.T) {
	tests := []struct {
		name          string
		leaderAddress string
		leaderID      string
		wantAddress   string
		wantID        string
	}{
		{
			name:          "both address and ID provided",
			leaderAddress: "192.168.1.100:8080",
			leaderID:      "node-1",
			wantAddress:   "192.168.1.100:8080",
			wantID:        "node-1",
		},
		{
			name:          "empty address and ID",
			leaderAddress: "",
			leaderID:      "",
			wantAddress:   "",
			wantID:        "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewLeaderRedirectError(tt.leaderAddress, tt.leaderID)
			testutil.AssertEqual(t, tt.wantAddress, err.LeaderAddress)
			testutil.AssertEqual(t, tt.wantID, err.LeaderID)
		})
	}
}

func TestLeaderRedirectError_Error(t *testing.T) {
	tests := []struct {
		name          string
		leaderAddress string
		leaderID      string
		want          string
	}{
		{
			name:          "both address and ID provided",
			leaderAddress: "192.168.1.100:8080",
			leaderID:      "node-1",
			want:          "server: not leader, redirect to node node-1 at 192.168.1.100:8080",
		},
		{
			name:          "only address provided",
			leaderAddress: "192.168.1.100:8080",
			leaderID:      "",
			want:          "server: not leader, leader information unavailable for redirect",
		},
		{
			name:          "only ID provided",
			leaderAddress: "",
			leaderID:      "node-1",
			want:          "server: not leader, leader information unavailable for redirect",
		},
		{
			name:          "neither address nor ID provided",
			leaderAddress: "",
			leaderID:      "",
			want:          "server: not leader, leader information unavailable for redirect",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &LeaderRedirectError{
				LeaderAddress: tt.leaderAddress,
				LeaderID:      tt.leaderID,
			}
			testutil.AssertEqual(t, tt.want, err.Error())
		})
	}
}

func TestValidationError_NewValidationError(t *testing.T) {
	field := "lock_id"
	value := "invalid-lock"
	message := "lock_id contains invalid characters"

	err := NewValidationError(field, value, message)

	testutil.AssertEqual(t, field, err.Field)
	testutil.AssertEqual(t, value, err.Value)
	testutil.AssertEqual(t, message, err.Message)
}

func TestValidationError_Error(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		value   any
		message string
		want    string
	}{
		{
			name:    "string value",
			field:   "lock_id",
			value:   "invalid-lock",
			message: "lock_id contains invalid characters",
			want:    "server: validation error for field 'lock_id' (value: invalid-lock): lock_id contains invalid characters",
		},
		{
			name:    "integer value",
			field:   "priority",
			value:   9999,
			message: "priority must be between -1000 and 1000",
			want:    "server: validation error for field 'priority' (value: 9999): priority must be between -1000 and 1000",
		},
		{
			name:    "nil value",
			field:   "ttl",
			value:   nil,
			message: "ttl is required",
			want:    "server: validation error for field 'ttl' (value: <nil>): ttl is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &ValidationError{
				Field:   tt.field,
				Value:   tt.value,
				Message: tt.message,
			}
			testutil.AssertEqual(t, tt.want, err.Error())
		})
	}
}

func TestServerError_NewServerError(t *testing.T) {
	operation := "acquire_lock"
	cause := errors.New("underlying error")
	message := "failed to process request"

	err := NewServerError(operation, cause, message)

	testutil.AssertEqual(t, operation, err.Operation)
	testutil.AssertEqual(t, cause, err.Cause)
	testutil.AssertEqual(t, message, err.Message)
}

func TestServerError_Error(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		cause     error
		message   string
		want      string
	}{
		{
			name:      "with cause",
			operation: "acquire_lock",
			cause:     errors.New("underlying error"),
			message:   "failed to process request",
			want:      "server: error during acquire_lock: failed to process request (cause: underlying error)",
		},
		{
			name:      "without cause",
			operation: "release_lock",
			cause:     nil,
			message:   "lock not found",
			want:      "server: error during release_lock: lock not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &ServerError{
				Operation: tt.operation,
				Cause:     tt.cause,
				Message:   tt.message,
			}
			testutil.AssertEqual(t, tt.want, err.Error())
		})
	}
}

func TestServerError_Unwrap(t *testing.T) {
	t.Run("with cause", func(t *testing.T) {
		cause := errors.New("underlying error")
		err := &ServerError{
			Operation: "test",
			Cause:     cause,
			Message:   "test message",
		}
		testutil.AssertEqual(t, cause, err.Unwrap())
	})

	t.Run("without cause", func(t *testing.T) {
		err := &ServerError{
			Operation: "test",
			Cause:     nil,
			Message:   "test message",
		}
		testutil.AssertEqual(t, nil, err.Unwrap())
	})
}

func TestErrorToProtoError_NilError(t *testing.T) {
	result := ErrorToProtoError(nil)
	testutil.AssertNil(t, result)
}

func TestErrorToProtoError_LeaderRedirectError(t *testing.T) {
	tests := []struct {
		name          string
		leaderAddress string
		leaderID      string
		wantDetails   map[string]string
	}{
		{
			name:          "both address and ID",
			leaderAddress: "192.168.1.100:8080",
			leaderID:      "node-1",
			wantDetails: map[string]string{
				"leader_address": "192.168.1.100:8080",
				"leader_id":      "node-1",
			},
		},
		{
			name:          "only address",
			leaderAddress: "192.168.1.100:8080",
			leaderID:      "",
			wantDetails: map[string]string{
				"leader_address": "192.168.1.100:8080",
			},
		},
		{
			name:          "only ID",
			leaderAddress: "",
			leaderID:      "node-1",
			wantDetails: map[string]string{
				"leader_id": "node-1",
			},
		},
		{
			name:          "neither address nor ID",
			leaderAddress: "",
			leaderID:      "",
			wantDetails:   map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewLeaderRedirectError(tt.leaderAddress, tt.leaderID)
			result := ErrorToProtoError(err)

			testutil.AssertNotNil(t, result)
			testutil.AssertEqual(t, pb.ErrorCode_NOT_LEADER, result.Code)
			testutil.AssertEqual(t, err.Error(), result.Message)
			testutil.AssertEqual(t, tt.wantDetails, result.Details)
		})
	}
}

func TestErrorToProtoError_ValidationError(t *testing.T) {
	err := NewValidationError("lock_id", "invalid", "invalid lock ID format")
	result := ErrorToProtoError(err)

	testutil.AssertNotNil(t, result)
	testutil.AssertEqual(t, pb.ErrorCode_INVALID_ARGUMENT, result.Code)
	testutil.AssertEqual(t, err.Message, result.Message)
	testutil.AssertEqual(t, map[string]string{
		"field": "lock_id",
		"value": "invalid",
	}, result.Details)
}

func TestErrorToProtoError_ServerError(t *testing.T) {
	tests := []struct {
		name        string
		operation   string
		cause       error
		message     string
		wantDetails map[string]string
	}{
		{
			name:      "with cause",
			operation: "acquire",
			cause:     errors.New("database error"),
			message:   "failed to acquire lock",
			wantDetails: map[string]string{
				"operation": "acquire",
				"cause":     "database error",
			},
		},
		{
			name:      "without cause",
			operation: "release",
			cause:     nil,
			message:   "lock not found",
			wantDetails: map[string]string{
				"operation": "release",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewServerError(tt.operation, tt.cause, tt.message)
			result := ErrorToProtoError(err)

			testutil.AssertNotNil(t, result)
			testutil.AssertEqual(t, pb.ErrorCode_INTERNAL_ERROR, result.Code)
			testutil.AssertEqual(t, tt.message, result.Message)
			testutil.AssertEqual(t, tt.wantDetails, result.Details)
		})
	}
}

func TestErrorToProtoError_PredefinedErrors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode pb.ErrorCode
	}{
		{"ErrNotLeader", ErrNotLeader, pb.ErrorCode_NOT_LEADER},
		{"ErrNoLeader", ErrNoLeader, pb.ErrorCode_NO_LEADER},
		{"ErrRequestTooLarge", ErrRequestTooLarge, pb.ErrorCode_INVALID_ARGUMENT},
		{"ErrRateLimited", ErrRateLimited, pb.ErrorCode_RATE_LIMITED},
		{"ErrRaftUnavailable", ErrRaftUnavailable, pb.ErrorCode_UNAVAILABLE},
		{"ErrLockManagerUnavailable", ErrLockManagerUnavailable, pb.ErrorCode_UNAVAILABLE},
		{"ErrServerNotStarted", ErrServerNotStarted, pb.ErrorCode_UNAVAILABLE},
		{"ErrServerStopped", ErrServerStopped, pb.ErrorCode_UNAVAILABLE},
		{"ErrInvalidRequest", ErrInvalidRequest, pb.ErrorCode_INVALID_ARGUMENT},
		{"ErrServerAlreadyStarted", ErrServerAlreadyStarted, pb.ErrorCode_INTERNAL_ERROR},
		{"ErrResponseTooLarge", ErrResponseTooLarge, pb.ErrorCode_INTERNAL_ERROR},
		{"ErrShutdownTimeout", ErrShutdownTimeout, pb.ErrorCode_INTERNAL_ERROR},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ErrorToProtoError(tt.err)

			testutil.AssertNotNil(t, result)
			testutil.AssertEqual(t, tt.wantCode, result.Code)
			testutil.AssertEqual(t, tt.err.Error(), result.Message)
		})
	}
}

func TestErrorToProtoError_WrappedErrors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode pb.ErrorCode
	}{
		{
			name:     "wrapped ErrNotLeader",
			err:      fmt.Errorf("operation failed: %w", ErrNotLeader),
			wantCode: pb.ErrorCode_NOT_LEADER,
		},
		{
			name:     "wrapped ErrRateLimited",
			err:      fmt.Errorf("request rejected: %w", ErrRateLimited),
			wantCode: pb.ErrorCode_RATE_LIMITED,
		},
		{
			name:     "wrapped ErrRaftUnavailable",
			err:      fmt.Errorf("consensus failed: %w", ErrRaftUnavailable),
			wantCode: pb.ErrorCode_UNAVAILABLE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ErrorToProtoError(tt.err)

			testutil.AssertNotNil(t, result)
			testutil.AssertEqual(t, tt.wantCode, result.Code)
			testutil.AssertContains(t, result.Message, "server:")
		})
	}
}

func TestErrorToProtoError_UnknownError(t *testing.T) {
	err := errors.New("some unknown error")
	result := ErrorToProtoError(err)

	testutil.AssertNotNil(t, result)
	testutil.AssertEqual(t, pb.ErrorCode_INTERNAL_ERROR, result.Code)
	testutil.AssertEqual(t, "An unexpected internal error occurred.", result.Message)
}

func TestErrorToProtoError_ComplexErrorChains(t *testing.T) {
	t.Run("ValidationError wrapped in ServerError", func(t *testing.T) {
		validationErr := NewValidationError("field", "value", "message")
		serverErr := NewServerError("operation", validationErr, "server message")

		result := ErrorToProtoError(serverErr)

		testutil.AssertNotNil(t, result)
		testutil.AssertEqual(t, pb.ErrorCode_INVALID_ARGUMENT, result.Code)
		testutil.AssertEqual(t, "message", result.Message)
		testutil.AssertEqual(t, map[string]string{
			"field": "field",
			"value": "value",
		}, result.Details)
	})

	t.Run("LeaderRedirectError wrapped in ServerError", func(t *testing.T) {
		redirectErr := NewLeaderRedirectError("addr", "id")
		serverErr := NewServerError("operation", redirectErr, "server message")

		result := ErrorToProtoError(serverErr)

		testutil.AssertNotNil(t, result)
		testutil.AssertEqual(t, pb.ErrorCode_NOT_LEADER, result.Code)
		testutil.AssertEqual(t, redirectErr.Error(), result.Message)
		testutil.AssertEqual(t, map[string]string{
			"leader_address": "addr",
			"leader_id":      "id",
		}, result.Details)
	})

	t.Run("ServerError with non-special cause", func(t *testing.T) {
		cause := errors.New("database connection failed")
		serverErr := NewServerError("operation", cause, "server message")

		result := ErrorToProtoError(serverErr)

		testutil.AssertNotNil(t, result)
		testutil.AssertEqual(t, pb.ErrorCode_INTERNAL_ERROR, result.Code)
		testutil.AssertEqual(t, "server message", result.Message)
		testutil.AssertEqual(t, map[string]string{
			"operation": "operation",
			"cause":     "database connection failed",
		}, result.Details)
	})
}

func TestErrorTypes_AsInterface(t *testing.T) {
	t.Run("LeaderRedirectError implements error", func(t *testing.T) {
		var err error = NewLeaderRedirectError("addr", "id")
		testutil.AssertNotNil(t, err)
		testutil.AssertContains(t, err.Error(), "server: not leader")
	})

	t.Run("ValidationError implements error", func(t *testing.T) {
		var err error = NewValidationError("field", "value", "message")
		testutil.AssertNotNil(t, err)
		testutil.AssertContains(t, err.Error(), "validation error")
	})

	t.Run("ServerError implements error", func(t *testing.T) {
		var err error = NewServerError("op", nil, "message")
		testutil.AssertNotNil(t, err)
		testutil.AssertContains(t, err.Error(), "error during")
	})
}

func TestErrors_WithErrorsIs(t *testing.T) {
	t.Run("errors.Is works with predefined errors", func(t *testing.T) {
		wrappedErr := fmt.Errorf("wrapped: %w", ErrNotLeader)
		testutil.AssertTrue(t, errors.Is(wrappedErr, ErrNotLeader))
	})

	t.Run("errors.Is works with ServerError", func(t *testing.T) {
		cause := ErrRaftUnavailable
		serverErr := NewServerError("op", cause, "message")
		testutil.AssertTrue(t, errors.Is(serverErr, cause))
	})
}

func TestErrors_WithErrorsAs(t *testing.T) {
	t.Run("errors.As works with LeaderRedirectError", func(t *testing.T) {
		originalErr := NewLeaderRedirectError("addr", "id")
		wrappedErr := fmt.Errorf("wrapped: %w", originalErr)

		var redirectErr *LeaderRedirectError
		testutil.AssertTrue(t, errors.As(wrappedErr, &redirectErr))
		testutil.AssertEqual(t, "addr", redirectErr.LeaderAddress)
		testutil.AssertEqual(t, "id", redirectErr.LeaderID)
	})

	t.Run("errors.As works with ValidationError", func(t *testing.T) {
		originalErr := NewValidationError("field", "value", "message")
		wrappedErr := fmt.Errorf("wrapped: %w", originalErr)

		var validationErr *ValidationError
		testutil.AssertTrue(t, errors.As(wrappedErr, &validationErr))
		testutil.AssertEqual(t, "field", validationErr.Field)
		testutil.AssertEqual(t, "value", validationErr.Value)
		testutil.AssertEqual(t, "message", validationErr.Message)
	})

	t.Run("errors.As works with ServerError", func(t *testing.T) {
		originalErr := NewServerError("op", nil, "message")
		wrappedErr := fmt.Errorf("wrapped: %w", originalErr)

		var serverErr *ServerError
		testutil.AssertTrue(t, errors.As(wrappedErr, &serverErr))
		testutil.AssertEqual(t, "op", serverErr.Operation)
		testutil.AssertEqual(t, "message", serverErr.Message)
	})
}
