package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/jathurchan/raftlock/logger"
	pb "github.com/jathurchan/raftlock/proto"
)

// RequestValidator defines the interface for validating incoming gRPC requests
// to the RaftLock server. Each method should return an error if the request is invalid.
type RequestValidator interface {
	// ValidateAcquireRequest validates an AcquireRequest.
	ValidateAcquireRequest(req *pb.AcquireRequest) error

	// ValidateReleaseRequest validates a ReleaseRequest.
	ValidateReleaseRequest(req *pb.ReleaseRequest) error

	// ValidateRenewRequest validates a RenewRequest.
	ValidateRenewRequest(req *pb.RenewRequest) error

	// ValidateGetLockInfoRequest validates a GetLockInfoRequest.
	ValidateGetLockInfoRequest(req *pb.GetLockInfoRequest) error

	// ValidateGetLocksRequest validates a GetLocksRequest.
	ValidateGetLocksRequest(req *pb.GetLocksRequest) error

	// ValidateEnqueueWaiterRequest validates an EnqueueWaiterRequest.
	ValidateEnqueueWaiterRequest(req *pb.EnqueueWaiterRequest) error

	// ValidateCancelWaitRequest validates a CancelWaitRequest.
	ValidateCancelWaitRequest(req *pb.CancelWaitRequest) error

	// ValidateBackoffAdviceRequest validates a BackoffAdviceRequest.
	ValidateBackoffAdviceRequest(req *pb.BackoffAdviceRequest) error

	// ValidateGetStatusRequest validates a GetStatusRequest.
	ValidateGetStatusRequest(req *pb.GetStatusRequest) error

	// ValidateHealthRequest validates a HealthRequest.
	ValidateHealthRequest(req *pb.HealthRequest) error
}

// requestValidator implements the RequestValidator interface.
type requestValidator struct {
	logger logger.Logger
}

// NewRequestValidator creates a new default request validator.
func NewRequestValidator(logger logger.Logger) RequestValidator {
	return &requestValidator{
		logger: logger,
	}
}

// ValidateAcquireRequest validates an acquire lock request.
func (v *requestValidator) ValidateAcquireRequest(req *pb.AcquireRequest) error {
	if err := v.validateLockID(req.LockId); err != nil {
		return err
	}
	if err := v.validateClientID(req.ClientId); err != nil {
		return err
	}
	if req.Ttl != nil {
		if err := v.validateTTL(req.Ttl.AsDuration()); err != nil {
			return err
		}
	}
	if req.WaitTimeout != nil {
		if err := v.validateWaitTimeout(req.WaitTimeout.AsDuration()); err != nil {
			return err
		}
	}
	if err := v.validatePriority(req.Priority); err != nil {
		return err
	}
	if err := v.validateMetadata(req.Metadata); err != nil {
		return err
	}
	if err := v.validateRequestID(req.RequestId); err != nil {
		return err
	}
	return nil
}

// ValidateReleaseRequest validates a release lock request.
func (v *requestValidator) ValidateReleaseRequest(req *pb.ReleaseRequest) error {
	if err := v.validateLockID(req.LockId); err != nil {
		return err
	}
	if err := v.validateClientID(req.ClientId); err != nil {
		return err
	}
	if req.Version <= 0 {
		return NewValidationError("version", req.Version, "version must be positive")
	}
	return nil
}

// ValidateRenewRequest validates a renew lock request.
func (v *requestValidator) ValidateRenewRequest(req *pb.RenewRequest) error {
	if err := v.validateLockID(req.LockId); err != nil {
		return err
	}
	if err := v.validateClientID(req.ClientId); err != nil {
		return err
	}
	if req.Version <= 0 {
		return NewValidationError("version", req.Version, "version must be positive")
	}
	if req.NewTtl == nil {
		return NewValidationError("new_ttl", nil, "new_ttl is required")
	}
	if err := v.validateTTL(req.NewTtl.AsDuration()); err != nil {
		return err
	}
	return nil
}

// ValidateGetLockInfoRequest validates a get lock info request.
func (v *requestValidator) ValidateGetLockInfoRequest(req *pb.GetLockInfoRequest) error {
	return v.validateLockID(req.LockId)
}

// ValidateGetLocksRequest validates a get locks request.
func (v *requestValidator) ValidateGetLocksRequest(req *pb.GetLocksRequest) error {
	if req.Limit < 0 {
		return NewValidationError("limit", req.Limit, "limit cannot be negative")
	}
	if req.Limit > MaxPageLimit {
		return NewValidationError("limit", req.Limit,
			fmt.Sprintf("limit cannot exceed %d", MaxPageLimit))
	}
	if req.Offset < 0 {
		return NewValidationError("offset", req.Offset, "offset cannot be negative")
	}
	if req.Filter != nil {
		if err := v.validateLockFilter(req.Filter); err != nil {
			return err
		}
	}
	return nil
}

// ValidateEnqueueWaiterRequest validates an enqueue waiter request.
func (v *requestValidator) ValidateEnqueueWaiterRequest(req *pb.EnqueueWaiterRequest) error {
	if err := v.validateLockID(req.LockId); err != nil {
		return err
	}
	if err := v.validateClientID(req.ClientId); err != nil {
		return err
	}
	if req.Timeout == nil {
		return NewValidationError("timeout", nil, "timeout is required for enqueue waiter")
	}
	if err := v.validateWaitTimeout(req.Timeout.AsDuration()); err != nil {
		return err
	}
	if err := v.validatePriority(req.Priority); err != nil {
		return err
	}
	if req.Version < 0 {
		return NewValidationError("version", req.Version, "version cannot be negative")
	}
	return nil
}

// ValidateCancelWaitRequest validates a cancel wait request.
func (v *requestValidator) ValidateCancelWaitRequest(req *pb.CancelWaitRequest) error {
	if err := v.validateLockID(req.LockId); err != nil {
		return err
	}
	if err := v.validateClientID(req.ClientId); err != nil {
		return err
	}
	if req.Version < 0 {
		return NewValidationError("version", req.Version, "version cannot be negative")
	}
	return nil
}

// ValidateBackoffAdviceRequest validates a backoff advice request.
func (v *requestValidator) ValidateBackoffAdviceRequest(req *pb.BackoffAdviceRequest) error {
	return v.validateLockID(req.LockId)
}

// ValidateGetStatusRequest validates a GetStatusRequest.
func (v *requestValidator) ValidateGetStatusRequest(req *pb.GetStatusRequest) error {
	// No specific validation rules for GetStatusRequest fields beyond what gRPC does.
	return nil
}

// ValidateHealthRequest validates a HealthRequest.
func (v *requestValidator) ValidateHealthRequest(req *pb.HealthRequest) error {
	// No specific validation rules for HealthRequest fields beyond what gRPC does.
	return nil
}

func (v *requestValidator) validateLockID(lockID string) error {
	if lockID == "" {
		return NewValidationError("lock_id", lockID, "lock_id cannot be empty")
	}
	if len(lockID) > MaxLockIDLength {
		return NewValidationError("lock_id", lockID, fmt.Sprintf(ErrMsgInvalidLockID, MaxLockIDLength))
	}
	if strings.ContainsAny(lockID, "\x00\n\r\t") {
		return NewValidationError("lock_id", lockID, "lock_id contains invalid characters (null, newline, tab)")
	}
	return nil
}

func (v *requestValidator) validateClientID(clientID string) error {
	if clientID == "" {
		return NewValidationError("client_id", clientID, "client_id cannot be empty")
	}
	if len(clientID) > MaxClientIDLength {
		return NewValidationError("client_id", clientID, fmt.Sprintf(ErrMsgInvalidClientID, MaxClientIDLength))
	}
	if strings.ContainsAny(clientID, "\x00\n\r\t") {
		return NewValidationError("client_id", clientID, "client_id contains invalid characters (null, newline, tab)")
	}
	return nil
}

func (v *requestValidator) validateTTL(ttl time.Duration) error {
	if ttl < MinLockTTL || ttl > MaxLockTTL {
		return NewValidationError("ttl", ttl.String(), fmt.Sprintf(ErrMsgInvalidTTL, MinLockTTL, MaxLockTTL))
	}
	return nil
}

func (v *requestValidator) validateWaitTimeout(timeout time.Duration) error {
	if timeout < MinWaitTimeout || timeout > MaxWaitTimeout {
		return NewValidationError("timeout", timeout.String(), fmt.Sprintf(ErrMsgInvalidTimeout, MinWaitTimeout, MaxWaitTimeout))
	}
	return nil
}

func (v *requestValidator) validatePriority(priority int32) error {
	if priority < MinPriority || priority > MaxPriority {
		return NewValidationError("priority", priority, fmt.Sprintf(ErrMsgInvalidPriority, MinPriority, MaxPriority))
	}
	return nil
}

func (v *requestValidator) validateMetadata(metadata map[string]string) error {
	if metadata == nil { // Allow nil or empty metadata map
		return nil
	}
	if len(metadata) > MaxMetadataEntries {
		return NewValidationError("metadata", fmt.Sprintf("entry_count: %d", len(metadata)), fmt.Sprintf(ErrMsgTooManyMetadata, MaxMetadataEntries))
	}
	for key, value := range metadata {
		if len(key) == 0 {
			return NewValidationError("metadata.key", key, "metadata key cannot be empty")
		}
		if len(key) > MaxMetadataKeyLength {
			return NewValidationError("metadata.key", key, fmt.Sprintf(ErrMsgMetadataKeyTooLong, MaxMetadataKeyLength))
		}
		if len(value) > MaxMetadataValueLength {
			return NewValidationError("metadata.value", fmt.Sprintf("len:%d", len(value)), fmt.Sprintf(ErrMsgMetadataValueTooLong, MaxMetadataValueLength))
		}
	}
	return nil
}

func (v *requestValidator) validateRequestID(requestID string) error {
	if requestID != "" && len(requestID) > MaxRequestIDLength {
		return NewValidationError("request_id", requestID, fmt.Sprintf("request_id length cannot exceed %d characters", MaxRequestIDLength))
	}
	return nil
}

func (v *requestValidator) validateLockFilter(filter *pb.LockFilter) error {
	if filter.LockIdPattern != "" && len(filter.LockIdPattern) > MaxLockIDLength {
		return NewValidationError("filter.lock_id_pattern", filter.LockIdPattern, fmt.Sprintf("lock_id_pattern too long (max %d)", MaxLockIDLength))
	}
	if filter.OwnerIdPattern != "" && len(filter.OwnerIdPattern) > MaxClientIDLength {
		return NewValidationError("filter.owner_id_pattern", filter.OwnerIdPattern, fmt.Sprintf("owner_id_pattern too long (max %d)", MaxClientIDLength))
	}
	if filter.ExpiresBefore != nil && !filter.ExpiresBefore.IsValid() {
		return NewValidationError("filter.expires_before", filter.ExpiresBefore, "invalid timestamp for expires_before")
	}
	if filter.ExpiresAfter != nil && !filter.ExpiresAfter.IsValid() {
		return NewValidationError("filter.expires_after", filter.ExpiresAfter, "invalid timestamp for expires_after")
	}
	if filter.MetadataFilter != nil {
		if err := v.validateMetadata(filter.MetadataFilter); err != nil {
			return NewValidationError("filter.metadata_filter", "sub-validation failed", err.Error())
		}
	}
	return nil
}
