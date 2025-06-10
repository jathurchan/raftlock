package client

import (
	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// protoToLock converts a protobuf Lock message to the internal Lock type.
// Returns nil if the input is nil.
func protoToLock(p *pb.Lock) *Lock {
	if p == nil {
		return nil
	}
	return &Lock{
		LockID:     p.LockId,
		OwnerID:    p.OwnerId,
		Version:    p.Version,
		AcquiredAt: p.AcquiredAt.AsTime(),
		ExpiresAt:  p.ExpiresAt.AsTime(),
		Metadata:   p.Metadata,
	}
}

// protoToLockInfo converts a protobuf LockInfo message to the internal LockInfo type.
// Returns nil if the input is nil.
func protoToLockInfo(p *pb.LockInfo) *LockInfo {
	if p == nil {
		return nil
	}
	waiters := make([]*WaiterInfo, len(p.WaitersInfo))
	for i, w := range p.WaitersInfo {
		waiters[i] = protoToWaiterInfo(w)
	}
	return &LockInfo{
		LockID:         p.LockId,
		OwnerID:        p.OwnerId,
		Version:        p.Version,
		AcquiredAt:     p.AcquiredAt.AsTime(),
		ExpiresAt:      p.ExpiresAt.AsTime(),
		WaiterCount:    p.WaiterCount,
		WaitersInfo:    waiters,
		Metadata:       p.Metadata,
		LastModifiedAt: p.LastModifiedAt.AsTime(),
	}
}

// protoToWaiterInfo converts a protobuf WaiterInfo message to the internal WaiterInfo type.
// Returns nil if the input is nil.
func protoToWaiterInfo(p *pb.WaiterInfo) *WaiterInfo {
	if p == nil {
		return nil
	}
	return &WaiterInfo{
		ClientID:   p.ClientId,
		EnqueuedAt: p.EnqueuedAt.AsTime(),
		TimeoutAt:  p.TimeoutAt.AsTime(),
		Priority:   p.Priority,
		Position:   p.Position,
	}
}

// protoToBackoffAdvice converts a protobuf BackoffAdvice message to the internal BackoffAdvice type.
// Returns nil if the input is nil.
func protoToBackoffAdvice(p *pb.BackoffAdvice) *BackoffAdvice {
	if p == nil {
		return nil
	}
	return &BackoffAdvice{
		InitialBackoff: p.InitialBackoff.AsDuration(),
		MaxBackoff:     p.MaxBackoff.AsDuration(),
		Multiplier:     p.Multiplier,
		JitterFactor:   p.JitterFactor,
	}
}

// protoToErrorDetail converts a protobuf ErrorDetail message to the internal ErrorDetail type.
// Returns nil if the input is nil.
func protoToErrorDetail(p *pb.ErrorDetail) *ErrorDetail {
	if p == nil {
		return nil
	}
	return &ErrorDetail{
		Code:    p.Code,
		Message: p.Message,
		Details: p.Details,
	}
}

// protoToAcquireResult converts a protobuf AcquireResponse to an AcquireResult.
// Returns nil if the input is nil.
func protoToAcquireResult(p *pb.AcquireResponse) *AcquireResult {
	if p == nil {
		return nil
	}
	return &AcquireResult{
		Acquired:              p.Acquired,
		Lock:                  protoToLock(p.Lock),
		BackoffAdvice:         protoToBackoffAdvice(p.BackoffAdvice),
		QueuePosition:         p.QueuePosition,
		EstimatedWaitDuration: p.EstimatedWaitDuration.AsDuration(),
		Error:                 protoToErrorDetail(p.Error),
	}
}

// protoToReleaseResult converts a protobuf ReleaseResponse to a ReleaseResult.
// Returns nil if the input is nil.
func protoToReleaseResult(p *pb.ReleaseResponse) *ReleaseResult {
	if p == nil {
		return nil
	}
	return &ReleaseResult{
		Released:       p.Released,
		WaiterPromoted: p.WaiterPromoted,
		Error:          protoToErrorDetail(p.Error),
	}
}

// protoToRenewResult converts a protobuf RenewResponse to a RenewResult.
// Returns nil if the input is nil.
func protoToRenewResult(p *pb.RenewResponse) *RenewResult {
	if p == nil {
		return nil
	}
	return &RenewResult{
		Renewed: p.Renewed,
		Lock:    protoToLock(p.Lock),
		Error:   protoToErrorDetail(p.Error),
	}
}

// protoToGetLocksResult converts a protobuf GetLocksResponse to a GetLocksResult.
// Returns nil if the input is nil.
func protoToGetLocksResult(p *pb.GetLocksResponse) *GetLocksResult {
	if p == nil {
		return nil
	}
	locks := make([]*LockInfo, len(p.Locks))
	for i, l := range p.Locks {
		locks[i] = protoToLockInfo(l)
	}
	return &GetLocksResult{
		Locks:         locks,
		TotalMatching: p.TotalMatchingFilter,
		HasMore:       p.HasMore,
	}
}

// lockFilterToProto converts a LockFilter to its protobuf representation.
// Returns nil if the input is nil.
func lockFilterToProto(f *LockFilter) *pb.LockFilter {
	if f == nil {
		return nil
	}
	pbFilter := &pb.LockFilter{
		LockIdPattern:  f.LockIDPattern,
		OwnerIdPattern: f.OwnerIDPattern,
		OnlyHeld:       f.OnlyHeld,
		OnlyContested:  f.OnlyContested,
		MetadataFilter: f.MetadataFilter,
	}
	if f.ExpiresBefore != nil {
		pbFilter.ExpiresBefore = timestamppb.New(*f.ExpiresBefore)
	}
	if f.ExpiresAfter != nil {
		pbFilter.ExpiresAfter = timestamppb.New(*f.ExpiresAfter)
	}
	return pbFilter
}
