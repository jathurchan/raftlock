package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jathurchan/raftlock/cmd/benchmark/internal"
	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// performSingleOperation executes one non-blocking lock acquire and release.
func (s *BenchmarkSuite) performSingleOperation(
	client pb.RaftLockClient,
	lockID, clientID string,
	ttl time.Duration,
) bool {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.OperationTimeout)
	defer cancel()

	start := time.Now()

	acquireResp, err := client.Acquire(ctx, &pb.AcquireRequest{
		LockId:   lockID,
		ClientId: clientID,
		Ttl:      durationpb.New(ttl),
		Wait:     false,
	})

	if err != nil || !acquireResp.Acquired {
		s.metrics.RecordRequest("acquire", time.Since(start), err)
		return false
	}

	time.Sleep(10 * time.Millisecond)

	releaseStart := time.Now()
	_, releaseErr := client.Release(ctx, &pb.ReleaseRequest{
		LockId:   lockID,
		ClientId: clientID,
		Version:  acquireResp.Lock.Version,
	})

	s.metrics.RecordRequest("acquire", time.Since(start), err)
	s.metrics.RecordRequest("release", time.Since(releaseStart), releaseErr)

	return releaseErr == nil
}

// performEnhancedOperation executes a lock operation with retry, tracking full result metadata.
func (s *BenchmarkSuite) performEnhancedOperation(
	ctx context.Context,
	lockID, clientID string,
) OperationResult {
	result := OperationResult{
		LockID:       lockID,
		ClientID:     clientID,
		StartTime:    time.Now(),
		RetryPattern: make([]time.Duration, 0),
	}

	retryConfig := internal.RetryConfig{
		MaxAttempts: s.config.MaxRetries,
		BaseDelay:   s.config.BaseBackoff,
		MaxDelay:    s.config.MaxBackoff,
		Multiplier:  s.config.BackoffMultiplier,
		Jitter:      s.config.JitterFactor,
	}

	err := internal.Retry(ctx, retryConfig, func() error {
		client := s.clients.getHealthyConnection()
		if client == nil {
			return fmt.Errorf("no healthy client available")
		}
		return s.attemptLockOperation(ctx, client, lockID, clientID, &result)
	})

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	result.Success = err == nil
	if err != nil {
		result.Error = err.Error()
	}

	return result
}

// attemptLockOperation performs one acquire-and-release cycle and updates result details.
func (s *BenchmarkSuite) attemptLockOperation(
	ctx context.Context,
	client pb.RaftLockClient,
	lockID, clientID string,
	result *OperationResult,
) error {
	opCtx, cancel := context.WithTimeout(ctx, s.config.OperationTimeout)
	defer cancel()

	attemptStart := time.Now()

	acquireStart := time.Now()
	acquireResp, err := client.Acquire(opCtx, &pb.AcquireRequest{
		LockId:   lockID,
		ClientId: clientID,
		Ttl:      durationpb.New(2 * time.Second),
		Wait:     false,
	})

	result.AcquireLatency = time.Since(acquireStart)
	result.Attempts++
	result.RetryPattern = append(result.RetryPattern, time.Since(attemptStart))

	if err != nil {
		result.LastError = err.Error()

		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.DeadlineExceeded:
				result.WasTimeout = true
			case codes.NotFound, codes.FailedPrecondition:
				result.LeaderRedirect = true
			}
		}

		return fmt.Errorf("acquire failed: %w", err)
	}

	if !acquireResp.Acquired {
		result.LastError = "lock not acquired"
		return fmt.Errorf("lock not acquired")
	}

	time.Sleep(25 * time.Millisecond)

	releaseStart := time.Now()
	_, releaseErr := client.Release(opCtx, &pb.ReleaseRequest{
		LockId:   lockID,
		ClientId: clientID,
		Version:  acquireResp.Lock.Version,
	})

	result.ReleaseLatency = time.Since(releaseStart)

	if releaseErr != nil {
		result.LastError = releaseErr.Error()
		return fmt.Errorf("release failed: %w", releaseErr)
	}

	return nil
}
