//go:build !production

package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
)

// rwOperationLocker defines an interface for concurrency-safe execution
// of critical sections using RWMutex. It includes optional timeout handling
// and slow-operation tracking. Both read and write operations are supported.
type rwOperationLocker interface {

	// DoRead executes the given function under a read lock.
	// It respects context cancellation and logs if the operation is slow or blocked.
	DoRead(ctx context.Context, fn func() error) error

	// DoWrite executes the given function under a write lock.
	// It supports optional lock acquisition timeout and slow operation tracking.
	DoWrite(ctx context.Context, fn func() error) error
}

// defaultRWOperationLocker is a concrete implementation of rwOperationLocker.
// It wraps a sync.RWMutex and augments it with timeout support, metrics,
// and logging for slow operations.
type defaultRWOperationLocker struct {
	lock            *sync.RWMutex
	logger          logger.Logger
	enableMetrics   bool
	slowOpsCounter  *atomic.Uint64
	lockTimeout     time.Duration
	useTimeout      bool
	slowOpThreshold time.Duration

	// test-only: hook to trigger behavior after lock acquisition
	afterLock func() // called only in tests to sync cancellation after lock is acquired
}

func newRWOperationLocker(
	lock *sync.RWMutex,
	logger logger.Logger,
	options FileStorageOptions,
	slowOpsCounter *atomic.Uint64,
) rwOperationLocker {
	return &defaultRWOperationLocker{
		lock:            lock,
		logger:          logger.WithComponent("locker"),
		enableMetrics:   options.Features.EnableMetrics,
		slowOpsCounter:  slowOpsCounter,
		lockTimeout:     time.Duration(options.LockTimeout) * time.Second,
		useTimeout:      options.Features.EnableLockTimeout,
		slowOpThreshold: defaultSlowOpThreshold,
	}
}

// DoRead acquires a read lock and executes the given function.
// If lock timeouts are enabled, it will fail if the lock cannot be acquired in time.
// It also logs and tracks operations exceeding the slowOpThreshold.
func (ol *defaultRWOperationLocker) DoRead(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if ol.useTimeout {
		return ol.withTimeout(ctx, fn, ol.lock.RLock, ol.lock.RUnlock, "read")
	}

	ol.lock.RLock()
	defer ol.lock.RUnlock()

	return ol.trackSlowOperation(fn, "read")
}

// DoWrite acquires a write lock and executes the given function.
// It handles optional timeouts and logs slow operations if metrics are enabled.
func (ol *defaultRWOperationLocker) DoWrite(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if ol.useTimeout {
		return ol.withTimeout(ctx, fn, ol.lock.Lock, ol.lock.Unlock, "write")
	}

	ol.lock.Lock()
	defer ol.lock.Unlock()

	return ol.trackSlowOperation(fn, "write")
}

// withTimeout attempts to acquire the lock using a timeout context.
// If the lock is acquired within the timeout, it runs the provided function
// and logs slow operations. Otherwise, it logs a warning and returns an error.
func (ol *defaultRWOperationLocker) withTimeout(
	ctx context.Context,
	fn func() error,
	lockFn func(),
	unlockFn func(),
	opType string,
) error {
	lockCtx, cancel := context.WithTimeout(ctx, ol.lockTimeout)
	defer cancel()

	acquired := make(chan struct{})
	go func() {
		lockFn()
		close(acquired)
	}()

	select {
	case <-acquired:
		defer unlockFn()

		// test-only hook
		if ol.afterLock != nil {
			ol.afterLock()
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		return ol.trackSlowOperation(fn, opType)

	case <-lockCtx.Done():
		ol.logger.Warnw("Lock acquisition timed out",
			"operation", opType,
			"timeout", ol.lockTimeout,
			"contextErr", ctx.Err(),
		)
		return fmt.Errorf("lock acquisition timeout for %s operation: %w", opType, lockCtx.Err())
	}
}

// trackSlowOperation measures execution time of the operation.
// If it exceeds the configured threshold, it logs a warning and updates metrics.
func (ol *defaultRWOperationLocker) trackSlowOperation(fn func() error, opType string) error {
	start := time.Now()
	err := fn()
	duration := time.Since(start)

	if duration > ol.slowOpThreshold {
		ol.logger.Warnw("Slow storage operation",
			"operation", opType,
			"duration", duration,
			"threshold", ol.slowOpThreshold,
		)

		if ol.enableMetrics && ol.slowOpsCounter != nil {
			ol.slowOpsCounter.Add(1)
		}
	}

	return err
}
