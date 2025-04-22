package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
)

const (
	testLockTimeout     = 50 * time.Millisecond
	testSlowOpThreshold = 10 * time.Millisecond
	testWaitMargin      = 20 * time.Millisecond
)

func newTestLocker(options FileStorageOptions, logger logger.Logger) (*defaultRWOperationLocker, *atomic.Uint64) {
	slowOpsCounter := &atomic.Uint64{}
	lock := &sync.RWMutex{}
	locker := newRWOperationLocker(lock, logger, options, slowOpsCounter).(*defaultRWOperationLocker)

	if locker.slowOpThreshold == defaultSlowOpThreshold {
		locker.slowOpThreshold = testSlowOpThreshold
	}
	if locker.lockTimeout == time.Duration(defaultLockTimeoutSeconds)*time.Second {
		locker.lockTimeout = testLockTimeout
	}

	return locker, slowOpsCounter
}

func TestRWOperationLocker_BasicReadWrite(t *testing.T) {
	mockLogger := logger.NewNoOpLogger().(*logger.NoOpLogger)
	opts := DefaultFileStorageOptions()
	opts.Features.EnableMetrics = false
	opts.Features.EnableLockTimeout = false
	locker, counter := newTestLocker(opts, mockLogger)

	t.Run("DoRead_Success", func(t *testing.T) {
		executed := false
		err := locker.DoRead(context.Background(), func() error {
			executed = true
			return nil
		})
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, executed)
		testutil.AssertEqual(t, uint64(0), counter.Load())
	})

	t.Run("DoWrite_Success", func(t *testing.T) {
		executed := false
		err := locker.DoWrite(context.Background(), func() error {
			executed = true
			return nil
		})
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, executed)
		testutil.AssertEqual(t, uint64(0), counter.Load())
	})
}

func TestRWOperationLocker_TimeoutAndSlowOps(t *testing.T) {
	var slowOpLogged, timeoutLogged bool
	mockLogger := logger.NewNoOpLogger().(*logger.NoOpLogger)
	mockLogger.WarnwFunc = func(msg string, keysAndValues ...any) {
		if msg == "Slow storage operation" {
			slowOpLogged = true
		} else if msg == "Lock acquisition timed out" {
			timeoutLogged = true
		}
	}

	opts := DefaultFileStorageOptions()
	opts.Features.EnableLockTimeout = true
	opts.Features.EnableMetrics = true
	locker, counter := newTestLocker(opts, mockLogger)

	t.Run("SlowOperationMetricsAndLog", func(t *testing.T) {
		slowOpLogged = false
		initial := counter.Load()
		err := locker.DoWrite(context.Background(), func() error {
			time.Sleep(testSlowOpThreshold + testWaitMargin)
			return nil
		})
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, slowOpLogged)
		testutil.AssertEqual(t, initial+1, counter.Load())
	})

	t.Run("LockTimeout", func(t *testing.T) {
		locker.lock.Lock()
		defer locker.lock.Unlock()

		timeoutLogged = false
		ctx, cancel := context.WithTimeout(context.Background(), testLockTimeout+testWaitMargin)
		defer cancel()

		err := locker.DoRead(ctx, func() error {
			t.Error("Should not run")
			return nil
		})

		testutil.AssertError(t, err)
		testutil.AssertTrue(t, timeoutLogged)
	})
}

func TestRWOperationLocker_ContextCancellation(t *testing.T) {
	mockLogger := logger.NewNoOpLogger().(*logger.NoOpLogger)
	opts := DefaultFileStorageOptions()
	opts.Features.EnableLockTimeout = true
	locker, _ := newTestLocker(opts, mockLogger)

	t.Run("ContextCancelledBefore", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		executed := false
		err := locker.DoWrite(ctx, func() error {
			executed = true
			return nil
		})
		testutil.AssertErrorIs(t, err, context.Canceled)
		testutil.AssertFalse(t, executed)
	})
}

func TestRWOperationLocker_Concurrency(t *testing.T) {
	t.Parallel()
	mockLogger := logger.NewNoOpLogger().(*logger.NoOpLogger)
	opts := DefaultFileStorageOptions()
	locker, _ := newTestLocker(opts, mockLogger)

	t.Run("ConcurrentReaders", func(t *testing.T) {
		var wg sync.WaitGroup
		var counter atomic.Int32
		const numReaders = 10
		for range numReaders {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := locker.DoRead(context.Background(), func() error {
					counter.Add(1)
					time.Sleep(5 * time.Millisecond)
					return nil
				})
				testutil.AssertNoError(t, err)
			}()
		}
		wg.Wait()
		testutil.AssertEqual(t, int32(numReaders), counter.Load())
	})

	t.Run("WriterBlocksReaders", func(t *testing.T) {
		var writeStart, readStart time.Time
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			_ = locker.DoWrite(context.Background(), func() error {
				writeStart = time.Now()
				time.Sleep(20 * time.Millisecond)
				return nil
			})
		}()

		time.Sleep(10 * time.Millisecond)

		go func() {
			defer wg.Done()
			_ = locker.DoRead(context.Background(), func() error {
				readStart = time.Now()
				return nil
			})
		}()

		wg.Wait()
		testutil.AssertTrue(t, readStart.After(writeStart) || readStart.Equal(writeStart))
	})
}

func TestRWOperationLocker_ContextCancelledBeforeExecution(t *testing.T) {
	mockLogger := logger.NewNoOpLogger().(*logger.NoOpLogger)
	opts := DefaultFileStorageOptions()
	locker, _ := newTestLocker(opts, mockLogger)

	t.Run("CancelledBefore_DoRead", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel right away
		err := locker.DoRead(ctx, func() error {
			t.Error("function should not run")
			return nil
		})
		testutil.AssertErrorIs(t, err, context.Canceled)
	})

	t.Run("CancelledBefore_DoWrite", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := locker.DoWrite(ctx, func() error {
			t.Error("function should not run")
			return nil
		})
		testutil.AssertErrorIs(t, err, context.Canceled)
	})
}

func TestRWOperationLocker_CoversCtxErrAfterLock(t *testing.T) {
	mockLogger := logger.NewNoOpLogger().(*logger.NoOpLogger)
	opts := DefaultFileStorageOptions()
	opts.Features.EnableLockTimeout = true

	ctx, cancel := context.WithCancel(context.Background())

	lock := &sync.RWMutex{}
	counter := &atomic.Uint64{}
	locker := newRWOperationLocker(lock, mockLogger, opts, counter).(*defaultRWOperationLocker)

	called := false

	locker.afterLock = func() {
		cancel()
		time.Sleep(10 * time.Millisecond)
	}

	err := locker.DoWrite(ctx, func() error {
		called = true
		return nil
	})

	testutil.AssertErrorIs(t, err, context.Canceled)
	testutil.AssertFalse(t, called, "Function should NOT be called if ctx.Err() is set after lock acquired")
}
