package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/testutil"
)

func TestNewAutoRenewer(t *testing.T) {
	handle := newMockLockHandle()

	t.Run("ValidParameters", func(t *testing.T) {
		renewer, err := NewAutoRenewer(handle, time.Second, 2*time.Second)
		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, renewer)

		ar := renewer.(*autoRenewer)
		testutil.AssertEqual(t, handle, ar.handle)
		testutil.AssertEqual(t, time.Second, ar.interval)
		testutil.AssertEqual(t, 2*time.Second, ar.ttl)
		testutil.AssertNotNil(t, ar.clock)
	})

	t.Run("WithCustomClock", func(t *testing.T) {
		clock := newMockClock()
		renewer, err := NewAutoRenewer(handle, time.Second, 2*time.Second, WithClock(clock))
		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, renewer)

		ar := renewer.(*autoRenewer)
		testutil.AssertEqual(t, clock, ar.clock)
	})

	t.Run("NilHandle", func(t *testing.T) {
		renewer, err := NewAutoRenewer(nil, time.Second, 2*time.Second)
		testutil.AssertError(t, err)
		testutil.AssertNil(t, renewer)
		testutil.AssertContains(t, err.Error(), "lock handle cannot be nil")
	})

	t.Run("ZeroInterval", func(t *testing.T) {
		renewer, err := NewAutoRenewer(handle, 0, 2*time.Second)
		testutil.AssertError(t, err)
		testutil.AssertNil(t, renewer)
		testutil.AssertContains(t, err.Error(), "renewal interval must be positive")
	})

	t.Run("NegativeInterval", func(t *testing.T) {
		renewer, err := NewAutoRenewer(handle, -time.Second, 2*time.Second)
		testutil.AssertError(t, err)
		testutil.AssertNil(t, renewer)
		testutil.AssertContains(t, err.Error(), "renewal interval must be positive")
	})

	t.Run("TTLNotGreaterThanInterval", func(t *testing.T) {
		renewer, err := NewAutoRenewer(handle, 2*time.Second, 2*time.Second)
		testutil.AssertError(t, err)
		testutil.AssertNil(t, renewer)
		testutil.AssertContains(t, err.Error(), "lock TTL (2s) must be greater than renewal interval (2s)")
	})

	t.Run("TTLLessThanInterval", func(t *testing.T) {
		renewer, err := NewAutoRenewer(handle, 2*time.Second, time.Second)
		testutil.AssertError(t, err)
		testutil.AssertNil(t, renewer)
		testutil.AssertContains(t, err.Error(), "lock TTL (1s) must be greater than renewal interval (2s)")
	})
}

func TestAutoRenewer_StartAndStop(t *testing.T) {
	handle := newMockLockHandle()
	clock := newMockClock()

	renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond, WithClock(clock))
	testutil.RequireNoError(t, err)

	t.Run("StartSetsContext", func(t *testing.T) {
		ctx := context.Background()
		renewer.Start(ctx)

		ar := renewer.(*autoRenewer)
		testutil.AssertNotNil(t, ar.ctx)
		testutil.AssertNotNil(t, ar.cancel)

		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = renewer.Stop(stopCtx)
	})

	t.Run("StopWithoutStart", func(t *testing.T) {
		handle2 := newMockLockHandle()
		renewer2, err := NewAutoRenewer(handle2, 100*time.Millisecond, 200*time.Millisecond)
		testutil.RequireNoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err = renewer2.Stop(ctx)
		testutil.AssertNoError(t, err)
	})

	t.Run("StopTimeout", func(t *testing.T) {
		handle2 := newMockLockHandle()
		renewer2, err := NewAutoRenewer(handle2, 100*time.Millisecond, 200*time.Millisecond)
		testutil.RequireNoError(t, err)

		renewer2.Start(context.Background())

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		time.Sleep(10 * time.Millisecond) // Ensure context expires

		err = renewer2.Stop(ctx)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "timeout waiting for auto-renewer to stop")
	})
}

func TestAutoRenewer_RenewalLoop(t *testing.T) {
	t.Run("SuccessfulRenewals", func(t *testing.T) {
		handle := newMockLockHandle()
		clock := newMockClock()

		renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond, WithClock(clock))
		testutil.RequireNoError(t, err)

		ctx := context.Background()
		renewer.Start(ctx)

		time.Sleep(50 * time.Millisecond)

		clock.Advance(100 * time.Millisecond)
		time.Sleep(20 * time.Millisecond)

		clock.Advance(100 * time.Millisecond)
		time.Sleep(20 * time.Millisecond)

		clock.Advance(100 * time.Millisecond)
		time.Sleep(20 * time.Millisecond)

		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = renewer.Stop(stopCtx)

		renewCalls := handle.getRenewCalls()
		testutil.AssertTrue(t, renewCalls >= 2, "Expected at least 2 renewal calls, got %d", renewCalls)

		ttls := handle.getRenewedTTLs()
		for _, ttl := range ttls {
			testutil.AssertEqual(t, 200*time.Millisecond, ttl)
		}
	})

	t.Run("LockNotHeld", func(t *testing.T) {
		handle := newMockLockHandle()
		handle.setHeld(false) // Lock is not held
		clock := newMockClock()

		renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond, WithClock(clock))
		testutil.RequireNoError(t, err)

		ctx := context.Background()
		renewer.Start(ctx)

		time.Sleep(50 * time.Millisecond)

		clock.Advance(100 * time.Millisecond)
		time.Sleep(20 * time.Millisecond)

		ar := renewer.(*autoRenewer)
		select {
		case <-ar.Done():
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Renewer should have stopped when lock not held")
		}

		err = ar.Err()
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "auto-renewal stopped: lock not held")
	})

	t.Run("RenewalError", func(t *testing.T) {
		handle := newMockLockHandle()
		renewErr := errors.New("renewal failed")
		handle.setRenewError(renewErr)
		clock := newMockClock()

		renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond, WithClock(clock))
		testutil.RequireNoError(t, err)

		ctx := context.Background()
		renewer.Start(ctx)

		time.Sleep(50 * time.Millisecond)

		clock.Advance(100 * time.Millisecond)
		time.Sleep(20 * time.Millisecond)

		ar := renewer.(*autoRenewer)
		select {
		case <-ar.Done():
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Renewer should have stopped on renewal error")
		}

		err = ar.Err()
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "auto-renewal failed")
		testutil.AssertContains(t, err.Error(), "renewal failed")
	})
}

func TestAutoRenewer_Done(t *testing.T) {
	handle := newMockLockHandle()

	t.Run("BeforeStart", func(t *testing.T) {
		renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond)
		testutil.RequireNoError(t, err)

		doneCh := renewer.Done()
		testutil.AssertNotNil(t, doneCh)

		select {
		case <-doneCh:
		case <-time.After(10 * time.Millisecond):
			t.Fatal("Done channel should be closed when not started")
		}
	})

	t.Run("AfterStart", func(t *testing.T) {
		renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond)
		testutil.RequireNoError(t, err)

		ctx := context.Background()
		renewer.Start(ctx)

		doneCh := renewer.Done()
		testutil.AssertNotNil(t, doneCh)

		select {
		case <-doneCh:
			t.Fatal("Done channel should not be closed while running")
		case <-time.After(10 * time.Millisecond):
		}

		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = renewer.Stop(stopCtx)

		select {
		case <-doneCh:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Done channel should be closed after stop")
		}
	})
}

func TestAutoRenewer_Err(t *testing.T) {
	handle := newMockLockHandle()

	t.Run("NoError", func(t *testing.T) {
		renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond)
		testutil.RequireNoError(t, err)

		testutil.AssertNoError(t, renewer.Err())
	})

	t.Run("WithError", func(t *testing.T) {
		renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond)
		testutil.RequireNoError(t, err)

		ar := renewer.(*autoRenewer)
		testErr := errors.New("test error")
		ar.setError(testErr)

		testutil.AssertEqual(t, testErr, renewer.Err())
	})
}

func TestAutoRenewer_ConcurrentAccess(t *testing.T) {
	handle := newMockLockHandle()
	renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond)
	testutil.RequireNoError(t, err)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		renewer.Start(ctx)
	}()

	for range 10 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = renewer.Err()
		}()
		go func() {
			defer wg.Done()
			_ = renewer.Done()
		}()
	}

	time.Sleep(100 * time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = renewer.Stop(ctx)
	}()

	wg.Wait()
}

func TestAutoRenewer_ContextCancellation(t *testing.T) {
	handle := newMockLockHandle()
	clock := newMockClock()

	renewer, err := NewAutoRenewer(handle, 100*time.Millisecond, 200*time.Millisecond, WithClock(clock))
	testutil.RequireNoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	renewer.Start(ctx)

	time.Sleep(50 * time.Millisecond)

	cancel()

	ar := renewer.(*autoRenewer)
	select {
	case <-ar.Done():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Renewer should have stopped on context cancellation")
	}

	err = ar.Err()
	if err != nil {
		testutil.AssertTrue(t, errors.Is(err, context.Canceled), "Expected context.Canceled error, got: %v", err)
	}
}

func TestWithClock(t *testing.T) {
	mockClock := newMockClock()

	opt := WithClock(mockClock)
	testutil.AssertNotNil(t, opt)

	opts := &AutoRenewerOptions{}
	opt(opts)

	testutil.AssertEqual(t, mockClock, opts.Clock)
}

func TestAutoRenewer_MultipleTickers(t *testing.T) {
	handle := newMockLockHandle()
	clock := newMockClock()

	renewer, err := NewAutoRenewer(handle, 50*time.Millisecond, 100*time.Millisecond, WithClock(clock))
	testutil.RequireNoError(t, err)

	ctx := context.Background()
	renewer.Start(ctx)

	time.Sleep(50 * time.Millisecond)

	for range 5 {
		clock.Advance(50 * time.Millisecond)
		time.Sleep(10 * time.Millisecond) // Small delay for processing
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = renewer.Stop(stopCtx)

	renewCalls := handle.getRenewCalls()
	testutil.AssertTrue(t, renewCalls >= 3, "Expected at least 3 renewal calls, got %d", renewCalls)
}
