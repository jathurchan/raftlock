package client

import (
	"context"
	"fmt"
	"time"
)

var (
	// NewLockHandleFunc is a function variable that allows for easy mocking in tests.
	NewLockHandleFunc = func(client RaftLockClient, lockID, clientID string) (LockHandle, error) {
		return NewLockHandle(client, lockID, clientID)
	}

	// NewAutoRenewerFunc is a function variable for mocking the auto-renewer.
	NewAutoRenewerFunc = func(handle LockHandle, interval, ttl time.Duration, opts ...AutoRenewerOption) (AutoRenewer, error) {
		return NewAutoRenewer(handle, interval, ttl, opts...)
	}
)

// DoWithLock acquires a lock, executes a function, and then releases the lock.
func DoWithLock(ctx context.Context, client RaftLockClient, lockID, clientID string, ttl time.Duration, fn func(ctx context.Context) error) (err error) {
	handle, err := NewLockHandleFunc(client, lockID, clientID)
	if err != nil {
		return fmt.Errorf("failed to create lock handle: %w", err)
	}
	defer func() {
		releaseErr := handle.Close(context.Background())
		if err == nil {
			err = releaseErr
		}
	}()

	if err = handle.Acquire(ctx, ttl, false); err != nil {
		return err
	}

	return fn(ctx)
}

// RunWithLock is similar to DoWithLock but also sets up an auto-renewer.
func RunWithLock(ctx context.Context, client RaftLockClient, lockID, clientID string, ttl, interval time.Duration, fn func(context.Context) error) (err error) {
	handle, err := NewLockHandleFunc(client, lockID, clientID)
	if err != nil {
		return fmt.Errorf("failed to create lock handle: %w", err)
	}

	defer func() {
		closeErr := handle.Close(context.Background())
		if err == nil {
			err = closeErr
		}
	}()

	if err = handle.Acquire(ctx, ttl, true); err != nil {
		return err
	}

	renewer, err := NewAutoRenewerFunc(handle, interval, ttl)
	if err != nil {
		return fmt.Errorf("failed to create auto-renewer: %w", err)
	}

	renewerCtx, cancel := context.WithCancel(ctx)
	renewer.Start(renewerCtx)

	defer func() {
		stopErr := renewer.Stop(context.Background())

		cancel()
		if err == nil {
			err = stopErr
		}
	}()

	return fn(renewerCtx)
}
