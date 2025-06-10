package client

import (
	"context"
	"fmt"
	"time"
)

// DoWithLock executes a function while holding a lock, automatically releasing it afterwards.
// The provided context is passed to the executed function.
func DoWithLock(ctx context.Context, client RaftLockClient, lockID, clientID string, ttl time.Duration, fn func(ctx context.Context) error) (err error) {
	handle, err := NewLockHandle(client, lockID, clientID)
	if err != nil {
		return fmt.Errorf("failed to create lock handle: %w", err)
	}
	defer func() {
		releaseErr := handle.Close(context.Background()) // Use background context for cleanup
		if err == nil {
			err = releaseErr
		}
	}()

	if err = handle.Acquire(ctx, ttl, false); err != nil {
		return err
	}

	return fn(ctx)
}

// RunWithLock executes a function while holding a lock with automatic renewal.
// The provided context is passed to the executed function and also controls the auto-renewer's lifecycle.
func RunWithLock(ctx context.Context, client RaftLockClient, lockID, clientID string, ttl, renewInterval time.Duration, fn func(ctx context.Context) error) (err error) {
	handle, err := NewLockHandle(client, lockID, clientID)
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

	renewer, err := NewAutoRenewer(handle, renewInterval, ttl)
	if err != nil {
		return fmt.Errorf("failed to create auto-renewer: %w", err)
	}

	renewCtx, cancelRenew := context.WithCancel(ctx)
	defer cancelRenew()

	renewer.Start(renewCtx)

	fnErr := fn(ctx)

	stopErr := renewer.Stop(context.Background())

	if fnErr != nil {
		return fnErr
	}
	return stopErr
}
