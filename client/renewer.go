package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jathurchan/raftlock/raft"
)

// AutoRenewer defines a background mechanism for automatically renewing a lock.
// It handles lifecycle management, graceful shutdown, and error reporting.
type AutoRenewer interface {
	// Start begins the auto-renewal process in a background goroutine.
	// The provided context is used to control the lifecycle of the renewal process.
	Start(ctx context.Context)

	// Stop gracefully stops the auto-renewal loop and waits for it to exit.
	// Returns any terminal error from the renewal process or shutdown.
	Stop(ctx context.Context) error

	// Done returns a channel that's closed when the auto-renewer has stopped.
	Done() <-chan struct{}

	// Err returns the error that caused the renewer to stop, if any.
	// Returns nil if stopped gracefully.
	Err() error
}

// autoRenewer implements AutoRenewer.
// It runs a background loop that periodically renews a lock handle.
// The loop is cancellable via context and reports any terminal failure.
type autoRenewer struct {
	handle   LockHandle    // LockHandle to renew periodically.
	interval time.Duration // Frequency of renewal attempts.
	ttl      time.Duration // TTL to request on each renewal.

	clock raft.Clock // Clock used for scheduling (mockable for tests).

	mu     sync.RWMutex
	ctx    context.Context    // Context controlling the renewal loop.
	cancel context.CancelFunc // Cancels the renewal loop.
	wg     sync.WaitGroup     // Waits for the renewal goroutine to exit in Stop.

	err error // Terminal error that stopped the renewer, if any.
}

// AutoRenewerOptions holds optional configuration for an AutoRenewer.
type AutoRenewerOptions struct {
	Clock raft.Clock
}

// AutoRenewerOption is a function that applies a configuration option to an AutoRenewer.
type AutoRenewerOption func(*AutoRenewerOptions)

// WithClock provides a custom clock implementation to the AutoRenewer,
// which is primarily useful for testing time-dependent behavior.
func WithClock(clock raft.Clock) AutoRenewerOption {
	return func(opts *AutoRenewerOptions) {
		opts.Clock = clock
	}
}

// NewAutoRenewer creates a new AutoRenewer for the given lock handle.
// It attempts to renew the lock at the specified interval using the given TTL.
// Optional configurations (e.g., custom clock for testing) can be provided via opts.
func NewAutoRenewer(handle LockHandle, interval, ttl time.Duration, opts ...AutoRenewerOption) (AutoRenewer, error) {
	if handle == nil {
		return nil, errors.New("lock handle cannot be nil")
	}
	if interval <= 0 {
		return nil, errors.New("renewal interval must be positive")
	}
	if ttl <= interval {
		return nil, fmt.Errorf("lock TTL (%v) must be greater than renewal interval (%v)", ttl, interval)
	}

	options := &AutoRenewerOptions{}
	for _, opt := range opts {
		opt(options)
	}

	clock := options.Clock
	if clock == nil {
		clock = raft.NewStandardClock()
	}

	return &autoRenewer{
		handle:   handle,
		interval: interval,
		ttl:      ttl,
		clock:    clock,
	}, nil
}

// Start begins the auto-renewal process in a background goroutine.
func (r *autoRenewer) Start(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cancel != nil {
		return
	}
	r.ctx, r.cancel = context.WithCancel(ctx)

	r.wg.Add(1)
	go r.run()
}

// Stop gracefully stops the auto-renewal process.
func (r *autoRenewer) Stop(ctx context.Context) error {
	r.mu.RLock()
	cancel := r.cancel
	r.mu.RUnlock()

	if cancel == nil {
		return nil // Not started
	}

	cancel()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return r.Err()
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for auto-renewer to stop: %w", ctx.Err())
	}
}

// Done returns a channel that is closed when the renewer stops.
func (r *autoRenewer) Done() <-chan struct{} {
	r.mu.RLock()
	ctx := r.ctx
	r.mu.RUnlock()

	if ctx == nil {
		// Return a closed channel if Start hasn't been called.
		closedCh := make(chan struct{})
		close(closedCh)
		return closedCh
	}
	return r.ctx.Done()
}

// Err returns the error that caused the renewer to stop.
func (r *autoRenewer) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.err
}

// setError sets the error field in a thread-safe way.
func (r *autoRenewer) setError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// run is the main renewal loop.
func (r *autoRenewer) run() {
	defer r.wg.Done()
	defer r.cancel() // Ensure context is canceled on exit

	ticker := r.clock.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			if !r.handle.IsHeld() {
				r.setError(fmt.Errorf("auto-renewal stopped: lock not held"))
				return
			}

			if err := r.handle.Renew(r.ctx, r.ttl); err != nil {
				r.setError(fmt.Errorf("auto-renewal failed: %w", err))
				return
			}

		case <-r.ctx.Done():
			r.setError(r.ctx.Err())
			return
		}
	}
}
