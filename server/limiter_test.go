package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
)

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		(err.Error() != "" &&
			(containsAny(err.Error(), []string{"deadline", "timeout", "exceed"})))
}

func containsAny(s string, substrings []string) bool {
	for _, substr := range substrings {
		if len(s) >= len(substr) {
			for i := 0; i <= len(s)-len(substr); i++ {
				match := true
				for j := range len(substr) {
					if s[i+j] != substr[j] && s[i+j] != substr[j]-32 && s[i+j] != substr[j]+32 {
						match = false
						break
					}
				}
				if match {
					return true
				}
			}
		}
	}
	return false
}

func TestNewTokenBucketRateLimiter(t *testing.T) {
	tests := []struct {
		name        string
		maxRequests int
		burst       int
		window      time.Duration
		expectInf   bool
	}{
		{
			name:        "valid rate limiter",
			maxRequests: 100,
			burst:       10,
			window:      time.Second,
			expectInf:   false,
		},
		{
			name:        "zero window should disable rate limiting",
			maxRequests: 100,
			burst:       10,
			window:      0,
			expectInf:   true,
		},
		{
			name:        "negative window should disable rate limiting",
			maxRequests: 100,
			burst:       10,
			window:      -time.Second,
			expectInf:   true,
		},
		{
			name:        "zero burst should be set to 1",
			maxRequests: 100,
			burst:       0,
			window:      time.Second,
			expectInf:   false,
		},
		{
			name:        "negative burst should be set to 1",
			maxRequests: 100,
			burst:       -5,
			window:      time.Second,
			expectInf:   false,
		},
		{
			name:        "zero burst with zero window",
			maxRequests: 100,
			burst:       0,
			window:      0,
			expectInf:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logger.NewNoOpLogger()
			rl := NewTokenBucketRateLimiter(tt.maxRequests, tt.burst, tt.window, logger)

			if rl == nil {
				t.Fatal("NewTokenBucketRateLimiter returned nil")
			}

			if rl.limiter == nil {
				t.Fatal("Internal limiter is nil")
			}

			if rl.logger == nil {
				t.Fatal("Logger is nil")
			}

			if tt.expectInf {
				// With infinite rate, all requests should be allowed
				for i := range 1000 {
					if !rl.Allow() {
						t.Errorf("Expected infinite rate limiter to allow all requests, but request %d was denied", i)
						break
					}
				}
			}
		})
	}
}

func TestTokenBucketRateLimiter_Allow(t *testing.T) {
	tests := []struct {
		name         string
		maxRequests  int
		burst        int
		window       time.Duration
		requests     int
		expectDenied bool
	}{
		{
			name:         "burst allows initial requests",
			maxRequests:  1,
			burst:        5,
			window:       time.Second,
			requests:     5,
			expectDenied: false,
		},
		{
			name:         "requests beyond burst are denied",
			maxRequests:  1,
			burst:        3,
			window:       time.Second,
			requests:     5,
			expectDenied: true,
		},
		{
			name:         "single request within burst",
			maxRequests:  10,
			burst:        1,
			window:       time.Second,
			requests:     1,
			expectDenied: false,
		},
		{
			name:         "infinite rate allows all",
			maxRequests:  100,
			burst:        10,
			window:       0, // This makes rate infinite
			requests:     100,
			expectDenied: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logger.NewNoOpLogger()
			rl := NewTokenBucketRateLimiter(tt.maxRequests, tt.burst, tt.window, logger)

			var denied bool
			for range tt.requests {
				if !rl.Allow() {
					denied = true
					break
				}
			}

			if denied != tt.expectDenied {
				t.Errorf("Expected denied=%v, got denied=%v after %d requests", tt.expectDenied, denied, tt.requests)
			}
		})
	}
}

func TestTokenBucketRateLimiter_AllowWithRecovery(t *testing.T) {
	logger := logger.NewNoOpLogger()
	rl := NewTokenBucketRateLimiter(10, 2, time.Second, logger) // 10 RPS, burst of 2

	// Exhaust the burst
	if !rl.Allow() {
		t.Fatal("First request should be allowed")
	}
	if !rl.Allow() {
		t.Fatal("Second request should be allowed")
	}
	if rl.Allow() {
		t.Fatal("Third request should be denied (burst exhausted)")
	}

	// Wait a bit for the rate limiter to recover
	time.Sleep(150 * time.Millisecond) // Should allow at least 1 more request

	if !rl.Allow() {
		t.Error("Request should be allowed after recovery period")
	}
}

func TestTokenBucketRateLimiter_Wait(t *testing.T) {
	tests := []struct {
		name            string
		maxRequests     int
		burst           int
		window          time.Duration
		initialRequests int
		waitTimeout     time.Duration
		expectTimeout   bool
	}{
		{
			name:            "wait succeeds when tokens available",
			maxRequests:     10,
			burst:           2,
			window:          time.Second,
			initialRequests: 1, // Use 1 token, 1 remaining
			waitTimeout:     100 * time.Millisecond,
			expectTimeout:   false,
		},
		{
			name:            "wait succeeds after token recovery",
			maxRequests:     10,
			burst:           1,
			window:          time.Second,
			initialRequests: 1,                      // Exhaust burst
			waitTimeout:     200 * time.Millisecond, // Should be enough for 1 token recovery
			expectTimeout:   false,
		},
		{
			name:            "wait times out when tokens not available quickly enough",
			maxRequests:     1, // Very slow recovery
			burst:           1,
			window:          time.Second,
			initialRequests: 1,                     // Exhaust burst
			waitTimeout:     50 * time.Millisecond, // Too short for recovery
			expectTimeout:   true,
		},
		{
			name:            "infinite rate never blocks",
			maxRequests:     100,
			burst:           1,
			window:          0, // Infinite rate
			initialRequests: 100,
			waitTimeout:     10 * time.Millisecond,
			expectTimeout:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logger.NewNoOpLogger()
			rl := NewTokenBucketRateLimiter(tt.maxRequests, tt.burst, tt.window, logger)

			// Consume initial requests
			for range tt.initialRequests {
				rl.Allow()
			}

			// Test Wait with timeout
			ctx, cancel := context.WithTimeout(context.Background(), tt.waitTimeout)
			defer cancel()

			err := rl.Wait(ctx)

			if tt.expectTimeout {
				if err == nil {
					t.Error("Expected Wait to timeout, but it succeeded")
				} else if !isTimeoutError(err) {
					t.Errorf("Expected timeout error, got %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected Wait to succeed, but got error: %v", err)
				}
			}
		})
	}
}

func TestTokenBucketRateLimiter_WaitCancellation(t *testing.T) {
	logger := logger.NewNoOpLogger()
	rl := NewTokenBucketRateLimiter(1, 1, time.Second, logger) // Very slow rate

	// Exhaust the burst
	if !rl.Allow() {
		t.Fatal("First request should be allowed")
	}

	// Create a context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Start Wait in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- rl.Wait(ctx)
	}()

	// Cancel the context after a short delay
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Wait should return with cancellation error
	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Wait did not return after context cancellation")
	}
}

func TestTokenBucketRateLimiter_ConcurrentAccess(t *testing.T) {
	logger := logger.NewNoOpLogger()
	rl := NewTokenBucketRateLimiter(100, 10, time.Second, logger)

	const numGoroutines = 50
	const requestsPerGoroutine = 10

	allowedCh := make(chan bool, numGoroutines*requestsPerGoroutine)

	for range numGoroutines {
		go func() {
			for range requestsPerGoroutine {
				allowedCh <- rl.Allow()
			}
		}()
	}

	allowed := 0
	denied := 0
	for range numGoroutines * requestsPerGoroutine {
		if <-allowedCh {
			allowed++
		} else {
			denied++
		}
	}

	// With a burst of 10, we should have at least 10 allowed requests
	if allowed < 10 {
		t.Errorf("Expected at least 10 allowed requests, got %d", allowed)
	}

	// Total should match
	if allowed+denied != numGoroutines*requestsPerGoroutine {
		t.Errorf("Total requests mismatch: allowed=%d, denied=%d, expected total=%d",
			allowed, denied, numGoroutines*requestsPerGoroutine)
	}
}

func TestTokenBucketRateLimiter_ConcurrentWait(t *testing.T) {
	logger := logger.NewNoOpLogger()
	rl := NewTokenBucketRateLimiter(100, 2, time.Second, logger) // 100 RPS, burst of 2

	const numGoroutines = 5
	errCh := make(chan error, numGoroutines)

	for range numGoroutines {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			errCh <- rl.Wait(ctx)
		}()
	}

	successes := 0
	timeouts := 0
	for i := 0; i < numGoroutines; i++ {
		err := <-errCh
		if err == nil {
			successes++
		} else if err == context.DeadlineExceeded {
			timeouts++
		} else {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	// We should have some successes (at least from the burst)
	if successes == 0 {
		t.Error("Expected at least some successful Wait operations")
	}

	// Total should match
	if successes+timeouts != numGoroutines {
		t.Errorf("Total operations mismatch: successes=%d, timeouts=%d, expected total=%d",
			successes, timeouts, numGoroutines)
	}
}

func TestTokenBucketRateLimiter_EdgeCases(t *testing.T) {
	t.Run("zero max requests", func(t *testing.T) {
		logger := logger.NewNoOpLogger()
		rl := NewTokenBucketRateLimiter(0, 5, time.Second, logger)

		// With 0 RPS, only burst should be available
		for i := range 5 {
			if !rl.Allow() {
				t.Errorf("Request %d should be allowed from burst", i)
			}
		}

		// Next request should be denied
		if rl.Allow() {
			t.Error("Request beyond burst should be denied with 0 RPS")
		}
	})

	t.Run("very large numbers", func(t *testing.T) {
		logger := logger.NewNoOpLogger()
		rl := NewTokenBucketRateLimiter(1000000, 1000000, time.Second, logger)

		// Should be able to make many requests immediately
		for i := range 1000 {
			if !rl.Allow() {
				t.Errorf("Request %d should be allowed with high limits", i)
			}
		}
	})

	t.Run("very small window", func(t *testing.T) {
		logger := logger.NewNoOpLogger()
		rl := NewTokenBucketRateLimiter(1000, 1, time.Nanosecond, logger)

		// Should allow at least the burst
		if !rl.Allow() {
			t.Error("First request should be allowed from burst")
		}
	})
}
