package internal

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// RetryConfig defines the configuration for retry logic with exponential backoff.
type RetryConfig struct {
	MaxAttempts       int                // Maximum number of retry attempts
	BaseDelay         time.Duration      // Initial delay before retrying
	MaxDelay          time.Duration      // Maximum delay between retries
	Multiplier        float64            // Backoff multiplier per attempt
	Jitter            float64            // Jitter factor to randomize delays
	RetryableErrors   map[error]bool     // Optional map of explicitly retryable errors
	RetryableCheckers []func(error) bool // Optional list of custom retryability check functions
}

// DefaultRetryConfig returns a RetryConfig with standard retry settings.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    5 * time.Second,
		Multiplier:  2.0,
		Jitter:      0.1,
	}
}

// RetryFunc defines a function signature for operations that may be retried.
type RetryFunc func() error

// Retry runs the given function with exponential backoff retry logic.
// Retries are attempted according to the provided RetryConfig.
func Retry(ctx context.Context, config RetryConfig, fn RetryFunc) error {
	var lastErr error

	for attempt := range config.MaxAttempts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
			if !isRetryable(err, config) {
				return err
			}
		}

		if attempt < config.MaxAttempts-1 {
			delay := calculateBackoff(attempt, config)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return lastErr
}

// isRetryable determines whether the error is retryable based on config.
func isRetryable(err error, config RetryConfig) bool {
	if config.RetryableErrors != nil {
		if retryable, ok := config.RetryableErrors[err]; ok {
			return retryable
		}
	}
	for _, checker := range config.RetryableCheckers {
		if checker(err) {
			return true
		}
	}
	// Default to retryable if no match found
	return true
}

// calculateBackoff computes the retry delay for a given attempt using exponential backoff with jitter.
func calculateBackoff(attempt int, config RetryConfig) time.Duration {
	delay := min(float64(config.BaseDelay)*math.Pow(config.Multiplier, float64(attempt)), float64(config.MaxDelay))

	if config.Jitter > 0 {
		jitter := delay * config.Jitter * (rand.Float64()*2 - 1)
		delay += jitter
	}

	if delay < 0 {
		delay = float64(config.BaseDelay)
	}

	return time.Duration(delay)
}

// RetryWithResult is a generic retry wrapper that returns a result and error.
// It retries the function based on the provided RetryConfig.
func RetryWithResult[T any](ctx context.Context, config RetryConfig, fn func() (T, error)) (T, error) {
	var lastErr error
	var zeroValue T

	for attempt := range config.MaxAttempts {
		select {
		case <-ctx.Done():
			return zeroValue, ctx.Err()
		default:
		}

		if result, err := fn(); err == nil {
			return result, nil
		} else {
			lastErr = err
			if !isRetryable(err, config) {
				return zeroValue, err
			}
		}

		if attempt < config.MaxAttempts-1 {
			delay := calculateBackoff(attempt, config)
			select {
			case <-ctx.Done():
				return zeroValue, ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return zeroValue, lastErr
}

// ExponentialBackoff calculates backoff duration based on attempt count using exponential growth.
// It caps the result at maxDelay.
func ExponentialBackoff(baseDelay, maxDelay time.Duration, multiplier float64, attempt int) time.Duration {
	delay := min(float64(baseDelay)*math.Pow(multiplier, float64(attempt)), float64(maxDelay))
	return time.Duration(delay)
}

// JitteredBackoff applies random jitter to exponential backoff to reduce load spikes.
// Jitter is applied symmetrically around the base delay.
func JitteredBackoff(baseDelay, maxDelay time.Duration, multiplier, jitter float64, attempt int) time.Duration {
	delay := ExponentialBackoff(baseDelay, maxDelay, multiplier, attempt)

	if jitter > 0 && jitter <= 1.0 {
		jitterAmount := float64(delay) * jitter * (rand.Float64()*2 - 1)
		delay = time.Duration(float64(delay) + jitterAmount)
		if delay < 0 {
			delay = baseDelay
		}
	}

	return delay
}
