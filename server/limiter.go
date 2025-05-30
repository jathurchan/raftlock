package server

import (
	"context"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"golang.org/x/time/rate"
)

// RateLimiter defines the interface for request rate limiting.
type RateLimiter interface {
	Allow() bool
	Wait(ctx context.Context) error
}

// TokenBucketRateLimiter implements rate limiting using a token bucket algorithm.
type TokenBucketRateLimiter struct {
	limiter *rate.Limiter
	logger  logger.Logger
}

// NewTokenBucketRateLimiter creates a new token bucket rate limiter.
func NewTokenBucketRateLimiter(maxRequests, burst int, window time.Duration, logger logger.Logger) *TokenBucketRateLimiter {
	var rps rate.Limit
	if window.Seconds() > 0 {
		rps = rate.Limit(float64(maxRequests) / window.Seconds())
	} else {
		rps = rate.Inf
		logger.Warnw("Rate limit window is zero or negative, disabling rate limiter.", "window", window)
	}
	if burst <= 0 {
		burst = 1
		if rps != rate.Inf {
			logger.Warnw("Rate limit burst is zero or negative, setting to 1.", "burst", burst)
		}
	}

	return &TokenBucketRateLimiter{
		limiter: rate.NewLimiter(rps, burst),
		logger:  logger,
	}
}

// Allow returns true if a request can proceed immediately.
func (rl *TokenBucketRateLimiter) Allow() bool {
	return rl.limiter.Allow()
}

// Wait blocks until a request can proceed or the context is cancelled.
func (rl *TokenBucketRateLimiter) Wait(ctx context.Context) error {
	return rl.limiter.Wait(ctx)
}
