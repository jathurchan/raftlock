package lock

import (
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
)

// LockManagerOption defines a function that applies a configuration setting
// to a LockManager during initialization.
type LockManagerOption func(*LockManagerConfig)

// LockManagerConfig holds configuration parameters for a LockManager instance.
type LockManagerConfig struct {
	// DefaultTTL is the fallback duration used when a lock is acquired
	// without an explicit TTL.
	DefaultTTL time.Duration

	// MaxTTL defines the upper bound for any lock’s TTL.
	// Lock acquisitions or renewals exceeding this value may be rejected or capped.
	MaxTTL time.Duration

	// TickInterval specifies how often the LockManager’s Tick method should be
	// invoked externally to handle time-based events like expirations and promotions.
	TickInterval time.Duration

	// MaxWaiters limits the number of clients allowed in a single lock’s wait queue.
	MaxWaiters int

	// EnablePriorityQueue controls whether wait queues use client-defined priorities
	// instead of simple FIFO ordering.
	EnablePriorityQueue bool

	Serializer Serializer
	Clock      raft.Clock
	Logger     logger.Logger
	Metrics    Metrics

	// EnableCache enables read caching for operations like GetLockInfo.
	// This may improve performance but can return slightly stale data.
	EnableCache bool

	// CacheTTL defines how long lock state remains valid in the cache,
	// if caching is enabled.
	CacheTTL time.Duration

	// MaxLocks limits the total number of locks that can be managed per node.
	MaxLocks int

	// CacheSize sets the maximum number of locks in the read cache.
	CacheSize int
}

// DefaultLockManagerConfig returns a LockManagerConfig with sensible defaults
// based on the predefined constants.
func DefaultLockManagerConfig() LockManagerConfig {
	return LockManagerConfig{
		DefaultTTL:          DefaultLockTTL,
		MaxTTL:              MaxLockTTL,
		TickInterval:        DefaultTickInterval,
		MaxWaiters:          DefaultMaxWaiters,
		EnablePriorityQueue: false,
		EnableCache:         false,
		CacheTTL:            DefaultCacheTTL,
		MaxLocks:            DefaultMaxLocks,
		CacheSize:           DefaultCacheSize,
	}
}

// WithDefaultTTL sets the default TTL for locks that don't specify one.
// The TTL must be between MinLockTTL and MaxLockTTL.
func WithDefaultTTL(ttl time.Duration) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if ttl >= MinLockTTL && ttl <= MaxLockTTL {
			cfg.DefaultTTL = ttl
		}
	}
}

// WithMaxTTL sets the maximum allowed TTL for any lock.
// The TTL must be at least MinLockTTL.
func WithMaxTTL(ttl time.Duration) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if ttl >= MinLockTTL {
			cfg.MaxTTL = ttl
		}
	}
}

// WithTickInterval sets how frequently the Tick method should be called.
func WithTickInterval(interval time.Duration) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if interval > 0 {
			cfg.TickInterval = interval
		}
	}
}

// WithMaxWaiters sets the maximum number of clients allowed in a lock's wait queue.
func WithMaxWaiters(max int) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if max > 0 {
			cfg.MaxWaiters = max
		}
	}
}

// WithPriorityQueue enables priority-based ordering in wait queues.
func WithPriorityQueue(enable bool) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		cfg.EnablePriorityQueue = enable
	}
}

// WithSerializer sets the serializer for decoding lock commands.
func WithSerializer(serializer Serializer) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if serializer != nil {
			cfg.Serializer = serializer
		}
	}
}

// WithClock sets the clock used for time-related operations in the lock manager.
func WithClock(clock raft.Clock) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if clock != nil {
			cfg.Clock = clock
		}
	}
}

// WithLogger sets the logger for internal events.
func WithLogger(logger logger.Logger) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if logger != nil {
			cfg.Logger = logger
		}
	}
}

// WithMetrics sets the metrics collector for operational data.
func WithMetrics(metrics Metrics) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if metrics != nil {
			cfg.Metrics = metrics
		}
	}
}

// WithCache enables or disables the read cache.
func WithCache(enable bool) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		cfg.EnableCache = enable
	}
}

// WithCacheTTL sets how long cached lock information remains valid.
func WithCacheTTL(ttl time.Duration) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if ttl > 0 {
			cfg.CacheTTL = ttl
		}
	}
}

// WithMaxLocks sets the maximum number of locks that can be managed per node.
func WithMaxLocks(max int) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if max > 0 {
			cfg.MaxLocks = max
		}
	}
}

// WithCacheSize sets the maximum number of locks in the read cache.
func WithCacheSize(size int) LockManagerOption {
	return func(cfg *LockManagerConfig) {
		if size > 0 {
			cfg.CacheSize = size
		}
	}
}
