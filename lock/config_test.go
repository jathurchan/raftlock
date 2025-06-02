package lock

import (
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
)

func assertDuration(t *testing.T, name string, got, want time.Duration) {
	t.Helper()
	if got != want {
		t.Errorf("Expected %s to be %v, got %v", name, want, got)
	}
}

func assertInt(t *testing.T, name string, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("Expected %s to be %d, got %d", name, want, got)
	}
}

func assertBool(t *testing.T, name string, got, want bool) {
	t.Helper()
	if got != want {
		t.Errorf("Expected %s to be %v, got %v", name, want, got)
	}
}

func TestLockConfig_DefaultLockManagerConfig(t *testing.T) {
	config := DefaultLockManagerConfig()

	assertDuration(t, "DefaultTTL", config.DefaultTTL, DefaultLockTTL)
	assertDuration(t, "MaxTTL", config.MaxTTL, MaxLockTTL)
	assertBool(t, "EnablePriorityQueue", config.EnablePriorityQueue, false)
	assertBool(t, "EnableCache", config.EnableCache, false)
	assertInt(t, "MaxWaiters", config.MaxWaiters, DefaultMaxWaiters)
}

func TestLockConfig_DurationOptions(t *testing.T) {
	tests := []struct {
		name         string
		option       func(time.Duration) LockManagerOption
		validValue   time.Duration
		invalidValue time.Duration
		defaultValue time.Duration
		getter       func(config *LockManagerConfig) time.Duration
		validator    func(time.Duration) bool
	}{
		{
			name:         "DefaultTTL",
			option:       WithDefaultTTL,
			validValue:   10 * time.Second,
			invalidValue: 100 * time.Millisecond,
			defaultValue: DefaultLockTTL,
			getter:       func(c *LockManagerConfig) time.Duration { return c.DefaultTTL },
			validator:    func(d time.Duration) bool { return d >= MinLockTTL && d <= MaxLockTTL },
		},
		{
			name:         "MaxTTL",
			option:       WithMaxTTL,
			validValue:   2 * time.Minute,
			invalidValue: 100 * time.Millisecond,
			defaultValue: MaxLockTTL,
			getter:       func(c *LockManagerConfig) time.Duration { return c.MaxTTL },
			validator:    func(d time.Duration) bool { return d >= MinLockTTL },
		},
		{
			name:         "TickInterval",
			option:       WithTickInterval,
			validValue:   500 * time.Millisecond,
			invalidValue: 0,
			defaultValue: DefaultTickInterval,
			getter:       func(c *LockManagerConfig) time.Duration { return c.TickInterval },
			validator:    func(d time.Duration) bool { return d > 0 },
		},
		{
			name:         "CacheTTL",
			option:       WithCacheTTL,
			validValue:   10 * time.Second,
			invalidValue: 0,
			defaultValue: DefaultCacheTTL,
			getter:       func(c *LockManagerConfig) time.Duration { return c.CacheTTL },
			validator:    func(d time.Duration) bool { return d > 0 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultLockManagerConfig()
			tt.option(tt.validValue)(&config)
			got := tt.getter(&config)
			if got != tt.validValue {
				t.Errorf("Expected %s to be %v, got %v", tt.name, tt.validValue, got)
			}

			config = DefaultLockManagerConfig()
			tt.option(tt.invalidValue)(&config)
			got = tt.getter(&config)
			if tt.validator(tt.invalidValue) {
				if got != tt.invalidValue {
					t.Errorf("Expected %s to be %v, got %v", tt.name, tt.invalidValue, got)
				}
			} else {
				if got != tt.defaultValue {
					t.Errorf("Expected %s to remain %v, got %v", tt.name, tt.defaultValue, got)
				}
			}
		})
	}
}

func TestLockConfig_IntegerOptions(t *testing.T) {
	tests := []struct {
		name         string
		option       func(int) LockManagerOption
		validValue   int
		invalidValue int
		defaultValue int
		getter       func(config *LockManagerConfig) int
	}{
		{
			name:         "MaxWaiters",
			option:       WithMaxWaiters,
			validValue:   50,
			invalidValue: 0,
			defaultValue: DefaultMaxWaiters,
			getter:       func(c *LockManagerConfig) int { return c.MaxWaiters },
		},
		{
			name:         "MaxLocks",
			option:       WithMaxLocks,
			validValue:   5000,
			invalidValue: 0,
			defaultValue: DefaultMaxLocks,
			getter:       func(c *LockManagerConfig) int { return c.MaxLocks },
		},
		{
			name:         "CacheSize",
			option:       WithCacheSize,
			validValue:   2000,
			invalidValue: 0,
			defaultValue: DefaultCacheSize,
			getter:       func(c *LockManagerConfig) int { return c.CacheSize },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultLockManagerConfig()
			tt.option(tt.validValue)(&config)
			got := tt.getter(&config)
			if got != tt.validValue {
				t.Errorf("Expected %s to be %d, got %d", tt.name, tt.validValue, got)
			}

			config = DefaultLockManagerConfig()
			tt.option(tt.invalidValue)(&config)
			got = tt.getter(&config)
			if tt.invalidValue > 0 {
				if got != tt.invalidValue {
					t.Errorf("Expected %s to be %d, got %d", tt.name, tt.invalidValue, got)
				}
			} else {
				if got != tt.defaultValue {
					t.Errorf("Expected %s to remain %d, got %d", tt.name, tt.defaultValue, got)
				}
			}
		})
	}
}

func TestLockConfig_BooleanOptions(t *testing.T) {
	tests := []struct {
		name   string
		option func(bool) LockManagerOption
		getter func(config *LockManagerConfig) bool
	}{
		{
			name:   "PriorityQueue",
			option: WithPriorityQueue,
			getter: func(c *LockManagerConfig) bool { return c.EnablePriorityQueue },
		},
		{
			name:   "Cache",
			option: WithCache,
			getter: func(c *LockManagerConfig) bool { return c.EnableCache },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultLockManagerConfig()
			tt.option(true)(&config)
			if !tt.getter(&config) {
				t.Errorf("Expected %s to be true", tt.name)
			}

			config = DefaultLockManagerConfig()
			tt.option(false)(&config)
			if tt.getter(&config) {
				t.Errorf("Expected %s to be false", tt.name)
			}
		})
	}
}

func TestWithDependencies(t *testing.T) {
	serializer := &JSONSerializer{}
	clock := raft.NewStandardClock()
	logger := logger.NewNoOpLogger()
	metrics := &NoOpMetrics{}

	config := DefaultLockManagerConfig()

	WithSerializer(serializer)(&config)
	if config.Serializer != serializer {
		t.Errorf("Expected Serializer to be set correctly")
	}

	WithClock(clock)(&config)
	if config.Clock != clock {
		t.Errorf("Expected Clock to be set correctly")
	}

	WithLogger(logger)(&config)
	if config.Logger != logger {
		t.Errorf("Expected Logger to be set correctly")
	}

	WithMetrics(metrics)(&config)
	if config.Metrics != metrics {
		t.Errorf("Expected Metrics to be set correctly")
	}

	originalSerializer := config.Serializer
	WithSerializer(nil)(&config)
	if config.Serializer != originalSerializer {
		t.Errorf("Expected Serializer to remain unchanged when set to nil")
	}
}
