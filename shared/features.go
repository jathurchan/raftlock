package shared

// FeatureFlags configures optional features and optimizations
// for RaftLock client and server behavior.
type FeatureFlags struct {
	Performance   PerformanceFlags
	Storage       StorageFlags
	Features      CoreFeatureFlags
	Observability ObservabilityFlags
}

// PerformanceFlags groups performance-related optimizations.
type PerformanceFlags struct {
	// EnableLeaderLeaseFastPath allows the leader to grant uncontested locks
	// without a full Raft round trip if it holds a valid lease.
	EnableLeaderLeaseFastPath bool

	// EnableReadOptimizedPaths enables fast, concurrent execution paths
	// for read-only and shared-lock operations.
	EnableReadOptimizedPaths bool

	// EnableAdaptiveBackoff lets clients adjust retry strategies
	// based on server-reported contention metrics.
	EnableAdaptiveBackoff bool
}

// StorageFlags groups storage-related optimizations.
type StorageFlags struct {
	// EnableBinaryLogFormat uses a compact binary format instead of JSON
	// for internal log entries to reduce storage and I/O.
	EnableBinaryLogFormat bool

	// EnableIndexMapping maintains an in-memory map of log index to offset
	// to speed up log lookups.
	EnableIndexMapping bool
}

// CoreFeatureFlags groups core functional features.
type CoreFeatureFlags struct {
	// EnableFencingTokens issues incrementing version numbers for lock operations
	// to prevent split-brain behavior caused by stale clients.
	EnableFencingTokens bool

	// EnableWaitQueues allows clients to wait for locks rather than failing immediately
	// when a lock is held by another client.
	EnableWaitQueues bool
}

// ObservabilityFlags groups observability-related features.
type ObservabilityFlags struct {
	// EnableMetrics records metrics for lock management,
	// consensus, and storage performance.
	EnableMetrics bool
}

// DefaultClientFeatureFlags returns the default feature flag configuration
// optimized for clients.
func DefaultClientFeatureFlags() *FeatureFlags {
	return &FeatureFlags{
		Performance: PerformanceFlags{
			EnableAdaptiveBackoff: true,
		},
		Features: CoreFeatureFlags{
			EnableFencingTokens: true,
			EnableWaitQueues:    true,
		},
		Observability: ObservabilityFlags{
			EnableMetrics: true,
		},
	}
}

// DefaultServerFeatureFlags returns the default feature flag configuration
// optimized for servers.
func DefaultServerFeatureFlags() *FeatureFlags {
	return &FeatureFlags{
		Performance: PerformanceFlags{
			EnableLeaderLeaseFastPath: true,
			EnableReadOptimizedPaths:  true,
			EnableAdaptiveBackoff:     true,
		},
		Storage: StorageFlags{
			EnableBinaryLogFormat: true,
			EnableIndexMapping:    true,
		},
		Features: CoreFeatureFlags{
			EnableFencingTokens: true,
			EnableWaitQueues:    true,
		},
		Observability: ObservabilityFlags{
			EnableMetrics: true,
		},
	}
}

// FeatureFlagOption defines a functional option for customizing feature flags.
type FeatureFlagOption func(*FeatureFlags)

// WithLeaderLeaseFastPath sets the leader lease optimization flag.
func WithLeaderLeaseFastPath(enabled bool) FeatureFlagOption {
	return func(flags *FeatureFlags) {
		flags.Performance.EnableLeaderLeaseFastPath = enabled
	}
}

// WithAdaptiveBackoff sets the adaptive backoff flag.
func WithAdaptiveBackoff(enabled bool) FeatureFlagOption {
	return func(flags *FeatureFlags) {
		flags.Performance.EnableAdaptiveBackoff = enabled
	}
}

// WithMetrics sets the metrics flag.
func WithMetrics(enabled bool) FeatureFlagOption {
	return func(flags *FeatureFlags) {
		flags.Observability.EnableMetrics = enabled
	}
}

// NewFeatureFlags returns a copy of the base flags with the given options applied.
func NewFeatureFlags(base *FeatureFlags, options ...FeatureFlagOption) *FeatureFlags {
	flags := *base
	for _, option := range options {
		option(&flags)
	}
	return &flags
}

// IsOptimized returns true if the primary performance and storage optimizations are enabled.
func (f *FeatureFlags) IsOptimized() bool {
	return f.Performance.EnableLeaderLeaseFastPath &&
		f.Performance.EnableReadOptimizedPaths &&
		f.Storage.EnableBinaryLogFormat &&
		f.Storage.EnableIndexMapping
}
