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
	// EnableLeaderLeaseFastPath allows a leader with a valid lease to grant uncontended locks
	// without performing a full consensus round, reducing latency.
	EnableLeaderLeaseFastPath bool

	// EnableReadOptimizedPaths activates fast read paths (e.g., using ReadIndex)
	// for lock status and other read-only operations, avoiding unnecessary log replication.
	EnableReadOptimizedPaths bool

	// EnableAdaptiveBackoff enables clients to dynamically adjust their retry intervals
	// based on contention metrics reported by the server.
	EnableAdaptiveBackoff bool
}

// StorageFlags groups storage-related optimizations.
type StorageFlags struct {
	// EnableBinaryLogFormat stores Raft log entries in a compact binary format
	// rather than JSON, reducing space and serialization overhead.
	EnableBinaryLogFormat bool

	// EnableIndexMapping maintains an in-memory index from log indices to file offsets,
	// improving random-access performance for log lookups.
	EnableIndexMapping bool
}

// CoreFeatureFlags groups core functional features.
type CoreFeatureFlags struct {
	// EnableFencingTokens assigns a monotonically increasing token to each lock acquisition,
	// preventing stale clients from issuing conflicting operations (i.e., protects against split-brain).
	EnableFencingTokens bool

	// EnableWaitQueues allows clients to wait asynchronously for a lock to become available
	// rather than immediately failing if it's already held.
	EnableWaitQueues bool
}

// ObservabilityFlags groups observability-related features.
type ObservabilityFlags struct {
	// EnableMetrics exposes performance, lock, and consensus metrics
	// for collection by observability systems.
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

// FeatureFlagOption is a functional option for modifying FeatureFlags at construction time.
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
