package storage

// StorageFeatureFlags allows enabling/disabling specific optimizations
type StorageFeatureFlags struct {
	// EnableBinaryFormat uses efficient binary serialization instead of JSON
	EnableBinaryFormat bool

	// EnableIndexMap maintains an in-memory index for O(log n) lookups
	EnableIndexMap bool

	// EnableAsyncTruncation performs log truncation asynchronously
	EnableAsyncTruncation bool

	// EnableChunkedIO uses chunked I/O for large operations
	EnableChunkedIO bool

	// EnableAtomicWrites uses write-to-temp-then-rename for atomicity
	EnableAtomicWrites bool

	// EnableLockTimeout adds timeouts to lock acquisition to prevent deadlocks
	EnableLockTimeout bool

	// EnableMetrics tracks performance metrics
	EnableMetrics bool
}

// DefaultFeatureFlags returns the optimal feature configuration
func DefaultFeatureFlags() StorageFeatureFlags {
	return StorageFeatureFlags{
		EnableBinaryFormat:    true,
		EnableIndexMap:        true,
		EnableAsyncTruncation: true,
		EnableChunkedIO:       true,
		EnableAtomicWrites:    true,
		EnableLockTimeout:     true,
		EnableMetrics:         true,
	}
}

// FileStorageOptions provides configuration options
type FileStorageOptions struct {
	// Feature flags for enabling/disabling optimizations
	Features StorageFeatureFlags

	// AutoTruncateOnSnapshot controls log truncation after snapshots
	AutoTruncateOnSnapshot bool

	// SyncOnAppend controls whether to force fsync after each append
	SyncOnAppend bool

	// RecoveryMode controls how aggressively to recover from failures
	RecoveryMode recoveryMode

	// RetainedLogSize is the minimum entries to keep after truncation
	RetainedLogSize uint64

	// ChunkSize for chunked I/O operations (bytes)
	ChunkSize int

	// LockTimeout for lock acquisition in seconds
	LockTimeout int
}

// DefaultFileStorageOptions returns optimal configuration values
func DefaultFileStorageOptions() FileStorageOptions {
	return FileStorageOptions{
		Features:               DefaultFeatureFlags(),
		AutoTruncateOnSnapshot: true,
		SyncOnAppend:           true,
		RecoveryMode:           normalMode,
		RetainedLogSize:        defaultRetainedLogSize,
		ChunkSize:              defaultChunkSizeBytes,
		LockTimeout:            defaultLockTimeoutSeconds,
	}
}
