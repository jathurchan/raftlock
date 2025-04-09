package storage

import (
	"testing"

	"github.com/jathurchan/raftlock/testutil"
)

func TestDefaultFeatureFlags(t *testing.T) {
	flags := DefaultFeatureFlags()

	expected := StorageFeatureFlags{
		EnableBinaryFormat:    true,
		EnableIndexMap:        true,
		EnableAsyncTruncation: true,
		EnableChunkedIO:       true,
		EnableAtomicWrites:    true,
		EnableLockTimeout:     true,
		EnableMetrics:         true,
	}

	testutil.AssertEqual(t, expected, flags, "DefaultFeatureFlags() returned unexpected values")
}

func TestDefaultFileStorageOptions(t *testing.T) {
	options := DefaultFileStorageOptions()

	expected := FileStorageOptions{
		Features:               DefaultFeatureFlags(),
		AutoTruncateOnSnapshot: true,
		SyncOnAppend:           true,
		RecoveryMode:           normalMode,
		RetainedLogSize:        defaultRetainedLogSize,
		ChunkSize:              defaultChunkSizeBytes,
		LockTimeout:            defaultLockTimeoutSeconds,
	}

	testutil.AssertEqual(t, expected, options, "DefaultFileStorageOptions() returned unexpected values")
}
