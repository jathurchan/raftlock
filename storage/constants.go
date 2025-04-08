package storage

const (
	// ownRWOthR represents file permission 0644 (owner read/write, others read).
	ownRWOthR = 0644

	// ownRWXOthRX represents directory permission 0755 (owner read/write/execute, others read/execute).
	ownRWXOthRX = 0755

	// defaultLockTimeoutSeconds is the default timeout for lock acquisition.
	defaultLockTimeoutSeconds = 5

	// defaultChunkSizeBytes is the default chunk size for large I/O operations (1MB).
	defaultChunkSizeBytes = 1024 * 1024

	// defaultRetainedLogSize is the default minimum number of log entries to keep after truncation.
	defaultRetainedLogSize = 100

	// defaultIndexMapInitialCapacity is the default initial capacity for the index-to-offset map.
	defaultIndexMapInitialCapacity = 1024

	// MaxEntrySize is the maximum allowed size for a log entry (64MB).
	maxEntrySizeBytes = 64 * 1024 * 1024

	// lengthPrefixSize is the size of length prefixes in bytes.
	lengthPrefixSize = 4

	// indexSize is the size of index field in binary format.
	indexSize = 8

	// termSize is the size of term field in binary format.
	termSize = 8

	// commandLengthSize is the size of command length field in binary format.
	commandLengthSize = 8

	// headerSize is the total size of binary entry header (index + term + data length).
	headerSize = indexSize + termSize + commandLengthSize

	// tmpSuffix is the suffix used for temporary files created during atomic write operations.
	tmpSuffix = ".tmp"

	// snapshotMetaFilename is the filename used to persist snapshot metadata (e.g., last included index and term).
	snapshotMetaFilename = "snapshot_meta.json"

	// snapshotDataFilename is the filename for the raw snapshot data representing a compacted state.
	snapshotDataFilename = "snapshot.dat"

	// snapshotMarkerFilename indicates an in-progress or recently committed snapshot operation.
	// Used for crash recovery to detect incomplete snapshots.
	snapshotMarkerFilename = "snapshot.marker"

	// recoveryMarkerFilename signals that the storage system is in an incomplete or recovering state.
	// Created during startup and removed after successful initialization.
	recoveryMarkerFilename = "recovery.marker"

	// metadataFilename stores log metadata such as the first and last persisted log index.
	metadataFilename = "metadata.json"

	// logFilename is the primary file for binary-encoded Raft log entries.
	logFilename = "log.dat"

	// stateFilename stores the persistent Raft state (current term and voted-for candidate).
	stateFilename = "state.json"

	// snapshotMarkerCommittedKey is a marker string written to the snapshot marker file
	// to indicate that the snapshot metadata has been successfully committed.
	snapshotMarkerCommittedKey = "meta_committed=true"
)
