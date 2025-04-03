package storage

import "errors"

var (
	// ErrStorageIO is returned when a low-level I/O failure occurs during a storage operation.
	ErrStorageIO = errors.New("raft: storage I/O error")

	// ErrUninitializedStorage is returned if the storage is used before it has been properly initialized.
	ErrUninitializedStorage = errors.New("raft: storage not initialized")

	// ErrEntryNotFound is returned when a requested log entry is missing or has been compacted.
	ErrEntryNotFound = errors.New("raft: log entry not found")

	// ErrIndexOutOfRange is returned when a requested log index range is invalid or inaccessible.
	ErrIndexOutOfRange = errors.New("raft: log index out of range")

	// ErrOutOfOrderEntries is returned when log entries are not ordered by ascending index.
	ErrOutOfOrderEntries = errors.New("raft: entries must be in strictly ascending index order")

	// ErrNonContiguousEntries is returned when entries do not immediately follow the last log index.
	ErrNonContiguousEntries = errors.New("raft: entries are not contiguous with the existing log")

	// ErrEmptyEntries is returned when AppendLogEntries is called with an empty slice.
	ErrEmptyEntries = errors.New("raft: no log entries provided")

	// ErrEntryConflict is returned when an entry conflicts with an existing index-term pair.
	ErrEntryConflict = errors.New("raft: log entry conflict at given index")

	// ErrCorruptedState is returned when persisted term or vote metadata is malformed or unreadable.
	ErrCorruptedState = errors.New("raft: corrupted persistent state")

	// ErrNoSnapshot is returned when a snapshot is requested but none is available.
	ErrNoSnapshot = errors.New("raft: no snapshot available")

	// ErrCorruptedSnapshot is returned when a snapshot is unreadable or structurally invalid.
	ErrCorruptedSnapshot = errors.New("raft: corrupted snapshot data")

	// ErrSnapshotOutOfDate is returned when a snapshot is older than the node's current applied state.
	ErrSnapshotOutOfDate = errors.New("raft: snapshot is stale and will not be applied")

	// ErrSnapshotMismatch is returned when snapshot metadata does not align with the expected log structure.
	ErrSnapshotMismatch = errors.New("raft: snapshot metadata does not match log history")

	// ErrInvalidLogRange is returned when a requested range has start >= end.
	ErrInvalidLogRange = errors.New("raft: invalid log range (start index must be less than end)")

	// ErrInvalidOffset is returned when an invalid file offset is encountered.
	ErrInvalidOffset = errors.New("raft: invalid file offset")

	// ErrCorruptedLog is returned when a log entry is structurally invalid or cannot be decoded.
	ErrCorruptedLog = errors.New("raft: corrupted log entry")
)
