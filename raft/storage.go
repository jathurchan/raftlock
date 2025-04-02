package raft

import "context"

// Storage defines the interface for persisting Raft state, log entries, and snapshots.
// Implementations must be thread-safe and support context-aware operations for proper cancellation and timeout handling.
type Storage interface {
	// SaveState atomically persists the current term and voted-for candidate.
	// Must be called after any update to persistent state.
	//
	// Returns:
	//   - ErrStorageIO if the operation fails due to I/O issues.
	//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
	SaveState(ctx context.Context, state PersistentState) error

	// LoadState retrieves the most recently persisted PersistentState.
	// Typically used during node startup or recovery.
	//
	// Returns:
	//   - ErrCorruptedState if the data is malformed or unreadable.
	//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
	LoadState(ctx context.Context) (PersistentState, error)

	// AppendLogEntries persists one or more log entries to the Raft log.
	// Entries must be non-empty, in strictly ascending index order, and contiguous with the existing log.
	//
	// Returns:
	//   - ErrEmptyEntries if the input is empty.
	//   - ErrOutOfOrderEntries if entries are not in ascending order.
	//   - ErrNonContiguousEntries if entries don't follow the last log index.
	//   - ErrStorageIO on I/O failure.
	//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
	AppendLogEntries(ctx context.Context, entries []LogEntry) error

	// GetLogEntries returns a slice of log entries within the range [start, end).
	//
	// Returns:
	//   - ErrInvalidLogRange if start >= end.
	//   - ErrIndexOutOfRange if the requested range includes missing or compacted entries.
	//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
	GetLogEntries(ctx context.Context, start, end Index) ([]LogEntry, error)

	// GetLogEntry returns the log entry at the given index.
	//
	// Returns:
	//   - ErrEntryNotFound if the entry is missing or compacted.
	//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
	GetLogEntry(ctx context.Context, index Index) (LogEntry, error)

	// TruncateLogSuffix deletes all log entries with indices >= the given index.
	// Used to resolve log conflicts during replication.
	//
	// Returns:
	//   - ErrIndexOutOfRange if the index is beyond the last log index.
	//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
	TruncateLogSuffix(ctx context.Context, index Index) error

	// TruncateLogPrefix deletes all log entries with indices < the given index.
	// Used during log compaction after snapshotting.
	//
	// Returns:
	//   - ErrIndexOutOfRange if the index is less than the first log index.
	//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
	TruncateLogPrefix(ctx context.Context, index Index) error

	// SaveSnapshot persists a snapshot and its metadata atomically.
	// The snapshot should represent the full compacted state at a specific log index.
	//
	// Returns:
	//   - ErrStorageIO on failure to persist the snapshot.
	//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
	SaveSnapshot(ctx context.Context, metadata SnapshotMetadata, data []byte) error

	// LoadSnapshot retrieves the latest snapshot and its metadata.
	//
	// Returns:
	//   - ErrNoSnapshot if no snapshot has been saved.
	//   - ErrCorruptedSnapshot if the snapshot is unreadable or invalid.
	//	 - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
	LoadSnapshot(ctx context.Context) (SnapshotMetadata, []byte, error)

	// LastLogIndex returns the highest index currently stored in the log.
	// Returns 0 if the log is empty.
	LastLogIndex() Index

	// FirstLogIndex returns the lowest index currently available in the log.
	// Returns 0 if the log is empty.
	FirstLogIndex() Index

	// Close releases all underlying resources used by the storage implementation.
	//
	// Returns:
	//   - ErrStorageIO if cleanup fails.
	Close() error
}
