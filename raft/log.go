package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

// LogManager defines the interface for managing Raft log operations.
// It handles log entry persistence, retrieval, manipulation, and provides
// a consistent view of the log state.
type LogManager interface {
	// Initialize loads the log state from storage during startup.
	// Returns an error if loading fails or if state is inconsistent.
	Initialize(ctx context.Context) error

	// GetLastIndex returns the index of the last entry in the log.
	// Returns 0 if the log is empty.
	GetLastIndex() types.Index

	// GetLastTerm returns the term of the last entry in the log.
	// Returns 0 if the log is empty.
	GetLastTerm() types.Term

	// GetConsistentLastState returns the last index and term as an atomic pair.
	// Avoids the race condition that could happen when calling GetLastIndex and
	// GetLastTerm separately.
	GetConsistentLastState() (types.Index, types.Term)

	// GetFirstIndex returns the index of the first available entry in the log.
	// Returns 0 if the log is empty.
	GetFirstIndex() types.Index

	// GetTerm returns the term of the log entry at the given index.
	// Returns an error if the entry is not found or has been compacted.
	GetTerm(ctx context.Context, index types.Index) (types.Term, error)

	// GetEntries returns log entries in the range [startIndex, endIndex).
	// Returns an empty slice if the range is empty or startIndex == endIndex.
	// Returns an error if the range includes compacted entries or is out of bounds.
	GetEntries(ctx context.Context, startIndex, endIndex types.Index) ([]types.LogEntry, error)

	// AppendEntries appends the given entries to the log.
	// Returns an error if the entries are not contiguous with the existing log
	// or if storage operations fail.
	AppendEntries(ctx context.Context, entries []types.LogEntry) error

	// TruncatePrefix removes log entries before the given index (exclusive).
	// Used for log compaction after snapshots.
	// Returns an error if truncation fails.
	TruncatePrefix(ctx context.Context, newFirstIndex types.Index) error

	// TruncateSuffix removes log entries at or after the given index.
	// Used during conflict resolution.
	// Returns an error if truncation fails.
	TruncateSuffix(ctx context.Context, newLastIndexPlusOne types.Index) error

	// IsConsistentWithStorage checks if the in-memory state matches the storage state.
	// Returns true if consistent, false with an error otherwise.
	IsConsistentWithStorage(ctx context.Context) (bool, error)

	// RebuildInMemoryState reconstructs the in-memory state from storage.
	// Used during recovery or when inconsistencies are detected.
	RebuildInMemoryState(ctx context.Context) error

	// Stop signals the log manager to shut down and release resources.
	Stop()
}
