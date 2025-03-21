package raft

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/jathurchan/raftlock/proto"
)

var (
	ErrNotFound        = errors.New("raft: log entry not found")
	ErrCorruptedData   = errors.New("raft: corrupted storage data")
	ErrIndexOutOfRange = errors.New("raft: log index out of range")
	ErrEmptyLog        = errors.New("raft: log empty")
	ErrStorageIO       = errors.New("raft: storage I/O error")
	// ErrNonContiguousEntries is returned when appending entries that don't form a continuous sequence
	ErrNonContiguousEntries = errors.New("non-contiguous log entries")
	// ErrOutOfOrderEntries is returned when entries are not in ascending order by index
	ErrOutOfOrderEntries = errors.New("entries are not in ascending order")
)

// RaftState represents the persistent state of a Raft node.
type RaftState struct {
	CurrentTerm uint64 // The current term of the Raft node.
	VotedFor    int64  // Candidate ID that received vote in current term (-1 if none)
}

// Storage defines the interface for persisting Raft node state and log entries.
// Implementations of this interface must be thread safe.
type Storage interface {
	// Persists the Raft node's current term and votedFor information.
	// The operation must be atomic: either the entire state is saved, or nothing.
	// Used during elections and when updating persistent state after leadership changes.
	// May return context errors if the operation is canceled or times out.
	SaveState(ctx context.Context, state RaftState) error

	// Retrieves the Raft node's persisted term and votedFor information.
	// Returns ErrCorruptedData if the persisted state is invalid.
	// Called during node initialization or restart to restore persistent state.
	// May return context errors if the operation is canceled or times out.
	LoadState(ctx context.Context) (RaftState, error)

	// Appends one or more log entries to the Raft log.
	// All entries must have contiguous indices starting from last index + 1.
	// The append operation must be atomic: either all entries are persisted, or none.
	// Returns ErrStorageIO if the append operation fails.
	// May return context errors if the operation is canceled or times out.
	AppendEntries(ctx context.Context, entries []*pb.LogEntry) error

	// Retrieves a slice of log entries within the specified index range [low, high).
	// Log indices start at 1. The `low` index is inclusive, and the `high` index is exclusive.
	// Returns an empty slice if adjusted range is invalid.
	// Returns ErrIndexOutOfRange if low > high.
	// May return context errors if the operation is canceled or times out.
	GetEntries(ctx context.Context, low, high uint64) ([]*pb.LogEntry, error)

	// Retrieves a single log entry at the given index.
	// Log indices start at 1.
	// Returns ErrNotFound if the index is outside the known range.
	// May return context errors if the operation is canceled or times out.
	GetEntry(ctx context.Context, index uint64) (*pb.LogEntry, error)

	// Returns the index of the last log entry in the Raft log.
	// Returns 0 if the log is empty.
	LastIndex() uint64

	// Returns the index of the first log entry in the Raft log.
	// Returns 0 if the log is empty.
	FirstIndex() uint64

	// Removes all log entries with indices >= the given index.
	// Used when log entries beyond a certain point are no longer valid—typically during log
	// replication when a follower's log conflicts with the leader's log. In such cases, the leader
	// will instruct the follower to delete its conflicting suffix and accept new entries.
	// Example:
	// 	Current log: [5, 6, 7, 8]
	// 	TruncateSuffix(index=7) -> Resulting log: [5, 6]
	// May return context errors if the operation is canceled or times out.
	TruncateSuffix(ctx context.Context, index uint64) error

	// Removes all log entries with indices < the given index.
	// If the given index is greater than all existing indices, it preserves only the
	// last entry to maintain the log's state. If the index is less than or equal to
	// the first entry's index, no truncation occurs.
	// Used for log compaction—after entries have been applied to the state machine and possibly
	// snapshotted. Removing older, already-applied entries helps reduce memory usage and prevent
	// unbounded log growth.
	// Example:
	// 	Current log: [10, 11, 12, 13]
	//	TruncatePrefix(index=12) -> Resulting log: [12, 13]
	// May return context errors if the operation is canceled or times out.
	TruncatePrefix(ctx context.Context, index uint64) error

	// Closes the storage, releasing any resources.
	Close() error
}

// StorageType represents the type of storage to use.
type StorageType string

const (
	// Memory storage (in-memory, non-persistent)
	MemoryStorageType StorageType = "memory"

	// File storage (file-based, persistent)
	FileStorageType StorageType = "file"
)

// StorageConfig contains configuration for storage.
type StorageConfig struct {
	Type StorageType // Type of storage to use
	Dir  string      // Directory to store persistent data

}

// Returns a default configuration for storage.
func DefaultStorageConfig() *StorageConfig {
	return &StorageConfig{
		Type: FileStorageType,
		Dir:  "../data/raft",
	}
}

// Creates a new storage instance based on configuration.
func NewStorage(config *StorageConfig) (Storage, error) {
	if config == nil {
		config = DefaultStorageConfig()
	}
	switch config.Type {
	case FileStorageType:
		return NewFileStorage(config)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.Type)
	}
}
