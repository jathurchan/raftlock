package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
)

var (
	ErrNotFound        = errors.New("raft: log entry not found")
	ErrCorruptedData   = errors.New("raft: corrupted storage data")
	ErrIndexOutOfRange = errors.New("raft: log index out of range")
	ErrNotImplemented  = errors.New("raft: operation not implemented")
)

// RaftState represents the persistent state of a Raft node.
type RaftState struct {
	CurrentTerm uint64 // The current term of the Raft node.
	VotedFor    int    // Candidate ID that received vote in current term (-1 if none)
}

// Storage defines the interface for persisting Raft node state and log entries.
type Storage interface {
	// Persists the Raft node's current term and votedFor information.
	// The operation must be atomic: either the entire state is saved, or nothing.
	SaveState(ctx context.Context, state RaftState) error

	// Retrieves the Raft node's persisted term and votedFor information.
	// Returns ErrCorruptedData if the persisted state is invalid.
	LoadState(ctx context.Context) (RaftState, error)

	// Appends one or more log entries to the Raft log.
	// Returns ErrStorageIO if the append operation fails.
	AppendEntries(ctx context.Context, entries []*pb.LogEntry) error

	// Retrieves a slice of log entries within the specified index range [low, high).
	// The 'low' index is inclusive, and the 'high' index is exclusive.
	// Returns ErrIndexOutOfRange if low > high
	GetEntries(ctx context.Context, low, high uint64) ([]*pb.LogEntry, error)

	// Retrieves a single log entry at the given index.
	// Returns ErrNotFound if the entry does not exist.
	GetEntry(ctx context.Context, index uint64) (*pb.LogEntry, error)

	// Returns the index of the last log entry in the Raft log.
	// Returns 0 if the log is empty.
	LastIndex() (uint64, error)

	// Returns the index of the first log entry in the Raft log.
	// Returns 0 if the log is empty.
	FirstIndex() (uint64, error)

	// Removes all log entries with indices >= the given index.
	// Used when conflicting entries are found during log replication.
	TruncateSuffix(ctx context.Context, index uint64) error

	// Removes all log entries with indices <= the given index.
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
	Type               StorageType   // Type of storage to use
	Dir                string        // Directory to store persistent data
	SyncWrites         bool          // Whether to sync writes to disk immediately (fsync)
	CompactionInterval time.Duration // How often TruncatePrefix is called to remove already applied entries
	MaxLogSize         int64         // Maximum size of the log in bytes before compaction
	MaxLogEntries      uint64        // Maximum number of log entries to keep

}

// Returns a default configuration for storage.
func DefaultStorageConfig() *StorageConfig {
	return &StorageConfig{
		Type:               FileStorageType,
		Dir:                "data/raft",
		SyncWrites:         true,
		CompactionInterval: 1 * time.Hour,
		MaxLogSize:         1024 * 1024 * 100, // 100 MB
		MaxLogEntries:      50000,             // 50 000 entries
	}
}

// Creates a new storage instance based on configuration.
func NewStorage(config *StorageConfig) (Storage, error) {
	if config == nil {
		config = DefaultStorageConfig()
	}
	switch config.Type {
	case MemoryStorageType:
		return NewMemoryStorage()
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.Type)
	}
}
