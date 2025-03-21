package raft

import (
	"context"
	"sync"

	pb "github.com/jathurchan/raftlock/proto"
)

// MemoryStorage implements the Storage interface using in-memory data structures.
// Useful for testing, development, benchmarking. Does not provide durability guarantees.
type MemoryStorage struct {
	mu    sync.RWMutex
	state RaftState
	log   []*pb.LogEntry
}

// Creates a new MemoryStorage instance.
func NewMemoryStorage() (Storage, error) {
	return &MemoryStorage{
		state: RaftState{CurrentTerm: 0, VotedFor: -1},
		log:   []*pb.LogEntry{},
	}, nil
}

// Persists the Raft node's current term and votedFor information.
// Always returns nil (no error) as this is an in-memory operation.
func (ms *MemoryStorage) SaveState(ctx context.Context, state RaftState) error {
	return ErrNotImplemented
}

// Retrieves the Raft node's persisted term and votedFor information.
// Always returns nil (no error) for error as this is an in-memory operation.
func (ms *MemoryStorage) LoadState(ctx context.Context) (RaftState, error) {
	return RaftState{}, ErrNotImplemented
}

// Appends one or more log entries to the Raft log.
// Always returns nil (no error) as this is an in-memory operation.
func (ms *MemoryStorage) AppendEntries(ctx context.Context, entries []*pb.LogEntry) error {
	return ErrNotImplemented
}

// GetEntries returns log entries in range [low, high).
// The 'low' index is inclusive, and the 'high' index is exclusive.
// Adjusts range to available entries. Returns empty slice if no entries in range.
// Returns ErrIndexOutOfRange if low > high.
func (ms *MemoryStorage) GetEntries(ctx context.Context, low, high uint64) ([]*pb.LogEntry, error) {
	return nil, ErrNotImplemented
}

// Retrieves the log entry at the specified index.
// Returns ErrNotFound if not available.
func (ms *MemoryStorage) GetEntry(ctx context.Context, index uint64) (*pb.LogEntry, error) {
	return nil, ErrNotImplemented
}

// Returns the index of the last log entry.
func (ms *MemoryStorage) LastIndex() uint64 {
	return 0
}

// FirstIndex returns the index of the first log entry.
func (fs *MemoryStorage) FirstIndex() uint64 {
	return 0
}

// Removes all log entries with indices greater than or equal to the given index.
func (fs *MemoryStorage) TruncateSuffix(ctx context.Context, index uint64) error {
	return ErrNotImplemented
}

// Removes all log entries with indices less than the given index.
func (fs *MemoryStorage) TruncatePrefix(ctx context.Context, index uint64) error {
	return ErrNotImplemented
}

// Releases resources (no-op for MemoryStorage).
func (ms *MemoryStorage) Close() error {
	return nil
}
