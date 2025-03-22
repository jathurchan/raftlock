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
	if err := ctx.Err(); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.state = state
	return nil
}

// Retrieves the Raft node's persisted term and votedFor information.
// Always returns nil (no error) for error as this is an in-memory operation.
func (ms *MemoryStorage) LoadState(ctx context.Context) (RaftState, error) {
	if err := ctx.Err(); err != nil {
		return RaftState{}, err
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.state, nil
}

// AppendEntries appends entries to the end of the log.
// All entries must have contiguous indices starting from lastIndex+1.
func (ms *MemoryStorage) AppendEntries(ctx context.Context, entries []*pb.LogEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	lastIndex := uint64(0)
	if len(ms.log) > 0 {
		lastIndex = ms.log[len(ms.log)-1].Index
	}
	if err := CheckEntriesContiguity(entries, lastIndex); err != nil {
		return err
	}
	ms.log = append(ms.log, entries...)
	return nil
}

// Returns log entries in the range [low, high).
// Log indices start at 1. `low` is inclusive, `high` is exclusive.
// The range is automatically adjusted to the available entries.
// Returns an empty slice if adjusted range is invalid.
// Returns ErrIndexOutOfRange if low > high.
func (ms *MemoryStorage) GetEntries(ctx context.Context, low, high uint64) ([]*pb.LogEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if low > high {
		return nil, ErrIndexOutOfRange
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.log) == 0 {
		return []*pb.LogEntry{}, nil
	}
	firstIndex := ms.log[0].Index
	lastIndex := ms.log[len(ms.log)-1].Index
	low = max(low, firstIndex)
	high = min(high, lastIndex+1)
	if low >= high {
		return []*pb.LogEntry{}, nil
	}
	start := int(low - firstIndex)
	end := int(high - firstIndex)
	return ms.log[start:end], nil
}

// Returns the log entry at the specified index.
// Log indices start at 1.
// If the index is out of bounds (i.e., before the first or after the last entry),
// ErrNotFound is returned.
func (ms *MemoryStorage) GetEntry(ctx context.Context, index uint64) (*pb.LogEntry, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.log) == 0 {
		return nil, ErrNotFound
	}
	firstIndex := ms.log[0].Index
	lastIndex := ms.log[len(ms.log)-1].Index
	if index < firstIndex || index > lastIndex {
		return nil, ErrNotFound
	}
	return ms.log[index-firstIndex], nil
}

// Returns the index of the last log entry.
func (ms *MemoryStorage) LastIndex() uint64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.log) == 0 {
		return 0
	}
	return ms.log[len(ms.log)-1].Index
}

// FirstIndex returns the index of the first log entry.
func (ms *MemoryStorage) FirstIndex() uint64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.log) == 0 {
		return 0
	}
	return ms.log[0].Index
}

// Removes all log entries with indices greater than or equal to the given index.
func (ms *MemoryStorage) TruncateSuffix(ctx context.Context, index uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.log) == 0 {
		return nil
	}
	firstIndex := ms.log[0].Index
	lastIndex := ms.log[len(ms.log)-1].Index
	if index > lastIndex {
		return nil // no need to truncate
	}
	if index <= firstIndex {
		ms.log = []*pb.LogEntry{} // clear all logs
		return nil
	}
	truncateIdx := int(index - firstIndex)
	ms.log = ms.log[:truncateIdx]
	return nil
}

// Removes all log entries with indices less than the given index.
// If the given index is greater than all existing indices, it preserves only the
// last entry to maintain the log's state. If the index is less than or equal to
// the first entry's index, no truncation occurs.
func (ms *MemoryStorage) TruncatePrefix(ctx context.Context, index uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.log) == 0 {
		return nil
	}
	firstIndex := ms.log[0].Index
	lastIndex := ms.log[len(ms.log)-1].Index
	if index > lastIndex {
		lastEntry := ms.log[len(ms.log)-1]
		ms.log = []*pb.LogEntry{lastEntry}
		return nil
	}
	if index <= firstIndex {
		return nil
	}
	truncateIdx := int(index - firstIndex)
	ms.log = ms.log[truncateIdx:]
	return nil
}

// Releases resources (no-op for MemoryStorage).
func (ms *MemoryStorage) Close() error {
	return nil
}
