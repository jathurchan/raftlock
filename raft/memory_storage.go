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
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.state = state
	return nil
}

// Retrieves the Raft node's persisted term and votedFor information.
// Always returns nil (no error) for error as this is an in-memory operation.
func (ms *MemoryStorage) LoadState(ctx context.Context) (RaftState, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.state, nil
}

// Appends one or more log entries to the Raft log.
// Always returns nil (no error) as this is an in-memory operation.
func (ms *MemoryStorage) AppendEntries(ctx context.Context, entries []*pb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.log = append(ms.log, entries...)
	return nil
}

// GetEntries returns log entries in range [low, high).
// The 'low' index is inclusive, and the 'high' index is exclusive.
// Adjusts range to available entries. Returns empty slice if no entries in range.
// Returns ErrIndexOutOfRange if low > high.
func (ms *MemoryStorage) GetEntries(ctx context.Context, low, high uint64) ([]*pb.LogEntry, error) {
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
	if low < firstIndex {
		low = firstIndex
	}
	if high > lastIndex+1 {
		high = lastIndex + 1
	}
	if low >= high {
		return []*pb.LogEntry{}, nil
	}
	lowIdx := int(low - firstIndex)
	highIdx := int(high - firstIndex)
	res := make([]*pb.LogEntry, highIdx-lowIdx)
	copy(res, ms.log[lowIdx:highIdx]) // prevent potential data races
	return res, nil
}

// Retrieves the log entry at the specified index.
// Returns ErrNotFound if not available.
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
	idx := int(index - firstIndex)
	return ms.log[idx], nil
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

// Returns the index of the first log entry.
func (ms *MemoryStorage) FirstIndex() uint64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.log) == 0 {
		return 0
	}
	return ms.log[0].Index
}

// Helper method to rewrite the Raft log file to only include entries in the range [low, high).
// It's used by both TruncatePrefix and TruncateSuffix to discard unwanted log entries.
func (ms *MemoryStorage) truncateLog(ctx context.Context, low, high uint64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.log) == 0 {
		return nil
	}
	firstIndex := ms.log[0].Index
	lastIndex := ms.log[len(ms.log)-1].Index

	if low > lastIndex || high <= firstIndex {
		return nil
	}

	if low < firstIndex {
		low = firstIndex
	}
	if high > lastIndex+1 {
		high = lastIndex + 1
	}

	lowOffset := low - firstIndex
	highOffset := high - firstIndex

	// Create new log with entries in range [low, high)
	ms.log = ms.log[lowOffset:highOffset]

	return nil
}

// Removes all log entries with indices greater than or equal to the given index.
func (ms *MemoryStorage) TruncateSuffix(ctx context.Context, index uint64) error {
	ms.truncateLog(ctx, ms.FirstIndex(), index)
	return nil
}

// Removes all log entries with indices less than or equal to the given index.
func (ms *MemoryStorage) TruncatePrefix(ctx context.Context, index uint64) error {
	ms.truncateLog(ctx, index, ms.LastIndex()+1)
	return nil
}

// Releases resources (no-op for MemoryStorage).
func (ms *MemoryStorage) Close() error {
	return nil
}
