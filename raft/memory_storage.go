package raft

import (
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
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		state: RaftState{CurrentTerm: 0, VotedFor: -1},
		log:   []*pb.LogEntry{},
	}
}

// Persists the Raft node's current term and votedFor information.
// Always returns nil (no error) as this is an in-memory operation.
func (ms *MemoryStorage) SaveState(state RaftState) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.state = state
	return nil
}

// Retrieves the Raft node's persisted term and votedFor information.
// Always returns nil (no error) for error as this is an in-memory operation.
func (ms *MemoryStorage) LoadState() (RaftState, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.state, nil
}

// Appends one or more log entries to the Raft log.
// Always returns nil (no error) as this is an in-memory operation.
func (ms *MemoryStorage) AppendEntries(entries []*pb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.log = append(ms.log, entries...)
	return nil
}

// GetEntries returns log entries in range [low, high).
// Adjusts range to available entries. Returns empty slice if no entries in range.
// Returns ErrIndexOutOfRange if low > high.
func (ms *MemoryStorage) GetEntries(low, high uint64) ([]*pb.LogEntry, error) {
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
func (ms *MemoryStorage) GetEntry(index uint64) (*pb.LogEntry, error) {
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
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.log) == 0 {
		return 0, nil
	}
	return ms.log[len(ms.log)-1].Index, nil
}

// Returns the index of the first log entry.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.log) == 0 {
		return 0, nil
	}
	return ms.log[0].Index, nil
}

// Helper method to handle both prefix and suffix truncating.
func (ms *MemoryStorage) truncateLog(index uint64, isPrefix bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.log) == 0 {
		return
	}
	firstIndex := ms.log[0].Index
	lastIndex := ms.log[len(ms.log)-1].Index

	if (isPrefix && index >= lastIndex) || (!isPrefix && index <= firstIndex) {
		// truncate all entries
		ms.log = []*pb.LogEntry{}
		return
	}
	if (isPrefix && index < firstIndex) || (!isPrefix && index > lastIndex) {
		// No entries to truncate
		return
	}
	offset := index - firstIndex
	if isPrefix {
		offset++
		ms.log = ms.log[offset:]
	} else {
		ms.log = ms.log[:offset]
	}
}

// Removes all log entries with indices greater than or equal to the given index.
func (ms *MemoryStorage) TruncateSuffix(index uint64) error {
	ms.truncateLog(index, false)
	return nil
}

// Removes all log entries with indices less than or equal to the given index.
func (ms *MemoryStorage) TruncatePrefix(index uint64) error {
	ms.truncateLog(index, true)
	return nil
}
