package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

// LogManager defines the interface for managing Raft log operations.
// It handles log entry persistence, retrieval, manipulation, and provides
// a consistent view of the log state.
type LogManager interface {
	// Initialize loads the log state from storage during startup.
	// Returns an error if loading fails or if state is inconsistent.
	Initialize(ctx context.Context) error

	// GetLastIndexUnsafe returns the index of the last entry in the log.
	// Returns 0 if the log is empty.
	// Callers must ensure thread safety by acquiring the appropriate Raft read lock.
	GetLastIndexUnsafe() types.Index

	// GetLastTermUnsafe returns the term of the last entry in the log.
	// Returns 0 if the log is empty.
	// Callers must ensure thread safety by acquiring the appropriate Raft read lock.
	GetLastTermUnsafe() types.Term

	// GetConsistentLastState returns the last index and term as an atomic pair,
	// protected by the Raft mutex. Avoids race conditions inherent in separate calls
	// to GetLastIndex and GetLastTerm when concurrent writes might occur.
	GetConsistentLastState() (types.Index, types.Term)

	// GetFirstIndex returns the index of the first available entry in the log.
	// Returns 0 if the log is empty.
	GetFirstIndex() types.Index

	// GetTerm returns the term of the log entry at the given index.
	// Returns an error if the entry is not found (ErrNotFound), has been compacted (ErrCompacted),
	// or if the node is shutting down (ErrShuttingDown). Safe for concurrent use.
	GetTerm(ctx context.Context, index types.Index) (types.Term, error)

	// GetTermUnsafe returns the term of the log entry at the given index without synchronization.
	// This method skips shutdown checks, metrics, and locking for performance.
	// Callers must ensure thread safety and log bounds are respected.
	GetTermUnsafe(ctx context.Context, index types.Index) (types.Term, error)

	// GetEntries returns log entries in the range [startIndex, endIndex).
	// Returns an empty slice if the range is empty or invalid.
	// Returns an error if the range includes compacted entries (ErrCompacted),
	// is out of bounds (ErrNotFound), or if the node is shutting down (ErrShuttingDown).
	GetEntries(ctx context.Context, startIndex, endIndex types.Index) ([]types.LogEntry, error)

	// AppendEntries appends the given entries to the log.
	// The entries must be contiguous with the existing log.
	// Returns an error if the entries are not contiguous, if storage operations fail,
	// or if the node is shutting down (ErrShuttingDown).
	AppendEntries(ctx context.Context, entries []types.LogEntry) error

	// TruncatePrefix removes log entries before the given index (exclusive).
	// Used for log compaction after snapshots.
	// Returns an error if the index is invalid, truncation fails,
	// or if the node is shutting down (ErrShuttingDown).
	TruncatePrefix(ctx context.Context, newFirstIndex types.Index) error

	// TruncateSuffix removes log entries at or after the given index.
	// Used during conflict resolution when appending entries.
	// Returns an error if the index is invalid, truncation fails,
	// or if the node is shutting down (ErrShuttingDown).
	TruncateSuffix(ctx context.Context, newLastIndexPlusOne types.Index) error

	// IsConsistentWithStorage checks if the in-memory cached log state (last index/term)
	// matches the state persisted in storage. Returns false and an error if inconsistent
	// or if the storage check fails.
	IsConsistentWithStorage(ctx context.Context) (bool, error)

	// RebuildInMemoryState reconstructs the cached in-memory state (last index/term)
	// from the persistent storage. Used during recovery or when inconsistencies are detected.
	RebuildInMemoryState(ctx context.Context) error

	// FindLastEntryWithTermUnsafe searches backward for the last log entry with the given term.
	// If searchFromHint is non-zero and less than the current last index, the scan starts from that index;
	// otherwise, it starts from the last index.
	// Returns the index of the matching entry, or 0 and ErrNotFound if the term is not found
	// or the scan encounters compacted log regions.
	//
	// This method does not perform locking. The caller must ensure thread safety.
	FindLastEntryWithTermUnsafe(ctx context.Context, term types.Term, searchFromHint types.Index) (types.Index, error)

	// FindFirstIndexInTermUnsafe scans backward from searchUpToIndex to find the first log index
	// that belongs to the given term. It assumes that log entries for a term are contiguous.
	// The scan stops at the first entry with a different term.
	//
	// Returns the index of the first entry in the term if found, or 0 and an error
	// (e.g., ErrNotFound or ErrCompacted) if the term is not present or is in a compacted region.
	//
	// This method does not perform locking. The caller must ensure thread safety.
	FindFirstIndexInTermUnsafe(ctx context.Context, term types.Term, searchUpToIndex types.Index) (types.Index, error)

	// Stop signals the log manager to shut down and release resources.
	// Note: Actual shutdown logic (stopping operations) relies on checking the
	// shared isShutdown flag.
	Stop()
}

// logManager implements the LogManager interface.
// It provides thread-safe access to log operations and maintains cached in-memory state
// (lastIndex, lastTerm) for performance, synchronized with persistent storage.
type logManager struct {
	mu         *sync.RWMutex // Raft's mutex protecting state fields.
	isShutdown *atomic.Bool  // Shared flag indicating Raft shutdown.

	id types.NodeID // ID of the local Raft node.

	logger  logger.Logger
	metrics Metrics
	storage storage.Storage

	lastIndex atomic.Uint64 // Cached index of the latest log entry.
	lastTerm  atomic.Uint64 // Cached term of the latest log entry.
}

// NewLogManager creates and initializes a new LogManager instance.
func NewLogManager(
	mu *sync.RWMutex,
	shutdownFlag *atomic.Bool,
	deps Dependencies,
	nodeID types.NodeID,
) LogManager {
	if mu == nil {
		panic("raft: NewLogManager requires non-nil mutex")
	}
	if shutdownFlag == nil {
		panic("raft: NewLogManager requires non-nil shutdownFlag")
	}
	if deps.Storage == nil {
		panic("raft: NewLogManager requires non-nil Storage dependency")
	}
	if deps.Metrics == nil {
		panic("raft: NewLogManager requires non-nil Metrics dependency")
	}
	if deps.Logger == nil {
		panic("raft: NewLogManager requires non-nil Logger dependency")
	}
	if nodeID == unknownNodeID {
		panic("raft: NewLogManager requires a non-empty nodeID")
	}

	return &logManager{
		mu:         mu,
		isShutdown: shutdownFlag,
		id:         nodeID,
		storage:    deps.Storage,
		metrics:    deps.Metrics,
		logger:     deps.Logger.WithComponent("log"),
	}
}

// Initialize loads the persisted last log index and term during system startup.
// It sets up the cached in-memory state for log tracking and reports initial metrics.
func (lm *logManager) Initialize(ctx context.Context) error {
	lm.logger.Infow("Initializing log manager", "nodeID", lm.id)

	lastIdx := lm.storage.LastLogIndex()
	firstIdx := lm.storage.FirstLogIndex()

	if lastIdx == 0 {
		lm.updateCachedState(0, 0)
		lm.logger.Infow("Log is empty, initialized with zero index and term")
		lm.metrics.ObserveLogState(0, 0, 0)
		return nil
	}

	term, err := lm.fetchEntryTerm(ctx, lastIdx)
	if err != nil {
		lm.logger.Errorw("Failed to get term for last log index during initialization", "lastIndex", lastIdx, "error", err)
		return fmt.Errorf("inconsistent storage: failed to fetch term for last index %d: %w", lastIdx, err)
	}

	lm.updateCachedState(lastIdx, term)
	lm.metrics.ObserveLogState(firstIdx, lastIdx, term)

	lm.logger.Infow("Log manager initialized successfully",
		"firstIndex", firstIdx,
		"lastIndex", lastIdx,
		"lastTerm", term)

	return nil
}

// GetLastIndexUnsafe returns the cached last index.
// Assumes lm.mu.RLock() is held
func (lm *logManager) GetLastIndexUnsafe() types.Index {
	return types.Index(lm.lastIndex.Load())
}

// GetLastTermUnsafe returns the cached last term.
// Assumes lm.mu.RLock() is held
func (lm *logManager) GetLastTermUnsafe() types.Term {
	return types.Term(lm.lastTerm.Load())
}

// GetConsistentLastState atomically returns the cached last index and term,
// protected by the Raft mutex to ensure consistency relative to concurrent writes
// (like appends or truncations) that update both index and term.
func (lm *logManager) GetConsistentLastState() (types.Index, types.Term) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lastIdx := lm.lastIndex.Load()
	lastTerm := lm.lastTerm.Load()

	return types.Index(lastIdx), types.Term(lastTerm)
}

// GetFirstIndex returns the current first log index from storage.
func (lm *logManager) GetFirstIndex() types.Index {
	// Always fetch dynamically from storage, as this changes independently of cached state.
	return lm.storage.FirstLogIndex()
}

// GetTerm returns the term of the log entry at the given index.
// Safe for concurrent use.
func (lm *logManager) GetTerm(ctx context.Context, index types.Index) (types.Term, error) {
	startTime := time.Now()
	success := false
	defer func() {
		lm.metrics.ObserveLogRead(LogReadTypeTerm, time.Since(startTime), success)
	}()

	if lm.isShutdown.Load() {
		return 0, ErrShuttingDown
	}

	term, err := lm.getTermInternal(ctx, index, false)
	if err == nil {
		success = true
	}
	return term, err
}

// GetTermUnsafe returns the term of the log entry at the given index without locking or metrics.
// The caller must ensure thread safety.
func (lm *logManager) GetTermUnsafe(ctx context.Context, index types.Index) (types.Term, error) {
	return lm.getTermInternal(ctx, index, true)
}

// getTermInternal returns the term of the log entry at the specified index.
// It first attempts to serve the request from a cached last index/term if applicable.
// If not cached, it verifies the index is within valid bounds and fetches the term from storage.
// If useUnlockedCache is true, it assumes the caller already holds lm.mu and accesses cache without locks.
func (lm *logManager) getTermInternal(ctx context.Context, index types.Index, useUnlockedCache bool) (types.Term, error) {
	if index == 0 {
		return 0, nil
	}

	var cachedTerm types.Term
	var ok bool

	if useUnlockedCache {
		cachedTerm, ok = lm.tryGetCachedTermUnlocked(index)
	} else {
		cachedTerm, ok = lm.tryGetCachedTerm(index)
	}
	if ok {
		return cachedTerm, nil
	}

	if err := lm.checkLogBounds(index); err != nil {
		return 0, err
	}

	entry, err := lm.storage.GetLogEntry(ctx, index)
	if err != nil {
		if errors.Is(err, storage.ErrEntryNotFound) {
			if index < lm.GetFirstIndex() {
				return 0, fmt.Errorf("requested index %d was compacted: %w", index, ErrCompacted)
			}
			return 0, fmt.Errorf("requested index %d was not found: %w", index, ErrNotFound)
		}
		return 0, fmt.Errorf("failed to get log entry %d from storage: %w", index, err)
	}

	return entry.Term, nil
}

// tryGetCachedTerm checks if the requested index is the cached last index
// and returns the cached term if available and valid (term > 0).
func (lm *logManager) tryGetCachedTerm(index types.Index) (types.Term, bool) {
	lastIdx, lastTerm := lm.GetConsistentLastState()
	return tryGetCachedTermCommon(index, lastIdx, lastTerm)
}

// tryGetCachedTermUnlocked checks if the requested index is the cached last index
// and returns the cached term if available and valid (term > 0).
// Assumes lm.mu.RLock() is held.
func (lm *logManager) tryGetCachedTermUnlocked(index types.Index) (types.Term, bool) {
	lastIdx := lm.GetLastIndexUnsafe()
	lastTerm := lm.GetLastTermUnsafe()
	return tryGetCachedTermCommon(index, lastIdx, lastTerm)
}

// tryGetCachedTermCommon checks if the given index matches the provided cached index and term,
// and that the term is valid (> 0). If so, returns the term and true.
func tryGetCachedTermCommon(index, cachedIndex types.Index, cachedTerm types.Term) (types.Term, bool) {
	if index == cachedIndex && cachedTerm > 0 {
		return cachedTerm, true
	}
	return 0, false
}

// checkLogBounds verifies that the index is within the valid log range [firstIndex, lastIndex].
// Returns specific errors ErrCompacted or ErrNotFound if out of bounds.
func (lm *logManager) checkLogBounds(index types.Index) error {
	firstIndex := lm.GetFirstIndex()
	lastIndex := lm.GetLastIndexUnsafe()

	if index < firstIndex {
		return fmt.Errorf("requested index %d is compacted (first available: %d): %w", index, firstIndex, ErrCompacted)
	}
	if index > lastIndex {
		return fmt.Errorf("requested index %d is beyond last index %d: %w", index, lastIndex, ErrNotFound)
	}
	return nil
}

// GetEntries returns log entries in the range [startIndex, endIndex).
func (lm *logManager) GetEntries(ctx context.Context, startIndex, endIndex types.Index) ([]types.LogEntry, error) {
	startTime := time.Now()
	var entries []types.LogEntry
	var err error
	success := false

	defer func() {
		lm.metrics.ObserveLogRead(LogReadTypeEntries, time.Since(startTime), success)
	}()

	if lm.isShutdown.Load() {
		return nil, ErrShuttingDown
	}

	if isEmptyRange(startIndex, endIndex) {
		success = true
		return nil, nil
	}

	effectiveEnd := lm.clampEndIndex(endIndex)
	if isEmptyRange(startIndex, effectiveEnd) {
		lm.metrics.ObserveLogRead(LogReadTypeEntries, time.Since(startTime), true)
		return nil, nil
	}

	entries, err = lm.storage.GetLogEntries(ctx, startIndex, effectiveEnd)
	if err != nil {
		return nil, lm.mapStorageReadError(err, startIndex, effectiveEnd)
	}

	if err = validateEntryRange(entries, startIndex, effectiveEnd); err != nil {
		lm.logger.Errorw("Storage returned inconsistent log entry range", "error", err, "requestedStart", startIndex, "requestedEnd", effectiveEnd)
		lm.metrics.ObserveLogConsistencyError()
		return nil, fmt.Errorf("internal storage error: %w", err)
	}

	success = true
	return entries, nil
}

// isEmptyRange returns true if the given log range [start, end) is empty or invalid.
func isEmptyRange(start, end types.Index) bool {
	return start >= end
}

// clampEndIndex ensures the requested end index does not exceed the log’s actual end index + 1.
func (lm *logManager) clampEndIndex(requestedEnd types.Index) types.Index {
	last := lm.GetLastIndexUnsafe()
	actualEnd := last + 1
	if requestedEnd > actualEnd {
		return actualEnd
	}
	return requestedEnd
}

// mapStorageReadError interprets errors from storage.GetLogEntries, mapping them to
// Raft-level errors like ErrCompacted or ErrNotFound, and checking for concurrent compaction.
func (lm *logManager) mapStorageReadError(err error, start, end types.Index) error {
	if errors.Is(err, storage.ErrIndexOutOfRange) {
		newFirst := lm.GetFirstIndex()
		if start < newFirst {
			return fmt.Errorf("entries [%d, %d) were compacted concurrently (first available: %d): %w",
				start, end, newFirst, ErrCompacted)
		}
		return fmt.Errorf("entries [%d, %d) not found: %w", start, end, ErrNotFound)
	}
	return fmt.Errorf("failed to retrieve entries [%d, %d): %w", start, end, err)
}

// validateEntryRange checks that the returned log entries match the expected range [start, end).
func validateEntryRange(entries []types.LogEntry, expectedStart, expectedEnd types.Index) error {
	if len(entries) == 0 {
		if expectedStart != expectedEnd {
			return fmt.Errorf("expected entries in range [%d, %d), but got none", expectedStart, expectedEnd)
		}
		return nil
	}

	firstIndex := entries[0].Index
	lastIndex := entries[len(entries)-1].Index + 1

	if firstIndex != expectedStart || lastIndex != expectedEnd {
		return fmt.Errorf("storage returned inconsistent range: got [%d, %d), expected [%d, %d)",
			firstIndex, lastIndex, expectedStart, expectedEnd)
	}

	return nil
}

// AppendEntries appends a batch of log entries to storage and updates cached state.
func (lm *logManager) AppendEntries(ctx context.Context, entries []types.LogEntry) error {
	startTime := time.Now()
	entryCount := len(entries)
	success := false
	var err error

	defer func() {
		lm.metrics.ObserveLogAppend(entryCount, time.Since(startTime), success)
	}()

	if lm.isShutdown.Load() {
		return ErrShuttingDown
	}

	if entryCount == 0 {
		success = true
		return nil
	}

	err = lm.storage.AppendLogEntries(ctx, entries)
	if err != nil {
		lm.logger.Errorw("Storage rejected AppendLogEntries", "error", err, "entryCount", entryCount, "firstIndex", entries[0].Index)
		return fmt.Errorf("log append failed: %w", err)
	}

	lastEntry := entries[entryCount-1]
	firstIndexInStorage := lm.syncLogStateFromAppend(lastEntry)

	lm.metrics.ObserveLogState(firstIndexInStorage, lastEntry.Index, lastEntry.Term)

	lm.logger.Debugw("Appended log entries successfully",
		"count", entryCount,
		"firstAppendedIndex", entries[0].Index,
		"lastAppendedIndex", lastEntry.Index,
		"lastAppendedTerm", lastEntry.Term,
		"latencyMs", time.Since(startTime).Milliseconds())

	success = true
	return nil
}

// syncLogStateFromAppend updates the cached last index and term after a successful append.
// Requires holding the lm.mu write lock.
// Returns the current first index from storage.
func (lm *logManager) syncLogStateFromAppend(lastAppendedEntry types.LogEntry) types.Index {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.lastIndex.Store(uint64(lastAppendedEntry.Index))
	lm.lastTerm.Store(uint64(lastAppendedEntry.Term))

	return lm.storage.FirstLogIndex()
}

// TruncatePrefix removes all log entries before newFirstIndex (exclusive).
func (lm *logManager) TruncatePrefix(ctx context.Context, newFirstIndex types.Index) error {
	startTime := time.Now()
	entriesRemoved := 0
	success := false
	var err error

	defer func() {
		lm.metrics.ObserveLogTruncate(LogTruncateTypePrefix, entriesRemoved, time.Since(startTime), success)
	}()

	if lm.isShutdown.Load() {
		return ErrShuttingDown
	}

	if newFirstIndex == 0 {
		err = fmt.Errorf("invalid truncation index: must be > 0")
		return err
	}

	oldFirstIndex := lm.GetFirstIndex()

	if newFirstIndex <= oldFirstIndex {
		lm.logger.Debugw("TruncatePrefix skipped: index less than or equal to current first index", "newFirstIndex", newFirstIndex, "oldFirstIndex", oldFirstIndex)
		success = true
		return nil
	}

	lastIndex := lm.GetLastIndexUnsafe()
	if newFirstIndex > lastIndex+1 {
		err = fmt.Errorf("invalid truncation index: %d is beyond last index %d", newFirstIndex, lastIndex)
		return err
	}

	err = lm.storage.TruncateLogPrefix(ctx, newFirstIndex)
	if err != nil {
		lm.logger.Errorw("Failed to truncate log prefix in storage", "newFirstIndex", newFirstIndex, "error", err)
		return fmt.Errorf("log prefix truncation to index %d failed: %w", newFirstIndex, err)
	}

	resultingFirstIndex := lm.storage.FirstLogIndex()
	entriesRemoved = int(resultingFirstIndex - oldFirstIndex)

	if entriesRemoved < 0 {
		lm.logger.Warnw("First index decreased after TruncatePrefix, expected increase or no change",
			"oldFirstIndex", oldFirstIndex, "newFirstIndex", resultingFirstIndex)
		entriesRemoved = 0 // To avoid negative count in metrics.
	}

	currentLastIndex := lm.GetLastIndexUnsafe()
	currentLastTerm := lm.GetLastTermUnsafe()
	lm.metrics.ObserveLogState(resultingFirstIndex, currentLastIndex, currentLastTerm)

	lm.logger.Infow("Log prefix truncated successfully",
		"requestedIndex", newFirstIndex,
		"oldFirstIndex", oldFirstIndex,
		"newFirstIndex", resultingFirstIndex,
		"entriesRemoved", entriesRemoved,
		"latencyMs", time.Since(startTime).Milliseconds())

	success = true
	return nil
}

// TruncateSuffix removes log entries at or after newLastIndexPlusOne.
func (lm *logManager) TruncateSuffix(ctx context.Context, newLastIndexPlusOne types.Index) error {
	startTime := time.Now()
	entriesRemoved := 0
	success := false
	var err error

	defer func() {
		lm.metrics.ObserveLogTruncate(LogTruncateTypeSuffix, entriesRemoved, time.Since(startTime), success)
	}()

	if lm.isShutdown.Load() {
		return ErrShuttingDown
	}

	oldLastIndex := lm.GetLastIndexUnsafe()

	if newLastIndexPlusOne > oldLastIndex {
		lm.logger.Debugw("TruncateSuffix skipped: index is beyond last log index",
			"index", newLastIndexPlusOne, "lastIndex", oldLastIndex)
		success = true
		return nil
	}

	if err := lm.storage.TruncateLogSuffix(ctx, newLastIndexPlusOne); err != nil {
		lm.logger.Errorw("Failed to truncate log suffix in storage",
			"newLastIndexPlusOne", newLastIndexPlusOne, "error", err)
		return fmt.Errorf("log suffix truncation from index %d failed: %w", newLastIndexPlusOne, err)
	}

	newLastIndex := lm.storage.LastLogIndex()

	if newLastIndex > oldLastIndex {
		lm.logger.Errorw("Inconsistent state: lastIndex increased after suffix truncation",
			"oldLast", oldLastIndex, "newLast", newLastIndex)
		lm.metrics.ObserveLogConsistencyError()
		return fmt.Errorf("suffix truncation led to inconsistent state: newLastIndex %d > oldLastIndex %d", newLastIndex, oldLastIndex)
	}

	var newLastTerm types.Term
	if newLastIndex > 0 {
		newLastTerm, err = lm.fetchEntryTerm(ctx, newLastIndex)
		if err != nil {
			lm.logger.Errorw("Failed to get term for new last index after suffix truncation",
				"index", newLastIndex, "error", err)
			lm.metrics.ObserveLogConsistencyError()
			_ = lm.RebuildInMemoryState(ctx)
			return fmt.Errorf("suffix truncation succeeded but failed to fetch new last term for index %d: %w", newLastIndex, err)
		}
	}

	if newLastIndex < oldLastIndex {
		entriesRemoved = int(oldLastIndex - newLastIndex)
	}

	firstIndex := lm.updateCachedStateAndGetFirst(newLastIndex, newLastTerm)
	lm.metrics.ObserveLogState(firstIndex, newLastIndex, newLastTerm)

	lm.logger.Infow("Log suffix truncated successfully",
		"requestedIndexPlusOne", newLastIndexPlusOne,
		"oldLastIndex", oldLastIndex,
		"newLastIndex", newLastIndex,
		"newLastTerm", newLastTerm,
		"entriesRemoved", entriesRemoved,
		"latencyMs", time.Since(startTime).Milliseconds())

	success = true
	return nil
}

// IsConsistentWithStorage verifies that the cached in-memory log state (last index/term)
// matches the persisted state in storage.
func (lm *logManager) IsConsistentWithStorage(ctx context.Context) (bool, error) {
	memLastIndex := lm.GetLastIndexUnsafe()
	memLastTerm := lm.GetLastTermUnsafe()

	storageLastIndex := lm.storage.LastLogIndex()

	if memLastIndex != storageLastIndex {
		lm.logger.Errorw("Log index mismatch detected", "memoryIndex", memLastIndex, "storageIndex", storageLastIndex)
		lm.metrics.ObserveLogConsistencyError()
		return false, fmt.Errorf("log index mismatch: memory=%d, storage=%d", memLastIndex, storageLastIndex)
	}

	if storageLastIndex == 0 {
		if memLastTerm != 0 {
			lm.logger.Errorw("Log term mismatch detected for empty log", "memoryTerm", memLastTerm)
			lm.metrics.ObserveLogConsistencyError()
			return false, fmt.Errorf("term mismatch for empty log: memory=%d, expected=0", memLastTerm)
		}
		return true, nil
	}

	storageTerm, err := lm.fetchEntryTerm(ctx, storageLastIndex)
	if err != nil {
		lm.logger.Errorw("Failed to fetch storage term for consistency check", "index", storageLastIndex, "error", err)
		return false, fmt.Errorf("unable to verify log consistency: failed to fetch term for last index %d: %w", storageLastIndex, err)
	}

	if memLastTerm != storageTerm {
		lm.logger.Errorw("Log term mismatch detected", "index", storageLastIndex, "memoryTerm", memLastTerm, "storageTerm", storageTerm)
		lm.metrics.ObserveLogConsistencyError()
		return false, fmt.Errorf("log term mismatch at index %d: memory=%d, storage=%d", storageLastIndex, memLastTerm, storageTerm)
	}

	lm.logger.Debugw("Log consistency check passed", "lastIndex", storageLastIndex, "lastTerm", storageTerm)
	return true, nil
}

// RebuildInMemoryState refreshes the cached lastIndex and lastTerm
// based on the current state of the log in persistent storage.
func (lm *logManager) RebuildInMemoryState(ctx context.Context) error {
	lm.logger.Infow("Rebuilding in-memory log state from storage...")

	lastIndex := lm.storage.LastLogIndex()
	firstIndex := lm.storage.FirstLogIndex()
	var lastTerm types.Term

	if lastIndex == 0 {
		lm.updateCachedState(0, 0)
		lm.metrics.ObserveLogState(firstIndex, 0, 0)
		lm.logger.Infow("Log is empty; cached state reset", "firstIndex", firstIndex)
		return nil
	}

	entryTerm, err := lm.fetchEntryTerm(ctx, lastIndex)
	if err != nil {
		lm.logger.Errorw("Failed to fetch last log entry during state rebuild",
			"lastIndex", lastIndex, "error", err)
		lm.updateCachedState(0, 0)
		lm.metrics.ObserveLogState(firstIndex, 0, 0)
		return fmt.Errorf("failed to rebuild state: could not fetch last entry term at index %d: %w", lastIndex, err)
	}
	lastTerm = entryTerm

	lm.updateCachedState(lastIndex, lastTerm)
	lm.metrics.ObserveLogState(firstIndex, lastIndex, lastTerm)

	lm.logger.Infow("In-memory log state rebuilt successfully from storage",
		"firstIndex", firstIndex,
		"lastIndex", lastIndex,
		"lastTerm", lastTerm)

	return nil
}

// FindLastEntryWithTermUnsafe searches backwards for the last entry with the given term.
// Starts search from `searchFromHint` if provided, otherwise from the last log index.
// Returns the index of the last entry with the given term, or ErrNotFound if not found.
// Returns early if it encounters an older term or a compacted log region.
func (lm *logManager) FindLastEntryWithTermUnsafe(ctx context.Context, term types.Term, searchFromHint types.Index) (types.Index, error) {
	if lm.isShutdown.Load() {
		return 0, ErrShuttingDown
	}
	if term == 0 {
		return 0, fmt.Errorf("cannot search for term 0") // Term 0 is reserved for pre-log entries
	}

	firstIdx := lm.GetFirstIndex()
	lastIdx := types.Index(lm.lastIndex.Load())

	startIdx := lastIdx
	if searchFromHint > 0 && searchFromHint < lastIdx {
		startIdx = searchFromHint
	}

	lm.logger.Debugw("Starting FindLastEntryWithTerm", "term", term, "startIndex", startIdx, "firstIndex", firstIdx)

	return lm.scanLogForTermUnsafe(ctx, startIdx, firstIdx, -1, func(t types.Term, idx types.Index) (bool, bool) {
		switch {
		case t == term:
			lm.logger.Debugw("Found last entry for term", "term", term, "index", idx)
			return true, false
		case t < term:
			lm.logger.Debugw("Stopping early due to older term", "term", term, "index", idx, "entryTerm", t)
			return false, true
		default:
			return false, false
		}
	})
}

// FindFirstIndexInTermUnsafe scans backward from searchUpToIndex to find the first index
// with the given term. It assumes entries for the same term appear contiguously.
// The scan stops when a different term is encountered after matches begin.
//
// Returns the lowest index for the term, or 0 with:
//   - ErrNotFound if the term isn't found,
//   - ErrCompacted if the region is unavailable,
//   - ErrShuttingDown if the node is stopping,
//   - context errors (canceled, deadline),
//   - or storage errors.
//
// Caller must ensure thread safety; no locks are acquired.
func (lm *logManager) FindFirstIndexInTermUnsafe(
	ctx context.Context,
	termToFind types.Term,
	searchUpToIndex types.Index,
) (types.Index, error) {
	if lm.isShutdown.Load() {
		return 0, ErrShuttingDown
	}
	if termToFind == 0 {
		return 0, fmt.Errorf("cannot search for term 0 in FindFirstIndexInTermUnsafe")
	}

	firstLogIdx := lm.GetFirstIndex()
	lastLogIdx := lm.GetLastIndexUnsafe()

	scanStartIndex := min(searchUpToIndex, lastLogIdx)

	if scanStartIndex < firstLogIdx || (lastLogIdx == 0 && scanStartIndex == 0 && firstLogIdx > 0) {
		if scanStartIndex < firstLogIdx {
			lm.logger.Debugw("FindFirstIndexInTermUnsafe: scan start index is before first log index",
				"termToFind", termToFind, "searchUpToIndex", searchUpToIndex,
				"scanStartIndex", scanStartIndex, "firstLogIdx", firstLogIdx)
			return 0, ErrNotFound
		}
	}

	if lastLogIdx == 0 {
		return 0, ErrNotFound
	}

	var firstIndexOfTermBlock types.Index = 0
	var scanErr error

	_, scanErr = lm.scanLogForTermUnsafe(ctx, scanStartIndex, firstLogIdx, -1, /* step backward */
		func(currentEntryTerm types.Term, currentEntryIndex types.Index) (signalScanToStop bool, stopEarlyForScan bool) {
			if currentEntryTerm == termToFind {
				firstIndexOfTermBlock = currentEntryIndex
				return false, false
			}

			if firstIndexOfTermBlock != 0 {
				return true, false
			}

			return false, false
		})

	if scanErr != nil {
		if firstIndexOfTermBlock == 0 {
			return 0, scanErr
		}

		isHardOperationalError := !errors.Is(scanErr, ErrNotFound) && !errors.Is(scanErr, ErrCompacted)
		if isHardOperationalError {
			lm.logger.Warnw("Hard error during FindFirstIndexInTermUnsafe after term block was identified",
				"termToFind", termToFind, "firstIndexOfTermBlock", firstIndexOfTermBlock, "scanError", scanErr)
			return 0, scanErr
		}
	}

	if firstIndexOfTermBlock == 0 {
		if scanErr == nil {
			lm.logger.Debugw("FindFirstIndexInTermUnsafe: scan completed without error, but no term index found.",
				"termToFind", termToFind, "searchUpToIndex", searchUpToIndex)
		}
		return 0, ErrNotFound
	}

	return firstIndexOfTermBlock, nil
}

// scanLogForTermUnsafe scans log terms from start to end (inclusive) in the given direction (+1 or -1).
// At each index, it calls the match function, which returns:
//   - matchFound = true to return the current index (match),
//   - stopEarly  = true to stop early without a match.
//
// Returns:
//   - The matching index,
//   - Or 0 and ErrNotFound, ErrCompacted, or other errors.
//
// Caller must ensure thread safety; no locks are acquired.
func (lm *logManager) scanLogForTermUnsafe(
	ctx context.Context,
	start, end types.Index,
	step int,
	match func(term types.Term, idx types.Index) (matchFound bool, stopEarly bool),
) (types.Index, error) {
	for i := start; (step < 0 && i >= end) || (step > 0 && i <= end); i += types.Index(step) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		term, err := lm.GetTermUnsafe(ctx, i)
		if err != nil {
			if errors.Is(err, ErrCompacted) {
				lm.logger.Infow("Scan encountered compacted log segment", "index", i, "start", start, "end", end)
				return 0, ErrCompacted
			}

			lm.logger.Warnw("Error during term scan from GetTermUnsafe", "index", i, "error", err)
			return 0, fmt.Errorf("term scan failed at index %d due to GetTermUnsafe: %w", i, err)
		}

		matchFound, stop := match(term, i)
		if matchFound {
			return i, nil // Match found, return the index where it was found.
		}
		if stop {
			return 0, ErrNotFound
		}
	}

	return 0, ErrNotFound
}

// Stop logs the shutdown signal for the LogManager.
func (lm *logManager) Stop() {
	if lm.isShutdown.Load() {
		lm.logger.Debugw("LogManager Stop() called multiple times or already shut down.")
		return
	}

	lm.logger.Infow("LogManager Stop() invoked; shutdown coordinated via shared flag.")
	lm.isShutdown.Store(true)
}

// updateCachedState updates the in-memory representation of the log’s last index and term.
func (lm *logManager) updateCachedState(index types.Index, term types.Term) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.updateCachedStateLocked(index, term)
}

// updateCachedStateAndGetFirst updates the cached last index/term under lock
// and returns the current first index from storage.
func (lm *logManager) updateCachedStateAndGetFirst(index types.Index, term types.Term) types.Index {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.updateCachedStateLocked(index, term)
	return lm.storage.FirstLogIndex()
}

// updateCachedStateLocked assumes the caller holds lm.mu.
func (lm *logManager) updateCachedStateLocked(index types.Index, term types.Term) {
	lm.lastIndex.Store(uint64(index))
	lm.lastTerm.Store(uint64(term))
}

// fetchEntryTerm fetches the term of the log entry at the given index from storage.
// Uses a context with a predefined internal timeout.
func (lm *logManager) fetchEntryTerm(ctx context.Context, index types.Index) (types.Term, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, logManagerOpTimeout)
	defer cancel()

	entry, err := lm.storage.GetLogEntry(fetchCtx, index)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			lm.logger.Warnw("Timeout fetching entry term internally", "index", index, "timeout", logManagerOpTimeout)
		} else if errors.Is(err, context.Canceled) {
			lm.logger.Infow("Context canceled fetching entry term internally", "index", index)
		} else if errors.Is(err, storage.ErrEntryNotFound) {
			lm.logger.Errorw("Storage reports entry not found during internal fetch", "index", index, "error", err)
		} else {
			lm.logger.Errorw("Failed to fetch entry term internally", "index", index, "error", err)
		}
		return 0, fmt.Errorf("failed to fetch term for index %d: %w", index, err)
	}
	return entry.Term, nil
}
