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
	// It does not acquire locks and is safe for concurrent use due to atomics,
	// but for a consistent view with last term, use GetConsistentLastState.
	GetLastIndexUnsafe() types.Index

	// GetLastTermUnsafe returns the term of the last entry in the log.
	// It does not acquire locks and is safe for concurrent use due to atomics,
	// but for a consistent view with last index, use GetConsistentLastState.
	GetLastTermUnsafe() types.Term

	// GetConsistentLastState returns the last index and term as a synchronized pair,
	// protected by the main Raft mutex.
	GetConsistentLastState() (types.Index, types.Term)

	// GetFirstIndex returns the index of the first available entry in the log.
	GetFirstIndex() types.Index

	// GetTerm returns the term of the log entry at a given index.
	// This method is thread-safe and acquires a read lock.
	GetTerm(ctx context.Context, index types.Index) (types.Term, error)

	// GetTermUnsafe returns the term of a log entry without acquiring locks.
	// The caller is responsible for ensuring thread safety if consistency with other state is required.
	GetTermUnsafe(ctx context.Context, index types.Index) (types.Term, error)

	// GetEntries returns log entries in the range [startIndex, endIndex).
	// This method is thread-safe.
	GetEntries(ctx context.Context, startIndex, endIndex types.Index) ([]types.LogEntry, error)

	// AppendEntries appends entries to the log. It is thread-safe and acquires a write lock.
	AppendEntries(ctx context.Context, entries []types.LogEntry) error

	// AppendEntriesUnsafe appends entries to the log without acquiring locks.
	// The caller MUST hold the write lock to ensure data consistency.
	AppendEntriesUnsafe(ctx context.Context, entries []types.LogEntry) error

	// TruncatePrefix removes log entries before a given index. Safe for concurrent use.
	TruncatePrefix(ctx context.Context, newFirstIndex types.Index) error

	// TruncatePrefixUnsafe removes log entries before a given index without acquiring locks.
	// The caller MUST hold the write lock.
	TruncatePrefixUnsafe(ctx context.Context, newFirstIndex types.Index) error

	// TruncateSuffix removes log entries from a given index to the end. Safe for concurrent use.
	TruncateSuffix(ctx context.Context, newLastIndexPlusOne types.Index) error

	// TruncateSuffixUnsafe removes log entries from a given index without acquiring locks.
	// The caller MUST hold the write lock.
	TruncateSuffixUnsafe(ctx context.Context, newLastIndexPlusOne types.Index) error

	// IsConsistentWithStorage checks if the in-memory log state matches persistent storage.
	IsConsistentWithStorage(ctx context.Context) (bool, error)

	// RebuildInMemoryState reconstructs cached state from persistent storage.
	RebuildInMemoryState(ctx context.Context) error

	// FindLastEntryWithTermUnsafe searches backward for the last entry with a given term.
	// This method does not acquire locks; the caller must ensure thread safety.
	FindLastEntryWithTermUnsafe(ctx context.Context, term types.Term, searchFromHint types.Index) (types.Index, error)

	// FindFirstIndexInTermUnsafe scans backward to find the first entry in a given term.
	// This method does not acquire locks; the caller must ensure thread safety.
	FindFirstIndexInTermUnsafe(ctx context.Context, term types.Term, searchUpToIndex types.Index) (types.Index, error)

	// RestoreFromSnapshot updates the log's state to reflect a newly installed snapshot.
	RestoreFromSnapshot(meta types.SnapshotMetadata)

	// GetLogStateForDebugging returns the current log state for debugging purposes.
	GetLogStateForDebugging() (firstIndex, lastIndex types.Index, lastTerm types.Term)

	// Stop signals the log manager to shut down.
	Stop()
}

// logManager implements the LogManager interface.
// It provides thread-safe access to log operations and maintains cached in-memory state
// (lastIndex, lastTerm) for performance, synchronized with persistent storage.
type logManager struct {
	mu         *sync.RWMutex // Raft's main mutex, protecting state across all managers.
	isShutdown *atomic.Bool  // Shared flag indicating Raft shutdown.

	id types.NodeID // ID of the local Raft node.

	logger  logger.Logger
	metrics Metrics
	storage storage.Storage

	// Atomics for safe, fast, but potentially unsynchronized reads of log boundaries.
	lastIndex atomic.Uint64 // Cached index of the latest log entry.
	lastTerm  atomic.Uint64 // Cached term of the latest log entry.

	// Snapshot metadata, protected by the main mutex.
	snapshotIndex types.Index
	snapshotTerm  types.Term
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
func (lm *logManager) Initialize(ctx context.Context) error {
	lm.logger.Infow("Initializing log manager...", "nodeID", lm.id)

	lastIdx := lm.storage.LastLogIndex()
	firstIdx := lm.storage.FirstLogIndex()

	if lastIdx == 0 {
		lm.updateCachedState(0, 0)
		lm.logger.Infow("Log is empty, initialized with zero index and term.")
		lm.metrics.ObserveLogState(0, 0, 0)
		return nil
	}

	term, err := lm.fetchEntryTerm(ctx, lastIdx)
	if err != nil {
		return fmt.Errorf("inconsistent storage: failed to fetch term for last index %d: %w", lastIdx, err)
	}

	lm.updateCachedState(lastIdx, term)
	lm.metrics.ObserveLogState(firstIdx, lastIdx, term)

	lm.logger.Infow("Log manager initialized successfully.", "firstIndex", firstIdx, "lastIndex", lastIdx, "lastTerm", term)
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
	if lm.isShutdown.Load() {
		return 0, ErrShuttingDown
	}

	lm.mu.RLock()
	term, err := lm.GetTermUnsafe(ctx, index)
	lm.mu.RUnlock()

	lm.metrics.ObserveLogRead(LogReadTypeTerm, time.Since(startTime), err == nil)
	return term, err
}

// GetTermUnsafe returns the term of the log entry at the given index without locking or metrics.
// It assumes the caller holds the read lock.
func (lm *logManager) GetTermUnsafe(ctx context.Context, index types.Index) (types.Term, error) {
	if index == 0 {
		return 0, nil
	}

	if index == lm.snapshotIndex {
		return lm.snapshotTerm, nil
	}

	if term, ok := lm.tryGetCachedTerm(index); ok {
		return term, nil
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

// tryGetCachedTermCommon checks if the given index matches the provided cached index and term,
// and that the term is valid (> 0). If so, returns the term and true.
func tryGetCachedTermCommon(
	index, cachedIndex types.Index,
	cachedTerm types.Term,
) (types.Term, bool) {
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
		return fmt.Errorf(
			"requested index %d is compacted (first available: %d): %w",
			index,
			firstIndex,
			ErrCompacted,
		)
	}
	if index > lastIndex {
		return fmt.Errorf(
			"requested index %d is beyond last index %d: %w",
			index,
			lastIndex,
			ErrNotFound,
		)
	}
	return nil
}

// GetEntries returns log entries in the range [startIndex, endIndex).
func (lm *logManager) GetEntries(
	ctx context.Context,
	startIndex, endIndex types.Index,
) ([]types.LogEntry, error) {
	startTime := time.Now()
	if lm.isShutdown.Load() {
		return nil, ErrShuttingDown
	}
	if startIndex >= endIndex {
		lm.metrics.ObserveLogRead(LogReadTypeEntries, time.Since(startTime), true)
		return nil, nil
	}

	// Clamp the end index to what's available to prevent reading beyond the log.
	effectiveEnd := lm.clampEndIndex(endIndex)
	if startIndex >= effectiveEnd {
		lm.metrics.ObserveLogRead(LogReadTypeEntries, time.Since(startTime), true)
		return nil, nil
	}

	entries, err := lm.storage.GetLogEntries(ctx, startIndex, effectiveEnd)
	if err != nil {
		return nil, lm.mapStorageReadError(err, startIndex, effectiveEnd)
	}

	if err = validateEntryRange(entries, startIndex, effectiveEnd); err != nil {
		lm.logger.Errorw("Storage returned inconsistent log entry range", "error", err, "requestedStart", startIndex, "requestedEnd", effectiveEnd)
		lm.metrics.ObserveLogConsistencyError()
		return nil, fmt.Errorf("internal storage error: %w", err)
	}

	lm.metrics.ObserveLogRead(LogReadTypeEntries, time.Since(startTime), true)
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
			return fmt.Errorf(
				"entries [%d, %d) were compacted concurrently (first available: %d): %w",
				start,
				end,
				newFirst,
				ErrCompacted,
			)
		}
		return fmt.Errorf("entries [%d, %d) not found: %w", start, end, ErrNotFound)
	}
	return fmt.Errorf("failed to retrieve entries [%d, %d): %w", start, end, err)
}

// validateEntryRange checks that the returned log entries match the expected range [start, end).
func validateEntryRange(entries []types.LogEntry, expectedStart, expectedEnd types.Index) error {
	if len(entries) == 0 {
		if expectedStart != expectedEnd {
			return fmt.Errorf(
				"expected entries in range [%d, %d), but got none",
				expectedStart,
				expectedEnd,
			)
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

// AppendEntries is the thread-safe wrapper for AppendEntriesUnsafe.
func (lm *logManager) AppendEntries(ctx context.Context, entries []types.LogEntry) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.AppendEntriesUnsafe(ctx, entries)
}

// AppendEntriesUnsafe appends entries, assuming the caller holds the lock.
func (lm *logManager) AppendEntriesUnsafe(ctx context.Context, entries []types.LogEntry) error {
	startTime := time.Now()
	entryCount := len(entries)
	var err error

	defer func() {
		lm.metrics.ObserveLogAppend(entryCount, time.Since(startTime), err == nil)
	}()

	if lm.isShutdown.Load() {
		return ErrShuttingDown
	}
	if entryCount == 0 {
		return nil
	}

	if err = lm.validateEntriesForAppend(entries); err != nil {
		return fmt.Errorf("invalid entries for append: %w", err)
	}

	currentLastIndex := lm.GetLastIndexUnsafe()
	if entries[0].Index != currentLastIndex+1 {
		err = fmt.Errorf("entries not contiguous: expected first index %d, got %d", currentLastIndex+1, entries[0].Index)
		return err
	}

	if err = lm.storage.AppendLogEntries(ctx, entries); err != nil {
		lm.logger.Errorw("Failed to append entries to storage", "error", err)
		return fmt.Errorf("storage append failed: %w", err)
	}

	lastEntry := entries[len(entries)-1]
	lm.updateCachedState(lastEntry.Index, lastEntry.Term)
	return nil
}

// validateEntriesForAppend checks that entries are valid for appending
func (lm *logManager) validateEntriesForAppend(entries []types.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Check that entries are contiguous
	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("entries not contiguous: entry %d has index %d, expected %d",
				i, entries[i].Index, entries[i-1].Index+1)
		}
	}

	// Check that entries have valid indices (> 0)
	for i, entry := range entries {
		if entry.Index == 0 {
			return fmt.Errorf("entry %d has invalid index 0", i)
		}
		if entry.Term == 0 {
			return fmt.Errorf("entry %d has invalid term 0", i)
		}
	}

	return nil
}

// TruncatePrefix is the thread-safe wrapper for TruncatePrefixUnsafe.
func (lm *logManager) TruncatePrefix(ctx context.Context, newFirstIndex types.Index) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.TruncatePrefixUnsafe(ctx, newFirstIndex)
}

// TruncatePrefixUnsafe truncates the log prefix, assuming the caller holds the lock.
func (lm *logManager) TruncatePrefixUnsafe(ctx context.Context, newFirstIndex types.Index) error {
	startTime := time.Now()
	entriesRemoved := 0
	var err error
	defer func() {
		lm.metrics.ObserveLogTruncate(LogTruncateTypePrefix, entriesRemoved, time.Since(startTime), err == nil)
	}()

	if lm.isShutdown.Load() {
		return ErrShuttingDown
	}
	if newFirstIndex == 0 {
		return fmt.Errorf("invalid truncation index: must be > 0")
	}

	oldFirstIndex := lm.GetFirstIndex()
	if newFirstIndex <= oldFirstIndex {
		return nil // No-op
	}
	lastIndex := lm.GetLastIndexUnsafe()
	if newFirstIndex > lastIndex+1 {
		return fmt.Errorf("invalid truncation index: %d is beyond last index %d", newFirstIndex, lastIndex)
	}

	err = lm.storage.TruncateLogPrefix(ctx, newFirstIndex)
	if err != nil {
		return fmt.Errorf("log prefix truncation to index %d failed: %w", newFirstIndex, err)
	}

	resultingFirstIndex := lm.storage.FirstLogIndex()
	if resultingFirstIndex > oldFirstIndex {
		entriesRemoved = int(resultingFirstIndex - oldFirstIndex)
	}
	return nil
}

// TruncateSuffix is the thread-safe wrapper for TruncateSuffixUnsafe.
func (lm *logManager) TruncateSuffix(ctx context.Context, newLastIndexPlusOne types.Index) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.TruncateSuffixUnsafe(ctx, newLastIndexPlusOne)
}

// TruncateSuffixUnsafe truncates the log suffix, assuming the caller holds the lock.
func (lm *logManager) TruncateSuffixUnsafe(ctx context.Context, newLastIndexPlusOne types.Index) error {
	startTime := time.Now()
	entriesRemoved := 0
	var err error
	defer func() {
		lm.metrics.ObserveLogTruncate(LogTruncateTypeSuffix, entriesRemoved, time.Since(startTime), err == nil)
	}()

	if lm.isShutdown.Load() {
		return ErrShuttingDown
	}

	oldLastIndex := lm.GetLastIndexUnsafe()
	if newLastIndexPlusOne > oldLastIndex {
		return nil // No-op
	}

	if err = lm.storage.TruncateLogSuffix(ctx, newLastIndexPlusOne); err != nil {
		return fmt.Errorf("log suffix truncation from index %d failed: %w", newLastIndexPlusOne, err)
	}

	newLastIndex := lm.storage.LastLogIndex()
	if newLastIndex > oldLastIndex {
		lm.metrics.ObserveLogConsistencyError()
		return fmt.Errorf("inconsistent state: lastIndex increased after suffix truncation (%d > %d)", newLastIndex, oldLastIndex)
	}

	var newLastTerm types.Term
	if newLastIndex > 0 {
		newLastTerm, err = lm.fetchEntryTerm(ctx, newLastIndex)
		if err != nil {
			lm.metrics.ObserveLogConsistencyError()
			_ = lm.RebuildInMemoryState(ctx) // Attempt to recover
			return fmt.Errorf("suffix truncation succeeded but failed to fetch new last term for index %d: %w", newLastIndex, err)
		}
	}

	if newLastIndex < oldLastIndex {
		entriesRemoved = int(oldLastIndex - newLastIndex)
	}

	lm.updateCachedState(newLastIndex, newLastTerm)
	return nil
}

// IsConsistentWithStorage verifies that the cached in-memory log state (last index/term)
// matches the persisted state in storage.
func (lm *logManager) IsConsistentWithStorage(ctx context.Context) (bool, error) {
	memLastIndex := lm.GetLastIndexUnsafe()
	memLastTerm := lm.GetLastTermUnsafe()
	storageLastIndex := lm.storage.LastLogIndex()

	if memLastIndex != storageLastIndex {
		lm.metrics.ObserveLogConsistencyError()
		return false, fmt.Errorf("log index mismatch: memory=%d, storage=%d", memLastIndex, storageLastIndex)
	}
	if storageLastIndex == 0 {
		return memLastTerm == 0, nil
	}
	storageTerm, err := lm.fetchEntryTerm(ctx, storageLastIndex)
	if err != nil {
		return false, fmt.Errorf("unable to verify consistency: %w", err)
	}
	if memLastTerm != storageTerm {
		lm.metrics.ObserveLogConsistencyError()
		return false, fmt.Errorf("log term mismatch at index %d: memory=%d, storage=%d", storageLastIndex, memLastTerm, storageTerm)
	}
	return true, nil
}

// RebuildInMemoryState refreshes the cached lastIndex and lastTerm
// based on the current state of the log in persistent storage.
func (lm *logManager) RebuildInMemoryState(ctx context.Context) error {
	lm.logger.Infow("Rebuilding in-memory log state from storage...")
	lastIndex := lm.storage.LastLogIndex()
	var lastTerm types.Term
	if lastIndex > 0 {
		entryTerm, err := lm.fetchEntryTerm(ctx, lastIndex)
		if err != nil {
			return fmt.Errorf("failed to rebuild state: %w", err)
		}
		lastTerm = entryTerm
	}
	lm.updateCachedState(lastIndex, lastTerm)
	lm.logger.Infow("In-memory log state rebuilt.", "lastIndex", lastIndex, "lastTerm", lastTerm)
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
		return 0, fmt.Errorf("cannot search for term 0")
	}

	firstIdx := lm.GetFirstIndex()
	lastIdx := lm.GetLastIndexUnsafe()
	startIdx := lastIdx
	if searchFromHint > 0 && searchFromHint < lastIdx {
		startIdx = searchFromHint
	}

	return lm.scanLogForTermUnsafe(ctx, startIdx, firstIdx, -1, func(t types.Term, idx types.Index) (bool, bool) {
		if t == term {
			return true, false
		}
		return false, t < term
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
func (lm *logManager) FindFirstIndexInTermUnsafe(ctx context.Context, termToFind types.Term, searchUpToIndex types.Index) (types.Index, error) {
	if lm.isShutdown.Load() {
		return 0, ErrShuttingDown
	}
	if termToFind == 0 {
		return 0, fmt.Errorf("cannot search for term 0")
	}

	firstLogIdx := lm.GetFirstIndex()
	lastLogIdx := lm.GetLastIndexUnsafe()
	scanStartIndex := min(searchUpToIndex, lastLogIdx)

	if scanStartIndex < firstLogIdx {
		return 0, ErrNotFound
	}
	if lastLogIdx == 0 {
		return 0, ErrNotFound
	}

	var firstIndexOfTermBlock types.Index
	_, err := lm.scanLogForTermUnsafe(ctx, scanStartIndex, firstLogIdx, -1, func(currentTerm types.Term, currentIndex types.Index) (bool, bool) {
		if currentTerm == termToFind {
			firstIndexOfTermBlock = currentIndex
			return false, false
		}
		return firstIndexOfTermBlock != 0, false
	})

	if firstIndexOfTermBlock == 0 {
		return 0, ErrNotFound
	}
	if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrCompacted) {
		return 0, err
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
				lm.logger.Infow(
					"Scan encountered compacted log segment",
					"index",
					i,
					"start",
					start,
					"end",
					end,
				)
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

func (lm *logManager) GetLogStateForDebugging() (firstIndex, lastIndex types.Index, lastTerm types.Term) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	firstIndex = lm.storage.FirstLogIndex()
	lastIndex = types.Index(lm.lastIndex.Load())
	lastTerm = types.Term(lm.lastTerm.Load())

	return firstIndex, lastIndex, lastTerm
}

func (lm *logManager) RestoreFromSnapshot(meta types.SnapshotMetadata) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.snapshotIndex = meta.LastIncludedIndex
	lm.snapshotTerm = meta.LastIncludedTerm

	// The log has been truncated by the snapshot manager, so we must update our state.
	lm.updateCachedState(meta.LastIncludedIndex, meta.LastIncludedTerm)
	lm.TruncatePrefixUnsafe(context.Background(), meta.LastIncludedIndex+1)
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

// updateCachedStateAndGetFirstLocked updates the cached last index/term under lock
// and returns the current first index from storage.
// The caller MUST hold the lm.mu write lock.
func (lm *logManager) updateCachedStateAndGetFirstLocked(index types.Index, term types.Term) types.Index {
	lm.updateCachedStateLocked(index, term)
	return lm.storage.FirstLogIndex()
}

// updateCachedStateLocked assumes the caller holds lm.mu.
func (lm *logManager) updateCachedStateLocked(index types.Index, term types.Term) {
	oldIndex := lm.lastIndex.Load()
	oldTerm := lm.lastTerm.Load()

	lm.lastIndex.Store(uint64(index))
	lm.lastTerm.Store(uint64(term))

	lm.logger.Debugw("Updated cached log state",
		"oldIndex", oldIndex,
		"oldTerm", oldTerm,
		"newIndex", index,
		"newTerm", term)
}

// fetchEntryTerm fetches the term of the log entry at the given index from storage.
// Uses a context with a predefined internal timeout.
func (lm *logManager) fetchEntryTerm(ctx context.Context, index types.Index) (types.Term, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, logManagerOpTimeout)
	defer cancel()

	entry, err := lm.storage.GetLogEntry(fetchCtx, index)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			lm.logger.Warnw(
				"Timeout fetching entry term internally",
				"index",
				index,
				"timeout",
				logManagerOpTimeout,
			)
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
