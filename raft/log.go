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

// LogDebugState contains comprehensive log state information for debugging and monitoring.
type LogDebugState struct {
	NodeID          types.NodeID      `json:"node_id"`
	FirstIndex      types.Index       `json:"first_index"`
	LastIndex       types.Index       `json:"last_index"`
	LastTerm        types.Term        `json:"last_term"`
	CachedLastIndex types.Index       `json:"cached_last_index"`
	CachedLastTerm  types.Term        `json:"cached_last_term"`
	LogSize         int               `json:"log_size"` // Number of entries between first and last
	IsEmpty         bool              `json:"is_empty"`
	IsConsistent    bool              `json:"is_consistent"` // Whether cache matches storage
	StorageMetrics  map[string]uint64 `json:"storage_metrics,omitempty"`
	LastChecked     time.Time         `json:"last_checked"`
}

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

	// GetFirstIndexUnsafe returns the index of the first available entry in the log without locking.
	// Returns 0 if the log is empty.
	// Callers must ensure thread safety by acquiring the appropriate Raft read lock.
	GetFirstIndexUnsafe() types.Index

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

	// GetEntriesUnsafe returns log entries in the range [startIndex, endIndex) without locking.
	// Returns an empty slice if the range is empty or invalid.
	// Returns an error if the range includes compacted entries (ErrCompacted) or is out of bounds.
	// Callers must ensure thread safety.
	GetEntriesUnsafe(ctx context.Context, startIndex, endIndex types.Index) ([]types.LogEntry, error)

	// AppendEntries appends the given entries to the log.
	// The entries must be contiguous with the existing log.
	// Returns an error if the entries are not contiguous, if storage operations fail,
	// or if the node is shutting down (ErrShuttingDown).
	AppendEntries(ctx context.Context, entries []types.LogEntry) error

	// AppendEntriesUnsafe appends the given entries to the log without acquiring locks.
	// The entries must be contiguous with the existing log.
	// Returns an error if the entries are not contiguous or if storage operations fail.
	// Callers must ensure thread safety.
	AppendEntriesUnsafe(ctx context.Context, entries []types.LogEntry) error

	// TruncatePrefix removes log entries before the given index (exclusive).
	// Used for log compaction after snapshots.
	// Returns an error if the index is invalid, truncation fails,
	// or if the node is shutting down (ErrShuttingDown).
	TruncatePrefix(ctx context.Context, newFirstIndex types.Index) error

	// TruncatePrefixUnsafe removes log entries before the given index (exclusive) without locking.
	// Used for log compaction after snapshots.
	// Returns an error if the index is invalid or truncation fails.
	// Callers must ensure thread safety.
	TruncatePrefixUnsafe(ctx context.Context, newFirstIndex types.Index) error

	// TruncateSuffix removes log entries at or after the given index.
	// Used during conflict resolution when appending entries.
	// Returns an error if the index is invalid, truncation fails,
	// or if the node is shutting down (ErrShuttingDown).
	TruncateSuffix(ctx context.Context, newLastIndexPlusOne types.Index) error

	// TruncateSuffixUnsafe removes log entries at or after the given index without locking.
	// Used during conflict resolution when appending entries.
	// Returns an error if the index is invalid or truncation fails.
	// Callers must ensure thread safety.
	TruncateSuffixUnsafe(ctx context.Context, newLastIndexPlusOne types.Index) error

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
	FindLastEntryWithTermUnsafe(
		ctx context.Context,
		term types.Term,
		searchFromHint types.Index,
	) (types.Index, error)

	// FindFirstIndexInTermUnsafe scans backward from searchUpToIndex to find the first log index
	// that belongs to the given term. It assumes that log entries for a term are contiguous.
	// The scan stops at the first entry with a different term.
	//
	// Returns the index of the first entry in the term if found, or 0 and an error
	// (e.g., ErrNotFound or ErrCompacted) if the term is not present or is in a compacted region.
	//
	// This method does not perform locking. The caller must ensure thread safety.
	FindFirstIndexInTermUnsafe(
		ctx context.Context,
		term types.Term,
		searchUpToIndex types.Index,
	) (types.Index, error)

	// Stop signals the log manager to shut down and release resources.
	// Note: Actual shutdown logic (stopping operations) relies on checking the
	// shared isShutdown flag.
	Stop()

	// GetLogStateForDebugging returns comprehensive log state information for debugging and monitoring.
	// This method is safe for concurrent use and provides a consistent snapshot of the log state.
	GetLogStateForDebugging() LogDebugState

	// RestoreFromSnapshot updates the log manager's state after a snapshot has been installed.
	// This should be called by the snapshot manager after successfully installing a snapshot
	// and truncating the log. It updates cached state and ensures consistency.
	RestoreFromSnapshot(ctx context.Context, meta types.SnapshotMetadata) error

	// RestoreFromSnapshotUnsafe updates the log manager's state after a snapshot has been installed
	// without acquiring locks. This should be called by the snapshot manager when it already holds
	// the appropriate locks. It updates cached state and ensures consistency.
	RestoreFromSnapshotUnsafe(ctx context.Context, meta types.SnapshotMetadata) error
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

	lm.logger.Debugw("Storage state during initialization",
		"firstIndex", firstIdx,
		"lastIndex", lastIdx,
		"nodeID", lm.id)

	if lastIdx == 0 {
		lm.updateCachedState(0, 0)
		lm.logger.Infow("Log is empty, initialized with zero index and term", "nodeID", lm.id)
		lm.metrics.ObserveLogState(0, 0, 0)
		return nil
	}

	term, err := lm.fetchEntryTerm(ctx, lastIdx)
	if err != nil {
		lm.logger.Errorw(
			"Failed to get term for last log index during initialization",
			"lastIndex",
			lastIdx,
			"error",
			err,
			"nodeID",
			lm.id,
		)
		return fmt.Errorf(
			"inconsistent storage: failed to fetch term for last index %d: %w",
			lastIdx,
			err,
		)
	}

	lm.updateCachedState(lastIdx, term)
	lm.metrics.ObserveLogState(firstIdx, lastIdx, term)

	lm.logger.Infow("Log manager initialized successfully",
		"firstIndex", firstIdx,
		"lastIndex", lastIdx,
		"lastTerm", term,
		"nodeID", lm.id)

	return nil
}

// GetLastIndexUnsafe returns the cached last index.
// Assumes lm.mu.RLock() is held
func (lm *logManager) GetLastIndexUnsafe() types.Index {
	lastIdx := types.Index(lm.lastIndex.Load())
	lm.logger.Debugw("GetLastIndexUnsafe called",
		"lastIndex", lastIdx,
		"nodeID", lm.id)
	return lastIdx
}

// GetLastTermUnsafe returns the cached last term.
// Assumes lm.mu.RLock() is held
func (lm *logManager) GetLastTermUnsafe() types.Term {
	lastTerm := types.Term(lm.lastTerm.Load())
	lm.logger.Debugw("GetLastTermUnsafe called",
		"lastTerm", lastTerm,
		"nodeID", lm.id)
	return lastTerm
}

// GetConsistentLastState atomically returns the cached last index and term,
// protected by the Raft mutex to ensure consistency relative to concurrent writes
// (like appends or truncations) that update both index and term.
func (lm *logManager) GetConsistentLastState() (types.Index, types.Term) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lastIdx := lm.lastIndex.Load()
	lastTerm := lm.lastTerm.Load()

	lm.logger.Debugw("GetConsistentLastState called",
		"lastIndex", lastIdx,
		"lastTerm", lastTerm,
		"nodeID", lm.id)

	return types.Index(lastIdx), types.Term(lastTerm)
}

// GetFirstIndex returns the current first log index from storage.
func (lm *logManager) GetFirstIndex() types.Index {
	// Always fetch dynamically from storage, as this changes independently of cached state.
	firstIdx := lm.storage.FirstLogIndex()
	lm.logger.Debugw("GetFirstIndex called",
		"firstIndex", firstIdx,
		"nodeID", lm.id)
	return firstIdx
}

// GetFirstIndexUnsafe returns the current first log index from storage without locking.
func (lm *logManager) GetFirstIndexUnsafe() types.Index {
	firstIdx := lm.storage.FirstLogIndex()
	lm.logger.Debugw("GetFirstIndexUnsafe called",
		"firstIndex", firstIdx,
		"nodeID", lm.id)
	return firstIdx
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
		lm.logger.Debugw("GetTerm called but node is shutting down",
			"index", index,
			"nodeID", lm.id)
		return 0, ErrShuttingDown
	}

	lm.logger.Debugw("GetTerm called",
		"index", index,
		"nodeID", lm.id)

	term, err := lm.getTermInternal(ctx, index, false)
	if err == nil {
		success = true
		lm.logger.Debugw("GetTerm completed successfully",
			"index", index,
			"term", term,
			"nodeID", lm.id)
	} else {
		lm.logger.Debugw("GetTerm failed",
			"index", index,
			"error", err,
			"nodeID", lm.id)
	}
	return term, err
}

// GetTermUnsafe returns the term of the log entry at the given index without locking or metrics.
// The caller must ensure thread safety.
func (lm *logManager) GetTermUnsafe(ctx context.Context, index types.Index) (types.Term, error) {
	lm.logger.Debugw("GetTermUnsafe called",
		"index", index,
		"nodeID", lm.id)

	term, err := lm.getTermInternal(ctx, index, true)
	if err == nil {
		lm.logger.Debugw("GetTermUnsafe completed successfully",
			"index", index,
			"term", term,
			"nodeID", lm.id)
	} else {
		lm.logger.Debugw("GetTermUnsafe failed",
			"index", index,
			"error", err,
			"nodeID", lm.id)
	}
	return term, err
}

// getTermInternal returns the term of the log entry at the specified index.
// It first attempts to serve the request from a cached last index/term if applicable.
// If not cached, it verifies the index is within valid bounds and fetches the term from storage.
// If useUnlockedCache is true, it assumes the caller already holds lm.mu and accesses cache without locks.
func (lm *logManager) getTermInternal(
	ctx context.Context,
	index types.Index,
	useUnlockedCache bool,
) (types.Term, error) {
	if index == 0 {
		lm.logger.Debugw("getTermInternal: index 0 requested, returning term 0",
			"nodeID", lm.id)
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
		lm.logger.Debugw("getTermInternal: served from cache",
			"index", index,
			"cachedTerm", cachedTerm,
			"nodeID", lm.id)
		return cachedTerm, nil
	}

	if err := lm.checkLogBounds(index, useUnlockedCache); err != nil {
		lm.logger.Debugw("getTermInternal: log bounds check failed",
			"index", index,
			"error", err,
			"nodeID", lm.id)
		return 0, err
	}

	lm.logger.Debugw("getTermInternal: fetching from storage",
		"index", index,
		"nodeID", lm.id)

	entry, err := lm.storage.GetLogEntry(ctx, index)
	if err != nil {
		if errors.Is(err, storage.ErrEntryNotFound) {
			firstIndex := lm.getFirstIndexForBounds(useUnlockedCache)
			if index < firstIndex {
				lm.logger.Debugw("getTermInternal: index was compacted",
					"index", index,
					"firstIndex", firstIndex,
					"nodeID", lm.id)
				return 0, fmt.Errorf("requested index %d was compacted: %w", index, ErrCompacted)
			}
			lm.logger.Debugw("getTermInternal: index not found",
				"index", index,
				"nodeID", lm.id)
			return 0, fmt.Errorf("requested index %d was not found: %w", index, ErrNotFound)
		}
		lm.logger.Warnw("getTermInternal: storage error",
			"index", index,
			"error", err,
			"nodeID", lm.id)
		return 0, fmt.Errorf("failed to get log entry %d from storage: %w", index, err)
	}

	lm.logger.Debugw("getTermInternal: fetched from storage successfully",
		"index", index,
		"term", entry.Term,
		"nodeID", lm.id)
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
func (lm *logManager) checkLogBounds(index types.Index, useUnlockedCache bool) error {
	var firstIndex, lastIndex types.Index

	if useUnlockedCache {
		firstIndex = lm.GetFirstIndexUnsafe()
		lastIndex = lm.GetLastIndexUnsafe()
	} else {
		firstIndex = lm.GetFirstIndex()
		lastIndex = lm.GetLastIndexUnsafe() // This is safe to call without locks
	}

	lm.logger.Debugw("checkLogBounds called",
		"index", index,
		"firstIndex", firstIndex,
		"lastIndex", lastIndex,
		"nodeID", lm.id)

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

// getFirstIndexForBounds returns the first index, using the appropriate method based on locking context.
func (lm *logManager) getFirstIndexForBounds(useUnlockedCache bool) types.Index {
	if useUnlockedCache {
		return lm.GetFirstIndexUnsafe()
	}
	return lm.GetFirstIndex()
}

// GetEntries returns log entries in the range [startIndex, endIndex).
func (lm *logManager) GetEntries(
	ctx context.Context,
	startIndex, endIndex types.Index,
) ([]types.LogEntry, error) {
	startTime := time.Now()
	var entries []types.LogEntry
	var err error
	success := false

	defer func() {
		lm.metrics.ObserveLogRead(LogReadTypeEntries, time.Since(startTime), success)
	}()

	if lm.isShutdown.Load() {
		lm.logger.Debugw("GetEntries called but node is shutting down",
			"startIndex", startIndex,
			"endIndex", endIndex,
			"nodeID", lm.id)
		return nil, ErrShuttingDown
	}

	lm.logger.Debugw("GetEntries called",
		"startIndex", startIndex,
		"endIndex", endIndex,
		"nodeID", lm.id)

	entries, err = lm.getEntriesInternal(ctx, startIndex, endIndex, false)
	if err == nil {
		success = true
		lm.logger.Debugw("GetEntries completed successfully",
			"startIndex", startIndex,
			"endIndex", endIndex,
			"entriesCount", len(entries),
			"nodeID", lm.id)
	} else {
		lm.logger.Debugw("GetEntries failed",
			"startIndex", startIndex,
			"endIndex", endIndex,
			"error", err,
			"nodeID", lm.id)
	}
	return entries, err
}

// GetEntriesUnsafe returns log entries in the range [startIndex, endIndex) without locking.
func (lm *logManager) GetEntriesUnsafe(
	ctx context.Context,
	startIndex, endIndex types.Index,
) ([]types.LogEntry, error) {
	lm.logger.Debugw("GetEntriesUnsafe called",
		"startIndex", startIndex,
		"endIndex", endIndex,
		"nodeID", lm.id)

	entries, err := lm.getEntriesInternal(ctx, startIndex, endIndex, true)
	if err == nil {
		lm.logger.Debugw("GetEntriesUnsafe completed successfully",
			"startIndex", startIndex,
			"endIndex", endIndex,
			"entriesCount", len(entries),
			"nodeID", lm.id)
	} else {
		lm.logger.Debugw("GetEntriesUnsafe failed",
			"startIndex", startIndex,
			"endIndex", endIndex,
			"error", err,
			"nodeID", lm.id)
	}
	return entries, err
}

// getEntriesInternal contains the core logic for retrieving log entries.
func (lm *logManager) getEntriesInternal(
	ctx context.Context,
	startIndex, endIndex types.Index,
	useUnlockedCache bool,
) ([]types.LogEntry, error) {
	if isEmptyRange(startIndex, endIndex) {
		lm.logger.Debugw("getEntriesInternal: empty range requested",
			"startIndex", startIndex,
			"endIndex", endIndex,
			"nodeID", lm.id)
		return nil, nil
	}

	effectiveEnd := lm.clampEndIndex(endIndex, useUnlockedCache)
	if isEmptyRange(startIndex, effectiveEnd) {
		lm.logger.Debugw("getEntriesInternal: empty range after clamping",
			"startIndex", startIndex,
			"originalEndIndex", endIndex,
			"effectiveEndIndex", effectiveEnd,
			"nodeID", lm.id)
		return nil, nil
	}

	lm.logger.Debugw("getEntriesInternal: fetching from storage",
		"startIndex", startIndex,
		"effectiveEndIndex", effectiveEnd,
		"nodeID", lm.id)

	entries, err := lm.storage.GetLogEntries(ctx, startIndex, effectiveEnd)
	if err != nil {
		mappedErr := lm.mapStorageReadError(err, startIndex, effectiveEnd, useUnlockedCache)
		lm.logger.Debugw("getEntriesInternal: storage error",
			"startIndex", startIndex,
			"effectiveEndIndex", effectiveEnd,
			"error", err,
			"mappedError", mappedErr,
			"nodeID", lm.id)
		return nil, mappedErr
	}

	if err = validateEntryRange(entries, startIndex, effectiveEnd); err != nil {
		lm.logger.Errorw(
			"Storage returned inconsistent log entry range",
			"error",
			err,
			"requestedStart",
			startIndex,
			"requestedEnd",
			effectiveEnd,
			"nodeID",
			lm.id,
		)
		lm.metrics.ObserveLogConsistencyError()
		return nil, fmt.Errorf("internal storage error: %w", err)
	}

	lm.logger.Debugw("getEntriesInternal: successfully retrieved entries",
		"startIndex", startIndex,
		"effectiveEndIndex", effectiveEnd,
		"entriesCount", len(entries),
		"nodeID", lm.id)

	return entries, nil
}

// isEmptyRange returns true if the given log range [start, end) is empty or invalid.
func isEmptyRange(start, end types.Index) bool {
	return start >= end
}

// clampEndIndex ensures the requested end index does not exceed the log's actual end index + 1.
func (lm *logManager) clampEndIndex(requestedEnd types.Index, useUnlockedCache bool) types.Index {
	var last types.Index
	if useUnlockedCache {
		last = lm.GetLastIndexUnsafe()
	} else {
		// For clamping, we can safely use the unsafe version since we only read the atomic value
		last = lm.GetLastIndexUnsafe()
	}

	actualEnd := last + 1
	if requestedEnd > actualEnd {
		lm.logger.Debugw("clampEndIndex: clamping end index",
			"requestedEnd", requestedEnd,
			"actualEnd", actualEnd,
			"lastIndex", last,
			"nodeID", lm.id)
		return actualEnd
	}
	return requestedEnd
}

// mapStorageReadError interprets errors from storage.GetLogEntries, mapping them to
// Raft-level errors like ErrCompacted or ErrNotFound, and checking for concurrent compaction.
func (lm *logManager) mapStorageReadError(err error, start, end types.Index, useUnlockedCache bool) error {
	if errors.Is(err, storage.ErrIndexOutOfRange) {
		newFirst := lm.getFirstIndexForBounds(useUnlockedCache)
		if start < newFirst {
			lm.logger.Debugw("mapStorageReadError: entries were compacted concurrently",
				"start", start,
				"end", end,
				"newFirstIndex", newFirst,
				"nodeID", lm.id)
			return fmt.Errorf(
				"entries [%d, %d) were compacted concurrently (first available: %d): %w",
				start,
				end,
				newFirst,
				ErrCompacted,
			)
		}
		lm.logger.Debugw("mapStorageReadError: entries not found",
			"start", start,
			"end", end,
			"nodeID", lm.id)
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

// AppendEntries appends a batch of log entries to storage and updates cached state.
func (lm *logManager) AppendEntries(ctx context.Context, entries []types.LogEntry) error {
	if lm.isShutdown.Load() {
		lm.logger.Debugw("AppendEntries called but node is shutting down",
			"entriesCount", len(entries),
			"nodeID", lm.id)
		return ErrShuttingDown
	}

	lm.logger.Debugw("AppendEntries called",
		"entriesCount", len(entries),
		"nodeID", lm.id)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.appendEntriesInternal(ctx, entries)
}

// AppendEntriesUnsafe appends a batch of log entries to storage and updates cached state without locking.
func (lm *logManager) AppendEntriesUnsafe(ctx context.Context, entries []types.LogEntry) error {
	lm.logger.Debugw("AppendEntriesUnsafe called",
		"entriesCount", len(entries),
		"nodeID", lm.id)

	return lm.appendEntriesInternal(ctx, entries)
}

// appendEntriesInternal contains the core logic for appending entries.
func (lm *logManager) appendEntriesInternal(ctx context.Context, entries []types.LogEntry) error {
	startTime := time.Now()
	entryCount := len(entries)
	success := false
	var err error

	defer func() {
		lm.metrics.ObserveLogAppend(entryCount, time.Since(startTime), success)
	}()

	if entryCount == 0 {
		success = true
		lm.logger.Debugw("appendEntriesInternal: no entries to append", "nodeID", lm.id)
		return nil
	}

	firstEntry := entries[0]
	lastEntry := entries[entryCount-1]
	lm.logger.Debugw("appendEntriesInternal: appending entries",
		"firstIndex", firstEntry.Index,
		"firstTerm", firstEntry.Term,
		"lastIndex", lastEntry.Index,
		"lastTerm", lastEntry.Term,
		"count", entryCount,
		"nodeID", lm.id)

	err = lm.storage.AppendLogEntries(ctx, entries)
	if err != nil {
		lm.logger.Errorw(
			"Storage rejected AppendLogEntries",
			"error",
			err,
			"entryCount",
			entryCount,
			"firstIndex",
			firstEntry.Index,
			"nodeID",
			lm.id,
		)
		return fmt.Errorf("log append failed: %w", err)
	}

	firstIndexInStorage := lm.syncLogStateFromAppendLocked(lastEntry)

	lm.metrics.ObserveLogState(firstIndexInStorage, lastEntry.Index, lastEntry.Term)

	lm.logger.Debugw("appendEntriesInternal: entries appended successfully",
		"count", entryCount,
		"firstAppendedIndex", firstEntry.Index,
		"lastAppendedIndex", lastEntry.Index,
		"lastAppendedTerm", lastEntry.Term,
		"latencyMs", time.Since(startTime).Milliseconds(),
		"nodeID", lm.id)

	success = true
	return nil
}

// syncLogStateFromAppendLocked updates the cached last index and term after a successful append.
// Requires holding the lm.mu write lock.
// Returns the current first index from storage.
func (lm *logManager) syncLogStateFromAppendLocked(lastAppendedEntry types.LogEntry) types.Index {
	lm.lastIndex.Store(uint64(lastAppendedEntry.Index))
	lm.lastTerm.Store(uint64(lastAppendedEntry.Term))

	firstIndex := lm.storage.FirstLogIndex()
	lm.logger.Debugw("syncLogStateFromAppendLocked: updated cached state",
		"newLastIndex", lastAppendedEntry.Index,
		"newLastTerm", lastAppendedEntry.Term,
		"firstIndex", firstIndex,
		"nodeID", lm.id)

	return firstIndex
}

// TruncatePrefix removes all log entries before newFirstIndex (exclusive).
func (lm *logManager) TruncatePrefix(ctx context.Context, newFirstIndex types.Index) error {
	if lm.isShutdown.Load() {
		lm.logger.Debugw("TruncatePrefix called but node is shutting down",
			"newFirstIndex", newFirstIndex,
			"nodeID", lm.id)
		return ErrShuttingDown
	}

	lm.logger.Debugw("TruncatePrefix called",
		"newFirstIndex", newFirstIndex,
		"nodeID", lm.id)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.truncatePrefixInternal(ctx, newFirstIndex)
}

// TruncatePrefixUnsafe removes all log entries before newFirstIndex (exclusive) without locking.
func (lm *logManager) TruncatePrefixUnsafe(ctx context.Context, newFirstIndex types.Index) error {
	lm.logger.Debugw("TruncatePrefixUnsafe called",
		"newFirstIndex", newFirstIndex,
		"nodeID", lm.id)

	return lm.truncatePrefixInternal(ctx, newFirstIndex)
}

// truncatePrefixInternal contains the core logic for prefix truncation.
func (lm *logManager) truncatePrefixInternal(ctx context.Context, newFirstIndex types.Index) error {
	startTime := time.Now()
	entriesRemoved := 0
	success := false
	var err error

	defer func() {
		lm.metrics.ObserveLogTruncate(
			LogTruncateTypePrefix,
			entriesRemoved,
			time.Since(startTime),
			success,
		)
	}()

	if newFirstIndex == 0 {
		err = fmt.Errorf("invalid truncation index: must be > 0")
		lm.logger.Errorw("truncatePrefixInternal: invalid index",
			"newFirstIndex", newFirstIndex,
			"nodeID", lm.id)
		return err
	}

	oldFirstIndex := lm.storage.FirstLogIndex()

	lm.logger.Debugw("truncatePrefixInternal: checking bounds",
		"newFirstIndex", newFirstIndex,
		"oldFirstIndex", oldFirstIndex,
		"nodeID", lm.id)

	if newFirstIndex <= oldFirstIndex {
		lm.logger.Debugw(
			"TruncatePrefix skipped: index less than or equal to current first index",
			"newFirstIndex",
			newFirstIndex,
			"oldFirstIndex",
			oldFirstIndex,
			"nodeID",
			lm.id,
		)
		success = true
		return nil
	}

	lastIndex := lm.GetLastIndexUnsafe()
	if newFirstIndex > lastIndex+1 {
		err = fmt.Errorf(
			"invalid truncation index: %d is beyond last index %d",
			newFirstIndex,
			lastIndex,
		)
		lm.logger.Errorw("truncatePrefixInternal: index beyond last",
			"newFirstIndex", newFirstIndex,
			"lastIndex", lastIndex,
			"nodeID", lm.id)
		return err
	}

	lm.logger.Infow("truncatePrefixInternal: performing truncation",
		"newFirstIndex", newFirstIndex,
		"oldFirstIndex", oldFirstIndex,
		"lastIndex", lastIndex,
		"nodeID", lm.id)

	err = lm.storage.TruncateLogPrefix(ctx, newFirstIndex)
	if err != nil {
		lm.logger.Errorw(
			"Failed to truncate log prefix in storage",
			"newFirstIndex",
			newFirstIndex,
			"error",
			err,
			"nodeID",
			lm.id,
		)
		return fmt.Errorf("log prefix truncation to index %d failed: %w", newFirstIndex, err)
	}

	resultingFirstIndex := lm.storage.FirstLogIndex()
	entriesRemoved = int(resultingFirstIndex - oldFirstIndex)

	if entriesRemoved < 0 {
		lm.logger.Warnw(
			"First index decreased after TruncatePrefix, expected increase or no change",
			"oldFirstIndex",
			oldFirstIndex,
			"newFirstIndex",
			resultingFirstIndex,
			"nodeID",
			lm.id,
		)
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
		"latencyMs", time.Since(startTime).Milliseconds(),
		"nodeID", lm.id)

	success = true
	return nil
}

// TruncateSuffix removes log entries at or after newLastIndexPlusOne.
func (lm *logManager) TruncateSuffix(ctx context.Context, newLastIndexPlusOne types.Index) error {
	if lm.isShutdown.Load() {
		lm.logger.Debugw("TruncateSuffix called but node is shutting down",
			"newLastIndexPlusOne", newLastIndexPlusOne,
			"nodeID", lm.id)
		return ErrShuttingDown
	}

	lm.logger.Debugw("TruncateSuffix called",
		"newLastIndexPlusOne", newLastIndexPlusOne,
		"nodeID", lm.id)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.truncateSuffixInternal(ctx, newLastIndexPlusOne)
}

// TruncateSuffixUnsafe removes log entries at or after newLastIndexPlusOne without locking.
func (lm *logManager) TruncateSuffixUnsafe(ctx context.Context, newLastIndexPlusOne types.Index) error {
	lm.logger.Debugw("TruncateSuffixUnsafe called",
		"newLastIndexPlusOne", newLastIndexPlusOne,
		"nodeID", lm.id)

	return lm.truncateSuffixInternal(ctx, newLastIndexPlusOne)
}

// truncateSuffixInternal contains the core logic for suffix truncation.
func (lm *logManager) truncateSuffixInternal(ctx context.Context, newLastIndexPlusOne types.Index) error {
	startTime := time.Now()
	entriesRemoved := 0
	success := false
	var err error

	defer func() {
		lm.metrics.ObserveLogTruncate(
			LogTruncateTypeSuffix,
			entriesRemoved,
			time.Since(startTime),
			success,
		)
	}()

	oldLastIndex := lm.GetLastIndexUnsafe()

	lm.logger.Debugw("truncateSuffixInternal: checking bounds",
		"newLastIndexPlusOne", newLastIndexPlusOne,
		"oldLastIndex", oldLastIndex,
		"nodeID", lm.id)

	if newLastIndexPlusOne > oldLastIndex {
		lm.logger.Debugw("TruncateSuffix skipped: index is beyond last log index",
			"index", newLastIndexPlusOne,
			"lastIndex", oldLastIndex,
			"nodeID", lm.id)
		success = true
		return nil
	}

	lm.logger.Infow("truncateSuffixInternal: performing truncation",
		"newLastIndexPlusOne", newLastIndexPlusOne,
		"oldLastIndex", oldLastIndex,
		"nodeID", lm.id)

	if err := lm.storage.TruncateLogSuffix(ctx, newLastIndexPlusOne); err != nil {
		lm.logger.Errorw("Failed to truncate log suffix in storage",
			"newLastIndexPlusOne", newLastIndexPlusOne,
			"error", err,
			"nodeID", lm.id)
		return fmt.Errorf(
			"log suffix truncation from index %d failed: %w",
			newLastIndexPlusOne,
			err,
		)
	}

	newLastIndex := lm.storage.LastLogIndex()

	if newLastIndex > oldLastIndex {
		lm.logger.Errorw("Inconsistent state: lastIndex increased after suffix truncation",
			"oldLast", oldLastIndex,
			"newLast", newLastIndex,
			"nodeID", lm.id)
		lm.metrics.ObserveLogConsistencyError()
		return fmt.Errorf(
			"suffix truncation led to inconsistent state: newLastIndex %d > oldLastIndex %d",
			newLastIndex,
			oldLastIndex,
		)
	}

	var newLastTerm types.Term
	if newLastIndex > 0 {
		newLastTerm, err = lm.fetchEntryTerm(ctx, newLastIndex)
		if err != nil {
			return fmt.Errorf("failed to fetch term for new last index %d after suffix truncation: %w", newLastIndex, err)
		}
	}

	if newLastIndex < oldLastIndex {
		entriesRemoved = int(oldLastIndex - newLastIndex)
	}

	firstIndex := lm.updateCachedStateAndGetFirstLocked(newLastIndex, newLastTerm)
	lm.metrics.ObserveLogState(firstIndex, newLastIndex, newLastTerm)

	lm.logger.Infow("Log suffix truncated successfully",
		"requestedIndexPlusOne", newLastIndexPlusOne,
		"oldLastIndex", oldLastIndex,
		"newLastIndex", newLastIndex,
		"newLastTerm", newLastTerm,
		"entriesRemoved", entriesRemoved,
		"latencyMs", time.Since(startTime).Milliseconds(),
		"nodeID", lm.id)

	success = true
	return nil
}

// IsConsistentWithStorage verifies that the cached in-memory log state (last index/term)
// matches the persisted state in storage.
func (lm *logManager) IsConsistentWithStorage(ctx context.Context) (bool, error) {
	memLastIndex := lm.GetLastIndexUnsafe()
	memLastTerm := lm.GetLastTermUnsafe()

	storageLastIndex := lm.storage.LastLogIndex()

	lm.logger.Debugw("IsConsistentWithStorage: checking consistency",
		"memoryLastIndex", memLastIndex,
		"memoryLastTerm", memLastTerm,
		"storageLastIndex", storageLastIndex,
		"nodeID", lm.id)

	if memLastIndex != storageLastIndex {
		lm.logger.Errorw(
			"Log index mismatch detected",
			"memoryIndex",
			memLastIndex,
			"storageIndex",
			storageLastIndex,
			"nodeID",
			lm.id,
		)
		lm.metrics.ObserveLogConsistencyError()
		return false, fmt.Errorf(
			"log index mismatch: memory=%d, storage=%d",
			memLastIndex,
			storageLastIndex,
		)
	}

	if storageLastIndex == 0 {
		if memLastTerm != 0 {
			lm.logger.Errorw("Log term mismatch detected for empty log",
				"memoryTerm", memLastTerm,
				"nodeID", lm.id)
			lm.metrics.ObserveLogConsistencyError()
			return false, fmt.Errorf(
				"term mismatch for empty log: memory=%d, expected=0",
				memLastTerm,
			)
		}
		lm.logger.Debugw("IsConsistentWithStorage: empty log is consistent", "nodeID", lm.id)
		return true, nil
	}

	storageTerm, err := lm.fetchEntryTerm(ctx, storageLastIndex)
	if err != nil {
		lm.logger.Errorw(
			"Failed to fetch storage term for consistency check",
			"index",
			storageLastIndex,
			"error",
			err,
			"nodeID",
			lm.id,
		)
		return false, fmt.Errorf(
			"unable to verify log consistency: failed to fetch term for last index %d: %w",
			storageLastIndex,
			err,
		)
	}

	if memLastTerm != storageTerm {
		lm.logger.Errorw(
			"Log term mismatch detected",
			"index",
			storageLastIndex,
			"memoryTerm",
			memLastTerm,
			"storageTerm",
			storageTerm,
			"nodeID",
			lm.id,
		)
		lm.metrics.ObserveLogConsistencyError()
		return false, fmt.Errorf(
			"log term mismatch at index %d: memory=%d, storage=%d",
			storageLastIndex,
			memLastTerm,
			storageTerm,
		)
	}

	lm.logger.Debugw(
		"Log consistency check passed",
		"lastIndex",
		storageLastIndex,
		"lastTerm",
		storageTerm,
		"nodeID",
		lm.id,
	)
	return true, nil
}

// RebuildInMemoryState refreshes the cached lastIndex and lastTerm
// based on the current state of the log in persistent storage.
func (lm *logManager) RebuildInMemoryState(ctx context.Context) error {
	lm.logger.Infow("Rebuilding in-memory log state from storage...", "nodeID", lm.id)

	lastIndex := lm.storage.LastLogIndex()
	firstIndex := lm.storage.FirstLogIndex()
	var lastTerm types.Term

	lm.logger.Debugw("RebuildInMemoryState: storage state",
		"lastIndex", lastIndex,
		"firstIndex", firstIndex,
		"nodeID", lm.id)

	if lastIndex == 0 {
		lm.updateCachedState(0, 0)
		lm.metrics.ObserveLogState(firstIndex, 0, 0)
		lm.logger.Infow("Log is empty; cached state reset",
			"firstIndex", firstIndex,
			"nodeID", lm.id)
		return nil
	}

	entryTerm, err := lm.fetchEntryTerm(ctx, lastIndex)
	if err != nil {
		lm.logger.Errorw("Failed to fetch last log entry during state rebuild",
			"lastIndex", lastIndex,
			"error", err,
			"nodeID", lm.id)
		lm.updateCachedState(0, 0)
		lm.metrics.ObserveLogState(firstIndex, 0, 0)
		return fmt.Errorf(
			"failed to rebuild state: could not fetch last entry term at index %d: %w",
			lastIndex,
			err,
		)
	}
	lastTerm = entryTerm

	lm.updateCachedState(lastIndex, lastTerm)
	lm.metrics.ObserveLogState(firstIndex, lastIndex, lastTerm)

	lm.logger.Infow("In-memory log state rebuilt successfully from storage",
		"firstIndex", firstIndex,
		"lastIndex", lastIndex,
		"lastTerm", lastTerm,
		"nodeID", lm.id)

	return nil
}

// FindLastEntryWithTermUnsafe searches backwards for the last entry with the given term.
// Starts search from `searchFromHint` if provided, otherwise from the last log index.
// Returns the index of the last entry with the given term, or ErrNotFound if not found.
// Returns early if it encounters an older term or a compacted log region.
func (lm *logManager) FindLastEntryWithTermUnsafe(
	ctx context.Context,
	term types.Term,
	searchFromHint types.Index,
) (types.Index, error) {
	if lm.isShutdown.Load() {
		lm.logger.Debugw("FindLastEntryWithTermUnsafe called but node is shutting down",
			"term", term,
			"searchFromHint", searchFromHint,
			"nodeID", lm.id)
		return 0, ErrShuttingDown
	}
	if term == 0 {
		lm.logger.Debugw("FindLastEntryWithTermUnsafe: cannot search for term 0",
			"nodeID", lm.id)
		return 0, fmt.Errorf("cannot search for term 0") // Term 0 is reserved for pre-log entries
	}

	firstIdx := lm.GetFirstIndexUnsafe()
	lastIdx := lm.GetLastIndexUnsafe()

	startIdx := lastIdx
	if searchFromHint > 0 && searchFromHint < lastIdx {
		startIdx = searchFromHint
	}

	lm.logger.Debugw(
		"FindLastEntryWithTermUnsafe: starting search",
		"term",
		term,
		"startIndex",
		startIdx,
		"firstIndex",
		firstIdx,
		"lastIndex",
		lastIdx,
		"nodeID",
		lm.id,
	)

	foundIndex, err := lm.scanLogForTermUnsafe(
		ctx,
		startIdx,
		firstIdx,
		-1,
		func(t types.Term, idx types.Index) (bool, bool) {
			switch {
			case t == term:
				lm.logger.Debugw("FindLastEntryWithTermUnsafe: found matching term",
					"term", term,
					"index", idx,
					"nodeID", lm.id)
				return true, false
			case t < term:
				lm.logger.Debugw(
					"FindLastEntryWithTermUnsafe: stopping early due to older term",
					"searchTerm",
					term,
					"index",
					idx,
					"entryTerm",
					t,
					"nodeID",
					lm.id,
				)
				return false, true
			default:
				return false, false
			}
		},
	)

	if err == nil && foundIndex > 0 {
		lm.logger.Debugw("FindLastEntryWithTermUnsafe: search completed successfully",
			"term", term,
			"foundIndex", foundIndex,
			"nodeID", lm.id)
	} else {
		lm.logger.Debugw("FindLastEntryWithTermUnsafe: search failed",
			"term", term,
			"error", err,
			"nodeID", lm.id)
	}

	return foundIndex, err
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
		lm.logger.Debugw("FindFirstIndexInTermUnsafe called but node is shutting down",
			"termToFind", termToFind,
			"searchUpToIndex", searchUpToIndex,
			"nodeID", lm.id)
		return 0, ErrShuttingDown
	}
	if termToFind == 0 {
		lm.logger.Debugw("FindFirstIndexInTermUnsafe: cannot search for term 0",
			"nodeID", lm.id)
		return 0, fmt.Errorf("cannot search for term 0 in FindFirstIndexInTermUnsafe")
	}

	firstLogIdx := lm.GetFirstIndexUnsafe()
	lastLogIdx := lm.GetLastIndexUnsafe()

	scanStartIndex := min(searchUpToIndex, lastLogIdx)

	lm.logger.Debugw("FindFirstIndexInTermUnsafe: starting search",
		"termToFind", termToFind,
		"searchUpToIndex", searchUpToIndex,
		"scanStartIndex", scanStartIndex,
		"firstLogIdx", firstLogIdx,
		"lastLogIdx", lastLogIdx,
		"nodeID", lm.id)

	if scanStartIndex < firstLogIdx || (lastLogIdx == 0 && scanStartIndex == 0 && firstLogIdx > 0) {
		if scanStartIndex < firstLogIdx {
			lm.logger.Debugw(
				"FindFirstIndexInTermUnsafe: scan start index is before first log index",
				"termToFind",
				termToFind,
				"searchUpToIndex",
				searchUpToIndex,
				"scanStartIndex",
				scanStartIndex,
				"firstLogIdx",
				firstLogIdx,
				"nodeID",
				lm.id,
			)
			return 0, ErrNotFound
		}
	}

	if lastLogIdx == 0 {
		lm.logger.Debugw("FindFirstIndexInTermUnsafe: log is empty",
			"nodeID", lm.id)
		return 0, ErrNotFound
	}

	var firstIndexOfTermBlock types.Index = 0
	var scanErr error

	_, scanErr = lm.scanLogForTermUnsafe(
		ctx,
		scanStartIndex,
		firstLogIdx,
		-1, /* step backward */
		func(currentEntryTerm types.Term, currentEntryIndex types.Index) (signalScanToStop bool, stopEarlyForScan bool) {
			if currentEntryTerm == termToFind {
				firstIndexOfTermBlock = currentEntryIndex
				lm.logger.Debugw("FindFirstIndexInTermUnsafe: found term match",
					"termToFind", termToFind,
					"index", currentEntryIndex,
					"nodeID", lm.id)
				return false, false
			}

			if firstIndexOfTermBlock != 0 {
				lm.logger.Debugw("FindFirstIndexInTermUnsafe: term block boundary found",
					"termToFind", termToFind,
					"firstIndexOfTermBlock", firstIndexOfTermBlock,
					"boundaryIndex", currentEntryIndex,
					"boundaryTerm", currentEntryTerm,
					"nodeID", lm.id)
				return true, false
			}

			return false, false
		},
	)

	if scanErr != nil {
		if firstIndexOfTermBlock == 0 {
			lm.logger.Debugw("FindFirstIndexInTermUnsafe: scan failed without finding term",
				"termToFind", termToFind,
				"scanError", scanErr,
				"nodeID", lm.id)
			return 0, scanErr
		}

		isHardOperationalError := !errors.Is(scanErr, ErrNotFound) &&
			!errors.Is(scanErr, ErrCompacted)
		if isHardOperationalError {
			lm.logger.Warnw(
				"Hard error during FindFirstIndexInTermUnsafe after term block was identified",
				"termToFind",
				termToFind,
				"firstIndexOfTermBlock",
				firstIndexOfTermBlock,
				"scanError",
				scanErr,
				"nodeID",
				lm.id,
			)
			return 0, scanErr
		}
	}

	if firstIndexOfTermBlock == 0 {
		if scanErr == nil {
			lm.logger.Debugw(
				"FindFirstIndexInTermUnsafe: scan completed without error, but no term index found",
				"termToFind",
				termToFind,
				"searchUpToIndex",
				searchUpToIndex,
				"nodeID",
				lm.id,
			)
		}
		return 0, ErrNotFound
	}

	lm.logger.Debugw("FindFirstIndexInTermUnsafe: search completed successfully",
		"termToFind", termToFind,
		"firstIndexOfTermBlock", firstIndexOfTermBlock,
		"nodeID", lm.id)

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
	lm.logger.Debugw("scanLogForTermUnsafe: starting scan",
		"start", start,
		"end", end,
		"step", step,
		"nodeID", lm.id)

	entriesScanned := 0
	for i := start; (step < 0 && i >= end) || (step > 0 && i <= end); i += types.Index(step) {
		if err := ctx.Err(); err != nil {
			lm.logger.Debugw("scanLogForTermUnsafe: context error",
				"error", err,
				"entriesScanned", entriesScanned,
				"nodeID", lm.id)
			return 0, err
		}

		term, err := lm.GetTermUnsafe(ctx, i)
		if err != nil {
			if errors.Is(err, ErrCompacted) {
				lm.logger.Infow(
					"scanLogForTermUnsafe: encountered compacted log segment",
					"index",
					i,
					"start",
					start,
					"end",
					end,
					"entriesScanned",
					entriesScanned,
					"nodeID",
					lm.id,
				)
				return 0, ErrCompacted
			}

			lm.logger.Warnw("scanLogForTermUnsafe: error during term scan from GetTermUnsafe",
				"index", i,
				"error", err,
				"entriesScanned", entriesScanned,
				"nodeID", lm.id)
			return 0, fmt.Errorf("term scan failed at index %d due to GetTermUnsafe: %w", i, err)
		}

		entriesScanned++
		lm.logger.Debugw("scanLogForTermUnsafe: scanned entry",
			"index", i,
			"term", term,
			"entriesScanned", entriesScanned,
			"nodeID", lm.id)

		matchFound, stop := match(term, i)
		if matchFound {
			lm.logger.Debugw("scanLogForTermUnsafe: match found",
				"index", i,
				"term", term,
				"entriesScanned", entriesScanned,
				"nodeID", lm.id)
			return i, nil // Match found, return the index where it was found.
		}
		if stop {
			lm.logger.Debugw("scanLogForTermUnsafe: early stop requested",
				"index", i,
				"term", term,
				"entriesScanned", entriesScanned,
				"nodeID", lm.id)
			return 0, ErrNotFound
		}
	}

	lm.logger.Debugw("scanLogForTermUnsafe: scan completed without match",
		"entriesScanned", entriesScanned,
		"nodeID", lm.id)
	return 0, ErrNotFound
}

// Stop logs the shutdown signal for the LogManager.
func (lm *logManager) Stop() {
	if lm.isShutdown.Load() {
		lm.logger.Debugw("LogManager Stop() called multiple times or already shut down.", "nodeID", lm.id)
		return
	}

	lm.logger.Infow("LogManager Stop() invoked; shutdown coordinated via shared flag.", "nodeID", lm.id)
	lm.isShutdown.Store(true)
}

// updateCachedState updates the in-memory representation of the log's last index and term.
func (lm *logManager) updateCachedState(index types.Index, term types.Term) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.updateCachedStateLocked(index, term)
}

// updateCachedStateAndGetFirstLocked updates the cached last index/term and returns first index.
// Assumes the caller holds lm.mu write lock.
func (lm *logManager) updateCachedStateAndGetFirstLocked(index types.Index, term types.Term) types.Index {
	lm.updateCachedStateLocked(index, term)
	return lm.storage.FirstLogIndex()
}

// updateCachedStateLocked assumes the caller holds lm.mu.
func (lm *logManager) updateCachedStateLocked(index types.Index, term types.Term) {
	lm.lastIndex.Store(uint64(index))
	lm.lastTerm.Store(uint64(term))
	lm.logger.Debugw("updateCachedStateLocked: cache updated",
		"index", index,
		"term", term,
		"nodeID", lm.id)
}

// fetchEntryTerm fetches the term of the log entry at the given index from storage.
// Uses a context with a predefined internal timeout.
func (lm *logManager) fetchEntryTerm(ctx context.Context, index types.Index) (types.Term, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, logManagerOpTimeout)
	defer cancel()

	lm.logger.Debugw("fetchEntryTerm: fetching term from storage",
		"index", index,
		"timeout", logManagerOpTimeout,
		"nodeID", lm.id)

	entry, err := lm.storage.GetLogEntry(fetchCtx, index)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			lm.logger.Warnw(
				"Timeout fetching entry term internally",
				"index",
				index,
				"timeout",
				logManagerOpTimeout,
				"nodeID",
				lm.id,
			)
		} else if errors.Is(err, context.Canceled) {
			lm.logger.Infow("Context canceled fetching entry term internally",
				"index", index,
				"nodeID", lm.id)
		} else if errors.Is(err, storage.ErrEntryNotFound) {
			lm.logger.Errorw("Storage reports entry not found during internal fetch",
				"index", index,
				"error", err,
				"nodeID", lm.id)
		} else {
			lm.logger.Errorw("Failed to fetch entry term internally",
				"index", index,
				"error", err,
				"nodeID", lm.id)
		}
		return 0, fmt.Errorf("failed to fetch term for index %d: %w", index, err)
	}

	lm.logger.Debugw("fetchEntryTerm: successfully fetched term",
		"index", index,
		"term", entry.Term,
		"nodeID", lm.id)
	return entry.Term, nil
}

// GetLogStateForDebugging returns comprehensive log state information for debugging and monitoring.
// This method is safe for concurrent use and provides a consistent snapshot of the log state.
func (lm *logManager) GetLogStateForDebugging() LogDebugState {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	firstIndex := lm.storage.FirstLogIndex()
	storageLastIndex := lm.storage.LastLogIndex()
	cachedLastIndex := types.Index(lm.lastIndex.Load())
	cachedLastTerm := types.Term(lm.lastTerm.Load())

	// Calculate log size
	logSize := 0
	if storageLastIndex >= firstIndex && storageLastIndex > 0 {
		logSize = int(storageLastIndex - firstIndex + 1)
	}

	isConsistent := cachedLastIndex == storageLastIndex

	var storageMetrics map[string]uint64
	if lm.storage != nil {
		storageMetrics = lm.storage.GetMetrics()
	}

	state := LogDebugState{
		NodeID:          lm.id,
		FirstIndex:      firstIndex,
		LastIndex:       storageLastIndex,
		LastTerm:        cachedLastTerm, // We assume cache is authoritative for term
		CachedLastIndex: cachedLastIndex,
		CachedLastTerm:  cachedLastTerm,
		LogSize:         logSize,
		IsEmpty:         storageLastIndex == 0,
		IsConsistent:    isConsistent,
		StorageMetrics:  storageMetrics,
		LastChecked:     time.Now(),
	}

	lm.logger.Debugw("GetLogStateForDebugging called",
		"firstIndex", firstIndex,
		"lastIndex", storageLastIndex,
		"cachedLastIndex", cachedLastIndex,
		"cachedLastTerm", cachedLastTerm,
		"logSize", logSize,
		"isConsistent", isConsistent,
		"nodeID", lm.id)

	return state
}

// RestoreFromSnapshot updates the log manager's state after a snapshot has been installed.
// This should be called by the snapshot manager after successfully installing a snapshot
// and truncating the log. It updates cached state and ensures consistency.
func (lm *logManager) RestoreFromSnapshot(ctx context.Context, meta types.SnapshotMetadata) error {
	if lm.isShutdown.Load() {
		lm.logger.Debugw("RestoreFromSnapshot called but node is shutting down",
			"snapshotIndex", meta.LastIncludedIndex,
			"snapshotTerm", meta.LastIncludedTerm,
			"nodeID", lm.id)
		return ErrShuttingDown
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.restoreFromSnapshotInternal(ctx, meta)
}

// RestoreFromSnapshotUnsafe updates the log manager's state after a snapshot has been installed
// without acquiring locks. This should be called by the snapshot manager when it already holds
// the appropriate locks. It updates cached state and ensures consistency.
func (lm *logManager) RestoreFromSnapshotUnsafe(ctx context.Context, meta types.SnapshotMetadata) error {
	lm.logger.Debugw("RestoreFromSnapshotUnsafe called",
		"snapshotIndex", meta.LastIncludedIndex,
		"snapshotTerm", meta.LastIncludedTerm,
		"nodeID", lm.id)

	return lm.restoreFromSnapshotInternal(ctx, meta)
}

// restoreFromSnapshotInternal contains the core logic for snapshot restoration.
func (lm *logManager) restoreFromSnapshotInternal(ctx context.Context, meta types.SnapshotMetadata) error {
	lm.logger.Infow("restoreFromSnapshotInternal: updating log state from snapshot",
		"snapshotIndex", meta.LastIncludedIndex,
		"snapshotTerm", meta.LastIncludedTerm,
		"previousCachedIndex", lm.lastIndex.Load(),
		"previousCachedTerm", lm.lastTerm.Load(),
		"nodeID", lm.id)

	// Validate snapshot metadata
	if meta.LastIncludedIndex == 0 {
		return fmt.Errorf("invalid snapshot metadata: LastIncludedIndex cannot be 0")
	}
	if meta.LastIncludedTerm == 0 {
		return fmt.Errorf("invalid snapshot metadata: LastIncludedTerm cannot be 0")
	}

	// Update our cached state to reflect the snapshot
	oldLastIndex := lm.lastIndex.Load()
	oldLastTerm := lm.lastTerm.Load()

	lm.updateCachedStateLocked(meta.LastIncludedIndex, meta.LastIncludedTerm)

	// Note: The snapshot manager should have already truncated the log prefix,
	// but we verify the state is consistent
	currentFirstIndex := lm.storage.FirstLogIndex()
	currentLastIndex := lm.storage.LastLogIndex()

	lm.logger.Infow("restoreFromSnapshotInternal: state updated",
		"snapshotIndex", meta.LastIncludedIndex,
		"snapshotTerm", meta.LastIncludedTerm,
		"oldCachedIndex", oldLastIndex,
		"oldCachedTerm", oldLastTerm,
		"newCachedIndex", lm.lastIndex.Load(),
		"newCachedTerm", lm.lastTerm.Load(),
		"storageFirstIndex", currentFirstIndex,
		"storageLastIndex", currentLastIndex,
		"nodeID", lm.id)

	// Verify consistency: after snapshot restoration, either:
	// 1. Log is empty (storage last index == snapshot index), or
	// 2. Log continues from snapshot index (first index == snapshot index + 1)
	expectedFirstIndex := meta.LastIncludedIndex + 1
	if currentLastIndex > meta.LastIncludedIndex {
		if currentFirstIndex != expectedFirstIndex {
			lm.logger.Warnw("restoreFromSnapshotInternal: log state inconsistent after snapshot",
				"snapshotIndex", meta.LastIncludedIndex,
				"expectedFirstIndex", expectedFirstIndex,
				"actualFirstIndex", currentFirstIndex,
				"actualLastIndex", currentLastIndex,
				"nodeID", lm.id)

			return fmt.Errorf("log state inconsistent after snapshot: expected first index %d, got %d",
				expectedFirstIndex, currentFirstIndex)
		}
	}

	lm.metrics.ObserveLogState(currentFirstIndex, meta.LastIncludedIndex, meta.LastIncludedTerm)

	lm.logger.Infow("restoreFromSnapshotInternal completed successfully",
		"snapshotIndex", meta.LastIncludedIndex,
		"snapshotTerm", meta.LastIncludedTerm,
		"nodeID", lm.id)

	return nil
}
