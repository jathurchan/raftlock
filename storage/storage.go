package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// StorageConfig defines configuration parameters for initializing FileStorage.
type StorageConfig struct {
	// Dir specifies the root directory where all storage-related files
	// (state, log, snapshots) will be persisted.
	Dir string
}

// FileStorage provides a thread-safe, durable implementation of the Storage interface
// using the local filesystem. It persists Raft's persistent state, log entries,
// and snapshots, and supports concurrent access and context-aware operations.
type FileStorage struct {
	// dir is the root directory used to store all Raft-related data on disk.
	dir string

	// options defines behavioral flags and tuning knobs for FileStorage.
	options FileStorageOptions

	// stateMu guards access to persistent state (state.json).
	stateMu sync.RWMutex

	// logMu guards access to log-related files (log.dat, metadata.json)
	// and the in-memory index-to-offset mapping.
	logMu sync.RWMutex

	// snapshotMu guards access to snapshot metadata and data files
	// (snapshot_meta.json, snapshot.dat).
	snapshotMu sync.RWMutex

	// firstLogIndex holds the index of the first log entry currently available in the log.
	// This value is updated during log compaction or snapshot installation.
	firstLogIndex atomic.Uint64

	// lastLogIndex holds the index of the last appended log entry.
	// It is updated during AppendLogEntries and reset during recovery or truncation.
	lastLogIndex atomic.Uint64

	// indexToOffsetMap maps log indices to their byte offset in log.dat for fast lookups.
	// Only used if Features.EnableIndexMap is enabled.
	indexToOffsetMap []types.IndexOffsetPair

	// status holds the current operational state of the storage, represented by a storageStatus value.
	// It is updated atomically to allow safe concurrent reads and writes.
	//
	// Valid values include:
	//   - storageStatusInitializing
	//   - storageStatusRecovering
	//   - storageStatusReady
	//   - storageStatusCorrupted
	//   - storageStatusClosed
	status atomic.Value // stores a storageStatus

	file       fileSystem
	serializer serializer
	index      indexService
	metadata   metadataService
	recovery   recoveryService

	// logger is the structured logger used for logging storage operations and events.
	logger logger.Logger

	// metrics holds various atomic counters for tracking performance statistics,
	// used only if Features.EnableMetrics is true.
	metrics struct {
		// appendOps counts the number of successful AppendLogEntries operations.
		appendOps atomic.Uint64
		// appendBytes counts the total bytes written by AppendLogEntries operations.
		appendBytes atomic.Uint64
		// readOps counts the number of read operations (GetLogEntry, GetLogEntries, LoadSnapshot).
		readOps atomic.Uint64
		// readBytes counts the total bytes read by read operations.
		readBytes atomic.Uint64
		// snapshotSaveOps counts the number of successful SaveSnapshot operations.
		snapshotSaveOps atomic.Uint64
		// snapshotLoadOps counts the number of successful LoadSnapshot operations.
		snapshotLoadOps atomic.Uint64
		// truncatePrefixOps counts the number of successful TruncateLogPrefix operations.
		truncatePrefixOps atomic.Uint64
		// truncateSuffixOps counts the number of successful TruncateLogSuffix operations.
		truncateSuffixOps atomic.Uint64
		// slowOperations counts the number of log operations exceeding a predefined threshold.
		slowOperations atomic.Uint64
	}
}

// NewFileStorage creates a new FileStorage with default options.
func NewFileStorage(cfg StorageConfig, logger logger.Logger) (Storage, error) {
	return NewFileStorageWithOptions(cfg, DefaultFileStorageOptions(), logger)
}

// NewFileStorageWithOptions creates a new FileStorage with custom options.
func NewFileStorageWithOptions(cfg StorageConfig, options FileStorageOptions, logger logger.Logger) (Storage, error) {

	if cfg.Dir == "" {
		return nil, errors.New("storage directory must be specified")
	}

	fileSystem := newFileSystem()
	serializer := getSerializer(options.Features.EnableBinaryFormat)
	entryReader := newLogEntryReader(maxEntrySizeBytes, lengthPrefixSize, serializer, logger)
	indexService := newIndexServiceWithReader(fileSystem, entryReader, logger)
	metadataService := newMetadataServiceWithDeps(fileSystem, serializer, indexService, logger)
	systemInfo := NewSystemInfo()
	recoveryService := newRecoveryService(fileSystem, serializer, logger, cfg.Dir, normalMode, indexService, metadataService, systemInfo)

	return newFileStorageWithDeps(cfg, options, fileSystem, serializer, indexService, metadataService, recoveryService, logger)
}

func getSerializer(enableBinary bool) serializer {
	if enableBinary {
		return newBinarySerializer()
	}
	return newJsonSerializer()
}

func newFileStorageWithDeps(
	cfg StorageConfig,
	options FileStorageOptions,
	fileSystem fileSystem,
	serializer serializer,
	indexService indexService,
	metadataService metadataService,
	recoveryService recoveryService,
	logger logger.Logger,
) (Storage, error) {
	if err := fileSystem.MkdirAll(cfg.Dir, ownRWXOthRX); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %q: %w", cfg.Dir, err)
	}

	logger = logger.WithComponent("storage")

	s := &FileStorage{
		dir:        cfg.Dir,
		options:    options,
		file:       fileSystem,
		serializer: serializer,
		index:      indexService,
		metadata:   metadataService,
		recovery:   recoveryService,
		logger:     logger,
	}

	s.status.Store(storageStatusInitializing)
	s.logger.Infow("Initializing storage", "dir", cfg.Dir)

	// Initialize the storage
	if err := s.initialize(); err != nil {
		return nil, err
	}

	return s, nil
}

// initialize handles storage initialization and recovery
func (s *FileStorage) initialize() error {
	// Check for recovery markers
	recoveryNeeded, err := s.recovery.CheckForRecoveryMarkers()
	if err != nil {
		return fmt.Errorf("failed checking recovery markers: %w", err)
	}

	if recoveryNeeded {
		s.status.Store(storageStatusRecovering)
		s.logger.Infow("Recovery markers found, entering recovery mode")

		if err := s.recovery.PerformRecovery(); err != nil {
			return fmt.Errorf("recovery failed: %w", err)
		}
	}

	if err := s.recovery.CreateRecoveryMarker(); err != nil {
		return fmt.Errorf("failed to create recovery marker: %w", err)
	}

	if err := s.recovery.CleanupTempFiles(); err != nil {
		s.logger.Warnw("Failed to clean up temporary files", "error", err)
	}

	if err := s.initStateAndMetadata(); err != nil {
		return err
	}

	s.recovery.RemoveRecoveryMarker()

	s.status.Store(storageStatusReady)
	s.logger.Infow("Storage initialized successfully",
		"firstIndex", s.firstLogIndex.Load(),
		"lastIndex", s.lastLogIndex.Load())

	return nil
}

// initStateAndMetadata initializes state and metadata, building index map
func (s *FileStorage) initStateAndMetadata() error {
	if err := s.loadInitialState(); err != nil {
		return err
	}

	s.logMu.Lock()
	defer s.logMu.Unlock()

	if err := s.ensureMetadataLocked(); err != nil {
		return err
	}

	if s.options.Features.EnableIndexMap {
		if err := s.rebuildIndexStateLocked(); err != nil {
			return err
		}
	}

	return nil
}

func (s *FileStorage) loadInitialState() error {
	if _, err := s.LoadState(context.Background()); err != nil &&
		!errors.Is(err, ErrStorageIO) && !errors.Is(err, ErrCorruptedState) {
		return fmt.Errorf("failed to load initial state: %w", err)
	}
	return nil
}

// ensureMetadataLocked assumes s.logMu is already held
func (s *FileStorage) ensureMetadataLocked() error {
	if err := s.loadMetadataLocked(); err != nil {
		if !os.IsNotExist(errors.Unwrap(err)) {
			return fmt.Errorf("failed to load log metadata: %w", err)
		}
		return s.saveMetadataLocked()
	}
	return nil
}

// rebuildIndexStateLocked assumes s.logMu is already held
func (s *FileStorage) rebuildIndexStateLocked() error {
	if err := s.buildIndexOffsetMapLocked(); err != nil {
		return fmt.Errorf("failed to build index map: %w", err)
	}
	if err := s.syncLogStateFromIndexMapLocked(); err != nil {
		return fmt.Errorf("log consistency check failed: %w", err)
	}
	if err := s.verifyInMemoryState(); err != nil {
		s.logger.Errorw("Post-recovery state mismatch", "error", err)
		return err
	}
	return nil
}

// loadMetadataLocked assumes s.logMu is already held
func (s *FileStorage) loadMetadataLocked() error {
	metadata, err := s.metadata.LoadMetadata(s.file.Path(s.dir, metadataFilename))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// File doesn't exist, initialize with empty state
			s.updateLogBoundsLocked(0, 0)
			return nil
		}
		return err // Error already wrapped and logged inside LoadMetadata
	}

	// Validate loaded metadata
	if err := s.metadata.ValidateMetadataRange(metadata.FirstIndex, metadata.LastIndex); err != nil {
		return err
	}

	s.updateLogBoundsLocked(metadata.FirstIndex, metadata.LastIndex)

	s.logger.Debugw("Loaded metadata",
		"firstIndex", metadata.FirstIndex,
		"lastIndex", metadata.LastIndex)

	return nil
}

// saveMetadata writes the current log metadata to disk atomically
// saveMetadataLocked assumes s.logMu is already held
func (s *FileStorage) saveMetadataLocked() error {
	metadata := logMetadata{
		FirstIndex: types.Index(s.firstLogIndex.Load()),
		LastIndex:  types.Index(s.lastLogIndex.Load()),
	}

	metadataPath := s.file.Path(s.dir, metadataFilename)
	useAtomic := s.options.Features.EnableAtomicWrites

	if err := s.metadata.SaveMetadata(metadataPath, metadata, useAtomic); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return nil
}

// buildIndexOffsetMapLocked scans the log file and builds the index-to-offset map.
// buildIndexOffsetMapLocked assumes s.logMu is already held
func (s *FileStorage) buildIndexOffsetMapLocked() error {
	if !s.options.Features.EnableIndexMap {
		return nil // Skip if feature disabled
	}

	logPath := s.file.Path(s.dir, logFilename)

	s.logger.Debugw("Building index offset map", "path", logPath)

	result, err := s.index.Build(logPath)
	if err != nil {
		return fmt.Errorf("failed to build index-offset map: %w", err)
	}

	s.indexToOffsetMap = result.IndexMap

	s.logger.Infow("Index offset map built",
		"entries", len(result.IndexMap),
		"truncated", result.Truncated,
		"lastValidIndex", result.LastValidIndex,
	)

	return nil
}

// updateLogBoundsLocked sets the in-memory first and last log indices atomically.
// updateLogBoundsLocked assumes s.logMu is already held
func (s *FileStorage) updateLogBoundsLocked(first, last types.Index) {
	s.firstLogIndex.Store(uint64(first))
	s.lastLogIndex.Store(uint64(last))
}

// syncLogStateFromIndexMapLocked ensures in-memory log index bounds and on-disk metadata
// are consistent with the actual log content, as represented by indexToOffsetMap.
// If discrepancies are found, it updates both in-memory and persistent metadata.
// This also validates internal consistency of the index map.
// syncLogStateFromIndexMapLocked assumes s.logMu is already held
func (s *FileStorage) syncLogStateFromIndexMapLocked() error {
	if !s.options.Features.EnableIndexMap {
		return nil // Skip if feature disabled
	}

	currentFirst := types.Index(s.firstLogIndex.Load())
	currentLast := types.Index(s.lastLogIndex.Load())

	metadataPath := s.file.Path(s.dir, metadataFilename)

	newFirst, newLast, err := s.metadata.SyncMetadataFromIndexMap(
		metadataPath,
		s.indexToOffsetMap,
		currentFirst,
		currentLast,
		"syncLogStateFromIndexMap",
		s.options.Features.EnableAtomicWrites,
	)

	if err != nil {
		return fmt.Errorf("syncLogStateFromIndexMap: failed to sync metadata: %w", err)
	}

	// Update in-memory bounds to match the new persisted values
	s.updateLogBoundsLocked(newFirst, newLast)

	return s.index.VerifyConsistency(s.indexToOffsetMap)
}

// verifyInMemoryState ensures that the in-memory log index bounds (firstLogIndex and lastLogIndex)
// are consistent with the actual contents of the log, as represented by indexToOffsetMap.
//
// This method is intended to be used after recovery or rebuilding operations (e.g., truncation,
// index map rebuild, metadata sync) to catch any silent discrepancies between the in-memory state
// and the true log content.
//
// Returns an error if a mismatch is detected, indicating potential internal inconsistency.
func (s *FileStorage) verifyInMemoryState() error {
	expected := s.index.GetBounds(s.indexToOffsetMap, 0, 0)

	memFirst := s.firstLogIndex.Load()
	memLast := s.lastLogIndex.Load()

	if uint64(expected.NewFirst) != memFirst || uint64(expected.NewLast) != memLast {
		return fmt.Errorf("in-memory state inconsistent: expected (%d-%d), got (%d-%d)",
			expected.NewFirst, expected.NewLast,
			memFirst, memLast)
	}

	return nil
}

// LoadState loads the most recently persisted PersistentState from disk.
// This should be called during node startup or recovery to restore the last known term and vote.
//
// Returns:
//   - A zero-valued PersistentState if no file is found (fresh start).
//   - ErrStorageIO if reading the file fails due to I/O issues.
//   - ErrCorruptedState if unmarshaling the file fails due to corruption or format issues.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) LoadState(ctx context.Context) (types.PersistentState, error) {
	if err := ctx.Err(); err != nil {
		s.logger.Warnw("LoadState aborted: context cancelled early", "error", err)
		return types.PersistentState{}, err
	}

	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	statePath := s.file.Path(s.dir, stateFilename)
	s.logger.Debugw("Attempting to read persisted state file", "path", statePath)

	data, err := s.file.ReadFile(statePath)
	if err != nil {
		if s.file.IsNotExist(err) {
			s.logger.Infow("State file does not exist, assuming fresh start", "path", statePath)
			return types.PersistentState{CurrentTerm: 0, VotedFor: ""}, nil
		}
		s.logger.Errorw("Failed to read state file", "path", statePath, "error", err)
		return types.PersistentState{}, fmt.Errorf("%w: failed to read state file %q: %v", ErrStorageIO, statePath, err)
	}

	state, err := s.serializer.UnmarshalState(data)
	if err != nil {
		s.logger.Errorw("Failed to decode state file", "path", statePath, "error", err)
		return types.PersistentState{}, fmt.Errorf("%w: failed to decode state file %q: %v", ErrCorruptedState, statePath, err)
	}

	s.logger.Infow("Successfully loaded persisted state", "path", statePath, "term", state.CurrentTerm, "votedFor", state.VotedFor)
	return state, nil
}

// SaveState atomically persists the current term and voted-for candidate.
// Must be called after any update to persistent state.
//
// Returns:
//   - ErrStorageIO if the operation fails due to I/O issues.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) SaveState(ctx context.Context, state types.PersistentState) error {
	panic("not implemented")
}

// AppendLogEntries persists one or more log entries to the Raft log.
// Entries must be non-empty, in strictly ascending index order, and contiguous with the existing log.
//
// Returns:
//   - ErrEmptyEntries if the input is empty.
//   - ErrOutOfOrderEntries if entries are not in ascending order.
//   - ErrNonContiguousEntries if entries don't follow the last log index.
//   - ErrStorageIO on I/O failure.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) AppendLogEntries(ctx context.Context, entries []types.LogEntry) error {
	panic("not implemented")
}

// GetLogEntries returns a slice of log entries within the range [start, end).
//
// Returns:
//   - ErrInvalidLogRange if start >= end.
//   - ErrIndexOutOfRange if the requested range includes missing or compacted entries.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) GetLogEntries(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
	panic("not implemented")
}

// GetLogEntry returns the log entry at the given index.
//
// Returns:
//   - ErrEntryNotFound if the entry is missing or compacted.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) GetLogEntry(ctx context.Context, index types.Index) (types.LogEntry, error) {
	panic("not implemented")
}

// TruncateLogSuffix deletes all log entries with indices >= the given index.
// Used to resolve log conflicts during replication.
//
// Returns:
//   - ErrIndexOutOfRange if the index is beyond the last log index.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) TruncateLogSuffix(ctx context.Context, index types.Index) error {
	// TODO: Call updateLogBounds() during a truncate
	panic("not implemented")
}

// TruncateLogPrefix deletes all log entries with indices < the given index.
// Used during log compaction after snapshotting.
//
// Returns:
//   - ErrIndexOutOfRange if the index is less than the first log index.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) TruncateLogPrefix(ctx context.Context, index types.Index) error {
	// TODO: Trim indexToOffsetMap during TruncateLogPrefix to prevent unbounded growth.
	// TODO: Call updateLogBounds() during a truncate
	panic("not implemented")
}

// SaveSnapshot persists a snapshot and its metadata atomically.
// The snapshot should represent the full compacted state at a specific log index.
//
// Returns:
//   - ErrStorageIO on failure to persist the snapshot.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) SaveSnapshot(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
	panic("not implemented")
}

// LoadSnapshot retrieves the latest snapshot and its metadata.
//
// Returns:
//   - ErrNoSnapshot if no snapshot has been saved.
//   - ErrCorruptedSnapshot if the snapshot is unreadable or invalid.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	panic("not implemented")
}

// LastLogIndex returns the highest index currently stored in the log.
// Returns 0 if the log is empty.
func (s *FileStorage) LastLogIndex() types.Index {
	return types.Index(s.lastLogIndex.Load())
}

// FirstLogIndex returns the lowest index currently available in the log.
// Returns 0 if the log is empty.
func (s *FileStorage) FirstLogIndex() types.Index {
	return types.Index(s.firstLogIndex.Load())
}

// Close releases all underlying resources used by the storage implementation.
//
// Returns:
//   - ErrStorageIO if cleanup fails.
func (s *FileStorage) Close() error {
	panic("not implemented")
}
