package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

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

	fileSystem     fileSystem
	serializer     serializer
	logAppender    logAppender
	logReader      logEntryReader
	logRewriter    logRewriter
	snapshotWriter snapshotWriter
	snapshotReader snapshotReader
	logLocker      rwOperationLocker
	snapshotLocker rwOperationLocker
	indexSvc       indexService
	metadataSvc    metadataService
	recoverySvc    recoveryService

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
	writer := newLogWriter(serializer, logger)
	logReader := newLogEntryReader(maxEntrySizeBytes, lengthPrefixSize, serializer, logger)
	logAppender := newLogAppender(
		fileSystem.Path(cfg.Dir, logFilename),
		fileSystem,
		writer,
		logger,
		options.SyncOnAppend,
	)
	logRewriter := newLogRewriter(fileSystem.Path(cfg.Dir, logFilename), fileSystem, serializer, logger)
	indexService := newIndexServiceWithReader(fileSystem, logReader, logger)
	metadataService := newMetadataServiceWithDeps(fileSystem, serializer, indexService, logger)
	systemInfo := NewSystemInfo()
	recoveryService := newRecoveryService(fileSystem, serializer, logger, cfg.Dir, normalMode, indexService, metadataService, systemInfo)

	return newFileStorageWithDeps(cfg, options, fileSystem, serializer, logAppender, logReader, logRewriter, indexService, metadataService, recoveryService, logger)
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
	logAppender logAppender,
	logReader logEntryReader,
	logRewriter logRewriter,
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
		dir:         cfg.Dir,
		options:     options,
		fileSystem:  fileSystem,
		serializer:  serializer,
		logAppender: logAppender,
		logReader:   logReader,
		logRewriter: logRewriter,
		indexSvc:    indexService,
		metadataSvc: metadataService,
		recoverySvc: recoveryService,
		logger:      logger,
	}

	s.logLocker = newRWOperationLocker(
		&s.logMu,
		logger,
		options,
		&s.metrics.slowOperations,
	)

	s.snapshotLocker = newRWOperationLocker(
		&s.snapshotMu,
		logger,
		options,
		&s.metrics.slowOperations,
	)

	s.snapshotWriter = newSnapshotWriter(
		fileSystem,
		serializer,
		logger,
		cfg.Dir,
		&snapshotWriteHooks{
			OnTempFilesWritten: func() {
				_ = s.recoverySvc.UpdateSnapshotMarkerStatus("files_written")
			},
			OnMetadataCommitted: func() {
				_ = s.recoverySvc.UpdateSnapshotMarkerStatus("meta_committed")
			},
		},
		options.Features.EnableChunkedIO,
		options.ChunkSize,
	)

	s.snapshotReader = newSnapshotReader(
		fileSystem,
		serializer,
		logger,
		cfg.Dir,
	)

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
	recoveryNeeded, err := s.recoverySvc.CheckForRecoveryMarkers()
	if err != nil {
		return fmt.Errorf("failed checking recovery markers: %w", err)
	}

	if recoveryNeeded {
		s.status.Store(storageStatusRecovering)
		s.logger.Infow("Recovery markers found, entering recovery mode")

		if err := s.recoverySvc.PerformRecovery(); err != nil {
			return fmt.Errorf("recovery failed: %w", err)
		}
	}

	if err := s.recoverySvc.CreateRecoveryMarker(); err != nil {
		return fmt.Errorf("failed to create recovery marker: %w", err)
	}

	if err := s.recoverySvc.CleanupTempFiles(); err != nil {
		s.logger.Warnw("Failed to clean up temporary files", "error", err)
	}

	if err := s.initStateAndMetadata(); err != nil {
		return err
	}

	s.recoverySvc.RemoveRecoveryMarker()

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
	metadata, err := s.metadataSvc.LoadMetadata(s.fileSystem.Path(s.dir, metadataFilename))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// File doesn't exist, initialize with empty state
			s.setInMemoryLogBoundsLocked(0, 0)
			return nil
		}
		return err // Error already wrapped and logged inside LoadMetadata
	}

	// Validate loaded metadata
	if err := s.metadataSvc.ValidateMetadataRange(metadata.FirstIndex, metadata.LastIndex); err != nil {
		return err
	}

	s.setInMemoryLogBoundsLocked(metadata.FirstIndex, metadata.LastIndex)

	s.logger.Debugw("Loaded metadata",
		"firstIndex", metadata.FirstIndex,
		"lastIndex", metadata.LastIndex)

	return nil
}

// saveMetadata writes the current log metadata to disk atomically
// saveMetadataLocked assumes s.logMu is already held
func (s *FileStorage) saveMetadataLocked() error {
	metadata := logMetadata{
		FirstIndex: s.FirstLogIndex(),
		LastIndex:  s.LastLogIndex(),
	}

	metadataPath := s.fileSystem.Path(s.dir, metadataFilename)
	useAtomic := s.options.Features.EnableAtomicWrites

	if err := s.metadataSvc.SaveMetadata(metadataPath, metadata, useAtomic); err != nil {
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

	logPath := s.fileSystem.Path(s.dir, logFilename)

	s.logger.Debugw("Building index offset map", "path", logPath)

	result, err := s.indexSvc.Build(logPath)
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

// syncLogStateFromIndexMapLocked ensures in-memory log index bounds and on-disk metadata
// are consistent with the actual log content, as represented by indexToOffsetMap.
// If discrepancies are found, it updates both in-memory and persistent metadata.
// This also validates internal consistency of the index map.
// syncLogStateFromIndexMapLocked assumes s.logMu is already held
func (s *FileStorage) syncLogStateFromIndexMapLocked() error {
	if !s.options.Features.EnableIndexMap {
		return nil // Skip if feature disabled
	}
	currentFirst := s.FirstLogIndex()
	currentLast := s.LastLogIndex()

	metadataPath := s.fileSystem.Path(s.dir, metadataFilename)

	newFirst, newLast, err := s.metadataSvc.SyncMetadataFromIndexMap(
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
	s.setInMemoryLogBoundsLocked(newFirst, newLast)

	return s.indexSvc.VerifyConsistency(s.indexToOffsetMap)
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

	statePath := s.fileSystem.Path(s.dir, stateFilename)
	s.logger.Debugw("Attempting to read persisted state file", "path", statePath)

	data, err := s.fileSystem.ReadFile(statePath)
	if err != nil {
		if s.fileSystem.IsNotExist(err) {
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
	if err := ctx.Err(); err != nil {
		s.logger.Warnw("SaveState aborted: context error", "error", err)
		return err
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	statePath := s.fileSystem.Path(s.dir, stateFilename)
	useAtomicWrite := s.options.Features.EnableAtomicWrites

	s.logger.Debugw("Attempting to persist state", "path", statePath, "term", state.CurrentTerm, "votedFor", state.VotedFor)

	data, err := s.serializer.MarshalState(state)
	if err != nil {
		s.logger.Errorw("Failed to encode state", "path", statePath, "error", err)
		return fmt.Errorf("%w: failed to marshal state: %v", ErrStorageIO, err)
	}

	if err := s.fileSystem.WriteMaybeAtomic(statePath, data, ownRWOthR, useAtomicWrite); err != nil {
		s.logger.Errorw("Failed to write state", "path", statePath, "error", err)
		return fmt.Errorf("%w: failed to write metadata file: %v", ErrStorageIO, err)
	}

	s.logger.Infow("State saved", "path", statePath)
	return nil
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
	currentLastIndex := types.Index(s.lastLogIndex.Load())

	return s.logLocker.DoWrite(ctx, func() error {
		result, err := s.logAppender.Append(ctx, entries, currentLastIndex)
		if err != nil {
			s.logger.Warnw("Failed to append log entries",
				"entryCount", len(entries),
				"currentLastIndex", currentLastIndex,
				"error", err,
			)
			return err
		}

		return s.commitAppendChangesLocked(
			result.Offsets,
			result.FirstIndex,
			result.LastIndex,
			currentLastIndex,
		)
	})
}

// commitAppendChangesLocked finalizes an append operation by updating in-memory state,
// appending to the index map (if enabled), and syncing both in-memory and persistent metadata.
// If state synchronization fails, it rolls back both in-memory and index map changes.
// Assumes s.logMu is already held by the caller.
func (s *FileStorage) commitAppendChangesLocked(
	newOffsets []types.IndexOffsetPair,
	firstNewIndex, lastNewIndex, currentLastIndex types.Index,
) error {
	s.logger.Debugw("Committing append changes",
		"firstNewIndex", firstNewIndex,
		"lastNewIndex", lastNewIndex,
		"currentLastIndex", currentLastIndex,
		"numNewOffsets", len(newOffsets),
	)
	s.maybeInitInMemoryLogBoundsLocked(firstNewIndex, lastNewIndex)

	if s.options.Features.EnableIndexMap {
		s.indexToOffsetMap = s.indexSvc.Append(s.indexToOffsetMap, newOffsets)
	}

	if err := s.syncLogStateFromIndexMapLocked(); err != nil {
		s.logger.Errorw("Failed to sync log state after append",
			"error", err,
			"firstNewIndex", firstNewIndex,
			"lastNewIndex", lastNewIndex,
		)

		wasFirstInit := firstNewIndex == s.FirstLogIndex()
		s.rollbackInMemoryState(currentLastIndex, wasFirstInit)

		if s.options.Features.EnableIndexMap {
			s.rollbackIndexMapState(len(newOffsets))
		}

		return fmt.Errorf("log append succeeded but state sync failed: %w", err)
	}

	s.logger.Infow("Append changes committed successfully",
		"firstNewIndex", firstNewIndex,
		"lastNewIndex", lastNewIndex,
	)

	return nil
}

// GetLogEntries returns a slice of log entries within the range [start, end).
//
// Returns:
//   - ErrInvalidLogRange if start >= end.
//   - ErrIndexOutOfRange if the requested range includes missing or compacted entries.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) GetLogEntries(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
	if start >= end {
		return nil, ErrInvalidLogRange
	}

	var entries []types.LogEntry
	err := s.logLocker.DoRead(ctx, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		firstIdx := s.FirstLogIndex()
		lastIdx := s.LastLogIndex()
		clampedStart, clampedEnd, validRange := clampLogRange(start, end, firstIdx, lastIdx)
		if !validRange {
			s.logger.Debugw("Requested range is outside log bounds", "start", start, "end", end, "first", firstIdx, "last", lastIdx)
			entries = []types.LogEntry{}
			return nil
		}

		var err error
		if s.options.Features.EnableIndexMap {
			entries, err = s.getEntriesViaIndexMap(ctx, clampedStart, clampedEnd)
		} else {
			entries, err = s.getEntriesViaScan(ctx, clampedStart, clampedEnd)
		}
		return err
	})

	return entries, err
}

func (s *FileStorage) getEntriesViaIndexMap(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
	logPath := s.fileSystem.Path(s.dir, logFilename)
	entries, totalBytes, err := s.indexSvc.ReadInRange(ctx, logPath, s.indexToOffsetMap, start, end)
	if err != nil {
		return nil, fmt.Errorf("index map read failed: %w", err)
	}
	s.trackMetrics(len(entries), totalBytes)
	s.logger.Debugw("Read entries via index map", "start", start, "end", end, "count", len(entries), "bytes", totalBytes)
	return entries, nil
}

func (s *FileStorage) getEntriesViaScan(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
	logPath := s.fileSystem.Path(s.dir, logFilename)
	file, err := s.fileSystem.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open log for scanning: %v", ErrStorageIO, err)
	}
	defer file.Close()

	entries, err := s.logReader.ScanRange(ctx, file, start, end)
	if err != nil {
		return nil, fmt.Errorf("scan range failed: %w", err)
	}

	estimatedSize := int64(len(entries)) * int64(lengthPrefixSize+maxEntrySizeBytes)
	s.trackMetrics(len(entries), estimatedSize)
	s.logger.Debugw("Scanned log for entries", "start", start, "end", end, "count", len(entries), "bytes", estimatedSize)

	return entries, nil
}

// GetLogEntry returns the log entry at the given index.
//
// Returns:
//   - ErrEntryNotFound if the entry is missing or compacted.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) GetLogEntry(ctx context.Context, index types.Index) (types.LogEntry, error) {
	var entry types.LogEntry
	err := s.logLocker.DoRead(ctx, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		first := s.FirstLogIndex()
		last := s.LastLogIndex()

		if index < first || index > last {
			return ErrEntryNotFound
		}

		var err error
		if s.options.Features.EnableIndexMap {
			entry, err = s.getEntryViaIndexMap(ctx, index)
		} else {
			entry, err = s.getEntryViaScan(ctx, index)
		}
		return err
	})

	return entry, err
}

func (s *FileStorage) getEntryViaIndexMap(ctx context.Context, index types.Index) (types.LogEntry, error) {
	logPath := s.fileSystem.Path(s.dir, logFilename)
	entries, totalBytes, err := s.indexSvc.ReadInRange(ctx, logPath, s.indexToOffsetMap, index, index+1)
	if err != nil {
		return types.LogEntry{}, fmt.Errorf("index map lookup failed: %w", err)
	}
	if len(entries) != 1 {
		return types.LogEntry{}, ErrEntryNotFound
	}
	s.trackMetrics(1, totalBytes)
	s.logger.Debugw("Read entry via index map", "index", index, "bytes", totalBytes)
	return entries[0], nil
}

func (s *FileStorage) getEntryViaScan(ctx context.Context, index types.Index) (types.LogEntry, error) {
	logPath := s.fileSystem.Path(s.dir, logFilename)
	file, err := s.fileSystem.Open(logPath)
	if err != nil {
		return types.LogEntry{}, fmt.Errorf("%w: could not open log file: %v", ErrStorageIO, err)
	}
	defer file.Close()

	s.logger.Debugw("Scanning log for single entry", "index", index)

	var (
		totalBytes     int64
		entriesScanned int
	)

	for {
		if err := ctx.Err(); err != nil {
			s.logger.Warnw("Context cancelled during log scan", "index", index, "error", err)
			return types.LogEntry{}, err
		}

		entry, n, err := s.logReader.ReadNext(file)
		totalBytes += n
		entriesScanned++

		switch {
		case err == io.EOF:
			s.trackMetrics(0, totalBytes)
			s.logger.Debugw("Entry not found", "index", index, "bytesScanned", totalBytes, "entriesScanned", entriesScanned)
			return types.LogEntry{}, ErrEntryNotFound

		case err != nil:
			s.logger.Warnw("Error reading log entry", "index", index, "error", err)
			return types.LogEntry{}, fmt.Errorf("log scan error: %w", err)

		case entry.Index == index:
			s.trackMetrics(1, totalBytes)
			s.logger.Debugw("Entry found", "index", index, "bytesScanned", totalBytes, "entriesScanned", entriesScanned)
			return entry, nil

		case entry.Index > index:
			s.trackMetrics(0, totalBytes)
			s.logger.Debugw("Entry index exceeded", "index", index, "bytesScanned", totalBytes, "entriesScanned", entriesScanned)
			return types.LogEntry{}, ErrEntryNotFound
		}
	}
}

// TruncateLogSuffix deletes all log entries with indices >= the given index.
// Used to resolve log conflicts during replication.
//
// Returns:
//   - ErrIndexOutOfRange if the index is beyond the last log index.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) TruncateLogSuffix(ctx context.Context, index types.Index) error {
	return s.logLocker.DoWrite(ctx, func() error {
		first := s.FirstLogIndex()
		last := s.LastLogIndex()

		s.logger.Infow("Attempting to truncate log suffix", "index", index, "first", first, "last", last)

		if index > last+1 {
			err := fmt.Errorf("%w: truncate suffix index %d beyond last log index %d", ErrIndexOutOfRange, index, last)
			s.logger.Warnw("Truncate suffix index out of range", "error", err, "index", index, "last", last)
			return err
		}
		if index <= first {
			s.logger.Infow("Truncating entire log (suffix)", "index", index, "first", first)
			return s.truncateLogRange(ctx, 0, 0)
		}

		err := s.truncateLogRange(ctx, first, index)
		if err != nil {
			s.logger.Errorw("Failed to truncate log suffix", "error", err, "from", first, "to", index)
			return err
		}

		if s.options.Features.EnableIndexMap {
			s.indexToOffsetMap = s.indexSvc.TruncateAfter(s.indexToOffsetMap, index-1)
			s.logger.Debugw("Truncated index-to-offset map after", "index", index-1)
		}

		s.metrics.truncateSuffixOps.Add(1)
		s.logger.Infow("Successfully truncated log suffix", "newLast", index-1)
		return nil
	})
}

// TruncateLogPrefix deletes all log entries with indices < the given index.
// Used during log compaction after snapshotting.
//
// Returns:
//   - ErrIndexOutOfRange if the index is less than the first log index.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) TruncateLogPrefix(ctx context.Context, index types.Index) error {
	return s.logLocker.DoWrite(ctx, func() error {
		first := s.FirstLogIndex()
		last := s.LastLogIndex()

		s.logger.Infow("Attempting to truncate log prefix", "index", index, "first", first, "last", last)

		if index <= first {
			s.logger.Debugw("No prefix truncation needed", "index", index, "first", first)
			return nil
		}
		if index > last+1 {
			err := fmt.Errorf("%w: truncate prefix index %d beyond last log index %d", ErrIndexOutOfRange, index, last)
			s.logger.Warnw("Truncate prefix index out of range", "error", err, "index", index, "last", last)
			return err
		}

		err := s.truncateLogRange(ctx, index, last+1)
		if err != nil {
			s.logger.Errorw("Failed to truncate log prefix", "error", err, "from", index, "to", last+1)
			return err
		}

		if s.options.Features.EnableIndexMap {
			s.indexToOffsetMap = s.indexSvc.TruncateBefore(s.indexToOffsetMap, index)
			s.logger.Debugw("Truncated index-to-offset map before", "index", index)
		}

		s.metrics.truncatePrefixOps.Add(1)
		return nil
	})
}

func (s *FileStorage) truncateLogRange(ctx context.Context, keepStart, keepEnd types.Index) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entries, err := s.GetLogEntries(ctx, keepStart, keepEnd)
	if err != nil {
		return fmt.Errorf("failed to load log entries for truncation: %w", err)
	}
	offsets, err := s.logRewriter.Rewrite(ctx, entries)
	if err != nil {
		return fmt.Errorf("failed to rewrite log file: %w", err)
	}

	if len(entries) == 0 {
		s.setInMemoryLogBoundsLocked(0, 0)
		s.indexToOffsetMap = nil
	} else {
		first := entries[0].Index
		last := entries[len(entries)-1].Index
		s.setInMemoryLogBoundsLocked(first, last)

		if s.options.Features.EnableIndexMap {
			s.indexToOffsetMap = offsets
		}
	}

	if err := s.saveMetadataLocked(); err != nil {
		return fmt.Errorf("truncate succeeded but failed to save metadata: %w", err)
	}

	s.logger.Infow("Log truncated",
		"keepStart", keepStart,
		"keepEnd", keepEnd,
		"newFirst", s.FirstLogIndex(),
		"newLast", s.LastLogIndex(),
		"entryCount", len(entries),
	)

	return nil
}

// SaveSnapshot persists a snapshot and its metadata atomically.
// The snapshot should represent the full compacted state at a specific log index.
//
// Returns:
//   - ErrStorageIO on failure to persist the snapshot.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) SaveSnapshot(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
	return s.snapshotLocker.DoWrite(ctx, func() error {
		s.logger.Infow("Saving snapshot",
			"lastIndex", metadata.LastIncludedIndex,
			"lastTerm", metadata.LastIncludedTerm,
			"size", len(data),
		)

		if err := s.recoverySvc.CreateSnapshotRecoveryMarker(metadata); err != nil {
			return err
		}
		defer s.recoverySvc.RemoveSnapshotRecoveryMarker()

		if err := s.snapshotWriter.Write(ctx, metadata, data); err != nil {
			return err
		}

		s.metrics.snapshotSaveOps.Add(1)
		s.maybeTruncateAfterSnapshot(ctx, metadata)
		return nil
	})
}

// maybeTruncateAfterSnapshot performs log prefix truncation after a snapshot is saved.
// If AutoTruncateOnSnapshot is disabled, no truncation is performed.
// If enabled, truncation is done either asynchronously or synchronously depending
// on the EnableAsyncTruncation feature flag.
//
// This helps reduce log size and supports faster recovery while keeping the truncation
// process non-blocking in most performance-sensitive environments.
func (s *FileStorage) maybeTruncateAfterSnapshot(ctx context.Context, metadata types.SnapshotMetadata) {
	if !s.options.AutoTruncateOnSnapshot {
		return
	}
	if s.options.Features.EnableAsyncTruncation {
		s.asyncTruncateLogPrefixFromSnapshot(metadata)
	} else {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, s.getTruncationTimeout())
		defer cancel()

		if err := s.TruncateLogPrefix(ctxWithTimeout, metadata.LastIncludedIndex+1); err != nil {
			s.logger.Warnw("Synchronous log truncation after snapshot failed", "error", err)
		}
	}
}

// asyncTruncateLogPrefixFromSnapshot launches a background goroutine to truncate
// the Raft log prefix based on the snapshot metadata. It uses a timeout defined
// by TruncationTimeout in the storage options, defaulting to 30 seconds if unset.
func (s *FileStorage) asyncTruncateLogPrefixFromSnapshot(metadata types.SnapshotMetadata) {
	go func() {
		timeout := s.options.TruncationTimeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}

		tCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if err := s.TruncateLogPrefix(tCtx, metadata.LastIncludedIndex+1); err != nil {
			s.logger.Warnw("Async log truncation failed", "error", err)
		}
	}()
}

// getTruncationTimeout returns the configured timeout duration for log truncation
// after a snapshot. If no custom timeout is specified, it defaults to 30 seconds.
func (s *FileStorage) getTruncationTimeout() time.Duration {
	if s.options.TruncationTimeout > 0 {
		return s.options.TruncationTimeout
	}
	return 30 * time.Second
}

// LoadSnapshot retrieves the latest snapshot and its metadata.
//
// Returns:
//   - ErrNoSnapshot if no snapshot has been saved.
//   - ErrCorruptedSnapshot if the snapshot is unreadable or invalid.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	var metadata types.SnapshotMetadata
	var data []byte

	err := s.snapshotLocker.DoRead(ctx, func() error {
		m, d, err := s.snapshotReader.Read(ctx)
		if err != nil {
			return err
		}
		metadata = m
		data = d

		s.metrics.snapshotLoadOps.Add(1)
		s.metrics.readBytes.Add(uint64(len(data)))
		return nil
	})

	return metadata, data, err
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
	currentStatus := s.status.Load().(storageStatus)
	if currentStatus == storageStatusClosed {
		s.logger.Warnw("Close called on already closed storage")
		return nil
	}

	s.logMu.Lock()
	defer s.logMu.Unlock()
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	// Re-check status after acquiring locks to handle potential race conditions
	// where status changed between the initial check and acquiring locks.
	currentStatus = s.status.Load().(storageStatus)
	if currentStatus == storageStatusClosed {
		s.logger.Warnw("Storage was closed while acquiring locks for Close")
		return nil
	}

	var closeErr error
	if err := s.recoverySvc.RemoveRecoveryMarker(); err != nil {
		s.logger.Errorw("Failed to remove recovery marker during close", "error", err)
		closeErr = fmt.Errorf("%w: failed to remove recovery marker: %v", ErrStorageIO, err)
	}

	s.status.Store(storageStatusClosed)

	s.logger.Infow("Storage closed",
		"firstIndex", s.firstLogIndex.Load(),
		"lastIndex", s.lastLogIndex.Load(),
		"error", closeErr,
	)

	return closeErr
}

// setInMemoryLogBoundsLocked sets the in-memory first and last log indices atomically.
// It assumes s.logMu is already held and that range was validated using ValidateMetadataRange.
func (s *FileStorage) setInMemoryLogBoundsLocked(first, last types.Index) {
	s.firstLogIndex.Store(uint64(first))
	s.lastLogIndex.Store(uint64(last))
}

// maybeInitInMemoryLogBoundsLocked sets the in-memory log bounds if the first index is unset (zero).
// It assumes s.logMu is already held and that range was validated using ValidateMetadataRange.
func (s *FileStorage) maybeInitInMemoryLogBoundsLocked(first, last types.Index) {
	currentFirst := s.FirstLogIndex()
	if currentFirst == 0 {
		s.firstLogIndex.Store(uint64(first))
	}
	s.lastLogIndex.Store(uint64(last))
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
	expected := s.indexSvc.GetBounds(s.indexToOffsetMap, 0, 0)

	memFirst := s.firstLogIndex.Load()
	memLast := s.lastLogIndex.Load()

	if uint64(expected.NewFirst) != memFirst || uint64(expected.NewLast) != memLast {
		return fmt.Errorf("in-memory state inconsistent: expected (%d-%d), got (%d-%d)",
			expected.NewFirst, expected.NewLast,
			memFirst, memLast)
	}

	return nil
}

// rollbackInMemoryState reverts the in-memory log index bounds after a failed append.
// If isFirstInit is true, it means the first index had just been initialized and should be reset to zero.
func (s *FileStorage) rollbackInMemoryState(previousLast types.Index, isFirstInit bool) {
	s.lastLogIndex.Store(uint64(previousLast))
	if isFirstInit {
		s.firstLogIndex.Store(0)
	}
}

// rollbackIndexMapState reverts the last N entries from the index-to-offset map after a failed append.
// If N exceeds the map size, the map is cleared entirely.
func (s *FileStorage) rollbackIndexMapState(count int) {
	s.indexToOffsetMap = s.indexSvc.TruncateLast(s.indexToOffsetMap, count)
}

func (fs *FileStorage) trackMetrics(count int, bytes int64) {
	if fs.options.Features.EnableMetrics {
		fs.metrics.readOps.Add(1)
		fs.metrics.readBytes.Add(uint64(bytes))
	}
}
