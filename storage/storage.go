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

type fileStorageDeps struct {
	FileSystem      fileSystem
	Serializer      serializer
	LogAppender     logAppender
	LogReader       logEntryReader
	LogRewriter     logRewriter
	IndexService    indexService
	MetadataService metadataService
	RecoveryService recoveryService
	SystemInfo      systemInfo
	Logger          logger.Logger
}

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

	metrics
}

// NewFileStorage creates a new FileStorage with default options.
func NewFileStorage(cfg StorageConfig, logger logger.Logger) (Storage, error) {
	return NewFileStorageWithOptions(cfg, DefaultFileStorageOptions(), logger)
}

// NewFileStorageWithOptions creates a new FileStorage with custom options.
func NewFileStorageWithOptions(
	cfg StorageConfig,
	options FileStorageOptions,
	logger logger.Logger,
) (Storage, error) {
	deps, err := DefaultFileStorageDeps(cfg, options, logger)
	if err != nil {
		return nil, err
	}
	return newFileStorageWithDeps(cfg, options, deps)
}

func DefaultFileStorageDeps(
	cfg StorageConfig,
	options FileStorageOptions,
	logger logger.Logger,
) (fileStorageDeps, error) {
	if cfg.Dir == "" {
		return fileStorageDeps{}, errors.New("storage directory must be specified")
	}

	fileSystem := newFileSystem()
	serializer := getSerializer(options.Features.EnableBinaryFormat)
	systemInfo := NewSystemInfo()
	logReader := newLogEntryReader(maxEntrySizeBytes, lengthPrefixSize, serializer, logger)
	indexService := newIndexServiceWithReader(fileSystem, logReader, logger)
	metadataService := newMetadataServiceWithDeps(fileSystem, serializer, indexService, logger)
	recoveryService := newRecoveryService(
		fileSystem,
		serializer,
		logger,
		cfg.Dir,
		normalMode,
		indexService,
		metadataService,
		systemInfo,
	)
	writer := newLogWriter(serializer, logger)

	return fileStorageDeps{
		FileSystem: fileSystem,
		Serializer: serializer,
		LogAppender: newLogAppender(
			fileSystem.Path(cfg.Dir, logFilename),
			fileSystem,
			writer,
			logger,
			options.SyncOnAppend,
		),
		LogReader: logReader,
		LogRewriter: newLogRewriter(
			fileSystem.Path(cfg.Dir, logFilename),
			fileSystem,
			serializer,
			logger,
		),
		IndexService:    indexService,
		MetadataService: metadataService,
		RecoveryService: recoveryService,
		SystemInfo:      systemInfo,
		Logger:          logger,
	}, nil
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
	deps fileStorageDeps,
) (Storage, error) {
	if err := deps.FileSystem.MkdirAll(cfg.Dir, ownRWXOthRX); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %q: %w", cfg.Dir, err)
	}

	logger := deps.Logger.WithComponent("storage")

	s := &FileStorage{
		dir:         cfg.Dir,
		options:     options,
		fileSystem:  deps.FileSystem,
		serializer:  deps.Serializer,
		logAppender: deps.LogAppender,
		logReader:   deps.LogReader,
		logRewriter: deps.LogRewriter,
		indexSvc:    deps.IndexService,
		metadataSvc: deps.MetadataService,
		recoverySvc: deps.RecoveryService,
		logger:      logger,
	}

	s.logLocker = newRWOperationLocker(&s.logMu, logger, options, &s.metrics.slowOperations)
	s.snapshotLocker = newRWOperationLocker(
		&s.snapshotMu,
		logger,
		options,
		&s.metrics.slowOperations,
	)

	s.snapshotWriter = newSnapshotWriter(
		deps.FileSystem,
		deps.Serializer,
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

	s.snapshotReader = newSnapshotReader(deps.FileSystem, deps.Serializer, logger, cfg.Dir)

	s.status.Store(storageStatusInitializing)
	s.logger.Infow("Initializing storage", "dir", cfg.Dir)

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

	if err := s.recoverySvc.RemoveRecoveryMarker(); err != nil {
		underlyingErr := errors.Unwrap(err)
		if underlyingErr == nil {
			underlyingErr = err
		}
		if !s.fileSystem.IsNotExist(underlyingErr) {
			s.logger.Errorw("Failed to remove recovery marker during close", "error", err)
		} else {
			s.logger.Debugw("Recovery marker not found during close (normal after successful init)", "error", err)
		}
	}

	if s.options.Features.EnableMetrics {
		s.updateLogSizeMetricUnlocked()
		s.updateMetadataSizeMetricUnlocked()
		s.updateIndexMapSizeMetricUnlocked()
	}

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

// loadInitialState loads the initial persistent state. It handles the case where
// the state file doesn't exist (returning nil) but propagates other errors
// returned by LoadState.
func (s *FileStorage) loadInitialState() error {
	_, err := s.LoadState(context.Background())
	if err != nil {
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
	startTime := time.Now()

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

		if s.options.Features.EnableMetrics {
			s.trackOperationLatency("state", startTime, &s.metrics.stateErrors, err)
		}

		return types.PersistentState{}, fmt.Errorf(
			"%w: failed to read state file %q: %w",
			ErrStorageIO,
			statePath,
			err,
		)
	}

	deserializeStart := time.Now()
	state, err := s.serializer.UnmarshalState(data)

	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("state", startTime, &s.metrics.stateErrors, err)
		s.trackOperationLatency("deserialization", deserializeStart, nil, err)

		s.metrics.stateOps.Add(1)
		s.metrics.stateSize.Store(uint64(len(data)))
	}

	if err != nil {
		s.logger.Errorw("Failed to decode state file", "path", statePath, "error", err)
		return types.PersistentState{}, fmt.Errorf(
			"%w: failed to decode state file %q: %w",
			ErrCorruptedState,
			statePath,
			err,
		)
	}

	s.logger.Infow(
		"Successfully loaded persisted state",
		"path",
		statePath,
		"term",
		state.CurrentTerm,
		"votedFor",
		state.VotedFor,
	)
	return state, nil
}

// SaveState atomically persists the current term and voted-for candidate.
// Must be called after any update to persistent state.
//
// Returns:
//   - ErrStorageIO if the operation fails due to I/O issues.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) SaveState(ctx context.Context, state types.PersistentState) error {
	startTime := time.Now()

	if err := ctx.Err(); err != nil {
		s.logger.Warnw("SaveState aborted: context error", "error", err)
		return err
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	statePath := s.fileSystem.Path(s.dir, stateFilename)
	useAtomicWrite := s.options.Features.EnableAtomicWrites

	s.logger.Debugw(
		"Attempting to persist state",
		"path",
		statePath,
		"term",
		state.CurrentTerm,
		"votedFor",
		state.VotedFor,
	)

	serializeStart := time.Now()
	data, err := s.serializer.MarshalState(state)

	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("serialization", serializeStart, nil, err)
	}

	if err != nil {
		s.logger.Errorw("Failed to encode state", "path", statePath, "error", err)
		s.trackOperationLatency("state", startTime, &s.metrics.stateErrors, err)
		return fmt.Errorf("%w: failed to marshal state: %w", ErrStorageIO, err)
	}

	if err := s.fileSystem.WriteMaybeAtomic(statePath, data, ownRWOthR, useAtomicWrite); err != nil {
		s.logger.Errorw("Failed to write state", "path", statePath, "error", err)
		s.trackOperationLatency("state", startTime, &s.metrics.stateErrors, err)
		return fmt.Errorf("%w: failed to write metadata file: %w", ErrStorageIO, err)
	}

	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("state", startTime, nil, nil)

		s.metrics.stateOps.Add(1)
		s.metrics.stateSize.Store(uint64(len(data)))
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
	startTime := time.Now()

	var totalBytes uint64
	if s.options.Features.EnableMetrics {
		for _, entry := range entries {
			estimateEntrySize := uint64(len(entry.Command) + headerSize)
			totalBytes += estimateEntrySize
		}
	}

	currentLastIndex := types.Index(s.lastLogIndex.Load())

	err := s.logLocker.DoWrite(ctx, func() error {
		result, err := s.logAppender.Append(ctx, entries, currentLastIndex)
		if err != nil {
			s.logger.Warnw("Failed to append log entries",
				"entryCount", len(entries),
				"currentLastIndex", currentLastIndex,
				"error", err,
			)
			return err
		}

		commitErr := s.commitAppendChangesLocked(
			result.Offsets,
			result.FirstIndex,
			result.LastIndex,
			currentLastIndex,
		)

		if commitErr == nil && s.options.Features.EnableMetrics {
			s.metrics.appendOps.Add(1)
			s.metrics.appendBytes.Add(totalBytes)
			s.updateLogSizeMetricUnlocked()
		}

		return commitErr
	})

	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("append", startTime, &s.metrics.appendErrors, err)
	}

	return err
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
func (s *FileStorage) GetLogEntries(
	ctx context.Context,
	start, end types.Index,
) ([]types.LogEntry, error) {
	startTime := time.Now()

	if start >= end {
		if s.options.Features.EnableMetrics {
			s.trackOperationLatency("read", startTime, &s.metrics.readErrors, ErrInvalidLogRange)
		}
		return nil, ErrInvalidLogRange
	}

	var entries []types.LogEntry
	err := s.logLocker.DoRead(ctx, func() error {
		var err error
		entries, err = s.getLogEntriesUnlocked(ctx, start, end)
		return err
	})

	// Track overall latency for the entire operation, including lock wait time.
	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("read", startTime, &s.metrics.readErrors, err)
	}

	return entries, err
}

func (s *FileStorage) getLogEntriesUnlocked(
	ctx context.Context,
	start, end types.Index,
) ([]types.LogEntry, error) {
	if start == end {
		return []types.LogEntry{}, nil
	}

	firstIdx := s.FirstLogIndex()
	lastIdx := s.LastLogIndex()
	clampedStart, clampedEnd, validRange := clampLogRange(start, end, firstIdx, lastIdx)
	if !validRange {
		s.logger.Debugw(
			"Requested range is outside log bounds",
			"start",
			start,
			"end",
			end,
			"first",
			firstIdx,
			"last",
			lastIdx,
		)
		return []types.LogEntry{}, ErrIndexOutOfRange
	}

	if s.options.Features.EnableIndexMap {
		return s.getEntriesViaIndexMap(ctx, clampedStart, clampedEnd)
	}

	return s.getEntriesViaScan(ctx, clampedStart, clampedEnd)
}

func (s *FileStorage) getEntriesViaIndexMap(
	ctx context.Context,
	start, end types.Index,
) ([]types.LogEntry, error) {
	logPath := s.fileSystem.Path(s.dir, logFilename)
	entries, totalBytes, err := s.indexSvc.ReadInRange(ctx, logPath, s.indexToOffsetMap, start, end)
	if err != nil {
		return nil, fmt.Errorf("index map read failed: %w", err)
	}

	if s.options.Features.EnableMetrics {
		s.trackMetrics(len(entries), totalBytes)
	}

	s.logger.Debugw(
		"Read entries via index map",
		"start",
		start,
		"end",
		end,
		"count",
		len(entries),
		"bytes",
		totalBytes,
	)
	return entries, nil
}

func (s *FileStorage) getEntriesViaScan(
	ctx context.Context,
	start, end types.Index,
) ([]types.LogEntry, error) {
	logPath := s.fileSystem.Path(s.dir, logFilename)
	file, err := s.fileSystem.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open log for scanning: %w", ErrStorageIO, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			s.logger.Errorw("error closing file: %v", err)
		}
	}()

	entries, err := s.logReader.ScanRange(ctx, file, start, end)
	if err != nil {
		return nil, fmt.Errorf("scan range failed: %w", err)
	}

	estimatedSize := int64(len(entries)) * int64(lengthPrefixSize+maxEntrySizeBytes)
	if s.options.Features.EnableMetrics {
		s.trackMetrics(len(entries), estimatedSize)
	}

	s.logger.Debugw(
		"Scanned log for entries",
		"start",
		start,
		"end",
		end,
		"count",
		len(entries),
		"bytes",
		estimatedSize,
	)

	return entries, nil
}

// GetLogEntry returns the log entry at the given index.
//
// Returns:
//   - ErrEntryNotFound if the entry is missing or compacted.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) GetLogEntry(ctx context.Context, index types.Index) (types.LogEntry, error) {
	startTime := time.Now()

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

		var readErr error
		if s.options.Features.EnableIndexMap {
			entry, readErr = s.getEntryViaIndexMap(ctx, index)
		} else {
			entry, readErr = s.getEntryViaScan(ctx, index)
		}
		return readErr
	})

	// Track overall latency for the entire operation, including lock wait time.
	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("read", startTime, &s.metrics.readErrors, err)
	}

	return entry, err
}

func (s *FileStorage) getEntryViaIndexMap(
	ctx context.Context,
	index types.Index,
) (types.LogEntry, error) {
	logPath := s.fileSystem.Path(s.dir, logFilename)
	entries, totalBytes, err := s.indexSvc.ReadInRange(
		ctx,
		logPath,
		s.indexToOffsetMap,
		index,
		index+1,
	)
	if err != nil {
		return types.LogEntry{}, fmt.Errorf("index map lookup failed: %w", err)
	}
	if len(entries) != 1 {
		return types.LogEntry{}, ErrEntryNotFound
	}

	if s.options.Features.EnableMetrics {
		s.trackMetrics(1, totalBytes)
	}

	s.logger.Debugw("Read entry via index map", "index", index, "bytes", totalBytes)
	return entries[0], nil
}

func (s *FileStorage) getEntryViaScan(
	ctx context.Context,
	index types.Index,
) (types.LogEntry, error) {
	logPath := s.fileSystem.Path(s.dir, logFilename)
	file, err := s.fileSystem.Open(logPath)
	if err != nil {
		return types.LogEntry{}, fmt.Errorf("%w: could not open log file: %w", ErrStorageIO, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			s.logger.Errorw("error closing file: %v", err)
		}
	}()

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
		case errors.Is(err, io.EOF):
			if s.options.Features.EnableMetrics {
				s.trackMetrics(0, totalBytes)
			}
			s.logger.Debugw(
				"Entry not found",
				"index",
				index,
				"bytesScanned",
				totalBytes,
				"entriesScanned",
				entriesScanned,
			)
			return types.LogEntry{}, ErrEntryNotFound

		case err != nil:
			s.logger.Warnw("Error reading log entry", "index", index, "error", err)
			return types.LogEntry{}, fmt.Errorf("log scan error: %w", err)

		case entry.Index == index:
			if s.options.Features.EnableMetrics {
				s.trackMetrics(1, totalBytes)
			}
			s.logger.Debugw(
				"Entry found",
				"index",
				index,
				"bytesScanned",
				totalBytes,
				"entriesScanned",
				entriesScanned,
			)
			return entry, nil

		case entry.Index > index:
			if s.options.Features.EnableMetrics {
				s.trackMetrics(0, totalBytes)
			}
			s.logger.Debugw(
				"Entry index exceeded",
				"index",
				index,
				"bytesScanned",
				totalBytes,
				"entriesScanned",
				entriesScanned,
			)
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
	startTime := time.Now()

	err := s.logLocker.DoWrite(ctx, func() error {
		first := s.FirstLogIndex()
		last := s.LastLogIndex()

		s.logger.Infow(
			"Attempting to truncate log suffix",
			"index",
			index,
			"first",
			first,
			"last",
			last,
		)

		if index > last+1 {
			err := fmt.Errorf(
				"%w: truncate suffix index %d beyond last log index %d",
				ErrIndexOutOfRange,
				index,
				last,
			)
			s.logger.Warnw(
				"Truncate suffix index out of range",
				"error",
				err,
				"index",
				index,
				"last",
				last,
			)
			return err
		}
		if index <= first {
			s.logger.Infow("Truncating entire log (suffix)", "index", index, "first", first)
			return s.truncateLogRange(ctx, 0, 0)
		}

		err := s.truncateLogRange(ctx, first, index)
		if err != nil {
			s.logger.Errorw(
				"Failed to truncate log suffix",
				"error",
				err,
				"from",
				first,
				"to",
				index,
			)
			return err
		}

		if s.options.Features.EnableIndexMap {
			s.indexToOffsetMap = s.indexSvc.TruncateAfter(s.indexToOffsetMap, index-1)
			s.logger.Debugw("Truncated index-to-offset map after", "index", index-1)
		}

		// Increment counter only on successful attempt within the lock
		if s.options.Features.EnableMetrics {
			s.metrics.truncateSuffixOps.Add(1)
		}

		s.logger.Infow("Successfully truncated log suffix", "newLast", index-1)
		return nil
	})

	// Track overall latency for the entire operation, including lock wait time.
	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("truncate_suffix", startTime, nil, err)
	}

	return err
}

// TruncateLogPrefix deletes all log entries with indices < the given index.
// Used during log compaction after snapshotting.
//
// Returns:
//   - ErrIndexOutOfRange if the index is less than the first log index.
//   - context.Canceled or context.DeadlineExceeded if the context is canceled or expired.
func (s *FileStorage) TruncateLogPrefix(ctx context.Context, index types.Index) error {
	startTime := time.Now()

	err := s.logLocker.DoWrite(ctx, func() error {
		first := s.FirstLogIndex()
		last := s.LastLogIndex()

		s.logger.Infow(
			"Attempting to truncate log prefix",
			"index",
			index,
			"first",
			first,
			"last",
			last,
		)

		if index <= first {
			s.logger.Debugw("No prefix truncation needed", "index", index, "first", first)
			return nil
		}
		if index > last+1 {
			err := fmt.Errorf(
				"%w: truncate prefix index %d beyond last log index %d",
				ErrIndexOutOfRange,
				index,
				last,
			)
			s.logger.Warnw(
				"Truncate prefix index out of range",
				"error",
				err,
				"index",
				index,
				"last",
				last,
			)
			return err
		}

		err := s.truncateLogRange(ctx, index, last+1)
		if err != nil {
			s.logger.Errorw(
				"Failed to truncate log prefix",
				"error",
				err,
				"from",
				index,
				"to",
				last+1,
			)
			return err
		}

		if s.options.Features.EnableIndexMap {
			s.indexToOffsetMap = s.indexSvc.TruncateBefore(s.indexToOffsetMap, index)
			s.logger.Debugw("Truncated index-to-offset map before", "index", index)
		}

		// Increment counter only on successful attempt within the lock
		if s.options.Features.EnableMetrics {
			s.metrics.truncatePrefixOps.Add(1)
		}
		s.logger.Infow("Successfully truncated log prefix", "newFirst", first)
		return nil
	})

	// Track overall latency for the entire operation, including lock wait time.
	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("truncate_prefix", startTime, nil, err)
	}

	return err
}

// truncateLogRange rewrites the log file keeping only entries in [keepStart, keepEnd).
// assumes logLocker write lock is held.
func (s *FileStorage) truncateLogRange(ctx context.Context, keepStart, keepEnd types.Index) error {
	startTime := time.Now()

	if err := ctx.Err(); err != nil {
		return err
	}

	var oldLogSize uint64
	if s.options.Features.EnableMetrics {
		logPath := s.fileSystem.Path(s.dir, logFilename)
		info, err := s.fileSystem.Stat(logPath)
		if err == nil {
			oldLogSize = uint64(info.Size())
		}
	}

	entries, err := s.getLogEntriesUnlocked(ctx, keepStart, keepEnd)
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

	if s.options.Features.EnableMetrics {
		s.metrics.compactionOps.Add(1)
		s.updateLogSizeMetricUnlocked()
		s.updateIndexMapSizeMetricUnlocked()

		newLogSize := s.metrics.logSize.Load()
		if oldLogSize > newLogSize {
			spaceReduced := oldLogSize - newLogSize
			s.logger.Infow(
				"Log compaction saved space",
				"bytesSaved",
				spaceReduced,
				"reductionPercentage",
				fmt.Sprintf("%.2f%%", float64(spaceReduced)/float64(oldLogSize)*100),
			)
		}

		s.trackOperationLatency("compaction", startTime, nil, nil)
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
func (s *FileStorage) SaveSnapshot(
	ctx context.Context,
	metadata types.SnapshotMetadata,
	data []byte,
) error {
	startTime := time.Now()

	err := s.snapshotLocker.DoWrite(ctx, func() error {
		s.logger.Infow("Saving snapshot",
			"lastIndex", metadata.LastIncludedIndex,
			"lastTerm", metadata.LastIncludedTerm,
			"size", len(data),
		)

		if err := s.recoverySvc.CreateSnapshotRecoveryMarker(metadata); err != nil {
			return err
		}
		defer func() {
			if err := s.recoverySvc.RemoveSnapshotRecoveryMarker(); err != nil {
				s.logger.Errorw("Failed to remove snapshot recovery marker", "error", err)
			}
		}()

		if err := s.snapshotWriter.Write(ctx, metadata, data); err != nil {
			return err
		}

		if s.options.Features.EnableMetrics {
			s.metrics.snapshotSaveOps.Add(1)
			s.metrics.snapshotSize.Store(uint64(len(data)))
		}

		s.maybeTruncateAfterSnapshot(ctx, metadata)
		return nil
	})

	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("snapshot_save", startTime, &s.metrics.snapshotErrors, err)
	}

	return err
}

// maybeTruncateAfterSnapshot performs log prefix truncation after a snapshot is saved.
// If AutoTruncateOnSnapshot is disabled, no truncation is performed.
// If enabled, truncation is done either asynchronously or synchronously depending
// on the EnableAsyncTruncation feature flag.
//
// This helps reduce log size and supports faster recovery while keeping the truncation
// process non-blocking in most performance-sensitive environments.
func (s *FileStorage) maybeTruncateAfterSnapshot(
	ctx context.Context,
	metadata types.SnapshotMetadata,
) {
	if !s.options.AutoTruncateOnSnapshot {
		return
	}
	if s.options.Features.EnableAsyncTruncation {
		s.asyncTruncateLogPrefixFromSnapshot(metadata)
	} else {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, s.getTruncationTimeout())
		defer cancel()

		err := s.TruncateLogPrefix(ctxWithTimeout, metadata.LastIncludedIndex+1)

		if err != nil {
			s.logger.Warnw("Synchronous log truncation after snapshot failed",
				"error", err,
				"index", metadata.LastIncludedIndex+1,
			)
		} else {
			s.logger.Infow("Synchronous log truncation after snapshot succeeded",
				"index", metadata.LastIncludedIndex+1,
			)
		}
	}
}

// asyncTruncateLogPrefixFromSnapshot launches a background goroutine to truncate
// the Raft log prefix based on the snapshot metadata. It uses a timeout defined
// by TruncationTimeout in the storage options, defaulting to 30 seconds if unset.
func (s *FileStorage) asyncTruncateLogPrefixFromSnapshot(metadata types.SnapshotMetadata) {
	go func() {
		timeout := s.getTruncationTimeout()

		tCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		s.logger.Infow("Starting async log truncation after snapshot",
			"truncateAfterIndex", metadata.LastIncludedIndex+1,
			"timeout", timeout,
		)

		if err := s.TruncateLogPrefix(tCtx, metadata.LastIncludedIndex+1); err != nil {
			s.logger.Warnw("Async log truncation after snapshot failed",
				"error", err,
				"truncateAfterIndex", metadata.LastIncludedIndex+1,
			)
		} else {
			s.logger.Infow("Async log truncation after snapshot succeeded",
				"truncateAfterIndex", metadata.LastIncludedIndex+1,
			)
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
	startTime := time.Now()

	var (
		metadata types.SnapshotMetadata
		data     []byte
	)

	err := s.snapshotLocker.DoRead(ctx, func() error {
		var readErr error
		metadata, data, readErr = s.snapshotReader.Read(ctx)
		if readErr != nil {
			return readErr
		}

		if s.options.Features.EnableMetrics {
			s.metrics.snapshotLoadOps.Add(1)
			s.metrics.readBytes.Add(uint64(len(data)))
			s.metrics.snapshotSize.Store(uint64(len(data)))
		}
		return nil
	})

	if s.options.Features.EnableMetrics {
		s.trackOperationLatency("snapshot_load", startTime, &s.metrics.snapshotErrors, err)
	}

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
//   - nil if the storage is already closed.
//   - ErrStorageIO if cleanup fails.
func (s *FileStorage) Close() error {
	startTime := time.Now()

	if !s.status.CompareAndSwap(storageStatusReady, storageStatusClosed) {
		currentStatus := s.status.Load().(storageStatus)
		if currentStatus == storageStatusClosed {
			s.logger.Warnw("Close called on already closed storage")
			return nil
		}
		s.logger.Errorw(
			"Close called on storage in unexpected status",
			"status",
			currentStatus.string(),
		)
		s.status.Store(storageStatusClosed)
	}

	var closeErr error
	if err := s.recoverySvc.RemoveRecoveryMarker(); err != nil {
		underlyingErr := errors.Unwrap(err)
		if underlyingErr == nil {
			underlyingErr = err
		}
		if !s.fileSystem.IsNotExist(underlyingErr) {
			s.logger.Errorw("Failed to remove recovery marker during close", "error", err)
			closeErr = fmt.Errorf("%w: failed to remove recovery marker: %w", ErrStorageIO, err)
		} else {
			s.logger.Debugw("Recovery marker not found during close (normal after successful init)", "error", err)
		}
	}

	if s.options.Features.EnableMetrics {
		if s.options.Features.EnableMetrics {
			s.trackOperationLatency("close", startTime, nil, closeErr)
		}
	}

	s.logger.Infow("Storage closed", "error", closeErr)

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

// trackOperationLatency records latency and errors for a specific operation type.
func (s *FileStorage) trackOperationLatency(
	opType string,
	startTime time.Time,
	errorCounter *atomic.Uint64,
	err error,
) {
	if !s.options.Features.EnableMetrics {
		return
	}

	durationNs := uint64(time.Since(startTime).Nanoseconds())

	var sumCounter, countCounter, maxCounter *atomic.Uint64
	var sampleOpType string = ""

	switch opType {
	case "append":
		sumCounter = &s.metrics.appendLatencySum
		countCounter = &s.metrics.appendLatencyCount
		maxCounter = &s.metrics.appendLatencyMax
		sampleOpType = "append"
	case "read": //
		sumCounter = &s.metrics.readLatencySum
		countCounter = &s.metrics.readLatencyCount
		maxCounter = &s.metrics.readLatencyMax
		sampleOpType = "read"
	case "state": //
		sumCounter = &s.metrics.stateLatencySum
		countCounter = &s.metrics.stateLatencyCount
		maxCounter = &s.metrics.stateLatencyMax
		sampleOpType = "state"
	case "snapshot_save", "snapshot_load":
		sumCounter = &s.metrics.snapshotLatencySum
		countCounter = &s.metrics.snapshotLatencyCount
		maxCounter = &s.metrics.snapshotLatencyMax
		sampleOpType = "snapshot"
	case "serialization":
		sumCounter = &s.metrics.serializationTimeSum
		countCounter = &s.metrics.serializationCount
		maxCounter = &s.metrics.serializationMax
	case "deserialization":
		sumCounter = &s.metrics.deserializationTimeSum
		countCounter = &s.metrics.deserializationCount
		maxCounter = &s.metrics.deserializationMax
	default:
		// Operation type not configured for detailed latency tracking
		// Update error counter if provided
		if err != nil && errorCounter != nil {
			errorCounter.Add(1)
		}
		return
	}

	if sumCounter != nil {
		sumCounter.Add(durationNs)
	}
	if countCounter != nil {
		countCounter.Add(1)
	}
	if err != nil && errorCounter != nil {
		errorCounter.Add(1)
	}

	if maxCounter != nil {
		updateMax(maxCounter, durationNs)
	}
	if sampleOpType != "" {
		s.metrics.recordLatencySample(sampleOpType, durationNs)
	}
}

// trackMetrics updates counters after a read operation request completes.
// It increments the readOps counter by 1, adds the number of entries found,
// and adds the total bytes read/scanned.
func (s *FileStorage) trackMetrics(numEntriesFound int, bytesReadOrScanned int64) {
	if !s.options.Features.EnableMetrics {
		return
	}

	// readOps counts the number of read requests initiated.
	s.metrics.readOps.Add(1)
	// readEntries counts the total number of entries returned by read requests.
	s.metrics.readEntries.Add(uint64(numEntriesFound))
	// readBytes counts the bytes transferred or scanned during the request.
	s.metrics.readBytes.Add(uint64(bytesReadOrScanned))
}

// updateLogSizeMetricUnlocked updates the log file size metric.
func (s *FileStorage) updateLogSizeMetricUnlocked() {
	if !s.options.Features.EnableMetrics {
		return
	}

	logPath := s.fileSystem.Path(s.dir, logFilename)
	info, err := s.fileSystem.Stat(logPath)
	if err != nil {
		s.logger.Debugw("Could not stat log file for size metric", "path", logPath, "error", err)
		return
	}

	s.metrics.logSize.Store(uint64(info.Size()))
}

// updateMetadataSizeMetricUnlocked updates the metadata file size metric.
func (s *FileStorage) updateMetadataSizeMetricUnlocked() {
	if !s.options.Features.EnableMetrics {
		return
	}

	metadataPath := s.fileSystem.Path(s.dir, metadataFilename)
	info, err := s.fileSystem.Stat(metadataPath)
	if err != nil {
		s.logger.Debugw(
			"Could not stat metadata file for size metric",
			"path",
			metadataPath,
			"error",
			err,
		)
		return
	}

	s.metrics.metadataSize.Store(uint64(info.Size()))
}

// updateIndexMapSizeMetricUnlocked updates the index map entry count metric.
func (s *FileStorage) updateIndexMapSizeMetricUnlocked() {
	if !s.options.Features.EnableMetrics {
		return
	}

	size := uint64(len(s.indexToOffsetMap))
	s.metrics.indexMapSize.Store(size)
	s.logger.Debugw("Updated index map size metric", "entries", size)
}

// ResetMetrics clears all collected performance and usage metrics counters and samples.
// This operation only has an effect if metrics collection is enabled in the storage options.
func (s *FileStorage) ResetMetrics() {
	s.metrics.Reset()
}

// GetMetrics returns a map containing the current values of all collected metrics.
// Keys are strings identifying the metric (e.g., "append_ops", "avg_read_latency_us").
// Values are uint64 representations of the metric value.
// Returns nil if metrics collection is disabled in the storage options.
func (s *FileStorage) GetMetrics() map[string]uint64 {
	if !s.options.Features.EnableMetrics {
		return nil
	}
	return s.metrics.ToMap()
}

// GetMetricsSummary returns a formatted, human-readable string summarizing key metrics.
// Includes operation counts, latencies (avg, max, p95, p99), storage sizes, and error rates.
// Returns a fixed string indicating "Storage metrics disabled" if metrics collection is disabled.
func (s *FileStorage) GetMetricsSummary() string {
	if !s.options.Features.EnableMetrics {
		return "Storage metrics disabled"
	}
	return s.metrics.Summary()
}
