package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// StorageConfig contains configuration for storage initialization.
type StorageConfig struct {
	// Dir is the directory where storage files are kept
	Dir string
}

// FileStorage implements the Storage interface using the local filesystem.
// It provides durable persistence for Raft state, log entries, and snapshots.
type FileStorage struct {
	// dir is the root directory where all storage files (log, state, snapshots) are kept.
	dir string
	// options holds the configuration settings for the FileStorage behavior and features.
	options FileStorageOptions

	// stateMu protects access to the persistent state file (state.json).
	stateMu sync.RWMutex
	// logMu protects access to the log file (log.dat), metadata file (metadata.json),
	// and the in-memory index map (indexToOffsetMap).
	logMu sync.RWMutex
	// snapshotMu protects access to snapshot files (snapshot_meta.json, snapshot.dat).
	snapshotMu sync.RWMutex

	// firstLogIndex holds the index of the first log entry currently available in log.dat.
	// This value is updated by log truncation operations (TruncateLogPrefix, snapshots).
	firstLogIndex atomic.Uint64
	// lastLogIndex holds the index of the most recent log entry appended to log.dat.
	lastLogIndex atomic.Uint64

	// indexToOffsetMap provides an in-memory mapping from log entry Index to its byte offset
	// within log.dat. This is used for efficient lookups when Features.EnableIndexMap is true.
	indexToOffsetMap []IndexOffsetPair

	// status stores the current operational status of the storage layer: a StorageStatus value.
	status atomic.Value

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
func NewFileStorage(cfg StorageConfig, logger logger.Logger) (*FileStorage, error) {
	return NewFileStorageWithOptions(cfg, DefaultFileStorageOptions(), logger)
}

// NewFileStorageWithOptions creates a new FileStorage with custom options.
func NewFileStorageWithOptions(cfg StorageConfig, options FileStorageOptions, logger logger.Logger) (*FileStorage, error) {
	if err := os.MkdirAll(cfg.Dir, OwnRWXOthRX); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %q: %w", cfg.Dir, err)
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	// Set component in logger
	logger = logger.WithComponent("storage")

	fs := &FileStorage{
		dir:              cfg.Dir,
		options:          options,
		indexToOffsetMap: make([]IndexOffsetPair, 0, DefaultIndexMapInitialCapacity), // Pre-allocate for performance
		logger:           logger,
	}

	// Set initial status
	fs.status.Store(StorageStatusInitializing)
	fs.logger.Infow("Initializing storage", "dir", cfg.Dir)

	// Initialize the storage
	if err := fs.initialize(); err != nil {
		return nil, err
	}

	return fs, nil
}

// LoadState retrieves the most recently persisted PersistentState
func (fs *FileStorage) LoadState(ctx context.Context) (types.PersistentState, error) {
	select {
	case <-ctx.Done():
		fs.logger.Warnw("LoadState aborted: context cancelled", "error", ctx.Err())
		return types.PersistentState{}, ctx.Err()
	default:
	}

	fs.stateMu.RLock()
	defer fs.stateMu.RUnlock()

	statePath := fs.stateFile()
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			defaultState := types.PersistentState{CurrentTerm: 0, VotedFor: ""}
			fs.logger.Infow("State file not found, returning default state",
				"path", statePath,
				"defaultTerm", defaultState.CurrentTerm,
			)
			return defaultState, nil
		}
		fs.logger.Errorw("Failed to read state file",
			"error", err,
			"path", statePath,
		)
		return types.PersistentState{}, fmt.Errorf("failed to read state file: %w", ErrStorageIO)
	}

	var state types.PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		fs.logger.Errorw("Failed to unmarshal persisted state",
			"error", err,
			"data", string(data),
		)
		return types.PersistentState{}, fmt.Errorf("failed to unmarshal state: %w", ErrCorruptedState)
	}

	fs.logger.Debugw("Successfully loaded persisted state",
		"term", state.CurrentTerm,
		"votedFor", state.VotedFor,
		"path", statePath,
	)
	return state, nil
}

// stateFile returns the path to the state file.
func (fs *FileStorage) stateFile() string {
	return filepath.Join(fs.dir, "state.json")
}

// metadataFile returns the path to the metadata file.
func (fs *FileStorage) metadataFile() string {
	return filepath.Join(fs.dir, "metadata.json")
}

// logFile returns the path to the log file.
func (fs *FileStorage) logFile() string {
	return filepath.Join(fs.dir, "log.dat")
}

// snapshotMetadataFile returns the path to the snapshot metadata file.
func (fs *FileStorage) snapshotMetadataFile() string {
	return filepath.Join(fs.dir, "snapshot_meta.json")
}

// snapshotDataFile returns the path to the snapshot data file.
func (fs *FileStorage) snapshotDataFile() string {
	return filepath.Join(fs.dir, "snapshot.dat")
}

// snapshotMarkerPath returns the path to the snapshot marker file.
func (fs *FileStorage) snapshotMarkerPath() string {
	return filepath.Join(fs.dir, "snapshot.marker")
}
