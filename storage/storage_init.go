package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jathurchan/raftlock/types"
)

// initialize handles storage initialization and recovery.
func (fs *FileStorage) initialize() error {
	// Check for recovery markers
	recoveryNeeded, err := fs.checkForRecoveryMarkers()
	if err != nil {
		return fmt.Errorf("failed checking recovery markers: %w", err)
	}

	if recoveryNeeded {
		fs.status.Store(StorageStatusRecovering)
		fs.logger.Infow("Recovery markers found, entering recovery mode")

		if err := fs.performRecovery(); err != nil {
			return fmt.Errorf("recovery failed: %w", err)
		}
	}

	// Create a new recovery marker
	if err := fs.createRecoveryMarker(); err != nil {
		return fmt.Errorf("failed to create recovery marker: %w", err)
	}

	// Clean up any temporary files
	if err := fs.cleanupTempFiles(); err != nil {
		fs.logger.Warnw("Failed to clean up temporary files", "error", err)
	}

	// Initialize state and metadata
	if err := fs.initStateAndMetadata(); err != nil {
		return err
	}

	// Remove recovery marker
	fs.removeRecoveryMarker()

	// Set status to ready
	fs.status.Store(StorageStatusReady)
	fs.logger.Infow("Storage initialized successfully",
		"firstIndex", fs.firstLogIndex.Load(),
		"lastIndex", fs.lastLogIndex.Load())

	return nil
}

// checkForRecoveryMarkers looks for evidence of interrupted operations.
func (fs *FileStorage) checkForRecoveryMarkers() (bool, error) {
	recoveryMarker := fs.recoveryMarkerPath()
	snapshotMarker := fs.snapshotMarkerPath()

	recoveryExists, err := fs.fileExists(recoveryMarker)
	if err != nil {
		return false, fmt.Errorf("%w: failed to check recovery marker: %v", ErrStorageIO, err)
	}

	snapshotExists, err := fs.fileExists(snapshotMarker)
	if err != nil {
		return false, fmt.Errorf("%w: failed to check snapshot marker: %v", ErrStorageIO, err)
	}

	return recoveryExists || snapshotExists, nil
}

// fileExists checks if a file exists at the given path
func (fs *FileStorage) fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// performRecovery attempts to recover from interrupted operations.
func (fs *FileStorage) performRecovery() error {
	if err := fs.recoverFromSnapshotOperation(); err != nil {
		return fmt.Errorf("snapshot recovery failed: %w", err)
	}

	if err := fs.checkAndRepairConsistency(); err != nil {
		return fmt.Errorf("consistency repair failed: %w", err)
	}

	fs.logger.Infow("Recovery completed successfully")
	return nil
}

// recoverFromSnapshotOperation recovers from interrupted snapshot operations.
func (fs *FileStorage) recoverFromSnapshotOperation() error {
	markerPath := fs.snapshotMarkerPath()
	exists, err := fs.fileExists(markerPath)
	if err != nil {
		return fmt.Errorf("%w: failed to check snapshot marker: %v", ErrStorageIO, err)
	}

	if !exists {
		return nil // No recovery needed
	}

	fs.logger.Infow("Found snapshot marker, recovering from interrupted snapshot operation")

	// Read marker to determine progress
	markerData, err := os.ReadFile(markerPath)
	if err != nil {
		return fmt.Errorf("%w: failed to read snapshot marker: %v", ErrStorageIO, err)
	}

	markerStr := string(markerData)
	metaCommitted := strings.Contains(markerStr, "meta_committed=true")

	// Perform snapshot recovery
	if err := fs.handleSnapshotRecovery(metaCommitted); err != nil {
		return err
	}

	// Remove marker after recovery
	if err := fs.removeFile(markerPath); err != nil {
		fs.logger.Warnw("Failed to remove snapshot marker", "path", markerPath, "error", err)
	}

	return nil
}

// handleSnapshotRecovery handles recovery based on snapshot marker state.
func (fs *FileStorage) handleSnapshotRecovery(metaCommitted bool) error {
	// Check for temporary files
	metaFile := fs.snapshotMetadataFile()
	dataFile := fs.snapshotDataFile()
	tmpMetaFile := metaFile + ".tmp"
	tmpDataFile := dataFile + ".tmp"

	tmpMetaExists, _ := fs.fileExists(tmpMetaFile)
	tmpDataExists, _ := fs.fileExists(tmpDataFile)
	metaExists, _ := fs.fileExists(metaFile)
	dataExists, _ := fs.fileExists(dataFile)

	if !metaCommitted {
		// Metadata not committed yet, clean up temp files
		return fs.cleanupIncompleteSnapshot(tmpMetaFile, tmpDataFile, tmpMetaExists, tmpDataExists)
	}

	if metaExists && tmpDataExists && !dataExists {
		// Metadata committed but data not yet, try to complete
		return fs.completeSnapshotDataCommit(tmpDataFile, dataFile, metaFile)
	}

	return nil
}

// cleanupIncompleteSnapshot removes temporary files for incomplete snapshot operations.
func (fs *FileStorage) cleanupIncompleteSnapshot(tmpMetaFile, tmpDataFile string, tmpMetaExists, tmpDataExists bool) error {
	if tmpMetaExists {
		if err := fs.removeFile(tmpMetaFile); err != nil {
			fs.logger.Warnw("Failed to remove temporary metadata file", "error", err)
		}
	}

	if tmpDataExists {
		if err := fs.removeFile(tmpDataFile); err != nil {
			fs.logger.Warnw("Failed to remove temporary data file", "error", err)
		}
	}

	fs.logger.Infow("Cleaned up incomplete snapshot operation")
	return nil
}

// completeSnapshotDataCommit completes an interrupted snapshot data commit.
func (fs *FileStorage) completeSnapshotDataCommit(tmpDataFile, dataFile, metaFile string) error {
	if err := os.Rename(tmpDataFile, dataFile); err != nil {
		fs.logger.Warnw("Failed to complete snapshot data file rename", "error", err)

		// Handle based on recovery mode
		if fs.options.RecoveryMode == RecoveryModeAggressive {
			if err := fs.removeFile(metaFile); err != nil {
				fs.logger.Warnw("Failed to remove snapshot metadata", "error", err)
			}
			fs.logger.Warnw("Removed snapshot metadata due to missing data file")
		}

		return fmt.Errorf("%w: failed to complete snapshot data commit: %v", ErrStorageIO, err)
	}

	fs.logger.Infow("Completed interrupted snapshot operation")
	return nil
}

// checkAndRepairConsistency verifies consistency between metadata and log file.
func (fs *FileStorage) checkAndRepairConsistency() error {
	metaFile := fs.metadataFile()
	logFile := fs.logFile()

	metaExists, err := fs.fileExists(metaFile)
	if err != nil {
		return fmt.Errorf("%w: failed to check metadata file: %v", ErrStorageIO, err)
	}

	logExists, err := fs.fileExists(logFile)
	if err != nil {
		return fmt.Errorf("%w: failed to check log file: %v", ErrStorageIO, err)
	}

	if metaExists && !logExists {
		return fs.handleMissingLogFile()
	} else if logExists && !metaExists {
		fs.logger.Warnw("Found log file but no metadata, will rebuild metadata")
		// Metadata will be rebuilt during initialization
	}

	return nil
}

// handleMissingLogFile handles the case where metadata exists but log file doesn't.
func (fs *FileStorage) handleMissingLogFile() error {
	fs.logger.Warnw("Found metadata but no log file")

	if fs.options.RecoveryMode != RecoveryModeConservative {
		fs.logMu.Lock()
		defer fs.logMu.Unlock()

		// Reset metadata to match missing log
		fs.firstLogIndex.Store(0)
		fs.lastLogIndex.Store(0)
		if err := fs.saveMetadata(); err != nil {
			return fmt.Errorf("failed to reset metadata: %w", err)
		}
		fs.logger.Infow("Metadata reset to match missing log file")
	} else {
		return fmt.Errorf("%w: log file missing but metadata exists", ErrCorruptedState)
	}

	return nil
}

// createRecoveryMarker writes a marker file with process information.
// The contained process info (PID, time, host) is for debugging; only
// the file's existence is checked programmatically.
func (fs *FileStorage) createRecoveryMarker() error {
	markerPath := filepath.Join(fs.dir, "recovery.marker")
	hostname, _ := os.Hostname()
	markerData := fmt.Sprintf("pid=%d,time=%d,host=%s",
		os.Getpid(), time.Now().UnixNano()/int64(time.Millisecond), hostname)

	if err := os.WriteFile(markerPath, []byte(markerData), OwnRWOthR); err != nil {
		return fmt.Errorf("%w: failed to write recovery marker: %v", ErrStorageIO, err)
	}

	return nil
}

// cleanupTempFiles removes temporary files from interrupted operations
func (fs *FileStorage) cleanupTempFiles() error {
	files := []string{
		fs.stateFile(),
		fs.metadataFile(),
		fs.logFile(),
		fs.snapshotMetadataFile(),
		fs.snapshotDataFile(),
	}

	var errs []error

	for _, f := range files {
		pattern := f + ".tmp"
		matches, err := filepath.Glob(pattern)
		if err != nil {
			errs = append(errs, fmt.Errorf("%w: error matching pattern %q: %v", ErrStorageIO, pattern, err))
			continue
		}

		for _, file := range matches {
			if err := fs.removeFile(file); err != nil && !os.IsNotExist(err) {
				errs = append(errs, fmt.Errorf("%w: error removing temporary file %q: %v", ErrStorageIO, file, err))
			}
		}
	}

	return errors.Join(errs...)
}

// removeFile removes a file with appropriate error handling
func (fs *FileStorage) removeFile(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		fs.logger.Warnw("Failed to remove file", "file", path, "error", err)
		return err
	}
	return nil
}

// initStateAndMetadata initializes state and metadata, building index map.
func (fs *FileStorage) initStateAndMetadata() error {
	// Load persistent state
	if _, err := fs.LoadState(context.Background()); err != nil &&
		!errors.Is(err, ErrStorageIO) && !errors.Is(err, ErrCorruptedState) {
		return fmt.Errorf("failed to load initial state: %w", err)
	}

	// Load log metadata
	if err := fs.loadMetadata(); err != nil {
		if !os.IsNotExist(errors.Unwrap(err)) {
			return fmt.Errorf("failed to load log metadata: %w", err)
		}

		// Initialize new metadata
		if err := fs.saveMetadata(); err != nil {
			return fmt.Errorf("failed to initialize metadata: %w", err)
		}
	}

	// Build index map if enabled
	if fs.options.Features.EnableIndexMap {
		if err := fs.buildIndexOffsetMap(); err != nil {
			return fmt.Errorf("failed to build index map: %w", err)
		}

		// Verify log consistency
		if err := fs.verifyLogConsistency(); err != nil {
			return fmt.Errorf("log consistency check failed: %w", err)
		}
	}

	return nil
}

// removeRecoveryMarker removes the recovery marker file
func (fs *FileStorage) removeRecoveryMarker() {
	markerPath := filepath.Join(fs.dir, "recovery.marker")
	if err := fs.removeFile(markerPath); err != nil && !os.IsNotExist(err) {
		fs.logger.Warnw("Failed to remove recovery marker", "error", err)
	}
}

// buildIndexOffsetMap rebuilds the index-to-offset mapping from log file
func (fs *FileStorage) buildIndexOffsetMap() error {
	if !fs.options.Features.EnableIndexMap {
		fs.logger.Debugw("Skipping index offset map build: feature disabled")
		return nil
	}

	// Create dependencies
	fileSystem := OsFileSystem{}
	reader := NewDefaultLogEntryReader(&fs.options, MaxEntrySizeBytes, LengthPrefixSize)
	handler := NewDefaultLogCorruptionHandler(fileSystem, fs.logger)
	builder := NewIndexBuilder(fileSystem, reader, handler, fs.logger)

	// Build index map
	logPath := fs.logFile()
	fs.logMu.Lock()
	defer fs.logMu.Unlock()

	indexMap, err := builder.BuildIndexOffsetMap(logPath, true)
	if err != nil {
		return err
	}

	fs.indexToOffsetMap = indexMap
	return fs.syncMetadataFromIndex("buildIndexOffsetMap")
}

// syncMetadataFromIndex synchronizes first/last log indices from index map
func (fs *FileStorage) syncMetadataFromIndex(operationContext string) error {
	manager := NewMetadataManager(fs.logger)

	currentFirst := types.Index(fs.firstLogIndex.Load())
	currentLast := types.Index(fs.lastLogIndex.Load())

	newFirst, newLast, changed, err := manager.GetIndicesFromMap(
		fs.indexToOffsetMap,
		currentFirst,
		currentLast,
		operationContext)

	if err != nil {
		return err
	}

	if changed {
		fs.firstLogIndex.Store(uint64(newFirst))
		fs.lastLogIndex.Store(uint64(newLast))

		if err := fs.saveMetadata(); err != nil {
			fs.logger.Errorw(fmt.Sprintf("Failed to persist metadata during %s", operationContext),
				"error", err)
			return fmt.Errorf("%w: failed saving metadata during %s", ErrStorageIO, operationContext)
		}
	}

	return nil
}

// verifyLogConsistency verifies the log is in a consistent state
func (fs *FileStorage) verifyLogConsistency() error {
	if !fs.options.Features.EnableIndexMap {
		fs.logger.Debugw("Skipping log consistency check: index map feature disabled")
		return nil
	}

	fs.logger.Debugw("Starting log consistency verification")

	if err := fs.syncMetadataFromIndex("verifyLogConsistency"); err != nil {
		return err
	}

	if err := fs.verifyLogContinuity(); err != nil {
		fs.logger.Errorw("Log continuity verification failed", "error", err)
		return err
	}

	fs.logger.Debugw("Log consistency verification completed successfully")
	return nil
}

// verifyLogContinuity verifies that log indices form a contiguous sequence
func (fs *FileStorage) verifyLogContinuity() error {
	verifier := NewLogConsistencyVerifier(fs.logger)
	return verifier.VerifyLogContinuity(fs.indexToOffsetMap)
}
