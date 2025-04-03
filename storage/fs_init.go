package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	recoveryMarker := filepath.Join(fs.dir, "recovery.marker")
	snapshotMarker := filepath.Join(fs.dir, "snapshot.marker")

	recoveryExists, err := fileExists(osFS{}, recoveryMarker)
	if err != nil {
		return false, fmt.Errorf("%w: failed to check recovery marker: %v", ErrStorageIO, err)
	}

	snapshotExists, err := fileExists(osFS{}, snapshotMarker)
	if err != nil {
		return false, fmt.Errorf("%w: failed to check snapshot marker: %v", ErrStorageIO, err)
	}

	return recoveryExists || snapshotExists, nil
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
	markerPath := filepath.Join(fs.dir, "snapshot.marker")
	exists, err := fileExists(osFS{}, markerPath)
	if err != nil {
		return fmt.Errorf("%w: failed to check snapshot marker: %v", ErrStorageIO, err)
	}

	if !exists {
		return nil // No recovery needed
	}

	fs.logger.Infow("Found snapshot marker, recovering from interrupted snapshot operation")

	defer func() {
		if err := os.Remove(markerPath); err != nil {
			fs.logger.Warnw("Failed to remove snapshot marker", "path", markerPath, "error", err)
		}
	}()

	// Read marker to determine progress
	markerData, err := os.ReadFile(markerPath)
	if err != nil {
		return fmt.Errorf("%w: failed to read snapshot marker: %v", ErrStorageIO, err)
	}

	markerStr := string(markerData)
	metaCommitted := strings.Contains(markerStr, "meta_committed=true")

	return fs.handleSnapshotRecovery(metaCommitted, markerPath)
}

// handleSnapshotRecovery handles recovery based on snapshot marker state.
func (fs *FileStorage) handleSnapshotRecovery(metaCommitted bool, markerPath string) error {
	// Check for temporary files
	tmpMetaExists, _ := fileExists(osFS{}, fs.snapshotMetadataFile()+".tmp")
	tmpDataExists, _ := fileExists(osFS{}, fs.snapshotDataFile()+".tmp")
	metaExists, _ := fileExists(osFS{}, fs.snapshotMetadataFile())
	dataExists, _ := fileExists(osFS{}, fs.snapshotDataFile())

	if !metaCommitted {
		// Metadata not committed yet, clean up temp files
		return fs.cleanupIncompleteSnapshot(tmpMetaExists, tmpDataExists)
	}

	if metaExists && tmpDataExists && !dataExists {
		// Metadata committed but data not yet, try to complete
		return fs.completeSnapshotDataCommit(tmpDataExists)
	}

	return nil
}

// cleanupIncompleteSnapshot removes temporary files for incomplete snapshot operations.
func (fs *FileStorage) cleanupIncompleteSnapshot(tmpMetaExists, tmpDataExists bool) error {
	if tmpMetaExists {
		if err := os.Remove(fs.snapshotMetadataFile() + ".tmp"); err != nil {
			fs.logger.Warnw("Failed to remove temporary metadata file", "error", err)
		}
	}

	if tmpDataExists {
		if err := os.Remove(fs.snapshotDataFile() + ".tmp"); err != nil {
			fs.logger.Warnw("Failed to remove temporary data file", "error", err)
		}
	}

	fs.logger.Infow("Cleaned up incomplete snapshot operation")
	return nil
}

// completeSnapshotDataCommit completes an interrupted snapshot data commit.
func (fs *FileStorage) completeSnapshotDataCommit(tmpDataExists bool) error {
	if tmpDataExists {
		if err := os.Rename(fs.snapshotDataFile()+".tmp", fs.snapshotDataFile()); err != nil {
			fs.logger.Warnw("Failed to complete snapshot data file rename", "error", err)

			// Handle based on recovery mode
			if fs.options.RecoveryMode == RecoveryModeAggressive {
				if err := os.Remove(fs.snapshotMetadataFile()); err != nil {
					fs.logger.Warnw("Failed to remove snapshot metadata", "error", err)
				}
				fs.logger.Warnw("Removed snapshot metadata due to missing data file")
			}

			return fmt.Errorf("%w: failed to complete snapshot data commit: %v", ErrStorageIO, err)
		} else {
			fs.logger.Infow("Completed interrupted snapshot operation")
		}
	}

	return nil
}

// checkAndRepairConsistency verifies consistency between metadata and log file.
func (fs *FileStorage) checkAndRepairConsistency() error {
	metaExists, err := fileExists(osFS{}, fs.metadataFile())
	if err != nil {
		return fmt.Errorf("%w: failed to check metadata file: %v", ErrStorageIO, err)
	}

	logExists, err := fileExists(osFS{}, fs.logFile())
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
			err := os.Remove(file)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("%w: error removing temporary file %q: %v", ErrStorageIO, file, err))
				fs.logger.Warnw("Failed to remove temporary file", "file", file, "error", err)
			}
		}
	}

	return errors.Join(errs...)
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
	if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
		fs.logger.Warnw("Failed to remove recovery marker", "error", err)
	}
}
