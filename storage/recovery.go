package storage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// recoveryMode defines the level of strictness to apply during recovery.
type recoveryMode int

const (
	// conservativeMode avoids any recovery actions that might risk data loss.
	conservativeMode recoveryMode = iota

	// normalMode allows safe recovery without risking integrity.
	normalMode

	// aggressiveMode prioritizes availability, tolerating some data loss.
	aggressiveMode
)

func (m recoveryMode) String() string {
	switch m {
	case conservativeMode:
		return "conservative"
	case normalMode:
		return "normal"
	case aggressiveMode:
		return "aggressive"
	default:
		return "unknown"
	}
}

// snapshotRecoveryState defines possible states during snapshot recovery.
type snapshotRecoveryState int

const (
	snapshotStateUnknown snapshotRecoveryState = iota
	snapshotStateIncomplete
	snapshotStateNeedsDataCommit
	snapshotStateClean
)

// recoveryService defines an interface for storage recovery operations.
type recoveryService interface {
	// CheckForRecoveryMarkers checks for the presence of recovery or snapshot marker files
	// to determine if a recovery operation is necessary.
	CheckForRecoveryMarkers() (bool, error)

	// PerformRecovery executes the full recovery procedure, including snapshot recovery
	// and consistency checks.
	PerformRecovery() error

	// CreateRecoveryMarker creates a file that indicates the system is in a recovery state,
	// including system metadata like PID and timestamp.
	CreateRecoveryMarker() error

	// CleanupTempFiles removes any temporary files left over from interrupted or incomplete operations.
	CleanupTempFiles() error

	// RemoveRecoveryMarker deletes the recovery marker file once recovery has been successfully completed.
	RemoveRecoveryMarker() error

	// CreateSnapshotRecoveryMarker writes a marker indicating that a snapshot operation is in progress,
	// including snapshot metadata such as last included index and term.
	CreateSnapshotRecoveryMarker(metadata types.SnapshotMetadata) error

	// UpdateSnapshotMarkerStatus updates the snapshot marker file with additional status information,
	// such as whether the snapshot data has been committed.
	UpdateSnapshotMarkerStatus(status string) error

	// RemoveSnapshotRecoveryMarker deletes the snapshot marker file once the snapshot operation is complete.
	RemoveSnapshotRecoveryMarker() error
}

// defaultRecoveryService is the default implementation of recoveryService.
type defaultRecoveryService struct {
	fs         fileSystem
	serializer serializer
	indexSvc   indexService
	logger     logger.Logger
	dir        string
	mode       recoveryMode
	metaSvc    metadataService
	proc       systemInfo

	recoverFromSnapshotOperationFunc  func() error
	CheckAndRepairConsistencyFunc     func() error
	RecoverSnapshotFunc               func(metaCommitted bool) error
	EvaluateSnapshotRecoveryStateFunc func(metaCommitted bool) (snapshotRecoveryState, error)
	cleanupIncompleteSnapshotFunc     func() error
	CompleteSnapshotDataCommitFunc    func() error
	SafeExistsFunc                    func(path string, label string) bool
	HandleMissingLogFileFunc          func(metaFile string) error
}

func newRecoveryService(
	fs fileSystem,
	serializer serializer,
	logger logger.Logger,
	dir string,
	mode recoveryMode,
	indexSvc indexService,
	metaSvc metadataService,
	proc systemInfo,
) recoveryService {
	return &defaultRecoveryService{
		fs:         fs,
		serializer: serializer,
		indexSvc:   indexSvc,
		logger:     logger.WithComponent("recovery"),
		dir:        dir,
		mode:       mode,
		metaSvc:    metaSvc,
		proc:       proc,
	}
}

func (r *defaultRecoveryService) path(name string) string    { return r.fs.Path(r.dir, name) }
func (r *defaultRecoveryService) tmpPath(name string) string { return name + tmpSuffix }

// CheckForRecoveryMarkers determines if recovery is needed based on marker files.
func (r *defaultRecoveryService) CheckForRecoveryMarkers() (bool, error) {
	recoveryExists, err := r.fs.Exists(r.path(recoveryMarkerFilename))
	if err != nil {
		return false, fmt.Errorf("%w: failed to check recovery marker: %w", ErrStorageIO, err)
	}
	snapshotExists, err := r.fs.Exists(r.path(snapshotMarkerFilename))
	if err != nil {
		return false, fmt.Errorf("%w: failed to check snapshot marker: %w", ErrStorageIO, err)
	}
	return recoveryExists || snapshotExists, nil
}

// PerformRecovery orchestrates all necessary recovery operations.
func (r *defaultRecoveryService) PerformRecovery() error {
	if err := r.recoverFromSnapshotOperation(); err != nil {
		r.logger.Errorw("Snapshot recovery failed", "error", err)
		return fmt.Errorf("snapshot recovery failed: %w", err)
	}
	if err := r.checkAndRepairConsistency(); err != nil {
		r.logger.Errorw("Consistency repair failed", "error", err)
		return fmt.Errorf("consistency repair failed: %w", err)
	}
	r.logger.Infow("Recovery completed successfully")
	return nil
}

// recoverFromSnapshotOperation handles recovery logic for in-progress snapshots.
func (r *defaultRecoveryService) recoverFromSnapshotOperation() error {
	if r.recoverFromSnapshotOperationFunc != nil {
		return r.recoverFromSnapshotOperationFunc()
	}

	markerPath := r.path(snapshotMarkerFilename)
	exists, err := r.fs.Exists(markerPath)
	if err != nil {
		return fmt.Errorf("%w: failed to check snapshot marker: %w", ErrStorageIO, err)
	}
	if !exists {
		return nil
	}

	r.logger.Infow("Snapshot marker found - recovering snapshot")
	metaCommitted, err := r.readSnapshotMarker(markerPath)
	if err != nil {
		return err
	}

	return r.recoverSnapshot(metaCommitted)
}

// readSnapshotMarker parses the snapshot marker file to detect commit status.
func (r *defaultRecoveryService) readSnapshotMarker(path string) (bool, error) {
	file, err := r.fs.Open(path)
	if err != nil {
		return false, fmt.Errorf("%w: failed to open snapshot marker: %w", ErrStorageIO, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			r.logger.Errorw("error closing file: %v", err)
		}
	}()

	data, err := file.ReadAll()
	if err != nil {
		return false, fmt.Errorf("%w: failed to read snapshot marker: %w", ErrStorageIO, err)
	}
	return strings.Contains(string(data), snapshotMarkerCommittedKey), nil
}

func (r *defaultRecoveryService) recoverSnapshot(metaCommitted bool) error {
	if r.RecoverSnapshotFunc != nil {
		return r.RecoverSnapshotFunc(metaCommitted)
	}

	state, err := r.evaluateSnapshotRecoveryState(metaCommitted)
	if err != nil {
		return err
	}

	switch state {
	case snapshotStateIncomplete:
		return r.cleanupIncompleteSnapshot()
	case snapshotStateNeedsDataCommit:
		return r.completeSnapshotDataCommit()
	default:
		_ = r.fs.Remove(r.path(snapshotMarkerFilename))
		return nil
	}
}

// evaluateSnapshotRecoveryState determines the current snapshot recovery state.
func (r *defaultRecoveryService) evaluateSnapshotRecoveryState(
	metaCommitted bool,
) (snapshotRecoveryState, error) {
	if r.EvaluateSnapshotRecoveryStateFunc != nil {
		return r.EvaluateSnapshotRecoveryStateFunc(metaCommitted)
	}

	metaFile := r.path(snapshotMetaFilename)
	dataFile := r.path(snapshotDataFilename)
	tmpMeta := r.tmpPath(metaFile)
	tmpData := r.tmpPath(dataFile)

	tmpMetaExists := r.safeExists(tmpMeta, "temp metadata")
	tmpDataExists := r.safeExists(tmpData, "temp data")
	metaExists := r.safeExists(metaFile, "metadata")
	dataExists := r.safeExists(dataFile, "data")

	if !metaCommitted {
		if tmpMetaExists || tmpDataExists {
			return snapshotStateIncomplete, nil
		}
		return snapshotStateClean, nil
	}

	if metaExists && tmpDataExists && !dataExists {
		return snapshotStateNeedsDataCommit, nil
	}

	return snapshotStateClean, nil
}

// cleanupIncompleteSnapshot removes partial snapshot files left from an incomplete write.
func (r *defaultRecoveryService) cleanupIncompleteSnapshot() error {
	if r.cleanupIncompleteSnapshotFunc != nil {
		return r.cleanupIncompleteSnapshotFunc()
	}

	metaFile := r.path(snapshotMetaFilename)
	dataFile := r.path(snapshotDataFilename)
	tmpMeta := r.tmpPath(metaFile)
	tmpData := r.tmpPath(dataFile)

	if r.safeExists(tmpMeta, "temp metadata") {
		_ = r.fs.Remove(tmpMeta)
	}
	if r.safeExists(tmpData, "temp data") {
		_ = r.fs.Remove(tmpData)
	}
	r.logger.Infow("Cleaned up incomplete snapshot")
	return nil
}

// completeSnapshotDataCommit finalizes a snapshot where only the data rename was pending.
func (r *defaultRecoveryService) completeSnapshotDataCommit() error {
	if r.CompleteSnapshotDataCommitFunc != nil {
		return r.CompleteSnapshotDataCommitFunc()
	}

	metaFile := r.path(snapshotMetaFilename)
	dataFile := r.path(snapshotDataFilename)
	tmpData := r.tmpPath(dataFile)

	if err := r.fs.Rename(tmpData, dataFile); err != nil {
		if r.mode == normalMode {
			removeErr := r.fs.Remove(metaFile)
			if removeErr != nil {
				r.logger.Errorw(
					"Failed to rollback metadata after data commit failure",
					"metadataFile",
					metaFile,
					"error",
					removeErr,
				)
				return fmt.Errorf(
					"%w: failed to complete snapshot commit and rollback metadata: data rename error (%w), metadata rollback error (%w)",
					ErrStorageIO,
					err,
					removeErr,
				)
			}
			r.logger.Warnw("Rolled back metadata due to failed data commit (normal mode)")
			return fmt.Errorf("%w: failed to complete snapshot commit: %w", ErrStorageIO, err)
		}
		if r.mode == aggressiveMode {
			_ = r.fs.Remove(metaFile)
			r.logger.Warnw("Removed metadata due to failed data commit (aggressive mode)")
		}
		return fmt.Errorf("%w: failed to complete snapshot commit: %w", ErrStorageIO, err)
	}
	r.logger.Infow("Snapshot data commit completed")
	return nil
}

// safeExists checks file existence and logs errors non-fatally.
func (r *defaultRecoveryService) safeExists(path string, label string) bool {
	if r.SafeExistsFunc != nil {
		return r.SafeExistsFunc(path, label)
	}

	exists, err := r.fs.Exists(path)
	if err != nil {
		r.logger.Warnw("Failed to check file existence", "label", label, "file", path, "error", err)
		return false
	}
	return exists
}

// checkAndRepairConsistency ensures log and metadata files are aligned.
func (r *defaultRecoveryService) checkAndRepairConsistency() error {
	if r.CheckAndRepairConsistencyFunc != nil {
		return r.CheckAndRepairConsistencyFunc()
	}

	metaFile := r.path(metadataFilename)
	logFile := r.path(logFilename)

	metaExists, err := r.fs.Exists(metaFile)
	if err != nil {
		return fmt.Errorf("%w: failed to check metadata: %w", ErrStorageIO, err)
	}
	logExists, err := r.fs.Exists(logFile)
	if err != nil {
		return fmt.Errorf("%w: failed to check log file: %w", ErrStorageIO, err)
	}

	if metaExists && !logExists {
		return r.handleMissingLogFile(metaFile)
	} else if logExists && !metaExists {
		r.logger.Warnw("Log exists but metadata missing, will rebuild metadata")
	}
	return nil
}

// handleMissingLogFile resolves state where metadata exists but log is missing.
func (r *defaultRecoveryService) handleMissingLogFile(metaFile string) error {
	if r.HandleMissingLogFileFunc != nil {
		return r.HandleMissingLogFileFunc(metaFile)
	}

	r.logger.Warnw("Metadata exists but log is missing")
	if r.mode != conservativeMode {
		metadata := logMetadata{FirstIndex: 0, LastIndex: 0}
		if err := r.metaSvc.SaveMetadata(metaFile, metadata, true); err != nil {
			return fmt.Errorf("%w: failed saving reset metadata: %w", ErrStorageIO, err)
		}
		r.logger.Infow("Reset metadata due to missing log")
		return nil
	}
	return fmt.Errorf("%w: log file missing but metadata exists", ErrCorruptedState)
}

// CreateRecoveryMarker writes a marker with system info for crash recovery detection.
func (r *defaultRecoveryService) CreateRecoveryMarker() error {
	data := fmt.Sprintf(
		"pid=%d,time=%d,host=%s",
		r.proc.PID(),
		r.proc.NowUnixMilli(),
		r.proc.Hostname(),
	)
	return r.fs.WriteFile(r.path(recoveryMarkerFilename), []byte(data), ownRWOthR)
}

// CleanupTempFiles removes any orphaned temporary files from past operations.
func (r *defaultRecoveryService) CleanupTempFiles() error {
	files := []string{
		r.path(stateFilename), r.path(metadataFilename), r.path(logFilename),
		r.path(snapshotMetaFilename), r.path(snapshotDataFilename),
	}

	var errs []error
	for _, file := range files {
		tmp := r.tmpPath(file)
		matches, err := r.fs.Glob(tmp)
		if err != nil {
			errs = append(errs, fmt.Errorf("%w: glob failed on %q: %w", ErrStorageIO, tmp, err))
			continue
		}
		for _, match := range matches {
			if err := r.fs.Remove(match); err != nil && !r.fs.IsNotExist(err) {
				r.logger.Warnw("Failed to remove temp file", "file", match, "error", err)
				errs = append(
					errs,
					fmt.Errorf("%w: failed to remove %q: %w", ErrStorageIO, match, err),
				)
			}
		}
	}
	return errors.Join(errs...)
}

// RemoveRecoveryMarker deletes the recovery marker file.
func (r *defaultRecoveryService) RemoveRecoveryMarker() error {
	return r.fs.Remove(r.path(recoveryMarkerFilename))
}

// CreateSnapshotRecoveryMarker writes a snapshot marker for crash recovery purposes.
func (r *defaultRecoveryService) CreateSnapshotRecoveryMarker(
	metadata types.SnapshotMetadata,
) error {
	markerPath := r.path(snapshotMarkerFilename)
	markerData := fmt.Sprintf(
		"pid=%d,time=%d,last_idx=%d,last_term=%d",
		r.proc.PID(),
		r.proc.NowUnixMilli(),
		metadata.LastIncludedIndex,
		metadata.LastIncludedTerm,
	)
	if err := r.fs.WriteFile(markerPath, []byte(markerData), ownRWOthR); err != nil {
		return fmt.Errorf("%w: failed to write snapshot marker: %w", ErrStorageIO, err)
	}
	return nil
}

// UpdateSnapshotMarkerStatus appends status information to the snapshot marker file.
func (r *defaultRecoveryService) UpdateSnapshotMarkerStatus(status string) error {
	markerPath := r.path(snapshotMarkerFilename)

	data, err := r.fs.ReadFile(markerPath)
	if err != nil {
		return fmt.Errorf("%w: failed to read snapshot marker: %w", ErrStorageIO, err)
	}

	updated := string(data) + "," + status
	if err := r.fs.WriteFile(markerPath, []byte(updated), ownRWOthR); err != nil {
		return fmt.Errorf("%w: failed to update snapshot marker: %w", ErrStorageIO, err)
	}

	return nil
}

// RemoveSnapshotRecoveryMarker deletes the snapshot marker file.
func (r *defaultRecoveryService) RemoveSnapshotRecoveryMarker() error {
	return r.fs.Remove(r.path(snapshotMarkerFilename))
}
