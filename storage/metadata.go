package storage

import (
	"errors"
	"fmt"
	"os"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// logMetadata represents the persistent metadata for the log.
type logMetadata struct {
	FirstIndex types.Index `json:"first_index"`
	LastIndex  types.Index `json:"last_index"`
}

// metadataService handles metadata operations.
type metadataService interface {
	// LoadMetadata loads metadata from the specified path.
	LoadMetadata(path string) (logMetadata, error)

	// SaveMetadata persists metadata to the specified path.
	SaveMetadata(path string, metadata logMetadata, useAtomicWrite bool) error

	// SyncMetadataFromIndexMap updates and persists metadata based on the index map.
	SyncMetadataFromIndexMap(path string, indexMap []types.IndexOffsetPair,
		currentFirst, currentLast types.Index, context string, useAtomicWrite bool) (types.Index, types.Index, error)

	// VerifyLogConsistency checks that the log is in a consistent state.
	VerifyLogConsistency(indexMap []types.IndexOffsetPair) error
}

// DefaultMetadataService implements MetadataService.
type defaultMetadataService struct {
	fs         FileSystem
	serializer Serializer
	logger     logger.Logger
}

// newMetadataService creates a new DefaultMetadataService with JSON serialization.
func newMetadataService(fs FileSystem, logger logger.Logger) *defaultMetadataService {
	return &defaultMetadataService{
		fs:         fs,
		serializer: NewJsonSerializer(),
		logger:     logger,
	}
}

// newMetadataServiceWithSerializer creates a new DefaultMetadataService with custom serializer.
func newMetadataServiceWithSerializer(fs FileSystem, serializer Serializer, logger logger.Logger) *defaultMetadataService {
	return &defaultMetadataService{
		fs:         fs,
		serializer: serializer,
		logger:     logger,
	}
}

// LoadMetadata reads and deserializes the log metadata from the given path.
func (m *defaultMetadataService) LoadMetadata(path string) (logMetadata, error) {
	var metadata logMetadata

	data, err := m.fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			m.logger.Infow("Metadata file does not exist", "path", path)
			return metadata, fmt.Errorf("metadata file not found: %w", os.ErrNotExist)
		}
		m.logger.Errorw("Failed to read metadata file", "error", err, "path", path)
		return metadata, fmt.Errorf("%w: failed to read metadata file: %v", ErrStorageIO, err)
	}

	metadata, err = m.serializer.UnmarshalMetadata(data)
	if err != nil {
		m.logger.Errorw("Failed to unmarshal metadata", "error", err, "raw", string(data))
		return metadata, fmt.Errorf("%w: failed to unmarshal metadata: %v", ErrCorruptedState, err)
	}

	m.logger.Debugw("Successfully loaded metadata",
		"path", path,
		"firstIndex", metadata.FirstIndex,
		"lastIndex", metadata.LastIndex,
	)

	return metadata, nil
}

// SaveMetadata serializes and writes metadata to the given file path.
// If useAtomicWrite is true, the write will be performed atomically using a temporary file and rename.
func (m *defaultMetadataService) SaveMetadata(path string, metadata logMetadata, useAtomicWrite bool) error {
	m.logger.Debugw("Saving metadata",
		"path", path,
		"useAtomicWrite", useAtomicWrite,
		"firstIndex", metadata.FirstIndex,
		"lastIndex", metadata.LastIndex,
	)

	data, err := m.serializer.MarshalMetadata(metadata)
	if err != nil {
		m.logger.Errorw("Failed to marshal metadata", "error", err)
		return fmt.Errorf("%w: failed to marshal metadata: %v", ErrStorageIO, err)
	}

	if useAtomicWrite {
		if err := m.atomicWriteFile(path, data, OwnRWOthR); err != nil {
			m.logger.Errorw("Atomic write failed", "path", path, "error", err)
			return err
		}
	}

	if err := m.fs.WriteFile(path, data, OwnRWOthR); err != nil {
		m.logger.Errorw("Non-atomic write failed", "path", path, "error", err)
		return fmt.Errorf("%w: failed to write metadata file: %v", ErrStorageIO, err)
	}

	m.logger.Infow("Metadata successfully saved", "path", path)
	return nil
}

// atomicWriteFile writes data to a temporary file and then atomically renames it to the target path.
// This ensures the file is never in a partially-written state.
func (m *defaultMetadataService) atomicWriteFile(targetPath string, data []byte, perm os.FileMode) error {
	dir := m.fs.Dir(targetPath)

	// Ensure the directory exists
	if err := m.fs.MkdirAll(dir, OwnRWXOthRX); err != nil {
		m.logger.Errorw("Failed to create parent directory for atomic write", "dir", dir, "error", err)
		return fmt.Errorf("%w: failed to create directory %q: %v", ErrStorageIO, dir, err)
	}

	// Write to temporary file
	tmpPath := targetPath + ".tmp"
	if err := m.fs.WriteFile(tmpPath, data, perm); err != nil {
		m.logger.Errorw("Failed to write temporary file during atomic write", "tmpPath", tmpPath, "error", err)
		return m.handleErrorWithCleanup(
			fmt.Errorf("%w: failed to write temporary file %q: %v", ErrStorageIO, tmpPath, err),
			tmpPath,
		)
	}

	// Atomically rename temporary file to final destination
	if err := m.fs.Rename(tmpPath, targetPath); err != nil {
		m.logger.Errorw("Failed to rename temporary file to target path", "tmpPath", tmpPath, "targetPath", targetPath, "error", err)
		return m.handleErrorWithCleanup(
			fmt.Errorf("%w: failed to rename temporary file %q to %q: %v", ErrStorageIO, tmpPath, targetPath, err),
			tmpPath,
		)
	}

	m.logger.Debugw("Atomic metadata write succeeded", "path", targetPath)
	return nil
}

// handleErrorWithCleanup attempts to remove the temporary file and combines any cleanup
// error with the primary error.
func (m *defaultMetadataService) handleErrorWithCleanup(primaryErr error, tmpPath string) error {
	if rmErr := os.Remove(tmpPath); rmErr != nil {
		return fmt.Errorf("%w; additionally failed to clean up temp file: %v", primaryErr, rmErr)
	}
	return primaryErr
}

// SyncMetadataFromIndexMap evaluates whether metadata needs to be updated based on the given index map.
// If changes are detected, it updates and persists the new metadata to the specified path.
// Returns the updated (or unchanged) first and last indices, along with any error encountered.
func (m *defaultMetadataService) SyncMetadataFromIndexMap(
	path string,
	indexMap []types.IndexOffsetPair,
	currentFirst, currentLast types.Index,
	context string,
	useAtomicWrite bool,
) (types.Index, types.Index, error) {

	newFirst, newLast, changed := m.getIndicesFromMap(indexMap, currentFirst, currentLast, context)
	if changed {
		m.logger.Debugw("No metadata sync needed; indices unchanged",
			"context", context,
			"path", path,
			"firstIndex", currentFirst,
			"lastIndex", currentLast)

		metadata := logMetadata{
			FirstIndex: newFirst,
			LastIndex:  newLast,
		}

		m.logger.Infow("Metadata sync required; persisting updated indices",
			"context", context,
			"path", path,
			"previousFirstIndex", currentFirst,
			"previousLastIndex", currentLast,
			"newFirstIndex", newFirst,
			"newLastIndex", newLast,
			"useAtomicWrite", useAtomicWrite)

		if err := m.SaveMetadata(path, metadata, useAtomicWrite); err != nil {
			m.logger.Errorw(fmt.Sprintf("Failed to persist metadata during %s", context),
				"error", err)
			return currentFirst, currentLast, fmt.Errorf("%w: failed saving metadata during %s",
				ErrStorageIO, context)
		}

		return newFirst, newLast, nil
	}

	m.logger.Debugw("Metadata successfully synced",
		"context", context,
		"path", path,
		"firstIndex", newFirst,
		"lastIndex", newLast)

	return currentFirst, currentLast, nil
}

// getIndicesFromMap checks if the metadata's first and last indices should be updated,
// based on the provided index map and current values.
// It returns the new (or unchanged) first and last indices, and a boolean indicating if an update is needed.
func (m *defaultMetadataService) getIndicesFromMap(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, context string) (types.Index, types.Index, bool) {

	if len(indexMap) == 0 {
		// No data to reflect; only reset if not already zeroed
		if currentFirst == 0 && currentLast == 0 {
			m.logger.Debugw("Index map is empty; metadata already zeroed",
				"context", context)
			return 0, 0, false
		}

		m.logger.Warnw("Index map is empty; resetting metadata to zero",
			"context", context,
			"previousFirstIndex", currentFirst,
			"previousLastIndex", currentLast)
		return 0, 0, true
	}

	// Get new bounds from map
	newFirst := indexMap[0].Index
	newLast := indexMap[len(indexMap)-1].Index

	if newFirst == currentFirst && newLast == currentLast {
		m.logger.Debugw("Metadata up-to-date; no change needed",
			"context", context,
			"firstIndex", currentFirst,
			"lastIndex", currentLast)
		return currentFirst, currentLast, false
	}

	m.logger.Infow("Metadata updated from index map",
		"context", context,
		"previousFirstIndex", currentFirst,
		"previousLastIndex", currentLast,
		"newFirstIndex", newFirst,
		"newLastIndex", newLast)

	return newFirst, newLast, true
}

// VerifyLogConsistency checks that the provided index map contains contiguous log entries.
// Returns an error if any discontinuity is found, indicating possible log corruption.
func (m *defaultMetadataService) VerifyLogConsistency(indexMap []types.IndexOffsetPair) error {
	if len(indexMap) == 0 {
		m.logger.Debugw("Log consistency check skipped: index map is empty")
		return nil
	}

	for i := 1; i < len(indexMap); i++ {
		prevIndex := indexMap[i-1].Index
		expectedNext := prevIndex + 1
		actualNext := indexMap[i].Index

		if actualNext != expectedNext {
			m.logger.Errorw("Log index discontinuity detected",
				"previousIndex", prevIndex,
				"expectedNextIndex", expectedNext,
				"actualNextIndex", actualNext,
				"discontinuityAtPosition", i)

			return fmt.Errorf("%w: log entries not contiguous at position %d (expected %d, got %d)",
				ErrCorruptedLog, i, expectedNext, actualNext)
		}
	}

	m.logger.Debugw("Log consistency verified: all entries are contiguous",
		"totalEntries", len(indexMap))
	return nil
}
