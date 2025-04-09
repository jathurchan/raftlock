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

	// ValidateMetadataRange ensures the index values are logically consistent.
	ValidateMetadataRange(firstIndex, lastIndex types.Index) error
}

// defaultMetadataService implements metadataService.
type defaultMetadataService struct {
	fs         fileSystem
	serializer serializer
	index      indexService
	logger     logger.Logger
}

// newMetadataServiceWithDeps creates a new DefaultMetadataService with custom serializer.
func newMetadataServiceWithDeps(fs fileSystem, serializer serializer, index indexService, logger logger.Logger) metadataService {
	return &defaultMetadataService{
		fs:         fs,
		serializer: serializer,
		index:      index,
		logger:     logger.WithComponent("metadata"),
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
		if err := m.atomicWriteFile(path, data, ownRWOthR); err != nil {
			m.logger.Errorw("Atomic write failed", "path", path, "error", err)
			return err
		}
	}

	if err := m.fs.WriteFile(path, data, ownRWOthR); err != nil {
		m.logger.Errorw("Non-atomic write failed", "path", path, "error", err)
		return fmt.Errorf("%w: failed to write metadata file: %v", ErrStorageIO, err)
	}

	m.logger.Infow("Metadata successfully saved", "path", path)
	return nil
}

// atomicWriteFile writes data to a temporary file and then atomically renames it to the target path.
// This ensures the file is never in a partially-written state.
func (m *defaultMetadataService) atomicWriteFile(targetPath string, data []byte, perm os.FileMode) error {
	return m.fs.AtomicWrite(targetPath, data, perm)
}

// handleErrorWithCleanup attempts to remove the temporary file and combines any cleanup
// error with the primary error.
func (m *defaultMetadataService) handleErrorWithCleanup(primaryErr error, tmpPath string) error {
	if rmErr := m.fs.Remove(tmpPath); rmErr != nil {
		return fmt.Errorf("%w; additionally failed to clean up temp file: %v", primaryErr, rmErr)
	}
	return primaryErr
}

// SyncMetadataFromIndexMap determines if metadata needs updating based on the provided index map.
// If bounds have changed, it updates and persists the new metadata to the specified path.
// Returns the new (or unchanged) first and last indices, and any error encountered.
func (m *defaultMetadataService) SyncMetadataFromIndexMap(
	path string,
	indexMap []types.IndexOffsetPair,
	currentFirst, currentLast types.Index,
	context string,
	useAtomicWrite bool,
) (types.Index, types.Index, error) {

	boundsRes := m.index.GetBounds(indexMap, currentFirst, currentLast)

	if !boundsRes.Changed {
		m.logger.Debugw("No metadata update required; bounds are unchanged",
			"context", context,
			"path", path,
			"firstIndex", currentFirst,
			"lastIndex", currentLast)
		return currentFirst, currentLast, nil
	}

	m.logger.Infow("Metadata bounds changed; saving update",
		"context", context,
		"path", path,
		"previousFirstIndex", currentFirst,
		"previousLastIndex", currentLast,
		"newFirstIndex", boundsRes.NewFirst,
		"newLastIndex", boundsRes.NewLast,
		"useAtomicWrite", useAtomicWrite)

	metadata := logMetadata{
		FirstIndex: boundsRes.NewFirst,
		LastIndex:  boundsRes.NewLast,
	}

	if err := m.SaveMetadata(path, metadata, useAtomicWrite); err != nil {
		m.logger.Errorw("Failed to save metadata",
			"context", context,
			"path", path,
			"error", err)
		return currentFirst, currentLast, fmt.Errorf("%w: failed saving metadata during %s", ErrStorageIO, context)
	}

	m.logger.Debugw("Metadata successfully saved",
		"context", context,
		"path", path,
		"firstIndex", boundsRes.NewFirst,
		"lastIndex", boundsRes.NewLast)

	return boundsRes.NewFirst, boundsRes.NewLast, nil
}

// ValidateMetadataRange ensures that the provided first and last index values
// represent a valid and logically consistent log range.
func (m *defaultMetadataService) ValidateMetadataRange(firstIndex, lastIndex types.Index) error {
	if lastIndex > 0 && firstIndex > lastIndex {
		return fmt.Errorf("%w: invalid metadata range (first %d > last %d)",
			ErrCorruptedState, firstIndex, lastIndex)
	}
	return nil
}
