package storage

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/jathurchan/raftlock/types"
)

// metadataSchema defines the structure for log metadata persistence.
type metadataSchema struct {
	FirstIndex types.Index `json:"first_index"`
	LastIndex  types.Index `json:"last_index"`
}

// loadMetadata reads and deserializes the log metadata.
func (fs *FileStorage) loadMetadata() error {
	fs.logMu.Lock()
	defer fs.logMu.Unlock()

	metadataPath := fs.metadataFile()
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, initialize with empty state
			fs.initializeEmptyMetadata()
			return nil
		}
		return fmt.Errorf("%w: failed to read metadata file %q: %v", ErrStorageIO, metadataPath, err)
	}

	var metadata metadataSchema
	if err := json.Unmarshal(data, &metadata); err != nil {
		fs.initializeEmptyMetadata()
		return fmt.Errorf("%w: failed to unmarshal metadata: %v", ErrCorruptedState, err)
	}

	if err := fs.validateMetadataRange(metadata.FirstIndex, metadata.LastIndex); err != nil {
		return err
	}

	fs.firstLogIndex.Store(uint64(metadata.FirstIndex))
	fs.lastLogIndex.Store(uint64(metadata.LastIndex))

	fs.logger.Debugw("Loaded metadata",
		"firstIndex", metadata.FirstIndex,
		"lastIndex", metadata.LastIndex)

	return nil
}

// initializeEmptyMetadata sets the metadata to represent an empty log.
// IMPORTANT: This should only be called while holding logMu.
func (fs *FileStorage) initializeEmptyMetadata() {
	fs.firstLogIndex.Store(0)
	fs.lastLogIndex.Store(0)
}

// validateMetadataRange checks that the metadata index values are consistent.
// A valid range requires that:
// 1. If lastIndex > 0, then firstIndex must be <= lastIndex
// 2. If lastIndex == 0, then firstIndex is expected to be 0 as well (empty log)
// This ensures the log maintains proper sequencing with firstIndex tracking the
// oldest entry and lastIndex tracking the newest entry in the log.
func (fs *FileStorage) validateMetadataRange(firstIndex, lastIndex types.Index) error {
	if firstIndex > lastIndex && lastIndex > 0 {
		return fmt.Errorf("%w: invalid metadata range (first %d > last %d)",
			ErrCorruptedState, firstIndex, lastIndex)
	}
	return nil
}

// saveMetadata writes the current log metadata to disk atomically.
// IMPORTANT: This should only be called while holding logMu.
func (fs *FileStorage) saveMetadata() error {
	metadata := metadataSchema{
		FirstIndex: types.Index(fs.firstLogIndex.Load()),
		LastIndex:  types.Index(fs.lastLogIndex.Load()),
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: failed to marshal metadata: %v", ErrStorageIO, err)
	}

	metadataPath := fs.metadataFile()

	if fs.options.Features.EnableAtomicWrites {
		return atomicWriteFile(metadataPath, data, OwnRWOthR)
	}

	return os.WriteFile(metadataPath, data, OwnRWOthR)
}
