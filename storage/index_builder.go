package storage

import (
	"fmt"
	"io"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// IndexBuilder is responsible for building and managing the index map
type IndexBuilder struct {
	fs                FileSystem
	reader            LogEntryReader
	corruptionHandler LogCorruptionHandler
	logger            logger.Logger
}

// NewIndexBuilder creates a new IndexBuilder
func NewIndexBuilder(fs FileSystem, reader LogEntryReader, handler LogCorruptionHandler, logger logger.Logger) *IndexBuilder {
	return &IndexBuilder{
		fs:                fs,
		reader:            reader,
		corruptionHandler: handler,
		logger:            logger,
	}
}

// BuildIndexOffsetMap scans the log file and builds the index-to-offset map.
func (b *IndexBuilder) BuildIndexOffsetMap(logPath string, enableMap bool) ([]types.IndexOffsetPair, error) {
	if !enableMap {
		b.logger.Debugw("Skipping index offset map build: feature disabled")
		return make([]types.IndexOffsetPair, 0), nil
	}

	exists, err := b.fs.Exists(logPath)
	if err != nil {
		b.logger.Errorw("Error checking log file existence", "error", err, "path", logPath)
		return nil, fmt.Errorf("%w: error checking log file existence", ErrStorageIO)
	}

	if !exists {
		b.logger.Infow("Log file does not exist, initializing empty index-offset map", "path", logPath)
		return make([]types.IndexOffsetPair, 0), nil
	}

	b.logger.Debugw("Building index offset map from log file", "path", logPath)

	indexMap, err := b.scanLogAndBuildMap(logPath)
	if err != nil {
		return nil, err
	}

	return indexMap, nil
}

// scanLogAndBuildMap scans the log file at logPath to rebuild the index-to-offset mapping.
// It handles and recovers from corruption by truncating the log at the failure point.
func (b *IndexBuilder) scanLogAndBuildMap(logPath string) ([]types.IndexOffsetPair, error) {
	file, err := b.fs.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to open log file %q: %v", ErrStorageIO, logPath, err)
	}
	defer file.Close()

	var (
		offset      int64
		lastIndex   types.Index
		entriesRead int
	)

	b.logger.Infow("Starting log scan and index rebuild", "logPath", logPath)

	indexMap := make([]types.IndexOffsetPair, DefaultIndexMapInitialCapacity)

	for {
		// entryOffset stores the offset where the current entry *starts*
		entryOffset := offset

		entry, bytesRead, err := b.reader.ReadNextEntry(file)
		offset += bytesRead

		if err == io.EOF {
			b.logger.Infow("Reached end of log during scan", "entriesRead", entriesRead, "finalOffset", offset)
			break
		}
		if err != nil {
			if handleErr := b.corruptionHandler.HandleCorruption(
				logPath, entryOffset, "Failed to read or parse log entry", err); handleErr != nil {
				return nil, fmt.Errorf("failed to handle log corruption: %w", handleErr)
			}
			// After handling corruption, we rebuild the map from what we've read so far
			return indexMap, nil
		}

		if lastIndex > 0 {
			if entry.Index <= lastIndex {
				if handleErr := b.corruptionHandler.HandleCorruption(
					logPath, entryOffset, "Out-of-order log entry",
					fmt.Errorf("index %d <= previous %d", entry.Index, lastIndex)); handleErr != nil {
					return nil, fmt.Errorf("failed to handle log corruption: %w", handleErr)
				}
				// After handling corruption, we rebuild the map from what we've read so far
				return indexMap, nil
			}
			if entry.Index > lastIndex+1 {
				if handleErr := b.corruptionHandler.HandleCorruption(
					logPath, entryOffset, "Gap in log index sequence",
					fmt.Errorf("index %d > previous %d + 1", entry.Index, lastIndex)); handleErr != nil {
					return nil, fmt.Errorf("failed to handle log corruption: %w", handleErr)
				}
				// After handling corruption, we rebuild the map from what we've read so far
				return indexMap, nil
			}
		}

		indexMap = append(indexMap, types.IndexOffsetPair{
			Index:  entry.Index,
			Offset: entryOffset,
		})
		lastIndex = entry.Index
		entriesRead++
	}

	b.logger.Infow("Log index map built successfully", "entries", entriesRead, "finalOffset", offset)
	return indexMap, nil
}
