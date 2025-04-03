package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/jathurchan/raftlock/types"
)

// IndexOffsetPair maps a log entry index to its file offset.
type IndexOffsetPair struct {
	Index  types.Index
	Offset int64
}

// buildIndexOffsetMap scans the log file and builds the index-to-offset map.
func (fs *FileStorage) buildIndexOffsetMap() error {
	if !fs.options.Features.EnableIndexMap {
		fs.logger.Debugw("Skipping index offset map build: feature disabled")
		return nil
	}

	logPath := fs.logFile()
	exists, err := fileExists(osFS{}, logPath)
	if err != nil {
		fs.logger.Errorw("Error checking log file existence", "error", err, "path", logPath)
		return fmt.Errorf("%w: error checking log file existence", ErrStorageIO)
	}

	if !exists {
		fs.logger.Infow("Log file does not exist, initializing empty index-offset map", "path", logPath)
		fs.indexToOffsetMap = make([]IndexOffsetPair, 0)
		return nil
	}

	fs.logMu.Lock()
	defer fs.logMu.Unlock()

	fs.logger.Debugw("Building index offset map from log file", "path", logPath)
	fs.indexToOffsetMap = make([]IndexOffsetPair, 0, DefaultIndexMapInitialCapacity)

	return fs.scanLogAndBuildMap(logPath)
}

// scanLogAndBuildMap scans the log file at logPath to rebuild the index-to-offset mapping.
// It handles and recovers from corruption by truncating the log at the failure point.
// IMPORTANT: Must be called with fs.logMu held.
func (fs *FileStorage) scanLogAndBuildMap(logPath string) error {
	file, err := os.Open(logPath)
	if err != nil {
		return fmt.Errorf("%w: failed to open log file %q: %v", ErrStorageIO, logPath, err)
	}
	defer file.Close()

	var (
		offset      int64
		lastIndex   types.Index
		entriesRead int
	)

	fs.logger.Infow("Starting log scan and index rebuild", "logPath", logPath)

	for {
		// entryOffset stores the offset where the current entry *starts*
		entryOffset := offset

		entry, bytesRead, err := fs.readNextEntry(file)
		offset += bytesRead

		if err == io.EOF {
			fs.logger.Infow("Reached end of log during scan", "entriesRead", entriesRead, "finalOffset", offset)
			break
		}
		if err != nil {
			return fs.handleCorruptionAndUpdateMetadata(file, logPath, entryOffset, "Failed to read or parse log entry", err)
		}

		if lastIndex > 0 {
			if entry.Index <= lastIndex {
				return fs.handleCorruptionAndUpdateMetadata(
					file, logPath, entryOffset,
					"Out-of-order log entry",
					fmt.Errorf("index %d <= previous %d", entry.Index, lastIndex))
			}
			if entry.Index > lastIndex+1 {
				return fs.handleCorruptionAndUpdateMetadata(
					file, logPath, entryOffset,
					"Gap in log index sequence",
					fmt.Errorf("index %d > previous %d + 1", entry.Index, lastIndex))
			}
		}

		fs.indexToOffsetMap = append(fs.indexToOffsetMap, IndexOffsetPair{
			Index:  entry.Index,
			Offset: entryOffset,
		})
		lastIndex = entry.Index
		entriesRead++
	}

	fs.logger.Infow("Log index map built successfully", "entries", entriesRead, "finalOffset", offset)
	return nil
}

// readNextEntry reads the next log entry starting from the current file offset.
// Returns the deserialized entry, total bytes read, or an error.
// Returns io.EOF if no complete entry is available.
func (fs *FileStorage) readNextEntry(file *os.File) (types.LogEntry, int64, error) {
	var bytesRead int64
	lenBuf := make([]byte, LengthPrefixSize)

	// Read length prefix
	n, err := io.ReadFull(file, lenBuf)
	bytesRead += int64(n)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return types.LogEntry{}, bytesRead, io.EOF
		}
		return types.LogEntry{}, bytesRead, fmt.Errorf("error reading entry length: %w", err)
	}

	length := binary.BigEndian.Uint32(lenBuf)
	if length == 0 || length > MaxEntrySizeBytes {
		return types.LogEntry{}, bytesRead, fmt.Errorf("%w: invalid entry length %d (max %d)", ErrCorruptedLog, length, MaxEntrySizeBytes)
	}

	// Read data
	data := make([]byte, length)
	m, err := io.ReadFull(file, data)
	bytesRead += int64(m)
	if err != nil {
		return types.LogEntry{}, bytesRead, fmt.Errorf("error reading entry data: %w", err)
	}

	// Deserialize
	entry, err := fs.deserializeEntry(data)
	if err != nil {
		return types.LogEntry{}, bytesRead, fmt.Errorf("%w: failed to deserialize entry: %v", ErrCorruptedLog, err)
	}

	return entry, bytesRead, nil
}

// deserializeEntry deserializes a log entry from raw bytes based on storage options.
func (fs *FileStorage) deserializeEntry(data []byte) (types.LogEntry, error) {
	if fs.options.Features.EnableBinaryFormat {
		return deserializeLogEntry(data)
	}
	var entry types.LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return types.LogEntry{}, err
	}
	return entry, nil
}

func (fs *FileStorage) handleCorruptionAndUpdateMetadata(file *os.File, logPath string, entryOffset int64, reason string, err error) error {
	corruptionErr := fs.handleCorruption(file, logPath, entryOffset, reason, err)
	if corruptionErr != nil {
		return fmt.Errorf("failed to handle log corruption: %w", corruptionErr)
	}
	return fs.syncMetadataFromIndex("corruption recovery")
}

// handleCorruption logs the corruption, closes the file handle, truncates the log file,
// and returns any error encountered during closing or truncation.
func (fs *FileStorage) handleCorruption(file *os.File, path string, offset int64, reason string, err error) error {
	fs.logger.Warnw("Log corruption detected; truncating log file",
		"reason", reason,
		"corruptionOffset", offset,
		"logPath", path,
		"error", err,
	)

	if closeErr := file.Close(); closeErr != nil {
		fs.logger.Errorw("Failed to close log file before truncating", "path", path, "error", closeErr)
	}

	if truncErr := fs.truncateLogAt(path, offset); truncErr != nil {
		fs.logger.Errorw("Failed to truncate corrupted log file", "path", path, "offset", offset, "error", truncErr)
		return truncErr
	}

	fs.logger.Infow("Successfully truncated corrupted log file", "path", path, "offset", offset)
	return nil
}

// truncateLogAt truncates the log file at the specified offset.
// Assumes the file handle associated with the path has already been closed by the caller.
func (fs *FileStorage) truncateLogAt(path string, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("invalid negative offset (%d) for truncation", offset)
	}
	return os.Truncate(path, offset)
}

// verifyLogConsistency ensures that the metadata (first/last log indices) aligns with the log content,
// and verifies that the log entries are continuous without gaps.
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

// syncMetadataFromMap updates the first/last log indices based on the
// current indexToOffsetMap and persists the result.
// IMPORTANT: Must be called with fs.logMu held.
func (fs *FileStorage) syncMetadataFromIndex(operationContext string) error {
	if len(fs.indexToOffsetMap) > 0 {
		newFirstIndex := fs.indexToOffsetMap[0].Index
		newLastIndex := fs.indexToOffsetMap[len(fs.indexToOffsetMap)-1].Index

		currentFirst := types.Index(fs.firstLogIndex.Load())
		currentLast := types.Index(fs.lastLogIndex.Load())
		if currentFirst == newFirstIndex && currentLast == newLastIndex {
			fs.logger.Debugw("Metadata already up to date, skipping update",
				"context", operationContext,
				"firstIndex", newFirstIndex,
				"lastIndex", newLastIndex)
			return nil
		}

		fs.firstLogIndex.Store(uint64(newFirstIndex))
		fs.lastLogIndex.Store(uint64(newLastIndex))

		fs.logger.Infow("Updated metadata from index map",
			"context", operationContext,
			"firstIndex", newFirstIndex,
			"lastIndex", newLastIndex)
	} else {
		// No entries in the map: reset state only if it's not already zeroed
		currentFirst := types.Index(fs.firstLogIndex.Load())
		currentLast := types.Index(fs.lastLogIndex.Load())

		if currentFirst == 0 && currentLast == 0 {
			fs.logger.Debugw("Index map empty and metadata already zeroed",
				"context", operationContext)
			return nil
		}

		fs.initializeEmptyMetadata()
		fs.logger.Warnw("Index map empty, reset metadata to empty state",
			"context", operationContext)
	}

	if err := fs.saveMetadata(); err != nil {
		fs.logger.Errorw(fmt.Sprintf("Failed to persist metadata during %s", operationContext),
			"error", err)
		return fmt.Errorf("%w: failed saving metadata during %s", ErrStorageIO, operationContext)
	}
	return nil
}

// verifyLogContinuity ensures that the indexToOffsetMap forms a contiguous sequence of log entries.
func (fs *FileStorage) verifyLogContinuity() error {
	mapLen := len(fs.indexToOffsetMap)

	if mapLen == 0 {
		fs.logger.Debugw("Log continuity check skipped: empty index map")
		return nil
	}

	for i := 1; i < mapLen; i++ {
		expected := fs.indexToOffsetMap[i-1].Index + 1
		actual := fs.indexToOffsetMap[i].Index

		if actual != expected {
			fs.logger.Errorw("Discontinuity detected in log index map",
				"previousIndex", fs.indexToOffsetMap[i-1].Index,
				"expectedNext", expected,
				"actualNext", actual)
			return fmt.Errorf("%w: log entries not contiguous at %d -> %d",
				ErrCorruptedLog, fs.indexToOffsetMap[i-1].Index, actual)
		}
	}

	fs.logger.Debugw("Log continuity verified: all entries are contiguous")
	return nil
}
