package storage

import (
	"fmt"
	"io"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

type buildResult struct {
	IndexMap       []types.IndexOffsetPair
	Truncated      bool
	LastValidIndex types.Index // Ignored if Truncated == false
}

type boundsResult struct {
	NewFirst types.Index
	NewLast  types.Index
	Changed  bool
	WasReset bool
}

// indexService defines operations for interpreting index-offset mappings derived from log files.
// These mappings allow efficient access to log entries by their index and enable validation of log continuity.
type indexService interface {
	// Build parses the log file at the specified path and constructs a mapping of log indices
	// to their corresponding byte offsets within the file.
	// If the log file is missing, it returns an empty result without error.
	// If corruption is detected during parsing, a truncated result is returned along with an error.
	Build(logPath string) (buildResult, error)

	// VerifyConsistency ensures that the provided index-offset map contains
	// strictly increasing and gapless indices. If any discontinuity or out-of-order
	// entry is found, it returns an error indicating log corruption.
	VerifyConsistency(indexMap []types.IndexOffsetPair) error

	// GetBounds analyzes the given index map and compares it against the current metadata range.
	// It returns a boundsResult indicating the new first and last indices, whether the metadata has changed,
	// and whether the change represents a full reset (i.e., the index map is empty).
	GetBounds(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult
}

// defaultIndexService provides a filesystem-backed implementation of indexService.
type defaultIndexService struct {
	fs     fileSystem
	reader logEntryReader
	logger logger.Logger
}

// newIndexServiceWithReader creates a new indexService using the provided FileSystem and LogEntryReader.
func newIndexServiceWithReader(
	fs fileSystem,
	reader logEntryReader,
	logger logger.Logger,
) indexService {
	return &defaultIndexService{
		fs:     fs,
		reader: reader,
		logger: logger.WithComponent("indexmap"),
	}
}

// Build constructs an index-offset map for a log file at logPath.
// If the log file doesn't exist, it returns an empty map.
func (is *defaultIndexService) Build(logPath string) (buildResult, error) {
	exists, err := is.fs.Exists(logPath)
	if err != nil {
		is.logger.Errorw("Error checking log file existence", "path", logPath)
		return buildResult{}, fmt.Errorf("%w: error checking log file existence", ErrStorageIO)
	}
	if !exists {
		is.logger.Infow("Log file does not exist, returning empty index map", "path", logPath)
		return buildResult{
			IndexMap:       []types.IndexOffsetPair{},
			Truncated:      false,
			LastValidIndex: 0,
		}, nil
	}

	return is.scanLogAndBuildMap(logPath)
}

// scanLogAndBuildMap reads entries from a log file and builds an index-offset map.
// Detects and handles corruption, enforces monotonic and gapless index order.
func (s *defaultIndexService) scanLogAndBuildMap(logPath string) (buildResult, error) {
	file, err := s.fs.Open(logPath)
	if err != nil {
		return buildResult{}, fmt.Errorf("%w: failed to open log file %q: %v", ErrStorageIO, logPath, err)
	}
	defer file.Close()

	var (
		offset      int64
		lastIndex   types.Index
		entriesRead int

		// Preallocate capacity for performance, but start with zero length.
		// This avoids including default-zero entries in the index map,
		// ensuring that all appended entries are valid log entries.
		indexMap = make([]types.IndexOffsetPair, 0, DefaultIndexMapInitialCapacity)
	)

	s.logger.Infow("Starting log scan to build index-offset map", "path", logPath)

	for {
		entryOffset := offset

		entry, bytesRead, err := s.reader.ReadNext(file)
		offset += bytesRead

		switch {
		case err == io.EOF:
			s.logger.Infow("Log scan complete", "entriesRead", entriesRead, "finalOffset", offset)
			return buildResult{
				IndexMap:       indexMap,
				Truncated:      false,
				LastValidIndex: 0,
			}, nil

		case err != nil:
			return s.buildCorruptionResult(indexMap, lastIndex,
				s.handleCorruption(logPath, entryOffset, "read failure", err))
		}

		// Ensure log indices are strictly increasing and contiguous
		if lastIndex > 0 {
			if entry.Index <= lastIndex {
				reason := fmt.Sprintf("out-of-order index (current=%d, last=%d)", entry.Index, lastIndex)
				return s.buildCorruptionResult(indexMap, lastIndex,
					s.handleCorruption(logPath, entryOffset, reason, nil))
			}
			if entry.Index > lastIndex+1 {
				reason := fmt.Sprintf("index gap (expected=%d, found=%d)", lastIndex+1, entry.Index)
				return s.buildCorruptionResult(indexMap, lastIndex,
					s.handleCorruption(logPath, entryOffset, reason, nil))
			}
		}

		// Append valid entry to the map
		indexMap = append(indexMap, types.IndexOffsetPair{
			Index:  entry.Index,
			Offset: entryOffset,
		})

		lastIndex = entry.Index
		entriesRead++
	}
}

// buildCorruptionResult centralizes returning a truncated result with error.
func (s *defaultIndexService) buildCorruptionResult(
	indexMap []types.IndexOffsetPair,
	lastIndex types.Index,
	corruptionErr error,
) (buildResult, error) {
	return buildResult{
		IndexMap:       indexMap,
		Truncated:      true,
		LastValidIndex: lastIndex,
	}, corruptionErr
}

// handleCorruption logs the corruption details and attempts to truncate the log at the given offset.
func (s *defaultIndexService) handleCorruption(path string, offset int64, reason string, err error) error {
	logFields := []any{
		"logPath", path,
		"offset", offset,
		"corruptionReason", reason,
	}
	if err != nil {
		logFields = append(logFields, "error", err)
	}

	s.logger.Warnw("Corruption detected during log scan", logFields...)

	truncErr := s.truncateLogAt(path, offset)
	if truncErr != nil {
		s.logger.Errorw("Failed to truncate log after corruption",
			"logPath", path,
			"offset", offset,
			"error", truncErr,
		)
		return fmt.Errorf("corruption at offset %d: %w", offset, truncErr)
	}

	s.logger.Infow("Corrupted log successfully truncated",
		"logPath", path,
		"offset", offset,
	)
	return nil
}

// truncateLogAt cuts the log file at the specified offset.
func (s *defaultIndexService) truncateLogAt(path string, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("invalid negative offset (%d) for truncation", offset)
	}
	return s.fs.Truncate(path, offset)
}

// VerifyConsistency checks that the index map contains strictly increasing and contiguous indices.
func (is *defaultIndexService) VerifyConsistency(indexMap []types.IndexOffsetPair) error {
	if len(indexMap) == 0 {
		is.logger.Debugw("Index map empty — skipping consistency check")
		return nil
	}

	for i := 1; i < len(indexMap); i++ {
		prev := indexMap[i-1].Index
		curr := indexMap[i].Index
		if curr != prev+1 {
			is.logger.Errorw("Log continuity broken",
				"expected", prev+1,
				"got", curr,
				"position", i)
			return fmt.Errorf("%w: log discontinuity at %d (expected %d, got %d)",
				ErrCorruptedLog, i, prev+1, curr)
		}
	}

	is.logger.Debugw("Log consistency OK", "entries", len(indexMap))
	return nil
}

// GetBounds computes the desired metadata bounds based on the contents of the index map,
// and compares them to the currently stored first and last indices.
func (s *defaultIndexService) GetBounds(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
	s.logger.Debugw("Calculating bounds",
		"currentFirst", currentFirst,
		"currentLast", currentLast,
		"indexMapLength", len(indexMap),
	)
	if len(indexMap) == 0 {
		if currentFirst == 0 && currentLast == 0 {
			s.logger.Debugw("Index map empty, no change to bounds")
			return boundsResult{0, 0, false, false}
		}
		s.logger.Infow("Index map is empty, resetting bounds",
			"previousFirst", currentFirst,
			"previousLast", currentLast,
		)
		return boundsResult{0, 0, true, true}
	}

	newFirst := indexMap[0].Index
	newLast := indexMap[len(indexMap)-1].Index

	if newFirst == currentFirst && newLast == currentLast {
		s.logger.Debugw("Bounds unchanged",
			"newFirst", newFirst,
			"newLast", newLast,
		)
		return boundsResult{newFirst, newLast, false, false}
	}

	s.logger.Infow("Bounds changed",
		"newFirst", newFirst,
		"newLast", newLast,
		"previousFirst", currentFirst,
		"previousLast", currentLast,
	)
	return boundsResult{newFirst, newLast, true, false}
}
