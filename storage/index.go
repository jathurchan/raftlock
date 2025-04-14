package storage

import (
	"context"
	"fmt"
	"io"
	"sort"

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

	// ReadInRange returns log entries in the specified [start, end) index range using the index map.
	// Offsets from the index map are used to seek entries directly within the log file.
	ReadInRange(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error)

	// VerifyConsistency ensures that the provided index-offset map contains
	// strictly increasing and gapless indices. If any discontinuity or out-of-order
	// entry is found, it returns an error indicating log corruption.
	VerifyConsistency(indexMap []types.IndexOffsetPair) error

	// GetBounds analyzes the given index map and compares it against the current metadata range.
	// It returns a boundsResult indicating the new first and last indices, whether the metadata has changed,
	// and whether the change represents a full reset (i.e., the index map is empty).
	GetBounds(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult

	// Append appends new index-offset entries to the given base map.
	// If additions is empty, the original map is returned unmodified.
	Append(base, additions []types.IndexOffsetPair) []types.IndexOffsetPair

	// TruncateLast removes the last 'count' entries from the index map.
	// If count is zero or less, the original map is returned.
	// If count is greater than or equal to the length of the map, an empty slice is returned.
	TruncateLast(indexMap []types.IndexOffsetPair, count int) []types.IndexOffsetPair

	// FindFirstIndexAtOrAfter returns the index of the first entry in the index map
	// whose log index is greater than or equal to the specified target.
	// If all entries are less than the target, it returns len(indexMap).
	// This is typically used for truncation and range scanning.
	//
	// The returned value can be used to slice or seek into the index map safely.
	FindFirstIndexAtOrAfter(indexMap []types.IndexOffsetPair, target types.Index) int

	// TruncateAfter returns a slice of the indexMap with all entries AFTER the specified index removed.
	// Keeps entries with Index <= target.
	TruncateAfter(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair

	// TruncateBefore returns a slice of the indexMap with all entries BEFORE the specified index removed.
	// Keeps entries with Index >= target.
	TruncateBefore(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair
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
func (is *defaultIndexService) scanLogAndBuildMap(logPath string) (buildResult, error) {
	file, err := is.fs.Open(logPath)
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
		indexMap = make([]types.IndexOffsetPair, 0, defaultIndexMapInitialCapacity)
	)

	is.logger.Infow("Starting log scan to build index-offset map", "path", logPath)

	for {
		entryOffset := offset

		entry, bytesRead, err := is.reader.ReadNext(file)
		offset += bytesRead

		switch {
		case err == io.EOF:
			is.logger.Infow("Log scan complete", "entriesRead", entriesRead, "finalOffset", offset)
			return buildResult{
				IndexMap:       indexMap,
				Truncated:      false,
				LastValidIndex: 0,
			}, nil

		case err != nil:
			return is.buildCorruptionResult(indexMap, lastIndex,
				is.handleCorruption(logPath, entryOffset, "read failure", err))
		}

		// Ensure log indices are strictly increasing and contiguous
		if lastIndex > 0 {
			if entry.Index <= lastIndex {
				reason := fmt.Sprintf("out-of-order index (current=%d, last=%d)", entry.Index, lastIndex)
				return is.buildCorruptionResult(indexMap, lastIndex,
					is.handleCorruption(logPath, entryOffset, reason, nil))
			}
			if entry.Index > lastIndex+1 {
				reason := fmt.Sprintf("index gap (expected=%d, found=%d)", lastIndex+1, entry.Index)
				return is.buildCorruptionResult(indexMap, lastIndex,
					is.handleCorruption(logPath, entryOffset, reason, nil))
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
func (is *defaultIndexService) buildCorruptionResult(
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
func (is *defaultIndexService) handleCorruption(path string, offset int64, reason string, err error) error {
	logFields := []any{
		"logPath", path,
		"offset", offset,
		"corruptionReason", reason,
	}
	if err != nil {
		logFields = append(logFields, "error", err)
	}

	is.logger.Warnw("Corruption detected during log scan", logFields...)

	truncErr := is.truncateLogAt(path, offset)
	if truncErr != nil {
		is.logger.Errorw("Failed to truncate log after corruption",
			"logPath", path,
			"offset", offset,
			"error", truncErr,
		)
		return fmt.Errorf("corruption at offset %d: %w", offset, truncErr)
	}

	is.logger.Infow("Corrupted log successfully truncated",
		"logPath", path,
		"offset", offset,
	)
	return nil
}

// truncateLogAt cuts the log file at the specified offset.
func (is *defaultIndexService) truncateLogAt(path string, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("invalid negative offset (%d) for truncation", offset)
	}
	return is.fs.Truncate(path, offset)
}

// ReadInRange extracts log entries between the given start and end indices (exclusive).
// Seeks directly to byte offsets using the index map for efficient access.
// Returns the entries, total bytes read, or an error.
func (is *defaultIndexService) ReadInRange(
	ctx context.Context,
	logPath string,
	indexMap []types.IndexOffsetPair,
	start, end types.Index,
) ([]types.LogEntry, int64, error) {
	if start >= end {
		is.logger.Debugw("Start index is greater than or equal to end index", "start", start, "end", end)
		return nil, 0, nil // Or return an error: fmt.Errorf("invalid index range: start (%d) >= end (%d)", start, end)
	}

	if len(indexMap) == 0 {
		is.logger.Debugw("Index map is empty", "start", start, "end", end)
		return nil, 0, nil
	}

	file, err := is.fs.Open(logPath)
	if err != nil {
		is.logger.Errorw("Failed to open log file for reading", "path", logPath, "error", err)
		return nil, 0, fmt.Errorf("%w: failed to open log file: %v", ErrStorageIO, err)
	}
	defer file.Close()

	startIdx := findStartIndexInMap(indexMap, start)
	estimatedCap := estimateCapacity(indexMap, start, end)

	entries, totalBytes, err := is.readEntriesInRange(ctx, file, indexMap, startIdx, end, estimatedCap)
	if err != nil {
		is.logger.Errorw("Failed to read entries in range", "start", start, "end", end, "error", err)
		return entries, totalBytes, err
	}

	is.logger.Debugw("ReadInRange completed",
		"logPath", logPath,
		"start", start,
		"end", end,
		"entriesRead", len(entries),
		"totalBytes", totalBytes,
	)
	return entries, totalBytes, nil
}

// findStartIndexInMap performs a binary search to locate the first index >= target.
func findStartIndexInMap(indexMap []types.IndexOffsetPair, target types.Index) int {
	return sort.Search(len(indexMap), func(i int) bool {
		return indexMap[i].Index >= target
	})
}

// estimateCapacity estimates the number of entries between start and end for slice preallocation.
func estimateCapacity(indexMap []types.IndexOffsetPair, start, end types.Index) int {
	startIdx := findStartIndexInMap(indexMap, start)
	endIdx := findStartIndexInMap(indexMap, end)
	capacity := endIdx - startIdx
	if capacity < 0 {
		return 0
	}
	return capacity
}

// readEntriesInRange reads and decodes log entries starting at startIdx up to (but not including) `end` index.
// Respects cancellation via context. Returns entries and cumulative bytes read.
func (is *defaultIndexService) readEntriesInRange(
	ctx context.Context,
	file file,
	indexMap []types.IndexOffsetPair,
	startIdx int,
	end types.Index,
	capacity int,
) ([]types.LogEntry, int64, error) {
	entries := make([]types.LogEntry, 0, capacity)
	var totalBytes int64

	for i := startIdx; i < len(indexMap); i++ {
		pair := indexMap[i]
		if pair.Index >= end {
			break
		}

		if i%20 == 0 && ctx.Err() != nil {
			is.logger.Warnw("Context canceled during ReadInRange", "cancelIndex", pair.Index, "error", ctx.Err())
			return entries, totalBytes, ctx.Err()
		}

		entry, n, err := is.reader.ReadAtOffset(file, pair.Offset, pair.Index)
		if err != nil {
			is.logger.Errorw("Failed to read entry", "index", pair.Index, "offset", pair.Offset, "error", err)
			return entries, totalBytes, err
		}

		entries = append(entries, entry)
		totalBytes += n
	}

	return entries, totalBytes, nil
}

// VerifyConsistency checks that the index map has strictly increasing and gapless indices.
// Returns an error if continuity is broken.
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

// Append extends the existing index-offset map with new entries.
func (s *defaultIndexService) Append(base, additions []types.IndexOffsetPair) []types.IndexOffsetPair {
	if len(additions) == 0 {
		s.logger.Debugw("No additions to append", "baseLen", len(base))
		return base
	}
	s.logger.Debugw("Appending entries to index map", "baseLen", len(base), "addLen", len(additions))
	return append(base, additions...)
}

// TruncateLast removes the last 'count' entries from the index-offset map.
func (s *defaultIndexService) TruncateLast(indexMap []types.IndexOffsetPair, count int) []types.IndexOffsetPair {
	if count <= 0 {
		s.logger.Debugw("No truncation needed", "mapSize", len(indexMap))
		return indexMap
	}

	if count >= len(indexMap) {
		s.logger.Warnw("Truncation count exceeds or matches map size — returning empty", "count", count, "mapSize", len(indexMap))
		return nil
	}

	newLen := len(indexMap) - count
	s.logger.Debugw("Truncating last entries", "originalLen", len(indexMap), "newLen", newLen)
	return indexMap[:newLen]
}

// FindFirstIndexAtOrAfter performs a binary search on the provided index map,
// returning the position of the first log index that is >= the target index.
// If no such index exists, it returns len(indexMap). This is useful for truncation
// operations, range lookups, and efficient seeking within the log.
//
// Example:
//
//	map = [{Index: 5}, {Index: 7}, {Index: 9}]
//	target = 6 -> returns 1 (index 7)
//	target = 10 -> returns 3 (past end)
func (s *defaultIndexService) FindFirstIndexAtOrAfter(indexMap []types.IndexOffsetPair, target types.Index) int {
	return sort.Search(len(indexMap), func(i int) bool {
		return indexMap[i].Index >= target
	})
}

func (s *defaultIndexService) TruncateAfter(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair {
	if len(indexMap) == 0 {
		return nil
	}
	pos := sort.Search(len(indexMap), func(i int) bool {
		return indexMap[i].Index > target
	})
	s.logger.Debugw("Truncating after index", "target", target, "originalLen", len(indexMap), "newLen", pos)
	return indexMap[:pos]
}

func (s *defaultIndexService) TruncateBefore(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair {
	if len(indexMap) == 0 {
		return nil
	}
	pos := sort.Search(len(indexMap), func(i int) bool {
		return indexMap[i].Index >= target
	})
	s.logger.Debugw("Truncating before index", "target", target, "originalLen", len(indexMap), "newLen", len(indexMap)-pos)
	return indexMap[pos:]
}
