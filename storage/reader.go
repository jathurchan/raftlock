package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// logEntryReader abstracts reading and decoding Raft log entries
// from a binary stream (e.g., a log file).
type logEntryReader interface {
	// ReadNext reads the next complete log entry from the current file offset.
	// Returns the parsed entry, number of bytes read, or an error (including io.EOF).
	ReadNext(file file) (types.LogEntry, int64, error)

	// ReadAtOffset seeks to a specific offset and reads a single log entry.
	// Optionally verifies the entry index against expectedIndex (if non-zero).
	ReadAtOffset(file file, offset int64, expectedIndex types.Index) (types.LogEntry, int64, error)

	// ScanRange reads all entries within [start, end) index range.
	// Returns an error if the context is canceled or if reading fails.
	ScanRange(ctx context.Context, file file, start, end types.Index) ([]types.LogEntry, error)
}

// defaultLogEntryReader provides a concrete implementation of LogEntryReader.
type defaultLogEntryReader struct {
	maxSize    int
	prefixSize int
	serializer serializer
	logger     logger.Logger
}

// newLogEntryReader constructs a defaultLogEntryReader with the given limits and serializer.
func newLogEntryReader(
	maxSize, prefixSize int,
	serializer serializer,
	log logger.Logger,
) logEntryReader {
	return &defaultLogEntryReader{
		maxSize:    maxSize,
		prefixSize: prefixSize,
		serializer: serializer,
		logger:     log.WithComponent("logreader"),
	}
}

// ReadNext attempts to read a complete log entry from the current offset of the file.
// It returns:
//   - the parsed LogEntry
//   - the total number of bytes read (prefix + body)
//   - an error, or io.EOF if the end of the file is reached without a full entry
//
// Format expected:
//
//	[prefixSize bytes] => uint32 (big-endian) indicating the entry length
//	[entry bytes]      => serialized log entry payload
func (r *defaultLogEntryReader) ReadNext(file file) (types.LogEntry, int64, error) {
	var bytesRead int64
	lenBuf := make([]byte, r.prefixSize)

	n, err := file.ReadFull(lenBuf)
	bytesRead += int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			r.logger.Debugw("EOF or partial read while reading prefix", "bytesRead", bytesRead)
			return types.LogEntry{}, bytesRead, io.EOF
		}
		r.logger.Errorw("Error reading entry length prefix", "error", err, "bytesRead", bytesRead)
		return types.LogEntry{}, bytesRead, err
	}

	length := binary.BigEndian.Uint32(lenBuf)
	if length == 0 || length > uint32(r.maxSize) {
		r.logger.Warnw("Invalid log entry length",
			"length", length,
			"maxAllowed", r.maxSize)
		return types.LogEntry{}, bytesRead, fmt.Errorf("%w: entry length %d (max %d)",
			ErrCorruptedLog, length, r.maxSize)
	}

	data := make([]byte, length)
	m, err := file.ReadFull(data)
	bytesRead += int64(m)
	if err != nil {
		r.logger.Errorw("Error reading log entry body", "error", err, "bytesRead", bytesRead)
		return types.LogEntry{}, bytesRead, err
	}

	entry, err := r.serializer.UnmarshalLogEntry(data)
	if err != nil {
		r.logger.Warnw("Failed to deserialize log entry", "error", err)
		return types.LogEntry{}, bytesRead, fmt.Errorf(
			"%w: deserialization failed: %w",
			ErrCorruptedLog,
			err,
		)
	}

	r.logger.Debugw("Read log entry", "index", entry.Index, "term", entry.Term, "size", bytesRead)
	return entry, bytesRead, nil
}

// ReadAtOffset seeks to the given offset and reads a log entry.
// If expectedIndex is non-zero, it verifies the entry’s index.
//
// Returns:
//   - Parsed LogEntry
//   - Total bytes read
//   - Error (including index mismatch or deserialization failure)
func (r *defaultLogEntryReader) ReadAtOffset(
	file file,
	offset int64,
	expectedIndex types.Index,
) (types.LogEntry, int64, error) {
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return types.LogEntry{}, 0, fmt.Errorf(
			"%w: failed to seek to offset: %w",
			ErrStorageIO,
			err,
		)
	}

	entry, bytesRead, err := r.ReadNext(file)
	if err != nil {
		return types.LogEntry{}, bytesRead, err
	}

	if expectedIndex != 0 && entry.Index != expectedIndex {
		return types.LogEntry{}, bytesRead, fmt.Errorf(
			"%w: index mismatch (expected %d, got %d)",
			ErrCorruptedLog,
			expectedIndex,
			entry.Index,
		)
	}

	return entry, bytesRead, nil
}

// ScanRange reads log entries from the file sequentially,
// returning only those within the [start, end) index range.
// Stops reading on EOF or when an entry exceeds the end index.
func (r *defaultLogEntryReader) ScanRange(
	ctx context.Context,
	file file,
	start, end types.Index,
) ([]types.LogEntry, error) {
	if start >= end {
		return []types.LogEntry{}, nil
	}

	entries := []types.LogEntry{}

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		entry, _, err := r.ReadNext(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("%w: failed to scan log entry: %w", ErrCorruptedLog, err)
		}

		if entry.Index < start {
			continue
		}

		if entry.Index >= end {
			break
		}

		entries = append(entries, entry)
	}

	return entries, nil
}
