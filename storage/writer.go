package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// logWriter defines an interface for writing a sequence of log entries to a file.
// It returns a list of index-offset pairs for the written entries and the final offset.
type logWriter interface {
	// WriteEntriesToFile writes a slice of log entries to the file starting at a given offset.
	// Returns:
	//   - index-offset pairs for written entries (may be partial)
	//   - final file offset after the last successful write
	//   - error if serialization or I/O fails
	WriteEntriesToFile(
		ctx context.Context,
		f file,
		startOffset int64,
		entries []types.LogEntry,
	) (offsets []types.IndexOffsetPair, finalOffset int64, err error)
}

// defaultLogWriter is the default implementation of logWriter.
type defaultLogWriter struct {
	serializer serializer
	logger     logger.Logger
}

// newLogWriter creates a new defaultLogWriter with the given serializer and logger.
func newLogWriter(serializer serializer, logger logger.Logger) logWriter {
	return &defaultLogWriter{
		serializer: serializer,
		logger:     logger.WithComponent("logwriter"),
	}
}

// WriteEntriesToFile serializes and writes log entries to the file starting at startOffset.
// If the context is canceled or an error occurs, it returns partial results.
func (w *defaultLogWriter) WriteEntriesToFile(
	ctx context.Context,
	f file,
	startOffset int64,
	entries []types.LogEntry,
) ([]types.IndexOffsetPair, int64, error) {
	offsets := make([]types.IndexOffsetPair, 0, len(entries))
	pos := startOffset
	lenBuf := make([]byte, lengthPrefixSize)

	for i, entry := range entries {
		// Periodically check for context cancellation to avoid excessive overhead
		if i%10 == 0 && ctx.Err() != nil {
			w.logger.Warnw("Write aborted: context cancelled",
				"index", entry.Index,
				"writtenEntries", len(offsets),
				"offset", pos,
				"error", ctx.Err(),
			)
			return offsets, pos, ctx.Err()
		}

		offsets = append(offsets, types.IndexOffsetPair{
			Index:  entry.Index,
			Offset: pos,
		})

		newPos, err := w.writeEntryToFile(f, entry, pos, lenBuf)
		if err != nil {
			w.logger.Errorw("Failed to write log entry",
				"index", entry.Index,
				"offset", pos,
				"error", err,
			)
			return nil, pos, fmt.Errorf(
				"%w: failed writing log entry at index %d",
				ErrStorageIO,
				entry.Index,
			)
		}
		pos = newPos
	}

	w.logger.Debugw("Successfully wrote log entries",
		"count", len(entries),
		"startOffset", startOffset,
		"endOffset", pos,
	)
	return offsets, pos, nil
}

// writeEntryToFile serializes and writes a single entry with a 4-byte length prefix.
// Returns new offset after the write or an error on failure.
func (w *defaultLogWriter) writeEntryToFile(
	f file,
	entry types.LogEntry,
	pos int64,
	lenBuf []byte,
) (int64, error) {
	data, err := w.serializer.MarshalLogEntry(entry)
	if err != nil {
		w.logger.Errorw("Failed to serialize log entry",
			"index", entry.Index,
			"error", err,
		)
		return pos, fmt.Errorf("serialize entry %d: %w", entry.Index, err)
	}

	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	for _, chunk := range [][]byte{lenBuf, data} {
		n, err := f.Write(chunk)
		if err != nil {
			w.logger.Errorw("Failed to write chunk to log file",
				"index", entry.Index,
				"chunkSize", len(chunk),
				"offset", pos,
				"error", err,
			)
			return pos, fmt.Errorf("write chunk for entry %d: %w", entry.Index, err)
		}
		pos += int64(n)
	}

	w.logger.Debugw("Wrote entry",
		"index", entry.Index,
		"entrySize", len(data),
		"totalBytes", pos,
	)
	return pos, nil
}

// appendResult represents the result of a log append operation.
type appendResult struct {
	FirstIndex types.Index
	LastIndex  types.Index
	Offsets    []types.IndexOffsetPair
}

// logAppender defines the interface for safely validating and appending log entries to disk.
type logAppender interface {
	Append(
		ctx context.Context,
		entries []types.LogEntry,
		currentLast types.Index,
	) (appendResult, error)
}

// defaultLogAppender implements logAppender.
type defaultLogAppender struct {
	logFilePath  string
	fileSystem   fileSystem
	writer       logWriter
	logger       logger.Logger
	syncOnAppend bool
}

// newLogAppender returns a new logAppender.
func newLogAppender(
	logFilePath string,
	fs fileSystem,
	writer logWriter,
	log logger.Logger,
	sync bool,
) logAppender {
	return &defaultLogAppender{
		logFilePath:  logFilePath,
		fileSystem:   fs,
		writer:       writer,
		logger:       log.WithComponent("logappender"),
		syncOnAppend: sync,
	}
}

// Append validates and appends a batch of log entries to the log file.
// It performs rollback on failure and optionally syncs to disk.
func (a *defaultLogAppender) Append(
	ctx context.Context,
	entries []types.LogEntry,
	currentLast types.Index,
) (appendResult, error) {
	if len(entries) == 0 {
		return appendResult{}, ErrEmptyEntries
	}

	firstIndex := entries[0].Index
	lastIndex := entries[len(entries)-1].Index

	if err := a.validateEntries(entries, currentLast, firstIndex, lastIndex); err != nil {
		return appendResult{}, err
	}

	file, startOffset, err := a.openLogFile()
	if err != nil {
		return appendResult{}, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			// Error path is handled by rollback
		}
	}()

	offsets, _, err := a.writer.WriteEntriesToFile(ctx, file, startOffset, entries)
	if err != nil {
		// Perform rollback if writing fails
		return appendResult{}, a.performRollback(file, startOffset, err)
	}

	if err := a.syncIfNeeded(file); err != nil {
		// Perform rollback if syncing fails
		return appendResult{}, a.performRollback(file, startOffset, err)
	}

	return appendResult{
		FirstIndex: firstIndex,
		LastIndex:  lastIndex,
		Offsets:    offsets,
	}, nil
}

// validateEntries checks index continuity and order.
func (a *defaultLogAppender) validateEntries(
	entries []types.LogEntry,
	currentLast, first, last types.Index,
) error {
	if first > last {
		return ErrOutOfOrderEntries
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return ErrOutOfOrderEntries
		}
	}
	if currentLast > 0 && first != currentLast+1 {
		return fmt.Errorf(
			"%w: expected first index %d, got %d",
			ErrNonContiguousEntries,
			currentLast+1,
			first,
		)
	}
	if currentLast == 0 && first != 1 {
		return fmt.Errorf(
			"%w: first index must be 1 for empty log, got %d",
			ErrNonContiguousEntries,
			first,
		)
	}
	return nil
}

// openLogFile opens the log file for appending and seeks to the end.
func (a *defaultLogAppender) openLogFile() (file, int64, error) {
	f, err := a.fileSystem.AppendFile(a.logFilePath)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: open log file: %w", ErrStorageIO, err)
	}
	pos, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		a.logger.Errorw("seek failed on log file: %w", err)
		return nil, 0, fmt.Errorf("%w: seek log file: %w", ErrStorageIO, err)
	}
	return f, pos, nil
}

// performRollback truncates the file back to its original state and handles all cleanup.
func (a *defaultLogAppender) performRollback(f file, startOffset int64, writeErr error) error {
	a.logger.Warnw("rolling back write operation",
		"offset", startOffset,
		"reason", writeErr,
	)

	if truncErr := a.fileSystem.Truncate(a.logFilePath, startOffset); truncErr != nil {
		a.logger.Errorw("rollback failed: unable to truncate file",
			"path", a.logFilePath,
			"offset", startOffset,
			"error", truncErr,
		)
	}

	if closeErr := f.Close(); closeErr != nil {
		a.logger.Errorw("rollback failed: unable to close file",
			"path", a.logFilePath,
			"error", closeErr,
		)
	}

	return fmt.Errorf("%w: %w", ErrStorageIO, writeErr)
}

// syncIfNeeded flushes the file if syncOnAppend is enabled.
func (a *defaultLogAppender) syncIfNeeded(f file) error {
	if !a.syncOnAppend {
		return nil
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
	return nil
}

// logRewriter defines an interface for replacing the entire log file with a new set of entries.
// It ensures atomic replacement and returns index-offset mappings for the new entries.
type logRewriter interface {
	// Rewrite replaces the log with the given entries, writing them from scratch.
	//
	// Returns:
	//   - slice of IndexOffsetPair for each entry written
	//   - error if the rewrite fails at any step
	Rewrite(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error)
}

// defaultLogRewriter provides a default implementation of logRewriter.
// It writes log entries to a temporary file, syncs it, then atomically renames it to replace the existing log.
type defaultLogRewriter struct {
	logPath string
	fs      fileSystem
	writer  logWriter
	logger  logger.Logger
}

// newLogRewriter creates a new defaultLogRewriter using the given path, filesystem, serializer, and logger.
func newLogRewriter(logPath string, fs fileSystem, ser serializer, log logger.Logger) logRewriter {
	return &defaultLogRewriter{
		logPath: logPath,
		fs:      fs,
		writer:  newLogWriter(ser, log),
		logger:  log.WithComponent("logrewriter"),
	}
}

// Rewrite replaces the current log file with a new one containing the given entries.
//
// The rewrite process:
//  1. Creates a temporary log file.
//  2. Writes entries using the logWriter.
//  3. Syncs the file to disk.
//  4. Atomically renames it over the original log.
//
// If any step fails, it performs cleanup and returns an appropriate error.
func (r *defaultLogRewriter) Rewrite(
	ctx context.Context,
	entries []types.LogEntry,
) ([]types.IndexOffsetPair, error) {
	tmpPath := r.fs.TempPath(r.logPath)

	tmpFile, err := r.prepareTempFile(tmpPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			r.cleanupTempFile(tmpFile, tmpPath)
		}
	}()

	offsets, finalOffset, err := r.writeAndSyncEntries(ctx, tmpFile, entries)
	if err != nil {
		return nil, err
	}

	if err := r.replaceLogFile(tmpPath); err != nil {
		return nil, err
	}

	r.logger.Infow("Log rewrite complete",
		"entries", len(entries),
		"finalOffset", finalOffset,
		"logPath", r.logPath,
	)

	return offsets, nil
}

// prepareTempFile creates and opens a temporary file for log rewriting.
//
// It sets up the file with appropriate permissions and returns an opened file handle.
func (r *defaultLogRewriter) prepareTempFile(tmpPath string) (file, error) {
	if err := r.fs.WriteMaybeAtomic(tmpPath, nil, ownRWOthR, false); err != nil {
		return nil, fmt.Errorf("%w: failed to create temporary log file: %w", ErrStorageIO, err)
	}

	tmpFile, err := r.fs.AppendFile(tmpPath)
	if err != nil {
		_ = r.fs.Remove(tmpPath)
		return nil, fmt.Errorf("%w: failed to open temporary log file: %w", ErrStorageIO, err)
	}

	return tmpFile, nil
}

// writeAndSyncEntries writes all log entries to the given file,
// ensures the data is flushed to disk, and closes the file.
func (r *defaultLogRewriter) writeAndSyncEntries(
	ctx context.Context,
	f file,
	entries []types.LogEntry,
) ([]types.IndexOffsetPair, int64, error) {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: failed to seek to start of temp file: %w", ErrStorageIO, err)
	}

	offsets, finalOffset, err := r.writer.WriteEntriesToFile(ctx, f, 0, entries)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: failed writing entries to temp file: %w", ErrStorageIO, err)
	}

	if err := f.Sync(); err != nil {
		return nil, 0, fmt.Errorf("%w: failed to sync temp log file: %w", ErrStorageIO, err)
	}

	if err := f.Close(); err != nil {
		return nil, 0, fmt.Errorf("%w: failed to close temp log file: %w", ErrStorageIO, err)
	}

	return offsets, finalOffset, nil
}

// replaceLogFile atomically replaces the original log file with the temporary one.
//
// It renames the temporary file to the live log path. If renaming fails,
// it removes the temporary file to avoid orphaned state.
func (r *defaultLogRewriter) replaceLogFile(tmpPath string) error {
	if err := r.fs.Rename(tmpPath, r.logPath); err != nil {
		_ = r.fs.Remove(tmpPath)
		return fmt.Errorf("%w: failed to rename temp file to %s: %w", ErrStorageIO, r.logPath, err)
	}
	return nil
}

// cleanupTempFile safely closes and removes a temporary log file.
//
// This is used during error recovery to avoid leaving corrupted or partial files on disk.
func (r *defaultLogRewriter) cleanupTempFile(f file, path string) {
	if f != nil {
		if err := f.Close(); err != nil {
			r.logger.Errorw("failed to close temp file during cleanup", "path", path, "error", err)
		}
	}
	if err := r.fs.Remove(path); err != nil && !r.fs.IsNotExist(err) {
		r.logger.Errorw("failed to remove temp file during cleanup", "path", path, "error", err)
	}
}
