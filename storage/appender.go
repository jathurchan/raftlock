package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// appendResult represents the result of a log append operation.
type appendResult struct {
	FirstIndex types.Index
	LastIndex  types.Index
	Offsets    []types.IndexOffsetPair
}

// logAppender defines the interface for appending log entries.
type logAppender interface {
	Append(ctx context.Context, entries []types.LogEntry, currentLast types.Index) (appendResult, error)
}

// defaultLogAppender implements logAppender.
type defaultLogAppender struct {
	logFilePath  string
	serializer   serializer
	fileSystem   fileSystem
	logger       logger.Logger
	syncOnAppend bool
}

// newLogAppender returns a new logAppender.
func newLogAppender(logFilePath string, fs fileSystem, ser serializer, log logger.Logger, sync bool) logAppender {
	return &defaultLogAppender{
		logFilePath:  logFilePath,
		fileSystem:   fs,
		serializer:   ser,
		logger:       log.WithComponent("logappender"),
		syncOnAppend: sync,
	}
}

func (a *defaultLogAppender) Append(ctx context.Context, entries []types.LogEntry, currentLast types.Index) (appendResult, error) {
	if len(entries) == 0 {
		return appendResult{}, ErrEmptyEntries
	}

	firstIndex := entries[0].Index
	lastIndex := entries[len(entries)-1].Index

	if err := a.validateEntries(entries, currentLast, firstIndex, lastIndex); err != nil {
		return appendResult{}, err
	}

	file, start, err := a.openLogFile()
	if err != nil {
		return appendResult{}, err
	}
	defer file.Close()

	offsets, err := a.writeToFile(ctx, file, entries, start)
	if err != nil {
		return appendResult{}, err
	}

	if err := a.syncIfNeeded(file, start); err != nil {
		return appendResult{}, err
	}

	return appendResult{
		FirstIndex: firstIndex,
		LastIndex:  lastIndex,
		Offsets:    offsets,
	}, nil
}

func (a *defaultLogAppender) validateEntries(entries []types.LogEntry, currentLast, first, last types.Index) error {
	if first > last {
		return ErrOutOfOrderEntries
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return ErrOutOfOrderEntries
		}
	}
	if currentLast > 0 && first != currentLast+1 {
		return fmt.Errorf("%w: expected first index %d, got %d", ErrNonContiguousEntries, currentLast+1, first)
	}
	if currentLast == 0 && first != 1 {
		return fmt.Errorf("%w: first index must be 1 for empty log, got %d", ErrNonContiguousEntries, first)
	}
	return nil
}

func (a *defaultLogAppender) openLogFile() (file, int64, error) {
	f, err := a.fileSystem.AppendFile(a.logFilePath)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: open log file: %v", ErrStorageIO, err)
	}
	pos, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		a.logger.Errorw("seek failed on log file: %v", err)
		return nil, 0, fmt.Errorf("%w: seek log file: %v", ErrStorageIO, err)
	}
	return f, pos, nil
}

func (a *defaultLogAppender) writeToFile(ctx context.Context, f file, entries []types.LogEntry, start int64) ([]types.IndexOffsetPair, error) {
	offsets := make([]types.IndexOffsetPair, 0, len(entries))
	pos := start
	lenBuf := make([]byte, lengthPrefixSize)

	for i, entry := range entries {
		if i%10 == 0 && ctx.Err() != nil {
			return nil, a.failAndRollback(f, start, "context canceled")
		}

		offsets = append(offsets, types.IndexOffsetPair{
			Index:  entry.Index,
			Offset: pos,
		})

		data, err := a.serializer.MarshalLogEntry(entry)
		if err != nil {
			return nil, a.failAndRollback(f, start, "serialize entry: %w", ErrStorageIO, err)
		}

		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		for _, chunk := range [][]byte{lenBuf, data} {
			n, err := f.Write(chunk)
			if err != nil {
				return nil, a.failAndRollback(f, start, "write failed: %w", ErrStorageIO, err)
			}
			pos += int64(n)
		}
	}
	return offsets, nil
}

func (a *defaultLogAppender) syncIfNeeded(f file, start int64) error {
	if a.syncOnAppend {
		if err := f.Sync(); err != nil {
			return a.failAndRollback(f, start, "sync failed: %w", ErrStorageIO, err)
		}
	}
	return nil
}

func (a *defaultLogAppender) rollback(f file, to int64) {
	_ = f.Close()
	err := a.fileSystem.Truncate(a.logFilePath, to)
	if err != nil {
		a.logger.Errorw("rollback failed during truncate to offset %d: %v", to, err)
	} else {
		a.logger.Warnw("rollback performed to offset %d", to)
	}
}

func (a *defaultLogAppender) failAndRollback(f file, start int64, format string, args ...any) error {
	_ = f.Close()
	_ = a.fileSystem.Truncate(a.logFilePath, start)
	a.logger.Warnw("rollback to offset %d after failure: "+format, append([]any{start}, args...)...)
	return fmt.Errorf(format, args...)
}
