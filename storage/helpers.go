package storage

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// atomicWrite safely writes data to a file atomically.
// It first writes the data to a temporary file, then renames it to the target path.
// This ensures that the file is either fully written or not modified at all in case of failure.
func atomicWrite(fs fileSystem, path string, data []byte, perm os.FileMode) error {
	dir := fs.Dir(path)
	if err := fs.MkdirAll(dir, ownRWXOthRX); err != nil {
		return fmt.Errorf("failed to create dir for atomic write: %w", err)
	}

	tmpPath := fs.TempPath(path)

	if err := fs.WriteFile(tmpPath, data, perm); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := atomicRenameOrCleanup(fs, tmpPath, path); err != nil {
		return err
	}

	return nil
}

// atomicRenameOrCleanup attempts to atomically rename a temporary file to its final destination.
//
// If the rename operation fails (e.g., due to permission issues or I/O errors),
// the temporary file is deleted to avoid leaving behind stale files.
//
// This function is typically used as the final step of an atomic write operation.
func atomicRenameOrCleanup(fs fileSystem, tmpPath, finalPath string) error {
	if err := fs.Rename(tmpPath, finalPath); err != nil {
		_ = fs.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// clampLogRange limits the given range [start, end) to fit within [first, last].
// Returns the adjusted range and a boolean indicating if the clamped range is non-empty.
//
// - If the input range lies completely outside [first, last], it returns (0, 0, false).
// - 'end' is exclusive; 'last' is inclusive.
// - Returned range is valid only if start < end.
func clampLogRange(start, end, first, last types.Index) (types.Index, types.Index, bool) {
	if end <= first || start > last {
		return 0, 0, false // completely outside range
	}
	if start < first {
		start = first
	}
	if end > last+1 {
		end = last + 1
	}
	if start >= end {
		return 0, 0, false
	}
	return start, end, true
}

// FailAndRollback performs error handling by:
// - Closing the file
// - Attempting to truncate it to a safe offset
// - Logging rollback status and returning a formatted wrapped error
func FailAndRollback(
	f file,
	fs fileSystem,
	path string,
	startOffset int64,
	log logger.Logger,
	context string,
	format string,
	args ...any,
) error {
	_ = f.Close()

	err := fs.Truncate(path, startOffset)
	if err != nil {
		log.Errorw("Rollback failed during truncate", "offset", startOffset, "error", err)
	} else {
		log.Warnw("Rollback performed to offset", "offset", startOffset)
	}

	wrapped := fmt.Errorf(format, args...)
	log.Warnw(fmt.Sprintf("Failure during %s: %v (rollback to offset %d)", context, wrapped, startOffset))
	return wrapped
}

// writeChunks writes the given data to a file in chunks, respecting the provided context.
// It aborts early if the context is canceled.
// Ensures file sync after all chunks are written.
func writeChunks(ctx context.Context, file file, data []byte, chunkSize int) error {
	dataLen := len(data)
	for offset := 0; offset < dataLen; offset += chunkSize {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		end := min(offset+chunkSize, dataLen)

		chunk := data[offset:end]

		n, err := file.Write(chunk)
		if err != nil {
			return fmt.Errorf("%w: failed to write snapshot chunk", ErrStorageIO)
		}
		if n != len(chunk) {
			return fmt.Errorf("%w: incomplete chunk write (%d/%d bytes)", ErrStorageIO, n, len(chunk))
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("%w: failed to sync file", ErrStorageIO)
	}

	return nil
}

// readChunks reads the given file in chunks, respecting the provided context.
// Returns an error if the context is canceled or if any read operation fails.
func readChunks(ctx context.Context, file file, size int64, chunkSize int) ([]byte, error) {
	data := make([]byte, size)
	var bytesRead int64

	for bytesRead < size {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		remaining := size - bytesRead
		toRead := min(remaining, int64(chunkSize))

		n, err := file.Read(data[bytesRead : bytesRead+toRead])
		if n == 0 && err == nil {
			return nil, fmt.Errorf("%w: read returned 0 bytes with no error", ErrCorruptedSnapshot)
		}
		if n > 0 {
			bytesRead += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				if bytesRead == size {
					break
				}
				return nil, fmt.Errorf("%w: unexpected EOF before reading all data", ErrCorruptedSnapshot)
			}
			return nil, fmt.Errorf("%w: failed to read snapshot chunk", ErrStorageIO)
		}
	}

	return data, nil
}
