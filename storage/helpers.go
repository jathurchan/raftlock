package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"slices"

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

// updateMax atomically updates the value of max if the given value is greater.
// Uses a compare-and-swap loop to safely update shared state.
func updateMax(max *atomic.Uint64, val uint64) {
	for {
		current := max.Load()
		if val <= current || max.CompareAndSwap(current, val) {
			break
		}
	}
}

// computePercentile calculates the value at the given percentile from a slice of samples.
// The input slice is copied and sorted to avoid modifying the original.
func computePercentile(samples []uint64, percentile float64) uint64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := make([]uint64, len(samples))
	copy(sorted, samples)
	slices.Sort(sorted)
	idx := int(float64(len(sorted)-1) * percentile)
	return sorted[idx]
}

// formatDurationNs formats a duration in nanoseconds into a human-readable string.
// Displays in milliseconds if >= 1,000,000 ns, otherwise in microseconds.
// Returns "0 ns" if the duration is zero.
func formatDurationNs(ns uint64) string {
	if ns == 0 {
		return "0 ns"
	}
	if ns >= 1_000_000 {
		return fmt.Sprintf("%.2f ms", float64(ns)/1_000_000)
	}
	return fmt.Sprintf("%d µs", ns/1_000)
}

// formatByteSize returns a human-readable string for the given byte size.
// Converts bytes into binary units (KiB, MiB, etc.) with two decimal precision.
func formatByteSize(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
