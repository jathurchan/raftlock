package storage

import (
	"fmt"
	"os"

	"github.com/jathurchan/raftlock/types"
)

func atomicWrite(fs fileSystem, path string, data []byte, perm os.FileMode) error {
	dir := fs.Dir(path)
	if err := fs.MkdirAll(dir, ownRWXOthRX); err != nil {
		return fmt.Errorf("failed to create dir for atomic write: %w", err)
	}

	tmpPath := fs.TempPath(path)

	if err := fs.WriteFile(tmpPath, data, perm); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := fs.Rename(tmpPath, path); err != nil {
		// Attempt cleanup
		_ = fs.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// clampLogRange clamps the given range [start, end) to be within the range [first, last].
// It returns the clamped start and end indices, and a boolean indicating whether the resulting range is valid (start < end).
// If the input range is completely outside [first, last], it returns 0, 0, false.
// Note that the 'end' parameter is exclusive, meaning the range includes indices from 'start' up to (but not including) 'end'.
// Similarly, 'last' is inclusive, so the valid range is from 'first' up to and including 'last'.
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
