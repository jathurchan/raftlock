package storage

import (
	"fmt"
	"os"
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
