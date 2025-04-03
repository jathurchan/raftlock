package storage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	tu "github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func TestFileStorage_InitializeEmptyMetadata(t *testing.T) {
	fs := &FileStorage{}

	// Initialize empty metadata
	fs.initializeEmptyMetadata()

	// Verify that firstLogIndex and lastLogIndex are both set to 0
	tu.AssertEqual(t, uint64(0), fs.firstLogIndex.Load())
	tu.AssertEqual(t, uint64(0), fs.lastLogIndex.Load())
}

func TestFileStorage_ValidateMetadataRange(t *testing.T) {
	tests := []struct {
		name       string
		firstIndex types.Index
		lastIndex  types.Index
		wantErr    bool
	}{
		{"valid empty range", 0, 0, false},
		{"valid single entry range", 1, 1, false},
		{"valid multiple entry range", 5, 10, false},
		{"invalid range - first greater than last", 10, 5, true},
		{"edge case - last is zero, first non-zero", 10, 0, false}, // allowed by spec
	}

	fs := &FileStorage{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fs.validateMetadataRange(tt.firstIndex, tt.lastIndex)
			if tt.wantErr {
				tu.AssertError(t, err)
				tu.AssertErrorIs(t, err, ErrCorruptedState)
			} else {
				tu.AssertNoError(t, err)
			}
		})
	}
}

func TestFileStorage_LoadMetadata(t *testing.T) {
	tempDir := t.TempDir()

	createTestDir := func(name string) string {
		path := filepath.Join(tempDir, name)
		tu.RequireNoError(t, os.Mkdir(path, 0755))
		return path
	}

	t.Run("load non-existent metadata file", func(t *testing.T) {
		testDir := createTestDir("non_existent")

		fs := &FileStorage{
			dir:    testDir,
			logger: &logger.NoOpLogger{},
		}

		err := fs.loadMetadata()
		tu.AssertNoError(t, err)
		tu.AssertEqual(t, uint64(0), fs.firstLogIndex.Load())
		tu.AssertEqual(t, uint64(0), fs.lastLogIndex.Load())
	})

	t.Run("load valid metadata file", func(t *testing.T) {
		testDir := createTestDir("valid")

		fs := &FileStorage{
			dir:    testDir,
			logger: &logger.NoOpLogger{},
		}

		metadata := metadataSchema{
			FirstIndex: 5,
			LastIndex:  10,
		}
		data, err := json.MarshalIndent(metadata, "", "  ")
		tu.RequireNoError(t, err)
		tu.RequireNoError(t, os.WriteFile(filepath.Join(testDir, "metadata.json"), data, 0644))

		err = fs.loadMetadata()
		tu.AssertNoError(t, err)
		tu.AssertEqual(t, uint64(5), fs.firstLogIndex.Load())
		tu.AssertEqual(t, uint64(10), fs.lastLogIndex.Load())
	})

	t.Run("load invalid json metadata file", func(t *testing.T) {
		testDir := createTestDir("invalid_json")

		fs := &FileStorage{
			dir:    testDir,
			logger: &logger.NoOpLogger{},
		}

		tu.RequireNoError(t, os.WriteFile(filepath.Join(testDir, "metadata.json"), []byte("{invalid json}"), 0644))

		err := fs.loadMetadata()
		tu.AssertError(t, err)
		tu.AssertErrorIs(t, err, ErrCorruptedState)
		tu.AssertEqual(t, uint64(0), fs.firstLogIndex.Load())
		tu.AssertEqual(t, uint64(0), fs.lastLogIndex.Load())
	})

	t.Run("load metadata file with invalid range", func(t *testing.T) {
		testDir := createTestDir("invalid_range")

		fs := &FileStorage{
			dir:    testDir,
			logger: &logger.NoOpLogger{},
		}

		metadata := metadataSchema{
			FirstIndex: 15,
			LastIndex:  10,
		}
		data, err := json.MarshalIndent(metadata, "", "  ")
		tu.RequireNoError(t, err)
		tu.RequireNoError(t, os.WriteFile(filepath.Join(testDir, "metadata.json"), data, 0644))

		err = fs.loadMetadata()
		tu.AssertError(t, err)
		tu.AssertErrorIs(t, err, ErrCorruptedState)
		tu.AssertContains(t, err.Error(), "invalid metadata range")
	})

	t.Run("load metadata file with lastIndex = 0 and firstIndex > 0 (unexpected empty state)", func(t *testing.T) {
		testDir := createTestDir("empty_inconsistent")

		fs := &FileStorage{
			dir:    testDir,
			logger: &logger.NoOpLogger{},
		}

		metadata := metadataSchema{
			FirstIndex: 5,
			LastIndex:  0,
		}
		data, err := json.MarshalIndent(metadata, "", "  ")
		tu.RequireNoError(t, err)
		tu.RequireNoError(t, os.WriteFile(filepath.Join(testDir, "metadata.json"), data, 0644))

		// This passes validateMetadataRange, but might be logically inconsistent depending on storage semantics.
		err = fs.loadMetadata()
		tu.AssertNoError(t, err)
		tu.AssertEqual(t, uint64(5), fs.firstLogIndex.Load())
		tu.AssertEqual(t, uint64(0), fs.lastLogIndex.Load())
	})

	t.Run("handle unreadable metadata file (permission denied)", func(t *testing.T) {
		testDir := createTestDir("unreadable")

		fs := &FileStorage{
			dir:    testDir,
			logger: &logger.NoOpLogger{},
		}

		metadataPath := filepath.Join(testDir, "metadata.json")
		tu.RequireNoError(t, os.WriteFile(metadataPath, []byte(`{}`), 0000)) // no permissions

		defer os.Chmod(metadataPath, 0644) // restore permissions after test

		err := fs.loadMetadata()
		tu.AssertError(t, err)
		tu.AssertErrorIs(t, err, ErrStorageIO)
	})
}

// Save original for restore in tests that monkey-patch it
var originalMarshalIndent = jsonMarshalIndent

func TestFileStorage_SaveMetadata(t *testing.T) {
	tempDir := t.TempDir()

	createTestDir := func(name string) string {
		path := filepath.Join(tempDir, name)
		tu.RequireNoError(t, os.Mkdir(path, 0755))
		return path
	}

	t.Run("atomic write succeeds", func(t *testing.T) {
		testDir := createTestDir("atomic_success")

		fs := &FileStorage{
			dir: testDir,
			options: FileStorageOptions{
				Features: StorageFeatureFlags{
					EnableAtomicWrites: true,
				},
			},
		}
		fs.firstLogIndex.Store(10)
		fs.lastLogIndex.Store(20)

		err := fs.saveMetadata()
		tu.AssertNoError(t, err)

		data, err := os.ReadFile(filepath.Join(testDir, "metadata.json"))
		tu.RequireNoError(t, err)

		var parsed metadataSchema
		tu.RequireNoError(t, json.Unmarshal(data, &parsed))
		tu.AssertEqual(t, types.Index(10), parsed.FirstIndex)
		tu.AssertEqual(t, types.Index(20), parsed.LastIndex)
	})

	t.Run("non-atomic write succeeds", func(t *testing.T) {
		testDir := createTestDir("non_atomic_success")

		fs := &FileStorage{
			dir: testDir,
			options: FileStorageOptions{
				Features: StorageFeatureFlags{
					EnableAtomicWrites: false,
				},
			},
		}
		fs.firstLogIndex.Store(1)
		fs.lastLogIndex.Store(2)

		err := fs.saveMetadata()
		tu.AssertNoError(t, err)

		data, err := os.ReadFile(filepath.Join(testDir, "metadata.json"))
		tu.RequireNoError(t, err)

		var parsed metadataSchema
		tu.RequireNoError(t, json.Unmarshal(data, &parsed))
		tu.AssertEqual(t, types.Index(1), parsed.FirstIndex)
		tu.AssertEqual(t, types.Index(2), parsed.LastIndex)
	})

	t.Run("marshal failure", func(t *testing.T) {
		defer func() { jsonMarshalIndent = originalMarshalIndent }()
		jsonMarshalIndent = func(v any, prefix, indent string) ([]byte, error) {
			return nil, errors.New("mock marshal error")
		}

		testDir := createTestDir("marshal_fail")

		fs := &FileStorage{
			dir: testDir,
			options: FileStorageOptions{
				Features: StorageFeatureFlags{
					EnableAtomicWrites: true,
				},
			},
		}
		fs.firstLogIndex.Store(100)
		fs.lastLogIndex.Store(200)

		err := fs.saveMetadata()
		tu.AssertError(t, err)
		tu.AssertErrorIs(t, err, ErrStorageIO)
		tu.AssertContains(t, err.Error(), "failed to marshal metadata")
	})

	t.Run("write failure (permission denied)", func(t *testing.T) {
		testDir := createTestDir("write_fail")

		fs := &FileStorage{
			dir: testDir,
			options: FileStorageOptions{
				Features: StorageFeatureFlags{
					EnableAtomicWrites: false,
				},
			},
		}

		metadataPath := filepath.Join(testDir, "metadata.json")
		file, err := os.Create(metadataPath)
		tu.RequireNoError(t, err)
		file.Close()

		tu.RequireNoError(t, os.Chmod(metadataPath, 0000))
		defer os.Chmod(metadataPath, 0644) // cleanup

		fs.firstLogIndex.Store(3)
		fs.lastLogIndex.Store(4)

		err = fs.saveMetadata()
		tu.AssertError(t, err)
		tu.AssertContains(t, err.Error(), "permission denied")
	})
}
