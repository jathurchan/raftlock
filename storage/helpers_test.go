package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jathurchan/raftlock/types"
)

func TestFileExists(t *testing.T) {
	tests := []struct {
		name         string
		statError    error
		expectExists bool
		expectError  bool
	}{
		{"file exists", nil, true, false},
		{"file doesn't exist", os.ErrNotExist, false, false},
		{"stat error", errors.New("some other error"), false, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := mockFS{err: tc.statError}
			exists, err := fileExists(fs, "somepath")

			if exists != tc.expectExists {
				t.Errorf("Expected exists=%v, got %v", tc.expectExists, exists)
			}

			if (err != nil) != tc.expectError {
				t.Errorf("Expected error: %v, got: %v", tc.expectError, err)
			}
		})
	}
}

func TestAtomicWriteFile(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T) (string, []byte)
		expectErr bool
		verify    func(t *testing.T, path string, data []byte)
	}{
		{
			name: "successful write",
			setup: func(t *testing.T) (string, []byte) {
				tmpDir := t.TempDir()
				return filepath.Join(tmpDir, "subdir", "testfile.txt"), []byte("hello world")
			},
			expectErr: false,
			verify: func(t *testing.T, path string, data []byte) {
				// Verify file content
				got, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("failed to read file: %v", err)
				}
				if !bytes.Equal(got, data) {
					t.Errorf("data mismatch: got %s, want %s", got, data)
				}

				// Temp file should not exist
				if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
					t.Errorf("temporary file should be cleaned up, got err: %v", err)
				}
			},
		},
		{
			name: "directory creation fails",
			setup: func(t *testing.T) (string, []byte) {
				tmpDir := t.TempDir()
				badPath := filepath.Join(tmpDir, "no_dir")
				os.WriteFile(badPath, []byte(""), 0600)
				return filepath.Join(badPath, "file.txt"), []byte("fail")
			},
			expectErr: true,
			verify:    func(t *testing.T, path string, data []byte) {},
		},
		{
			name: "write temporary file fails with cleanup error",
			setup: func(t *testing.T) (string, []byte) {
				tmpDir := t.TempDir()
				subDir := filepath.Join(tmpDir, "subdir")
				// Create directory with no write permission
				if err := os.MkdirAll(subDir, 0500); err != nil {
					t.Fatalf("failed to create test directory: %v", err)
				}
				return filepath.Join(subDir, "file.txt"), []byte("data")
			},
			expectErr: true,
			verify:    func(t *testing.T, path string, data []byte) {},
		},
		{
			name: "rename fails",
			setup: func(t *testing.T) (string, []byte) {
				tmpDir := t.TempDir()
				testPath := filepath.Join(tmpDir, "file.txt")
				// Pre-create a directory where the file should go (rename to dir fails)
				os.Mkdir(testPath, 0755)
				return testPath, []byte("data")
			},
			expectErr: true,
			verify: func(t *testing.T, path string, data []byte) {
				// Check that the .tmp file is cleaned up
				if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
					t.Errorf("expected temp file to be cleaned up on rename failure")
				}
			},
		},
		{
			name: "cleanup error after write failure",
			setup: func(t *testing.T) (string, []byte) {
				// This test case simulates a cleanup error after a primary error
				// For this test to work properly, we'd need to mock the os.Remove function
				// As a simpler approach, we'll use handleErrorWithCleanup directly in a subtest
				return "", []byte{}
			},
			expectErr: true,
			verify: func(t *testing.T, path string, data []byte) {
				// In this subtest we'll directly test handleErrorWithCleanup
				t.Run("direct cleanup error test", func(t *testing.T) {
					// Create a temp dir but immediately remove it
					// so the cleanup will fail trying to remove a non-existent path
					tmpDir := t.TempDir()
					tmpPath := filepath.Join(tmpDir, "nonexistent.tmp")

					primaryErr := errors.New("primary error")
					err := handleErrorWithCleanup(primaryErr, tmpPath)

					if err == nil {
						t.Error("expected error but got nil")
					}

					if err != nil && err == primaryErr {
						t.Error("expected a combined error with cleanup info, but got only primary error")
					}

					// The error should contain both the primary error and the cleanup error
					if err != nil && !strings.Contains(err.Error(), "primary error") {
						t.Errorf("error doesn't contain primary error info: %v", err)
					}
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path, data := tc.setup(t)
			err := atomicWriteFile(path, data, 0644)

			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}

			if !tc.expectErr {
				tc.verify(t, path, data)
			}
		})
	}
}

func TestLogEntrySerialization(t *testing.T) {
	testCases := []struct {
		name      string
		entry     types.LogEntry
		corruptFn func([]byte) []byte
		expectErr bool
	}{
		{
			name: "basic serialization and deserialization",
			entry: types.LogEntry{
				Index:   42,
				Term:    7,
				Command: []byte("test command"),
			},
			corruptFn: nil,
			expectErr: false,
		},
		{
			name: "empty command",
			entry: types.LogEntry{
				Index:   1,
				Term:    1,
				Command: []byte{},
			},
			corruptFn: nil,
			expectErr: false,
		},
		{
			name: "corrupted checksum",
			entry: types.LogEntry{
				Index:   42,
				Term:    7,
				Command: []byte("test command"),
			},
			corruptFn: func(data []byte) []byte {
				checksumOffset := HeaderSize + len([]byte("test command"))
				data[checksumOffset] ^= 0xFF // Flip some bits in the checksum
				return data
			},
			expectErr: true,
		},
		{
			name: "corrupted command",
			entry: types.LogEntry{
				Index:   42,
				Term:    7,
				Command: []byte("test command"),
			},
			corruptFn: func(data []byte) []byte {
				commandStart := HeaderSize
				data[commandStart] ^= 0xFF // Flip some bits in the command
				return data
			},
			expectErr: true,
		},
		{
			name: "truncated data",
			entry: types.LogEntry{
				Index:   42,
				Term:    7,
				Command: []byte("test command"),
			},
			corruptFn: func(data []byte) []byte {
				return data[:len(data)-5] // Truncate the data
			},
			expectErr: true,
		},
		{
			name: "extra data after checksum",
			entry: types.LogEntry{
				Index:   42,
				Term:    7,
				Command: []byte("test command"),
			},
			corruptFn: func(data []byte) []byte {
				return append(data, []byte("extra")...) // Add extra bytes
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Serialize
			serialized := serializeLogEntry(tc.entry)

			// Apply corruption if specified
			if tc.corruptFn != nil {
				serialized = tc.corruptFn(serialized)
			}

			// Deserialize
			deserialized, err := deserializeLogEntry(serialized)

			if tc.expectErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Compare original and deserialized entries
			if deserialized.Index != tc.entry.Index {
				t.Errorf("Index mismatch: got %v, want %v", deserialized.Index, tc.entry.Index)
			}
			if deserialized.Term != tc.entry.Term {
				t.Errorf("Term mismatch: got %v, want %v", deserialized.Term, tc.entry.Term)
			}
			if !bytes.Equal(deserialized.Command, tc.entry.Command) {
				t.Errorf("Command mismatch: got %v, want %v", deserialized.Command, tc.entry.Command)
			}
		})
	}
}

func TestHeaderParsing(t *testing.T) {
	tests := []struct {
		name         string
		setupHeader  func() []byte
		expectIndex  types.Index
		expectTerm   types.Term
		expectCmdLen int
		expectErr    bool
	}{
		{
			name: "valid header",
			setupHeader: func() []byte {
				data := make([]byte, HeaderSize+10+ChecksumSize)
				writeHeader(data, 42, 7, 10)
				return data
			},
			expectIndex:  42,
			expectTerm:   7,
			expectCmdLen: 10,
			expectErr:    false,
		},
		{
			name: "short header",
			setupHeader: func() []byte {
				return make([]byte, HeaderSize-1)
			},
			expectErr: true,
		},
		{
			name: "excessive command length",
			setupHeader: func() []byte {
				data := make([]byte, HeaderSize+5+ChecksumSize)
				writeHeader(data, 42, 7, 10) // cmdLen=10 but only 5 bytes available
				return data
			},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupHeader()
			index, term, cmdLen, err := parseHeader(data)

			if (err != nil) != tc.expectErr {
				t.Errorf("expected error: %v, got: %v", tc.expectErr, err)
				return
			}

			if !tc.expectErr {
				if index != tc.expectIndex || term != tc.expectTerm || cmdLen != tc.expectCmdLen {
					t.Errorf("got (%v, %v, %v), want (%v, %v, %v)",
						index, term, cmdLen, tc.expectIndex, tc.expectTerm, tc.expectCmdLen)
				}
			}
		})
	}
}

func TestChecksumVerification(t *testing.T) {
	data := []byte("test data for checksum")
	checksum := computeChecksum(data)

	tests := []struct {
		name      string
		checksum  uint32
		expectErr bool
	}{
		{"correct checksum", checksum, false},
		{"incorrect checksum", checksum ^ 0xFFFFFFFF, true}, // Invert all bits
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyChecksum(data, tc.checksum)
			if (err != nil) != tc.expectErr {
				t.Errorf("expected error: %v, got: %v", tc.expectErr, err)
			}
		})
	}
}

// mockFS implements fileStatter interface for testing
type mockFS struct {
	err error
}

func (m mockFS) Stat(name string) (os.FileInfo, error) {
	return nil, m.err
}

func TestOSFSStat(t *testing.T) {
	// Create a temporary file to ensure we have a file that exists
	f, err := os.CreateTemp("", "test-file")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filename := f.Name()
	f.Close()
	defer os.Remove(filename)

	// Test Stat with existing file
	fs := osFS{}
	fileInfo, err := fs.Stat(filename)
	if err != nil {
		t.Errorf("osFS.Stat() error = %v, expected nil for existing file", err)
	}

	// Verify the returned fileInfo is valid
	if fileInfo == nil {
		t.Error("osFS.Stat() returned nil fileInfo")
	} else {
		if fileInfo.Name() != filepath.Base(filename) {
			t.Errorf("Expected filename %s, got %s", filepath.Base(filename), fileInfo.Name())
		}
	}

	// Test Stat with non-existent file to ensure os.Stat call is properly passed through
	nonExistentFile := filepath.Join(t.TempDir(), "nonexistent-file")
	_, err = fs.Stat(nonExistentFile)
	if !os.IsNotExist(err) {
		t.Errorf("Expected IsNotExist error, got %v", err)
	}
}
