package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/jathurchan/raftlock/testutil"
)

// Helper function to create a temporary file with content
func createTempFile(t *testing.T, dir, pattern, content string) string {
	t.Helper()
	tmpFile, err := os.CreateTemp(dir, pattern)
	testutil.RequireNoError(t, err, "Failed to create temp file")
	defer tmpFile.Close()

	_, err = tmpFile.WriteString(content)
	testutil.RequireNoError(t, err, "Failed to write to temp file")
	err = tmpFile.Sync()
	testutil.RequireNoError(t, err, "Failed to sync temp file")

	return tmpFile.Name()
}

func TestAtomicWrite(t *testing.T) {
	testPath := "test/dir/file.txt"
	testData := []byte("atomic test data")
	perm := os.FileMode(0644)

	t.Run("Success", func(t *testing.T) {
		fs := newMockFileSystem()
		err := fs.AtomicWrite(testPath, testData, perm)
		testutil.AssertNoError(t, err)
	})

	t.Run("MkdirAllError", func(t *testing.T) {
		fs := newMockFileSystem()
		expectedErr := errors.New("mkdir error")
		fs.MkdirAllFunc = func(path string, perm os.FileMode) error {
			return expectedErr
		}
		err := fs.AtomicWrite(testPath, testData, perm)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to create dir for atomic write")
		testutil.AssertContains(t, err.Error(), expectedErr.Error())
	})

	t.Run("WriteFileError", func(t *testing.T) {
		fs := newMockFileSystem()
		expectedErr := errors.New("write error")
		fs.WriteFileFunc = func(name string, data []byte, perm os.FileMode) error {
			return expectedErr
		}
		err := fs.AtomicWrite(testPath, testData, perm)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to write temp file")
		testutil.AssertContains(t, err.Error(), expectedErr.Error())
	})

	t.Run("RenameError", func(t *testing.T) {
		fs := newMockFileSystem()
		expectedErr := errors.New("rename error")

		removeCalled := false
		fs.RenameFunc = func(oldPath, newPath string) error {
			return expectedErr
		}
		fs.RemoveFunc = func(name string) error {
			removeCalled = true
			return nil
		}

		err := fs.AtomicWrite(testPath, testData, perm)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to rename temp file")
		testutil.AssertContains(t, err.Error(), expectedErr.Error())
		testutil.AssertTrue(t, removeCalled, "Expected Remove to be called on rename failure")
	})
}

// TestWriteMaybeAtomic tests the WriteMaybeAtomic method
func TestWriteMaybeAtomic(t *testing.T) {
	tempDir := t.TempDir()
	fs := newFileSystem() // Use the real file system
	path := filepath.Join(tempDir, "maybe_atomic.txt")
	testData := []byte("test data for maybe atomic write")
	perm := os.FileMode(0644)

	t.Run("AtomicTrue", func(t *testing.T) {
		// Test with atomic flag set to true
		err := fs.WriteMaybeAtomic(path, testData, perm, true)
		testutil.AssertNoError(t, err)

		// Verify file was written correctly
		data, err := fs.ReadFile(path)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, testData, data)
	})

	t.Run("AtomicFalse", func(t *testing.T) {
		// Test with atomic flag set to false
		err := fs.WriteMaybeAtomic(path, []byte("non-atomic test"), perm, false)
		testutil.AssertNoError(t, err)

		// Verify file was written correctly with direct write
		data, err := fs.ReadFile(path)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, []byte("non-atomic test"), data)
	})
}

// TestTempPath tests the TempPath method
func TestTempPath(t *testing.T) {
	fs := newFileSystem()

	t.Run("DefaultImplementation", func(t *testing.T) {
		path := "test/path/file.txt"
		tempPath := fs.TempPath(path)
		testutil.AssertEqual(t, path+tmpSuffix, tempPath)
	})
}

// TestPath tests the Path method
func TestPath(t *testing.T) {
	fs := newFileSystem()

	t.Run("BasicPath", func(t *testing.T) {
		dir := "dir"
		file := "file.txt"
		path := fs.Path(dir, file)
		testutil.AssertEqual(t, filepath.Join(dir, file), path)
	})

	t.Run("WithEmptyDir", func(t *testing.T) {
		dir := ""
		file := "file.txt"
		path := fs.Path(dir, file)
		testutil.AssertEqual(t, "file.txt", path)
	})

	t.Run("WithEmptyFile", func(t *testing.T) {
		dir := "dir"
		file := ""
		path := fs.Path(dir, file)
		testutil.AssertEqual(t, "dir", path)
	})
}

// TestNewDefaultFileSystem verifies constructor functions
func TestNewDefaultFileSystem(t *testing.T) {
	t.Run("DefaultConstructor", func(t *testing.T) {
		fs := newFileSystem()
		testutil.AssertNotNil(t, fs)

		// Verify it uses os.Stat by checking a real path
		tempDir := t.TempDir()
		exists, err := fs.Exists(tempDir)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, exists)
	})

	t.Run("WithStatConstructor", func(t *testing.T) {
		customStatCalled := false
		customStat := func(name string) (os.FileInfo, error) {
			customStatCalled = true
			return nil, errors.New("custom stat error")
		}

		fs := newFileSystemWithStat(customStat)
		testutil.AssertNotNil(t, fs)

		// Test the custom stat function was used
		tempDir := t.TempDir()
		exists, err := fs.Exists(tempDir)

		testutil.AssertFalse(t, exists)
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, customStatCalled)
		testutil.AssertContains(t, err.Error(), "custom stat error")
	})

	t.Run("WithNilStatConstructor", func(t *testing.T) {
		fs := newFileSystemWithStat(nil)
		testutil.AssertNotNil(t, fs)

		// Verify it falls back to os.Stat
		tempDir := t.TempDir()
		exists, err := fs.Exists(tempDir)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, exists)
	})
}

// TestFileSystemBasicIO tests reading and writing operations
func TestFileSystemBasicIO(t *testing.T) {
	fs := newFileSystem()
	tempDir := t.TempDir()
	content := "test content"

	t.Run("ReadWriteFile", func(t *testing.T) {
		filePath := filepath.Join(tempDir, "readwrite.txt")
		writeData := []byte(content)
		perm := os.FileMode(0644)

		// Write and verify
		err := fs.WriteFile(filePath, writeData, perm)
		testutil.AssertNoError(t, err)

		// Read and verify content
		readData, err := fs.ReadFile(filePath)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, writeData, readData)

		// Test non-existent file
		_, err = fs.ReadFile(filepath.Join(tempDir, "non_existent.txt"))
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err))
	})

	t.Run("Open", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "open_*.txt", content)

		// Open successfully
		f, err := fs.Open(filePath)
		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, f)
		defer f.Close()

		// Read content through file interface
		data, err := f.ReadAll()
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, content, string(data))

		// Test open non-existent file
		_, err = fs.Open(filepath.Join(tempDir, "non_existent.txt"))
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err))
	})
}

// TestFileSystemModifications tests file modification operations
func TestFileSystemModifications(t *testing.T) {
	fs := newFileSystem()
	tempDir := t.TempDir()

	t.Run("Truncate", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "truncate_*.txt", "1234567890")
		var expectedSize int64 = 5

		// Truncate and verify
		err := fs.Truncate(filePath, expectedSize)
		testutil.AssertNoError(t, err)

		// Verify file size
		stat, err := os.Stat(filePath)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, expectedSize, stat.Size())

		// Verify truncated content
		data, err := fs.ReadFile(filePath)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, "12345", string(data))

		// Test non-existent file
		err = fs.Truncate(filepath.Join(tempDir, "non_existent.txt"), 0)
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err))
	})

	t.Run("Rename", func(t *testing.T) {
		oldPath := createTempFile(t, tempDir, "old_*.txt", "rename me")
		newPath := filepath.Join(tempDir, "new.txt")

		// Rename and verify
		err := fs.Rename(oldPath, newPath)
		testutil.AssertNoError(t, err)

		// Check existence
		exists, err := fs.Exists(newPath)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, exists)

		exists, err = fs.Exists(oldPath)
		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, exists)

		// Test non-existent source
		err = fs.Rename(filepath.Join(tempDir, "non_existent.txt"), filepath.Join(tempDir, "another.txt"))
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err))
	})

	t.Run("Remove", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "remove_*.txt", "delete me")

		// Remove and verify
		err := fs.Remove(filePath)
		testutil.AssertNoError(t, err)

		exists, err := fs.Exists(filePath)
		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, exists)

		// Test non-existent file
		err = fs.Remove(filepath.Join(tempDir, "non_existent.txt"))
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err))
	})
}

// TestDirectoryOperations tests directory-related operations
func TestDirectoryOperations(t *testing.T) {
	fs := newFileSystem()
	tempDir := t.TempDir()

	t.Run("MkdirAll", func(t *testing.T) {
		dirPath := filepath.Join(tempDir, "parent", "child")
		perm := os.FileMode(0755)

		// Create and verify
		err := fs.MkdirAll(dirPath, perm)
		testutil.AssertNoError(t, err)

		exists, err := fs.Exists(dirPath)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, exists)

		// Creating an existing directory should succeed
		err = fs.MkdirAll(dirPath, perm)
		testutil.AssertNoError(t, err)
	})

	t.Run("Exists", func(t *testing.T) {
		// File exists
		filePath := createTempFile(t, tempDir, "exists_*.txt", "content")
		exists, err := fs.Exists(filePath)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, exists)

		// File doesn't exist
		exists, err = fs.Exists(filepath.Join(tempDir, "non_existent.txt"))
		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, exists)

		// Custom error case
		customFS := newFileSystemWithStat(func(name string) (os.FileInfo, error) {
			return nil, errors.New("custom error")
		})
		exists, err = customFS.Exists(filePath)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "custom error")
		testutil.AssertFalse(t, exists)
	})

	t.Run("Glob", func(t *testing.T) {
		// Create test files
		file1 := createTempFile(t, tempDir, "glob1_*.txt", "")
		file2 := createTempFile(t, tempDir, "glob2_*.txt", "")
		pattern := filepath.Join(tempDir, "glob*_*.txt")

		// Test glob matching
		matches, err := fs.Glob(pattern)
		testutil.AssertNoError(t, err)

		// Sort for deterministic comparison
		expected := []string{file1, file2}
		sort.Strings(expected)
		sort.Strings(matches)
		testutil.AssertEqual(t, expected, matches)

		// Test glob with no matches
		matches, err = fs.Glob(filepath.Join(tempDir, "nomatch_*.txt"))
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 0, len(matches))

		// Test invalid pattern
		_, err = fs.Glob("[") // Invalid pattern
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, filepath.ErrBadPattern)
	})
}

// TestPathUtilities tests path manipulation methods
func TestPathUtilities(t *testing.T) {
	fs := newFileSystem()

	t.Run("Join", func(t *testing.T) {
		testCases := []struct {
			elements []string
			expected string
		}{
			{[]string{"a", "b", "c"}, "a/b/c"},
			{[]string{"/a", "b/"}, "/a/b"},
			{[]string{"a/", "/b"}, "a/b"},
			{[]string{}, ""},
		}

		for _, tc := range testCases {
			result := fs.Join(tc.elements...)
			testutil.AssertEqual(t, tc.expected, result)
		}
	})

	t.Run("Dir", func(t *testing.T) {
		testCases := []struct {
			path     string
			expected string
		}{
			{"a/b/c.txt", "a/b"},
			{"/a/b/c", "/a/b"},
			{"filename.txt", "."},
			{"a/", "a"},
		}

		for _, tc := range testCases {
			result := fs.Dir(tc.path)
			testutil.AssertEqual(t, tc.expected, result)
		}
	})
}

// TestFileOperations tests the file interface methods
func TestFileOperations(t *testing.T) {
	fs := newFileSystem()
	tempDir := t.TempDir()
	content := "0123456789"

	t.Run("ReadFull", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "readfull_*.txt", content)
		f, err := fs.Open(filePath)
		testutil.RequireNoError(t, err)
		defer f.Close()

		// Read part of the file
		buf := make([]byte, 5)
		n, err := f.ReadFull(buf)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 5, n)
		testutil.AssertEqual(t, "01234", string(buf))

		// Read more than remaining (should get ErrUnexpectedEOF)
		buf = make([]byte, 6)
		n, err = f.ReadFull(buf)
		testutil.AssertErrorIs(t, err, io.ErrUnexpectedEOF)
		testutil.AssertEqual(t, 5, n)
		testutil.AssertEqual(t, "56789", string(buf[:n]))

		// Try to read at EOF
		buf = make([]byte, 1)
		n, err = f.ReadFull(buf)
		testutil.AssertErrorIs(t, err, io.EOF)
		testutil.AssertEqual(t, 0, n)
	})

	t.Run("ReadAll", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "readall_*.txt", content)
		f, err := fs.Open(filePath)
		testutil.RequireNoError(t, err)
		defer f.Close()

		// Read entire file
		data, err := f.ReadAll()
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, content, string(data))

		// ReadAll on an already consumed file should return empty without error
		data, err = f.ReadAll()
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 0, len(data))
	})

	t.Run("Seek", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "seek_*.txt", content)
		f, err := fs.Open(filePath)
		testutil.RequireNoError(t, err)
		defer f.Close()

		// Test Seek with different whence values
		seekTests := []struct {
			offset    int64
			whence    int
			expected  int64
			readBytes int
			readData  string
		}{
			{3, io.SeekStart, 3, 3, "345"},
			{-2, io.SeekEnd, 8, 2, "89"},
			{-3, io.SeekCurrent, 7, 3, "789"},
			{0, io.SeekStart, 0, 5, "01234"},
		}

		for i, test := range seekTests {
			pos, err := f.Seek(test.offset, test.whence)
			testutil.AssertNoError(t, err, fmt.Sprintf("seek test %d failed", i))
			testutil.AssertEqual(t, test.expected, pos)

			// Read data to verify position
			buf := make([]byte, test.readBytes)
			n, err := f.Read(buf)
			testutil.AssertNoError(t, err)
			testutil.AssertEqual(t, test.readBytes, n)
			testutil.AssertEqual(t, test.readData, string(buf))
		}
	})
}

// TestErrorHandling tests IsNotExist behavior with various error types
func TestErrorHandling(t *testing.T) {
	fs := newFileSystem()

	t.Run("IsNotExist", func(t *testing.T) {
		testCases := []struct {
			err      error
			expected bool
		}{
			{os.ErrNotExist, true},
			{fmt.Errorf("wrapped: %w", os.ErrNotExist), true},
			{io.EOF, false},
			{errors.New("other error"), false},
			{nil, false},
		}

		for _, tc := range testCases {
			result := fs.IsNotExist(tc.err)
			testutil.AssertEqual(t, tc.expected, result)
		}
	})
}
