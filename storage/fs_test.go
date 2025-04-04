package storage

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"

	"github.com/jathurchan/raftlock/testutil"
)

func TestNewDefaultFileSystem(t *testing.T) {
	_ = newDefaultFileSystem()
}

// Helper function to create a temporary file with content
func createTempFile(t *testing.T, dir, pattern, content string) string {
	t.Helper()
	tmpFile, err := os.CreateTemp(dir, pattern)
	testutil.RequireNoError(t, err, "Failed to create temp file")

	_, err = tmpFile.WriteString(content)
	testutil.RequireNoError(t, err, "Failed to write to temp file")
	err = tmpFile.Close()
	testutil.RequireNoError(t, err, "Failed to close temp file")
	return tmpFile.Name()
}

func TestDefaultFileSystem(t *testing.T) {
	fs := defaultFileSystem{}
	tempDir := t.TempDir()
	content := "test content"

	// Test ReadFile (success and error cases)
	t.Run("ReadFile", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "readfile_*.txt", content)

		data, err := fs.ReadFile(filePath)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, []byte(content), data)

		_, err = fs.ReadFile(filepath.Join(tempDir, "non_existent_file.txt"))
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err), "Error should be IsNotExist")
	})

	// Test Open (success and error cases)
	t.Run("Open", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "open_*.txt", content)

		f, err := fs.Open(filePath)
		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, f)
		defer f.Close()

		_, ok := f.(*defaultFile)
		testutil.AssertTrue(t, ok, "Opened file should be of type *defaultFile")

		_, err = fs.Open(filepath.Join(tempDir, "non_existent_file.txt"))
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err), "Error should be IsNotExist")
	})

	// Test Exists
	t.Run("Exists", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "exists_*.txt", content)

		exists, err := fs.Exists(filePath)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, exists, "File should exist")

		exists, err = fs.Exists(filepath.Join(tempDir, "non_existent"))
		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, exists, "Path should not exist")
	})

	// Test Truncate
	t.Run("Truncate", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "truncate_*.txt", "1234567890")

		err := fs.Truncate(filePath, 5)
		testutil.AssertNoError(t, err)

		stat, errStat := os.Stat(filePath)
		testutil.AssertNoError(t, errStat)
		testutil.AssertEqual(t, int64(5), stat.Size())

		err = fs.Truncate(filepath.Join(tempDir, "non_existent_file.txt"), 0)
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err), "Error should be IsNotExist")
	})

	// Test WriteFile
	t.Run("WriteFile", func(t *testing.T) {
		filePath := filepath.Join(tempDir, "writefile_test.txt")
		writeContent := []byte("data to write")
		perm := os.FileMode(0644)

		err := fs.WriteFile(filePath, writeContent, perm)
		testutil.AssertNoError(t, err)

		readData, errRead := os.ReadFile(filePath)
		testutil.AssertNoError(t, errRead)
		testutil.AssertEqual(t, writeContent, readData)

		err = fs.WriteFile(tempDir, writeContent, perm)
		testutil.AssertError(t, err, "Writing to a directory path should fail")
	})

	// Test Rename
	t.Run("Rename", func(t *testing.T) {
		oldPath := createTempFile(t, tempDir, "rename_old_*.txt", "move me")
		newPath := filepath.Join(tempDir, "rename_new.txt")

		err := fs.Rename(oldPath, newPath)
		testutil.AssertNoError(t, err)

		_, errStatOld := os.Stat(oldPath)
		testutil.AssertTrue(t, os.IsNotExist(errStatOld), "Old path should not exist")
		_, errStatNew := os.Stat(newPath)
		testutil.AssertNoError(t, errStatNew, "New path should exist")

		err = fs.Rename(filepath.Join(tempDir, "non_existent_old"), newPath)
		testutil.AssertError(t, err)
	})

	// Test MkdirAll
	t.Run("MkdirAll", func(t *testing.T) {
		nestedDirPath := filepath.Join(tempDir, "parent", "child")
		perm := os.FileMode(0755)

		err := fs.MkdirAll(nestedDirPath, perm)
		testutil.AssertNoError(t, err)

		stat, errStat := os.Stat(nestedDirPath)
		testutil.AssertNoError(t, errStat)
		testutil.AssertTrue(t, stat.IsDir())

		filePath := createTempFile(t, tempDir, "mkdir_file_*.txt", "")
		err = fs.MkdirAll(filePath, perm)
		testutil.AssertError(t, err, "MkdirAll on a file path should fail")
	})

	// Test Dir and Join (path operations)
	t.Run("PathOperations", func(t *testing.T) {
		testutil.AssertEqual(t, "a/b", fs.Dir("a/b/c.txt"))
		testutil.AssertEqual(t, ".", fs.Dir("a"))
		testutil.AssertEqual(t, "/", fs.Dir("/a"))

		testutil.AssertEqual(t, filepath.Join("a", "b", "c"), fs.Join("a", "b", "c"))
		testutil.AssertEqual(t, filepath.Join("a", "c"), fs.Join("a", "", "c"))
	})

	// Test Remove
	t.Run("Remove", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "remove_file_*.txt", "delete me")

		err := fs.Remove(filePath)
		testutil.AssertNoError(t, err)

		err = fs.Remove(filepath.Join(tempDir, "non_existent"))
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err), "Error should be IsNotExist")
	})

	// Test IsNotExist
	t.Run("IsNotExist", func(t *testing.T) {
		testutil.AssertTrue(t, fs.IsNotExist(os.ErrNotExist), "Should be true for os.ErrNotExist")
		testutil.AssertTrue(t, fs.IsNotExist(syscall.ENOENT), "Should be true for syscall.ENOENT")

		testutil.AssertFalse(t, fs.IsNotExist(nil), "Should be false for nil error")
		testutil.AssertFalse(t, fs.IsNotExist(io.EOF), "Should be false for other errors like io.EOF")
	})

	// Test Glob
	t.Run("Glob", func(t *testing.T) {
		f1 := createTempFile(t, tempDir, "glob_match1_*.txt", "")
		f2 := createTempFile(t, tempDir, "glob_match2_*.txt", "")
		_ = createTempFile(t, tempDir, "glob_other_*.dat", "")

		pattern := filepath.Join(tempDir, "glob_match*.txt")
		matches, err := fs.Glob(pattern)
		testutil.AssertNoError(t, err)

		expected := []string{f1, f2}
		sort.Strings(matches)
		sort.Strings(expected)
		testutil.AssertEqual(t, expected, matches)

		badPattern := filepath.Join(tempDir, "[")
		_, err = fs.Glob(badPattern)
		testutil.AssertError(t, err, "Glob with bad pattern should fail")
		testutil.AssertErrorIs(t, err, filepath.ErrBadPattern)
	})
}

func TestDefaultFile(t *testing.T) {
	fs := defaultFileSystem{}
	tempDir := t.TempDir()
	content := "0123456789"

	// Test ReadFull
	t.Run("ReadFull", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "readfull_*.txt", content)

		f, err := fs.Open(filePath)
		testutil.RequireNoError(t, err)
		defer f.Close()

		// Read full buffer
		buf5 := make([]byte, 5)
		n, err := f.ReadFull(buf5)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 5, n)
		testutil.AssertEqual(t, "01234", string(buf5))

		// Read with unexpected EOF
		buf6 := make([]byte, 6)
		n, err = f.ReadFull(buf6)
		testutil.AssertErrorIs(t, err, io.ErrUnexpectedEOF)
		testutil.AssertEqual(t, 5, n)
		testutil.AssertEqual(t, "56789", string(buf6[:n]))

		// Read at EOF
		buf1 := make([]byte, 1)
		n, err = f.ReadFull(buf1)
		testutil.AssertErrorIs(t, err, io.EOF)
		testutil.AssertEqual(t, 0, n)
	})

	// Test ReadAll
	t.Run("ReadAll", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "readall_*.txt", "read all this")

		f, err := fs.Open(filePath)
		testutil.RequireNoError(t, err)
		defer f.Close()

		data, err := f.ReadAll()
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, []byte("read all this"), data)

		// ReadAll on exhausted reader
		data, err = f.ReadAll()
		testutil.AssertNoError(t, err)
		testutil.AssertEmpty(t, data)
	})

	// Test Close
	t.Run("Close", func(t *testing.T) {
		filePath := createTempFile(t, tempDir, "close_*.txt", "a")

		f, err := fs.Open(filePath)
		testutil.RequireNoError(t, err)

		err = f.Close()
		testutil.AssertNoError(t, err)

		// Verify operation on closed file fails
		buf := make([]byte, 1)
		_, err = f.Read(buf)
		testutil.AssertError(t, err, "Read on closed file should error")

		// Double close check
		err = f.Close()
		testutil.AssertError(t, err, "Second close should error")
	})
}
