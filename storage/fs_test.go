package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/testutil"
)

// TestFileSystemBasics tests basic filesystem operations like reading, writing, and file existence
func TestFileSystemBasics(t *testing.T) {
	tempDir := t.TempDir()
	fs := newFileSystem()
	testFile := filepath.Join(tempDir, "test.txt")
	nonExistentFile := filepath.Join(tempDir, "nonexistent.txt")
	sampleContent := []byte("test content")

	// Test file write and read
	t.Run("WriteAndRead", func(t *testing.T) {
		err := fs.WriteFile(testFile, sampleContent, 0644)
		testutil.AssertNoError(t, err)

		exists, err := fs.Exists(testFile)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, exists)

		content, err := fs.ReadFile(testFile)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, sampleContent, content)

		// Test Open and file read
		file, err := fs.Open(testFile)
		testutil.AssertNoError(t, err)
		defer func() { _ = file.Close() }()

		buf := make([]byte, len(sampleContent))
		n, err := file.Read(buf)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, len(sampleContent), n)
		testutil.AssertEqual(t, sampleContent, buf)

		// Check nonexistent file
		_, err = fs.ReadFile(nonExistentFile)
		testutil.AssertTrue(t, fs.IsNotExist(err))

		exists, err = fs.Exists(nonExistentFile)
		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, exists)
	})

	// Test Stat functionality and custom stat function
	t.Run("StatFunctionality", func(t *testing.T) {
		err := fs.WriteFile(testFile, sampleContent, 0644)
		testutil.AssertNoError(t, err)

		info, err := fs.Stat(testFile)
		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, info)
		testutil.AssertEqual(t, int64(len(sampleContent)), info.Size())
		testutil.AssertFalse(t, info.IsDir())

		dirInfo, err := fs.Stat(tempDir)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, dirInfo.IsDir())

		// Test nil stat function (should default to os.Stat)
		fs := newFileSystemWithStat(nil)
		testutil.AssertNotNil(t, fs)

		info, err = fs.Stat(tempDir)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, info.IsDir())

		// Test custom stat function
		customFS := newFileSystemWithStat(func(name string) (os.FileInfo, error) {
			if name == testFile {
				return nil, errors.New("custom stat error")
			}
			return os.Stat(name)
		})

		_, err = customFS.Stat(testFile)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "custom stat error")
	})

	// Test path manipulation functions
	t.Run("PathOperations", func(t *testing.T) {
		// Test Path method with representative cases
		cases := []struct {
			dir, file, expected string
		}{
			{"dir", "file.txt", filepath.Join("dir", "file.txt")},
			{"/absolute/dir", "file.txt", filepath.Join("/absolute/dir", "file.txt")},
			{"", "file.txt", "file.txt"},
			{"dir", "", "dir"},
		}

		for _, tc := range cases {
			result := fs.Path(tc.dir, tc.file)
			testutil.AssertEqual(t, tc.expected, result)
		}

		// Test Dir method
		dirName := fs.Dir(filepath.Join(tempDir, "file.txt"))
		testutil.AssertEqual(t, tempDir, dirName)

		// Test Join method
		nestedPath := filepath.Join("a", "b", "c")
		joined := fs.Join("a", "b", "c")
		testutil.AssertEqual(t, nestedPath, joined)
	})

	// Test MkdirAll
	t.Run("DirectoryCreation", func(t *testing.T) {
		nestedDir := filepath.Join(tempDir, "nested", "dir")
		err := fs.MkdirAll(nestedDir, 0755)
		testutil.AssertNoError(t, err)

		exists, err := fs.Exists(nestedDir)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, exists)
	})
}

// TestFileModification tests file-modifying operations like truncate, append, rename, and remove
func TestFileModification(t *testing.T) {
	tempDir := t.TempDir()
	fs := newFileSystem()
	testFile := filepath.Join(tempDir, "test.txt")

	// Test truncate
	t.Run("Truncate", func(t *testing.T) {
		err := fs.WriteFile(testFile, []byte("longer content than before"), 0644)
		testutil.AssertNoError(t, err)

		err = fs.Truncate(testFile, 5)
		testutil.AssertNoError(t, err)

		content, err := fs.ReadFile(testFile)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, []byte("longe"), content)
	})

	// Test append
	t.Run("Append", func(t *testing.T) {
		err := fs.WriteFile(testFile, []byte("start"), 0644)
		testutil.AssertNoError(t, err)

		appendFile, err := fs.AppendFile(testFile)
		testutil.AssertNoError(t, err)

		_, err = appendFile.Write([]byte(" appended"))
		testutil.AssertNoError(t, err)
		err = appendFile.Sync()
		testutil.AssertNoError(t, err)
		_ = appendFile.Close()

		content, err := fs.ReadFile(testFile)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, []byte("start appended"), content)

		// Test append on non-existent file (should create it)
		newFile := filepath.Join(tempDir, "new_from_append.txt")
		appendFile, err = fs.AppendFile(newFile)
		testutil.AssertNoError(t, err)

		_, err = appendFile.Write([]byte("new file content"))
		testutil.AssertNoError(t, err)
		_ = appendFile.Close()

		content, err = fs.ReadFile(newFile)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, []byte("new file content"), content)
	})

	// Test rename and remove
	t.Run("RenameAndRemove", func(t *testing.T) {
		err := fs.WriteFile(testFile, []byte("test content"), 0644)
		testutil.AssertNoError(t, err)

		newPath := filepath.Join(tempDir, "renamed.txt")
		err = fs.Rename(testFile, newPath)
		testutil.AssertNoError(t, err)

		exists, err := fs.Exists(newPath)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, exists)

		exists, err = fs.Exists(testFile)
		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, exists)

		err = fs.Remove(newPath)
		testutil.AssertNoError(t, err)

		exists, err = fs.Exists(newPath)
		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, exists)
	})
}

// TestFileInterface tests the file interface methods like Seek, ReadFull, and ReadAll
func TestFileInterface(t *testing.T) {
	tempDir := t.TempDir()
	fs := newFileSystem()
	testFile := filepath.Join(tempDir, "file.txt")

	err := fs.WriteFile(testFile, []byte("0123456789"), 0644)
	testutil.AssertNoError(t, err)

	// Test Seek
	t.Run("Seek", func(t *testing.T) {
		file, err := fs.Open(testFile)
		testutil.AssertNoError(t, err)
		defer func() { _ = file.Close() }()

		pos, err := file.Seek(3, io.SeekStart)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, int64(3), pos)

		buf := make([]byte, 3)
		n, err := file.Read(buf)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 3, n)
		testutil.AssertEqual(t, []byte("345"), buf)
	})

	// Test ReadFull
	t.Run("ReadFull", func(t *testing.T) {
		file, err := fs.Open(testFile)
		testutil.AssertNoError(t, err)
		defer func() { _ = file.Close() }()

		buf := make([]byte, 5)
		n, err := file.ReadFull(buf)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 5, n)
		testutil.AssertEqual(t, []byte("01234"), buf)
	})

	// Test ReadAll
	t.Run("ReadAll", func(t *testing.T) {
		file, err := fs.Open(testFile)
		testutil.AssertNoError(t, err)
		defer func() { _ = file.Close() }()

		allContent, err := file.ReadAll()
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, []byte("0123456789"), allContent)
	})
}

// TestAtomicWrite tests atomic write operations and edge cases
func TestAtomicWrite(t *testing.T) {
	tempDir := t.TempDir()
	testPath := filepath.Join(tempDir, "test.txt")
	fs := newFileSystem()

	// Test basic atomic write
	t.Run("BasicAtomicWrite", func(t *testing.T) {
		err := fs.AtomicWrite(testPath, []byte("atomic content"), 0644)
		testutil.AssertNoError(t, err)

		content, err := fs.ReadFile(testPath)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, []byte("atomic content"), content)
	})

	// Test interrupted write
	t.Run("InterruptedWrite", func(t *testing.T) {
		originalContent := []byte("original content")

		err := fs.WriteFile(testPath, originalContent, 0644)
		testutil.AssertNoError(t, err)

		// Create a custom filesystem that fails during rename
		mockFS := &mockFileSystem{
			WriteFileFunc: func(name string, data []byte, perm os.FileMode) error {
				return nil
			},
			MkdirAllFunc: func(path string, perm os.FileMode) error {
				return nil
			},
			DirFunc: func(path string) string {
				return filepath.Dir(path)
			},
			RenameFunc: func(oldPath, newPath string) error {
				return errors.New("simulated rename failure")
			},
			RemoveFunc: func(name string) error {
				return nil
			},
		}

		err = atomicWrite(mockFS, testPath, []byte("new content"), 0644)
		testutil.AssertError(t, err)

		// Original content should be preserved
		content, err := fs.ReadFile(testPath)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, originalContent, content)
	})

	// Test permissions are preserved
	t.Run("PermissionsPreserved", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Skipping permissions test when running as root")
		}

		testPerm := os.FileMode(0600) // Only owner can read/write
		err := fs.AtomicWrite(testPath, []byte("secret"), testPerm)
		testutil.AssertNoError(t, err)

		info, err := os.Stat(testPath)
		testutil.AssertNoError(t, err)
		// Only check user permissions since platform umask might affect other bits
		testutil.AssertEqual(t, testPerm&0700, info.Mode()&0700)
	})

	// Test WriteMaybeAtomic
	t.Run("WriteMaybeAtomic", func(t *testing.T) {
		atomicPath := filepath.Join(tempDir, "atomic_maybe.txt")
		nonAtomicPath := filepath.Join(tempDir, "non_atomic_maybe.txt")
		testContent := []byte("test content")

		// Atomic write
		err := fs.WriteMaybeAtomic(atomicPath, testContent, 0644, true)
		testutil.AssertNoError(t, err)
		content, err := fs.ReadFile(atomicPath)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, testContent, content)

		// Non-atomic write
		err = fs.WriteMaybeAtomic(nonAtomicPath, testContent, 0644, false)
		testutil.AssertNoError(t, err)
		content, err = fs.ReadFile(nonAtomicPath)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, testContent, content)

		// Test code path selection with mock
		mockFS := newMockFileSystem()
		mockFS.WriteFileFunc = func(name string, data []byte, perm os.FileMode) error {
			if filepath.Base(name) == "direct.txt" {
				return nil
			}
			return errors.New("unexpected WriteFile call")
		}
		mockFS.atomicWriteErr = errors.New("simulated atomic write error")

		// Direct write should succeed
		err = mockFS.WriteMaybeAtomic(
			filepath.Join(tempDir, "direct.txt"),
			testContent,
			0644,
			false,
		)
		testutil.AssertNoError(t, err)

		// Atomic write should fail with the mocked error
		err = mockFS.WriteMaybeAtomic(
			filepath.Join(tempDir, "should-fail.txt"),
			testContent,
			0644,
			true,
		)
		testutil.AssertError(t, err)
	})
}

// TestGlobPattern tests the Glob method
func TestGlobPattern(t *testing.T) {
	tempDir := t.TempDir()
	fs := newFileSystem()

	// Setup test files
	err := fs.WriteFile(filepath.Join(tempDir, "glob1.txt"), []byte("1"), 0644)
	testutil.AssertNoError(t, err)
	err = fs.WriteFile(filepath.Join(tempDir, "glob2.txt"), []byte("2"), 0644)
	testutil.AssertNoError(t, err)
	err = fs.WriteFile(filepath.Join(tempDir, "other.txt"), []byte("3"), 0644)
	testutil.AssertNoError(t, err)

	// Test matching pattern
	matches, err := fs.Glob(filepath.Join(tempDir, "glob*.txt"))
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, 2, len(matches))

	// Test invalid pattern
	_, err = fs.Glob("[")
	testutil.AssertError(t, err)
}

// TestConcurrentAccess tests concurrent file operations
func TestConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	fs := newFileSystem()
	concurrentFile := filepath.Join(tempDir, "concurrent.txt")

	err := fs.WriteFile(concurrentFile, []byte("initial"), 0644)
	testutil.AssertNoError(t, err)

	// Test concurrent reads
	t.Run("ConcurrentReads", func(t *testing.T) {
		var wg sync.WaitGroup
		const goroutines = 5
		const iterations = 10

		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				for range iterations {
					content, err := fs.ReadFile(concurrentFile)
					testutil.AssertNoError(t, err)
					testutil.AssertNotNil(t, content)
					time.Sleep(time.Millisecond) // Small delay to allow interleaving
				}
			}()
		}
		wg.Wait()
	})

	// Test concurrent appends
	t.Run("ConcurrentAppends", func(t *testing.T) {
		appendFile := filepath.Join(tempDir, "append.txt")
		err = fs.WriteFile(appendFile, []byte(""), 0644)
		testutil.AssertNoError(t, err)

		var wg sync.WaitGroup
		const goroutines = 5
		const iterations = 10

		wg.Add(goroutines)
		for i := range goroutines {
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					f, err := fs.AppendFile(appendFile)
					testutil.AssertNoError(t, err)
					_, err = f.Write([]byte{byte('A' + i)})
					testutil.AssertNoError(t, err)
					err = f.Sync()
					testutil.AssertNoError(t, err)
					_ = f.Close()
					time.Sleep(time.Millisecond) // Small delay to allow interleaving
				}
			}()
		}
		wg.Wait()

		content, err := fs.ReadFile(appendFile)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, goroutines*iterations, len(content))
	})
}

// TestContextTimeout tests file operations with context timeouts
func TestContextTimeout(t *testing.T) {
	tempDir := t.TempDir()
	fs := newFileSystem()
	testFile := filepath.Join(tempDir, "large.txt")

	// Create a large file (1MB)
	largeData := bytes.Repeat([]byte("a"), 1024*1024)
	err := fs.WriteFile(testFile, largeData, 0644)
	testutil.AssertNoError(t, err)

	// Test read with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	err = func() error {
		// First check if context is already done
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Context not done yet, proceed with file operation
		}

		file, err := fs.Open(testFile)
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()

		// Try to read the file with periodic context checks
		buf := make([]byte, 1024)
		for {
			// Check context before each chunk read
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Context not done yet, continue reading
			}

			_, err := file.Read(buf)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}

			// Simulate some processing time
			time.Sleep(time.Millisecond)
		}

		return nil
	}()

	// Expect a context deadline exceeded error
	testutil.AssertError(t, err)
	testutil.AssertTrue(t, errors.Is(err, context.DeadlineExceeded))
}

// TestErrorCases tests various error conditions
func TestErrorCases(t *testing.T) {
	tempDir := t.TempDir()
	fs := newFileSystem()
	nonExistentFile := filepath.Join(tempDir, "nonexistent.txt")

	// Test open error
	t.Run("OpenError", func(t *testing.T) {
		file, err := fs.Open(nonExistentFile)
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs.IsNotExist(err))
		testutil.AssertTrue(t, file == nil, "Expected file to be nil on error")
	})

	// Test exists error
	t.Run("ExistsError", func(t *testing.T) {
		permErr := errors.New("permission denied")
		mockFS := newFileSystemWithStat(func(name string) (os.FileInfo, error) {
			return nil, permErr
		})

		exists, err := mockFS.Exists(tempDir)
		testutil.AssertFalse(t, exists)
		testutil.AssertError(t, err)
		testutil.AssertEqual(t, permErr, err)
	})

	// Test AppendFile error
	t.Run("AppendFileError", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Skipping AppendFile error test when running as root")
		}

		readOnlyDir := filepath.Join(tempDir, "readonly")
		err := os.Mkdir(readOnlyDir, 0500) // Read-only directory
		testutil.AssertNoError(t, err)
		defer func() { _ = os.Chmod(readOnlyDir, 0700) }() // Ensure we can clean up

		readOnlyFile := filepath.Join(readOnlyDir, "cantappend.txt")

		file, err := fs.AppendFile(readOnlyFile)
		testutil.AssertError(t, err)
		testutil.AssertTrue(t, file == nil, "Expected file to be nil on error")
	})
}
