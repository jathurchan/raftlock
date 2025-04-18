package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

// TestAtomicWriteSuccess tests that atomicWrite successfully writes data atomically
func TestAtomicWriteSuccess(t *testing.T) {
	mockFS := newMockFileSystem()
	testPath := "/test/file.txt"
	testData := []byte("test data")

	err := atomicWrite(mockFS, testPath, testData, 0644)
	testutil.AssertNoError(t, err)

	// Check that the file was written
	data, err := mockFS.ReadFile(testPath)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, testData, data)
}

// TestAtomicWriteFailures tests various failure scenarios for atomicWrite
func TestAtomicWriteFailures(t *testing.T) {
	t.Run("MkdirFailure", func(t *testing.T) {
		mockFS := newMockFileSystem()
		mockFS.mkdirAllErr = errors.New("mkdir failed")
		testPath := "/test/file.txt"
		testData := []byte("test data")

		err := atomicWrite(mockFS, testPath, testData, 0644)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to create dir")
	})

	t.Run("WriteFileFailure", func(t *testing.T) {
		mockFS := newMockFileSystem()
		mockFS.writeFileErr = errors.New("write failed")
		testPath := "/test/file.txt"
		testData := []byte("test data")

		err := atomicWrite(mockFS, testPath, testData, 0644)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to write temp file")
	})

	t.Run("RenameFailure", func(t *testing.T) {
		mockFS := newMockFileSystem()
		mockFS.renameErr = errors.New("rename failed")
		testPath := "/test/file.txt"
		testData := []byte("test data")

		err := atomicWrite(mockFS, testPath, testData, 0644)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to rename temp file")

		// Ensure the temp file was removed
		tmpPath := testPath + ".tmp"
		exists, _ := mockFS.Exists(tmpPath)
		testutil.AssertFalse(t, exists)
	})

	t.Run("RemoveFailureAfterRenameFailure", func(t *testing.T) {
		mockFS := newMockFileSystem()
		mockFS.renameErr = errors.New("rename failed")
		mockFS.removeErr = errors.New("remove failed")
		testPath := "/test/file.txt"
		testData := []byte("test data")

		err := atomicWrite(mockFS, testPath, testData, 0644)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to rename temp file")
		// The function should still return the rename error, not the remove error
		testutil.AssertContains(t, err.Error(), "rename failed")
	})
}

// TestAtomicRenameOrCleanup tests the atomicRenameOrCleanup function
func TestAtomicRenameOrCleanup(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockFS := newMockFileSystem()
		tmpPath := "/test/file.tmp"
		finalPath := "/test/file.txt"
		testData := []byte("test data")

		// Write to temp file
		err := mockFS.WriteFile(tmpPath, testData, 0644)
		testutil.AssertNoError(t, err)

		// Rename
		err = atomicRenameOrCleanup(mockFS, tmpPath, finalPath)
		testutil.AssertNoError(t, err)

		// Check final file exists and temp doesn't
		exists, _ := mockFS.Exists(finalPath)
		testutil.AssertTrue(t, exists)

		exists, _ = mockFS.Exists(tmpPath)
		testutil.AssertFalse(t, exists)
	})

	t.Run("RenameFailure", func(t *testing.T) {
		mockFS := newMockFileSystem()
		tmpPath := "/test/file.tmp"
		finalPath := "/test/file.txt"
		testData := []byte("test data")

		// Write to temp file
		err := mockFS.WriteFile(tmpPath, testData, 0644)
		testutil.AssertNoError(t, err)

		// Set rename to fail
		mockFS.renameErr = errors.New("rename failed")

		// Rename should fail and temp file should be removed
		err = atomicRenameOrCleanup(mockFS, tmpPath, finalPath)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to rename temp file")

		// Check temp file was removed
		exists, _ := mockFS.Exists(tmpPath)
		testutil.AssertFalse(t, exists)
	})
}

// TestClampLogRange tests the behavior of clampLogRange function
func TestClampLogRange(t *testing.T) {
	testCases := []struct {
		name        string
		start       types.Index
		end         types.Index
		first       types.Index
		last        types.Index
		expStart    types.Index
		expEnd      types.Index
		expNonEmpty bool
	}{
		{
			name:        "Range fully within bounds",
			start:       5,
			end:         10,
			first:       1,
			last:        15,
			expStart:    5,
			expEnd:      10,
			expNonEmpty: true,
		},
		{
			name:        "Range partially below lower bound",
			start:       0,
			end:         10,
			first:       5,
			last:        15,
			expStart:    5,
			expEnd:      10,
			expNonEmpty: true,
		},
		{
			name:        "Range partially above upper bound",
			start:       10,
			end:         20,
			first:       5,
			last:        15,
			expStart:    10,
			expEnd:      16, // last + 1
			expNonEmpty: true,
		},
		{
			name:        "Range completely below lower bound",
			start:       0,
			end:         5,
			first:       10,
			last:        15,
			expStart:    0,
			expEnd:      0,
			expNonEmpty: false,
		},
		{
			name:        "Range completely above upper bound",
			start:       20,
			end:         25,
			first:       10,
			last:        15,
			expStart:    0,
			expEnd:      0,
			expNonEmpty: false,
		},
		{
			name:        "Start equals end",
			start:       10,
			end:         10,
			first:       5,
			last:        15,
			expStart:    0,
			expEnd:      0,
			expNonEmpty: false,
		},
		{
			name:        "Start greater than end",
			start:       15,
			end:         10,
			first:       5,
			last:        20,
			expStart:    0,
			expEnd:      0,
			expNonEmpty: false,
		},
		{
			name:        "Empty range after clamping",
			start:       5,
			end:         10,
			first:       10,
			last:        15,
			expStart:    0,
			expEnd:      0,
			expNonEmpty: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start, end, nonEmpty := clampLogRange(tc.start, tc.end, tc.first, tc.last)
			testutil.AssertEqual(t, tc.expStart, start)
			testutil.AssertEqual(t, tc.expEnd, end)
			testutil.AssertEqual(t, tc.expNonEmpty, nonEmpty)
		})
	}
}

// TestFailAndRollback tests the FailAndRollback function
func TestFailAndRollback(t *testing.T) {
	mockLog := &logger.NoOpLogger{
		ErrorwFunc: func(msg string, keysAndValues ...any) {},
		WarnwFunc:  func(msg string, keysAndValues ...any) {},
	}

	t.Run("TruncateSuccess", func(t *testing.T) {
		mockFS := newMockFileSystem()
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
		}

		path := "/test/file.txt"
		mockFS.WriteFile(path, []byte("0123456789"), 0644)
		startOffset := int64(5)

		err := FailAndRollback(mockFile, mockFS, path, startOffset, mockLog, "test", "test error: %v", errors.New("original error"))
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "test error: original error")

		// Verify file was truncated
		testutil.AssertEqual(t, path, mockFS.truncatedPath)
		testutil.AssertEqual(t, startOffset, mockFS.truncatedSize)
	})

	t.Run("TruncateFailure", func(t *testing.T) {
		mockFS := newMockFileSystem()
		mockFS.truncateErr = errors.New("truncate error")
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
		}

		path := "/test/file.txt"
		startOffset := int64(5)

		err := FailAndRollback(mockFile, mockFS, path, startOffset, mockLog, "test", "test error: %v", errors.New("original error"))
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "test error: original error")
	})
}

// TestWriteChunks tests the writeChunks function
func TestWriteChunks(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("0123456789ABCDEF")
		chunkSize := 4
		var written []byte
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				written = append(written, p...)
				return len(p), nil
			},
		}

		err := writeChunks(ctx, mockFile, data, chunkSize)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, data, written)
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		data := []byte("0123456789ABCDEF")
		chunkSize := 4
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
		}

		err := writeChunks(ctx, mockFile, data, chunkSize)
		testutil.AssertError(t, err)
		testutil.AssertEqual(t, context.Canceled, err)
	})

	t.Run("WriteError", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("0123456789ABCDEF")
		chunkSize := 4
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				return 0, errors.New("write error")
			},
		}

		err := writeChunks(ctx, mockFile, data, chunkSize)
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
		testutil.AssertContains(t, err.Error(), "failed to write snapshot chunk")
	})

	t.Run("PartialWrite", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("0123456789ABCDEF")
		chunkSize := 4
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				// Return only half the bytes were written
				return len(p) / 2, nil
			},
		}

		err := writeChunks(ctx, mockFile, data, chunkSize)
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
		testutil.AssertContains(t, err.Error(), "incomplete chunk write")
	})

	t.Run("SyncError", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("0123456789ABCDEF")
		chunkSize := 4
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			SyncFunc: func() error {
				return errors.New("sync error")
			},
		}

		err := writeChunks(ctx, mockFile, data, chunkSize)
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
		testutil.AssertContains(t, err.Error(), "failed to sync file")
	})
}

// TestReadChunks tests the readChunks function
func TestReadChunks(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("0123456789ABCDEF")
		mockFile := &mockFile{
			Reader: bytes.NewReader(data),
		}

		result, err := readChunks(ctx, mockFile, int64(len(data)), 4)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, data, result)
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		data := []byte("0123456789ABCDEF")
		mockFile := &mockFile{
			Reader: bytes.NewReader(data),
		}

		result, err := readChunks(ctx, mockFile, int64(len(data)), 4)
		testutil.AssertError(t, err)
		testutil.AssertEqual(t, context.Canceled, err)
		testutil.AssertTrue(t, result == nil)
	})

	t.Run("ReadError", func(t *testing.T) {
		ctx := context.Background()
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			ReadFunc: func(p []byte) (int, error) {
				return 0, errors.New("read error")
			},
		}

		result, err := readChunks(ctx, mockFile, 10, 4)
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
		testutil.AssertContains(t, err.Error(), "failed to read snapshot chunk")
		testutil.AssertTrue(t, result == nil)
	})

	t.Run("IncompleteRead_EarlyEOF", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("0123456789") // 10 bytes

		// Simulate a reader that returns only 10 bytes, no matter what
		var offset int
		mockFile := &mockFile{
			ReadFunc: func(p []byte) (int, error) {
				if offset >= len(data) {
					return 0, io.EOF
				}
				n := copy(p, data[offset:])
				offset += n
				return n, nil
			},
		}

		// Attempt to read more bytes than the mockFile provides (15 expected)
		result, err := readChunks(ctx, mockFile, int64(len(data)+5), 4)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrCorruptedSnapshot)
		testutil.AssertContains(t, err.Error(), "unexpected EOF before reading all data")
		testutil.AssertTrue(t, result == nil)
	})

	t.Run("CompleteRead_WithEOF", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("0123456789")

		reader := bytes.NewReader(data)
		mockFile := &mockFile{
			ReadFunc: func(p []byte) (int, error) {
				n, err := reader.Read(p)
				if err == nil && reader.Len() == 0 {
					err = io.EOF
				}
				return n, err
			},
		}

		result, err := readChunks(ctx, mockFile, int64(len(data)), 4)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, data, result)
	})

	t.Run("IncompleteRead_SilentShortRead", func(t *testing.T) {
		ctx := context.Background()
		expectedSize := int64(10)
		data := []byte("0123456") // only 7 bytes provided

		var offset int
		mockFile := &mockFile{
			ReadFunc: func(p []byte) (int, error) {
				if offset >= len(data) {
					// Do NOT return io.EOF
					return 0, nil // silent short read
				}
				n := copy(p, data[offset:])
				offset += n
				return n, nil
			},
		}

		result, err := readChunks(ctx, mockFile, expectedSize, 4)
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrCorruptedSnapshot)
		testutil.AssertContains(t, err.Error(), "read returned 0 bytes with no error")
		testutil.AssertTrue(t, result == nil)
	})
}
