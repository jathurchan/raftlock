package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

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

// TestUpdateMax tests the updateMax atomic function
func TestUpdateMax(t *testing.T) {
	tests := []struct {
		name        string
		initialVal  uint64
		newVal      uint64
		expectedVal uint64
	}{
		{
			name:        "New value greater than current",
			initialVal:  10,
			newVal:      20,
			expectedVal: 20,
		},
		{
			name:        "New value equal to current",
			initialVal:  10,
			newVal:      10,
			expectedVal: 10,
		},
		{
			name:        "New value less than current",
			initialVal:  10,
			newVal:      5,
			expectedVal: 10,
		},
		{
			name:        "Current value zero",
			initialVal:  0,
			newVal:      5,
			expectedVal: 5,
		},
		{
			name:        "New value zero",
			initialVal:  10,
			newVal:      0,
			expectedVal: 10,
		},
		{
			name:        "Both values zero",
			initialVal:  0,
			newVal:      0,
			expectedVal: 0,
		},
		{
			name:        "Very large values",
			initialVal:  ^uint64(0) - 10,
			newVal:      ^uint64(0),
			expectedVal: ^uint64(0),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var max atomic.Uint64
			max.Store(tc.initialVal)

			updateMax(&max, tc.newVal)

			result := max.Load()
			testutil.AssertEqual(t, tc.expectedVal, result)
		})
	}
}

// TestComputePercentile tests the computePercentile function
func TestComputePercentile(t *testing.T) {
	tests := []struct {
		name       string
		samples    []uint64
		percentile float64
		expected   uint64
	}{
		{
			name:       "Empty samples",
			samples:    []uint64{},
			percentile: 0.5,
			expected:   0,
		},
		{
			name:       "Single sample",
			samples:    []uint64{42},
			percentile: 0.5,
			expected:   42,
		},
		{
			name:       "Median (odd number of samples)",
			samples:    []uint64{1, 3, 5, 7, 9},
			percentile: 0.5,
			expected:   5,
		},
		{
			name:       "Median (even number of samples)",
			samples:    []uint64{1, 3, 5, 7},
			percentile: 0.5,
			expected:   3, // index 1.5 rounded down to 1
		},
		{
			name: "95th percentile",
			samples: []uint64{
				1,
				2,
				3,
				4,
				5,
				6,
				7,
				8,
				9,
				10,
				11,
				12,
				13,
				14,
				15,
				16,
				17,
				18,
				19,
				20,
			},
			percentile: 0.95,
			expected:   19, // index 18.05 rounded down to 18
		},
		{
			name:       "0th percentile",
			samples:    []uint64{5, 10, 15, 20, 25},
			percentile: 0.0,
			expected:   5,
		},
		{
			name:       "100th percentile",
			samples:    []uint64{5, 10, 15, 20, 25},
			percentile: 1.0,
			expected:   25,
		},
		{
			name:       "Unsorted input",
			samples:    []uint64{15, 5, 25, 10, 20},
			percentile: 0.5,
			expected:   15,
		},
		{
			name:       "Duplicate values",
			samples:    []uint64{1, 1, 1, 2, 2, 3, 3, 3},
			percentile: 0.5,
			expected:   2, // index 3.5 rounded down to 3
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := computePercentile(tc.samples, tc.percentile)
			testutil.AssertEqual(
				t,
				tc.expected,
				result,
				"Expected %d for percentile %.2f but got %d",
				tc.expected,
				tc.percentile,
				result,
			)

			// Verify the original slice is not modified
			if len(tc.samples) > 0 {
				originalSampleCopy := make([]uint64, len(tc.samples))
				copy(originalSampleCopy, tc.samples)
				computePercentile(tc.samples, tc.percentile)

				// Check the samples weren't modified
				for i := range tc.samples {
					testutil.AssertEqual(t, originalSampleCopy[i], tc.samples[i],
						"Original sample at index %d was modified", i)
				}
			}
		})
	}
}

// TestFormatDurationNs tests the formatDurationNs function
func TestFormatDurationNs(t *testing.T) {
	tests := []struct {
		name     string
		ns       uint64
		expected string
	}{
		{
			name:     "Zero nanoseconds",
			ns:       0,
			expected: "0 ns",
		},
		{
			name:     "Under millisecond threshold",
			ns:       500,
			expected: "0 µs",
		},
		{
			name:     "Microseconds",
			ns:       5000,
			expected: "5 µs",
		},
		{
			name:     "Almost millisecond",
			ns:       999_999,
			expected: "999 µs",
		},
		{
			name:     "Exactly millisecond",
			ns:       1_000_000,
			expected: "1.00 ms",
		},
		{
			name:     "Multiple milliseconds",
			ns:       5_000_000,
			expected: "5.00 ms",
		},
		{
			name:     "Fractional milliseconds",
			ns:       1_234_567,
			expected: "1.23 ms",
		},
		{
			name:     "Seconds in milliseconds",
			ns:       1_000_000_000,
			expected: "1000.00 ms",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := formatDurationNs(tc.ns)
			testutil.AssertEqual(t, tc.expected, result)
		})
	}
}

// TestFormatByteSize tests the formatByteSize function
func TestFormatByteSize(t *testing.T) {
	tests := []struct {
		name     string
		bytes    uint64
		expected string
	}{
		{
			name:     "Zero bytes",
			bytes:    0,
			expected: "0 B",
		},
		{
			name:     "Small number of bytes",
			bytes:    123,
			expected: "123 B",
		},
		{
			name:     "Exactly 1 KiB",
			bytes:    1024,
			expected: "1.00 KiB",
		},
		{
			name:     "Fractional KiB",
			bytes:    1500,
			expected: "1.46 KiB",
		},
		{
			name:     "Exactly 1 MiB",
			bytes:    1024 * 1024,
			expected: "1.00 MiB",
		},
		{
			name:     "Fractional MiB",
			bytes:    1536 * 1024, // 1.5 MiB
			expected: "1.50 MiB",
		},
		{
			name:     "Exactly 1 GiB",
			bytes:    1024 * 1024 * 1024,
			expected: "1.00 GiB",
		},
		{
			name:     "Exactly 1 TiB",
			bytes:    1024 * 1024 * 1024 * 1024,
			expected: "1.00 TiB",
		},
		{
			name:     "Exactly 1 PiB",
			bytes:    1024 * 1024 * 1024 * 1024 * 1024,
			expected: "1.00 PiB",
		},
		{
			name:     "Exactly 1 EiB",
			bytes:    1024 * 1024 * 1024 * 1024 * 1024 * 1024,
			expected: "1.00 EiB",
		},
		{
			name:     "Large value beyond EiB",
			bytes:    1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 10, // 10 EiB
			expected: "10.00 EiB",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := formatByteSize(tc.bytes)
			testutil.AssertEqual(t, tc.expected, result)
		})
	}
}
