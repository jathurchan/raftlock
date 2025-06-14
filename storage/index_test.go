package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

var errForcedReadFailure = errors.New("forced read failure")

func setupTestIndexService() (indexService, *mockFileSystem, *mockLogEntryReader) {
	mockFs := newMockFileSystem()
	mockReader := newMockLogEntryReader()
	lg := logger.NewNoOpLogger()
	is := newIndexServiceWithReader(mockFs, mockReader, lg)
	return is, mockFs, mockReader
}

func TestIndexService_Build(t *testing.T) {
	logPath := "test.log"

	type errorCheck struct {
		shouldError bool
		check       func(t *testing.T, err error)
	}

	tests := []struct {
		name             string
		setupMocks       func(*mockFileSystem, *mockLogEntryReader)
		expectedResult   buildResult
		errorCheck       errorCheck
		expectedTruncate bool
		truncateOffset   int64
	}{
		{
			name: "FileDoesNotExist",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				// Default state: file doesn't exist
			},
			expectedResult: buildResult{
				IndexMap:       []types.IndexOffsetPair{},
				Truncated:      false,
				LastValidIndex: 0,
			},
			errorCheck: errorCheck{
				shouldError: false,
			},
		},
		{
			name: "ErrorCheckingFileExistence",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.existsErr = errors.New("disk read error")
			},
			expectedResult: buildResult{},
			errorCheck: errorCheck{
				shouldError: true,
				check: func(t *testing.T, err error) {
					testutil.AssertErrorIs(t, err, ErrStorageIO)
					testutil.AssertContains(t, err.Error(), "error checking log file existence")
				},
			},
			expectedTruncate: false,
		},
		{
			name: "ErrorOpeningFile",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{} // File exists
				fs.openErr = errors.New("permission denied")
			},
			expectedResult: buildResult{},
			errorCheck: errorCheck{
				shouldError: true,
				check: func(t *testing.T, err error) {
					testutil.AssertErrorIs(t, err, ErrStorageIO)
					testutil.AssertContains(t, err.Error(), "failed to open log file")
				},
			},
			expectedTruncate: false,
		},
		{
			name: "EmptyLogFile",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{} // Empty file
			},
			expectedResult: buildResult{
				IndexMap:       []types.IndexOffsetPair{},
				Truncated:      false,
				LastValidIndex: 0,
			},
			errorCheck: errorCheck{
				shouldError: false,
			},
		},
		{
			name: "SuccessfulBuild_SingleEntry",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01}
				r.AddEntry(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}, 10)
			},
			expectedResult: buildResult{
				IndexMap:       []types.IndexOffsetPair{{Index: 1, Offset: 0}},
				Truncated:      false,
				LastValidIndex: 0,
			},
			errorCheck: errorCheck{
				shouldError: false,
			},
		},
		{
			name: "SuccessfulBuild_MultipleEntries",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
				r.AddEntry(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}, 10)
				r.AddEntry(types.LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")}, 15)
				r.AddEntry(types.LogEntry{Term: 2, Index: 3, Command: []byte("cmd3")}, 20)
			},
			expectedResult: buildResult{
				IndexMap: []types.IndexOffsetPair{
					{Index: 1, Offset: 0},
					{Index: 2, Offset: 10},
					{Index: 3, Offset: 25},
				},
				Truncated:      false,
				LastValidIndex: 0,
			},
			errorCheck: errorCheck{
				shouldError: false,
			},
		},
		{
			name: "Corruption_ReadFailure_TruncateSuccess",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
				r.AddEntry(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}, 10)
				r.AddEntry(types.LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")}, 15)
				r.AddError(errors.New("simulated read error"), 5)
			},
			expectedResult: buildResult{
				IndexMap: []types.IndexOffsetPair{
					{Index: 1, Offset: 0},
					{Index: 2, Offset: 10},
				},
				Truncated:      true,
				LastValidIndex: 2,
			},
			errorCheck: errorCheck{
				shouldError: false,
			},
			expectedTruncate: true,
			truncateOffset:   25,
		},
		{
			name: "Corruption_ReadFailure_TruncateFailure",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
				r.AddEntry(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}, 10)
				r.AddEntry(types.LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")}, 15)
				r.AddError(errors.New("simulated read error"), 5)
				fs.truncateErr = errors.New("disk full")
			},
			expectedResult: buildResult{
				IndexMap: []types.IndexOffsetPair{
					{Index: 1, Offset: 0},
					{Index: 2, Offset: 10},
				},
				Truncated:      true,
				LastValidIndex: 2,
			},
			errorCheck: errorCheck{
				shouldError: true,
				check: func(t *testing.T, err error) {
					testutil.AssertContains(t, err.Error(), "corruption at offset 25")
					testutil.AssertContains(t, err.Error(), "disk full")
				},
			},
			expectedTruncate: true,
			truncateOffset:   25,
		},
		{
			name: "Corruption_OutOfOrderIndex",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
				r.AddEntry(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}, 10)
				r.AddEntry(types.LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")}, 15)
				r.AddEntry(
					types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd3")},
					20,
				) // Out of order
			},
			expectedResult: buildResult{
				IndexMap: []types.IndexOffsetPair{
					{Index: 1, Offset: 0},
					{Index: 2, Offset: 10},
				},
				Truncated:      true,
				LastValidIndex: 2,
			},
			errorCheck: errorCheck{
				shouldError: false,
			},
			expectedTruncate: true,
			truncateOffset:   25,
		},
		{
			name: "Corruption_IndexGap",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
				r.AddEntry(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}, 10)
				r.AddEntry(types.LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")}, 15)
				r.AddEntry(
					types.LogEntry{Term: 1, Index: 4, Command: []byte("cmd3")},
					20,
				) // Gap (should be 3)
			},
			expectedResult: buildResult{
				IndexMap: []types.IndexOffsetPair{
					{Index: 1, Offset: 0},
					{Index: 2, Offset: 10},
				},
				Truncated:      true,
				LastValidIndex: 2,
			},
			errorCheck: errorCheck{
				shouldError: false,
			},
			expectedTruncate: true,
			truncateOffset:   25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is, mfs, mr := setupTestIndexService()
			mfs.resetTruncate() // Reset truncate tracking

			// Setup the test case
			tt.setupMocks(mfs, mr)

			// Call the method under test
			result, err := is.Build(logPath)

			// Verify error behavior
			if tt.errorCheck.shouldError {
				testutil.AssertError(t, err)
				if err != nil && tt.errorCheck.check != nil {
					tt.errorCheck.check(t, err)
				}
			} else {
				testutil.AssertNoError(t, err)
			}

			// Verify result
			testutil.AssertEqual(t, tt.expectedResult.IndexMap, result.IndexMap)
			testutil.AssertEqual(t, tt.expectedResult.Truncated, result.Truncated)

			if result.Truncated {
				testutil.AssertEqual(t, tt.expectedResult.LastValidIndex, result.LastValidIndex)
			}

			// Verify truncation behavior if expected
			if tt.expectedTruncate {
				testutil.AssertEqual(t, logPath, mfs.truncatedPath)
				testutil.AssertEqual(t, tt.truncateOffset, mfs.truncatedSize)
			}
		})
	}
}

func TestIndexService_NegativeTruncateOffset(t *testing.T) {
	is, _, _ := setupTestIndexService()
	svc, ok := is.(*defaultIndexService)
	if !ok {
		t.Fatal("Failed to cast to defaultIndexService")
	}

	err := svc.truncateLogAt("test.log", -10)
	testutil.AssertError(t, err)
	testutil.AssertContains(t, err.Error(), "invalid negative offset (-10)")
}

func TestIndexService_VerifyConsistency(t *testing.T) {
	tests := []struct {
		name       string
		indexMap   []types.IndexOffsetPair
		errorCheck struct {
			shouldError bool
			errorMsg    string
		}
	}{
		{
			name:     "EmptyMap",
			indexMap: []types.IndexOffsetPair{},
			errorCheck: struct {
				shouldError bool
				errorMsg    string
			}{shouldError: false},
		},
		{
			name:     "SingleEntryMap",
			indexMap: []types.IndexOffsetPair{{Index: 5, Offset: 100}},
			errorCheck: struct {
				shouldError bool
				errorMsg    string
			}{shouldError: false},
		},
		{
			name: "ConsistentMap",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 25},
			},
			errorCheck: struct {
				shouldError bool
				errorMsg    string
			}{shouldError: false},
		},
		{
			name: "ConsistentMap_StartsAbove1",
			indexMap: []types.IndexOffsetPair{
				{Index: 10, Offset: 100},
				{Index: 11, Offset: 120},
				{Index: 12, Offset: 145},
			},
			errorCheck: struct {
				shouldError bool
				errorMsg    string
			}{shouldError: false},
		},
		{
			name: "InconsistentMap_Gap",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 4, Offset: 25}, // Gap (missing 3)
			},
			errorCheck: struct {
				shouldError bool
				errorMsg    string
			}{
				shouldError: true,
				errorMsg:    "log discontinuity at 2 (expected 3, got 4)",
			},
		},
		{
			name: "InconsistentMap_OutOfOrder",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 1, Offset: 25}, // Out of order (should be 3)
			},
			errorCheck: struct {
				shouldError bool
				errorMsg    string
			}{
				shouldError: true,
				errorMsg:    "log discontinuity at 2 (expected 3, got 1)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is, _, _ := setupTestIndexService()
			err := is.VerifyConsistency(tt.indexMap)

			if tt.errorCheck.shouldError {
				testutil.AssertError(t, err)
				if err != nil {
					testutil.AssertErrorIs(t, err, ErrCorruptedLog)
					testutil.AssertContains(t, err.Error(), tt.errorCheck.errorMsg)
				}
			} else {
				testutil.AssertNoError(t, err)
			}
		})
	}
}

func TestIndexService_GetBounds(t *testing.T) {
	tests := []struct {
		name         string
		indexMap     []types.IndexOffsetPair
		currentFirst types.Index
		currentLast  types.Index
		expected     boundsResult
	}{
		{
			name:         "Empty index map, no current bounds",
			indexMap:     []types.IndexOffsetPair{},
			currentFirst: 0,
			currentLast:  0,
			expected: boundsResult{
				NewFirst: 0,
				NewLast:  0,
				Changed:  false,
				WasReset: false,
			},
		},
		{
			name:         "Empty index map, existing current bounds",
			indexMap:     []types.IndexOffsetPair{},
			currentFirst: 1,
			currentLast:  5,
			expected: boundsResult{
				NewFirst: 0,
				NewLast:  0,
				Changed:  true,
				WasReset: true,
			},
		},
		{
			name: "Non-empty index map, same bounds",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			currentFirst: 1,
			currentLast:  3,
			expected: boundsResult{
				NewFirst: 1,
				NewLast:  3,
				Changed:  false,
				WasReset: false,
			},
		},
		{
			name: "Non-empty index map, different first",
			indexMap: []types.IndexOffsetPair{
				{Index: 2, Offset: 0},
				{Index: 3, Offset: 10},
				{Index: 4, Offset: 20},
			},
			currentFirst: 1,
			currentLast:  4,
			expected: boundsResult{
				NewFirst: 2,
				NewLast:  4,
				Changed:  true,
				WasReset: false,
			},
		},
		{
			name: "Non-empty index map, different last",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
				{Index: 4, Offset: 30},
			},
			currentFirst: 1,
			currentLast:  3,
			expected: boundsResult{
				NewFirst: 1,
				NewLast:  4,
				Changed:  true,
				WasReset: false,
			},
		},
		{
			name: "Non-empty index map, both different",
			indexMap: []types.IndexOffsetPair{
				{Index: 2, Offset: 0},
				{Index: 3, Offset: 10},
				{Index: 4, Offset: 20},
				{Index: 5, Offset: 30},
			},
			currentFirst: 1,
			currentLast:  4,
			expected: boundsResult{
				NewFirst: 2,
				NewLast:  5,
				Changed:  true,
				WasReset: false,
			},
		},
		{
			name: "Non-empty index map, current first higher",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
			currentFirst: 2,
			currentLast:  2,
			expected: boundsResult{
				NewFirst: 1,
				NewLast:  2,
				Changed:  true,
				WasReset: false,
			},
		},
		{
			name: "Non-empty index map, current last lower",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			currentFirst: 1,
			currentLast:  2,
			expected: boundsResult{
				NewFirst: 1,
				NewLast:  3,
				Changed:  true,
				WasReset: false,
			},
		},
		{
			name:         "Non-empty index map, single entry, same bounds",
			indexMap:     []types.IndexOffsetPair{{Index: 5, Offset: 100}},
			currentFirst: 5,
			currentLast:  5,
			expected: boundsResult{
				NewFirst: 5,
				NewLast:  5,
				Changed:  false,
				WasReset: false,
			},
		},
		{
			name:         "Non-empty index map, single entry, different first",
			indexMap:     []types.IndexOffsetPair{{Index: 6, Offset: 100}},
			currentFirst: 5,
			currentLast:  5,
			expected: boundsResult{
				NewFirst: 6,
				NewLast:  6,
				Changed:  true,
				WasReset: false,
			},
		},
		{
			name:         "Non-empty index map, single entry, different last (should also change first)",
			indexMap:     []types.IndexOffsetPair{{Index: 7, Offset: 100}},
			currentFirst: 7,
			currentLast:  6,
			expected: boundsResult{
				NewFirst: 7,
				NewLast:  7,
				Changed:  true, // Corrected expectation
				WasReset: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is, _, _ := setupTestIndexService()
			result := is.GetBounds(tt.indexMap, tt.currentFirst, tt.currentLast)
			testutil.AssertEqual(t, tt.expected, result)
		})
	}
}

// TestIndexService_Append tests the Append method of the indexService
func TestIndexService_Append(t *testing.T) {
	tests := []struct {
		name      string
		base      []types.IndexOffsetPair
		additions []types.IndexOffsetPair
		expected  []types.IndexOffsetPair
	}{
		{
			name: "Append to empty base",
			base: []types.IndexOffsetPair{},
			additions: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
			expected: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
		},
		{
			name: "Append to non-empty base",
			base: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
			additions: []types.IndexOffsetPair{
				{Index: 3, Offset: 20},
				{Index: 4, Offset: 30},
			},
			expected: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
				{Index: 4, Offset: 30},
			},
		},
		{
			name: "Append empty additions",
			base: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
			additions: []types.IndexOffsetPair{},
			expected: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
		},
		{
			name:      "Both empty",
			base:      []types.IndexOffsetPair{},
			additions: []types.IndexOffsetPair{},
			expected:  []types.IndexOffsetPair{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is, _, _ := setupTestIndexService()
			result := is.Append(tt.base, tt.additions)
			testutil.AssertEqual(t, tt.expected, result)
		})
	}
}

// TestIndexService_TruncateLast tests the TruncateLast method of the indexService
func TestIndexService_TruncateLast(t *testing.T) {
	tests := []struct {
		name     string
		indexMap []types.IndexOffsetPair
		count    int
		expected []types.IndexOffsetPair
	}{
		{
			name: "Truncate zero entries",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			count: 0,
			expected: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
		},
		{
			name: "Truncate some entries",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
				{Index: 4, Offset: 30},
			},
			count: 2,
			expected: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
		},
		{
			name: "Truncate all entries",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
			count:    2,
			expected: []types.IndexOffsetPair{},
		},
		{
			name: "Truncate more than available",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
			count:    5,
			expected: []types.IndexOffsetPair{},
		},
		{
			name:     "Truncate empty map",
			indexMap: []types.IndexOffsetPair{},
			count:    1,
			expected: []types.IndexOffsetPair{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is, _, _ := setupTestIndexService()
			result := is.TruncateLast(tt.indexMap, tt.count)
			testutil.AssertEqual(t, tt.expected, result)
		})
	}
}

// TestIndexService_FindFirstIndexAtOrAfter tests the FindFirstIndexAtOrAfter method
func TestIndexService_FindFirstIndexAtOrAfter(t *testing.T) {
	tests := []struct {
		name        string
		indexMap    []types.IndexOffsetPair
		target      types.Index
		expectedPos int
	}{
		{
			name: "Find exact match at beginning",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			target:      1,
			expectedPos: 0,
		},
		{
			name: "Find exact match in middle",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			target:      2,
			expectedPos: 1,
		},
		{
			name: "Find exact match at end",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			target:      3,
			expectedPos: 2,
		},
		{
			name: "Find next higher when no exact match",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
			target:      7,
			expectedPos: 1, // Should return position of Index 10
		},
		{
			name: "Target is lower than all indices",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
			target:      1,
			expectedPos: 0, // Should return the first position
		},
		{
			name: "Target is higher than all indices",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
			target:      20,
			expectedPos: 3, // Should return len(indexMap)
		},
		{
			name:        "Empty index map",
			indexMap:    []types.IndexOffsetPair{},
			target:      5,
			expectedPos: 0, // Should return 0 for empty map
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is, _, _ := setupTestIndexService()
			result := is.FindFirstIndexAtOrAfter(tt.indexMap, tt.target)
			testutil.AssertEqual(t, tt.expectedPos, result)
		})
	}
}

// TestIndexService_TruncateAfter tests the TruncateAfter method
func TestIndexService_TruncateAfter(t *testing.T) {
	tests := []struct {
		name     string
		indexMap []types.IndexOffsetPair
		target   types.Index
		expected []types.IndexOffsetPair
	}{
		{
			name: "Truncate after exact match",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
				{Index: 4, Offset: 30},
			},
			target: 2,
			expected: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
		},
		{
			name: "Target is first index",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			target: 1,
			expected: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
			},
		},
		{
			name: "Target is last index",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			target: 3,
			expected: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
		},
		{
			name: "Target not found, truncate after closest below",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
			target: 7,
			expected: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
			},
		},
		{
			name: "Target lower than all indices",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
			target:   1,
			expected: []types.IndexOffsetPair{},
		},
		{
			name: "Target higher than all indices",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
			target: 20,
			expected: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
		},
		{
			name:     "Empty index map",
			indexMap: []types.IndexOffsetPair{},
			target:   5,
			expected: []types.IndexOffsetPair{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is, _, _ := setupTestIndexService()
			result := is.TruncateAfter(tt.indexMap, tt.target)
			testutil.AssertEqual(t, tt.expected, result)
		})
	}
}

// TestIndexService_TruncateBefore tests the TruncateBefore method
func TestIndexService_TruncateBefore(t *testing.T) {
	tests := []struct {
		name     string
		indexMap []types.IndexOffsetPair
		target   types.Index
		expected []types.IndexOffsetPair
	}{
		{
			name: "Truncate before exact match",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
				{Index: 4, Offset: 30},
			},
			target: 3,
			expected: []types.IndexOffsetPair{
				{Index: 3, Offset: 20},
				{Index: 4, Offset: 30},
			},
		},
		{
			name: "Target is first index",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			target: 1,
			expected: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
		},
		{
			name: "Target is last index",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			target: 3,
			expected: []types.IndexOffsetPair{
				{Index: 3, Offset: 20},
			},
		},
		{
			name: "Target not found, truncate before closest above",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
			target: 7,
			expected: []types.IndexOffsetPair{
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
		},
		{
			name: "Target lower than all indices",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
			target: 1,
			expected: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
		},
		{
			name: "Target higher than all indices",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
				{Index: 15, Offset: 20},
			},
			target:   20,
			expected: []types.IndexOffsetPair{},
		},
		{
			name:     "Empty index map",
			indexMap: []types.IndexOffsetPair{},
			target:   5,
			expected: []types.IndexOffsetPair{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is, _, _ := setupTestIndexService()
			result := is.TruncateBefore(tt.indexMap, tt.target)
			testutil.AssertEqual(t, tt.expected, result)
		})
	}
}

// TestIndexService_ReadInRange tests the ReadInRange method
func TestIndexService_ReadInRange(t *testing.T) {
	logPath := "test.log"

	tests := []struct {
		name              string
		setupMocks        func(*mockFileSystem, *mockLogEntryReader)
		indexMap          []types.IndexOffsetPair
		start             types.Index
		end               types.Index
		expected          []types.LogEntry
		expectedBytesRead int64
		expectError       bool
		errorType         error
	}{
		{
			name: "Read full range",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
				r.AddEntryWithOffset(
					types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")},
					10,
					0,
				)
				r.AddEntryWithOffset(
					types.LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")},
					15,
					10,
				)
				r.AddEntryWithOffset(
					types.LogEntry{Term: 2, Index: 3, Command: []byte("cmd3")},
					20,
					25,
				)
			},
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 25},
			},
			start: 1,
			end:   4,
			expected: []types.LogEntry{
				{Term: 1, Index: 1, Command: []byte("cmd1")},
				{Term: 1, Index: 2, Command: []byte("cmd2")},
				{Term: 2, Index: 3, Command: []byte("cmd3")},
			},
			expectedBytesRead: 45,
			expectError:       false,
		},
		{
			name: "Read partial range",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
				r.AddEntryWithOffset(
					types.LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")},
					15,
					10,
				)
			},
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 25},
			},
			start: 2,
			end:   3,
			expected: []types.LogEntry{
				{Term: 1, Index: 2, Command: []byte("cmd2")},
			},
			expectedBytesRead: 15,
			expectError:       false,
		},
		{
			name: "Empty range",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
			},
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 25},
			},
			start:             2,
			end:               2,
			expected:          nil,
			expectedBytesRead: 0,
			expectError:       true,
		},
		{
			name: "Start greater than end",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
			},
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 25},
			},
			start:             3,
			end:               2,
			expected:          []types.LogEntry{},
			expectedBytesRead: 0,
			expectError:       true,
			errorType:         ErrInvalidLogRange,
		},
		{
			name: "Empty index map",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
			},
			indexMap:          []types.IndexOffsetPair{},
			start:             1,
			end:               3,
			expected:          []types.LogEntry{},
			expectedBytesRead: 0,
			expectError:       true,
			errorType:         ErrIndexOutOfRange,
		},
		{
			name: "Range out of bounds",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
			},
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 25},
			},
			start:             4,
			end:               6,
			expected:          []types.LogEntry{},
			expectedBytesRead: 0,
			expectError:       true,
			errorType:         ErrIndexOutOfRange,
		},
		{
			name: "File open error",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.openErr = errors.New("file open error")
			},
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 25},
			},
			start:       1,
			end:         4,
			expected:    []types.LogEntry{},
			expectError: true,
			errorType:   ErrStorageIO,
		},
		{
			name: "Context cancelled",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				// Setup that will trigger context cancellation in ScanRange
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
				// ScanRange detects context cancellation
			},
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 25},
			},
			start:       1,
			end:         4,
			expected:    nil,
			expectError: true,
			errorType:   context.Canceled,
		},
		{
			name: "ReadAtOffset returns EOF with partial entry",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01}
				r.AddEOF(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}, 10, 0)
			},
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
			},
			start: 1,
			end:   2,
			expected: []types.LogEntry{
				{Term: 1, Index: 1, Command: []byte("cmd1")},
			},
			expectedBytesRead: 10,
			expectError:       false,
		},
		{
			name: "ReadAtOffset returns non-EOF error",
			setupMocks: func(fs *mockFileSystem, r *mockLogEntryReader) {
				fs.files[logPath] = []byte{0x01, 0x02, 0x03}
				r.AddEntryWithOffset(
					types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")},
					10,
					0,
				)
				r.AddEntryWithOffset(types.LogEntry{Term: 1, Index: 2, Command: nil}, 0, 10)
				r.readErrors[1] = errForcedReadFailure
			},
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
			},
			start:             1,
			end:               3,
			expected:          []types.LogEntry{{Term: 1, Index: 1, Command: []byte("cmd1")}},
			expectedBytesRead: 10,
			expectError:       true,
			errorType:         errForcedReadFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is, mfs, mr := setupTestIndexService()

			// Setup mocks
			tt.setupMocks(mfs, mr)

			// Create a context that might be cancelled
			ctx := context.Background()
			if tt.errorType == context.Canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(context.Background())
				cancel() // Cancel immediately to simulate cancellation
			}

			entries, bytesRead, err := is.ReadInRange(ctx, logPath, tt.indexMap, tt.start, tt.end)

			// Verify error behavior
			if tt.expectError {
				testutil.AssertError(t, err)
				if tt.errorType != nil {
					testutil.AssertErrorIs(t, err, tt.errorType)
				}
			} else {
				testutil.AssertNoError(t, err)
				testutil.AssertEqual(t, tt.expected, entries)
				if tt.expectedBytesRead > 0 {
					testutil.AssertEqual(t, tt.expectedBytesRead, bytesRead)
				}
			}
		})
	}
}

// TestIndexServiceEdgeCases tests edge cases not covered in other tests
func TestIndexServiceEdgeCases(t *testing.T) {
	t.Run("Build with reader returning invalid entries", func(t *testing.T) {
		is, mfs, mr := setupTestIndexService()
		logPath := "test.log"

		// Setup mocks: add entries with duplicate indices
		mfs.files[logPath] = []byte{0x01, 0x02, 0x03}
		mr.AddEntry(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}, 10)
		mr.AddEntry(
			types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1-duplicate")},
			15,
		) // Duplicate index

		result, err := is.Build(logPath)
		testutil.AssertNoError(t, err) // Should not error, but truncate
		testutil.AssertEqual(t, 1, len(result.IndexMap))
		testutil.AssertTrue(t, result.Truncated)
		testutil.AssertEqual(t, types.Index(1), result.LastValidIndex)
	})

	t.Run("ReadInRange with zero entries between indices", func(t *testing.T) {
		is, mfs, mr := setupTestIndexService()
		logPath := "test.log"

		// Setup an index map with a gap (missing entry for index 2)
		indexMap := []types.IndexOffsetPair{
			{Index: 1, Offset: 0},
			{Index: 3, Offset: 20}, // Note: Index 2 is missing
		}

		// Setup the file and reader
		mfs.files[logPath] = []byte{0x01, 0x02}
		mr.AddEntryWithOffset(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}, 10, 0)
		mr.AddEntryWithOffset(types.LogEntry{Term: 2, Index: 3, Command: []byte("cmd3")}, 15, 20)

		// Try to read a range that includes the missing index
		entries, _, err := is.ReadInRange(context.Background(), logPath, indexMap, 1, 4)

		// This should not error, but should skip the missing index
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 2, len(entries))
		testutil.AssertEqual(t, types.Index(1), entries[0].Index)
		testutil.AssertEqual(t, types.Index(3), entries[1].Index)
	})

	t.Run("VerifyConsistency with extremely large indices", func(t *testing.T) {
		is, _, _ := setupTestIndexService()

		// Create an index map with extremely large, but valid indices
		indexMap := []types.IndexOffsetPair{
			{Index: 1000000, Offset: 0},
			{Index: 1000001, Offset: 10},
			{Index: 1000002, Offset: 20},
		}

		err := is.VerifyConsistency(indexMap)
		testutil.AssertNoError(t, err)
	})
}

func TestEstimateCapacity(t *testing.T) {
	tests := []struct {
		name     string
		indexMap []types.IndexOffsetPair
		start    types.Index
		end      types.Index
		wantCap  int
	}{
		{
			name: "Normal increasing range",
			indexMap: []types.IndexOffsetPair{
				{Index: 1, Offset: 0},
				{Index: 2, Offset: 10},
				{Index: 3, Offset: 20},
			},
			start:   2,
			end:     4,
			wantCap: 2, // entries with index 2, 3
		},
		{
			name: "endIdx < startIdx (capacity < 0)",
			indexMap: []types.IndexOffsetPair{
				{Index: 5, Offset: 0},
				{Index: 10, Offset: 10},
			},
			start:   11, // startIdx = 2 (not found)
			end:     7,  // endIdx = 1 (points to 10)
			wantCap: 0,  // because capacity = -1 → clamp to 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateCapacity(tt.indexMap, tt.start, tt.end)
			if got != tt.wantCap {
				t.Errorf("estimateCapacity() = %d, want %d", got, tt.wantCap)
			}
		})
	}
}
