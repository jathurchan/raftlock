package storage

import (
	"errors"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

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
				r.AddEntry(types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd3")}, 20) // Out of order
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
				r.AddEntry(types.LogEntry{Term: 1, Index: 4, Command: []byte("cmd3")}, 20) // Gap (should be 3)
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
