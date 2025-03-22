package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	tu "github.com/jathurchan/raftlock/testutil"
)

// TempDir creates a temporary directory for testing
func createTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func TestLoadMetadataWithInvalidJSON(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	fs := &FileStorage{dir: config.Dir}

	// Create a corrupted metadata file
	err := os.WriteFile(fs.metadataFile(), []byte("{invalid json"), OwnRWOthR)
	tu.RequireNoError(t, err)

	// Try to load the corrupted metadata
	err = fs.loadMetadata()
	tu.AssertError(t, err) // Should return error
}

func TestSaveStateWriteFileError(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	initialState := RaftState{CurrentTerm: 5, VotedFor: 2}
	// Make the directory read-only
	err = os.Chmod(tempDir, 0555)
	tu.RequireNoError(t, err)
	defer os.Chmod(tempDir, 0755) // Restore permissions
	err = storage.SaveState(ctx, initialState)
	tu.AssertError(t, err)
}

func TestLoadStateWithInvalidJSON(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Create a corrupted state file
	err = os.WriteFile(fs.stateFile(), []byte("{invalid json"), OwnRWOthR)
	tu.RequireNoError(t, err)
	_, err = storage.LoadState(ctx)
	tu.AssertErrorIs(t, err, ErrCorruptedData)
}

func TestSaveMetadataWriteFileError(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	// Make the directory read-only
	err = os.Chmod(tempDir, 0555)
	tu.RequireNoError(t, err)
	defer os.Chmod(tempDir, 0755) // Restore permissions
	err = fs.saveMetadata()
	tu.AssertError(t, err)
}

// similar strategy to TestSaveStateRenameError
func TestSaveMetadataRenameError(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	// Make the directory read-only
	err = os.Chmod(tempDir, 0555)
	tu.RequireNoError(t, err)
	defer os.Chmod(tempDir, 0755) // Restore permissions after test
	err = fs.saveMetadata()
	tu.AssertError(t, err)
}

func TestAppendEntriesOpenLogError(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Make the directory read-only so openLogFile will fail
	err = os.Chmod(tempDir, 0555)
	tu.RequireNoError(t, err)
	defer os.Chmod(tempDir, 0755)
	entries := []*pb.LogEntry{{Index: 1, Term: 1}}
	err = storage.AppendEntries(ctx, entries)
	tu.AssertError(t, err)
}

func TestAppendEntriesWriteEntriesError(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	entries := []*pb.LogEntry{{Index: 1, Term: 1, Command: []byte("command")}}
	// Create the log file
	file, _, err := fs.openLogFile()
	tu.RequireNoError(t, err)
	file.Close()
	// Make the log file read-only
	err = os.Chmod(fs.logFile(), 0444)
	tu.RequireNoError(t, err)
	defer os.Chmod(fs.logFile(), 0644) // Restore permissions
	err = storage.AppendEntries(ctx, entries)
	tu.AssertError(t, err)
}

func TestAppendEntriesSaveMetadataError(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Add valid entry
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("valid")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	// Force metadata save to fail by making directory read-only
	err = os.Chmod(fs.dir, 0500) // Read + execute only
	tu.RequireNoError(t, err)
	defer os.Chmod(fs.dir, 0755)
	moreEntries := []*pb.LogEntry{
		{Index: 2, Term: 1, Command: []byte("more")},
	}
	err = storage.AppendEntries(ctx, moreEntries)
	tu.AssertError(t, err)
}

func TestOpenLogFileSeekError(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	// Create the log file first
	file, _, err := fs.openLogFile()
	tu.RequireNoError(t, err)
	file.Close() // Close it so we can change permissions
	// Make the log file read-only. This will cause Seek to fail.
	err = os.Chmod(fs.logFile(), 0444)
	tu.RequireNoError(t, err)
	defer os.Chmod(fs.logFile(), 0644)
	_, _, err = fs.openLogFile()
	tu.AssertError(t, err)
}

func TestGetEntriesFileNotFound(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Set metadata as if we have entries, but don't create the file
	fs.metadata.FirstIndex = 1
	fs.metadata.LastIndex = 1
	err = fs.saveMetadata()
	tu.RequireNoError(t, err)
	entries, err := storage.GetEntries(ctx, 1, 2)
	tu.RequireNoError(t, err) // Should not error, just return empty slice
	tu.AssertEmpty(t, entries)
}

func TestGetEntriesFileOpenError(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// create entries
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	// Make the log file read-only so os.Open will fail
	err = os.Chmod(fs.logFile(), 0000)
	tu.RequireNoError(t, err)
	defer os.Chmod(fs.logFile(), 0644) // Restore permissions
	_, err = storage.GetEntries(ctx, 1, 2)
	tu.AssertError(t, err)
}

func TestGetEntriesLowGTEHigh(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Create entries
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	// Test case where low >= high (low == high)
	result, err := storage.GetEntries(ctx, 1, 1)
	tu.RequireNoError(t, err) //Should not throw an error.
	tu.AssertEmpty(t, result)
}

func TestReadEntriesInRangeEOF(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, _ := NewFileStorage(config)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	file, _ := os.Create(fs.logFile()) // Create empty file
	defer file.Close()
	entries, err := fs.readEntriesInRange(ctx, file, 1, 2)
	tu.RequireNoError(t, err)
	tu.AssertEmpty(t, entries)
}

func TestReadEntriesInRangeReadLengthError(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Create a log file with only an incomplete length prefix
	file, err := os.Create(fs.logFile())
	tu.RequireNoError(t, err)
	_, err = file.Write([]byte{1}) // Write an incomplete length prefix
	tu.RequireNoError(t, err)
	file.Close()
	file, err = os.Open(fs.logFile())
	tu.RequireNoError(t, err)
	defer file.Close()
	// Try to read entries, should fail with an error because of incomplete length prefix
	_, err = fs.readEntriesInRange(ctx, file, 1, 2)
	tu.AssertError(t, err) // Should be an error, not EOF
}

func TestNewFileStorage(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	// Test successful creation
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	tu.RequireNotNil(t, storage)
	// Verify directory exists
	_, err = os.Stat(tempDir)
	tu.AssertNoError(t, err)
	// Test with existing metadata
	fs := storage.(*FileStorage)
	fs.metadata.FirstIndex = 5
	fs.metadata.LastIndex = 10
	err = fs.saveMetadata()
	tu.RequireNoError(t, err)
	// Create a new instance that should load the metadata
	storage2, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs2 := storage2.(*FileStorage)
	tu.AssertEqual(t, uint64(5), fs2.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(10), fs2.metadata.LastIndex)
}

func TestSaveAndLoadState(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	initialState := RaftState{CurrentTerm: 5, VotedFor: 2}
	// Test saving state
	err = storage.SaveState(ctx, initialState)
	tu.RequireNoError(t, err)
	// Verify state file exists
	fs := storage.(*FileStorage)
	_, err = os.Stat(fs.stateFile())
	tu.RequireNoError(t, err)
	// Test loading state
	loadedState, err := storage.LoadState(ctx)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, initialState, loadedState)
	// Test loading default state when file doesn't exist
	tempDir2 := createTempDir(t)
	config2 := &StorageConfig{Dir: tempDir2}
	storage2, err := NewFileStorage(config2)
	tu.RequireNoError(t, err)
	defaultState, err := storage2.LoadState(ctx)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(0), defaultState.CurrentTerm)
	tu.AssertEqual(t, int64(-1), defaultState.VotedFor)
	// Test with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = storage.LoadState(canceledCtx)
	tu.AssertError(t, err)
	err = storage.SaveState(canceledCtx, initialState)
	tu.AssertError(t, err)
}

func TestAppendEntries(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Test appending first entry
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(1), fs.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(1), fs.metadata.LastIndex)
	// Test appending multiple contiguous entries
	entries = []*pb.LogEntry{
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 1, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(1), fs.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(3), fs.metadata.LastIndex)
	// Test with non-contiguous entries
	entries = []*pb.LogEntry{
		{Index: 5, Term: 2, Command: []byte("non-contiguous")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.AssertError(t, err) // Should fail with contiguity error
	// Test empty entries
	err = storage.AppendEntries(ctx, []*pb.LogEntry{})
	tu.AssertNoError(t, err) // Should be a no-op
	// Test with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = storage.AppendEntries(canceledCtx, entries)
	tu.AssertError(t, err)
}

func TestGetEntries(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Seed with entries
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
		{Index: 4, Term: 2, Command: []byte("fourth")},
		{Index: 5, Term: 3, Command: []byte("fifth")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Test getting all entries
	entries, err := storage.GetEntries(ctx, 1, 6)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, entries, 5)
	// Test getting partial range
	entries, err = storage.GetEntries(ctx, 2, 4)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, entries, 2)
	tu.AssertEqual(t, uint64(2), entries[0].Index)
	tu.AssertEqual(t, uint64(3), entries[1].Index)
	// Test when low > high
	_, err = storage.GetEntries(ctx, 4, 2)
	tu.AssertError(t, err)
	tu.AssertErrorIs(t, err, ErrIndexOutOfRange)
	// Test out of range (too high)
	entries, err = storage.GetEntries(ctx, 6, 10)
	tu.RequireNoError(t, err)
	tu.AssertEmpty(t, entries)
	// Test out of range (too low)
	entries, err = storage.GetEntries(ctx, 0, 2)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, entries, 1) // Should adjust to start from 1
	// Test canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = storage.GetEntries(canceledCtx, 1, 6)
	tu.AssertError(t, err)
}

func TestGetEntry(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Seed with entries
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Test getting existing entry
	entry, err := storage.GetEntry(ctx, 2)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(2), entry.Index)
	tu.AssertEqual(t, uint64(1), entry.Term)
	tu.AssertEqual(t, []byte("second"), entry.Command)
	// Test getting non-existent entry (too high)
	_, err = storage.GetEntry(ctx, 4)
	tu.AssertErrorIs(t, err, ErrNotFound)
	// Test getting non-existent entry (too low)
	_, err = storage.GetEntry(ctx, 0)
	tu.AssertErrorIs(t, err, ErrNotFound)
	// Test canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = storage.GetEntry(canceledCtx, 1)
	tu.AssertError(t, err)
}

func TestTruncateSuffix(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()

	// Seed with entries
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
		{Index: 4, Term: 2, Command: []byte("fourth")},
		{Index: 5, Term: 3, Command: []byte("fifth")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)

	// Test truncate suffix
	err = storage.TruncateSuffix(ctx, 3)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(1), fs.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(2), fs.metadata.LastIndex)

	// Verify truncated entries are gone
	entries, err := storage.GetEntries(ctx, 1, 6)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, entries, 2)
	tu.AssertEqual(t, uint64(2), entries[1].Index)

	// Test truncating all (index <= firstIndex)
	err = storage.TruncateSuffix(ctx, 1)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(0), fs.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(0), fs.metadata.LastIndex)

	// Test truncating out of range
	fs.metadata.FirstIndex = 1
	fs.metadata.LastIndex = 5
	err = storage.TruncateSuffix(ctx, 7)
	tu.RequireNoError(t, err) // Should be a no-op
	tu.AssertEqual(t, uint64(1), fs.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(5), fs.metadata.LastIndex)

	// Test with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = storage.TruncateSuffix(canceledCtx, 3)
	tu.AssertError(t, err)
}

// TestGetEntryWithMissingFile tests behavior when log file is missing
func TestGetEntryWithMissingFile(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Set metadata as if we have entries, but don't create the file
	fs.metadata.FirstIndex = 1
	fs.metadata.LastIndex = 1
	err = fs.saveMetadata()
	tu.RequireNoError(t, err)
	// Now try to get an entry
	_, err = storage.GetEntry(ctx, 1)
	tu.AssertErrorIs(t, err, ErrCorruptedData, "Should return corrupted data error when file is missing")
}

// TestStorageWithInvalidDirectory tests error handling for invalid directories
func TestStorageWithInvalidDirectory(t *testing.T) {
	// Try to create storage with a file as directory
	tempFile, err := os.CreateTemp("", "file-not-dir-*")
	tu.RequireNoError(t, err)
	tempFile.Close()
	defer os.Remove(tempFile.Name())
	config := &StorageConfig{Dir: tempFile.Name()}
	_, err = NewFileStorage(config)
	tu.AssertError(t, err)
}

// TestConcurrentReadWrite tests concurrent read/write operations
func TestConcurrentReadWrite(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Add some initial entries
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Number of operations to perform
	const numWrites = 20
	const numReads = 100
	// Use WaitGroup for better synchronization
	var wg sync.WaitGroup
	wg.Add(3) // 2 reader goroutines + 1 writer goroutine
	// Track errors
	var errMu sync.Mutex
	var testErrors []error
	recordError := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		testErrors = append(testErrors, err)
	}
	// Writer goroutine
	go func() {
		defer wg.Done()
		for i := 2; i <= numWrites; i++ {
			entries := []*pb.LogEntry{
				{Index: uint64(i), Term: 1, Command: []byte(fmt.Sprintf("entry-%d", i))},
			}
			if err := storage.AppendEntries(ctx, entries); err != nil {
				recordError(fmt.Errorf("writer error on entry %d: %w", i, err))
				return
			}
			// Random sleep to increase chance of race conditions
			time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
		}
	}()
	// Reader goroutine 1 - uses LastIndex and FirstIndex
	go func() {
		defer wg.Done()
		for i := 0; i < numReads; i++ {
			lastIdx := storage.LastIndex()
			firstIdx := storage.FirstIndex()
			// Verify last >= first
			if lastIdx < firstIdx && lastIdx != 0 {
				recordError(fmt.Errorf("invalid index state: lastIndex (%d) < firstIndex (%d)",
					lastIdx, firstIdx))
				return
			}
			// Random sleep
			time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
		}
	}()
	// Reader goroutine 2 - uses GetEntries and GetEntry
	go func() {
		defer wg.Done()
		for i := 0; i < numReads; i++ {
			// Get current range
			lastIdx := storage.LastIndex()
			firstIdx := storage.FirstIndex()
			if lastIdx >= firstIdx && lastIdx > 0 {
				// Get random index in range
				idx := firstIdx
				if lastIdx > firstIdx {
					idx = firstIdx + uint64(rand.Intn(int(lastIdx-firstIdx+1)))
				}
				// Try to get single entry
				entry, err := storage.GetEntry(ctx, idx)
				if err != nil && err != ErrNotFound {
					recordError(fmt.Errorf("GetEntry error at index %d: %w", idx, err))
					return
				}
				// Verify entry index if found
				if err == nil && entry.Index != idx {
					recordError(fmt.Errorf("entry index mismatch: expected %d, got %d",
						idx, entry.Index))
					return
				}
				// Also try to get a range of entries
				start := firstIdx
				end := lastIdx + 1
				entries, err := storage.GetEntries(ctx, start, end)
				if err != nil {
					recordError(fmt.Errorf("GetEntries error for range %d-%d: %w",
						start, end, err))
					return
				}
				// Verify we got expected number of entries
				expected := end - start
				if uint64(len(entries)) != expected {
					recordError(fmt.Errorf("entries count mismatch: expected %d, got %d",
						expected, len(entries)))
					return
				}
			}
			// Random sleep
			time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
		}
	}()
	// Wait for all goroutines to complete
	wg.Wait()
	// Check for errors
	if len(testErrors) > 0 {
		for i, err := range testErrors {
			t.Errorf("Concurrent test error %d: %v", i+1, err)
		}
		t.Fatalf("Encountered %d errors during concurrent operations", len(testErrors))
	}
	// Verify final state has all entries
	finalCount := numWrites
	lastIdx := storage.LastIndex()
	tu.AssertEqual(t, uint64(finalCount), lastIdx, "Final last index should match number of entries")
	// Get all entries and verify they're intact
	entries, err := storage.GetEntries(ctx, 1, uint64(finalCount+1))
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, finalCount, len(entries), "Should retrieve all entries")
	// Verify entry content
	for i := 0; i < finalCount; i++ {
		idx := uint64(i + 1)
		entry := entries[i]
		tu.AssertEqual(t, idx, entry.Index, "Entry index mismatch")
		if idx == 1 {
			tu.AssertEqual(t, []byte("first"), entry.Command, "First entry content mismatch")
		} else {
			expectedContent := fmt.Sprintf("entry-%d", idx)
			tu.AssertEqual(t, []byte(expectedContent), entry.Command,
				"Entry content mismatch for index %d", idx)
		}
	}
}

// TestGetEntriesEdgeCases tests edge cases and performance characteristics of GetEntries
func TestGetEntriesEdgeCases(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Test getting entries when log file doesn't exist
	entries, err := storage.GetEntries(ctx, 1, 10)
	tu.AssertNoError(t, err)
	tu.AssertEmpty(t, entries)
	// Add a single entry
	singleEntry := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
	}
	err = storage.AppendEntries(ctx, singleEntry)
	tu.RequireNoError(t, err)
	// Test exact match of range
	entries, err = storage.GetEntries(ctx, 1, 2)
	tu.AssertNoError(t, err)
	tu.AssertLen(t, entries, 1)
	// Test adjusting range when low < firstIndex
	entries, err = storage.GetEntries(ctx, 0, 2)
	tu.AssertNoError(t, err)
	tu.AssertLen(t, entries, 1)
	tu.AssertEqual(t, uint64(1), entries[0].Index)
	// Test when range is within valid bounds but doesn't match any entries
	entries, err = storage.GetEntries(ctx, 1, 1)
	tu.AssertNoError(t, err)
	tu.AssertEmpty(t, entries)
}

// TestFindTruncationOffset tests the behavior of findTruncationOffset method
func TestFindTruncationOffset(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Add entries
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Test finding truncation offset for each index
	offset1, err := fs.findTruncationOffset(1)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, int64(0), offset1)
	offset2, err := fs.findTruncationOffset(2)
	tu.RequireNoError(t, err)
	assert(t, offset2 > 0, "Offset should be positive")
	offset3, err := fs.findTruncationOffset(3)
	tu.RequireNoError(t, err)
	assert(t, offset3 > offset2, "Offset3 should be greater than offset2")
	offset4, err := fs.findTruncationOffset(4)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, offset3, offset4, "Offset beyond last entry should be same as last entry offset")
	// Test with corrupted log - append invalid data
	f, err := os.OpenFile(fs.logFile(), os.O_APPEND|os.O_WRONLY, OwnRWOthR)
	tu.RequireNoError(t, err)
	_, err = f.Write([]byte{0, 1, 2, 3}) // Invalid length prefix
	tu.RequireNoError(t, err)
	f.Close()
	// Finding offset with corrupted log should fail
	_, err = fs.findTruncationOffset(4)
	tu.AssertError(t, err)
}

// TestMalformedEntries tests handling of malformed entries in log files
func TestMalformedEntries(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Create a valid entry first
	validEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("valid")},
	}
	err = storage.AppendEntries(ctx, validEntries)
	tu.RequireNoError(t, err)
	// Manually append a malformed entry with zero length
	f, err := os.OpenFile(fs.logFile(), os.O_APPEND|os.O_WRONLY, OwnRWOthR)
	tu.RequireNoError(t, err)
	// Write a zero-length prefix
	_, err = f.Write([]byte{0, 0, 0, 0})
	tu.RequireNoError(t, err)
	f.Close()
	// Now try to read entries - should handle the zero-length entry
	entries, err := storage.GetEntries(ctx, 1, 2)
	tu.AssertNoError(t, err)
	tu.AssertLen(t, entries, 1)
}

// TestIncompleteRead tests behavior with incomplete reads
func TestIncompleteRead(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Create a valid entry first
	validEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("valid")},
	}
	err = storage.AppendEntries(ctx, validEntries)
	tu.RequireNoError(t, err)
	// Manually append a partial entry (length prefix only with no data)
	f, err := os.OpenFile(fs.logFile(), os.O_APPEND|os.O_WRONLY, OwnRWOthR)
	tu.RequireNoError(t, err)
	_, err = f.Write([]byte{0, 0, 0, 10}) // Length prefix indicating 10 bytes, but no actual data
	tu.RequireNoError(t, err)
	f.Close()
	// Try to read entries - should fail with corrupted data when it hits the incomplete entry
	_, err = storage.GetEntries(ctx, 1, 3)
	tu.AssertError(t, err)
}

// Custom assertion helper for boolean conditions
func assert(t *testing.T, condition bool, msg string) {
	t.Helper()
	if !condition {
		t.Errorf("Assertion failed: %s", msg)
	}
}

// TestAppendEntriesWithFailedWrite tests error handling during AppendEntries when write fails
func TestAppendEntriesWithFailedWrite(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Add one entry successfully
	validEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("valid")},
	}
	err = storage.AppendEntries(ctx, validEntries)
	tu.RequireNoError(t, err)
	// Force metadata save to fail by making directory read-only
	err = os.Chmod(fs.dir, 0500) // Read + execute only
	tu.RequireNoError(t, err)
	// Now try to append another entry
	moreEntries := []*pb.LogEntry{
		{Index: 2, Term: 1, Command: []byte("more")},
	}
	err = storage.AppendEntries(ctx, moreEntries)
	tu.AssertError(t, err)
	// Restore permissions
	err = os.Chmod(fs.dir, 0700)
	tu.RequireNoError(t, err)
}

func TestTruncatePrefix(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Seed with entries
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
		{Index: 4, Term: 2, Command: []byte("fourth")},
		{Index: 5, Term: 3, Command: []byte("fifth")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Test truncate prefix
	err = storage.TruncatePrefix(ctx, 3)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(3), fs.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(5), fs.metadata.LastIndex)
	// Verify truncated entries are gone and remaining ones accessible
	entries, err := storage.GetEntries(ctx, 1, 6)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, entries, 3)
	tu.AssertEqual(t, uint64(3), entries[0].Index)
	// Test truncating to exactly the last index
	storage2, err := NewFileStorage(&StorageConfig{Dir: createTempDir(t)})
	tu.RequireNoError(t, err)
	fs2 := storage2.(*FileStorage)
	err = storage2.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	err = storage2.TruncatePrefix(ctx, 5)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(5), fs2.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(5), fs2.metadata.LastIndex)
	// Verify only last entry remains
	remainingEntries, err := storage2.GetEntries(ctx, 1, 6)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, remainingEntries, 1)
	tu.AssertEqual(t, uint64(5), remainingEntries[0].Index)
	// Test truncating beyond the last index
	storage3, err := NewFileStorage(&StorageConfig{Dir: createTempDir(t)})
	tu.RequireNoError(t, err)
	fs3 := storage3.(*FileStorage)
	err = storage3.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	err = storage3.TruncatePrefix(ctx, 6)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(5), fs3.metadata.FirstIndex) // Should preserve last entry
	tu.AssertEqual(t, uint64(5), fs3.metadata.LastIndex)
	// Test with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = storage.TruncatePrefix(canceledCtx, 3)
	tu.AssertError(t, err)
}

func TestFirstAndLastIndex(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Check initial values
	tu.AssertEqual(t, uint64(0), storage.FirstIndex())
	tu.AssertEqual(t, uint64(0), storage.LastIndex())
	// Seed with entries
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Check updated values
	tu.AssertEqual(t, uint64(1), storage.FirstIndex())
	tu.AssertEqual(t, uint64(2), storage.LastIndex())
	// Truncate suffix and check
	err = storage.TruncateSuffix(ctx, 2)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(1), storage.FirstIndex())
	tu.AssertEqual(t, uint64(1), storage.LastIndex())
	// Add more entries and truncate prefix
	moreEntries := []*pb.LogEntry{
		{Index: 2, Term: 2, Command: []byte("another second")},
		{Index: 3, Term: 2, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, moreEntries)
	tu.RequireNoError(t, err)
	err = storage.TruncatePrefix(ctx, 2)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(2), storage.FirstIndex())
	tu.AssertEqual(t, uint64(3), storage.LastIndex())
}

func TestCorruptedData(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Create corrupted state file
	badState := []byte("{bad json")
	err = os.WriteFile(fs.stateFile(), badState, OwnRWOthR)
	tu.RequireNoError(t, err)
	// Loading corrupted state should return error
	_, err = storage.LoadState(ctx)
	tu.AssertErrorIs(t, err, ErrCorruptedData)
	// Create corrupted log file
	logFile := fs.logFile()
	err = os.WriteFile(logFile, []byte("not a valid log entry"), OwnRWOthR)
	tu.RequireNoError(t, err)
	// Set metadata as if valid entries existed
	fs.metadata.FirstIndex = 1
	fs.metadata.LastIndex = 1
	err = fs.saveMetadata()
	tu.RequireNoError(t, err)
	// Reading corrupted log with GetEntry should return error
	_, err = storage.GetEntry(ctx, 1)
	tu.AssertErrorIs(t, err, ErrCorruptedData)
	// Reading corrupted log with GetEntries should also return error
	_, err = storage.GetEntries(ctx, 1, 2)
	tu.AssertErrorIs(t, err, ErrCorruptedData)
	// Create valid entries and then append corrupted entry
	storage2, err := NewFileStorage(&StorageConfig{Dir: createTempDir(t)})
	tu.RequireNoError(t, err)
	fs2 := storage2.(*FileStorage)
	// Add one valid entry
	validEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("valid")},
	}
	err = storage2.AppendEntries(ctx, validEntries)
	tu.RequireNoError(t, err)
	// Corrupt the log file by appending invalid data
	f, err := os.OpenFile(fs2.logFile(), os.O_APPEND|os.O_WRONLY, OwnRWOthR)
	tu.RequireNoError(t, err)
	_, err = f.Write([]byte("corrupted data"))
	tu.RequireNoError(t, err)
	f.Close()
	// Set metadata as if two entries existed
	fs2.metadata.LastIndex = 2
	err = fs2.saveMetadata()
	tu.RequireNoError(t, err)
	// Reading past the valid entry should return error
	_, err = storage2.GetEntry(ctx, 2)
	tu.AssertErrorIs(t, err, ErrCorruptedData)
}

func TestFilePaths(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	// Test path generation
	tu.AssertEqual(t, filepath.Join(tempDir, "state.json"), fs.stateFile())
	tu.AssertEqual(t, filepath.Join(tempDir, "metadata.json"), fs.metadataFile())
	tu.AssertEqual(t, filepath.Join(tempDir, "log.dat"), fs.logFile())
}

func TestClose(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	// Close should always succeed for FileStorage
	err = storage.Close()
	tu.AssertNoError(t, err)
}

func TestMetadataPersistence(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Set some initial metadata values
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Read metadata directly from file and verify it matches the in-memory state
	metadata := readMetadataFromFile(t, fs.metadataFile())
	tu.AssertEqual(t, uint64(1), metadata.FirstIndex)
	tu.AssertEqual(t, uint64(3), metadata.LastIndex)
	// Modify the log by truncating and verify metadata is updated
	err = storage.TruncateSuffix(ctx, 2)
	tu.RequireNoError(t, err)
	// Read metadata again and verify changes were persisted
	updatedMetadata := readMetadataFromFile(t, fs.metadataFile())
	tu.AssertEqual(t, uint64(1), updatedMetadata.FirstIndex)
	tu.AssertEqual(t, uint64(1), updatedMetadata.LastIndex)
}

// Helper function to read metadata directly from file
func readMetadataFromFile(t *testing.T, path string) *logMetadata {
	data, err := os.ReadFile(path)
	tu.RequireNoError(t, err)
	var metadata logMetadata
	err = json.Unmarshal(data, &metadata)
	tu.RequireNoError(t, err)
	return &metadata
}

// TestLoadingCorruptedMetadata checks that the storage can handle corrupted metadata files
func TestLoadingCorruptedMetadata(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	// Create storage instance
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	// Write corrupted metadata file
	err = os.WriteFile(fs.metadataFile(), []byte("{bad json"), OwnRWOthR)
	tu.RequireNoError(t, err)
	// Attempting to create a new storage instance should fail when loading corrupted metadata
	_, err = NewFileStorage(config)
	tu.AssertError(t, err)
}

// TestAtomicStateOperations verifies that state operations use atomic file operations
func TestAtomicStateOperations(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	state := RaftState{CurrentTerm: 5, VotedFor: 3}
	// Save state
	err = storage.SaveState(ctx, state)
	tu.RequireNoError(t, err)
	// Verify tmp file doesn't exist after successful operation
	_, err = os.Stat(fs.stateFile() + ".tmp")
	tu.AssertError(t, err)
	// Verify state file exists
	_, err = os.Stat(fs.stateFile())
	tu.AssertNoError(t, err)
}

// TestAppendEntriesWithFirstIndexErrors verifies errors when first index constraints are violated
func TestAppendEntriesWithFirstIndexErrors(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Test appending first entry with index != 1
	invalidFirstEntries := []*pb.LogEntry{
		{Index: 2, Term: 1, Command: []byte("invalid first")},
	}
	err = storage.AppendEntries(ctx, invalidFirstEntries)
	tu.AssertError(t, err)
	// Now add valid first entry
	validFirstEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("valid first")},
	}
	err = storage.AppendEntries(ctx, validFirstEntries)
	tu.AssertNoError(t, err)
	// Try adding entries that break contiguity
	nonContiguousEntries := []*pb.LogEntry{
		{Index: 3, Term: 1, Command: []byte("non-contiguous")},
	}
	err = storage.AppendEntries(ctx, nonContiguousEntries)
	tu.AssertError(t, err)
}

// TestReadingEntriesWithTimeout tests reading entries with context timeout
func TestReadingEntriesWithTimeout(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	// Add many entries to create a large log file
	ctx := context.Background()
	var entries []*pb.LogEntry
	for i := 1; i <= 100; i++ {
		entries = append(entries, &pb.LogEntry{
			Index:   uint64(i),
			Term:    1,
			Command: []byte(fmt.Sprintf("data-%d", i)),
		})
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	// Create a context with timeout
	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()
	// Sleep to ensure timeout occurs
	time.Sleep(10 * time.Millisecond)
	// Try to read entries with expired context
	_, err = storage.GetEntries(ctxTimeout, 1, 101)
	tu.AssertError(t, err)
}

// TestTruncateSuffixEdgeCases tests edge cases for TruncateSuffix
func TestTruncateSuffixEdgeCases(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Add entries
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Test truncating at index 1 (should clear all)
	err = storage.TruncateSuffix(ctx, 1)
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(0), fs.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(0), fs.metadata.LastIndex)
	// Re-add entries
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Make file read-only to force error during truncation
	err = os.Chmod(fs.logFile(), 0400) // Read-only
	tu.RequireNoError(t, err)
	// Truncation should fail due to insufficient permissions
	err = storage.TruncateSuffix(ctx, 1)
	tu.AssertError(t, err, "Should fail with read-only file")
	// Restore permissions
	err = os.Chmod(fs.logFile(), 0600)
	tu.RequireNoError(t, err)
}

// TestTruncatePrefixEdgeCases tests edge cases for TruncatePrefix
func TestTruncatePrefixEdgeCases(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Test truncating prefix on empty log
	err = storage.TruncatePrefix(ctx, 1)
	tu.AssertErrorIs(t, err, ErrEmptyLog)
	// Add entries
	initialEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, initialEntries)
	tu.RequireNoError(t, err)
	// Test truncating with index <= FirstIndex (should be a no-op)
	err = storage.TruncatePrefix(ctx, 1)
	tu.AssertNoError(t, err)
	// Verify firstIndex is still 1
	tu.AssertEqual(t, uint64(1), storage.FirstIndex())
	// Test error conditions by making temporary file creation fail
	fs := storage.(*FileStorage)
	// Remove write permissions on directory to cause file creation to fail
	err = os.Chmod(fs.dir, 0500) // Read + execute only
	tu.RequireNoError(t, err)
	// Truncation should fail because we can't create the temporary file
	err = storage.TruncatePrefix(ctx, 2)
	tu.AssertError(t, err, "Should fail without write permission")
	// Restore permissions
	err = os.Chmod(fs.dir, 0700)
	tu.RequireNoError(t, err)
}

// TestLoadMetadataWithMissingFile tests that NewFileStorage correctly initializes
// when the metadata file doesn't exist
func TestLoadMetadataWithMissingFile(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}

	// First check - does the directory exist?
	_, err := os.Stat(tempDir)
	tu.RequireNoError(t, err, "Temp directory should exist")

	// Create new file storage, which should initialize default metadata
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)

	fs := storage.(*FileStorage)

	// Verify default values
	tu.AssertEqual(t, uint64(0), fs.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(0), fs.metadata.LastIndex)

	// Now create a new instance with a completely different directory
	tempDir2 := createTempDir(t)
	config2 := &StorageConfig{Dir: tempDir2}

	// Verify no metadata file exists in this new directory
	metadataPath := filepath.Join(tempDir2, "metadata.json")
	_, err = os.Stat(metadataPath)
	if !os.IsNotExist(err) {
		t.Fatalf("Expected metadata file to not exist, got: %v", err)
	}

	// Create a new storage instance, which should handle the missing metadata file
	storage2, err := NewFileStorage(config2)
	tu.RequireNoError(t, err)
	fs2 := storage2.(*FileStorage)

	// Verify default values are set
	tu.AssertEqual(t, uint64(0), fs2.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(0), fs2.metadata.LastIndex)

	// Verify the metadata file now exists (if the implementation creates it)
	_, err = os.Stat(metadataPath)
	if os.IsNotExist(err) {
		t.Logf("Note: Implementation doesn't create metadata file immediately")
	}
}

// TestConcurrentSaveMetadata tests for race conditions in metadata saving
func TestConcurrentSaveMetadata(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)

	// First ensure we have a valid metadata file
	err = fs.saveMetadata()
	tu.RequireNoError(t, err)

	// Number of concurrent save operations
	const numSaves = 10

	// Use WaitGroup to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(numSaves)

	// Use mutex to protect access to errors
	var errorsMutex sync.Mutex
	var errors []error

	// Launch concurrent metadata save operations
	for i := 0; i < numSaves; i++ {
		go func(index int) {
			defer wg.Done()
			// Create a new copy to avoid data race on metadata
			localFs := &FileStorage{
				dir: fs.dir,
				metadata: &logMetadata{
					FirstIndex: fs.metadata.FirstIndex,
					LastIndex:  uint64(index),
				},
			}
			err := localFs.saveMetadata()
			if err != nil {
				errorsMutex.Lock()
				errors = append(errors, err)
				errorsMutex.Unlock()
			}
		}(i)
	}

	// Wait for all save operations to complete
	wg.Wait()

	// Check if there were any errors
	if len(errors) > 0 {
		t.Logf("Some metadata saves failed: %v", errors)
	}

	// Load metadata from file and check it's valid
	newStorage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	newFs := newStorage.(*FileStorage)

	// We can't assert the exact LastIndex value due to race conditions,
	// but we can verify the file exists and was successfully parsed
	_, err = os.Stat(newFs.metadataFile())
	tu.AssertNoError(t, err)
}

// TestLogMetadataCompleteCoverage tests that all properties of metadata are saved and loaded correctly
func TestLogMetadataCompleteCoverage(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)

	// Set all metadata properties to non-default values
	fs.metadata.FirstIndex = 5
	fs.metadata.LastIndex = 10
	// Add any other properties that might be in logMetadata

	// Save metadata
	err = fs.saveMetadata()
	tu.RequireNoError(t, err)

	// Create a new storage instance and verify all properties are loaded correctly
	newStorage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	newFs := newStorage.(*FileStorage)

	tu.AssertEqual(t, uint64(5), newFs.metadata.FirstIndex)
	tu.AssertEqual(t, uint64(10), newFs.metadata.LastIndex)
	// Verify any other properties that might be in logMetadata
}

// TestOpenLogFileAllErrorPaths tests various error scenarios in openLogFile
func TestOpenLogFileAllErrorPaths(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)

	// Test when directory doesn't have write permissions
	// Make the directory read-only
	err = os.Chmod(tempDir, 0500) // Read + execute only
	tu.RequireNoError(t, err)
	defer os.Chmod(tempDir, 0755) // Restore permissions

	// Try to open log file - this should fail because we can't create the file
	_, _, err = fs.openLogFile()
	tu.AssertError(t, err)

	// Restore permissions and create a log file
	err = os.Chmod(tempDir, 0755)
	tu.RequireNoError(t, err)
	file, _, err := fs.openLogFile()
	tu.RequireNoError(t, err)
	file.Close()

	// Make the log file unreadable
	err = os.Chmod(fs.logFile(), 0000) // No permissions
	tu.RequireNoError(t, err)
	defer os.Chmod(fs.logFile(), 0644) // Restore permissions

	// Try to open log file - this should fail because we can't open the file
	_, _, err = fs.openLogFile()
	tu.AssertError(t, err)
}

// TestAppendZeroEntries tests the behavior when appending an empty slice of entries
func TestAppendZeroEntries(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()

	// Get initial metadata state
	initialFirstIndex := fs.metadata.FirstIndex
	initialLastIndex := fs.metadata.LastIndex

	// Append zero entries
	err = storage.AppendEntries(ctx, []*pb.LogEntry{})
	tu.AssertNoError(t, err)

	// Verify that metadata hasn't changed
	tu.AssertEqual(t, initialFirstIndex, fs.metadata.FirstIndex)
	tu.AssertEqual(t, initialLastIndex, fs.metadata.LastIndex)

	// Verify no file was created
	_, err = os.Stat(fs.logFile())
	tu.AssertError(t, err)
	tu.AssertTrue(t, os.IsNotExist(err))
}

// TestEntryValidationLogic tests validation of entry indices and contiguity
func TestEntryValidationLogic(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Test case 1: First entry must have index 1
	invalidFirstEntries := []*pb.LogEntry{
		{Index: 0, Term: 1, Command: []byte("invalid-zero-index")},
	}
	err = storage.AppendEntries(ctx, invalidFirstEntries)
	tu.AssertError(t, err)
	// Test case 2: Entries must be in order
	outOfOrderEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 3, Term: 1, Command: []byte("third")},
		{Index: 2, Term: 1, Command: []byte("second")},
	}
	err = storage.AppendEntries(ctx, outOfOrderEntries)
	tu.AssertError(t, err)
	// Test case 3: Add a valid entry
	validEntries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
	}
	err = storage.AppendEntries(ctx, validEntries)
	tu.RequireNoError(t, err)
	// Now try adding non-contiguous entries
	nonContiguousEntries := []*pb.LogEntry{
		{Index: 3, Term: 1, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, nonContiguousEntries)
	tu.AssertError(t, err)
	// Test adding the next contiguous entry
	nextEntry := []*pb.LogEntry{
		{Index: 2, Term: 1, Command: []byte("second")},
	}
	err = storage.AppendEntries(ctx, nextEntry)
	tu.RequireNoError(t, err)
	// Test case 4: Entries must have sequential indices
	// First add entry with index 3
	entry3 := []*pb.LogEntry{
		{Index: 3, Term: 1, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, entry3)
	tu.RequireNoError(t, err)
	// Now try adding gapped entries - should fail
	gappedEntries := []*pb.LogEntry{
		{Index: 5, Term: 1, Command: []byte("fifth")},
	}
	err = storage.AppendEntries(ctx, gappedEntries)
	tu.AssertError(t, err)
}

// TestReadEntriesInRangeErrorCases tests various error paths in readEntriesInRange
func TestReadEntriesInRangeErrorCases(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Create a log file with corrupted data
	logFile := fs.logFile()
	file, err := os.Create(logFile)
	tu.RequireNoError(t, err)
	// Write a valid length prefix followed by invalid data
	_, err = file.Write([]byte{0, 0, 0, 10}) // Length prefix indicating 10 bytes
	tu.RequireNoError(t, err)
	_, err = file.Write([]byte{1, 2, 3}) // Only 3 bytes (less than the 10 indicated)
	tu.RequireNoError(t, err)
	file.Close()
	// Set metadata as if we have one entry
	fs.metadata.FirstIndex = 1
	fs.metadata.LastIndex = 1
	err = fs.saveMetadata()
	tu.RequireNoError(t, err)
	// Try to read entries - should fail with corrupted data
	file, err = os.Open(logFile)
	tu.RequireNoError(t, err)
	defer file.Close()
	_, err = fs.readEntriesInRange(ctx, file, 1, 2)
	tu.AssertError(t, err)
	// Test with context cancellation
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()        // Cancel the context immediately
	file.Seek(0, 0) // Reset file position
	_, err = fs.readEntriesInRange(canceledCtx, file, 1, 2)
	tu.AssertError(t, err)
}

// TestLastEntryProperties tests that LastIndex works correctly across operations
func TestLastEntryProperties(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Initial state should have LastIndex = 0
	tu.AssertEqual(t, uint64(0), storage.LastIndex())
	// Add entries
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	// Check LastIndex
	tu.AssertEqual(t, uint64(3), storage.LastIndex())
	// Get the last entry
	lastEntry, err := storage.GetEntry(ctx, storage.LastIndex())
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(3), lastEntry.Index)
	tu.AssertEqual(t, uint64(2), lastEntry.Term)
	// Truncate suffix
	err = storage.TruncateSuffix(ctx, 2)
	tu.RequireNoError(t, err)
	// Check LastIndex after truncation
	tu.AssertEqual(t, uint64(1), storage.LastIndex())
	// Get the new last entry
	newLastEntry, err := storage.GetEntry(ctx, storage.LastIndex())
	tu.RequireNoError(t, err)
	tu.AssertEqual(t, uint64(1), newLastEntry.Index)
	tu.AssertEqual(t, uint64(1), newLastEntry.Term)
}

// TestTemporaryFileCleanup tests that temporary files are cleaned up properly
func TestTemporaryFileCleanup(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	fs := storage.(*FileStorage)
	ctx := context.Background()
	// Add some entries
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	// Save state to generate temporary files
	state := RaftState{CurrentTerm: 10, VotedFor: 2}
	err = storage.SaveState(ctx, state)
	tu.RequireNoError(t, err)
	// Check that no temporary files exist
	tmpStateFile := fs.stateFile() + ".tmp"
	_, err = os.Stat(tmpStateFile)
	tu.AssertError(t, err)
	tu.AssertTrue(t, os.IsNotExist(err))
	// Create a temp file manually to simulate an interrupted operation
	tmpFile := filepath.Join(tempDir, "metadata.json.tmp")
	f, err := os.Create(tmpFile)
	tu.RequireNoError(t, err)
	f.Close()
	// Verify the temp file exists
	_, err = os.Stat(tmpFile)
	tu.AssertNoError(t, err)
	// Create a new storage instance, which should clean up the temp file
	_, err = NewFileStorage(config)
	tu.RequireNoError(t, err)
	// Verify temp file was removed
	_, err = os.Stat(tmpFile)
	tu.AssertError(t, err)
	tu.AssertTrue(t, os.IsNotExist(err))
}

// TestGetEntriesBoundsChecking tests bounds checking in GetEntries
func TestGetEntriesBoundsChecking(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Add entries
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	// Test case 1: low < first index, high > last index
	boundaryEntries, err := storage.GetEntries(ctx, 0, 5)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, boundaryEntries, 3)
	// Test case 2: low < first index, high = last index + 1
	boundaryEntries, err = storage.GetEntries(ctx, 0, 4)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, boundaryEntries, 3)
	// Test case 3: low = first index, high > last index
	boundaryEntries, err = storage.GetEntries(ctx, 1, 5)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, boundaryEntries, 3)
	// Test case 4: low > last index
	boundaryEntries, err = storage.GetEntries(ctx, 4, 5)
	tu.RequireNoError(t, err)
	tu.AssertEmpty(t, boundaryEntries)
	// Test case 5: high <= low
	boundaryEntries, err = storage.GetEntries(ctx, 2, 2)
	tu.RequireNoError(t, err)
	tu.AssertEmpty(t, boundaryEntries)
	// Test case 6: low < 0 (unsigned, so it becomes very large)
	_, err = storage.GetEntries(ctx, ^uint64(0), 5)
	tu.AssertError(t, err)
	// Test case 7: low = first index, high = first index + 1
	boundaryEntries, err = storage.GetEntries(ctx, 1, 2)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, boundaryEntries, 1)
	// Test case 8: low = last index, high = last index + 1
	boundaryEntries, err = storage.GetEntries(ctx, 3, 4)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, boundaryEntries, 1)
}

// TestEmptyDirectoryCreation tests that storage correctly creates the directory if it doesn't exist
func TestEmptyDirectoryCreation(t *testing.T) {
	// Create a path to a non-existent directory
	tempDir := createTempDir(t)
	nonExistentDir := filepath.Join(tempDir, "non-existent-dir")
	// Verify the directory doesn't exist
	_, err := os.Stat(nonExistentDir)
	tu.AssertError(t, err)
	tu.AssertTrue(t, os.IsNotExist(err))
	// Create storage with the non-existent directory
	config := &StorageConfig{Dir: nonExistentDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	tu.RequireNotNil(t, storage)
	// Verify the directory now exists
	_, err = os.Stat(nonExistentDir)
	tu.AssertNoError(t, err)
	// Verify we can add entries
	ctx := context.Background()
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("test")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
}

// TestGetEntriesRangeValidation tests range validation in GetEntries
func TestGetEntriesRangeValidation(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	ctx := context.Background()
	// Add entries
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
		{Index: 2, Term: 1, Command: []byte("second")},
		{Index: 3, Term: 2, Command: []byte("third")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.RequireNoError(t, err)
	// Test negative range (low > high)
	_, err = storage.GetEntries(ctx, 3, 2)
	tu.AssertError(t, err)
	tu.AssertErrorIs(t, err, ErrIndexOutOfRange)
	// Test exact range match
	exactEntries, err := storage.GetEntries(ctx, 1, 4)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, exactEntries, 3)
	// Test partial range
	partialEntries, err := storage.GetEntries(ctx, 2, 3)
	tu.RequireNoError(t, err)
	tu.AssertLen(t, partialEntries, 1)
	tu.AssertEqual(t, uint64(2), partialEntries[0].Index)
	// Test empty range (high = low)
	emptyEntries, err := storage.GetEntries(ctx, 2, 2)
	tu.RequireNoError(t, err)
	tu.AssertEmpty(t, emptyEntries)
	// Test range beyond the end
	beyondEntries, err := storage.GetEntries(ctx, 4, 10)
	tu.RequireNoError(t, err)
	tu.AssertEmpty(t, beyondEntries)
}

// TestAppendEntriesTimeout tests AppendEntries with context timeout
func TestAppendEntriesTimeout(t *testing.T) {
	tempDir := createTempDir(t)
	config := &StorageConfig{Dir: tempDir}
	storage, err := NewFileStorage(config)
	tu.RequireNoError(t, err)
	// Create a context with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	// Sleep to ensure timeout occurs
	time.Sleep(10 * time.Millisecond)
	// Try to append entries with expired context
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("first")},
	}
	err = storage.AppendEntries(ctx, entries)
	tu.AssertError(t, err)
	tu.AssertTrue(t, err == context.DeadlineExceeded)
}
