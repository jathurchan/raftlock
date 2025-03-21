package raft

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
)

// Storage Tests Overview:
// - testSaveAndLoadState: Tests saving and loading Raft state (currentTerm, votedFor)
// - testAppendAndGetEntries: Tests adding log entries and retrieving them
// - testTruncateSuffix: Tests removing log entries from the end
// - testTruncatePrefix: Tests removing log entries from the beginning
// - testEmptyLog: Tests operations on an empty log
// - testContextCancellation: Tests that operations respect context cancellation
// - testAppendOverwrite: Tests overwriting existing entries
// - testNonContiguousAppend: Tests error handling for non-contiguous appends
// - testLargeAppend: Tests appending many entries at once
// - testConcurrentAccess: Tests thread safety with concurrent operations
// - testPersistence: Tests that data persists across storage restarts (FileStorage only)
// - testCorruptedState: Tests handling of corrupted state data (FileStorage only)

// Verifies that a storage implementation can correctly
// save and load Raft state (currentTerm and votedFor).
func testSaveAndLoadState(t *testing.T, s Storage) {
	ctx := context.Background()

	// Initial state should be term 0, votedFor -1
	initialState, err := s.LoadState(ctx)
	if err != nil {
		t.Fatalf("Failed to load initial state: %v", err)
	}
	assertEqual(t, uint64(0), initialState.CurrentTerm)
	assertEqual(t, -1, initialState.VotedFor)

	// Save a new state
	newState := RaftState{CurrentTerm: 5, VotedFor: 2}
	err = s.SaveState(ctx, newState)
	assertNoError(t, err)

	// Load it back and verify
	loadedState, err := s.LoadState(ctx)
	assertNoError(t, err)
	assertEqual(t, newState.CurrentTerm, loadedState.CurrentTerm)
	assertEqual(t, newState.VotedFor, loadedState.VotedFor)

	// Update the state
	updatedState := RaftState{CurrentTerm: 6, VotedFor: 3}
	err = s.SaveState(ctx, updatedState)
	assertNoError(t, err)

	// Load it again and verify
	loadedState, err = s.LoadState(ctx)
	assertNoError(t, err)
	assertEqual(t, updatedState.CurrentTerm, loadedState.CurrentTerm)
	assertEqual(t, updatedState.VotedFor, loadedState.VotedFor)
}

// Verifies that a storage implementation can correctly
// append log entries and retrieve them with various range queries.
func testAppendAndGetEntries(t *testing.T, s Storage) {
	ctx := context.Background()
	// Initially the log should be empty
	assertEqual(t, uint64(0), s.FirstIndex())
	assertEqual(t, uint64(0), s.LastIndex())

	// Append entries
	entries := []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("command1")},
		{Term: 1, Index: 2, Command: []byte("command2")},
		{Term: 1, Index: 3, Command: []byte("command3")},
		{Term: 2, Index: 4, Command: []byte("command4")},
		{Term: 2, Index: 5, Command: []byte("command5")},
	}
	err := s.AppendEntries(ctx, entries)
	assertNoError(t, err)

	// Verify first and last indices
	assertEqual(t, uint64(1), s.FirstIndex())
	assertEqual(t, uint64(5), s.LastIndex())

	// Get a single entry
	entry, err := s.GetEntry(ctx, 3)
	assertNoError(t, err)
	assertEqual(t, uint64(1), entry.Term)
	assertEqual(t, uint64(3), entry.Index)
	assertBytesEqual(t, []byte("command3"), entry.Command)

	// Get entries in a range
	rangeEntries, err := s.GetEntries(ctx, 2, 5)
	assertNoError(t, err)
	assertEqual(t, 3, len(rangeEntries))
	assertEqual(t, uint64(2), rangeEntries[0].Index)
	assertEqual(t, uint64(3), rangeEntries[1].Index)
	assertEqual(t, uint64(4), rangeEntries[2].Index)

	// Get all entries
	allEntries, err := s.GetEntries(ctx, 1, 6)
	assertNoError(t, err)
	assertEqual(t, 5, len(allEntries))

	// Get out of range - index too high
	nonExistentEntry, err := s.GetEntry(ctx, 10)
	assertErrorIs(t, err, ErrIndexOutOfRange)
	if nonExistentEntry != nil {
		t.Errorf("Expected nil entry, got: %v", nonExistentEntry)
	}

	// Get out of range entries - should return empty slice
	emptyEntries, err := s.GetEntries(ctx, 6, 10)
	assertNoError(t, err)
	assertEqual(t, 0, len(emptyEntries))

	// Try invalid range
	_, err = s.GetEntries(ctx, 5, 3)
	assertErrorIs(t, err, ErrIndexOutOfRange)

	// Append more entries
	moreEntries := []*pb.LogEntry{
		{Term: 2, Index: 6, Command: []byte("command6")},
		{Term: 3, Index: 7, Command: []byte("command7")},
	}
	err = s.AppendEntries(ctx, moreEntries)
	assertNoError(t, err)

	// Verify updated indices
	assertEqual(t, uint64(1), s.FirstIndex())
	assertEqual(t, uint64(7), s.LastIndex())

	// Get entries across the original and new appendages
	crossEntries, err := s.GetEntries(ctx, 5, 8)
	assertNoError(t, err)
	assertEqual(t, 3, len(crossEntries))
	assertEqual(t, uint64(5), crossEntries[0].Index)
	assertEqual(t, uint64(6), crossEntries[1].Index)
	assertEqual(t, uint64(7), crossEntries[2].Index)
}

// Verifies that a storage implementation can correctly
// remove all log entries with index >= given index.
func testTruncateSuffix(t *testing.T, s Storage) {
	ctx := context.Background()

	// Append entries
	entries := []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("command1")},
		{Term: 1, Index: 2, Command: []byte("command2")},
		{Term: 1, Index: 3, Command: []byte("command3")},
		{Term: 2, Index: 4, Command: []byte("command4")},
		{Term: 2, Index: 5, Command: []byte("command5")},
	}
	err := s.AppendEntries(ctx, entries)
	assertNoError(t, err)

	// Truncate from index 3
	err = s.TruncateSuffix(ctx, 3)
	assertNoError(t, err)

	// Verify indices
	assertEqual(t, uint64(1), s.FirstIndex())
	assertEqual(t, uint64(2), s.LastIndex())

	// Verify that entries 3, 4, 5 are gone
	entry, err := s.GetEntry(ctx, 2)
	assertNoError(t, err)
	assertEqual(t, uint64(2), entry.Index)

	_, err = s.GetEntry(ctx, 3)
	assertErrorIs(t, err, ErrNotFound)

	// Get all remaining entries
	remainingEntries, err := s.GetEntries(ctx, 1, 10)
	assertNoError(t, err)
	assertEqual(t, 2, len(remainingEntries))
	assertEqual(t, uint64(1), remainingEntries[0].Index)
	assertEqual(t, uint64(2), remainingEntries[1].Index)

	// Truncate at a higher index than exists
	err = s.TruncateSuffix(ctx, 10)
	assertNoError(t, err)
	assertEqual(t, uint64(1), s.FirstIndex())
	assertEqual(t, uint64(2), s.LastIndex()) // Should not change
}

// Verifies that a storage implementation can correctly
// remove all log entries with index <= given index.
func testTruncatePrefix(t *testing.T, s Storage) {
	ctx := context.Background()

	// Append entries
	entries := []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("command1")},
		{Term: 1, Index: 2, Command: []byte("command2")},
		{Term: 1, Index: 3, Command: []byte("command3")},
		{Term: 2, Index: 4, Command: []byte("command4")},
		{Term: 2, Index: 5, Command: []byte("command5")},
	}
	err := s.AppendEntries(ctx, entries)
	assertNoError(t, err)

	// Truncate prefix up to index 2
	err = s.TruncatePrefix(ctx, 2)
	assertNoError(t, err)

	// Verify indices
	assertEqual(t, uint64(3), s.FirstIndex())
	assertEqual(t, uint64(5), s.LastIndex())

	// Verify that entries 1 and 2 are gone
	_, err = s.GetEntry(ctx, 1)
	assertErrorIs(t, err, ErrNotFound)

	_, err = s.GetEntry(ctx, 2)
	assertErrorIs(t, err, ErrNotFound)

	// Get all remaining entries
	remainingEntries, err := s.GetEntries(ctx, 1, 10)
	assertNoError(t, err)
	assertEqual(t, 3, len(remainingEntries))
	assertEqual(t, uint64(3), remainingEntries[0].Index)
	assertEqual(t, uint64(4), remainingEntries[1].Index)
	assertEqual(t, uint64(5), remainingEntries[2].Index)

	// Truncate at a lower index than exists
	err = s.TruncatePrefix(ctx, 1)
	assertNoError(t, err)
	assertEqual(t, uint64(3), s.FirstIndex()) // Should not change
	assertEqual(t, uint64(5), s.LastIndex())
}

// Verifies that a storage implementation correctly handles
// operations on an empty log.
func testEmptyLog(t *testing.T, s Storage) {
	ctx := context.Background()

	// Initial state with empty log
	assertEqual(t, uint64(0), s.FirstIndex())
	assertEqual(t, uint64(0), s.LastIndex())

	// Get entries from empty log
	entries, err := s.GetEntries(ctx, 1, 5)
	assertNoError(t, err)
	assertEqual(t, 0, len(entries))

	// Get single entry from empty log
	_, err = s.GetEntry(ctx, 1)
	assertErrorIs(t, err, ErrNotFound)

	// Truncate operations on empty log
	err = s.TruncatePrefix(ctx, 1)
	assertNoError(t, err)

	err = s.TruncateSuffix(ctx, 1)
	assertNoError(t, err)

	// Indices should still be 0
	assertEqual(t, uint64(0), s.FirstIndex())
	assertEqual(t, uint64(0), s.LastIndex())
}

// Verifies that operations respect context cancellation.
func testContextCancellation(t *testing.T, s Storage) {
	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel the context
	cancel()

	// All operations should return context.Canceled
	_, err := s.LoadState(ctx)
	assertErrorIs(t, err, context.Canceled)

	err = s.SaveState(ctx, RaftState{})
	assertErrorIs(t, err, context.Canceled)

	err = s.AppendEntries(ctx, []*pb.LogEntry{{Term: 1, Index: 1}})
	assertErrorIs(t, err, context.Canceled)

	_, err = s.GetEntries(ctx, 1, 2)
	assertErrorIs(t, err, context.Canceled)

	_, err = s.GetEntry(ctx, 1)
	assertErrorIs(t, err, context.Canceled)
}

// Verifies that when entries are appended with indices that
// already exist, the new entries replace the old ones.
func testAppendOverwrite(t *testing.T, s Storage) {
	ctx := context.Background()

	initialEntries := []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 1, Index: 3, Command: []byte("cmd3")},
	}
	err := s.AppendEntries(ctx, initialEntries)
	assertNoError(t, err)

	conflictingEntries := []*pb.LogEntry{
		{Term: 2, Index: 2, Command: []byte("new2")},
		{Term: 2, Index: 3, Command: []byte("new3")},
		{Term: 2, Index: 4, Command: []byte("new4")},
	}
	err = s.AppendEntries(ctx, conflictingEntries)
	assertNoError(t, err)

	entries, err := s.GetEntries(ctx, 1, 5)
	assertNoError(t, err)
	assertEqual(t, 4, len(entries))
	assertBytesEqual(t, []byte("cmd1"), entries[0].Command)
	assertBytesEqual(t, []byte("new2"), entries[1].Command)
	assertBytesEqual(t, []byte("new3"), entries[2].Command)
	assertBytesEqual(t, []byte("new4"), entries[3].Command)
}

// Verifies that appending entries with non-contiguous
// indices returns an error.
func testNonContiguousAppend(t *testing.T, s Storage) {
	ctx := context.Background()

	err := s.AppendEntries(ctx, []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
	})
	assertNoError(t, err)

	err = s.AppendEntries(ctx, []*pb.LogEntry{
		{Term: 1, Index: 3, Command: []byte("cmd3")},
	})

	assertError(t, err)
}

// Verifies that the storage can handle a large number of entries.
func testLargeAppend(t *testing.T, s Storage) {
	ctx := context.Background()
	const count = 1000 // Reduced from 10000 to make tests faster

	entries := make([]*pb.LogEntry, count)
	for i := 0; i < count; i++ {
		entries[i] = &pb.LogEntry{
			Term:    1,
			Index:   uint64(i + 1),
			Command: []byte("bulk"),
		}
	}

	err := s.AppendEntries(ctx, entries)
	assertNoError(t, err)

	assertEqual(t, uint64(1), s.FirstIndex())
	assertEqual(t, uint64(count), s.LastIndex())
}

// Verifies that the storage can be safely accessed concurrently.
func testConcurrentAccess(t *testing.T, s Storage) {
	ctx := context.Background()

	// Add some initial entries
	entries := []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("command1")},
		{Term: 1, Index: 2, Command: []byte("command2")},
	}
	err := s.AppendEntries(ctx, entries)
	assertNoError(t, err)

	// Concurrently append and read
	const goroutines = 5  // Reduced from 10
	const operations = 10 // Reduced from 20

	var wg sync.WaitGroup
	wg.Add(goroutines * 2) // for both readers and writers

	// Writers
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < operations; j++ {
				index := uint64(3 + id*operations + j)
				entry := &pb.LogEntry{
					Term:    1,
					Index:   index,
					Command: []byte("cmd"),
				}
				err := s.AppendEntries(ctx, []*pb.LogEntry{entry})
				if err != nil {
					t.Errorf("Concurrent append failed: %v", err)
				}

				// Small delay to increase chance of race conditions
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Readers
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < operations; j++ {
				_, err := s.LoadState(ctx)
				if err != nil {
					t.Errorf("Concurrent load state failed: %v", err)
				}

				_, err = s.GetEntries(ctx, 1, s.LastIndex()+1)
				// Error is acceptable here as LastIndex might change during concurrent operations
				// But we should still check for unexpected errors
				if err != nil && !errors.Is(err, ErrIndexOutOfRange) {
					t.Errorf("Concurrent get entries failed with unexpected error: %v", err)
				}

				// Small delay to increase chance of race conditions
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

// Runs a complete suite of tests against any Storage implementation.
func testStorage(t *testing.T, s Storage) {
	t.Run("SaveAndLoadState", func(t *testing.T) {
		testSaveAndLoadState(t, s)
	})

	t.Run("AppendAndGetEntries", func(t *testing.T) {
		testAppendAndGetEntries(t, s)
	})

	t.Run("TruncateSuffix", func(t *testing.T) {
		testTruncateSuffix(t, s)
	})

	t.Run("TruncatePrefix", func(t *testing.T) {
		testTruncatePrefix(t, s)
	})

	t.Run("EmptyLog", func(t *testing.T) {
		testEmptyLog(t, s)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		testContextCancellation(t, s)
	})

	t.Run("AppendOverwrite", func(t *testing.T) {
		testAppendOverwrite(t, s)
	})

	t.Run("NonContiguousAppend", func(t *testing.T) {
		testNonContiguousAppend(t, s)
	})

	t.Run("LargeAppend", func(t *testing.T) {
		testLargeAppend(t, s)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		testConcurrentAccess(t, s)
	})
}

// Verifies that MemoryStorage correctly implements the Storage interface.
func TestMemoryStorage(t *testing.T) {
	s, err := NewMemoryStorage()
	if err != nil {
		t.Fatalf("Failed to create memory storage: %v", err)
	}
	defer s.Close()

	testStorage(t, s)
}

// Verifies that FileStorage correctly implements the Storage interface.
func TestFileStorage(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "raft-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &StorageConfig{
		Type: FileStorageType,
		Dir:  tempDir,
	}

	s, err := NewFileStorage(config)
	if err != nil {
		t.Fatalf("Failed to create file storage: %v", err)
	}
	defer s.Close()

	testStorage(t, s)
}

// Verifies that FileStorage persists data across restarts.
func TestFilePersistence(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "raft-persistence-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()

	// Create a new storage instance
	config := &StorageConfig{
		Type: FileStorageType,
		Dir:  tempDir,
	}

	s1, err := NewFileStorage(config)
	if err != nil {
		t.Fatalf("Failed to create file storage: %v", err)
	}

	// Save state and append entries
	state := RaftState{CurrentTerm: 42, VotedFor: 3}
	err = s1.SaveState(ctx, state)
	assertNoError(t, err)

	entries := []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("command1")},
		{Term: 1, Index: 2, Command: []byte("command2")},
	}
	err = s1.AppendEntries(ctx, entries)
	assertNoError(t, err)

	// Close the storage
	err = s1.Close()
	assertNoError(t, err)

	// Create a new storage instance with the same directory
	s2, err := NewFileStorage(config)
	if err != nil {
		t.Fatalf("Failed to create second file storage: %v", err)
	}
	defer s2.Close()

	// Verify loaded state
	loadedState, err := s2.LoadState(ctx)
	assertNoError(t, err)
	assertEqual(t, state.CurrentTerm, loadedState.CurrentTerm)
	assertEqual(t, state.VotedFor, loadedState.VotedFor)

	// Verify loaded entries
	assertEqual(t, uint64(1), s2.FirstIndex())
	assertEqual(t, uint64(2), s2.LastIndex())

	loadedEntries, err := s2.GetEntries(ctx, 1, 3)
	assertNoError(t, err)
	assertEqual(t, 2, len(loadedEntries))
	assertEqual(t, uint64(1), loadedEntries[0].Index)
	assertEqual(t, uint64(2), loadedEntries[1].Index)
	assertBytesEqual(t, []byte("command1"), loadedEntries[0].Command)
	assertBytesEqual(t, []byte("command2"), loadedEntries[1].Command)
}

// Tests FileStorage's handling of corrupted state data.
func TestFileStorageCorruptedState(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raft-test-corrupted")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create invalid state file
	stateFile := filepath.Join(tempDir, "state.json")
	err = os.WriteFile(stateFile, []byte("invalid json"), 0644)
	assertNoError(t, err)

	config := &StorageConfig{
		Type: FileStorageType,
		Dir:  tempDir,
	}

	s, err := NewFileStorage(config)
	if err != nil {
		t.Fatalf("Failed to create file storage: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Loading corrupted state should return ErrCorruptedData
	_, err = s.LoadState(ctx)
	assertErrorIs(t, err, ErrCorruptedData)
}

// Tests the creation of different storage types.
func TestNewStorage(t *testing.T) {
	// Test memory storage
	config := &StorageConfig{
		Type: MemoryStorageType,
	}
	s, err := NewStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	_, ok := s.(*MemoryStorage)
	assertTrue(t, ok)
	s.Close()

	// Test file storage
	tempDir, err := os.MkdirTemp("", "raft-test-factory")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config = &StorageConfig{
		Type: FileStorageType,
		Dir:  tempDir,
	}
	s, err = NewStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	_, ok = s.(*FileStorage)
	assertTrue(t, ok)
	s.Close()

	// Test unsupported type
	config = &StorageConfig{
		Type: "unsupported",
	}
	_, err = NewStorage(config)
	assertError(t, err)

	// Test nil config
	s, err = NewStorage(nil)
	assertNoError(t, err)
	assertNotNil(t, s)
	s.Close()
}

// Tests the DefaultStorageConfig function.
func TestDefaultStorageConfig(t *testing.T) {
	config := DefaultStorageConfig()
	assertEqual(t, FileStorageType, config.Type)
	assertEqual(t, "../data/raft", config.Dir)
}

// Helper functions for assertions
func assertEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

func assertBytesEqual(t *testing.T, expected, actual []byte) {
	if !bytes.Equal(expected, actual) {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func assertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func assertErrorIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Errorf("Expected error to be %v, got %v", target, err)
	}
}

func assertTrue(t *testing.T, condition bool) {
	t.Helper()
	if !condition {
		t.Error("Expected condition to be true")
	}
}

func assertNotNil(t *testing.T, obj any) {
	t.Helper()
	if obj == nil {
		t.Error("Expected non-nil value")
	}
}
