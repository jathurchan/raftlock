package raft

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"slices"

	pb "github.com/jathurchan/raftlock/proto"
)

type storageTestSuite struct {
	t       *testing.T
	ctx     context.Context
	storage Storage
	tempDir string
}

func setupStorageTest(t *testing.T, storageType StorageType) *storageTestSuite {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	var storage Storage
	var err error
	var tempDir string

	if storageType == FileStorageType {
		tempDir, err = os.MkdirTemp("", "raft-test-*")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(tempDir) })

		config := &StorageConfig{
			Type: FileStorageType,
			Dir:  tempDir,
		}
		storage, err = NewStorage(config)
	} else {
		storage, err = NewStorage(&StorageConfig{Type: MemoryStorageType})
	}
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if storage == nil {
		t.Fatal("Storage is nil")
	}

	return &storageTestSuite{
		t:       t,
		ctx:     ctx,
		storage: storage,
		tempDir: tempDir,
	}
}

// Helper method to create log entries for testing
func createTestLogEntries(startIndex uint64, count int) []*pb.LogEntry {
	entries := make([]*pb.LogEntry, count)
	for i := range count {
		entries[i] = &pb.LogEntry{
			Index:   startIndex + uint64(i),
			Term:    uint64(i) + 1,
			Command: []byte("test data " + string(rune('A'+i))),
		}
	}
	return entries
}

// Helper method to verify log entries match expected values
func verifyLogEntries(t *testing.T, actual, expected []*pb.LogEntry) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("Entry count mismatch: got %d, want %d", len(actual), len(expected))
	}
	for i, expectedEntry := range expected {
		if actual[i].Index != expectedEntry.Index {
			t.Errorf("Entry %d index mismatch: got %d, want %d", i, actual[i].Index, expectedEntry.Index)
		}
		if actual[i].Term != expectedEntry.Term {
			t.Errorf("Entry %d term mismatch: got %d, want %d", i, actual[i].Term, expectedEntry.Term)
		}
		if !bytes.Equal(actual[i].Command, expectedEntry.Command) {
			t.Errorf("Entry %d data mismatch: got %s, want %s", i, actual[i].Command, expectedEntry.Command)
		}
	}
}

// Test state persistence and retrieval
func testRaftStatePersistence(t *testing.T, suite *storageTestSuite) {
	// Test default initial state
	initialState, err := suite.storage.LoadState(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to load initial state: %v", err)
	}
	if initialState.CurrentTerm != 0 {
		t.Errorf("Unexpected initial CurrentTerm: got %d, want 0", initialState.CurrentTerm)
	}
	if initialState.VotedFor != -1 {
		t.Errorf("Unexpected initial VotedFor: got %d, want -1", initialState.VotedFor)
	}

	// Test saving and loading updated state
	updatedState := RaftState{
		CurrentTerm: 42,
		VotedFor:    3,
	}
	err = suite.storage.SaveState(suite.ctx, updatedState)
	if err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	loadedState, err := suite.storage.LoadState(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to load updated state: %v", err)
	}
	if loadedState.CurrentTerm != updatedState.CurrentTerm {
		t.Errorf("Unexpected loaded CurrentTerm: got %d, want %d", loadedState.CurrentTerm, updatedState.CurrentTerm)
	}
	if loadedState.VotedFor != updatedState.VotedFor {
		t.Errorf("Unexpected loaded VotedFor: got %d, want %d", loadedState.VotedFor, updatedState.VotedFor)
	}

	// Test overwriting existing state
	finalState := RaftState{
		CurrentTerm: 99,
		VotedFor:    5,
	}
	err = suite.storage.SaveState(suite.ctx, finalState)
	if err != nil {
		t.Fatalf("Failed to save final state: %v", err)
	}

	loadedState, err = suite.storage.LoadState(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to load final state: %v", err)
	}
	if loadedState.CurrentTerm != finalState.CurrentTerm {
		t.Errorf("Unexpected final CurrentTerm: got %d, want %d", loadedState.CurrentTerm, finalState.CurrentTerm)
	}
	if loadedState.VotedFor != finalState.VotedFor {
		t.Errorf("Unexpected final VotedFor: got %d, want %d", loadedState.VotedFor, finalState.VotedFor)
	}
}

// Test appending and retrieving log entries
func testLogEntryOperations(t *testing.T, suite *storageTestSuite) {
	// Test empty log
	entries, err := suite.storage.GetEntries(suite.ctx, 1, 10)
	if err != nil {
		t.Fatalf("Failed to get entries from empty log: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected empty entries, got %d entries", len(entries))
	}

	if suite.storage.FirstIndex() != 0 {
		t.Errorf("Unexpected FirstIndex: got %d, want 0", suite.storage.FirstIndex())
	}
	if suite.storage.LastIndex() != 0 {
		t.Errorf("Unexpected LastIndex: got %d, want 0", suite.storage.LastIndex())
	}

	// Append first set of entries
	testEntries := createTestLogEntries(1, 5)
	err = suite.storage.AppendEntries(suite.ctx, testEntries)
	if err != nil {
		t.Fatalf("Failed to append entries: %v", err)
	}

	// Verify indices updated correctly
	if suite.storage.FirstIndex() != 1 {
		t.Errorf("Unexpected FirstIndex after append: got %d, want 1", suite.storage.FirstIndex())
	}
	if suite.storage.LastIndex() != 5 {
		t.Errorf("Unexpected LastIndex after append: got %d, want 5", suite.storage.LastIndex())
	}

	// Test retrieving all entries
	retrievedEntries, err := suite.storage.GetEntries(suite.ctx, 1, 6)
	if err != nil {
		t.Fatalf("Failed to retrieve all entries: %v", err)
	}
	verifyLogEntries(t, retrievedEntries, testEntries)

	// Test getting a subset of entries
	subsetEntries, err := suite.storage.GetEntries(suite.ctx, 2, 4)
	if err != nil {
		t.Fatalf("Failed to retrieve subset of entries: %v", err)
	}
	verifyLogEntries(t, subsetEntries, testEntries[1:3])

	// Test getting a single entry
	singleEntry, err := suite.storage.GetEntry(suite.ctx, 3)
	if err != nil {
		t.Fatalf("Failed to retrieve single entry: %v", err)
	}
	if singleEntry.Index != testEntries[2].Index {
		t.Errorf("Single entry index mismatch: got %d, want %d", singleEntry.Index, testEntries[2].Index)
	}
	if singleEntry.Term != testEntries[2].Term {
		t.Errorf("Single entry term mismatch: got %d, want %d", singleEntry.Term, testEntries[2].Term)
	}
	if !bytes.Equal(singleEntry.Command, testEntries[2].Command) {
		t.Errorf("Single entry data mismatch: got %s, want %s", singleEntry.Command, testEntries[2].Command)
	}

	// Append more entries
	moreEntries := createTestLogEntries(6, 3)
	err = suite.storage.AppendEntries(suite.ctx, moreEntries)
	if err != nil {
		t.Fatalf("Failed to append more entries: %v", err)
	}

	// Verify updated indices
	if suite.storage.FirstIndex() != 1 {
		t.Errorf("Unexpected FirstIndex after second append: got %d, want 1", suite.storage.FirstIndex())
	}
	if suite.storage.LastIndex() != 8 {
		t.Errorf("Unexpected LastIndex after second append: got %d, want 8", suite.storage.LastIndex())
	}

	// Verify all entries
	allEntries, err := suite.storage.GetEntries(suite.ctx, 1, 9)
	if err != nil {
		t.Fatalf("Failed to retrieve all entries after second append: %v", err)
	}

	combined := slices.Clone(testEntries)
	combined = append(combined, moreEntries...)
	verifyLogEntries(t, allEntries, combined)
}

// Test entry not found cases
func testEntryNotFound(t *testing.T, suite *storageTestSuite) {
	// Prepare some entries
	entries := createTestLogEntries(1, 5)
	err := suite.storage.AppendEntries(suite.ctx, entries)
	if err != nil {
		t.Fatalf("Failed to append entries: %v", err)
	}

	// Test getting entry before the first index
	_, err = suite.storage.GetEntry(suite.ctx, 0)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound for index before first, got: %v", err)
	}

	// Test getting entry after the last index
	_, err = suite.storage.GetEntry(suite.ctx, 10)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound for index after last, got: %v", err)
	}

	// Test invalid range
	_, err = suite.storage.GetEntries(suite.ctx, 10, 5)
	if !errors.Is(err, ErrIndexOutOfRange) {
		t.Errorf("Expected ErrIndexOutOfRange for invalid range, got: %v", err)
	}

	// Test empty range due to constraints
	result, err := suite.storage.GetEntries(suite.ctx, 10, 11)
	if err != nil {
		t.Fatalf("GetEntries failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Expected empty result for out-of-range bounds, got %d entries", len(result))
	}
}

// Test truncating entries from the start of the log
func testTruncatePrefix(t *testing.T, suite *storageTestSuite) {
	// Prepare entries
	entries := createTestLogEntries(1, 10)
	err := suite.storage.AppendEntries(suite.ctx, entries)
	if err != nil {
		t.Fatalf("Failed to append entries: %v", err)
	}

	// Truncate first half
	err = suite.storage.TruncatePrefix(suite.ctx, 6)
	if err != nil {
		t.Fatalf("Failed to truncate prefix: %v", err)
	}

	// Verify first index updated
	if suite.storage.FirstIndex() != 6 {
		t.Errorf("Unexpected FirstIndex after truncate prefix: got %d, want 6", suite.storage.FirstIndex())
	}
	if suite.storage.LastIndex() != 10 {
		t.Errorf("Unexpected LastIndex after truncate prefix: got %d, want 10", suite.storage.LastIndex())
	}

	// Verify remaining entries
	remainingEntries, err := suite.storage.GetEntries(suite.ctx, 6, 11)
	if err != nil {
		t.Fatalf("Failed to get remaining entries: %v", err)
	}
	verifyLogEntries(t, remainingEntries, entries[5:])

	// Verify truncated entries are gone
	for i := uint64(1); i < 6; i++ {
		_, err = suite.storage.GetEntry(suite.ctx, i)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for truncated entry %d, got: %v", i, err)
		}
	}
}

// Test truncating entries from the end of the log
func testTruncateSuffix(t *testing.T, suite *storageTestSuite) {
	// Prepare entries
	entries := createTestLogEntries(1, 10)
	err := suite.storage.AppendEntries(suite.ctx, entries)
	if err != nil {
		t.Fatalf("Failed to append entries: %v", err)
	}

	// Truncate second half
	err = suite.storage.TruncateSuffix(suite.ctx, 6)
	if err != nil {
		t.Fatalf("Failed to truncate suffix: %v", err)
	}

	// Verify last index updated
	if suite.storage.FirstIndex() != 1 {
		t.Errorf("Unexpected FirstIndex after truncate suffix: got %d, want 1", suite.storage.FirstIndex())
	}
	if suite.storage.LastIndex() != 5 {
		t.Errorf("Unexpected LastIndex after truncate suffix: got %d, want 5", suite.storage.LastIndex())
	}

	// Verify remaining entries
	remainingEntries, err := suite.storage.GetEntries(suite.ctx, 1, 6)
	if err != nil {
		t.Fatalf("Failed to get remaining entries: %v", err)
	}
	verifyLogEntries(t, remainingEntries, entries[:5])

	// Verify truncated entries are gone
	for i := uint64(6); i <= 10; i++ {
		_, err = suite.storage.GetEntry(suite.ctx, i)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for truncated entry %d, got: %v", i, err)
		}
	}
}

// Test contiguity check
func testEntriesContiguity(t *testing.T, suite *storageTestSuite) {
	// Valid contiguous entries starting from empty log
	entries1 := createTestLogEntries(1, 3)
	err := suite.storage.AppendEntries(suite.ctx, entries1)
	if err != nil {
		t.Fatalf("Failed to append first contiguous entries: %v", err)
	}

	// Valid contiguous entries continuing from existing log
	entries2 := createTestLogEntries(4, 3)
	err = suite.storage.AppendEntries(suite.ctx, entries2)
	if err != nil {
		t.Fatalf("Failed to append second contiguous entries: %v", err)
	}

	// Invalid non-contiguous entries (gap)
	entries3 := createTestLogEntries(8, 2)
	err = suite.storage.AppendEntries(suite.ctx, entries3)
	if err == nil {
		t.Error("Expected error for non-contiguous entries with gap, got nil")
	}

	// Invalid non-contiguous entries (overlap)
	entries4 := createTestLogEntries(5, 3)
	err = suite.storage.AppendEntries(suite.ctx, entries4)
	if err == nil {
		t.Error("Expected error for non-contiguous entries with overlap, got nil")
	}
}

// Test cancelled context
func testCancelledContext(t *testing.T, suite *storageTestSuite) {
	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Test all operations with cancelled context
	_, err := suite.storage.LoadState(ctx)
	if err == nil {
		t.Error("Expected error for LoadState with cancelled context, got nil")
	}

	err = suite.storage.SaveState(ctx, RaftState{CurrentTerm: 5, VotedFor: 1})
	if err == nil {
		t.Error("Expected error for SaveState with cancelled context, got nil")
	}

	entries := createTestLogEntries(1, 3)
	err = suite.storage.AppendEntries(ctx, entries)
	if err == nil {
		t.Error("Expected error for AppendEntries with cancelled context, got nil")
	}

	_, err = suite.storage.GetEntries(ctx, 1, 3)
	if err == nil {
		t.Error("Expected error for GetEntries with cancelled context, got nil")
	}

	_, err = suite.storage.GetEntry(ctx, 1)
	if err == nil {
		t.Error("Expected error for GetEntry with cancelled context, got nil")
	}

	err = suite.storage.TruncatePrefix(ctx, 2)
	if err == nil {
		t.Error("Expected error for TruncatePrefix with cancelled context, got nil")
	}

	err = suite.storage.TruncateSuffix(ctx, 2)
	if err == nil {
		t.Error("Expected error for TruncateSuffix with cancelled context, got nil")
	}
}

// Test edge cases for memory storage
func testMemoryStorageEdgeCases(t *testing.T) {
	suite := setupStorageTest(t, MemoryStorageType)

	// Test handling empty AppendEntries
	err := suite.storage.AppendEntries(suite.ctx, []*pb.LogEntry{})
	if err != nil {
		t.Errorf("AppendEntries with empty slice failed: %v", err)
	}

	// Test truncate on empty log
	err = suite.storage.TruncatePrefix(suite.ctx, 5)
	if err != nil {
		t.Errorf("TruncatePrefix on empty log failed: %v", err)
	}

	err = suite.storage.TruncateSuffix(suite.ctx, 5)
	if err != nil {
		t.Errorf("TruncateSuffix on empty log failed: %v", err)
	}

	// Test Close method
	err = suite.storage.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// Test file storage specific features
func testFileStorageSpecific(t *testing.T) {
	suite := setupStorageTest(t, FileStorageType)

	// Append some entries for testing
	entries := createTestLogEntries(1, 5)
	err := suite.storage.AppendEntries(suite.ctx, entries)
	if err != nil {
		t.Fatalf("Failed to append entries: %v", err)
	}

	// Test persistence across restarts
	state := RaftState{CurrentTerm: 99, VotedFor: 3}
	err = suite.storage.SaveState(suite.ctx, state)
	if err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	// Verify files were created
	fileStorage, ok := suite.storage.(*FileStorage)
	if !ok {
		t.Fatal("Failed to cast to FileStorage")
	}

	files := []string{
		fileStorage.stateFile(),
		fileStorage.metadataFile(),
		fileStorage.logFile(),
	}

	for _, file := range files {
		_, err := os.Stat(file)
		if err != nil {
			t.Errorf("File %s should exist, got error: %v", file, err)
		}
	}

	// Create a new storage instance pointing to the same directory
	newStorage, err := NewStorage(&StorageConfig{
		Type: FileStorageType,
		Dir:  suite.tempDir,
	})
	if err != nil {
		t.Fatalf("Failed to create new storage instance: %v", err)
	}

	// Verify state was persisted
	loadedState, err := newStorage.LoadState(suite.ctx)
	if err != nil {
		t.Fatalf("Failed to load state from new instance: %v", err)
	}
	if loadedState.CurrentTerm != state.CurrentTerm {
		t.Errorf("CurrentTerm not persisted: got %d, want %d", loadedState.CurrentTerm, state.CurrentTerm)
	}
	if loadedState.VotedFor != state.VotedFor {
		t.Errorf("VotedFor not persisted: got %d, want %d", loadedState.VotedFor, state.VotedFor)
	}

	// Verify entries were persisted
	if newStorage.FirstIndex() != 1 {
		t.Errorf("FirstIndex not persisted: got %d, want 1", newStorage.FirstIndex())
	}
	if newStorage.LastIndex() != 5 {
		t.Errorf("LastIndex not persisted: got %d, want 5", newStorage.LastIndex())
	}

	retrievedEntries, err := newStorage.GetEntries(suite.ctx, 1, 6)
	if err != nil {
		t.Fatalf("Failed to retrieve entries from new instance: %v", err)
	}
	verifyLogEntries(t, retrievedEntries, entries)
}

// Test file corruption scenarios
func testFileCorruption(t *testing.T) {
	suite := setupStorageTest(t, FileStorageType)

	// Append some entries
	entries := createTestLogEntries(1, 3)
	err := suite.storage.AppendEntries(suite.ctx, entries)
	if err != nil {
		t.Fatalf("Failed to append entries: %v", err)
	}

	fileStorage, ok := suite.storage.(*FileStorage)
	if !ok {
		t.Fatal("Failed to cast to FileStorage")
	}

	// Corrupt the state file
	err = os.WriteFile(fileStorage.stateFile(), []byte("not valid json"), 0644)
	if err != nil {
		t.Fatalf("Failed to corrupt state file: %v", err)
	}

	// Attempt to load corrupted state
	_, err = suite.storage.LoadState(suite.ctx)
	if err == nil {
		t.Error("Expected error loading corrupted state, got nil")
	}

	// Corrupt the log file
	logFile, err := os.OpenFile(fileStorage.logFile(), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	_, err = logFile.WriteString("corrupted data")
	if err != nil {
		t.Fatalf("Failed to write corrupted data: %v", err)
	}
	logFile.Close()

	// Attempts to read corrupted log should fail gracefully
	_, err = suite.storage.GetEntries(suite.ctx, 1, 4)
	if err == nil {
		t.Error("Expected error reading corrupted log, got nil")
	}
}

// Memory Storage Tests
func TestMemoryStorage(t *testing.T) {
	t.Run("StatePersistence", func(t *testing.T) {
		testRaftStatePersistence(t, setupStorageTest(t, MemoryStorageType))
	})

	t.Run("LogEntryOperations", func(t *testing.T) {
		testLogEntryOperations(t, setupStorageTest(t, MemoryStorageType))
	})

	t.Run("EntryNotFound", func(t *testing.T) {
		testEntryNotFound(t, setupStorageTest(t, MemoryStorageType))
	})

	t.Run("TruncatePrefix", func(t *testing.T) {
		testTruncatePrefix(t, setupStorageTest(t, MemoryStorageType))
	})

	t.Run("TruncateSuffix", func(t *testing.T) {
		testTruncateSuffix(t, setupStorageTest(t, MemoryStorageType))
	})

	t.Run("EntriesContiguity", func(t *testing.T) {
		testEntriesContiguity(t, setupStorageTest(t, MemoryStorageType))
	})

	t.Run("CancelledContext", func(t *testing.T) {
		testCancelledContext(t, setupStorageTest(t, MemoryStorageType))
	})

	t.Run("EdgeCases", func(t *testing.T) {
		testMemoryStorageEdgeCases(t)
	})
}

// File Storage Tests
func TestFileStorage(t *testing.T) {
	t.Run("StatePersistence", func(t *testing.T) {
		testRaftStatePersistence(t, setupStorageTest(t, FileStorageType))
	})

	t.Run("LogEntryOperations", func(t *testing.T) {
		testLogEntryOperations(t, setupStorageTest(t, FileStorageType))
	})

	t.Run("EntryNotFound", func(t *testing.T) {
		testEntryNotFound(t, setupStorageTest(t, FileStorageType))
	})

	t.Run("TruncatePrefix", func(t *testing.T) {
		testTruncatePrefix(t, setupStorageTest(t, FileStorageType))
	})

	t.Run("TruncateSuffix", func(t *testing.T) {
		testTruncateSuffix(t, setupStorageTest(t, FileStorageType))
	})

	t.Run("EntriesContiguity", func(t *testing.T) {
		testEntriesContiguity(t, setupStorageTest(t, FileStorageType))
	})

	t.Run("CancelledContext", func(t *testing.T) {
		testCancelledContext(t, setupStorageTest(t, FileStorageType))
	})

	t.Run("FileSpecific", func(t *testing.T) {
		testFileStorageSpecific(t)
	})

	t.Run("FileCorruption", func(t *testing.T) {
		testFileCorruption(t)
	})
}

// Test NewStorage factory function
func TestNewStorage(t *testing.T) {
	t.Run("MemoryStorage", func(t *testing.T) {
		storage, err := NewStorage(&StorageConfig{Type: MemoryStorageType})
		if err != nil {
			t.Fatalf("Failed to create memory storage: %v", err)
		}
		_, ok := storage.(*MemoryStorage)
		if !ok {
			t.Error("Storage is not *MemoryStorage")
		}
	})

	t.Run("FileStorage", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "raft-test-*")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tempDir)

		storage, err := NewStorage(&StorageConfig{
			Type: FileStorageType,
			Dir:  tempDir,
		})
		if err != nil {
			t.Fatalf("Failed to create file storage: %v", err)
		}
		_, ok := storage.(*FileStorage)
		if !ok {
			t.Error("Storage is not *FileStorage")
		}
	})

	t.Run("DefaultConfig", func(t *testing.T) {
		storage, err := NewStorage(nil)
		if err != nil {
			t.Fatalf("Failed to create storage with nil config: %v", err)
		}
		_, ok := storage.(*FileStorage)
		if !ok {
			t.Error("Default storage is not *FileStorage")
		}
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		_, err := NewStorage(&StorageConfig{Type: "unsupported"})
		if err == nil {
			t.Error("Expected error for unsupported storage type, got nil")
		}
	})

	t.Run("InvalidDir", func(t *testing.T) {
		// Use a file as directory to trigger an error
		tmpFile, err := os.CreateTemp("", "raft-test-*")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())

		_, err = NewStorage(&StorageConfig{
			Type: FileStorageType,
			Dir:  tmpFile.Name(), // This is a file, not a directory
		})
		if err == nil {
			t.Error("Expected error for invalid directory, got nil")
		}
	})
}

// Test CheckEntriesContiguity helper function
func TestCheckEntriesContiguity(t *testing.T) {
	t.Run("ValidContiguity", func(t *testing.T) {
		entries := createTestLogEntries(6, 3)
		err := CheckEntriesContiguity(entries, 5)
		if err != nil {
			t.Errorf("Valid contiguity check failed: %v", err)
		}
	})

	t.Run("EmptyEntries", func(t *testing.T) {
		err := CheckEntriesContiguity([]*pb.LogEntry{}, 5)
		if err != nil {
			t.Errorf("Empty entries contiguity check failed: %v", err)
		}
	})

	t.Run("NonContiguousWithPrevious", func(t *testing.T) {
		entries := createTestLogEntries(7, 3)
		err := CheckEntriesContiguity(entries, 5)
		if err == nil {
			t.Error("Expected error for non-contiguous with previous, got nil")
		}
	})

	t.Run("NonContiguousWithinEntries", func(t *testing.T) {
		entries := []*pb.LogEntry{
			{Index: 6, Term: 1},
			{Index: 8, Term: 1}, // Gap here
			{Index: 9, Term: 1},
		}
		err := CheckEntriesContiguity(entries, 5)
		if err == nil {
			t.Error("Expected error for non-contiguous within entries, got nil")
		}
	})
}
