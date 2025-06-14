package raft

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

func setupLogManager(t *testing.T) (*logManager, *mockStorage, *mockMetrics) {
	storage := newMockStorage()
	metrics := newMockMetrics()
	mu := &sync.RWMutex{}
	isShutdown := &atomic.Bool{}

	deps := Dependencies{
		Storage: storage,
		Metrics: metrics,
		Logger:  &logger.NoOpLogger{},
	}

	lm := NewLogManager(mu, isShutdown, deps, "test-node").(*logManager)
	return lm, storage, metrics
}

func prepareTestEntries(startIndex types.Index, count int, term types.Term) []types.LogEntry {
	entries := make([]types.LogEntry, count)
	for i := range count {
		entries[i] = types.LogEntry{
			Term:    term,
			Index:   startIndex + types.Index(i),
			Command: []byte("test-command"),
		}
	}
	return entries
}

func assertEntriesEqual(t *testing.T, expected, actual []types.LogEntry) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Fatalf("Entry count mismatch: expected %d, got %d", len(expected), len(actual))
	}

	for i := range expected {
		if expected[i].Index != actual[i].Index ||
			expected[i].Term != actual[i].Term {
			t.Fatalf("Entry mismatch at %d: expected (%d,%d), got (%d,%d)",
				i, expected[i].Index, expected[i].Term, actual[i].Index, actual[i].Term)
		}
	}
}

func TestRaftLog_NewLogManager_Panics(t *testing.T) {
	validDeps := Dependencies{
		Storage: newMockStorage(),
		Metrics: newMockMetrics(),
		Logger:  &logger.NoOpLogger{},
	}
	validMu := &sync.RWMutex{}
	validShutdown := &atomic.Bool{}
	validNodeID := types.NodeID("test-node")

	cases := []struct {
		name     string
		mu       *sync.RWMutex
		shutdown *atomic.Bool
		deps     Dependencies
		nodeID   types.NodeID
		wantMsg  string
	}{
		{
			name:     "NilMutex",
			mu:       nil,
			shutdown: validShutdown,
			deps:     validDeps,
			nodeID:   validNodeID,
			wantMsg:  "raft: NewLogManager requires non-nil mutex",
		},
		{
			name:     "NilShutdownFlag",
			mu:       validMu,
			shutdown: nil,
			deps:     validDeps,
			nodeID:   validNodeID,
			wantMsg:  "raft: NewLogManager requires non-nil shutdownFlag",
		},
		{
			name:     "NilStorage",
			mu:       validMu,
			shutdown: validShutdown,
			deps: Dependencies{
				Storage: nil,
				Metrics: validDeps.Metrics,
				Logger:  validDeps.Logger,
			},
			nodeID:  validNodeID,
			wantMsg: "raft: NewLogManager requires non-nil Storage dependency",
		},
		{
			name:     "NilMetrics",
			mu:       validMu,
			shutdown: validShutdown,
			deps: Dependencies{
				Storage: validDeps.Storage,
				Metrics: nil,
				Logger:  validDeps.Logger,
			},
			nodeID:  validNodeID,
			wantMsg: "raft: NewLogManager requires non-nil Metrics dependency",
		},
		{
			name:     "NilLogger",
			mu:       validMu,
			shutdown: validShutdown,
			deps: Dependencies{
				Storage: validDeps.Storage,
				Metrics: validDeps.Metrics,
				Logger:  nil,
			},
			nodeID:  validNodeID,
			wantMsg: "raft: NewLogManager requires non-nil Logger dependency",
		},
		{
			name:     "EmptyNodeID",
			mu:       validMu,
			shutdown: validShutdown,
			deps:     validDeps,
			nodeID:   types.NodeID(""),
			wantMsg:  "raft: NewLogManager requires a non-empty nodeID",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("Expected panic for case %s, but no panic occurred", tc.name)
				}
				if r != tc.wantMsg {
					t.Errorf("Unexpected panic message: got %q, want %q", r, tc.wantMsg)
				}
			}()
			_ = NewLogManager(tc.mu, tc.shutdown, tc.deps, tc.nodeID)
		})
	}
}

func TestRaftLog_LogManager_Initialize_EmptyLog(t *testing.T) {
	lm, _, metrics := setupLogManager(t)

	err := lm.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if lastIdx := lm.GetLastIndexUnsafe(); lastIdx != 0 {
		t.Errorf("Expected last index 0, got %d", lastIdx)
	}

	if lastTerm := lm.GetLastTermUnsafe(); lastTerm != 0 {
		t.Errorf("Expected last term 0, got %d", lastTerm)
	}

	if metrics.logStateCount != 1 {
		t.Errorf("Expected 1 log state update, got %d", metrics.logStateCount)
	}
}

func TestRaftLog_LogManager_Initialize_WithExistingLog(t *testing.T) {
	lm, storage, metrics := setupLogManager(t)

	entries := prepareTestEntries(1, 3, 1)
	err := storage.AppendLogEntries(context.Background(), entries)
	if err != nil {
		t.Fatalf("Failed to prepare test entries: %v", err)
	}

	err = lm.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if lastIdx := lm.GetLastIndexUnsafe(); lastIdx != 3 {
		t.Errorf("Expected last index 3, got %d", lastIdx)
	}

	if lastTerm := lm.GetLastTermUnsafe(); lastTerm != 1 {
		t.Errorf("Expected last term 1, got %d", lastTerm)
	}

	if metrics.logStateCount != 1 {
		t.Errorf("Expected 1 log state update, got %d", metrics.logStateCount)
	}
}

func TestRaftLog_LogManager_Initialize_StorageError(t *testing.T) {
	lm, storage, _ := setupLogManager(t)

	entries := prepareTestEntries(1, 1, 1)
	err := storage.AppendLogEntries(context.Background(), entries)
	if err != nil {
		t.Fatalf("Failed to prepare test entries: %v", err)
	}

	storage.setFailure("GetLogEntry", errors.New("storage error"))

	err = lm.Initialize(context.Background())
	if err == nil {
		t.Fatalf("Initialize should have failed due to storage error")
	}
}

func TestRaftLog_LogManager_GetConsistentLastState(t *testing.T) {
	lm, _, _ := setupLogManager(t)

	idx, term := lm.GetConsistentLastState()
	if idx != 0 || term != 0 {
		t.Errorf("Expected (0,0), got (%d,%d)", idx, term)
	}

	entries := prepareTestEntries(1, 3, 2)
	ctx := context.Background()
	if err := lm.AppendEntries(ctx, entries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	idx, term = lm.GetConsistentLastState()
	if idx != 3 || term != 2 {
		t.Errorf("Expected (3,2), got (%d,%d)", idx, term)
	}
}

func TestRaftLog_LogManager_GetFirstIndex(t *testing.T) {
	lm, _, _ := setupLogManager(t)

	if idx := lm.GetFirstIndex(); idx != 0 {
		t.Errorf("Expected first index 0 for empty log, got %d", idx)
	}

	entries := prepareTestEntries(1, 3, 1)
	ctx := context.Background()
	if err := lm.AppendEntries(ctx, entries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	if idx := lm.GetFirstIndex(); idx != 1 {
		t.Errorf("Expected first index 1, got %d", idx)
	}
}

func TestRaftLog_LogManager_GetTerm(t *testing.T) {
	lm, _, _ := setupLogManager(t)
	ctx := context.Background()

	term, err := lm.GetTerm(ctx, 0)
	if err != nil {
		t.Errorf("GetTerm for index 0 should return 0, not error: %v", err)
	}
	if term != 0 {
		t.Errorf("Expected term 0, got %d", term)
	}

	_, err = lm.GetTerm(ctx, 1)
	if err == nil || !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound for non-existent entry, got: %v", err)
	}

	entries := []types.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 2, Index: 3, Command: []byte("cmd3")},
	}
	if err := lm.AppendEntries(ctx, entries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	for _, e := range entries {
		term, err := lm.GetTerm(ctx, e.Index)
		if err != nil {
			t.Errorf("GetTerm failed for index %d: %v", e.Index, err)
		}
		if term != e.Term {
			t.Errorf("Expected term %d for index %d, got %d", e.Term, e.Index, term)
		}
	}

	_, err = lm.GetTerm(ctx, 4)
	if err == nil || !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound for index 4, got: %v", err)
	}
}

func TestRaftLog_LogManager_GetTerm_ShuttingDown(t *testing.T) {
	lm, _, _ := setupLogManager(t)
	ctx := context.Background()

	lm.isShutdown.Store(true)
	_, err := lm.GetTerm(ctx, 1)
	if !errors.Is(err, ErrShuttingDown) {
		t.Errorf("Expected ErrShuttingDown, got: %v", err)
	}
}

func TestRaftLog_LogManager_GetTerm_ErrorPaths(t *testing.T) {
	lm, store, _ := setupLogManager(t)
	ctx := context.Background()

	entries := prepareTestEntries(1, 3, 1)
	if err := lm.AppendEntries(ctx, entries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	if err := lm.TruncatePrefix(ctx, 3); err != nil { // removes 1 and 2
		t.Fatalf("TruncatePrefix failed: %v", err)
	}

	t.Run("compacted index triggers expected ErrCompacted", func(t *testing.T) {
		_, err := lm.GetTerm(ctx, 1)
		if err == nil || !errors.Is(err, ErrCompacted) {
			t.Errorf("Expected ErrCompacted for index 1, got: %v", err)
		}
	})

	t.Run("non-compacted index triggers fallback storage error", func(t *testing.T) {
		if err := lm.AppendEntries(ctx, prepareTestEntries(4, 1, 2)); err != nil {
			t.Fatalf("AppendEntries failed: %v", err)
		}

		store.setFailure("GetLogEntry", errors.New("simulated storage failure"))
		defer store.clearFailures()

		_, err := lm.GetTerm(ctx, 3)
		if err == nil {
			t.Fatal("Expected storage error for index 3, got nil")
		}
		if !strings.Contains(
			err.Error(),
			"failed to get log entry 3 from storage: simulated storage failure",
		) {
			t.Errorf("Unexpected error for index 3: %v", err)
		}
	})

	t.Run("fallback path triggers ErrNotFound", func(t *testing.T) {
		lm, store, _ := setupLogManager(t)
		ctx := context.Background()

		_ = lm.AppendEntries(ctx, prepareTestEntries(1, 3, 1))    // indices 1–3
		store.setFailure("GetLogEntry", storage.ErrEntryNotFound) // simulate unexpected miss

		_, err := lm.GetTerm(ctx, 2) // 2 is within valid bounds
		if err == nil || !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected fallback ErrNotFound, got: %v", err)
		}

		store.clearFailures()
	})
}

func TestRaftLog_LogManager_GetTermUnsafe(t *testing.T) {
	lm, _, _ := setupLogManager(t)
	ctx := context.Background()

	entries := []types.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 2, Index: 2, Command: []byte("cmd2")},
	}
	if err := lm.AppendEntries(ctx, entries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	for _, e := range entries {
		term, err := lm.GetTermUnsafe(ctx, e.Index)
		if err != nil {
			t.Errorf("GetTermUnsafe failed for index %d: %v", e.Index, err)
		}
		if term != e.Term {
			t.Errorf("Expected term %d for index %d, got %d", e.Term, e.Index, term)
		}
	}
}

func TestRaftLog_LogManager_GetEntries(t *testing.T) {
	lm, _, _ := setupLogManager(t)
	ctx := context.Background()

	t.Run("EmptyLogReturnsNoError", func(t *testing.T) {
		_, err := lm.GetEntries(ctx, 1, 3)
		if err != nil {
			t.Errorf("Expected no error for empty log, got: %v", err)
		}
	})

	testEntries := []types.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 2, Index: 3, Command: []byte("cmd3")},
		{Term: 2, Index: 4, Command: []byte("cmd4")},
		{Term: 3, Index: 5, Command: []byte("cmd5")},
	}

	if err := lm.AppendEntries(ctx, testEntries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	t.Run("ValidRangeSubset", func(t *testing.T) {
		entries, err := lm.GetEntries(ctx, 2, 5)
		if err != nil {
			t.Fatalf("GetEntries failed: %v", err)
		}
		assertEntriesEqual(t, testEntries[1:4], entries)
	})

	t.Run("FullRangeRetrieval", func(t *testing.T) {
		entries, err := lm.GetEntries(ctx, 1, 6)
		if err != nil {
			t.Fatalf("GetEntries failed: %v", err)
		}
		assertEntriesEqual(t, testEntries, entries)
	})

	t.Run("EmptyRangeReturnsEmpty", func(t *testing.T) {
		entries, err := lm.GetEntries(ctx, 3, 3)
		if err != nil || len(entries) != 0 {
			t.Errorf("Expected empty slice, got: %v, err: %v", entries, err)
		}
	})

	t.Run("InvalidStartIndex", func(t *testing.T) {
		_, err := lm.GetEntries(ctx, 0, 2)
		if err == nil {
			t.Errorf("Expected error for start index < first index")
		}
	})

	t.Run("EndBeyondLastIndex", func(t *testing.T) {
		entries, err := lm.GetEntries(ctx, 3, 10)
		if err != nil {
			t.Fatalf("GetEntries failed: %v", err)
		}
		assertEntriesEqual(t, testEntries[2:], entries)
	})
}

func TestRaftLog_LogManager_GetEntries_ShuttingDown(t *testing.T) {
	lm, _, _ := setupLogManager(t)
	ctx := context.Background()

	lm.isShutdown.Store(true)

	_, err := lm.GetEntries(ctx, 1, 2)
	if !errors.Is(err, ErrShuttingDown) {
		t.Errorf("Expected ErrShuttingDown, got: %v", err)
	}
}

func TestRaftLog_LogManager_GetEntries_InconsistentRange(t *testing.T) {
	lm, store, metrics := setupLogManager(t)
	ctx := context.Background()

	entries := []types.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 1, Index: 3, Command: []byte("cmd3")},
	}

	if err := lm.AppendEntries(ctx, entries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	store.hookGetLogEntries = func(start, end types.Index) []types.LogEntry {
		return []types.LogEntry{
			{Index: 2, Term: 1, Command: []byte("bad")},
			{Index: 3, Term: 1, Command: []byte("bad")},
		}
	}
	defer func() { store.hookGetLogEntries = nil }()

	_, err := lm.GetEntries(ctx, 1, 3)
	if err == nil {
		t.Fatalf("Expected error due to inconsistent storage response, got nil")
	}
	if !strings.Contains(err.Error(), "internal storage error") {
		t.Fatalf("Expected internal storage error, got: %v", err)
	}

	if metrics.logConsistencyErr != 1 {
		t.Errorf("Expected logConsistencyErr = 1, got %d", metrics.logConsistencyErr)
	}
}

func TestRaftLog_LogManager_GetEntries_EmptyResultWhenExpectedRange(t *testing.T) {
	lm, store, metrics := setupLogManager(t)
	ctx := context.Background()

	_ = lm.AppendEntries(ctx, []types.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
	})

	store.hookGetLogEntries = func(start, end types.Index) []types.LogEntry {
		return nil
	}
	defer func() { store.hookGetLogEntries = nil }()

	_, err := lm.GetEntries(ctx, 1, 3)
	if err == nil || !strings.Contains(err.Error(), "expected entries in range") {
		t.Errorf("Expected validateEntryRange error, got: %v", err)
	}

	if metrics.logConsistencyErr != 1 {
		t.Errorf("Expected logConsistencyErr to be incremented, got %d", metrics.logConsistencyErr)
	}
}

func TestLogManager_GetEntries_StorageErrors(t *testing.T) {
	lm, store, _ := setupLogManager(t)
	ctx := context.Background()

	err := lm.AppendEntries(ctx, []types.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
	})
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	t.Run("ReturnsNotFoundOnIndexOutOfRange", func(t *testing.T) {
		store.setFailure("GetLogEntries", storage.ErrIndexOutOfRange)
		defer store.clearFailures()

		_, err := lm.GetEntries(ctx, 1, 3)
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("Expected not found error, got: %v", err)
		}
	})

	t.Run("ReturnsGenericErrorOnOtherFailure", func(t *testing.T) {
		store.setFailure("GetLogEntries", errors.New("boom"))
		defer store.clearFailures()

		_, err := lm.GetEntries(ctx, 1, 3)
		if err == nil || !strings.Contains(err.Error(), "failed to retrieve") {
			t.Errorf("Expected retrieval failure, got: %v", err)
		}
	})
}

func TestRaftLog_LogManager_AppendEntries(t *testing.T) {
	lm, storage, metrics := setupLogManager(t)
	ctx := context.Background()

	t.Run("AppendEmptySlice", func(t *testing.T) {
		err := lm.AppendEntries(ctx, []types.LogEntry{})
		if err != nil {
			t.Errorf("AppendEntries should succeed with empty slice: %v", err)
		}
		metrics.logAppendCount = 0 // Reset for next test
	})

	t.Run("AppendInitialEntries", func(t *testing.T) {
		entries := prepareTestEntries(1, 3, 1)
		err := lm.AppendEntries(ctx, entries)
		if err != nil {
			t.Fatalf("AppendEntries failed: %v", err)
		}
		if got := lm.GetLastIndexUnsafe(); got != 3 {
			t.Errorf("Expected last index 3, got %d", got)
		}
		if metrics.logAppendCount != 1 {
			t.Errorf("Expected 1 log append, got %d", metrics.logAppendCount)
		}
	})

	t.Run("AppendContiguousEntries", func(t *testing.T) {
		entries := prepareTestEntries(4, 2, 2)
		err := lm.AppendEntries(ctx, entries)
		if err != nil {
			t.Fatalf("AppendEntries failed: %v", err)
		}
		if got := lm.GetLastIndexUnsafe(); got != 5 {
			t.Errorf("Expected last index 5, got %d", got)
		}
		if got := lm.GetLastTermUnsafe(); got != 2 {
			t.Errorf("Expected last term 2, got %d", got)
		}
	})

	t.Run("VerifyAllEntries", func(t *testing.T) {
		expected := append(
			prepareTestEntries(1, 3, 1),
			prepareTestEntries(4, 2, 2)...,
		)
		entries, err := lm.GetEntries(ctx, 1, 6)
		if err != nil {
			t.Fatalf("GetEntries failed: %v", err)
		}
		assertEntriesEqual(t, expected, entries)
	})

	t.Run("AppendNonContiguousEntriesFails", func(t *testing.T) {
		nonContiguous := prepareTestEntries(7, 2, 3) // Skips index 6
		err := lm.AppendEntries(ctx, nonContiguous)
		if err == nil {
			t.Errorf("Expected error for non-contiguous entries, got nil")
		}
	})

	t.Run("StorageFailureIsHandled", func(t *testing.T) {
		storage.setFailure("AppendLogEntries", errors.New("storage error"))
		defer storage.clearFailures()

		err := lm.AppendEntries(ctx, prepareTestEntries(6, 1, 2))
		if err == nil {
			t.Errorf("Expected error when storage fails")
		}
	})
}

func TestRaftLog_LogManager_AppendEntries_ShuttingDown(t *testing.T) {
	lm, _, _ := setupLogManager(t)
	ctx := context.Background()

	lm.isShutdown.Store(true)

	err := lm.AppendEntries(ctx, []types.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
	})
	if !errors.Is(err, ErrShuttingDown) {
		t.Errorf("Expected ErrShuttingDown, got: %v", err)
	}
}

func TestRaftLog_LogManager_TruncatePrefix(t *testing.T) {
	lm, storage, metrics := setupLogManager(t)
	ctx := context.Background()

	entries := prepareTestEntries(1, 5, 1)
	if err := lm.AppendEntries(ctx, entries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	t.Run("RejectsInvalidIndexZero", func(t *testing.T) {
		err := lm.TruncatePrefix(ctx, 0)
		if err == nil {
			t.Errorf("Expected error for index 0")
		}
	})

	t.Run("TruncatesFromMiddle", func(t *testing.T) {
		metrics.logTruncateCount = 0

		err := lm.TruncatePrefix(ctx, 3)
		if err != nil {
			t.Fatalf("TruncatePrefix failed: %v", err)
		}

		if metrics.logTruncateCount != 1 {
			t.Errorf("Expected 1 log truncate, got %d", metrics.logTruncateCount)
		}

		if idx := lm.GetFirstIndex(); idx != 3 {
			t.Errorf("Expected first index 3 after truncation, got %d", idx)
		}

		t.Run("TruncatedEntriesAreInaccessible", func(t *testing.T) {
			_, err := lm.GetEntries(ctx, 1, 3)
			if err == nil || !errors.Is(err, ErrCompacted) {
				t.Errorf("Expected ErrCompacted for truncated entries, got: %v", err)
			}
		})
	})

	t.Run("TruncatingAlreadyTruncatedIndexIsNoop", func(t *testing.T) {
		metrics.logTruncateCount = 0

		err := lm.TruncatePrefix(ctx, 2) // Already compacted
		if err != nil {
			t.Errorf("Expected no error for already-truncated index, got: %v", err)
		}

		if metrics.logTruncateCount != 1 {
			t.Errorf(
				"Expected 1 log truncate metric even for no-op, got %d",
				metrics.logTruncateCount,
			)
		}
	})

	t.Run("RejectsTruncateBeyondLastIndex", func(t *testing.T) {
		err := lm.TruncatePrefix(ctx, 10)
		if err == nil {
			t.Errorf("Expected error for index > lastIndex+1")
		}
	})

	t.Run("HandlesStorageError", func(t *testing.T) {
		storage.setFailure("TruncateLogPrefix", errors.New("storage error"))
		defer storage.clearFailures()

		err := lm.TruncatePrefix(ctx, 4)
		if err == nil {
			t.Errorf("Expected error when storage fails")
		}
	})

	t.Run("RejectsTruncateWhenShuttingDown", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()

		lm.isShutdown.Store(true)

		err := lm.TruncatePrefix(ctx, 5)
		if !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected ErrShuttingDown, got: %v", err)
		}
	})
}

func TestRaftLog_LogManager_TruncateSuffix(t *testing.T) {
	lm, storage, metrics := setupLogManager(t)
	ctx := context.Background()

	entries := prepareTestEntries(1, 5, 1)
	if err := lm.AppendEntries(ctx, entries); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	t.Run("TruncatesFromMiddle", func(t *testing.T) {
		metrics.logTruncateCount = 0

		err := lm.TruncateSuffix(ctx, 3)
		if err != nil {
			t.Fatalf("TruncateSuffix failed: %v", err)
		}

		if got := lm.GetLastIndexUnsafe(); got != 2 {
			t.Errorf("Expected last index 2 after truncation, got %d", got)
		}

		if metrics.logTruncateCount != 1 {
			t.Errorf("Expected 1 log truncate, got %d", metrics.logTruncateCount)
		}

		t.Run("TruncatedEntriesAreGone", func(t *testing.T) {
			entries, err := lm.GetEntries(ctx, 3, 6)
			if err != nil {
				t.Errorf("Expected no error for truncated range, got: %v", err)
			}
			if len(entries) != 0 {
				t.Errorf("Expected 0 entries after truncation, got: %d", len(entries))
			}
		})

		t.Run("RemainingEntriesAreIntact", func(t *testing.T) {
			remaining, err := lm.GetEntries(ctx, 1, 3)
			if err != nil {
				t.Fatalf("GetEntries failed: %v", err)
			}
			assertEntriesEqual(t, entries[:2], remaining)
		})
	})

	t.Run("TruncatingPastLastIndexIsNoop", func(t *testing.T) {
		err := lm.TruncateSuffix(ctx, 5) // lastIndex = 2, so this is > last
		if err != nil {
			t.Errorf("TruncateSuffix with index > lastIndex should succeed: %v", err)
		}
	})

	t.Run("HandlesStorageError", func(t *testing.T) {
		storage.setFailure("TruncateLogSuffix", errors.New("storage error"))
		defer storage.clearFailures()

		err := lm.TruncateSuffix(ctx, 2)
		if err == nil {
			t.Errorf("Expected error when storage fails")
		}
	})

	t.Run("HandlesEmptyLogCase", func(t *testing.T) {
		if err := lm.TruncateSuffix(ctx, 1); err != nil {
			t.Fatalf("TruncateSuffix failed: %v", err)
		}

		if got := lm.GetLastIndexUnsafe(); got != 0 {
			t.Errorf("Expected last index 0 for empty log, got %d", got)
		}

		err := lm.TruncateSuffix(ctx, 1)
		if err != nil {
			t.Errorf("TruncateSuffix on empty log should succeed: %v", err)
		}
	})

	t.Run("RejectsTruncateSuffixWhenShuttingDown", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()

		lm.isShutdown.Store(true)

		err := lm.TruncateSuffix(ctx, 3)
		if !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected ErrShuttingDown, got: %v", err)
		}
	})

	t.Run("HandlesFailureToFetchTermAfterSuffixTruncation", func(t *testing.T) {
		lm, store, metrics := setupLogManager(t)
		ctx := context.Background()

		_ = lm.AppendEntries(ctx, prepareTestEntries(1, 5, 1))

		store.setFailure("GetLogEntry", errors.New("fetch term error"))

		err := lm.TruncateSuffix(ctx, 3)
		if err == nil || !strings.Contains(err.Error(), "failed to fetch new last term") {
			t.Errorf("Expected fetch term failure after truncation, got: %v", err)
		}

		if metrics.logConsistencyErr != 1 {
			t.Errorf("Expected log consistency error to be recorded")
		}

		store.clearFailures()
	})

	t.Run("FailsWhenLastIndexIncreasesAfterTruncation", func(t *testing.T) {

		lm, store, metrics := setupLogManager(t)
		ctx := context.Background()

		_ = lm.AppendEntries(ctx, prepareTestEntries(1, 5, 1)) // real lastIndex = 5

		oldLastIndex := lm.GetLastIndexUnsafe()

		store.hookLastLogIndex = func() types.Index {
			return oldLastIndex + 2 // simulate last index grew after truncation
		}
		defer func() { store.hookLastLogIndex = nil }()

		err := lm.TruncateSuffix(ctx, 3) // should truncate [3, 4, 5]

		if err == nil || !strings.Contains(err.Error(), "inconsistent state") {
			t.Fatalf("Expected truncation inconsistency error, got: %v", err)
		}

		if metrics.logConsistencyErr != 1 {
			t.Errorf(
				"Expected log consistency error to be recorded, got %d",
				metrics.logConsistencyErr,
			)
		}
	})

}
func TestRaftLog_LogManager_IsConsistentWithStorage(t *testing.T) {
	t.Run("EmptyLogIsConsistent", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()

		consistent, err := lm.IsConsistentWithStorage(ctx)
		if err != nil {
			t.Fatalf("IsConsistentWithStorage failed: %v", err)
		}
		if !consistent {
			t.Errorf("Empty log should be consistent with storage")
		}
	})

	t.Run("DetectsTermMismatchForEmptyLog", func(t *testing.T) {
		lm, _, metrics := setupLogManager(t)
		ctx := context.Background()

		// Inject memory corruption: term set while log is still empty
		lm.lastIndex.Store(0)
		lm.lastTerm.Store(99)

		consistent, err := lm.IsConsistentWithStorage(ctx)
		if err == nil || consistent {
			t.Errorf(
				"Expected inconsistency due to term mismatch for empty log, got: consistent=%v, err=%v",
				consistent,
				err,
			)
		}

		if metrics.logConsistencyErr != 1 {
			t.Errorf("Expected 1 log consistency error, got %d", metrics.logConsistencyErr)
		}
	})

	t.Run("DetectsIndexMismatch", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()

		_ = lm.AppendEntries(ctx, prepareTestEntries(1, 3, 1)) // real lastIndex = 3
		lm.lastIndex.Store(10)                                 // corrupt in-memory index

		consistent, err := lm.IsConsistentWithStorage(ctx)
		if err == nil || consistent {
			t.Errorf(
				"Expected inconsistency due to index mismatch, got: consistent=%v, err=%v",
				consistent,
				err,
			)
		}
	})

	t.Run("DetectsTermMismatch", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()

		entries := prepareTestEntries(1, 3, 1)
		_ = lm.AppendEntries(ctx, entries)

		lm.lastIndex.Store(uint64(entries[len(entries)-1].Index)) // correct index
		lm.lastTerm.Store(99)                                     // incorrect term

		consistent, err := lm.IsConsistentWithStorage(ctx)
		if err == nil || consistent {
			t.Errorf("Expected inconsistency due to term mismatch")
		}
	})

	t.Run("HandlesStorageFailure", func(t *testing.T) {
		lm, store, _ := setupLogManager(t)
		ctx := context.Background()

		entries := prepareTestEntries(1, 3, 1)
		_ = lm.AppendEntries(ctx, entries)

		store.setFailure("GetLogEntry", errors.New("mock storage error"))

		_, err := lm.IsConsistentWithStorage(ctx)
		if err == nil {
			t.Errorf("Expected error when storage fails")
		}
	})

	t.Run("LogIsConsistentAfterAppend", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()

		entries := prepareTestEntries(1, 3, 1) // entries 1–3, term = 1
		if err := lm.AppendEntries(ctx, entries); err != nil {
			t.Fatalf("AppendEntries failed: %v", err)
		}

		last := entries[len(entries)-1]

		lm.lastIndex.Store(uint64(last.Index))
		lm.lastTerm.Store(uint64(last.Term))

		consistent, err := lm.IsConsistentWithStorage(ctx)
		if err != nil {
			t.Fatalf("IsConsistentWithStorage failed: %v", err)
		}
		if !consistent {
			t.Errorf("Expected consistent state, got inconsistent")
		}
	})
}

func TestRaftLog_LogManager_RebuildInMemoryState(t *testing.T) {
	t.Run("HandlesEmptyLog", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()

		err := lm.RebuildInMemoryState(ctx)
		if err != nil {
			t.Fatalf("RebuildInMemoryState failed on empty log: %v", err)
		}

		if got := lm.GetLastIndexUnsafe(); got != 0 {
			t.Errorf("Expected last index 0 for empty log, got %d", got)
		}
	})

	t.Run("RebuildsCorruptedState", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()

		entries := prepareTestEntries(1, 3, 2)
		if err := lm.AppendEntries(ctx, entries); err != nil {
			t.Fatalf("AppendEntries failed: %v", err)
		}

		// Corrupt in-memory state
		lm.lastIndex.Store(10)
		lm.lastTerm.Store(99)

		err := lm.RebuildInMemoryState(ctx)
		if err != nil {
			t.Fatalf("RebuildInMemoryState failed after corruption: %v", err)
		}

		if got := lm.GetLastIndexUnsafe(); got != 3 {
			t.Errorf("Expected last index 3 after rebuild, got %d", got)
		}

		if got := lm.GetLastTermUnsafe(); got != 2 {
			t.Errorf("Expected last term 2 after rebuild, got %d", got)
		}
	})

	t.Run("FailsOnStorageError", func(t *testing.T) {
		lm, store, _ := setupLogManager(t)
		ctx := context.Background()

		_ = lm.AppendEntries(ctx, prepareTestEntries(1, 2, 1))

		store.setFailure("GetLogEntry", errors.New("mock storage failure"))
		defer store.clearFailures()

		err := lm.RebuildInMemoryState(ctx)
		if err == nil {
			t.Errorf("Expected error from storage failure, got nil")
		}
	})
}

func TestRaftLog_LogManager_FindLastEntryWithTermUnsafe(t *testing.T) {
	lm, _, _ := setupLogManager(t)
	ctx := context.Background()

	entries := []types.LogEntry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2},
		{Term: 2, Index: 3}, {Term: 2, Index: 4},
		{Term: 3, Index: 5}, {Term: 3, Index: 6},
		{Term: 4, Index: 7},
	}
	_ = lm.AppendEntries(ctx, entries)

	t.Run("FindLastForTerm1", func(t *testing.T) {
		idx, err := lm.FindLastEntryWithTermUnsafe(ctx, 1, 0)
		if err != nil || idx != 2 {
			t.Errorf("Expected index 2 for term 1, got %d (err: %v)", idx, err)
		}
	})

	t.Run("FindLastForTerm2", func(t *testing.T) {
		idx, err := lm.FindLastEntryWithTermUnsafe(ctx, 2, 0)
		if err != nil || idx != 4 {
			t.Errorf("Expected index 4 for term 2, got %d (err: %v)", idx, err)
		}
	})

	t.Run("FindLastForTerm3WithHint", func(t *testing.T) {
		idx, err := lm.FindLastEntryWithTermUnsafe(ctx, 3, 7)
		if err != nil || idx != 6 {
			t.Errorf("Expected index 6 for term 3, got %d (err: %v)", idx, err)
		}
	})

	t.Run("TermNotFound", func(t *testing.T) {
		_, err := lm.FindLastEntryWithTermUnsafe(ctx, 5, 0)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for missing term, got: %v", err)
		}
	})

	t.Run("InvalidTermZero", func(t *testing.T) {
		_, err := lm.FindLastEntryWithTermUnsafe(ctx, 0, 0)
		if err == nil {
			t.Errorf("Expected error for invalid term 0")
		}
	})

	t.Run("FailsIfShuttingDown", func(t *testing.T) {
		lm.isShutdown.Store(true)
		_, err := lm.FindLastEntryWithTermUnsafe(ctx, 1, 0)
		if !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected ErrShuttingDown, got: %v", err)
		}
	})
}

func TestRaftLog_LogManager_FindFirstIndexInTermUnsafe(t *testing.T) {
	lm, _, _ := setupLogManager(t)
	ctx := context.Background()

	entries := []types.LogEntry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2},
		{Term: 2, Index: 3}, {Term: 2, Index: 4},
		{Term: 3, Index: 5}, {Term: 3, Index: 6},
		{Term: 4, Index: 7},
	}
	_ = lm.AppendEntries(ctx, entries)

	t.Run("FirstForTerm1", func(t *testing.T) {
		idx, err := lm.FindFirstIndexInTermUnsafe(ctx, 1, 2)
		if err != nil || idx != 1 {
			t.Errorf("Expected index 1 for term 1, got %d (err: %v)", idx, err)
		}
	})

	t.Run("FirstForTerm2", func(t *testing.T) {
		idx, err := lm.FindFirstIndexInTermUnsafe(ctx, 2, 4) // OK: term 2 spans 3-4
		if err != nil || idx != 3 {
			t.Errorf("Expected index 3 for term 2, got %d (err: %v)", idx, err)
		}
	})

	t.Run("FirstForTerm3", func(t *testing.T) {
		idx, err := lm.FindFirstIndexInTermUnsafe(ctx, 3, 6) // OK: term 3 spans 5-6
		if err != nil || idx != 5 {
			t.Errorf("Expected index 5 for term 3, got %d (err: %v)", idx, err)
		}
	})

	t.Run("MissingTerm", func(t *testing.T) {
		_, err := lm.FindFirstIndexInTermUnsafe(ctx, 5, 7) // Term 5 doesn't exist
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for term 5, got: %v", err)
		}
	})

	t.Run("InvalidTermZero", func(t *testing.T) {
		_, err := lm.FindFirstIndexInTermUnsafe(ctx, 0, 7)
		if err == nil {
			t.Errorf("Expected error for invalid term 0")
		}
	})

	t.Run("SearchBeforeFirstIndex", func(t *testing.T) {
		_, err := lm.FindFirstIndexInTermUnsafe(ctx, 1, 0)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for out-of-bounds search, got: %v", err)
		}
	})

	t.Run("ReturnsErrShuttingDown", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()
		lm.isShutdown.Store(true)
		_, err := lm.FindFirstIndexInTermUnsafe(ctx, 1, 1)
		if !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected ErrShuttingDown, got %v", err)
		}
	})

	t.Run("Hard error after finding index", func(t *testing.T) {
		lm, store, _ := setupLogManager(t)
		ctx := context.Background()

		entries := prepareTestEntries(1, 3, 1) // Same term
		_ = lm.AppendEntries(ctx, entries)

		store.setFailure("GetLogEntry", errors.New("hard failure"))
		defer store.clearFailures()

		store.hookGetLogEntry = func(index types.Index) {
			if index == 1 {
				store.clearFailures() // Let it pass for index 1
			} else {
				store.setFailure("GetLogEntry", errors.New("hard failure"))
			}
		}

		_, err := lm.FindFirstIndexInTermUnsafe(ctx, 1, 3)
		if err == nil || !strings.Contains(err.Error(), "GetTermUnsafe") ||
			!strings.Contains(err.Error(), "hard failure") {
			t.Errorf("Expected wrapped hard failure error after term block, got: %v", err)
		}
	})

	t.Run("ScanWithoutErrorButNoTermMatch", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)
		ctx := context.Background()

		entries := prepareTestEntries(1, 5, 1)
		_ = lm.AppendEntries(ctx, entries)

		_, err := lm.FindFirstIndexInTermUnsafe(ctx, 2, 5) // Term 2 does not exist
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound when scan finds no match, got: %v", err)
		}
	})

	t.Run("ScanContextCancelled", func(t *testing.T) {
		lm, _, _ := setupLogManager(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := lm.FindFirstIndexInTermUnsafe(ctx, 1, 1)

		if err == nil || !(errors.Is(err, context.Canceled) || errors.Is(err, ErrNotFound)) {
			t.Errorf("Expected context cancellation or not found, got: %v", err)
		}
	})

	t.Run("ScanTermError", func(t *testing.T) {
		lm, store, _ := setupLogManager(t)
		ctx := context.Background()

		_ = lm.AppendEntries(ctx, prepareTestEntries(1, 2, 1))
		store.setFailure("GetLogEntry", errors.New("simulated failure"))
		defer store.clearFailures()

		_, err := lm.FindFirstIndexInTermUnsafe(ctx, 1, 2)
		if err == nil || !strings.Contains(err.Error(), "term scan failed") {
			t.Errorf("Expected term scan failure, got: %v", err)
		}
	})

}

func TestRaftLog_LogManager_Stop(t *testing.T) {
	lm, _, _ := setupLogManager(t)

	lm.Stop()
	lm.Stop() // Should not panic

	if !lm.isShutdown.Load() {
		t.Errorf("isShutdown flag should be set after Stop()")
	}
}
