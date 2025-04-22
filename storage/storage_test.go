package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func TestNewFileStorage(t *testing.T) {
	t.Run("Creation with default options", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithIndexBounds(0, 0).
			Build()

		testutil.AssertEqual(t, "/test", s.dir)
		checkStorage(t, s, 0, 0, storageStatusReady)
	})

	t.Run("Empty directory error", func(t *testing.T) {
		logger := logger.NewNoOpLogger()
		_, err := NewFileStorage(StorageConfig{Dir: ""}, logger)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "storage directory must be specified")
	})

	t.Run("MkdirAll failure", func(t *testing.T) {
		mockFS := newMockFileSystem()
		mockFS.mkdirAllErr = errors.New("mkdir failed")

		builder := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.fs = mockFS
			})

		_, err := newFileStorageWithDeps(
			builder.cfg,
			builder.opts,
			mockFS,
			builder.deps.serializer,
			builder.deps.logAppender,
			builder.deps.logReader,
			builder.deps.logRewriter,
			builder.deps.indexSvc,
			builder.deps.metadataSvc,
			builder.deps.recoverySvc,
			builder.deps.logger,
		)

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to create storage directory")
	})
}

func TestInitialize(t *testing.T) {
	t.Run("Recovery marker present", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.recoverySvc.recoveryNeeded = true
			}).
			Build()

		s.status.Store(storageStatusInitializing)

		err := s.initialize()

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, deps.recoverySvc.checkForMarkersCalled)
		testutil.AssertTrue(t, deps.recoverySvc.performRecoveryCalled)
		testutil.AssertTrue(t, deps.recoverySvc.createMarkerCalled)
		testutil.AssertTrue(t, deps.recoverySvc.cleanupTempFilesCalled)
		testutil.AssertTrue(t, deps.recoverySvc.removeMarkerCalled)
		checkStorage(t, s, 1, 1, storageStatusReady)
	})

	t.Run("Error checking recovery markers", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.recoverySvc.checkForMarkersErr = errors.New("marker check failed")
			}).
			Build()

		s.status.Store(storageStatusInitializing)

		err := s.initialize()

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed checking recovery markers")
		testutil.AssertTrue(t, deps.recoverySvc.checkForMarkersCalled)
		testutil.AssertFalse(t, deps.recoverySvc.performRecoveryCalled)
	})

	t.Run("Error performing recovery", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.recoverySvc.recoveryNeeded = true
				deps.recoverySvc.performRecoveryErr = errors.New("recovery failed")
			}).
			Build()

		s.status.Store(storageStatusInitializing)

		err := s.initialize()

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "recovery failed")
		testutil.AssertTrue(t, deps.recoverySvc.checkForMarkersCalled)
		testutil.AssertTrue(t, deps.recoverySvc.performRecoveryCalled)
		testutil.AssertFalse(t, deps.recoverySvc.createMarkerCalled)
	})

	t.Run("Error creating recovery marker", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.recoverySvc.createMarkerErr = errors.New("marker creation failed")
			}).
			Build()

		s.status.Store(storageStatusInitializing)

		err := s.initialize()

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to create recovery marker")
		testutil.AssertTrue(t, deps.recoverySvc.checkForMarkersCalled)
		testutil.AssertFalse(t, deps.recoverySvc.performRecoveryCalled)
		testutil.AssertTrue(t, deps.recoverySvc.createMarkerCalled)
	})
}

func TestSaveAndLoadState(t *testing.T) {
	t.Run("SaveState success", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.serializer.marshalStateFunc = func(state types.PersistentState) ([]byte, error) {
					return []byte("serialized-state"), nil
				}
			}).
			Build()

		state := types.PersistentState{CurrentTerm: 5, VotedFor: "node1"}
		err := s.SaveState(context.Background(), state)

		testutil.AssertNoError(t, err)
		path := deps.fs.Join("/test", stateFilename)
		data, err := deps.fs.ReadFile(path)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, "serialized-state", string(data))
	})

	t.Run("SaveState with context cancellation", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		state := types.PersistentState{CurrentTerm: 5, VotedFor: "node1"}
		err := s.SaveState(ctx, state)

		testutil.AssertError(t, err)
		testutil.AssertEqual(t, context.Canceled, err)
	})

	t.Run("SaveState serialization error", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.serializer.marshalStateFunc = func(state types.PersistentState) ([]byte, error) {
					return nil, errors.New("serialization error")
				}
			}).
			Build()

		state := types.PersistentState{CurrentTerm: 5, VotedFor: "node1"}
		err := s.SaveState(context.Background(), state)

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to marshal state")
		testutil.AssertErrorIs(t, err, ErrStorageIO)
	})

	t.Run("LoadState success", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.serializer.unmarshalStateFunc = func(data []byte) (types.PersistentState, error) {
					if string(data) == "serialized-state" {
						return types.PersistentState{CurrentTerm: 5, VotedFor: "node1"}, nil
					}
					return types.PersistentState{}, errors.New("unexpected data")
				}
			}).
			Build()

		path := deps.fs.Join("/test", stateFilename)
		deps.fs.WriteFile(path, []byte("serialized-state"), 0644)

		state, err := s.LoadState(context.Background())

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, types.Term(5), state.CurrentTerm)
		testutil.AssertEqual(t, types.NodeID("node1"), state.VotedFor)
	})

	t.Run("LoadState file not found", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).Build()

		path := deps.fs.Join("/test", stateFilename)
		deps.fs.Remove(path)

		state, err := s.LoadState(context.Background())

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, types.Term(0), state.CurrentTerm)
		testutil.AssertEqual(t, types.NodeID(""), state.VotedFor)
	})

	t.Run("LoadState with context cancellation", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := s.LoadState(ctx)

		testutil.AssertError(t, err)
		testutil.AssertEqual(t, context.Canceled, err)
	})
}

func TestAppendLogEntries(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logAppender.appendResult = appendResult{
					FirstIndex: 2,
					LastIndex:  3,
					Offsets: []types.IndexOffsetPair{
						{Index: 2, Offset: 100},
						{Index: 3, Offset: 200},
					},
				}

				deps.metadataSvc.SyncMetadataFromIndexMapFunc = func(
					path string,
					indexMap []types.IndexOffsetPair,
					currentFirst, currentLast types.Index,
					context string,
					useAtomicWrite bool,
				) (types.Index, types.Index, error) {
					return 1, 3, nil
				}
			}).
			Build()

		entries := []types.LogEntry{
			{Index: 2, Term: 1, Command: []byte("cmd2")},
			{Index: 3, Term: 1, Command: []byte("cmd3")},
		}
		err := s.AppendLogEntries(context.Background(), entries)

		testutil.AssertNoError(t, err)
		verifyAppendCall(t, deps.logAppender, entries, 1)
		checkStorage(t, s, 1, 3, storageStatusReady)
	})

	t.Run("Empty entries", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logAppender.appendErr = ErrEmptyEntries
			}).
			Build()

		err := s.AppendLogEntries(context.Background(), []types.LogEntry{})

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrEmptyEntries)
	})

	t.Run("Out of order entries", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logAppender.appendErr = ErrOutOfOrderEntries
			}).
			Build()

		entries := []types.LogEntry{
			{Index: 3, Term: 1, Command: []byte("cmd3")},
			{Index: 2, Term: 1, Command: []byte("cmd2")},
		}
		err := s.AppendLogEntries(context.Background(), entries)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrOutOfOrderEntries)
	})

	t.Run("Non-contiguous entries", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logAppender.appendErr = ErrNonContiguousEntries
			}).
			Build()

		entries := []types.LogEntry{
			{Index: 3, Term: 1, Command: []byte("cmd3")},
			{Index: 5, Term: 1, Command: []byte("cmd5")},
		}
		err := s.AppendLogEntries(context.Background(), entries)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNonContiguousEntries)
	})

	t.Run("Context canceled", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		entries := []types.LogEntry{
			{Index: 2, Term: 1, Command: []byte("cmd2")},
		}
		err := s.AppendLogEntries(ctx, entries)

		testutil.AssertError(t, err)
		testutil.AssertEqual(t, context.Canceled, err)
	})
}

func TestGetLogEntries(t *testing.T) {
	t.Run("Invalid range", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		_, err := s.GetLogEntries(context.Background(), 5, 5)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrInvalidLogRange)
	})

	t.Run("Range outside of log bounds", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
					return nil, 0, ErrIndexOutOfRange
				}

			}).
			Build()

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		entries, err := s.GetLogEntries(context.Background(), 2, 4)

		testutil.AssertErrorIs(t, err, ErrIndexOutOfRange)
		testutil.AssertEqual(t, 0, len(entries))
		testutil.AssertNotNil(t, entries)
	})

	t.Run("Success with index map", func(t *testing.T) {
		expectedEntries := []types.LogEntry{
			{Index: 6, Term: 2, Command: []byte("cmd6")},
			{Index: 7, Term: 2, Command: []byte("cmd7")},
		}

		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
					testutil.AssertEqual(t, types.Index(6), start)
					testutil.AssertEqual(t, types.Index(8), end)
					return expectedEntries, 200, nil
				}

			}).
			Build()

		s.options.Features.EnableIndexMap = true

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		entries, err := s.GetLogEntries(context.Background(), 6, 8)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 2, len(entries))
		testutil.AssertEqual(t, expectedEntries, entries)
	})

	t.Run("Success with scan", func(t *testing.T) {
		expectedEntries := []types.LogEntry{
			{Index: 8, Term: 3, Command: []byte("cmd8")},
			{Index: 9, Term: 3, Command: []byte("cmd9")},
		}

		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logReader.ScanRangeFunc = func(ctx context.Context, file file, start, end types.Index) ([]types.LogEntry, error) {
					testutil.AssertEqual(t, types.Index(8), start)
					testutil.AssertEqual(t, types.Index(10), end)
					return expectedEntries, nil
				}

			}).
			Build()

		s.options.Features.EnableIndexMap = false

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		entries, err := s.GetLogEntries(context.Background(), 8, 10)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 2, len(entries))
		testutil.AssertEqual(t, expectedEntries, entries)
	})

	t.Run("Context canceled", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := s.GetLogEntries(ctx, 1, 2)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})
}

func TestGetLogEntry(t *testing.T) {
	t.Run("Success with index map", func(t *testing.T) {
		expectedEntry := types.LogEntry{Index: 7, Term: 2, Command: []byte("cmd7")}

		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
					testutil.AssertEqual(t, types.Index(7), start)
					testutil.AssertEqual(t, types.Index(8), end)
					return []types.LogEntry{expectedEntry}, 100, nil
				}

			}).
			Build()

		s.options.Features.EnableIndexMap = true

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		entry, err := s.GetLogEntry(context.Background(), 7)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, expectedEntry, entry)
	})

	t.Run("Success with scan", func(t *testing.T) {
		targetEntry := types.LogEntry{Index: 8, Term: 3, Command: []byte("cmd8")}

		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logReader.AddEntry(types.LogEntry{Index: 5, Term: 2, Command: []byte("cmd5")}, 50)
				deps.logReader.AddEntry(types.LogEntry{Index: 6, Term: 2, Command: []byte("cmd6")}, 50)
				deps.logReader.AddEntry(targetEntry, 50)

			}).
			Build()

		s.options.Features.EnableIndexMap = false

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		entry, err := s.GetLogEntry(context.Background(), 8)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, targetEntry, entry)
	})

	t.Run("Index out of range", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		_, err := s.GetLogEntry(context.Background(), 3)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrEntryNotFound)

		_, err = s.GetLogEntry(context.Background(), 15)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrEntryNotFound)
	})

	t.Run("Context canceled", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := s.GetLogEntry(ctx, 1)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})
}

func TestTruncateLogSuffix(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		expectedEntries := []types.LogEntry{
			{Index: 5, Term: 2, Command: []byte("cmd5")},
			{Index: 6, Term: 2, Command: []byte("cmd6")},
		}
		mockOffsets := []types.IndexOffsetPair{
			{Index: 5, Offset: 0},
			{Index: 6, Offset: 100},
		}
		rewriteCalled := false

		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
					return expectedEntries, 200, nil
				}
				deps.logRewriter.rewriteFunc = func(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error) {
					rewriteCalled = true
					testutil.AssertEqual(t, expectedEntries, entries)
					return mockOffsets, nil
				}
				deps.metadataSvc.SaveMetadataFunc = func(path string, metadata logMetadata, useAtomic bool) error {
					return nil
				}
				deps.metadataSvc.SyncMetadataFromIndexMapFunc = func(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, context string, useAtomicWrite bool) (types.Index, types.Index, error) {
					return 5, 6, nil
				}
				deps.indexSvc.GetBoundsFunc = func(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
					return boundsResult{NewFirst: 5, NewLast: 6}
				}

			}).
			Build()

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)
		s.options.Features.EnableIndexMap = true

		err := s.TruncateLogSuffix(context.Background(), 7)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, rewriteCalled, "Rewrite should have been called")
		checkStorage(t, s, 5, 6, storageStatusReady)
	})

	t.Run("Index out of range - too high", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		err := s.TruncateLogSuffix(context.Background(), 12)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrIndexOutOfRange)
	})

	t.Run("Truncate entire log", func(t *testing.T) {
		rewriteCalled := false

		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logRewriter.rewriteFunc = func(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error) {
					rewriteCalled = true
					testutil.AssertEqual(t, 0, len(entries), "Should truncate everything")
					return []types.IndexOffsetPair{}, nil
				}

			}).
			Build()

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		err := s.TruncateLogSuffix(context.Background(), 3)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, rewriteCalled, "Rewrite should have been called")
		checkStorage(t, s, 0, 0, storageStatusReady)
	})

	t.Run("Rewrite error", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logRewriter.rewriteFunc = func(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error) {
					return nil, errors.New("rewrite error")
				}

			}).
			Build()

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		err := s.TruncateLogSuffix(context.Background(), 7)

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "rewrite error")
	})

	t.Run("Context canceled", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := s.TruncateLogSuffix(ctx, 7)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})
}

func TestTruncateLogPrefix(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		expectedEntries := []types.LogEntry{
			{Index: 8, Term: 3, Command: []byte("cmd8")},
			{Index: 9, Term: 3, Command: []byte("cmd9")},
			{Index: 10, Term: 3, Command: []byte("cmd10")},
		}

		rewriteCalled := false
		mockOffsets := []types.IndexOffsetPair{
			{Index: 8, Offset: 0},
			{Index: 9, Offset: 100},
			{Index: 10, Offset: 200},
		}

		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
					return expectedEntries, 300, nil
				}
				deps.logRewriter.rewriteFunc = func(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error) {
					rewriteCalled = true
					testutil.AssertEqual(t, expectedEntries, entries)
					return mockOffsets, nil
				}

			}).
			Build()

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		err := s.TruncateLogPrefix(context.Background(), 8)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, rewriteCalled, "Rewrite should have been called")
		checkStorage(t, s, 8, 10, storageStatusReady)
	})

	t.Run("No truncation needed", func(t *testing.T) {
		rewriteCalled := false
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logRewriter.rewriteFunc = func(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error) {
					rewriteCalled = true
					return nil, nil
				}

			}).
			Build()

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		err := s.TruncateLogPrefix(context.Background(), 3)

		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, rewriteCalled, "Rewrite should not have been called")
		checkStorage(t, s, 5, 10, storageStatusReady)
	})

	t.Run("Index out of range - too high", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		err := s.TruncateLogPrefix(context.Background(), 12)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrIndexOutOfRange)
	})

	t.Run("Context canceled", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := s.TruncateLogPrefix(ctx, 7)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})
}

func TestSaveAndLoadSnapshot(t *testing.T) {
	t.Run("SaveSnapshot success", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).Build()

		metadata := types.SnapshotMetadata{
			LastIncludedIndex: 100,
			LastIncludedTerm:  5,
		}
		snapshotData := []byte("test-snapshot-data")

		err := s.SaveSnapshot(context.Background(), metadata, snapshotData)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, deps.recoverySvc.createSnapshotMarkerCalled)
		testutil.AssertTrue(t, deps.recoverySvc.removeSnapshotMarkerCalled)
		testutil.AssertEqual(t, metadata, deps.recoverySvc.lastSnapshotMetadata)
		testutil.AssertEqual(t, 1, deps.snapshotWriter.writeCall.callCount)
		testutil.AssertEqual(t, metadata, deps.snapshotWriter.writeCall.metadata)
		testutil.AssertEqual(t, snapshotData, deps.snapshotWriter.writeCall.data)
	})

	t.Run("SaveSnapshot with auto truncate", func(t *testing.T) {
		truncatedEntries := []types.LogEntry{
			{Index: 101, Term: 5, Command: []byte("cmd101")},
			{Index: 102, Term: 5, Command: []byte("cmd102")},
		}

		mockOffsets := []types.IndexOffsetPair{
			{Index: 101, Offset: 0},
			{Index: 102, Offset: 100},
		}

		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
					return truncatedEntries, 200, nil
				}

				deps.logRewriter.rewriteFunc = func(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error) {
					return mockOffsets, nil
				}

			}).
			Build()

		s.options.AutoTruncateOnSnapshot = true

		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(200)

		metadata := types.SnapshotMetadata{
			LastIncludedIndex: 100,
			LastIncludedTerm:  5,
		}
		snapshotData := []byte("test-snapshot-data")

		err := s.SaveSnapshot(context.Background(), metadata, snapshotData)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, deps.recoverySvc.createSnapshotMarkerCalled)
		testutil.AssertTrue(t, deps.recoverySvc.removeSnapshotMarkerCalled)
		testutil.AssertEqual(t, 1, deps.snapshotWriter.writeCall.callCount)
	})

	t.Run("SaveSnapshot context canceled", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := s.SaveSnapshot(ctx, types.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}, []byte("data"))

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})

	t.Run("SaveSnapshot marker creation failure", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.recoverySvc.createSnapshotMarkerErr = errors.New("marker creation failed")

			}).
			Build()

		err := s.SaveSnapshot(context.Background(), types.SnapshotMetadata{}, []byte{})

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "marker creation failed")
	})

	t.Run("SaveSnapshot write failure", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.snapshotWriter.writeErr = errors.New("snapshot write failed")

			}).
			Build()

		err := s.SaveSnapshot(context.Background(), types.SnapshotMetadata{}, []byte{})

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "snapshot write failed")
		testutil.AssertTrue(t, deps.recoverySvc.removeSnapshotMarkerCalled,
			"Should cleanup marker on failure")
	})

	t.Run("LoadSnapshot success", func(t *testing.T) {
		expectedMetadata := types.SnapshotMetadata{
			LastIncludedIndex: 100,
			LastIncludedTerm:  5,
		}
		expectedData := []byte("test-snapshot-data")

		s, deps := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.snapshotReader.readResult.metadata = expectedMetadata
				deps.snapshotReader.readResult.data = expectedData

			}).
			Build()

		metadata, data, err := s.LoadSnapshot(context.Background())

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, expectedMetadata, metadata)
		testutil.AssertEqual(t, expectedData, data)
		testutil.AssertEqual(t, 1, deps.snapshotReader.readCall.callCount)
	})

	t.Run("LoadSnapshot no snapshot", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.snapshotReader.readErr = ErrNoSnapshot

			}).
			Build()

		_, _, err := s.LoadSnapshot(context.Background())

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNoSnapshot)
	})

	t.Run("LoadSnapshot corrupted", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.snapshotReader.readErr = ErrCorruptedSnapshot

			}).
			Build()

		_, _, err := s.LoadSnapshot(context.Background())

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrCorruptedSnapshot)
	})

	t.Run("LoadSnapshot context canceled", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).Build()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := s.LoadSnapshot(ctx)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})
}

func TestClose(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).Build()

		err := s.Close()

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, deps.recoverySvc.removeMarkerCalled)
		checkStorage(t, s, 1, 1, storageStatusClosed) // Status should be closed
	})

	t.Run("Already closed", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).Build()

		_ = s.Close()

		deps.recoverySvc.removeMarkerCalled = false

		err := s.Close()

		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, deps.recoverySvc.removeMarkerCalled,
			"Should not try to remove marker again")
	})

	t.Run("Remove marker error", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.recoverySvc.removeMarkerErr = errors.New("marker removal failed")

			}).
			Build()

		err := s.Close()

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "marker removal failed")
		checkStorage(t, s, 1, 1, storageStatusClosed)
	})
}

func TestInMemoryStateConsistency(t *testing.T) {
	t.Run("verifyInMemoryState success", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.indexSvc.GetBoundsFunc = func(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
					return boundsResult{
						NewFirst: 10,
						NewLast:  20,
						Changed:  false,
					}
				}

			}).
			Build()

		s.firstLogIndex.Store(10)
		s.lastLogIndex.Store(20)

		err := s.verifyInMemoryState()
		testutil.AssertNoError(t, err)
	})

	t.Run("verifyInMemoryState failure", func(t *testing.T) {
		s, _ := newTestStorageBuilder(t).
			WithDeps(func(deps *mockStorageDependencies) {
				deps.indexSvc.GetBoundsFunc = func(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
					return boundsResult{
						NewFirst: 15, // Different from in-memory state
						NewLast:  25, // Different from in-memory state
						Changed:  true,
					}
				}
			}).
			Build()

		s.firstLogIndex.Store(10)
		s.lastLogIndex.Store(20)

		err := s.verifyInMemoryState()
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "in-memory state inconsistent")
	})
}
