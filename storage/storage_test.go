package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func TestNewFileStorageWithOptions(t *testing.T) {
	t.Run("Success with default options", func(t *testing.T) {
		tempDir := t.TempDir()

		fs, err := newFileStorageWithOptions(StorageConfig{Dir: tempDir}, DefaultFileStorageOptions(), logger.NewNoOpLogger())

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, fs)

		fileStorage, ok := fs.(*FileStorage)
		testutil.AssertTrue(t, ok, "Expected *FileStorage type")

		testutil.AssertEqual(t, types.Index(0), fileStorage.FirstLogIndex(), "FirstLogIndex for empty log")
		testutil.AssertEqual(t, types.Index(0), fileStorage.LastLogIndex(), "LastLogIndex for empty log")

		status := fileStorage.status.Load().(storageStatus)
		testutil.AssertEqual(t, storageStatusReady, status)

		err = fs.Close()
		testutil.AssertNoError(t, err, "Close should not fail")
	})

	t.Run("Failure with empty directory", func(t *testing.T) {
		fs, err := newFileStorageWithOptions(StorageConfig{Dir: ""}, DefaultFileStorageOptions(), logger.NewNoOpLogger())

		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs == nil)
		testutil.AssertContains(t, err.Error(), "storage directory must be specified")
	})

	t.Run("Custom options propagation", func(t *testing.T) {
		tempDir := t.TempDir()

		customOpts := DefaultFileStorageOptions()
		customOpts.ChunkSize = 12345
		customOpts.Features.EnableBinaryFormat = false
		customOpts.Features.EnableMetrics = false

		fs, err := newFileStorageWithOptions(StorageConfig{Dir: tempDir}, customOpts, logger.NewNoOpLogger())

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, fs)

		fileStorage, ok := fs.(*FileStorage)
		testutil.AssertTrue(t, ok, "Expected *FileStorage type")

		testutil.AssertEqual(t, 12345, fileStorage.options.ChunkSize)
		testutil.AssertFalse(t, fileStorage.options.Features.EnableBinaryFormat)
		testutil.AssertFalse(t, fileStorage.options.Features.EnableMetrics)

		testutil.AssertEqual(t, types.Index(0), fileStorage.FirstLogIndex(), "FirstLogIndex for empty log (custom opts)")
		testutil.AssertEqual(t, types.Index(0), fileStorage.LastLogIndex(), "LastLogIndex for empty log (custom opts)")
		status := fileStorage.status.Load().(storageStatus)
		testutil.AssertEqual(t, storageStatusReady, status)

		err = fs.Close()
		testutil.AssertNoError(t, err, "Close should not fail")
	})
}

func TestDefaultFileStorageDeps(t *testing.T) {
	t.Run("Valid configuration", func(t *testing.T) {
		tempDir := t.TempDir()
		config := StorageConfig{Dir: tempDir}
		options := DefaultFileStorageOptions()
		log := logger.NewNoOpLogger()

		deps, err := DefaultFileStorageDeps(config, options, log)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, deps.FileSystem)
		testutil.AssertNotNil(t, deps.Serializer)
		testutil.AssertNotNil(t, deps.LogAppender)
		testutil.AssertNotNil(t, deps.LogReader)
		testutil.AssertNotNil(t, deps.LogRewriter)
		testutil.AssertNotNil(t, deps.IndexService)
		testutil.AssertNotNil(t, deps.MetadataService)
		testutil.AssertNotNil(t, deps.RecoveryService)
		testutil.AssertNotNil(t, deps.SystemInfo)
		testutil.AssertEqual(t, log, deps.Logger)

		_, isBinary := deps.Serializer.(binarySerializer)
		testutil.AssertTrue(t, isBinary, "Expected binary serializer with default options")
	})

	t.Run("Empty directory error", func(t *testing.T) {
		config := StorageConfig{Dir: ""}
		options := DefaultFileStorageOptions()
		log := logger.NewNoOpLogger()

		deps, err := DefaultFileStorageDeps(config, options, log)

		testutil.AssertError(t, err)
		testutil.AssertEqual(t, "storage directory must be specified", err.Error())
		testutil.AssertTrue(t, reflect.DeepEqual(deps, fileStorageDeps{}), "Expected empty deps on error")
	})

	t.Run("Serializer selection", func(t *testing.T) {
		tempDir := t.TempDir()
		config := StorageConfig{Dir: tempDir}
		log := logger.NewNoOpLogger()

		// Test with binary format disabled
		options := DefaultFileStorageOptions()
		options.Features.EnableBinaryFormat = false

		deps, err := DefaultFileStorageDeps(config, options, log)
		testutil.AssertNoError(t, err)

		_, isJSON := deps.Serializer.(jsonSerializer)
		testutil.AssertTrue(t, isJSON, "Expected JSON serializer when binary format is disabled")

		// Test with binary format enabled
		options.Features.EnableBinaryFormat = true

		deps, err = DefaultFileStorageDeps(config, options, log)
		testutil.AssertNoError(t, err)

		_, isBinary := deps.Serializer.(binarySerializer)
		testutil.AssertTrue(t, isBinary, "Expected binary serializer when binary format is enabled")
	})
}

func TestNewFileStorage_MkdirError(t *testing.T) {
	deps := newMockStorageDependencies()
	deps.fs.mkdirAllErr = errors.New("cannot create dir")

	_, err := newFileStorageWithDeps(StorageConfig{Dir: "/test"}, DefaultFileStorageOptions(), fileStorageDeps{
		FileSystem: deps.fs,
		Serializer: deps.serializer,
		Logger:     deps.logger,
	})

	testutil.AssertError(t, err)
	testutil.AssertContains(t, err.Error(), "failed to create storage directory")
}

func TestGetSerializer(t *testing.T) {
	jsonSer := getSerializer(false)
	_, isJson := jsonSer.(jsonSerializer)
	testutil.AssertTrue(t, isJson, "Expected jsonSerializer")

	binSer := getSerializer(true)
	_, isBin := binSer.(binarySerializer)
	testutil.AssertTrue(t, isBin, "Expected binarySerializer")
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

func TestNewFileStorageWithDeps(t *testing.T) {
	t.Run("Success with valid deps", func(t *testing.T) {
		tempDir := t.TempDir()
		deps := newMockStorageDependencies()
		options := DefaultFileStorageOptions()

		deps.indexSvc.BuildFunc = func(logPath string) (buildResult, error) {
			return buildResult{
				IndexMap:       []types.IndexOffsetPair{},
				Truncated:      false,
				LastValidIndex: 0,
			}, nil
		}

		fs, err := newFileStorageWithDeps(StorageConfig{Dir: tempDir}, options, fileStorageDeps{
			FileSystem:      deps.fs,
			Serializer:      deps.serializer,
			LogAppender:     deps.logAppender,
			LogReader:       deps.logReader,
			LogRewriter:     deps.logRewriter,
			IndexService:    deps.indexSvc,
			MetadataService: deps.metadataSvc,
			RecoveryService: deps.recoverySvc,
			SystemInfo:      deps.systemInfo,
			Logger:          deps.logger,
		})

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, fs)

		fileStorage := fs.(*FileStorage)
		testutil.AssertEqual(t, tempDir, fileStorage.dir)
		testutil.AssertEqual(t, options, fileStorage.options)

		testutil.AssertNotNil(t, fileStorage.logLocker)
		testutil.AssertNotNil(t, fileStorage.snapshotLocker)
		testutil.AssertNotNil(t, fileStorage.snapshotWriter)
		testutil.AssertNotNil(t, fileStorage.snapshotReader)

		_ = fs.Close()
	})

	t.Run("Error creating directory", func(t *testing.T) {
		deps := newMockStorageDependencies()
		deps.fs.mkdirAllErr = errors.New("failed to create directory")

		fs, err := newFileStorageWithDeps(StorageConfig{Dir: "/test"}, DefaultFileStorageOptions(), fileStorageDeps{
			FileSystem:      deps.fs,
			Serializer:      deps.serializer,
			LogAppender:     deps.logAppender,
			LogReader:       deps.logReader,
			LogRewriter:     deps.logRewriter,
			IndexService:    deps.indexSvc,
			MetadataService: deps.metadataSvc,
			RecoveryService: deps.recoverySvc,
			SystemInfo:      deps.systemInfo,
			Logger:          deps.logger,
		})

		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs == nil)
		testutil.AssertContains(t, err.Error(), "failed to create storage directory")
	})

	t.Run("Initialization failure", func(t *testing.T) {
		tempDir := t.TempDir()
		deps := newMockStorageDependencies()
		deps.recoverySvc.checkForMarkersErr = errors.New("recovery check failed")

		fs, err := newFileStorageWithDeps(StorageConfig{Dir: tempDir}, DefaultFileStorageOptions(), fileStorageDeps{
			FileSystem:      deps.fs,
			Serializer:      deps.serializer,
			LogAppender:     deps.logAppender,
			LogReader:       deps.logReader,
			LogRewriter:     deps.logRewriter,
			IndexService:    deps.indexSvc,
			MetadataService: deps.metadataSvc,
			RecoveryService: deps.recoverySvc,
			SystemInfo:      deps.systemInfo,
			Logger:          deps.logger,
		})

		testutil.AssertError(t, err)
		testutil.AssertTrue(t, fs == nil)
		testutil.AssertContains(t, err.Error(), "failed checking recovery markers")
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

func TestInitialize_CleanupTempFilesError(t *testing.T) {
	s, deps := newTestStorageBuilder(t).
		WithDeps(func(deps *mockStorageDependencies) {
			deps.recoverySvc.cleanupTempFilesErr = errors.New("cleanup failed")
		}).
		Build()

	s.status.Store(storageStatusInitializing)

	err := s.initialize()

	testutil.AssertNoError(t, err)
	testutil.AssertTrue(t, deps.recoverySvc.cleanupTempFilesCalled)
	checkStorage(t, s, 1, 1, storageStatusReady)
}

func TestInitialize_LoadInitialStateError(t *testing.T) {
	expectedErr := errors.New("load state failed badly")
	s, _ := newTestStorageBuilder(t).
		WithDeps(func(deps *mockStorageDependencies) {
			deps.fs.ReadFileFunc = func(name string) ([]byte, error) {
				if name == deps.fs.Join("/test", stateFilename) {
					return nil, expectedErr // Return a generic error, not ErrStorageIO or ErrCorruptedState
				}
				return nil, os.ErrNotExist
			}
		}).
		Build()

	s.status.Store(storageStatusInitializing)

	err := s.initialize()

	testutil.AssertError(t, err)
	testutil.AssertContains(t, err.Error(), "failed to load initial state")
	testutil.AssertTrue(t, errors.Is(err, expectedErr), "Expected the original load error")
}

func TestInitialize_EnsureMetadataError_NonNotExist(t *testing.T) {
	loadMetaErr := errors.New("disk read error")
	s, _ := newTestStorageBuilder(t).
		WithDeps(func(deps *mockStorageDependencies) {
			deps.metadataSvc.LoadMetadataFunc = func(path string) (logMetadata, error) {
				return logMetadata{}, loadMetaErr
			}
		}).
		Build()

	s.status.Store(storageStatusInitializing)

	err := s.initialize()

	testutil.AssertError(t, err)
	testutil.AssertContains(t, err.Error(), "failed to load log metadata")
	testutil.AssertTrue(t, errors.Is(err, loadMetaErr))
}

func TestInitialize_RebuildIndexMapError(t *testing.T) {
	buildIndexErr := errors.New("index build failed")
	s, _ := newTestStorageBuilder(t).
		WithOptions(FileStorageOptions{Features: StorageFeatureFlags{EnableIndexMap: true}}). // Enable Index Map
		WithDeps(func(deps *mockStorageDependencies) {
			deps.indexSvc.BuildFunc = func(logPath string) (buildResult, error) {
				return buildResult{}, buildIndexErr
			}
		}).
		Build()

	s.status.Store(storageStatusInitializing)

	err := s.initialize()

	testutil.AssertError(t, err)
	testutil.AssertContains(t, err.Error(), "failed to build index map")
	testutil.AssertTrue(t, errors.Is(err, buildIndexErr))
}

func TestInitialize_SyncLogStateFromIndexMapError(t *testing.T) {
	syncErr := errors.New("metadata sync failed")
	s, _ := newTestStorageBuilder(t).
		WithOptions(FileStorageOptions{Features: StorageFeatureFlags{EnableIndexMap: true}}). // Enable Index Map
		WithDeps(func(deps *mockStorageDependencies) {
			deps.metadataSvc.SyncMetadataFromIndexMapFunc = func(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, context string, useAtomicWrite bool) (types.Index, types.Index, error) {
				return 0, 0, syncErr
			}
		}).
		Build()

	s.status.Store(storageStatusInitializing)

	err := s.initialize()

	testutil.AssertError(t, err)
	testutil.AssertContains(t, err.Error(), "log consistency check failed")
	testutil.AssertTrue(t, errors.Is(err, syncErr))
}

func TestInitialize_VerifyInMemoryStateError(t *testing.T) {
	s, _ := newTestStorageBuilder(t).
		WithOptions(FileStorageOptions{Features: StorageFeatureFlags{EnableIndexMap: true}}). // Enable Index Map
		WithDeps(func(deps *mockStorageDependencies) {
			// Make GetBounds return different values than the initial state (1, 1)
			deps.indexSvc.GetBoundsFunc = func(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
				return boundsResult{NewFirst: 5, NewLast: 10, Changed: true} // Mismatch with initial (1, 1)
			}
		}).
		Build()

	s.status.Store(storageStatusInitializing)

	err := s.initialize()

	testutil.AssertError(t, err)
	testutil.AssertContains(t, err.Error(), "in-memory state inconsistent: expected (5-10), got (1-1)")
}

func TestMetricsDisabled(t *testing.T) {
	opts := DefaultFileStorageOptions()
	opts.Features.EnableMetrics = false

	s, _ := newTestStorageBuilder(t).
		WithOptions(opts).
		Build()

	_ = s.SaveState(context.Background(), types.PersistentState{CurrentTerm: 1, VotedFor: "node1"})
	_ = s.AppendLogEntries(context.Background(), []types.LogEntry{{Index: 2, Term: 1, Command: []byte("cmd2")}})
	_, _ = s.GetLogEntry(context.Background(), 1) // first=1, last=2 after append
	_ = s.TruncateLogSuffix(context.Background(), 2)
	_ = s.SaveSnapshot(context.Background(), types.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}, []byte("snap"))
	_, _, _ = s.LoadSnapshot(context.Background())
	_ = s.Close()

	metricsMap := s.GetMetrics()
	testutil.AssertTrue(t, metricsMap == nil, "GetMetrics should return nil when disabled")

	summary := s.GetMetricsSummary()
	testutil.AssertEqual(t, "Storage metrics disabled", summary)

	testutil.AssertEqual(t, uint64(0), s.metrics.appendOps.Load(), "FileStorage appendOps metric should be 0 when metrics are disabled")
	testutil.AssertEqual(t, uint64(0), s.metrics.stateOps.Load())
	testutil.AssertEqual(t, uint64(0), s.metrics.readOps.Load())
	testutil.AssertEqual(t, uint64(0), s.metrics.snapshotSaveOps.Load())
	testutil.AssertEqual(t, uint64(0), s.metrics.snapshotLoadOps.Load())
	testutil.AssertEqual(t, uint64(0), s.metrics.truncateSuffixOps.Load())
	testutil.AssertEqual(t, uint64(0), s.metrics.slowOperations.Load())
}

// Test for metrics tracking when enabled
func TestMetricsEnabled(t *testing.T) {
	opts := DefaultFileStorageOptions()
	opts.Features.EnableMetrics = true

	s, _ := newTestStorageBuilder(t).
		WithOptions(opts).
		WithIndexBounds(1, 1).
		WithDeps(func(deps *mockStorageDependencies) {
			deps.logAppender.appendFunc = func(ctx context.Context, entries []types.LogEntry, currentLast types.Index) (appendResult, error) {
				return appendResult{
					FirstIndex: entries[0].Index,
					LastIndex:  entries[len(entries)-1].Index,
					Offsets: []types.IndexOffsetPair{
						{Index: entries[0].Index, Offset: 0},
					},
				}, nil
			}

			deps.serializer.marshalStateFunc = func(state types.PersistentState) ([]byte, error) {
				return []byte("serialized-state"), nil
			}

			deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
				if start == 2 && end == 3 { // Simulate reading 1 entry
					return []types.LogEntry{{Index: 2, Term: 1, Command: []byte("cmd2")}}, 10, nil
				}
				if start == 1 && end == 2 { // Simulate reading for truncation
					return []types.LogEntry{{Index: 1, Term: 1, Command: []byte("cmd1-original")}}, 15, nil
				}
				return []types.LogEntry{}, 0, ErrIndexOutOfRange // Default mock response
			}

			deps.snapshotWriter.writeFunc = func(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
				return nil
			}

			deps.snapshotReader.readFunc = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
				return types.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}, []byte("snap-data"), nil
			}

			deps.logRewriter.rewriteFunc = func(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error) {
				result := make([]types.IndexOffsetPair, len(entries))
				for i, entry := range entries {
					result[i] = types.IndexOffsetPair{Index: entry.Index, Offset: int64(i * 100)} // Mock offset
				}
				return result, nil
			}

			deps.metadataSvc.SyncMetadataFromIndexMapFunc = func(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, context string, useAtomicWrite bool) (types.Index, types.Index, error) {
				if context == "AppendLogEntries" {
					return 1, 2, nil // After appending entry 2
				}
				if context == "TruncateLogSuffix" {
					return 1, 1, nil // After truncating back to index 1
				}
				return currentFirst, currentLast, nil // Default
			}
			deps.indexSvc.GetBoundsFunc = func(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
				if len(indexMap) == 0 { // Simulate truncation result
					return boundsResult{NewFirst: 1, NewLast: 1, Changed: true}
				}
				// Simulate append result
				return boundsResult{NewFirst: 1, NewLast: 2, Changed: true}
			}

		}).
		Build()

	err := s.SaveState(context.Background(), types.PersistentState{CurrentTerm: 1, VotedFor: "node1"})
	testutil.AssertNoError(t, err, "SaveState failed")

	err = s.AppendLogEntries(context.Background(), []types.LogEntry{{Index: 2, Term: 1, Command: []byte("cmd2")}})
	testutil.AssertNoError(t, err, "AppendLogEntries failed")

	_, err = s.GetLogEntry(context.Background(), 2) // Should succeed as lastLogIndex is now 2
	testutil.AssertNoError(t, err, "GetLogEntry failed")

	err = s.TruncateLogSuffix(context.Background(), 2) // Truncate back to index 1 (keep entry 1)
	testutil.AssertNoError(t, err, "TruncateLogSuffix failed")

	err = s.SaveSnapshot(context.Background(), types.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1}, []byte("snap"))
	testutil.AssertNoError(t, err, "SaveSnapshot failed")

	_, _, err = s.LoadSnapshot(context.Background())
	testutil.AssertNoError(t, err, "LoadSnapshot failed")

	metricsMap := s.GetMetrics()
	testutil.AssertNotNil(t, metricsMap, "GetMetrics should return non-nil map")

	// Check that each operation incremented its counter at least once
	testutil.AssertTrue(t, metricsMap["state_ops"] > 0, "state_ops should be > 0")
	testutil.AssertTrue(t, metricsMap["append_ops"] > 0, "append_ops should be > 0")
	testutil.AssertTrue(t, metricsMap["read_ops"] > 0, "read_ops should be > 0")
	testutil.AssertTrue(t, metricsMap["read_entries"] > 0, "read_entries should be > 0")
	testutil.AssertTrue(t, metricsMap["read_bytes"] > 0, "read_bytes should be > 0")
	testutil.AssertTrue(t, metricsMap["truncate_suffix_ops"] > 0, "truncate_suffix_ops should be > 0")
	testutil.AssertTrue(t, metricsMap["snapshot_save_ops"] > 0, "snapshot_save_ops should be > 0")
	testutil.AssertTrue(t, metricsMap["snapshot_load_ops"] > 0, "snapshot_load_ops should be > 0")
	testutil.AssertTrue(t, metricsMap["snapshot_size"] > 0, "snapshot_size should be > 0")

	summary := s.GetMetricsSummary()
	testutil.AssertFalse(t, summary == "Storage metrics disabled", "Summary should contain metrics data")

	s.ResetMetrics()
	resetMetrics := s.GetMetrics()
	testutil.AssertEqual(t, uint64(0), resetMetrics["append_ops"], "append_ops should be reset to 0")
	testutil.AssertEqual(t, uint64(0), resetMetrics["state_ops"], "state_ops should be reset to 0")
	testutil.AssertEqual(t, uint64(0), resetMetrics["read_ops"], "read_ops should be reset to 0")
	testutil.AssertEqual(t, uint64(0), resetMetrics["snapshot_save_ops"], "snapshot_save_ops should be reset to 0")
	testutil.AssertEqual(t, uint64(0), resetMetrics["snapshot_load_ops"], "snapshot_load_ops should be reset to 0")
	testutil.AssertEqual(t, uint64(0), resetMetrics["truncate_suffix_ops"], "truncate_suffix_ops should be reset to 0")

	testutil.AssertEqual(t, uint64(0), resetMetrics["avg_append_latency_us"], "avg_append_latency_us should be reset to 0")
	testutil.AssertEqual(t, uint64(0), resetMetrics["p95_append_latency_us"], "p95_append_latency_us should be reset to 0")
}

// TestFileStorage_IndexMapOption verifies that GetLogEntry uses the index map
// when enabled, and falls back to scanning when disabled.
func TestFileStorage_IndexMapOption(t *testing.T) {
	entry1 := types.LogEntry{Index: 1, Term: 1, Command: []byte("cmd1")}

	t.Run("IndexMapEnabled", func(t *testing.T) {
		opts := DefaultFileStorageOptions()
		opts.Features.EnableIndexMap = true
		s, deps := newTestStorageBuilder(t).
			WithOptions(opts).
			WithIndexBounds(0, 0). // Start empty
			Build()

		deps.logAppender.appendResult = appendResult{FirstIndex: 1, LastIndex: 1, Offsets: []types.IndexOffsetPair{{Index: 1, Offset: 0}}}
		deps.metadataSvc.SyncMetadataFromIndexMapFunc = func(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, opContext string, useAtomicWrite bool) (types.Index, types.Index, error) {
			return 1, 1, nil // Update bounds after append
		}

		err := s.AppendLogEntries(context.Background(), []types.LogEntry{entry1})
		testutil.AssertNoError(t, err, "AppendLogEntries failed")

		s.logMu.RLock() // Need lock to safely access indexToOffsetMap
		mapLen := len(s.indexToOffsetMap)
		s.logMu.RUnlock()
		testutil.AssertEqual(t, 1, mapLen, "Index map should have 1 entry when enabled")

		readInRangeCalled := false
		deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
			testutil.AssertEqual(t, types.Index(1), start, "ReadInRange start index mismatch")
			testutil.AssertEqual(t, types.Index(2), end, "ReadInRange end index mismatch")
			readInRangeCalled = true
			return []types.LogEntry{entry1}, 50, nil // Return the expected entry
		}

		_, err = s.GetLogEntry(context.Background(), 1)

		testutil.AssertNoError(t, err, "GetLogEntry failed")
		testutil.AssertTrue(t, readInRangeCalled, "ReadInRangeFunc should have been called when index map is enabled")
	})

	t.Run("IndexMapDisabled", func(t *testing.T) {
		opts := DefaultFileStorageOptions()
		opts.Features.EnableIndexMap = false
		s, deps := newTestStorageBuilder(t).
			WithOptions(opts).
			WithIndexBounds(0, 0).
			Build()

		deps.logAppender.appendResult = appendResult{FirstIndex: 1, LastIndex: 1, Offsets: []types.IndexOffsetPair{{Index: 1, Offset: 0}}}
		deps.indexSvc.GetBoundsFunc = func(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
			if currentFirst == 0 && currentLast == 0 { // Before append
				return boundsResult{NewFirst: 0, NewLast: 0}
			}
			return boundsResult{NewFirst: 1, NewLast: 1} // After append

		}
		deps.metadataSvc.SaveMetadataFunc = func(path string, metadata logMetadata, useAtomic bool) error {
			testutil.AssertEqual(t, types.Index(1), metadata.FirstIndex)
			testutil.AssertEqual(t, types.Index(1), metadata.LastIndex)
			return nil
		}

		err := s.AppendLogEntries(context.Background(), []types.LogEntry{entry1})
		testutil.AssertNoError(t, err, "AppendLogEntries failed")

		testutil.AssertTrue(t, s.indexToOffsetMap == nil, "Index map should be nil when disabled")
		testutil.AssertEqual(t, types.Index(1), s.FirstLogIndex(), "FirstLogIndex mismatch")
		testutil.AssertEqual(t, types.Index(1), s.LastLogIndex(), "LastLogIndex mismatch")

		readInRangeCalled := false
		deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
			readInRangeCalled = true // This flag should remain false
			return nil, 0, errors.New("ReadInRangeFunc should NOT be called when index map is disabled")
		}

		deps.logReader.AddEntry(entry1, 50)

		retrievedEntry, err := s.GetLogEntry(context.Background(), 1)

		testutil.AssertNoError(t, err, "GetLogEntry failed")
		testutil.AssertEqual(t, entry1, retrievedEntry, "Retrieved entry mismatch")
		testutil.AssertFalse(t, readInRangeCalled, "ReadInRangeFunc should NOT have been called when index map is disabled")
	})
}

// TestAsyncTruncateAfterSnapshot verifies that log truncation occurs asynchronously
// after saving a snapshot when configured.
func TestAsyncTruncateAfterSnapshot(t *testing.T) {
	opts := DefaultFileStorageOptions()
	opts.Features.EnableAsyncTruncation = true
	opts.AutoTruncateOnSnapshot = true
	opts.TruncationTimeout = 100 * time.Millisecond // Use a short timeout for the test

	expectedRemainingEntries := []types.LogEntry{
		{Index: 101, Term: 5, Command: []byte("cmd101")},
		{Index: 102, Term: 5, Command: []byte("cmd102")},
	}

	// Channel to signal when the asynchronous truncation (rewrite) happens
	truncationCompleted := make(chan struct{})

	s, _ := newTestStorageBuilder(t).
		WithOptions(opts).
		WithIndexBounds(5, 200). // Initial bounds
		WithDeps(func(deps *mockStorageDependencies) {
			// Return the entries that should survive truncation
			deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
				if start == 101 {
					return expectedRemainingEntries, 200, nil
				}
				return []types.LogEntry{}, 0, nil
			}

			deps.logRewriter.rewriteFunc = func(ctx context.Context, entriesToRewrite []types.LogEntry) ([]types.IndexOffsetPair, error) {
				testutil.AssertEqual(t, expectedRemainingEntries, entriesToRewrite, "Incorrect entries passed to rewrite")

				close(truncationCompleted) // Signal asynchronous rewrite (truncation) has occurred

				pairs := make([]types.IndexOffsetPair, len(entriesToRewrite))
				for i, entry := range entriesToRewrite {
					pairs[i] = types.IndexOffsetPair{
						Index:  entry.Index,
						Offset: int64(i * 100), // Arbitrary mock offset
					}
				}
				return pairs, nil
			}
		}).
		Build()

	snapshotMetadata := types.SnapshotMetadata{
		LastIncludedIndex: 100, // Truncation should remove entries <= 100
		LastIncludedTerm:  5,
	}
	snapshotData := []byte("test-snapshot-data")

	// Save the snapshot, which should trigger the asynchronous truncation via the logRewriter.Rewrite mock.
	err := s.SaveSnapshot(context.Background(), snapshotMetadata, snapshotData)
	testutil.AssertNoError(t, err, "SaveSnapshot failed")

	// Wait for the asynchronous truncation to complete or time out.
	select {
	case <-truncationCompleted:
		t.Log("Async truncation completed successfully.")
	case <-time.After(500 * time.Millisecond): // Generous timeout for the test
		t.Fatal("Test timed out: Async truncation did not complete")
	}
}

// Test for synchronous truncation after snapshot
func TestSyncTruncateAfterSnapshot(t *testing.T) {
	opts := DefaultFileStorageOptions()
	opts.Features.EnableAsyncTruncation = false // Force synchronous truncation
	opts.AutoTruncateOnSnapshot = true

	var truncationCalled bool
	var mu sync.Mutex // Protect access to truncationCalled

	s, _ := newTestStorageBuilder(t).
		WithOptions(opts).
		WithDeps(func(deps *mockStorageDependencies) {
			// return some entries that would be kept to avoid TruncateLogPrefix exiting early
			deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
				if start == 101 { // Simulate reading entries > 100
					return []types.LogEntry{{Index: 101, Term: 5}}, 100, nil
				}
				return []types.LogEntry{}, 0, nil
			}

			deps.logRewriter.rewriteFunc = func(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error) {
				mu.Lock()
				truncationCalled = true
				mu.Unlock()

				pairs := make([]types.IndexOffsetPair, len(entries))
				for i, entry := range entries {
					pairs[i] = types.IndexOffsetPair{
						Index:  entry.Index,
						Offset: int64(i * 100), // Mock offset
					}
				}
				return pairs, nil // Return mock offsets and nil error
			}
		}).
		Build()

	// Set initial log bounds
	s.firstLogIndex.Store(5)
	s.lastLogIndex.Store(200)

	metadata := types.SnapshotMetadata{
		LastIncludedIndex: 100, // Truncation should occur for index < 101
		LastIncludedTerm:  5,
	}
	snapshotData := []byte("test-snapshot-data")

	// Save snapshot (should trigger sync truncation)
	err := s.SaveSnapshot(context.Background(), metadata, snapshotData)

	testutil.AssertNoError(t, err)

	mu.Lock()
	finalTruncationCalled := truncationCalled
	mu.Unlock()
	testutil.AssertTrue(t, finalTruncationCalled, "Synchronous truncation's rewriteFunc should have been called")
}

// Test for chunked I/O support
func TestChunkedIO(t *testing.T) {
	opts := DefaultFileStorageOptions()
	opts.Features.EnableChunkedIO = true
	opts.ChunkSize = 100 // Small chunk size for testing

	// Mock data larger than chunk size
	largeData := make([]byte, 250)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	var chunkedWriteUsed atomic.Bool
	var chunkedReadUsed atomic.Bool

	s, _ := newTestStorageBuilder(t).
		WithOptions(opts).
		WithDeps(func(deps *mockStorageDependencies) {
			deps.snapshotWriter.writeFunc = func(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
				if len(data) > opts.ChunkSize && opts.Features.EnableChunkedIO {
					chunkedWriteUsed.Store(true)
				}

				return nil // Simulate success
			}

			deps.snapshotReader.readResult.data = largeData
			deps.snapshotReader.readResult.metadata = types.SnapshotMetadata{
				LastIncludedIndex: 100,
				LastIncludedTerm:  5,
			}

			deps.snapshotReader.readFunc = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
				if len(deps.snapshotReader.readResult.data) > opts.ChunkSize && opts.Features.EnableChunkedIO {
					chunkedReadUsed.Store(true)
				}

				return deps.snapshotReader.readResult.metadata, deps.snapshotReader.readResult.data, nil
			}
		}).
		Build()

	metadata := types.SnapshotMetadata{
		LastIncludedIndex: 100,
		LastIncludedTerm:  5,
	}

	err := s.SaveSnapshot(context.Background(), metadata, largeData)
	testutil.AssertNoError(t, err, "SaveSnapshot failed")
	testutil.AssertTrue(t, chunkedWriteUsed.Load(), "Mock writeFunc should have detected conditions for chunked write")

	_, _, err = s.LoadSnapshot(context.Background())
	testutil.AssertNoError(t, err, "LoadSnapshot failed")
	testutil.AssertTrue(t, chunkedReadUsed.Load(), "Mock readFunc should have detected conditions for chunked read")
}

// Test for empty log handling
func TestEmptyLogOperations(t *testing.T) {
	s, _ := newTestStorageBuilder(t).
		WithIndexBounds(0, 0). // Empty log
		Build()

	testutil.AssertEqual(t, types.Index(0), s.FirstLogIndex())
	testutil.AssertEqual(t, types.Index(0), s.LastLogIndex())

	entries, err := s.GetLogEntries(context.Background(), 1, 2)
	testutil.AssertError(t, err)
	testutil.AssertErrorIs(t, err, ErrIndexOutOfRange)
	testutil.AssertEqual(t, 0, len(entries))

	_, err = s.GetLogEntry(context.Background(), 1)
	testutil.AssertError(t, err)
	testutil.AssertErrorIs(t, err, ErrEntryNotFound)

	err = s.TruncateLogPrefix(context.Background(), 1)
	testutil.AssertNoError(t, err, "TruncateLogPrefix on empty log should be no-op")

	err = s.TruncateLogSuffix(context.Background(), 1)
	testutil.AssertNoError(t, err, "TruncateLogSuffix on empty log should either empty the log or be a no-op")
}

// Test for lock timeout behavior
func TestLockTimeout(t *testing.T) {
	opts := DefaultFileStorageOptions()
	opts.Features.EnableLockTimeout = true
	opts.LockTimeout = 1 // 1 second timeout

	s, _ := newTestStorageBuilder(t).
		WithOptions(opts).
		Build()

	// Replace the logLocker with our test locker that simulates lock contention
	logMu := &sync.RWMutex{}
	lockAcquiredCh := make(chan struct{})

	// Keep a write lock during the test to force timeout
	logMu.Lock()
	defer logMu.Unlock()

	testLocker := &defaultRWOperationLocker{
		lock:           logMu,
		logger:         logger.NewNoOpLogger(),
		enableMetrics:  true,
		slowOpsCounter: &s.metrics.slowOperations,
		lockTimeout:    time.Duration(opts.LockTimeout) * time.Second,
		useTimeout:     opts.Features.EnableLockTimeout,
		afterLock:      func() { close(lockAcquiredCh) },
	}

	s.logLocker = testLocker

	errCh := make(chan error)
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// This will block waiting for the lock
		err := s.AppendLogEntries(ctx, []types.LogEntry{{Index: 1, Term: 1, Command: []byte("cmd")}})
		errCh <- err
	}()

	// Wait for timeout or unexpected completion
	select {
	case err := <-errCh:
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "lock acquisition timeout")
	case <-lockAcquiredCh:
		t.Fatal("Lock should not have been acquired")
	case <-time.After(time.Duration(opts.LockTimeout)*time.Second + 100*time.Millisecond):
		t.Fatal("Test took too long - something is wrong with the timeout mechanism")
	}
}

// TestConcurrentOperations verifies basic concurrency safety of appends and reads.
func TestConcurrentOperations(t *testing.T) {
	opts := DefaultFileStorageOptions()
	opts.Features.EnableIndexMap = false

	var mockCurrentLastIndex atomic.Uint64
	mockCurrentLastIndex.Store(0)

	s, _ := newTestStorageBuilder(t).
		WithOptions(opts).
		WithIndexBounds(0, 0).
		WithDeps(func(deps *mockStorageDependencies) {
			deps.logAppender.appendFunc = func(ctx context.Context, entries []types.LogEntry, currentLast types.Index) (appendResult, error) {
				time.Sleep(time.Duration(rand.Intn(5)+1) * time.Millisecond)

				firstAppendIdx := entries[0].Index
				expectedNext := types.Index(mockCurrentLastIndex.Load() + 1)

				if firstAppendIdx != expectedNext {
					return appendResult{}, fmt.Errorf("%w: test mock expected %d, got %d", ErrNonContiguousEntries, expectedNext, firstAppendIdx)
				}

				// Simulate successful append result for the single entry
				newLastIdx := firstAppendIdx
				mockCurrentLastIndex.Store(uint64(newLastIdx))

				return appendResult{
					FirstIndex: firstAppendIdx,
					LastIndex:  newLastIdx,
					Offsets:    []types.IndexOffsetPair{{Index: firstAppendIdx, Offset: int64(firstAppendIdx * 10)}}, // Mock offset
				}, nil
			}

			deps.metadataSvc.SyncMetadataFromIndexMapFunc = func(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, opContext string, useAtomicWrite bool) (types.Index, types.Index, error) {
				newLast := types.Index(mockCurrentLastIndex.Load())
				newFirst := types.Index(1) // log always starts at 1 after first write
				if newLast == 0 {
					newFirst = 0 // Handle empty case
				}
				return newFirst, newLast, nil
			}

			deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
				time.Sleep(time.Duration(rand.Intn(2)+1) * time.Millisecond) // Simulate read work

				currentMockLast := types.Index(mockCurrentLastIndex.Load())
				// Adjust check for empty log or reads starting beyond mock state
				if start == 0 || start > currentMockLast || start >= end {
					if start > currentMockLast+1 { // Allow reading one past the end
						return []types.LogEntry{}, 0, ErrIndexOutOfRange
					}
				}

				effectiveEnd := min(end, currentMockLast+1)
				count := max(int(effectiveEnd-start), 0)

				result := make([]types.LogEntry, 0, count)
				for i := start; i < effectiveEnd; i++ {
					// Only generate if within the actual mock bounds
					if i <= currentMockLast {
						result = append(result, types.LogEntry{
							Index:   i,
							Term:    1,
							Command: fmt.Appendf(nil, "cmd%d", i),
						})
					}
				}
				if len(result) == 0 && start <= currentMockLast+1 {
					if start < end && start > currentMockLast {
						return []types.LogEntry{}, 0, ErrIndexOutOfRange
					}
				}

				return result, int64(len(result) * 10), nil
			}

		}).
		Build()

	const numGoroutines = 5
	const operationsPerGoroutine = 5
	totalExpectedAppends := uint64(numGoroutines * operationsPerGoroutine)

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	errorCh := make(chan error, numGoroutines*operationsPerGoroutine*2)
	var successfulAppends atomic.Uint64

	for i := range numGoroutines {
		go func(writerID int) {
			defer wg.Done()
			for j := range operationsPerGoroutine {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)

				writeAttemptIndex := types.Index(mockCurrentLastIndex.Load() + 1)

				entries := []types.LogEntry{{
					Index:   writeAttemptIndex,
					Term:    1,
					Command: fmt.Appendf(nil, "w%d-op%d", writerID, j),
				}}

				err := s.AppendLogEntries(ctx, entries)
				if err != nil {
					// Only collect unexpected errors
					if !errors.Is(err, ErrNonContiguousEntries) && !errors.Is(err, context.DeadlineExceeded) {
						errorCh <- fmt.Errorf("writer %d, op %d failed unexpectedly: %w", writerID, j, err)
					}
				} else {
					successfulAppends.Add(1)
				}
				cancel()
			}
		}(i)
	}

	// Start reader goroutines
	for i := range numGoroutines {
		go func(readerID int) {
			defer wg.Done()
			for j := range operationsPerGoroutine {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond) // Adjusted timeout

				// Read a random valid range based on storage's current view
				currentLast := s.LastLogIndex()
				start := types.Index(1)
				if currentLast > 0 {
					start = types.Index(rand.Intn(int(currentLast))) + 1
				}
				end := currentLast + 1 // Read up to the last known written index + 1

				if end > start { // Ensure range is valid before attempting read
					_, err := s.GetLogEntries(ctx, start, end)
					// We expect IndexOutOfRange sometimes if reads race ahead of writes slightly
					// or context deadlines. Unexpected errors are problems.
					if err != nil && !errors.Is(err, ErrIndexOutOfRange) && !errors.Is(err, context.DeadlineExceeded) {
						errorCh <- fmt.Errorf("reader %d, op %d failed (last=%d, start=%d, end=%d): %w", readerID, j, currentLast, start, end, err)
					}
				}
				cancel()
				time.Sleep(time.Duration(rand.Intn(3)+1) * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	close(errorCh)

	var unexpectedErrs []error
	for err := range errorCh {
		unexpectedErrs = append(unexpectedErrs, err)
	}

	if len(unexpectedErrs) > 0 {
		for _, err := range unexpectedErrs {
			t.Errorf("Unexpected error during concurrent operations: %v", err)
		}
	}

	finalSuccessfulAppends := types.Index(successfulAppends.Load())
	finalStorageLastIndex := s.LastLogIndex()

	t.Logf("Total Append Attempts: %d", totalExpectedAppends)
	t.Logf("Successful Appends: %d", finalSuccessfulAppends)
	t.Logf("Final Storage Last Index: %d", finalStorageLastIndex)

	testutil.AssertEqual(t, finalSuccessfulAppends, finalStorageLastIndex, "Storage last index should match the count of successful appends")
}

// TestLargeLogEntries verifies handling of entries near and exceeding the size limit.
func TestLargeLogEntries(t *testing.T) {
	t.Run("AppendValidLargeEntry", func(t *testing.T) {
		largeCommand := make([]byte, maxEntrySizeBytes-100) // Just under the limit
		largeEntry := types.LogEntry{
			Index:   1,
			Term:    1,
			Command: largeCommand,
		}

		s, _ := newTestStorageBuilder(t).
			WithIndexBounds(0, 0). // Start empty
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logAppender.appendFunc = func(ctx context.Context, entries []types.LogEntry, currentLast types.Index) (appendResult, error) {
					firstIdx := types.Index(0)
					lastIdx := types.Index(0)
					offsets := make([]types.IndexOffsetPair, 0, len(entries))
					if len(entries) > 0 {
						firstIdx = entries[0].Index
						lastIdx = entries[len(entries)-1].Index
						for i, e := range entries {
							offsets = append(offsets, types.IndexOffsetPair{Index: e.Index, Offset: int64(i * 1000)}) // Mock offsets
						}
					}

					return appendResult{
						FirstIndex: firstIdx,
						LastIndex:  lastIdx,
						Offsets:    offsets,
					}, nil // Return success
				}

				deps.metadataSvc.SyncMetadataFromIndexMapFunc = func(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, opContext string, useAtomicWrite bool) (types.Index, types.Index, error) {
					if len(indexMap) > 0 {
						return indexMap[0].Index, indexMap[len(indexMap)-1].Index, nil
					}

					if !useAtomicWrite && opContext == "AppendLogEntries" {
						return 1, 1, nil // Based on the single largeEntry append
					}
					return 0, 0, nil
				}
			}).
			Build()

		err := s.AppendLogEntries(context.Background(), []types.LogEntry{largeEntry})

		testutil.AssertNoError(t, err, "Should handle large entries within max size")
		testutil.AssertEqual(t, types.Index(1), s.LastLogIndex(), "LastLogIndex should be updated")
	})

	t.Run("RejectTooLargeEntry", func(t *testing.T) {
		tooLargeCommand := make([]byte, maxEntrySizeBytes+100) // Exceeds the limit
		tooLargeEntry := types.LogEntry{
			Index:   1,
			Term:    1,
			Command: tooLargeCommand,
		}

		s, _ := newTestStorageBuilder(t).
			WithIndexBounds(0, 0). // Start empty
			WithDeps(func(deps *mockStorageDependencies) {
				deps.logAppender.appendFunc = func(ctx context.Context, entries []types.LogEntry, currentLast types.Index) (appendResult, error) {
					for _, entry := range entries {
						if len(entry.Command) >= maxEntrySizeBytes {
							return appendResult{}, fmt.Errorf("entry too large: command size %d bytes", len(entry.Command))
						}
					}
					// Should not be reached in this test case if entry is too large
					return appendResult{FirstIndex: 1, LastIndex: 1, Offsets: []types.IndexOffsetPair{{Index: 1, Offset: 0}}}, nil
				}
				// No metadata sync needed as append should fail
			}).
			Build()

		err := s.AppendLogEntries(context.Background(), []types.LogEntry{tooLargeEntry})

		testutil.AssertError(t, err, "Should reject entries larger than max size")
		testutil.AssertContains(t, err.Error(), "entry too large", "Error message mismatch")
	})
}

func TestRollbackInMemoryState(t *testing.T) {
	t.Run("Regular rollback", func(t *testing.T) {
		s := &FileStorage{}
		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		previousLast := types.Index(8)
		isFirstInit := false

		s.rollbackInMemoryState(previousLast, isFirstInit)

		testutil.AssertEqual(t, types.Index(5), s.FirstLogIndex(), "FirstLogIndex should remain unchanged")
		testutil.AssertEqual(t, types.Index(8), s.LastLogIndex(), "LastLogIndex should be rolled back")
	})

	t.Run("First index initialization rollback", func(t *testing.T) {
		s := &FileStorage{}
		s.firstLogIndex.Store(5)
		s.lastLogIndex.Store(10)

		previousLast := types.Index(8)
		isFirstInit := true

		s.rollbackInMemoryState(previousLast, isFirstInit)

		testutil.AssertEqual(t, types.Index(0), s.FirstLogIndex(), "FirstLogIndex should be reset to 0")
		testutil.AssertEqual(t, types.Index(8), s.LastLogIndex(), "LastLogIndex should be rolled back")
	})
}

func TestRollbackIndexMapState(t *testing.T) {
	t.Run("Partial rollback", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).Build()

		s.indexToOffsetMap = []types.IndexOffsetPair{
			{Index: 1, Offset: 0},
			{Index: 2, Offset: 100},
			{Index: 3, Offset: 200},
			{Index: 4, Offset: 300},
			{Index: 5, Offset: 400},
		}

		s.logMu.Lock()
		s.rollbackIndexMapState(2) // Rollback last 2 entries
		mapLen := len(s.indexToOffsetMap)
		mapCopy := make([]types.IndexOffsetPair, mapLen) // for assertion after unlock
		copy(mapCopy, s.indexToOffsetMap)
		s.logMu.Unlock()

		testutil.AssertEqual(t, 3, len(s.indexToOffsetMap), "Should have 3 entries after rollback")
		if mapLen == 3 {
			testutil.AssertEqual(t, types.Index(1), s.indexToOffsetMap[0].Index, "First entry should be index 1")
			testutil.AssertEqual(t, types.Index(3), s.indexToOffsetMap[2].Index, "Last entry should be index 3")
		}

		testutil.AssertTrue(t, deps.indexSvc.truncateLastCalled, "TruncateLast should have been called")
		testutil.AssertEqual(t, 2, deps.indexSvc.truncateLastCount, "TruncateLast should be called with count=2")
	})

	t.Run("Full rollback", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).Build()

		s.indexToOffsetMap = []types.IndexOffsetPair{
			{Index: 1, Offset: 0},
			{Index: 2, Offset: 100},
		}
		s.logMu.Lock()
		s.rollbackIndexMapState(5) // Rollback more entries than that exist
		mapLen := len(s.indexToOffsetMap)
		s.logMu.Unlock()

		testutil.AssertEqual(t, 0, mapLen, "Should have empty index map after full rollback")

		testutil.AssertTrue(t, deps.indexSvc.truncateLastCalled, "TruncateLast should have been called")
		testutil.AssertEqual(t, 5, deps.indexSvc.truncateLastCount, "TruncateLast should be called with count=5")
	})

	t.Run("No rollback needed", func(t *testing.T) {
		s, deps := newTestStorageBuilder(t).Build()

		originalMap := []types.IndexOffsetPair{
			{Index: 1, Offset: 0},
			{Index: 2, Offset: 100},
			{Index: 3, Offset: 200},
		}
		s.indexToOffsetMap = originalMap
		originalMapCopy := make([]types.IndexOffsetPair, len(originalMap))
		copy(originalMapCopy, originalMap)

		s.logMu.Lock()
		s.rollbackIndexMapState(0)
		mapLen := len(s.indexToOffsetMap)
		currentMap := s.indexToOffsetMap
		s.logMu.Unlock()

		// Map should remain unchanged
		testutil.AssertEqual(t, 3, mapLen, "Index map should be unchanged")
		testutil.AssertEqual(t, originalMap, currentMap, "Index map content should be unchanged")

		testutil.AssertTrue(t, deps.indexSvc.truncateLastCalled, "TruncateLast should have been called even with count 0")
		testutil.AssertEqual(t, 0, deps.indexSvc.truncateLastCount, "TruncateLast should be called with count=0")
	})
}
