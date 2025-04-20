package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

// TestSnapshotWriterSuccess tests the basic success path for the snapshot writer
func TestSnapshotWriterSuccess(t *testing.T) {
	log := &logger.NoOpLogger{}
	fs := newMockFileSystem()
	serializer := newJsonSerializer()
	dir := "/test/snapshot"
	fs.dir = dir

	tempFilesWritten := false
	metadataCommitted := false
	hooks := &snapshotWriteHooks{
		OnTempFilesWritten: func() {
			tempFilesWritten = true
		},
		OnMetadataCommitted: func() {
			metadataCommitted = true
		},
	}

	writer := newSnapshotWriter(fs, serializer, log, dir, hooks, false, 0)

	metadata := types.SnapshotMetadata{
		LastIncludedIndex: 100,
		LastIncludedTerm:  5,
	}

	snapshotData := []byte("test snapshot data")

	err := writer.Write(context.Background(), metadata, snapshotData)
	testutil.AssertNoError(t, err)

	testutil.AssertTrue(t, tempFilesWritten)
	testutil.AssertTrue(t, metadataCommitted)

	metaPath := fs.Path(dir, snapshotMetaFilename)
	dataPath := fs.Path(dir, snapshotDataFilename)
	exists, err := fs.Exists(metaPath)
	testutil.AssertNoError(t, err)
	testutil.AssertTrue(t, exists)
	exists, err = fs.Exists(dataPath)
	testutil.AssertNoError(t, err)
	testutil.AssertTrue(t, exists)

	metaBytes, err := fs.ReadFile(metaPath)
	testutil.AssertNoError(t, err)
	parsedMeta, err := serializer.UnmarshalSnapshotMetadata(metaBytes)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, metadata, parsedMeta)
	dataBytes, err := fs.ReadFile(dataPath)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, snapshotData, dataBytes)
}

// TestSnapshotWriterChunkedIO tests that the writer correctly handles chunked I/O for large snapshots
func TestSnapshotWriterChunkedIO(t *testing.T) {
	log := &logger.NoOpLogger{}
	fs := newMockFileSystem()
	serializer := newJsonSerializer()
	dir := "/test/snapshot"
	fs.dir = dir

	metaPath := fs.Path(dir, snapshotMetaFilename)
	dataPath := fs.Path(dir, snapshotDataFilename)
	dataTmpPath := fs.TempPath(dataPath)

	chunkSize := 1024
	snapshotSize := chunkSize*3 + 100

	largeData := make([]byte, snapshotSize)
	for i := range snapshotSize {
		largeData[i] = byte(i % 256)
	}

	var writtenChunks [][]byte

	fs.WriteFileFunc = func(name string, data []byte, perm os.FileMode) error {
		fs.files[name] = data
		return nil
	}

	// Handle opening files with specific mock behavior
	fs.OpenFunc = func(name string) (file, error) {
		if name == dataTmpPath {
			if _, exists := fs.files[name]; !exists {
				fs.files[name] = []byte{}
			}

			return &mockFile{
				Reader: bytes.NewReader(nil),
				WriteFunc: func(p []byte) (int, error) {
					chunk := make([]byte, len(p))
					copy(chunk, p)
					writtenChunks = append(writtenChunks, chunk)

					fs.files[name] = append(fs.files[name], p...)
					return len(p), nil
				},
				SyncFunc:  func() error { return nil },
				CloseFunc: func() error { return nil },
			}, nil
		}

		return fs.defaultOpen(name)
	}

	writer := newSnapshotWriter(fs, serializer, log, dir, nil, true, chunkSize)

	metadata := types.SnapshotMetadata{
		LastIncludedIndex: 100,
		LastIncludedTerm:  5,
	}

	err := writer.Write(context.Background(), metadata, largeData)
	testutil.AssertNoError(t, err)

	testutil.AssertTrue(t, len(writtenChunks) > 1)

	var reconstructed []byte
	for _, chunk := range writtenChunks {
		reconstructed = append(reconstructed, chunk...)
	}
	testutil.AssertEqual(t, largeData, reconstructed)

	exists, err := fs.Exists(dataPath)
	testutil.AssertNoError(t, err)
	testutil.AssertTrue(t, exists, "Final data file should exist")

	exists, err = fs.Exists(metaPath)
	testutil.AssertNoError(t, err)
	testutil.AssertTrue(t, exists, "Final metadata file should exist")
}

// TestSnapshotWriterErrors tests various error paths in the snapshot writer
func TestSnapshotWriterErrors(t *testing.T) {
	log := &logger.NoOpLogger{}
	dir := "/test/snapshot"
	baseSerializer := newJsonSerializer()
	baseMetadata := types.SnapshotMetadata{LastIncludedIndex: 1}
	baseData := []byte("data")
	largeData := make([]byte, 2048)

	fsForPath := newMockFileSystem()
	fsForPath.dir = dir
	metaPath := fsForPath.Path(dir, snapshotMetaFilename)
	dataPath := fsForPath.Path(dir, snapshotDataFilename)
	metaTmpPath := fsForPath.TempPath(metaPath)
	dataTmpPath := fsForPath.TempPath(dataPath)

	tests := []struct {
		name                  string
		setupFS               func(*mockFileSystem)
		setupSerializer       func() serializer
		inputMetadata         types.SnapshotMetadata
		inputData             []byte
		enableChunkedIO       bool
		chunkSize             int
		expectedErrorIs       error
		expectedErrorContains string
		verifyCleanup         func(*testing.T, *mockFileSystem)
	}{
		{
			name:    "MarshalError",
			setupFS: func(fs *mockFileSystem) {},
			setupSerializer: func() serializer {
				return &mockSerializer{
					marshalSnapshotMetadataFunc: func(metadata types.SnapshotMetadata) ([]byte, error) {
						return nil, errors.New("marshal error")
					},
				}
			},
			inputMetadata:         baseMetadata,
			inputData:             baseData,
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "marshal",
		},
		{
			name: "MetadataTempWriteError",
			setupFS: func(fs *mockFileSystem) {
				fs.writeFileErr = errors.New("write file error")
			},
			setupSerializer:       func() serializer { return baseSerializer },
			inputMetadata:         baseMetadata,
			inputData:             baseData,
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "write temp snapshot metadata",
			verifyCleanup: func(t *testing.T, fs *mockFileSystem) {
				exists, _ := fs.Exists(metaTmpPath)
				testutil.AssertFalse(t, exists)
			},
		},
		{
			name: "DataFileOpenError",
			setupFS: func(fs *mockFileSystem) {
				fs.OpenFunc = func(name string) (file, error) {
					if name == dataTmpPath {
						return nil, errors.New("open error")
					}
					return fs.defaultOpen(name)
				}
			},
			setupSerializer:       func() serializer { return baseSerializer },
			inputMetadata:         baseMetadata,
			inputData:             baseData,
			enableChunkedIO:       true,
			chunkSize:             1024,
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "could not open snapshot file",
			verifyCleanup: func(t *testing.T, fs *mockFileSystem) {
				exists, _ := fs.Exists(metaTmpPath)
				testutil.AssertFalse(t, exists)
			},
		},
		{
			name: "DataWriteError",
			setupFS: func(fs *mockFileSystem) {
				fs.OpenFunc = func(name string) (file, error) {
					if name == dataTmpPath {
						return &mockFile{
							Reader:    bytes.NewReader(nil),
							WriteFunc: func(p []byte) (int, error) { return 0, errors.New("write error") },
							SyncFunc:  func() error { return nil },
							CloseFunc: func() error { return nil },
						}, nil
					}
					return fs.defaultOpen(name)
				}
			},
			setupSerializer:       func() serializer { return baseSerializer },
			inputMetadata:         baseMetadata,
			inputData:             baseData,
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "failed to write snapshot data",
			verifyCleanup: func(t *testing.T, fs *mockFileSystem) {
				exists, _ := fs.Exists(metaTmpPath)
				testutil.AssertFalse(t, exists)
				exists, _ = fs.Exists(dataTmpPath)
				testutil.AssertFalse(t, exists)
			},
		},
		{
			name: "ChunkedWriteError",
			setupFS: func(fs *mockFileSystem) {
				fs.OpenFunc = func(name string) (file, error) {
					if name == dataTmpPath {
						writeCount := 0
						return &mockFile{
							Reader: bytes.NewReader(nil),
							WriteFunc: func(p []byte) (int, error) {
								writeCount++
								if writeCount > 1 {
									return 0, errors.New("chunk write error")
								}
								return len(p), nil
							},
							SyncFunc:  func() error { return nil },
							CloseFunc: func() error { return nil },
						}, nil
					}
					return fs.defaultOpen(name)
				}
			},
			setupSerializer:       func() serializer { return baseSerializer },
			inputMetadata:         baseMetadata,
			inputData:             largeData,
			enableChunkedIO:       true,
			chunkSize:             1024,
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "failed to write snapshot chunk",
			verifyCleanup: func(t *testing.T, fs *mockFileSystem) {
				exists, _ := fs.Exists(metaTmpPath)
				testutil.AssertFalse(t, exists)
				exists, _ = fs.Exists(dataTmpPath)
				testutil.AssertFalse(t, exists)
			},
		},
		{
			name: "MetadataRenameError",
			setupFS: func(fs *mockFileSystem) {
				fs.RenameFunc = func(oldPath, newPath string) error {
					if newPath == metaPath {
						return errors.New("rename error")
					}
					return fs.defaultRename(oldPath, newPath)
				}
			},
			setupSerializer:       func() serializer { return baseSerializer },
			inputMetadata:         baseMetadata,
			inputData:             baseData,
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "commit snapshot metadata",
			verifyCleanup: func(t *testing.T, fs *mockFileSystem) {
				exists, _ := fs.Exists(metaTmpPath)
				testutil.AssertFalse(t, exists)
				exists, _ = fs.Exists(dataTmpPath)
				testutil.AssertFalse(t, exists)
			},
		},
		{
			name: "DataRenameError",
			setupFS: func(fs *mockFileSystem) {
				fs.RenameFunc = func(oldPath, newPath string) error {
					if newPath == dataPath {
						return errors.New("data rename error")
					}
					return fs.defaultRename(oldPath, newPath)
				}
			},
			setupSerializer:       func() serializer { return baseSerializer },
			inputMetadata:         baseMetadata,
			inputData:             baseData,
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "commit snapshot data",
			verifyCleanup: func(t *testing.T, fs *mockFileSystem) {
				// After data rename fails, temp data file should be cleaned up
				// and the successfully renamed metadata file should be rolled back
				exists, _ := fs.Exists(dataTmpPath)
				testutil.AssertFalse(t, exists)
				exists, _ = fs.Exists(metaPath)
				testutil.AssertFalse(t, exists)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newMockFileSystem()
			fs.dir = dir
			tt.setupFS(fs)
			serializer := tt.setupSerializer()

			writer := newSnapshotWriter(fs, serializer, log, dir, nil, tt.enableChunkedIO, tt.chunkSize)

			err := writer.Write(context.Background(), tt.inputMetadata, tt.inputData)

			testutil.AssertError(t, err)
			if tt.expectedErrorIs != nil {
				testutil.AssertErrorIs(t, err, tt.expectedErrorIs)
			}
			if tt.expectedErrorContains != "" {
				testutil.AssertContains(t, err.Error(), tt.expectedErrorContains)
			}

			if tt.verifyCleanup != nil {
				tt.verifyCleanup(t, fs)
			}
		})
	}

	// Separate test for context cancellation as it's slightly different setup
	t.Run("ContextCanceled", func(t *testing.T) {
		fs := newMockFileSystem()
		fs.dir = dir
		writer := newSnapshotWriter(fs, baseSerializer, log, dir, nil, false, 0)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := writer.Write(ctx, baseMetadata, baseData)
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})
}

// TestSnapshotReaderSuccess tests the basic success path for the snapshot reader
func TestSnapshotReaderSuccess(t *testing.T) {
	log := &logger.NoOpLogger{}
	fs := newMockFileSystem()
	serializer := newJsonSerializer()
	dir := "/test/snapshot"
	fs.dir = dir
	metaPath := fs.Path(dir, snapshotMetaFilename)
	dataPath := fs.Path(dir, snapshotDataFilename)

	metadata := types.SnapshotMetadata{
		LastIncludedIndex: 100,
		LastIncludedTerm:  5,
	}
	snapshotData := []byte("test snapshot data")

	metaBytes, err := serializer.MarshalSnapshotMetadata(metadata)
	testutil.AssertNoError(t, err)
	err = fs.WriteFile(metaPath, metaBytes, 0644)
	testutil.AssertNoError(t, err)
	err = fs.WriteFile(dataPath, snapshotData, 0644)
	testutil.AssertNoError(t, err)

	reader := newSnapshotReader(fs, serializer, log, dir)

	readMeta, readData, err := reader.Read(context.Background())
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, metadata, readMeta)
	testutil.AssertEqual(t, snapshotData, readData)
}

// TestSnapshotReaderChunkedIO tests that the reader correctly handles chunked I/O for large snapshots
func TestSnapshotReaderChunkedIO(t *testing.T) {
	log := &logger.NoOpLogger{}
	fs := newMockFileSystem()
	serializer := newJsonSerializer()
	dir := "/test/snapshot"
	fs.dir = dir
	metaPath := fs.Path(dir, snapshotMetaFilename)
	dataPath := fs.Path(dir, snapshotDataFilename)

	chunkSize := 1024
	snapshotSize := chunkSize*3 + 100
	largeData := make([]byte, snapshotSize)
	for i := range snapshotSize {
		largeData[i] = byte(i % 256)
	}

	metadata := types.SnapshotMetadata{
		LastIncludedIndex: 100,
		LastIncludedTerm:  5,
	}
	metaBytes, err := serializer.MarshalSnapshotMetadata(metadata)
	testutil.AssertNoError(t, err)
	err = fs.WriteFile(metaPath, metaBytes, 0644)
	testutil.AssertNoError(t, err)
	err = fs.WriteFile(dataPath, largeData, 0644)
	testutil.AssertNoError(t, err)

	fs.StatFunc = func(name string) (os.FileInfo, error) {
		if name == dataPath {
			return &mockFileInfo{sizeVal: int64(snapshotSize)}, nil
		}
		return fs.defaultStat(name)
	}

	var readCalls int
	fs.OpenFunc = func(name string) (file, error) {
		if name == dataPath {
			actualReader := bytes.NewReader(largeData)
			return &mockFile{
				Reader: actualReader,
				ReadFunc: func(p []byte) (int, error) {
					readCalls++
					return actualReader.Read(p)
				},
				CloseFunc: func() error { return nil },
			}, nil
		}
		return fs.defaultOpen(name)
	}

	reader := &defaultSnapshotReader{
		fs:              fs,
		serializer:      serializer,
		logger:          log,
		dir:             dir,
		enableChunkedIO: true,
		chunkSize:       chunkSize,
	}

	readMeta, readData, err := reader.Read(context.Background())
	testutil.AssertNoError(t, err)

	testutil.AssertEqual(t, metadata, readMeta)
	testutil.AssertEqual(t, largeData, readData)
	testutil.AssertTrue(t, readCalls > 1, "Expected multiple reads for chunked IO")
}

// TestSnapshotReaderErrors tests various error paths in the snapshot reader
func TestSnapshotReaderErrors(t *testing.T) {
	log := &logger.NoOpLogger{}
	baseSerializer := newJsonSerializer()
	dir := "/test/snapshot"

	fsForPath := newMockFileSystem()
	fsForPath.dir = dir
	metaPath := fsForPath.Path(dir, snapshotMetaFilename)
	dataPath := fsForPath.Path(dir, snapshotDataFilename)

	validMeta := types.SnapshotMetadata{LastIncludedIndex: 10, LastIncludedTerm: 5}
	validMetaBytes, _ := baseSerializer.MarshalSnapshotMetadata(validMeta)

	tests := []struct {
		name                  string
		setupFS               func(*mockFileSystem)
		enableChunkedIO       bool
		chunkSize             int
		expectedErrorIs       error
		expectedErrorContains string
	}{
		{
			name:            "NoSnapshotMetadata",
			setupFS:         func(fs *mockFileSystem) {},
			expectedErrorIs: ErrNoSnapshot,
		},
		{
			name: "MetadataReadError",
			setupFS: func(fs *mockFileSystem) {
				fs.ReadFileFunc = func(name string) ([]byte, error) {
					if name == metaPath {
						return nil, errors.New("read error")
					}
					return nil, os.ErrNotExist
				}
				fs.ExistsFunc = func(name string) (bool, error) {
					if name == metaPath {
						return true, nil
					}
					return false, nil
				}
			},
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "could not read snapshot metadata",
		},
		{
			name: "InvalidMetadata",
			setupFS: func(fs *mockFileSystem) {
				fs.files[metaPath] = []byte("invalid json")
			},
			expectedErrorIs:       ErrCorruptedSnapshot,
			expectedErrorContains: "invalid snapshot metadata",
		},
		{
			name: "ZeroIndexMetadata",
			setupFS: func(fs *mockFileSystem) {
				invalidMeta := types.SnapshotMetadata{LastIncludedIndex: 0, LastIncludedTerm: 5}
				metaBytes, _ := baseSerializer.MarshalSnapshotMetadata(invalidMeta)
				fs.files[metaPath] = metaBytes
			},
			expectedErrorIs:       ErrCorruptedSnapshot,
			expectedErrorContains: "LastIncludedIndex cannot be zero",
		},
		{
			name: "MissingDataFile",
			setupFS: func(fs *mockFileSystem) {
				fs.files[metaPath] = validMetaBytes
				fs.StatFunc = func(name string) (os.FileInfo, error) {
					if name == dataPath {
						return nil, os.ErrNotExist
					}
					return fs.defaultStat(name)
				}
				fs.IsNotExistFunc = func(err error) bool {
					return errors.Is(err, os.ErrNotExist)
				}
			},
			expectedErrorIs:       ErrCorruptedSnapshot,
			expectedErrorContains: "snapshot data file does not exist",
		},
		{
			name: "DataReadError (NonChunked)",
			setupFS: func(fs *mockFileSystem) {
				fs.files[metaPath] = validMetaBytes
				fs.StatFunc = func(name string) (os.FileInfo, error) {
					if name == dataPath {
						return &mockFileInfo{sizeVal: 100}, nil
					}
					return fs.defaultStat(name)
				}
				fs.ReadFileFunc = func(name string) ([]byte, error) {
					if name == metaPath {
						return validMetaBytes, nil
					}
					if name == dataPath {
						return nil, errors.New("data read error")
					}
					return nil, os.ErrNotExist
				}
			},
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "failed to read snapshot file",
		},
		{
			name: "DataStatError",
			setupFS: func(fs *mockFileSystem) {
				fs.files[metaPath] = validMetaBytes
				fs.StatFunc = func(name string) (os.FileInfo, error) {
					if name == dataPath {
						return nil, errors.New("stat permission error")
					}
					return fs.defaultStat(name)
				}
				fs.IsNotExistFunc = func(err error) bool { return false }
			},
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "unable to stat snapshot file",
		},
		{
			name: "ChunkedReadError",
			setupFS: func(fs *mockFileSystem) {
				fs.files[metaPath] = validMetaBytes
				fs.StatFunc = func(name string) (os.FileInfo, error) {
					if name == dataPath {
						return &mockFileInfo{sizeVal: 2048}, nil
					}
					return fs.defaultStat(name)
				}
				fs.OpenFunc = func(name string) (file, error) {
					if name == dataPath {
						readCount := 0
						return &mockFile{
							Reader: bytes.NewReader(make([]byte, 2048)),
							ReadFunc: func(p []byte) (int, error) {
								readCount++
								if readCount > 1 {
									return 0, errors.New("chunked read error")
								}
								return len(p), nil
							},
							CloseFunc: func() error { return nil },
						}, nil
					}
					return fs.defaultOpen(name)
				}
			},
			enableChunkedIO:       true,
			chunkSize:             1024,
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "chunked read failed",
		},
		{
			name: "ChunkedOpenError",
			setupFS: func(fs *mockFileSystem) {
				fs.files[metaPath] = validMetaBytes
				fs.StatFunc = func(name string) (os.FileInfo, error) {
					if name == dataPath {
						return &mockFileInfo{sizeVal: 2048}, nil
					}
					return fs.defaultStat(name)
				}
				fs.OpenFunc = func(name string) (file, error) {
					if name == dataPath {
						return nil, errors.New("chunked open error")
					}
					return fs.defaultOpen(name)
				}
			},
			enableChunkedIO:       true,
			chunkSize:             1024,
			expectedErrorIs:       ErrStorageIO,
			expectedErrorContains: "unable to open snapshot file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newMockFileSystem()
			fs.dir = dir
			tt.setupFS(fs)

			reader := &defaultSnapshotReader{
				fs:              fs,
				serializer:      baseSerializer,
				logger:          log,
				dir:             dir,
				enableChunkedIO: tt.enableChunkedIO,
				chunkSize:       tt.chunkSize,
			}

			_, _, err := reader.Read(context.Background())

			testutil.AssertError(t, err)
			if tt.expectedErrorIs != nil {
				testutil.AssertErrorIs(t, err, tt.expectedErrorIs)
			}
			if tt.expectedErrorContains != "" {
				testutil.AssertContains(t, err.Error(), tt.expectedErrorContains)
			}
		})
	}

	t.Run("ContextCanceled", func(t *testing.T) {
		fs := newMockFileSystem()
		fs.dir = dir
		fs.files[metaPath] = validMetaBytes
		fs.files[dataPath] = []byte("data")
		fs.StatFunc = func(name string) (os.FileInfo, error) {
			if name == dataPath {
				return &mockFileInfo{sizeVal: 4}, nil
			}
			return fs.defaultStat(name)
		}

		reader := newSnapshotReader(fs, baseSerializer, log, dir)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := reader.Read(ctx)
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})
}

// TestHooksAndLifecycle tests the complete snapshot lifecycle with hooks
func TestHooksAndLifecycle(t *testing.T) {
	log := &logger.NoOpLogger{}
	fs := newMockFileSystem()
	serializer := newJsonSerializer()
	dir := "/test/snapshot"
	fs.dir = dir

	// Tracking hook execution
	hooksSequence := []string{}
	hooks := &snapshotWriteHooks{
		OnTempFilesWritten: func() {
			hooksSequence = append(hooksSequence, "tempFilesWritten")
		},
		OnMetadataCommitted: func() {
			hooksSequence = append(hooksSequence, "metadataCommitted")
		},
	}

	writer := newSnapshotWriter(fs, serializer, log, dir, hooks, false, 0)

	metadata := types.SnapshotMetadata{
		LastIncludedIndex: 100,
		LastIncludedTerm:  5,
	}
	snapshotData := []byte("test snapshot data")

	err := writer.Write(context.Background(), metadata, snapshotData)
	testutil.AssertNoError(t, err)

	testutil.AssertLen(t, hooksSequence, 2)
	if len(hooksSequence) == 2 {
		testutil.AssertEqual(t, "tempFilesWritten", hooksSequence[0])
		testutil.AssertEqual(t, "metadataCommitted", hooksSequence[1])
	}

	reader := newSnapshotReader(fs, serializer, log, dir)

	readMeta, readData, err := reader.Read(context.Background())
	testutil.AssertNoError(t, err)

	testutil.AssertEqual(t, metadata, readMeta)
	testutil.AssertEqual(t, snapshotData, readData)
}

// TestConcurrentOperations tests snapshot operations under concurrent access
func TestConcurrentOperations(t *testing.T) {
	log := &logger.NoOpLogger{}
	fs := newMockFileSystem()
	serializer := newJsonSerializer()
	dir := "/test/snapshot"
	fs.dir = dir

	metaPath := fs.Path(dir, snapshotMetaFilename)
	dataPath := fs.Path(dir, snapshotDataFilename)
	metaTmpPath := fs.TempPath(metaPath)
	dataTmpPath := fs.TempPath(dataPath)

	initialMeta := types.SnapshotMetadata{
		LastIncludedIndex: 50,
		LastIncludedTerm:  2,
	}
	initialData := []byte("initial snapshot")
	initialMetaBytes, _ := serializer.MarshalSnapshotMetadata(initialMeta)
	_ = fs.WriteFile(metaTmpPath, initialMetaBytes, 0644)
	_ = fs.WriteFile(dataTmpPath, initialData, 0644)
	_ = fs.defaultRename(metaTmpPath, metaPath)
	_ = fs.defaultRename(dataTmpPath, dataPath)

	// Test concurrent read while writing is in progress
	t.Run("WriteWhileReading_Synchronized", func(t *testing.T) {
		writer := newSnapshotWriter(fs, serializer, log, dir, nil, false, 0)
		newMeta := types.SnapshotMetadata{
			LastIncludedIndex: 200,
			LastIncludedTerm:  10,
		}
		newData := []byte("new snapshot data")

		err := writer.Write(context.Background(), newMeta, newData)
		testutil.AssertNoError(t, err, "Writer failed")

		reader := newSnapshotReader(fs, serializer, log, dir)
		readMeta, readData, err := reader.Read(context.Background())
		testutil.AssertNoError(t, err, "Reader failed")

		testutil.AssertEqual(t, newMeta, readMeta, "Reader should get new metadata")
		testutil.AssertEqual(t, newData, readData, "Reader should get new data")
	})

	t.Run("ConcurrentReads", func(t *testing.T) {
		const numReaders = 5
		var wg sync.WaitGroup
		errCh := make(chan error, numReaders)

		wg.Add(numReaders)
		for range numReaders {
			go func() {
				defer wg.Done()
				reader := newSnapshotReader(fs, serializer, log, dir)
				_, _, err := reader.Read(context.Background())
				if err != nil {
					errCh <- err
				}
			}()
		}

		wg.Wait()
		close(errCh)

		// Check if any reader encountered an error
		for err := range errCh {
			t.Errorf("Concurrent read failed: %v", err)
		}
	})
}

// TestSnapshotReaderWithContextDeadline tests that the snapshot reader respects context deadlines
func TestSnapshotReaderWithContextDeadline(t *testing.T) {
	log := &logger.NoOpLogger{}
	fs := newMockFileSystem()
	serializer := newJsonSerializer()
	dir := "/test/snapshot"
	fs.dir = dir
	metaPath := fs.Path(dir, snapshotMetaFilename)
	dataPath := fs.Path(dir, snapshotDataFilename)

	metadata := types.SnapshotMetadata{
		LastIncludedIndex: 100,
		LastIncludedTerm:  5,
	}
	metaBytes, err := serializer.MarshalSnapshotMetadata(metadata)
	testutil.AssertNoError(t, err)
	err = fs.WriteFile(metaPath, metaBytes, 0644)
	testutil.AssertNoError(t, err)

	largeData := make([]byte, 1*1024*1024)
	err = fs.WriteFile(dataPath, largeData, 0644)
	testutil.AssertNoError(t, err)

	fs.StatFunc = func(name string) (os.FileInfo, error) {
		if name == dataPath {
			return &mockFileInfo{sizeVal: int64(len(largeData))}, nil
		}
		return fs.defaultStat(name)
	}

	fs.OpenFunc = func(name string) (file, error) {
		if name == dataPath {
			actualReader := bytes.NewReader(largeData)
			return &mockFile{
				Reader: actualReader,
				ReadFunc: func(p []byte) (int, error) {
					// Simulate slow reads
					time.Sleep(50 * time.Millisecond)
					return actualReader.Read(p)
				},
				CloseFunc: func() error { return nil },
			}, nil
		}
		return fs.defaultOpen(name)
	}

	reader := &defaultSnapshotReader{
		fs:              fs,
		serializer:      serializer,
		logger:          log,
		dir:             dir,
		enableChunkedIO: true,
		chunkSize:       1024,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond) // Shorter than total read time
	defer cancel()

	_, _, err = reader.Read(ctx)

	testutil.AssertError(t, err)
	testutil.AssertContains(t, err.Error(), "context deadline exceeded")
}

// TestCleanupAfterPartialWrite tests the cleanup mechanisms after a partial write
func TestCleanupAfterPartialWrite(t *testing.T) {
	log := &logger.NoOpLogger{}
	fs := newMockFileSystem()
	serializer := newJsonSerializer()
	dir := "/test/snapshot"
	fs.dir = dir

	metaPath := fs.Path(dir, snapshotMetaFilename)
	dataPath := fs.Path(dir, snapshotDataFilename)
	dataTmpPath := fs.TempPath(dataPath)

	fs.RenameFunc = func(oldPath, newPath string) error {
		if newPath == dataPath {
			return errors.New("data rename error")
		}
		return fs.defaultRename(oldPath, newPath)
	}

	var removedFiles []string
	fs.RemoveFunc = func(name string) error {
		removedFiles = append(removedFiles, name)
		return nil
	}

	writer := newSnapshotWriter(fs, serializer, log, dir, nil, false, 0)

	err := writer.Write(context.Background(), types.SnapshotMetadata{LastIncludedIndex: 1}, []byte("data"))
	testutil.AssertError(t, err)
	testutil.AssertErrorIs(t, err, ErrStorageIO)
	testutil.AssertContains(t, err.Error(), "commit snapshot data")

	var foundDataTmp, foundMetaPath bool
	for _, path := range removedFiles {
		if path == dataTmpPath {
			foundDataTmp = true
		}
		if path == metaPath {
			foundMetaPath = true
		}
	}
	testutil.AssertTrue(t, foundDataTmp, "Expected temp data file to be removed")
	testutil.AssertTrue(t, foundMetaPath, "Expected committed metadata to be rolled back")
}

// TestSnapshotEndToEnd tests a complete end-to-end workflow of writing and reading snapshots
func TestSnapshotEndToEnd(t *testing.T) {
	log := &logger.NoOpLogger{}
	serializer := newJsonSerializer()
	dir := "/test/snapshots"

	testCases := []struct {
		name          string
		dataSize      int
		chunkSize     int
		enableChunked bool
	}{
		{"Small", 100, 0, false},
		{"Medium", 10 * 1024, 1024, true},
		{"Large", 1 * 1024 * 1024, 256 * 1024, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			fs.dir = filepath.Join(dir, tc.name)

			// Create test data
			metadata := types.SnapshotMetadata{
				LastIncludedIndex: types.Index(100 + tc.dataSize/1000), // Vary metadata slightly
				LastIncludedTerm:  5,
			}
			data := make([]byte, tc.dataSize)
			for i := range data {
				data[i] = byte(i % 256)
			}

			writer := newSnapshotWriter(fs, serializer, log, fs.dir, nil, tc.enableChunked, tc.chunkSize)

			err := writer.Write(context.Background(), metadata, data)
			testutil.AssertNoError(t, err)

			reader := &defaultSnapshotReader{
				fs:              fs,
				serializer:      serializer,
				logger:          log,
				dir:             fs.dir,
				enableChunkedIO: tc.enableChunked,
				chunkSize:       tc.chunkSize,
			}

			readMeta, readData, err := reader.Read(context.Background())
			testutil.AssertNoError(t, err)

			testutil.AssertEqual(t, metadata, readMeta)
			testutil.AssertEqual(t, len(data), len(readData))

			if len(data) > 1024 {
				testutil.AssertEqual(t, data[:100], readData[:100])
				testutil.AssertEqual(t, data[len(data)-100:], readData[len(readData)-100:])
			} else {
				testutil.AssertEqual(t, data, readData)
			}
		})
	}
}
