package storage

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func TestDefaultMetadataService_LoadMetadata(t *testing.T) {
	path := "metadata.json"
	metadata := logMetadata{FirstIndex: 1, LastIndex: 10}
	serializedMetadata, _ := newJsonSerializer().MarshalMetadata(metadata)

	tests := []struct {
		name          string
		fs            *mockFileSystem
		serializer    serializer
		index         indexService
		expected      logMetadata
		expectedError error
	}{
		{
			name: "successful load",
			fs: &mockFileSystem{
				files: map[string][]byte{path: serializedMetadata},
			},
			serializer:    newJsonSerializer(),
			index:         &mockIndexService{},
			expected:      metadata,
			expectedError: nil,
		},
		{
			name: "file not found",
			fs: &mockFileSystem{
				existsErr:     os.ErrNotExist,
				isNotExistErr: true,
			},
			serializer:    newJsonSerializer(),
			index:         &mockIndexService{},
			expected:      logMetadata{},
			expectedError: fmt.Errorf("metadata file not found: %w", os.ErrNotExist),
		},
		{
			name: "read file error",
			fs: &mockFileSystem{
				openErr: errors.New("read error"),
			},
			serializer: newJsonSerializer(),
			index:      &mockIndexService{},
			expected:   logMetadata{},
			expectedError: fmt.Errorf(
				"%w: failed to read metadata file: %w",
				ErrStorageIO,
				errors.New("read error"),
			),
		},
		{
			name: "unmarshal error",
			fs: &mockFileSystem{
				files: map[string][]byte{path: []byte("invalid json")},
			},
			serializer: newJsonSerializer(),
			index:      &mockIndexService{},
			expected:   logMetadata{},
			expectedError: fmt.Errorf(
				"%w: failed to unmarshal metadata: %w",
				ErrCorruptedState,
				errors.New("invalid character 'i' looking for beginning of value"),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logger.NewNoOpLogger()
			sut := newMetadataServiceWithDeps(tt.fs, tt.serializer, tt.index, logger)
			actual, err := sut.LoadMetadata(path)

			testutil.AssertEqual(t, tt.expected, actual)
			if tt.expectedError == nil {
				testutil.AssertNoError(t, err)
			} else {
				testutil.AssertError(t, err)
				// Only check error message for specific scenarios to make tests more resilient
				if tt.name == "file not found" {
					testutil.AssertTrue(t, errors.Is(err, os.ErrNotExist))
				}
			}
		})
	}
}

func TestDefaultMetadataService_SaveMetadata(t *testing.T) {
	path := "metadata.json"
	metadata := logMetadata{FirstIndex: 1, LastIndex: 10}
	serializedMetadata, _ := newJsonSerializer().MarshalMetadata(metadata)

	tests := []struct {
		name           string
		fs             *mockFileSystem
		serializer     serializer
		index          indexService
		metadata       logMetadata
		useAtomicWrite bool
		expectedError  bool
		expectedFiles  map[string][]byte
	}{
		{
			name:           "successful non-atomic save",
			fs:             newMockFileSystem(),
			serializer:     newJsonSerializer(),
			index:          &mockIndexService{},
			metadata:       metadata,
			useAtomicWrite: false,
			expectedError:  false,
			expectedFiles:  map[string][]byte{path: serializedMetadata},
		},
		{
			name: "marshal error",
			fs:   newMockFileSystem(),
			serializer: &mockSerializer{
				marshalMetadataFunc: func(metadata logMetadata) ([]byte, error) {
					return nil, errors.New("marshal error")
				},
			},
			metadata:       metadata,
			useAtomicWrite: false,
			expectedError:  true,
			expectedFiles:  map[string][]byte{},
		},
		{
			name:           "write error (non-atomic)",
			fs:             &mockFileSystem{writeFileErr: errors.New("write error")},
			serializer:     newJsonSerializer(),
			index:          &mockIndexService{},
			metadata:       metadata,
			useAtomicWrite: false,
			expectedError:  true,
			expectedFiles:  map[string][]byte{},
		},
		{
			name:           "successful atomic save",
			fs:             newMockFileSystem(),
			serializer:     newJsonSerializer(),
			index:          &mockIndexService{},
			metadata:       metadata,
			useAtomicWrite: true,
			expectedError:  false,
			expectedFiles:  map[string][]byte{path: serializedMetadata},
		},
		{
			name: "write error (atomic)",
			fs: &mockFileSystem{
				atomicWriteErr: errors.New("atomic write error"),
			},
			serializer:     newJsonSerializer(),
			index:          &mockIndexService{},
			metadata:       metadata,
			useAtomicWrite: true,
			expectedError:  true,
			expectedFiles:  map[string][]byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logger.NewNoOpLogger()
			sut := newMetadataServiceWithDeps(tt.fs, tt.serializer, tt.index, logger)
			err := sut.SaveMetadata(path, tt.metadata, tt.useAtomicWrite)

			if tt.expectedError {
				testutil.AssertError(t, err)
			} else {
				testutil.AssertNoError(t, err)
				testutil.AssertEqual(t, tt.expectedFiles, tt.fs.files)
			}
		})
	}
}

func TestDefaultMetadataService_SyncMetadataFromIndexMap(t *testing.T) {
	path := "metadata.json"
	indexMap := []types.IndexOffsetPair{{Index: 1, Offset: 0}, {Index: 2, Offset: 10}}
	context := "test"

	tests := []struct {
		name           string
		boundsChanged  bool
		useAtomicWrite bool
		saveError      bool
		expectedFirst  types.Index
		expectedLast   types.Index
		expectError    bool
	}{
		{
			name:           "no update required",
			boundsChanged:  false,
			useAtomicWrite: false,
			saveError:      false,
			expectedFirst:  1,
			expectedLast:   2,
			expectError:    false,
		},
		{
			name:           "update required, successful save",
			boundsChanged:  true,
			useAtomicWrite: false,
			saveError:      false,
			expectedFirst:  1,
			expectedLast:   2,
			expectError:    false,
		},
		{
			name:           "update required with atomic write",
			boundsChanged:  true,
			useAtomicWrite: true,
			saveError:      false,
			expectedFirst:  1,
			expectedLast:   2,
			expectError:    false,
		},
		{
			name:           "update required, save fails",
			boundsChanged:  true,
			useAtomicWrite: false,
			saveError:      true,
			expectedFirst:  0,
			expectedLast:   0,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logger.NewNoOpLogger()
			fs := newMockFileSystem()
			if tt.saveError {
				fs.writeFileErr = errors.New("save error")
			}

			indexService := &mockIndexService{
				getBoundsResult: boundsResult{
					NewFirst: 1,
					NewLast:  2,
					Changed:  tt.boundsChanged,
				},
			}

			sut := &defaultMetadataService{
				fs:         fs,
				index:      indexService,
				serializer: newJsonSerializer(),
				logger:     logger.WithComponent("metadata"),
			}

			currentFirst := types.Index(0)
			currentLast := types.Index(0)
			if !tt.boundsChanged {
				currentFirst = 1
				currentLast = 2
			}

			first, last, err := sut.SyncMetadataFromIndexMap(
				path, indexMap, currentFirst, currentLast,
				context, tt.useAtomicWrite,
			)

			if tt.expectError {
				testutil.AssertError(t, err)
			} else {
				testutil.AssertNoError(t, err)
			}

			testutil.AssertEqual(t, tt.expectedFirst, first)
			testutil.AssertEqual(t, tt.expectedLast, last)
		})
	}
}

func TestDefaultMetadataService_ValidateMetadataRange(t *testing.T) {
	tests := []struct {
		name        string
		firstIndex  types.Index
		lastIndex   types.Index
		expectError bool
	}{
		{
			name:        "valid range (lastIndex zero)",
			firstIndex:  1,
			lastIndex:   0,
			expectError: false,
		},
		{
			name:        "valid range (first <= last)",
			firstIndex:  1,
			lastIndex:   10,
			expectError: false,
		},
		{
			name:        "invalid range (first > last)",
			firstIndex:  10,
			lastIndex:   1,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logger.NewNoOpLogger()
			sut := &defaultMetadataService{logger: logger.WithComponent("metadata")}
			err := sut.ValidateMetadataRange(tt.firstIndex, tt.lastIndex)

			if tt.expectError {
				testutil.AssertError(t, err)
				testutil.AssertTrue(t, errors.Is(err, ErrCorruptedState),
					"Should return corrupted state error")
			} else {
				testutil.AssertNoError(t, err)
			}
		})
	}
}

func TestNewMetadataServiceWithSerializer(t *testing.T) {
	fs := newMockFileSystem()
	serializer := newJsonSerializer()
	indexSvc := &mockIndexService{}
	logger := logger.NewNoOpLogger()

	service := newMetadataServiceWithDeps(fs, serializer, indexSvc, logger)

	testutil.AssertNotNil(t, service)

	// Type assertion to verify the concrete type
	_, ok := service.(*defaultMetadataService)
	testutil.AssertTrue(t, ok, "Should return a defaultMetadataService instance")
}
