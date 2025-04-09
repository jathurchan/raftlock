package storage

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/jathurchan/raftlock/logger"
)

func TestNewRecoveryService(t *testing.T) {
	// Test default constructor
	fs := newMockFileSystem()
	log := &logger.NoOpLogger{}

	// Test with serializer constructor
	serializer := newJsonSerializer()
	indexSvc := &mockIndexService{}
	metadataSvc := &mockMetadataService{}
	sysInfo := &mockSystemInfo{
		PIDFunc:          func() int { return 123 },
		HostnameFunc:     func() string { return "test-host" },
		NowUnixMilliFunc: func() int64 { return 1000 },
	}
	rs := newRecoveryService(fs, serializer, log, "/tmp/raft", normalMode, indexSvc, metadataSvc, sysInfo)

	if rs == nil {
		t.Fatal("Expected non-nil recovery service from dependencies constructor")
	}
}

func TestCheckForRecoveryMarkers(t *testing.T) {
	tests := []struct {
		name           string
		recoveryExists bool
		snapshotExists bool
		existsErr      error
		expectedResult bool
		expectedErr    bool
	}{
		{
			name:           "No markers",
			recoveryExists: false,
			snapshotExists: false,
			expectedResult: false,
			expectedErr:    false,
		},
		{
			name:           "Recovery marker exists",
			recoveryExists: true,
			snapshotExists: false,
			expectedResult: true,
			expectedErr:    false,
		},
		{
			name:           "Snapshot marker exists",
			recoveryExists: false,
			snapshotExists: true,
			expectedResult: true,
			expectedErr:    false,
		},
		{
			name:           "Both markers exist",
			recoveryExists: true,
			snapshotExists: true,
			expectedResult: true,
			expectedErr:    false,
		},
		{
			name:           "Error checking recovery marker",
			recoveryExists: false,
			existsErr:      errors.New("exists error"),
			expectedResult: false,
			expectedErr:    true,
		},
		{
			name:           "Error checking snapshot marker",
			recoveryExists: false,
			existsErr:      errors.New("snapshot exists error"),
			expectedResult: false,
			expectedErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			fs.ExistsFunc = func(path string) (bool, error) {
				if tc.existsErr != nil {
					if tc.name == "Error checking recovery marker" && path == "/tmp/raft/recovery.marker" {
						return false, tc.existsErr
					} else if tc.name == "Error checking snapshot marker" && path == "/tmp/raft/snapshot.marker" {
						return false, tc.existsErr
					}
				}
				if path == "/tmp/raft/recovery.marker" {
					return tc.recoveryExists, nil
				}
				if path == "/tmp/raft/snapshot.marker" {
					return tc.snapshotExists, nil
				}
				return false, nil
			}

			rs := newRecoveryService(fs, &mockSerializer{}, &logger.NoOpLogger{}, "/tmp/raft", normalMode, &mockIndexService{}, &mockMetadataService{}, &mockSystemInfo{})
			result, err := rs.CheckForRecoveryMarkers()

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if result != tc.expectedResult {
				t.Errorf("Expected result %v but got %v", tc.expectedResult, result)
			}
		})
	}
}

func TestPerformRecovery(t *testing.T) {
	tests := []struct {
		name                 string
		snapshotRecoveryErr  error
		consistencyRepairErr error
		expectedErr          bool
	}{
		{
			name:                 "Successful recovery",
			snapshotRecoveryErr:  nil,
			consistencyRepairErr: nil,
			expectedErr:          false,
		},
		{
			name:                 "Snapshot recovery fails",
			snapshotRecoveryErr:  errors.New("snapshot recovery error"),
			consistencyRepairErr: nil,
			expectedErr:          true,
		},
		{
			name:                 "Consistency repair fails",
			snapshotRecoveryErr:  nil,
			consistencyRepairErr: errors.New("consistency repair error"),
			expectedErr:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a defaultRecoveryService but inject mock snapshot recovery and consistency checker
			rs := &defaultRecoveryService{
				logger: &logger.NoOpLogger{},
			}

			// Use reflection to access the unexported methods
			rs.recoverFromSnapshotOperationFunc = func() error {
				return tc.snapshotRecoveryErr
			}

			rs.CheckAndRepairConsistencyFunc = func() error {
				return tc.consistencyRepairErr
			}

			// Call the method under test
			err := rs.PerformRecovery()

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestRecoverFromSnapshotOperation(t *testing.T) {
	tests := []struct {
		name           string
		markerExists   bool
		existsErr      error
		readMarkerErr  error
		markerContains bool
		recoverErr     error
		expectedErr    bool
	}{
		{
			name:         "No snapshot marker",
			markerExists: false,
			expectedErr:  false,
		},
		{
			name:         "Error checking marker",
			markerExists: false,
			existsErr:    errors.New("exists error"),
			expectedErr:  true,
		},
		{
			name:          "Error reading marker",
			markerExists:  true,
			readMarkerErr: errors.New("read error"),
			expectedErr:   true,
		},
		{
			name:           "Successful recovery",
			markerExists:   true,
			markerContains: false,
			recoverErr:     nil,
			expectedErr:    false,
		},
		{
			name:           "Recovery error",
			markerExists:   true,
			markerContains: true,
			recoverErr:     errors.New("recover error"),
			expectedErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()

			// Setup mock behavior
			fs.ExistsFunc = func(path string) (bool, error) {
				if tc.existsErr != nil {
					return false, tc.existsErr
				}
				if path == "/tmp/raft/snapshot.marker" {
					return tc.markerExists, nil
				}
				return false, nil
			}

			// Mock file behavior for reading marker
			fs.OpenFunc = func(name string) (file, error) {
				if tc.readMarkerErr != nil {
					return nil, tc.readMarkerErr
				}
				var content string
				if tc.markerContains {
					content = "meta_committed=true"
				} else {
					content = "some other content"
				}
				return &mockFile{Reader: newMockReader([]byte(content))}, nil
			}

			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
			}

			// Inject mock for RecoverSnapshot
			rs.RecoverSnapshotFunc = func(metaCommitted bool) error {
				if tc.markerContains != metaCommitted {
					t.Errorf("Expected metaCommitted to be %v but got %v", tc.markerContains, metaCommitted)
				}
				return tc.recoverErr
			}

			// Call the method under test
			err := rs.recoverFromSnapshotOperation()

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestReadSnapshotMarker(t *testing.T) {
	tests := []struct {
		name                  string
		openErr               error
		readAllErr            error
		fileContent           string
		expectedMetaCommitted bool
		expectedErr           bool
	}{
		{
			name:                  "Marker contains committed key",
			fileContent:           "some data\nmeta_committed=true\nmore data",
			expectedMetaCommitted: true,
			expectedErr:           false,
		},
		{
			name:                  "Marker does not contain committed key",
			fileContent:           "some data\nother_key=value\nmore data",
			expectedMetaCommitted: false,
			expectedErr:           false,
		},
		{
			name:                  "Empty marker file",
			fileContent:           "",
			expectedMetaCommitted: false,
			expectedErr:           false,
		},
		{
			name:                  "Error opening marker file",
			openErr:               errors.New("open error"),
			expectedMetaCommitted: false,
			expectedErr:           true,
		},
		{
			name:                  "Error reading marker file",
			fileContent:           "some data",
			readAllErr:            errors.New("read error"),
			expectedMetaCommitted: false,
			expectedErr:           true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
			}

			fs.OpenFunc = func(name string) (file, error) {
				if tc.openErr != nil {
					return nil, tc.openErr
				}
				return &mockFile{Reader: newMockReader([]byte(tc.fileContent)), ReadAllFunc: func() ([]byte, error) {
					if tc.readAllErr != nil {
						return nil, tc.readAllErr
					}
					return []byte(tc.fileContent), nil
				}}, nil
			}

			metaCommitted, err := rs.readSnapshotMarker("/tmp/raft/snapshot.marker")

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if metaCommitted != tc.expectedMetaCommitted {
				t.Errorf("Expected metaCommitted to be %v but got %v", tc.expectedMetaCommitted, metaCommitted)
			}
		})
	}
}

func TestRecoverSnapshot(t *testing.T) {
	tests := []struct {
		name          string
		metaCommitted bool
		evaluateState snapshotRecoveryState
		evaluateErr   error
		cleanupErr    error
		commitErr     error
		expectedErr   bool
	}{
		{
			name:          "Incomplete snapshot",
			metaCommitted: false,
			evaluateState: snapshotStateIncomplete,
			cleanupErr:    nil,
			expectedErr:   false,
		},
		{
			name:          "Cleanup error",
			metaCommitted: false,
			evaluateState: snapshotStateIncomplete,
			cleanupErr:    errors.New("cleanup error"),
			expectedErr:   true,
		},
		{
			name:          "Needs data commit",
			metaCommitted: true,
			evaluateState: snapshotStateNeedsDataCommit,
			commitErr:     nil,
			expectedErr:   false,
		},
		{
			name:          "Commit error",
			metaCommitted: true,
			evaluateState: snapshotStateNeedsDataCommit,
			commitErr:     errors.New("commit error"),
			expectedErr:   true,
		},
		{
			name:          "Clean state",
			metaCommitted: true,
			evaluateState: snapshotStateClean,
			expectedErr:   false,
		},
		{
			name:          "Evaluate error",
			metaCommitted: true,
			evaluateErr:   errors.New("evaluate error"),
			expectedErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
			}

			// Inject mocks
			rs.EvaluateSnapshotRecoveryStateFunc = func(metaCommitted bool) (snapshotRecoveryState, error) {
				if tc.evaluateErr != nil {
					return snapshotStateUnknown, tc.evaluateErr
				}
				return tc.evaluateState, nil
			}

			rs.cleanupIncompleteSnapshotFunc = func() error {
				return tc.cleanupErr
			}

			rs.CompleteSnapshotDataCommitFunc = func() error {
				return tc.commitErr
			}

			// Call the method under test
			err := rs.recoverSnapshot(tc.metaCommitted)

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestEvaluateSnapshotRecoveryState(t *testing.T) {
	tests := []struct {
		name          string
		metaCommitted bool
		tmpMetaExists bool
		tmpDataExists bool
		metaExists    bool
		dataExists    bool
		expectedState snapshotRecoveryState
	}{
		{
			name:          "Incomplete snapshot - tmp meta exists",
			metaCommitted: false,
			tmpMetaExists: true,
			tmpDataExists: false,
			metaExists:    false,
			dataExists:    false,
			expectedState: snapshotStateIncomplete,
		},
		{
			name:          "Incomplete snapshot - tmp data exists",
			metaCommitted: false,
			tmpMetaExists: false,
			tmpDataExists: true,
			metaExists:    false,
			dataExists:    false,
			expectedState: snapshotStateIncomplete,
		},
		{
			name:          "Clean state - no meta committed",
			metaCommitted: false,
			tmpMetaExists: false,
			tmpDataExists: false,
			metaExists:    true,
			dataExists:    true,
			expectedState: snapshotStateClean,
		},
		{
			name:          "Needs data commit",
			metaCommitted: true,
			tmpMetaExists: false,
			tmpDataExists: true,
			metaExists:    true,
			dataExists:    false,
			expectedState: snapshotStateNeedsDataCommit,
		},
		{
			name:          "Clean state - meta committed",
			metaCommitted: true,
			tmpMetaExists: false,
			tmpDataExists: false,
			metaExists:    true,
			dataExists:    true,
			expectedState: snapshotStateClean,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()

			// Setup SafeExists mock behavior
			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
			}

			rs.SafeExistsFunc = func(path string, label string) bool {
				if path == fs.Join("/tmp/raft", snapshotMetaFilename+tmpSuffix) {
					return tc.tmpMetaExists
				}
				if path == fs.Join("/tmp/raft", snapshotDataFilename+tmpSuffix) {
					return tc.tmpDataExists
				}
				if path == fs.Join("/tmp/raft", snapshotMetaFilename) {
					return tc.metaExists
				}
				if path == fs.Join("/tmp/raft", snapshotDataFilename) {
					return tc.dataExists
				}
				return false
			}

			// Call the method under test
			state, err := rs.evaluateSnapshotRecoveryState(tc.metaCommitted)

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if state != tc.expectedState {
				t.Errorf("Expected state %v but got %v", tc.expectedState, state)
			}
		})
	}
}

func TestCleanupIncompleteSnapshot(t *testing.T) {
	tests := []struct {
		name          string
		tmpMetaExists bool
		tmpDataExists bool
		removeErr     error
		expectedErr   bool
	}{
		{
			name:          "Cleanup both files",
			tmpMetaExists: true,
			tmpDataExists: true,
			expectedErr:   false,
		},
		{
			name:          "Only tmp meta exists",
			tmpMetaExists: true,
			tmpDataExists: false,
			expectedErr:   false,
		},
		{
			name:          "Only tmp data exists",
			tmpMetaExists: false,
			tmpDataExists: true,
			expectedErr:   false,
		},
		{
			name:          "No tmp files exist",
			tmpMetaExists: false,
			tmpDataExists: false,
			expectedErr:   false,
		},
		{
			name:          "Remove error",
			tmpMetaExists: true,
			removeErr:     errors.New("remove error"),
			// We don't expect an error to be returned, just logged
			expectedErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
			}

			rs.SafeExistsFunc = func(path string, label string) bool {
				if path == fs.Join("/tmp/raft", snapshotMetaFilename+tmpSuffix) {
					return tc.tmpMetaExists
				}
				if path == fs.Join("/tmp/raft", snapshotDataFilename+tmpSuffix) {
					return tc.tmpDataExists
				}
				return false
			}

			fs.RemoveFunc = func(name string) error {
				return tc.removeErr
			}

			// Call the method under test
			err := rs.cleanupIncompleteSnapshot()

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestCompleteSnapshotDataCommit(t *testing.T) {
	tests := []struct {
		name        string
		renameErr   error
		mode        recoveryMode
		expectedErr bool
	}{
		{
			name:        "Successful commit",
			renameErr:   nil,
			mode:        normalMode,
			expectedErr: false,
		},
		{
			name:        "Rename error - normal mode",
			renameErr:   errors.New("rename error"),
			mode:        normalMode,
			expectedErr: true,
		},
		{
			name:        "Rename error - aggressive mode",
			renameErr:   errors.New("rename error"),
			mode:        aggressiveMode,
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
				mode:   tc.mode,
			}

			fs.RenameFunc = func(oldPath, newPath string) error {
				return tc.renameErr
			}

			// Track if Remove was called for the metadata file in aggressive mode
			var metaRemoved bool
			fs.RemoveFunc = func(name string) error {
				if name == rs.path(snapshotMetaFilename) {
					metaRemoved = true
				}
				return nil
			}

			// Call the method under test
			err := rs.completeSnapshotDataCommit()

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// In aggressive mode with rename error, metadata should be removed
			if tc.renameErr != nil && tc.mode == aggressiveMode {
				if !metaRemoved {
					t.Error("Expected metadata file to be removed in aggressive mode with rename error")
				}
			}
		})
	}
}

func TestSafeExists(t *testing.T) {
	tests := []struct {
		name       string
		existsErr  error
		fileExists bool
		expected   bool
	}{
		{
			name:       "File exists",
			fileExists: true,
			expected:   true,
		},
		{
			name:       "File doesn't exist",
			fileExists: false,
			expected:   false,
		},
		{
			name:      "Error checking existence",
			existsErr: errors.New("exists error"),
			expected:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			fs.ExistsFunc = func(path string) (bool, error) {
				if tc.existsErr != nil {
					return false, tc.existsErr
				}
				return tc.fileExists, nil
			}

			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
			}

			// Call the method under test
			result := rs.safeExists("/tmp/file", "test file")

			if result != tc.expected {
				t.Errorf("Expected %v but got %v", tc.expected, result)
			}
		})
	}
}

func TestCheckAndRepairConsistency(t *testing.T) {
	tests := []struct {
		name         string
		metaExists   bool
		logExists    bool
		existsErr    error
		logExistsErr error
		handleErr    error
		expectedErr  bool
	}{
		{
			name:        "Both meta and log exist",
			metaExists:  true,
			logExists:   true,
			expectedErr: false,
		},
		{
			name:        "Meta exists but log missing",
			metaExists:  true,
			logExists:   false,
			handleErr:   nil,
			expectedErr: false,
		},
		{
			name:        "Meta exists but log missing with error",
			metaExists:  true,
			logExists:   false,
			handleErr:   errors.New("handle error"),
			expectedErr: true,
		},
		{
			name:        "Log exists but meta missing",
			metaExists:  false,
			logExists:   true,
			expectedErr: false,
		},
		{
			name:        "Neither meta nor log exists",
			metaExists:  false,
			logExists:   false,
			expectedErr: false,
		},
		{
			name:        "Error checking meta existence",
			existsErr:   errors.New("exists error"),
			expectedErr: true,
		},
		{
			name:         "Error checking log existence",
			metaExists:   true,
			logExistsErr: errors.New("log exists error"),
			expectedErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
			}

			var existsCallCount int
			fs.ExistsFunc = func(path string) (bool, error) {
				existsCallCount++

				// First call is for metadata file
				if existsCallCount == 1 {
					if tc.existsErr != nil {
						return false, tc.existsErr
					}
					if path == fs.Join("/tmp/raft", metadataFilename) {
						return tc.metaExists, nil
					}
				} else if existsCallCount == 2 {
					// Second call is for log file
					if tc.logExistsErr != nil {
						return false, tc.logExistsErr // Return error for log file check
					}
					if path == fs.Join("/tmp/raft", logFilename) {
						return tc.logExists, nil
					}
				}
				return false, nil
			}

			// Mock for HandleMissingLogFile
			rs.HandleMissingLogFileFunc = func(metaFile string) error {
				return tc.handleErr
			}

			// Call the method under test
			err := rs.checkAndRepairConsistency()

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if tc.logExistsErr != nil && err != nil {
				if !errors.Is(err, ErrStorageIO) {
					t.Errorf("Expected error to wrap ErrStorageIO but got: %v", err)
				}
				if !strings.Contains(err.Error(), "failed to check log file") {
					t.Errorf("Expected error message to contain 'failed to check log file' but got: %v", err)
				}
			}
		})
	}
}

func TestHandleMissingLogFile(t *testing.T) {
	tests := []struct {
		name        string
		mode        recoveryMode
		saveErr     error
		expectedErr bool
	}{
		{
			name:        "Conservative mode",
			mode:        conservativeMode,
			expectedErr: true,
		},
		{
			name:        "Normal mode - success",
			mode:        normalMode,
			saveErr:     nil,
			expectedErr: false,
		},
		{
			name:        "Normal mode - save error",
			mode:        normalMode,
			saveErr:     errors.New("save error"),
			expectedErr: true,
		},
		{
			name:        "Aggressive mode - success",
			mode:        aggressiveMode,
			saveErr:     nil,
			expectedErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metaSvc := &mockMetadataService{}
			metaSvc.SaveMetadataFunc = func(path string, metadata logMetadata, useAtomicWrite bool) error {
				metaSvc.saveCalled = true

				// Verify the metadata is reset correctly
				if metadata.FirstIndex != 0 || metadata.LastIndex != 0 {
					t.Errorf("Expected reset metadata but got %+v", metadata)
				}
				return tc.saveErr
			}

			rs := &defaultRecoveryService{
				logger:  &logger.NoOpLogger{},
				mode:    tc.mode,
				metaSvc: metaSvc,
			}

			// Call the method under test
			err := rs.handleMissingLogFile("/tmp/raft/metadata.json")

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// In conservative mode, SaveMetadata should not be called
			if tc.mode == conservativeMode && metaSvc.saveCalled {
				t.Error("Expected SaveMetadata to not be called in conservative mode")
			}

			// In non-conservative modes, SaveMetadata should be called
			if tc.mode != conservativeMode && !metaSvc.saveCalled {
				t.Error("Expected SaveMetadata to be called in non-conservative mode")
			}
		})
	}
}

func TestCreateRecoveryMarker(t *testing.T) {
	tests := []struct {
		name        string
		writeErr    error
		expectedErr bool
	}{
		{
			name:        "Success",
			writeErr:    nil,
			expectedErr: false,
		},
		{
			name:        "Write error",
			writeErr:    errors.New("write error"),
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			fs.WriteFileFunc = func(name string, data []byte, perm os.FileMode) error {
				return tc.writeErr
			}

			sysInfo := &mockSystemInfo{
				PIDFunc:          func() int { return 12345 },
				HostnameFunc:     func() string { return "test-host" },
				NowUnixMilliFunc: func() int64 { return 1617293982123 },
			}

			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
				proc:   sysInfo,
			}

			// Call the method under test
			err := rs.CreateRecoveryMarker()

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestCleanupTempFiles(t *testing.T) {
	tests := []struct {
		name          string
		globErr       error
		removeErr     error
		fileNotExists bool // If true, fs.IsNotExist(err) will return true for removeErr
		expectedErr   bool
	}{
		{
			name:        "Success - no temp files",
			expectedErr: false,
		},
		{
			name:        "Success - with temp files",
			expectedErr: false,
		},
		{
			name:        "Glob error",
			globErr:     errors.New("glob error"),
			expectedErr: true,
		},
		{
			name:        "Remove error",
			removeErr:   errors.New("remove error"),
			expectedErr: true,
		},
		{
			name:          "File doesn't exist error - should be ignored",
			removeErr:     errors.New("file doesn't exist"),
			fileNotExists: true,
			expectedErr:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()

			// Setup mock filesystem
			fs.GlobFunc = func(pattern string) ([]string, error) {
				if tc.globErr != nil {
					return nil, tc.globErr
				}

				// For testing "with temp files" case, return some matches
				if tc.name == "Success - with temp files" || tc.name == "Remove error" || tc.name == "File doesn't exist error - should be ignored" {
					return []string{pattern}, nil
				}
				return []string{}, nil
			}

			fs.RemoveFunc = func(name string) error {
				return tc.removeErr
			}

			fs.IsNotExistFunc = func(err error) bool {
				return tc.fileNotExists
			}

			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
			}

			// Call the method under test
			err := rs.CleanupTempFiles()

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// For file not exists case, we should not return an error
			if tc.fileNotExists && err != nil {
				t.Errorf("Expected no error for non-existent file but got: %v", err)
			}
		})
	}
}

func TestRemoveRecoveryMarker(t *testing.T) {
	tests := []struct {
		name        string
		removeErr   error
		expectedErr bool
	}{
		{
			name:        "Success",
			removeErr:   nil,
			expectedErr: false,
		},
		{
			name:        "Remove error",
			removeErr:   errors.New("remove error"),
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			fs.RemoveFunc = func(name string) error {
				if name == "/tmp/raft/recovery.marker" {
					return tc.removeErr
				}
				return nil
			}

			rs := &defaultRecoveryService{
				fs:     fs,
				logger: &logger.NoOpLogger{},
				dir:    "/tmp/raft",
			}

			// Call the method under test
			err := rs.RemoveRecoveryMarker()

			if tc.expectedErr && err == nil {
				t.Fatal("Expected error but got nil")
			}

			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

// Helper function to create a bytes.Reader without having to import bytes
func newMockReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}
