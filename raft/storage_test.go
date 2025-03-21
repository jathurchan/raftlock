package raft

import (
	"testing"

	tu "github.com/jathurchan/raftlock/testutil"
)

func TestDefaultStorageConfig(t *testing.T) {
	config := DefaultStorageConfig()

	// Verify the default settings
	tu.AssertNotNil(t, config, "Default config should not be nil")
	tu.AssertEqual(t, FileStorageType, config.Type, "Default storage type should be FileStorageType")
	tu.AssertEqual(t, "../data/raft", config.Dir, "Default dir should be '../data/raft'")
}

func TestNewStorage(t *testing.T) {
	tests := []struct {
		name        string
		config      *StorageConfig
		expectedErr bool
		errMsg      string
	}{
		{
			name:        "nil config uses default",
			config:      nil,
			expectedErr: false,
		},
		{
			name: "file storage type",
			config: &StorageConfig{
				Type: FileStorageType,
				Dir:  "/tmp/raft-test",
			},
			expectedErr: false,
		},
		{
			name: "unsupported storage type",
			config: &StorageConfig{
				Type: "unsupported",
				Dir:  "/tmp/raft-test",
			},
			expectedErr: true,
			errMsg:      "unsupported storage type: unsupported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage, err := NewStorage(tt.config)

			if tt.expectedErr {
				tu.AssertError(t, err, "Expected an error for %s", tt.name)
				if tt.errMsg != "" {
					tu.AssertEqual(t, tt.errMsg, err.Error(), "Error message does not match")
				}
			} else {
				tu.AssertNoError(t, err, "Unexpected error for %s: %v", tt.name, err)
				tu.AssertNotNil(t, storage, "Storage should not be nil for %s", tt.name)

				// Check type of storage
				if tt.config == nil || tt.config.Type == FileStorageType {
					_, ok := storage.(*FileStorage)
					if !ok {
						t.Errorf("Expected FileStorage type but got different type")
					}
				}
			}
		})
	}
}
