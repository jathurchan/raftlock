package raft

import (
	"testing"
)

func TestDefaultRaftOptions(t *testing.T) {
	options := DefaultRaftOptions()

	if options.ElectionTickCount != 10 {
		t.Errorf("Expected ElectionTickCount to be 10, got %d", options.ElectionTickCount)
	}
	if options.HeartbeatTickCount != 1 {
		t.Errorf("Expected HeartbeatTickCount to be 1, got %d", options.HeartbeatTickCount)
	}
	if options.ElectionRandomizationFactor != 0.2 {
		t.Errorf("Expected ElectionRandomizationFactor to be 0.2, got %f", options.ElectionRandomizationFactor)
	}
	if options.MaxLogEntriesPerRequest != 100 {
		t.Errorf("Expected MaxLogEntriesPerRequest to be 100, got %d", options.MaxLogEntriesPerRequest)
	}
	if options.SnapshotThreshold != 10000 {
		t.Errorf("Expected SnapshotThreshold to be 10000, got %d", options.SnapshotThreshold)
	}
	if options.StorageSyncDelay != 5 {
		t.Errorf("Expected StorageSyncDelay to be 5, got %d", options.StorageSyncDelay)
	}
	if options.LogCompactionMinEntries != 5000 {
		t.Errorf("Expected LogCompactionMinEntries to be 5000, got %d", options.LogCompactionMinEntries)
	}
	if options.ApplyTickCount != 1 {
		t.Errorf("Expected ApplyTickCount to be 1, got %d", options.ApplyTickCount)
	}

	// Ensure default options are valid
	if err := options.Validate(); err != nil {
		t.Errorf("Default options should be valid, got error: %v", err)
	}
}

func TestWithElectionTicks(t *testing.T) {
	options := DefaultRaftOptions()

	// Test valid value
	modified := options.WithElectionTicks(20)
	if modified.ElectionTickCount != 20 {
		t.Errorf("Expected ElectionTickCount to be 20, got %d", modified.ElectionTickCount)
	}

	// Test that original is unchanged
	if options.ElectionTickCount != 10 {
		t.Errorf("Original options should be unchanged, expected 10, got %d", options.ElectionTickCount)
	}

	// Test panic with invalid value
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic with non-positive ElectionTickCount")
		}
	}()
	options.WithElectionTicks(0)
}

func TestWithHeartbeatTicks(t *testing.T) {
	options := DefaultRaftOptions()

	// Test valid value
	modified := options.WithHeartbeatTicks(2)
	if modified.HeartbeatTickCount != 2 {
		t.Errorf("Expected HeartbeatTickCount to be 2, got %d", modified.HeartbeatTickCount)
	}

	// Test that original is unchanged
	if options.HeartbeatTickCount != 1 {
		t.Errorf("Original options should be unchanged, expected 1, got %d", options.HeartbeatTickCount)
	}

	// Test panic with invalid value
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic with non-positive HeartbeatTickCount")
		}
	}()
	options.WithHeartbeatTicks(0)
}

func TestWithRandomizationFactor(t *testing.T) {
	options := DefaultRaftOptions()

	// Test valid value
	modified := options.WithRandomizationFactor(0.5)
	if modified.ElectionRandomizationFactor != 0.5 {
		t.Errorf("Expected ElectionRandomizationFactor to be 0.5, got %f", modified.ElectionRandomizationFactor)
	}

	// Test that original is unchanged
	if options.ElectionRandomizationFactor != 0.2 {
		t.Errorf("Original options should be unchanged, expected 0.2, got %f", options.ElectionRandomizationFactor)
	}

	// Test panic with invalid value
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic with negative ElectionRandomizationFactor")
		}
	}()
	options.WithRandomizationFactor(-0.1)
}

func TestWithMaxEntriesPerRequest(t *testing.T) {
	options := DefaultRaftOptions()

	// Test valid value
	modified := options.WithMaxEntriesPerRequest(200)
	if modified.MaxLogEntriesPerRequest != 200 {
		t.Errorf("Expected MaxLogEntriesPerRequest to be 200, got %d", modified.MaxLogEntriesPerRequest)
	}

	// Test that original is unchanged
	if options.MaxLogEntriesPerRequest != 100 {
		t.Errorf("Original options should be unchanged, expected 100, got %d", options.MaxLogEntriesPerRequest)
	}

	// Test panic with invalid value
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic with non-positive MaxLogEntriesPerRequest")
		}
	}()
	options.WithMaxEntriesPerRequest(0)
}

func TestWithSnapshotThreshold(t *testing.T) {
	options := DefaultRaftOptions()

	// Test valid value
	modified := options.WithSnapshotThreshold(20000)
	if modified.SnapshotThreshold != 20000 {
		t.Errorf("Expected SnapshotThreshold to be 20000, got %d", modified.SnapshotThreshold)
	}

	// Test that original is unchanged
	if options.SnapshotThreshold != 10000 {
		t.Errorf("Original options should be unchanged, expected 10000, got %d", options.SnapshotThreshold)
	}

	// Test panic with invalid value
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic with non-positive SnapshotThreshold")
		}
	}()
	options.WithSnapshotThreshold(0)
}

func TestWithStorageSyncDelay(t *testing.T) {
	options := DefaultRaftOptions()

	// Test valid value
	modified := options.WithStorageSyncDelay(10)
	if modified.StorageSyncDelay != 10 {
		t.Errorf("Expected StorageSyncDelay to be 10, got %d", modified.StorageSyncDelay)
	}

	// Test that original is unchanged
	if options.StorageSyncDelay != 5 {
		t.Errorf("Original options should be unchanged, expected 5, got %d", options.StorageSyncDelay)
	}

	// Test panic with invalid value
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic with non-positive StorageSyncDelay")
		}
	}()
	options.WithStorageSyncDelay(0)
}

func TestWithLogCompactionMinEntries(t *testing.T) {
	options := DefaultRaftOptions()

	// Test valid value
	modified := options.WithLogCompactionMinEntries(6000)
	if modified.LogCompactionMinEntries != 6000 {
		t.Errorf("Expected LogCompactionMinEntries to be 6000, got %d", modified.LogCompactionMinEntries)
	}

	// Test that original is unchanged
	if options.LogCompactionMinEntries != 5000 {
		t.Errorf("Original options should be unchanged, expected 5000, got %d", options.LogCompactionMinEntries)
	}

	// Test panic with invalid value
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic with non-positive LogCompactionMinEntries")
		}
	}()
	options.WithLogCompactionMinEntries(0)
}

func TestWithApplyTickCount(t *testing.T) {
	options := DefaultRaftOptions()

	// Test valid value
	modified := options.WithApplyTickCount(2)
	if modified.ApplyTickCount != 2 {
		t.Errorf("Expected ApplyTickCount to be 2, got %d", modified.ApplyTickCount)
	}

	// Test that original is unchanged
	if options.ApplyTickCount != 1 {
		t.Errorf("Original options should be unchanged, expected 1, got %d", options.ApplyTickCount)
	}

	// Test panic with invalid value
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic with non-positive ApplyTickCount")
		}
	}()
	options.WithApplyTickCount(0)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name          string
		modifyOptions func(RaftOptions) RaftOptions
		wantError     bool
	}{
		{
			name:          "default options are valid",
			modifyOptions: func(o RaftOptions) RaftOptions { return o },
			wantError:     false,
		},
		{
			name:          "negative HeartbeatTickCount",
			modifyOptions: func(o RaftOptions) RaftOptions { o.HeartbeatTickCount = -1; return o },
			wantError:     true,
		},
		{
			name:          "zero HeartbeatTickCount",
			modifyOptions: func(o RaftOptions) RaftOptions { o.HeartbeatTickCount = 0; return o },
			wantError:     true,
		},
		{
			name:          "ElectionTickCount < 3 * HeartbeatTickCount",
			modifyOptions: func(o RaftOptions) RaftOptions { o.HeartbeatTickCount = 4; o.ElectionTickCount = 11; return o },
			wantError:     true,
		},
		{
			name:          "negative ElectionRandomizationFactor",
			modifyOptions: func(o RaftOptions) RaftOptions { o.ElectionRandomizationFactor = -0.1; return o },
			wantError:     true,
		},
		{
			name:          "ElectionRandomizationFactor > 1.0",
			modifyOptions: func(o RaftOptions) RaftOptions { o.ElectionRandomizationFactor = 1.1; return o },
			wantError:     true,
		},
		{
			name:          "zero MaxLogEntriesPerRequest",
			modifyOptions: func(o RaftOptions) RaftOptions { o.MaxLogEntriesPerRequest = 0; return o },
			wantError:     true,
		},
		{
			name:          "SnapshotThreshold < 1000",
			modifyOptions: func(o RaftOptions) RaftOptions { o.SnapshotThreshold = 999; return o },
			wantError:     true,
		},
		{
			name:          "zero StorageSyncDelay",
			modifyOptions: func(o RaftOptions) RaftOptions { o.StorageSyncDelay = 0; return o },
			wantError:     true,
		},
		{
			name:          "StorageSyncDelay > 100",
			modifyOptions: func(o RaftOptions) RaftOptions { o.StorageSyncDelay = 101; return o },
			wantError:     true,
		},
		{
			name:          "zero LogCompactionMinEntries",
			modifyOptions: func(o RaftOptions) RaftOptions { o.LogCompactionMinEntries = 0; return o },
			wantError:     true,
		},
		{
			name:          "zero ApplyTickCount",
			modifyOptions: func(o RaftOptions) RaftOptions { o.ApplyTickCount = 0; return o },
			wantError:     true,
		},
		{
			name:          "HeartbeatTickCount >= ElectionTickCount",
			modifyOptions: func(o RaftOptions) RaftOptions { o.HeartbeatTickCount = 10; o.ElectionTickCount = 10; return o },
			wantError:     true,
		},
		{
			name: "valid custom options",
			modifyOptions: func(o RaftOptions) RaftOptions {
				return o.WithElectionTicks(20).
					WithHeartbeatTicks(2).
					WithRandomizationFactor(0.5).
					WithMaxEntriesPerRequest(200).
					WithSnapshotThreshold(20000).
					WithStorageSyncDelay(10).
					WithLogCompactionMinEntries(6000).
					WithApplyTickCount(2)
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := tt.modifyOptions(DefaultRaftOptions())
			err := options.Validate()

			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestChainedMethods(t *testing.T) {
	options := DefaultRaftOptions().
		WithElectionTicks(15).
		WithHeartbeatTicks(2).
		WithRandomizationFactor(0.3).
		WithMaxEntriesPerRequest(150).
		WithSnapshotThreshold(15000).
		WithStorageSyncDelay(8).
		WithLogCompactionMinEntries(7500).
		WithApplyTickCount(2)

	if options.ElectionTickCount != 15 {
		t.Errorf("Expected ElectionTickCount to be 15, got %d", options.ElectionTickCount)
	}
	if options.HeartbeatTickCount != 2 {
		t.Errorf("Expected HeartbeatTickCount to be 2, got %d", options.HeartbeatTickCount)
	}
	if options.ElectionRandomizationFactor != 0.3 {
		t.Errorf("Expected ElectionRandomizationFactor to be 0.3, got %f", options.ElectionRandomizationFactor)
	}
	if options.MaxLogEntriesPerRequest != 150 {
		t.Errorf("Expected MaxLogEntriesPerRequest to be 150, got %d", options.MaxLogEntriesPerRequest)
	}
	if options.SnapshotThreshold != 15000 {
		t.Errorf("Expected SnapshotThreshold to be 15000, got %d", options.SnapshotThreshold)
	}
	if options.StorageSyncDelay != 8 {
		t.Errorf("Expected StorageSyncDelay to be 8, got %d", options.StorageSyncDelay)
	}
	if options.LogCompactionMinEntries != 7500 {
		t.Errorf("Expected LogCompactionMinEntries to be 7500, got %d", options.LogCompactionMinEntries)
	}
	if options.ApplyTickCount != 2 {
		t.Errorf("Expected ApplyTickCount to be 2, got %d", options.ApplyTickCount)
	}

	if err := options.Validate(); err != nil {
		t.Errorf("Chained methods should produce valid options, got error: %v", err)
	}
}
