package raft

import (
	"testing"
)

func TestDefaultOptions(t *testing.T) {
	options := DefaultOptions()

	// Verify default values
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

func TestDefaultFeatureFlags(t *testing.T) {
	flags := DefaultFeatureFlags()

	if !flags.EnableReadIndex {
		t.Errorf("Expected EnableReadIndex to be true by default")
	}
	if !flags.EnableLeaderLease {
		t.Errorf("Expected EnableLeaderLease to be true by default")
	}
	if !flags.PreVoteEnabled {
		t.Errorf("Expected PreVoteEnabled to be true by default")
	}

	// Ensure default flags are valid
	if err := flags.Validate(); err != nil {
		t.Errorf("Default feature flags should be valid, got error: %v", err)
	}
}

func TestDefaultTuningParams(t *testing.T) {
	params := DefaultTuningParams()

	if params.MaxApplyBatchSize != 10 {
		t.Errorf("Expected MaxApplyBatchSize to be 10, got %d", params.MaxApplyBatchSize)
	}
	if params.MaxSnapshotChunkSize != 0 {
		t.Errorf("Expected MaxSnapshotChunkSize to be 0, got %d", params.MaxSnapshotChunkSize)
	}

	// Ensure default params are valid
	if err := params.Validate(); err != nil {
		t.Errorf("Default tuning params should be valid, got error: %v", err)
	}
}

func TestOptionsWithMethods(t *testing.T) {
	baseOptions := DefaultOptions()

	// Test each "With" method individually and verify result
	t.Run("WithElectionTicks", func(t *testing.T) {
		modified := baseOptions.WithElectionTicks(20)
		if modified.ElectionTickCount != 20 {
			t.Errorf("Expected ElectionTickCount to be 20, got %d", modified.ElectionTickCount)
		}
		// Original should be unchanged
		if baseOptions.ElectionTickCount != 10 {
			t.Errorf("Original options should be unchanged")
		}
	})

	t.Run("WithHeartbeatTicks", func(t *testing.T) {
		modified := baseOptions.WithHeartbeatTicks(2)
		if modified.HeartbeatTickCount != 2 {
			t.Errorf("Expected HeartbeatTickCount to be 2, got %d", modified.HeartbeatTickCount)
		}
	})

	t.Run("WithRandomizationFactor", func(t *testing.T) {
		modified := baseOptions.WithRandomizationFactor(0.5)
		if modified.ElectionRandomizationFactor != 0.5 {
			t.Errorf("Expected ElectionRandomizationFactor to be 0.5, got %f", modified.ElectionRandomizationFactor)
		}
	})

	t.Run("WithMaxEntriesPerRequest", func(t *testing.T) {
		modified := baseOptions.WithMaxEntriesPerRequest(200)
		if modified.MaxLogEntriesPerRequest != 200 {
			t.Errorf("Expected MaxLogEntriesPerRequest to be 200, got %d", modified.MaxLogEntriesPerRequest)
		}
	})

	t.Run("WithSnapshotThreshold", func(t *testing.T) {
		modified := baseOptions.WithSnapshotThreshold(20000)
		if modified.SnapshotThreshold != 20000 {
			t.Errorf("Expected SnapshotThreshold to be 20000, got %d", modified.SnapshotThreshold)
		}
	})

	t.Run("WithStorageSyncDelay", func(t *testing.T) {
		modified := baseOptions.WithStorageSyncDelay(10)
		if modified.StorageSyncDelay != 10 {
			t.Errorf("Expected StorageSyncDelay to be 10, got %d", modified.StorageSyncDelay)
		}
	})

	t.Run("WithLogCompactionMinEntries", func(t *testing.T) {
		modified := baseOptions.WithLogCompactionMinEntries(6000)
		if modified.LogCompactionMinEntries != 6000 {
			t.Errorf("Expected LogCompactionMinEntries to be 6000, got %d", modified.LogCompactionMinEntries)
		}
	})

	t.Run("WithApplyTickCount", func(t *testing.T) {
		modified := baseOptions.WithApplyTickCount(2)
		if modified.ApplyTickCount != 2 {
			t.Errorf("Expected ApplyTickCount to be 2, got %d", modified.ApplyTickCount)
		}
	})

	// Test chained methods
	t.Run("ChainedMethods", func(t *testing.T) {
		modified := baseOptions.
			WithElectionTicks(15).
			WithHeartbeatTicks(2).
			WithRandomizationFactor(0.3).
			WithMaxEntriesPerRequest(150).
			WithSnapshotThreshold(15000).
			WithStorageSyncDelay(8).
			WithLogCompactionMinEntries(7500).
			WithApplyTickCount(2)

		if modified.ElectionTickCount != 15 {
			t.Errorf("Expected ElectionTickCount to be 15, got %d", modified.ElectionTickCount)
		}
		if modified.HeartbeatTickCount != 2 {
			t.Errorf("Expected HeartbeatTickCount to be 2, got %d", modified.HeartbeatTickCount)
		}
		if modified.ElectionRandomizationFactor != 0.3 {
			t.Errorf("Expected ElectionRandomizationFactor to be 0.3, got %f", modified.ElectionRandomizationFactor)
		}
		if err := modified.Validate(); err != nil {
			t.Errorf("Chained method options should be valid, got error: %v", err)
		}
	})
}

func TestOptionsValidate(t *testing.T) {
	baseOptions := DefaultOptions()

	testCases := []struct {
		name          string
		modifyOptions func(Options) Options
		wantError     bool
	}{
		{
			name:          "default options are valid",
			modifyOptions: func(o Options) Options { return o },
			wantError:     false,
		},
		{
			name:          "negative HeartbeatTickCount",
			modifyOptions: func(o Options) Options { o.HeartbeatTickCount = -1; return o },
			wantError:     true,
		},
		{
			name:          "zero HeartbeatTickCount",
			modifyOptions: func(o Options) Options { o.HeartbeatTickCount = 0; return o },
			wantError:     true,
		},
		{
			name: "HeartbeatTickCount >= ElectionTickCount",
			modifyOptions: func(o Options) Options {
				o.HeartbeatTickCount = 10
				return o
			},
			wantError: true,
		},
		{
			name: "ElectionTickCount < 3 * HeartbeatTickCount",
			modifyOptions: func(o Options) Options {
				o.HeartbeatTickCount = 4
				o.ElectionTickCount = 11
				return o
			},
			wantError: true,
		},
		{
			name:          "ElectionRandomizationFactor negative",
			modifyOptions: func(o Options) Options { o.ElectionRandomizationFactor = -0.1; return o },
			wantError:     true,
		},
		{
			name:          "ElectionRandomizationFactor > 1.0",
			modifyOptions: func(o Options) Options { o.ElectionRandomizationFactor = 1.1; return o },
			wantError:     true,
		},
		{
			name:          "non-positive MaxLogEntriesPerRequest",
			modifyOptions: func(o Options) Options { o.MaxLogEntriesPerRequest = 0; return o },
			wantError:     true,
		},
		{
			name:          "SnapshotThreshold too low",
			modifyOptions: func(o Options) Options { o.SnapshotThreshold = 999; return o },
			wantError:     true,
		},
		{
			name:          "non-positive StorageSyncDelay",
			modifyOptions: func(o Options) Options { o.StorageSyncDelay = 0; return o },
			wantError:     true,
		},
		{
			name:          "StorageSyncDelay too high",
			modifyOptions: func(o Options) Options { o.StorageSyncDelay = 101; return o },
			wantError:     true,
		},
		{
			name:          "non-positive LogCompactionMinEntries",
			modifyOptions: func(o Options) Options { o.LogCompactionMinEntries = 0; return o },
			wantError:     true,
		},
		{
			name:          "non-positive ApplyTickCount",
			modifyOptions: func(o Options) Options { o.ApplyTickCount = 0; return o },
			wantError:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			options := tc.modifyOptions(baseOptions)
			err := options.Validate()

			if (err != nil) != tc.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tc.wantError)
			}
		})
	}
}

func TestFeatureFlagsValidate(t *testing.T) {
	testCases := []struct {
		name      string
		flags     FeatureFlags
		wantError bool
	}{
		{
			name:      "default flags are valid",
			flags:     DefaultFeatureFlags(),
			wantError: false,
		},
		{
			name: "EnableLeaderLease requires EnableReadIndex",
			flags: FeatureFlags{
				EnableReadIndex:   false,
				EnableLeaderLease: true,
				PreVoteEnabled:    true,
			},
			wantError: true,
		},
		{
			name: "valid custom configuration",
			flags: FeatureFlags{
				EnableReadIndex:   true,
				EnableLeaderLease: false,
				PreVoteEnabled:    false,
			},
			wantError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.flags.Validate()

			if (err != nil) != tc.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tc.wantError)
			}
		})
	}
}

func TestTuningParamsValidate(t *testing.T) {
	testCases := []struct {
		name      string
		params    TuningParams
		wantError bool
	}{
		{
			name:      "default params are valid",
			params:    DefaultTuningParams(),
			wantError: false,
		},
		{
			name: "non-positive MaxApplyBatchSize",
			params: TuningParams{
				MaxApplyBatchSize:    0,
				MaxSnapshotChunkSize: 0,
			},
			wantError: true,
		},
		{
			name: "negative MaxSnapshotChunkSize",
			params: TuningParams{
				MaxApplyBatchSize:    10,
				MaxSnapshotChunkSize: -1,
			},
			wantError: true,
		},
		{
			name: "valid custom params",
			params: TuningParams{
				MaxApplyBatchSize:    20,
				MaxSnapshotChunkSize: 1024,
			},
			wantError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.params.Validate()

			if (err != nil) != tc.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tc.wantError)
			}
		})
	}
}

func TestNewConfig(t *testing.T) {
	validNodeID := NodeID("node1")
	validPeers := []PeerConfig{
		{ID: "node1", Address: "localhost:1234"},
		{ID: "node2", Address: "localhost:1235"},
	}

	// Mocks for required components
	type mockStorage struct{ Storage }
	type mockTransport struct{ Transport }
	type mockStateMachine struct{ StateMachine }

	validStorage := &mockStorage{}
	validTransport := &mockTransport{}
	validStateMachine := &mockStateMachine{}

	t.Run("valid configuration", func(t *testing.T) {
		config, err := NewConfig(
			validNodeID,
			validPeers,
			DefaultOptions(),
			DefaultFeatureFlags(),
			DefaultTuningParams(),
			validStorage,
			validTransport,
			validStateMachine,
			nil, // Logger is optional
			nil, // Metrics is optional
		)

		if err != nil {
			t.Errorf("Expected no error with valid config, got: %v", err)
		}

		// Verify config fields were set correctly
		if config.ID != validNodeID {
			t.Errorf("Expected ID %q, got %q", validNodeID, config.ID)
		}
		if len(config.Peers) != len(validPeers) {
			t.Errorf("Expected %d peers, got %d", len(validPeers), len(config.Peers))
		}
		// Default logger and metrics should be provided when nil is passed
		if config.Logger == nil {
			t.Errorf("Expected non-nil Logger")
		}
		if config.Metrics == nil {
			t.Errorf("Expected non-nil Metrics")
		}
	})

	t.Run("invalid node ID", func(t *testing.T) {
		_, err := NewConfig(
			"", // Empty node ID
			validPeers,
			DefaultOptions(),
			DefaultFeatureFlags(),
			DefaultTuningParams(),
			validStorage,
			validTransport,
			validStateMachine,
			nil,
			nil,
		)

		if err == nil {
			t.Errorf("Expected error with empty node ID, got nil")
		}
	})

	t.Run("invalid peer configuration", func(t *testing.T) {
		invalidPeers := []PeerConfig{
			{ID: "node1", Address: "localhost:1234"},
			{ID: "node1", Address: "localhost:1235"}, // Duplicate ID
		}

		_, err := NewConfig(
			validNodeID,
			invalidPeers,
			DefaultOptions(),
			DefaultFeatureFlags(),
			DefaultTuningParams(),
			validStorage,
			validTransport,
			validStateMachine,
			nil,
			nil,
		)

		if err == nil {
			t.Errorf("Expected error with duplicate peer IDs, got nil")
		}
	})

	t.Run("invalid options", func(t *testing.T) {
		invalidOptions := DefaultOptions()
		invalidOptions.HeartbeatTickCount = 0 // Invalid value

		_, err := NewConfig(
			validNodeID,
			validPeers,
			invalidOptions,
			DefaultFeatureFlags(),
			DefaultTuningParams(),
			validStorage,
			validTransport,
			validStateMachine,
			nil,
			nil,
		)

		if err == nil {
			t.Errorf("Expected error with invalid options, got nil")
		}
	})

	t.Run("invalid feature flags", func(t *testing.T) {
		invalidFlags := FeatureFlags{
			EnableReadIndex:   false,
			EnableLeaderLease: true, // Invalid: EnableLeaderLease requires EnableReadIndex
			PreVoteEnabled:    true,
		}

		_, err := NewConfig(
			validNodeID,
			validPeers,
			DefaultOptions(),
			invalidFlags,
			DefaultTuningParams(),
			validStorage,
			validTransport,
			validStateMachine,
			nil,
			nil,
		)

		if err == nil {
			t.Errorf("Expected error with invalid feature flags, got nil")
		}
	})

	t.Run("invalid tuning params", func(t *testing.T) {
		invalidTuningParams := TuningParams{
			MaxApplyBatchSize:    0, // Invalid: must be positive
			MaxSnapshotChunkSize: 0,
		}

		_, err := NewConfig(
			validNodeID,
			validPeers,
			DefaultOptions(),
			DefaultFeatureFlags(),
			invalidTuningParams,
			validStorage,
			validTransport,
			validStateMachine,
			nil,
			nil,
		)

		if err == nil {
			t.Errorf("Expected error with invalid tuning params, got nil")
		}
	})

	t.Run("missing required components", func(t *testing.T) {
		_, err := NewConfig(
			validNodeID,
			validPeers,
			DefaultOptions(),
			DefaultFeatureFlags(),
			DefaultTuningParams(),
			nil, // Missing storage
			validTransport,
			validStateMachine,
			nil,
			nil,
		)

		if err == nil {
			t.Errorf("Expected error with missing required component, got nil")
		}
	})
}

func TestOptionsValidatePanics(t *testing.T) {
	// Test that "With" methods panic with invalid values
	testCases := []struct {
		name     string
		testFunc func()
	}{
		{
			name: "WithElectionTicks with non-positive value",
			testFunc: func() {
				DefaultOptions().WithElectionTicks(0)
			},
		},
		{
			name: "WithHeartbeatTicks with non-positive value",
			testFunc: func() {
				DefaultOptions().WithHeartbeatTicks(0)
			},
		},
		{
			name: "WithRandomizationFactor with negative value",
			testFunc: func() {
				DefaultOptions().WithRandomizationFactor(-0.1)
			},
		},
		{
			name: "WithMaxEntriesPerRequest with non-positive value",
			testFunc: func() {
				DefaultOptions().WithMaxEntriesPerRequest(0)
			},
		},
		{
			name: "WithSnapshotThreshold with non-positive value",
			testFunc: func() {
				DefaultOptions().WithSnapshotThreshold(0)
			},
		},
		{
			name: "WithStorageSyncDelay with non-positive value",
			testFunc: func() {
				DefaultOptions().WithStorageSyncDelay(0)
			},
		},
		{
			name: "WithLogCompactionMinEntries with non-positive value",
			testFunc: func() {
				DefaultOptions().WithLogCompactionMinEntries(0)
			},
		},
		{
			name: "WithApplyTickCount with non-positive value",
			testFunc: func() {
				DefaultOptions().WithApplyTickCount(0)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("Expected panic but none occurred")
				}
			}()

			tc.testFunc()
		})
	}
}

func TestValidatePeerSet(t *testing.T) {
	validNodeID := NodeID("node1")
	validPeers := []PeerConfig{
		{ID: "node1", Address: "localhost:1234"},
		{ID: "node2", Address: "localhost:1235"},
		{ID: "node3", Address: "localhost:1236"},
	}

	testCases := []struct {
		name      string
		nodeID    NodeID
		peers     []PeerConfig
		wantError bool
	}{
		{
			name:      "valid configuration",
			nodeID:    validNodeID,
			peers:     validPeers,
			wantError: false,
		},
		{
			name:      "empty node ID",
			nodeID:    "",
			peers:     validPeers,
			wantError: true,
		},
		{
			name:      "empty peer list",
			nodeID:    validNodeID,
			peers:     []PeerConfig{},
			wantError: true,
		},
		{
			name:   "peer with empty ID",
			nodeID: validNodeID,
			peers: []PeerConfig{
				{ID: "node1", Address: "localhost:1234"},
				{ID: "", Address: "localhost:1235"},
			},
			wantError: true,
		},
		{
			name:   "peer with empty address",
			nodeID: validNodeID,
			peers: []PeerConfig{
				{ID: "node1", Address: "localhost:1234"},
				{ID: "node2", Address: ""},
			},
			wantError: true,
		},
		{
			name:   "duplicate peer IDs",
			nodeID: validNodeID,
			peers: []PeerConfig{
				{ID: "node1", Address: "localhost:1234"},
				{ID: "node2", Address: "localhost:1235"},
				{ID: "node2", Address: "localhost:1236"},
			},
			wantError: true,
		},
		{
			name:   "node ID not in peer list",
			nodeID: "node4",
			peers: []PeerConfig{
				{ID: "node1", Address: "localhost:1234"},
				{ID: "node2", Address: "localhost:1235"},
				{ID: "node3", Address: "localhost:1236"},
			},
			wantError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePeerSet(tc.nodeID, tc.peers)

			if (err != nil) != tc.wantError {
				t.Errorf("validatePeerSet() error = %v, wantError %v", err, tc.wantError)
			}
		})
	}
}
