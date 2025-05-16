package raft

import (
	"errors"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
)

func TestRaftDependencies_Validate_Success(t *testing.T) {
	deps := Dependencies{
		Storage: &mockStorage{},
		Network: &mockNetworkManager{},
		Applier: &mockApplier{},
		Clock:   &mockClock{},
		Rand:    &mockRand{},
	}

	err := deps.Validate()
	testutil.AssertNoError(t, err, "Expected valid dependencies to pass validation")

	deps.Logger = nil
	deps.Metrics = nil
	err = deps.Validate()
	testutil.AssertNoError(t, err, "Expected valid dependencies with explicitly nil optional fields to pass validation")

	deps.Logger = logger.NewNoOpLogger()
	deps.Metrics = NewNoOpMetrics()
	err = deps.Validate()
	testutil.AssertNoError(t, err, "Expected valid dependencies with all fields to pass validation")
}

func TestRaftDependencies_Validate_NilStruct(t *testing.T) {
	var deps *Dependencies = nil

	err := deps.Validate()
	testutil.AssertError(t, err, "Expected error when Dependencies struct is nil")
	testutil.AssertErrorIs(t, errors.Unwrap(err), ErrMissingDependencies,
		"Expected ErrMissingDependencies when Dependencies struct is nil")
	testutil.AssertContains(t, err.Error(), "dependencies struct cannot be nil",
		"Expected error message to mention 'dependencies struct cannot be nil'")
}

func TestRaftDependencies_Validate_MissingDependencies(t *testing.T) {
	testCases := []struct {
		name            string
		dependencies    Dependencies
		expectedMissing string
	}{
		{
			name: "Missing Storage",
			dependencies: Dependencies{
				Storage: nil,
				Network: &mockNetworkManager{},
				Applier: &mockApplier{},
				Clock:   &mockClock{},
				Rand:    &mockRand{},
			},
			expectedMissing: "Storage dependency cannot be nil",
		},
		{
			name: "Missing Network",
			dependencies: Dependencies{
				Storage: &mockStorage{},
				Network: nil,
				Applier: &mockApplier{},
				Clock:   &mockClock{},
				Rand:    &mockRand{},
			},
			expectedMissing: "Network dependency cannot be nil",
		},
		{
			name: "Missing Applier",
			dependencies: Dependencies{
				Storage: &mockStorage{},
				Network: &mockNetworkManager{},
				Applier: nil,
				Clock:   &mockClock{},
				Rand:    &mockRand{},
			},
			expectedMissing: "Applier dependency cannot be nil",
		},
		{
			name: "Missing Clock",
			dependencies: Dependencies{
				Storage: &mockStorage{},
				Network: &mockNetworkManager{},
				Applier: &mockApplier{},
				Clock:   nil,
				Rand:    &mockRand{},
			},
			expectedMissing: "Clock dependency cannot be nil",
		},
		{
			name: "Missing Rand",
			dependencies: Dependencies{
				Storage: &mockStorage{},
				Network: &mockNetworkManager{},
				Applier: &mockApplier{},
				Clock:   &mockClock{},
				Rand:    nil,
			},
			expectedMissing: "Rand dependency cannot be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.dependencies.Validate()

			testutil.AssertError(t, err, "Expected error for %s", tc.name)
			testutil.AssertErrorIs(t, errors.Unwrap(err), ErrMissingDependencies,
				"Expected ErrMissingDependencies for %s", tc.name)
			testutil.AssertContains(t, err.Error(), tc.expectedMissing,
				"Expected error message to mention '%s'", tc.expectedMissing)
		})
	}
}
