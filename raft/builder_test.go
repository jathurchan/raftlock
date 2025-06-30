package raft

import (
	"net"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

func TestRaftBuilder_NewRaftBuilder(t *testing.T) {
	builder := NewRaftBuilder()

	if builder == nil {
		t.Fatal("Expected non-nil builder")
	}

	if builder.config.ID != "" {
		t.Errorf("Expected empty ID, got %s", builder.config.ID)
	}

	if builder.applier != nil {
		t.Error("Expected nil applier")
	}

	if builder.logger != nil {
		t.Error("Expected nil logger")
	}

	if builder.metrics != nil {
		t.Error("Expected nil metrics")
	}

	if builder.clock != nil {
		t.Error("Expected nil clock")
	}

	if builder.rand != nil {
		t.Error("Expected nil rand")
	}

	if builder.storage != nil {
		t.Error("Expected nil storage")
	}
}

func TestRaftBuilder_RaftBuilder_WithConfig(t *testing.T) {
	builder := NewRaftBuilder()

	config := Config{
		ID: "node1",
		Peers: map[types.NodeID]PeerConfig{
			"node1": {Address: "localhost:8000"},
			"node2": {Address: "localhost:8001"},
		},
	}

	result := builder.WithConfig(config)

	if result != builder {
		t.Error("WithConfig should return the builder for chaining")
	}

	if builder.config.ID != "node1" {
		t.Errorf("Expected ID node1, got %s", builder.config.ID)
	}

	if len(builder.config.Peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(builder.config.Peers))
	}

	if builder.config.Peers["node1"].Address != "localhost:8000" {
		t.Errorf("Expected address localhost:8000, got %s", builder.config.Peers["node1"].Address)
	}
}

func TestRaftBuilder_RaftBuilder_WithApplier(t *testing.T) {
	builder := NewRaftBuilder()
	applier := &mockApplier{}

	result := builder.WithApplier(applier)

	if result != builder {
		t.Error("WithApplier should return the builder for chaining")
	}

	if builder.applier != applier {
		t.Error("Applier not properly set")
	}
}

func TestRaftBuilder_RaftBuilder_WithLogger(t *testing.T) {
	builder := NewRaftBuilder()
	logger := logger.NewNoOpLogger()

	result := builder.WithLogger(logger)

	if result != builder {
		t.Error("WithLogger should return the builder for chaining")
	}

	if builder.logger != logger {
		t.Error("Logger not properly set")
	}
}

func TestRaftBuilder_RaftBuilder_WithMetrics(t *testing.T) {
	builder := NewRaftBuilder()
	metrics := NewNoOpMetrics()

	result := builder.WithMetrics(metrics)

	if result != builder {
		t.Error("WithMetrics should return the builder for chaining")
	}

	if builder.metrics != metrics {
		t.Error("Metrics not properly set")
	}
}

func TestRaftBuilder_RaftBuilder_WithClock(t *testing.T) {
	builder := NewRaftBuilder()
	clock := NewStandardClock()

	result := builder.WithClock(clock)

	if result != builder {
		t.Error("WithClock should return the builder for chaining")
	}

	if builder.clock != clock {
		t.Error("Clock not properly set")
	}
}

func TestRaftBuilder_RaftBuilder_WithRand(t *testing.T) {
	builder := NewRaftBuilder()
	rand := NewStandardRand()

	result := builder.WithRand(rand)

	if result != builder {
		t.Error("WithRand should return the builder for chaining")
	}

	if builder.rand != rand {
		t.Error("Rand not properly set")
	}
}

func TestRaftBuilder_RaftBuilder_WithStorage(t *testing.T) {
	builder := NewRaftBuilder()
	storage := &mockStorage{}

	result := builder.WithStorage(storage)

	if result != builder {
		t.Error("WithStorage should return the builder for chaining")
	}

	if builder.storage != storage {
		t.Error("Storage not properly set")
	}
}

func TestRaftBuilder_WithListener(t *testing.T) {
	builder := NewRaftBuilder()

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	result := builder.WithListener(listener)

	if result != builder {
		t.Error("WithListener should return the builder for chaining")
	}
}

func TestRaftBuilder_RaftBuilder_Validate(t *testing.T) {
	builder := NewRaftBuilder()

	err := builder.validate()
	if err == nil {
		t.Error("Expected error for empty builder")
	}

	builder.WithConfig(Config{ID: "node1"})
	err = builder.validate()
	if err == nil {
		t.Error("Expected error with only config set")
	}

	builder.WithApplier(&mockApplier{})
	err = builder.validate()
	if err == nil {
		t.Error("Expected error with config and applier set")
	}

	builder.WithLogger(logger.NewNoOpLogger())
	err = builder.validate()
	if err == nil {
		t.Error("Expected error with config, applier, network manager, and logger set")
	}

	builder.WithStorage(&mockStorage{})
	err = builder.validate()
	if err != nil {
		t.Errorf("Unexpected error with all required components set: %v", err)
	}
}

func TestRaftBuilder_RaftBuilder_SetDefaults(t *testing.T) {
	builder := NewRaftBuilder()

	builder.WithConfig(Config{ID: "node1"})
	builder.WithApplier(&mockApplier{})
	builder.WithStorage(&mockStorage{})

	builder.setDefaults()

	if builder.logger == nil {
		t.Error("Expected logger to be defaulted")
	}

	if builder.metrics == nil {
		t.Error("Expected metrics to be defaulted")
	}

	if builder.clock == nil {
		t.Error("Expected clock to be defaulted")
	}

	if builder.rand == nil {
		t.Error("Expected rand to be defaulted")
	}

	customLogger := logger.NewNoOpLogger()
	customMetrics := NewNoOpMetrics()
	customClock := NewStandardClock()
	customRand := NewStandardRand()

	builder = NewRaftBuilder()
	builder.WithConfig(Config{ID: "node1"})
	builder.WithApplier(&mockApplier{})
	builder.WithLogger(customLogger)
	builder.WithMetrics(customMetrics)
	builder.WithClock(customClock)
	builder.WithRand(customRand)
	builder.WithStorage(&mockStorage{})

	builder.setDefaults()

	if builder.logger != customLogger {
		t.Error("Logger should not be defaulted when provided")
	}

	if builder.metrics != customMetrics {
		t.Error("Metrics should not be defaulted when provided")
	}

	if builder.clock != customClock {
		t.Error("Clock should not be defaulted when provided")
	}

	if builder.rand != customRand {
		t.Error("Rand should not be defaulted when provided")
	}
}

func TestRaftBuilder_RaftBuilder_CreateDependencies(t *testing.T) {
	builder := NewRaftBuilder()

	storage := &mockStorage{}
	applier := &mockApplier{}
	logger := logger.NewNoOpLogger()
	metrics := NewNoOpMetrics()
	clock := NewStandardClock()
	rand := NewStandardRand()

	builder.WithStorage(storage)
	builder.WithApplier(applier)
	builder.WithLogger(logger)
	builder.WithMetrics(metrics)
	builder.WithClock(clock)
	builder.WithRand(rand)

	deps := builder.createDependencies()

	if deps.Storage != storage {
		t.Error("Storage not correctly set in dependencies")
	}

	if deps.Applier != applier {
		t.Error("Applier not correctly set in dependencies")
	}

	if deps.Logger != logger {
		t.Error("Logger not correctly set in dependencies")
	}

	if deps.Metrics != metrics {
		t.Error("Metrics not correctly set in dependencies")
	}

	if deps.Clock != clock {
		t.Error("Clock not correctly set in dependencies")
	}

	if deps.Rand != rand {
		t.Error("Rand not correctly set in dependencies")
	}
}

func TestRaftBuilder_CalculateQuorumSize(t *testing.T) {
	tests := []struct {
		clusterSize int
		quorum      int
	}{
		{0, 0},
		{-1, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
		{6, 4},
		{7, 4},
	}

	for _, test := range tests {
		got := calculateQuorumSize(test.clusterSize)
		if got != test.quorum {
			t.Errorf("calculateQuorumSize(%d) = %d, want %d", test.clusterSize, got, test.quorum)
		}
	}
}

func TestRaftNodeBuilder_BuildSucceedsWithAllComponents(t *testing.T) {
	builder := NewRaftBuilder()

	builder.WithConfig(Config{
		ID: "node1",
		Peers: map[types.NodeID]PeerConfig{
			"node1": {Address: "localhost:8000"},
		},
		Options: Options{
			ApplyEntryTimeout:   500 * time.Millisecond,
			FetchEntriesTimeout: 500 * time.Millisecond,
		},
		TuningParams: TuningParams{
			MaxApplyBatchSize: 10,
		},
	})
	builder.WithApplier(&mockApplier{})
	builder.WithLogger(logger.NewNoOpLogger())
	builder.WithMetrics(NewNoOpMetrics())
	builder.WithClock(NewStandardClock())
	builder.WithRand(NewStandardRand())
	builder.WithStorage(&mockStorage{})

	raft, err := builder.Build()

	if err != nil {
		t.Errorf("Build failed with all components set: %v", err)
	}

	if raft == nil {
		t.Error("Expected non-nil Raft instance")
	}
}

func TestRaftNodeBuilder_BuildWithTimeoutDefaults(t *testing.T) {
	builder := NewRaftBuilder()

	builder.WithConfig(Config{
		ID: "node1",
		Peers: map[types.NodeID]PeerConfig{
			"node1": {Address: "localhost:8000"},
		},
		Options: Options{
			ApplyEntryTimeout:   0, // Should be defaulted
			FetchEntriesTimeout: 0, // Should be defaulted
		},
	})
	builder.WithApplier(&mockApplier{})
	builder.WithLogger(logger.NewNoOpLogger())
	builder.WithStorage(&mockStorage{})

	raft, err := builder.Build()

	if err != nil {
		t.Errorf("Build failed with unset timeouts: %v", err)
	}

	if raft == nil {
		t.Error("Expected non-nil Raft instance")
	}
}
