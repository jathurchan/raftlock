package benchmark

import (
	"context"
	"time"
)

// Command is a key-value operation.
type Command struct {
	Op    string `json:"op"` // "get", "set", or "delete"
	Key   string `json:"key"`
	Value int    `json:"value"`
}

// Result is the response from a command.
type Result struct {
	Value int  `json:"value"`
	Found bool `json:"found"`
	Error bool `json:"error"`
}

// BenchmarkNode represents a node in the benchmark cluster.
type BenchmarkNode interface {
	// Start initializes and runs the node.
	Start() error

	// Stop gracefully shuts down the node.
	Stop() error

	// NodeID returns the node's unique identifier.
	NodeID() string

	// IsLeader reports if the node is the current leader.
	IsLeader() bool

	// Propose submits a write command to the Raft consensus protocol.
	// Fails if the node isn't the leader or consensus fails.
	Propose(ctx context.Context, cmd Command) (Result, error)

	// Read performs a linearizable read.
	// The implementation depends on the configuration:
	// - **Baseline**: Reads through the consensus log (slow).
	// - **ReadIndex**: Uses the Raft ReadIndex protocol (faster).
	// - **LeaderLease**: Uses leader leases for even faster reads.
	Read(ctx context.Context, cmd Command) (Result, error)
}

// Config defines the benchmark scenario's configuration.
type Config struct {
	// Name is a unique identifier for the scenario (e.g., "ReadIndex+LeaderLease").
	Name string

	// EnableReadIndex enables the ReadIndex optimization for linearizable reads.
	// This reduces latency by avoiding the full Raft log.
	EnableReadIndex bool

	// EnableLeaderLease enables the leader lease optimization for faster reads.
	// Requires `EnableReadIndex`. Allows the leader to serve reads without network round-trips.
	EnableLeaderLease bool

	// NumClients is the number of concurrent clients generating load.
	NumClients int

	// TestDuration is how long the benchmark runs after warmup.
	TestDuration time.Duration

	// WarmupDuration is the time spent warming up the system before measurements.
	WarmupDuration time.Duration

	// ReadWriteRatio is the proportion of reads to writes (e.g., 0.95 = 95% reads).
	ReadWriteRatio float64

	// KeySpace is the number of unique keys in the workload.
	KeySpace int

	// DataDogAddr is the StatsD agent address for metrics (e.g., "127.0.0.1:8125").
	DataDogAddr string

	// MetricsInterval is the frequency of periodic metric reports to DataDog.
	MetricsInterval time.Duration
}

// BenchmarkResult contains performance metrics from a single run.
type BenchmarkResult struct {
	// ScenarioName identifies the benchmark configuration.
	ScenarioName string

	// IsLinearizable reports if the scenario passed formal linearizability verification.
	IsLinearizable bool

	// P50LatencyMs is the 50th percentile (median) latency in milliseconds.
	P50LatencyMs float64

	// P95LatencyMs is the 95th percentile latency in milliseconds.
	P95LatencyMs float64

	// P99LatencyMs is the 99th percentile latency in milliseconds.
	P99LatencyMs float64

	// P999LatencyMs is the 99.9th percentile latency in milliseconds.
	P999LatencyMs float64

	// MaxLatencyMs is the maximum observed latency.
	MaxLatencyMs float64

	// AvgLatencyMs is the average latency.
	AvgLatencyMs float64

	// ReadP50Ms is the 50th percentile latency for read operations.
	ReadP50Ms float64

	// ReadP99Ms is the 99th percentile latency for read operations.
	ReadP99Ms float64

	// WriteP50Ms is the 50th percentile latency for write operations.
	WriteP50Ms float64

	// WriteP99Ms is the 99th percentile latency for write operations.
	WriteP99Ms float64

	// ReadsPerSecond is the read operation throughput.
	ReadsPerSecond float64

	// WritesPerSecond is the write operation throughput.
	WritesPerSecond float64

	// TotalOpsPerSec is the combined read and write throughput.
	TotalOpsPerSec float64

	// ReadErrors is the total number of failed read operations.
	ReadErrors int

	// WriteErrors is the total number of failed write operations.
	WriteErrors int

	// ErrorRate is the percentage of operations that failed.
	ErrorRate float64

	// TotalOperations is the total number of operations attempted.
	TotalOperations int64

	// ActualDuration is the real wall-clock time the benchmark ran.
	ActualDuration time.Duration
}
