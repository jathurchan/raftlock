package main

import (
	"time"

	"github.com/jathurchan/raftlock/cmd/benchmark/internal"
	"github.com/jathurchan/raftlock/logger"
)

// BenchmarkSuite coordinates the execution of a complete benchmark run.
type BenchmarkSuite struct {
	config    *Config
	clients   *clientPool
	dockerMgr *internal.DockerManager
	metrics   *internal.MetricsCollector
	logger    logger.Logger
	clientID  string
	results   *BenchmarkResults
	startTime time.Time
}

// BenchmarkResults contains all results from a benchmark suite run.
type BenchmarkResults struct {
	SuiteConfig           *Config                  `json:"suite_config" yaml:"suite_config"`
	StartTime             time.Time                `json:"start_time" yaml:"start_time"`
	EndTime               time.Time                `json:"end_time" yaml:"end_time"`
	TotalDuration         string                   `json:"total_duration" yaml:"total_duration"`
	UncontestedResults    *UncontestedBenchmark    `json:"uncontested_results,omitempty" yaml:"uncontested_results,omitempty"`
	ContentionResults     *ContentionBenchmark     `json:"contention_results,omitempty" yaml:"contention_results,omitempty"`
	FaultToleranceResults *FaultToleranceBenchmark `json:"fault_tolerance_results,omitempty" yaml:"fault_tolerance_results,omitempty"`
	Summary               *BenchmarkSummary        `json:"summary" yaml:"summary"`
	SystemInfo            *SystemInfo              `json:"system_info" yaml:"system_info"`
}

// UncontestedBenchmark reports performance metrics without contention.
type UncontestedBenchmark struct {
	WithLeaderOptimization    LatencyStats `json:"with_leader_optimization" yaml:"with_leader_optimization"`
	WithoutLeaderOptimization LatencyStats `json:"without_leader_optimization" yaml:"without_leader_optimization"`
	ImprovementFactor         float64      `json:"improvement_factor" yaml:"improvement_factor"`
	ThroughputBaseline        float64      `json:"throughput_baseline_ops_sec" yaml:"throughput_baseline_ops_sec"`
	OptimizationEffectiveness string       `json:"optimization_effectiveness" yaml:"optimization_effectiveness"`
}

// ContentionBenchmark reports metrics under varying contention levels.
type ContentionBenchmark struct {
	LowContention         ContentionStats `json:"low_contention" yaml:"low_contention"`
	MediumContention      ContentionStats `json:"medium_contention" yaml:"medium_contention"`
	HighContention        ContentionStats `json:"high_contention" yaml:"high_contention"`
	BackoffEffectiveness  float64         `json:"backoff_effectiveness_percent" yaml:"backoff_effectiveness_percent"`
	ThroughputDegradation float64         `json:"throughput_degradation_percent" yaml:"throughput_degradation_percent"`
	ContentionModel       ContentionModel `json:"contention_model" yaml:"contention_model"`
	ScalabilityAssessment string          `json:"scalability_assessment" yaml:"scalability_assessment"`
}

// FaultToleranceBenchmark captures system behavior during faults and recovery.
type FaultToleranceBenchmark struct {
	BeforeFailure      OperationalStats `json:"before_failure" yaml:"before_failure"`
	DuringFailure      OperationalStats `json:"during_failure" yaml:"during_failure"`
	AfterRecovery      OperationalStats `json:"after_recovery" yaml:"after_recovery"`
	LeaderElectionTime string           `json:"leader_election_time" yaml:"leader_election_time"`
	SystemAvailability float64          `json:"system_availability_percent" yaml:"system_availability_percent"`
	RecoveryTime       string           `json:"recovery_time" yaml:"recovery_time"`
	DataConsistency    ConsistencyCheck `json:"data_consistency" yaml:"data_consistency"`
	ResilienceGrade    string           `json:"resilience_grade" yaml:"resilience_grade"`
}

// BenchmarkSummary provides a high-level summary of benchmark results.
type BenchmarkSummary struct {
	OverallScore        float64                `json:"overall_score" yaml:"overall_score"`
	PerformanceGrade    string                 `json:"performance_grade" yaml:"performance_grade"`
	ResilienceGrade     string                 `json:"resilience_grade" yaml:"resilience_grade"`
	ProductionReadiness bool                   `json:"production_readiness" yaml:"production_readiness"`
	KeyMetrics          map[string]interface{} `json:"key_metrics" yaml:"key_metrics"`
	Recommendations     []string               `json:"recommendations" yaml:"recommendations"`
	RiskAssessment      []string               `json:"risk_assessment" yaml:"risk_assessment"`
	ComplianceStatus    map[string]bool        `json:"compliance_status" yaml:"compliance_status"`
}

// LatencyStats reports distribution metrics for latency measurements.
type LatencyStats struct {
	Description         string  `json:"description" yaml:"description"`
	Count               int64   `json:"count" yaml:"count"`
	SuccessfulCount     int64   `json:"successful_count" yaml:"successful_count"`
	FailedCount         int64   `json:"failed_count" yaml:"failed_count"`
	SuccessRate         float64 `json:"success_rate_percent" yaml:"success_rate_percent"`
	Mean                string  `json:"mean" yaml:"mean"`
	Median              string  `json:"median_p50" yaml:"median_p50"`
	P90                 string  `json:"p90" yaml:"p90"`
	P95                 string  `json:"p95" yaml:"p95"`
	P99                 string  `json:"p99" yaml:"p99"`
	P999                string  `json:"p999" yaml:"p999"`
	Min                 string  `json:"min" yaml:"min"`
	Max                 string  `json:"max" yaml:"max"`
	StdDev              string  `json:"std_dev" yaml:"std_dev"`
	ThroughputOpsPerSec float64 `json:"throughput_ops_per_sec" yaml:"throughput_ops_per_sec"`
}

// ContentionStats reports system performance under specific contention levels.
type ContentionStats struct {
	Level                 string  `json:"level" yaml:"level"`
	Workers               int     `json:"workers" yaml:"workers"`
	Resources             int     `json:"resources" yaml:"resources"`
	TotalOperations       int64   `json:"total_operations" yaml:"total_operations"`
	SuccessfulOperations  int64   `json:"successful_operations" yaml:"successful_operations"`
	FailedOperations      int64   `json:"failed_operations" yaml:"failed_operations"`
	TimeoutOperations     int64   `json:"timeout_operations" yaml:"timeout_operations"`
	LeaderRedirects       int64   `json:"leader_redirects" yaml:"leader_redirects"`
	Throughput            float64 `json:"throughput_ops_sec" yaml:"throughput_ops_sec"`
	SuccessRate           float64 `json:"success_rate_percent" yaml:"success_rate_percent"`
	AverageRetries        float64 `json:"average_retries" yaml:"average_retries"`
	AverageLatency        string  `json:"average_latency" yaml:"average_latency"`
	ContentionCoefficient float64 `json:"contention_coefficient" yaml:"contention_coefficient"`
	EfficiencyRating      string  `json:"efficiency_rating" yaml:"efficiency_rating"`
}

// OperationalStats tracks performance across defined operational phases.
type OperationalStats struct {
	Phase               string  `json:"phase" yaml:"phase"`
	Duration            string  `json:"duration" yaml:"duration"`
	TotalOps            int64   `json:"total_ops" yaml:"total_ops"`
	SuccessfulOps       int64   `json:"successful_ops" yaml:"successful_ops"`
	FailedOps           int64   `json:"failed_ops" yaml:"failed_ops"`
	TimeoutOps          int64   `json:"timeout_ops" yaml:"timeout_ops"`
	LeaderRedirects     int64   `json:"leader_redirects" yaml:"leader_redirects"`
	Throughput          float64 `json:"throughput_ops_sec" yaml:"throughput_ops_sec"`
	ErrorRate           float64 `json:"error_rate_percent" yaml:"error_rate_percent"`
	AvgResponseTime     string  `json:"avg_response_time" yaml:"avg_response_time"`
	ResourceUtilization float64 `json:"resource_utilization_percent" yaml:"resource_utilization_percent"`
}

// ContentionModel models how contention affects system performance.
type ContentionModel struct {
	BaselineThroughput     float64 `json:"baseline_throughput" yaml:"baseline_throughput"`
	ContentionFactor       float64 `json:"contention_factor" yaml:"contention_factor"`
	BackoffOptimalness     float64 `json:"backoff_optimalness" yaml:"backoff_optimalness"`
	ResourceUtilization    float64 `json:"resource_utilization" yaml:"resource_utilization"`
	ScalabilityCoefficient float64 `json:"scalability_coefficient" yaml:"scalability_coefficient"`
	BottleneckAnalysis     string  `json:"bottleneck_analysis" yaml:"bottleneck_analysis"`
}

// ConsistencyCheck tracks data consistency across the cluster.
type ConsistencyCheck struct {
	TotalChecks          int64   `json:"total_checks" yaml:"total_checks"`
	ConsistentReads      int64   `json:"consistent_reads" yaml:"consistent_reads"`
	InconsistentReads    int64   `json:"inconsistent_reads" yaml:"inconsistent_reads"`
	ConsistencyRate      float64 `json:"consistency_rate_percent" yaml:"consistency_rate_percent"`
	LinearizabilityScore float64 `json:"linearizability_score" yaml:"linearizability_score"`
	ConsistencyGrade     string  `json:"consistency_grade" yaml:"consistency_grade"`
}

// SystemInfo holds metadata about the system and environment under test.
type SystemInfo struct {
	BenchmarkVersion string            `json:"benchmark_version" yaml:"benchmark_version"`
	StartTimestamp   time.Time         `json:"start_timestamp" yaml:"start_timestamp"`
	EndTimestamp     time.Time         `json:"end_timestamp" yaml:"end_timestamp"`
	TestEnvironment  string            `json:"test_environment" yaml:"test_environment"`
	ClusterSize      int               `json:"cluster_size" yaml:"cluster_size"`
	ClientVersion    string            `json:"client_version" yaml:"client_version"`
	GoVersion        string            `json:"go_version" yaml:"go_version"`
	Platform         string            `json:"platform" yaml:"platform"`
	ConfigSummary    map[string]string `json:"config_summary" yaml:"config_summary"`
}

// OperationResult tracks detailed metrics for a single operation.
type OperationResult struct {
	LockID         string          `json:"lock_id" yaml:"lock_id"`
	ClientID       string          `json:"client_id" yaml:"client_id"`
	StartTime      time.Time       `json:"start_time" yaml:"start_time"`
	EndTime        time.Time       `json:"end_time" yaml:"end_time"`
	Duration       time.Duration   `json:"duration" yaml:"duration"`
	AcquireLatency time.Duration   `json:"acquire_latency" yaml:"acquire_latency"`
	ReleaseLatency time.Duration   `json:"release_latency" yaml:"release_latency"`
	Attempts       int             `json:"attempts" yaml:"attempts"`
	Success        bool            `json:"success" yaml:"success"`
	Error          string          `json:"error,omitempty" yaml:"error,omitempty"`
	LastError      string          `json:"last_error,omitempty" yaml:"last_error,omitempty"`
	LeaderRedirect bool            `json:"leader_redirect" yaml:"leader_redirect"`
	WasTimeout     bool            `json:"was_timeout" yaml:"was_timeout"`
	RetryPattern   []time.Duration `json:"retry_pattern,omitempty" yaml:"retry_pattern,omitempty"`
}
