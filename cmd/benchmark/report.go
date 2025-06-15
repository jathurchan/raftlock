package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Reporter defines a generic interface for benchmark result reporters.
type Reporter interface {
	Generate(results *BenchmarkResults) error
}

// NewReporter returns a Reporter based on the output format in the config.
// It also returns the writer used for output, which may need to be closed by the caller.
func NewReporter(cfg *Config) (Reporter, io.WriteCloser, error) {
	var writer io.WriteCloser = os.Stdout

	if cfg.OutputFile != "" {
		f, err := os.Create(cfg.OutputFile)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create output file %s: %w", cfg.OutputFile, err)
		}
		writer = f
	}

	switch strings.ToLower(cfg.OutputFormat) {
	case "json":
		return &JsonReporter{writer: writer}, writer, nil
	case "text":
		return &TextReporter{writer: writer, verbose: cfg.Verbose}, writer, nil
	default:
		if writer != os.Stdout {
			writer.Close()
		}
		return nil, nil, fmt.Errorf("unsupported output format: %s", cfg.OutputFormat)
	}
}

// TextReporter generates a human-readable tabular report.
type TextReporter struct {
	writer  io.Writer
	verbose bool
}

// Generate writes a formatted benchmark report to the configured output.
func (r *TextReporter) Generate(results *BenchmarkResults) error {
	w := tabwriter.NewWriter(r.writer, 0, 0, 3, ' ', 0)
	p := func(format string, a ...any) {
		fmt.Fprintf(w, format+"\n", a...)
	}

	// Header
	p("🚀 RaftLock Comprehensive Benchmark Report")
	p("==========================================")
	p("Generated: %s", time.Now().Format(time.RFC1123))
	p("Test Duration: %s", results.TotalDuration)
	p("Benchmark Version: %s", results.SystemInfo.BenchmarkVersion)
	p("Cluster Size: %d nodes", results.SystemInfo.ClusterSize)
	p("")

	// Sections
	r.printExecutiveSummary(p, results)

	if results.UncontestedResults != nil {
		r.printUncontestedResults(p, results.UncontestedResults)
	}
	if results.ContentionResults != nil {
		r.printContentionResults(p, results.ContentionResults)
	}
	if results.FaultToleranceResults != nil {
		r.printFaultToleranceResults(p, results.FaultToleranceResults)
	}
	if results.Summary != nil {
		r.printRecommendations(p, results.Summary)
		r.printRiskAssessment(p, results.Summary)
		r.printComplianceStatus(p, results.Summary)
	}

	return w.Flush()
}

// printExecutiveSummary prints high-level summary metrics.
func (r *TextReporter) printExecutiveSummary(p func(string, ...any), results *BenchmarkResults) {
	p("🎯 EXECUTIVE SUMMARY")
	p("===================")

	summary := results.Summary
	if summary == nil {
		p("No summary available.")
		p("")
		return
	}

	p("Overall Score:\t%.1f/100", summary.OverallScore)
	p("Performance Grade:\t%s", summary.PerformanceGrade)
	p("Resilience Grade:\t%s", summary.ResilienceGrade)
	p("Production Ready:\t%v", summary.ProductionReadiness)

	p("\n📊 Key Metrics:")

	titleCase := cases.Title(language.English)
	for key, value := range summary.KeyMetrics {
		normalized := strings.ReplaceAll(key, "_", " ")
		displayKey := titleCase.String(normalized)
		p("  %s:\t%v", displayKey, value)
	}

	p("")
}

// printUncontestedResults prints metrics from uncontested tests.
func (r *TextReporter) printUncontestedResults(p func(string, ...any), results *UncontestedBenchmark) {
	p("📊 UNCONTESTED LATENCY ANALYSIS")
	p("===============================")
	p("Leader Optimization Factor:\t%.2fx", results.ImprovementFactor)
	p("Baseline Throughput:\t%.2f ops/sec", results.ThroughputBaseline)
	p("Optimization Effectiveness:\t%s", results.OptimizationEffectiveness)
	p("")

	p("Performance Comparison:")
	p("Metric\tLeader Optimized\tFollower Redirected")
	p("------\t----------------\t-------------------")
	p("Mean Latency\t%s\t%s", results.WithLeaderOptimization.Mean, results.WithoutLeaderOptimization.Mean)
	p("P95 Latency\t%s\t%s", results.WithLeaderOptimization.P95, results.WithoutLeaderOptimization.P95)
	p("P99 Latency\t%s\t%s", results.WithLeaderOptimization.P99, results.WithoutLeaderOptimization.P99)
	p("Success Rate\t%.2f%%\t%.2f%%", results.WithLeaderOptimization.SuccessRate, results.WithoutLeaderOptimization.SuccessRate)
	p("Throughput\t%.2f ops/s\t%.2f ops/s", results.WithLeaderOptimization.ThroughputOpsPerSec, results.WithoutLeaderOptimization.ThroughputOpsPerSec)
	p("")
}

// printContentionResults prints performance data under contention scenarios.
func (r *TextReporter) printContentionResults(p func(string, ...any), results *ContentionBenchmark) {
	p("⚔️ CONTENTION PERFORMANCE ANALYSIS")
	p("===================================")
	p("Scalability Assessment:\t%s", results.ScalabilityAssessment)
	p("Throughput Degradation:\t%.2f%%", results.ThroughputDegradation*100)
	p("Backoff Effectiveness:\t%.2f%%", results.BackoffEffectiveness)
	p("")

	p("Contention Level Comparison:")
	p("Metric\tLow\tMedium\tHigh")
	p("------\t---\t------\t----")
	p("Workers\t%d\t%d\t%d", results.LowContention.Workers, results.MediumContention.Workers, results.HighContention.Workers)
	p("Resources\t%d\t%d\t%d", results.LowContention.Resources, results.MediumContention.Resources, results.HighContention.Resources)
	p("Throughput (ops/s)\t%.2f\t%.2f\t%.2f", results.LowContention.Throughput, results.MediumContention.Throughput, results.HighContention.Throughput)
	p("Success Rate\t%.2f%%\t%.2f%%\t%.2f%%", results.LowContention.SuccessRate, results.MediumContention.SuccessRate, results.HighContention.SuccessRate)
	p("Avg Retries\t%.2f\t%.2f\t%.2f", results.LowContention.AverageRetries, results.MediumContention.AverageRetries, results.HighContention.AverageRetries)
	p("Efficiency\t%s\t%s\t%s", results.LowContention.EfficiencyRating, results.MediumContention.EfficiencyRating, results.HighContention.EfficiencyRating)
	p("")

	p("Contention Model Analysis:")
	p("  Baseline Throughput:\t%.2f ops/s", results.ContentionModel.BaselineThroughput)
	p("  Contention Factor:\t%.2f", results.ContentionModel.ContentionFactor)
	p("  Scalability Coefficient:\t%.2f", results.ContentionModel.ScalabilityCoefficient)
	p("  Bottleneck Analysis:\t%s", results.ContentionModel.BottleneckAnalysis)
	p("")
}

// printFaultToleranceResults prints results related to system resilience.
func (r *TextReporter) printFaultToleranceResults(p func(string, ...any), results *FaultToleranceBenchmark) {
	p("🛡️ FAULT TOLERANCE & RESILIENCE")
	p("================================")
	p("Resilience Grade:\t%s", results.ResilienceGrade)
	p("System Availability:\t%.4f%%", results.SystemAvailability)
	p("Leader Election Time:\t%s", results.LeaderElectionTime)
	p("Recovery Time:\t%s", results.RecoveryTime)
	p("Data Consistency:\t%.4f%% (%s)", results.DataConsistency.ConsistencyRate, results.DataConsistency.ConsistencyGrade)
	p("")

	p("Operational Performance by Phase:")
	p("Phase\tThroughput (ops/s)\tError Rate\tAvg Response Time")
	p("-----\t------------------\t----------\t-----------------")
	p("Baseline\t%.2f\t%.2f%%\t%s", results.BeforeFailure.Throughput, results.BeforeFailure.ErrorRate, results.BeforeFailure.AvgResponseTime)
	p("During Failure\t%.2f\t%.2f%%\t%s", results.DuringFailure.Throughput, results.DuringFailure.ErrorRate, results.DuringFailure.AvgResponseTime)
	p("After Recovery\t%.2f\t%.2f%%\t%s", results.AfterRecovery.Throughput, results.AfterRecovery.ErrorRate, results.AfterRecovery.AvgResponseTime)
	p("")
}

// printRecommendations outputs improvement suggestions.
func (r *TextReporter) printRecommendations(p func(string, ...any), summary *BenchmarkSummary) {
	p("💡 RECOMMENDATIONS")
	p("==================")
	if len(summary.Recommendations) == 0 {
		p("No specific recommendations - system performance is excellent.")
	} else {
		for i, rec := range summary.Recommendations {
			p("%d. %s", i+1, rec)
		}
	}
	p("")
}

// printRiskAssessment outputs potential system risks.
func (r *TextReporter) printRiskAssessment(p func(string, ...any), summary *BenchmarkSummary) {
	p("⚠️ RISK ASSESSMENT")
	p("==================")
	if len(summary.RiskAssessment) == 0 {
		p("No significant risks identified.")
	} else {
		for _, risk := range summary.RiskAssessment {
			p("• %s", risk)
		}
	}
	p("")
}

// printComplianceStatus reports on compliance with defined benchmarks.
func (r *TextReporter) printComplianceStatus(p func(string, ...any), summary *BenchmarkSummary) {
	p("✅ COMPLIANCE STATUS")
	p("====================")

	caser := cases.Title(language.English)

	for requirement, compliant := range summary.ComplianceStatus {
		status := "❌ FAIL"
		if compliant {
			status = "✅ PASS"
		}

		// Normalize and format the requirement key
		words := strings.ReplaceAll(requirement, "_", " ")
		displayReq := caser.String(words)

		p("%s\t%s", displayReq, status)
	}

	p("")
}

// JsonReporter outputs results in JSON format.
type JsonReporter struct {
	writer io.Writer
}

// Generate writes benchmark results in formatted JSON.
func (r *JsonReporter) Generate(results *BenchmarkResults) error {
	encoder := json.NewEncoder(r.writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(results)
}
