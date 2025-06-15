package main

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// calculateLatencyStats computes detailed latency statistics from raw data.
func calculateLatencyStats(description string, latencies []time.Duration, successful, total int64) LatencyStats {
	if total == 0 {
		return LatencyStats{
			Description:     description,
			Count:           0,
			SuccessfulCount: 0,
			FailedCount:     0,
			SuccessRate:     100.0,
		}
	}

	failed := total - successful
	successRate := float64(successful) * 100.0 / float64(total)

	if len(latencies) == 0 {
		return LatencyStats{
			Description:     description,
			Count:           total,
			SuccessfulCount: successful,
			FailedCount:     failed,
			SuccessRate:     successRate,
		}
	}

	// Sort latencies for percentile calculations
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate mean
	var sum time.Duration
	for _, lat := range latencies {
		sum += lat
	}
	n := len(latencies)
	mean := sum / time.Duration(n)

	// Calculate standard deviation
	var variance float64
	for _, lat := range latencies {
		diff := float64(lat - mean)
		variance += diff * diff
	}
	variance /= float64(n)
	stdDev := time.Duration(math.Sqrt(variance))

	// Calculate throughput
	throughput := calculateThroughput(latencies, successful)

	return LatencyStats{
		Description:         description,
		Count:               total,
		SuccessfulCount:     successful,
		FailedCount:         failed,
		SuccessRate:         successRate,
		Mean:                mean.String(),
		Median:              percentile(latencies, 50).String(),
		P90:                 percentile(latencies, 90).String(),
		P95:                 percentile(latencies, 95).String(),
		P99:                 percentile(latencies, 99).String(),
		P999:                percentile(latencies, 99.9).String(),
		Min:                 latencies[0].String(),
		Max:                 latencies[n-1].String(),
		StdDev:              stdDev.String(),
		ThroughputOpsPerSec: throughput,
	}
}

// percentile returns the specified percentile from a sorted duration slice.
func percentile(sortedDurations []time.Duration, p float64) time.Duration {
	if len(sortedDurations) == 0 {
		return 0
	}

	if p <= 0 {
		return sortedDurations[0]
	}
	if p >= 100 {
		return sortedDurations[len(sortedDurations)-1]
	}

	index := p / 100.0 * float64(len(sortedDurations))

	if index == float64(int(index)) {
		// Exact index
		idx := int(index) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sortedDurations) {
			idx = len(sortedDurations) - 1
		}
		return sortedDurations[idx]
	}

	// Interpolate between two values
	lower := int(math.Floor(index)) - 1
	upper := int(math.Ceil(index)) - 1

	if lower < 0 {
		lower = 0
	}
	if upper >= len(sortedDurations) {
		upper = len(sortedDurations) - 1
	}
	if lower >= len(sortedDurations) {
		lower = len(sortedDurations) - 1
	}

	if lower == upper {
		return sortedDurations[lower]
	}

	// Linear interpolation
	fraction := index - math.Floor(index)
	lowerVal := float64(sortedDurations[lower])
	upperVal := float64(sortedDurations[upper])
	interpolated := lowerVal + fraction*(upperVal-lowerVal)

	return time.Duration(interpolated)
}

// calculateThroughput estimates throughput in operations per second.
func calculateThroughput(latencies []time.Duration, successful int64) float64 {
	if len(latencies) == 0 || successful == 0 {
		return 0.0
	}

	// Calculate average latency
	var sum time.Duration
	for _, lat := range latencies {
		sum += lat
	}
	avgLatency := sum / time.Duration(len(latencies))

	if avgLatency <= 0 {
		return 0.0
	}

	return 1.0 / avgLatency.Seconds()
}

// calculateContentionCoefficient returns a contention score based on worker/resource ratio and retry penalty.
func calculateContentionCoefficient(workers, resources int, avgRetries float64) float64 {
	if resources == 0 {
		return math.Inf(1) // Infinite contention
	}

	contentionRatio := float64(workers) / float64(resources)
	retryPenalty := 1.0 + (avgRetries / 10.0) // Penalty factor for retries

	return contentionRatio * retryPenalty
}

// calculateResourceUtilization estimates system efficiency during contention scenarios.
func (s *BenchmarkSuite) calculateResourceUtilization(stats ContentionStats) float64 {
	if stats.TotalOperations == 0 {
		return 0.0
	}

	// Factor in success rate, throughput, and retry efficiency
	baseUtilization := stats.SuccessRate / 100.0
	throughputFactor := math.Min(1.0, stats.Throughput/1000.0) // Normalize to reasonable scale
	retryPenalty := 1.0 / (1.0 + stats.AverageRetries/5.0)     // Penalty for excessive retries

	utilization := baseUtilization * throughputFactor * retryPenalty * 100.0

	if utilization > 100.0 {
		utilization = 100.0
	}

	return utilization
}

// calculateBackoffEffectiveness assesses how well the system handles increasing contention.
func (s *BenchmarkSuite) calculateBackoffEffectiveness(low, medium, high ContentionStats) float64 {
	if low.SuccessRate == 0 {
		return 0.0
	}

	// How well does the system maintain performance under increasing contention?
	lowToMedium := medium.SuccessRate / low.SuccessRate
	mediumToHigh := high.SuccessRate / medium.SuccessRate

	// Average the degradation ratios
	avgDegradation := (lowToMedium + mediumToHigh) / 2.0

	// Convert to effectiveness percentage
	effectiveness := avgDegradation * 100.0
	if effectiveness > 100.0 {
		effectiveness = 100.0
	}

	return effectiveness
}

// generateBenchmarkSummary aggregates key metrics, computes scores, and generates final benchmark analysis.
func (s *BenchmarkSuite) generateBenchmarkSummary() {
	if s.results.Summary == nil {
		s.results.Summary = &BenchmarkSummary{
			KeyMetrics:       make(map[string]interface{}),
			Recommendations:  make([]string, 0),
			RiskAssessment:   make([]string, 0),
			ComplianceStatus: make(map[string]bool),
		}
	}

	// Calculate individual scores
	perfScore := s.calculateOverallPerformanceScore()
	resilScore := s.calculateResilienceScore()

	// Overall score is weighted average
	s.results.Summary.OverallScore = (perfScore*0.6 + resilScore*0.4) // Weight performance higher
	s.results.Summary.PerformanceGrade = scoreToGrade(perfScore)
	s.results.Summary.ResilienceGrade = scoreToGrade(resilScore)
	s.results.Summary.ProductionReadiness = s.assessProductionReadiness()
	s.results.Summary.KeyMetrics = s.generateKeyMetrics()
	s.results.Summary.Recommendations = s.generateRecommendations()
	s.results.Summary.RiskAssessment = s.generateRiskAssessment()
	s.results.Summary.ComplianceStatus = s.generateComplianceStatus()
}

// calculateOverallPerformanceScore computes a weighted score based on latency, throughput, and success rate.
func (s *BenchmarkSuite) calculateOverallPerformanceScore() float64 {
	var scores []float64
	var weights []float64

	// Uncontested performance (weight: 30%)
	if r := s.results.UncontestedResults; r != nil {
		p99, _ := time.ParseDuration(r.WithLeaderOptimization.P99)
		if p99 > 0 {
			// Score based on P99 latency (lower is better)
			latencyScore := math.Max(0, 100-float64(p99.Milliseconds())/10) // 10ms = 90 points
			scores = append(scores, latencyScore)
			weights = append(weights, 0.3)
		}

		// Throughput score (weight: 20%)
		if r.ThroughputBaseline > 0 {
			throughputScore := math.Min(100, r.ThroughputBaseline/100*10) // 1000 ops/s = 100 points
			scores = append(scores, throughputScore)
			weights = append(weights, 0.2)
		}
	}

	// Contention performance (weight: 50%)
	if r := s.results.ContentionResults; r != nil {
		// High contention success rate is most important
		scores = append(scores, r.HighContention.SuccessRate)
		weights = append(weights, 0.5)
	}

	return weightedAverage(scores, weights)
}

// calculateResilienceScore computes a weighted score for system resilience (availability, consistency, recovery).
func (s *BenchmarkSuite) calculateResilienceScore() float64 {
	var scores []float64
	var weights []float64

	if r := s.results.FaultToleranceResults; r != nil {
		// System availability (weight: 40%)
		scores = append(scores, r.SystemAvailability)
		weights = append(weights, 0.4)

		// Leader election time (weight: 30%)
		electionTime, _ := time.ParseDuration(r.LeaderElectionTime)
		if electionTime > 0 {
			electionScore := math.Max(0, 100-(electionTime.Seconds()*5)) // 20s = 0 points
			scores = append(scores, electionScore)
			weights = append(weights, 0.3)
		}

		// Data consistency (weight: 30%)
		scores = append(scores, r.DataConsistency.ConsistencyRate)
		weights = append(weights, 0.3)
	}

	return weightedAverage(scores, weights)
}

// weightedAverage returns the weighted average of the given scores.
func weightedAverage(scores, weights []float64) float64 {
	if len(scores) == 0 || len(scores) != len(weights) {
		return 0.0
	}

	var sum, weightSum float64
	for i, score := range scores {
		sum += score * weights[i]
		weightSum += weights[i]
	}

	if weightSum == 0 {
		return 0.0
	}

	return sum / weightSum
}

// scoreToGrade converts a numeric score into a letter-grade representation.
func scoreToGrade(score float64) string {
	switch {
	case score >= 95:
		return "A+ (Exceptional)"
	case score >= 90:
		return "A (Excellent)"
	case score >= 85:
		return "A- (Very Good)"
	case score >= 80:
		return "B+ (Good)"
	case score >= 75:
		return "B (Above Average)"
	case score >= 70:
		return "B- (Average)"
	case score >= 65:
		return "C+ (Below Average)"
	case score >= 60:
		return "C (Poor)"
	case score >= 50:
		return "D (Very Poor)"
	default:
		return "F (Fail)"
	}
}

// assessProductionReadiness checks if the benchmarked system meets minimum thresholds for production use.
func (s *BenchmarkSuite) assessProductionReadiness() bool {
	summary := s.results.Summary
	if summary == nil {
		return false
	}

	// Must pass all critical thresholds
	criticalChecks := []bool{
		summary.OverallScore >= 70,                                              // Overall performance
		s.results.ContentionResults.HighContention.SuccessRate >= 85,            // High contention performance
		s.results.FaultToleranceResults.SystemAvailability >= 99.5,              // High availability
		s.results.FaultToleranceResults.DataConsistency.ConsistencyRate >= 99.9, // Data consistency
	}

	// Check leader election time
	if electionTime, err := time.ParseDuration(s.results.FaultToleranceResults.LeaderElectionTime); err == nil {
		criticalChecks = append(criticalChecks, electionTime <= 15*time.Second)
	}

	// All checks must pass
	for _, check := range criticalChecks {
		if !check {
			return false
		}
	}

	return true
}

// generateKeyMetrics extracts key metrics for summary and reporting.
func (s *BenchmarkSuite) generateKeyMetrics() map[string]interface{} {
	metrics := make(map[string]interface{})

	if r := s.results.UncontestedResults; r != nil {
		metrics["baseline_p99_latency"] = r.WithLeaderOptimization.P99
		metrics["baseline_throughput"] = fmt.Sprintf("%.2f ops/sec", r.ThroughputBaseline)
		metrics["leader_optimization_factor"] = fmt.Sprintf("%.2fx", r.ImprovementFactor)
	}

	if r := s.results.ContentionResults; r != nil {
		metrics["high_contention_success_rate"] = fmt.Sprintf("%.2f%%", r.HighContention.SuccessRate)
		metrics["throughput_degradation"] = fmt.Sprintf("%.2f%%", r.ThroughputDegradation*100)
		metrics["backoff_effectiveness"] = fmt.Sprintf("%.2f%%", r.BackoffEffectiveness)
	}

	if r := s.results.FaultToleranceResults; r != nil {
		metrics["leader_election_time"] = r.LeaderElectionTime
		metrics["system_availability"] = fmt.Sprintf("%.4f%%", r.SystemAvailability)
		metrics["data_consistency_rate"] = fmt.Sprintf("%.4f%%", r.DataConsistency.ConsistencyRate)
		metrics["recovery_time"] = r.RecoveryTime
	}

	// Calculate composite metrics
	duration := s.results.EndTime.Sub(s.results.StartTime)
	metrics["total_test_duration"] = duration.String()
	metrics["benchmark_score"] = fmt.Sprintf("%.1f/100", s.results.Summary.OverallScore)

	return metrics
}

// generateRecommendations provides actionable performance and reliability improvement suggestions.
func (s *BenchmarkSuite) generateRecommendations() []string {
	var recs []string

	if s.results.Summary.OverallScore >= 90 {
		recs = append(recs, "System performs excellently across all benchmarks. Ready for production deployment.")
		return recs
	}

	// Performance recommendations
	if s.results.UncontestedResults != nil {
		if p99, _ := time.ParseDuration(s.results.UncontestedResults.WithLeaderOptimization.P99); p99 > 100*time.Millisecond {
			recs = append(recs, "Consider optimizing lock acquisition path to reduce P99 latency below 100ms.")
		}

		if s.results.UncontestedResults.ImprovementFactor < 2.0 {
			recs = append(recs, "Leader optimization shows limited benefit. Review client-side leader discovery logic.")
		}
	}

	// Contention recommendations
	if s.results.ContentionResults != nil {
		if s.results.ContentionResults.HighContention.SuccessRate < 85 {
			recs = append(recs, "High contention success rate is below 85%. Consider implementing adaptive backoff or queue management.")
		}

		if s.results.ContentionResults.ThroughputDegradation > 0.7 {
			recs = append(recs, "Severe throughput degradation under contention. Review lock management algorithms.")
		}

		if s.results.ContentionResults.BackoffEffectiveness < 60 {
			recs = append(recs, "Backoff strategy is ineffective. Consider exponential backoff with jitter.")
		}
	}

	// Resilience recommendations
	if s.results.FaultToleranceResults != nil {
		if s.results.FaultToleranceResults.SystemAvailability < 99.9 {
			recs = append(recs, "System availability is below 99.9%. Optimize leader election and failover procedures.")
		}

		if electionTime, _ := time.ParseDuration(s.results.FaultToleranceResults.LeaderElectionTime); electionTime > 10*time.Second {
			recs = append(recs, "Leader election time exceeds 10 seconds. Tune election timeouts and heartbeat intervals.")
		}

		if s.results.FaultToleranceResults.DataConsistency.ConsistencyRate < 99.99 {
			recs = append(recs, "Data consistency rate is below 99.99%. Review Raft implementation and commit procedures.")
		}
	}

	if len(recs) == 0 {
		recs = append(recs, "No specific recommendations. System performance is within acceptable ranges.")
	}

	return recs
}

// generateRiskAssessment identifies potential performance and resilience risks.
func (s *BenchmarkSuite) generateRiskAssessment() []string {
	var risks []string

	// Performance risks
	if s.results.UncontestedResults != nil {
		if p99, _ := time.ParseDuration(s.results.UncontestedResults.WithLeaderOptimization.P99); p99 > 500*time.Millisecond {
			risks = append(risks, "HIGH: P99 latency exceeds 500ms, may impact user experience")
		}

		if s.results.UncontestedResults.ThroughputBaseline < 100 {
			risks = append(risks, "MEDIUM: Low baseline throughput may cause bottlenecks under load")
		}
	}

	// Contention risks
	if s.results.ContentionResults != nil {
		if s.results.ContentionResults.HighContention.SuccessRate < 70 {
			risks = append(risks, "HIGH: Poor performance under contention, system may fail under peak load")
		}

		if s.results.ContentionResults.HighContention.AverageRetries > 5 {
			risks = append(risks, "MEDIUM: High retry rate indicates inefficient contention handling")
		}
	}

	// Resilience risks
	if s.results.FaultToleranceResults != nil {
		if s.results.FaultToleranceResults.SystemAvailability < 99.5 {
			risks = append(risks, "HIGH: Low availability may not meet SLA requirements")
		}

		if electionTime, _ := time.ParseDuration(s.results.FaultToleranceResults.LeaderElectionTime); electionTime > 20*time.Second {
			risks = append(risks, "HIGH: Slow leader election creates extended downtime windows")
		}

		if s.results.FaultToleranceResults.DataConsistency.ConsistencyRate < 99.9 {
			risks = append(risks, "CRITICAL: Data consistency issues detected, risk of data corruption")
		}
	}

	if len(risks) == 0 {
		risks = append(risks, "No significant risks identified")
	}

	return risks
}

// generateComplianceStatus checks system metrics against defined compliance thresholds.
func (s *BenchmarkSuite) generateComplianceStatus() map[string]bool {
	compliance := make(map[string]bool)

	// Performance compliance (SLA-style requirements)
	compliance["p99_latency_under_100ms"] = true
	if s.results.UncontestedResults != nil {
		if p99, _ := time.ParseDuration(s.results.UncontestedResults.WithLeaderOptimization.P99); p99 > 100*time.Millisecond {
			compliance["p99_latency_under_100ms"] = false
		}
	}

	// Availability compliance (99.9% uptime)
	compliance["availability_99_9_percent"] = false
	if s.results.FaultToleranceResults != nil {
		compliance["availability_99_9_percent"] = s.results.FaultToleranceResults.SystemAvailability >= 99.9
	}

	// Consistency compliance (99.99% consistency)
	compliance["consistency_99_99_percent"] = false
	if s.results.FaultToleranceResults != nil {
		compliance["consistency_99_99_percent"] = s.results.FaultToleranceResults.DataConsistency.ConsistencyRate >= 99.99
	}

	// Contention compliance (85% success under high load)
	compliance["high_contention_85_percent_success"] = false
	if s.results.ContentionResults != nil {
		compliance["high_contention_85_percent_success"] = s.results.ContentionResults.HighContention.SuccessRate >= 85.0
	}

	// Recovery compliance (leader election under 15 seconds)
	compliance["leader_election_under_15s"] = false
	if s.results.FaultToleranceResults != nil {
		if electionTime, err := time.ParseDuration(s.results.FaultToleranceResults.LeaderElectionTime); err == nil {
			compliance["leader_election_under_15s"] = electionTime <= 15*time.Second
		}
	}

	return compliance
}
