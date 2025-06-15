package internal

import (
	"maps"
	"slices"
	"sync"
	"time"
)

// MetricsCollector captures performance and error telemetry from operations.
type MetricsCollector struct {
	mu sync.RWMutex

	requestCounts    map[string]int64            // total requests by operation
	requestLatencies map[string][]time.Duration  // latencies by operation
	errorCounts      map[string]map[string]int64 // errors by operation and message

	startTime        time.Time // time when collection started
	operationCount   int64     // total number of operations
	bytesTransferred int64     // total bytes transferred

	throughputHistory []ThroughputSample // recorded throughput snapshots
	latencyHistory    []LatencySample    // recorded latency observations
}

// ThroughputSample represents a single throughput measurement.
type ThroughputSample struct {
	Timestamp  time.Time
	Operations int64
	Duration   time.Duration
}

// LatencySample records latency data for a single operation.
type LatencySample struct {
	Timestamp time.Time
	Operation string
	Latency   time.Duration
	Success   bool
}

// NewMetricsCollector initializes and returns a new MetricsCollector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		requestCounts:     make(map[string]int64),
		requestLatencies:  make(map[string][]time.Duration),
		errorCounts:       make(map[string]map[string]int64),
		throughputHistory: make([]ThroughputSample, 0),
		latencyHistory:    make([]LatencySample, 0),
		startTime:         time.Now(),
	}
}

// RecordRequest logs a completed request, its latency, and any associated error.
func (mc *MetricsCollector) RecordRequest(operation string, latency time.Duration, err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.requestCounts[operation]++
	mc.requestLatencies[operation] = append(mc.requestLatencies[operation], latency)
	mc.operationCount++

	mc.latencyHistory = append(mc.latencyHistory, LatencySample{
		Timestamp: time.Now(),
		Operation: operation,
		Latency:   latency,
		Success:   err == nil,
	})

	if err != nil {
		if mc.errorCounts[operation] == nil {
			mc.errorCounts[operation] = make(map[string]int64)
		}
		mc.errorCounts[operation][err.Error()]++
	}
}

// RecordThroughput logs a throughput data point.
func (mc *MetricsCollector) RecordThroughput(operations int64, duration time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.throughputHistory = append(mc.throughputHistory, ThroughputSample{
		Timestamp:  time.Now(),
		Operations: operations,
		Duration:   duration,
	})
}

// MetricsSummary contains a high-level overview of collected metrics.
type MetricsSummary struct {
	TotalOperations int64
	TotalDuration   time.Duration
	OperationCounts map[string]int64
	ErrorRates      map[string]float64
	AvgLatencies    map[string]time.Duration
}

// GetMetricsSummary returns a snapshot summary of aggregated metrics.
func (mc *MetricsCollector) GetMetricsSummary() MetricsSummary {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	summary := MetricsSummary{
		TotalOperations: mc.operationCount,
		TotalDuration:   time.Since(mc.startTime),
		OperationCounts: make(map[string]int64),
		ErrorRates:      make(map[string]float64),
		AvgLatencies:    make(map[string]time.Duration),
	}

	for op, count := range mc.requestCounts {
		summary.OperationCounts[op] = count

		var errorTotal int64
		for _, c := range mc.errorCounts[op] {
			errorTotal += c
		}
		if count > 0 {
			summary.ErrorRates[op] = float64(errorTotal) / float64(count)
		}

		if latencies := mc.requestLatencies[op]; len(latencies) > 0 {
			var total time.Duration
			for _, l := range latencies {
				total += l
			}
			summary.AvgLatencies[op] = total / time.Duration(len(latencies))
		}
	}

	return summary
}

// GetLatencyPercentiles returns selected latency percentiles for the given operation.
func (mc *MetricsCollector) GetLatencyPercentiles(operation string, percentiles []float64) map[float64]time.Duration {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	latencies := mc.requestLatencies[operation]
	if len(latencies) == 0 {
		return map[float64]time.Duration{}
	}

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	slices.Sort(sorted)

	results := make(map[float64]time.Duration)
	n := len(sorted)
	for _, p := range percentiles {
		if p < 0 || p > 100 {
			continue
		}
		rank := int((p / 100.0) * float64(n-1))
		results[p] = sorted[rank]
	}
	return results
}

// GetThroughputHistory returns a copy of recorded throughput samples.
func (mc *MetricsCollector) GetThroughputHistory() []ThroughputSample {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	out := make([]ThroughputSample, len(mc.throughputHistory))
	copy(out, mc.throughputHistory)
	return out
}

// GetLatencyHistory returns a copy of recorded latency samples.
func (mc *MetricsCollector) GetLatencyHistory() []LatencySample {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	out := make([]LatencySample, len(mc.latencyHistory))
	copy(out, mc.latencyHistory)
	return out
}

// OperationStats contains detailed metrics for a specific operation.
type OperationStats struct {
	Operation      string
	TotalRequests  int64
	AverageLatency time.Duration
	LatencyData    []time.Duration
	ErrorBreakdown map[string]int64
}

// GetOperationStats returns detailed metrics for a single operation.
func (mc *MetricsCollector) GetOperationStats(op string) OperationStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	stats := OperationStats{
		Operation:      op,
		ErrorBreakdown: make(map[string]int64),
	}

	if count, ok := mc.requestCounts[op]; ok {
		stats.TotalRequests = count
	}
	if latencies, ok := mc.requestLatencies[op]; ok {
		stats.LatencyData = append([]time.Duration(nil), latencies...)
		if len(latencies) > 0 {
			var total time.Duration
			for _, l := range latencies {
				total += l
			}
			stats.AverageLatency = total / time.Duration(len(latencies))
		}
	}
	if errs, ok := mc.errorCounts[op]; ok {
		maps.Copy(stats.ErrorBreakdown, errs)
	}

	return stats
}

// Reset clears all collected metrics and restarts the collector.
func (mc *MetricsCollector) Reset() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.requestCounts = make(map[string]int64)
	mc.requestLatencies = make(map[string][]time.Duration)
	mc.errorCounts = make(map[string]map[string]int64)
	mc.throughputHistory = nil
	mc.latencyHistory = nil
	mc.operationCount = 0
	mc.bytesTransferred = 0
	mc.startTime = time.Now()
}

// AddBytes adds to the total number of bytes transferred.
func (mc *MetricsCollector) AddBytes(n int64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.bytesTransferred += n
}

// GetBytesTransferred returns the total number of bytes recorded.
func (mc *MetricsCollector) GetBytesTransferred() int64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.bytesTransferred
}

// GetUptime returns the duration since the collector was started or reset.
func (mc *MetricsCollector) GetUptime() time.Duration {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return time.Since(mc.startTime)
}
