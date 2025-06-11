package storage

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
)

// latencySampler holds the buffer and lock for a specific latency metric type.
type latencySampler struct {
	mu *sync.Mutex
	b  *[]uint64 // Pointer to the slice in the metrics struct
}

// metrics holds atomic counters for tracking storage performance statistics.
// Used only if Features.EnableMetrics is true.
type metrics struct {
	// --- Core Operations ---
	appendOps   atomic.Uint64 // Count of log append operations.
	appendBytes atomic.Uint64 // Total bytes appended to the log.
	readOps     atomic.Uint64 // Count of log read requests.
	readEntries atomic.Uint64 // Total number of log entries read.
	readBytes   atomic.Uint64 // Total bytes read from the log.
	stateOps    atomic.Uint64 // Count of persistent state save/load operations.

	// --- Snapshot Lifecycle ---
	snapshotSaveOps atomic.Uint64 // Count of snapshots saved.
	snapshotLoadOps atomic.Uint64 // Count of snapshots loaded.

	// --- Log Maintenance ---
	truncatePrefixOps atomic.Uint64 // Count of log prefix truncations (compaction).
	truncateSuffixOps atomic.Uint64 // Count of log suffix truncations (conflict resolution).
	indexRebuildOps   atomic.Uint64 // Count of index map rebuilds.
	compactionOps     atomic.Uint64 // Count of log compactions (rewrites).

	// --- Latency Tracking (Totals & Counts in Nanoseconds) ---
	appendLatencySum       atomic.Uint64 // Sum for average calculation.
	appendLatencyCount     atomic.Uint64 // Count for average calculation.
	readLatencySum         atomic.Uint64
	readLatencyCount       atomic.Uint64
	stateLatencySum        atomic.Uint64
	stateLatencyCount      atomic.Uint64
	snapshotLatencySum     atomic.Uint64
	snapshotLatencyCount   atomic.Uint64
	serializationTimeSum   atomic.Uint64
	serializationCount     atomic.Uint64
	deserializationTimeSum atomic.Uint64
	deserializationCount   atomic.Uint64

	// --- Latency Tracking (Maximums in Nanoseconds) ---
	appendLatencyMax   atomic.Uint64 // Max observed latency.
	readLatencyMax     atomic.Uint64
	stateLatencyMax    atomic.Uint64
	snapshotLatencyMax atomic.Uint64
	serializationMax   atomic.Uint64
	deserializationMax atomic.Uint64

	// --- Latency Tracking (Samples for Percentiles) ---
	// Simple fixed-size sliding window buffers.
	appendLatencySamples   []uint64
	readLatencySamples     []uint64
	stateLatencySamples    []uint64
	snapshotLatencySamples []uint64

	// Map operation names to their respective samplers. Lazily initialized.
	latencySamplers map[string]*latencySampler
	samplerInitOnce sync.Once  // Ensures latencySamplers is initialized safely.
	samplerMapLock  sync.Mutex // Protects latencySamplers map initialization/access.

	// --- Storage Sizes (Bytes) ---
	logSize      atomic.Uint64 // Current size of the log file.
	stateSize    atomic.Uint64 // Current size of the state file.
	snapshotSize atomic.Uint64 // Size of the last saved/loaded snapshot.
	metadataSize atomic.Uint64 // Current size of the metadata file.
	indexMapSize atomic.Uint64 // Number of entries in the index map.

	// --- Error Tracking ---
	appendErrors   atomic.Uint64 // Count of failed append operations.
	readErrors     atomic.Uint64 // Count of failed read operations.
	stateErrors    atomic.Uint64 // Count of failed state operations.
	snapshotErrors atomic.Uint64 // Count of failed snapshot operations.
	slowOperations atomic.Uint64 // Count of operations exceeding the slow threshold.
}

// initSamplers initializes the map used by recordLatencySample.
func (m *metrics) initSamplers() {
	m.samplerInitOnce.Do(func() {
		m.latencySamplers = map[string]*latencySampler{
			"append":   {mu: &sync.Mutex{}, b: &m.appendLatencySamples},
			"read":     {mu: &sync.Mutex{}, b: &m.readLatencySamples},
			"state":    {mu: &sync.Mutex{}, b: &m.stateLatencySamples},
			"snapshot": {mu: &sync.Mutex{}, b: &m.snapshotLatencySamples},
		}
	})
}

// recordLatencySample adds a latency value to the sliding window buffer for the given operation type.
// This is used for calculating percentiles later.
func (m *metrics) recordLatencySample(op string, val uint64) {
	m.samplerMapLock.Lock()
	if m.latencySamplers == nil {
		m.initSamplers()
	}
	sampler, ok := m.latencySamplers[op]
	m.samplerMapLock.Unlock()

	if !ok {
		return // Unknown operation type
	}

	sampler.mu.Lock()
	defer sampler.mu.Unlock()

	if len(*sampler.b) >= latencySampleLimit {
		// Remove the oldest sample
		*sampler.b = (*sampler.b)[1:]
	}
	*sampler.b = append(*sampler.b, val)
}

// addAverageLatency calculates and adds the average latency for an operation to the metrics map.
func addAverageLatency(metricsMap map[string]uint64, name string, sum, count *atomic.Uint64) {
	c := count.Load()
	if c > 0 {
		metricsMap["avg_"+name+"_latency_us"] = sum.Load() / c / 1000
	} else {
		metricsMap["avg_"+name+"_latency_us"] = 0
	}
}

func (m *metrics) addPercentileMetrics(metricsMap map[string]uint64) {
	m.samplerMapLock.Lock()
	if m.latencySamplers == nil {
		m.initSamplers()
	}
	m.samplerMapLock.Unlock()

	for op, sampler := range m.latencySamplers {
		sampler.mu.Lock()
		samplesCopy := make([]uint64, len(*sampler.b))
		copy(samplesCopy, *sampler.b)
		sampler.mu.Unlock()

		if len(samplesCopy) > 0 {
			metricsMap["p95_"+op+"_latency_us"] = computePercentile(samplesCopy, 0.95) / 1000
			metricsMap["p99_"+op+"_latency_us"] = computePercentile(samplesCopy, 0.99) / 1000
		} else {
			metricsMap["p95_"+op+"_latency_us"] = 0
			metricsMap["p99_"+op+"_latency_us"] = 0
		}
	}
}

// ToMap returns the current metrics as a map[string]uint64.
// Derived metrics like averages and percentiles are calculated on demand.
func (m *metrics) ToMap() map[string]uint64 {
	metricsMap := m.loadRawMetrics()
	m.addDerivedAverages(metricsMap)
	m.addPercentileMetrics(metricsMap)
	m.addDerivedStats(metricsMap)

	return metricsMap
}

func (m *metrics) loadRawMetrics() map[string]uint64 {
	return map[string]uint64{
		"append_ops":                  m.appendOps.Load(),
		"append_bytes":                m.appendBytes.Load(),
		"read_ops":                    m.readOps.Load(),
		"read_bytes":                  m.readBytes.Load(),
		"read_entries":                m.readEntries.Load(),
		"state_ops":                   m.stateOps.Load(),
		"snapshot_save_ops":           m.snapshotSaveOps.Load(),
		"snapshot_load_ops":           m.snapshotLoadOps.Load(),
		"truncate_prefix_ops":         m.truncatePrefixOps.Load(),
		"truncate_suffix_ops":         m.truncateSuffixOps.Load(),
		"index_rebuild_ops":           m.indexRebuildOps.Load(),
		"compaction_ops":              m.compactionOps.Load(),
		"log_size":                    m.logSize.Load(),
		"state_size":                  m.stateSize.Load(),
		"snapshot_size":               m.snapshotSize.Load(),
		"metadata_size":               m.metadataSize.Load(),
		"index_map_size":              m.indexMapSize.Load(),
		"append_errors":               m.appendErrors.Load(),
		"read_errors":                 m.readErrors.Load(),
		"state_errors":                m.stateErrors.Load(),
		"snapshot_errors":             m.snapshotErrors.Load(),
		"slow_operations":             m.slowOperations.Load(),
		"max_append_latency_ns":       m.appendLatencyMax.Load(),
		"max_read_latency_ns":         m.readLatencyMax.Load(),
		"max_state_latency_ns":        m.stateLatencyMax.Load(),
		"max_snapshot_latency_ns":     m.snapshotLatencyMax.Load(),
		"max_serialization_time_ns":   m.serializationMax.Load(),
		"max_deserialization_time_ns": m.deserializationMax.Load(),
	}
}

func (m *metrics) addDerivedAverages(metricsMap map[string]uint64) {
	addAverageLatency(metricsMap, "append", &m.appendLatencySum, &m.appendLatencyCount)
	addAverageLatency(metricsMap, "read", &m.readLatencySum, &m.readLatencyCount)
	addAverageLatency(metricsMap, "state", &m.stateLatencySum, &m.stateLatencyCount)
	addAverageLatency(metricsMap, "snapshot", &m.snapshotLatencySum, &m.snapshotLatencyCount)
	addAverageLatency(
		metricsMap,
		"serialization_time",
		&m.serializationTimeSum,
		&m.serializationCount,
	)
	addAverageLatency(
		metricsMap,
		"deserialization_time",
		&m.deserializationTimeSum,
		&m.deserializationCount,
	)
}

func (m *metrics) addDerivedStats(metricsMap map[string]uint64) {
	appendOps := m.appendOps.Load()
	readOps := m.readOps.Load()

	if appendOps > 0 {
		metricsMap["bytes_per_append"] = m.appendBytes.Load() / appendOps
	}
	if readOps > 0 {
		metricsMap["bytes_per_read"] = m.readBytes.Load() / readOps
	}

	totalOps := appendOps +
		readOps +
		m.stateOps.Load() +
		m.snapshotSaveOps.Load() +
		m.snapshotLoadOps.Load()

	totalErr := m.appendErrors.Load() +
		m.readErrors.Load() +
		m.stateErrors.Load() +
		m.snapshotErrors.Load()

	if totalOps > 0 {
		metricsMap["error_rate_ppt"] = (totalErr * 1000) / totalOps
	}
}

// appendLatencySummary formats and writes latency details (Avg, Max, P95, P99) to the buffer.
func appendLatencySummary(
	buf *bytes.Buffer,
	name string,
	sum, count, max *atomic.Uint64,
	sampler *latencySampler,
) {
	c := count.Load()
	avg := uint64(0)
	if c > 0 {
		avg = sum.Load() / c
	}

	sampler.mu.Lock()
	p95 := computePercentile(*sampler.b, 0.95)
	p99 := computePercentile(*sampler.b, 0.99)
	sampler.mu.Unlock()

	buf.WriteString(fmt.Sprintf("  Avg %s: %s\n", name, formatDurationNs(avg)))
	buf.WriteString(fmt.Sprintf("  Max %s: %s\n", name, formatDurationNs(max.Load())))
	buf.WriteString(fmt.Sprintf("  P95 %s: %s\n", name, formatDurationNs(p95)))
	buf.WriteString(fmt.Sprintf("  P99 %s: %s\n", name, formatDurationNs(p99)))
}

// Summary returns a human-readable summary string of the current metrics.
func (m *metrics) Summary() string {
	var buf bytes.Buffer

	m.writeHeader(&buf)
	m.writeOperations(&buf)
	m.ensureSamplers()
	m.writeLatencies(&buf)
	m.writeStorage(&buf)
	m.writeErrors(&buf)
	m.writeErrorRate(&buf)

	return buf.String()
}

func (m *metrics) writeHeader(buf *bytes.Buffer) {
	buf.WriteString("Storage Metrics Summary:\n")
	buf.WriteString("========================\n\n")
}

func (m *metrics) writeOperations(buf *bytes.Buffer) {
	buf.WriteString("Operations:\n")
	buf.WriteString(fmt.Sprintf("  Append Ops: %d\n", m.appendOps.Load()))
	buf.WriteString(fmt.Sprintf("  Read Ops: %d\n", m.readOps.Load()))
	buf.WriteString(fmt.Sprintf("  State Ops: %d\n", m.stateOps.Load()))
	buf.WriteString(
		fmt.Sprintf(
			"  Snapshots: %d saved / %d loaded\n",
			m.snapshotSaveOps.Load(),
			m.snapshotLoadOps.Load(),
		),
	)
	buf.WriteString(
		fmt.Sprintf(
			"  Truncations: %d prefix / %d suffix\n",
			m.truncatePrefixOps.Load(),
			m.truncateSuffixOps.Load(),
		),
	)
}

func (m *metrics) ensureSamplers() {
	m.samplerMapLock.Lock()
	defer m.samplerMapLock.Unlock()
	if m.latencySamplers == nil {
		m.initSamplers()
	}
}

func (m *metrics) writeLatencies(buf *bytes.Buffer) {
	buf.WriteString("\nLatencies:\n")
	appendLatencySummary(
		buf,
		"Append",
		&m.appendLatencySum,
		&m.appendLatencyCount,
		&m.appendLatencyMax,
		m.latencySamplers["append"],
	)
	appendLatencySummary(
		buf,
		"Read",
		&m.readLatencySum,
		&m.readLatencyCount,
		&m.readLatencyMax,
		m.latencySamplers["read"],
	)
}

func (m *metrics) writeStorage(buf *bytes.Buffer) {
	buf.WriteString("\nStorage:\n")
	buf.WriteString(fmt.Sprintf("  Log Size: %s\n", formatByteSize(m.logSize.Load())))
	buf.WriteString(fmt.Sprintf("  State Size: %s\n", formatByteSize(m.stateSize.Load())))
	buf.WriteString(fmt.Sprintf("  Snapshot Size: %s\n", formatByteSize(m.snapshotSize.Load())))
	buf.WriteString(fmt.Sprintf("  Index Entries: %d\n", m.indexMapSize.Load()))
}

func (m *metrics) writeErrors(buf *bytes.Buffer) {
	buf.WriteString("\nErrors:\n")
	buf.WriteString(fmt.Sprintf("  Append Errors: %d\n", m.appendErrors.Load()))
	buf.WriteString(fmt.Sprintf("  Read Errors: %d\n", m.readErrors.Load()))
	buf.WriteString(fmt.Sprintf("  State Errors: %d\n", m.stateErrors.Load()))
	buf.WriteString(fmt.Sprintf("  Snapshot Errors: %d\n", m.snapshotErrors.Load()))
}

func (m *metrics) writeErrorRate(buf *bytes.Buffer) {
	totalOps := m.appendOps.Load() + m.readOps.Load() + m.stateOps.Load() +
		m.snapshotSaveOps.Load() + m.snapshotLoadOps.Load()
	totalErr := m.appendErrors.Load() + m.readErrors.Load() +
		m.stateErrors.Load() + m.snapshotErrors.Load()

	buf.WriteString(fmt.Sprintf("\nSlow Operations: %d\n", m.slowOperations.Load()))

	if totalOps > 0 {
		errorRate := float64(totalErr) / float64(totalOps) * 100.0
		buf.WriteString(fmt.Sprintf("Error Rate: %.2f%% (%d/%d)\n", errorRate, totalErr, totalOps))
	} else {
		buf.WriteString("Error Rate: N/A\n")
	}
}

// Reset sets all metric counters back to zero and clears sample buffers.
func (m *metrics) Reset() {
	m.appendOps.Store(0)
	m.appendBytes.Store(0)
	m.readOps.Store(0)
	m.readBytes.Store(0)
	m.stateOps.Store(0)
	m.slowOperations.Store(0)
	m.snapshotSaveOps.Store(0)
	m.snapshotLoadOps.Store(0)
	m.truncatePrefixOps.Store(0)
	m.truncateSuffixOps.Store(0)
	m.appendLatencySum.Store(0)
	m.appendLatencyCount.Store(0)
	m.readLatencySum.Store(0)
	m.readLatencyCount.Store(0)
	m.stateLatencySum.Store(0)
	m.stateLatencyCount.Store(0)
	m.snapshotLatencySum.Store(0)
	m.snapshotLatencyCount.Store(0)
	m.serializationTimeSum.Store(0)
	m.serializationCount.Store(0)
	m.deserializationTimeSum.Store(0)
	m.deserializationCount.Store(0)
	m.logSize.Store(0)
	m.stateSize.Store(0)
	m.snapshotSize.Store(0)
	m.metadataSize.Store(0)
	m.indexMapSize.Store(0)
	m.appendErrors.Store(0)
	m.readErrors.Store(0)
	m.stateErrors.Store(0)
	m.snapshotErrors.Store(0)
	m.indexRebuildOps.Store(0)
	m.compactionOps.Store(0)
	m.appendLatencyMax.Store(0)
	m.readLatencyMax.Store(0)
	m.stateLatencyMax.Store(0)
	m.snapshotLatencyMax.Store(0)
	m.serializationMax.Store(0)
	m.deserializationMax.Store(0)

	// Reset sample buffers and ensure map is initialized for next use
	m.samplerMapLock.Lock()
	if m.latencySamplers == nil {
		m.initSamplers()
	}
	for _, sampler := range m.latencySamplers {
		sampler.mu.Lock()
		*sampler.b = (*sampler.b)[:0]
		sampler.mu.Unlock()
	}
	m.samplerMapLock.Unlock()
}
