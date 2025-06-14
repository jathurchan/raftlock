package storage

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/testutil"
)

func TestMetricsToMap(t *testing.T) {
	m := &metrics{}

	m.appendOps.Store(100)
	m.appendBytes.Store(1024)
	m.readOps.Store(50)
	m.readBytes.Store(512)
	m.snapshotSaveOps.Store(5)
	m.slowOperations.Store(2)

	m.appendLatencyCount.Store(100)
	m.appendLatencySum.Store(100 * 1000000)
	m.appendLatencyMax.Store(5000000)

	metricsMap := m.ToMap()

	testutil.AssertEqual(t, uint64(100), metricsMap["append_ops"])
	testutil.AssertEqual(t, uint64(1024), metricsMap["append_bytes"])
	testutil.AssertEqual(t, uint64(50), metricsMap["read_ops"])
	testutil.AssertEqual(t, uint64(512), metricsMap["read_bytes"])
	testutil.AssertEqual(t, uint64(5), metricsMap["snapshot_save_ops"])
	testutil.AssertEqual(t, uint64(2), metricsMap["slow_operations"])

	testutil.AssertEqual(t, uint64(1000), metricsMap["avg_append_latency_us"])
	testutil.AssertEqual(t, uint64(10), metricsMap["bytes_per_append"])
	testutil.AssertEqual(t, uint64(10), metricsMap["bytes_per_read"])
}

func TestMetricsReset(t *testing.T) {
	m := &metrics{}

	m.appendOps.Store(100)
	m.readOps.Store(50)
	m.appendLatencyCount.Store(100)
	m.appendLatencySum.Store(100 * 1000000)
	m.appendLatencyMax.Store(5000000)

	m.appendLatencySamples = []uint64{1000000, 2000000, 3000000}

	m.Reset()

	testutil.AssertEqual(t, uint64(0), m.appendOps.Load())
	testutil.AssertEqual(t, uint64(0), m.readOps.Load())
	testutil.AssertEqual(t, uint64(0), m.appendLatencyCount.Load())
	testutil.AssertEqual(t, uint64(0), m.appendLatencySum.Load())
	testutil.AssertEqual(t, uint64(0), m.appendLatencyMax.Load())

	m.samplerMapLock.Lock()
	if m.latencySamplers != nil {
		sampler := m.latencySamplers["append"]
		if sampler != nil {
			sampler.mu.Lock()
			testutil.AssertEqual(t, 0, len(*sampler.b))
			sampler.mu.Unlock()
		}
	}
	m.samplerMapLock.Unlock()
}

func TestMetricsRecordLatencySample(t *testing.T) {
	m := &metrics{}

	m.appendLatencySamples = []uint64{}
	m.initSamplers()

	m.recordLatencySample("append", 1000000)
	m.recordLatencySample("append", 2000000)
	m.recordLatencySample("append", 3000000)

	m.samplerMapLock.Lock()
	defer m.samplerMapLock.Unlock()

	sampler := m.latencySamplers["append"]
	sampler.mu.Lock()
	defer sampler.mu.Unlock()

	testutil.AssertEqual(t, 3, len(*sampler.b))
	testutil.AssertEqual(t, uint64(1000000), (*sampler.b)[0])
	testutil.AssertEqual(t, uint64(2000000), (*sampler.b)[1])
	testutil.AssertEqual(t, uint64(3000000), (*sampler.b)[2])
}

func TestMetricsLatencySampleLimit(t *testing.T) {
	m := &metrics{}

	m.appendLatencySamples = []uint64{}
	m.initSamplers()

	for i := 0; i < latencySampleLimit+10; i++ {
		m.recordLatencySample("append", uint64(i*1000000))
	}

	m.samplerMapLock.Lock()
	defer m.samplerMapLock.Unlock()

	sampler := m.latencySamplers["append"]
	sampler.mu.Lock()
	defer sampler.mu.Unlock()

	testutil.AssertEqual(t, latencySampleLimit, len(*sampler.b))

	// Check that the first sample is the 11th one (index 10) after overflow
	testutil.AssertEqual(t, uint64(10*1000000), (*sampler.b)[0])
	// Check that the last sample is the one with the highest value
	testutil.AssertEqual(
		t,
		uint64((latencySampleLimit+9)*1000000),
		(*sampler.b)[latencySampleLimit-1],
	)
}

func TestPercentileCalculation(t *testing.T) {
	tests := []struct {
		name       string
		samples    []uint64
		percentile float64
		expected   uint64
	}{
		{
			name:       "P50 of sorted data",
			samples:    []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
			percentile: 0.5,
			expected:   5,
		},
		{
			name: "P95 of sorted data",
			samples: []uint64{
				1,
				2,
				3,
				4,
				5,
				6,
				7,
				8,
				9,
				10,
				11,
				12,
				13,
				14,
				15,
				16,
				17,
				18,
				19,
				20,
			},
			percentile: 0.95,
			expected:   19,
		},
		{
			name:       "Empty samples",
			samples:    []uint64{},
			percentile: 0.95,
			expected:   0,
		},
		{
			name:       "Single sample",
			samples:    []uint64{42},
			percentile: 0.99,
			expected:   42,
		},
		{
			name:       "Unsorted data",
			samples:    []uint64{5, 1, 3, 4, 2},
			percentile: 0.5,
			expected:   3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := computePercentile(tc.samples, tc.percentile)
			testutil.AssertEqual(t, tc.expected, result)
		})
	}
}

func TestMetricsSummary(t *testing.T) {
	m := &metrics{}

	m.appendOps.Store(100)
	m.readOps.Store(50)
	m.stateOps.Store(25)
	m.snapshotSaveOps.Store(5)
	m.snapshotLoadOps.Store(3)
	m.truncatePrefixOps.Store(2)
	m.truncateSuffixOps.Store(1)
	m.logSize.Store(1024 * 1024)
	m.appendErrors.Store(2)
	m.slowOperations.Store(3)

	m.appendLatencyCount.Store(100)
	m.appendLatencySum.Store(100 * 1000000)
	m.appendLatencyMax.Store(5000000)

	m.appendLatencySamples = []uint64{1000000, 2000000, 3000000, 4000000, 5000000}
	m.readLatencySamples = []uint64{500000, 1500000, 2500000}
	m.initSamplers()

	summary := m.Summary()

	testutil.AssertTrue(t, strings.Contains(summary, "Storage Metrics Summary:"))
	testutil.AssertTrue(t, strings.Contains(summary, "Operations:"))
	testutil.AssertTrue(t, strings.Contains(summary, "Append Ops: 100"))
	testutil.AssertTrue(t, strings.Contains(summary, "Read Ops: 50"))
	testutil.AssertTrue(t, strings.Contains(summary, "Latencies:"))
	testutil.AssertTrue(t, strings.Contains(summary, "Storage:"))

	// Check for log file size in MiB format (1024*1024 bytes) - match the actual implementation
	testutil.AssertTrue(
		t,
		strings.Contains(summary, "1.00 MiB"),
		"Expected summary to contain '1.00 MiB'",
	)

	testutil.AssertTrue(t, strings.Contains(summary, "Errors:"))
	testutil.AssertTrue(t, strings.Contains(summary, "Append Errors: 2"))
	testutil.AssertTrue(t, strings.Contains(summary, "Slow Operations: 3"))
}

func TestConcurrentMetricsAccess(t *testing.T) {
	m := &metrics{}

	m.initSamplers()

	const goroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()

			for j := range iterations {
				m.appendOps.Add(1)
				m.appendBytes.Add(uint64(100 + id))

				m.appendLatencySum.Add(uint64(id * 1000000))
				m.appendLatencyCount.Add(1)

				if j%10 == 0 {
					m.recordLatencySample("append", uint64(id*1000000))
				}

				if j%20 == 0 {
					_ = m.ToMap()
				}

				if j%50 == 0 {
					_ = m.Summary()
				}

				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	metricsMap := m.ToMap()
	testutil.AssertEqual(t, uint64(goroutines*iterations), metricsMap["append_ops"])
}

func TestMetricsSummaryWithNoOperations(t *testing.T) {
	m := &metrics{}

	// All operations are 0, so error rate should be "N/A"
	summary := m.Summary()

	testutil.AssertTrue(t, strings.Contains(summary, "Error Rate: N/A"))
}

func TestMetricsRecordLatencySampleWithLazySamplerInit(t *testing.T) {
	m := &metrics{}

	// Do not initialize samplers, so they should be nil
	testutil.AssertTrue(t, m.latencySamplers == nil, "latencySamplers should be nil initially")

	// recordLatencySample should initialize samplers if needed
	m.recordLatencySample("append", 1000000)

	testutil.AssertTrue(t, m.latencySamplers != nil, "latencySamplers should be initialized")

	m.samplerMapLock.Lock()
	defer m.samplerMapLock.Unlock()

	sampler := m.latencySamplers["append"]
	testutil.AssertTrue(t, sampler != nil, "Sampler for 'append' should exist")

	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	testutil.AssertEqual(t, 1, len(*sampler.b))
	testutil.AssertEqual(t, uint64(1000000), (*sampler.b)[0])
}

func TestMetricsRecordLatencySampleWithUnknownOperationType(t *testing.T) {
	m := &metrics{}

	m.appendLatencySamples = []uint64{}
	m.initSamplers()

	// This should be safely ignored since "unknown_operation" is not a known operation type
	m.recordLatencySample("unknown_operation", 1000000)

	// Make sure none of the known samplers were affected
	m.samplerMapLock.Lock()
	defer m.samplerMapLock.Unlock()

	for opType, sampler := range m.latencySamplers {
		sampler.mu.Lock()
		testutil.AssertEqual(t, 0, len(*sampler.b), "Sampler for %s should be empty", opType)
		sampler.mu.Unlock()
	}
}
