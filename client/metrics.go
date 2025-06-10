package client

import (
	"sync"
	"sync/atomic"
	"time"
)

// OperationMetrics holds counters and latency data for a specific client operation.
type OperationMetrics struct {
	// Requests is the total number of times the operation was attempted.
	Requests uint64

	// Successes is the number of successful attempts.
	Successes uint64

	// Failures is the number of failed attempts.
	Failures uint64

	// Retries is the number of retry attempts.
	Retries uint64

	// Latency is the cumulative latency across all attempts.
	Latency time.Duration

	// MaxLatency is the longest single observed latency.
	MaxLatency time.Duration
}

// MetricsSnapshot represents a point-in-time view of client metrics.
type MetricsSnapshot struct {
	// Operations holds metrics for each named operation (e.g., "Acquire", "Release").
	Operations map[string]OperationMetrics

	// LeaderRedirects is the total number of leader redirection events.
	LeaderRedirects uint64

	// Connections is the current number of active client connections.
	Connections int
}

// Metrics defines an interface for collecting client-side operational metrics.
type Metrics interface {
	// IncrSuccess increments the success count for the given operation.
	IncrSuccess(operation string)

	// IncrFailure increments the failure count for the given operation.
	IncrFailure(operation string)

	// IncrRetry increments the retry count for the given operation.
	IncrRetry(operation string)

	// IncrLeaderRedirect increments the count of leader redirection events.
	IncrLeaderRedirect()

	// ObserveLatency records the latency for the given operation.
	ObserveLatency(operation string, d time.Duration)

	// SetConnectionCount sets the current number of active connections.
	SetConnectionCount(count int)

	// GetRequestCount returns the total number of requests for the given operation.
	GetRequestCount(operation string) uint64

	// GetSuccessCount returns the total number of successful requests for the given operation.
	GetSuccessCount(operation string) uint64

	// GetFailureCount returns the total number of failed requests for the given operation.
	GetFailureCount(operation string) uint64

	// GetRetryCount returns the total number of retries for the given operation.
	GetRetryCount(operation string) uint64

	// GetSuccessRate returns the success rate for the given operation as a fraction.
	GetSuccessRate(operation string) float64

	// GetAverageLatency returns the average latency for the given operation.
	GetAverageLatency(operation string) time.Duration

	// GetMaxLatency returns the maximum observed latency for the given operation.
	GetMaxLatency(operation string) time.Duration

	// GetLeaderRedirectCount returns the total number of leader redirections.
	GetLeaderRedirectCount() uint64

	// GetConnectionCount returns the current number of active connections.
	GetConnectionCount() int

	// Reset clears all collected metrics.
	Reset()

	// Snapshot returns a snapshot of the current metric values.
	Snapshot() MetricsSnapshot
}

// opCounters stores atomic counters for a single operation.
type opCounters struct {
	requests     atomic.Uint64 // Total request count.
	successes    atomic.Uint64 // Successful request count.
	failures     atomic.Uint64 // Failed request count.
	retries      atomic.Uint64 // Retry count.
	latencySum   atomic.Uint64 // Total latency in nanoseconds.
	latencyCount atomic.Uint64 // Number of latency samples.
	maxLatency   atomic.Uint64 // Maximum observed latency in nanoseconds.
}

// metrics is a thread-safe implementation of the Metrics interface
// using atomic operations and minimal locking.
type metrics struct {
	mu              sync.RWMutex           // Guards access to the counters map.
	counters        map[string]*opCounters // Per-operation counters.
	leaderRedirects atomic.Uint64          // Total leader redirect events.
	connections     atomic.Int64           // Current connection count.
}

// newMetrics returns a new instance of atomicMetrics.
func newMetrics() Metrics {
	return &metrics{
		counters: make(map[string]*opCounters),
	}
}

// getOrCreateCounters returns the opCounters for the given operation,
// creating it if it doesn't already exist.
func (m *metrics) getOrCreateCounters(operation string) *opCounters {
	m.mu.RLock()
	c, ok := m.counters[operation]
	m.mu.RUnlock()
	if ok {
		return c
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring the write lock.
	if c, ok = m.counters[operation]; ok {
		return c
	}

	c = &opCounters{}
	m.counters[operation] = c
	return c
}

// IncrSuccess increments the request and success counters.
func (m *metrics) IncrSuccess(operation string) {
	c := m.getOrCreateCounters(operation)
	c.requests.Add(1)
	c.successes.Add(1)
}

// IncrFailure increments the request and failure counters.
func (m *metrics) IncrFailure(operation string) {
	c := m.getOrCreateCounters(operation)
	c.requests.Add(1)
	c.failures.Add(1)
}

// IncrRetry increments the retry counter.
func (m *metrics) IncrRetry(operation string) {
	m.getOrCreateCounters(operation).retries.Add(1)
}

// IncrLeaderRedirect increments the leader redirect counter.
func (m *metrics) IncrLeaderRedirect() {
	m.leaderRedirects.Add(1)
}

// ObserveLatency records a latency measurement for the given operation.
func (m *metrics) ObserveLatency(operation string, d time.Duration) {
	c := m.getOrCreateCounters(operation)
	nanos := uint64(d.Nanoseconds())

	c.latencySum.Add(nanos)
	c.latencyCount.Add(1)

	for {
		currentMax := c.maxLatency.Load()
		if nanos <= currentMax {
			break
		}
		if c.maxLatency.CompareAndSwap(currentMax, nanos) {
			break
		}
	}
}

// SetConnectionCount sets the current number of active connections.
func (m *metrics) SetConnectionCount(count int) {
	m.connections.Store(int64(count))
}

// GetRequestCount returns the total request count.
func (m *metrics) GetRequestCount(operation string) uint64 {
	return m.getOrCreateCounters(operation).requests.Load()
}

// GetSuccessCount returns the total success count.
func (m *metrics) GetSuccessCount(operation string) uint64 {
	return m.getOrCreateCounters(operation).successes.Load()
}

// GetFailureCount returns the total failure count.
func (m *metrics) GetFailureCount(operation string) uint64 {
	return m.getOrCreateCounters(operation).failures.Load()
}

// GetRetryCount returns the total retry count.
func (m *metrics) GetRetryCount(operation string) uint64 {
	return m.getOrCreateCounters(operation).retries.Load()
}

// GetSuccessRate returns the ratio of successes to total requests.
func (m *metrics) GetSuccessRate(operation string) float64 {
	c := m.getOrCreateCounters(operation)
	req := c.requests.Load()
	if req == 0 {
		return 0
	}
	return float64(c.successes.Load()) / float64(req)
}

// GetAverageLatency returns the average latency for the operation.
func (m *metrics) GetAverageLatency(operation string) time.Duration {
	c := m.getOrCreateCounters(operation)
	sum := c.latencySum.Load()
	count := c.latencyCount.Load()
	if count == 0 {
		return 0
	}
	return time.Duration(sum / count)
}

// GetMaxLatency returns the highest recorded latency for the operation.
func (m *metrics) GetMaxLatency(operation string) time.Duration {
	return time.Duration(m.getOrCreateCounters(operation).maxLatency.Load())
}

// GetLeaderRedirectCount returns the total number of leader redirections.
func (m *metrics) GetLeaderRedirectCount() uint64 {
	return m.leaderRedirects.Load()
}

// GetConnectionCount returns the current connection count.
func (m *metrics) GetConnectionCount() int {
	return int(m.connections.Load())
}

// Reset clears all metrics and resets counters to zero.
func (m *metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters = make(map[string]*opCounters)
	m.leaderRedirects.Store(0)
	m.connections.Store(0)
}

// Snapshot returns a point-in-time snapshot of all collected metrics.
func (m *metrics) Snapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snap := MetricsSnapshot{
		Operations:      make(map[string]OperationMetrics, len(m.counters)),
		LeaderRedirects: m.leaderRedirects.Load(),
		Connections:     int(m.connections.Load()),
	}

	for op, c := range m.counters {
		snap.Operations[op] = OperationMetrics{
			Requests:   c.requests.Load(),
			Successes:  c.successes.Load(),
			Failures:   c.failures.Load(),
			Retries:    c.retries.Load(),
			Latency:    m.GetAverageLatency(op),
			MaxLatency: m.GetMaxLatency(op),
		}
	}

	return snap
}

// noOpMetrics is a black-hole implementation of the Metrics interface
// that performs no operations. It is used when metrics are disabled.
type noOpMetrics struct{}

func (n *noOpMetrics) IncrSuccess(string)                     {}
func (n *noOpMetrics) IncrFailure(string)                     {}
func (n *noOpMetrics) IncrRetry(string)                       {}
func (n *noOpMetrics) IncrLeaderRedirect()                    {}
func (n *noOpMetrics) ObserveLatency(string, time.Duration)   {}
func (n *noOpMetrics) SetConnectionCount(int)                 {}
func (n *noOpMetrics) GetRequestCount(string) uint64          { return 0 }
func (n *noOpMetrics) GetSuccessCount(string) uint64          { return 0 }
func (n *noOpMetrics) GetFailureCount(string) uint64          { return 0 }
func (n *noOpMetrics) GetRetryCount(string) uint64            { return 0 }
func (n *noOpMetrics) GetSuccessRate(string) float64          { return 0.0 }
func (n *noOpMetrics) GetAverageLatency(string) time.Duration { return 0 }
func (n *noOpMetrics) GetMaxLatency(string) time.Duration     { return 0 }
func (n *noOpMetrics) GetLeaderRedirectCount() uint64         { return 0 }
func (n *noOpMetrics) GetConnectionCount() int                { return 0 }
func (n *noOpMetrics) Reset()                                 {}
func (n *noOpMetrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{Operations: make(map[string]OperationMetrics)}
}
