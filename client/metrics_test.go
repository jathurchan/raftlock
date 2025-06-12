package client

import (
	"sync"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/testutil"
)

func TestMetrics_IncrSuccess(t *testing.T) {
	m := newMetrics()

	m.IncrSuccess("test_op")
	testutil.AssertEqual(t, uint64(1), m.GetRequestCount("test_op"))
	testutil.AssertEqual(t, uint64(1), m.GetSuccessCount("test_op"))
	testutil.AssertEqual(t, uint64(0), m.GetFailureCount("test_op"))

	m.IncrSuccess("test_op")
	testutil.AssertEqual(t, uint64(2), m.GetRequestCount("test_op"))
	testutil.AssertEqual(t, uint64(2), m.GetSuccessCount("test_op"))

	m.IncrSuccess("other_op")
	testutil.AssertEqual(t, uint64(1), m.GetRequestCount("other_op"))
	testutil.AssertEqual(t, uint64(1), m.GetSuccessCount("other_op"))
}

func TestMetrics_IncrFailure(t *testing.T) {
	m := newMetrics()

	m.IncrFailure("test_op")
	testutil.AssertEqual(t, uint64(1), m.GetRequestCount("test_op"))
	testutil.AssertEqual(t, uint64(0), m.GetSuccessCount("test_op"))
	testutil.AssertEqual(t, uint64(1), m.GetFailureCount("test_op"))

	m.IncrFailure("test_op")
	testutil.AssertEqual(t, uint64(2), m.GetRequestCount("test_op"))
	testutil.AssertEqual(t, uint64(2), m.GetFailureCount("test_op"))
}

func TestMetrics_IncrRetry(t *testing.T) {
	m := newMetrics()

	m.IncrRetry("test_op")
	testutil.AssertEqual(t, uint64(1), m.GetRetryCount("test_op"))

	m.IncrRetry("test_op")
	m.IncrRetry("test_op")
	testutil.AssertEqual(t, uint64(3), m.GetRetryCount("test_op"))

	testutil.AssertEqual(t, uint64(0), m.GetRequestCount("test_op"))
}

func TestMetrics_IncrLeaderRedirect(t *testing.T) {
	m := newMetrics()

	m.IncrLeaderRedirect()
	testutil.AssertEqual(t, uint64(1), m.GetLeaderRedirectCount())

	m.IncrLeaderRedirect()
	m.IncrLeaderRedirect()
	testutil.AssertEqual(t, uint64(3), m.GetLeaderRedirectCount())
}

func TestMetrics_ObserveLatency(t *testing.T) {
	m := newMetrics()

	latency1 := 100 * time.Millisecond
	m.ObserveLatency("test_op", latency1)
	testutil.AssertEqual(t, latency1, m.GetAverageLatency("test_op"))
	testutil.AssertEqual(t, latency1, m.GetMaxLatency("test_op"))

	latency2 := 200 * time.Millisecond
	m.ObserveLatency("test_op", latency2)
	expectedAvg := (latency1 + latency2) / 2
	testutil.AssertEqual(t, expectedAvg, m.GetAverageLatency("test_op"))
	testutil.AssertEqual(t, latency2, m.GetMaxLatency("test_op")) // Should be the max

	latency3 := 50 * time.Millisecond
	m.ObserveLatency("test_op", latency3)
	expectedAvg = (latency1 + latency2 + latency3) / 3
	testutil.AssertEqual(t, expectedAvg, m.GetAverageLatency("test_op"))
	testutil.AssertEqual(t, latency2, m.GetMaxLatency("test_op")) // Should still be latency2
}

func TestMetrics_SetConnectionCount(t *testing.T) {
	m := newMetrics()

	m.SetConnectionCount(5)
	testutil.AssertEqual(t, 5, m.GetConnectionCount())

	m.SetConnectionCount(10)
	testutil.AssertEqual(t, 10, m.GetConnectionCount())

	m.SetConnectionCount(0)
	testutil.AssertEqual(t, 0, m.GetConnectionCount())
}

func TestMetrics_GetSuccessRate(t *testing.T) {
	m := newMetrics()

	testutil.AssertEqual(t, 0.0, m.GetSuccessRate("test_op"))

	m.IncrSuccess("test_op")
	m.IncrSuccess("test_op")
	testutil.AssertEqual(t, 1.0, m.GetSuccessRate("test_op"))

	m.IncrFailure("test_op")
	expectedRate := 2.0 / 3.0 // 2 successes out of 3 total requests
	testutil.AssertEqual(t, expectedRate, m.GetSuccessRate("test_op"))

	m.IncrFailure("other_op")
	m.IncrFailure("other_op")
	testutil.AssertEqual(t, 0.0, m.GetSuccessRate("other_op"))
}

func TestMetrics_GetAverageLatency_NoObservations(t *testing.T) {
	m := newMetrics()

	testutil.AssertEqual(t, time.Duration(0), m.GetAverageLatency("test_op"))
}

func TestMetrics_GetMaxLatency_NoObservations(t *testing.T) {
	m := newMetrics()

	testutil.AssertEqual(t, time.Duration(0), m.GetMaxLatency("test_op"))
}

func TestMetrics_Reset(t *testing.T) {
	m := newMetrics()

	m.IncrSuccess("test_op")
	m.IncrFailure("test_op")
	m.IncrRetry("test_op")
	m.IncrLeaderRedirect()
	m.ObserveLatency("test_op", 100*time.Millisecond)
	m.SetConnectionCount(5)

	testutil.AssertEqual(t, uint64(2), m.GetRequestCount("test_op"))
	testutil.AssertEqual(t, uint64(1), m.GetLeaderRedirectCount())
	testutil.AssertEqual(t, 5, m.GetConnectionCount())

	m.Reset()

	testutil.AssertEqual(t, uint64(0), m.GetRequestCount("test_op"))
	testutil.AssertEqual(t, uint64(0), m.GetSuccessCount("test_op"))
	testutil.AssertEqual(t, uint64(0), m.GetFailureCount("test_op"))
	testutil.AssertEqual(t, uint64(0), m.GetRetryCount("test_op"))
	testutil.AssertEqual(t, uint64(0), m.GetLeaderRedirectCount())
	testutil.AssertEqual(t, 0, m.GetConnectionCount())
	testutil.AssertEqual(t, time.Duration(0), m.GetAverageLatency("test_op"))
	testutil.AssertEqual(t, time.Duration(0), m.GetMaxLatency("test_op"))
	testutil.AssertEqual(t, 0.0, m.GetSuccessRate("test_op"))
}

func TestMetrics_Snapshot(t *testing.T) {
	m := newMetrics()

	m.IncrSuccess("op1")
	m.IncrSuccess("op1")
	m.IncrFailure("op1")
	m.IncrRetry("op1")
	m.ObserveLatency("op1", 100*time.Millisecond)
	m.ObserveLatency("op1", 200*time.Millisecond)

	m.IncrSuccess("op2")
	m.ObserveLatency("op2", 50*time.Millisecond)

	m.IncrLeaderRedirect()
	m.IncrLeaderRedirect()
	m.SetConnectionCount(3)

	snapshot := m.Snapshot()

	testutil.AssertEqual(t, uint64(2), snapshot.LeaderRedirects)
	testutil.AssertEqual(t, 3, snapshot.Connections)
	testutil.AssertEqual(t, 2, len(snapshot.Operations))

	op1Metrics, exists := snapshot.Operations["op1"]
	testutil.AssertTrue(t, exists)
	testutil.AssertEqual(t, uint64(3), op1Metrics.Requests)
	testutil.AssertEqual(t, uint64(2), op1Metrics.Successes)
	testutil.AssertEqual(t, uint64(1), op1Metrics.Failures)
	testutil.AssertEqual(t, uint64(1), op1Metrics.Retries)
	testutil.AssertEqual(t, 150*time.Millisecond, op1Metrics.Latency) // Average of 100ms and 200ms
	testutil.AssertEqual(t, 200*time.Millisecond, op1Metrics.MaxLatency)

	op2Metrics, exists := snapshot.Operations["op2"]
	testutil.AssertTrue(t, exists)
	testutil.AssertEqual(t, uint64(1), op2Metrics.Requests)
	testutil.AssertEqual(t, uint64(1), op2Metrics.Successes)
	testutil.AssertEqual(t, uint64(0), op2Metrics.Failures)
	testutil.AssertEqual(t, uint64(0), op2Metrics.Retries)
	testutil.AssertEqual(t, 50*time.Millisecond, op2Metrics.Latency)
	testutil.AssertEqual(t, 50*time.Millisecond, op2Metrics.MaxLatency)
}

func TestMetrics_ConcurrentAccess(t *testing.T) {
	m := newMetrics()
	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			opName := "concurrent_op"

			for j := 0; j < numOperations; j++ {
				if j%3 == 0 {
					m.IncrSuccess(opName)
				} else if j%3 == 1 {
					m.IncrFailure(opName)
				} else {
					m.IncrRetry(opName)
				}

				m.ObserveLatency(opName, time.Duration(j)*time.Millisecond)
				m.IncrLeaderRedirect()
			}
		}(i)
	}

	wg.Wait()

	// For operations 0-99: j%3==0 occurs 34 times, j%3==1 occurs 33 times, j%3==2 occurs 33 times
	expectedSuccesses := uint64(numGoroutines * 34)
	expectedFailures := uint64(numGoroutines * 33)
	expectedRetries := uint64(numGoroutines * 33)
	expectedRequests := expectedSuccesses + expectedFailures
	expectedRedirects := uint64(numGoroutines * numOperations)

	testutil.AssertEqual(t, expectedRequests, m.GetRequestCount("concurrent_op"))
	testutil.AssertEqual(t, expectedSuccesses, m.GetSuccessCount("concurrent_op"))
	testutil.AssertEqual(t, expectedFailures, m.GetFailureCount("concurrent_op"))
	testutil.AssertEqual(t, expectedRetries, m.GetRetryCount("concurrent_op"))
	testutil.AssertEqual(t, expectedRedirects, m.GetLeaderRedirectCount())
}

func TestMetrics_MaxLatencyRaceCondition(t *testing.T) {
	m := newMetrics()
	const numGoroutines = 5
	const numObservations = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range numObservations {
				latency := time.Duration(id*100+j) * time.Millisecond
				m.ObserveLatency("race_test", latency)
			}
		}(i)
	}

	wg.Wait()

	expectedMax := time.Duration(4*100+19) * time.Millisecond
	testutil.AssertEqual(t, expectedMax, m.GetMaxLatency("race_test"))

	expectedObservations := uint64(numGoroutines * numObservations)
	actualObservations := m.(*metrics).getOrCreateCounters("race_test").latencyCount.Load()
	testutil.AssertEqual(t, expectedObservations, actualObservations)
}

func TestNoOpMetrics(t *testing.T) {
	m := &noOpMetrics{}

	m.IncrSuccess("test")
	m.IncrFailure("test")
	m.IncrRetry("test")
	m.IncrLeaderRedirect()
	m.ObserveLatency("test", time.Second)
	m.SetConnectionCount(5)

	testutil.AssertEqual(t, uint64(0), m.GetRequestCount("test"))
	testutil.AssertEqual(t, uint64(0), m.GetSuccessCount("test"))
	testutil.AssertEqual(t, uint64(0), m.GetFailureCount("test"))
	testutil.AssertEqual(t, uint64(0), m.GetRetryCount("test"))
	testutil.AssertEqual(t, 0.0, m.GetSuccessRate("test"))
	testutil.AssertEqual(t, time.Duration(0), m.GetAverageLatency("test"))
	testutil.AssertEqual(t, time.Duration(0), m.GetMaxLatency("test"))
	testutil.AssertEqual(t, uint64(0), m.GetLeaderRedirectCount())
	testutil.AssertEqual(t, 0, m.GetConnectionCount())

	m.Reset() // Should not panic

	snapshot := m.Snapshot()
	testutil.AssertNotNil(t, snapshot.Operations)
	testutil.AssertEqual(t, 0, len(snapshot.Operations))
	testutil.AssertEqual(t, uint64(0), snapshot.LeaderRedirects)
	testutil.AssertEqual(t, 0, snapshot.Connections)
}

func TestMetrics_ZeroDivisionProtection(t *testing.T) {
	m := newMetrics()

	testutil.AssertEqual(t, time.Duration(0), m.GetAverageLatency("nonexistent"))

	testutil.AssertEqual(t, 0.0, m.GetSuccessRate("nonexistent"))
}

func TestMetrics_LargeNumbers(t *testing.T) {
	m := newMetrics()

	const largeNumber = 1000000
	for i := 0; i < largeNumber; i++ {
		if i%2 == 0 {
			m.IncrSuccess("large_test")
		} else {
			m.IncrFailure("large_test")
		}
	}

	testutil.AssertEqual(t, uint64(largeNumber), m.GetRequestCount("large_test"))
	testutil.AssertEqual(t, uint64(largeNumber/2), m.GetSuccessCount("large_test"))
	testutil.AssertEqual(t, uint64(largeNumber/2), m.GetFailureCount("large_test"))
	testutil.AssertEqual(t, 0.5, m.GetSuccessRate("large_test"))
}

func TestMetrics_OperationIsolation(t *testing.T) {
	m := newMetrics()

	m.IncrSuccess("op1")
	m.IncrSuccess("op1")
	m.IncrFailure("op2")
	m.IncrRetry("op3")

	testutil.AssertEqual(t, uint64(2), m.GetRequestCount("op1"))
	testutil.AssertEqual(t, uint64(2), m.GetSuccessCount("op1"))
	testutil.AssertEqual(t, uint64(0), m.GetFailureCount("op1"))
	testutil.AssertEqual(t, uint64(0), m.GetRetryCount("op1"))

	testutil.AssertEqual(t, uint64(1), m.GetRequestCount("op2"))
	testutil.AssertEqual(t, uint64(0), m.GetSuccessCount("op2"))
	testutil.AssertEqual(t, uint64(1), m.GetFailureCount("op2"))
	testutil.AssertEqual(t, uint64(0), m.GetRetryCount("op2"))

	testutil.AssertEqual(t, uint64(0), m.GetRequestCount("op3"))
	testutil.AssertEqual(t, uint64(0), m.GetSuccessCount("op3"))
	testutil.AssertEqual(t, uint64(0), m.GetFailureCount("op3"))
	testutil.AssertEqual(t, uint64(1), m.GetRetryCount("op3"))
}

func TestMetrics_LatencyPrecision(t *testing.T) {
	m := newMetrics()

	smallLatency := time.Nanosecond
	m.ObserveLatency("precision_test", smallLatency)
	testutil.AssertEqual(t, smallLatency, m.GetAverageLatency("precision_test"))
	testutil.AssertEqual(t, smallLatency, m.GetMaxLatency("precision_test"))

	largeLatency := time.Hour
	m.ObserveLatency("precision_test", largeLatency)
	expectedAvg := (smallLatency + largeLatency) / 2
	testutil.AssertEqual(t, expectedAvg, m.GetAverageLatency("precision_test"))
	testutil.AssertEqual(t, largeLatency, m.GetMaxLatency("precision_test"))
}
