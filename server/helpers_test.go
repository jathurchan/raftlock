package server

import (
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

type testClock struct {
	mu       sync.RWMutex
	current  time.Time
	tickers  []*testTicker
	timers   []*testTimer
	tickerID int64
}

func newTestClock() *testClock {
	return &testClock{
		current: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (tc *testClock) Now() time.Time {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.current
}

func (tc *testClock) Since(t time.Time) time.Duration {
	return tc.Now().Sub(t)
}

func (tc *testClock) Until(t time.Time) time.Duration {
	return t.Sub(tc.Now())
}

func (tc *testClock) After(d time.Duration) <-chan time.Time {
	c := make(chan time.Time, 1)
	go func() {
		time.Sleep(1 * time.Millisecond) // Minimal delay for test scheduling
		c <- tc.Now().Add(d)
	}()
	return c
}

func (tc *testClock) NewTicker(d time.Duration) raft.Ticker {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	id := atomic.AddInt64(&tc.tickerID, 1)
	ticker := &testTicker{
		id:       id,
		clock:    tc,
		interval: d,
		ch:       make(chan time.Time, 10), // Buffered to prevent blocking
		done:     make(chan struct{}),
	}
	tc.tickers = append(tc.tickers, ticker)
	return ticker
}

func (tc *testClock) NewTimer(d time.Duration) raft.Timer {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	timer := &testTimer{
		clock:    tc,
		duration: d,
		ch:       make(chan time.Time, 1),
		done:     make(chan struct{}),
	}
	tc.timers = append(tc.timers, timer)
	return timer
}

func (tc *testClock) Sleep(d time.Duration) {
	time.Sleep(1 * time.Millisecond) // Minimal sleep for tests
}

func (tc *testClock) Advance(d time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.current = tc.current.Add(d)

	// Notify all active tickers
	for _, ticker := range tc.tickers {
		if !ticker.isStopped() {
			ticker.triggerTick(tc.current)
		}
	}

	// Trigger any timers that should fire
	for _, timer := range tc.timers {
		if !timer.isStopped() && timer.shouldTrigger(tc.current) {
			timer.triggerTimer(tc.current)
		}
	}
}

func (tc *testClock) AdvanceAndTrigger(d time.Duration) {
	tc.Advance(d)
	time.Sleep(5 * time.Millisecond) // Allow tick processing
}

type testTicker struct {
	id       int64
	clock    *testClock
	interval time.Duration
	ch       chan time.Time
	done     chan struct{}
	stopped  atomic.Bool
	mu       sync.Mutex
}

func (tt *testTicker) Chan() <-chan time.Time {
	return tt.ch
}

func (tt *testTicker) Stop() {
	if tt.stopped.CompareAndSwap(false, true) {
		tt.mu.Lock()
		defer tt.mu.Unlock()
		close(tt.done)
	}
}

func (tt *testTicker) Reset(d time.Duration) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	if tt.stopped.Load() {
		return
	}

	tt.interval = d
}

func (tt *testTicker) isStopped() bool {
	return tt.stopped.Load()
}

func (tt *testTicker) triggerTick(now time.Time) {
	if tt.isStopped() {
		return
	}

	select {
	case tt.ch <- now:
	case <-tt.done:
	default:
		// Channel full, skip this tick
	}
}

type testTimer struct {
	clock     *testClock
	duration  time.Duration
	ch        chan time.Time
	done      chan struct{}
	stopped   atomic.Bool
	triggered atomic.Bool
	startTime time.Time
	mu        sync.Mutex
}

func (tt *testTimer) Chan() <-chan time.Time {
	return tt.ch
}

func (tt *testTimer) Stop() bool {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	if tt.stopped.Load() || tt.triggered.Load() {
		return false
	}

	if tt.stopped.CompareAndSwap(false, true) {
		close(tt.done)
		return true
	}
	return false
}

func (tt *testTimer) Reset(d time.Duration) bool {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	wasActive := !tt.stopped.Load() && !tt.triggered.Load()

	tt.stopped.Store(false)
	tt.triggered.Store(false)
	tt.duration = d
	tt.startTime = tt.clock.Now()

	return wasActive
}

func (tt *testTimer) isStopped() bool {
	return tt.stopped.Load() || tt.triggered.Load()
}

func (tt *testTimer) shouldTrigger(now time.Time) bool {
	if tt.triggered.Load() || tt.stopped.Load() {
		return false
	}
	return now.Sub(tt.startTime) >= tt.duration
}

func (tt *testTimer) triggerTimer(now time.Time) {
	if tt.triggered.CompareAndSwap(false, true) {
		select {
		case tt.ch <- now:
		case <-tt.done:
		default:
		}
	}
}

type mockServerMetrics struct {
	mu                sync.RWMutex
	activeConnections int
}

func newMockServerMetrics() *mockServerMetrics {
	return &mockServerMetrics{}
}

func (m *mockServerMetrics) SetActiveConnections(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeConnections = count
}

func (m *mockServerMetrics) getActiveConnections() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeConnections
}

// Implement all other ServerMetrics methods as no-ops for testing
func (m *mockServerMetrics) IncrGRPCRequest(method string, success bool)                  {}
func (m *mockServerMetrics) IncrLeaderRedirect(method string)                             {}
func (m *mockServerMetrics) IncrRetry(method string)                                      {}
func (m *mockServerMetrics) IncrRaftProposal(operation types.LockOperation, success bool) {}
func (m *mockServerMetrics) IncrValidationError(method string, errorType string)          {}
func (m *mockServerMetrics) IncrClientError(method string, errorCode pb.ErrorCode)        {}
func (m *mockServerMetrics) IncrServerError(method string, errorType string)              {}
func (m *mockServerMetrics) IncrQueueOverflow(queueType string)                           {}
func (m *mockServerMetrics) IncrLockExpiration()                                          {}
func (m *mockServerMetrics) ObserveRequestLatency(method string, latency time.Duration)   {}
func (m *mockServerMetrics) ObserveRaftProposalLatency(operation types.LockOperation, latency time.Duration) {
}
func (m *mockServerMetrics) ObserveQueueLength(queueType string, length int)  {}
func (m *mockServerMetrics) ObserveRequestSize(method string, sizeBytes int)  {}
func (m *mockServerMetrics) ObserveResponseSize(method string, sizeBytes int) {}
func (m *mockServerMetrics) IncrConcurrentRequests(method string, delta int)  {}
func (m *mockServerMetrics) IncrHealthCheck(healthy bool)                     {}
func (m *mockServerMetrics) SetServerState(isLeader bool, isHealthy bool)     {}
func (m *mockServerMetrics) SetRaftTerm(term types.Term)                      {}
func (m *mockServerMetrics) SetRaftCommitIndex(index types.Index)             {}
func (m *mockServerMetrics) Reset()                                           {}
