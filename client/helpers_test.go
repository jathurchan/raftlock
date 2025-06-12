package client

import (
	"sync"
	"time"

	"github.com/jathurchan/raftlock/raft"
	"google.golang.org/grpc"
)

type mockConnector struct {
	conn      *grpc.ClientConn
	err       error
	returnNil bool // Add flag to return nil instead of empty connection

	// GetConnectionFunc is a function that will be called by the mock's GetConnection method.
	GetConnectionFunc func(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error)
}

func (m *mockConnector) GetConnection(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if m.GetConnectionFunc != nil {
		return m.GetConnectionFunc(endpoint, opts...)
	}

	if m.err != nil {
		return nil, m.err
	}
	if m.returnNil {
		return nil, nil // Return nil connection safely
	}
	return m.conn, nil
}

// Update setupTestClient to use safe nil connections:
func setupTestClient(config Config) (*baseClientImpl, *mockConnector, *mockClock, *mockRand) {
	if len(config.Endpoints) == 0 {
		config.Endpoints = []string{"endpoint1", "endpoint2"}
	}
	base, err := newBaseClient(config)
	if err != nil {
		panic(err)
	}

	client := base.(*baseClientImpl)
	connector := &mockConnector{
		conn:      nil,  // Don't create a real grpc.ClientConn
		returnNil: true, // Return nil connections
	}
	clock := newMockClock()
	rand := newMockRand()
	rand.setFloat64s(0.5)

	client.connector = connector
	client.clock = clock
	client.rand = rand

	return client, connector, clock, rand
}

// mockClock provides a controllable, deterministic time source for testing.
// It fully implements the raft.Clock interface and includes a mechanism
// to synchronize with goroutines waiting on its timers.
type mockClock struct {
	mu          sync.Mutex
	currentTime time.Time
	timers      []*mockTimer
	tickers     []*mockTicker
	// waiter is used to signal when a goroutine starts waiting on a timer.
	// This helps synchronize tests and avoid racy time.Sleep calls.
	waiter chan struct{}
}

// newMockClock creates a new mock clock initialized to the current real time.
func newMockClock() *mockClock {
	return &mockClock{
		currentTime: time.Now(),
		// Using buffered channel to avoid deadlocking the code under test if the
		// test goroutine isn't immediately ready to receive the signal.
		waiter: make(chan struct{}, 100),
	}
}

func (m *mockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentTime
}

func (m *mockClock) Since(t time.Time) time.Duration {
	return m.Now().Sub(t)
}

func (m *mockClock) After(d time.Duration) <-chan time.Time {
	timer := m.NewTimer(d)
	// Only signal if someone is actually waiting
	select {
	case m.waiter <- struct{}{}:
	default:
		// Don't block if no one is listening
	}
	return timer.Chan()
}

func (m *mockClock) Sleep(d time.Duration) {
	m.Advance(d)
}

func (m *mockClock) NewTimer(d time.Duration) raft.Timer {
	m.mu.Lock()
	defer m.mu.Unlock()
	timer := &mockTimer{
		C:       make(chan time.Time, 1),
		clock:   m,
		expires: m.currentTime.Add(d),
		active:  true,
	}
	m.timers = append(m.timers, timer)
	return timer
}

func (m *mockClock) NewTicker(d time.Duration) raft.Ticker {
	m.mu.Lock()
	defer m.mu.Unlock()
	ticker := &mockTicker{
		C:        make(chan time.Time, 1),
		clock:    m,
		interval: d,
		nextTick: m.currentTime.Add(d),
		active:   true,
	}
	m.tickers = append(m.tickers, ticker)
	return ticker
}

// Advance moves the mock clock's time forward, firing any expired timers or tickers.
func (m *mockClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newTime := m.currentTime.Add(d)
	m.currentTime = newTime

	// Fire timers
	var activeTimers []*mockTimer
	for _, timer := range m.timers {
		if timer.active && !timer.expires.After(newTime) {
			timer.active = false
			select {
			case timer.C <- timer.expires:
			default:
			}
		} else if timer.active {
			activeTimers = append(activeTimers, timer)
		}
	}
	m.timers = activeTimers

	// Fire tickers
	for _, ticker := range m.tickers {
		if ticker.active {
			for !ticker.nextTick.After(newTime) {
				select {
				case ticker.C <- ticker.nextTick:
				default:
				}
				ticker.nextTick = ticker.nextTick.Add(ticker.interval)
			}
		}
	}
}

// mockTimer implements the raft.Timer interface for testing.
type mockTimer struct {
	C       chan time.Time
	clock   *mockClock
	expires time.Time
	active  bool
}

func (mt *mockTimer) Chan() <-chan time.Time { return mt.C }

func (mt *mockTimer) Stop() bool {
	mt.clock.mu.Lock()
	defer mt.clock.mu.Unlock()
	if !mt.active {
		return false
	}
	mt.active = false
	return true
}

func (mt *mockTimer) Reset(d time.Duration) bool {
	mt.clock.mu.Lock()
	defer mt.clock.mu.Unlock()
	wasActive := mt.active
	mt.expires = mt.clock.currentTime.Add(d)
	mt.active = true
	return wasActive
}

// mockTicker implements the raft.Ticker interface for testing.
type mockTicker struct {
	C        chan time.Time
	clock    *mockClock
	interval time.Duration
	nextTick time.Time
	active   bool
}

func (mt *mockTicker) Chan() <-chan time.Time { return mt.C }

func (mt *mockTicker) Stop() {
	mt.clock.mu.Lock()
	defer mt.clock.mu.Unlock()
	mt.active = false
}

func (mt *mockTicker) Reset(d time.Duration) {
	mt.clock.mu.Lock()
	defer mt.clock.mu.Unlock()
	mt.interval = d
	mt.nextTick = mt.clock.currentTime.Add(d)
	mt.active = true
}

// mockRand provides a predictable source of random numbers for testing.
// It implements the raft.Rand interface.
type mockRand struct {
	mu       sync.Mutex
	float64s []float64
	fIdx     int
	intns    []int
	iIdx     int
}

func newMockRand() *mockRand {
	return &mockRand{}
}

func (m *mockRand) setFloat64s(vals ...float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.float64s = vals
	m.fIdx = 0
}

func (m *mockRand) Float64() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fIdx < len(m.float64s) {
		val := m.float64s[m.fIdx]
		m.fIdx++
		return val
	}
	return 0.5 // Default fallback value
}

func (m *mockRand) IntN(n int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.iIdx < len(m.intns) {
		val := m.intns[m.iIdx]
		m.iIdx++
		if val >= n {
			return n - 1
		}
		return val
	}
	return n / 2 // Default fallback value
}
