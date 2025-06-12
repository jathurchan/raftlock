package client

import (
	"context"
	"sync"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
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

func (m *mockClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newTime := m.currentTime.Add(d)
	m.currentTime = newTime

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

// mockRaftLockClient provides a comprehensive mock for pb.RaftLockClient
type mockRaftLockClient struct {
	acquireFunc          func(ctx context.Context, req *pb.AcquireRequest, opts ...grpc.CallOption) (*pb.AcquireResponse, error)
	releaseFunc          func(ctx context.Context, req *pb.ReleaseRequest, opts ...grpc.CallOption) (*pb.ReleaseResponse, error)
	renewFunc            func(ctx context.Context, req *pb.RenewRequest, opts ...grpc.CallOption) (*pb.RenewResponse, error)
	getLockInfoFunc      func(ctx context.Context, req *pb.GetLockInfoRequest, opts ...grpc.CallOption) (*pb.GetLockInfoResponse, error)
	getLocksFunc         func(ctx context.Context, req *pb.GetLocksRequest, opts ...grpc.CallOption) (*pb.GetLocksResponse, error)
	enqueueWaiterFunc    func(ctx context.Context, req *pb.EnqueueWaiterRequest, opts ...grpc.CallOption) (*pb.EnqueueWaiterResponse, error)
	cancelWaitFunc       func(ctx context.Context, req *pb.CancelWaitRequest, opts ...grpc.CallOption) (*pb.CancelWaitResponse, error)
	getBackoffAdviceFunc func(ctx context.Context, req *pb.BackoffAdviceRequest, opts ...grpc.CallOption) (*pb.BackoffAdviceResponse, error)
	getStatusFunc        func(ctx context.Context, req *pb.GetStatusRequest, opts ...grpc.CallOption) (*pb.GetStatusResponse, error)
	healthFunc           func(ctx context.Context, req *pb.HealthRequest, opts ...grpc.CallOption) (*pb.HealthResponse, error)
}

func (m *mockRaftLockClient) Acquire(ctx context.Context, req *pb.AcquireRequest, opts ...grpc.CallOption) (*pb.AcquireResponse, error) {
	if m.acquireFunc != nil {
		return m.acquireFunc(ctx, req, opts...)
	}
	return &pb.AcquireResponse{Acquired: true}, nil
}

func (m *mockRaftLockClient) Release(ctx context.Context, req *pb.ReleaseRequest, opts ...grpc.CallOption) (*pb.ReleaseResponse, error) {
	if m.releaseFunc != nil {
		return m.releaseFunc(ctx, req, opts...)
	}
	return &pb.ReleaseResponse{Released: true}, nil
}

func (m *mockRaftLockClient) Renew(ctx context.Context, req *pb.RenewRequest, opts ...grpc.CallOption) (*pb.RenewResponse, error) {
	if m.renewFunc != nil {
		return m.renewFunc(ctx, req, opts...)
	}
	return &pb.RenewResponse{Renewed: true}, nil
}

func (m *mockRaftLockClient) GetLockInfo(ctx context.Context, req *pb.GetLockInfoRequest, opts ...grpc.CallOption) (*pb.GetLockInfoResponse, error) {
	if m.getLockInfoFunc != nil {
		return m.getLockInfoFunc(ctx, req, opts...)
	}
	return &pb.GetLockInfoResponse{}, nil
}

func (m *mockRaftLockClient) GetLocks(ctx context.Context, req *pb.GetLocksRequest, opts ...grpc.CallOption) (*pb.GetLocksResponse, error) {
	if m.getLocksFunc != nil {
		return m.getLocksFunc(ctx, req, opts...)
	}
	return &pb.GetLocksResponse{}, nil
}

func (m *mockRaftLockClient) EnqueueWaiter(ctx context.Context, req *pb.EnqueueWaiterRequest, opts ...grpc.CallOption) (*pb.EnqueueWaiterResponse, error) {
	if m.enqueueWaiterFunc != nil {
		return m.enqueueWaiterFunc(ctx, req, opts...)
	}
	return &pb.EnqueueWaiterResponse{Enqueued: true}, nil
}

func (m *mockRaftLockClient) CancelWait(ctx context.Context, req *pb.CancelWaitRequest, opts ...grpc.CallOption) (*pb.CancelWaitResponse, error) {
	if m.cancelWaitFunc != nil {
		return m.cancelWaitFunc(ctx, req, opts...)
	}
	return &pb.CancelWaitResponse{Cancelled: true}, nil
}

func (m *mockRaftLockClient) GetBackoffAdvice(ctx context.Context, req *pb.BackoffAdviceRequest, opts ...grpc.CallOption) (*pb.BackoffAdviceResponse, error) {
	if m.getBackoffAdviceFunc != nil {
		return m.getBackoffAdviceFunc(ctx, req, opts...)
	}
	return &pb.BackoffAdviceResponse{}, nil
}

func (m *mockRaftLockClient) GetStatus(ctx context.Context, req *pb.GetStatusRequest, opts ...grpc.CallOption) (*pb.GetStatusResponse, error) {
	if m.getStatusFunc != nil {
		return m.getStatusFunc(ctx, req, opts...)
	}
	return &pb.GetStatusResponse{}, nil
}

func (m *mockRaftLockClient) Health(ctx context.Context, req *pb.HealthRequest, opts ...grpc.CallOption) (*pb.HealthResponse, error) {
	if m.healthFunc != nil {
		return m.healthFunc(ctx, req, opts...)
	}
	return &pb.HealthResponse{}, nil
}

// Enhanced mockRaftLockClient for testing admin functions
type adminMockClient struct {
	getStatusFunc        func(ctx context.Context, req *pb.GetStatusRequest, opts ...grpc.CallOption) (*pb.GetStatusResponse, error)
	healthFunc           func(ctx context.Context, req *pb.HealthRequest, opts ...grpc.CallOption) (*pb.HealthResponse, error)
	getBackoffAdviceFunc func(ctx context.Context, req *pb.BackoffAdviceRequest, opts ...grpc.CallOption) (*pb.BackoffAdviceResponse, error)
}

func (m *adminMockClient) Acquire(ctx context.Context, req *pb.AcquireRequest, opts ...grpc.CallOption) (*pb.AcquireResponse, error) {
	return &pb.AcquireResponse{}, nil
}

func (m *adminMockClient) Release(ctx context.Context, req *pb.ReleaseRequest, opts ...grpc.CallOption) (*pb.ReleaseResponse, error) {
	return &pb.ReleaseResponse{}, nil
}

func (m *adminMockClient) Renew(ctx context.Context, req *pb.RenewRequest, opts ...grpc.CallOption) (*pb.RenewResponse, error) {
	return &pb.RenewResponse{}, nil
}

func (m *adminMockClient) GetLockInfo(ctx context.Context, req *pb.GetLockInfoRequest, opts ...grpc.CallOption) (*pb.GetLockInfoResponse, error) {
	return &pb.GetLockInfoResponse{}, nil
}

func (m *adminMockClient) GetLocks(ctx context.Context, req *pb.GetLocksRequest, opts ...grpc.CallOption) (*pb.GetLocksResponse, error) {
	return &pb.GetLocksResponse{}, nil
}

func (m *adminMockClient) EnqueueWaiter(ctx context.Context, req *pb.EnqueueWaiterRequest, opts ...grpc.CallOption) (*pb.EnqueueWaiterResponse, error) {
	return &pb.EnqueueWaiterResponse{}, nil
}

func (m *adminMockClient) CancelWait(ctx context.Context, req *pb.CancelWaitRequest, opts ...grpc.CallOption) (*pb.CancelWaitResponse, error) {
	return &pb.CancelWaitResponse{}, nil
}

func (m *adminMockClient) GetBackoffAdvice(ctx context.Context, req *pb.BackoffAdviceRequest, opts ...grpc.CallOption) (*pb.BackoffAdviceResponse, error) {
	if m.getBackoffAdviceFunc != nil {
		return m.getBackoffAdviceFunc(ctx, req, opts...)
	}
	return &pb.BackoffAdviceResponse{}, nil
}

func (m *adminMockClient) GetStatus(ctx context.Context, req *pb.GetStatusRequest, opts ...grpc.CallOption) (*pb.GetStatusResponse, error) {
	if m.getStatusFunc != nil {
		return m.getStatusFunc(ctx, req, opts...)
	}
	return &pb.GetStatusResponse{}, nil
}

func (m *adminMockClient) Health(ctx context.Context, req *pb.HealthRequest, opts ...grpc.CallOption) (*pb.HealthResponse, error) {
	if m.healthFunc != nil {
		return m.healthFunc(ctx, req, opts...)
	}
	return &pb.HealthResponse{}, nil
}

// mockBaseClient provides a mock implementation of baseClient for testing
type mockBaseClient struct {
	executeWithRetryFunc func(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error
	getCurrentLeaderFunc func() string
	setCurrentLeaderFunc func(leader string)
	isConnectedFunc      func() bool
	closeFunc            func() error
	setRetryPolicyFunc   func(policy RetryPolicy)
	setRandFunc          func(r raft.Rand)
	getMetricsFunc       func() Metrics
	setConnectorFunc     func(connector connector)

	// State for testing
	currentLeader string
	connected     bool
	metrics       Metrics
	closed        bool
}

func (m *mockBaseClient) executeWithRetry(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error {
	if m.executeWithRetryFunc != nil {
		return m.executeWithRetryFunc(ctx, operation, fn)
	}
	return nil
}

func (m *mockBaseClient) getCurrentLeader() string {
	if m.getCurrentLeaderFunc != nil {
		return m.getCurrentLeaderFunc()
	}
	return m.currentLeader
}

func (m *mockBaseClient) setCurrentLeader(leader string) {
	if m.setCurrentLeaderFunc != nil {
		m.setCurrentLeaderFunc(leader)
		return
	}
	m.currentLeader = leader
}

func (m *mockBaseClient) isConnected() bool {
	if m.isConnectedFunc != nil {
		return m.isConnectedFunc()
	}
	return m.connected
}

func (m *mockBaseClient) close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	m.closed = true
	return nil
}

func (m *mockBaseClient) setRetryPolicy(policy RetryPolicy) {
	if m.setRetryPolicyFunc != nil {
		m.setRetryPolicyFunc(policy)
	}
}

func (m *mockBaseClient) setRand(r raft.Rand) {
	if m.setRandFunc != nil {
		m.setRandFunc(r)
	}
}

func (m *mockBaseClient) getMetrics() Metrics {
	if m.getMetricsFunc != nil {
		return m.getMetricsFunc()
	}
	if m.metrics == nil {
		m.metrics = &noOpMetrics{}
	}
	return m.metrics
}

func (m *mockBaseClient) setConnector(connector connector) {
	if m.setConnectorFunc != nil {
		m.setConnectorFunc(connector)
	}
}
