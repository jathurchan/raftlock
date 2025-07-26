package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

type mockClock struct {
	mu       sync.RWMutex
	current  time.Time
	tickers  []*mockTicker
	timers   []*mockTimer
	tickerID int64
}

func newMockClock() *mockClock {
	return &mockClock{
		current: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (tc *mockClock) Now() time.Time {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.current
}

func (tc *mockClock) Since(t time.Time) time.Duration {
	return tc.Now().Sub(t)
}

func (tc *mockClock) Until(t time.Time) time.Duration {
	return t.Sub(tc.Now())
}

func (tc *mockClock) After(d time.Duration) <-chan time.Time {
	c := make(chan time.Time, 1)
	go func() {
		time.Sleep(1 * time.Millisecond) // Minimal delay for test scheduling
		c <- tc.Now().Add(d)
	}()
	return c
}

func (tc *mockClock) NewTicker(d time.Duration) raft.Ticker {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	id := atomic.AddInt64(&tc.tickerID, 1)
	ticker := &mockTicker{
		id:       id,
		clock:    tc,
		interval: d,
		ch:       make(chan time.Time, 10), // Buffered to prevent blocking
		done:     make(chan struct{}),
	}
	tc.tickers = append(tc.tickers, ticker)
	return ticker
}

func (tc *mockClock) NewTimer(d time.Duration) raft.Timer {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	timer := &mockTimer{
		clock:    tc,
		duration: d,
		ch:       make(chan time.Time, 1),
		done:     make(chan struct{}),
	}
	tc.timers = append(tc.timers, timer)
	return timer
}

func (tc *mockClock) Sleep(d time.Duration) {
	time.Sleep(1 * time.Millisecond) // Minimal sleep for tests
}

func (tc *mockClock) Advance(d time.Duration) {
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

func (tc *mockClock) AdvanceAndTrigger(d time.Duration) {
	tc.Advance(d)
	time.Sleep(5 * time.Millisecond) // Allow tick processing
}

type mockTicker struct {
	id       int64
	clock    *mockClock
	interval time.Duration
	ch       chan time.Time
	done     chan struct{}
	stopped  atomic.Bool
	mu       sync.Mutex
}

func (tt *mockTicker) Chan() <-chan time.Time {
	return tt.ch
}

func (tt *mockTicker) Stop() {
	if tt.stopped.CompareAndSwap(false, true) {
		tt.mu.Lock()
		defer tt.mu.Unlock()
		close(tt.done)
	}
}

func (tt *mockTicker) Reset(d time.Duration) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	if tt.stopped.Load() {
		return
	}

	tt.interval = d
}

func (tt *mockTicker) isStopped() bool {
	return tt.stopped.Load()
}

func (tt *mockTicker) triggerTick(now time.Time) {
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

type mockTimer struct {
	clock     *mockClock
	duration  time.Duration
	ch        chan time.Time
	done      chan struct{}
	stopped   atomic.Bool
	triggered atomic.Bool
	startTime time.Time
	mu        sync.Mutex
}

func (tt *mockTimer) Chan() <-chan time.Time {
	return tt.ch
}

func (tt *mockTimer) Stop() bool {
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

func (tt *mockTimer) Reset(d time.Duration) bool {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	wasActive := !tt.stopped.Load() && !tt.triggered.Load()

	tt.stopped.Store(false)
	tt.triggered.Store(false)
	tt.duration = d
	tt.startTime = tt.clock.Now()

	return wasActive
}

func (tt *mockTimer) isStopped() bool {
	return tt.stopped.Load() || tt.triggered.Load()
}

func (tt *mockTimer) shouldTrigger(now time.Time) bool {
	if tt.triggered.Load() || tt.stopped.Load() {
		return false
	}
	return now.Sub(tt.startTime) >= tt.duration
}

func (tt *mockTimer) triggerTimer(now time.Time) {
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

func (m *mockServerMetrics) ObserveRaftProposalLatency(
	operation types.LockOperation,
	latency time.Duration,
) {
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

type mockNetworkManager struct {
	mu                      sync.Mutex
	localAddrVal            string
	peerStatusMap           map[types.NodeID]types.PeerConnectionStatus
	sendRequestVoteFunc     func(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error)
	sendAppendEntriesFunc   func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error)
	sendInstallSnapshotFunc func(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error)
	resetConnectionCalled   bool
	resetConnectionPeerID   types.NodeID
}

// NewMockNetworkManager creates a new mockNetworkManager with default values.
func NewMockNetworkManager() *mockNetworkManager {
	return &mockNetworkManager{
		localAddrVal:  "localhost:8080",
		peerStatusMap: make(map[types.NodeID]types.PeerConnectionStatus),
	}
}

func (m *mockNetworkManager) Start() error {
	// No-op for mock
	return nil
}

func (m *mockNetworkManager) Stop() error {
	// No-op for mock
	return nil
}

func (m *mockNetworkManager) SendRequestVote(
	ctx context.Context,
	target types.NodeID,
	args *types.RequestVoteArgs,
) (*types.RequestVoteReply, error) {
	if m.sendRequestVoteFunc != nil {
		return m.sendRequestVoteFunc(ctx, target, args)
	}
	return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
}

func (m *mockNetworkManager) SendAppendEntries(
	ctx context.Context,
	target types.NodeID,
	args *types.AppendEntriesArgs,
) (*types.AppendEntriesReply, error) {
	if m.sendAppendEntriesFunc != nil {
		return m.sendAppendEntriesFunc(ctx, target, args)
	}
	return &types.AppendEntriesReply{Term: args.Term, Success: true}, nil
}

func (m *mockNetworkManager) SendInstallSnapshot(
	ctx context.Context,
	target types.NodeID,
	args *types.InstallSnapshotArgs,
) (*types.InstallSnapshotReply, error) {
	if m.sendInstallSnapshotFunc != nil {
		return m.sendInstallSnapshotFunc(ctx, target, args)
	}
	return &types.InstallSnapshotReply{Term: args.Term}, nil
}

func (m *mockNetworkManager) PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	status, exists := m.peerStatusMap[peer]
	if !exists {
		return types.PeerConnectionStatus{Connected: false}, raft.ErrPeerNotFound
	}
	return status, nil
}

func (m *mockNetworkManager) LocalAddr() string {
	return m.localAddrVal
}

func (m *mockNetworkManager) ResetConnection(ctx context.Context, peerID types.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetConnectionCalled = true
	m.resetConnectionPeerID = peerID
	// In a real test, you might want to simulate a reconnection delay or success/failure.
	// For now, it just records that it was called.
	return nil
}

// SetLocalAddr sets the address returned by LocalAddr().
func (m *mockNetworkManager) SetLocalAddr(addr string) {
	m.localAddrVal = addr
}

// SetPeerStatus sets the connection status for a specific peer.
func (m *mockNetworkManager) SetPeerStatus(peerID types.NodeID, status types.PeerConnectionStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peerStatusMap[peerID] = status
}

// SetSendRequestVoteFunc sets a custom function for SendRequestVote.
func (m *mockNetworkManager) SetSendRequestVoteFunc(
	f func(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error),
) {
	m.sendRequestVoteFunc = f
}

// SetSendAppendEntriesFunc sets a custom function for SendAppendEntries.
func (m *mockNetworkManager) SetSendAppendEntriesFunc(
	f func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error),
) {
	m.sendAppendEntriesFunc = f
}

// SetSendInstallSnapshotFunc sets a custom function for SendInstallSnapshot.
func (m *mockNetworkManager) SetSendInstallSnapshotFunc(
	f func(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error),
) {
	m.sendInstallSnapshotFunc = f
}

// WasResetConnectionCalled checks if ResetConnection was called and returns the peer ID.
func (m *mockNetworkManager) WasResetConnectionCalled() (bool, types.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resetConnectionCalled, m.resetConnectionPeerID
}
