package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/lock"
	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/storage"
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

type mockRaft struct {
	isLeader       bool
	leaderID       types.NodeID
	term           types.Term
	commitIndex    types.Index
	lastApplied    types.Index
	status         types.RaftStatus
	applyCh        chan types.ApplyMsg
	leaderChangeCh chan types.NodeID
	proposeFunc    func(ctx context.Context, cmd []byte) (types.Index, types.Term, bool, error)
	readIndexFunc  func(ctx context.Context) (types.Index, error)
}

func (m *mockRaft) SetNetworkManager(nm raft.NetworkManager) {}

func (m *mockRaft) Start() error { return nil }

func (m *mockRaft) Stop(ctx context.Context) error { return nil }

func (m *mockRaft) Tick(ctx context.Context) {}

func (m *mockRaft) Propose(ctx context.Context, command []byte) (types.Index, types.Term, bool, error) {
	if m.proposeFunc != nil {
		return m.proposeFunc(ctx, command)
	}
	return 1, 1, m.isLeader, nil
}

func (m *mockRaft) ReadIndex(ctx context.Context) (types.Index, error) {
	if m.readIndexFunc != nil {
		return m.readIndexFunc(ctx)
	}
	if !m.isLeader {
		return 0, raft.ErrNotLeader
	}
	return m.commitIndex, nil
}

func (m *mockRaft) Status() types.RaftStatus {
	return m.status
}

func (m *mockRaft) GetState() (types.Term, bool) {
	return m.term, m.isLeader
}

func (m *mockRaft) GetLeaderID() types.NodeID {
	return m.leaderID
}

func (m *mockRaft) GetCommitIndex() types.Index {
	return m.commitIndex
}

func (m *mockRaft) ApplyChannel() <-chan types.ApplyMsg {
	return m.applyCh
}

func (m *mockRaft) LeaderChangeChannel() <-chan types.NodeID {
	return m.leaderChangeCh
}

func (m *mockRaft) RequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
	return nil, nil
}

func (m *mockRaft) AppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
	return nil, nil
}

func (m *mockRaft) InstallSnapshot(ctx context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
	return nil, nil
}

type mockLockManager struct {
	locks        map[types.LockID]*types.LockInfo
	applyFunc    func(ctx context.Context, index types.Index, cmdData []byte) (any, error)
	getInfoFunc  func(ctx context.Context, lockID types.LockID) (*types.LockInfo, error)
	getLocksFunc func(ctx context.Context, filter lock.LockFilter, limit int, offset int) ([]*types.LockInfo, int, error)
	tickFunc     func(ctx context.Context) int
}

func (m *mockLockManager) Apply(ctx context.Context, index types.Index, cmdData []byte) (any, error) {
	if m.applyFunc != nil {
		return m.applyFunc(ctx, index, cmdData)
	}
	return nil, nil
}

func (m *mockLockManager) Snapshot(ctx context.Context) (types.Index, []byte, error) {
	return 0, []byte("{}"), nil
}

func (m *mockLockManager) RestoreSnapshot(ctx context.Context, lastIncludedIndex types.Index, lastIncludedTerm types.Term, snapshotData []byte) error {
	m.locks = make(map[types.LockID]*types.LockInfo)
	return nil
}

func (m *mockLockManager) ApplyAcquire(ctx context.Context, lockID types.LockID, clientID types.ClientID, ttl time.Duration, version types.Index) (*types.LockInfo, error) {
	return nil, nil
}

func (m *mockLockManager) ApplyRelease(ctx context.Context, lockID types.LockID, clientID types.ClientID, version types.Index) (bool, error) {
	return true, nil
}

func (m *mockLockManager) ApplyRenew(ctx context.Context, lockID types.LockID, clientID types.ClientID, version types.Index, ttl time.Duration) error {
	return nil
}

func (m *mockLockManager) ApplyWaitQueue(ctx context.Context, lockID types.LockID, clientID types.ClientID, timeout time.Duration, version types.Index, priority int) (int, error) {
	return 0, nil
}

func (m *mockLockManager) ApplyCancelWait(ctx context.Context, lockID types.LockID, clientID types.ClientID, version types.Index) (bool, error) {
	return true, nil
}

func (m *mockLockManager) GetLockInfo(ctx context.Context, lockID types.LockID) (*types.LockInfo, error) {
	if m.getInfoFunc != nil {
		return m.getInfoFunc(ctx, lockID)
	}
	if info, exists := m.locks[lockID]; exists {
		return info, nil
	}
	return nil, lock.ErrLockNotFound
}

func (m *mockLockManager) GetLocks(ctx context.Context, filter lock.LockFilter, limit int, offset int) ([]*types.LockInfo, int, error) {
	if m.getLocksFunc != nil {
		return m.getLocksFunc(ctx, filter, limit, offset)
	}
	return []*types.LockInfo{}, 0, nil
}

func (m *mockLockManager) Tick(ctx context.Context) int {
	if m.tickFunc != nil {
		return m.tickFunc(ctx)
	}
	return 0
}

func (m *mockLockManager) Close() error {
	return nil
}

func (m *mockLockManager) GetActiveLockCount(ctx context.Context) (int, error) {
	return len(m.locks), nil
}

func (m *mockLockManager) GetTotalWaiterCount(ctx context.Context) (int, error) {
	return 0, nil
}

type mockStorage struct{}

func (m *mockStorage) SaveState(ctx context.Context, state types.PersistentState) error {
	return nil
}

func (m *mockStorage) LoadState(ctx context.Context) (types.PersistentState, error) {
	return types.PersistentState{}, nil
}

func (m *mockStorage) AppendLogEntries(ctx context.Context, entries []types.LogEntry) error {
	return nil
}

func (m *mockStorage) GetLogEntries(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
	return []types.LogEntry{}, nil
}

func (m *mockStorage) GetLogEntry(ctx context.Context, index types.Index) (types.LogEntry, error) {
	return types.LogEntry{}, nil
}

func (m *mockStorage) TruncateLogSuffix(ctx context.Context, index types.Index) error {
	return nil
}

func (m *mockStorage) TruncateLogPrefix(ctx context.Context, index types.Index) error {
	return nil
}

func (m *mockStorage) SaveSnapshot(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
	return nil
}

func (m *mockStorage) LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	return types.SnapshotMetadata{}, nil, storage.ErrNoSnapshot
}

func (m *mockStorage) LastLogIndex() types.Index {
	return 0
}

func (m *mockStorage) FirstLogIndex() types.Index {
	return 0
}

func (m *mockStorage) Close() error {
	return nil
}

func (m *mockStorage) ResetMetrics() {}

func (m *mockStorage) GetMetrics() map[string]uint64 {
	return nil
}

func (m *mockStorage) GetMetricsSummary() string {
	return ""
}

type mockNetworkManager struct{}

func (m *mockNetworkManager) Start() error {
	return nil
}

func (m *mockNetworkManager) Stop() error {
	return nil
}

func (m *mockNetworkManager) SendRequestVote(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
	return nil, nil
}

func (m *mockNetworkManager) SendAppendEntries(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
	return nil, nil
}

func (m *mockNetworkManager) SendInstallSnapshot(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
	return nil, nil
}

func (m *mockNetworkManager) PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error) {
	return types.PeerConnectionStatus{}, nil
}

func (m *mockNetworkManager) LocalAddr() string {
	return "localhost:8080"
}

type mockProposalTracker struct {
	proposals map[types.ProposalID]*types.PendingProposal
	trackFunc func(proposal *types.PendingProposal) error
}

func (m *mockProposalTracker) Track(proposal *types.PendingProposal) error {
	if m.trackFunc != nil {
		return m.trackFunc(proposal)
	}
	if m.proposals == nil {
		m.proposals = make(map[types.ProposalID]*types.PendingProposal)
	}
	m.proposals[proposal.ID] = proposal
	return nil
}

func (m *mockProposalTracker) HandleAppliedCommand(applyMsg types.ApplyMsg) {}

func (m *mockProposalTracker) HandleSnapshotApplied(snapshotIndex types.Index, snapshotTerm types.Term) {
}

func (m *mockProposalTracker) ClientCancel(proposalID types.ProposalID, reason error) bool {
	return false
}

func (m *mockProposalTracker) GetPendingCount() int64 {
	return int64(len(m.proposals))
}

func (m *mockProposalTracker) GetStats() types.ProposalStats {
	return types.ProposalStats{}
}

func (m *mockProposalTracker) GetPendingProposal(proposalID types.ProposalID) (types.PendingProposal, bool) {
	proposal, exists := m.proposals[proposalID]
	if !exists {
		return types.PendingProposal{}, false
	}
	return *proposal, true
}

func (m *mockProposalTracker) Cleanup() int {
	return 0
}

func (m *mockProposalTracker) Close() error {
	return nil
}

type mockRateLimiter struct {
	allow bool
	err   error
}

func (m *mockRateLimiter) Allow() bool                    { return m.allow }
func (m *mockRateLimiter) Wait(ctx context.Context) error { return m.err }
