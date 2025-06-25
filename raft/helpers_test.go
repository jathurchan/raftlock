package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

type mockStorage struct {
	mu            sync.RWMutex
	log           []types.LogEntry
	firstLogIndex types.Index
	state         types.PersistentState
	failOps       map[string]error

	saveCounter int
	loadCounter int

	snapshotAttempted bool
	snapshotSaved     bool
	hookLoadSnapshot  func(ctx context.Context) (types.SnapshotMetadata, []byte, error)
	hookSaveSnapshot  func()

	hookGetLogEntry   func(index types.Index)
	hookGetLogEntries func(start, end types.Index) []types.LogEntry
	hookFirstLogIndex func() types.Index
	hookLastLogIndex  func() types.Index
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		log:     make([]types.LogEntry, 0),
		failOps: make(map[string]error),
	}
}

func (ms *mockStorage) getSnapshotAttempted() bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.snapshotAttempted
}

func (ms *mockStorage) setFailure(op string, err error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.failOps[op] = err
}

func (ms *mockStorage) clearFailures() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.failOps = make(map[string]error)
}

func (ms *mockStorage) checkFailure(op string) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err, ok := ms.failOps[op]; ok {
		return err
	}
	return nil
}

func (ms *mockStorage) SaveState(ctx context.Context, state types.PersistentState) error {
	if err := ms.checkFailure("SaveState"); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.state = state
	ms.saveCounter++
	return nil
}

func (ms *mockStorage) LoadState(ctx context.Context) (types.PersistentState, error) {
	if err := ms.checkFailure("LoadState"); err != nil {
		return types.PersistentState{}, err
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	ms.loadCounter++
	return ms.state, nil
}

func (ms *mockStorage) AppendLogEntries(ctx context.Context, entries []types.LogEntry) error {
	if err := ms.checkFailure("AppendLogEntries"); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if len(entries) == 0 {
		return storage.ErrEmptyEntries
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Index <= entries[i-1].Index {
			return storage.ErrOutOfOrderEntries
		}
	}
	if len(ms.log) > 0 {
		if entries[0].Index != ms.log[len(ms.log)-1].Index+1 {
			return storage.ErrNonContiguousEntries
		}
	} else if entries[0].Index > 1 {
		expectedFirst := ms.firstLogIndex
		if expectedFirst == 0 {
			expectedFirst = 1
		}
		if entries[0].Index != expectedFirst {
			return storage.ErrNonContiguousEntries
		}
	}

	ms.log = append(ms.log, entries...)
	return nil
}

func (ms *mockStorage) GetLogEntries(
	ctx context.Context,
	start, end types.Index,
) ([]types.LogEntry, error) {
	if err := ms.checkFailure("GetLogEntries"); err != nil {
		return nil, err
	}
	if ms.hookGetLogEntries != nil {
		return ms.hookGetLogEntries(start, end), nil
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if start >= end {
		return nil, storage.ErrInvalidLogRange
	}
	if len(ms.log) == 0 && start > 0 {
		return nil, storage.ErrIndexOutOfRange
	}
	if len(ms.log) == 0 && start == 0 {
		return []types.LogEntry{}, nil
	}

	effectiveFirstLogIndex := ms.firstLogIndex
	if len(ms.log) > 0 && effectiveFirstLogIndex == 0 {
		effectiveFirstLogIndex = ms.log[0].Index
	}

	if start < effectiveFirstLogIndex || start > ms.log[len(ms.log)-1].Index+1 {
		return nil, storage.ErrIndexOutOfRange
	}

	var result []types.LogEntry
	for _, entry := range ms.log {
		if entry.Index >= start && entry.Index < end {
			result = append(result, entry)
		}
	}

	return result, nil
}

func (ms *mockStorage) GetLogEntry(ctx context.Context, index types.Index) (types.LogEntry, error) {
	if err := ms.checkFailure("GetLogEntry"); err != nil {
		return types.LogEntry{}, err
	}

	if ms.hookGetLogEntry != nil {
		ms.hookGetLogEntry(index)
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.log) == 0 {
		return types.LogEntry{}, storage.ErrEntryNotFound
	}

	firstIdx := ms.firstLogIndex
	if firstIdx == 0 && len(ms.log) > 0 {
		firstIdx = ms.log[0].Index
	}
	if index < firstIdx || index > ms.log[len(ms.log)-1].Index {
		return types.LogEntry{}, storage.ErrEntryNotFound
	}

	for _, entry := range ms.log {
		if entry.Index == index {
			return entry, nil
		}
	}
	return types.LogEntry{}, storage.ErrEntryNotFound
}

func (ms *mockStorage) TruncateLogSuffix(ctx context.Context, index types.Index) error {
	if err := ms.checkFailure("TruncateLogSuffix"); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if len(ms.log) == 0 {
		return nil
	}

	firstIdx := ms.firstLogIndex
	if firstIdx == 0 && len(ms.log) > 0 {
		firstIdx = ms.log[0].Index
	}

	if index <= firstIdx {
		ms.log = []types.LogEntry{}
		ms.firstLogIndex = 0
		return nil
	}

	var newLog []types.LogEntry
	for _, entry := range ms.log {
		if entry.Index < index {
			newLog = append(newLog, entry)
		}
	}
	ms.log = newLog
	return nil
}

func (ms *mockStorage) TruncateLogPrefix(ctx context.Context, index types.Index) error {
	if err := ms.checkFailure("TruncateLogPrefix"); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if len(ms.log) == 0 || index <= 1 {
		return nil
	}

	firstIdx := ms.firstLogIndex
	if firstIdx == 0 && len(ms.log) > 0 {
		firstIdx = ms.log[0].Index
	}

	if index <= firstIdx {
		return nil
	}

	var newLog []types.LogEntry
	newFirstIndex := index
	foundNewFirst := false
	for _, entry := range ms.log {
		if entry.Index >= index {
			if !foundNewFirst {
				newFirstIndex = entry.Index
				foundNewFirst = true
			}
			newLog = append(newLog, entry)
		}
	}
	ms.log = newLog
	if len(newLog) > 0 {
		ms.firstLogIndex = newFirstIndex
	} else {
		if index > firstIdx {
			ms.firstLogIndex = index
		}
	}
	return nil
}

func (ms *mockStorage) SaveSnapshot(
	ctx context.Context,
	metadata types.SnapshotMetadata,
	data []byte,
) error {

	if ms.hookSaveSnapshot != nil {
		defer ms.hookSaveSnapshot()
	}

	if err := ms.checkFailure("SaveSnapshot"); err != nil {
		return err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.snapshotAttempted = true
	ms.snapshotSaved = true
	return nil
}

func (ms *mockStorage) LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	if ms.hookLoadSnapshot != nil {
		return ms.hookLoadSnapshot(ctx)
	}
	if err := ms.checkFailure("LoadSnapshot"); err != nil {
		return types.SnapshotMetadata{}, nil, err
	}
	return types.SnapshotMetadata{}, nil, storage.ErrNoSnapshot
}

func (ms *mockStorage) LastLogIndex() types.Index {
	if ms.hookLastLogIndex != nil {
		return ms.hookLastLogIndex()
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.log) == 0 {
		if ms.firstLogIndex > 0 {
			return ms.firstLogIndex - 1
		}
		return 0
	}
	return ms.log[len(ms.log)-1].Index
}

func (ms *mockStorage) FirstLogIndex() types.Index {
	if ms.hookFirstLogIndex != nil {
		return ms.hookFirstLogIndex()
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.log) == 0 {
		return ms.firstLogIndex
	}

	if ms.firstLogIndex > 0 {
		return ms.firstLogIndex
	}
	return ms.log[0].Index
}

func (ms *mockStorage) Close() error { return nil }

func (ms *mockStorage) ResetMetrics() {}

func (ms *mockStorage) GetMetrics() map[string]uint64 {
	return nil
}

func (ms *mockStorage) GetMetricsSummary() string {
	return ""
}

type mockNetworkManager struct {
	mu                 sync.RWMutex // Protects access to function pointers
	requestVoteSuccess bool
	requestVoteReplies map[types.NodeID]*types.RequestVoteReply

	startFunc               func() error
	sendAppendEntriesFunc   func(context.Context, types.NodeID, *types.AppendEntriesArgs) (*types.AppendEntriesReply, error)
	sendRequestVoteFunc     func(context.Context, types.NodeID, *types.RequestVoteArgs) (*types.RequestVoteReply, error)
	sendInstallSnapshotFunc func(context.Context, types.NodeID, *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error)
	appendEntriesCallCount  int
	heartbeatCallCount      int

	wg *sync.WaitGroup

	stopFunc func() error
}

func newMockNetworkManager() *mockNetworkManager {
	return &mockNetworkManager{
		sendAppendEntriesFunc: func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
			return &types.AppendEntriesReply{
				Term:    args.Term,
				Success: true,
			}, nil
		},
		sendRequestVoteFunc: func(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
			return &types.RequestVoteReply{
				Term:        args.Term,
				VoteGranted: true,
			}, nil
		},
		sendInstallSnapshotFunc: func(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
			return &types.InstallSnapshotReply{
				Term: args.Term,
			}, nil
		},
	}
}

func (m *mockNetworkManager) setSendAppendEntriesFunc(
	f func(context.Context, types.NodeID, *types.AppendEntriesArgs) (*types.AppendEntriesReply, error),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendAppendEntriesFunc = f
}

func (m *mockNetworkManager) Start() error {
	if m.startFunc != nil {
		return m.startFunc()

	}
	return nil
}

func (m *mockNetworkManager) Stop() error {
	if m.stopFunc != nil {
		return m.stopFunc()
	}
	return nil
}

func (m *mockNetworkManager) SendRequestVote(
	ctx context.Context,
	target types.NodeID,
	args *types.RequestVoteArgs,
) (*types.RequestVoteReply, error) {
	m.mu.RLock()
	fn := m.sendRequestVoteFunc
	m.mu.RUnlock()

	if fn != nil {
		return fn(ctx, target, args)
	}
	if !m.requestVoteSuccess {
		return nil, fmt.Errorf("mock network error")
	}

	reply, ok := m.requestVoteReplies[target]
	if !ok {
		return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
	}
	return reply, nil
}

func (m *mockNetworkManager) SendAppendEntries(
	ctx context.Context,
	target types.NodeID,
	args *types.AppendEntriesArgs,
) (*types.AppendEntriesReply, error) {
	m.mu.Lock()
	m.appendEntriesCallCount++
	if len(args.Entries) == 0 {
		m.heartbeatCallCount++
	}
	fn := m.sendAppendEntriesFunc
	m.mu.Unlock()

	if m.wg != nil {
		defer m.wg.Done()
	}

	if fn == nil {
		return nil, fmt.Errorf("sendAppendEntriesFunc not set")
	}

	return fn(ctx, target, args)
}

func (m *mockNetworkManager) SendInstallSnapshot(
	ctx context.Context,
	target types.NodeID,
	args *types.InstallSnapshotArgs,
) (*types.InstallSnapshotReply, error) {
	m.mu.RLock()
	fn := m.sendInstallSnapshotFunc
	m.mu.RUnlock()

	if fn == nil {
		return &types.InstallSnapshotReply{Term: args.Term}, nil
	}
	return fn(ctx, target, args)
}

func (m *mockNetworkManager) PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error) {
	return types.PeerConnectionStatus{Connected: true}, nil
}

func (m *mockNetworkManager) LocalAddr() string {
	return "localhost:1234"
}

func (m *mockNetworkManager) getAndResetCallCounts() (appendEntries, heartbeats int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	appendEntries = m.appendEntriesCallCount
	heartbeats = m.heartbeatCallCount
	m.appendEntriesCallCount = 0
	m.heartbeatCallCount = 0
	return
}

func (m *mockNetworkManager) setWaitGroup(wg *sync.WaitGroup) {
	m.wg = wg
}

type mockApplier struct {
	mu         sync.Mutex
	applied    map[types.Index][]byte
	applyCalls int

	restoreErr      error
	snapshotData    []byte
	applierRestored bool
	snapshotFunc    func(ctx context.Context) (types.Index, []byte, error)
	applyFunc       func(ctx context.Context, index types.Index, command []byte) (resultData any, err error)
}

func (m *mockApplier) Apply(
	ctx context.Context,
	index types.Index,
	command []byte,
) (resultData any, err error) {
	if m.applyFunc != nil {
		return m.applyFunc(ctx, index, command)
	}
	return nil, nil
}

func (m *mockApplier) Snapshot(ctx context.Context) (types.Index, []byte, error) {
	if m.snapshotFunc != nil {
		return m.snapshotFunc(ctx)
	}
	return 0, nil, nil
}

func (m *mockApplier) RestoreSnapshot(
	ctx context.Context,
	lastIncludedIndex types.Index,
	lastIncludedTerm types.Term,
	snapshotData []byte,
) error {
	m.applierRestored = true
	m.snapshotData = snapshotData
	return m.restoreErr
}

type mockClock struct {
	nowVal       time.Time
	mu           sync.Mutex
	afterChannel chan time.Time
	timers       []*mockTimer
	tickers      []*mockTicker
}

func newMockClock() *mockClock {
	return &mockClock{
		nowVal: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (m *mockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nowVal
}

func (m *mockClock) Since(t time.Time) time.Duration {
	return m.Now().Sub(t)
}

func (m *mockClock) TriggerAfter(d time.Duration, t *testing.T) {
	m.mu.Lock()
	m.nowVal = m.nowVal.Add(d)
	currentAfterChan := m.afterChannel
	currentTime := m.nowVal
	m.mu.Unlock()

	if currentAfterChan != nil {
		select {
		case currentAfterChan <- currentTime:
		case <-time.After(50 * time.Millisecond):
			if t != nil {
				t.Logf(
					"mockClock.TriggerAfter: timeout waiting for test to receive from afterChannel",
				)
			}
		}
	}
}

func (m *mockClock) After(d time.Duration) <-chan time.Time {
	m.mu.Lock()
	ch := make(chan time.Time, 1)
	m.afterChannel = ch
	m.mu.Unlock()

	go func() {
		time.Sleep(1 * time.Millisecond) // Minimal real sleep to allow channel return
		m.mu.Lock()
		m.nowVal = m.nowVal.Add(d)
		sendTime := m.nowVal
		m.mu.Unlock()
		ch <- sendTime
	}()
	return ch
}

func (m *mockClock) NewTicker(d time.Duration) Ticker {
	m.mu.Lock()
	defer m.mu.Unlock()
	ticker := &mockTicker{
		C:         make(chan time.Time, 1), // Buffered to allow advanceTime to send
		d:         d,
		clock:     m,
		active:    true,
		resetChan: make(chan time.Duration),
		stopChan:  make(chan struct{}),
	}
	m.tickers = append(m.tickers, ticker)
	go ticker.run()
	return ticker
}

func (m *mockClock) NewTimer(d time.Duration) Timer {
	m.mu.Lock()
	defer m.mu.Unlock()

	timer := newMockTimer(d, m)
	m.timers = append(m.timers, timer) // Keep track of it
	return timer
}

func (m *mockClock) Sleep(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nowVal = m.nowVal.Add(d)
}

func (m *mockClock) advanceTime(d time.Duration) {
	m.mu.Lock()
	m.nowVal = m.nowVal.Add(d)
	for _, timer := range m.timers {
		timer.checkAndSignal(m.nowVal)
	}
	for _, ticker := range m.tickers {
		ticker.checkAndSignal(m.nowVal)
	}
	m.mu.Unlock()
}

type mockTicker struct {
	C         chan time.Time
	d         time.Duration
	clock     *mockClock
	nextTick  time.Time
	active    bool
	mu        sync.Mutex
	resetChan chan time.Duration
	stopChan  chan struct{}
}

func (m *mockTicker) Chan() <-chan time.Time {
	return m.C
}

func (m *mockTicker) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active {
		m.active = false
		close(m.stopChan)
	}
}

func (m *mockTicker) Reset(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d <= 0 {
		panic("non-positive interval for mockTicker.Reset")
	}
	m.d = d
	m.nextTick = m.clock.Now().Add(d)
	select {
	case m.resetChan <- d:
	default:
	}
}

func (m *mockTicker) run() {
	m.mu.Lock()
	m.nextTick = m.clock.Now().Add(m.d)
	currentDuration := m.d
	m.mu.Unlock()

	for {
		m.mu.Lock()
		isActive := m.active
		nextT := m.nextTick
		m.mu.Unlock()

		if !isActive {
			return
		}

		now := m.clock.Now()
		var waitTime time.Duration
		if now.Before(nextT) {
			waitTime = nextT.Sub(now)
		}

		select {
		case <-time.After(waitTime):
			m.mu.Lock()
			if !m.active {
				m.mu.Unlock()
				return
			}
			if !m.clock.Now().Before(m.nextTick) {
				select {
				case m.C <- m.nextTick:
				default:
				}
				m.nextTick = m.nextTick.Add(currentDuration)
			}
			m.mu.Unlock()
		case newDuration := <-m.resetChan:
			m.mu.Lock()
			currentDuration = newDuration
			m.mu.Unlock()
		case <-m.stopChan:
			return
		}
	}
}

func (m *mockTicker) checkAndSignal(currentTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active && !currentTime.Before(m.nextTick) {
		select {
		case m.C <- m.nextTick:
		default:
		}
		m.nextTick = m.nextTick.Add(m.d)
	}
}

type mockTimer struct {
	C         chan time.Time
	mu        sync.Mutex
	mockClock *mockClock // ref to mock clock
	expiresAt time.Time  // when this timer should expire according to mock clock
	active    bool
	d         time.Duration // duration of the timer
}

func newMockTimer(d time.Duration, clock *mockClock) *mockTimer {
	mt := &mockTimer{
		C:         make(chan time.Time, 1),
		mockClock: clock,
		d:         d,
		active:    true,
	}
	mt.expiresAt = clock.Now().Add(d)
	return mt
}

func (m *mockTimer) Chan() <-chan time.Time {
	return m.C
}

func (m *mockTimer) Stop() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active {
		m.active = false
		select {
		case <-m.C:
		default:
		}
		return true
	}
	return false
}

func (m *mockTimer) Reset(d time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	wasActive := m.active
	m.active = true
	m.d = d
	m.expiresAt = m.mockClock.Now().Add(d)

	select {
	case <-m.C:
	default:
	}
	return wasActive
}

func (m *mockTimer) checkAndSignal(currentTime time.Time) {
	m.mu.Lock()
	if m.active && !currentTime.Before(m.expiresAt) {
		select {
		case m.C <- m.expiresAt:
			m.active = false
		default:
		}
	}
	m.mu.Unlock()
}

type mockRand struct{}

func (m *mockRand) IntN(n int) int {
	if n <= 0 {
		panic("IntN called with non-positive n")
	}
	return 0
}

func (m *mockRand) Float64() float64 {
	return 0.0
}

type mockMetrics struct {
	mu                sync.Mutex
	logStateCount     int
	logAppendCount    int
	logReadCount      int
	logConsistencyErr int
	logTruncateCount  int

	lastTruncateType    LogTruncateType
	lastEntriesRemoved  int
	lastTruncateLatency time.Duration
	lastTruncateSuccess bool

	peerReplicationCalls []struct {
		peerID  types.NodeID
		success bool
		reason  ReplicationResult
	}

	grpcClientRPCSuccessTotal  map[string]int
	grpcClientRPCFailuresTotal map[string]int
	grpcClientRPCLatencyHist   map[string][]float64
}

func newMockMetrics() *mockMetrics {
	return &mockMetrics{
		peerReplicationCalls: make([]struct {
			peerID  types.NodeID
			success bool
			reason  ReplicationResult
		}, 0),
		grpcClientRPCSuccessTotal:  make(map[string]int),
		grpcClientRPCFailuresTotal: make(map[string]int),
		grpcClientRPCLatencyHist:   make(map[string][]float64),
	}
}
func (mm *mockMetrics) IncCounter(name string, labels ...string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	key := name
	for i := 0; i < len(labels); i += 2 {
		key += fmt.Sprintf(",%s=%s", labels[i], labels[i+1])
	}
	if name == "grpc_client_rpc_success_total" {
		mm.grpcClientRPCSuccessTotal[key]++
	} else if name == "grpc_client_rpc_failures_total" {
		mm.grpcClientRPCFailuresTotal[key]++
	}
}
func (mm *mockMetrics) AddCounter(name string, value float64, labels ...string) {}
func (mm *mockMetrics) SetGauge(name string, value float64, labels ...string)   {}
func (mm *mockMetrics) ObserveHistogram(name string, value float64, labels ...string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	key := name
	for i := 0; i < len(labels); i += 2 {
		key += fmt.Sprintf(",%s=%s", labels[i], labels[i+1])
	}
	if name == "grpc_client_rpc_latency_seconds" {
		mm.grpcClientRPCLatencyHist[key] = append(mm.grpcClientRPCLatencyHist[key], value)
	}
}
func (mm *mockMetrics) ObserveCommitIndex(index types.Index)                        {}
func (mm *mockMetrics) ObserveAppliedIndex(index types.Index)                       {}
func (mm *mockMetrics) ObserveTerm(term types.Term)                                 {}
func (mm *mockMetrics) ObserveLeaderChange(newLeader types.NodeID, term types.Term) {}
func (mm *mockMetrics) ObserveLeaderNotificationDropped()                           {}
func (mm *mockMetrics) ObserveLeadershipLost(term types.Term, reason string)        {}
func (mm *mockMetrics) ObserveApplyNotificationDropped()                            {}
func (mm *mockMetrics) ObserveApplyLoopStopped(reason string)                       {}

func (mm *mockMetrics) ObserveRoleChange(
	newRole types.NodeRole,
	oldRole types.NodeRole,
	term types.Term,
) {
}
func (mm *mockMetrics) ObserveElectionStart(term types.Term, reason ElectionReason) {}
func (mm *mockMetrics) ObserveVoteGranted(term types.Term)                          {}
func (mm *mockMetrics) ObserveLogState(firstIndex, lastIndex types.Index, lastTerm types.Term) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.logStateCount++
}
func (mm *mockMetrics) ObserveLogAppend(entryCount int, latency time.Duration, success bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.logAppendCount++
}
func (mm *mockMetrics) ObserveLogRead(readType LogReadType, latency time.Duration, success bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.logReadCount++
}

func (mm *mockMetrics) ObserveLogTruncate(
	truncateType LogTruncateType,
	entriesRemoved int,
	latency time.Duration,
	success bool,
) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.logTruncateCount++
	mm.lastTruncateType = truncateType
	mm.lastEntriesRemoved = entriesRemoved
	mm.lastTruncateLatency = latency
	mm.lastTruncateSuccess = success
}
func (mm *mockMetrics) ObserveLogConsistencyError() {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.logConsistencyErr++
}
func (mm *mockMetrics) ObserveElectionElapsed(nodeID types.NodeID, term types.Term, ticks int) {}
func (mm *mockMetrics) ObserveProposal(success bool, reason ProposalResult)                    {}
func (mm *mockMetrics) ObserveReadIndex(success bool, path string)                             {}

func (mm *mockMetrics) ObserveSnapshot(
	action SnapshotAction,
	status SnapshotStatus,
	labels ...string,
) {
}
func (mm *mockMetrics) ObserveSnapshotRecovery(status SnapshotStatus, reason SnapshotReason) {}

func (mm *mockMetrics) ObservePeerReplication(
	peerID types.NodeID,
	success bool,
	reason ReplicationResult,
) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.peerReplicationCalls = append(mm.peerReplicationCalls, struct {
		peerID  types.NodeID
		success bool
		reason  ReplicationResult
	}{peerID, success, reason})
}

func (mm *mockMetrics) ObserveHeartbeat(peerID types.NodeID, success bool, latency time.Duration) {}
func (mm *mockMetrics) ObserveHeartbeatSent()                                                     {}
func (mm *mockMetrics) ObserveAppendEntriesHeartbeat()                                            {}
func (mm *mockMetrics) ObserveAppendEntriesReplication()                                          {}
func (mm *mockMetrics) ObserveAppendEntriesRejected(reason string)                                {}
func (mm *mockMetrics) ObserveEntriesReceived(count int)                                          {}
func (mm *mockMetrics) ObserveCommandBytesReceived(bytes int)                                     {}
func (mm *mockMetrics) ObserveTick(role types.NodeRole)                                           {}
func (mm *mockMetrics) ObserveComponentStopTimeout(component string)                              {}
func (mm *mockMetrics) ObserveCommitCheckTriggered()                                              {}
func (mm *mockMetrics) ObserveCommitCheckPending()                                                {}

func (mm *mockMetrics) getLastPeerReplicationCall() (peerID types.NodeID, success bool, reason ReplicationResult, found bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if len(mm.peerReplicationCalls) == 0 {
		return "", false, "", false
	}
	lastCall := mm.peerReplicationCalls[len(mm.peerReplicationCalls)-1]
	return lastCall.peerID, lastCall.success, lastCall.reason, true
}

func (mm *mockMetrics) resetPeerReplicationCalls() {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.peerReplicationCalls = make([]struct {
		peerID  types.NodeID
		success bool
		reason  ReplicationResult
	}, 0)
}

type mockLeaderInitializer struct {
	initializeStateCalled bool
	sendHeartbeatsCalled  bool

	initializeLeaderStateFunc func()
	sendHeartbeatsFunc        func(ctx context.Context)
}

func (m *mockLeaderInitializer) InitializeLeaderState() {
	m.initializeStateCalled = true

	if m.initializeLeaderStateFunc != nil {
		m.initializeLeaderStateFunc()
		return
	}
}

func (m *mockLeaderInitializer) SendHeartbeats(ctx context.Context) {
	m.sendHeartbeatsCalled = true

	if m.sendHeartbeatsFunc != nil {
		m.sendHeartbeatsFunc(ctx)
		return
	}
}

type mockStateManager struct {
	mu           sync.Mutex
	currentTerm  types.Term
	currentRole  types.NodeRole
	leaderID     types.NodeID
	votedFor     types.NodeID
	commitIndex  types.Index
	lastApplied  types.Index
	becomeLeader bool // controls mock behavior for BecomeLeader

	lastAppliedUpdated bool
	commitIndexUpdated bool

	initializeFunc              func(ctx context.Context) error
	getStateFunc                func() (types.Term, types.NodeRole, types.NodeID)
	getStateUnsafeFunc          func() (types.Term, types.NodeRole, types.NodeID)
	checkTermAndStepDownFunc    func(ctx context.Context, rpcTerm types.Term, rpcLeader types.NodeID) (steppedDown bool, previousTerm types.Term)
	becomeFollowerFunc          func(ctx context.Context, term types.Term, leaderID types.NodeID)
	becomeLeaderFunc            func(ctx context.Context) bool
	becomeCandidateFunc         func(ctx context.Context, reason ElectionReason) bool
	grantVoteFunc               func(ctx context.Context, candidateID types.NodeID, term types.Term) bool
	updateCommitIndexFunc       func(newCommitIndex types.Index) bool
	updateCommitIndexUnsafeFunc func(newCommitIndex types.Index) bool
	updateLastAppliedFunc       func(newLastApplied types.Index) bool
	getLastAppliedFunc          func() types.Index
}

func newMockStateManager() *mockStateManager {
	return &mockStateManager{
		currentTerm:  1,
		currentRole:  types.RoleFollower,
		leaderID:     "node1",
		commitIndex:  0,
		lastApplied:  0,
		becomeLeader: false,
	}
}

func (m *mockStateManager) Initialize(ctx context.Context) error {
	if m.initializeFunc != nil {
		return m.initializeFunc(ctx)
	}
	return nil
}
func (m *mockStateManager) GetState() (types.Term, types.NodeRole, types.NodeID) {
	if m.getStateFunc != nil {
		return m.getStateFunc()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.GetStateUnsafe()
}
func (m *mockStateManager) GetStateUnsafe() (types.Term, types.NodeRole, types.NodeID) {
	if m.getStateUnsafeFunc != nil {
		return m.getStateUnsafeFunc()
	}
	return m.currentTerm, m.currentRole, m.leaderID
}

func (m *mockStateManager) GetLastKnownLeader() types.NodeID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.leaderID
}
func (m *mockStateManager) BecomeCandidate(ctx context.Context, reason ElectionReason) bool {
	if m.becomeCandidateFunc != nil {
		return m.becomeCandidateFunc(ctx, reason)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTerm++
	m.currentRole = types.RoleCandidate
	m.votedFor = "node1" // Self ID hardcoded for mock simplicity
	m.leaderID = ""      // Candidate doesn't know leader
	return true
}
func (m *mockStateManager) BecomeLeader(ctx context.Context) bool {
	if m.becomeLeaderFunc != nil {
		return m.becomeLeaderFunc(ctx)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentRole != types.RoleCandidate {
		return false
	}
	if !m.becomeLeader {
		return false
	}
	m.currentRole = types.RoleLeader
	m.leaderID = "node1" // Self ID
	return true
}

func (m *mockStateManager) BecomeFollower(
	ctx context.Context,
	term types.Term,
	leaderID types.NodeID,
) {
	if m.becomeFollowerFunc != nil {
		m.becomeFollowerFunc(ctx, term, leaderID)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if term > m.currentTerm {
		m.currentTerm = term
		m.votedFor = "" // Reset vote when term increases
	} else if term < m.currentTerm {
		return // Ignore stale calls
	}
	m.currentRole = types.RoleFollower
	m.leaderID = leaderID
}

func (m *mockStateManager) CheckTermAndStepDown(
	ctx context.Context,
	rpcTerm types.Term,
	rpcLeader types.NodeID,
) (bool, types.Term) {
	if m.checkTermAndStepDownFunc != nil {
		return m.checkTermAndStepDownFunc(ctx, rpcTerm, rpcLeader)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	prevTerm := m.currentTerm
	steppedDown := false
	if rpcTerm > m.currentTerm {
		m.currentTerm = rpcTerm
		m.currentRole = types.RoleFollower
		m.leaderID = rpcLeader
		m.votedFor = "" // Reset vote on term change
		steppedDown = true
	} else if rpcTerm == m.currentTerm {
		if m.currentRole != types.RoleFollower {
			m.currentRole = types.RoleFollower
			m.leaderID = rpcLeader // Update leader hint
			steppedDown = true
		} else if m.leaderID != rpcLeader && rpcLeader != "" {
			m.leaderID = rpcLeader // Update leader hint if already follower
		}
	}
	return steppedDown, prevTerm
}

func (m *mockStateManager) GrantVote(
	ctx context.Context,
	candidateID types.NodeID,
	term types.Term,
) bool {
	if m.grantVoteFunc != nil {
		return m.grantVoteFunc(ctx, candidateID, term)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if term < m.currentTerm {
		return false // Stale term
	}
	if term > m.currentTerm { // New term
		m.currentTerm = term
		m.votedFor = ""                    // Reset vote for new term
		m.currentRole = types.RoleFollower // Higher term means step down
		m.leaderID = ""
	}

	if m.votedFor == "" || m.votedFor == candidateID {
		m.votedFor = candidateID
		return true
	}
	return false
}
func (m *mockStateManager) UpdateCommitIndex(newCommitIndex types.Index) bool {
	if m.updateCommitIndexFunc != nil {
		return m.updateCommitIndexFunc(newCommitIndex)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.UpdateCommitIndexUnsafe(newCommitIndex)
}
func (m *mockStateManager) UpdateCommitIndexUnsafe(newCommitIndex types.Index) bool {
	if m.updateCommitIndexUnsafeFunc != nil {
		return m.updateCommitIndexUnsafeFunc(newCommitIndex)
	}
	if newCommitIndex > m.commitIndex {
		m.commitIndex = newCommitIndex
		m.commitIndexUpdated = true
		return true
	}
	return false
}
func (m *mockStateManager) UpdateLastApplied(newLastApplied types.Index) bool {
	if m.updateLastAppliedFunc != nil {
		return m.updateLastAppliedFunc(newLastApplied)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if newLastApplied > m.lastApplied && newLastApplied <= m.commitIndex {
		m.lastApplied = newLastApplied
		m.lastAppliedUpdated = true
		return true
	}
	return false
}
func (m *mockStateManager) GetCommitIndex() types.Index {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitIndex
}
func (m *mockStateManager) GetCommitIndexUnsafe() types.Index {
	return m.commitIndex
}
func (m *mockStateManager) GetLastApplied() types.Index {
	if m.getLastAppliedFunc != nil {
		return m.getLastAppliedFunc()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastApplied
}
func (m *mockStateManager) GetLastAppliedUnsafe() types.Index {
	return m.lastApplied
}
func (m *mockStateManager) Stop() {}

type mockLogManager struct {
	lastIndex  types.Index
	lastTerm   types.Term
	firstIndex types.Index
	entries    map[types.Index]types.LogEntry
	mu         sync.Mutex

	// Function overrides for testing
	initializeFunc                  func(ctx context.Context) error
	getTermFunc                     func(ctx context.Context, index types.Index) (types.Term, error)
	getTermUnsafeFunc               func(ctx context.Context, index types.Index) (types.Term, error)
	getEntriesFunc                  func(ctx context.Context, start, end types.Index) ([]types.LogEntry, error)
	getEntriesUnsafeFunc            func(ctx context.Context, start, end types.Index) ([]types.LogEntry, error)
	appendEntriesFunc               func(ctx context.Context, entries []types.LogEntry) error
	appendEntriesUnsafeFunc         func(ctx context.Context, entries []types.LogEntry) error
	truncatePrefixFunc              func(ctx context.Context, newFirstIndex types.Index) error
	truncatePrefixUnsafeFunc        func(ctx context.Context, newFirstIndex types.Index) error
	truncateSuffixFunc              func(ctx context.Context, newLastIndexPlusOne types.Index) error
	truncateSuffixUnsafeFunc        func(ctx context.Context, newLastIndexPlusOne types.Index) error
	findFirstIndexInTermUnsafeFunc  func(ctx context.Context, term types.Term, searchUpToIndex types.Index) (types.Index, error)
	findLastEntryWithTermUnsafeFunc func(ctx context.Context, term types.Term, searchFromHint types.Index) (types.Index, error)
	restoreFromSnapshotFunc         func(ctx context.Context, meta types.SnapshotMetadata) error
	restoreFromSnapshotUnsafeFunc   func(ctx context.Context, meta types.SnapshotMetadata) error
}

func newMockLogManager() *mockLogManager {
	return &mockLogManager{
		lastIndex:  0,
		lastTerm:   0,
		firstIndex: 1, // Default to 1 for non-empty log after initialization
		entries:    make(map[types.Index]types.LogEntry),
	}
}

func (m *mockLogManager) Initialize(ctx context.Context) error {
	if m.initializeFunc != nil {
		return m.initializeFunc(ctx)
	}
	return nil
}

func (m *mockLogManager) GetLastIndexUnsafe() types.Index {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIndex
}

func (m *mockLogManager) GetLastTermUnsafe() types.Term {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastTerm
}

func (m *mockLogManager) GetConsistentLastState() (types.Index, types.Term) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIndex, m.lastTerm
}

func (m *mockLogManager) GetFirstIndex() types.Index {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 && m.lastIndex == 0 { // Truly empty
		return 0
	}
	return m.firstIndex
}

func (m *mockLogManager) GetFirstIndexUnsafe() types.Index {
	// Mock doesn't need separate unsafe implementation since it's already simple
	return m.GetFirstIndex()
}

func (m *mockLogManager) GetTerm(ctx context.Context, index types.Index) (types.Term, error) {
	if m.getTermFunc != nil {
		return m.getTermFunc(ctx, index)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getTermUnsafeLocked(index)
}

func (m *mockLogManager) GetTermUnsafe(ctx context.Context, index types.Index) (types.Term, error) {
	if m.getTermUnsafeFunc != nil {
		return m.getTermUnsafeFunc(ctx, index)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getTermUnsafeLocked(index)
}

func (m *mockLogManager) getTermUnsafeLocked(index types.Index) (types.Term, error) {
	if index == 0 {
		return 0, nil
	}
	firstIdx := m.firstIndex
	if len(m.entries) == 0 && m.lastIndex == 0 {
		firstIdx = 0
	}

	if index < firstIdx {
		return 0, ErrCompacted
	}
	if index > m.lastIndex {
		return 0, ErrNotFound
	}
	entry, ok := m.entries[index]
	if !ok {
		return 0, ErrNotFound
	}
	return entry.Term, nil
}

func (m *mockLogManager) GetEntries(
	ctx context.Context,
	start, end types.Index,
) ([]types.LogEntry, error) {
	if m.getEntriesFunc != nil {
		return m.getEntriesFunc(ctx, start, end)
	}
	return m.getEntriesInternal(start, end)
}

func (m *mockLogManager) GetEntriesUnsafe(
	ctx context.Context,
	start, end types.Index,
) ([]types.LogEntry, error) {
	if m.getEntriesUnsafeFunc != nil {
		return m.getEntriesUnsafeFunc(ctx, start, end)
	}
	return m.getEntriesInternal(start, end)
}

func (m *mockLogManager) getEntriesInternal(start, end types.Index) ([]types.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	firstIdx := m.firstIndex
	if len(m.entries) == 0 && m.lastIndex == 0 {
		firstIdx = 0
	}

	if start < firstIdx {
		return nil, ErrCompacted
	}
	if start > m.lastIndex || start >= end {
		return nil, nil
	}

	entries := make([]types.LogEntry, 0, end-start)
	for i := start; i < end && i <= m.lastIndex; i++ {
		entry, ok := m.entries[i]
		if !ok {
			return nil, ErrNotFound
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (m *mockLogManager) AppendEntries(ctx context.Context, entries []types.LogEntry) error {
	if m.appendEntriesFunc != nil {
		err := m.appendEntriesFunc(ctx, entries)
		if err == nil && len(entries) > 0 {
			m.updateEntriesAfterAppend(entries)
		}
		return err
	}
	return m.appendEntriesInternal(entries)
}

func (m *mockLogManager) AppendEntriesUnsafe(ctx context.Context, entries []types.LogEntry) error {
	if m.appendEntriesUnsafeFunc != nil {
		err := m.appendEntriesUnsafeFunc(ctx, entries)
		if err == nil && len(entries) > 0 {
			m.updateEntriesAfterAppend(entries)
		}
		return err
	}
	return m.appendEntriesInternal(entries)
}

func (m *mockLogManager) updateEntriesAfterAppend(entries []types.LogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range entries {
		m.entries[entry.Index] = entry
		if entry.Index > m.lastIndex {
			m.lastIndex = entry.Index
			m.lastTerm = entry.Term
		}
		if m.firstIndex == 0 && entry.Index > 0 {
			m.firstIndex = entry.Index
		} else if entry.Index < m.firstIndex {
			m.firstIndex = entry.Index
		}
	}
}

func (m *mockLogManager) appendEntriesInternal(entries []types.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	if m.lastIndex > 0 && entries[0].Index != m.lastIndex+1 {
		return storage.ErrNonContiguousEntries
	}

	for _, entry := range entries {
		m.entries[entry.Index] = entry
		if entry.Index > m.lastIndex {
			m.lastIndex = entry.Index
			m.lastTerm = entry.Term
		}
		if (m.firstIndex == 0 || m.firstIndex == 1 && len(m.entries) == 0) && entry.Index > 0 {
			m.firstIndex = entry.Index
		} else if entry.Index < m.firstIndex && entry.Index > 0 {
			m.firstIndex = entry.Index
		}
	}
	if m.firstIndex == 0 && m.lastIndex > 0 && len(m.entries) > 0 {
		m.firstIndex = m.entries[m.lastIndex].Index
		for idx := range m.entries {
			if idx < m.firstIndex {
				m.firstIndex = idx
			}
		}
	}

	return nil
}

func (m *mockLogManager) TruncatePrefix(ctx context.Context, newFirstIndex types.Index) error {
	if m.truncatePrefixFunc != nil {
		return m.truncatePrefixFunc(ctx, newFirstIndex)
	}
	return m.truncatePrefixInternal(newFirstIndex)
}

func (m *mockLogManager) TruncatePrefixUnsafe(
	ctx context.Context,
	newFirstIndex types.Index,
) error {
	if m.truncatePrefixUnsafeFunc != nil {
		return m.truncatePrefixUnsafeFunc(ctx, newFirstIndex)
	}
	return m.truncatePrefixInternal(newFirstIndex)
}

func (m *mockLogManager) truncatePrefixInternal(newFirstIndex types.Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if newFirstIndex <= m.firstIndex {
		return nil
	}
	if newFirstIndex > m.lastIndex+1 {
		return fmt.Errorf(
			"truncate prefix %d beyond last log index %d+1",
			newFirstIndex,
			m.lastIndex,
		)
	}

	for i := m.firstIndex; i < newFirstIndex; i++ {
		delete(m.entries, i)
	}
	m.firstIndex = newFirstIndex
	if m.firstIndex > m.lastIndex && m.lastIndex != 0 {
		m.lastIndex = 0
		m.lastTerm = 0
	} else if len(m.entries) == 0 {
		m.lastIndex = 0
		m.lastTerm = 0
		m.firstIndex = 0
	}

	return nil
}

func (m *mockLogManager) TruncateSuffix(
	ctx context.Context,
	newLastIndexPlusOne types.Index,
) error {
	if m.truncateSuffixFunc != nil {
		return m.truncateSuffixFunc(ctx, newLastIndexPlusOne)
	}
	return m.truncateSuffixInternal(newLastIndexPlusOne)
}

func (m *mockLogManager) TruncateSuffixUnsafe(
	ctx context.Context,
	newLastIndexPlusOne types.Index,
) error {
	if m.truncateSuffixUnsafeFunc != nil {
		return m.truncateSuffixUnsafeFunc(ctx, newLastIndexPlusOne)
	}
	return m.truncateSuffixInternal(newLastIndexPlusOne)
}

func (m *mockLogManager) truncateSuffixInternal(newLastIndexPlusOne types.Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if newLastIndexPlusOne > m.lastIndex+1 {
		return nil
	}
	if newLastIndexPlusOne <= m.firstIndex {
		m.entries = make(map[types.Index]types.LogEntry)
		m.lastIndex = 0
		m.lastTerm = 0
		m.firstIndex = 0
		return nil
	}

	newLastIndex := newLastIndexPlusOne - 1

	for i := newLastIndexPlusOne; i <= m.lastIndex; i++ {
		delete(m.entries, i)
	}

	m.lastIndex = newLastIndex
	if newLastIndex == 0 {
		m.lastTerm = 0
		m.firstIndex = 0
	} else if entry, ok := m.entries[newLastIndex]; ok {
		m.lastTerm = entry.Term
	} else {
		var maxFoundIdx types.Index = 0
		for idx := range m.entries {
			if idx <= newLastIndex && idx > maxFoundIdx {
				maxFoundIdx = idx
			}
		}
		if maxFoundIdx > 0 {
			m.lastTerm = m.entries[maxFoundIdx].Term
			m.lastIndex = maxFoundIdx
		} else {
			m.lastTerm = 0
			if len(m.entries) == 0 {
				m.firstIndex = 0
			}
		}
	}

	return nil
}

func (m *mockLogManager) IsConsistentWithStorage(ctx context.Context) (bool, error) {
	return true, nil
}

func (m *mockLogManager) RebuildInMemoryState(ctx context.Context) error {
	return nil
}

func (m *mockLogManager) FindLastEntryWithTermUnsafe(
	ctx context.Context,
	term types.Term,
	searchFromHint types.Index,
) (types.Index, error) {
	if m.findLastEntryWithTermUnsafeFunc != nil {
		return m.findLastEntryWithTermUnsafeFunc(ctx, term, searchFromHint)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	startIdx := m.lastIndex
	if searchFromHint > 0 && searchFromHint < startIdx {
		startIdx = searchFromHint
	}

	for i := startIdx; i >= m.firstIndex; i-- {
		entry, ok := m.entries[i]
		if !ok {
			continue
		}
		if entry.Term == term {
			return i, nil
		}
		if entry.Term < term {
			break
		}
	}
	return 0, ErrNotFound
}

func (m *mockLogManager) FindFirstIndexInTermUnsafe(
	ctx context.Context,
	term types.Term,
	searchUpToIndex types.Index,
) (types.Index, error) {
	if m.findFirstIndexInTermUnsafeFunc != nil {
		return m.findFirstIndexInTermUnsafeFunc(ctx, term, searchUpToIndex)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	firstIndexOfTerm := types.Index(0)
	for i := searchUpToIndex; i >= m.firstIndex; i-- {
		entry, ok := m.entries[i]
		if !ok {
			continue
		}
		if entry.Term == term {
			firstIndexOfTerm = i
		} else if entry.Term < term && firstIndexOfTerm != 0 {
			return firstIndexOfTerm, nil
		} else if entry.Term < term {
			return 0, ErrNotFound
		}
	}

	if firstIndexOfTerm != 0 {
		return firstIndexOfTerm, nil
	}
	return 0, ErrNotFound
}

// NEW: GetLogStateForDebugging
func (m *mockLogManager) GetLogStateForDebugging() LogDebugState {
	m.mu.Lock()
	defer m.mu.Unlock()

	logSize := 0
	if m.lastIndex >= m.firstIndex && m.lastIndex > 0 {
		logSize = int(m.lastIndex - m.firstIndex + 1)
	}

	return LogDebugState{
		NodeID:          types.NodeID("mock"),
		FirstIndex:      m.firstIndex,
		LastIndex:       m.lastIndex,
		LastTerm:        m.lastTerm,
		CachedLastIndex: m.lastIndex,
		CachedLastTerm:  m.lastTerm,
		LogSize:         logSize,
		IsEmpty:         m.lastIndex == 0,
		IsConsistent:    true, // Mock is always consistent
		StorageMetrics:  nil,
		LastChecked:     time.Now(),
	}
}

func (m *mockLogManager) RestoreFromSnapshot(
	ctx context.Context,
	meta types.SnapshotMetadata,
) error {
	if m.restoreFromSnapshotFunc != nil {
		return m.restoreFromSnapshotFunc(ctx, meta)
	}
	return m.restoreFromSnapshotInternal(meta)
}

func (m *mockLogManager) RestoreFromSnapshotUnsafe(
	ctx context.Context,
	meta types.SnapshotMetadata,
) error {
	if m.restoreFromSnapshotUnsafeFunc != nil {
		return m.restoreFromSnapshotUnsafeFunc(ctx, meta)
	}
	return m.restoreFromSnapshotInternal(meta)
}

func (m *mockLogManager) restoreFromSnapshotInternal(meta types.SnapshotMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update cached state to reflect the snapshot
	m.lastIndex = meta.LastIncludedIndex
	m.lastTerm = meta.LastIncludedTerm

	// The snapshot manager should have already truncated the log,
	// so we just update our state to be consistent
	return nil
}

func (m *mockLogManager) Stop() {
	// No-op for mock
}

func (m *mockLogManager) SetState(firstIndex, lastIndex types.Index, lastTerm types.Term) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.firstIndex = firstIndex
	m.lastIndex = lastIndex
	m.lastTerm = lastTerm
}

func (m *mockLogManager) AddEntry(index types.Index, term types.Term, command []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := types.LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	m.entries[index] = entry

	if index > m.lastIndex {
		m.lastIndex = index
		m.lastTerm = term
	}

	if m.firstIndex == 0 || index < m.firstIndex {
		m.firstIndex = index
	}
}

func (m *mockLogManager) GetEntryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

func (m *mockLogManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make(map[types.Index]types.LogEntry)
	m.firstIndex = 0
	m.lastIndex = 0
	m.lastTerm = 0
}

type mockSnapshotManager struct {
	snapshots map[types.NodeID]bool
	mu        sync.Mutex
	meta      types.SnapshotMetadata

	handleInstallSnapshotResult *types.InstallSnapshotReply
	handleInstallSnapshotErr    error

	initializeFunc   func(ctx context.Context) error
	tickFunc         func(ctx context.Context)
	sendSnapshotFunc func(ctx context.Context, targetID types.NodeID, term types.Term)
}

func newMockSnapshotManager() *mockSnapshotManager {
	return &mockSnapshotManager{
		snapshots: make(map[types.NodeID]bool),
	}
}

func (m *mockSnapshotManager) SetReplicationStateUpdater(updater ReplicationStateUpdater) {}

func (m *mockSnapshotManager) SetNetworkManager(nm NetworkManager) {}

func (m *mockSnapshotManager) Initialize(ctx context.Context) error {
	if m.initializeFunc != nil {
		return m.initializeFunc(ctx)
	}

	return nil
}

func (m *mockSnapshotManager) Tick(ctx context.Context) {
	if m.tickFunc != nil {
		m.tickFunc(ctx)
		return
	}
}

func (m *mockSnapshotManager) HandleInstallSnapshot(
	ctx context.Context,
	args *types.InstallSnapshotArgs,
) (*types.InstallSnapshotReply, error) {
	if m.handleInstallSnapshotErr != nil {
		return nil, m.handleInstallSnapshotErr
	}

	if m.handleInstallSnapshotResult != nil {
		return m.handleInstallSnapshotResult, nil
	}

	m.mu.Lock()
	m.meta = types.SnapshotMetadata{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
	}
	m.mu.Unlock()
	return &types.InstallSnapshotReply{Term: args.Term}, nil
}

func (m *mockSnapshotManager) SendSnapshot(
	ctx context.Context,
	targetID types.NodeID,
	term types.Term,
) {
	if m.sendSnapshotFunc != nil {
		m.sendSnapshotFunc(ctx, targetID, term)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots[targetID] = true
}

func (m *mockSnapshotManager) GetSnapshotMetadata() types.SnapshotMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.meta
}

func (m *mockSnapshotManager) GetSnapshotMetadataUnsafe() types.SnapshotMetadata {
	return m.meta
}
func (m *mockSnapshotManager) Stop() {}

type mockReplicationStateUpdater struct {
	peerUpdatedAfterSnapshot bool
	snapshotProgressUpdated  bool
	advanceCommitIndexCalled bool
	lastPeerID               types.NodeID
	lastSnapshotIndex        types.Index
	lastInProgress           bool
}

func (m *mockReplicationStateUpdater) UpdatePeerAfterSnapshotSend(
	peerID types.NodeID,
	snapshotIndex types.Index,
) {
	m.peerUpdatedAfterSnapshot = true
	m.lastPeerID = peerID
	m.lastSnapshotIndex = snapshotIndex
}

func (m *mockReplicationStateUpdater) SetPeerSnapshotInProgress(
	peerID types.NodeID,
	inProgress bool,
) {
	m.snapshotProgressUpdated = true
	m.lastPeerID = peerID
	m.lastInProgress = inProgress
}

func (m *mockReplicationStateUpdater) MaybeAdvanceCommitIndex() {
	m.advanceCommitIndexCalled = true
}

type mockRPCHandler struct {
	mu sync.Mutex

	requestVoteCalls     int
	appendEntriesCalls   int
	installSnapshotCalls int

	requestVoteFunc     func(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteReply, error)
	appendEntriesFunc   func(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error)
	installSnapshotFunc func(ctx context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error)

	requestVoteErr     error
	appendEntriesErr   error
	installSnapshotErr error
}

func (m *mockRPCHandler) RequestVote(
	ctx context.Context,
	args *types.RequestVoteArgs,
) (*types.RequestVoteReply, error) {
	m.mu.Lock()
	m.requestVoteCalls++
	errToRet := m.requestVoteErr
	m.mu.Unlock()

	if errToRet != nil {
		return nil, errToRet
	}
	if m.requestVoteFunc != nil {
		return m.requestVoteFunc(ctx, args)
	}
	return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
}

func (m *mockRPCHandler) AppendEntries(
	ctx context.Context,
	args *types.AppendEntriesArgs,
) (*types.AppendEntriesReply, error) {
	m.mu.Lock()
	m.appendEntriesCalls++
	errToRet := m.appendEntriesErr
	m.mu.Unlock()

	if errToRet != nil {
		return nil, errToRet
	}
	if m.appendEntriesFunc != nil {
		return m.appendEntriesFunc(ctx, args)
	}
	return &types.AppendEntriesReply{Term: args.Term, Success: true}, nil
}

func (m *mockRPCHandler) InstallSnapshot(
	ctx context.Context,
	args *types.InstallSnapshotArgs,
) (*types.InstallSnapshotReply, error) {
	m.mu.Lock()
	m.installSnapshotCalls++
	errToRet := m.installSnapshotErr
	m.mu.Unlock()

	if errToRet != nil {
		return nil, errToRet
	}
	if m.installSnapshotFunc != nil {
		return m.installSnapshotFunc(ctx, args)
	}
	return &types.InstallSnapshotReply{Term: args.Term}, nil
}

func (m *mockRPCHandler) SetRequestVoteError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestVoteErr = err
}

func (m *mockRPCHandler) SetAppendEntriesError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendEntriesErr = err
}

func (m *mockRPCHandler) SetInstallSnapshotError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.installSnapshotErr = err
}

func (m *mockRPCHandler) ResetErrors() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestVoteErr = nil
	m.appendEntriesErr = nil
	m.installSnapshotErr = nil
}

func (m *mockRPCHandler) ResetCallCounts() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestVoteCalls = 0
	m.appendEntriesCalls = 0
	m.installSnapshotCalls = 0
}

type mockElectionManager struct {
	mu                      sync.Mutex
	initializeErr           error
	tickCalled              int
	resetTimerCalled        int
	handleRequestVoteResult *types.RequestVoteReply
	handleRequestVoteErr    error

	initializeFunc func(ctx context.Context) error
	tickFunc       func(ctx context.Context)
}

func (m *mockElectionManager) SetNetworkManager(nm NetworkManager) {}

func (m *mockElectionManager) Initialize(ctx context.Context) error {
	if m.initializeFunc != nil {
		return m.initializeFunc(ctx)
	}

	return m.initializeErr
}

func (m *mockElectionManager) Tick(ctx context.Context) {
	if m.tickFunc != nil {
		m.tickFunc(ctx)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.tickCalled++
}

func (m *mockElectionManager) ResetTimerOnHeartbeat() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetTimerCalled++
}

func (m *mockElectionManager) HandleRequestVote(
	ctx context.Context,
	args *types.RequestVoteArgs,
) (*types.RequestVoteReply, error) {
	if m.handleRequestVoteErr != nil {
		return nil, m.handleRequestVoteErr
	}
	if m.handleRequestVoteResult != nil {
		return m.handleRequestVoteResult, nil
	}
	return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
}

func (m *mockElectionManager) Stop() {}

type mockReplicationManager struct {
	mu                        sync.Mutex
	tickCalled                int
	proposeResult             types.Index
	proposeTerm               types.Term
	proposeIsLeader           bool
	proposeErr                error
	verifyLeadershipResult    types.Index
	verifyLeadershipErr       error
	handleAppendEntriesResult *types.AppendEntriesReply
	handleAppendEntriesErr    error
	replicationStatus         map[types.NodeID]types.PeerState
	initializeLeaderCalled    int
	sendHeartbeatsCalled      int

	tickFunc                              func(ctx context.Context)
	proposeFunc                           func(ctx context.Context, command []byte) (types.Index, types.Term, bool, error)
	verifyLeadershipAndGetCommitIndexFunc func(ctx context.Context) (types.Index, error)
}

func (m *mockReplicationManager) SetNetworkManager(nm NetworkManager) {}

func (m *mockReplicationManager) Tick(ctx context.Context) {
	if m.tickFunc != nil {
		m.tickFunc(ctx)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.tickCalled++
}

func (m *mockReplicationManager) InitializeLeaderState() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initializeLeaderCalled++
}

func (m *mockReplicationManager) Propose(
	ctx context.Context,
	command []byte,
) (types.Index, types.Term, bool, error) {
	if m.proposeFunc != nil {
		return m.proposeFunc(ctx, command)
	}
	if m.proposeErr != nil {
		return 0, 0, m.proposeIsLeader, m.proposeErr
	}
	return m.proposeResult, m.proposeTerm, m.proposeIsLeader, nil
}

func (m *mockReplicationManager) SendHeartbeats(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendHeartbeatsCalled++
}

func (m *mockReplicationManager) MaybeAdvanceCommitIndex() {}

func (m *mockReplicationManager) HasValidLease(ctx context.Context) bool {
	return true
}

func (m *mockReplicationManager) VerifyLeadershipAndGetCommitIndex(
	ctx context.Context,
) (types.Index, error) {
	if m.verifyLeadershipAndGetCommitIndexFunc != nil {
		return m.verifyLeadershipAndGetCommitIndexFunc(ctx)
	}

	if m.verifyLeadershipErr != nil {
		return 0, m.verifyLeadershipErr
	}
	return m.verifyLeadershipResult, nil
}

func (m *mockReplicationManager) HandleAppendEntries(
	ctx context.Context,
	args *types.AppendEntriesArgs,
) (*types.AppendEntriesReply, error) {
	if m.handleAppendEntriesErr != nil {
		return nil, m.handleAppendEntriesErr
	}
	if m.handleAppendEntriesResult != nil {
		return m.handleAppendEntriesResult, nil
	}
	return &types.AppendEntriesReply{Term: args.Term, Success: true}, nil
}

func (m *mockReplicationManager) GetPeerReplicationStatusUnsafe() map[types.NodeID]types.PeerState {
	return m.replicationStatus
}

func (m *mockReplicationManager) ReplicateToPeer(
	ctx context.Context,
	peerID types.NodeID,
	isHeartbeat bool,
) {
}

func (m *mockReplicationManager) UpdatePeerAfterSnapshotSend(
	peerID types.NodeID,
	snapshotIndex types.Index,
) {
}

func (m *mockReplicationManager) SetPeerSnapshotInProgress(peerID types.NodeID, inProgress bool) {}

func (m *mockReplicationManager) Stop() {}
