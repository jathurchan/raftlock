package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	hookSaveState     func(ctx context.Context, state types.PersistentState) error
	hookLoadState     func(ctx context.Context) (types.PersistentState, error)
	hookLoadSnapshot  func(ctx context.Context) (types.SnapshotMetadata, []byte, error)
	hookSaveSnapshot  func()

	hookGetLogEntry   func(index types.Index)
	hookGetLogEntries func(start, end types.Index) []types.LogEntry
	hookFirstLogIndex func() types.Index
	hookLastLogIndex  func() types.Index
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		state: types.PersistentState{
			CurrentTerm: 0,
			VotedFor:    unknownNodeID,
		},
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
	if ms.hookSaveState != nil {
		return ms.hookSaveState(ctx, state)
	}
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
	if ms.hookLoadState != nil {
		return ms.hookLoadState(ctx)
	}
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
	requestVoteErrors  map[types.NodeID]error

	callCount int

	startFunc               func() error
	sendAppendEntriesFunc   func(context.Context, types.NodeID, *types.AppendEntriesArgs) (*types.AppendEntriesReply, error)
	sendRequestVoteFunc     func(context.Context, types.NodeID, *types.RequestVoteArgs) (*types.RequestVoteReply, error)
	sendInstallSnapshotFunc func(context.Context, types.NodeID, *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error)
	appendEntriesCallCount  int
	heartbeatCallCount      int

	wg *sync.WaitGroup

	stopFunc func() error
}

// Ensure mockNetworkManager implements NetworkManager
var _ NetworkManager = &mockNetworkManager{}

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
	m.callCount++
	m.mu.RUnlock()

	if err, exists := m.requestVoteErrors[target]; exists {
		return nil, err
	}

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

func (m *mockNetworkManager) ResetConnection(ctx context.Context, peerID types.NodeID) error {
	return nil
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

// MockClock is a mock implementation of the Clock interface.
type mockClock struct {
	nowVal  time.Time
	mu      sync.RWMutex
	timers  []*mockTimer
	tickers []*mockTicker
}

// Ensure mockClock implements Clock
var _ Clock = &mockClock{}

func newMockClock() *mockClock {
	return &mockClock{
		nowVal: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (m *mockClock) Now() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nowVal
}

func (m *mockClock) Since(t time.Time) time.Duration {
	return m.Now().Sub(t)
}

// TriggerAfter advances time and fires any timers that should expire
func (m *mockClock) TriggerAfter(d time.Duration, t *testing.T) {
	m.mu.Lock()
	m.nowVal = m.nowVal.Add(d)
	currentTime := m.nowVal

	// Fire expired timers
	for _, timer := range m.timers {
		timer.checkAndSignal(currentTime)
	}

	// Fire expired tickers
	for _, ticker := range m.tickers {
		ticker.checkAndSignal(currentTime)
	}
	m.mu.Unlock()
}

// After creates a timer that must be manually triggered
func (m *mockClock) After(d time.Duration) <-chan time.Time {
	timer := m.NewTimer(d)
	return timer.Chan()
}

func (m *mockClock) NewTicker(d time.Duration) Ticker {
	m.mu.Lock()
	defer m.mu.Unlock()

	ticker := &mockTicker{
		C:        make(chan time.Time, 1),
		d:        d,
		clock:    m,
		active:   true,
		nextTick: m.nowVal.Add(d),
	}
	m.tickers = append(m.tickers, ticker)
	return ticker
}

func (m *mockClock) NewTimer(d time.Duration) Timer {
	m.mu.Lock()
	defer m.mu.Unlock()

	timer := &mockTimer{
		C:         make(chan time.Time, 1),
		mockClock: m,
		active:    true,
		d:         d,
		expiresAt: m.nowVal.Add(d),
	}
	m.timers = append(m.timers, timer)
	return timer
}

func (m *mockClock) Sleep(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nowVal = m.nowVal.Add(d)
}

func (m *mockClock) Advance(d time.Duration) {
	m.mu.Lock()
	m.nowVal = m.nowVal.Add(d)
	currentTime := m.nowVal

	// Fire expired timers
	for _, timer := range m.timers {
		timer.checkAndSignal(currentTime)
	}

	// Fire expired tickers
	for _, ticker := range m.tickers {
		ticker.checkAndSignal(currentTime)
	}
	m.mu.Unlock()
}

type mockTicker struct {
	C        chan time.Time
	d        time.Duration
	clock    *mockClock
	nextTick time.Time
	active   bool
	mu       sync.RWMutex
}

func (m *mockTicker) Chan() <-chan time.Time {
	return m.C
}

func (m *mockTicker) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active = false
}

func (m *mockTicker) Reset(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d <= 0 {
		panic("non-positive interval for mockTicker.Reset")
	}
	m.d = d
	m.nextTick = m.clock.Now().Add(d)
}

func (m *mockTicker) checkAndSignal(currentTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.active {
		return
	}

	if !currentTime.Before(m.nextTick) {
		select {
		case m.C <- m.nextTick:
			// Successfully sent
		default:
			// Channel full, skip this tick
		}
		m.nextTick = m.nextTick.Add(m.d)
	}
}

type mockTimer struct {
	C         chan time.Time
	mu        sync.RWMutex
	mockClock *mockClock
	expiresAt time.Time
	active    bool
	d         time.Duration
}

func (m *mockTimer) Chan() <-chan time.Time {
	return m.C
}

func (m *mockTimer) Stop() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active {
		m.active = false
		// Drain the channel if needed
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

	// Drain the channel
	select {
	case <-m.C:
	default:
	}

	return wasActive
}

func (m *mockTimer) checkAndSignal(currentTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active && !currentTime.Before(m.expiresAt) {
		select {
		case m.C <- m.expiresAt:
			m.active = false // One-shot timer
		default:
			// Channel full, timer already fired
		}
	}
}

type mockRand struct {
	floatVal float64
	intVal   int
}

func (m *mockRand) IntN(n int) int {
	if n <= 0 {
		panic("IntN called with non-positive n")
	}
	return m.intVal
}

func (m *mockRand) Float64() float64 {
	return m.floatVal
}

type mockMetrics struct {
	mu                sync.Mutex
	logStateCount     int
	logAppendCount    int
	logReadCount      int
	logConsistencyErr int
	logTruncateCount  int

	termObservations          []types.Term
	roleChangeObservations    []RoleChangeData
	electionStartObservations []ElectionStartData
	voteGrantedObservations   []types.Term
	commitIndexObservations   []types.Index
	appliedIndexObservations  []types.Index

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

type RoleChangeData struct {
	NewRole      types.NodeRole
	PreviousRole types.NodeRole
	Term         types.Term
}

type ElectionStartData struct {
	Term   types.Term
	Reason ElectionReason
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
	switch name {
	case "grpc_client_rpc_success_total":
		mm.grpcClientRPCSuccessTotal[key]++
	case "grpc_client_rpc_failures_total":
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
func (mm *mockMetrics) ObserveCommitIndex(index types.Index) {
	mm.commitIndexObservations = append(mm.commitIndexObservations, index)
}
func (mm *mockMetrics) ObserveAppliedIndex(index types.Index) {
	mm.appliedIndexObservations = append(mm.appliedIndexObservations, index)
}
func (mm *mockMetrics) ObserveTerm(term types.Term) {
	mm.termObservations = append(mm.termObservations, term)
}
func (mm *mockMetrics) ObserveLeaderChange(newLeader types.NodeID, term types.Term) {}
func (mm *mockMetrics) ObserveLeaderNotificationDropped()                           {}
func (mm *mockMetrics) ObserveLeadershipLost(term types.Term, reason string)        {}
func (mm *mockMetrics) ObserveApplyNotificationDropped()                            {}
func (mm *mockMetrics) ObserveApplyLoopStopped(reason string)                       {}

func (mm *mockMetrics) ObserveRoleChange(newRole, previousRole types.NodeRole, term types.Term) {
	mm.roleChangeObservations = append(mm.roleChangeObservations, RoleChangeData{
		NewRole:      newRole,
		PreviousRole: previousRole,
		Term:         term,
	})
}
func (mm *mockMetrics) ObserveElectionStart(term types.Term, reason ElectionReason) {
	mm.electionStartObservations = append(mm.electionStartObservations, ElectionStartData{
		Term:   term,
		Reason: reason,
	})
}
func (mm *mockMetrics) ObserveVoteGranted(term types.Term) {
	mm.voteGrantedObservations = append(mm.voteGrantedObservations, term)
}
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

type mockLeaderInitializer struct {
	initCalls      int
	heartbeatCalls int

	initializeStateCalled bool
	sendHeartbeatsCalled  bool

	initializeLeaderStateFunc func()
	sendHeartbeatsFunc        func(ctx context.Context)
}

var _ LeaderInitializer = &mockLeaderInitializer{}

func (m *mockLeaderInitializer) InitializeLeaderState() {
	m.initializeStateCalled = true

	m.initCalls++

	if m.initializeLeaderStateFunc != nil {
		m.initializeLeaderStateFunc()
		return
	}
}

func (m *mockLeaderInitializer) SendHeartbeats(ctx context.Context) {
	m.sendHeartbeatsCalled = true

	m.heartbeatCalls++

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

	becomeLeaderCalls int
	becomeResult      bool
	steppedDown       bool
	grantVoteResult   bool

	lastAppliedUpdated bool
	commitIndexUpdated bool

	initializeFunc              func(ctx context.Context) error
	getStateFunc                func() (types.Term, types.NodeRole, types.NodeID)
	getStateUnsafeFunc          func() (types.Term, types.NodeRole, types.NodeID)
	checkTermAndStepDownFunc    func(ctx context.Context, rpcTerm types.Term, rpcLeader types.NodeID) (steppedDown bool, previousTerm types.Term)
	becomeFollowerFunc          func(ctx context.Context, term types.Term, leaderID types.NodeID)
	becomeLeaderFunc            func(ctx context.Context) bool
	becomeCandidateFunc         func(ctx context.Context, reason ElectionReason) bool
	becomeCandidateForTermFunc  func(ctx context.Context, term types.Term) bool
	grantVoteFunc               func(ctx context.Context, candidateID types.NodeID, term types.Term) bool
	updateCommitIndexFunc       func(newCommitIndex types.Index) bool
	updateCommitIndexUnsafeFunc func(newCommitIndex types.Index) bool
	updateLastAppliedFunc       func(newLastApplied types.Index) bool
	getLastAppliedFunc          func() types.Index

	lastRoleChange time.Time

	metrics Metrics

	termChangeCount atomic.Uint64
}

func newMockStateManager(m Metrics) *mockStateManager {
	if m == nil {
		m = NewNoOpMetrics() // Fallback if no metrics instance is provided
	}

	sm := &mockStateManager{
		currentTerm:    1,
		currentRole:    types.RoleFollower,
		leaderID:       "node1",
		commitIndex:    0,
		lastApplied:    0,
		becomeLeader:   false,
		lastRoleChange: time.Now(),
		metrics:        m, // Initialize with a no-op metrics for safety
	}

	sm.termChangeCount.Store(0)
	return sm
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
	m.lastRoleChange = time.Now()
	return true
}

func (m *mockStateManager) BecomeCandidateForTerm(ctx context.Context, term types.Term) bool {
	if m.becomeCandidateForTermFunc != nil {
		return m.becomeCandidateForTermFunc(ctx, term)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if term <= m.currentTerm {
		return false
	}
	m.currentTerm = term
	m.currentRole = types.RoleCandidate
	m.votedFor = "node1" // Self ID hardcoded for mock simplicity
	m.leaderID = ""      // Candidate doesn't know leader
	return true
}

func (m *mockStateManager) BecomeLeaderUnsafe(ctx context.Context) bool {
	if m.becomeLeaderFunc != nil {
		return m.becomeLeaderFunc(ctx)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.becomeLeaderCalls++

	if m.becomeResult {
		m.currentRole = types.RoleLeader
		m.leaderID = "test-node"
		return m.becomeResult
	}

	if m.currentRole != types.RoleCandidate {
		return false
	}
	if !m.becomeLeader {
		return false
	}
	m.currentRole = types.RoleLeader
	m.leaderID = "node1" // Self ID
	m.lastRoleChange = time.Now()
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
	m.lastRoleChange = time.Now()
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
	if rpcTerm > m.currentTerm {
		m.currentTerm = rpcTerm
		m.currentRole = types.RoleFollower
		m.leaderID = rpcLeader
		m.votedFor = "" // Reset vote on term change
		m.steppedDown = true
		m.termChangeCount.Add(1)
		m.metrics.ObserveTerm(rpcTerm)
	} else if rpcTerm == m.currentTerm {
		if m.currentRole != types.RoleFollower {
			m.currentRole = types.RoleFollower
			m.leaderID = rpcLeader // Update leader hint
			m.steppedDown = true
		} else if m.leaderID != rpcLeader && rpcLeader != "" {
			m.leaderID = rpcLeader // Update leader hint if already follower
		}
	}
	return m.steppedDown, prevTerm
}

func (m *mockStateManager) GrantVote(
	ctx context.Context,
	candidateID types.NodeID,
	term types.Term,
) bool {
	if m.grantVoteFunc != nil {
		return m.grantVoteFunc(ctx, candidateID, term)
	}

	if m.grantVoteResult {
		m.votedFor = candidateID
		m.currentTerm = term
		return m.grantVoteResult
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// If we've already voted in this term for someone else, reject
	if m.currentTerm == term && m.votedFor != "" && m.votedFor != candidateID {
		return false
	}

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

func (m *mockStateManager) GetLeaderInfo() (currentLeader, lastKnownLeader types.NodeID, hasLeader bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.leaderID, m.leaderID, m.leaderID != ""
}

func (m *mockStateManager) GetLeaderInfoUnsafe() (currentLeader, lastKnownLeader types.NodeID, hasLeader bool) {
	return m.leaderID, m.leaderID, m.leaderID != ""
}
func (m *mockStateManager) GetVotedFor() types.NodeID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.votedFor
}
func (m *mockStateManager) GetVotedForUnsafe() types.NodeID {
	return m.votedFor
}

func (m *mockStateManager) GetTimeSinceLastRoleChange() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return time.Since(m.lastRoleChange)
}

func (m *mockStateManager) Stop() {}

// MockLogManager is a mock implementation of the LogManager interface.
type mockLogManager struct {
	lastIndex  types.Index
	lastTerm   types.Term
	firstIndex types.Index
	entries    map[types.Index]types.LogEntry
	mu         sync.Mutex

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
	getConsistentLastStateFunc      func() (types.Index, types.Term)
	getFirstIndexFunc               func() types.Index
	getFirstIndexUnsafeFunc         func() types.Index
	getLastIndexUnsafeFunc          func() types.Index
	getLastTermUnsafeFunc           func() types.Term
}

// Ensure mockLogManager implements LogManager
var _ LogManager = &mockLogManager{}

func (m *mockLogManager) Initialize(ctx context.Context) error {
	if m.initializeFunc != nil {
		return m.initializeFunc(ctx)
	}
	return nil
}

func (m *mockLogManager) GetLastIndexUnsafe() types.Index {
	if m.getLastIndexUnsafeFunc != nil {
		return m.getLastIndexUnsafeFunc()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIndex
}

func (m *mockLogManager) GetLastTermUnsafe() types.Term {
	if m.getLastTermUnsafeFunc != nil {
		return m.getLastTermUnsafeFunc()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastTerm
}

func (m *mockLogManager) GetConsistentLastState() (types.Index, types.Term) {
	if m.getConsistentLastStateFunc != nil {
		return m.getConsistentLastStateFunc()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIndex, m.lastTerm
}

func (m *mockLogManager) GetFirstIndex() types.Index {
	if m.getFirstIndexFunc != nil {
		return m.getFirstIndexFunc()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 && m.lastIndex == 0 { // Truly empty
		return 0
	}
	return m.firstIndex
}

func (m *mockLogManager) GetFirstIndexUnsafe() types.Index {
	if m.getFirstIndexUnsafeFunc != nil {
		return m.getFirstIndexUnsafeFunc()
	}
	return m.GetFirstIndex() // Mock doesn't need separate unsafe implementation since it's already simple
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
		if m.firstIndex == 0 && entry.Index > 0 {
			m.firstIndex = entry.Index
		} else if entry.Index < m.firstIndex {
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

	m.lastIndex = meta.LastIncludedIndex
	m.lastTerm = meta.LastIncludedTerm

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

// newMockLogManager creates a new mockLogManager with default empty state.
func newMockLogManager() *mockLogManager {
	return &mockLogManager{
		entries: make(map[types.Index]types.LogEntry),
	}
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

func WaitForCondition(
	t testing.TB,
	timeout, interval time.Duration,
	condition func() bool,
	description string,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Timeout waiting for condition to be met within %v: %s", timeout, description)
		}
		time.Sleep(interval) // Use real sleep for polling interval
	}
}
