package raft

import (
	"context"
	"fmt"
	"sync"
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
		return storage.ErrNonContiguousEntries
	}

	ms.log = append(ms.log, entries...)
	return nil
}

func (ms *mockStorage) GetLogEntries(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
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
	if len(ms.log) == 0 {
		return nil, storage.ErrIndexOutOfRange
	}

	firstIdx := ms.firstLogIndex
	if firstIdx == 0 && len(ms.log) > 0 {
		firstIdx = ms.log[0].Index
	}

	if start < firstIdx || start >= firstIdx+types.Index(len(ms.log)) {
		return nil, storage.ErrIndexOutOfRange
	}

	startPos := int(start - firstIdx)
	endPos := int(end - firstIdx)
	if endPos > len(ms.log) {
		endPos = len(ms.log)
	}
	return ms.log[startPos:endPos], nil
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
	if index < firstIdx || index >= firstIdx+types.Index(len(ms.log)) {
		return types.LogEntry{}, storage.ErrEntryNotFound
	}

	pos := int(index - firstIdx)
	return ms.log[pos], nil
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
		return nil
	}

	pos := int(index - firstIdx)
	if pos > len(ms.log) {
		return nil
	}
	ms.log = ms.log[:pos]
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

	pos := int(index - firstIdx)
	if pos >= len(ms.log) {
		ms.log = []types.LogEntry{}
		ms.firstLogIndex = index
		return nil
	}

	ms.log = ms.log[pos:]
	ms.firstLogIndex = index
	return nil
}

func (ms *mockStorage) SaveSnapshot(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
	if err := ms.checkFailure("SaveSnapshot"); err != nil {
		return err
	}
	return nil
}

func (ms *mockStorage) LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
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
		return 0
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
	requestVoteSuccess bool
	requestVoteReplies map[types.NodeID]*types.RequestVoteReply

	SendRequestVoteFunc func(context.Context, types.NodeID, *types.RequestVoteArgs) (*types.RequestVoteReply, error)
}

func (m *mockNetworkManager) Start() error {
	return nil
}

func (m *mockNetworkManager) Stop() error {
	return nil
}

func (m *mockNetworkManager) SendRequestVote(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
	if m.SendRequestVoteFunc != nil {
		return m.SendRequestVoteFunc(ctx, target, args)
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
	return ""
}

type mockApplier struct{}

func (m *mockApplier) Apply(ctx context.Context, index types.Index, command []byte) error {
	return nil
}

func (m *mockApplier) Snapshot(ctx context.Context) (types.Index, []byte, error) {
	return 0, nil, nil
}

func (m *mockApplier) RestoreSnapshot(ctx context.Context, lastIncludedIndex types.Index, lastIncludedTerm types.Term, snapshotData []byte) error {
	return nil
}

type mockClock struct{}

func (m *mockClock) Now() time.Time {
	return time.Time{}
}

func (m *mockClock) Since(t time.Time) time.Duration {
	return 0
}

func (m *mockClock) After(d time.Duration) <-chan time.Time {
	return nil
}

func (m *mockClock) NewTicker(d time.Duration) Ticker {
	return nil
}

func (m *mockClock) NewTimer(d time.Duration) Timer {
	return nil
}

func (m *mockClock) Sleep(d time.Duration) {
}

type mockRand struct{}

func (m *mockRand) IntN(n int) int {
	return 0
}

func (m *mockRand) Float64() float64 {
	return 0
}

type mockMetrics struct {
	logStateCount     int
	logAppendCount    int
	logReadCount      int
	logConsistencyErr int
	logTruncateCount  int

	lastTruncateType    LogTruncateType
	lastEntriesRemoved  int
	lastTruncateLatency time.Duration
	lastTruncateSuccess bool
}

func newMockMetrics() *mockMetrics {
	return &mockMetrics{}
}

func (mm *mockMetrics) IncCounter(name string, labels ...string)                      {}
func (mm *mockMetrics) AddCounter(name string, value float64, labels ...string)       {}
func (mm *mockMetrics) SetGauge(name string, value float64, labels ...string)         {}
func (mm *mockMetrics) ObserveHistogram(name string, value float64, labels ...string) {}
func (mm *mockMetrics) ObserveCommitIndex(index types.Index)                          {}
func (mm *mockMetrics) ObserveAppliedIndex(index types.Index)                         {}
func (mm *mockMetrics) ObserveTerm(term types.Term)                                   {}
func (mm *mockMetrics) ObserveLeaderChange(newLeader types.NodeID, term types.Term)   {}
func (mm *mockMetrics) ObserveLeaderNotificationDropped()                             {}
func (mm *mockMetrics) ObserveLeadershipLost(term types.Term, reason string)          {}
func (mm *mockMetrics) ObserveApplyNotificationDropped()                              {}
func (mm *mockMetrics) ObserveApplyLoopStopped(reason string)                         {}
func (mm *mockMetrics) ObserveRoleChange(newRole types.NodeRole, oldRole types.NodeRole, term types.Term) {
}
func (mm *mockMetrics) ObserveElectionStart(term types.Term, reason ElectionReason) {}
func (mm *mockMetrics) ObserveVoteGranted(term types.Term)                          {}

func (mm *mockMetrics) ObserveLogState(firstIndex, lastIndex types.Index, lastTerm types.Term) {
	mm.logStateCount++
}

func (mm *mockMetrics) ObserveLogAppend(entryCount int, latency time.Duration, success bool) {
	mm.logAppendCount++
}

func (mm *mockMetrics) ObserveLogRead(readType LogReadType, latency time.Duration, success bool) {
	mm.logReadCount++
}

func (mm *mockMetrics) ObserveLogConsistencyError() {
	mm.logConsistencyErr++
}

func (mm *mockMetrics) ObserveLogTruncate(truncateType LogTruncateType, entriesRemoved int, latency time.Duration, success bool) {
	mm.logTruncateCount++
	mm.lastTruncateType = truncateType
	mm.lastEntriesRemoved = entriesRemoved
	mm.lastTruncateLatency = latency
	mm.lastTruncateSuccess = success
}

func (mm *mockMetrics) ObserveElectionElapsed(nodeID types.NodeID, term types.Term, ticks int) {}
func (mm *mockMetrics) ObserveProposal(success bool, reason ProposalResult)                    {}
func (mm *mockMetrics) ObserveReadIndex(success bool, path string)                             {}
func (mm *mockMetrics) ObserveSnapshot(action SnapshotAction, status SnapshotStatus, labels ...string) {
}
func (mm *mockMetrics) ObserveSnapshotRecovery(status SnapshotStatus, reason SnapshotReason) {}
func (mm *mockMetrics) ObservePeerReplication(peerID types.NodeID, success bool, reason ReplicationResult) {
}
func (mm *mockMetrics) ObserveHeartbeat(peerID types.NodeID, success bool, latency time.Duration) {}
func (mm *mockMetrics) ObserveHeartbeatSent()                                                     {}
func (mm *mockMetrics) ObserveAppendEntriesHeartbeat()                                            {}
func (mm *mockMetrics) ObserveAppendEntriesReplication()                                          {}
func (mm *mockMetrics) ObserveEntriesReceived(count int)                                          {}
func (mm *mockMetrics) ObserveCommandBytesReceived(bytes int)                                     {}
func (mm *mockMetrics) ObserveAppendEntriesRejected(reason string)                                {}
func (mm *mockMetrics) ObserveTick(role types.NodeRole)                                           {}
func (mm *mockMetrics) ObserveComponentStopTimeout(component string)                              {}

type mockLeaderInitializer struct {
	initializeStateCalled bool
	sendHeartbeatsCalled  bool
}

func (m *mockLeaderInitializer) InitializeLeaderState() {
	m.initializeStateCalled = true
}

func (m *mockLeaderInitializer) SendHeartbeats(ctx context.Context) {
	m.sendHeartbeatsCalled = true
}

type mockStateManager struct {
	term types.Term
	role types.NodeRole

	BecomeCandidateFunc func(context.Context, ElectionReason) bool
	GetStateFunc        func() (types.Term, types.NodeRole, types.NodeID)
}

func (m *mockStateManager) Initialize(ctx context.Context) error { return nil }
func (m *mockStateManager) GetState() (types.Term, types.NodeRole, types.NodeID) {
	if m.GetStateFunc != nil {
		return m.GetStateFunc()
	}
	return m.term, m.role, "node1"
}
func (m *mockStateManager) GetStateUnsafe() (types.Term, types.NodeRole, types.NodeID) {
	return m.term, m.role, "node1"
}
func (m *mockStateManager) GetLastKnownLeader() types.NodeID { return "node1" }
func (m *mockStateManager) BecomeCandidate(ctx context.Context, reason ElectionReason) bool {
	if m.BecomeCandidateFunc != nil {
		return m.BecomeCandidateFunc(ctx, reason)
	}
	m.term++
	m.role = types.RoleCandidate
	return true
}
func (m *mockStateManager) BecomeLeader(ctx context.Context) bool {
	m.role = types.RoleLeader
	return true
}
func (m *mockStateManager) BecomeFollower(ctx context.Context, term types.Term, leaderID types.NodeID) {
	m.term = term
	m.role = types.RoleFollower
}
func (m *mockStateManager) CheckTermAndStepDown(ctx context.Context, rpcTerm types.Term, rpcLeader types.NodeID) (bool, types.Term) {
	if rpcTerm > m.term {
		previousTerm := m.term
		m.term = rpcTerm
		m.role = types.RoleFollower
		return true, previousTerm
	}
	return false, m.term
}
func (m *mockStateManager) GrantVote(ctx context.Context, candidateID types.NodeID, term types.Term) bool {
	return true
}
func (m *mockStateManager) UpdateCommitIndex(newCommitIndex types.Index) bool       { return true }
func (m *mockStateManager) UpdateCommitIndexUnsafe(newCommitIndex types.Index) bool { return true }
func (m *mockStateManager) UpdateLastApplied(newLastApplied types.Index) bool       { return true }
func (m *mockStateManager) GetCommitIndex() types.Index                             { return 0 }
func (m *mockStateManager) GetCommitIndexUnsafe() types.Index                       { return 0 }
func (m *mockStateManager) GetLastApplied() types.Index                             { return 0 }
func (m *mockStateManager) GetLastAppliedUnsafe() types.Index                       { return 0 }
func (m *mockStateManager) Stop()                                                   {}

type mockLogManager struct {
	lastIndex types.Index
	lastTerm  types.Term
}

func (m *mockLogManager) Initialize(ctx context.Context) error { return nil }
func (m *mockLogManager) GetLastIndexUnsafe() types.Index      { return m.lastIndex }
func (m *mockLogManager) GetLastTermUnsafe() types.Term        { return m.lastTerm }
func (m *mockLogManager) GetConsistentLastState() (types.Index, types.Term) {
	return m.lastIndex, m.lastTerm
}
func (m *mockLogManager) GetFirstIndex() types.Index { return 1 }
func (m *mockLogManager) GetTerm(ctx context.Context, index types.Index) (types.Term, error) {
	return 1, nil
}
func (m *mockLogManager) GetTermUnsafe(ctx context.Context, index types.Index) (types.Term, error) {
	return 1, nil
}
func (m *mockLogManager) GetEntries(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
	return nil, nil
}
func (m *mockLogManager) AppendEntries(ctx context.Context, entries []types.LogEntry) error {
	return nil
}
func (m *mockLogManager) TruncatePrefix(ctx context.Context, newFirstIndex types.Index) error {
	return nil
}
func (m *mockLogManager) TruncateSuffix(ctx context.Context, newLastIndexPlusOne types.Index) error {
	return nil
}
func (m *mockLogManager) IsConsistentWithStorage(ctx context.Context) (bool, error) { return true, nil }
func (m *mockLogManager) RebuildInMemoryState(ctx context.Context) error            { return nil }
func (m *mockLogManager) FindLastEntryWithTermUnsafe(ctx context.Context, term types.Term, searchFromHint types.Index) (types.Index, error) {
	return 0, nil
}
func (m *mockLogManager) FindFirstIndexInTermUnsafe(ctx context.Context, term types.Term, searchUpToIndex types.Index) (types.Index, error) {
	return 0, nil
}
func (m *mockLogManager) Stop() {}
