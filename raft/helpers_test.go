package raft

import (
	"context"
	"time"

	"github.com/jathurchan/raftlock/types"
)

type mockStorage struct {
	state       types.PersistentState
	saveErr     error
	loadErr     error
	saveCounter int
	loadCounter int
}

func (ms *mockStorage) SaveState(ctx context.Context, state types.PersistentState) error {
	ms.state = state
	ms.saveCounter++
	return ms.saveErr
}

func (ms *mockStorage) LoadState(ctx context.Context) (types.PersistentState, error) {
	ms.loadCounter++
	return ms.state, ms.loadErr
}

func (ms *mockStorage) AppendLogEntries(ctx context.Context, entries []types.LogEntry) error {
	return nil
}
func (ms *mockStorage) GetLogEntries(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
	return nil, nil
}
func (ms *mockStorage) GetLogEntry(ctx context.Context, index types.Index) (types.LogEntry, error) {
	return types.LogEntry{}, nil
}
func (ms *mockStorage) TruncateLogSuffix(ctx context.Context, index types.Index) error {
	return nil
}
func (ms *mockStorage) TruncateLogPrefix(ctx context.Context, index types.Index) error {
	return nil
}
func (ms *mockStorage) SaveSnapshot(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
	return nil
}
func (ms *mockStorage) LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	return types.SnapshotMetadata{}, nil, nil
}
func (ms *mockStorage) LastLogIndex() types.Index     { return 0 }
func (ms *mockStorage) FirstLogIndex() types.Index    { return 0 }
func (ms *mockStorage) Close() error                  { return nil }
func (ms *mockStorage) ResetMetrics()                 {}
func (ms *mockStorage) GetMetrics() map[string]uint64 { return nil }
func (ms *mockStorage) GetMetricsSummary() string     { return "" }

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
