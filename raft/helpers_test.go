package raft

import (
	"context"
	"time"

	"github.com/jathurchan/raftlock/types"
)

// mockStorage implements the storage.Storage interface
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
	return nil, nil
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
	return types.SnapshotMetadata{}, nil, nil
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

func (m *mockStorage) ResetMetrics() {
}

func (m *mockStorage) GetMetrics() map[string]uint64 {
	return nil
}

func (m *mockStorage) GetMetricsSummary() string {
	return ""
}

// mockNetwork implements the NetworkManager interface
type mockNetwork struct{}

func (m *mockNetwork) Start() error {
	return nil
}

func (m *mockNetwork) Stop() error {
	return nil
}

func (m *mockNetwork) SendRequestVote(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
	return nil, nil
}

func (m *mockNetwork) SendAppendEntries(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
	return nil, nil
}

func (m *mockNetwork) SendInstallSnapshot(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
	return nil, nil
}

func (m *mockNetwork) PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error) {
	return types.PeerConnectionStatus{}, nil
}

func (m *mockNetwork) LocalAddr() string {
	return ""
}

// mockApplier implements the Applier interface
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

// mockClock implements the Clock interface
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

// mockRand implements the Rand interface
type mockRand struct{}

func (m *mockRand) IntN(n int) int {
	return 0
}

func (m *mockRand) Float64() float64 {
	return 0
}
