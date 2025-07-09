package raft

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func setupSnapshotManager(
	t *testing.T,
) (*snapshotManager, *mockStorage, *mockStateManager, *mockApplier, *mockReplicationStateUpdater) {
	metrics := newMockMetrics()
	mockStorage := newMockStorage()
	stateMgr := newMockStateManager(metrics)
	applier := &mockApplier{}
	logMgr := newMockLogManager()
	clock := newMockClock()
	replicationUpdater := &mockReplicationStateUpdater{}

	deps := SnapshotManagerDeps{
		Mu: &sync.RWMutex{},
		ID: "node1",
		Config: Config{
			Options: Options{SnapshotThreshold: 5, LogCompactionMinEntries: 3},
		},
		Storage:           mockStorage,
		Applier:           applier,
		PeerStateUpdater:  replicationUpdater,
		StateMgr:          stateMgr,
		LogMgr:            logMgr,
		NetworkMgr:        newMockNetworkManager(),
		Metrics:           metrics,
		Logger:            logger.NewStdLogger("debug"),
		Clock:             clock,
		IsShutdown:        new(atomic.Bool),
		NotifyCommitCheck: func() {}, // will be overwritten
	}

	smInterface, err := NewSnapshotManager(deps)
	if err != nil {
		t.Fatalf("failed to create snapshot manager: %v", err)
	}
	sm := smInterface.(*snapshotManager)

	sm.SetReplicationStateUpdater(replicationUpdater)

	return sm, mockStorage, stateMgr, applier, replicationUpdater
}

func TestRaftSnapshot_NoOpMethods(t *testing.T) {
	updater := NoOpReplicationStateUpdater{}

	updater.UpdatePeerAfterSnapshotSend("1", 100)
	updater.SetPeerSnapshotInProgress("2", true)
	updater.MaybeAdvanceCommitIndex()
}

func TestRaftSnapshot_NewSnapshotManager(t *testing.T) {
	metrics := newMockMetrics()

	newValidDeps := func() SnapshotManagerDeps {
		return SnapshotManagerDeps{
			Mu:                &sync.RWMutex{},
			ID:                "node1",
			Storage:           newMockStorage(),
			Applier:           &mockApplier{},
			LogMgr:            newMockLogManager(),
			StateMgr:          newMockStateManager(metrics),
			NetworkMgr:        newMockNetworkManager(),
			Metrics:           metrics,
			Logger:            &logger.NoOpLogger{},
			Clock:             newMockClock(),
			IsShutdown:        &atomic.Bool{},
			PeerStateUpdater:  NoOpReplicationStateUpdater{},
			NotifyCommitCheck: func() {},
			Config:            Config{},
		}
	}

	tests := []struct {
		name          string
		modifyDeps    func(d SnapshotManagerDeps) SnapshotManagerDeps
		expectedError bool
	}{
		{
			name: "valid dependencies",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				return d
			},
			expectedError: false,
		},
		{
			name: "nil mutex",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.Mu = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "empty ID",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.ID = ""
				return d
			},
			expectedError: true,
		},
		{
			name: "nil storage",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.Storage = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "nil applier",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.Applier = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "nil log manager",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.LogMgr = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "nil state manager",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.StateMgr = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "nil peer state updater",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.PeerStateUpdater = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "nil metrics",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.Metrics = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "nil logger",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.Logger = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "nil clock",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.Clock = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "nil shutdown flag",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.IsShutdown = nil
				return d
			},
			expectedError: true,
		},
		{
			name: "nil notifyCommitCheck",
			modifyDeps: func(d SnapshotManagerDeps) SnapshotManagerDeps {
				d.NotifyCommitCheck = nil
				return d
			},
			expectedError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps := tc.modifyDeps(newValidDeps())
			sm, err := NewSnapshotManager(deps)

			if tc.expectedError {
				if err == nil {
					t.Fatalf("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if sm == nil {
					t.Fatalf("Expected non-nil SnapshotManager, got nil")
				}
			}
		})
	}
}

func TestRaftSnapshot_SetReplicationStateUpdater(t *testing.T) {
	updater := new(mockReplicationStateUpdater)

	sm, _, _, _, _ := setupSnapshotManager(t)
	sm.SetReplicationStateUpdater(updater)

	updaterField := sm.peerStateUpdater
	if updaterField != updater {
		t.Fatalf("Expected peerStateUpdater to be set, got different value")
	}

	sm.notifyCommitCheck()
	if !updater.advanceCommitIndexCalled {
		t.Fatalf("Expected MaybeAdvanceCommitIndex to be called")
	}
}

func TestRaftSnapshot_Initialize_NoSnapshot(t *testing.T) {
	sm, mockStorage, stateMgr, _, _ := setupSnapshotManager(t)

	mockStorage.setFailure("LoadSnapshot", storage.ErrNoSnapshot)

	err := sm.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize failed unexpectedly: %v", err)
	}

	meta := sm.GetSnapshotMetadata()
	if meta.LastIncludedIndex != 0 || meta.LastIncludedTerm != 0 {
		t.Errorf(
			"Expected metadata (0,0), got (%d,%d)",
			meta.LastIncludedIndex,
			meta.LastIncludedTerm,
		)
	}

	if stateMgr.commitIndexUpdated {
		t.Errorf("Expected no commit index update on no snapshot")
	}
}

func TestRaftSnapshot_Initialize_WithSnapshot(t *testing.T) {
	sm, mockStorage, stateMgr, _, _ := setupSnapshotManager(t)

	expectedMeta := types.SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 2}
	expectedData := []byte("snapshot")

	mockStorage.hookLoadSnapshot = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
		return expectedMeta, expectedData, nil
	}

	err := sm.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	meta := sm.GetSnapshotMetadata()
	if meta != expectedMeta {
		t.Errorf("Expected metadata %+v, got %+v", expectedMeta, meta)
	}

	if !stateMgr.commitIndexUpdated {
		t.Errorf("Expected commit index to be updated")
	}
}

func TestRaftSnapshot_Initialize_SnapshotOlderThanLastApplied(t *testing.T) {
	sm, mockStorage, stateMgr, _, _ := setupSnapshotManager(t)
	stateMgr.lastApplied = 10 // Already ahead

	snapshotMeta := types.SnapshotMetadata{LastIncludedIndex: 5, LastIncludedTerm: 1}
	mockStorage.hookLoadSnapshot = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
		return snapshotMeta, []byte("old"), nil
	}

	err := sm.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if !stateMgr.commitIndexUpdated {
		t.Errorf("Expected commit index to be updated even for older snapshot")
	}
}

func TestRaftSnapshot_Initialize_CorruptedSnapshot(t *testing.T) {
	sm, mockStorage, _, _, _ := setupSnapshotManager(t)

	mockStorage.setFailure("LoadSnapshot", storage.ErrCorruptedSnapshot)

	err := sm.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Expected no error with corrupted snapshot, got %v", err)
	}

	meta := sm.GetSnapshotMetadata()
	if meta.LastIncludedIndex != 0 || meta.LastIncludedTerm != 0 {
		t.Errorf("Expected default metadata on corruption, got %+v", meta)
	}
}

func TestRaftSnapshot_Initialize_ShutdownContext(t *testing.T) {
	sm, _, _, _, _ := setupSnapshotManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sm.Initialize(ctx)
	if err != ErrShuttingDown {
		t.Errorf("Expected ErrShuttingDown, got %v", err)
	}
}

func TestRaftSnapshot_Initialize_LoadSnapshotFailure(t *testing.T) {
	sm, mockStorage, _, _, _ := setupSnapshotManager(t)

	realErr := fmt.Errorf("disk failure")
	mockStorage.hookLoadSnapshot = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
		return types.SnapshotMetadata{}, nil, realErr
	}

	err := sm.Initialize(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed to load snapshot") {
		t.Errorf("Expected load snapshot error, got %v", err)
	}
}

func TestRaftSnapshot_LoadSnapshot_UnexpectedError(t *testing.T) {
	sm, mockStorage, _, _, _ := setupSnapshotManager(t)

	mockStorage.hookLoadSnapshot = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
		return types.SnapshotMetadata{}, nil, fmt.Errorf("boom")
	}

	_, _, err := sm.loadSnapshot(context.Background())
	if err == nil || !strings.Contains(err.Error(), "storage.LoadSnapshot failed") {
		t.Errorf("Expected wrapped storage error, got %v", err)
	}
}

func TestRaftSnapshot_Initialize_FailedRestoreTriggersInitializeError(t *testing.T) {
	sm, mockStorage, stateMgr, applier, _ := setupSnapshotManager(t)

	stateMgr.lastApplied = 1
	stateMgr.commitIndex = 1

	snapshotMeta := types.SnapshotMetadata{LastIncludedIndex: 10, LastIncludedTerm: 2}
	mockStorage.hookLoadSnapshot = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
		return snapshotMeta, []byte("mock snapshot data"), nil
	}

	applier.restoreErr = fmt.Errorf("restore error")

	err := sm.Initialize(context.Background())
	if err == nil ||
		!strings.Contains(err.Error(), "failed to restore state during initialization") {
		t.Fatalf("Expected wrapped restore error, got: %v", err)
	}
}

func TestRaftSnapshot_MaybeTruncateLog_LogsWarningOnFailure(t *testing.T) {
	sm, _, _, _, _ := setupSnapshotManager(t)

	var wasCalled bool
	sm.logMgr.(*mockLogManager).truncatePrefixFunc = func(ctx context.Context, index types.Index) error {
		wasCalled = true
		return fmt.Errorf("fail to truncate")
	}

	sm.maybeTruncateLog(context.Background(), 10)

	if !wasCalled {
		t.Errorf("Expected truncatePrefixFunc to be called")
	}
}

func TestRaftSnapshot_SnapshotThresholdReached(t *testing.T) {
	sm, mockStorage, stateMgr, applier, _ := setupSnapshotManager(t)

	sm.cfg.Options.SnapshotThreshold = 5
	stateMgr.lastApplied = 10
	sm.lastSnapshotIndex = 0

	applier.snapshotFunc = func(ctx context.Context) (types.Index, []byte, error) {
		return 10, []byte("snapshot-data"), nil
	}

	logMgr := sm.logMgr.(*mockLogManager)
	logMgr.entries[10] = types.LogEntry{
		Index:   10,
		Term:    2,
		Command: []byte("dummy"),
	}
	logMgr.lastIndex = 10
	logMgr.lastTerm = 2

	sm.Tick(context.Background())
	time.Sleep(50 * time.Millisecond)

	if !mockStorage.getSnapshotAttempted() {
		t.Errorf("Expected snapshot to be attempted")
	}
}

func TestRaftSnapshot_Tick_ThresholdNotReached(t *testing.T) {
	sm, mockStore, stateMgr, _, _ := setupSnapshotManager(t)

	sm.cfg.Options.SnapshotThreshold = 10
	stateMgr.lastApplied = 5
	sm.lastSnapshotIndex = 0 // 5 - 0 = 5 < 10 → should skip snapshot

	sm.Tick(context.Background())
	time.Sleep(50 * time.Millisecond)

	if mockStore.snapshotAttempted {
		t.Errorf("Snapshot should not be attempted: threshold not met")
	}
}

func TestRaftSnapshot_Tick_ThresholdZero_DisablesSnapshot(t *testing.T) {
	sm, mockStore, _, _, _ := setupSnapshotManager(t)
	sm.cfg.Options.SnapshotThreshold = 0 // disabled

	sm.Tick(context.Background())
	time.Sleep(20 * time.Millisecond)

	if mockStore.snapshotAttempted {
		t.Errorf("Snapshot should not be attempted when threshold is zero")
	}
}

func TestRaftSnapshot_Tick_AlreadyInProgress(t *testing.T) {
	sm, mockStore, stateMgr, applier, _ := setupSnapshotManager(t)

	sm.snapshotCreateInProgress.Store(true)

	stateMgr.lastApplied = 10
	sm.cfg.Options.SnapshotThreshold = 5
	sm.lastSnapshotIndex = 0

	applier.snapshotFunc = func(ctx context.Context) (types.Index, []byte, error) {
		t.Fatal("Snapshot function should not be called")
		return 0, nil, nil
	}

	sm.Tick(context.Background())
	time.Sleep(20 * time.Millisecond)

	if mockStore.snapshotAttempted {
		t.Errorf("Snapshot should not be attempted when already in progress")
	}
}

func TestRaftSnapshot_Tick_ShutdownEarlyExit(t *testing.T) {
	sm, _, _, _, _ := setupSnapshotManager(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sm.cfg.Options.SnapshotThreshold = 5
	sm.lastSnapshotIndex = 0

	sm.Tick(ctx)
}

func TestRaftSnapshot_Capture_ApplierError(t *testing.T) {
	sm, _, stateMgr, applier, _ := setupSnapshotManager(t)

	stateMgr.lastApplied = 10
	sm.cfg.Options.SnapshotThreshold = 5
	sm.lastSnapshotIndex = 0

	applier.snapshotFunc = func(ctx context.Context) (types.Index, []byte, error) {
		return 0, nil, fmt.Errorf("applier error")
	}

	sm.Tick(context.Background())
	time.Sleep(20 * time.Millisecond)
}

func TestRaftSnapshot_Capture_EmptySnapshot(t *testing.T) {
	sm, _, stateMgr, applier, _ := setupSnapshotManager(t)

	stateMgr.lastApplied = 10
	sm.cfg.Options.SnapshotThreshold = 5
	sm.lastSnapshotIndex = 0

	applier.snapshotFunc = func(ctx context.Context) (types.Index, []byte, error) {
		return 0, nil, nil
	}

	sm.Tick(context.Background())
	time.Sleep(20 * time.Millisecond)
}

func TestRaftSnapshot_Capture_ZeroIndexWithData(t *testing.T) {
	sm, _, stateMgr, applier, _ := setupSnapshotManager(t)

	stateMgr.lastApplied = 10
	sm.cfg.Options.SnapshotThreshold = 5
	sm.lastSnapshotIndex = 0

	applier.snapshotFunc = func(ctx context.Context) (types.Index, []byte, error) {
		return 0, []byte("data"), nil
	}

	sm.Tick(context.Background())
	time.Sleep(20 * time.Millisecond)
}

func TestRaftSnapshot_Capture_LogTermFailure(t *testing.T) {
	sm, _, stateMgr, applier, _ := setupSnapshotManager(t)

	stateMgr.lastApplied = 10
	sm.cfg.Options.SnapshotThreshold = 5
	sm.lastSnapshotIndex = 0

	applier.snapshotFunc = func(ctx context.Context) (types.Index, []byte, error) {
		return 10, []byte("valid"), nil
	}

	sm.Tick(context.Background())
	time.Sleep(20 * time.Millisecond)
}

func TestSnapshot_Persist_StorageFails(t *testing.T) {
	sm, mockStore, stateMgr, applier, _ := setupSnapshotManager(t)

	stateMgr.lastApplied = 10
	sm.cfg.Options.SnapshotThreshold = 5
	sm.lastSnapshotIndex = 0

	applier.snapshotFunc = func(ctx context.Context) (types.Index, []byte, error) {
		return 10, []byte("valid"), nil
	}

	sm.logMgr.(*mockLogManager).entries[10] = types.LogEntry{Index: 10, Term: 1}
	mockStore.setFailure("SaveSnapshot", fmt.Errorf("save failed"))

	sm.Tick(context.Background())
	time.Sleep(20 * time.Millisecond)
}

func TestRaftSnapshot_HandleInstall_StaleSnapshotIgnored(t *testing.T) {
	sm, mockStore, stateMgr, _, _ := setupSnapshotManager(t)

	sm.lastSnapshotIndex = 15
	sm.lastSnapshotTerm = 3

	staleArgs := &types.InstallSnapshotArgs{
		Term:              4,
		LeaderID:          "leader1",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              []byte("stale snapshot"),
	}

	_, err := sm.HandleInstallSnapshot(context.Background(), staleArgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockStore.snapshotSaved {
		t.Errorf("Stale snapshot should be ignored and not persisted")
	}

	if stateMgr.currentTerm != staleArgs.Term || stateMgr.currentRole != types.RoleFollower {
		t.Errorf("Expected term and role to update despite rejecting snapshot")
	}
}

func TestRaftSnapshot_HandleInstall_ValidSnapshot(t *testing.T) {
	sm, mockStore, _, applier, _ := setupSnapshotManager(t)

	sm.isShutdown.Store(false)

	sm.lastSnapshotIndex = 5
	sm.lastSnapshotTerm = 1

	applier.restoreErr = nil

	args := &types.InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "leader1",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              []byte("valid snapshot"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	fmt.Println(">>> Calling HandleInstallSnapshot")
	_, err := sm.HandleInstallSnapshot(ctx, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fmt.Println(">>> HandleInstallSnapshot returned")

	if !mockStore.snapshotSaved {
		t.Errorf("Expected snapshot to be saved")
	}

	if !applier.applierRestored {
		t.Errorf("Expected snapshot to be applied to state machine")
	}

	meta := sm.GetSnapshotMetadata()
	if meta.LastIncludedIndex != args.LastIncludedIndex ||
		meta.LastIncludedTerm != args.LastIncludedTerm {
		t.Errorf("Snapshot metadata mismatch: got (%d,%d), expected (%d,%d)",
			meta.LastIncludedIndex, meta.LastIncludedTerm,
			args.LastIncludedIndex, args.LastIncludedTerm)
	}
}

func TestRaftSnapshot_Tick_ShutdownDuringSnapshot(t *testing.T) {
	sm, mockStore, stateMgr, applier, _ := setupSnapshotManager(t)

	stateMgr.lastApplied = 10
	sm.cfg.Options.SnapshotThreshold = 5
	sm.lastSnapshotIndex = 0

	applier.snapshotFunc = func(ctx context.Context) (types.Index, []byte, error) {
		sm.isShutdown.Store(true)
		return 10, []byte("data"), nil
	}

	sm.logMgr.(*mockLogManager).entries[10] = types.LogEntry{Index: 10, Term: 1}

	sm.Tick(context.Background())
	time.Sleep(20 * time.Millisecond)

	if mockStore.snapshotAttempted {
		t.Errorf("Snapshot should be aborted on shutdown")
	}
}

func TestSendSnapshot_Success(t *testing.T) {
	sm, mockStorage, stateMgr, _, updater := setupSnapshotManager(t)
	mockNetwork := &mockNetworkManager{}
	sm.networkMgr = mockNetwork

	snapshotMeta := types.SnapshotMetadata{LastIncludedIndex: 10, LastIncludedTerm: 2}
	snapshotData := []byte("test snapshot data")

	mockStorage.hookLoadSnapshot = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
		return snapshotMeta, snapshotData, nil
	}

	stateMgr.currentTerm = 3
	stateMgr.currentRole = types.RoleLeader

	mockNetwork.sendInstallSnapshotFunc = func(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
		return &types.InstallSnapshotReply{Term: stateMgr.currentTerm}, nil
	}

	targetID := types.NodeID("follower1")
	sm.SendSnapshot(context.Background(), targetID, stateMgr.currentTerm)

	if !updater.snapshotProgressUpdated {
		t.Errorf("Expected snapshot in-progress flag to be set and cleared")
	}
	if !updater.peerUpdatedAfterSnapshot {
		t.Errorf("Expected peer state to be updated after successful snapshot")
	}
	if !updater.advanceCommitIndexCalled {
		t.Errorf("Expected commit index advancement to be triggered")
	}
}

func TestSendSnapshot_LoadSnapshotError(t *testing.T) {
	sm, mockStorage, mockStateMgr, _, _ := setupSnapshotManager(t)
	mockNetwork := &mockNetworkManager{}
	sm.networkMgr = mockNetwork

	updater := new(mockReplicationStateUpdater)
	sm.SetReplicationStateUpdater(updater)

	mockStorage.setFailure("LoadSnapshot", errors.New("load failure"))

	mockStateMgr.currentTerm = 3
	mockStateMgr.currentRole = types.RoleLeader

	mockNetwork.sendInstallSnapshotFunc = func(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
		t.Errorf("Should not attempt to send snapshot when load fails")
		return nil, nil
	}

	targetID := types.NodeID("follower1")
	sm.SendSnapshot(context.Background(), targetID, mockStateMgr.currentTerm)

	if !updater.snapshotProgressUpdated {
		t.Errorf("Expected snapshot in-progress flag to be updated regardless of result")
	}

	if updater.peerUpdatedAfterSnapshot {
		t.Errorf("No peer update should happen when snapshot load fails")
	}
}

func TestSendSnapshot_EmptySnapshot(t *testing.T) {
	sm, mockStorage, mockStateMgr, _, _ := setupSnapshotManager(t)
	mockNetwork := &mockNetworkManager{}
	sm.networkMgr = mockNetwork

	updater := new(mockReplicationStateUpdater)
	sm.SetReplicationStateUpdater(updater)

	mockStorage.hookLoadSnapshot = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
		return types.SnapshotMetadata{}, []byte{}, nil
	}

	mockStateMgr.currentTerm = 3
	mockStateMgr.currentRole = types.RoleLeader

	mockNetwork.sendInstallSnapshotFunc = func(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
		t.Errorf("Should not attempt to send empty snapshot")
		return nil, nil
	}

	targetID := types.NodeID("follower1")
	sm.SendSnapshot(context.Background(), targetID, mockStateMgr.currentTerm)

	if updater.peerUpdatedAfterSnapshot {
		t.Errorf("No peer update should happen for empty snapshot")
	}
}

func TestSendSnapshot_RpcError(t *testing.T) {
	sm, mockStorage, mockStateMgr, _, _ := setupSnapshotManager(t)
	mockNetwork := &mockNetworkManager{}
	sm.networkMgr = mockNetwork

	updater := new(mockReplicationStateUpdater)
	sm.SetReplicationStateUpdater(updater)

	snapshotMeta := types.SnapshotMetadata{LastIncludedIndex: 10, LastIncludedTerm: 2}
	snapshotData := []byte("test snapshot data")

	mockStorage.hookLoadSnapshot = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
		return snapshotMeta, snapshotData, nil
	}

	mockStateMgr.currentTerm = 3
	mockStateMgr.currentRole = types.RoleLeader

	mockNetwork.sendInstallSnapshotFunc = func(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
		return nil, errors.New("rpc failure")
	}

	targetID := types.NodeID("follower1")
	sm.SendSnapshot(context.Background(), targetID, mockStateMgr.currentTerm)

	if !updater.snapshotProgressUpdated {
		t.Errorf("Expected snapshot in-progress flag to be updated regardless of result")
	}

	if updater.peerUpdatedAfterSnapshot {
		t.Errorf("No peer update should happen when RPC fails")
	}
}

func TestSendSnapshot_HigherTermInReply(t *testing.T) {
	sm, mockStorage, mockStateMgr, _, _ := setupSnapshotManager(t)
	mockNetwork := &mockNetworkManager{}
	sm.networkMgr = mockNetwork

	updater := new(mockReplicationStateUpdater)
	sm.SetReplicationStateUpdater(updater)

	snapshotMeta := types.SnapshotMetadata{LastIncludedIndex: 10, LastIncludedTerm: 2}
	snapshotData := []byte("test snapshot data")

	mockStorage.hookLoadSnapshot = func(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
		return snapshotMeta, snapshotData, nil
	}

	mockStateMgr.currentTerm = 3
	mockStateMgr.currentRole = types.RoleLeader

	mockNetwork.sendInstallSnapshotFunc = func(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
		return &types.InstallSnapshotReply{Term: 4}, nil // Higher term
	}

	targetID := types.NodeID("follower1")
	sm.SendSnapshot(context.Background(), targetID, mockStateMgr.currentTerm)

	if mockStateMgr.currentTerm != 4 || mockStateMgr.currentRole != types.RoleFollower {
		t.Errorf("Expected step down with term update to 4, got term=%d, role=%s",
			mockStateMgr.currentTerm, mockStateMgr.currentRole)
	}

	if updater.peerUpdatedAfterSnapshot {
		t.Errorf("No peer update should happen after stepping down")
	}
}

func setupSnapshotManagerForCompactionTest(
	t *testing.T,
	logCompactionMinEntries int,
) (*snapshotManager, *mockLogManager) {
	t.Helper()
	metrics := newMockMetrics()

	mockStorage := newMockStorage()
	stateMgr := newMockStateManager(metrics)
	applier := &mockApplier{}
	logMgr := newMockLogManager()
	clock := newMockClock()
	replicationUpdater := &mockReplicationStateUpdater{}

	deps := SnapshotManagerDeps{
		Mu: &sync.RWMutex{},
		ID: "node1",
		Config: Config{
			Options: Options{
				SnapshotThreshold:       5,
				LogCompactionMinEntries: logCompactionMinEntries,
			},
		},
		Storage:           mockStorage,
		Applier:           applier,
		StateMgr:          stateMgr,
		LogMgr:            logMgr,
		NetworkMgr:        newMockNetworkManager(),
		Metrics:           metrics,
		Logger:            logger.NewNoOpLogger(),
		Clock:             clock,
		IsShutdown:        new(atomic.Bool),
		PeerStateUpdater:  replicationUpdater,
		NotifyCommitCheck: func() { replicationUpdater.MaybeAdvanceCommitIndex() },
	}

	smInterface, err := NewSnapshotManager(deps)
	if err != nil {
		t.Fatalf("failed to create snapshot manager: %v", err)
	}
	sm := smInterface.(*snapshotManager)
	sm.SetReplicationStateUpdater(replicationUpdater)

	return sm, logMgr
}

func TestRaftSnapshot_SnapshotManager_MaybeTriggerLogCompaction(t *testing.T) {
	baseSnapshotMeta := types.SnapshotMetadata{
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
	}

	tests := []struct {
		name                  string
		logCompactionMin      int
		snapshotMeta          types.SnapshotMetadata
		logLastIndex          types.Index
		expectTruncate        bool
		expectedTruncateIndex types.Index
		truncateShouldError   bool
	}{
		{
			name:                  "CompactionTriggered_SufficientEntries",
			logCompactionMin:      5,
			snapshotMeta:          baseSnapshotMeta,
			logLastIndex:          20,
			expectTruncate:        true,
			expectedTruncateIndex: 11, // baseSnapshotMeta.LastIncludedIndex + 1
		},
		{
			name:                  "CompactionTriggered_ExactEntries",
			logCompactionMin:      5,
			snapshotMeta:          baseSnapshotMeta,
			logLastIndex:          15, // entriesAfterSnapshot = 15 - 10 = 5. 5 >= 5.
			expectTruncate:        true,
			expectedTruncateIndex: 11,
		},
		{
			name:             "CompactionNotTriggered_NotEnoughEntries",
			logCompactionMin: 10,
			snapshotMeta:     baseSnapshotMeta,
			logLastIndex:     15, // entriesAfterSnapshot = 15 - 10 = 5. 5 < 10.
			expectTruncate:   false,
		},
		{
			name:             "CompactionNotTriggered_Disabled_ZeroMinEntries",
			logCompactionMin: 0, // Disabled
			snapshotMeta:     baseSnapshotMeta,
			logLastIndex:     100, // Would trigger if enabled
			expectTruncate:   false,
		},
		{
			name:             "CompactionNotTriggered_Disabled_NegativeMinEntries",
			logCompactionMin: -1, // Disabled
			snapshotMeta:     baseSnapshotMeta,
			logLastIndex:     100, // Would trigger if enabled
			expectTruncate:   false,
		},
		{
			name:             "CompactionTriggered_SnapshotAtZeroIndex",
			logCompactionMin: 3,
			snapshotMeta: types.SnapshotMetadata{
				LastIncludedIndex: 0,
				LastIncludedTerm:  0,
			},
			logLastIndex:          5, // entriesAfterSnapshot = 5 - 0 = 5. 5 >= 3.
			expectTruncate:        true,
			expectedTruncateIndex: 1, // 0 + 1
		},
		{
			name:             "CompactionNotTriggered_LogEndsAtSnapshotIndex",
			logCompactionMin: 1,
			snapshotMeta:     baseSnapshotMeta,
			logLastIndex:     10, // entriesAfterSnapshot = 10 - 10 = 0. 0 < 1.
			expectTruncate:   false,
		},
		{
			name:                  "CompactionTriggered_TruncateErrorLogged",
			logCompactionMin:      5,
			snapshotMeta:          baseSnapshotMeta,
			logLastIndex:          20,
			expectTruncate:        true, // It will be called
			expectedTruncateIndex: 11,
			truncateShouldError:   true, // Simulate logMgr.TruncatePrefix error
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sm, mockLogMgr := setupSnapshotManagerForCompactionTest(t, tc.logCompactionMin)

			mockLogMgr.mu.Lock()
			mockLogMgr.lastIndex = tc.logLastIndex
			mockLogMgr.mu.Unlock()

			var truncateCalled atomic.Bool
			var actualTruncateIndex atomic.Uint64

			mockLogMgr.truncatePrefixFunc = func(ctx context.Context, newFirstIndex types.Index) error {
				truncateCalled.Store(true)
				actualTruncateIndex.Store(uint64(newFirstIndex))
				if tc.truncateShouldError {
					return fmt.Errorf("simulated truncation error")
				}
				return nil
			}

			sm.maybeTriggerLogCompaction(context.Background(), tc.snapshotMeta)

			if tc.expectTruncate {
				if !truncateCalled.Load() {
					t.Errorf("Expected log compaction to be triggered, but it was not.")
				}
				if truncateCalled.Load() &&
					types.Index(actualTruncateIndex.Load()) != tc.expectedTruncateIndex {
					t.Errorf(
						"Expected truncation at index %d, got %d.",
						tc.expectedTruncateIndex,
						actualTruncateIndex.Load(),
					)
				}
			} else {
				if truncateCalled.Load() {
					t.Errorf("Expected log compaction NOT to be triggered, but it was (at index %d).", actualTruncateIndex.Load())
				}
			}
		})
	}
}

// setupSnapshotManagerForStopTest is a helper to prepare a snapshotManager with controllable internals for Stop() tests.
func setupSnapshotManagerForStopTest(t *testing.T) (*snapshotManager, *mockClock, *atomic.Int32, *sync.WaitGroup) { // Use atomic.Int33 to match ElectionManager's concurrentOps
	metrics := newMockMetrics()
	mockStorage := newMockStorage()
	stateMgr := newMockStateManager(metrics)
	applier := &mockApplier{}
	logMgr := newMockLogManager() // Make sure newMockLogManager is defined in helpers_test.go
	mockClock := newMockClock()
	replicationUpdater := &mockReplicationStateUpdater{}
	isShutdown := new(atomic.Bool)

	deps := SnapshotManagerDeps{
		Mu: &sync.RWMutex{},
		ID: "node1",
		Config: Config{
			Options: Options{SnapshotThreshold: 5, LogCompactionMinEntries: 3},
		},
		Storage:           mockStorage,
		Applier:           applier,
		PeerStateUpdater:  replicationUpdater,
		StateMgr:          stateMgr,
		LogMgr:            logMgr,
		NetworkMgr:        newMockNetworkManager(),
		Metrics:           metrics,
		Logger:            logger.NewNoOpLogger(),
		Clock:             mockClock,
		IsShutdown:        isShutdown,
		NotifyCommitCheck: func() {}, // will be overwritten or used by mock
	}

	smInterface, err := NewSnapshotManager(deps)
	testutil.AssertNoError(t, err)
	sm := smInterface.(*snapshotManager) // Cast to concrete type to access internals

	return sm, mockClock, sm.snapshotOpsCounter, sm.snapshotOpsWg
}

func TestSnapshotManager_Stop(t *testing.T) {
	t.Run("NoActiveOperations", func(t *testing.T) {
		sm, _, opsCounter, _ := setupSnapshotManagerForStopTest(t)

		opsCounter.Store(0) // Ensure no active operations

		// Call Stop(). It should return immediately.
		sm.Stop()

		// Assertions: No panics, and the counter remains 0.
		testutil.AssertEqual(t, int32(0), opsCounter.Load(), "Ops counter should remain 0")
	})

	t.Run("GracefulCompletion", func(t *testing.T) {
		sm, _, opsCounter, wg := setupSnapshotManagerForStopTest(t)

		opsCounter.Store(2) // Simulate 2 active operations starting
		wg.Add(2)           // Add 2 to the WaitGroup counter

		go func() {
			defer func() { // Ensure atomic counter is decremented even if panics
				wg.Done()
				opsCounter.Add(-1)
			}()

			defer func() {
				wg.Done()
				opsCounter.Add(-1)
			}()
		}()

		time.Sleep(1 * time.Millisecond)

		sm.Stop()

		testutil.AssertEqual(t, int32(0), opsCounter.Load(), "Ops counter should be 0 after graceful stop")
	})

	t.Run("Timeout", func(t *testing.T) {
		sm, mockClock, opsCounter, wg := setupSnapshotManagerForStopTest(t)

		opsCounter.Store(1) // Simulate 1 active operation
		wg.Add(1)           // Add 1 to the WaitGroup counter (but never call Done())

		// Launch a goroutine to explicitly advance the mock clock past the timeout
		// so that the timer inside sm.Stop() expires.
		go func() {
			mockClock.Advance(defaultSnapshotStopTimeout + time.Millisecond) // Advance mock clock past timeout
		}()

		// Call Stop(). It should wait until the mock clock is advanced by the goroutine
		// and the timer inside Stop() fires, then exit.
		sm.Stop()

		// Assertions: Ops counter should remain its initial value (1), as operation timed out.
		testutil.AssertEqual(t, int32(1), opsCounter.Load(), "Ops counter should remain 1 after timeout")
	})

	t.Run("MultipleCallsIdempotent", func(t *testing.T) {
		sm, _, opsCounter, _ := setupSnapshotManagerForStopTest(t)

		opsCounter.Store(0) // Ensure initial state is clean

		// Call Stop() multiple times. Only the first call should execute the full logic.
		sm.Stop()
		sm.Stop()
		sm.Stop()

		// Assertions: No panics, and the counter remains 0, confirming idempotency.
		testutil.AssertEqual(t, int32(0), opsCounter.Load(), "Ops counter should remain 0 after multiple idempotent stops")
	})
}
