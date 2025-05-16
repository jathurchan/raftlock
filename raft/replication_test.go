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
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func setupTestReplicationManager(t *testing.T) (*replicationManager, *mockLogManager, *mockStateManager, *mockNetworkManager, *mockSnapshotManager, *mockClock) {
	t.Helper()

	mu := &sync.RWMutex{}
	isShutdown := &atomic.Bool{}

	logMgr := newMockLogManager()
	stateMgr := newMockStateManager()
	networkMgr := newMockNetworkManager()
	snapshotMgr := newMockSnapshotManager()
	metrics := newMockMetrics()
	clock := newMockClock()

	stateMgr.currentRole = types.RoleLeader
	stateMgr.leaderID = "node1"

	applyCh := make(chan struct{}, 10)

	config := Config{
		ID: "node1",
		Peers: map[types.NodeID]PeerConfig{
			"node1": {ID: "node1", Address: "localhost:8001"},
			"node2": {ID: "node2", Address: "localhost:8002"},
			"node3": {ID: "node3", Address: "localhost:8003"},
		},
		Options: Options{
			HeartbeatTickCount:      1,
			ElectionTickCount:       10,
			MaxLogEntriesPerRequest: 100,
		},
		FeatureFlags: FeatureFlags{
			EnableLeaderLease: true,
			EnableReadIndex:   true,
		},
	}

	deps := ReplicationManagerDeps{
		Mu:             mu,
		ID:             config.ID,
		Peers:          config.Peers,
		QuorumSize:     2, // For a 3-node cluster
		Cfg:            config,
		StateMgr:       stateMgr,
		LogMgr:         logMgr,
		SnapshotMgr:    snapshotMgr,
		NetworkMgr:     networkMgr,
		Metrics:        metrics,
		Logger:         logger.NewNoOpLogger(),
		Clock:          clock,
		IsShutdownFlag: isShutdown,
		ApplyNotifyCh:  applyCh,
	}

	rm, err := NewReplicationManager(deps)
	if err != nil {
		t.Fatalf("Failed to create ReplicationManager: %v", err)
	}

	rm.(*replicationManager).InitializeLeaderState()

	return rm.(*replicationManager), logMgr, stateMgr, networkMgr, snapshotMgr, clock
}

func TestRaftReplication_NewReplicationManager(t *testing.T) {
	var (
		mu         = &sync.RWMutex{}
		isShutdown = &atomic.Bool{}
		applyCh    = make(chan struct{}, 10)
	)

	config := Config{
		ID: "node1",
		Peers: map[types.NodeID]PeerConfig{
			"node1": {ID: "node1", Address: "localhost:8001"},
			"node2": {ID: "node2", Address: "localhost:8002"},
			"node3": {ID: "node3", Address: "localhost:8003"},
		},
		Options: Options{
			HeartbeatTickCount: 1,
			ElectionTickCount:  0, // Triggers warning and fallback to default
		},
		FeatureFlags: FeatureFlags{
			EnableLeaderLease: true, // Enables lease duration calculation
		},
	}

	deps := ReplicationManagerDeps{
		Mu:             mu,
		ID:             config.ID,
		Peers:          config.Peers,
		QuorumSize:     2,
		Cfg:            config,
		StateMgr:       newMockStateManager(),
		LogMgr:         newMockLogManager(),
		SnapshotMgr:    newMockSnapshotManager(),
		NetworkMgr:     newMockNetworkManager(),
		Metrics:        newMockMetrics(),
		Logger:         logger.NewNoOpLogger(),
		Clock:          newMockClock(),
		IsShutdownFlag: isShutdown,
		ApplyNotifyCh:  applyCh,
	}

	t.Run("valid initialization", func(t *testing.T) {
		rm, err := NewReplicationManager(deps)
		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, rm)
		rm.Stop()
	})

	t.Run("missing LogMgr should fail", func(t *testing.T) {
		badDeps := deps
		badDeps.LogMgr = nil
		_, err := NewReplicationManager(badDeps)
		testutil.AssertError(t, err)
	})

	t.Run("invalid quorum size should fail", func(t *testing.T) {
		badDeps := deps
		badDeps.QuorumSize = 0
		_, err := NewReplicationManager(badDeps)
		testutil.AssertError(t, err)
	})
}

func TestRaftReplication_ValidateReplicationManagerDeps(t *testing.T) {
	base := ReplicationManagerDeps{
		Mu:             &sync.RWMutex{},
		ID:             "node1",
		Peers:          map[types.NodeID]PeerConfig{},
		QuorumSize:     1,
		Cfg:            Config{},
		StateMgr:       newMockStateManager(),
		LogMgr:         newMockLogManager(),
		SnapshotMgr:    newMockSnapshotManager(),
		NetworkMgr:     newMockNetworkManager(),
		Metrics:        newMockMetrics(),
		Logger:         logger.NewNoOpLogger(),
		Clock:          newMockClock(),
		IsShutdownFlag: &atomic.Bool{},
		ApplyNotifyCh:  make(chan struct{}, 1),
	}

	tests := []struct {
		name       string
		modify     func(d ReplicationManagerDeps) ReplicationManagerDeps
		wantErr    bool
		errorMatch string
	}{
		{"missing Mu", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.Mu = nil
			return d
		}, true, "Mu"},

		{"missing ID", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.ID = ""
			return d
		}, true, "ID"},

		{"missing NetworkMgr", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.NetworkMgr = nil
			return d
		}, true, "NetworkMgr"},

		{"missing StateMgr", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.StateMgr = nil
			return d
		}, true, "StateMgr"},

		{"missing LogMgr", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.LogMgr = nil
			return d
		}, true, "LogMgr"},

		{"missing SnapshotMgr", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.SnapshotMgr = nil
			return d
		}, true, "SnapshotMgr"},

		{"missing Metrics", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.Metrics = nil
			return d
		}, true, "Metrics"},

		{"missing Logger", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.Logger = nil
			return d
		}, true, "Logger"},

		{"missing Clock", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.Clock = nil
			return d
		}, true, "Clock"},

		{"missing ApplyNotifyCh", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.ApplyNotifyCh = nil
			return d
		}, true, "ApplyNotifyCh"},

		{"missing IsShutdownFlag", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.IsShutdownFlag = nil
			return d
		}, true, "IsShutdownFlag"},

		{"zero quorum size", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.QuorumSize = 0
			return d
		}, true, "QuorumSize"},

		{"nil Peers map", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			d.Peers = nil
			return d
		}, true, "Peers"},

		{"all valid", func(d ReplicationManagerDeps) ReplicationManagerDeps {
			return d
		}, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := tt.modify(base)
			err := validateReplicationManagerDeps(deps)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), tt.errorMatch) {
					t.Errorf("Expected error containing %q, got: %v", tt.errorMatch, err)
				}
			} else if err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}

func TestRaftReplication_Tick(t *testing.T) {
	t.Run("returns early if shutdown flag is set manually", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		rm.isShutdown.Store(true)

		rm.Tick(context.Background())
	})

	t.Run("returns early if node is not leader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "node2"

		rm.Tick(context.Background())
		rm.Stop()
	})

	t.Run("uses default heartbeat interval if configured value is invalid", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		rm.cfg.Options.HeartbeatTickCount = 0

		interval := rm.getHeartbeatInterval()
		testutil.AssertEqual(t, DefaultHeartbeatTickCount, interval, "expected fallback to default heartbeat interval")
		rm.Stop()
	})

	t.Run("invokes MaybeAdvanceCommitIndex on commit notification", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = "node1"

		rm.notifyCommitCh <- struct{}{}

		// Prevent heartbeat from triggering
		rm.mu.Lock()
		rm.heartbeatElapsed = 0
		rm.mu.Unlock()

		rm.Tick(context.Background())
		rm.Stop()
	})

	t.Run("sends heartbeats when interval elapses", func(t *testing.T) {
		rm, _, stateMgr, network, _, _ := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = "node1"

		var wg sync.WaitGroup
		wg.Add(2)
		network.setWaitGroup(&wg)

		// Simulate elapsed heartbeat interval
		rm.mu.Lock()
		rm.heartbeatElapsed = rm.getHeartbeatInterval()
		rm.mu.Unlock()

		rm.Tick(context.Background())
		wg.Wait()

		appendEntries, heartbeats := network.getAndResetCallCounts()
		testutil.AssertEqual(t, 2, appendEntries, "should send AppendEntries to 2 peers")
		testutil.AssertEqual(t, 2, heartbeats, "should send heartbeats to 2 peers")
		rm.Stop()
	})
}

func TestRaftReplication_InitializeLeaderState(t *testing.T) {
	t.Run("initializes state when node is leader", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)

		logMgr.entries[1] = types.LogEntry{Term: 1, Index: 1}
		logMgr.entries[2] = types.LogEntry{Term: 1, Index: 2}
		logMgr.lastIndex = 2
		logMgr.lastTerm = 1

		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = "node1"

		rm.InitializeLeaderState()

		rm.mu.RLock()
		defer rm.mu.RUnlock()

		for peerID, ps := range rm.peerStates {
			if peerID == rm.id {
				continue
			}
			testutil.AssertEqual(t, types.Index(3), ps.NextIndex)
			testutil.AssertEqual(t, types.Index(0), ps.MatchIndex)
			testutil.AssertTrue(t, ps.IsActive)
		}

		testutil.AssertEqual(t, 0, rm.heartbeatElapsed)

		if rm.leaderLeaseEnabled {
			testutil.AssertFalse(t, rm.leaseExpiry.IsZero())
		}
	})

	t.Run("returns early with warning if not leader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "node2"

		rm.InitializeLeaderState()
	})
}

func TestRaftReplication_SendHeartbeats(t *testing.T) {
	t.Run("sends heartbeats to all peers when leader", func(t *testing.T) {
		rm, _, stateMgr, network, _, _ := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = "node1"

		var wg sync.WaitGroup
		wg.Add(2)
		network.setWaitGroup(&wg)

		rm.SendHeartbeats(context.Background())
		wg.Wait()

		appendEntries, heartbeats := network.getAndResetCallCounts()
		testutil.AssertEqual(t, 2, appendEntries, "Should send AppendEntries to both peers")
		testutil.AssertEqual(t, 2, heartbeats, "All messages should be heartbeats")
		rm.Stop()
	})

	t.Run("does nothing if not leader", func(t *testing.T) {
		rm, _, stateMgr, network, _, _ := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "node2"

		rm.SendHeartbeats(context.Background())
		appendEntries, _ := network.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries, "Follower should not send heartbeats")
		rm.Stop()
	})

	t.Run("does nothing if context is canceled", func(t *testing.T) {
		rm, _, stateMgr, network, _, _ := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = "node1"

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		rm.SendHeartbeats(ctx)
		appendEntries, _ := network.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries, "Should not send heartbeats with canceled context")
		rm.Stop()
	})
}

func TestRaftReplication_ReplicateToPeerInternal_Success(t *testing.T) {
	rm, logMgr, _, network, _, _ := setupTestReplicationManager(t)

	logMgr.entries[1] = types.LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")}
	logMgr.entries[2] = types.LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")}
	logMgr.entries[3] = types.LogEntry{Term: 1, Index: 3, Command: []byte("cmd3")}
	logMgr.lastIndex = 3
	logMgr.lastTerm = 1

	rm.mu.Lock()
	ps := rm.peerStates["node2"]
	ps.NextIndex = 1
	ps.MatchIndex = 0
	rm.mu.Unlock()

	network.sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
		t.Logf("AppendEntries sent: to=%s term=%d entries=%d", target, args.Term, len(args.Entries))

		reply := &types.AppendEntriesReply{
			Term:    args.Term,
			Success: true,
		}

		start := rm.clock.Now()
		end := start.Add(1 * time.Millisecond)

		rm.mu.Lock()
		defer rm.mu.Unlock()
		rm.handleAppendReply(target, args.Term, args, reply, false, end.Sub(start))

		return reply, nil
	}

	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)

	rm.mu.RLock()
	ps = rm.peerStates["node2"]
	testutil.AssertEqual(t, types.Index(4), ps.NextIndex, "expected updated NextIndex")
	testutil.AssertEqual(t, types.Index(3), ps.MatchIndex, "expected updated MatchIndex")
	testutil.AssertTrue(t, ps.IsActive, "expected peer to be active")
}

func TestRaftReplication_ReplicateToPeerInternal_LogConflict(t *testing.T) {
	rm, logMgr, _, network, _, _ := setupTestReplicationManager(t)

	logMgr.entries[1] = types.LogEntry{Term: 1, Index: 1}
	logMgr.entries[2] = types.LogEntry{Term: 2, Index: 2}
	logMgr.entries[3] = types.LogEntry{Term: 2, Index: 3}
	logMgr.lastIndex = 3
	logMgr.lastTerm = 2

	rm.mu.Lock()
	ps := rm.peerStates["node2"]
	ps.NextIndex = 2
	ps.MatchIndex = 2
	rm.mu.Unlock()

	network.sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
		return &types.AppendEntriesReply{
			Term:          args.Term,
			Success:       false,
			ConflictTerm:  1,
			ConflictIndex: 2,
		}, nil
	}

	rm.replicateToPeerInternal(context.Background(), "node2", 2, 0, false)

	rm.mu.RLock()
	ps = rm.peerStates["node2"]
	testutil.AssertEqual(t, types.Index(2), ps.NextIndex, "expected NextIndex rollback due to conflict")
	testutil.AssertEqual(t, types.Index(2), ps.MatchIndex, "expected MatchIndex unchanged")
	testutil.AssertTrue(t, ps.IsActive, "expected peer to remain active")
	rm.mu.RUnlock()
}

func TestRaftReplication_ReplicateToPeerInternal_HigherTerm(t *testing.T) {
	rm, logMgr, stateMgr, network, _, _ := setupTestReplicationManager(t)

	logMgr.entries[1] = types.LogEntry{Term: 1, Index: 1}
	logMgr.lastIndex = 1
	logMgr.lastTerm = 1

	stateMgr.currentTerm = 1
	stateMgr.currentRole = types.RoleLeader
	stateMgr.leaderID = "node1"

	rm.mu.Lock()
	ps := rm.peerStates["node2"]
	ps.NextIndex = 1
	ps.MatchIndex = 0
	rm.mu.Unlock()

	network.sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
		return &types.AppendEntriesReply{
			Term:    2,
			Success: false,
		}, nil
	}

	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)

	term, role, _ := stateMgr.GetState()
	testutil.AssertEqual(t, types.Term(2), term, "expected term to update after higher term reply")
	testutil.AssertEqual(t, types.RoleFollower, role, "expected role to downgrade to follower")
}

func TestRaftReplication_ReplicateToPeerInternal_NetworkError(t *testing.T) {
	rm, logMgr, _, network, _, _ := setupTestReplicationManager(t)

	logMgr.entries[1] = types.LogEntry{Term: 1, Index: 1}
	logMgr.lastIndex = 1
	logMgr.lastTerm = 1

	rm.mu.Lock()
	ps := rm.peerStates["node2"]
	ps.NextIndex = 1
	ps.MatchIndex = 0
	ps.IsActive = true
	rm.mu.Unlock()

	expectedErr := errors.New("network error")
	network.sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
		return nil, expectedErr
	}

	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)

	rm.mu.RLock()
	ps = rm.peerStates["node2"]
	testutil.AssertFalse(t, ps.IsActive, "expected peer to be marked inactive after error")
	rm.mu.RUnlock()
}

func setupReplicationWithPeer(t *testing.T, nextIndex types.Index, entries map[types.Index]types.LogEntry, snapshotInProgress bool) (*replicationManager, *mockLogManager) {
	rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)

	for idx, entry := range entries {
		logMgr.entries[idx] = entry
	}
	logMgr.lastIndex = types.Index(len(entries))

	rm.mu.Lock()
	ps := rm.peerStates["node2"]
	ps.NextIndex = nextIndex
	ps.MatchIndex = 0
	ps.SnapshotInProgress = snapshotInProgress
	rm.mu.Unlock()

	return rm, logMgr
}

func TestRaftReplication_ReplicateToPeerInternal_ContextCancelled(t *testing.T) {
	rm, _ := setupReplicationWithPeer(t, 1, nil, false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rm.replicateToPeerInternal(ctx, "node2", 1, 0, false)
}

func TestRaftReplication_ReplicateToPeerInternal_WhenShutdown(t *testing.T) {
	rm, _ := setupReplicationWithPeer(t, 1, nil, false)

	rm.isShutdown.Store(true)
	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)
}

func TestRaftReplication_ReplicateToPeerInternal_SnapshotAlreadyInProgress(t *testing.T) {
	rm, _ := setupReplicationWithPeer(t, 0, nil, true) // nextIndex < firstLogIndex, snapshot active

	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)
}

func TestRaftReplication_ReplicateToPeerInternal_PeerAlreadyUpToDate(t *testing.T) {
	entries := map[types.Index]types.LogEntry{
		1: {Term: 1, Index: 1, Command: []byte("cmd")},
		2: {Term: 1, Index: 2, Command: []byte("cmd")},
		3: {Term: 1, Index: 3, Command: []byte("cmd")},
	}
	rm, logMgr := setupReplicationWithPeer(t, 4, entries, false) // NextIndex > lastIndex

	logMgr.lastIndex = 3
	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)
}

func TestReplicateToPeerInternal_PrevLogInfoUnavailable(t *testing.T) {
	entries := map[types.Index]types.LogEntry{
		2: {Term: 1, Index: 2, Command: []byte("cmd")},
	}
	rm, _ := setupReplicationWithPeer(t, 3, entries, false)

	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)
}

func TestRaftReplication_ReplicateToPeerInternal_EntriesCompacted_NilReturned(t *testing.T) {
	rm, logMgr := setupReplicationWithPeer(t, 1, nil, false)

	logMgr.lastIndex = 3
	logMgr.entries = map[types.Index]types.LogEntry{} // simulate compaction (empty)

	logMgr.appendEntriesFunc = func(ctx context.Context, entries []types.LogEntry) error {
		return nil
	}

	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)
}

func TestRaftReplication_ReplicateToPeerInternal_GetEntriesFailsWithCompaction(t *testing.T) {
	rm, logMgr := setupReplicationWithPeer(t, 1, nil, false)

	logMgr.lastIndex = 5
	logMgr.entries = map[types.Index]types.LogEntry{} // no data = compaction

	logMgr.appendEntriesFunc = func(ctx context.Context, entries []types.LogEntry) error {
		return errors.New("should not be called")
	}

	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)
}

func TestRaftReplication_ReplicateToPeerInternal_TriggersSnapshotWhenLogUnavailable(t *testing.T) {
	rm, logMgr, _, network, _, _ := setupTestReplicationManager(t)

	logMgr.lastIndex = 5
	logMgr.firstIndex = 3 // means entries at index 1–2 are compacted

	rm.mu.Lock()
	ps := rm.peerStates["node2"]
	ps.NextIndex = 1              // behind first index
	ps.SnapshotInProgress = false // snapshot should trigger
	ps.MatchIndex = 0
	rm.mu.Unlock()

	snapshotTriggered := make(chan struct{}, 1)

	rm.snapshotMgr = &mockSnapshotManager{
		snapshots: map[types.NodeID]bool{},
	}
	rm.snapshotMgr.(*mockSnapshotManager).sendSnapshotFunc = func(ctx context.Context, target types.NodeID, term types.Term) {
		snapshotTriggered <- struct{}{}
	}

	network.sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
		t.Fatal("AppendEntries should not be called when snapshot is initiated")
		return nil, nil
	}

	rm.replicateToPeerInternal(context.Background(), "node2", 1, 0, false)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	select {
	case <-snapshotTriggered: // Expected
	case <-ctx.Done():
		t.Fatal("expected snapshot to be triggered")
	}
}

func TestRaftReplication_ReplicateToPeerInternal_PrevLogInfoUnavailable_ReturnsEarly(t *testing.T) {
	rm, logMgr, _, network, _, _ := setupTestReplicationManager(t)

	logMgr.lastIndex = 5
	logMgr.entries = map[types.Index]types.LogEntry{
		3: {Term: 2, Index: 3, Command: []byte("cmd3")},
		4: {Term: 2, Index: 4, Command: []byte("cmd4")},
	}

	rm.mu.Lock()
	ps := rm.peerStates["node2"]
	ps.NextIndex = 3 // prevLogIndex = 2
	ps.MatchIndex = 0
	rm.mu.Unlock()

	called := false
	network.sendAppendEntriesFunc = func(ctx context.Context, peerID types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
		called = true
		return &types.AppendEntriesReply{Term: args.Term, Success: true}, nil
	}

	rm.replicateToPeerInternal(context.Background(), "node2", 2, 0, false)

	if called {
		t.Fatal("AppendEntries should not be called when prevLogInfo is unavailable")
	}
}

func TestRaftReplication_ReplicateToPeerInternal_PrevLogTermCompacted_StartsSnapshot(t *testing.T) {
	rm, logMgr, _, network, _, _ := setupTestReplicationManager(t)

	rm.mu.Lock()
	ps := rm.peerStates["node2"]
	ps.NextIndex = 3 // so prevLogIndex = 2
	ps.MatchIndex = 0
	rm.mu.Unlock()

	// Simulate compaction at index 2
	logMgr.lastIndex = 5
	logMgr.entries = map[types.Index]types.LogEntry{
		3: {Term: 2, Index: 3},
		4: {Term: 2, Index: 4},
	}
	logMgr.appendEntriesFunc = func(ctx context.Context, entries []types.LogEntry) error {
		return nil
	}

	logMgr.getTermFunc = func(ctx context.Context, index types.Index) (types.Term, error) {
		if index == 2 {
			return 0, ErrCompacted
		}
		return 0, nil
	}

	snapshotTriggered := make(chan struct{}, 1)
	rm.snapshotMgr = &mockSnapshotManager{
		sendSnapshotFunc: func(ctx context.Context, peerID types.NodeID, term types.Term) {
			snapshotTriggered <- struct{}{}
		},
	}

	network.sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
		t.Fatal("AppendEntries should not be called when snapshot is triggered due to compacted prevLogIndex")
		return nil, nil
	}

	rm.replicateToPeerInternal(context.Background(), "node2", 2, 0, false)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	select {
	case <-snapshotTriggered:
	case <-ctx.Done():
		t.Fatal("expected snapshot to be triggered due to compacted prevLogTerm")
	}
}

func TestRaftReplication_GetEntriesForPeer_DefaultMaxEntriesUsed(t *testing.T) {
	rm, logMgr := setupReplicationWithPeer(t, 1, nil, false)
	rm.cfg.Options.MaxLogEntriesPerRequest = 0 // force default

	logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
	logMgr.lastIndex = 1

	entries := rm.getEntriesForPeer(context.Background(), "node2", 1, 1, 1)
	if entries == nil || len(entries) != 1 {
		t.Fatal("expected one entry to be returned with default max entries")
	}
}

func TestRaftReplication_GetEntriesForPeer_EmptyRangeWarning(t *testing.T) {
	rm, _ := setupReplicationWithPeer(t, 5, nil, false)

	rm.cfg.Options.MaxLogEntriesPerRequest = 0

	entries := rm.getEntriesForPeer(context.Background(), "node2", 5, 4, 1)
	if entries != nil {
		t.Fatal("expected nil due to empty range")
	}
}

func TestRaftReplication_ReplicateToPeerInternal_SnapshotOnCompactedGetEntries(t *testing.T) {
	rm, logMgr, stateMgr, _, snapshotMgr, _ := setupTestReplicationManager(t)

	peerID := types.NodeID("node2")
	stateMgr.currentTerm = 1

	rm.mu.Lock()
	rm.peerStates[peerID] = &types.PeerState{NextIndex: 2}
	rm.mu.Unlock()

	logMgr.firstIndex = 1
	logMgr.lastIndex = 5
	logMgr.lastTerm = 1

	logMgr.getTermFunc = func(_ context.Context, idx types.Index) (types.Term, error) {
		if idx == 1 {
			return 1, nil
		}
		return 0, ErrNotFound
	}

	logMgr.getEntriesFunc = func(_ context.Context, start, _ types.Index) ([]types.LogEntry, error) {
		t.Logf("Simulating ErrCompacted on GetEntries for start=%d", start)
		if start == 2 {
			return nil, ErrCompacted
		}
		t.Fatalf("Unexpected GetEntries call: start=%d", start)
		return nil, nil
	}

	triggered := make(chan struct{}, 1)
	snapshotMgr.sendSnapshotFunc = func(_ context.Context, target types.NodeID, _ types.Term) {
		if target == peerID {
			triggered <- struct{}{}
		}
	}
	rm.snapshotMgr = snapshotMgr

	rm.replicateToPeerInternal(context.Background(), peerID, 1, 0, false)

	select {
	case <-triggered:
		t.Log("Snapshot successfully triggered.")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Expected snapshot to be triggered due to compacted log entries")
	}
}

func TestReplicateToPeerInternal_EmptyEntriesForValidRange(t *testing.T) {
	rm, logMgr, stateMgr, networkMgr, snapshotMgr, _ := setupTestReplicationManager(t)
	metrics := rm.metrics.(*mockMetrics)
	metrics.resetPeerReplicationCalls()

	peerID := types.NodeID("node2")
	stateMgr.currentTerm = 1

	rm.mu.Lock()
	rm.peerStates[peerID] = &types.PeerState{NextIndex: 2}
	rm.mu.Unlock()

	logMgr.firstIndex = 1
	logMgr.lastIndex = 5
	logMgr.lastTerm = 1

	logMgr.getTermFunc = func(_ context.Context, idx types.Index) (types.Term, error) {
		if idx == 1 {
			return 1, nil
		}
		return 0, ErrNotFound
	}

	logMgr.getEntriesFunc = func(_ context.Context, start, end types.Index) ([]types.LogEntry, error) {
		t.Logf("Mock GetEntries returning empty slice for [%d, %d)", start, end)
		return []types.LogEntry{}, nil
	}

	var appendCalled, snapshotCalled bool
	networkMgr.sendAppendEntriesFunc = func(_ context.Context, _ types.NodeID, _ *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
		appendCalled = true
		t.Error("AppendEntries should not be called")
		return nil, nil
	}
	snapshotMgr.sendSnapshotFunc = func(_ context.Context, _ types.NodeID, _ types.Term) {
		snapshotCalled = true
		t.Error("SendSnapshot should not be called")
	}

	rm.snapshotMgr = snapshotMgr

	rm.replicateToPeerInternal(context.Background(), peerID, 1, 0, false)

	if appendCalled {
		t.Error("Expected no AppendEntries call, but one occurred")
	}
	if snapshotCalled {
		t.Error("Expected no snapshot, but one was triggered")
	}

	time.Sleep(50 * time.Millisecond)

	id, success, reason, found := metrics.getLastPeerReplicationCall()
	if !found {
		t.Fatal("Expected ObservePeerReplication call, got none")
	}
	if id != peerID {
		t.Errorf("Wrong peer ID in metric: got %s, want %s", id, peerID)
	}
	if success {
		t.Error("Expected failure metric, got success")
	}
	if reason != ReplicationResultFailed {
		t.Errorf("Wrong replication result: got %s, want %s", reason, ReplicationResultFailed)
	}
}

func TestRaftReplication_HandleLogInconsistency(t *testing.T) {
	t.Run("ConflictTerm exists in leader log", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)

		logMgr.entries[1] = types.LogEntry{Term: 1, Index: 1}
		logMgr.entries[2] = types.LogEntry{Term: 2, Index: 2}
		logMgr.entries[3] = types.LogEntry{Term: 2, Index: 3}
		logMgr.entries[4] = types.LogEntry{Term: 3, Index: 4}
		logMgr.lastIndex = 4
		logMgr.lastTerm = 3

		setPeerState(t, rm, "node2", 4, 3)

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  2,
			ConflictIndex: 2,
		}

		rm.handleLogInconsistency("node2", 1, reply, false, 0)

		assertNextIndex(t, rm, "node2", 4)
	})

	t.Run("ConflictTerm not found in leader log", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)

		logMgr.entries[1] = types.LogEntry{Term: 1, Index: 1}
		logMgr.entries[2] = types.LogEntry{Term: 2, Index: 2}
		logMgr.entries[3] = types.LogEntry{Term: 2, Index: 3}
		logMgr.entries[4] = types.LogEntry{Term: 3, Index: 4}
		logMgr.lastIndex = 4
		logMgr.lastTerm = 3

		setPeerState(t, rm, "node2", 4, 3)

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  4, // Not present
			ConflictIndex: 2,
		}

		rm.handleLogInconsistency("node2", 1, reply, false, 0)

		assertNextIndex(t, rm, "node2", 2)
	})

	t.Run("No conflict term or index (legacy follower)", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		setPeerState(t, rm, "node2", 10, 5)

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: 0,
		}

		rm.handleLogInconsistency("node2", 1, reply, false, 0)

		assertNextIndex(t, rm, "node2", 9)
	})

	t.Run("MatchIndex hint provided", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		setPeerState(t, rm, "node2", 10, 5)

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictIndex: 7,
			MatchIndex:    8,
		}

		rm.handleLogInconsistency("node2", 1, reply, false, 0)

		assertNextIndex(t, rm, "node2", 7)
		assertMatchIndex(t, rm, "node2", 8)
	})

	t.Run("Unknown peer ID triggers error", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  2,
			ConflictIndex: 2,
		}

		rm.handleLogInconsistency("unknown-node", 1, reply, false, 0)
	})

	t.Run("ConflictTerm optimization skipped due to high index", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)

		logMgr.entries[1] = types.LogEntry{Term: 1, Index: 1}
		logMgr.entries[2] = types.LogEntry{Term: 2, Index: 2}
		logMgr.entries[3] = types.LogEntry{Term: 2, Index: 3}
		logMgr.lastIndex = 3
		logMgr.lastTerm = 2

		setPeerState(t, rm, "node2", 2, 0)

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  2,
			ConflictIndex: 1, // < conflictTermIndex
		}

		rm.handleLogInconsistency("node2", 1, reply, false, 0)

		assertNextIndex(t, rm, "node2", 1)
	})

	t.Run("Failed heartbeat triggers metric observation", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)

		setPeerState(t, rm, "node2", 5, 3)

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: 0,
		}

		rm.handleLogInconsistency("node2", 1, reply, true, 42*time.Millisecond)
	})
}

func setPeerState(t *testing.T, rm *replicationManager, nodeID types.NodeID, nextIndex, matchIndex types.Index) {
	t.Helper()
	rm.mu.Lock()
	defer rm.mu.Unlock()
	state := rm.peerStates[nodeID]
	state.NextIndex = nextIndex
	state.MatchIndex = matchIndex
}

func assertNextIndex(t *testing.T, rm *replicationManager, nodeID types.NodeID, expected types.Index) {
	t.Helper()
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	testutil.AssertEqual(t, expected, rm.peerStates[nodeID].NextIndex, "NextIndex mismatch")
}

func assertMatchIndex(t *testing.T, rm *replicationManager, nodeID types.NodeID, expected types.Index) {
	t.Helper()
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	testutil.AssertEqual(t, expected, rm.peerStates[nodeID].MatchIndex, "MatchIndex mismatch")
}

func TestRaftReplication_TriggerCommitCheck(t *testing.T) {
	t.Run("concurrent calls do not block", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)

		var wg sync.WaitGroup
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rm.triggerCommitCheck()
			}()
		}

		wg.Wait()

		select { // Drain to ensure it's not blocked
		case <-rm.notifyCommitCh:
		default:
			t.Error("Expected notification channel to have at least one item")
		}
	})

	t.Run("trigger does nothing if shutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)

		rm.isShutdown.Store(true) // simulate without closing
		rm.triggerCommitCheck()
	})
}

func TestRaftElection_LeaderLease_UpdateBehavior(t *testing.T) {
	tests := []struct {
		name                string
		leaderLeaseEnabled  bool
		advanceTime         time.Duration
		expectLeaseExtended bool
	}{
		{
			name:                "LeaseEnabled_ShouldExtend",
			leaderLeaseEnabled:  true,
			advanceTime:         1 * time.Second,
			expectLeaseExtended: true,
		},
		{
			name:                "LeaseDisabled_ShouldNotExtend",
			leaderLeaseEnabled:  false,
			advanceTime:         2 * time.Second,
			expectLeaseExtended: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm, _, _, _, _, clock := setupTestReplicationManager(t)

			startTime := clock.Now()

			rm.mu.Lock()
			rm.leaderLeaseEnabled = tt.leaderLeaseEnabled
			rm.leaseDuration = 10 * time.Second
			rm.leaseExpiry = startTime.Add(5 * time.Second)
			rm.lastQuorumHeartbeat = startTime
			rm.mu.Unlock()

			clock.advanceTime(tt.advanceTime)

			rm.updateLeaderLease()

			rm.mu.RLock()
			leaseExpiry := rm.leaseExpiry
			lastHeartbeat := rm.lastQuorumHeartbeat
			rm.mu.RUnlock()

			if tt.expectLeaseExtended {
				testutil.AssertTrue(t, leaseExpiry.After(startTime.Add(5*time.Second)),
					"Expected lease to be extended beyond original expiry")
				testutil.AssertTrue(t, lastHeartbeat.After(startTime),
					"Expected last quorum heartbeat time to be updated")
			} else {
				testutil.AssertEqual(t, startTime.Add(5*time.Second), leaseExpiry,
					"Lease expiry should not be modified when lease is disabled")
				testutil.AssertEqual(t, startTime, lastHeartbeat,
					"Last quorum heartbeat should not change when lease is disabled")
			}
		})
	}
}

func TestRaftReplication_UpdateLeaderLease_NoOpWhenDisabled(t *testing.T) {
	rm, _, _, _, _, _ := setupTestReplicationManager(t)

	rm.mu.Lock()
	rm.leaderLeaseEnabled = false
	before := rm.leaseExpiry
	rm.mu.Unlock()

	rm.updateLeaderLease()

	rm.mu.RLock()
	after := rm.leaseExpiry
	rm.mu.RUnlock()

	testutil.AssertEqual(t, before, after, "Lease expiry should not change when lease is disabled")
}

func TestRaftReplication_MaybeAdvanceCommitIndex(t *testing.T) {
	t.Run("AdvancesWhenQuorumReplicatedCurrentTermEntries", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.commitIndex = 0

		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1}
		logMgr.entries[3] = types.LogEntry{Index: 3, Term: 1}
		logMgr.lastIndex = 3
		logMgr.lastTerm = 1

		rm.mu.Lock()
		rm.peerStates["node2"].MatchIndex = 2
		rm.peerStates["node3"].MatchIndex = 1
		rm.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()
		testutil.AssertEqual(t, types.Index(2), stateMgr.commitIndex, "Commit index should advance to 2")

		rm.mu.Lock()
		rm.peerStates["node3"].MatchIndex = 3
		rm.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()
		testutil.AssertEqual(t, types.Index(3), stateMgr.commitIndex, "Commit index should advance to 3")
	})

	t.Run("SkipsOlderTermEntriesEvenWithQuorum", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.currentTerm = 2
		stateMgr.currentRole = types.RoleLeader

		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1}
		logMgr.entries[3] = types.LogEntry{Index: 3, Term: 2}
		logMgr.lastIndex = 3
		logMgr.lastTerm = 2

		rm.mu.Lock()
		rm.peerStates["node2"].MatchIndex = 2
		rm.peerStates["node3"].MatchIndex = 2
		rm.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()
		testutil.AssertEqual(t, types.Index(0), stateMgr.commitIndex, "Should not commit older term entries")

		rm.mu.Lock()
		rm.peerStates["node2"].MatchIndex = 3
		rm.peerStates["node3"].MatchIndex = 3
		rm.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()
		testutil.AssertEqual(t, types.Index(3), stateMgr.commitIndex, "Should commit index 3 from current term")
	})

	t.Run("NoopIfAlreadyShutdown", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		rm.isShutdown.Store(true)
		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		rm.MaybeAdvanceCommitIndex()
		testutil.AssertEqual(t, types.Index(0), stateMgr.commitIndex)
	})

	t.Run("NoopIfNotLeader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleFollower
		rm.MaybeAdvanceCommitIndex()
		testutil.AssertEqual(t, types.Index(0), stateMgr.commitIndex)
	})

	t.Run("NoopIfCommitAlreadyUpToDate", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.commitIndex = 2

		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1}
		logMgr.lastIndex = 2

		rm.mu.Lock()
		rm.peerStates["node2"].MatchIndex = 2
		rm.peerStates["node3"].MatchIndex = 2
		rm.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()
		testutil.AssertEqual(t, types.Index(2), stateMgr.commitIndex)
	})

	t.Run("SkipsCommitWhenTermCheckFails", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		stateMgr.currentTerm = 2
		stateMgr.currentRole = types.RoleLeader

		logMgr.getTermFunc = func(ctx context.Context, index types.Index) (types.Term, error) {
			return 0, fmt.Errorf("term lookup failed")
		}

		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 2}
		logMgr.lastIndex = 2

		rm.mu.Lock()
		rm.peerStates["node2"].MatchIndex = 2
		rm.peerStates["node3"].MatchIndex = 2
		rm.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()
		testutil.AssertEqual(t, types.Index(0), stateMgr.commitIndex, "Commit should not advance when term check fails")
	})

	t.Run("ReturnWhenPotentialCommitIndexIsNotGreater", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.commitIndex = 2

		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1}
		logMgr.entries[3] = types.LogEntry{Index: 3, Term: 1}
		logMgr.lastIndex = 3
		logMgr.lastTerm = 1

		rm.mu.Lock()
		// Quorum has only up to index 2, which is equal to commitIndex
		rm.peerStates["node2"].MatchIndex = 2
		rm.peerStates["node3"].MatchIndex = 2
		rm.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()

		testutil.AssertEqual(t, types.Index(2), stateMgr.commitIndex, "Should early return without advancing commit index")
	})
}

func TestRaftReplication_Propose(t *testing.T) {
	t.Run("SuccessWhenLeader", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = "node1"

		command := []byte("test command")
		index, term, isLeader, err := rm.Propose(context.Background(), command)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, isLeader)
		testutil.AssertEqual(t, types.Term(1), term)
		testutil.AssertEqual(t, types.Index(1), index)

		testutil.AssertEqual(t, types.Index(1), logMgr.lastIndex)
		testutil.AssertEqual(t, types.Term(1), logMgr.lastTerm)
		entry, found := logMgr.entries[1]
		testutil.AssertTrue(t, found)
		testutil.AssertEqual(t, command, entry.Command)
	})

	t.Run("FailsWhenNotLeader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "node2"

		_, _, isLeader, err := rm.Propose(context.Background(), []byte("cmd"))
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotLeader)
		testutil.AssertFalse(t, isLeader)
	})

	t.Run("SucceedsWithEmptyCommand", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = "node1"
		stateMgr.currentTerm = 1

		_, _, isLeader, err := rm.Propose(context.Background(), []byte{})
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, isLeader)
	})

	t.Run("FailsWhenLogAppendFails", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		stateMgr.currentRole = types.RoleLeader
		stateMgr.currentTerm = 1
		stateMgr.leaderID = "node1"

		expectedErr := errors.New("append failed")
		logMgr.appendEntriesFunc = func(ctx context.Context, entries []types.LogEntry) error {
			return expectedErr
		}

		_, _, isLeader, err := rm.Propose(context.Background(), []byte("fail cmd"))
		testutil.AssertErrorIs(t, err, expectedErr)
		testutil.AssertTrue(t, isLeader)
	})

	t.Run("FailsWhenShuttingDown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		rm.isShutdown.Store(true)

		_, _, isLeader, err := rm.Propose(context.Background(), []byte("cmd"))
		testutil.AssertErrorIs(t, err, ErrShuttingDown)
		testutil.AssertFalse(t, isLeader)
	})
}

func TestRaftReplication_ReplicateToAllPeers_AbortCases(t *testing.T) {
	t.Run("AbortsIfShuttingDown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		rm.isShutdown.Store(true)

		rm.replicateToAllPeers(context.Background(), 1)
	})

	t.Run("AbortsIfNoLongerLeaderOrWrongTerm", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

		originalTerm := types.Term(1) // stale term
		stateMgr.currentTerm = 2
		stateMgr.currentRole = types.RoleFollower // Not leader anymore
		stateMgr.leaderID = "node2"

		rm.replicateToAllPeers(context.Background(), originalTerm)
	})
}

func TestHasValidLease(t *testing.T) {
	t.Run("ValidLeaseWhenLeaderAndLeaseEnabled", func(t *testing.T) {
		rm, _, stateMgr, _, _, clock := setupTestReplicationManager(t)

		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = "node1"

		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseDuration = 10 * time.Second
		rm.leaseExpiry = clock.Now().Add(5 * time.Second) // Not yet expired
		rm.mu.Unlock()

		valid := rm.HasValidLease(context.Background())
		testutil.AssertTrue(t, valid, "Expected lease to be valid")
	})

	t.Run("LeaseExpired", func(t *testing.T) {
		rm, _, stateMgr, _, _, clock := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleLeader

		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(-1 * time.Second) // Already expired
		rm.mu.Unlock()

		valid := rm.HasValidLease(context.Background())
		testutil.AssertFalse(t, valid, "Expected lease to be expired")
	})

	t.Run("LeaseDisabled", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleLeader

		rm.mu.Lock()
		rm.leaderLeaseEnabled = false                     // explicitly disabled
		rm.leaseExpiry = time.Now().Add(10 * time.Second) // even if not expired
		rm.mu.Unlock()

		valid := rm.HasValidLease(context.Background())
		testutil.AssertFalse(t, valid, "Expected lease to be invalid when disabled")
	})

	t.Run("NotLeader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.currentRole = types.RoleFollower // not leader

		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = time.Now().Add(5 * time.Second)
		rm.mu.Unlock()

		valid := rm.HasValidLease(context.Background())
		testutil.AssertFalse(t, valid, "Expected lease to be invalid when not leader")
	})
}

func TestRaftReplication_VerifyLeadershipAndGetCommitIndex(t *testing.T) {
	t.Run("LeaderLease_FastPath", func(t *testing.T) {
		rm, _, stateMgr, _, _, clock := setupTestReplicationManager(t)

		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = rm.id
		stateMgr.commitIndex = 5

		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseDuration = 10 * time.Second
		rm.leaseExpiry = clock.Now().Add(5 * time.Second)
		rm.mu.Unlock()

		index, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, types.Index(5), index)
	})

	t.Run("LeaderLease_QuorumSuccess", func(t *testing.T) {
		rm, _, stateMgr, _, _, clock := setupTestReplicationManager(t)

		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = rm.id
		stateMgr.commitIndex = 5

		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(-1 * time.Second) // Expired
		rm.mu.Unlock()

		original := rm.networkMgr.(*mockNetworkManager).sendAppendEntriesFunc
		defer func() { rm.networkMgr.(*mockNetworkManager).sendAppendEntriesFunc = original }()
		rm.networkMgr.(*mockNetworkManager).sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
			return &types.AppendEntriesReply{Term: args.Term, Success: true}, nil
		}

		index, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, types.Index(5), index)
	})

	t.Run("LeaderLease_QuorumFails", func(t *testing.T) {
		rm, _, stateMgr, _, _, clock := setupTestReplicationManager(t)

		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = rm.id
		stateMgr.commitIndex = 5

		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(-1 * time.Second)
		rm.mu.Unlock()

		original := rm.networkMgr.(*mockNetworkManager).sendAppendEntriesFunc
		defer func() { rm.networkMgr.(*mockNetworkManager).sendAppendEntriesFunc = original }()
		rm.networkMgr.(*mockNetworkManager).sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
			return nil, fmt.Errorf("unreachable")
		}

		index, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "read index quorum verification failed")
		testutil.AssertErrorIs(t, errors.Unwrap(err), ErrQuorumUnreachable)
		testutil.AssertEqual(t, types.Index(0), index)
	})

	t.Run("NotLeader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "node2"

		_, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())
		testutil.AssertErrorIs(t, err, ErrNotLeader)
	})

	t.Run("ShuttingDown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		rm.isShutdown.Store(true)

		_, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())
		testutil.AssertErrorIs(t, err, ErrShuttingDown)
	})

	t.Run("QuorumTimeout", func(t *testing.T) {
		rm, _, stateMgr, networkMgrMock, _, _ := setupTestReplicationManager(t)
		rm.leaderLeaseEnabled = false

		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = rm.id

		networkMgrMock.sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := rm.VerifyLeadershipAndGetCommitIndex(ctx)
		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "read index quorum check timed out")
		testutil.AssertErrorIs(t, errors.Unwrap(err), ErrTimeout)
	})
}

func TestRaftReplication_VerifyLeadershipViaQuorum(t *testing.T) {
	tests := []struct {
		name             string
		cancelContext    bool
		mockSendFunc     func() func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error)
		expectedQuorumOK bool
	}{
		{
			name:          "ContextCancelled",
			cancelContext: true,
			mockSendFunc: func() func(context.Context, types.NodeID, *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
				return nil // unused
			},
			expectedQuorumOK: false,
		},
		{
			name: "InsufficientResponses",
			mockSendFunc: func() func(context.Context, types.NodeID, *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
				return func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
					return nil, fmt.Errorf("unreachable")
				}
			},
			expectedQuorumOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
			stateMgr.currentTerm = 2
			stateMgr.currentRole = types.RoleLeader
			stateMgr.leaderID = rm.id

			ctx := context.Background()
			if tt.cancelContext {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			}

			if tt.mockSendFunc != nil {
				rm.networkMgr.(*mockNetworkManager).sendAppendEntriesFunc = tt.mockSendFunc()
			}

			ok := rm.verifyLeadershipViaQuorum(ctx, 2)
			testutil.AssertEqual(t, tt.expectedQuorumOK, ok)
		})
	}
}

func TestRaftReplication_VerifyLeadershipViaQuorum_HigherTermResponse(t *testing.T) {
	rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

	stateMgr.currentTerm = 2
	stateMgr.currentRole = types.RoleLeader
	stateMgr.leaderID = rm.id

	rm.networkMgr.(*mockNetworkManager).sendAppendEntriesFunc = func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
		return &types.AppendEntriesReply{Term: args.Term + 1, Success: false}, nil
	}

	ctx := context.Background()
	ok := rm.verifyLeadershipViaQuorum(ctx, 2)
	testutil.AssertFalse(t, ok)
}

func TestRaftReplication_HandleAppendEntries(t *testing.T) {
	defaultArgs := func(term types.Term, leaderID types.NodeID, entries ...types.LogEntry) *types.AppendEntriesArgs {
		return &types.AppendEntriesArgs{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      entries,
			LeaderCommit: 0,
		}
	}

	t.Run("ShutdownNode", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		rm.isShutdown.Store(true)

		args := defaultArgs(1, "leader1")
		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertErrorIs(t, err, ErrShuttingDown)
		testutil.AssertNil(t, reply)
	})

	t.Run("StaleTerm_InitialCheck", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		args := defaultArgs(4, "leader1")

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, reply)
		testutil.AssertFalse(t, reply.Success)
		testutil.AssertEqual(t, types.Term(5), reply.Term)
	})

	t.Run("StepDownOnHigherTerm_OrRoleChange", func(t *testing.T) {
		rm, logMgr, stateMgrMock, _, _, _ := setupTestReplicationManager(t)

		initialTerm := types.Term(5)
		stateMgrMock.mu.Lock()
		stateMgrMock.currentTerm = initialTerm
		stateMgrMock.currentRole = types.RoleCandidate
		stateMgrMock.leaderID = ""
		stateMgrMock.mu.Unlock()

		rpcTerm := types.Term(6)
		leaderID := types.NodeID("newLeader")
		args := defaultArgs(rpcTerm, leaderID, types.LogEntry{Term: rpcTerm, Index: 1, Command: []byte("cmd1")})
		args.LeaderCommit = 0

		stateMgrMock.checkTermAndStepDownFunc = func(ctx context.Context, rpcTermCalled types.Term, rpcLeaderCalled types.NodeID) (bool, types.Term) {
			stateMgrMock.mu.Lock()
			defer stateMgrMock.mu.Unlock()
			if rpcTermCalled >= stateMgrMock.currentTerm {
				stateMgrMock.currentTerm = rpcTermCalled
				stateMgrMock.currentRole = types.RoleFollower
				stateMgrMock.leaderID = rpcLeaderCalled
				return true, initialTerm
			}
			return false, initialTerm
		}
		defer func() { stateMgrMock.checkTermAndStepDownFunc = nil }()

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, reply)
		testutil.AssertTrue(t, reply.Success)
		testutil.AssertEqual(t, rpcTerm, reply.Term)

		term, role, leader := stateMgrMock.GetState()
		testutil.AssertEqual(t, rpcTerm, term)
		testutil.AssertEqual(t, types.RoleFollower, role)
		testutil.AssertEqual(t, leaderID, leader)
		testutil.AssertEqual(t, types.Index(1), logMgr.lastIndex)
	})

	t.Run("StaleTerm_UnderLock", func(t *testing.T) {
		rm, _, stateMgrMock, _, _, _ := setupTestReplicationManager(t)

		rpcTerm := types.Term(5)
		termUnderLock := types.Term(6)

		stateMgrMock.mu.Lock()
		stateMgrMock.currentTerm = termUnderLock
		stateMgrMock.currentRole = types.RoleFollower
		stateMgrMock.leaderID = "leader1"
		stateMgrMock.mu.Unlock()

		stateMgrMock.checkTermAndStepDownFunc = func(ctx context.Context, rpcTermCalled types.Term, rpcLeaderCalled types.NodeID) (bool, types.Term) {
			return false, termUnderLock
		}
		defer func() { stateMgrMock.checkTermAndStepDownFunc = nil }()

		args := defaultArgs(rpcTerm, "leader1")

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, reply)
		testutil.AssertFalse(t, reply.Success)
		testutil.AssertEqual(t, termUnderLock, reply.Term)
	})

	t.Run("ProcessEntriesError_AppendFailed", func(t *testing.T) {
		rm, logMgrMock, stateMgrMock, _, _, _ := setupTestReplicationManager(t)

		rpcTerm := types.Term(5)
		stateMgrMock.mu.Lock()
		stateMgrMock.currentTerm = rpcTerm
		stateMgrMock.currentRole = types.RoleFollower
		stateMgrMock.leaderID = "leader1"
		stateMgrMock.mu.Unlock()

		logMgrMock.mu.Lock()
		logMgrMock.entries = map[types.Index]types.LogEntry{1: {Term: rpcTerm, Index: 1}}
		logMgrMock.firstIndex = 1
		logMgrMock.lastIndex = 1
		logMgrMock.lastTerm = rpcTerm
		logMgrMock.mu.Unlock()

		args := &types.AppendEntriesArgs{
			Term:         rpcTerm,
			LeaderID:     "leader1",
			PrevLogIndex: 1,
			PrevLogTerm:  rpcTerm,
			Entries:      []types.LogEntry{{Term: rpcTerm, Index: 2, Command: []byte("cmd")}},
			LeaderCommit: 1,
		}

		expectedErr := errors.New("simulated failure")
		logMgrMock.appendEntriesFunc = func(ctx context.Context, entries []types.LogEntry) error {
			return expectedErr
		}
		defer func() { logMgrMock.appendEntriesFunc = nil }()

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertError(t, err)
		testutil.AssertTrue(t, strings.Contains(err.Error(), "failed to process/append"))
		testutil.AssertErrorIs(t, errors.Unwrap(err), expectedErr)
		testutil.AssertNotNil(t, reply)
		testutil.AssertFalse(t, reply.Success)
		testutil.AssertEqual(t, rpcTerm, reply.Term)
	})

	t.Run("ProcessEntriesError_TruncateFailed", func(t *testing.T) {
		rm, logMgrMock, stateMgrMock, _, _, _ := setupTestReplicationManager(t)

		rpcTerm := types.Term(5)
		stateMgrMock.mu.Lock()
		stateMgrMock.currentTerm = rpcTerm
		stateMgrMock.currentRole = types.RoleFollower
		stateMgrMock.leaderID = "leader1"
		stateMgrMock.mu.Unlock()

		logMgrMock.mu.Lock()
		logMgrMock.entries = map[types.Index]types.LogEntry{
			1: {Term: rpcTerm, Index: 1},
			2: {Term: 4, Index: 2},
		}
		logMgrMock.firstIndex = 1
		logMgrMock.lastIndex = 2
		logMgrMock.lastTerm = 4
		logMgrMock.mu.Unlock()

		args := &types.AppendEntriesArgs{
			Term:         rpcTerm,
			LeaderID:     "leader1",
			PrevLogIndex: 1,
			PrevLogTerm:  rpcTerm,
			Entries:      []types.LogEntry{{Term: rpcTerm, Index: 2, Command: []byte("fix")}},
			LeaderCommit: 1,
		}

		expectedErr := errors.New("truncate failed")
		logMgrMock.truncateSuffixFunc = func(ctx context.Context, newLastIndexPlusOne types.Index) error {
			return expectedErr
		}
		defer func() { logMgrMock.truncateSuffixFunc = nil }()

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertError(t, err)
		testutil.AssertTrue(t, strings.Contains(err.Error(), "failed to process/append"))
		testutil.AssertNotNil(t, reply)
		testutil.AssertFalse(t, reply.Success)
		testutil.AssertEqual(t, rpcTerm, reply.Term)
	})

	t.Run("IdempotentAppend", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "leader1"
		stateMgr.mu.Unlock()

		entry := types.LogEntry{Term: 5, Index: 1, Command: []byte("cmd")}
		logMgr.AppendEntries(context.Background(), []types.LogEntry{entry})

		args := defaultArgs(5, "leader1", entry)
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
		args.LeaderCommit = 1

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, reply.Success)
		testutil.AssertEqual(t, types.Index(1), reply.MatchIndex)
	})

	t.Run("AppendWithMiddleConflict", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 6
		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "leader1"
		stateMgr.mu.Unlock()

		logMgr.AppendEntries(context.Background(), []types.LogEntry{
			{Term: 6, Index: 1, Command: []byte("cmd1")},
			{Term: 6, Index: 2, Command: []byte("cmd2-old")},
		})

		args := defaultArgs(6, "leader1",
			types.LogEntry{Term: 6, Index: 2, Command: []byte("cmd2-new")},
			types.LogEntry{Term: 6, Index: 3, Command: []byte("cmd3")},
		)
		args.PrevLogIndex = 1
		args.PrevLogTerm = 6

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, reply.Success)
		testutil.AssertEqual(t, types.Index(3), reply.MatchIndex)
	})

	t.Run("CommitIndexUpdatedByLeader", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 7
		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "leaderX"
		stateMgr.mu.Unlock()

		logMgr.AppendEntries(context.Background(), []types.LogEntry{
			{Term: 7, Index: 1},
			{Term: 7, Index: 2},
		})

		args := defaultArgs(7, "leaderX")
		args.PrevLogIndex = 2
		args.PrevLogTerm = 7
		args.LeaderCommit = 2

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, reply.Success)
		testutil.AssertEqual(t, types.Index(2), stateMgr.GetCommitIndex())
	})

	t.Run("AppendBeyondLocalLog", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 10
		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "leaderX"
		stateMgr.mu.Unlock()

		entries := []types.LogEntry{
			{Term: 10, Index: 1},
			{Term: 10, Index: 2},
			{Term: 10, Index: 3},
		}

		args := defaultArgs(10, "leaderX", entries...)
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
		args.LeaderCommit = 3

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, reply.Success)
		testutil.AssertEqual(t, types.Index(3), reply.MatchIndex)
		testutil.AssertEqual(t, types.Index(3), stateMgr.GetCommitIndex())
	})
}

func TestRaftReplication_GetPeerReplicationStatusUnsafe(t *testing.T) {
	rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)

	stateMgr.currentTerm = 1
	stateMgr.currentRole = types.RoleLeader
	stateMgr.leaderID = "node1"

	logMgr.lastIndex = 10

	rm.mu.Lock()
	rm.peerStates["node2"] = &types.PeerState{
		NextIndex:  8,
		MatchIndex: 7,
		IsActive:   true,
	}
	rm.peerStates["node3"] = &types.PeerState{
		NextIndex:  6,
		MatchIndex: 5,
		IsActive:   false,
	}
	rm.mu.Unlock()

	t.Run("Leader returns correct replication status", func(t *testing.T) {
		status := rm.GetPeerReplicationStatusUnsafe()
		testutil.AssertEqual(t, 2, len(status), "Should return two peer statuses")

		node2, ok := status["node2"]
		testutil.AssertTrue(t, ok, "node2 should be in peer status")
		testutil.AssertEqual(t, types.Index(8), node2.NextIndex)
		testutil.AssertEqual(t, types.Index(7), node2.MatchIndex)
		testutil.AssertTrue(t, node2.IsActive)
		testutil.AssertEqual(t, types.Index(3), node2.ReplicationLag)

		node3, ok := status["node3"]
		testutil.AssertTrue(t, ok, "node3 should be in peer status")
		testutil.AssertEqual(t, types.Index(6), node3.NextIndex)
		testutil.AssertEqual(t, types.Index(5), node3.MatchIndex)
		testutil.AssertFalse(t, node3.IsActive)
		testutil.AssertEqual(t, types.Index(5), node3.ReplicationLag)
	})

	t.Run("Follower returns empty status map", func(t *testing.T) {
		stateMgr.currentRole = types.RoleFollower
		status := rm.GetPeerReplicationStatusUnsafe()
		testutil.AssertEqual(t, 0, len(status), "Follower should return no replication status")
	})

	t.Run("Shutdown returns empty status map", func(t *testing.T) {
		stateMgr.currentRole = types.RoleLeader // reset role to leader
		rm.isShutdown.Store(true)
		status := rm.GetPeerReplicationStatusUnsafe()
		testutil.AssertEqual(t, 0, len(status), "Shutdown should return no replication status")
	})
}

func TestRaftReplication_SetPeerSnapshotInProgress(t *testing.T) {
	rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
	rmID := rm.id

	stateMgr.currentTerm = 1
	stateMgr.currentRole = types.RoleLeader
	stateMgr.leaderID = rmID

	rm.mu.Lock()
	peer, ok := rm.peerStates["node2"]
	if !ok {
		t.Fatalf("peerStates does not contain node2")
	}
	peer.SnapshotInProgress = false
	rm.mu.Unlock()

	t.Run("Set to true", func(t *testing.T) {
		rm.SetPeerSnapshotInProgress("node2", true)

		rm.mu.RLock()
		defer rm.mu.RUnlock()
		testutil.AssertTrue(t, rm.peerStates["node2"].SnapshotInProgress, "Expected SnapshotInProgress to be true")
	})

	t.Run("Set to false", func(t *testing.T) {
		rm.SetPeerSnapshotInProgress("node2", false)

		rm.mu.RLock()
		defer rm.mu.RUnlock()
		testutil.AssertFalse(t, rm.peerStates["node2"].SnapshotInProgress, "Expected SnapshotInProgress to be false")
	})

	t.Run("No-op on shutdown", func(t *testing.T) {
		rm.SetPeerSnapshotInProgress("node2", true)
		rm.isShutdown.Store(true)

		rm.SetPeerSnapshotInProgress("node2", false) // should be ignored

		rm.mu.RLock()
		defer rm.mu.RUnlock()
		testutil.AssertTrue(t, rm.peerStates["node2"].SnapshotInProgress, "Expected no change due to shutdown")
	})

	t.Run("Graceful handling of missing peer", func(t *testing.T) {
		rm.isShutdown.Store(false)

		rm.SetPeerSnapshotInProgress("nonexistent-node", true)
	})
}

func TestReplicateToPeer(t *testing.T) {
	rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
	rmID := rm.id

	t.Run("Leader should send AppendEntries", func(t *testing.T) {
		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = rmID

		logMgr.lastIndex = 1
		logMgr.lastTerm = 1
		logMgr.entries[1] = types.LogEntry{Term: 1, Index: 1, Command: []byte("entry1")}

		rm.mu.RLock()
		psNode2, ok := rm.peerStates["node2"]
		if !ok {
			rm.mu.RUnlock()
			t.Fatalf("peerStates does not contain node2")
		}
		if psNode2.NextIndex != 1 {
			rm.mu.RUnlock()
			t.Fatalf("Expected peerStates[\"node2\"].NextIndex to be 1, got %d", psNode2.NextIndex)
		}
		rm.mu.RUnlock()

		var wg sync.WaitGroup
		wg.Add(1)

		mockNet := rm.networkMgr.(*mockNetworkManager)
		mockNet.setWaitGroup(&wg)
		mockNet.getAndResetCallCounts()

		rm.ReplicateToPeer(context.Background(), "node2", false)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			appendEntries, _ := mockNet.getAndResetCallCounts()
			testutil.AssertEqual(t, 1, appendEntries, "Should call AppendEntries once")
		case <-time.After(1 * time.Second):
			t.Fatal("Timed out waiting for replication to complete")
		}
	})

	t.Run("Follower should not replicate", func(t *testing.T) {
		mockNet := rm.networkMgr.(*mockNetworkManager)
		mockNet.getAndResetCallCounts()

		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "someOtherLeader"

		rm.ReplicateToPeer(context.Background(), "node2", false)

		time.Sleep(20 * time.Millisecond)
		appendEntries, _ := mockNet.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries, "Should not call AppendEntries when not leader")
	})
}

func TestUpdatePeerAfterSnapshotSend(t *testing.T) {
	peerID := types.NodeID("node2")

	t.Run("NormalUpdate_SnapshotIndexAdvancesMatchIndex", func(t *testing.T) {
		rm, _, _, _, _, clockMock := setupTestReplicationManager(t)
		initialTime := clockMock.Now()

		rm.mu.Lock()
		ps, ok := rm.peerStates[peerID]
		if !ok {
			t.Fatalf("Peer %s not found in peerStates after setup", peerID)
		}
		ps.MatchIndex = 5
		ps.NextIndex = 6
		ps.SnapshotInProgress = true
		ps.IsActive = false
		ps.LastActive = time.Time{}
		rm.mu.Unlock()

		newSnapshotIndex := types.Index(10)
		rm.UpdatePeerAfterSnapshotSend(peerID, newSnapshotIndex)

		rm.mu.RLock()
		updatedPs := rm.peerStates[peerID]
		rm.mu.RUnlock()

		testutil.AssertEqual(t, newSnapshotIndex, updatedPs.MatchIndex, "MatchIndex should be updated to snapshotIndex")
		testutil.AssertEqual(t, newSnapshotIndex+1, updatedPs.NextIndex, "NextIndex should be snapshotIndex + 1")
		testutil.AssertFalse(t, updatedPs.SnapshotInProgress, "SnapshotInProgress should be false")
		testutil.AssertTrue(t, updatedPs.IsActive, "IsActive should be true")
		testutil.AssertTrue(t, updatedPs.LastActive.After(initialTime) || updatedPs.LastActive.Equal(initialTime), "LastActive should be updated to current clock time")

		select {
		case <-rm.notifyCommitCh:
		case <-time.After(50 * time.Millisecond):
			t.Error("Expected triggerCommitCheck to send on notifyCommitCh")
		}
	})

	t.Run("SnapshotIndexDoesNotAdvanceMatchIndex", func(t *testing.T) {
		rm, _, _, _, _, clockMock := setupTestReplicationManager(t)
		initialTime := clockMock.Now()

		currentMatchIndex := types.Index(15)
		rm.mu.Lock()
		ps := rm.peerStates[peerID]
		ps.MatchIndex = currentMatchIndex
		ps.NextIndex = currentMatchIndex + 1
		ps.SnapshotInProgress = true
		ps.IsActive = false
		rm.mu.Unlock()

		lowerSnapshotIndex := types.Index(10)
		rm.UpdatePeerAfterSnapshotSend(peerID, lowerSnapshotIndex)

		rm.mu.RLock()
		updatedPs := rm.peerStates[peerID]
		rm.mu.RUnlock()

		testutil.AssertEqual(t, currentMatchIndex, updatedPs.MatchIndex, "MatchIndex should NOT be updated as snapshotIndex is lower")
		testutil.AssertEqual(t, lowerSnapshotIndex+1, updatedPs.NextIndex, "NextIndex should be snapshotIndex + 1")
		testutil.AssertFalse(t, updatedPs.SnapshotInProgress, "SnapshotInProgress should be false")
		testutil.AssertTrue(t, updatedPs.IsActive, "IsActive should be true")
		testutil.AssertTrue(t, updatedPs.LastActive.After(initialTime) || updatedPs.LastActive.Equal(initialTime), "LastActive should be updated")

		select {
		case <-rm.notifyCommitCh:
		case <-time.After(50 * time.Millisecond):
			t.Error("Expected triggerCommitCheck to send on notifyCommitCh")
		}
	})

	t.Run("PeerNotFound", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		unknownPeerID := types.NodeID("unknownPeer")

		rm.mu.Lock()
		delete(rm.peerStates, unknownPeerID)
		rm.mu.Unlock()

		rm.UpdatePeerAfterSnapshotSend(unknownPeerID, 10)

		rm.mu.RLock()
		_, exists := rm.peerStates[unknownPeerID]
		rm.mu.RUnlock()
		testutil.AssertFalse(t, exists, "Peer state for unknown peer should not have been created")

		select {
		case <-rm.notifyCommitCh:
			t.Error("triggerCommitCheck should not have been called for a non-existent peer")
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("ShutdownNode", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)

		rm.mu.Lock()
		ps := rm.peerStates[peerID]
		originalMatchIndex := types.Index(5)
		originalNextIndex := types.Index(6)
		ps.MatchIndex = originalMatchIndex
		ps.NextIndex = originalNextIndex
		ps.SnapshotInProgress = true
		ps.IsActive = false
		rm.mu.Unlock()

		rm.isShutdown.Store(true)

		rm.UpdatePeerAfterSnapshotSend(peerID, 10)

		rm.mu.RLock()
		currentPs := rm.peerStates[peerID]
		rm.mu.RUnlock()

		testutil.AssertEqual(t, originalMatchIndex, currentPs.MatchIndex, "MatchIndex should not change on shutdown")
		testutil.AssertEqual(t, originalNextIndex, currentPs.NextIndex, "NextIndex should not change on shutdown")
		testutil.AssertTrue(t, currentPs.SnapshotInProgress, "SnapshotInProgress should not change on shutdown")
		testutil.AssertFalse(t, currentPs.IsActive, "IsActive should not change on shutdown")

		// triggerCommitCheck should not be called if shutdown
		select {
		case <-rm.notifyCommitCh:
			t.Error("triggerCommitCheck should not have been called on shutdown")
		case <-time.After(50 * time.Millisecond):
			// Expected
		}
	})

	t.Run("UpdateSetsLastActiveTimeCorrectly", func(t *testing.T) {
		rm, _, _, _, _, clockMock := setupTestReplicationManager(t)

		specificTime := time.Date(2025, 5, 13, 10, 0, 0, 0, time.UTC)
		clockMock.nowVal = specificTime

		rm.mu.Lock()
		ps := rm.peerStates[peerID]
		ps.LastActive = time.Time{}
		rm.mu.Unlock()

		rm.UpdatePeerAfterSnapshotSend(peerID, 10)

		rm.mu.RLock()
		updatedPs := rm.peerStates[peerID]
		rm.mu.RUnlock()

		testutil.AssertEqual(t, specificTime, updatedPs.LastActive, "LastActive should be set to the mock clock's current time")
	})
}
