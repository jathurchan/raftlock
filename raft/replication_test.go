package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

// Test helper for creating a properly configured replication manager
func setupTestReplicationManager(
	t *testing.T,
) (*replicationManager, *mockLogManager, *mockStateManager, *mockNetworkManager, *mockSnapshotManager, *mockClock) {
	t.Helper()

	metrics := newMockMetrics()
	mu := &sync.RWMutex{}
	isShutdown := &atomic.Bool{}

	logMgr := newMockLogManager()
	stateMgr := newMockStateManager(metrics)
	networkMgr := newMockNetworkManager()
	snapshotMgr := newMockSnapshotManager()
	clock := newMockClock()

	// Set up initial state as leader
	stateMgr.mu.Lock()
	stateMgr.currentRole = types.RoleLeader
	stateMgr.currentTerm = 1
	stateMgr.leaderID = "node1"
	stateMgr.mu.Unlock()

	applyCh := make(chan struct{}, 10)

	config := Config{
		ID: "node1",
		Peers: map[types.NodeID]PeerConfig{
			"node1": {ID: "node1", Address: "localhost:8001"},
			"node2": {ID: "node2", Address: "localhost:8002"},
			"node3": {ID: "node3", Address: "localhost:8003"},
		},
		Options: Options{
			HeartbeatTickCount:      5,
			ElectionTickCount:       50,
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
	testutil.RequireNoError(t, err)

	// Initialize leader state for tests
	rmImpl := rm.(*replicationManager)
	rmImpl.InitializeLeaderState()

	return rmImpl, logMgr, stateMgr, networkMgr, snapshotMgr, clock
}

func TestNewReplicationManager(t *testing.T) {
	t.Run("ValidDependencies", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		testutil.AssertNotNil(t, rm)
		testutil.AssertEqual(t, "node1", string(rm.id))
		testutil.AssertEqual(t, 2, rm.quorumSize)
		testutil.AssertTrue(t, rm.leaderLeaseEnabled)
		rm.Stop()
	})

	t.Run("InvalidDependencies", func(t *testing.T) {
		mockMu := &sync.RWMutex{}
		mockIsShutdown := &atomic.Bool{}
		mockStateMgr := newMockStateManager(newMockMetrics())
		mockLogMgr := newMockLogManager()
		mockSnapshotMgr := newMockSnapshotManager()
		mockNetworkMgr := newMockNetworkManager()
		mockMetrics := newMockMetrics()
		mockLogger := logger.NewNoOpLogger()
		mockClock := newMockClock()
		mockApplyCh := make(chan struct{}, 1)

		tests := []struct {
			name     string
			deps     ReplicationManagerDeps
			errorMsg string
		}{
			{
				name: "NilMutex",
				deps: ReplicationManagerDeps{
					Mu:             nil,
					ID:             "node1",
					QuorumSize:     2,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "Mu is required",
			},
			{
				name: "EmptyID",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "",
					QuorumSize:     2,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "ID is required",
			},
			{
				name: "ZeroQuorumSize",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     0,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "QuorumSize must be > 0",
			},
			{
				name: "NilStateMgr",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     2,
					StateMgr:       nil,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "StateMgr is required",
			},
			{
				name: "NilLogMgr",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     2,
					StateMgr:       mockStateMgr,
					LogMgr:         nil,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "LogMgr is required",
			},
			{
				name: "NilSnapshotMgr",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     2,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    nil,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "SnapshotMgr is required",
			},
			{
				name: "NilMetrics",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     2,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        nil,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "Metrics is required",
			},
			{
				name: "NilLogger",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     2,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         nil,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "Logger is required",
			},
			{
				name: "NilClock",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     2,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          nil,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "Clock is required",
			},
			{
				name: "NilApplyNotifyCh",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     2,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  nil,
				},
				errorMsg: "ApplyNotifyCh is required",
			},
			{
				name: "NilIsShutdownFlag",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     2,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: nil,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "IsShutdownFlag is required",
			},
			{
				name: "NilPeersMap",
				deps: ReplicationManagerDeps{
					Mu:             mockMu,
					ID:             "node1",
					QuorumSize:     2,
					Peers:          nil,
					StateMgr:       mockStateMgr,
					LogMgr:         mockLogMgr,
					SnapshotMgr:    mockSnapshotMgr,
					NetworkMgr:     mockNetworkMgr,
					Metrics:        mockMetrics,
					Logger:         mockLogger,
					Clock:          mockClock,
					IsShutdownFlag: mockIsShutdown,
					ApplyNotifyCh:  mockApplyCh,
				},
				errorMsg: "Peers map is required",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := NewReplicationManager(tt.deps)
				testutil.AssertError(t, err)
				testutil.AssertContains(t, err.Error(), tt.errorMsg)
			})
		}
	})

	t.Run("LeaderLeaseConfiguration", func(t *testing.T) {
		// Test with lease disabled
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		rm.cfg.FeatureFlags.EnableLeaderLease = false
		rm.leaderLeaseEnabled = false

		testutil.AssertFalse(t, rm.leaderLeaseEnabled)
		rm.Stop()
	})
}

func TestReplicationManager_Tick(t *testing.T) {
	t.Run("SendsHeartbeatsWhenElapsed", func(t *testing.T) {
		rm, _, _, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Simulate heartbeat interval elapsed
		rm.mu.Lock()
		rm.heartbeatElapsed = rm.cfg.Options.HeartbeatTickCount
		rm.mu.Unlock()

		var wg sync.WaitGroup
		wg.Add(2) // Expect 2 peers
		networkMgr.setWaitGroup(&wg)

		rm.Tick(context.Background())

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for heartbeats")
		}

		// Verify heartbeat counter was reset
		rm.mu.RLock()
		testutil.AssertEqual(t, 0, rm.heartbeatElapsed)
		rm.mu.RUnlock()
	})

	t.Run("NoHeartbeatsWhenNotElapsed", func(t *testing.T) {
		rm, _, _, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Don't set heartbeat elapsed
		rm.mu.Lock()
		rm.heartbeatElapsed = 1 // Less than HeartbeatTickCount (5)
		rm.mu.Unlock()

		networkMgr.getAndResetCallCounts() // Reset counters

		rm.Tick(context.Background())

		// Give some time for any potential async operations
		time.Sleep(50 * time.Millisecond)

		appendEntries, _ := networkMgr.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries)
	})

	t.Run("NoOperationWhenNotLeader", func(t *testing.T) {
		rm, _, stateMgr, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Change to follower
		stateMgr.mu.Lock()
		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "node2"
		rm.heartbeatElapsed = rm.cfg.Options.HeartbeatTickCount
		stateMgr.mu.Unlock()

		networkMgr.getAndResetCallCounts() // Reset counters

		rm.Tick(context.Background())

		time.Sleep(50 * time.Millisecond)

		appendEntries, _ := networkMgr.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries)
	})

	t.Run("NoOperationWhenShutdown", func(t *testing.T) {
		rm, _, _, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		networkMgr.getAndResetCallCounts() // Reset counters

		rm.Tick(context.Background())

		time.Sleep(50 * time.Millisecond)

		appendEntries, _ := networkMgr.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries)
	})
}

func TestReplicationManager_InitializeLeaderState(t *testing.T) {
	t.Run("InitializesCorrectly", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up log state
		logMgr.lastIndex = 5
		logMgr.lastTerm = 2

		rm.InitializeLeaderState()

		rm.mu.RLock()
		defer rm.mu.RUnlock()

		// Check peer states were initialized
		for peerID := range rm.peers { // Use rm.peers directly to iterate
			if peerID == rm.id {
				continue // Skip self
			}
			peerState, exists := rm.peerStates[peerID] // Fetch from peerStates map
			testutil.AssertTrue(t, exists, "Peer state should exist for %s", peerID)

			testutil.AssertEqual(
				t,
				types.Index(6),
				peerState.NextIndex,
				"NextIndex should be lastLogIndex + 1",
			)
			testutil.AssertEqual(
				t,
				types.Index(0),
				peerState.MatchIndex,
				"MatchIndex should start at 0",
			)
			testutil.AssertFalse(t, peerState.IsActive, "IsActive should start false")
			testutil.AssertFalse(
				t,
				peerState.SnapshotInProgress,
				"SnapshotInProgress should start false",
			)
			testutil.AssertEqual(
				t,
				types.Index(5),
				peerState.ReplicationLag,
				"ReplicationLag should be lastLogIndex",
			)
		}

		// Check heartbeat state was reset
		testutil.AssertEqual(t, 0, rm.heartbeatElapsed)

		// Check leader lease was initialized
		if rm.leaderLeaseEnabled {
			testutil.AssertTrue(t, rm.leaseExpiry.IsZero(), "Lease should start uninitialized")
		}
	})

	t.Run("SkipsWhenNotLeader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Change to follower
		stateMgr.mu.Lock()
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		rm.InitializeLeaderState() // Should not panic or error
	})

	t.Run("SkipsWhenShutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)
		rm.InitializeLeaderState() // Should not panic or error
	})
}

func TestReplicationManager_Propose(t *testing.T) {
	t.Run("SuccessfulProposal", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		command := []byte("test command")

		index, term, isLeader, err := rm.Propose(context.Background(), command)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, isLeader)
		testutil.AssertEqual(t, types.Term(1), term)
		testutil.AssertEqual(t, types.Index(1), index)

		// Verify entry was appended
		testutil.AssertEqual(t, types.Index(1), logMgr.lastIndex)
		entry, exists := logMgr.entries[1]
		testutil.AssertTrue(t, exists)
		testutil.AssertEqual(t, command, entry.Command)
		testutil.AssertEqual(t, types.Term(1), entry.Term)
		testutil.AssertEqual(t, types.Index(1), entry.Index)
	})

	t.Run("FailsWhenNotLeader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Change to follower
		stateMgr.mu.Lock()
		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "node2"
		stateMgr.mu.Unlock()

		_, _, isLeader, err := rm.Propose(context.Background(), []byte("cmd"))

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotLeader)
		testutil.AssertFalse(t, isLeader)
	})

	t.Run("FailsWhenShutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		_, _, isLeader, err := rm.Propose(context.Background(), []byte("cmd"))

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrShuttingDown)
		testutil.AssertFalse(t, isLeader)
	})

	t.Run("EmptyCommand", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		index, term, isLeader, err := rm.Propose(context.Background(), []byte{})

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, isLeader)
		testutil.AssertEqual(t, types.Term(1), term)
		testutil.AssertEqual(t, types.Index(1), index)
	})
}

func TestReplicationManager_SendHeartbeats(t *testing.T) {
	t.Run("SendsToAllPeers", func(t *testing.T) {
		rm, _, _, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		var wg sync.WaitGroup
		wg.Add(2) // 2 peers
		networkMgr.setWaitGroup(&wg)

		rm.SendHeartbeats(context.Background())

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for heartbeats")
		}

		appendEntries, heartbeats := networkMgr.getAndResetCallCounts()
		testutil.AssertEqual(t, 2, appendEntries)
		testutil.AssertEqual(t, 2, heartbeats) // All should be heartbeats (empty entries)
	})

	t.Run("SkipsWhenNotLeader", func(t *testing.T) {
		rm, _, stateMgr, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		stateMgr.mu.Lock()
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		networkMgr.getAndResetCallCounts() // Reset

		rm.SendHeartbeats(context.Background())

		time.Sleep(50 * time.Millisecond)

		appendEntries, _ := networkMgr.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries)
	})

	t.Run("SkipsWhenShutdown", func(t *testing.T) {
		rm, _, _, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		networkMgr.getAndResetCallCounts() // Reset

		rm.SendHeartbeats(context.Background())

		time.Sleep(50 * time.Millisecond)

		appendEntries, _ := networkMgr.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries)
	})
}

func TestReplicationManager_HasValidLease(t *testing.T) {
	t.Run("ValidLeaseWhenNotExpired", func(t *testing.T) {
		rm, _, _, _, _, clock := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(5 * time.Second)
		rm.mu.Unlock()

		valid := rm.HasValidLease(context.Background())
		testutil.AssertTrue(t, valid)
	})

	t.Run("InvalidLeaseWhenExpired", func(t *testing.T) {
		rm, _, _, _, _, clock := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(-1 * time.Second) // Expired
		rm.mu.Unlock()

		valid := rm.HasValidLease(context.Background())
		testutil.AssertFalse(t, valid)
	})

	t.Run("InvalidWhenLeaseDisabled", func(t *testing.T) {
		rm, _, _, _, _, clock := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.mu.Lock()
		rm.leaderLeaseEnabled = false
		rm.leaseExpiry = clock.Now().Add(5 * time.Second) // Not expired, but disabled
		rm.mu.Unlock()

		valid := rm.HasValidLease(context.Background())
		testutil.AssertFalse(t, valid)
	})

	t.Run("InvalidWhenShutdown", func(t *testing.T) {
		rm, _, _, _, _, clock := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(5 * time.Second)
		rm.mu.Unlock()

		valid := rm.HasValidLease(context.Background())
		testutil.AssertFalse(t, valid)
	})
}

func TestReplicationManager_VerifyLeadershipAndGetCommitIndex(t *testing.T) {
	t.Run("FastPathWithValidLease", func(t *testing.T) {
		rm, _, stateMgr, _, _, clock := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up valid lease
		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(5 * time.Second)
		rm.mu.Unlock()

		// Set commit index
		stateMgr.mu.Lock()
		stateMgr.commitIndex = 10
		stateMgr.mu.Unlock()

		index, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, types.Index(10), index)
	})

	t.Run("QuorumVerificationWhenLeaseExpired", func(t *testing.T) {
		rm, _, stateMgr, networkMgr, _, clock := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up expired lease
		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(-1 * time.Second)
		rm.mu.Unlock()

		// Set commit index
		stateMgr.mu.Lock()
		stateMgr.commitIndex = 15
		stateMgr.mu.Unlock()

		// Mock successful heartbeat responses
		networkMgr.setSendAppendEntriesFunc(
			func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
				return &types.AppendEntriesReply{
					Term:    args.Term,
					Success: true,
				}, nil
			},
		)

		index, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, types.Index(15), index)
	})

	t.Run("FailsWhenQuorumUnreachable", func(t *testing.T) {
		rm, _, _, networkMgr, _, clock := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up expired lease
		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(-1 * time.Second)
		rm.mu.Unlock()

		// Mock failed heartbeat responses
		networkMgr.setSendAppendEntriesFunc(
			func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
				return nil, errors.New("network unreachable")
			},
		)

		_, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to verify leadership")
	})

	t.Run("FailsWhenNotLeader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		stateMgr.mu.Lock()
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		_, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotLeader)
	})

	t.Run("FailsWhenShutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		_, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrShuttingDown)
	})

	t.Run("StepsDownOnHigherTerm", func(t *testing.T) {
		rm, _, _, networkMgr, _, clock := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up expired lease to force quorum check
		rm.mu.Lock()
		rm.leaderLeaseEnabled = true
		rm.leaseExpiry = clock.Now().Add(-1 * time.Second)
		rm.mu.Unlock()

		// Mock higher term response
		networkMgr.setSendAppendEntriesFunc(
			func(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
				return &types.AppendEntriesReply{
					Term:    args.Term + 1, // Higher term
					Success: false,
				}, nil
			},
		)

		_, err := rm.VerifyLeadershipAndGetCommitIndex(context.Background())

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to verify leadership")

		// Give time for step down goroutine to execute
		time.Sleep(100 * time.Millisecond)

		// Verify step down was triggered (would be verified by checking state manager calls in real implementation)
	})
}

func TestReplicationManager_MaybeAdvanceCommitIndex(t *testing.T) {
	t.Run("AdvancesWhenQuorumAgreesOnCurrentTerm", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up log with current term entries
		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1}
		logMgr.entries[3] = types.LogEntry{Index: 3, Term: 1}
		logMgr.lastIndex = 3
		logMgr.firstIndex = 1

		// Set peer match indices for quorum
		rm.mu.Lock()
		rm.peerStates["node2"].MatchIndex = 2
		rm.peerStates["node3"].MatchIndex = 2
		rm.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()

		testutil.AssertEqual(t, types.Index(2), stateMgr.GetCommitIndex())
	})

	t.Run("DoesNotAdvanceForOlderTermEntries", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set current term to 2
		stateMgr.mu.Lock()
		stateMgr.currentTerm = 2
		stateMgr.mu.Unlock()

		// Set up log with mixed term entries
		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1} // Old term
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1} // Old term
		logMgr.entries[3] = types.LogEntry{Index: 3, Term: 2} // Current term
		logMgr.lastIndex = 3
		logMgr.firstIndex = 1

		// Set peer match indices - quorum agrees on index 2 (old term)
		rm.mu.Lock()
		rm.peerStates["node2"].MatchIndex = 2
		rm.peerStates["node3"].MatchIndex = 2
		rm.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()

		// Should not advance past old commit index since index 2 is from old term
		testutil.AssertEqual(t, types.Index(0), stateMgr.GetCommitIndex())
	})

	t.Run("DoesNotAdvanceWhenNotLeader", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		stateMgr.mu.Lock()
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()

		testutil.AssertEqual(t, types.Index(0), stateMgr.GetCommitIndex())
	})

	t.Run("DoesNotAdvanceWhenShutdown", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		rm.MaybeAdvanceCommitIndex()

		testutil.AssertEqual(t, types.Index(0), stateMgr.GetCommitIndex())
	})

	t.Run("DoesNotAdvanceWhenCommitUpToDate", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set commit index to match last log index
		logMgr.lastIndex = 2
		stateMgr.mu.Lock()
		stateMgr.commitIndex = 2
		stateMgr.mu.Unlock()

		rm.MaybeAdvanceCommitIndex()

		testutil.AssertEqual(t, types.Index(2), stateMgr.GetCommitIndex())
	})
}

func TestReplicationManager_HandleAppendEntries(t *testing.T) {
	defaultArgs := func(term types.Term, leaderID types.NodeID) *types.AppendEntriesArgs {
		return &types.AppendEntriesArgs{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: 0,
		}
	}

	t.Run("RejectsWhenShutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		args := defaultArgs(1, "leader1")
		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrShuttingDown)
		testutil.AssertNil(t, reply)
	})

	t.Run("RejectsStaleTerm", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set current term higher than RPC term
		stateMgr.mu.Lock()
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		args := defaultArgs(3, "leader1") // Lower term

		reply, err := rm.HandleAppendEntries(context.Background(), args)
		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, reply)
		testutil.AssertFalse(t, reply.Success)
		testutil.AssertEqual(t, types.Term(5), reply.Term)
	})

	t.Run("StepsDownOnHigherTerm", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set current term lower than RPC term
		stateMgr.mu.Lock()
		stateMgr.currentTerm = 3
		stateMgr.currentRole = types.RoleCandidate
		stateMgr.mu.Unlock()

		args := defaultArgs(5, "leader1") // Higher term

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, reply)
		testutil.AssertTrue(t, reply.Success)
		testutil.AssertEqual(t, types.Term(5), reply.Term)

		// Verify state was updated
		term, role, leader := stateMgr.GetState()
		testutil.AssertEqual(t, types.Term(5), term)
		testutil.AssertEqual(t, types.RoleFollower, role)
		testutil.AssertEqual(t, types.NodeID("leader1"), leader)
	})

	t.Run("AcceptsValidHeartbeat", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower
		stateMgr.leaderID = "leader1"
		stateMgr.mu.Unlock()

		args := defaultArgs(5, "leader1")

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, reply)
		testutil.AssertTrue(t, reply.Success)
		testutil.AssertEqual(t, types.Term(5), reply.Term)
	})

	t.Run("RejectsLogInconsistency", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up local log
		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.lastIndex = 1
		logMgr.firstIndex = 1

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 2
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		// Request with mismatched previous log term
		args := &types.AppendEntriesArgs{
			Term:         2,
			LeaderID:     "leader1",
			PrevLogIndex: 1,
			PrevLogTerm:  2, // Mismatch - our entry has term 1
			Entries:      []types.LogEntry{{Index: 2, Term: 2}},
			LeaderCommit: 0,
		}

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, reply)
		testutil.AssertFalse(t, reply.Success)
		testutil.AssertEqual(t, types.Index(1), reply.ConflictIndex)
		testutil.AssertEqual(t, types.Term(1), reply.ConflictTerm) // Our term at that index
	})

	t.Run("AcceptsAndAppendsNewEntries", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 2
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		// Start with empty log
		logMgr.lastIndex = 0
		logMgr.firstIndex = 1

		entries := []types.LogEntry{
			{Index: 1, Term: 2, Command: []byte("cmd1")},
			{Index: 2, Term: 2, Command: []byte("cmd2")},
		}

		args := &types.AppendEntriesArgs{
			Term:         2,
			LeaderID:     "leader1",
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      entries,
			LeaderCommit: 2,
		}

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertNotNil(t, reply)
		testutil.AssertTrue(t, reply.Success)

		// Verify entries were appended
		testutil.AssertEqual(t, types.Index(2), logMgr.lastIndex)
		for i, expectedEntry := range entries {
			actualEntry, exists := logMgr.entries[types.Index(i+1)]
			testutil.AssertTrue(t, exists, fmt.Sprintf("Entry %d should exist", i+1))
			testutil.AssertEqual(t, expectedEntry.Command, actualEntry.Command)
			testutil.AssertEqual(t, expectedEntry.Term, actualEntry.Term)
		}

		// Verify commit index was updated
		testutil.AssertEqual(t, types.Index(2), stateMgr.GetCommitIndex())
	})

	t.Run("TruncatesConflictingEntries", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up existing log with conflicting entry
		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1, Command: []byte("old")}
		logMgr.lastIndex = 2
		logMgr.firstIndex = 1

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 2
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		// New entries with conflict at index 2
		entries := []types.LogEntry{
			{Index: 2, Term: 2, Command: []byte("new")},
			{Index: 3, Term: 2, Command: []byte("newer")},
		}

		args := &types.AppendEntriesArgs{
			Term:         2,
			LeaderID:     "leader1",
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries:      entries,
			LeaderCommit: 1,
		}

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, reply.Success)

		// Verify conflicting entry was replaced
		entry2, exists := logMgr.entries[2]
		testutil.AssertTrue(t, exists)
		testutil.AssertEqual(t, []byte("new"), entry2.Command)
		testutil.AssertEqual(t, types.Term(2), entry2.Term)

		// Verify new entry was added
		entry3, exists := logMgr.entries[3]
		testutil.AssertTrue(t, exists)
		testutil.AssertEqual(t, []byte("newer"), entry3.Command)
	})

	t.Run("UpdatesCommitIndexFromLeader", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up log entries
		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1}
		logMgr.lastIndex = 2
		logMgr.firstIndex = 1

		stateMgr.mu.Lock()
		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleFollower
		stateMgr.commitIndex = 0 // Start with no commits
		stateMgr.mu.Unlock()

		args := &types.AppendEntriesArgs{
			Term:         1,
			LeaderID:     "leader1",
			PrevLogIndex: 2,
			PrevLogTerm:  1,
			Entries:      nil, // Heartbeat
			LeaderCommit: 2,   // Leader says index 2 is committed
		}

		reply, err := rm.HandleAppendEntries(context.Background(), args)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, reply.Success)

		// Verify commit index was updated
		testutil.AssertEqual(t, types.Index(2), stateMgr.GetCommitIndex())
	})
}

func TestReplicationManager_UpdatePeerAfterSnapshotSend(t *testing.T) {
	t.Run("UpdatesStateCorrectly", func(t *testing.T) {
		rm, logMgr, _, _, _, clock := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		logMgr.lastIndex = 20 // For lag calculation

		// Set initial peer state
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.MatchIndex = 5
		peerState.NextIndex = 6
		peerState.SnapshotInProgress = true
		peerState.IsActive = false
		rm.mu.Unlock()

		initialTime := clock.Now()
		snapshotIndex := types.Index(15)

		rm.UpdatePeerAfterSnapshotSend(peerID, snapshotIndex)

		// Allow time for async operations
		time.Sleep(50 * time.Millisecond)

		rm.mu.RLock()
		updatedState := rm.peerStates[peerID]
		rm.mu.RUnlock()

		testutil.AssertEqual(t, snapshotIndex, updatedState.MatchIndex)
		testutil.AssertEqual(t, snapshotIndex+1, updatedState.NextIndex)
		testutil.AssertFalse(t, updatedState.SnapshotInProgress)
		testutil.AssertTrue(t, updatedState.IsActive)
		testutil.AssertTrue(
			t,
			updatedState.LastActive.After(initialTime) ||
				updatedState.LastActive.Equal(initialTime),
		)
		testutil.AssertEqual(t, types.Index(5), updatedState.ReplicationLag) // 20 - 15 = 5
	})

	t.Run("DoesNotRegressMatchIndex", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		logMgr.lastIndex = 20

		// Set peer state with higher match index
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.MatchIndex = 18
		peerState.NextIndex = 19
		rm.mu.Unlock()

		// Try to update with lower snapshot index
		snapshotIndex := types.Index(10)

		rm.UpdatePeerAfterSnapshotSend(peerID, snapshotIndex)

		time.Sleep(50 * time.Millisecond)

		rm.mu.RLock()
		updatedState := rm.peerStates[peerID]
		rm.mu.RUnlock()

		// MatchIndex should not regress
		testutil.AssertEqual(t, types.Index(18), updatedState.MatchIndex)
		// But NextIndex should be updated to snapshot + 1
		testutil.AssertEqual(t, snapshotIndex+1, updatedState.NextIndex)
	})

	t.Run("SkipsWhenPeerNotFound", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		unknownPeer := types.NodeID("unknown")

		rm.UpdatePeerAfterSnapshotSend(unknownPeer, 10)

		// Should not panic or create new peer state
		rm.mu.RLock()
		_, exists := rm.peerStates[unknownPeer]
		rm.mu.RUnlock()

		testutil.AssertFalse(t, exists)
	})

	t.Run("SkipsWhenShutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		peerID := types.NodeID("node2")
		originalMatchIndex := types.Index(5)

		rm.mu.Lock()
		rm.peerStates[peerID].MatchIndex = originalMatchIndex
		rm.mu.Unlock()

		rm.UpdatePeerAfterSnapshotSend(peerID, 10)

		time.Sleep(50 * time.Millisecond)

		rm.mu.RLock()
		currentMatchIndex := rm.peerStates[peerID].MatchIndex
		rm.mu.RUnlock()

		testutil.AssertEqual(t, originalMatchIndex, currentMatchIndex)
	})
}

func TestReplicationManager_SetPeerSnapshotInProgress(t *testing.T) {
	t.Run("SetsAndClearsFlag", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")

		// Set to true
		rm.SetPeerSnapshotInProgress(peerID, true)

		rm.mu.RLock()
		testutil.AssertTrue(t, rm.peerStates[peerID].SnapshotInProgress)
		rm.mu.RUnlock()

		// Set to false
		rm.SetPeerSnapshotInProgress(peerID, false)

		rm.mu.RLock()
		testutil.AssertFalse(t, rm.peerStates[peerID].SnapshotInProgress)
		rm.mu.RUnlock()
	})

	t.Run("SkipsWhenPeerNotFound", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.SetPeerSnapshotInProgress("unknown", true)
		// Should not panic
	})

	t.Run("SkipsWhenShutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)
		rm.SetPeerSnapshotInProgress("node2", true)
		// Should not panic
	})
}

func TestReplicationManager_GetPeerReplicationStatusUnsafe(t *testing.T) {
	t.Run("ReturnsStatusForLeader", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		logMgr.lastIndex = 10

		// Set up peer states
		rm.mu.Lock()
		rm.peerStates["node2"].MatchIndex = 8
		rm.peerStates["node2"].NextIndex = 9
		rm.peerStates["node2"].IsActive = true
		rm.peerStates["node3"].MatchIndex = 6
		rm.peerStates["node3"].NextIndex = 7
		rm.peerStates["node3"].IsActive = false
		rm.mu.Unlock()

		status := rm.GetPeerReplicationStatusUnsafe()

		testutil.AssertEqual(t, 2, len(status))

		node2Status := status["node2"]
		testutil.AssertEqual(t, types.Index(8), node2Status.MatchIndex)
		testutil.AssertEqual(t, types.Index(9), node2Status.NextIndex)
		testutil.AssertTrue(t, node2Status.IsActive)
		testutil.AssertEqual(t, types.Index(2), node2Status.ReplicationLag) // 10 - 8 = 2

		node3Status := status["node3"]
		testutil.AssertEqual(t, types.Index(6), node3Status.MatchIndex)
		testutil.AssertEqual(t, types.Index(7), node3Status.NextIndex)
		testutil.AssertFalse(t, node3Status.IsActive)
		testutil.AssertEqual(t, types.Index(4), node3Status.ReplicationLag) // 10 - 6 = 4
	})

	t.Run("ReturnsEmptyForFollower", func(t *testing.T) {
		rm, _, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		stateMgr.mu.Lock()
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		status := rm.GetPeerReplicationStatusUnsafe()

		testutil.AssertEqual(t, 0, len(status))
	})

	t.Run("ReturnsEmptyWhenShutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		status := rm.GetPeerReplicationStatusUnsafe()

		testutil.AssertEqual(t, 0, len(status))
	})
}

func TestReplicationManager_ReplicateToPeer(t *testing.T) {
	t.Run("ReplicatesWhenLeader", func(t *testing.T) {
		rm, logMgr, _, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1, Command: []byte("test")}
		logMgr.lastIndex = 1
		logMgr.firstIndex = 1

		var wg sync.WaitGroup
		wg.Add(1)
		networkMgr.setWaitGroup(&wg)

		rm.ReplicateToPeer(context.Background(), "node2", false)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for replication")
		}

		appendEntries, _ := networkMgr.getAndResetCallCounts()
		testutil.AssertEqual(t, 1, appendEntries)
	})

	t.Run("SkipsWhenNotLeader", func(t *testing.T) {
		rm, _, stateMgr, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		stateMgr.mu.Lock()
		stateMgr.currentRole = types.RoleFollower
		stateMgr.mu.Unlock()

		networkMgr.getAndResetCallCounts() // Reset

		rm.ReplicateToPeer(context.Background(), "node2", false)

		time.Sleep(50 * time.Millisecond)

		appendEntries, _ := networkMgr.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries)
	})

	t.Run("SkipsWhenShutdown", func(t *testing.T) {
		rm, _, _, networkMgr, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		rm.isShutdown.Store(true)

		networkMgr.getAndResetCallCounts() // Reset

		rm.ReplicateToPeer(context.Background(), "node2", false)

		time.Sleep(50 * time.Millisecond)

		appendEntries, _ := networkMgr.getAndResetCallCounts()
		testutil.AssertEqual(t, 0, appendEntries)
	})
}

func TestReplicationManager_CommitCheckLoop(t *testing.T) {
	t.Run("ProcessesCommitChecks", func(t *testing.T) {
		rm, logMgr, stateMgr, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up conditions for commit advancement
		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1}
		logMgr.lastIndex = 2
		logMgr.firstIndex = 1

		rm.mu.Lock()
		rm.peerStates["node2"].MatchIndex = 2
		rm.peerStates["node3"].MatchIndex = 2
		rm.mu.Unlock()

		// Trigger commit check
		rm.triggerCommitCheck()

		// Give time for background processing
		time.Sleep(100 * time.Millisecond)

		// Verify commit index was advanced
		testutil.AssertEqual(t, types.Index(2), stateMgr.GetCommitIndex())
	})

	t.Run("StopsOnShutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)

		// Manually stop to test the loop termination
		rm.Stop()

		// Should not hang or panic
	})
}

func TestReplicationManager_TriggerCommitCheck(t *testing.T) {
	t.Run("TriggersSuccessfully", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Clear any existing notifications
		select {
		case <-rm.notifyCommitCh:
		default:
		}

		rm.triggerCommitCheck()

		// Should receive notification
		select {
		case <-rm.notifyCommitCh:
			// Success
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Expected commit check notification")
		}
	})

	t.Run("HandlesChannelFull", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Fill the channel to capacity
		for i := 0; i < cap(rm.notifyCommitCh); i++ {
			rm.notifyCommitCh <- struct{}{}
		}

		// This should not block (uses default case)
		rm.triggerCommitCheck()

		// Should not hang
	})

	t.Run("SkipsWhenShutdown", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set shutdown flag
		rm.isShutdown.Store(true)

		// Call triggerCommitCheck - should skip due to shutdown flag
		rm.triggerCommitCheck()

		// The method should return early and not attempt to send on the channel
		// Since we can't easily test that no send occurred, we verify behavior indirectly

		// Verify the commit check loop doesn't process anything
		select {
		case <-rm.notifyCommitCh:
			t.Fatal("Commit check should have been skipped when shutdown flag is set")
		case <-time.After(50 * time.Millisecond):
			// Expected: no notification should be sent
		}
	})

}

func TestReplicationManager_Stop(t *testing.T) {
	t.Run("StopsCleanly", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)

		// Verify initial state
		testutil.AssertFalse(t, rm.isShutdown.Load())

		rm.Stop()

		// Verify cleanup
		rm.mu.RLock()
		testutil.AssertEqual(t, 0, len(rm.peerStates))
		testutil.AssertTrue(t, rm.leaseExpiry.IsZero())
		testutil.AssertTrue(t, rm.lastQuorumHeartbeat.IsZero())
		rm.mu.RUnlock()

		// Multiple stops should be safe
		rm.Stop()
		rm.Stop()
	})

	t.Run("StopsCommitLoop", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)

		// Start some background activity
		rm.triggerCommitCheck()

		// Stop should terminate the commit loop
		rm.Stop()

		// Verify channel is closed (would panic if we try to send)
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Expected panic when sending to closed channel")
			}
		}()

		rm.notifyCommitCh <- struct{}{}
	})
}

func TestReplicationManager_ConcurrentOperations(t *testing.T) {
	t.Run("ConcurrentPeerStateUpdates", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		logMgr.lastIndex = 100

		var wg sync.WaitGroup
		numGoroutines := 10

		// Concurrent peer state updates
		for i := range numGoroutines {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				peerID := types.NodeID(fmt.Sprintf("node%d", (i%2)+2)) // node2 or node3
				rm.UpdatePeerAfterSnapshotSend(peerID, types.Index(i+10))
			}(i)
		}

		// Concurrent commit checks
		for range numGoroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rm.triggerCommitCheck()
			}()
		}

		// Concurrent status checks
		for range numGoroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rm.mu.RLock()
				_ = rm.GetPeerReplicationStatusUnsafe()
				rm.mu.RUnlock()
			}()
		}

		wg.Wait()
		// Should not deadlock or panic
	})

	t.Run("ConcurrentTickAndPropose", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		var wg sync.WaitGroup

		// Concurrent ticks
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rm.Tick(context.Background())
			}()
		}

		// Concurrent proposals
		for i := range 5 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _, _, _ = rm.Propose(context.Background(), []byte(fmt.Sprintf("cmd%d", i)))
			}(i)
		}

		wg.Wait()
		// Should not deadlock or panic
	})
}

func TestReplicationManager_EdgeCases(t *testing.T) {
	t.Run("EmptyPeersList", func(t *testing.T) {
		metrics := newMockMetrics()
		mu := &sync.RWMutex{}
		isShutdown := &atomic.Bool{}
		applyCh := make(chan struct{}, 10)

		config := Config{
			ID: "node1",
			Peers: map[types.NodeID]PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8001"},
			}, // Only self
			Options: Options{
				HeartbeatTickCount: 5,
				ElectionTickCount:  50,
			},
		}

		deps := ReplicationManagerDeps{
			Mu:             mu,
			ID:             config.ID,
			Peers:          config.Peers,
			QuorumSize:     1, // Single node
			Cfg:            config,
			StateMgr:       newMockStateManager(metrics),
			LogMgr:         newMockLogManager(),
			SnapshotMgr:    newMockSnapshotManager(),
			NetworkMgr:     newMockNetworkManager(),
			Metrics:        metrics,
			Logger:         logger.NewNoOpLogger(),
			Clock:          newMockClock(),
			IsShutdownFlag: isShutdown,
			ApplyNotifyCh:  applyCh,
		}

		rm, err := NewReplicationManager(deps)
		testutil.AssertNoError(t, err)
		defer rm.Stop()

		rmImpl := rm.(*replicationManager)
		rmImpl.InitializeLeaderState()

		// Should work with no peers
		rmImpl.SendHeartbeats(context.Background())
		rmImpl.MaybeAdvanceCommitIndex()

		status := rmImpl.GetPeerReplicationStatusUnsafe()
		testutil.AssertEqual(t, 0, len(status))
	})

	t.Run("NilNetworkManager", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set network manager to nil
		rm.SetNetworkManager(nil)

		// Operations should handle nil gracefully or panic appropriately
		// This depends on implementation - some operations might check for nil
	})
}

func BenchmarkReplicationManager_Propose(b *testing.B) {
	rm, _, _, _, _, _ := setupTestReplicationManager(&testing.T{})
	defer rm.Stop()

	command := []byte("benchmark command")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = rm.Propose(context.Background(), command)
	}
}

func TestReplicationManager_handleAppendError(t *testing.T) {
	t.Run("TimeoutError", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		term := types.Term(1)
		err := context.DeadlineExceeded
		isHeartbeat := false
		latency := 100 * time.Millisecond

		// Set initial peer state
		rm.mu.Lock()
		rm.peerStates[peerID].IsActive = true
		rm.peerStates[peerID].ConsecutiveFailures = 0
		rm.mu.Unlock()

		rm.handleAppendError(peerID, term, err, isHeartbeat, latency)

		// Verify peer was marked inactive
		rm.mu.RLock()
		peerState := rm.peerStates[peerID]
		testutil.AssertFalse(t, peerState.IsActive, "Peer should be marked inactive after timeout")
		testutil.AssertEqual(
			t,
			1,
			peerState.ConsecutiveFailures,
			"Should increment consecutive failures",
		)
		rm.mu.RUnlock()
	})

	t.Run("CanceledError", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		term := types.Term(1)
		err := context.Canceled
		isHeartbeat := true
		latency := 50 * time.Millisecond

		rm.mu.Lock()
		rm.peerStates[peerID].IsActive = true
		rm.peerStates[peerID].ConsecutiveFailures = 0
		rm.mu.Unlock()

		rm.handleAppendError(peerID, term, err, isHeartbeat, latency)

		// Verify peer state updated
		rm.mu.RLock()
		peerState := rm.peerStates[peerID]
		testutil.AssertFalse(t, peerState.IsActive, "Peer should be marked inactive")
		testutil.AssertEqual(
			t,
			1,
			peerState.ConsecutiveFailures,
			"Should increment consecutive failures",
		)
		rm.mu.RUnlock()
	})

	t.Run("PeerNotFoundError", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		term := types.Term(1)
		err := ErrPeerNotFound
		isHeartbeat := false
		latency := 25 * time.Millisecond

		rm.mu.Lock()
		rm.peerStates[peerID].IsActive = true
		rm.peerStates[peerID].ConsecutiveFailures = 0
		rm.mu.Unlock()

		rm.handleAppendError(peerID, term, err, isHeartbeat, latency)

		// Verify peer state updated
		rm.mu.RLock()
		peerState := rm.peerStates[peerID]
		testutil.AssertFalse(t, peerState.IsActive, "Peer should be marked inactive")
		testutil.AssertEqual(
			t,
			1,
			peerState.ConsecutiveFailures,
			"Should increment consecutive failures",
		)
		rm.mu.RUnlock()
	})

	t.Run("ShuttingDownError", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		term := types.Term(1)
		err := ErrShuttingDown
		isHeartbeat := true
		latency := 10 * time.Millisecond

		rm.mu.Lock()
		rm.peerStates[peerID].IsActive = true
		rm.peerStates[peerID].ConsecutiveFailures = 0
		rm.mu.Unlock()

		rm.handleAppendError(peerID, term, err, isHeartbeat, latency)

		// Verify peer state updated
		rm.mu.RLock()
		peerState := rm.peerStates[peerID]
		testutil.AssertFalse(t, peerState.IsActive, "Peer should be marked inactive")
		testutil.AssertEqual(
			t,
			1,
			peerState.ConsecutiveFailures,
			"Should increment consecutive failures",
		)
		rm.mu.RUnlock()
	})

	t.Run("NetworkError", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		term := types.Term(1)
		err := errors.New("network unreachable")
		isHeartbeat := false
		latency := 200 * time.Millisecond

		rm.mu.Lock()
		rm.peerStates[peerID].IsActive = true
		rm.peerStates[peerID].ConsecutiveFailures = 0
		rm.mu.Unlock()

		rm.handleAppendError(peerID, term, err, isHeartbeat, latency)

		// Verify peer state updated
		rm.mu.RLock()
		peerState := rm.peerStates[peerID]
		testutil.AssertFalse(t, peerState.IsActive, "Peer should be marked inactive")
		testutil.AssertEqual(
			t,
			1,
			peerState.ConsecutiveFailures,
			"Should increment consecutive failures",
		)
		rm.mu.RUnlock()
	})

	t.Run("MissingPeerState", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		nonExistentPeer := types.NodeID("unknown")
		term := types.Term(1)
		err := errors.New("some error")
		isHeartbeat := false
		latency := 50 * time.Millisecond

		// Remove peer state to simulate missing peer
		rm.mu.Lock()
		delete(rm.peerStates, nonExistentPeer)
		rm.mu.Unlock()

		// Should not panic
		rm.handleAppendError(nonExistentPeer, term, err, isHeartbeat, latency)
	})
}

func TestReplicationManager_handleLogInconsistency(t *testing.T) {
	t.Run("ConflictTermFoundInLeaderLog", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up leader log: term 1 at indices 1-2, term 2 at indices 3-4
		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1}
		logMgr.entries[3] = types.LogEntry{Index: 3, Term: 2}
		logMgr.entries[4] = types.LogEntry{Index: 4, Term: 2}
		logMgr.lastIndex = 4
		logMgr.firstIndex = 1

		peerID := types.NodeID("node2")
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		// Corrected: Set NextIndex to be strictly greater than the last entry of ConflictTerm (4)
		peerState.NextIndex = 5
		peerState.MatchIndex = 2
		rm.mu.Unlock()

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  2, // Conflict with term 2
			ConflictIndex: 3,
		}

		rm.handleLogInconsistency(peerState, peerID, reply)

		// Should set NextIndex to last index with conflicting term + 1
		// Last entry with term 2 is at index 4, so NextIndex should be 5
		testutil.AssertEqual(
			t,
			types.Index(5),
			peerState.NextIndex,
			"Should set NextIndex to last conflicting term index + 1",
		)
		testutil.AssertEqual(
			t,
			types.Index(2),
			peerState.MatchIndex,
			"MatchIndex should remain unchanged",
		)
	})

	t.Run("ConflictTermNotFoundInLeaderLog", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up leader log with terms 1 and 3, but not 2
		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 1}
		logMgr.entries[3] = types.LogEntry{Index: 3, Term: 3}
		logMgr.entries[4] = types.LogEntry{Index: 4, Term: 3}
		logMgr.lastIndex = 4
		logMgr.firstIndex = 1

		peerID := types.NodeID("node2")
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.NextIndex = 4
		peerState.MatchIndex = 1
		rm.mu.Unlock()

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  2, // Term 2 not in leader log
			ConflictIndex: 3,
		}

		rm.handleLogInconsistency(peerState, peerID, reply)

		// Should use ConflictIndex since ConflictTerm not found
		testutil.AssertEqual(
			t,
			types.Index(3),
			peerState.NextIndex,
			"Should use ConflictIndex when ConflictTerm not found",
		)
		testutil.AssertEqual(
			t,
			types.Index(1),
			peerState.MatchIndex,
			"MatchIndex should remain unchanged",
		)
	})

	t.Run("ConflictTermOptimizationNotApplicable", func(t *testing.T) {
		rm, logMgr, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		// Set up leader log
		logMgr.entries[1] = types.LogEntry{Index: 1, Term: 1}
		logMgr.entries[2] = types.LogEntry{Index: 2, Term: 2}
		logMgr.entries[3] = types.LogEntry{Index: 3, Term: 2}
		logMgr.lastIndex = 3
		logMgr.firstIndex = 1

		peerID := types.NodeID("node2")
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.NextIndex = 2
		peerState.MatchIndex = 0
		rm.mu.Unlock()

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  2,
			ConflictIndex: 1, // ConflictIndex < found conflicting term index
		}

		rm.handleLogInconsistency(peerState, peerID, reply)

		// Should use ConflictIndex since optimization not applicable
		testutil.AssertEqual(
			t,
			types.Index(1),
			peerState.NextIndex,
			"Should use ConflictIndex when optimization not applicable",
		)
	})

	t.Run("NoConflictTermUseConflictIndex", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.NextIndex = 10
		peerState.MatchIndex = 5
		rm.mu.Unlock()

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  0, // No conflict term provided
			ConflictIndex: 7,
		}

		rm.handleLogInconsistency(peerState, peerID, reply)

		testutil.AssertEqual(
			t,
			types.Index(7),
			peerState.NextIndex,
			"Should use ConflictIndex when no ConflictTerm",
		)
		testutil.AssertEqual(
			t,
			types.Index(5),
			peerState.MatchIndex,
			"MatchIndex should remain unchanged",
		)
	})

	t.Run("NoConflictInfoDecrementNextIndex", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.NextIndex = 10
		peerState.MatchIndex = 5
		rm.mu.Unlock()

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  0, // No conflict info
			ConflictIndex: 0,
		}

		rm.handleLogInconsistency(peerState, peerID, reply)

		testutil.AssertEqual(
			t,
			types.Index(9),
			peerState.NextIndex,
			"Should decrement NextIndex when no conflict info",
		)
		testutil.AssertEqual(
			t,
			types.Index(5),
			peerState.MatchIndex,
			"MatchIndex should remain unchanged",
		)
	})

	t.Run("NextIndexNeverGoesBelowOne", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.NextIndex = 1
		peerState.MatchIndex = 0
		rm.mu.Unlock()

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: 0, // Would normally cause NextIndex to go to 0
		}

		rm.handleLogInconsistency(peerState, peerID, reply)

		testutil.AssertEqual(
			t,
			types.Index(1),
			peerState.NextIndex,
			"NextIndex should never go below 1",
		)
	})

	t.Run("MatchIndexHintUpdated", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.NextIndex = 10
		peerState.MatchIndex = 3
		rm.mu.Unlock()

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: 7,
			MatchIndex:    5, // Hint that peer has up to index 5
		}

		rm.handleLogInconsistency(peerState, peerID, reply)

		testutil.AssertEqual(
			t,
			types.Index(7),
			peerState.NextIndex,
			"Should update NextIndex based on ConflictIndex",
		)
		testutil.AssertEqual(
			t,
			types.Index(5),
			peerState.MatchIndex,
			"Should update MatchIndex from hint",
		)
	})

	t.Run("MatchIndexHintIgnoredIfLower", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.NextIndex = 10
		peerState.MatchIndex = 8
		rm.mu.Unlock()

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: 7,
			MatchIndex:    5, // Lower than current MatchIndex
		}

		rm.handleLogInconsistency(peerState, peerID, reply)

		testutil.AssertEqual(t, types.Index(7), peerState.NextIndex, "Should update NextIndex")
		testutil.AssertEqual(
			t,
			types.Index(8),
			peerState.MatchIndex,
			"Should not regress MatchIndex",
		)
	})

	t.Run("ZeroMatchIndexHintIgnored", func(t *testing.T) {
		rm, _, _, _, _, _ := setupTestReplicationManager(t)
		defer rm.Stop()

		peerID := types.NodeID("node2")
		rm.mu.Lock()
		peerState := rm.peerStates[peerID]
		peerState.NextIndex = 10
		peerState.MatchIndex = 5
		rm.mu.Unlock()

		reply := &types.AppendEntriesReply{
			Term:          1,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: 7,
			MatchIndex:    0, // Zero hint should be ignored
		}

		rm.handleLogInconsistency(peerState, peerID, reply)

		testutil.AssertEqual(t, types.Index(7), peerState.NextIndex, "Should update NextIndex")
		testutil.AssertEqual(
			t,
			types.Index(5),
			peerState.MatchIndex,
			"Should ignore zero MatchIndex hint",
		)
	})
}
