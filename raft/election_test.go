// election_test.go
package raft

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func createTestElectionManager(
	t *testing.T,
) (*electionManager, *mockStateManager, *mockLogManager, *mockNetworkManager, *mockMetrics, *mockClock, *mockRand) {
	metrics := newMockMetrics()

	stateMgr := newMockStateManager(metrics)
	stateMgr.currentTerm = 1
	stateMgr.currentRole = types.RoleFollower
	stateMgr.votedFor = ""

	logMgr := &mockLogManager{
		lastIndex: 10,
		lastTerm:  1,
		entries:   make(map[types.Index]types.LogEntry),
	}

	networkMgr := newMockNetworkManager()
	leaderInit := &mockLeaderInitializer{}
	logger := logger.NewNoOpLogger()
	rand := &mockRand{}
	clock := newMockClock()

	peers := map[types.NodeID]PeerConfig{
		"node1": {Address: "localhost:8081"},
		"node2": {Address: "localhost:8082"},
		"node3": {Address: "localhost:8083"},
	}

	mu := &sync.RWMutex{}
	isShutdown := &atomic.Bool{}

	deps := ElectionManagerDeps{
		ID:                "node1",
		Peers:             peers,
		QuorumSize:        2,
		Mu:                mu,
		IsShutdown:        isShutdown,
		StateMgr:          stateMgr,
		LogMgr:            logMgr,
		NetworkMgr:        networkMgr,
		LeaderInitializer: leaderInit,
		Metrics:           metrics,
		Logger:            logger,
		Rand:              rand,
		Clock:             clock,
		Config: Config{
			Options: Options{
				ElectionTickCount:           50,
				ElectionRandomizationFactor: 2.0,
				HeartbeatTickCount:          5,
			},
		},
	}

	em, err := NewElectionManager(deps)
	testutil.RequireNoError(t, err, "Failed to create election manager")

	return em.(*electionManager), stateMgr, logMgr, networkMgr, metrics, clock, rand
}

func TestNewElectionManager(t *testing.T) {
	t.Run("ValidDependencies", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		testutil.AssertNotNil(t, em, "Expected election manager to be created")
		testutil.AssertEqual(t, types.NodeID("node1"), em.id, "Node ID mismatch")
		testutil.AssertEqual(t, 2, em.quorumSize, "Quorum size mismatch")
		// Check that defaults are applied
		testutil.AssertEqual(t, 50, em.electionTickCount, "ElectionTickCount mismatch")
		testutil.AssertEqual(t, 2.0, em.randomizationFactor, "RandomizationFactor mismatch")
		testutil.AssertNotNil(t, em.splitVoteDetector, "SplitVoteDetector should be initialized")
	})

	t.Run("InvalidDependencies", func(t *testing.T) {
		deps := ElectionManagerDeps{} // Empty deps, should fail validation
		_, err := NewElectionManager(deps)
		testutil.AssertError(t, err, "Expected error for invalid dependencies")
		testutil.AssertContains(
			t,
			err.Error(),
			"ID must be provided",
			"Error message should mention ID",
		)
	})
}

func TestValidateElectionManagerDeps(t *testing.T) {
	// A valid base set of dependencies to modify for specific test cases
	baseDeps := ElectionManagerDeps{
		ID:                "node1",
		Peers:             make(map[types.NodeID]PeerConfig),
		QuorumSize:        2,
		Mu:                &sync.RWMutex{},
		IsShutdown:        &atomic.Bool{},
		StateMgr:          newMockStateManager(newMockMetrics()),
		LogMgr:            &mockLogManager{},
		NetworkMgr:        newMockNetworkManager(),
		LeaderInitializer: &mockLeaderInitializer{},
		Metrics:           newMockMetrics(),
		Logger:            logger.NewNoOpLogger(),
		Rand:              &mockRand{},
		Clock:             newMockClock(),
		Config:            Config{},
	}

	testCases := []struct {
		name    string
		modify  func(deps *ElectionManagerDeps) // Function to modify baseDeps for the test case
		wantErr bool
		errMsg  string
	}{
		{
			name: "ValidDependencies",
			modify: func(deps *ElectionManagerDeps) {
				// No modification needed, baseDeps is valid
			},
			wantErr: false,
		},
		{
			name: "EmptyID",
			modify: func(deps *ElectionManagerDeps) {
				deps.ID = ""
			},
			wantErr: true,
			errMsg:  "ID must be provided",
		},
		{
			name: "InvalidQuorumSize",
			modify: func(deps *ElectionManagerDeps) {
				deps.QuorumSize = 0
			},
			wantErr: true,
			errMsg:  "quorum size must be positive",
		},
		{
			name: "NilMutex",
			modify: func(deps *ElectionManagerDeps) {
				deps.Mu = nil
			},
			wantErr: true,
			errMsg:  "mutex (Mu) must not be nil",
		},
		{
			name: "NilShutdownFlag",
			modify: func(deps *ElectionManagerDeps) {
				deps.IsShutdown = nil
			},
			wantErr: true,
			errMsg:  "shutdown flag (IsShutdown) must not be nil",
		},
		{
			name: "NilStateManager",
			modify: func(deps *ElectionManagerDeps) {
				deps.StateMgr = nil
			},
			wantErr: true,
			errMsg:  "state manager (StateMgr) must not be nil",
		},
		{
			name: "NilLogManager",
			modify: func(deps *ElectionManagerDeps) {
				deps.LogMgr = nil
			},
			wantErr: true,
			errMsg:  "log manager (LogMgr) must not be nil",
		},
		{
			name: "NilNetworkManager", // This is explicitly allowed in SetNetworkManager, but here it's a direct dependency of ElectionManagerDeps
			modify: func(deps *ElectionManagerDeps) {
				deps.NetworkMgr = nil
			},
			wantErr: false, // NetworkMgr can be nil at EM construction, set later. The validateElectionManagerDeps function does not check it.
		},
		{
			name: "NilLeaderInitializer",
			modify: func(deps *ElectionManagerDeps) {
				deps.LeaderInitializer = nil
			},
			wantErr: true,
			errMsg:  "leader initializer (LeaderInitializer) must not be nil",
		},
		{
			name: "NilMetrics",
			modify: func(deps *ElectionManagerDeps) {
				deps.Metrics = nil
			},
			wantErr: true,
			errMsg:  "metrics implementation (Metrics) must not be nil",
		},
		{
			name: "NilLogger",
			modify: func(deps *ElectionManagerDeps) {
				deps.Logger = nil
			},
			wantErr: true,
			errMsg:  "logger implementation (Logger) must not be nil",
		},
		{
			name: "NilRand",
			modify: func(deps *ElectionManagerDeps) {
				deps.Rand = nil
			},
			wantErr: true,
			errMsg:  "random number generator (Rand) must not be nil",
		},
		{
			name: "NilClock",
			modify: func(deps *ElectionManagerDeps) {
				deps.Clock = nil
			},
			wantErr: true,
			errMsg:  "clock implementation (Clock) must not be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deps := baseDeps // Create a copy of baseDeps for each test case
			tc.modify(&deps)

			err := validateElectionManagerDeps(deps)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateElectionManagerDeps() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && err != nil {
				testutil.AssertContains(
					t,
					err.Error(),
					tc.errMsg,
					"Error message mismatch",
				) // Called as a separate statement
			}
		})
	}
}

func TestSetNetworkManager(t *testing.T) {
	em, _, _, _, _, _, _ := createTestElectionManager(t)

	newNetworkMgr := newMockNetworkManager()
	em.SetNetworkManager(newNetworkMgr)

	testutil.AssertEqual(t, newNetworkMgr, em.networkMgr, "Network manager was not set correctly")
}

func TestInitialize(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		em, _, _, networkMgr, _, _, _ := createTestElectionManager(t)
		em.networkMgr = networkMgr // Ensure networkMgr is set before Initialize

		ctx := context.Background()
		err := em.Initialize(ctx)
		testutil.AssertNoError(t, err, "Initialize() failed unexpectedly")

		// Verify randomized period was reset (should be non-zero after initialization)
		testutil.AssertPositive(
			t,
			uint64(em.randomizedPeriod),
			"Randomized period should be positive after initialization",
		)
	})

	t.Run("ShuttingDown", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.isShutdown.Store(true)

		ctx := context.Background()
		err := em.Initialize(ctx)
		testutil.AssertErrorIs(t, err, ErrShuttingDown, "Expected ErrShuttingDown")
	})

	t.Run("NoNetworkManager", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.networkMgr = nil // Unset it after creation

		ctx := context.Background()
		err := em.Initialize(ctx)
		testutil.AssertError(t, err, "Expected error when network manager is nil")
		testutil.AssertContains(
			t,
			err.Error(),
			"network manager must be set",
			"Error message should be descriptive",
		)
	})
}

func TestResetTimerOnHeartbeat(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		em, _, _, _, _, clock, randMock := createTestElectionManager(t)

		initialRandomizedPeriod := em.randomizedPeriod

		randMock.floatVal = 0.9
		clock.Advance(1 * time.Millisecond)

		em.ResetTimerOnHeartbeat()

		testutil.AssertEqual(t, 0, em.electionElapsed, "Expected electionElapsed to be reset to 0")

		testutil.AssertNotEqual(
			t,
			initialRandomizedPeriod,
			em.randomizedPeriod,
			"Expected randomized period to be reset (different value)",
		)
		testutil.AssertPositive(
			t,
			uint64(em.randomizedPeriod),
			"Randomized period should be positive after reset",
		)
	})

	t.Run("ShuttingDown", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.isShutdown.Store(true)
		em.electionElapsed = 10 // Should not change

		em.ResetTimerOnHeartbeat()
		testutil.AssertEqual(
			t,
			10,
			em.electionElapsed,
			"electionElapsed should not change when shutting down",
		)
	})
}

func TestTick(t *testing.T) {
	t.Run("FollowerTick_NoElection", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentRole = types.RoleFollower
		em.electionElapsed = em.randomizedPeriod - 1 // One tick away from triggering

		ctx := context.Background()
		em.Tick(ctx)

		testutil.AssertEqual(
			t,
			em.randomizedPeriod,
			em.electionElapsed,
			"Election elapsed should increment by 1",
		)
		// Should not have started election yet
		testutil.AssertFalse(t, em.electionInFlight.Load(), "Election should not be in flight yet")
	})

	t.Run("FollowerTick_TriggersElection", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentRole = types.RoleFollower
		em.electionElapsed = em.randomizedPeriod

		em.startElection = func(ctx context.Context) {
			stateMgr.currentTerm++
			stateMgr.currentRole = types.RoleCandidate

			em.resetElectionTimeoutPeriod()
		}

		ctx := context.Background()
		em.Tick(ctx)

		testutil.AssertEqual(
			t,
			types.RoleCandidate,
			stateMgr.currentRole,
			"Role should change to Candidate after election trigger",
		)
		testutil.AssertEqual(
			t,
			0,
			em.electionElapsed,
			"Election elapsed should reset after election trigger",
		)
	})

	t.Run("LeaderTick", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentRole = types.RoleLeader
		initialElapsed := em.electionElapsed

		ctx := context.Background()
		em.Tick(ctx)

		testutil.AssertEqual(
			t,
			initialElapsed,
			em.electionElapsed,
			"Leader should not increment election elapsed",
		)
	})

	t.Run("ShuttingDown", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.isShutdown.Store(true)
		initialElapsed := em.electionElapsed

		ctx := context.Background()
		em.Tick(ctx)
		testutil.AssertEqual(
			t,
			initialElapsed,
			em.electionElapsed,
			"Tick should do nothing when shutting down",
		)
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		initialElapsed := em.electionElapsed

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel context immediately

		em.Tick(ctx)
		testutil.AssertEqual(
			t,
			initialElapsed,
			em.electionElapsed,
			"Tick should do nothing when context is cancelled",
		)
	})

	t.Run("StopChannelClosed", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		initialElapsed := em.electionElapsed
		close(em.stopCh) // Close the stop channel

		ctx := context.Background()
		em.Tick(ctx)
		testutil.AssertEqual(
			t,
			initialElapsed,
			em.electionElapsed,
			"Tick should do nothing when stop channel is closed",
		)
	})

	t.Run("TooManyConcurrentOps", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.concurrentOps.Store(maxConcurrentElections) // Set to max allowed

		ctx := context.Background()
		initialElapsed := em.electionElapsed
		em.Tick(ctx)
		testutil.AssertEqual(
			t,
			initialElapsed,
			em.electionElapsed,
			"Tick should be skipped if too many concurrent operations",
		)
	})
}

func TestHandleRequestVote(t *testing.T) {
	t.Run("GrantVote_CandidateTermHigher", func(t *testing.T) {
		em, stateMgr, logMgr, _, metrics, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 1
		stateMgr.currentRole = types.RoleFollower
		stateMgr.votedFor = ""

		logMgr.lastIndex = 0
		logMgr.lastTerm = 0

		stateMgr.grantVoteFunc = func(ctx context.Context, candidateID types.NodeID, term types.Term) bool {
			stateMgr.votedFor = candidateID
			metrics.ObserveVoteGranted(term)
			return true
		}

		args := &types.RequestVoteArgs{
			Term:         2, // Candidate's term is higher
			CandidateID:  "node2",
			LastLogIndex: 5, // Candidate's log index
			LastLogTerm:  1, // Candidate's log term
			IsPreVote:    false,
		}

		ctx := context.Background()
		reply, err := em.HandleRequestVote(ctx, args)

		testutil.AssertNoError(t, err, "HandleRequestVote() failed unexpectedly")
		testutil.AssertTrue(t, reply.VoteGranted, "Expected vote to be granted")
		testutil.AssertEqual(t, types.Term(2), reply.Term, "Expected reply term 2")

		// Verify state changes in stateMgr
		testutil.AssertEqual(
			t,
			types.Term(2),
			stateMgr.currentTerm,
			"StateManager current term should update",
		)
		testutil.AssertEqual(
			t,
			types.RoleFollower,
			stateMgr.currentRole,
			"StateManager role should remain follower",
		)
		testutil.AssertEqual(
			t,
			types.NodeID("node2"),
			stateMgr.leaderID,
			"StateManager leaderID should update to candidateID",
		)
		testutil.AssertEqual(
			t,
			types.NodeID("node2"),
			stateMgr.votedFor,
			"StateManager votedFor should update",
		)

		// Check metrics (from stateManager)
		testutil.AssertLen(
			t,
			metrics.voteGrantedObservations,
			1,
			"VoteGranted metric should be observed",
		)
		testutil.AssertEqual(
			t,
			types.Term(2),
			metrics.voteGrantedObservations[0],
			"VoteGranted metric term mismatch",
		)
		testutil.AssertLen(
			t,
			metrics.termObservations,
			1,
			"Term observation metric should be observed",
		)
		testutil.AssertEqual(
			t,
			types.Term(2),
			metrics.termObservations[0],
			"Term observation term mismatch",
		)
	})

	t.Run("GrantVote_SameTerm", func(t *testing.T) {
		em, stateMgr, logMgr, _, metrics, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 2
		stateMgr.currentRole = types.RoleFollower
		stateMgr.votedFor = "" // No vote yet in term 2
		logMgr.lastIndex = 5
		logMgr.lastTerm = 2

		stateMgr.grantVoteFunc = func(ctx context.Context, candidateID types.NodeID, term types.Term) bool {
			return true
		}

		args := &types.RequestVoteArgs{
			Term:         2,
			CandidateID:  "node2",
			LastLogIndex: 5,
			LastLogTerm:  2,
			IsPreVote:    false,
		}

		ctx := context.Background()
		reply, err := em.HandleRequestVote(ctx, args)
		testutil.AssertNoError(t, err, "HandleRequestVote() failed unexpectedly")
		testutil.AssertTrue(t, reply.VoteGranted, "Expected vote to be granted")
		testutil.AssertEqual(t, types.Term(2), reply.Term, "Expected reply term 2")

		// No term change, so no new term metric
		testutil.AssertLen(t, metrics.termObservations, 0, "No new term observation expected")
	})

	t.Run("RejectVote_StaleTerm", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower

		args := &types.RequestVoteArgs{
			Term:         3,
			CandidateID:  "node2",
			LastLogIndex: 5,
			LastLogTerm:  1,
			IsPreVote:    false,
		}

		ctx := context.Background()
		reply, err := em.HandleRequestVote(ctx, args)
		testutil.AssertNoError(t, err, "HandleRequestVote() failed unexpectedly")
		testutil.AssertFalse(t, reply.VoteGranted, "Expected vote to be rejected for stale term")
		testutil.AssertEqual(t, types.Term(5), reply.Term, "Reply term should be current term (5)")
	})

	t.Run("RejectVote_AlreadyVoted", func(t *testing.T) {
		em, stateMgr, logMgr, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 2
		stateMgr.currentRole = types.RoleFollower
		stateMgr.votedFor = "node3" // Already voted for node3 in term 2
		logMgr.lastIndex = 5
		logMgr.lastTerm = 1

		stateMgr.grantVoteFunc = func(ctx context.Context, candidateID types.NodeID, term types.Term) bool {
			return false // Simulate state manager rejecting due to already voted
		}

		args := &types.RequestVoteArgs{
			Term:         2,
			CandidateID:  "node2",
			LastLogIndex: 5,
			LastLogTerm:  1,
			IsPreVote:    false,
		}

		ctx := context.Background()
		reply, err := em.HandleRequestVote(ctx, args)
		testutil.AssertNoError(t, err, "HandleRequestVote() failed unexpectedly")
		testutil.AssertFalse(t, reply.VoteGranted, "Expected vote to be rejected - already voted")
	})

	t.Run("RejectVote_LogNotUpToDate", func(t *testing.T) {
		em, stateMgr, logMgr, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 2
		stateMgr.currentRole = types.RoleFollower
		stateMgr.votedFor = ""
		logMgr.lastIndex = 10 // Our log is longer
		logMgr.lastTerm = 3   // Our term is higher

		args := &types.RequestVoteArgs{
			Term:         2,
			CandidateID:  "node2",
			LastLogIndex: 5, // Candidate's log is shorter
			LastLogTerm:  2, // Candidate's term is lower
			IsPreVote:    false,
		}

		ctx := context.Background()
		reply, err := em.HandleRequestVote(ctx, args)
		testutil.AssertNoError(t, err, "HandleRequestVote() failed unexpectedly")
		testutil.AssertFalse(
			t,
			reply.VoteGranted,
			"Expected vote to be rejected - log not up to date",
		)
		testutil.AssertEqual(t, types.Term(2), reply.Term, "Reply term should be current term (2)")
	})

	t.Run("ShuttingDown", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		em.isShutdown.Store(true)
		// Set a known term for the reply
		stateMgr.currentTerm = 10

		args := &types.RequestVoteArgs{
			Term:        11,
			CandidateID: "node2",
			IsPreVote:   false,
		}

		ctx := context.Background()
		reply, err := em.HandleRequestVote(ctx, args)
		testutil.AssertErrorIs(t, err, ErrShuttingDown, "Expected ErrShuttingDown")
		testutil.AssertFalse(
			t,
			reply.VoteGranted,
			"Expected vote to be rejected when shutting down",
		)
		testutil.AssertEqual(
			t,
			types.Term(10),
			reply.Term,
			"Reply term should be current term even when shutting down",
		)
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 1

		args := &types.RequestVoteArgs{
			Term:        2,
			CandidateID: "node2",
			IsPreVote:   false,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel context immediately

		reply, err := em.HandleRequestVote(ctx, args)
		testutil.AssertErrorIs(t, err, context.Canceled, "Expected context.Canceled")
		testutil.AssertFalse(
			t,
			reply.VoteGranted,
			"Expected vote to be rejected when context cancelled",
		)
		testutil.AssertEqual(t, types.Term(1), reply.Term, "Reply term should be current term (1)")
	})
}

func TestIsLogUpToDate(t *testing.T) {
	em, _, _, _, _, _, _ := createTestElectionManager(
		t,
	) // em is not directly used but provides context

	testCases := []struct {
		name               string
		candidateLastTerm  types.Term
		candidateLastIndex types.Index
		ourLastTerm        types.Term
		ourLastIndex       types.Index
		expected           bool
	}{
		{
			name:               "HigherTerm",
			candidateLastTerm:  3,
			candidateLastIndex: 5,
			ourLastTerm:        2,
			ourLastIndex:       10, // Our index is higher, but candidate's term is higher
			expected:           true,
		},
		{
			name:               "SameTermHigherIndex",
			candidateLastTerm:  2,
			candidateLastIndex: 10,
			ourLastTerm:        2,
			ourLastIndex:       5,
			expected:           true,
		},
		{
			name:               "SameTermSameIndex",
			candidateLastTerm:  2,
			candidateLastIndex: 5,
			ourLastTerm:        2,
			ourLastIndex:       5,
			expected:           true,
		},
		{
			name:               "LowerTerm",
			candidateLastTerm:  1,
			candidateLastIndex: 10,
			ourLastTerm:        2,
			ourLastIndex:       5,
			expected:           false,
		},
		{
			name:               "SameTermLowerIndex",
			candidateLastTerm:  2,
			candidateLastIndex: 3,
			ourLastTerm:        2,
			ourLastIndex:       5,
			expected:           false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := em.isLogUpToDate(
				tc.candidateLastTerm,
				tc.candidateLastIndex,
				tc.ourLastTerm,
				tc.ourLastIndex,
			)
			testutil.AssertEqual(t, tc.expected, result, "isLogUpToDate() result mismatch")
		})
	}
}

func TestResetElectionTimeoutPeriod(t *testing.T) {
	em, _, _, _, _, _, randMock := createTestElectionManager(t)

	initialPeriod := em.randomizedPeriod
	em.electionElapsed = 10

	// Make the mock rand deterministic for testing the reset logic
	randMock.floatVal = 0.5 // Example float value
	randMock.intVal = 5     // Example int value

	em.resetElectionTimeoutPeriod()

	testutil.AssertEqual(t, 0, em.electionElapsed, "Expected electionElapsed to be 0")
	testutil.AssertPositive(t, uint64(em.randomizedPeriod), "Expected positive randomizedPeriod")

	testutil.AssertNotEqual(
		t,
		initialPeriod,
		em.randomizedPeriod,
		"Expected randomized period to be recalculated and differ",
	)
}

func TestRecordVote(t *testing.T) {
	em, _, _, _, _, _, _ := createTestElectionManager(t)
	em.resetVoteTracking(1) // Reset for term 1
	em.quorumSize = 2

	t.Run("GrantedVote", func(t *testing.T) {
		em.recordVote("node1", true)
		testutil.AssertEqual(t, 1, em.voteCount, "Expected vote count 1")
		testutil.AssertTrue(t, em.votesReceived["node1"], "Expected vote from node1 to be recorded")
	})

	t.Run("RejectedVote", func(t *testing.T) {
		initialCount := em.voteCount // Should be 1 from previous sub-test
		em.recordVote("node2", false)
		testutil.AssertEqual(
			t,
			initialCount,
			em.voteCount,
			"Expected vote count to remain unchanged for rejected vote",
		)
		testutil.AssertFalse(
			t,
			em.votesReceived["node2"],
			"Expected rejected vote not to be recorded",
		)
	})

	t.Run("DuplicateVote", func(t *testing.T) {
		initialCount := em.voteCount // Should be 1
		em.recordVote("node1", true) // Already voted for node1 in the same term
		testutil.AssertEqual(
			t,
			initialCount,
			em.voteCount,
			"Expected vote count to remain unchanged for duplicate vote",
		)
	})
}

func TestResetVoteTracking(t *testing.T) {
	em, _, _, _, _, _, _ := createTestElectionManager(t)

	em.resetVoteTracking(1)

	em.recordVote("node1", true)
	em.recordVote("node2", true)

	testutil.AssertEqual(t, 2, em.voteCount, "Pre-condition: voteCount should be 2")
	testutil.AssertEqual(t, types.Term(1), em.voteTerm, "Pre-condition: voteTerm should be 1")
	testutil.AssertLen(
		t,
		em.votesReceived,
		2,
		"Pre-condition: votesReceived map should have 2 entries",
	)

	newTerm := types.Term(5)
	em.resetVoteTracking(newTerm)

	if em.voteCount != 0 {
		t.Errorf("Expected vote count to be 0 after reset, got %d", em.voteCount)
	}
	if em.voteTerm != 5 {
		t.Errorf("Expected vote term to be 5, got %d", em.voteTerm)
	}
	if len(em.votesReceived) != 0 {
		t.Errorf("Expected votes received map to be empty, got %d entries", len(em.votesReceived))
	}
}

func TestTrackElectionAttempt(t *testing.T) {
	em, _, _, _, _, _, _ := createTestElectionManager(t)

	em.splitVoteDetector = &SplitVoteDetector{
		maxHistorySize: 5,
		termHistory:    make([]types.Term, 0, 5),
	}

	initialAttempts := em.splitVoteDetector.electionAttempts // Should be 0

	em.trackElectionAttempt(5) // First attempt (term 5)

	testutil.AssertEqual(
		t,
		initialAttempts+1,
		em.splitVoteDetector.electionAttempts,
		"Expected election attempts to increment by 1",
	)
	testutil.AssertLen(t, em.splitVoteDetector.termHistory, 1, "Expected term history length 1")
	testutil.AssertEqual(
		t,
		types.Term(5),
		em.splitVoteDetector.termHistory[0],
		"Expected term 5 in history",
	)

	// Test history overflow (add 5 more, total 8; should trim to 5)
	for i := 4; i <= 8; i++ { // This loop runs 5 times (i=4,5,6,7,8)
		em.trackElectionAttempt(types.Term(i + 10))
	}
	testutil.AssertEqual(
		t,
		em.splitVoteDetector.maxHistorySize,
		len(em.splitVoteDetector.termHistory),
		"Expected history to be trimmed to max size",
	)
	testutil.AssertEqual(
		t,
		types.Term(14),
		em.splitVoteDetector.termHistory[0],
		"Expected oldest term to be 14 after trimming",
	)
	testutil.AssertEqual(
		t,
		6,
		em.splitVoteDetector.electionAttempts,
		"Expected 6 attempts total (1 initial + 5 from loop)",
	)

	// Test attempts reset when `electionAttempts` exceeds 10 but is reset to 5
	em.splitVoteDetector.electionAttempts = 15 // Simulate attempts exceeding 10
	em.trackElectionAttempt(types.Term(20))
	testutil.AssertEqual(
		t,
		5,
		em.splitVoteDetector.electionAttempts,
		"Expected attempts to be reset to 5 after exceeding 10",
	)

	// Test `trackElectionAttempt` when `electionAttempts` is already at 5 (after a reset)
	em.splitVoteDetector.electionAttempts = 5
	em.trackElectionAttempt(types.Term(21))
	testutil.AssertEqual(
		t,
		6,
		em.splitVoteDetector.electionAttempts,
		"Expected attempts to increment normally from 5",
	)
}

func TestResetElectionState(t *testing.T) {
	em, _, _, _, _, _, _ := createTestElectionManager(t)

	// Set up some state to be reset
	em.electionState.Store(int32(ElectionStateVoting))
	em.electionInFlight.Store(true)
	em.recordVote("node1", true) // Ensure voteCount is > 0
	em.electionElapsed = 10
	em.randomizedPeriod = 100 // Set for checking reset logic

	em.resetElectionState("test reason")

	testutil.AssertEqual(
		t,
		int32(ElectionStateIdle),
		em.electionState.Load(),
		"Expected election state to be idle",
	)
	testutil.AssertFalse(t, em.electionInFlight.Load(), "Expected election in flight to be false")
	testutil.AssertEqual(t, 0, em.voteCount, "Expected vote count to be 0")
	testutil.AssertLen(t, em.votesReceived, 0, "Expected votes received map to be empty")
	testutil.AssertEqual(t, 0, em.electionElapsed, "Expected election elapsed to be 0")
	testutil.AssertNotEqual(t, 100, em.randomizedPeriod, "Expected randomized period to be reset")
	testutil.AssertPositive(
		t,
		uint64(em.randomizedPeriod),
		"Expected randomized period to be positive",
	)
}

func TestProcessVoteReply(t *testing.T) {
	t.Run("SuccessfulVote", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleCandidate
		em.electionState.Store(int32(ElectionStateVoting))
		em.resetVoteTracking(5) // Sets em.voteCount = 0, em.voteTerm = 5
		em.quorumSize = 2       // Need 1 more vote after self-vote

		em.recordVote(em.id, true) // Self-vote (em.voteCount becomes 1)

		reply := &types.RequestVoteReply{
			Term:        5,
			VoteGranted: true,
		}

		stateMgr.becomeLeaderFunc = func(context.Context) bool {
			stateMgr.currentRole = types.RoleLeader
			return true
		}

		em.processVoteReply(
			"node2",
			5,
			5,
			reply,
		) // This will cause em.voteCount to go to 2, achieve quorum, and then be reset to 0 by becomeLeader's cleanup

		testutil.AssertEqual(
			t,
			types.RoleLeader,
			stateMgr.currentRole,
			"Expected role to become leader after successful quorum",
		)
		testutil.AssertEqual(
			t,
			0,
			em.voteCount,
			"Expected vote count to be 0 after successful leader transition and state reset",
		)
		testutil.AssertFalse(
			t,
			em.hasQuorum(),
			"Expected not to have quorum after state reset",
		) // Implicitly checks voteCount is < quorumSize
		testutil.AssertEqual(
			t,
			0,
			em.splitVoteDetector.consecutiveFails,
			"Consecutive fails should be reset",
		)
		testutil.AssertEqual(
			t,
			0,
			em.splitVoteDetector.electionAttempts,
			"Election attempts should be reset",
		)
	})

	t.Run("HigherTermInReply", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleCandidate
		em.electionState.Store(int32(ElectionStateVoting))
		em.resetVoteTracking(5)

		t.Logf(
			"DEBUG: Before processVoteReply, initial electionAttempts: %d",
			em.splitVoteDetector.electionAttempts,
		)

		initialElectionAttempts := em.splitVoteDetector.electionAttempts

		reply := &types.RequestVoteReply{
			Term:        7, // Higher term
			VoteGranted: false,
		}

		em.processVoteReply("node2", 5, 5, reply)

		t.Logf(
			"DEBUG: After processVoteReply, final electionAttempts: %d",
			em.splitVoteDetector.electionAttempts,
		)

		testutil.AssertEqual(
			t,
			types.Term(7),
			stateMgr.currentTerm,
			"Expected term to be updated to 7",
		)
		testutil.AssertEqual(
			t,
			types.RoleFollower,
			stateMgr.currentRole,
			"Expected role to be follower",
		)
		testutil.AssertEqual(
			t,
			int32(ElectionStateIdle),
			em.electionState.Load(),
			"Election state should be idle after stepping down",
		)
		testutil.AssertEqual(
			t,
			initialElectionAttempts+1,
			em.splitVoteDetector.electionAttempts,
			"Election attempts should increment due to failure",
		)
	})

	t.Run("NotInVotingState", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.electionState.Store(int32(ElectionStateIdle)) // Not in voting state
		initialCount := em.voteCount                     // Should be 0 initially

		reply := &types.RequestVoteReply{
			Term:        5,
			VoteGranted: true,
		}

		em.processVoteReply("node2", 5, 5, reply)
		testutil.AssertEqual(
			t,
			initialCount,
			em.voteCount,
			"Expected vote count to remain unchanged when not in voting state",
		)
	})

	t.Run("ShuttingDown", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.isShutdown.Store(true)

		reply := &types.RequestVoteReply{
			Term:        5,
			VoteGranted: true,
		}
		// Should not panic, and ideally do nothing
		em.processVoteReply("node2", 5, 5, reply)
	})

	t.Run("VoteRejected", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleCandidate
		em.electionState.Store(int32(ElectionStateVoting))
		em.resetVoteTracking(5)
		initialElectionAttempts := em.splitVoteDetector.electionAttempts

		reply := &types.RequestVoteReply{
			Term:        5,
			VoteGranted: false, // Vote rejected
		}

		em.processVoteReply("node2", 5, 5, reply)

		testutil.AssertEqual(
			t,
			initialElectionAttempts+1,
			em.splitVoteDetector.electionAttempts,
			"Expected election attempts to increment when vote rejected",
		)
		testutil.AssertEqual(
			t,
			0,
			em.voteCount,
			"Expected vote count to remain 0 after rejected vote (no self-vote yet)",
		)
	})
}

func TestRecordElectionFailure(t *testing.T) {
	em, _, _, _, _, _, _ := createTestElectionManager(t)

	initialFails := em.splitVoteDetector.consecutiveFails // Should be 0
	em.recordElectionFailure()

	testutil.AssertEqual(
		t,
		initialFails+1,
		em.splitVoteDetector.consecutiveFails,
		"Expected consecutive fails to increment",
	)
	testutil.AssertFalse(
		t,
		em.splitVoteDetector.lastFailTime.IsZero(),
		"Expected last fail time to be set",
	)
}

func TestBecomeLeader(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleCandidate
		stateMgr.becomeLeaderFunc = func(ctx context.Context) bool {
			stateMgr.currentRole = types.RoleLeader
			stateMgr.leaderID = em.id
			return true
		}
		leaderInit := &mockLeaderInitializer{}
		em.leaderInitializer = leaderInit

		var wg sync.WaitGroup // Declare a WaitGroup
		wg.Add(1)             // Indicate one goroutine to wait for

		leaderInit.sendHeartbeatsFunc = func(ctx context.Context) {
			defer wg.Done() // Decrement WaitGroup counter when this function exits
			leaderInit.sendHeartbeatsCalled = true
		}

		ctx := context.Background()
		em.becomeLeader(ctx, 5) // This launches the goroutine

		wg.Wait() // Wait here until the heartbeat goroutine calls wg.Done()

		currentTerm, role, leaderID := stateMgr.GetState()
		testutil.AssertEqual(t, types.RoleLeader, role, "Expected role to be leader")
		testutil.AssertEqual(t, types.Term(5), currentTerm, "Expected term to remain 5")
		testutil.AssertEqual(t, em.id, leaderID, "Expected leader ID to be self ID")

		testutil.AssertEqual(
			t,
			0,
			em.splitVoteDetector.consecutiveFails,
			"Consecutive fails should be reset",
		)
		testutil.AssertEqual(
			t,
			0,
			em.splitVoteDetector.electionAttempts,
			"Election attempts should be reset",
		)
		testutil.AssertTrue(
			t,
			leaderInit.initializeStateCalled,
			"InitializeLeaderState should be called",
		)
		testutil.AssertTrue(t, leaderInit.sendHeartbeatsCalled, "SendHeartbeats should be called")
	})

	t.Run("TermMismatch", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 7 // Actual term
		stateMgr.currentRole = types.RoleCandidate

		ctx := context.Background()
		em.becomeLeader(ctx, 5) // Expected term is 5

		// Should not become leader, should step down to follower
		_, role, _ := stateMgr.GetState()
		testutil.AssertNotEqual(
			t,
			types.RoleLeader,
			role,
			"Should not become leader with term mismatch",
		)
		// Corrected expectation: should become Follower, not remain Candidate
		testutil.AssertEqual(
			t,
			types.RoleFollower,
			role,
			"Expected role to become follower after term mismatch",
		)
	})

	t.Run("NotCandidate", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower // Not a candidate

		ctx := context.Background()
		em.becomeLeader(ctx, 5)

		// Should not become leader, should remain follower
		_, role, _ := stateMgr.GetState()
		testutil.AssertNotEqual(
			t,
			types.RoleLeader,
			role,
			"Should not become leader when not candidate",
		)
		testutil.AssertEqual(t, types.RoleFollower, role, "Should remain follower")
	})

	t.Run("ShuttingDown", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		em.isShutdown.Store(true)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleCandidate // Role before calling becomeLeader

		ctx := context.Background()
		em.becomeLeader(ctx, 5)

		// Should not become leader
		_, role, _ := stateMgr.GetState()
		testutil.AssertNotEqual(
			t,
			types.RoleLeader,
			role,
			"Should not become leader when shutting down",
		)
		// Corrected expectation: should become Follower when shutting down
		testutil.AssertEqual(
			t,
			types.RoleFollower,
			role,
			"Expected role to become follower after shutting down",
		)
	})

	t.Run("BecomeLeaderFailsInStateManager", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleCandidate
		stateMgr.becomeLeaderFunc = func(context.Context) bool { return false } // Simulate state manager failure

		ctx := context.Background()
		em.becomeLeader(ctx, 5)

		// Should not become leader, should step down to follower
		_, role, _ := stateMgr.GetState()
		testutil.AssertNotEqual(
			t,
			types.RoleLeader,
			role,
			"Should not become leader when BecomeLeader fails",
		)
		testutil.AssertEqual(
			t,
			types.RoleFollower,
			role,
			"Expected role to become follower after failed transition",
		)
	})
}

func TestDefaultStartElection(t *testing.T) {
	t.Run("Success_BecomesCandidate", func(t *testing.T) {
		em, stateMgr, logMgr, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower
		logMgr.lastIndex = 10
		logMgr.lastTerm = 5

		// Set up mock to ensure state manager transitions to candidate
		stateMgr.becomeCandidateFunc = func(ctx context.Context, reason ElectionReason) bool {
			stateMgr.currentTerm++ // Increment term as per BecomeCandidate logic
			stateMgr.currentRole = types.RoleCandidate
			stateMgr.votedFor = em.id // Vote for self
			return true
		}

		em.electionInFlight.Store(true)
		defer em.electionInFlight.Store(false)

		ctx := context.Background()
		em.defaultStartElection(ctx)

		// Should have become candidate
		term, role, _ := stateMgr.GetState()
		testutil.AssertEqual(t, types.RoleCandidate, role, "Expected role to be candidate")
		testutil.AssertEqual(t, types.Term(6), term, "Expected term to increment")
		testutil.AssertEqual(t, 1, em.voteCount, "Expected at least 1 vote (self-vote)")
		testutil.AssertTrue(t, em.votesReceived[em.id], "Expected self-vote to be recorded")
		testutil.AssertTrue(t, em.electionInFlight.Load(), "Election should be in flight")
	})

	t.Run("Success_AchievesQuorumImmediately", func(t *testing.T) {
		em, stateMgr, logMgr, networkMgr, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower
		logMgr.lastIndex = 10
		logMgr.lastTerm = 5
		em.quorumSize = 1 // Set quorum to 1 for easy immediate leader
		// Mock network responses for immediate quorum
		networkMgr.sendRequestVoteFunc = func(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
			return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
		}

		stateMgr.becomeCandidateFunc = func(ctx context.Context, reason ElectionReason) bool {
			stateMgr.currentTerm++
			stateMgr.currentRole = types.RoleCandidate
			stateMgr.votedFor = em.id
			return true
		}
		stateMgr.becomeLeaderFunc = func(ctx context.Context) bool {
			stateMgr.currentRole = types.RoleLeader
			stateMgr.leaderID = em.id
			return true
		}

		ctx := context.Background()
		em.defaultStartElection(ctx)

		// Give time for concurrent goroutines (e.g. heartbeat)
		time.Sleep(50 * time.Millisecond)

		term, role, leaderID := stateMgr.GetState()
		testutil.AssertEqual(
			t,
			types.RoleLeader,
			role,
			"Expected role to be leader immediately after quorum",
		)
		testutil.AssertEqual(t, em.id, leaderID, "Expected leader ID to be self")
		testutil.AssertEqual(t, types.Term(6), term, "Expected term to increment for leader")
	})

	t.Run("ShuttingDown", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		em.isShutdown.Store(true)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower

		ctx := context.Background()
		em.defaultStartElection(ctx)

		// Should not change role or start election
		_, role, _ := stateMgr.GetState()
		testutil.AssertEqual(
			t,
			types.RoleFollower,
			role,
			"Role should not change when shutting down",
		)
		testutil.AssertFalse(t, em.electionInFlight.Load(), "Election should not be in flight")
	})

	t.Run("BecomeCandidateFails", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower
		stateMgr.becomeCandidateFunc = func(ctx context.Context, reason ElectionReason) bool { return false } // Simulate failure

		ctx := context.Background()
		em.defaultStartElection(ctx)

		// Should remain follower
		_, role, _ := stateMgr.GetState()
		testutil.AssertEqual(
			t,
			types.RoleFollower,
			role,
			"Should remain follower when BecomeCandidate fails",
		)
		testutil.AssertFalse(t, em.electionInFlight.Load(), "Election should not be in flight")
	})
}

func TestDefaultHandleElectionTimeout(t *testing.T) {
	t.Run("Success_StartsElection", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentRole = types.RoleFollower
		em.lastElectionTime.Store(0) // Simulate no recent election

		// Mock the actual election start, as defaultHandleElectionTimeout calls it
		var electionStarted atomic.Bool
		em.startElection = func(ctx context.Context) {
			electionStarted.Store(true)
			em.electionState.Store(int32(ElectionStateVoting)) // Simulate state change
			em.electionInFlight.Store(false)                   // Allow Tick to run again
		}

		ctx := context.Background()
		em.defaultHandleElectionTimeout(ctx)

		testutil.AssertTrue(t, electionStarted.Load(), "Expected election to be started")
		testutil.AssertPositive(
			t,
			uint64(em.electionCount.Load()),
			"Expected election count to increment",
		)
		testutil.AssertNotEqual(
			t,
			int64(0),
			em.lastElectionTime.Load(),
			"Expected last election time to be updated",
		)
	})

	t.Run("ShuttingDown", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.isShutdown.Store(true)

		ctx := context.Background()
		em.defaultHandleElectionTimeout(ctx)

		testutil.AssertEqual(
			t,
			uint64(0),
			em.electionCount.Load(),
			"Should not start election when shutting down",
		)
	})

	t.Run("ElectionInFlight", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.electionInFlight.Store(true) // Simulate an ongoing election

		ctx := context.Background()
		em.defaultHandleElectionTimeout(ctx)

		testutil.AssertEqual(
			t,
			uint64(0),
			em.electionCount.Load(),
			"Should not start another election if one is already in flight",
		)
	})

	t.Run("TooSoonAfterLastElection", func(t *testing.T) {
		em, stateMgr, _, _, _, clock, _ := createTestElectionManager(t)
		stateMgr.currentRole = types.RoleFollower
		em.lastElectionTime.Store(clock.Now().UnixMilli()) // Set last election to now

		ctx := context.Background()
		em.defaultHandleElectionTimeout(ctx)

		testutil.AssertEqual(
			t,
			uint64(0),
			em.electionCount.Load(),
			"Should not start election if too soon after last one",
		)
		// The randomizedPeriod should be reset in this case
		testutil.AssertEqual(t, 0, em.electionElapsed, "electionElapsed should be reset")
	})

	t.Run("NotFollower", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentRole = types.RoleLeader // Not a follower

		ctx := context.Background()
		em.defaultHandleElectionTimeout(ctx)

		testutil.AssertEqual(
			t,
			uint64(0),
			em.electionCount.Load(),
			"Should not start election when not a follower",
		)
	})

	t.Run("NotIdleState", func(t *testing.T) {
		em, stateMgr, _, _, _, _, _ := createTestElectionManager(t)
		stateMgr.currentRole = types.RoleFollower
		em.electionState.Store(int32(ElectionStateVoting)) // Not idle

		ctx := context.Background()
		em.defaultHandleElectionTimeout(ctx)

		testutil.AssertEqual(
			t,
			uint64(0),
			em.electionCount.Load(),
			"Should not start election if not in idle state",
		)
	})
}

func TestBroadcastVoteRequests(t *testing.T) {
	t.Run("Success_SendsToAllPeers", func(t *testing.T) {
		em, _, _, networkMgr, _, _, _ := createTestElectionManager(t)
		networkMgr.getAndResetCallCounts() // Reset mock network manager call counts

		var mu sync.Mutex
		callCounts := make(map[types.NodeID]int)

		networkMgr.sendRequestVoteFunc = func(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
			mu.Lock()
			callCounts[target]++
			mu.Unlock()
			return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
		}

		args := &types.RequestVoteArgs{
			Term:         5,
			CandidateID:  "node1",
			LastLogIndex: 10,
			LastLogTerm:  5,
			IsPreVote:    false,
		}

		ctx := context.Background()
		em.broadcastVoteRequests(ctx, args)

		// Give some time for goroutines to complete, as they run concurrently
		time.Sleep(100 * time.Millisecond)

		// Should have sent to 2 peers (excluding self: node2, node3)
		expectedPeers := []types.NodeID{"node2", "node3"}
		for _, peerID := range expectedPeers {
			testutil.AssertEqual(t, 1, callCounts[peerID], "Expected 1 vote request to %s", peerID)
		}
		testutil.AssertEqual(
			t,
			len(expectedPeers),
			len(callCounts),
			"Expected calls only to other peers",
		)
	})

	t.Run("ContextCancelled_AbortsRequests", func(t *testing.T) {
		em, _, _, networkMgr, _, _, _ := createTestElectionManager(t)
		networkMgr.getAndResetCallCounts()

		var mu sync.Mutex
		var actualCallCount int
		networkMgr.sendRequestVoteFunc = func(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
			// Simulate a long-running RPC that gets cancelled
			select {
			case <-ctx.Done():
				mu.Lock()
				actualCallCount++ // Count how many calls actually started
				mu.Unlock()
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond): // Longer than test timeout
				mu.Lock()
				actualCallCount++
				mu.Unlock()
				return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
			}
		}

		args := &types.RequestVoteArgs{
			Term:        5,
			CandidateID: "node1",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		em.broadcastVoteRequests(ctx, args)

		// Wait for the context to finish or goroutines to process cancellation
		time.Sleep(100 * time.Millisecond)

		// The actual call count should be <= number of peers, but not necessarily 0 if some started before cancellation.
		// The key is that it doesn't block indefinitely and errors are handled.
		testutil.AssertLessThanEqual(t, uint64(actualCallCount), uint64(len(em.peers)-1))
	})
}

func TestSendVoteRequest(t *testing.T) {
	t.Run("Success_VoteGranted", func(t *testing.T) {
		em, stateMgr, _, networkMgr, _, _, _ := createTestElectionManager(t)
		em.electionState.Store(int32(ElectionStateVoting))
		em.resetVoteTracking(5) // Candidate's current term is 5
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleCandidate // To pass processVoteReply's initial checks

		networkMgr.sendRequestVoteFunc = func(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
			return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
		}
		// Mock the state manager's grantVoteFunc to succeed when called by processVoteReply
		stateMgr.grantVoteFunc = func(ctx context.Context, candidateID types.NodeID, term types.Term) bool {
			return true // Simulate successful vote grant by state manager
		}

		args := &types.RequestVoteArgs{
			Term:        5,
			CandidateID: "node1",
		}

		ctx := context.Background()
		em.sendVoteRequest(ctx, "node2", args, 5) // Current term is 5

		testutil.AssertEqual(
			t,
			1,
			em.voteCount,
			"Expected vote count 1 after successful request and processing",
		)
		testutil.AssertEqual(
			t,
			0,
			em.splitVoteDetector.electionAttempts,
			"Election attempts should not increment on success",
		)
		testutil.AssertTrue(
			t,
			networkMgr.callCount >= 1,
			"Network manager SendRequestVote should have been called",
		)
	})

	t.Run("NetworkError", func(t *testing.T) {
		em, _, _, networkMgr, _, _, _ := createTestElectionManager(t)
		em.electionState.Store(int32(ElectionStateVoting))
		em.resetVoteTracking(
			5,
		) // Candidate's current term is 5
		initialElectionAttempts := em.splitVoteDetector.electionAttempts // Should be 0

		networkMgr.sendRequestVoteFunc = func(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
			return nil, errors.New("simulated network error")
		}

		args := &types.RequestVoteArgs{
			Term:        5,
			CandidateID: "node1",
		}

		ctx := context.Background()
		em.sendVoteRequest(ctx, "node2", args, 5)

		testutil.AssertEqual(t, 0, em.voteCount, "Expected vote count to remain 0 on network error")
		testutil.AssertEqual(
			t,
			initialElectionAttempts+1,
			em.splitVoteDetector.electionAttempts,
			"Expected election attempts to increment on network error",
		)
		testutil.AssertTrue(
			t,
			networkMgr.callCount >= 1,
			"Network manager SendRequestVote should have been called",
		)
	})

	t.Run("ReplyHigherTerm_StepsDown", func(t *testing.T) {
		em, stateMgr, _, networkMgr, _, _, _ := createTestElectionManager(t)
		em.electionState.Store(int32(ElectionStateVoting))
		em.resetVoteTracking(5) // Candidate's current term is 5
		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleCandidate

		networkMgr.sendRequestVoteFunc = func(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
			return &types.RequestVoteReply{
				Term:        7,
				VoteGranted: false,
			}, nil // Peer returns higher term
		}
		// Mock CheckTermAndStepDown in stateMgr to perform the step down
		stateMgr.checkTermAndStepDownFunc = func(ctx context.Context, rpcTerm types.Term, rpcLeader types.NodeID) (steppedDown bool, previousTerm types.Term) {
			if rpcTerm > stateMgr.currentTerm {
				stateMgr.currentTerm = rpcTerm
				stateMgr.currentRole = types.RoleFollower
				return true, 5
			}
			return false, 5
		}

		args := &types.RequestVoteArgs{
			Term:        5,
			CandidateID: "node1",
		}

		ctx := context.Background()
		em.sendVoteRequest(ctx, "node2", args, 5)

		// Verify state manager stepped down
		testutil.AssertEqual(
			t,
			types.Term(7),
			stateMgr.currentTerm,
			"StateManager term should update to higher term from reply",
		)
		testutil.AssertEqual(
			t,
			types.RoleFollower,
			stateMgr.currentRole,
			"StateManager role should be follower after stepping down",
		)
		testutil.AssertEqual(
			t,
			int32(ElectionStateIdle),
			em.electionState.Load(),
			"Election state should reset to idle",
		)
	})
}

func TestApplyDefaults(t *testing.T) {
	t.Run("InvalidElectionTickCount", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.electionTickCount = 0 // Invalid value

		em.applyDefaults()

		testutil.AssertEqual(
			t,
			DefaultElectionTickCount,
			em.electionTickCount,
			"Expected default election tick count",
		)
	})

	t.Run("LowRandomizationFactor", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.randomizationFactor = 0.5 // Lower than recommended

		em.applyDefaults()

		testutil.AssertEqual(
			t,
			DefaultElectionRandomizationFactor,
			em.randomizationFactor,
			"Expected default randomization factor",
		)
	})

	t.Run("ElectionTickCountLessThanHeartbeatTickCount", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.electionTickCount = 3 // Less than DefaultHeartbeatTickCount (5) in config

		em.applyDefaults()

		testutil.AssertEqual(
			t,
			3,
			em.electionTickCount,
			"Expected electionTickCount to remain 3 if > 0 but < HeartbeatTickCount",
		)
	})

	t.Run("ValidConfiguration", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		originalTickCount := em.electionTickCount       // Should be 50 from createTestElectionManager
		originalRandomization := em.randomizationFactor // Should be 2.0

		em.applyDefaults()

		testutil.AssertEqual(
			t,
			originalTickCount,
			em.electionTickCount,
			"Valid tick count should not change",
		)
		testutil.AssertEqual(
			t,
			originalRandomization,
			em.randomizationFactor,
			"Valid randomization factor should not change",
		)
	})
}

func TestStop(t *testing.T) {
	t.Run("Success_NoConcurrentOperations", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		// No concurrent operations started for this test

		em.Stop() // First stop call
		// Should be able to call multiple times without panic
		em.Stop() // Second stop call, should be idempotent
		em.Stop() // Third stop call
	})

	t.Run("WithConcurrentOperations_CompletesGracefully", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)

		// Simulate concurrent operations by incrementing the counter
		em.concurrentOps.Add(2)

		// Start a goroutine that will decrement the counter after a real-time delay
		go func() {
			time.Sleep(50 * time.Millisecond) // CORRECTED: Use real-time sleep
			em.concurrentOps.Add(-2)
		}()

		start := time.Now() // Real time measurement
		em.Stop()           // This uses real timers for its waiting logic
		elapsed := time.Since(start)

		// This assertion will now correctly check if Stop waited for the real 50ms delay
		testutil.AssertTrue(
			t,
			elapsed >= 50*time.Millisecond,
			"Stop should wait for concurrent operations to complete",
		)
		testutil.AssertTrue(
			t,
			elapsed < 2*time.Second,
			"Stop waited too long, expected less than 2 seconds",
		)
	})

	t.Run("WithConcurrentOperations_Timeout", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)

		// Simulate an operation that never completes
		em.concurrentOps.Add(1) // Never decremented

		start := time.Now()
		em.Stop()
		elapsed := time.Since(start)

		// Stop should timeout after approximately 2 seconds (default `stop` timeout)
		testutil.AssertTrue(
			t,
			elapsed >= 2*time.Second,
			"Stop should timeout after the configured period",
		)
		testutil.AssertTrue(
			t,
			elapsed < 3*time.Second,
			"Stop timeout too long, expected around 2 seconds",
		)
		testutil.AssertEqual(
			t,
			int32(1),
			em.concurrentOps.Load(),
			"Concurrent operations count should remain 1 after timeout",
		)
	})
}

func TestElectionManagerEdgeCases(t *testing.T) {
	t.Run("ZeroQuorumSize", func(t *testing.T) {
		deps := ElectionManagerDeps{
			ID:                "node1",
			QuorumSize:        0, // Invalid quorum size
			Mu:                &sync.RWMutex{},
			IsShutdown:        &atomic.Bool{},
			StateMgr:          newMockStateManager(newMockMetrics()),
			LogMgr:            &mockLogManager{},
			NetworkMgr:        newMockNetworkManager(),
			LeaderInitializer: &mockLeaderInitializer{},
			Metrics:           newMockMetrics(),
			Logger:            logger.NewNoOpLogger(),
			Rand:              &mockRand{},
			Clock:             newMockClock(),
			Config:            Config{},
		}

		_, err := NewElectionManager(deps)
		testutil.AssertError(t, err, "Expected error for zero quorum size")
		testutil.AssertContains(
			t,
			err.Error(),
			"quorum size must be positive",
			"Error message should be descriptive",
		)
	})

	t.Run("EmptyPeersMap", func(t *testing.T) {
		mu := &sync.RWMutex{}
		isShutdown := &atomic.Bool{}

		deps := ElectionManagerDeps{
			ID:                "node1",
			Peers:             make(map[types.NodeID]PeerConfig), // Empty peers map
			QuorumSize:        1,                                 // Quorum size 1 is valid for a single node (self) or empty cluster if configured that way
			Mu:                mu,
			IsShutdown:        isShutdown,
			StateMgr:          newMockStateManager(newMockMetrics()),
			LogMgr:            &mockLogManager{},
			NetworkMgr:        newMockNetworkManager(),
			LeaderInitializer: &mockLeaderInitializer{},
			Metrics:           newMockMetrics(),
			Logger:            logger.NewNoOpLogger(),
			Rand:              &mockRand{},
			Clock:             newMockClock(),
			Config: Config{
				Options: Options{
					ElectionTickCount:           50,
					ElectionRandomizationFactor: 2.0,
					HeartbeatTickCount:          5,
				},
			},
		}

		em, err := NewElectionManager(deps)
		testutil.AssertNoError(t, err, "Unexpected error with empty peers and quorum 1")
		testutil.AssertNotNil(t, em, "Expected election manager to be created with empty peers")
		testutil.AssertEqual(t, 1, em.(*electionManager).quorumSize, "Quorum size should be 1")
		testutil.AssertLen(
			t,
			em.(*electionManager).peers,
			0,
			"Peers map should be empty",
		) // Only self if it was in the map, but it's not here initially.
	})

	t.Run("QuorumLargerThanAvailablePeers", func(t *testing.T) {
		em, _, _, _, _, _, _ := createTestElectionManager(t)
		em.quorumSize = 5 // Larger than the 3 peers in the test setup (node1, node2, node3)

		// Set up votes for all existing nodes
		em.resetVoteTracking(1)
		em.recordVote("node1", true)
		em.recordVote("node2", true)
		em.recordVote("node3", true)

		// Should not have quorum even with all votes
		testutil.AssertFalse(
			t,
			em.hasQuorum(),
			"Should not have quorum when quorum size > available votes",
		)
	})
}
