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

const (
	testNodeID = types.NodeID("node-1")
)

func createTestStateManager(t *testing.T) (*stateManager, *mockStorage, *mockMetrics) {
	t.Helper()

	storage := newMockStorage()
	metrics := newMockMetrics()
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	mu := &sync.RWMutex{}
	isShutdown := &atomic.Bool{}
	leaderChangeCh := make(chan types.NodeID, 1)

	deps := StateManagerDeps{
		ID:             testNodeID,
		Mu:             mu,
		IsShutdown:     isShutdown,
		LeaderChangeCh: leaderChangeCh,
		Logger:         logger,
		Metrics:        metrics,
		Clock:          clock,
		Storage:        storage,
	}

	sm, err := NewStateManager(deps)
	testutil.RequireNoError(t, err)

	return sm.(*stateManager), storage, metrics
}

func createTestStateManagerWithShutdown(t *testing.T) (*stateManager, *mockStorage, *mockMetrics, *atomic.Bool) {
	t.Helper()

	sm, storage, metrics := createTestStateManager(t)
	return sm, storage, metrics, sm.isShutdown
}

// Tests for NewStateManager and validation

func TestNewStateManager_ValidDeps(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	testutil.AssertEqual(t, testNodeID, sm.id)
	testutil.AssertEqual(t, types.RoleFollower, sm.role)
	testutil.AssertEqual(t, types.Term(0), sm.currentTerm)
	testutil.AssertEqual(t, unknownNodeID, sm.votedFor)
	testutil.AssertEqual(t, unknownNodeID, sm.leaderID)
	testutil.AssertEqual(t, unknownNodeID, sm.lastKnownLeaderID)
	testutil.AssertEqual(t, types.Index(0), sm.commitIndex)
	testutil.AssertEqual(t, types.Index(0), sm.lastApplied)
}

func TestNewStateManager_InvalidDeps(t *testing.T) {
	tests := []struct {
		name string
		deps StateManagerDeps
	}{
		{
			name: "missing node ID",
			deps: StateManagerDeps{
				ID:             unknownNodeID,
				Mu:             &sync.RWMutex{},
				IsShutdown:     &atomic.Bool{},
				LeaderChangeCh: make(chan types.NodeID, 1),
				Logger:         logger.NewNoOpLogger(),
				Metrics:        newMockMetrics(),
				Storage:        newMockStorage(),
			},
		},
		{
			name: "nil mutex",
			deps: StateManagerDeps{
				ID:             testNodeID,
				Mu:             nil,
				IsShutdown:     &atomic.Bool{},
				LeaderChangeCh: make(chan types.NodeID, 1),
				Logger:         logger.NewNoOpLogger(),
				Metrics:        newMockMetrics(),
				Storage:        newMockStorage(),
			},
		},
		{
			name: "nil shutdown flag",
			deps: StateManagerDeps{
				ID:             testNodeID,
				Mu:             &sync.RWMutex{},
				IsShutdown:     nil,
				LeaderChangeCh: make(chan types.NodeID, 1),
				Logger:         logger.NewNoOpLogger(),
				Metrics:        newMockMetrics(),
				Storage:        newMockStorage(),
			},
		},
		{
			name: "nil leader change channel",
			deps: StateManagerDeps{
				ID:             testNodeID,
				Mu:             &sync.RWMutex{},
				IsShutdown:     &atomic.Bool{},
				LeaderChangeCh: nil,
				Logger:         logger.NewNoOpLogger(),
				Metrics:        newMockMetrics(),
				Storage:        newMockStorage(),
			},
		},
		{
			name: "nil logger",
			deps: StateManagerDeps{
				ID:             testNodeID,
				Mu:             &sync.RWMutex{},
				IsShutdown:     &atomic.Bool{},
				LeaderChangeCh: make(chan types.NodeID, 1),
				Logger:         nil,
				Metrics:        newMockMetrics(),
				Storage:        newMockStorage(),
			},
		},
		{
			name: "nil metrics",
			deps: StateManagerDeps{
				ID:             testNodeID,
				Mu:             &sync.RWMutex{},
				IsShutdown:     &atomic.Bool{},
				LeaderChangeCh: make(chan types.NodeID, 1),
				Logger:         logger.NewNoOpLogger(),
				Metrics:        nil,
				Storage:        newMockStorage(),
			},
		},
		{
			name: "nil storage",
			deps: StateManagerDeps{
				ID:             testNodeID,
				Mu:             &sync.RWMutex{},
				IsShutdown:     &atomic.Bool{},
				LeaderChangeCh: make(chan types.NodeID, 1),
				Logger:         logger.NewNoOpLogger(),
				Metrics:        newMockMetrics(),
				Storage:        nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStateManager(tt.deps)
			testutil.AssertError(t, err)
		})
	}
}

// Tests for Initialize

func TestStateManager_Initialize_Success(t *testing.T) {
	sm, storage, metrics := createTestStateManager(t)

	// Set up storage to return a specific state
	expectedState := types.PersistentState{
		CurrentTerm: 5,
		VotedFor:    types.NodeID("candidate-1"),
	}
	storage.state = expectedState

	err := sm.Initialize(context.Background())
	testutil.AssertNoError(t, err)

	testutil.AssertEqual(t, expectedState.CurrentTerm, sm.currentTerm)
	testutil.AssertEqual(t, expectedState.VotedFor, sm.votedFor)
	testutil.AssertEqual(t, types.RoleFollower, sm.role)
	testutil.AssertEqual(t, unknownNodeID, sm.leaderID)

	// Check metrics were updated
	testutil.AssertLen(t, metrics.termObservations, 1)
	testutil.AssertEqual(t, expectedState.CurrentTerm, metrics.termObservations[0])
}

func TestStateManager_Initialize_LoadError_InitializesDefaults(t *testing.T) {
	sm, storage, metrics := createTestStateManager(t)

	// Set up storage to return an error on load
	storage.hookLoadState = func(ctx context.Context) (types.PersistentState, error) {
		return types.PersistentState{}, errors.New("storage error")
	}

	err := sm.Initialize(context.Background())
	testutil.AssertNoError(t, err)

	// Should initialize with defaults
	testutil.AssertEqual(t, types.Term(0), sm.currentTerm)
	testutil.AssertEqual(t, unknownNodeID, sm.votedFor)
	testutil.AssertEqual(t, types.RoleFollower, sm.role)

	// Check metrics were updated with default values
	testutil.AssertLen(t, metrics.termObservations, 1)
	testutil.AssertEqual(t, types.Term(0), metrics.termObservations[0])
}

func TestStateManager_Initialize_DefaultStatePersistError(t *testing.T) {
	sm, storage, _ := createTestStateManager(t)

	// Set up storage to return an error on both load and save
	storage.hookLoadState = func(ctx context.Context) (types.PersistentState, error) {
		return types.PersistentState{}, errors.New("load error")
	}
	storage.hookSaveState = func(ctx context.Context, state types.PersistentState) error {
		return errors.New("save error")
	}

	err := sm.Initialize(context.Background())
	testutil.AssertError(t, err)
}

func TestStateManager_Initialize_ShuttingDown(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	isShutdown.Store(true)

	err := sm.Initialize(context.Background())
	testutil.AssertError(t, err)
	testutil.AssertContains(t, err.Error(), "shutting down")
}

// Tests for GetState methods

func TestStateManager_GetState(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 5
	sm.role = types.RoleCandidate
	sm.leaderID = types.NodeID("leader-1")

	term, role, leader := sm.GetState()
	testutil.AssertEqual(t, types.Term(5), term)
	testutil.AssertEqual(t, types.RoleCandidate, role)
	testutil.AssertEqual(t, types.NodeID("leader-1"), leader)
}

func TestStateManager_GetStateUnsafe(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 3
	sm.role = types.RoleLeader
	sm.leaderID = testNodeID

	term, role, leader := sm.GetStateUnsafe()
	testutil.AssertEqual(t, types.Term(3), term)
	testutil.AssertEqual(t, types.RoleLeader, role)
	testutil.AssertEqual(t, testNodeID, leader)
}

func TestStateManager_GetSnapshot(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 7
	sm.role = types.RoleLeader
	sm.leaderID = testNodeID
	sm.commitIndex = 10
	sm.lastApplied = 8
	sm.votedFor = testNodeID
	sm.lastKnownLeaderID = testNodeID

	snapshot := sm.GetSnapshot()
	testutil.AssertEqual(t, types.Term(7), snapshot.Term)
	testutil.AssertEqual(t, types.RoleLeader, snapshot.Role)
	testutil.AssertEqual(t, testNodeID, snapshot.LeaderID)
	testutil.AssertEqual(t, types.Index(10), snapshot.CommitIndex)
	testutil.AssertEqual(t, types.Index(8), snapshot.LastApplied)
	testutil.AssertEqual(t, testNodeID, snapshot.VotedFor)
	testutil.AssertEqual(t, testNodeID, snapshot.LastKnownLeader)
}

func TestStateManager_GetLastKnownLeader(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	expectedLeader := types.NodeID("old-leader")
	sm.lastKnownLeaderID = expectedLeader

	leader := sm.GetLastKnownLeader()
	testutil.AssertEqual(t, expectedLeader, leader)
}

// Tests for BecomeCandidate

func TestStateManager_BecomeCandidate_Success(t *testing.T) {
	sm, storage, metrics := createTestStateManager(t)

	// Initialize to be a follower
	sm.currentTerm = 2
	sm.role = types.RoleFollower
	sm.votedFor = unknownNodeID

	result := sm.BecomeCandidate(context.Background(), ElectionReasonTimeout)
	testutil.AssertTrue(t, result)

	// Check state changes
	testutil.AssertEqual(t, types.Term(3), sm.currentTerm)
	testutil.AssertEqual(t, types.RoleCandidate, sm.role)
	testutil.AssertEqual(t, testNodeID, sm.votedFor)
	testutil.AssertEqual(t, unknownNodeID, sm.leaderID)

	// Check persistence
	testutil.AssertEqual(t, types.Term(3), storage.state.CurrentTerm)
	testutil.AssertEqual(t, testNodeID, storage.state.VotedFor)

	// Check metrics
	testutil.AssertLen(t, metrics.electionStartObservations, 1)
	testutil.AssertEqual(t, types.Term(3), metrics.electionStartObservations[0].Term)
	testutil.AssertEqual(t, ElectionReasonTimeout, metrics.electionStartObservations[0].Reason)

	testutil.AssertLen(t, metrics.roleChangeObservations, 1)
	testutil.AssertEqual(t, types.RoleCandidate, metrics.roleChangeObservations[0].NewRole)
	testutil.AssertEqual(t, types.RoleFollower, metrics.roleChangeObservations[0].PreviousRole)
}

func TestStateManager_BecomeCandidate_NotFollower(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.role = types.RoleLeader

	result := sm.BecomeCandidate(context.Background(), ElectionReasonTimeout)
	testutil.AssertFalse(t, result)
}

func TestStateManager_BecomeCandidate_PersistenceFailure(t *testing.T) {
	sm, storage, _ := createTestStateManager(t)

	sm.currentTerm = 2
	sm.role = types.RoleFollower
	originalTerm := sm.currentTerm
	originalVotedFor := sm.votedFor

	// Set up storage to fail persistence
	storage.hookSaveState = func(ctx context.Context, state types.PersistentState) error {
		return errors.New("persistence error")
	}

	result := sm.BecomeCandidate(context.Background(), ElectionReasonTimeout)
	testutil.AssertFalse(t, result)

	// State should be rolled back
	testutil.AssertEqual(t, originalTerm, sm.currentTerm)
	testutil.AssertEqual(t, originalVotedFor, sm.votedFor)
	testutil.AssertEqual(t, types.RoleFollower, sm.role)
}

func TestStateManager_BecomeCandidate_ShuttingDown(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	isShutdown.Store(true)

	result := sm.BecomeCandidate(context.Background(), ElectionReasonTimeout)
	testutil.AssertFalse(t, result)
}

// Tests for BecomeCandidateForTerm

func TestStateManager_BecomeCandidateForTerm_Success(t *testing.T) {
	sm, storage, metrics := createTestStateManager(t)

	sm.currentTerm = 2
	sm.role = types.RoleFollower

	result := sm.BecomeCandidateForTerm(context.Background(), 5)
	testutil.AssertTrue(t, result)

	testutil.AssertEqual(t, types.Term(5), sm.currentTerm)
	testutil.AssertEqual(t, types.RoleCandidate, sm.role)
	testutil.AssertEqual(t, testNodeID, sm.votedFor)

	// Check persistence
	testutil.AssertEqual(t, types.Term(5), storage.state.CurrentTerm)
	testutil.AssertEqual(t, testNodeID, storage.state.VotedFor)

	// Check metrics
	testutil.AssertLen(t, metrics.termObservations, 1)
	testutil.AssertEqual(t, types.Term(5), metrics.termObservations[0])
}

func TestStateManager_BecomeCandidateForTerm_NotFollower(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.role = types.RoleCandidate

	result := sm.BecomeCandidateForTerm(context.Background(), 5)
	testutil.AssertFalse(t, result)
}

func TestStateManager_BecomeCandidateForTerm_TermNotHigher(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 5
	sm.role = types.RoleFollower

	result := sm.BecomeCandidateForTerm(context.Background(), 5)
	testutil.AssertFalse(t, result)

	result = sm.BecomeCandidateForTerm(context.Background(), 3)
	testutil.AssertFalse(t, result)
}

func TestStateManager_BecomeCandidateForTerm_PersistenceFailure(t *testing.T) {
	sm, storage, _ := createTestStateManager(t)

	sm.currentTerm = 2
	sm.role = types.RoleFollower
	originalState := StateSnapshot{
		Term:     sm.currentTerm,
		Role:     sm.role,
		VotedFor: sm.votedFor,
		LeaderID: sm.leaderID,
	}

	storage.hookSaveState = func(ctx context.Context, state types.PersistentState) error {
		return errors.New("persistence error")
	}

	result := sm.BecomeCandidateForTerm(context.Background(), 5)
	testutil.AssertFalse(t, result)

	// State should be rolled back
	testutil.AssertEqual(t, originalState.Term, sm.currentTerm)
	testutil.AssertEqual(t, originalState.Role, sm.role)
	testutil.AssertEqual(t, originalState.VotedFor, sm.votedFor)
	testutil.AssertEqual(t, originalState.LeaderID, sm.leaderID)
}

// Tests for BecomeLeader

func TestStateManager_BecomeLeader_Success(t *testing.T) {
	sm, _, metrics := createTestStateManager(t)

	sm.currentTerm = 3
	sm.role = types.RoleCandidate

	result := sm.BecomeLeader(context.Background())
	testutil.AssertTrue(t, result)

	testutil.AssertEqual(t, types.Term(3), sm.currentTerm)
	testutil.AssertEqual(t, types.RoleLeader, sm.role)
	testutil.AssertEqual(t, testNodeID, sm.leaderID)

	// Check metrics
	testutil.AssertLen(t, metrics.roleChangeObservations, 1)
	testutil.AssertEqual(t, types.RoleLeader, metrics.roleChangeObservations[0].NewRole)
	testutil.AssertEqual(t, types.RoleCandidate, metrics.roleChangeObservations[0].PreviousRole)

	// Check leader change notification was sent
	select {
	case leader := <-sm.leaderChangeCh:
		testutil.AssertEqual(t, testNodeID, leader)
	default:
		t.Error("Expected leader change notification")
	}
}

func TestStateManager_BecomeLeader_NotCandidate(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.role = types.RoleFollower

	result := sm.BecomeLeader(context.Background())
	testutil.AssertFalse(t, result)
}

func TestStateManager_BecomeLeader_ShuttingDown(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	sm.role = types.RoleCandidate
	isShutdown.Store(true)

	result := sm.BecomeLeader(context.Background())
	testutil.AssertFalse(t, result)
}

// Tests for BecomeFollower

func TestStateManager_BecomeFollower_HigherTerm(t *testing.T) {
	sm, storage, metrics := createTestStateManager(t)

	sm.currentTerm = 2
	sm.role = types.RoleCandidate
	leaderID := types.NodeID("new-leader")

	sm.BecomeFollower(context.Background(), 5, leaderID)

	testutil.AssertEqual(t, types.Term(5), sm.currentTerm)
	testutil.AssertEqual(t, types.RoleFollower, sm.role)
	testutil.AssertEqual(t, leaderID, sm.leaderID)
	testutil.AssertEqual(t, leaderID, sm.lastKnownLeaderID)
	testutil.AssertEqual(t, unknownNodeID, sm.votedFor) // Reset vote for higher term

	// Check persistence
	testutil.AssertEqual(t, types.Term(5), storage.state.CurrentTerm)
	testutil.AssertEqual(t, unknownNodeID, storage.state.VotedFor)

	// Check metrics
	testutil.AssertLen(t, metrics.termObservations, 1)
	testutil.AssertEqual(t, types.Term(5), metrics.termObservations[0])

	testutil.AssertLen(t, metrics.roleChangeObservations, 1)
	testutil.AssertEqual(t, types.RoleFollower, metrics.roleChangeObservations[0].NewRole)
}

func TestStateManager_BecomeFollower_SameTerm(t *testing.T) {
	sm, storage, _ := createTestStateManager(t)

	sm.currentTerm = 5
	sm.role = types.RoleCandidate
	sm.votedFor = testNodeID
	leaderID := types.NodeID("leader-1")

	originalState := storage.state

	sm.BecomeFollower(context.Background(), 5, leaderID)

	testutil.AssertEqual(t, types.Term(5), sm.currentTerm)
	testutil.AssertEqual(t, types.RoleFollower, sm.role)
	testutil.AssertEqual(t, leaderID, sm.leaderID)
	testutil.AssertEqual(t, testNodeID, sm.votedFor) // Vote should not be reset

	// Persistence should not change for same term
	testutil.AssertEqual(t, originalState, storage.state)
}

func TestStateManager_BecomeFollower_LeaderChangeNotification(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	oldLeader := types.NodeID("old-leader")
	newLeader := types.NodeID("new-leader")

	sm.leaderID = oldLeader
	sm.BecomeFollower(context.Background(), 3, newLeader)

	// Check leader change notification was sent
	select {
	case leader := <-sm.leaderChangeCh:
		testutil.AssertEqual(t, newLeader, leader)
	default:
		t.Error("Expected leader change notification")
	}
}

func TestStateManager_BecomeFollower_ShuttingDown(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	isShutdown.Store(true)
	sm.BecomeFollower(context.Background(), 5, types.NodeID("leader"))

	// Should not change state when shutting down
	testutil.AssertEqual(t, types.Term(0), sm.currentTerm)
	testutil.AssertEqual(t, types.RoleFollower, sm.role)
}

// Tests for CheckTermAndStepDown

func TestStateManager_CheckTermAndStepDown_HigherTerm(t *testing.T) {
	sm, storage, metrics := createTestStateManager(t)

	sm.currentTerm = 3
	sm.role = types.RoleLeader
	rpcLeader := types.NodeID("new-leader")

	steppedDown, prevTerm := sm.CheckTermAndStepDown(context.Background(), 5, rpcLeader)

	testutil.AssertTrue(t, steppedDown)
	testutil.AssertEqual(t, types.Term(3), prevTerm)
	testutil.AssertEqual(t, types.Term(5), sm.currentTerm)
	testutil.AssertEqual(t, types.RoleFollower, sm.role)
	testutil.AssertEqual(t, rpcLeader, sm.leaderID)
	testutil.AssertEqual(t, unknownNodeID, sm.votedFor) // Reset vote

	// Check persistence
	testutil.AssertEqual(t, types.Term(5), storage.state.CurrentTerm)
	testutil.AssertEqual(t, unknownNodeID, storage.state.VotedFor)

	// Check metrics
	testutil.AssertLen(t, metrics.termObservations, 1)
	testutil.AssertEqual(t, types.Term(5), metrics.termObservations[0])

	// Check leader change notification
	select {
	case leader := <-sm.leaderChangeCh:
		testutil.AssertEqual(t, rpcLeader, leader)
	default:
		t.Error("Expected leader change notification")
	}
}

func TestStateManager_CheckTermAndStepDown_LowerOrEqualTerm(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 5
	sm.role = types.RoleLeader

	// Test with lower term
	steppedDown, prevTerm := sm.CheckTermAndStepDown(context.Background(), 3, types.NodeID("leader"))
	testutil.AssertFalse(t, steppedDown)
	testutil.AssertEqual(t, types.Term(5), prevTerm)

	// Test with equal term
	steppedDown, prevTerm = sm.CheckTermAndStepDown(context.Background(), 5, types.NodeID("leader"))
	testutil.AssertFalse(t, steppedDown)
	testutil.AssertEqual(t, types.Term(5), prevTerm)

	// State should remain unchanged
	testutil.AssertEqual(t, types.Term(5), sm.currentTerm)
	testutil.AssertEqual(t, types.RoleLeader, sm.role)
}

func TestStateManager_CheckTermAndStepDown_ShuttingDown(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	sm.currentTerm = 3
	isShutdown.Store(true)

	steppedDown, prevTerm := sm.CheckTermAndStepDown(context.Background(), 5, types.NodeID("leader"))

	testutil.AssertFalse(t, steppedDown)
	testutil.AssertEqual(t, types.Term(3), prevTerm)
}

// Tests for GrantVote

func TestStateManager_GrantVote_Success(t *testing.T) {
	sm, storage, metrics := createTestStateManager(t)

	sm.currentTerm = 3
	candidateID := types.NodeID("candidate-1")

	result := sm.GrantVote(context.Background(), candidateID, 3)
	testutil.AssertTrue(t, result)

	testutil.AssertEqual(t, candidateID, sm.votedFor)

	// Check persistence
	testutil.AssertEqual(t, types.Term(3), storage.state.CurrentTerm)
	testutil.AssertEqual(t, candidateID, storage.state.VotedFor)

	// Check metrics
	testutil.AssertLen(t, metrics.voteGrantedObservations, 1)
	testutil.AssertEqual(t, types.Term(3), metrics.voteGrantedObservations[0])
}

func TestStateManager_GrantVote_HigherTerm(t *testing.T) {
	sm, storage, metrics := createTestStateManager(t)

	sm.currentTerm = 3
	sm.votedFor = types.NodeID("other-candidate")
	candidateID := types.NodeID("candidate-1")

	result := sm.GrantVote(context.Background(), candidateID, 5)
	testutil.AssertTrue(t, result)

	testutil.AssertEqual(t, types.Term(5), sm.currentTerm)
	testutil.AssertEqual(t, candidateID, sm.votedFor)

	// Check persistence
	testutil.AssertEqual(t, types.Term(5), storage.state.CurrentTerm)
	testutil.AssertEqual(t, candidateID, storage.state.VotedFor)

	// Check metrics for term update
	testutil.AssertLen(t, metrics.termObservations, 1)
	testutil.AssertEqual(t, types.Term(5), metrics.termObservations[0])
}

func TestStateManager_GrantVote_LowerTerm(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 5
	candidateID := types.NodeID("candidate-1")

	result := sm.GrantVote(context.Background(), candidateID, 3)
	testutil.AssertFalse(t, result)

	// State should remain unchanged
	testutil.AssertEqual(t, types.Term(5), sm.currentTerm)
	testutil.AssertEqual(t, unknownNodeID, sm.votedFor)
}

func TestStateManager_GrantVote_AlreadyVoted(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 3
	sm.votedFor = types.NodeID("other-candidate")
	candidateID := types.NodeID("candidate-1")

	result := sm.GrantVote(context.Background(), candidateID, 3)
	testutil.AssertFalse(t, result)

	// Vote should remain unchanged
	testutil.AssertEqual(t, types.NodeID("other-candidate"), sm.votedFor)
}

func TestStateManager_GrantVote_VoteForSameCandidate(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	candidateID := types.NodeID("candidate-1")
	sm.currentTerm = 3
	sm.votedFor = candidateID

	result := sm.GrantVote(context.Background(), candidateID, 3)
	testutil.AssertTrue(t, result)

	testutil.AssertEqual(t, candidateID, sm.votedFor)
}

func TestStateManager_GrantVote_PersistenceFailure(t *testing.T) {
	sm, storage, _ := createTestStateManager(t)

	sm.currentTerm = 3
	candidateID := types.NodeID("candidate-1")

	storage.hookSaveState = func(ctx context.Context, state types.PersistentState) error {
		return errors.New("persistence error")
	}

	result := sm.GrantVote(context.Background(), candidateID, 3)
	testutil.AssertFalse(t, result)

	// Vote should remain unchanged
	testutil.AssertEqual(t, unknownNodeID, sm.votedFor)
}

func TestStateManager_GrantVote_ShuttingDown(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	isShutdown.Store(true)
	candidateID := types.NodeID("candidate-1")

	result := sm.GrantVote(context.Background(), candidateID, 3)
	testutil.AssertFalse(t, result)
}

// Tests for index management

func TestStateManager_UpdateCommitIndex_Success(t *testing.T) {
	sm, _, metrics := createTestStateManager(t)

	result := sm.UpdateCommitIndex(5)
	testutil.AssertTrue(t, result)
	testutil.AssertEqual(t, types.Index(5), sm.commitIndex)

	// Check metrics
	testutil.AssertLen(t, metrics.commitIndexObservations, 1)
	testutil.AssertEqual(t, types.Index(5), metrics.commitIndexObservations[0])
}

func TestStateManager_UpdateCommitIndex_NotHigher(t *testing.T) {
	sm, _, metrics := createTestStateManager(t)

	sm.commitIndex = 5

	result := sm.UpdateCommitIndex(3)
	testutil.AssertFalse(t, result)
	testutil.AssertEqual(t, types.Index(5), sm.commitIndex)

	result = sm.UpdateCommitIndex(5)
	testutil.AssertFalse(t, result)
	testutil.AssertEqual(t, types.Index(5), sm.commitIndex)

	// No metrics should be recorded for failed updates
	testutil.AssertLen(t, metrics.commitIndexObservations, 0)
}

func TestStateManager_UpdateCommitIndexUnsafe(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	result := sm.UpdateCommitIndexUnsafe(10)
	testutil.AssertTrue(t, result)
	testutil.AssertEqual(t, types.Index(10), sm.commitIndex)

	result = sm.UpdateCommitIndexUnsafe(8)
	testutil.AssertFalse(t, result)
	testutil.AssertEqual(t, types.Index(10), sm.commitIndex)
}

func TestStateManager_UpdateLastApplied_Success(t *testing.T) {
	sm, _, metrics := createTestStateManager(t)

	sm.commitIndex = 10

	result := sm.UpdateLastApplied(5)
	testutil.AssertTrue(t, result)
	testutil.AssertEqual(t, types.Index(5), sm.lastApplied)

	// Check metrics
	testutil.AssertLen(t, metrics.appliedIndexObservations, 1)
	testutil.AssertEqual(t, types.Index(5), metrics.appliedIndexObservations[0])
}

func TestStateManager_UpdateLastApplied_ExceedsCommitIndex(t *testing.T) {
	sm, _, metrics := createTestStateManager(t)

	sm.commitIndex = 5

	result := sm.UpdateLastApplied(10)
	testutil.AssertFalse(t, result)
	testutil.AssertEqual(t, types.Index(0), sm.lastApplied)

	// No metrics should be recorded for failed updates
	testutil.AssertLen(t, metrics.appliedIndexObservations, 0)
}

func TestStateManager_UpdateLastApplied_NotHigher(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.commitIndex = 10
	sm.lastApplied = 5

	result := sm.UpdateLastApplied(3)
	testutil.AssertFalse(t, result)
	testutil.AssertEqual(t, types.Index(5), sm.lastApplied)

	result = sm.UpdateLastApplied(5)
	testutil.AssertFalse(t, result)
	testutil.AssertEqual(t, types.Index(5), sm.lastApplied)
}

func TestStateManager_GetCommitIndex(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.commitIndex = 15
	index := sm.GetCommitIndex()
	testutil.AssertEqual(t, types.Index(15), index)
}

func TestStateManager_GetCommitIndexUnsafe(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.commitIndex = 20
	index := sm.GetCommitIndexUnsafe()
	testutil.AssertEqual(t, types.Index(20), index)
}

func TestStateManager_GetLastApplied(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.lastApplied = 12
	index := sm.GetLastApplied()
	testutil.AssertEqual(t, types.Index(12), index)
}

func TestStateManager_GetLastAppliedUnsafe(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.lastApplied = 8
	index := sm.GetLastAppliedUnsafe()
	testutil.AssertEqual(t, types.Index(8), index)
}

// Tests for role checking utility methods

func TestStateManager_IsLeader(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.role = types.RoleLeader
	testutil.AssertTrue(t, sm.IsLeader())

	sm.role = types.RoleFollower
	testutil.AssertFalse(t, sm.IsLeader())

	sm.role = types.RoleCandidate
	testutil.AssertFalse(t, sm.IsLeader())
}

func TestStateManager_IsCandidate(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.role = types.RoleCandidate
	testutil.AssertTrue(t, sm.IsCandidate())

	sm.role = types.RoleFollower
	testutil.AssertFalse(t, sm.IsCandidate())

	sm.role = types.RoleLeader
	testutil.AssertFalse(t, sm.IsCandidate())
}

func TestStateManager_IsFollower(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.role = types.RoleFollower
	testutil.AssertTrue(t, sm.IsFollower())

	sm.role = types.RoleLeader
	testutil.AssertFalse(t, sm.IsFollower())

	sm.role = types.RoleCandidate
	testutil.AssertFalse(t, sm.IsFollower())
}

// Tests for Stop method

func TestStateManager_Stop(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	testutil.AssertFalse(t, isShutdown.Load())

	sm.Stop()
	testutil.AssertTrue(t, isShutdown.Load())

	// Calling stop multiple times should be safe
	sm.Stop()
	testutil.AssertTrue(t, isShutdown.Load())
}

// Tests for GetLeaderInfo methods

func TestStateManager_GetLeaderInfo(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.leaderID = types.NodeID("current-leader")
	sm.lastKnownLeaderID = types.NodeID("last-known-leader")

	current, lastKnown, hasLeader := sm.GetLeaderInfo()
	testutil.AssertEqual(t, types.NodeID("current-leader"), current)
	testutil.AssertEqual(t, types.NodeID("last-known-leader"), lastKnown)
	testutil.AssertTrue(t, hasLeader)

	// Test when no current leader
	sm.leaderID = unknownNodeID
	current, lastKnown, hasLeader = sm.GetLeaderInfo()
	testutil.AssertEqual(t, unknownNodeID, current)
	testutil.AssertEqual(t, types.NodeID("last-known-leader"), lastKnown)
	testutil.AssertFalse(t, hasLeader)
}

func TestStateManager_GetLeaderInfoUnsafe(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.leaderID = types.NodeID("leader-1")
	sm.lastKnownLeaderID = types.NodeID("old-leader")

	current, lastKnown, hasLeader := sm.GetLeaderInfoUnsafe()
	testutil.AssertEqual(t, types.NodeID("leader-1"), current)
	testutil.AssertEqual(t, types.NodeID("old-leader"), lastKnown)
	testutil.AssertTrue(t, hasLeader)
}

// Tests for advanced state management methods

func TestStateManager_CompareAndUpdateTerm_Success(t *testing.T) {
	sm, storage, metrics := createTestStateManager(t)

	sm.currentTerm = 3
	sm.votedFor = types.NodeID("candidate-1")

	updated, currentTerm := sm.CompareAndUpdateTerm(context.Background(), 5)
	testutil.AssertTrue(t, updated)
	testutil.AssertEqual(t, types.Term(5), currentTerm)
	testutil.AssertEqual(t, types.Term(5), sm.currentTerm)
	testutil.AssertEqual(t, unknownNodeID, sm.votedFor) // Vote should be reset

	// Check persistence
	testutil.AssertEqual(t, types.Term(5), storage.state.CurrentTerm)
	testutil.AssertEqual(t, unknownNodeID, storage.state.VotedFor)

	// Check metrics
	testutil.AssertLen(t, metrics.termObservations, 1)
	testutil.AssertEqual(t, types.Term(5), metrics.termObservations[0])
}

func TestStateManager_CompareAndUpdateTerm_NotHigher(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 5

	updated, currentTerm := sm.CompareAndUpdateTerm(context.Background(), 3)
	testutil.AssertFalse(t, updated)
	testutil.AssertEqual(t, types.Term(5), currentTerm)

	updated, currentTerm = sm.CompareAndUpdateTerm(context.Background(), 5)
	testutil.AssertFalse(t, updated)
	testutil.AssertEqual(t, types.Term(5), currentTerm)
}

func TestStateManager_CompareAndUpdateTerm_PersistenceFailure(t *testing.T) {
	sm, storage, _ := createTestStateManager(t)

	sm.currentTerm = 3
	originalVotedFor := sm.votedFor

	storage.hookSaveState = func(ctx context.Context, state types.PersistentState) error {
		return errors.New("persistence error")
	}

	updated, currentTerm := sm.CompareAndUpdateTerm(context.Background(), 5)
	testutil.AssertFalse(t, updated)
	testutil.AssertEqual(t, types.Term(3), currentTerm)
	testutil.AssertEqual(t, types.Term(3), sm.currentTerm)
	testutil.AssertEqual(t, originalVotedFor, sm.votedFor)
}

func TestStateManager_CompareAndUpdateTerm_ShuttingDown(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	sm.currentTerm = 3
	isShutdown.Store(true)

	updated, currentTerm := sm.CompareAndUpdateTerm(context.Background(), 5)
	testutil.AssertFalse(t, updated)
	testutil.AssertEqual(t, types.Term(3), currentTerm)
}

func TestStateManager_GetVotedFor(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	candidateID := types.NodeID("candidate-1")
	sm.votedFor = candidateID

	voted := sm.GetVotedFor()
	testutil.AssertEqual(t, candidateID, voted)
}

func TestStateManager_GetVotedForUnsafe(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	candidateID := types.NodeID("candidate-2")
	sm.votedFor = candidateID

	voted := sm.GetVotedForUnsafe()
	testutil.AssertEqual(t, candidateID, voted)
}

func TestStateManager_HasVoted(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	testutil.AssertFalse(t, sm.HasVoted())

	sm.votedFor = types.NodeID("candidate-1")
	testutil.AssertTrue(t, sm.HasVoted())
}

func TestStateManager_HasVotedUnsafe(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	testutil.AssertFalse(t, sm.HasVotedUnsafe())

	sm.votedFor = types.NodeID("candidate-1")
	testutil.AssertTrue(t, sm.HasVotedUnsafe())
}

func TestStateManager_CanVoteFor(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	candidateID := types.NodeID("candidate-1")
	otherID := types.NodeID("other-candidate")

	// Can vote when no vote cast
	testutil.AssertTrue(t, sm.CanVoteFor(candidateID))
	testutil.AssertTrue(t, sm.CanVoteFor(otherID))

	// Can vote for same candidate again
	sm.votedFor = candidateID
	testutil.AssertTrue(t, sm.CanVoteFor(candidateID))
	testutil.AssertFalse(t, sm.CanVoteFor(otherID))
}

func TestStateManager_CanVoteForUnsafe(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	candidateID := types.NodeID("candidate-1")
	otherID := types.NodeID("other-candidate")

	testutil.AssertTrue(t, sm.CanVoteForUnsafe(candidateID))

	sm.votedFor = candidateID
	testutil.AssertTrue(t, sm.CanVoteForUnsafe(candidateID))
	testutil.AssertFalse(t, sm.CanVoteForUnsafe(otherID))
}

func TestStateManager_GetTimeSinceLastRoleChange(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	// Set a specific time for testing
	testTime := sm.clock.Now().Add(-5 * time.Minute)
	sm.lastRoleChange = testTime

	duration := sm.GetTimeSinceLastRoleChange()
	testutil.AssertTrue(t, duration >= 5*time.Minute)
}

func TestStateManager_IsInTransition(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	// Should not be in transition as follower with old role change
	sm.role = types.RoleFollower
	sm.lastRoleChange = sm.clock.Now().Add(-time.Second)
	testutil.AssertFalse(t, sm.IsInTransition())

	// Should be in transition as candidate
	sm.role = types.RoleCandidate
	testutil.AssertTrue(t, sm.IsInTransition())

	// Should be in transition with recent role change
	sm.role = types.RoleFollower
	sm.lastRoleChange = sm.clock.Now()
	testutil.AssertTrue(t, sm.IsInTransition())

	// Should be in transition with persistence in progress
	sm.lastRoleChange = sm.clock.Now().Add(-time.Second)
	sm.persistenceInProgress.Store(true)
	testutil.AssertTrue(t, sm.IsInTransition())
}

// Tests for state validation

func TestStateManager_ValidateState(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	// Valid state
	sm.commitIndex = 10
	sm.lastApplied = 8
	sm.role = types.RoleFollower
	sm.leaderID = unknownNodeID
	sm.votedFor = unknownNodeID

	issues := sm.ValidateState()
	testutil.AssertLen(t, issues, 0)

	// Invalid state: lastApplied > commitIndex
	sm.lastApplied = 12
	issues = sm.ValidateState()
	testutil.AssertLen(t, issues, 1)
	testutil.AssertContains(t, issues[0], "lastApplied")
	testutil.AssertContains(t, issues[0], "commitIndex")

	// Reset to valid state completely
	sm.lastApplied = 8
	sm.commitIndex = 10
	sm.role = types.RoleFollower
	sm.leaderID = unknownNodeID
	sm.votedFor = unknownNodeID

	// Verify state is valid again
	issues = sm.ValidateState()
	testutil.AssertLen(t, issues, 0)

	// Invalid state: leader role but wrong leaderID
	sm.role = types.RoleLeader
	sm.leaderID = types.NodeID("other-node")
	issues = sm.ValidateState()
	testutil.AssertLen(t, issues, 1)
	testutil.AssertContains(t, issues[0], "leader role")

	// Fix leader ID and reset to valid state
	sm.leaderID = testNodeID
	sm.role = types.RoleLeader
	issues = sm.ValidateState()
	testutil.AssertLen(t, issues, 0)

	// Invalid state: candidate role but has leader
	sm.role = types.RoleCandidate
	sm.leaderID = types.NodeID("some-leader")
	sm.votedFor = testNodeID // Ensure candidate voted for self
	issues = sm.ValidateState()
	testutil.AssertLen(t, issues, 1)
	testutil.AssertContains(t, issues[0], "candidate role")

	// Fix candidate state
	sm.leaderID = unknownNodeID
	sm.votedFor = testNodeID
	sm.role = types.RoleCandidate
	issues = sm.ValidateState()
	testutil.AssertLen(t, issues, 0)

	// Invalid state: candidate but didn't vote for self
	sm.role = types.RoleCandidate
	sm.leaderID = unknownNodeID
	sm.votedFor = types.NodeID("other-candidate")
	issues = sm.ValidateState()
	testutil.AssertLen(t, issues, 1)
	testutil.AssertContains(t, issues[0], "voted for")
}

// Tests for debugging methods

func TestStateManager_GetStateInfo(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 5
	sm.role = types.RoleLeader
	sm.leaderID = testNodeID
	sm.commitIndex = 10
	sm.lastApplied = 8
	sm.roleChangeCount.Store(3)
	sm.termChangeCount.Store(2)

	info := sm.GetStateInfo()

	testutil.AssertEqual(t, testNodeID, info["nodeID"])
	testutil.AssertEqual(t, types.Term(5), info["currentTerm"])
	testutil.AssertEqual(t, "Leader", info["role"])
	testutil.AssertEqual(t, testNodeID, info["leaderID"])
	testutil.AssertEqual(t, types.Index(10), info["commitIndex"])
	testutil.AssertEqual(t, types.Index(8), info["lastApplied"])
	testutil.AssertEqual(t, uint64(3), info["roleChangeCount"])
	testutil.AssertEqual(t, uint64(2), info["termChangeCount"])
}

func TestStateManager_GetRoleChangeCount(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.roleChangeCount.Store(5)
	count := sm.GetRoleChangeCount()
	testutil.AssertEqual(t, uint64(5), count)
}

func TestStateManager_GetTermChangeCount(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.termChangeCount.Store(7)
	count := sm.GetTermChangeCount()
	testutil.AssertEqual(t, uint64(7), count)
}

func TestStateManager_GetPersistFailureCount(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.persistFailureCount.Store(2)
	count := sm.GetPersistFailureCount()
	testutil.AssertEqual(t, uint64(2), count)
}

// Tests for election preparation methods

func TestStateManager_PrepareForElection_Success(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.currentTerm = 3
	sm.role = types.RoleFollower

	newTerm, success := sm.PrepareForElection(context.Background())
	testutil.AssertTrue(t, success)
	testutil.AssertEqual(t, types.Term(4), newTerm)
}

func TestStateManager_PrepareForElection_NotFollower(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	sm.role = types.RoleLeader

	newTerm, success := sm.PrepareForElection(context.Background())
	testutil.AssertFalse(t, success)
	testutil.AssertEqual(t, sm.currentTerm, newTerm)
}

func TestStateManager_PrepareForElection_ShuttingDown(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	isShutdown.Store(true)

	newTerm, success := sm.PrepareForElection(context.Background())
	testutil.AssertFalse(t, success)
	testutil.AssertEqual(t, types.Term(0), newTerm)
}

func TestStateManager_ResetElectionState(t *testing.T) {
	sm, _, metrics := createTestStateManager(t)

	sm.role = types.RoleCandidate
	sm.currentTerm = 5

	sm.ResetElectionState(context.Background())

	testutil.AssertEqual(t, types.RoleFollower, sm.role)
	testutil.AssertEqual(t, unknownNodeID, sm.leaderID)

	// Check metrics
	testutil.AssertLen(t, metrics.roleChangeObservations, 1)
	testutil.AssertEqual(t, types.RoleFollower, metrics.roleChangeObservations[0].NewRole)
}

func TestStateManager_ResetElectionState_AlreadyFollower(t *testing.T) {
	sm, _, metrics := createTestStateManager(t)

	sm.role = types.RoleFollower

	sm.ResetElectionState(context.Background())

	// Should not record any role change
	testutil.AssertLen(t, metrics.roleChangeObservations, 0)
}

func TestStateManager_ResetElectionState_ShuttingDown(t *testing.T) {
	sm, _, _, isShutdown := createTestStateManagerWithShutdown(t)

	sm.role = types.RoleCandidate
	isShutdown.Store(true)

	sm.ResetElectionState(context.Background())

	// Should not change state when shutting down
	testutil.AssertEqual(t, types.RoleCandidate, sm.role)
}

// Test concurrency safety
func TestStateManager_ConcurrentAccess(t *testing.T) {
	sm, _, _ := createTestStateManager(t)

	err := sm.Initialize(context.Background())
	testutil.AssertNoError(t, err)

	// Start multiple goroutines accessing state concurrently
	done := make(chan bool, 3)

	// Goroutine 1: Read state repeatedly
	go func() {
		for range 100 {
			sm.GetState()
			sm.GetCommitIndex()
			sm.GetLastApplied()
			sm.GetLastKnownLeader()
		}
		done <- true
	}()

	// Goroutine 2: Update indices
	go func() {
		for i := range 100 {
			sm.UpdateCommitIndex(types.Index(i))
			if i <= int(sm.GetCommitIndex()) {
				sm.UpdateLastApplied(types.Index(i))
			}
		}
		done <- true
	}()

	// Goroutine 3: Role transitions
	go func() {
		for i := range 10 {
			sm.BecomeCandidate(context.Background(), ElectionReasonTimeout)
			sm.BecomeFollower(context.Background(), types.Term(i+1), types.NodeID("leader"))
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	for range 3 {
		<-done
	}

	// Verify state is still consistent
	issues := sm.ValidateState()
	testutil.AssertLen(t, issues, 0)
}
