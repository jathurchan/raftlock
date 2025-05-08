package raft

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

// newTestStateManager creates a state manager for testing
func newTestStateManager() (*stateManager, *mockStorage, chan types.NodeID) {
	mu := &sync.RWMutex{}
	isShutdown := &atomic.Bool{}
	mockStore := newMockStorage()
	leaderChangeCh := make(chan types.NodeID, 5)
	deps := Dependencies{
		Storage: mockStore,
		Logger:  &logger.NoOpLogger{},
		Metrics: &noOpMetrics{},
	}

	sm := NewStateManager(mu, isShutdown, deps, "node1", leaderChangeCh)
	return sm.(*stateManager), mockStore, leaderChangeCh
}

// setState is a helper to set the initial state of a state manager
func setState(sm *stateManager, term types.Term, role types.NodeRole, votedFor, leaderID types.NodeID) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.currentTerm = term
	sm.role = role
	sm.votedFor = votedFor
	sm.leaderID = leaderID
}

// getState is a helper to get the current state
func getState(sm *stateManager) (types.Term, types.NodeRole, types.NodeID, types.NodeID) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.currentTerm, sm.role, sm.votedFor, sm.leaderID
}

func TestRaftState_NewStateManager_InvalidArgs(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for invalid arguments, but got none")
		}
	}()

	NewStateManager(nil, nil, Dependencies{}, "", nil)
}

func TestRaftState_Initialize(t *testing.T) {
	t.Run("successful load", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		mockStore.state = types.PersistentState{CurrentTerm: 5, VotedFor: "node2"}

		err := sm.Initialize(context.Background())
		if err != nil {
			t.Errorf("Initialize() unexpected error: %v", err)
		}

		term, role, _, _ := getState(sm)
		if term != 5 || role != types.RoleFollower {
			t.Errorf("Initialize() got term=%v, role=%v; want term=5, role=Follower", term, role)
		}
	})

	t.Run("corrupted state", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		mockStore.setFailure("LoadState", storage.ErrCorruptedState)

		err := sm.Initialize(context.Background())
		if err != nil {
			t.Errorf("Initialize() with corrupted state unexpected error: %v", err)
		}

		if mockStore.saveCounter != 1 {
			t.Errorf("Initialize() with corrupted state should persist once, got %d persists", mockStore.saveCounter)
		}

		term, role, _, _ := getState(sm)
		if term != 0 || role != types.RoleFollower {
			t.Errorf("Initialize() corrupted state got term=%v, role=%v; want term=0, role=Follower", term, role)
		}
	})

	t.Run("load state error (non-corruption)", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		mockStore.setFailure("LoadState", errors.New("disk read failure"))

		err := sm.Initialize(context.Background())
		if err == nil || err.Error() != "failed to load persistent state: disk read failure" {
			t.Errorf("Expected load error, got: %v", err)
		}
	})

	t.Run("persist default state error after corrupted state", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		mockStore.setFailure("LoadState", storage.ErrCorruptedState)
		mockStore.setFailure("SaveState", errors.New("disk write failure"))

		err := sm.Initialize(context.Background())
		if err == nil || !strings.Contains(err.Error(), "failed to persist initial default state") {
			t.Errorf("Expected persist error, got: %v", err)
		}
	})
}

func TestRaftState_GetMethods(t *testing.T) {
	sm, _, _ := newTestStateManager()

	initialTerm := types.Term(5)
	initialRole := types.RoleLeader
	initialLeaderID := types.NodeID("node1")
	initialLastKnownLeaderID := types.NodeID("node1")
	initialCommitIndex := types.Index(15)
	initialLastApplied := types.Index(10)

	sm.mu.Lock()
	sm.currentTerm = initialTerm
	sm.role = initialRole
	sm.leaderID = initialLeaderID
	sm.lastKnownLeaderID = initialLastKnownLeaderID
	sm.commitIndex = initialCommitIndex
	sm.lastApplied = initialLastApplied
	sm.mu.Unlock()

	t.Run("Accessors", func(t *testing.T) {
		tests := []struct {
			name    string
			runTest func(t *testing.T)
		}{
			{
				name: "GetState",
				runTest: func(t *testing.T) {
					term, role, leaderID := sm.GetState()
					if term != initialTerm {
						t.Errorf("GetState() got term=%v, want %v", term, initialTerm)
					}
					if role != initialRole {
						t.Errorf("GetState() got role=%v, want %v", role, initialRole)
					}
					if leaderID != initialLeaderID {
						t.Errorf("GetState() got leaderID=%v, want %v", leaderID, initialLeaderID)
					}
				},
			},
			{
				name: "GetStateUnsafe",
				runTest: func(t *testing.T) {
					sm.mu.RLock()
					term, role, leaderID := sm.GetStateUnsafe()
					sm.mu.RUnlock()

					if term != initialTerm {
						t.Errorf("GetStateUnsafe() got term=%v, want %v", term, initialTerm)
					}
					if role != initialRole {
						t.Errorf("GetStateUnsafe() got role=%v, want %v", role, initialRole)
					}
					if leaderID != initialLeaderID {
						t.Errorf("GetStateUnsafe() got leaderID=%v, want %v", leaderID, initialLeaderID)
					}
				},
			},
			{
				name: "GetLastKnownLeader",
				runTest: func(t *testing.T) {
					leaderID := sm.GetLastKnownLeader()
					if leaderID != initialLastKnownLeaderID {
						t.Errorf("GetLastKnownLeader() got %v, want %v", leaderID, initialLastKnownLeaderID)
					}

					sm.mu.Lock()
					sm.leaderID = ""
					sm.mu.Unlock()

					leaderID = sm.GetLastKnownLeader()
					if leaderID != initialLastKnownLeaderID {
						t.Errorf("GetLastKnownLeader() after leader unknown, got %v, want %v", leaderID, initialLastKnownLeaderID)
					}
				},
			},
			{
				name: "GetCommitIndex",
				runTest: func(t *testing.T) {
					commitIndex := sm.GetCommitIndex()
					if commitIndex != initialCommitIndex {
						t.Errorf("GetCommitIndex() got %v, want %v", commitIndex, initialCommitIndex)
					}
				},
			},
			{
				name: "GetCommitIndexUnsafe",
				runTest: func(t *testing.T) {
					sm.mu.RLock()
					commitIndex := sm.GetCommitIndexUnsafe()
					sm.mu.RUnlock()

					if commitIndex != initialCommitIndex {
						t.Errorf("GetCommitIndexUnsafe() got %v, want %v", commitIndex, initialCommitIndex)
					}
				},
			},
			{
				name: "GetLastApplied",
				runTest: func(t *testing.T) {
					lastApplied := sm.GetLastApplied()
					if lastApplied != initialLastApplied {
						t.Errorf("GetLastApplied() got %v, want %v", lastApplied, initialLastApplied)
					}
				},
			},
			{
				name: "GetLastAppliedUnsafe",
				runTest: func(t *testing.T) {
					sm.mu.RLock()
					lastApplied := sm.GetLastAppliedUnsafe()
					sm.mu.RUnlock()

					if lastApplied != initialLastApplied {
						t.Errorf("GetLastAppliedUnsafe() got %v, want %v", lastApplied, initialLastApplied)
					}
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, tt.runTest)
		}
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		var wg sync.WaitGroup
		iterations := 100

		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range iterations {
					_, _, _ = sm.GetState()
					_ = sm.GetCommitIndex()
					_ = sm.GetLastApplied()
					_ = sm.GetLastKnownLeader()
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				sm.mu.Lock()
				sm.commitIndex++
				sm.lastApplied++
				sm.mu.Unlock()
			}
		}()

		wg.Wait()

		finalCommitIndex := sm.GetCommitIndex()
		finalLastApplied := sm.GetLastApplied()

		if finalCommitIndex < initialCommitIndex {
			t.Errorf("Concurrent access test: commitIndex unexpectedly decreased")
		}

		if finalLastApplied < initialLastApplied {
			t.Errorf("Concurrent access test: lastApplied unexpectedly decreased")
		}
	})
}

func TestRaftState_BecomeCandidate(t *testing.T) {
	t.Run("successful transition", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 1, types.RoleFollower, "", "")

		success := sm.BecomeCandidate(context.Background(), ElectionReasonTimeout)
		if !success {
			t.Errorf("BecomeCandidate() = %v, want true", success)
		}

		term, role, votedFor, _ := getState(sm)
		if term != 2 || role != types.RoleCandidate || votedFor != "node1" {
			t.Errorf("BecomeCandidate() got term=%v, role=%v, votedFor=%v; want term=2, role=Candidate, votedFor=node1",
				term, role, votedFor)
		}
	})

	t.Run("invalid transition from leader", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 5, types.RoleLeader, "node1", "node1")

		success := sm.BecomeCandidate(context.Background(), ElectionReasonTimeout)
		if success {
			t.Errorf("BecomeCandidate() from Leader = %v, want false", success)
		}

		term, role, _, _ := getState(sm)
		if term != 5 || role != types.RoleLeader {
			t.Errorf("BecomeCandidate() from Leader should not change state")
		}
	})

	t.Run("storage error", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		setState(sm, 3, types.RoleFollower, "", "")
		mockStore.setFailure("SaveState", storage.ErrStorageIO)

		success := sm.BecomeCandidate(context.Background(), ElectionReasonTimeout)
		if success {
			t.Errorf("BecomeCandidate() with storage error = %v, want false", success)
		}

		term, role, _, _ := getState(sm)
		if term != 3 || role != types.RoleFollower {
			t.Errorf("BecomeCandidate() with storage error should rollback state")
		}
	})

	t.Run("attempt when shutdown", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 4, types.RoleFollower, "", "")
		sm.Stop()

		success := sm.BecomeCandidate(context.Background(), ElectionReasonTimeout)
		if success {
			t.Errorf("BecomeCandidate() on shutdown = %v, want false", success)
		}

		term, role, _, _ := getState(sm)
		if term != 4 || role != types.RoleFollower {
			t.Errorf("BecomeCandidate() on shutdown should not change state")
		}
	})
}

func TestRaftState_BecomeLeader(t *testing.T) {
	t.Run("successful transition", func(t *testing.T) {
		sm, _, leaderCh := newTestStateManager()
		setState(sm, 3, types.RoleCandidate, "node1", "")

		for len(leaderCh) > 0 { // Drain channel
			<-leaderCh
		}

		success := sm.BecomeLeader(context.Background())
		if !success {
			t.Errorf("BecomeLeader() = %v, want true", success)
		}

		term, role, _, leaderID := getState(sm)
		if term != 3 || role != types.RoleLeader || leaderID != "node1" {
			t.Errorf("BecomeLeader() got term=%v, role=%v, leaderID=%v; want term=3, role=Leader, leaderID=node1",
				term, role, leaderID)
		}

		select {
		case newLeader := <-leaderCh:
			if newLeader != "node1" {
				t.Errorf("BecomeLeader() notification = %v, want node1", newLeader)
			}
		default:
			t.Error("BecomeLeader() expected leader notification, got none")
		}
	})

	t.Run("invalid transition from follower", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 2, types.RoleFollower, "", "")

		success := sm.BecomeLeader(context.Background())
		if success {
			t.Errorf("BecomeLeader() from Follower = %v, want false", success)
		}

		_, role, _, _ := getState(sm)
		if role != types.RoleFollower {
			t.Errorf("BecomeLeader() from Follower should not change role")
		}
	})

	t.Run("attempt when shutdown", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 3, types.RoleCandidate, "node1", "")
		sm.Stop()

		success := sm.BecomeLeader(context.Background())
		if success {
			t.Errorf("BecomeLeader() on shutdown = %v, want false", success)
		}

		_, role, _, leaderID := getState(sm)
		if role != types.RoleCandidate || leaderID != "" {
			t.Errorf("BecomeLeader() on shutdown should not change role or leaderID")
		}
	})
}

func TestRaftState_BecomeFollower(t *testing.T) {
	t.Run("higher term", func(t *testing.T) {
		sm, mockStore, leaderCh := newTestStateManager()
		setState(sm, 2, types.RoleCandidate, "node1", "")
		mockStore.saveCounter = 0

		for len(leaderCh) > 0 { // Drain channel
			<-leaderCh
		}

		sm.BecomeFollower(context.Background(), 3, "node2")

		term, role, votedFor, leaderID := getState(sm)
		if term != 3 || role != types.RoleFollower || votedFor != "" || leaderID != "node2" {
			t.Errorf("BecomeFollower() got term=%v, role=%v, votedFor=%v, leaderID=%v; want term=3, role=Follower, votedFor=, leaderID=node2",
				term, role, votedFor, leaderID)
		}

		if mockStore.saveCounter != 1 {
			t.Errorf("BecomeFollower() with term change persistence count = %v, want 1", mockStore.saveCounter)
		}

		select {
		case newLeader := <-leaderCh:
			if newLeader != "node2" {
				t.Errorf("BecomeFollower() notification = %v, want node2", newLeader)
			}
		default:
			t.Error("BecomeFollower() expected leader notification, got none")
		}
	})

	t.Run("same term new leader", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		setState(sm, 7, types.RoleFollower, "node2", "")
		mockStore.saveCounter = 0

		sm.BecomeFollower(context.Background(), 7, "node2")

		term, role, votedFor, leaderID := getState(sm)
		if term != 7 || role != types.RoleFollower || votedFor != "node2" || leaderID != "node2" {
			t.Errorf("BecomeFollower() same term unexpected state change")
		}

		if mockStore.saveCounter != 0 {
			t.Errorf("BecomeFollower() same term persistence count = %v, want 0", mockStore.saveCounter)
		}
	})

	t.Run("lower term", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 10, types.RoleFollower, "", "node3")

		sm.BecomeFollower(context.Background(), 9, "node2")

		term, role, _, leaderID := getState(sm)
		if term != 10 || role != types.RoleFollower || leaderID != "node3" {
			t.Errorf("BecomeFollower() lower term should not change state")
		}
	})

	t.Run("attempt when shutdown", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 5, types.RoleCandidate, "node1", "")
		sm.Stop()

		sm.BecomeFollower(context.Background(), 6, "node2")

		term, role, votedFor, leaderID := getState(sm)
		if term != 5 || role != types.RoleCandidate || votedFor != "node1" || leaderID != "" {
			t.Errorf("BecomeFollower() on shutdown should not change state")
		}
	})

	t.Run("persistence failure rolls back", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		setState(sm, 4, types.RoleCandidate, "node1", "")

		mockStore.setFailure("SaveState", errors.New("disk write failure"))

		sm.BecomeFollower(context.Background(), 5, "node2")

		term, role, votedFor, leaderID := getState(sm)
		if term != 4 || role != types.RoleFollower || votedFor != "node1" || leaderID != "node2" {
			t.Errorf("BecomeFollower() on persist failure rolled back term/vote, but still set leader; got term=%v, role=%v, votedFor=%v, leaderID=%v",
				term, role, votedFor, leaderID)
		}
	})

	t.Run("no-op when already follower with same term and leader", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		setState(sm, 6, types.RoleFollower, "", "node2")

		mockStore.saveCounter = 0 // should remain 0

		sm.BecomeFollower(context.Background(), 6, "node2")

		term, role, _, leaderID := getState(sm)
		if term != 6 || role != types.RoleFollower || leaderID != "node2" {
			t.Errorf("BecomeFollower() no-op failed; got term=%v, role=%v, leaderID=%v; want 6, Follower, node2", term, role, leaderID)
		}

		if mockStore.saveCounter != 0 {
			t.Errorf("BecomeFollower() no-op should not persist; got saveCounter=%v, want 0", mockStore.saveCounter)
		}
	})
}

func TestRaftState_CheckTermAndStepDown(t *testing.T) {
	t.Run("higher term", func(t *testing.T) {
		sm, _, leaderCh := newTestStateManager()
		setState(sm, 3, types.RoleLeader, "node1", "node1")

		for len(leaderCh) > 0 { // Drain channel
			<-leaderCh
		}

		steppedDown, prevTerm := sm.CheckTermAndStepDown(context.Background(), 4, "node2")
		if !steppedDown || prevTerm != 3 {
			t.Errorf("CheckTermAndStepDown() got steppedDown=%v, prevTerm=%v; want steppedDown=true, prevTerm=3",
				steppedDown, prevTerm)
		}

		term, role, votedFor, leaderID := getState(sm)
		if term != 4 || role != types.RoleFollower || votedFor != "" || leaderID != "node2" {
			t.Errorf("CheckTermAndStepDown() got term=%v, role=%v, votedFor=%v, leaderID=%v; want term=4, role=Follower, votedFor=, leaderID=node2",
				term, role, votedFor, leaderID)
		}

		select {
		case newLeader := <-leaderCh:
			if newLeader != "node2" {
				t.Errorf("CheckTermAndStepDown() notification = %v, want node2", newLeader)
			}
		default:
			t.Error("CheckTermAndStepDown() expected leader notification, got none")
		}
	})

	t.Run("same term update leader", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 8, types.RoleFollower, "", "")

		steppedDown, _ := sm.CheckTermAndStepDown(context.Background(), 8, "node3")
		if !steppedDown {
			t.Errorf("CheckTermAndStepDown() same term got steppedDown=%v, want true", steppedDown)
		}

		_, _, _, leaderID := getState(sm)
		if leaderID != "node3" {
			t.Errorf("CheckTermAndStepDown() same term got leaderID=%v, want node3", leaderID)
		}
	})

	t.Run("lower term", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 10, types.RoleLeader, "node1", "node1")

		steppedDown, prevTerm := sm.CheckTermAndStepDown(context.Background(), 8, "node2")
		if steppedDown || prevTerm != 10 {
			t.Errorf("CheckTermAndStepDown() lower term got steppedDown=%v, prevTerm=%v; want steppedDown=false, prevTerm=10",
				steppedDown, prevTerm)
		}

		_, role, _, leaderID := getState(sm)
		if role != types.RoleLeader || leaderID != "node1" {
			t.Errorf("CheckTermAndStepDown() lower term state changed unexpectedly")
		}
	})

	t.Run("called on shutdown node", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 5, types.RoleFollower, "", "")
		sm.Stop()

		steppedDown, prevTerm := sm.CheckTermAndStepDown(context.Background(), 6, "node2")
		if steppedDown || prevTerm != 5 {
			t.Errorf("CheckTermAndStepDown() on shutdown got steppedDown=%v, prevTerm=%v; want false, 5", steppedDown, prevTerm)
		}
	})

	t.Run("persistence failure during step down", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		setState(sm, 3, types.RoleLeader, "node1", "node1")
		mockStore.setFailure("SaveState", errors.New("disk write error"))

		steppedDown, prevTerm := sm.CheckTermAndStepDown(context.Background(), 4, "node2")
		if !steppedDown || prevTerm != 3 {
			t.Errorf("CheckTermAndStepDown() with persist error got steppedDown=%v, prevTerm=%v; want true, 3", steppedDown, prevTerm)
		}

		term, role, votedFor, leaderID := getState(sm)
		if term != 3 || role != types.RoleFollower || votedFor != "node1" || leaderID != "node2" {
			t.Errorf("CheckTermAndStepDown() after persist failure did not roll back correctly")
		}
	})

	t.Run("same term step down from non-follower", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 6, types.RoleLeader, "node1", "node1") // not a follower

		steppedDown, prevTerm := sm.CheckTermAndStepDown(context.Background(), 6, "node3")
		if !steppedDown || prevTerm != 6 {
			t.Errorf("CheckTermAndStepDown() same term non-follower got steppedDown=%v, prevTerm=%v; want true, 6", steppedDown, prevTerm)
		}

		_, role, _, leaderID := getState(sm)
		if role != types.RoleFollower || leaderID != "node3" {
			t.Errorf("CheckTermAndStepDown() same term non-follower should become follower with new leader")
		}
	})
}

func TestRaftState_GrantVote(t *testing.T) {
	t.Run("grant first vote", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		setState(sm, 5, types.RoleFollower, "", "")
		mockStore.saveCounter = 0

		granted := sm.GrantVote(context.Background(), "node2", 5)
		if !granted {
			t.Errorf("GrantVote() = %v, want true", granted)
		}

		_, _, votedFor, _ := getState(sm)
		if votedFor != "node2" {
			t.Errorf("GrantVote() votedFor = %v, want node2", votedFor)
		}

		if mockStore.saveCounter != 1 {
			t.Errorf("GrantVote() persistence count = %v, want 1", mockStore.saveCounter)
		}
	})

	t.Run("already voted for different candidate", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 7, types.RoleFollower, "node2", "node2")

		granted := sm.GrantVote(context.Background(), "node3", 7)
		if granted {
			t.Errorf("GrantVote() different candidate = %v, want false", granted)
		}

		_, _, votedFor, _ := getState(sm)
		if votedFor != "node2" {
			t.Errorf("GrantVote() votedFor changed unexpectedly")
		}
	})

	t.Run("lower term request", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 10, types.RoleFollower, "", "")

		granted := sm.GrantVote(context.Background(), "node2", 9)
		if granted {
			t.Errorf("GrantVote() lower term = %v, want false", granted)
		}
	})

	t.Run("vote rejected due to shutdown", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		setState(sm, 5, types.RoleFollower, "", "")
		sm.Stop()

		granted := sm.GrantVote(context.Background(), "node2", 5)
		if granted {
			t.Errorf("GrantVote() on shutdown = %v, want false", granted)
		}

		_, _, votedFor, _ := getState(sm)
		if votedFor != "" {
			t.Errorf("GrantVote() on shutdown should not vote, got votedFor=%v", votedFor)
		}
	})

	t.Run("persistence failure rolls back vote", func(t *testing.T) {
		sm, mockStore, _ := newTestStateManager()
		setState(sm, 6, types.RoleFollower, "", "")
		mockStore.setFailure("SaveState", errors.New("disk write failure"))

		granted := sm.GrantVote(context.Background(), "node2", 6)
		if granted {
			t.Errorf("GrantVote() with persist failure = %v, want false", granted)
		}

		_, _, votedFor, _ := getState(sm)
		if votedFor != "" {
			t.Errorf("GrantVote() with persist failure should rollback vote, got votedFor=%v", votedFor)
		}
	})
}

func TestRaftState_UpdateCommitIndex(t *testing.T) {
	t.Run("successful update", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		sm.mu.Lock()
		sm.commitIndex = 5
		sm.mu.Unlock()

		updated := sm.UpdateCommitIndex(10)
		if !updated {
			t.Errorf("UpdateCommitIndex() = %v, want true", updated)
		}

		if sm.GetCommitIndex() != 10 {
			t.Errorf("UpdateCommitIndex() commit index = %v, want 10", sm.GetCommitIndex())
		}
	})

	t.Run("equal or lower value", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		sm.mu.Lock()
		sm.commitIndex = 15
		sm.mu.Unlock()

		updated := sm.UpdateCommitIndex(12)
		if updated {
			t.Errorf("UpdateCommitIndex() lower value = %v, want false", updated)
		}

		if sm.GetCommitIndex() != 15 {
			t.Errorf("UpdateCommitIndex() commit index changed unexpectedly")
		}
	})
}

func TestRaftState_UpdateLastApplied(t *testing.T) {
	t.Run("successful update", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		sm.mu.Lock()
		sm.commitIndex = 10
		sm.lastApplied = 5
		sm.mu.Unlock()

		updated := sm.UpdateLastApplied(8)
		if !updated {
			t.Errorf("UpdateLastApplied() = %v, want true", updated)
		}

		if sm.GetLastApplied() != 8 {
			t.Errorf("UpdateLastApplied() last applied = %v, want 8", sm.GetLastApplied())
		}
	})

	t.Run("beyond commit index", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		sm.mu.Lock()
		sm.commitIndex = 25
		sm.lastApplied = 20
		sm.mu.Unlock()

		updated := sm.UpdateLastApplied(30)
		if updated {
			t.Errorf("UpdateLastApplied() beyond commit = %v, want false", updated)
		}

		if sm.GetLastApplied() != 20 {
			t.Errorf("UpdateLastApplied() last applied changed unexpectedly")
		}
	})

	t.Run("no-op when newLastApplied is not greater", func(t *testing.T) {
		sm, _, _ := newTestStateManager()
		sm.mu.Lock()
		sm.commitIndex = 15
		sm.lastApplied = 10
		sm.mu.Unlock()

		updated := sm.UpdateLastApplied(10)
		if updated {
			t.Errorf("UpdateLastApplied() with equal value = %v, want false", updated)
		}
		if sm.GetLastApplied() != 10 {
			t.Errorf("UpdateLastApplied() with equal value changed lastApplied unexpectedly")
		}

		updated = sm.UpdateLastApplied(9)
		if updated {
			t.Errorf("UpdateLastApplied() with lower value = %v, want false", updated)
		}
		if sm.GetLastApplied() != 10 {
			t.Errorf("UpdateLastApplied() with lower value changed lastApplied unexpectedly")
		}
	})
}

func TestStop(t *testing.T) {
	sm, _, _ := newTestStateManager()

	if sm.isShutdown.Load() {
		t.Error("StateManager initially shutdown")
	}

	sm.Stop()

	if !sm.isShutdown.Load() {
		t.Error("StateManager.Stop() did not set shutdown flag")
	}

	sm.Stop() // Call again to test multiple calls

	if !sm.isShutdown.Load() {
		t.Error("StateManager.Stop() second call unset shutdown flag")
	}
}

func TestSetRoleAndLeaderLocked_NoChange(t *testing.T) {
	sm, _, _ := newTestStateManager()

	initialRole := types.RoleLeader
	initialLeader := types.NodeID("node1")
	initialTerm := types.Term(5)

	sm.mu.Lock()
	sm.role = initialRole
	sm.leaderID = initialLeader
	sm.currentTerm = initialTerm
	sm.mu.Unlock()

	sm.mu.Lock()
	sm.setRoleAndLeaderLocked(initialRole, initialLeader, initialTerm)
	sm.mu.Unlock()

	term, role, _, leaderID := getState(sm)
	if role != initialRole || leaderID != initialLeader || term != initialTerm {
		t.Errorf("setRoleAndLeaderLocked() with no change modified state unexpectedly: role=%v, leaderID=%v, term=%v",
			role, leaderID, term)
	}
}

func TestNotifyLeaderChange_SkippedDueToShutdown(t *testing.T) {
	sm, _, leaderCh := newTestStateManager()
	sm.Stop()

	sm.notifyLeaderChangeNonBlocking("nodeX", 99)

	select {
	case <-leaderCh:
		t.Errorf("Expected no notification due to shutdown, but got one")
	default:
		// Expected path (no notification)
	}
}

func TestNotifyLeaderChange_ChannelFull(t *testing.T) {
	sm, _, leaderCh := newTestStateManager()

	for i := range cap(leaderCh) {
		leaderCh <- types.NodeID(fmt.Sprintf("node%d", i))
	}

	sm.notifyLeaderChangeNonBlocking("overflowNode", 42)
}
