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
	"github.com/jathurchan/raftlock/types"
)

func TestRaftElection_ElectionManager_Initialize(t *testing.T) {
	tests := []struct {
		name         string
		initTerm     types.Term
		initRole     types.NodeRole
		expectError  bool
		cancelCtx    bool
		withShutdown bool
	}{
		{"term 0 as follower", 0, types.RoleFollower, false, false, false},
		{"term 0 as candidate", 0, types.RoleCandidate, true, false, false},
		{"term 0 as leader", 0, types.RoleLeader, true, false, false},
		{"term 1 as follower", 1, types.RoleFollower, false, false, false},
		{"term 1 as candidate", 1, types.RoleCandidate, false, false, false},
		{"term 1 as leader", 1, types.RoleLeader, false, false, false},
		{"context cancelled", 0, types.RoleFollower, true, true, false},
		{"shutdown flag set", 0, types.RoleFollower, true, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mu := &sync.RWMutex{}
			shutdown := &atomic.Bool{}
			if tt.withShutdown {
				shutdown.Store(true)
			}

			storage := newMockStorage()
			storage.state = types.PersistentState{CurrentTerm: tt.initTerm}

			stateMgr := NewStateManager(mu, shutdown, Dependencies{
				Storage: storage,
				Logger:  logger.NewNoOpLogger(),
				Metrics: newMockMetrics(),
			}, "node1", make(chan types.NodeID, 1))

			if err := stateMgr.Initialize(context.Background()); err != nil {
				t.Fatalf("state manager init failed: %v", err)
			}

			s := stateMgr.(*stateManager)
			s.mu.Lock()
			s.currentTerm = tt.initTerm
			s.role = tt.initRole
			s.mu.Unlock()

			logMgr := NewLogManager(mu, shutdown, Dependencies{
				Storage: storage,
				Logger:  logger.NewNoOpLogger(),
				Metrics: newMockMetrics(),
			}, "node1")

			if err := logMgr.Initialize(context.Background()); err != nil {
				t.Fatalf("log manager init failed: %v", err)
			}

			deps := ElectionManagerDeps{
				ID:                "node1",
				Peers:             map[types.NodeID]PeerConfig{"node1": {ID: "node1", Address: "addr1"}},
				QuorumSize:        1,
				Mu:                mu,
				IsShutdown:        shutdown,
				StateMgr:          stateMgr,
				LogMgr:            logMgr,
				NetworkMgr:        &mockNetworkManager{},
				LeaderInitializer: &mockLeaderInitializer{},
				Metrics:           newMockMetrics(),
				Logger:            logger.NewNoOpLogger(),
				Rand:              &mockRand{},
				Config: Config{
					Options: Options{
						ElectionTickCount:           DefaultElectionTickCount,
						HeartbeatTickCount:          DefaultHeartbeatTickCount,
						ElectionRandomizationFactor: DefaultElectionRandomizationFact,
					},
					FeatureFlags: FeatureFlags{PreVoteEnabled: true},
				},
			}

			eMgr, err := NewElectionManager(deps)
			if err != nil {
				t.Fatalf("failed to create election manager: %v", err)
			}

			ctx := context.Background()
			if tt.cancelCtx {
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}

			err = eMgr.Initialize(ctx)

			if tt.expectError && err == nil {
				t.Errorf("expected error, got nil")
			} else if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestRaftElection_ValidateElectionManagerDeps(t *testing.T) {
	base := ElectionManagerDeps{
		ID:                "node1",
		Peers:             map[types.NodeID]PeerConfig{"node1": {ID: "node1", Address: "addr1"}},
		QuorumSize:        1,
		Mu:                &sync.RWMutex{},
		IsShutdown:        &atomic.Bool{},
		StateMgr:          &mockStateManager{},
		LogMgr:            &mockLogManager{},
		NetworkMgr:        &mockNetworkManager{},
		LeaderInitializer: &mockLeaderInitializer{},
		Metrics:           newMockMetrics(),
		Logger:            logger.NewNoOpLogger(),
		Rand:              &mockRand{},
		Config:            Config{},
	}

	tests := []struct {
		name      string
		modify    func(d ElectionManagerDeps) ElectionManagerDeps
		expectErr string
	}{
		{"missing ID", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.ID = unknownNodeID
			return d
		}, "ID must be provided"},

		{"zero quorum size", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.QuorumSize = 0
			return d
		}, "quorum size must be positive"},

		{"nil Mu", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.Mu = nil
			return d
		}, "mutex (Mu) must not be nil"},

		{"nil IsShutdown", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.IsShutdown = nil
			return d
		}, "shutdown flag (IsShutdown) must not be nil"},

		{"nil StateMgr", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.StateMgr = nil
			return d
		}, "state manager (StateMgr) must not be nil"},

		{"nil LogMgr", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.LogMgr = nil
			return d
		}, "log manager (LogMgr) must not be nil"},

		{"nil NetworkMgr", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.NetworkMgr = nil
			return d
		}, "network manager (NetworkMgr) must not be nil"},

		{"nil LeaderInitializer", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.LeaderInitializer = nil
			return d
		}, "leader initializer (LeaderInitializer) must not be nil"},

		{"nil Metrics", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.Metrics = nil
			return d
		}, "metrics implementation (Metrics) must not be nil"},

		{"nil Logger", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.Logger = nil
			return d
		}, "logger implementation (Logger) must not be nil"},

		{"nil Rand", func(d ElectionManagerDeps) ElectionManagerDeps {
			d.Rand = nil
			return d
		}, "random number generator (Rand) must not be nil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateElectionManagerDeps(tt.modify(base))
			if err == nil || !strings.Contains(err.Error(), tt.expectErr) {
				t.Errorf("expected error to contain %q, got %v", tt.expectErr, err)
			}
		})
	}
}

func TestRaftElection_NewElectionManager_InvalidDepsWrapsError(t *testing.T) {
	deps := ElectionManagerDeps{
		ID:                "node1",
		Peers:             map[types.NodeID]PeerConfig{},
		QuorumSize:        1,
		Mu:                &sync.RWMutex{},
		IsShutdown:        &atomic.Bool{},
		StateMgr:          &mockStateManager{},
		LogMgr:            &mockLogManager{},
		NetworkMgr:        &mockNetworkManager{},
		LeaderInitializer: &mockLeaderInitializer{},
		Metrics:           newMockMetrics(),
		Logger:            logger.NewNoOpLogger(),
		Rand:              nil, // force error
		Config:            Config{},
	}

	_, err := NewElectionManager(deps)
	if err == nil {
		t.Fatal("expected error but got nil")
	}

	if !strings.Contains(err.Error(), "invalid election manager dependencies") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRaftElection_NewElectionManager_ValidationWarnings(t *testing.T) {
	base := ElectionManagerDeps{
		ID:                "node1",
		Peers:             map[types.NodeID]PeerConfig{"node1": {ID: "node1", Address: "addr1"}},
		QuorumSize:        1,
		Mu:                &sync.RWMutex{},
		IsShutdown:        &atomic.Bool{},
		StateMgr:          &mockStateManager{},
		LogMgr:            &mockLogManager{},
		NetworkMgr:        &mockNetworkManager{},
		LeaderInitializer: &mockLeaderInitializer{},
		Metrics:           newMockMetrics(),
		Logger:            logger.NewNoOpLogger(),
		Rand:              &mockRand{},
	}

	t.Run("ElectionTickCount <= HeartbeatTickCount", func(t *testing.T) {
		deps := base
		deps.Config.Options.ElectionTickCount = DefaultHeartbeatTickCount
		deps.Config.Options.HeartbeatTickCount = DefaultHeartbeatTickCount

		if _, err := NewElectionManager(deps); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Invalid ElectionRandomizationFactor", func(t *testing.T) {
		deps := base
		deps.Config.Options.ElectionTickCount = DefaultElectionTickCount
		deps.Config.Options.HeartbeatTickCount = DefaultHeartbeatTickCount
		deps.Config.Options.ElectionRandomizationFactor = 1.5 // invalid

		if _, err := NewElectionManager(deps); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestRaftElection_ElectionManager_Tick(t *testing.T) {
	tests := []struct {
		name                string
		tickCount           int
		electionTickCount   int
		expectElectionStart bool
		role                types.NodeRole
		randomizationFactor float64
		cancelContext       bool
		setShutdown         bool
	}{
		{
			name:                "Leader should not trigger election regardless of tick count",
			tickCount:           20,
			electionTickCount:   10,
			expectElectionStart: false,
			role:                types.RoleLeader,
			randomizationFactor: 0,
		},
		{
			name:                "Follower without enough ticks should not trigger election",
			tickCount:           5,
			electionTickCount:   10,
			expectElectionStart: false,
			role:                types.RoleFollower,
			randomizationFactor: 0,
		},
		{
			name:                "Follower with enough ticks should trigger election",
			tickCount:           15,
			electionTickCount:   10,
			expectElectionStart: true,
			role:                types.RoleFollower,
			randomizationFactor: 0,
		},
		{
			name:                "Tick skipped due to canceled context",
			tickCount:           1,
			electionTickCount:   1,
			expectElectionStart: false,
			role:                types.RoleFollower,
			randomizationFactor: 0,
			cancelContext:       true,
		},
		{
			name:                "Tick skipped due to shutdown flag",
			tickCount:           1,
			electionTickCount:   1,
			expectElectionStart: false,
			role:                types.RoleFollower,
			randomizationFactor: 0,
			setShutdown:         true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mu := &sync.RWMutex{}
			shutdownFlag := &atomic.Bool{}

			mockStorage := newMockStorage()
			mockMetrics := newMockMetrics()
			mockLogger := logger.NewNoOpLogger()
			mockRand := &mockRand{}

			stateMgr := NewStateManager(mu, shutdownFlag, Dependencies{
				Storage: mockStorage,
				Logger:  mockLogger,
				Metrics: mockMetrics,
			}, "node1", make(chan types.NodeID, 1))

			stateMgr.(*stateManager).mu.Lock()
			stateMgr.(*stateManager).currentTerm = 1
			stateMgr.(*stateManager).role = tc.role
			stateMgr.(*stateManager).mu.Unlock()

			logMgr := NewLogManager(mu, shutdownFlag, Dependencies{
				Storage: mockStorage,
				Logger:  mockLogger,
				Metrics: mockMetrics,
			}, "node1")

			mockLeaderInit := &mockLeaderInitializer{}
			electionTriggered := make(chan struct{}, 1)

			deps := ElectionManagerDeps{
				ID:                "node1",
				Peers:             map[types.NodeID]PeerConfig{"node1": {ID: "node1", Address: "addr1"}},
				QuorumSize:        1,
				Mu:                mu,
				IsShutdown:        shutdownFlag,
				StateMgr:          stateMgr,
				LogMgr:            logMgr,
				NetworkMgr:        &mockNetworkManager{},
				LeaderInitializer: mockLeaderInit,
				Metrics:           mockMetrics,
				Logger:            mockLogger,
				Rand:              mockRand,
				Config: Config{
					Options: Options{
						ElectionTickCount:           tc.electionTickCount,
						HeartbeatTickCount:          1,
						ElectionRandomizationFactor: tc.randomizationFactor,
					},
					FeatureFlags: FeatureFlags{
						PreVoteEnabled: true,
					},
				},
			}

			ctx := context.Background()
			cancel := func() {}
			if tc.cancelContext {
				c, cancelFn := context.WithCancel(ctx)
				ctx = c
				cancel = cancelFn
			}

			electionMgr, err := NewElectionManager(deps)
			if err != nil {
				t.Fatalf("Failed to create ElectionManager: %v", err)
			}

			if err := electionMgr.Initialize(ctx); err != nil {
				if !tc.setShutdown {
					t.Fatalf("Failed to initialize ElectionManager: %v", err)
				}
			}

			if tc.setShutdown {
				shutdownFlag.Store(true)
			}

			if tc.cancelContext {
				cancel()
			}

			em := electionMgr.(*electionManager)
			originalHandler := em.handleElectionTimeout
			em.handleElectionTimeout = func(ctx context.Context) {
				electionTriggered <- struct{}{}
			}
			defer func() { em.handleElectionTimeout = originalHandler }()

			for range tc.tickCount {
				electionMgr.Tick(ctx)
			}

			if tc.expectElectionStart {
				select {
				case <-electionTriggered: // success
				case <-time.After(100 * time.Millisecond):
					t.Errorf("Expected election to start but it didn't")
				}
			} else {
				select {
				case <-electionTriggered:
					t.Errorf("Expected no election to start but it did")
				case <-time.After(50 * time.Millisecond): // success
				}
			}
		})
	}
}

func TestRaftElection_ElectionManager_defaultHandleElectionTimeout(t *testing.T) {
	t.Run("Aborts when context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		em := &electionManager{
			id:         "node1",
			logger:     logger.NewNoOpLogger(),
			isShutdown: &atomic.Bool{},
		}

		em.defaultHandleElectionTimeout(ctx)
	})

	t.Run("Aborts when node is shutting down", func(t *testing.T) {
		ctx := context.Background()
		shutdown := &atomic.Bool{}
		shutdown.Store(true)

		em := &electionManager{
			id:         "node1",
			logger:     logger.NewNoOpLogger(),
			isShutdown: shutdown,
		}

		em.defaultHandleElectionTimeout(ctx)
	})

	t.Run("Skips election if already leader", func(t *testing.T) {
		ctx := context.Background()

		em := &electionManager{
			id:         "node1",
			logger:     logger.NewNoOpLogger(),
			isShutdown: &atomic.Bool{},
			stateMgr: &mockStateManager{
				role: types.RoleLeader,
			},
		}

		em.defaultHandleElectionTimeout(ctx)
	})

	t.Run("Starts pre-vote if enabled (minimal setup)", func(t *testing.T) {
		ctx := context.Background()
		called := false

		em := &electionManager{
			id:            "node1",
			logger:        logger.NewNoOpLogger(),
			isShutdown:    &atomic.Bool{},
			enablePreVote: true,
			stateMgr:      &mockStateManager{role: types.RoleFollower},
		}

		em.startPreVote = func(_ context.Context) {
			called = true
		}

		em.defaultHandleElectionTimeout(ctx)

		if !called {
			t.Errorf("Expected startPreVote to be called")
		}
	})

	t.Run("Starts pre-vote if enabled (more complete mock)", func(t *testing.T) {
		ctx := context.Background()
		called := false

		em := &electionManager{
			id:            "node1",
			logger:        logger.NewNoOpLogger(),
			isShutdown:    &atomic.Bool{},
			enablePreVote: true,
			mu:            &sync.RWMutex{},
			stateMgr: &mockStateManager{
				role: types.RoleFollower,
				GetStateFunc: func() (types.Term, types.NodeRole, types.NodeID) {
					return 1, types.RoleFollower, "node1"
				},
			},
			metrics:    newMockMetrics(),
			rand:       &mockRand{},
			logMgr:     &mockLogManager{},
			networkMgr: &mockNetworkManager{},
		}

		em.startPreVote = func(_ context.Context) {
			called = true
		}

		em.defaultHandleElectionTimeout(ctx)

		if !called {
			t.Errorf("Expected startPreVote to be called")
		}
	})
}

func TestRaftElection_ElectionManager_StartElection(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name             string
		singleNode       bool
		networkSucceeds  bool
		expectLeadership bool
	}{
		{
			name:             "Single-node cluster becomes leader immediately",
			singleNode:       true,
			networkSucceeds:  false,
			expectLeadership: true,
		},
		{
			name:             "Multi-node cluster with votes granted",
			singleNode:       false,
			networkSucceeds:  true,
			expectLeadership: true,
		},
		{
			name:             "Multi-node cluster with failed vote requests",
			singleNode:       false,
			networkSucceeds:  false,
			expectLeadership: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mu := &sync.RWMutex{}
			shutdown := &atomic.Bool{}
			logger := logger.NewNoOpLogger()

			mockNetwork := &mockNetworkManager{
				requestVoteSuccess: tc.networkSucceeds,
				requestVoteReplies: map[types.NodeID]*types.RequestVoteReply{
					"node2": {Term: 1, VoteGranted: true},
					"node3": {Term: 1, VoteGranted: true},
				},
			}

			peers := map[types.NodeID]PeerConfig{
				"node1": {ID: "node1", Address: "addr1"},
			}
			if !tc.singleNode {
				peers["node2"] = PeerConfig{ID: "node2", Address: "addr2"}
				peers["node3"] = PeerConfig{ID: "node3", Address: "addr3"}
			}

			leaderInit := &mockLeaderInitializer{}
			stateMgr := &mockStateManager{term: 0, role: types.RoleFollower}

			deps := ElectionManagerDeps{
				ID:                "node1",
				Peers:             peers,
				QuorumSize:        (len(peers) / 2) + 1,
				Mu:                mu,
				IsShutdown:        shutdown,
				StateMgr:          stateMgr,
				LogMgr:            &mockLogManager{},
				NetworkMgr:        mockNetwork,
				LeaderInitializer: leaderInit,
				Metrics:           newMockMetrics(),
				Logger:            logger,
				Rand:              &mockRand{},
				Config: Config{
					Options: Options{
						ElectionTickCount:           DefaultElectionTickCount,
						HeartbeatTickCount:          DefaultHeartbeatTickCount,
						ElectionRandomizationFactor: 0,
					},
					FeatureFlags: FeatureFlags{PreVoteEnabled: true},
				},
			}

			em, err := NewElectionManager(deps)
			if err != nil {
				t.Fatalf("Failed to create ElectionManager: %v", err)
			}

			em.(*electionManager).startElection(ctx)
			time.Sleep(50 * time.Millisecond)

			if tc.expectLeadership {
				if stateMgr.role != types.RoleLeader {
					t.Errorf("Expected leader role, got: %s", stateMgr.role)
				}
				if !leaderInit.initializeStateCalled {
					t.Error("Expected InitializeLeaderState to be called")
				}
				if !leaderInit.sendHeartbeatsCalled {
					t.Error("Expected SendHeartbeats to be called")
				}
			} else {
				if stateMgr.role != types.RoleCandidate {
					t.Errorf("Expected candidate role, got: %s", stateMgr.role)
				}
			}
		})
	}
}

func TestRaftElection_ElectionManager_ElectionEdgeCases(t *testing.T) {
	tests := []struct {
		name                string
		cancelContext       bool
		shutdownBefore      bool
		failBecomeCandidate bool
		returnNonCandidate  bool
		voteReply           *types.RequestVoteReply
		verify              func(t *testing.T, em *electionManager)
	}{
		{"Context cancelled before election", true, false, false, false, nil, nil},
		{"Shutdown before election starts", false, true, false, false, nil, nil},
		{"Fail to become candidate", false, false, true, false, nil, nil},
		{"Wrong role after BecomeCandidate", false, false, false, true, nil, nil},
		{"Vote RPC fails (nil reply)", false, false, false, false, nil, nil},
		{"Vote denied by peer", false, false, false, false, &types.RequestVoteReply{Term: 1, VoteGranted: false}, nil},
		{"Higher term in vote reply", false, false, false, false, &types.RequestVoteReply{Term: 5, VoteGranted: false}, nil},
		{"Stale vote reply ignored", false, false, false, false, &types.RequestVoteReply{Term: 1, VoteGranted: true}, nil},
		{
			"Shutdown during processVoteReply",
			false, true, false, false,
			&types.RequestVoteReply{Term: 1, VoteGranted: true},
			func(t *testing.T, em *electionManager) {
				em.processVoteReply(context.Background(), "node2", 1, &types.RequestVoteReply{Term: 1, VoteGranted: true})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.cancelContext {
				cancelCtx, cancel := context.WithCancel(context.Background())
				cancel()
				ctx = cancelCtx
			}

			mu := &sync.RWMutex{}
			shutdown := &atomic.Bool{}
			if tc.shutdownBefore {
				shutdown.Store(true)
			}

			stateMgr := &mockStateManager{term: 0, role: types.RoleFollower}

			if tc.failBecomeCandidate {
				stateMgr.BecomeCandidateFunc = func(ctx context.Context, r ElectionReason) bool {
					return false
				}
			}
			if tc.returnNonCandidate {
				stateMgr.GetStateFunc = func() (types.Term, types.NodeRole, types.NodeID) {
					return stateMgr.term, types.RoleFollower, "node1"
				}
			}

			mockNet := &mockNetworkManager{requestVoteSuccess: tc.voteReply != nil}
			if tc.voteReply != nil {
				mockNet.requestVoteReplies = map[types.NodeID]*types.RequestVoteReply{
					"node2": tc.voteReply,
				}
			}

			em, err := NewElectionManager(ElectionManagerDeps{
				ID:                "node1",
				Peers:             map[types.NodeID]PeerConfig{"node1": {}, "node2": {}},
				QuorumSize:        2,
				Mu:                mu,
				IsShutdown:        shutdown,
				StateMgr:          stateMgr,
				LogMgr:            &mockLogManager{},
				NetworkMgr:        mockNet,
				LeaderInitializer: &mockLeaderInitializer{},
				Metrics:           newMockMetrics(),
				Logger:            logger.NewNoOpLogger(),
				Rand:              &mockRand{},
				Config: Config{
					Options:      Options{ElectionTickCount: 5, HeartbeatTickCount: 1, ElectionRandomizationFactor: 0},
					FeatureFlags: FeatureFlags{PreVoteEnabled: false},
				},
			})
			if err != nil {
				t.Fatalf("NewElectionManager failed: %v", err)
			}

			manager := em.(*electionManager)
			manager.startElection(ctx)
			time.Sleep(50 * time.Millisecond)

			if tc.verify != nil {
				tc.verify(t, manager)
			}
		})
	}
}

func TestRaftElection_ElectionManager_PreVote(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		preVoteGranted bool
		expectElection bool
	}{
		{"PreVote_GrantedStartsElection", true, true},
		{"PreVote_DeniedDoesNotStartElection", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startElectionCh := make(chan struct{}, 1)

			em := setupTestElectionManager(t)
			em.startElection = func(_ context.Context) {
				startElectionCh <- struct{}{}
			}
			em.resetVotesReceived()

			em.networkMgr = &mockNetworkManager{
				requestVoteReplies: map[types.NodeID]*types.RequestVoteReply{
					"node2": {Term: 2, VoteGranted: tt.preVoteGranted},
					"node3": {Term: 2, VoteGranted: tt.preVoteGranted},
				},
				requestVoteSuccess: true,
			}

			em.startPreVote(ctx)

			select {
			case <-startElectionCh:
				if !tt.expectElection {
					t.Error("Unexpected election triggered")
				}
			case <-time.After(200 * time.Millisecond):
				if tt.expectElection {
					t.Error("Expected election to start but it did not")
				}
			}
		})
	}
}

func TestRaftElection_ElectionManager_PreVote_StartConditions(t *testing.T) {
	tests := []struct {
		name      string
		configure func(ctx context.Context, em *electionManager)
	}{
		{
			name: "ContextCancelled",
			configure: func(_ context.Context, em *electionManager) {
				ctx := cancelledContext()
				em.startPreVote(ctx)
			},
		},
		{
			name: "NodeIsShutdown",
			configure: func(ctx context.Context, em *electionManager) {
				em.isShutdown.Store(true)
				em.startPreVote(ctx)
			},
		},
		{
			name: "AlreadyLeader",
			configure: func(ctx context.Context, em *electionManager) {
				em.stateMgr = &mockStateManager{
					term: 2,
					role: types.RoleLeader,
				}
				em.startPreVote(ctx)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			em := setupTestElectionManager(t)
			tt.configure(context.Background(), em)
		})
	}
}

func cancelledContext() context.Context {
	c, cancel := context.WithCancel(context.Background())
	cancel()
	return c
}

func TestRaftElection_ElectionManager_PreVote_RequestFailures(t *testing.T) {
	tests := []struct {
		name            string
		errorToSimulate error
	}{
		{"ContextCanceled", context.Canceled},
		{"DeadlineExceeded", context.DeadlineExceeded},
		{"GenericNetworkError", errors.New("mock network failure")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			em := setupTestElectionManager(t)
			args := &types.RequestVoteArgs{
				Term:         2,
				CandidateID:  "node1",
				LastLogIndex: 10,
				LastLogTerm:  1,
				IsPreVote:    true,
			}

			em.networkMgr = &mockNetworkManager{
				SendRequestVoteFunc: func(_ context.Context, _ types.NodeID, _ *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
					return nil, tt.errorToSimulate
				},
			}

			em.sendPreVoteRequest(context.Background(), "node2", args, 1)

		})
	}
}

func TestRaftElection_ElectionManager_PreVote_ProcessReplySkips(t *testing.T) {
	t.Run("SkipIfNodeShutdown", func(t *testing.T) {
		em := setupTestElectionManager(t)
		em.isShutdown.Store(true)

		em.processPreVoteReply(
			"node2", 2, 1,
			&types.RequestVoteReply{Term: 2, VoteGranted: true},
		)
	})

	t.Run("SkipIfHigherTermReply", func(t *testing.T) {
		em := setupTestElectionManager(t)

		em.processPreVoteReply(
			"node2", 2, 1,
			&types.RequestVoteReply{Term: 99, VoteGranted: false},
		)
	})

	t.Run("SkipIfStaleReply_TermChangedLocally", func(t *testing.T) {
		em := setupTestElectionManager(t)

		em.stateMgr = &mockStateManager{
			term: 3,
			role: types.RoleFollower,
			GetStateFunc: func() (types.Term, types.NodeRole, types.NodeID) {
				return 3, types.RoleFollower, "node1"
			},
		}

		em.processPreVoteReply(
			"node2", 2, 1,
			&types.RequestVoteReply{Term: 2, VoteGranted: true},
		)
	})
}

func setupTestElectionManager(t *testing.T) *electionManager {
	mu := &sync.RWMutex{}
	shutdown := &atomic.Bool{}
	logger := logger.NewNoOpLogger()

	em, err := NewElectionManager(ElectionManagerDeps{
		ID:                "node1",
		Peers:             map[types.NodeID]PeerConfig{"node1": {}, "node2": {}},
		QuorumSize:        2,
		Mu:                mu,
		IsShutdown:        shutdown,
		StateMgr:          &mockStateManager{term: 1, role: types.RoleFollower},
		LogMgr:            &mockLogManager{},
		NetworkMgr:        &mockNetworkManager{},
		LeaderInitializer: &mockLeaderInitializer{},
		Metrics:           newMockMetrics(),
		Logger:            logger,
		Rand:              &mockRand{},
		Config: Config{
			Options: Options{
				ElectionTickCount:           10,
				HeartbeatTickCount:          1,
				ElectionRandomizationFactor: 0,
			},
			FeatureFlags: FeatureFlags{PreVoteEnabled: true},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create ElectionManager: %v", err)
	}
	return em.(*electionManager)
}

func TestRaftElection_ElectionManager_HandleRequestVote(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		currentTerm    types.Term
		requestTerm    types.Term
		votedFor       types.NodeID
		candidateID    types.NodeID
		localLastIndex types.Index
		localLastTerm  types.Term
		reqLastIndex   types.Index
		reqLastTerm    types.Term
		isPreVote      bool
		expectVote     bool
		expectError    bool
		cancelContext  bool
		shutdown       bool
	}{
		{
			name:        "Reject vote - lower term",
			currentTerm: 5,
			requestTerm: 4,
			expectVote:  false,
		},
		{
			name:           "Grant vote - higher term, log up to date",
			currentTerm:    5,
			requestTerm:    6,
			reqLastTerm:    5,
			reqLastIndex:   10,
			localLastIndex: 10,
			localLastTerm:  5,
			expectVote:     true,
		},
		{
			name:           "Reject vote - log not up to date (lower term)",
			currentTerm:    5,
			requestTerm:    6,
			reqLastTerm:    4,
			reqLastIndex:   10,
			localLastIndex: 10,
			localLastTerm:  5,
			expectVote:     false,
		},
		{
			name:           "Reject vote - log not up to date (lower index)",
			currentTerm:    5,
			requestTerm:    6,
			reqLastTerm:    5,
			reqLastIndex:   9,
			localLastIndex: 10,
			localLastTerm:  5,
			expectVote:     false,
		},
		{
			name:        "Reject vote - already voted for other",
			currentTerm: 5,
			requestTerm: 5,
			votedFor:    "node3",
			expectVote:  false,
		},
		{
			name:        "Grant vote - same term, not voted yet",
			currentTerm: 5,
			requestTerm: 5,
			expectVote:  true,
		},
		{
			name:        "Grant pre-vote - higher term",
			currentTerm: 5,
			requestTerm: 6,
			votedFor:    "node3",
			isPreVote:   true,
			expectVote:  true,
		},
		{
			name:           "Reject pre-vote - log not up to date",
			currentTerm:    5,
			requestTerm:    6,
			reqLastTerm:    5,
			reqLastIndex:   9,
			localLastTerm:  5,
			localLastIndex: 10,
			isPreVote:      true,
			expectVote:     false,
		},
		{
			name:          "Reject vote - context cancelled",
			currentTerm:   5,
			requestTerm:   6,
			cancelContext: true,
			expectVote:    false,
			expectError:   true,
		},
		{
			name:        "Reject vote - node is shutting down",
			currentTerm: 5,
			requestTerm: 6,
			shutdown:    true,
			expectVote:  false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mu := &sync.RWMutex{}
			shutdownFlag := &atomic.Bool{}
			shutdownFlag.Store(tt.shutdown)

			mockStorage := newMockStorage()
			mockMetrics := newMockMetrics()
			mockLogger := logger.NewNoOpLogger()
			mockRand := &mockRand{}

			stateMgr := NewStateManager(mu, shutdownFlag, Dependencies{
				Storage: mockStorage,
				Logger:  mockLogger,
				Metrics: mockMetrics,
			}, "node1", make(chan types.NodeID, 1))

			stateMgr.(*stateManager).mu.Lock()
			stateMgr.(*stateManager).currentTerm = tt.currentTerm
			stateMgr.(*stateManager).votedFor = tt.votedFor
			stateMgr.(*stateManager).role = types.RoleFollower
			stateMgr.(*stateManager).mu.Unlock()

			logMgr := &mockLogManager{
				lastIndex: tt.localLastIndex,
				lastTerm:  tt.localLastTerm,
			}

			deps := ElectionManagerDeps{
				ID:                "node1",
				Peers:             map[types.NodeID]PeerConfig{"node1": {}, "node2": {}},
				QuorumSize:        2,
				Mu:                mu,
				IsShutdown:        shutdownFlag,
				StateMgr:          stateMgr,
				LogMgr:            logMgr,
				NetworkMgr:        &mockNetworkManager{},
				LeaderInitializer: &mockLeaderInitializer{},
				Metrics:           mockMetrics,
				Logger:            mockLogger,
				Rand:              mockRand,
				Config: Config{
					Options: Options{
						ElectionTickCount:           DefaultElectionTickCount,
						HeartbeatTickCount:          DefaultHeartbeatTickCount,
						ElectionRandomizationFactor: 0,
					},
					FeatureFlags: FeatureFlags{
						PreVoteEnabled: true,
					},
				},
			}

			electionMgr, err := NewElectionManager(deps)
			if err != nil {
				t.Fatalf("Failed to create ElectionManager: %v", err)
			}
			if err := electionMgr.Initialize(ctx); err != nil {
				if !tt.shutdown {
					if err := electionMgr.Initialize(ctx); err != nil {
						t.Fatalf("Failed to initialize ElectionManager: %v", err)
					}
				}
			}

			testCtx := ctx
			var cancelFunc context.CancelFunc
			if tt.cancelContext {
				testCtx, cancelFunc = context.WithCancel(ctx)
				cancelFunc()
			}

			args := &types.RequestVoteArgs{
				Term:         tt.requestTerm,
				CandidateID:  "node2",
				LastLogIndex: tt.reqLastIndex,
				LastLogTerm:  tt.reqLastTerm,
				IsPreVote:    tt.isPreVote,
			}

			reply, err := electionMgr.HandleRequestVote(testCtx, args)

			if tt.expectError && err == nil {
				t.Fatalf("Expected error but got nil")
			} else if !tt.expectError && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if reply == nil {
				t.Fatalf("Expected reply but got nil")
			}

			if reply.VoteGranted != tt.expectVote {
				t.Errorf("Expected VoteGranted = %v, got %v", tt.expectVote, reply.VoteGranted)
			}
		})
	}
}

func TestElectionManager_ResetTimerOnHeartbeat(t *testing.T) {
	t.Run("Resets timer when active", func(t *testing.T) {
		ctx := context.Background()

		mu := &sync.RWMutex{}
		shutdownFlag := &atomic.Bool{}

		mockStorage := newMockStorage()
		mockMetrics := newMockMetrics()
		mockLogger := logger.NewNoOpLogger()
		mockRand := &mockRand{}

		stateMgr := NewStateManager(mu, shutdownFlag, Dependencies{
			Storage: mockStorage,
			Logger:  mockLogger,
			Metrics: mockMetrics,
		}, "node1", make(chan types.NodeID, 1))

		logMgr := &mockLogManager{lastIndex: 10, lastTerm: 5}

		electionMgr, err := NewElectionManager(ElectionManagerDeps{
			ID:                "node1",
			Peers:             map[types.NodeID]PeerConfig{"node1": {ID: "node1", Address: "addr1"}},
			QuorumSize:        1,
			Mu:                mu,
			IsShutdown:        shutdownFlag,
			StateMgr:          stateMgr,
			LogMgr:            logMgr,
			NetworkMgr:        &mockNetworkManager{},
			LeaderInitializer: &mockLeaderInitializer{},
			Metrics:           mockMetrics,
			Logger:            mockLogger,
			Rand:              mockRand,
			Config: Config{
				Options: Options{
					ElectionTickCount:           10,
					HeartbeatTickCount:          1,
					ElectionRandomizationFactor: 0,
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to create ElectionManager: %v", err)
		}

		if err := electionMgr.Initialize(ctx); err != nil {
			t.Fatalf("Failed to initialize ElectionManager: %v", err)
		}

		em := electionMgr.(*electionManager)

		for range 5 {
			em.Tick(ctx)
		}

		em.mu.RLock()
		elapsed := em.electionElapsed
		em.mu.RUnlock()

		if elapsed != 5 {
			t.Errorf("Expected electionElapsed = 5, got %d", elapsed)
		}

		electionMgr.ResetTimerOnHeartbeat()

		em.mu.RLock()
		resetElapsed := em.electionElapsed
		em.mu.RUnlock()

		if resetElapsed != 0 {
			t.Errorf("Expected electionElapsed reset to 0, got %d", resetElapsed)
		}
	})

	t.Run("Skips timer reset when shutting down", func(t *testing.T) {

		mu := &sync.RWMutex{}
		shutdownFlag := &atomic.Bool{}
		shutdownFlag.Store(true)

		mockLogger := logger.NewNoOpLogger()
		electionMgr := &electionManager{
			mu:                mu,
			isShutdown:        shutdownFlag,
			logger:            mockLogger,
			rand:              &mockRand{},
			stateMgr:          &mockStateManager{},
			logMgr:            &mockLogManager{},
			metrics:           newMockMetrics(),
			id:                "node1",
			peers:             map[types.NodeID]PeerConfig{"node1": {}},
			quorumSize:        1,
			electionTickCount: 10,
		}

		electionMgr.mu.Lock()
		electionMgr.electionElapsed = 5
		electionMgr.mu.Unlock()

		electionMgr.ResetTimerOnHeartbeat()

		electionMgr.mu.RLock()
		elapsed := electionMgr.electionElapsed
		electionMgr.mu.RUnlock()

		if elapsed != 5 {
			t.Errorf("Expected electionElapsed to remain 5 due to shutdown, got %d", elapsed)
		}
	})
}

func TestRaftElection_ElectionManager_RecordVoteAndCheckQuorum(t *testing.T) {
	tests := []struct {
		name                string
		voters              []types.NodeID
		quorumSize          int
		expectQuorumReached bool
	}{
		{
			name:                "Self vote sufficient in 1-node cluster",
			voters:              []types.NodeID{"node1"},
			quorumSize:          1,
			expectQuorumReached: true,
		},
		{
			name:                "2-node cluster, only self vote",
			voters:              []types.NodeID{"node1"},
			quorumSize:          2,
			expectQuorumReached: false,
		},
		{
			name:                "2-node cluster, both votes",
			voters:              []types.NodeID{"node1", "node2"},
			quorumSize:          2,
			expectQuorumReached: true,
		},
		{
			name:                "5-node cluster, 3 votes",
			voters:              []types.NodeID{"node1", "node2", "node3"},
			quorumSize:          3,
			expectQuorumReached: true,
		},
		{
			name:                "5-node cluster, 2 votes only",
			voters:              []types.NodeID{"node1", "node2"},
			quorumSize:          3,
			expectQuorumReached: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mu := &sync.RWMutex{}
			mockLogger := logger.NewNoOpLogger()
			mockRand := &mockRand{}
			mockMetrics := newMockMetrics()

			peers := map[types.NodeID]PeerConfig{}
			for i := 1; i <= 5; i++ {
				id := types.NodeID(fmt.Sprintf("node%d", i))
				peers[id] = PeerConfig{ID: id, Address: fmt.Sprintf("addr%d", i)}
			}

			electionMgr, err := NewElectionManager(ElectionManagerDeps{
				ID:                "node1",
				Peers:             peers,
				QuorumSize:        tt.quorumSize,
				Mu:                mu,
				IsShutdown:        &atomic.Bool{},
				StateMgr:          &mockStateManager{},
				LogMgr:            &mockLogManager{},
				NetworkMgr:        &mockNetworkManager{},
				LeaderInitializer: &mockLeaderInitializer{},
				Metrics:           mockMetrics,
				Logger:            mockLogger,
				Rand:              mockRand,
				Config: Config{
					Options: Options{
						ElectionTickCount:           DefaultElectionTickCount,
						ElectionRandomizationFactor: 0,
					},
				},
			})
			if err != nil {
				t.Fatalf("Failed to create ElectionManager: %v", err)
			}

			em := electionMgr.(*electionManager)
			em.resetVotesReceived()

			quorum := false
			for _, voter := range tt.voters {
				if voter == "node1" {
					em.voteMu.Lock()
					voteCount := len(em.votesReceived)
					em.voteMu.Unlock()
					if voteCount >= tt.quorumSize {
						quorum = true
					}
					continue
				}
				quorum = em.recordVoteAndCheckQuorum(voter, 1)
			}

			if quorum != tt.expectQuorumReached {
				t.Errorf("Expected quorumReached = %v, got %v", tt.expectQuorumReached, quorum)
			}

			em.voteMu.Lock()
			voteCount := len(em.votesReceived)
			em.voteMu.Unlock()

			if voteCount != len(tt.voters) {
				t.Errorf("Expected %d votes recorded, got %d", len(tt.voters), voteCount)
			}
		})
	}
}
