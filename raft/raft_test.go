package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

func createTestRaftNode(t *testing.T) (*raftNode, *mockStateManager, *mockLogManager, *mockElectionManager, *mockSnapshotManager, *mockReplicationManager, *mockNetworkManager, *mockApplier) {
	t.Helper()

	nodeID := types.NodeID("node1")
	mu := &sync.RWMutex{}

	stateMgr := newMockStateManager()
	logMgr := newMockLogManager()
	electionMgr := &mockElectionManager{}
	snapshotMgr := newMockSnapshotManager()
	replicationMgr := &mockReplicationManager{
		replicationStatus: make(map[types.NodeID]types.PeerState),
	}
	networkMgr := newMockNetworkManager()
	applier := &mockApplier{}
	mockLog := logger.NewNoOpLogger()
	metrics := newMockMetrics()
	clock := newMockClock()

	return &raftNode{
		id:                  nodeID,
		mu:                  mu,
		stateMgr:            stateMgr,
		logMgr:              logMgr,
		electionMgr:         electionMgr,
		snapshotMgr:         snapshotMgr,
		replicationMgr:      replicationMgr,
		networkMgr:          networkMgr,
		applier:             applier,
		logger:              mockLog,
		metrics:             metrics,
		clock:               clock,
		stopCh:              make(chan struct{}),
		applyCh:             make(chan types.ApplyMsg, 100),
		applyNotifyCh:       make(chan struct{}, 1),
		leaderChangeCh:      make(chan types.NodeID, 1),
		applyLoopStopCh:     make(chan struct{}),
		applyLoopDoneCh:     make(chan struct{}),
		applyEntryTimeout:   1 * time.Second,
		fetchEntriesTimeout: 1 * time.Second,
	}, stateMgr, logMgr, electionMgr, snapshotMgr, replicationMgr, networkMgr, applier
}

func TestRaft_RaftNode_Start(t *testing.T) {
	t.Run("successful start", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		go func() {
			time.Sleep(10 * time.Millisecond)
			close(r.applyLoopDoneCh)
		}()

		err := r.Start()
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("network manager start error", func(t *testing.T) {
		r, _, _, _, _, _, networkMgr, _ := createTestRaftNode(t)

		networkMgr.sendAppendEntriesFunc = nil // simulate init error
		networkMgr.startFunc = func() error {
			return errors.New("network start error")
		}

		err := r.Start()
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if !strings.Contains(err.Error(), "network initialization failed") {
			t.Errorf("Expected network start error, got %v", err)
		}
	})

	t.Run("state manager initialize error", func(t *testing.T) {
		r, stateMgr, _, _, _, _, _, _ := createTestRaftNode(t)

		stateMgr.initializeFunc = func(context.Context) error {
			return errors.New("state init error")
		}

		err := r.Start()
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if !strings.Contains(err.Error(), "state manager initialization failed") {
			t.Errorf("Expected state manager error, got %v", err)
		}
	})

	t.Run("log manager initialize error", func(t *testing.T) {
		r, _, logMgr, _, _, _, _, _ := createTestRaftNode(t)

		logMgr.initializeFunc = func(context.Context) error {
			return errors.New("log init error")
		}

		err := r.Start()
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if !strings.Contains(err.Error(), "log manager initialization failed") {
			t.Errorf("Expected log manager error, got %v", err)
		}
	})

	t.Run("snapshot manager initialize error", func(t *testing.T) {
		r, _, _, _, snapshotMgr, _, _, _ := createTestRaftNode(t)

		snapshotMgr.initializeFunc = func(context.Context) error {
			return errors.New("snapshot init error")
		}

		err := r.Start()
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if !strings.Contains(err.Error(), "snapshot manager initialization failed") {
			t.Errorf("Expected snapshot manager error, got %v", err)
		}
	})

	t.Run("election manager initialize error", func(t *testing.T) {
		r, _, _, electionMgr, _, _, _, _ := createTestRaftNode(t)

		electionMgr.initializeFunc = func(context.Context) error {
			return errors.New("election init error")
		}

		err := r.Start()
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if !strings.Contains(err.Error(), "election manager initialization failed") {
			t.Errorf("Expected election manager error, got %v", err)
		}
	})

	t.Run("start when already shutdown", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		r.isShutdown.Store(true)

		err := r.Start()
		if err == nil {
			t.Errorf("Expected error, got nil")
		}
		if !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected shutdown error, got %v", err)
		}
	})
}

func TestRaft_RaftNode_Stop(t *testing.T) {
	t.Run("successful stop", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		go func() {
			time.Sleep(10 * time.Millisecond)
			close(r.applyLoopDoneCh)
		}()

		ctx := context.Background()
		err := r.Stop(ctx)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if !r.isShutdown.Load() {
			t.Errorf("Expected isShutdown to be true")
		}
	})

	t.Run("stop when already stopped", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		go func() {
			time.Sleep(10 * time.Millisecond)
			close(r.applyLoopDoneCh)
		}()

		r.isShutdown.Store(true)

		ctx := context.Background()
		err := r.Stop(ctx)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("stop context canceled", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := r.Stop(ctx)
		if err == nil {
			t.Errorf("Expected context canceled error, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})

	t.Run("network manager stop error", func(t *testing.T) {
		r, _, _, _, _, _, networkMgr, _ := createTestRaftNode(t)

		go func() {
			time.Sleep(10 * time.Millisecond)
			close(r.applyLoopDoneCh)
		}()

		networkMgr.stopFunc = func() error {
			return errors.New("network stop error")
		}

		ctx := context.Background()
		err := r.Stop(ctx)

		if err != nil {
			t.Errorf("Expected no error despite network error, got %v", err)
		}
	})
}

func TestRaft_RaftNode_Tick(t *testing.T) {
	t.Run("follower tick", func(t *testing.T) {
		r, stateMgr, _, electionMgr, snapshotMgr, _, _, _ := createTestRaftNode(t)

		stateMgr.currentRole = types.RoleFollower

		electionTickCalled := 0
		snapshotTickCalled := 0

		electionMgr.tickFunc = func(ctx context.Context) {
			electionTickCalled++
		}
		snapshotMgr.tickFunc = func(ctx context.Context) {
			snapshotTickCalled++
		}

		ctx := context.Background()
		r.Tick(ctx)

		if electionTickCalled != 1 {
			t.Errorf("Expected election manager Tick to be called once, got %d", electionTickCalled)
		}
		if snapshotTickCalled != 1 {
			t.Errorf("Expected snapshot manager Tick to be called once, got %d", snapshotTickCalled)
		}
	})

	t.Run("leader tick", func(t *testing.T) {
		r, stateMgr, _, electionMgr, snapshotMgr, replicationMgr, _, _ := createTestRaftNode(t)

		stateMgr.currentRole = types.RoleLeader

		electionTickCalled := 0
		replicationTickCalled := 0
		snapshotTickCalled := 0

		electionMgr.tickFunc = func(ctx context.Context) {
			electionTickCalled++
		}
		replicationMgr.tickFunc = func(ctx context.Context) {
			replicationTickCalled++
		}
		snapshotMgr.tickFunc = func(ctx context.Context) {
			snapshotTickCalled++
		}

		ctx := context.Background()
		r.Tick(ctx)

		if electionTickCalled != 0 {
			t.Errorf("Expected election manager Tick not to be called, got %d", electionTickCalled)
		}
		if replicationTickCalled != 1 {
			t.Errorf("Expected replication manager Tick to be called once, got %d", replicationTickCalled)
		}
		if snapshotTickCalled != 1 {
			t.Errorf("Expected snapshot manager Tick to be called once, got %d", snapshotTickCalled)
		}
	})

	t.Run("tick when shutdown", func(t *testing.T) {
		r, _, _, electionMgr, snapshotMgr, replicationMgr, _, _ := createTestRaftNode(t)

		r.isShutdown.Store(true)

		electionTickCalled := 0
		replicationTickCalled := 0
		snapshotTickCalled := 0

		electionMgr.tickFunc = func(ctx context.Context) {
			electionTickCalled++
		}
		replicationMgr.tickFunc = func(ctx context.Context) {
			replicationTickCalled++
		}
		snapshotMgr.tickFunc = func(ctx context.Context) {
			snapshotTickCalled++
		}

		ctx := context.Background()
		r.Tick(ctx)

		if electionTickCalled != 0 {
			t.Errorf("Expected election manager Tick not to be called during shutdown, got %d", electionTickCalled)
		}
		if replicationTickCalled != 0 {
			t.Errorf("Expected replication manager Tick not to be called during shutdown, got %d", replicationTickCalled)
		}
		if snapshotTickCalled != 0 {
			t.Errorf("Expected snapshot manager Tick not to be called during shutdown, got %d", snapshotTickCalled)
		}
	})

	t.Run("candidate tick", func(t *testing.T) {
		r, stateMgr, _, electionMgr, snapshotMgr, replicationMgr, _, _ := createTestRaftNode(t)

		stateMgr.currentRole = types.RoleCandidate

		electionTickCalled := 0
		replicationTickCalled := 0
		snapshotTickCalled := 0

		electionMgr.tickFunc = func(ctx context.Context) {
			electionTickCalled++
		}
		replicationMgr.tickFunc = func(ctx context.Context) {
			replicationTickCalled++
		}
		snapshotMgr.tickFunc = func(ctx context.Context) {
			snapshotTickCalled++
		}

		ctx := context.Background()
		r.Tick(ctx)

		if electionTickCalled != 1 {
			t.Errorf("Expected election manager Tick to be called once, got %d", electionTickCalled)
		}
		if replicationTickCalled != 0 {
			t.Errorf("Expected replication manager Tick not to be called, got %d", replicationTickCalled)
		}
		if snapshotTickCalled != 1 {
			t.Errorf("Expected snapshot manager Tick to be called once, got %d", snapshotTickCalled)
		}
	})
}

func TestRaftNode_Propose(t *testing.T) {
	t.Run("successful proposal", func(t *testing.T) {
		r, _, _, _, _, replicationMgr, _, _ := createTestRaftNode(t)

		expectedIndex := types.Index(10)
		expectedTerm := types.Term(2)
		expectedIsLeader := true

		replicationMgr.proposeFunc = func(ctx context.Context, command []byte) (types.Index, types.Term, bool, error) {
			return expectedIndex, expectedTerm, expectedIsLeader, nil
		}

		ctx := context.Background()
		command := []byte("test command")

		index, term, isLeader, err := r.Propose(ctx, command)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if index != expectedIndex {
			t.Errorf("Expected index %d, got %d", expectedIndex, index)
		}
		if term != expectedTerm {
			t.Errorf("Expected term %d, got %d", expectedTerm, term)
		}
		if !isLeader {
			t.Errorf("Expected isLeader true, got false")
		}
	})

	t.Run("not leader error", func(t *testing.T) {
		r, _, _, _, _, replicationMgr, _, _ := createTestRaftNode(t)

		replicationMgr.proposeFunc = func(ctx context.Context, command []byte) (types.Index, types.Term, bool, error) {
			return 0, 0, false, ErrNotLeader
		}

		ctx := context.Background()
		command := []byte("test command")

		_, _, isLeader, err := r.Propose(ctx, command)

		if err == nil {
			t.Errorf("Expected not leader error, got nil")
		}
		if !errors.Is(err, ErrNotLeader) {
			t.Errorf("Expected ErrNotLeader, got %v", err)
		}
		if isLeader {
			t.Errorf("Expected isLeader false, got true")
		}
	})

	t.Run("empty command", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		ctx := context.Background()
		command := []byte{}

		_, _, _, err := r.Propose(ctx, command)

		if err == nil {
			t.Errorf("Expected empty command error, got nil")
		}
	})

	t.Run("propose when shutdown", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		r.isShutdown.Store(true)

		ctx := context.Background()
		command := []byte("test command")

		_, _, _, err := r.Propose(ctx, command)

		if err == nil {
			t.Errorf("Expected shutdown error, got nil")
		}
		if !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected ErrShuttingDown, got %v", err)
		}
	})
}

func TestRaft_RaftNode_ReadIndex(t *testing.T) {
	t.Run("successful read index", func(t *testing.T) {
		r, _, _, _, _, replicationMgr, _, _ := createTestRaftNode(t)

		expectedIndex := types.Index(10)
		replicationMgr.verifyLeadershipAndGetCommitIndexFunc = func(ctx context.Context) (types.Index, error) {
			return expectedIndex, nil
		}

		ctx := context.Background()
		index, err := r.ReadIndex(ctx)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if index != expectedIndex {
			t.Errorf("Expected index %d, got %d", expectedIndex, index)
		}
	})

	t.Run("not leader error", func(t *testing.T) {
		r, _, _, _, _, replicationMgr, _, _ := createTestRaftNode(t)

		replicationMgr.verifyLeadershipAndGetCommitIndexFunc = func(ctx context.Context) (types.Index, error) {
			return 0, ErrNotLeader
		}

		ctx := context.Background()
		_, err := r.ReadIndex(ctx)

		if err == nil {
			t.Errorf("Expected not leader error, got nil")
		}
		if !errors.Is(err, ErrNotLeader) {
			t.Errorf("Expected ErrNotLeader, got %v", err)
		}
	})

	t.Run("read index when shutdown", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		r.isShutdown.Store(true)

		ctx := context.Background()
		_, err := r.ReadIndex(ctx)

		if err == nil {
			t.Errorf("Expected shutdown error, got nil")
		}
		if !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected ErrShuttingDown, got %v", err)
		}
	})
}

func TestRaft_RaftNode_Status(t *testing.T) {
	t.Run("get status", func(t *testing.T) {
		r, stateMgr, logMgr, _, snapshotMgr, replicationMgr, _, _ := createTestRaftNode(t)

		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleLeader
		stateMgr.leaderID = "node1"
		stateMgr.commitIndex = 20
		stateMgr.lastApplied = 15

		logMgr.lastIndex = 25
		logMgr.lastTerm = 5

		snapshotMgr.meta = types.SnapshotMetadata{
			LastIncludedIndex: 10,
			LastIncludedTerm:  3,
		}

		peer2State := types.PeerState{
			NextIndex:  22,
			MatchIndex: 21,
			IsActive:   true,
		}
		replicationMgr.replicationStatus = map[types.NodeID]types.PeerState{
			"node2": peer2State,
		}

		status := r.Status()

		if status.ID != r.id {
			t.Errorf("Expected ID %s, got %s", r.id, status.ID)
		}
		if status.Term != stateMgr.currentTerm {
			t.Errorf("Expected Term %d, got %d", stateMgr.currentTerm, status.Term)
		}
		if status.Role != stateMgr.currentRole {
			t.Errorf("Expected Role %s, got %s", stateMgr.currentRole, status.Role)
		}
		if status.LeaderID != stateMgr.leaderID {
			t.Errorf("Expected LeaderID %s, got %s", stateMgr.leaderID, status.LeaderID)
		}
		if status.LastLogIndex != logMgr.lastIndex {
			t.Errorf("Expected LastLogIndex %d, got %d", logMgr.lastIndex, status.LastLogIndex)
		}
		if status.LastLogTerm != logMgr.lastTerm {
			t.Errorf("Expected LastLogTerm %d, got %d", logMgr.lastTerm, status.LastLogTerm)
		}
		if status.CommitIndex != stateMgr.commitIndex {
			t.Errorf("Expected CommitIndex %d, got %d", stateMgr.commitIndex, status.CommitIndex)
		}
		if status.LastApplied != stateMgr.lastApplied {
			t.Errorf("Expected LastApplied %d, got %d", stateMgr.lastApplied, status.LastApplied)
		}
		if status.SnapshotIndex != snapshotMgr.meta.LastIncludedIndex {
			t.Errorf("Expected SnapshotIndex %d, got %d", snapshotMgr.meta.LastIncludedIndex, status.SnapshotIndex)
		}
		if status.SnapshotTerm != snapshotMgr.meta.LastIncludedTerm {
			t.Errorf("Expected SnapshotTerm %d, got %d", snapshotMgr.meta.LastIncludedTerm, status.SnapshotTerm)
		}

		if _, exists := status.Replication["node2"]; !exists {
			t.Errorf("Expected replication status for node2, but it's missing")
		} else {
			nodeStatus := status.Replication["node2"]
			if nodeStatus.NextIndex != peer2State.NextIndex {
				t.Errorf("Expected NextIndex %d, got %d", peer2State.NextIndex, nodeStatus.NextIndex)
			}
			if nodeStatus.MatchIndex != peer2State.MatchIndex {
				t.Errorf("Expected MatchIndex %d, got %d", peer2State.MatchIndex, nodeStatus.MatchIndex)
			}
			if nodeStatus.IsActive != peer2State.IsActive {
				t.Errorf("Expected IsActive %v, got %v", peer2State.IsActive, nodeStatus.IsActive)
			}
		}
	})

	t.Run("status for follower", func(t *testing.T) {
		r, stateMgr, _, _, _, _, _, _ := createTestRaftNode(t)

		stateMgr.currentRole = types.RoleFollower

		status := r.Status()

		if len(status.Replication) != 0 {
			t.Errorf("Expected empty Replication map for follower, got %d entries", len(status.Replication))
		}
	})
}

func TestRaft_RaftNode_GetState(t *testing.T) {
	t.Run("get state when leader", func(t *testing.T) {
		r, stateMgr, _, _, _, _, _, _ := createTestRaftNode(t)

		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleLeader

		term, isLeader := r.GetState()

		if term != stateMgr.currentTerm {
			t.Errorf("Expected term %d, got %d", stateMgr.currentTerm, term)
		}
		if !isLeader {
			t.Errorf("Expected isLeader true, got false")
		}
		if !r.isLeader.Load() {
			t.Errorf("Expected isLeader atomic boolean to be true")
		}
	})

	t.Run("get state when follower", func(t *testing.T) {
		r, stateMgr, _, _, _, _, _, _ := createTestRaftNode(t)

		stateMgr.currentTerm = 5
		stateMgr.currentRole = types.RoleFollower

		term, isLeader := r.GetState()

		if term != stateMgr.currentTerm {
			t.Errorf("Expected term %d, got %d", stateMgr.currentTerm, term)
		}
		if isLeader {
			t.Errorf("Expected isLeader false, got true")
		}
		if r.isLeader.Load() {
			t.Errorf("Expected isLeader atomic boolean to be false")
		}
	})
}

func TestRaft_RaftNode_Getters(t *testing.T) {
	t.Run("get leader ID", func(t *testing.T) {
		r, stateMgr, _, _, _, _, _, _ := createTestRaftNode(t)

		expectedLeaderID := types.NodeID("leader1")
		stateMgr.leaderID = expectedLeaderID

		leaderID := r.GetLeaderID()

		if leaderID != expectedLeaderID {
			t.Errorf("Expected leader ID %s, got %s", expectedLeaderID, leaderID)
		}
	})

	t.Run("get commit index", func(t *testing.T) {
		r, stateMgr, _, _, _, _, _, _ := createTestRaftNode(t)

		expectedCommitIndex := types.Index(15)
		stateMgr.commitIndex = expectedCommitIndex

		commitIndex := r.GetCommitIndex()

		if commitIndex != expectedCommitIndex {
			t.Errorf("Expected commit index %d, got %d", expectedCommitIndex, commitIndex)
		}
	})

	t.Run("apply channel", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		got := r.ApplyChannel()
		want := r.applyCh

		if got != want {
			t.Errorf("ApplyChannel() returned unexpected channel: got %v, want %v", got, want)
		}
	})

	t.Run("leader change channel", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		got := r.LeaderChangeChannel()
		want := r.leaderChangeCh

		if got != want {
			t.Errorf("LeaderChangeChannel() returned unexpected channel: got %v, want %v", got, want)
		}
	})
}

func TestRaft_RaftNode_RPCHandlers(t *testing.T) {
	t.Run("request vote", func(t *testing.T) {
		r, _, _, electionMgr, _, _, _, _ := createTestRaftNode(t)

		args := &types.RequestVoteArgs{
			Term:         5,
			CandidateID:  "node2",
			LastLogIndex: 10,
			LastLogTerm:  4,
		}

		expectedReply := &types.RequestVoteReply{Term: 5, VoteGranted: true}
		electionMgr.handleRequestVoteResult = expectedReply

		ctx := context.Background()
		reply, err := r.RequestVote(ctx, args)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if reply.Term != expectedReply.Term {
			t.Errorf("Expected term %d, got %d", expectedReply.Term, reply.Term)
		}
		if reply.VoteGranted != expectedReply.VoteGranted {
			t.Errorf("Expected VoteGranted %v, got %v", expectedReply.VoteGranted, reply.VoteGranted)
		}
	})

	t.Run("request vote when shutdown", func(t *testing.T) {
		r, stateMgr, _, _, _, _, _, _ := createTestRaftNode(t)
		r.isShutdown.Store(true)
		stateMgr.currentTerm = 3

		args := &types.RequestVoteArgs{Term: 5, CandidateID: "node2"}
		ctx := context.Background()
		reply, err := r.RequestVote(ctx, args)

		if err == nil || !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected ErrShuttingDown, got %v", err)
		}
		if reply.Term != stateMgr.currentTerm {
			t.Errorf("Expected term %d, got %d", stateMgr.currentTerm, reply.Term)
		}
		if reply.VoteGranted {
			t.Errorf("Expected VoteGranted false, got true")
		}
	})

	t.Run("request vote with error", func(t *testing.T) {
		r, _, _, electionMgr, _, _, _, _ := createTestRaftNode(t)
		expectedErr := errors.New("vote error")
		electionMgr.handleRequestVoteErr = expectedErr

		args := &types.RequestVoteArgs{Term: 5, CandidateID: "node2"}
		ctx := context.Background()
		_, err := r.RequestVote(ctx, args)

		if err == nil || !errors.Is(err, expectedErr) {
			t.Errorf("Expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("append entries", func(t *testing.T) {
		r, stateMgr, _, electionMgr, _, replicationMgr, _, _ := createTestRaftNode(t)

		args := &types.AppendEntriesArgs{
			Term:         5,
			LeaderID:     "node2",
			PrevLogIndex: 9,
			PrevLogTerm:  3,
			Entries:      []types.LogEntry{{Index: 10, Term: 5}},
			LeaderCommit: 8,
		}

		expectedReply := &types.AppendEntriesReply{Term: 5, Success: true}
		replicationMgr.handleAppendEntriesResult = expectedReply
		stateMgr.currentTerm = 5

		ctx := context.Background()
		reply, err := r.AppendEntries(ctx, args)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if reply.Term != expectedReply.Term {
			t.Errorf("Expected term %d, got %d", expectedReply.Term, reply.Term)
		}
		if reply.Success != expectedReply.Success {
			t.Errorf("Expected Success %v, got %v", expectedReply.Success, reply.Success)
		}
		if electionMgr.resetTimerCalled != 1 {
			t.Errorf("Expected election timer reset once, got %d", electionMgr.resetTimerCalled)
		}
	})

	t.Run("append entries heartbeat", func(t *testing.T) {
		r, stateMgr, _, electionMgr, _, replicationMgr, _, _ := createTestRaftNode(t)

		args := &types.AppendEntriesArgs{
			Term:         5,
			LeaderID:     "node2",
			PrevLogIndex: 9,
			PrevLogTerm:  3,
			Entries:      nil,
			LeaderCommit: 8,
		}

		expectedReply := &types.AppendEntriesReply{Term: 5, Success: true}
		replicationMgr.handleAppendEntriesResult = expectedReply
		stateMgr.currentTerm = 5

		ctx := context.Background()
		reply, err := r.AppendEntries(ctx, args)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if reply.Term != expectedReply.Term {
			t.Errorf("Expected term %d, got %d", expectedReply.Term, reply.Term)
		}
		if reply.Success != expectedReply.Success {
			t.Errorf("Expected Success %v, got %v", expectedReply.Success, reply.Success)
		}
		if electionMgr.resetTimerCalled != 1 {
			t.Errorf("Expected election timer reset once, got %d", electionMgr.resetTimerCalled)
		}
	})

	t.Run("append entries when shutdown", func(t *testing.T) {
		r, stateMgr, _, _, _, _, _, _ := createTestRaftNode(t)
		r.isShutdown.Store(true)
		stateMgr.currentTerm = 3

		args := &types.AppendEntriesArgs{Term: 5, LeaderID: "node2"}
		ctx := context.Background()
		reply, err := r.AppendEntries(ctx, args)

		if err == nil || !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected ErrShuttingDown, got %v", err)
		}
		if reply.Term != stateMgr.currentTerm {
			t.Errorf("Expected term %d, got %d", stateMgr.currentTerm, reply.Term)
		}
		if reply.Success {
			t.Errorf("Expected Success false, got true")
		}
	})

	t.Run("append entries with error", func(t *testing.T) {
		r, _, _, _, _, replicationMgr, _, _ := createTestRaftNode(t)
		expectedErr := errors.New("append error")
		replicationMgr.handleAppendEntriesErr = expectedErr

		args := &types.AppendEntriesArgs{Term: 5, LeaderID: "node2"}
		ctx := context.Background()
		_, err := r.AppendEntries(ctx, args)

		if err == nil || !errors.Is(err, expectedErr) {
			t.Errorf("Expected %v, got %v", expectedErr, err)
		}
	})

	t.Run("install snapshot", func(t *testing.T) {
		r, stateMgr, _, electionMgr, snapshotMgr, _, _, _ := createTestRaftNode(t)

		args := &types.InstallSnapshotArgs{
			Term:              5,
			LeaderID:          "node2",
			LastIncludedIndex: 10,
			LastIncludedTerm:  3,
			Data:              []byte("snapshot data"),
		}

		expectedReply := &types.InstallSnapshotReply{Term: 5}
		snapshotMgr.handleInstallSnapshotResult = expectedReply
		stateMgr.currentTerm = 5

		ctx := context.Background()
		reply, err := r.InstallSnapshot(ctx, args)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if reply.Term != expectedReply.Term {
			t.Errorf("Expected term %d, got %d", expectedReply.Term, reply.Term)
		}
		if electionMgr.resetTimerCalled != 1 {
			t.Errorf("Expected election timer reset once, got %d", electionMgr.resetTimerCalled)
		}
	})

	t.Run("install snapshot when shutdown", func(t *testing.T) {
		r, stateMgr, _, _, _, _, _, _ := createTestRaftNode(t)
		r.isShutdown.Store(true)
		stateMgr.currentTerm = 3

		args := &types.InstallSnapshotArgs{Term: 5, LeaderID: "node2"}
		ctx := context.Background()
		reply, err := r.InstallSnapshot(ctx, args)

		if err == nil || !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Expected ErrShuttingDown, got %v", err)
		}
		if reply.Term != stateMgr.currentTerm {
			t.Errorf("Expected term %d, got %d", stateMgr.currentTerm, reply.Term)
		}
	})

	t.Run("install snapshot with error", func(t *testing.T) {
		r, _, _, _, snapshotMgr, _, _, _ := createTestRaftNode(t)
		expectedErr := errors.New("snapshot error")
		snapshotMgr.handleInstallSnapshotErr = expectedErr

		args := &types.InstallSnapshotArgs{Term: 5, LeaderID: "node2"}
		ctx := context.Background()
		_, err := r.InstallSnapshot(ctx, args)

		if err == nil || !errors.Is(err, expectedErr) {
			t.Errorf("Expected %v, got %v", expectedErr, err)
		}
	})
}

func TestRaft_RaftNode_ApplyCommittedEntries(t *testing.T) {
	t.Run("apply entries successfully", func(t *testing.T) {
		r, stateMgr, logMgr, _, _, _, _, applier := createTestRaftNode(t)

		logMgr.entries = map[types.Index]types.LogEntry{
			1: {Index: 1, Term: 1, Command: []byte("cmd1")},
			2: {Index: 2, Term: 1, Command: []byte("cmd2")},
			3: {Index: 3, Term: 2, Command: []byte("cmd3")},
		}
		logMgr.firstIndex = 1
		logMgr.lastIndex = 3
		logMgr.lastTerm = 2

		stateMgr.commitIndex = 3
		stateMgr.lastApplied = 0

		applier.applied = make(map[types.Index][]byte)
		applier.applyFunc = func(ctx context.Context, index types.Index, command []byte) error {
			applier.mu.Lock()
			defer applier.mu.Unlock()
			applier.applied[index] = command
			applier.applyCalls++
			return nil
		}

		go func() { r.applyNotifyCh <- struct{}{} }()
		go r.runApplyLoop()

		time.Sleep(100 * time.Millisecond)

		close(r.applyLoopStopCh)
		<-r.applyLoopDoneCh

		if applier.applyCalls != 3 {
			t.Errorf("Expected 3 apply calls, got %d", applier.applyCalls)
		}

		if stateMgr.lastApplied != 3 {
			t.Errorf("Expected lastApplied to be 3, got %d", stateMgr.lastApplied)
		}

		for i := 1; i <= 3; i++ {
			want := []byte(fmt.Sprintf("cmd%d", i))
			if !bytes.Equal(applier.applied[types.Index(i)], want) {
				t.Errorf("Command at index %d not applied correctly", i)
			}
		}
	})

	t.Run("handle compacted log during apply", func(t *testing.T) {
		r, stateMgr, logMgr, _, _, _, _, _ := createTestRaftNode(t)

		logMgr.firstIndex = 3
		logMgr.entries = map[types.Index]types.LogEntry{
			3: {Index: 3, Term: 2, Command: []byte("cmd3")},
		}
		stateMgr.commitIndex = 3
		stateMgr.lastApplied = 1

		go func() { r.applyNotifyCh <- struct{}{} }()
		go r.runApplyLoop()

		time.Sleep(100 * time.Millisecond)

		close(r.applyLoopStopCh)
		<-r.applyLoopDoneCh

		if stateMgr.lastApplied != 1 {
			t.Errorf("Expected lastApplied to remain 1, got %d", stateMgr.lastApplied)
		}
	})

	t.Run("stop apply loop", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		go r.runApplyLoop()
		close(r.applyLoopStopCh)

		select {
		case <-r.applyLoopDoneCh:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Apply loop did not stop in time")
		}
	})
}

func TestRaft_RaftNode_HandleLeaderChanges(t *testing.T) {
	t.Run("handle leader change", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		done := make(chan struct{})
		go func() {
			r.handleLeaderChanges()
			close(done)
		}()

		// 1. Change to another node
		r.leaderChangeCh <- "node2"
		time.Sleep(20 * time.Millisecond)

		if r.isLeader.Load() {
			t.Errorf("Expected isLeader to be false when leader is node2")
		}

		// 2. Change to self
		r.leaderChangeCh <- r.id
		time.Sleep(20 * time.Millisecond)

		if !r.isLeader.Load() {
			t.Errorf("Expected isLeader to be true when leader is self")
		}

		// 3. Change to unknown/empty
		r.leaderChangeCh <- ""
		time.Sleep(20 * time.Millisecond)

		if r.isLeader.Load() {
			t.Errorf("Expected isLeader to be false when leader is unknown")
		}

		// Stop the goroutine
		close(r.stopCh)

		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Leader change handler did not stop")
		}
	})

	t.Run("channel close", func(t *testing.T) {
		r, _, _, _, _, _, _, _ := createTestRaftNode(t)

		done := make(chan struct{})
		go func() {
			r.handleLeaderChanges()
			close(done)
		}()

		close(r.leaderChangeCh)

		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Leader change handler did not exit after channel close")
		}

		if r.isLeader.Load() {
			t.Errorf("Expected isLeader to be false after channel close")
		}
	})
}
