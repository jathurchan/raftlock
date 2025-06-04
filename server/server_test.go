package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/lock"
	"github.com/jathurchan/raftlock/logger"
	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func createTestServer(t *testing.T) *raftLockServer {
	config := RaftLockServerConfig{
		NodeID:        "test-node",
		ListenAddress: "localhost:0",
		Peers: map[types.NodeID]raft.PeerConfig{
			"test-node": {ID: "test-node", Address: "localhost:8080"},
		},
		DataDir:              "/tmp/test",
		RequestTimeout:       time.Second,
		ShutdownTimeout:      time.Second,
		MaxRequestSize:       1024,
		MaxResponseSize:      1024,
		MaxConcurrentReqs:    10,
		Logger:               logger.NewNoOpLogger(),
		Metrics:              NewNoOpServerMetrics(),
		Clock:                newMockClock(),
		Serializer:           &lock.JSONSerializer{},
		HealthCheckInterval:  time.Second,
		HealthCheckTimeout:   time.Second,
		EnableLeaderRedirect: true,
		RedirectTimeout:      time.Second,
	}

	server, err := NewRaftLockServer(config)
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}

	s := server.(*raftLockServer)
	s.raftNode = &mockRaft{
		isLeader:       true,
		leaderID:       "test-node",
		term:           1,
		commitIndex:    1,
		lastApplied:    1,
		applyCh:        make(chan types.ApplyMsg, 1),
		leaderChangeCh: make(chan types.NodeID, 1),
		status: types.RaftStatus{
			ID:          "test-node",
			Role:        types.RoleLeader,
			Term:        1,
			LeaderID:    "test-node",
			CommitIndex: 1,
			LastApplied: 1,
		},
	}
	s.lockManager = &mockLockManager{
		locks: make(map[types.LockID]*types.LockInfo),
	}
	s.storage = &mockStorage{}
	s.networkMgr = &mockNetworkManager{}
	s.proposalTracker = &mockProposalTracker{}
	s.state.Store(ServerStateRunning)
	s.isLeader.Store(true)
	s.currentLeaderID.Store(types.NodeID("test-node"))

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to create mock listener: %v", err)
	}
	s.listener = listener

	return s
}

func createCustomTestServer(t *testing.T, modifyFn func(s *raftLockServer)) *raftLockServer {
	s := createTestServer(t)
	if modifyFn != nil {
		modifyFn(s)
	}
	return s
}

func TestServer_NewRaftLockServer(t *testing.T) {
	config := RaftLockServerConfig{
		NodeID:        "test-node",
		ListenAddress: "localhost:8080",
		Peers: map[types.NodeID]raft.PeerConfig{
			"test-node": {ID: "test-node", Address: "localhost:8080"},
		},
		DataDir:              "/tmp/test",
		RequestTimeout:       time.Second,
		ShutdownTimeout:      time.Second,
		MaxRequestSize:       1024,
		MaxResponseSize:      1024,
		MaxConcurrentReqs:    10,
		Logger:               logger.NewNoOpLogger(),
		Metrics:              NewNoOpServerMetrics(),
		Clock:                newMockClock(),
		Serializer:           &lock.JSONSerializer{},
		HealthCheckInterval:  time.Second,
		HealthCheckTimeout:   time.Second,
		EnableLeaderRedirect: true,
		RedirectTimeout:      time.Second,
	}

	server, err := NewRaftLockServer(config)
	if err != nil {
		t.Fatalf("NewRaftLockServer failed: %v", err)
	}

	if server == nil {
		t.Fatal("NewRaftLockServer returned nil")
	}

	s := server.(*raftLockServer)
	if s.config.NodeID != config.NodeID {
		t.Errorf("Expected NodeID %s, got %s", config.NodeID, s.config.NodeID)
	}

	if s.validator == nil {
		t.Error("Expected validator to be initialized")
	}

	if s.connManager == nil {
		t.Error("Expected connection manager to be initialized")
	}

	if s.proposalTracker == nil {
		t.Error("Expected proposal tracker to be initialized")
	}
}

func TestServer_NewRaftLockServer_InvalidConfig(t *testing.T) {
	config := RaftLockServerConfig{
		// Missing required fields
	}

	_, err := NewRaftLockServer(config)
	if err == nil {
		t.Fatal("Expected error for invalid config")
	}
}

func TestServer_NewRaftLockServer_ValidationFailure(t *testing.T) {
	config := DefaultRaftLockServerConfig()
	config.NodeID = ""

	_, err := NewRaftLockServer(config)
	if err == nil {
		t.Fatal("Expected error for invalid config but got nil")
	}
	if !strings.Contains(err.Error(), "invalid server configuration") {
		t.Errorf("Expected error message to contain 'invalid server configuration', got: %s", err.Error())
	}
}

func TestServer_IsLeader(t *testing.T) {
	server := createTestServer(t)

	// Test when leader
	server.isLeader.Store(true)
	if !server.IsLeader() {
		t.Error("Expected IsLeader to return true")
	}

	// Test when not leader
	server.isLeader.Store(false)
	if server.IsLeader() {
		t.Error("Expected IsLeader to return false")
	}
}

func TestServer_GetLeaderAddress(t *testing.T) {
	server := createTestServer(t)

	// Test with known leader
	server.currentLeaderID.Store(types.NodeID("test-node"))
	addr := server.GetLeaderAddress()
	if addr != "localhost:8080" {
		t.Errorf("Expected address localhost:8080, got %s", addr)
	}

	// Test with unknown leader
	server.currentLeaderID.Store(types.NodeID("unknown-node"))
	addr = server.GetLeaderAddress()
	if addr != "" {
		t.Errorf("Expected empty address, got %s", addr)
	}

	// Test with empty leader
	server.currentLeaderID.Store(types.NodeID(""))
	addr = server.GetLeaderAddress()
	if addr != "" {
		t.Errorf("Expected empty address, got %s", addr)
	}
}

func TestServer_GetNodeID(t *testing.T) {
	server := createTestServer(t)

	nodeID := server.GetNodeID()
	if nodeID != "test-node" {
		t.Errorf("Expected node ID test-node, got %s", nodeID)
	}
}

func TestServer_ExtractMethodName(t *testing.T) {
	tests := []struct {
		fullMethod string
		expected   string
	}{
		{"/raftlock.RaftLock/Acquire", "Acquire"},
		{"/package.Service/Method", "Method"},
		{"Method", "Method"},
		{"", ""},
		{"/", ""},
		{"/service/", ""},
	}

	for _, test := range tests {
		result := extractMethodName(test.fullMethod)
		if result != test.expected {
			t.Errorf("extractMethodName(%s) = %s, expected %s", test.fullMethod, result, test.expected)
		}
	}
}

func TestServer_CheckServerState(t *testing.T) {
	server := createTestServer(t)

	// Test running state
	server.state.Store(ServerStateRunning)
	err := server.checkServerState()
	if err != nil {
		t.Errorf("Expected no error for running state, got %v", err)
	}

	// Test starting state
	server.state.Store(ServerStateStarting)
	err = server.checkServerState()
	if err != ErrServerNotStarted {
		t.Errorf("Expected ErrServerNotStarted, got %v", err)
	}

	// Test stopping state
	server.state.Store(ServerStateStopping)
	err = server.checkServerState()
	if err != ErrServerStopped {
		t.Errorf("Expected ErrServerStopped, got %v", err)
	}

	// Test stopped state
	server.state.Store(ServerStateStopped)
	err = server.checkServerState()
	if err != ErrServerStopped {
		t.Errorf("Expected ErrServerStopped, got %v", err)
	}
}

func TestServer_RequireLeader(t *testing.T) {
	server := createTestServer(t)

	// Test when is leader
	server.isLeader.Store(true)
	err := server.requireLeader("TestMethod")
	if err != nil {
		t.Errorf("Expected no error when is leader, got %v", err)
	}

	// Test when not leader with known leader address
	server.isLeader.Store(false)
	server.currentLeaderID.Store(types.NodeID("test-node"))
	err = server.requireLeader("TestMethod")
	var redirectErr *LeaderRedirectError
	if !errors.As(err, &redirectErr) {
		t.Errorf("Expected LeaderRedirectError, got %v", err)
	}
	if redirectErr.LeaderAddress != "localhost:8080" {
		t.Errorf("Expected leader address localhost:8080, got %s", redirectErr.LeaderAddress)
	}

	// Test when not leader with unknown leader address
	server.currentLeaderID.Store(types.NodeID(""))
	err = server.requireLeader("TestMethod")
	if err != ErrNoLeader {
		t.Errorf("Expected ErrNoLeader, got %v", err)
	}
}

func TestServer_ValidateAcquireRequest(t *testing.T) {
	server := createTestServer(t)

	// Valid request
	req := &pb.AcquireRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Ttl:      durationpb.New(time.Minute),
	}
	err := server.validateAcquireRequest(req)
	if err != nil {
		t.Errorf("Expected no error for valid request, got %v", err)
	}

	// Invalid request - empty lock ID
	req.LockId = ""
	err = server.validateAcquireRequest(req)
	if err == nil {
		t.Error("Expected error for empty lock ID")
	}
}

func TestServer_ResolveTTL(t *testing.T) {
	server := createTestServer(t)

	// Valid TTL
	req := &pb.AcquireRequest{
		Ttl: durationpb.New(time.Minute),
	}
	ttl, err := server.resolveTTL(req)
	if err != nil {
		t.Errorf("Expected no error for valid TTL, got %v", err)
	}
	if ttl != time.Minute {
		t.Errorf("Expected TTL %v, got %v", time.Minute, ttl)
	}

	// No TTL (should use default)
	req.Ttl = nil
	ttl, err = server.resolveTTL(req)
	if err != nil {
		t.Errorf("Expected no error for nil TTL, got %v", err)
	}
	if ttl != lock.DefaultLockTTL {
		t.Errorf("Expected default TTL %v, got %v", lock.DefaultLockTTL, ttl)
	}

	// Invalid TTL (too short)
	req.Ttl = durationpb.New(time.Millisecond)
	_, err = server.resolveTTL(req)
	if err == nil {
		t.Error("Expected error for TTL too short")
	}

	// Invalid TTL (too long)
	req.Ttl = durationpb.New(time.Hour * 25)
	_, err = server.resolveTTL(req)
	if err == nil {
		t.Error("Expected error for TTL too long")
	}
}

func TestServer_BuildAcquireCommand(t *testing.T) {
	server := createTestServer(t)

	req := &pb.AcquireRequest{
		LockId:    "test-lock",
		ClientId:  "test-client",
		Priority:  5,
		Wait:      true,
		RequestId: "req-123",
	}
	ttl := time.Minute
	waitTimeout := time.Second * 30

	cmd := server.buildAcquireCommand(req, ttl, waitTimeout)

	if cmd.Op != types.OperationAcquire {
		t.Errorf("Expected operation %s, got %s", types.OperationAcquire, cmd.Op)
	}
	if cmd.LockID != types.LockID(req.LockId) {
		t.Errorf("Expected lock ID %s, got %s", req.LockId, cmd.LockID)
	}
	if cmd.ClientID != types.ClientID(req.ClientId) {
		t.Errorf("Expected client ID %s, got %s", req.ClientId, cmd.ClientID)
	}
	if cmd.TTL != ttl.Milliseconds() {
		t.Errorf("Expected TTL %d, got %d", ttl.Milliseconds(), cmd.TTL)
	}
	if cmd.Priority != int(req.Priority) {
		t.Errorf("Expected priority %d, got %d", req.Priority, cmd.Priority)
	}
	if cmd.Wait != req.Wait {
		t.Errorf("Expected wait %t, got %t", req.Wait, cmd.Wait)
	}
	if cmd.WaitTimeout != waitTimeout.Milliseconds() {
		t.Errorf("Expected wait timeout %d, got %d", waitTimeout.Milliseconds(), cmd.WaitTimeout)
	}
	if cmd.RequestID != req.RequestId {
		t.Errorf("Expected request ID %s, got %s", req.RequestId, cmd.RequestID)
	}
}

func TestServer_ParseAcquireProposal(t *testing.T) {
	server := createTestServer(t)

	lockInfo := &types.LockInfo{
		LockID:  "test-lock",
		OwnerID: "test-client",
		Version: 1,
	}
	result, err := server.parseAcquireProposal(lockInfo)
	if err != nil {
		t.Errorf("Expected no error for valid LockInfo, got %v", err)
	}
	if result != lockInfo {
		t.Error("Expected same LockInfo to be returned")
	}

	result, err = server.parseAcquireProposal(nil)
	if err != nil {
		t.Errorf("Expected no error for nil data, got %v", err)
	}
	if result != nil {
		t.Error("Expected nil result for nil data")
	}

	_, err = server.parseAcquireProposal("invalid")
	if err == nil {
		t.Error("Expected error for invalid data type")
	}
}

func TestServer_LockInfoToProtoLock(t *testing.T) {
	server := createTestServer(t)

	result := server.lockInfoToProtoLock(nil)
	if result != nil {
		t.Error("Expected nil result for nil input")
	}

	now := time.Now()
	lockInfo := &types.LockInfo{
		LockID:     "test-lock",
		OwnerID:    "test-client",
		Version:    1,
		AcquiredAt: now,
		ExpiresAt:  now.Add(time.Minute),
		Metadata:   map[string]string{"key": "value"},
	}

	result = server.lockInfoToProtoLock(lockInfo)
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if result.LockId != string(lockInfo.LockID) {
		t.Errorf("Expected lock ID %s, got %s", lockInfo.LockID, result.LockId)
	}
	if result.OwnerId != string(lockInfo.OwnerID) {
		t.Errorf("Expected owner ID %s, got %s", lockInfo.OwnerID, result.OwnerId)
	}
	if result.Version != int64(lockInfo.Version) {
		t.Errorf("Expected version %d, got %d", lockInfo.Version, result.Version)
	}
	if result.Metadata["key"] != "value" {
		t.Error("Expected metadata to be copied")
	}
}

func TestServer_LockInfoToProto(t *testing.T) {
	server := createTestServer(t)

	result := server.lockInfoToProto(nil)
	if result != nil {
		t.Error("Expected nil result for nil input")
	}

	now := time.Now()
	lockInfo := &types.LockInfo{
		LockID:       "test-lock",
		OwnerID:      "test-client",
		Version:      1,
		AcquiredAt:   now,
		ExpiresAt:    now.Add(time.Minute),
		WaiterCount:  2,
		Metadata:     map[string]string{"key": "value"},
		LastModified: now,
		WaitersInfo: []types.WaiterInfo{
			{
				ClientID:   "waiter-1",
				EnqueuedAt: now,
				TimeoutAt:  now.Add(time.Minute),
				Priority:   1,
				Position:   0,
			},
		},
	}

	result = server.lockInfoToProto(lockInfo)
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if result.LockId != string(lockInfo.LockID) {
		t.Errorf("Expected lock ID %s, got %s", lockInfo.LockID, result.LockId)
	}
	if result.WaiterCount != int32(lockInfo.WaiterCount) {
		t.Errorf("Expected waiter count %d, got %d", lockInfo.WaiterCount, result.WaiterCount)
	}
	if len(result.WaitersInfo) != 1 {
		t.Errorf("Expected 1 waiter info, got %d", len(result.WaitersInfo))
	}
}

func TestServer_CalculateBackoffDuration(t *testing.T) {
	server := createTestServer(t)
	mockClock := server.clock.(*mockClock)
	now := mockClock.Now()

	lockInfo := &types.LockInfo{
		LockID:      "test-lock",
		OwnerID:     "test-client",
		ExpiresAt:   now.Add(40 * time.Second), // 40s remaining
		WaiterCount: 0,
	}
	duration := server.calculateBackoffDuration(lockInfo)
	if duration <= 0 {
		t.Error("Expected positive duration for active lock")
	}

	lockInfo.WaiterCount = 2
	durationWithWaiters := server.calculateBackoffDuration(lockInfo)

	if durationWithWaiters <= duration {
		t.Errorf("Expected longer duration with more waiters: %v vs %v (both may be hitting MaxBackoffAdvice=%v)",
			durationWithWaiters, duration, MaxBackoffAdvice)
	}
}

func TestServer_LockInfoToBackoffAdvice(t *testing.T) {
	server := createTestServer(t)

	// Test with nil info
	advice := server.lockInfoToBackoffAdvice(nil)
	if advice == nil {
		t.Fatal("Expected non-nil advice")
	}
	if advice.InitialBackoff.AsDuration() != DefaultBackoffInitial {
		t.Errorf("Expected initial backoff %v, got %v", DefaultBackoffInitial, advice.InitialBackoff.AsDuration())
	}

	// Test with available lock
	now := time.Now()
	lockInfo := &types.LockInfo{
		LockID:    "test-lock",
		OwnerID:   "",
		ExpiresAt: now.Add(time.Minute),
	}
	advice = server.lockInfoToBackoffAdvice(lockInfo)
	if advice.InitialBackoff.AsDuration() != DefaultBackoffInitial {
		t.Error("Expected default backoff for available lock")
	}

	// Test with held lock
	lockInfo.OwnerID = "test-client"
	advice = server.lockInfoToBackoffAdvice(lockInfo)
	if advice.InitialBackoff.AsDuration() <= 0 {
		t.Error("Expected positive backoff for held lock")
	}
}

func TestServer_PerformHealthCheck(t *testing.T) {
	server := createTestServer(t)

	// Test healthy state
	server.state.Store(ServerStateRunning)
	healthy := server.performHealthCheck()
	if !healthy {
		t.Error("Expected healthy server to return true")
	}

	// Test unhealthy state (not running)
	server.state.Store(ServerStateStarting)
	healthy = server.performHealthCheck()
	if healthy {
		t.Error("Expected non-running server to return false")
	}

	// Test with nil raft node
	server.state.Store(ServerStateRunning)
	server.raftNode = nil
	healthy = server.performHealthCheck()
	if healthy {
		t.Error("Expected server with nil raft node to return false")
	}
}

func TestServer_HandleLeaderChange(t *testing.T) {
	server := createTestServer(t)

	// Test becoming leader
	server.isLeader.Store(false)
	server.handleLeaderChange(types.NodeID("test-node"))
	if !server.isLeader.Load() {
		t.Error("Expected isLeader to be true")
	}
	if server.currentLeaderID.Load().(types.NodeID) != "test-node" {
		t.Error("Expected currentLeaderID to be updated")
	}

	// Test losing leadership
	server.handleLeaderChange(types.NodeID("other-node"))
	if server.isLeader.Load() {
		t.Error("Expected isLeader to be false")
	}
	if server.currentLeaderID.Load().(types.NodeID) != "other-node" {
		t.Error("Expected currentLeaderID to be updated")
	}
}

func TestServer_Health(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	req := &pb.HealthRequest{
		ServiceName: "test-service",
	}

	resp, err := server.Health(ctx, req)
	if err != nil {
		t.Errorf("Health request failed: %v", err)
	}

	if resp.Status != pb.HealthStatus_SERVING {
		t.Errorf("Expected SERVING status, got %v", resp.Status)
	}

	if resp.HealthInfo == nil {
		t.Error("Expected health info to be populated")
	}
}

func TestServer_GetStatus(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	req := &pb.GetStatusRequest{
		IncludeReplicationDetails: true,
	}

	resp, err := server.GetStatus(ctx, req)
	if err != nil {
		t.Errorf("GetStatus request failed: %v", err)
	}

	if resp.RaftStatus == nil {
		t.Error("Expected raft status to be populated")
	}

	if resp.RaftStatus.NodeId != "test-node" {
		t.Errorf("Expected node ID test-node, got %s", resp.RaftStatus.NodeId)
	}
}

func TestServer_Acquire_Success(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	// Mock successful acquisition
	lockInfo := &types.LockInfo{
		LockID:     "test-lock",
		OwnerID:    "test-client",
		Version:    1,
		AcquiredAt: time.Now(),
		ExpiresAt:  time.Now().Add(time.Minute),
	}

	mockLM := server.lockManager.(*mockLockManager)
	mockLM.applyFunc = func(ctx context.Context, index types.Index, cmdData []byte) (any, error) {
		return lockInfo, nil
	}

	mockRaft := server.raftNode.(*mockRaft)
	mockRaft.proposeFunc = func(ctx context.Context, cmd []byte) (types.Index, types.Term, bool, error) {
		return 1, 1, true, nil
	}

	// Mock proposal tracker to immediately return result
	mockTracker := server.proposalTracker.(*mockProposalTracker)
	mockTracker.trackFunc = func(proposal *types.PendingProposal) error {
		go func() {
			proposal.ResultCh <- types.ProposalResult{
				Success: true,
				Data:    lockInfo,
			}
		}()
		return nil
	}

	req := &pb.AcquireRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Ttl:      durationpb.New(time.Minute),
	}

	resp, err := server.Acquire(ctx, req)
	if err != nil {
		t.Errorf("Acquire request failed: %v", err)
	}

	if !resp.Acquired {
		t.Error("Expected lock to be acquired")
	}

	if resp.Lock == nil {
		t.Error("Expected lock info to be populated")
	}

	if resp.Lock.LockId != "test-lock" {
		t.Errorf("Expected lock ID test-lock, got %s", resp.Lock.LockId)
	}
}

func TestServer_Acquire_NotLeader(t *testing.T) {
	server := createTestServer(t)
	server.isLeader.Store(false)
	ctx := context.Background()

	req := &pb.AcquireRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
	}

	_, err := server.Acquire(ctx, req)
	if err == nil {
		t.Error("Expected error when not leader")
	}

	// Check that it's a gRPC status error
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("Expected FailedPrecondition status, got %v", status.Code(err))
	}
}

func TestServer_Acquire_ValidationError(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	req := &pb.AcquireRequest{
		LockId:   "", // Invalid empty lock ID
		ClientId: "test-client",
	}

	_, err := server.Acquire(ctx, req)
	if err == nil {
		t.Error("Expected validation error")
	}

	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument status, got %v", status.Code(err))
	}
}

func TestServer_ResolveAcquireWaitTimeout(t *testing.T) {
	server := createTestServer(t)
	s := server

	tests := []struct {
		name            string
		req             *pb.AcquireRequest
		expectedTimeout time.Duration
		expectError     bool
		errorMsgContent string // Check for a substring in the error message
	}{
		{
			name:            "Wait is false, WaitTimeout ignored",
			req:             &pb.AcquireRequest{Wait: false, WaitTimeout: durationpb.New(10 * time.Second)},
			expectedTimeout: 0,
			expectError:     false,
		},
		{
			name:            "Wait is true, WaitTimeout is nil, uses default",
			req:             &pb.AcquireRequest{Wait: true, WaitTimeout: nil},
			expectedTimeout: lock.DefaultWaitQueueTimeout,
			expectError:     false,
		},
		{
			name:            "Wait is true, WaitTimeout is invalid protobuf duration (e.g., negative seconds, valid nanos of different sign - though durationpb.New handles simple cases)",
			req:             &pb.AcquireRequest{Wait: true, WaitTimeout: &durationpb.Duration{Seconds: 1, Nanos: -1000}},
			expectedTimeout: lock.DefaultWaitQueueTimeout, // Falls back to default because !req.WaitTimeout.IsValid()
			expectError:     false,
		},
		{
			name:            "Wait is true, WaitTimeout is negative duration (semantically invalid)",
			req:             &pb.AcquireRequest{Wait: true, WaitTimeout: durationpb.New(-5 * time.Second)},
			expectedTimeout: 0, // Duration is 0 when an error is returned for out-of-bounds
			expectError:     true,
			errorMsgContent: fmt.Sprintf(ErrMsgInvalidTimeout, lock.MinWaitQueueTimeout, lock.MaxWaitQueueTimeout),
		},
		{
			name:            "Wait is true, WaitTimeout too short",
			req:             &pb.AcquireRequest{Wait: true, WaitTimeout: durationpb.New(lock.MinWaitQueueTimeout - time.Millisecond)},
			expectedTimeout: 0,
			expectError:     true,
			errorMsgContent: fmt.Sprintf(ErrMsgInvalidTimeout, lock.MinWaitQueueTimeout, lock.MaxWaitQueueTimeout),
		},
		{
			name:            "Wait is true, WaitTimeout too long",
			req:             &pb.AcquireRequest{Wait: true, WaitTimeout: durationpb.New(lock.MaxWaitQueueTimeout + time.Second)},
			expectedTimeout: 0,
			expectError:     true,
			errorMsgContent: fmt.Sprintf(ErrMsgInvalidTimeout, lock.MinWaitQueueTimeout, lock.MaxWaitQueueTimeout),
		},
		{
			name:            "Wait is true, WaitTimeout valid (min boundary)",
			req:             &pb.AcquireRequest{Wait: true, WaitTimeout: durationpb.New(lock.MinWaitQueueTimeout)},
			expectedTimeout: lock.MinWaitQueueTimeout,
			expectError:     false,
		},
		{
			name:            "Wait is true, WaitTimeout valid (max boundary)",
			req:             &pb.AcquireRequest{Wait: true, WaitTimeout: durationpb.New(lock.MaxWaitQueueTimeout)},
			expectedTimeout: lock.MaxWaitQueueTimeout,
			expectError:     false,
		},
		{
			name:            "Wait is true, WaitTimeout valid (in between)",
			req:             &pb.AcquireRequest{Wait: true, WaitTimeout: durationpb.New(30 * time.Second)},
			expectedTimeout: 30 * time.Second,
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			timeout, err := s.resolveAcquireWaitTimeout(tt.req)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected an error, but got nil")
				} else {
					st, ok := status.FromError(err)
					if !ok {
						t.Errorf("Expected gRPC status error, got %T: %v", err, err)
					} else {
						if !strings.Contains(st.Message(), tt.errorMsgContent) {
							t.Errorf("Expected error message to contain %q, got %q", tt.errorMsgContent, st.Message())
						}
						if st.Code() != codes.InvalidArgument {
							t.Errorf("Expected error code InvalidArgument, got %s", st.Code())
						}
					}
				}
				if timeout != 0 {
					t.Errorf("Expected timeout to be 0 when an error is returned, got %v", timeout)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, but got: %v", err)
				}
				if timeout != tt.expectedTimeout {
					t.Errorf("Expected timeout %v, got %v", tt.expectedTimeout, timeout)
				}
			}
		})
	}
}

func TestServer_HandleAcquireError(t *testing.T) {
	server := createTestServer(t)
	mockLM := server.lockManager.(*mockLockManager)

	tests := []struct {
		name              string
		inputErr          error
		req               *pb.AcquireRequest
		mockGetInfo       *types.LockInfo
		mockGetInfoErr    error
		expectResponse    bool // True if we expect an AcquireResponse, false if we expect a direct gRPC error
		expectedAcquired  bool
		expectedErrorCode pb.ErrorCode // For AcquireResponse.Error
		expectedGRPCCode  codes.Code   // For direct gRPC error
		expectBackoff     bool
	}{
		{
			name:              "LockHeld, not waiting, lock info found",
			inputErr:          lock.ErrLockHeld,
			req:               &pb.AcquireRequest{LockId: "l1", ClientId: "c1", Wait: false},
			mockGetInfo:       &types.LockInfo{LockID: "l1", OwnerID: "c2", ExpiresAt: server.clock.Now().Add(time.Minute)},
			expectResponse:    true,
			expectedAcquired:  false,
			expectedErrorCode: pb.ErrorCode_LOCK_HELD,
			expectBackoff:     true,
		},
		{
			name:              "LockHeld, not waiting, lock info not found",
			inputErr:          lock.ErrLockHeld,
			req:               &pb.AcquireRequest{LockId: "l1", ClientId: "c1", Wait: false},
			mockGetInfoErr:    lock.ErrLockNotFound,
			expectResponse:    true,
			expectedAcquired:  false,
			expectedErrorCode: pb.ErrorCode_LOCK_HELD,
			expectBackoff:     true, // Default backoff advice
		},
		{
			name:             "LockHeld, is waiting",
			inputErr:         lock.ErrLockHeld,
			req:              &pb.AcquireRequest{LockId: "l1", ClientId: "c1", Wait: true},
			expectResponse:   false,                   // Should return direct gRPC error
			expectedGRPCCode: codes.ResourceExhausted, // Assuming ErrLockHeld maps to this
		},
		{
			name:             "NotLeader error",
			inputErr:         ErrNotLeader,
			req:              &pb.AcquireRequest{LockId: "l1", ClientId: "c1"},
			expectResponse:   false,
			expectedGRPCCode: codes.FailedPrecondition,
		},
		{
			name:             "Context deadline exceeded",
			inputErr:         context.DeadlineExceeded,
			req:              &pb.AcquireRequest{LockId: "l1", ClientId: "c1"},
			expectResponse:   false,
			expectedGRPCCode: codes.DeadlineExceeded,
		},
		{
			name:             "Generic internal error",
			inputErr:         errors.New("some internal failure"),
			req:              &pb.AcquireRequest{LockId: "l1", ClientId: "c1"},
			expectResponse:   false,
			expectedGRPCCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLM.getInfoFunc = func(ctx context.Context, lockID types.LockID) (*types.LockInfo, error) {
				return tt.mockGetInfo, tt.mockGetInfoErr
			}

			resp, err := server.handleAcquireError(tt.inputErr, tt.req)

			if tt.expectResponse {
				if err != nil {
					t.Fatalf("Expected no direct error, but got: %v", err)
				}
				if resp == nil {
					t.Fatal("Expected an AcquireResponse, but got nil")
				}
				if resp.Acquired != tt.expectedAcquired {
					t.Errorf("Expected Acquired %v, got %v", tt.expectedAcquired, resp.Acquired)
				}
				if resp.Error == nil {
					t.Error("Expected Error in response, but got nil")
				} else if resp.Error.Code != tt.expectedErrorCode {
					t.Errorf("Expected ErrorCode %s, got %s", tt.expectedErrorCode, resp.Error.Code)
				}
				if tt.expectBackoff {
					if resp.BackoffAdvice == nil {
						t.Error("Expected BackoffAdvice, but got nil")
					}
				} else {
					if resp.BackoffAdvice != nil {
						t.Error("Expected no BackoffAdvice, but got one")
					}
				}
			} else {
				if resp != nil {
					t.Fatalf("Expected no AcquireResponse, but got one: %+v", resp)
				}
				if err == nil {
					t.Fatal("Expected a direct gRPC error, but got nil")
				}
				s, ok := status.FromError(err)
				if !ok {
					t.Fatalf("Expected gRPC status error, got %T: %v", err, err)
				}
				if s.Code() != tt.expectedGRPCCode {
					t.Errorf("Expected gRPC code %s, got %s", tt.expectedGRPCCode, s.Code())
				}
			}
		})
	}
}

func TestServer_Release_Success(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	// Mock successful release
	mockLM := server.lockManager.(*mockLockManager)
	mockLM.applyFunc = func(ctx context.Context, index types.Index, cmdData []byte) (any, error) {
		return true, nil // waiter promoted
	}

	mockRaft := server.raftNode.(*mockRaft)
	mockRaft.proposeFunc = func(ctx context.Context, cmd []byte) (types.Index, types.Term, bool, error) {
		return 1, 1, true, nil
	}

	mockTracker := server.proposalTracker.(*mockProposalTracker)
	mockTracker.trackFunc = func(proposal *types.PendingProposal) error {
		go func() {
			proposal.ResultCh <- types.ProposalResult{
				Success: true,
				Data:    true,
			}
		}()
		return nil
	}

	req := &pb.ReleaseRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Version:  1,
	}

	resp, err := server.Release(ctx, req)
	if err != nil {
		t.Errorf("Release request failed: %v", err)
	}

	if !resp.Released {
		t.Error("Expected lock to be released")
	}

	if !resp.WaiterPromoted {
		t.Error("Expected waiter to be promoted")
	}
}

func TestGetLockInfo_Success(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	lockInfo := &types.LockInfo{
		LockID:      "test-lock",
		OwnerID:     "test-client",
		Version:     1,
		WaiterCount: 2,
	}

	mockLM := server.lockManager.(*mockLockManager)
	mockLM.getInfoFunc = func(ctx context.Context, lockID types.LockID) (*types.LockInfo, error) {
		return lockInfo, nil
	}

	mockRaft := server.raftNode.(*mockRaft)
	mockRaft.readIndexFunc = func(ctx context.Context) (types.Index, error) {
		return 1, nil
	}

	server.lastCommitIndex.Store(1) // Set commit index to match read index

	req := &pb.GetLockInfoRequest{
		LockId:         "test-lock",
		IncludeWaiters: true,
	}

	resp, err := server.GetLockInfo(ctx, req)
	if err != nil {
		t.Errorf("GetLockInfo request failed: %v", err)
	}

	if resp.LockInfo == nil {
		t.Fatal("Expected lock info to be populated")
	}

	if resp.LockInfo.LockId != "test-lock" {
		t.Errorf("Expected lock ID test-lock, got %s", resp.LockInfo.LockId)
	}

	if resp.LockInfo.WaiterCount != 2 {
		t.Errorf("Expected waiter count 2, got %d", resp.LockInfo.WaiterCount)
	}
}

func TestGetLockInfo_NotFound(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	mockLM := server.lockManager.(*mockLockManager)
	mockLM.getInfoFunc = func(ctx context.Context, lockID types.LockID) (*types.LockInfo, error) {
		return nil, lock.ErrLockNotFound
	}

	mockRaft := server.raftNode.(*mockRaft)
	mockRaft.readIndexFunc = func(ctx context.Context) (types.Index, error) {
		return 1, nil
	}

	server.lastCommitIndex.Store(1)

	req := &pb.GetLockInfoRequest{
		LockId: "nonexistent-lock",
	}

	resp, err := server.GetLockInfo(ctx, req)
	if err != nil {
		t.Errorf("GetLockInfo request failed: %v", err)
	}

	if resp.LockInfo != nil {
		t.Error("Expected lock info to be nil for not found")
	}

	if resp.Error == nil {
		t.Error("Expected error to be populated")
	}

	if resp.Error.Code != pb.ErrorCode_LOCK_NOT_FOUND {
		t.Errorf("Expected LOCK_NOT_FOUND error code, got %v", resp.Error.Code)
	}
}

func TestErrorToGRPCStatus(t *testing.T) {
	server := createTestServer(t)

	err := server.errorToGRPCStatus(nil)
	if err != nil {
		t.Errorf("Expected nil for nil input, got %v", err)
	}

	validationErr := NewValidationError("test_field", "test_value", "test message")
	err = server.errorToGRPCStatus(validationErr)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument status, got %v", status.Code(err))
	}

	redirectErr := NewLeaderRedirectError("localhost:8080", "leader-node")
	err = server.errorToGRPCStatus(redirectErr)
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("Expected FailedPrecondition status, got %v", status.Code(err))
	}

	serverErr := NewServerError("test_operation", errors.New("cause"), "test message")
	err = server.errorToGRPCStatus(serverErr)
	if status.Code(err) != codes.Internal {
		t.Errorf("Expected Internal status, got %v", status.Code(err))
	}
}

func TestServer_Start_InvalidInitialStates(t *testing.T) {
	tests := []struct {
		name          string
		initialState  ServerOperationalState
		expectedError error  // Use specific error for ErrServerAlreadyStarted
		expectedMsg   string // Use for fmt.Errorf cases
	}{
		{
			name:          "Start when already running",
			initialState:  ServerStateRunning,
			expectedError: ErrServerAlreadyStarted,
		},
		{
			name:         "Start when stopping",
			initialState: ServerStateStopping,
			expectedMsg:  fmt.Sprintf("cannot start server in state %s", ServerStateStopping),
		},
		{
			name:         "Start when stopped",
			initialState: ServerStateStopped,
			expectedMsg:  fmt.Sprintf("cannot start server in state %s", ServerStateStopped),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createTestServer(t)

			s.state.Store(tt.initialState)

			err := s.Start(context.Background())

			if tt.expectedError != nil {
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("Expected error %v, got %v", tt.expectedError, err)
				}
			} else if tt.expectedMsg != "" {
				if err == nil {
					t.Errorf("Expected error containing message %q, but got nil error", tt.expectedMsg)
				} else if err.Error() != tt.expectedMsg {
					t.Errorf("Expected error message %q, got %q", tt.expectedMsg, err.Error())
				}
			} else {
				t.Errorf("Test case for invalid state %s did not specify an expected error or message", tt.initialState)
			}
		})
	}
}

func TestServer_Start_Successful(t *testing.T) {
	cfg := RaftLockServerConfig{
		NodeID:              "test-node-success",
		ListenAddress:       "localhost:0", // Ephemeral port
		Peers:               map[types.NodeID]raft.PeerConfig{"test-node-success": {ID: "test-node-success", Address: "localhost:0"}},
		DataDir:             t.TempDir(),
		RequestTimeout:      time.Second,
		ShutdownTimeout:     time.Second,
		MaxRequestSize:      1024,
		MaxResponseSize:     1024,
		MaxConcurrentReqs:   10,
		Logger:              logger.NewNoOpLogger(),
		Metrics:             NewNoOpServerMetrics(),
		Clock:               newMockClock(),
		Serializer:          &lock.JSONSerializer{},
		HealthCheckInterval: DefaultHealthCheckInterval,
		HealthCheckTimeout:  DefaultHealthCheckTimeout,
	}

	server, err := NewRaftLockServer(cfg)
	if err != nil {
		t.Fatalf("NewRaftLockServer failed: %v", err)
	}
	s := server.(*raftLockServer)

	s.storage = &mockStorage{}
	s.lockManager = &mockLockManager{}
	s.raftNode = &mockRaft{}
	s.networkMgr = &mockNetworkManager{}

	err = s.Start(context.Background())
	if err != nil {
		t.Fatalf("Start() failed unexpectedly: %v", err)
	}

	if s.state.Load() != ServerStateRunning {
		t.Errorf("Expected server state to be ServerStateRunning, got %v", s.state.Load())
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelStop()
	if err := s.Stop(stopCtx); err != nil {
		t.Errorf("Stop() after successful Start() failed: %v", err)
	}
}

func TestServer_Stop_Success(t *testing.T) {
	server := createTestServer(t)
	server.state.Store(ServerStateRunning)

	server.stopCh = make(chan struct{})

	ctx := context.Background()
	err := server.Stop(ctx)
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}

	if server.state.Load().(ServerOperationalState) != ServerStateStopped {
		t.Error("Expected server state to be stopped")
	}

	if !server.shutdownStarted.Load() {
		t.Error("Expected shutdown flag to be set")
	}
}

func TestServer_Stop_AlreadyStopped(t *testing.T) {
	server := createTestServer(t)
	server.shutdownStarted.Store(true)

	ctx := context.Background()
	err := server.Stop(ctx)
	if err != nil {
		t.Errorf("Stop should be idempotent, got error: %v", err)
	}
}

func TestServer_Stop_Timeout(t *testing.T) {
	server := createTestServer(t)
	server.state.Store(ServerStateRunning)

	server.stopCh = make(chan struct{})

	// Add a long-running task to the wait group
	server.taskWg.Add(1)
	taskFinished := make(chan struct{})
	go func() {
		defer server.taskWg.Done()
		defer close(taskFinished)
		// Simulate a task that takes longer than our test timeout
		time.Sleep(100 * time.Millisecond)
	}()

	// Use a very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := server.Stop(ctx)
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Errorf("Stop() took too long (%v), should respect context timeout", elapsed)
	}

	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected timeout-related error or nil, got %v", err)
	}

	select {
	case <-taskFinished:
		// Task completed
	case <-time.After(200 * time.Millisecond):
		t.Error("Background task didn't complete within reasonable time")
	}
}

func TestServer_Renew_Success(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	renewedLockInfo := &types.LockInfo{
		LockID:     "test-lock",
		OwnerID:    "test-client",
		Version:    2,
		AcquiredAt: time.Now(),
		ExpiresAt:  time.Now().Add(2 * time.Minute),
	}

	mockRaft := server.raftNode.(*mockRaft)
	mockRaft.proposeFunc = func(ctx context.Context, cmd []byte) (types.Index, types.Term, bool, error) {
		return 1, 1, true, nil
	}

	mockTracker := server.proposalTracker.(*mockProposalTracker)
	mockTracker.trackFunc = func(proposal *types.PendingProposal) error {
		go func() {
			proposal.ResultCh <- types.ProposalResult{
				Success: true,
				Data:    renewedLockInfo,
			}
		}()
		return nil
	}

	req := &pb.RenewRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Version:  1,
		NewTtl:   durationpb.New(2 * time.Minute),
	}

	resp, err := server.Renew(ctx, req)
	if err != nil {
		t.Errorf("Renew request failed: %v", err)
	}

	if !resp.Renewed {
		t.Error("Expected lock to be renewed")
	}

	if resp.Lock == nil {
		t.Fatal("Expected updated lock info")
	}

	if resp.Lock.Version != 2 {
		t.Errorf("Expected version 2, got %d", resp.Lock.Version)
	}

	if resp.Lock.LockId != "test-lock" {
		t.Errorf("Expected lock ID test-lock, got %s", resp.Lock.LockId)
	}
}

func TestServer_Renew_ValidationError(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	req := &pb.RenewRequest{
		LockId:   "", // Invalid empty lock ID
		ClientId: "test-client",
		Version:  1,
		NewTtl:   durationpb.New(time.Minute),
	}

	_, err := server.Renew(ctx, req)
	if err == nil {
		t.Error("Expected validation error")
	}

	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument status, got %v", status.Code(err))
	}
}

func TestServer_Renew_NotLeader(t *testing.T) {
	server := createTestServer(t)
	server.isLeader.Store(false)
	ctx := context.Background()

	req := &pb.RenewRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Version:  1,
		NewTtl:   durationpb.New(time.Minute),
	}

	_, err := server.Renew(ctx, req)
	if err == nil {
		t.Error("Expected error when not leader")
	}

	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("Expected FailedPrecondition status, got %v", status.Code(err))
	}
}

func TestServer_Renew_ProposalFailure(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	mockTracker := server.proposalTracker.(*mockProposalTracker)
	mockTracker.trackFunc = func(proposal *types.PendingProposal) error {
		go func() {
			proposal.ResultCh <- types.ProposalResult{
				Success: false,
				Error:   lock.ErrLockNotHeld,
			}
		}()
		return nil
	}

	req := &pb.RenewRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Version:  1,
		NewTtl:   durationpb.New(time.Minute),
	}

	_, err := server.Renew(ctx, req)
	if err == nil {
		t.Error("Expected error for proposal failure")
	}
}

func TestServer_GetLocks_Success(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	locks := []*types.LockInfo{
		{
			LockID:      "lock-1",
			OwnerID:     "client-1",
			Version:     1,
			WaiterCount: 0,
			Metadata:    map[string]string{"env": "prod"},
		},
		{
			LockID:      "lock-2",
			OwnerID:     "client-2",
			Version:     2,
			WaiterCount: 1,
			Metadata:    map[string]string{"env": "test"},
		},
	}

	mockLM := server.lockManager.(*mockLockManager)
	mockLM.getLocksFunc = func(ctx context.Context, filter lock.LockFilter, limit int, offset int) ([]*types.LockInfo, int, error) {
		return locks, len(locks), nil
	}

	req := &pb.GetLocksRequest{
		Limit:          10,
		Offset:         0,
		IncludeWaiters: true,
	}

	resp, err := server.GetLocks(ctx, req)
	if err != nil {
		t.Errorf("GetLocks request failed: %v", err)
	}

	if len(resp.Locks) != 2 {
		t.Errorf("Expected 2 locks, got %d", len(resp.Locks))
	}

	if resp.TotalMatchingFilter != 2 {
		t.Errorf("Expected total 2, got %d", resp.TotalMatchingFilter)
	}

	if resp.HasMore {
		t.Error("Expected HasMore to be false")
	}

	if resp.Locks[0].LockId != "lock-1" {
		t.Errorf("Expected lock ID lock-1, got %s", resp.Locks[0].LockId)
	}

	if resp.Locks[1].WaiterCount != 1 {
		t.Errorf("Expected waiter count 1, got %d", resp.Locks[1].WaiterCount)
	}
}

func TestServer_GetLocks_WithFilter(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	var capturedFilter lock.LockFilter

	mockLM := server.lockManager.(*mockLockManager)
	mockLM.getLocksFunc = func(ctx context.Context, filter lock.LockFilter, limit int, offset int) ([]*types.LockInfo, int, error) {
		capturedFilter = filter

		testLock := &types.LockInfo{
			LockID:   "test-lock",
			OwnerID:  "test-client",
			Metadata: map[string]string{"env": "test"},
		}

		if filter(testLock) {
			return []*types.LockInfo{testLock}, 1, nil
		}
		return []*types.LockInfo{}, 0, nil
	}

	req := &pb.GetLocksRequest{
		Filter: &pb.LockFilter{
			LockIdPattern: "test",
			OnlyHeld:      true,
			MetadataFilter: map[string]string{
				"env": "test",
			},
		},
		Limit:  10,
		Offset: 0,
	}

	resp, err := server.GetLocks(ctx, req)
	if err != nil {
		t.Errorf("GetLocks with filter failed: %v", err)
	}

	if len(resp.Locks) != 1 {
		t.Errorf("Expected 1 lock, got %d", len(resp.Locks))
	}

	if capturedFilter == nil {
		t.Error("Expected filter to be captured")
	}

	nonMatchingLock := &types.LockInfo{
		LockID:   "other-lock",
		OwnerID:  "test-client",
		Metadata: map[string]string{"env": "prod"}, // Different metadata
	}

	if capturedFilter(nonMatchingLock) {
		t.Error("Filter should not match lock with different metadata")
	}
}

func TestServer_GetLocks_ValidationError(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	req := &pb.GetLocksRequest{
		Limit:  -1, // Invalid negative limit
		Offset: 0,
	}

	_, err := server.GetLocks(ctx, req)
	if err == nil {
		t.Error("Expected validation error for negative limit")
	}

	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument status, got %v", status.Code(err))
	}
}

func TestServer_GetLocks_LimitCapping(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	tests := []struct {
		name          string
		requestLimit  int32
		expectedLimit int
	}{
		{
			name:          "zero limit uses default",
			requestLimit:  0,
			expectedLimit: DefaultPageLimit,
		},
		{
			name:          "valid limit under max",
			requestLimit:  50,
			expectedLimit: 50,
		},
		{
			name:          "limit at max boundary",
			requestLimit:  MaxPageLimit,
			expectedLimit: MaxPageLimit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedLimit int

			mockLM := server.lockManager.(*mockLockManager)
			mockLM.getLocksFunc = func(ctx context.Context, filter lock.LockFilter, limit int, offset int) ([]*types.LockInfo, int, error) {
				capturedLimit = limit
				return []*types.LockInfo{}, 0, nil
			}

			req := &pb.GetLocksRequest{
				Limit:  tt.requestLimit,
				Offset: 0,
			}

			_, err := server.GetLocks(ctx, req)
			if err != nil {
				t.Errorf("GetLocks failed: %v", err)
			}

			if capturedLimit != tt.expectedLimit {
				t.Errorf("Expected limit %d, got %d", tt.expectedLimit, capturedLimit)
			}
		})
	}
}

func TestServer_EnqueueWaiter_Success(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	mockRaft := server.raftNode.(*mockRaft)
	mockRaft.proposeFunc = func(ctx context.Context, cmd []byte) (types.Index, types.Term, bool, error) {
		return 1, 1, true, nil
	}

	mockTracker := server.proposalTracker.(*mockProposalTracker)
	mockTracker.trackFunc = func(proposal *types.PendingProposal) error {
		go func() {
			proposal.ResultCh <- types.ProposalResult{
				Success: true,
				Data:    1, // Position in queue
			}
		}()
		return nil
	}

	mockLM := server.lockManager.(*mockLockManager)
	mockLM.getInfoFunc = func(ctx context.Context, lockID types.LockID) (*types.LockInfo, error) {
		return &types.LockInfo{
			LockID:      lockID,
			OwnerID:     "other-client",
			ExpiresAt:   time.Now().Add(time.Minute),
			WaiterCount: 2,
			WaitersInfo: []types.WaiterInfo{
				{Position: 0}, {Position: 1},
			},
		}, nil
	}

	req := &pb.EnqueueWaiterRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Timeout:  durationpb.New(time.Minute),
		Priority: 5,
		Version:  1,
	}

	resp, err := server.EnqueueWaiter(ctx, req)
	if err != nil {
		t.Errorf("EnqueueWaiter request failed: %v", err)
	}

	if !resp.Enqueued {
		t.Error("Expected client to be enqueued")
	}

	if resp.Position != 1 {
		t.Errorf("Expected position 1, got %d", resp.Position)
	}

	if resp.EstimatedWaitDuration == nil {
		t.Error("Expected estimated wait duration")
	}

	if resp.EstimatedWaitDuration.AsDuration() <= 0 {
		t.Error("Expected positive estimated wait duration")
	}
}

func TestServer_EnqueueWaiter_ValidationError(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	tests := []struct {
		name string
		req  *pb.EnqueueWaiterRequest
	}{
		{
			name: "empty lock ID",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "",
				ClientId: "test-client",
				Timeout:  durationpb.New(time.Minute),
			},
		},
		{
			name: "empty client ID",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "",
				Timeout:  durationpb.New(time.Minute),
			},
		},
		{
			name: "missing timeout",
			req: &pb.EnqueueWaiterRequest{
				LockId:   "test-lock",
				ClientId: "test-client",
				Timeout:  nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.EnqueueWaiter(ctx, tt.req)
			if err == nil {
				t.Error("Expected validation error")
			}

			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("Expected InvalidArgument status, got %v", status.Code(err))
			}
		})
	}
}

func TestServer_EnqueueWaiter_NotLeader(t *testing.T) {
	server := createTestServer(t)
	server.isLeader.Store(false)
	ctx := context.Background()

	req := &pb.EnqueueWaiterRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Timeout:  durationpb.New(time.Minute),
	}

	_, err := server.EnqueueWaiter(ctx, req)
	if err == nil {
		t.Error("Expected error when not leader")
	}

	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("Expected FailedPrecondition status, got %v", status.Code(err))
	}
}

func TestServer_CancelWait_Success(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	mockRaft := server.raftNode.(*mockRaft)
	mockRaft.proposeFunc = func(ctx context.Context, cmd []byte) (types.Index, types.Term, bool, error) {
		return 1, 1, true, nil
	}

	mockTracker := server.proposalTracker.(*mockProposalTracker)
	mockTracker.trackFunc = func(proposal *types.PendingProposal) error {
		go func() {
			proposal.ResultCh <- types.ProposalResult{
				Success: true,
				Data:    true, // Successfully cancelled
			}
		}()
		return nil
	}

	req := &pb.CancelWaitRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Version:  1,
	}

	resp, err := server.CancelWait(ctx, req)
	if err != nil {
		t.Errorf("CancelWait request failed: %v", err)
	}

	if !resp.Cancelled {
		t.Error("Expected wait to be cancelled")
	}

	if resp.Error != nil {
		t.Errorf("Expected no error, got %v", resp.Error)
	}
}

func TestServer_CancelWait_NotWaiting(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	mockRaft := server.raftNode.(*mockRaft)
	mockRaft.proposeFunc = func(ctx context.Context, cmd []byte) (types.Index, types.Term, bool, error) {
		return 1, 1, true, nil
	}

	mockTracker := server.proposalTracker.(*mockProposalTracker)
	mockTracker.trackFunc = func(proposal *types.PendingProposal) error {
		go func() {
			proposal.ResultCh <- types.ProposalResult{
				Success: true,
				Data:    false, // Not found/cancelled
			}
		}()
		return nil
	}

	req := &pb.CancelWaitRequest{
		LockId:   "test-lock",
		ClientId: "test-client",
		Version:  1,
	}

	resp, err := server.CancelWait(ctx, req)
	if err != nil {
		t.Errorf("CancelWait request failed: %v", err)
	}

	if resp.Cancelled {
		t.Error("Expected wait not to be cancelled")
	}

	if resp.Error == nil {
		t.Error("Expected error to be populated")
	}

	if resp.Error.Code != pb.ErrorCode_NOT_WAITING {
		t.Errorf("Expected NOT_WAITING error, got %v", resp.Error.Code)
	}
}

func TestServer_CancelWait_ValidationError(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	req := &pb.CancelWaitRequest{
		LockId:   "", // Invalid empty lock ID
		ClientId: "test-client",
		Version:  1,
	}

	_, err := server.CancelWait(ctx, req)
	if err == nil {
		t.Error("Expected validation error")
	}

	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument status, got %v", status.Code(err))
	}
}

func TestServer_GetBackoffAdvice_Success(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	mockLM := server.lockManager.(*mockLockManager)
	mockLM.getInfoFunc = func(ctx context.Context, lockID types.LockID) (*types.LockInfo, error) {
		return &types.LockInfo{
			LockID:      lockID,
			OwnerID:     "other-client",
			ExpiresAt:   time.Now().Add(time.Minute),
			WaiterCount: 3,
		}, nil
	}

	req := &pb.BackoffAdviceRequest{
		LockId: "test-lock",
	}

	resp, err := server.GetBackoffAdvice(ctx, req)
	if err != nil {
		t.Errorf("GetBackoffAdvice request failed: %v", err)
	}

	if resp.InitialBackoff == nil {
		t.Error("Expected initial backoff to be set")
	}

	if resp.MaxBackoff == nil {
		t.Error("Expected max backoff to be set")
	}

	if resp.Multiplier <= 0 {
		t.Error("Expected positive multiplier")
	}

	if resp.JitterFactor < 0 || resp.JitterFactor > 1 {
		t.Errorf("Expected jitter factor between 0 and 1, got %f", resp.JitterFactor)
	}

	if resp.InitialBackoff.AsDuration() <= 0 {
		t.Error("Expected positive initial backoff")
	}

	if resp.MaxBackoff.AsDuration() <= resp.InitialBackoff.AsDuration() {
		t.Error("Expected max backoff to be greater than initial backoff")
	}
}

func TestServer_GetBackoffAdvice_ValidationError(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	req := &pb.BackoffAdviceRequest{
		LockId: "", // Invalid empty lock ID
	}

	_, err := server.GetBackoffAdvice(ctx, req)
	if err == nil {
		t.Error("Expected validation error")
	}

	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument status, got %v", status.Code(err))
	}
}

func TestServer_GetBackoffAdvice_LockManagerError(t *testing.T) {
	server := createTestServer(t)
	ctx := context.Background()

	mockLM := server.lockManager.(*mockLockManager)
	mockLM.getInfoFunc = func(ctx context.Context, lockID types.LockID) (*types.LockInfo, error) {
		return nil, errors.New("internal error")
	}

	req := &pb.BackoffAdviceRequest{
		LockId: "test-lock",
	}

	resp, err := server.GetBackoffAdvice(ctx, req)
	if err != nil {
		t.Errorf("GetBackoffAdvice should handle lock manager errors gracefully, got: %v", err)
	}

	// Should return default advice even on errors
	if resp.InitialBackoff.AsDuration() != 100*time.Millisecond {
		t.Error("Expected default initial backoff on error")
	}
}

func TestServer_ResolveEnqueueWaitTimeout(t *testing.T) {
	server := createTestServer(t)

	tests := []struct {
		name     string
		input    *durationpb.Duration
		expected time.Duration
	}{
		{
			name:     "valid timeout",
			input:    durationpb.New(time.Minute),
			expected: time.Minute,
		},
		{
			name:     "nil timeout",
			input:    nil,
			expected: lock.DefaultWaitQueueTimeout,
		},
		{
			name:     "too short timeout",
			input:    durationpb.New(time.Millisecond),
			expected: lock.DefaultWaitQueueTimeout,
		},
		{
			name:     "too long timeout",
			input:    durationpb.New(time.Hour),
			expected: lock.DefaultWaitQueueTimeout,
		},
		{
			name:     "zero timeout",
			input:    durationpb.New(0),
			expected: lock.DefaultWaitQueueTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := server.resolveEnqueueWaitTimeout(tt.input)
			if result != tt.expected {
				t.Errorf("Expected timeout %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestServer_ParseEnqueuePositionResult(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		expected    any
		expectError bool
	}{
		{
			name:        "valid integer",
			input:       5,
			expected:    5,
			expectError: false,
		},
		{
			name:        "zero position",
			input:       0,
			expected:    0,
			expectError: false,
		},
		{
			name:        "invalid string type",
			input:       "invalid",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "invalid float type",
			input:       5.5,
			expected:    nil,
			expectError: true,
		},
		{
			name:        "nil input",
			input:       nil,
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseEnqueuePositionResult(tt.input)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
				if result != tt.expected {
					t.Errorf("Expected result %v, got %v", tt.expected, result)
				}
			}
		})
	}
}

func TestServer_EstimateEnqueueWaitDuration(t *testing.T) {
	server := createTestServer(t)

	mockLM := server.lockManager.(*mockLockManager)

	tests := []struct {
		name         string
		lockInfo     *types.LockInfo
		position     int
		lockMgrError error
		expectZero   bool
		expectPos    bool
	}{
		{
			name: "no owner and no waiters",
			lockInfo: &types.LockInfo{
				LockID:      "test-lock",
				OwnerID:     "",
				WaiterCount: 0,
			},
			position:   0,
			expectZero: true,
		},
		{
			name: "has owner and waiters",
			lockInfo: &types.LockInfo{
				LockID:      "test-lock",
				OwnerID:     "owner",
				ExpiresAt:   time.Now().Add(time.Minute),
				WaiterCount: 2,
				WaitersInfo: []types.WaiterInfo{
					{Position: 0},
					{Position: 1},
				},
			},
			position:  2,
			expectPos: true,
		},
		{
			name:         "lock manager error",
			lockMgrError: errors.New("test error"),
			position:     0,
			expectZero:   true,
		},
		{
			name:       "nil lock info",
			lockInfo:   nil,
			position:   0,
			expectZero: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLM.getInfoFunc = func(ctx context.Context, lockID types.LockID) (*types.LockInfo, error) {
				return tt.lockInfo, tt.lockMgrError
			}

			duration := server.estimateEnqueueWaitDuration("test-lock", tt.position)

			if tt.expectZero {
				if duration != 0 {
					t.Errorf("Expected 0 duration, got %v", duration)
				}
			} else if tt.expectPos {
				if duration <= 0 {
					t.Error("Expected positive duration")
				}
			}
		})
	}
}

func TestServer_ProtoFilterToLockFilter(t *testing.T) {
	server := createTestServer(t)

	t.Run("nil filter returns FilterAll", func(t *testing.T) {
		filter := server.protoFilterToLockFilter(nil)
		testLock := &types.LockInfo{LockID: "any-lock"}
		if !filter(testLock) {
			t.Error("Nil filter should accept all locks")
		}
	})

	t.Run("lock ID pattern filter", func(t *testing.T) {
		protoFilter := &pb.LockFilter{
			LockIdPattern: "test",
		}
		filter := server.protoFilterToLockFilter(protoFilter)

		matchingLock := &types.LockInfo{LockID: "test-lock"}
		if !filter(matchingLock) {
			t.Error("Filter should match lock with pattern in ID")
		}

		nonMatchingLock := &types.LockInfo{LockID: "other-lock"}
		if filter(nonMatchingLock) {
			t.Error("Filter should not match lock without pattern in ID")
		}
	})

	t.Run("owner ID pattern filter", func(t *testing.T) {
		protoFilter := &pb.LockFilter{
			OwnerIdPattern: "client",
		}
		filter := server.protoFilterToLockFilter(protoFilter)

		matchingLock := &types.LockInfo{OwnerID: "test-client"}
		if !filter(matchingLock) {
			t.Error("Filter should match lock with pattern in owner ID")
		}

		nonMatchingLock := &types.LockInfo{OwnerID: "admin"}
		if filter(nonMatchingLock) {
			t.Error("Filter should not match lock without pattern in owner ID")
		}
	})

	t.Run("only held filter", func(t *testing.T) {
		protoFilter := &pb.LockFilter{
			OnlyHeld: true,
		}
		filter := server.protoFilterToLockFilter(protoFilter)

		heldLock := &types.LockInfo{OwnerID: "client"}
		if !filter(heldLock) {
			t.Error("Filter should match held lock when OnlyHeld is true")
		}

		unheldLock := &types.LockInfo{OwnerID: ""}
		if filter(unheldLock) {
			t.Error("Filter should not match unheld lock when OnlyHeld is true")
		}
	})

	t.Run("metadata filter", func(t *testing.T) {
		protoFilter := &pb.LockFilter{
			MetadataFilter: map[string]string{
				"env": "test",
				"app": "myapp",
			},
		}
		filter := server.protoFilterToLockFilter(protoFilter)

		matchingLock := &types.LockInfo{
			Metadata: map[string]string{
				"env":   "test",
				"app":   "myapp",
				"extra": "value",
			},
		}
		if !filter(matchingLock) {
			t.Error("Filter should match lock with all required metadata")
		}

		nonMatchingLock := &types.LockInfo{
			Metadata: map[string]string{
				"env": "test",
				// missing "app": "myapp"
			},
		}
		if filter(nonMatchingLock) {
			t.Error("Filter should not match lock missing required metadata")
		}

		// Test with nil metadata
		nilMetadataLock := &types.LockInfo{Metadata: nil}
		if filter(nilMetadataLock) {
			t.Error("Filter should not match lock with nil metadata")
		}
	})

	t.Run("time-based filters", func(t *testing.T) {
		now := time.Now()
		protoFilter := &pb.LockFilter{
			ExpiresBefore: timestamppb.New(now.Add(time.Hour)),
			ExpiresAfter:  timestamppb.New(now.Add(-time.Hour)),
		}
		filter := server.protoFilterToLockFilter(protoFilter)

		matchingLock := &types.LockInfo{
			ExpiresAt: now.Add(30 * time.Minute), // Between -1h and +1h
		}
		if !filter(matchingLock) {
			t.Error("Filter should match lock within time range")
		}

		tooEarlyLock := &types.LockInfo{
			ExpiresAt: now.Add(-2 * time.Hour), // Before -1h
		}
		if filter(tooEarlyLock) {
			t.Error("Filter should not match lock expiring too early")
		}

		tooLateLock := &types.LockInfo{
			ExpiresAt: now.Add(2 * time.Hour), // After +1h
		}
		if filter(tooLateLock) {
			t.Error("Filter should not match lock expiring too late")
		}
	})

	t.Run("complex combined filter", func(t *testing.T) {
		now := time.Now()
		protoFilter := &pb.LockFilter{
			LockIdPattern:  "test",
			OwnerIdPattern: "client",
			OnlyHeld:       true,
			ExpiresBefore:  timestamppb.New(now.Add(time.Hour)),
			MetadataFilter: map[string]string{"env": "prod"},
		}
		filter := server.protoFilterToLockFilter(protoFilter)

		// Lock that matches all criteria
		matchingLock := &types.LockInfo{
			LockID:    "test-lock",
			OwnerID:   "test-client",
			ExpiresAt: now.Add(30 * time.Minute),
			Metadata:  map[string]string{"env": "prod", "other": "value"},
		}
		if !filter(matchingLock) {
			t.Error("Filter should match lock meeting all criteria")
		}

		// Lock that fails one criterion (wrong metadata)
		wrongMetadataLock := &types.LockInfo{
			LockID:    "test-lock",
			OwnerID:   "test-client",
			ExpiresAt: now.Add(30 * time.Minute),
			Metadata:  map[string]string{"env": "dev"}, // Wrong env
		}
		if filter(wrongMetadataLock) {
			t.Error("Filter should not match lock with wrong metadata")
		}
	})
}

func TestServer_UnaryInterceptor(t *testing.T) {
	server := createTestServer(t)
	mockHandler := func(ctx context.Context, req any) (any, error) {
		return "response", nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/RaftLock/TestMethod"}

	t.Run("server not running", func(t *testing.T) {
		server.state.Store(ServerStateStarting)
		_, err := server.unaryInterceptor(context.Background(), "request", info, mockHandler)
		st, _ := status.FromError(err)
		if st.Code() != codes.Unavailable || !strings.Contains(st.Message(), ErrServerNotStarted.Error()) {
			t.Errorf("Expected Unavailable with ErrServerNotStarted, got code %v, msg %s", st.Code(), st.Message())
		}
	})

	t.Run("rate limited", func(t *testing.T) {
		server.state.Store(ServerStateRunning)
		server.rateLimiter = &mockRateLimiter{allow: false} // Force rate limit
		_, err := server.unaryInterceptor(context.Background(), "request", info, mockHandler)
		st, _ := status.FromError(err)
		if st.Code() != codes.ResourceExhausted || !strings.Contains(st.Message(), "rate limit exceeded") {
			t.Errorf("Expected ResourceExhausted, got %v", st.Code())
		}
		server.rateLimiter = nil // Reset for other tests
	})

	t.Run("concurrency limit exceeded", func(t *testing.T) {
		server.state.Store(ServerStateRunning)

		for range server.config.MaxConcurrentReqs {
			server.requestSemaphore <- struct{}{}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond) // Short timeout
		defer cancel()
		_, err := server.unaryInterceptor(ctx, "request", info, mockHandler)
		st, _ := status.FromError(err)
		if st.Code() != codes.DeadlineExceeded { // Expect timeout because it can't acquire slot
			t.Errorf("Expected DeadlineExceeded due to concurrency limit, got %v", st.Code())
		}

		for range server.config.MaxConcurrentReqs {
			<-server.requestSemaphore
		}
	})

	t.Run("successful request", func(t *testing.T) {
		server.state.Store(ServerStateRunning)
		server.rateLimiter = nil // Disable rate limiting for this sub-test
		resp, err := server.unaryInterceptor(context.Background(), "request", info, mockHandler)
		if err != nil {
			t.Errorf("Expected no error for successful request, got %v", err)
		}
		if resp != "response" {
			t.Errorf("Expected response 'response', got '%v'", resp)
		}
	})

	t.Run("request timeout applied", func(t *testing.T) {
		server.state.Store(ServerStateRunning)
		server.rateLimiter = nil
		server.config.RequestTimeout = 5 * time.Millisecond // Very short timeout

		slowHandler := func(ctx context.Context, req any) (any, error) {
			select {
			case <-time.After(10 * time.Millisecond): // Longer than request timeout
				return "response", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		_, err := server.unaryInterceptor(context.Background(), "request", info, slowHandler)
		st, _ := status.FromError(err)

		if st.Code() != codes.DeadlineExceeded {
			t.Errorf("Expected DeadlineExceeded due to request timeout, got code %v, msg %s", st.Code(), st.Message())
		}
		server.config.RequestTimeout = time.Second // Reset for other tests
	})
}

func TestServer_InitializeStorage(t *testing.T) {
	server := createTestServer(t)
	server.config.DataDir = "/tmp/test-init-storage"
	err := server.initializeStorage()
	if err != nil {
		t.Errorf("initializeStorage failed: %v", err)
	}
	if server.storage == nil {
		t.Error("Expected storage to be initialized")
	}
}

func TestServer_InitializeLockManager(t *testing.T) {
	server := createTestServer(t)
	err := server.initializeLockManager()
	if err != nil {
		t.Errorf("initializeLockManager failed: %v", err)
	}
	if server.lockManager == nil {
		t.Error("Expected lockManager to be initialized")
	}
}

func TestServer_InitializeRaftNode(t *testing.T) {
	server := createTestServer(t)
	// Replace mock with nil to allow real initialization
	server.raftNode = nil
	server.storage = &mockStorage{} // Reuse mock
	server.lockManager = &mockLockManager{}
	err := server.initializeRaftNode()
	if err != nil {
		t.Errorf("initializeRaftNode failed: %v", err)
	}
	if server.raftNode == nil {
		t.Error("Expected raftNode to be initialized")
	}
}

func TestServer_InitializeNetworkManager(t *testing.T) {
	server := createTestServer(t)
	// Replace mock raftNode with real one using initializer
	server.raftNode = &mockRaft{}
	err := server.initializeNetworkManager()
	if err != nil {
		t.Errorf("initializeNetworkManager failed: %v", err)
	}
	if server.networkMgr == nil {
		t.Error("Expected networkMgr to be initialized")
	}
}

func TestServer_InitializeAndStartGRPCServer(t *testing.T) {
	server := createTestServer(t)
	// Use a free port for listener to avoid conflicts
	server.config.ListenAddress = "localhost:0"
	err := server.initializeAndStartGRPCServer()
	if err != nil {
		t.Errorf("initializeAndStartGRPCServer failed: %v", err)
	}
	if server.grpcServer == nil {
		t.Error("Expected grpcServer to be initialized")
	}
	if server.listener == nil {
		t.Error("Expected listener to be initialized")
	}
	// Shutdown server to release resources
	server.grpcServer.Stop()
}

func TestServer_StartBackgroundTasks(t *testing.T) {
	server := createTestServer(t)
	server.stopCh = make(chan struct{})
	server.startBackgroundTasks()
	time.Sleep(10 * time.Millisecond)
	close(server.stopCh)
	server.taskWg.Wait()
}

func TestServer_RunBackgroundTask(t *testing.T) {
	server := createTestServer(t)
	called := false
	fn := func() {
		called = true
	}
	server.runBackgroundTask("test-task", fn)
	server.taskWg.Wait()
	if !called {
		t.Error("Expected background task to be executed")
	}
}

func TestServer_RunRaftTickLoop(t *testing.T) {
	server := createTestServer(t)
	server.stopCh = make(chan struct{})
	mockClock := server.clock.(*mockClock)
	go server.runRaftTickLoop()
	mockClock.AdvanceAndTrigger(DefaultRaftTickInterval)
	close(server.stopCh)
	server.taskWg.Wait()
}

func TestServer_RunLeaderChangeMonitor(t *testing.T) {
	server := createTestServer(t)
	mockRaft := server.raftNode.(*mockRaft)
	ch := make(chan types.NodeID, 1)
	mockRaft.leaderChangeCh = ch
	server.stopCh = make(chan struct{})
	ch <- "new-leader"

	go server.runLeaderChangeMonitor()
	time.Sleep(10 * time.Millisecond)
	close(server.stopCh)
	server.taskWg.Wait()

	if server.currentLeaderID.Load().(types.NodeID) != "new-leader" {
		t.Error("Expected leader ID to be updated")
	}
}

func TestServer_RunApplyMessageProcessor(t *testing.T) {
	server := createTestServer(t)
	mockRaft := server.raftNode.(*mockRaft)
	applyCh := make(chan types.ApplyMsg, 1)
	mockRaft.applyCh = applyCh
	server.stopCh = make(chan struct{})

	applyCh <- types.ApplyMsg{
		CommandValid: true,
		CommandIndex: 1,
		CommandTerm:  1,
	}

	go server.runApplyMessageProcessor()
	time.Sleep(10 * time.Millisecond)
	close(server.stopCh)
	server.taskWg.Wait()
}

func TestServer_HandleApplyMessage_Command(t *testing.T) {
	server := createTestServer(t)
	msg := types.ApplyMsg{
		CommandValid: true,
		CommandIndex: 10,
		CommandTerm:  2,
	}
	server.handleApplyMessage(msg)
	if server.lastCommitIndex.Load() != 10 {
		t.Error("Expected lastCommitIndex to be updated")
	}
}

func TestServer_HandleApplyMessage_Snapshot(t *testing.T) {
	server := createTestServer(t)
	msg := types.ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: 20,
		SnapshotTerm:  3,
	}
	server.handleApplyMessage(msg)
	if server.lastCommitIndex.Load() != 20 {
		t.Error("Expected lastCommitIndex to be updated for snapshot")
	}
}

func TestServer_RunHealthCheckLoop(t *testing.T) {
	server := createTestServer(t)
	mockClock := server.clock.(*mockClock)
	server.config.HealthCheckInterval = 10 * time.Millisecond
	server.stopCh = make(chan struct{})

	go server.runHealthCheckLoop()
	mockClock.AdvanceAndTrigger(10 * time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	close(server.stopCh)
	server.taskWg.Wait()
}

func TestServer_RunLockManagerTickLoop_Expired(t *testing.T) {
	server := createTestServer(t)
	server.stopCh = make(chan struct{})

	mockLM := server.lockManager.(*mockLockManager)
	mockClock := server.clock.(*mockClock)

	tickHappened := make(chan bool, 1)    // To detect Tick() call
	loopStarted := make(chan struct{}, 1) // To detect loop readiness

	mockLM.tickFunc = func(ctx context.Context) int {
		tickHappened <- true
		return 1
	}

	go func() {
		loopStarted <- struct{}{} // Signal loop is launching
		server.runLockManagerTickLoop()
	}()

	// Wait until the loop goroutine has started
	select {
	case <-loopStarted:
		// Proceed to trigger the tick
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for tick loop to start")
	}

	mockClock.AdvanceAndTrigger(DefaultLockManagerTickInterval)

	select {
	case <-tickHappened:
		// Test passed
	case <-time.After(1 * time.Second):
		t.Error("Expected LockManager.Tick to be called but it timed out")
	}

	close(server.stopCh)
	server.taskWg.Wait()
}

func TestServer_RunLeaderChangeMonitor_ChannelClosed(t *testing.T) {
	server := createTestServer(t)
	server.stopCh = make(chan struct{})
	mockRaftNode := server.raftNode.(*mockRaft)
	closedCh := make(chan types.NodeID)
	close(closedCh)
	mockRaftNode.leaderChangeCh = closedCh

	server.taskWg.Add(1) // Explicitly Add for the goroutine started by this test
	go func() {
		defer server.taskWg.Done()
		server.runLeaderChangeMonitor()
	}()

	server.taskWg.Wait()

	// Assertions are now made after the goroutine has completed
	if server.isLeader.Load() {
		t.Error("Expected isLeader to be false after channel close")
	}
	currentLeader := server.currentLeaderID.Load()
	if id, ok := currentLeader.(types.NodeID); !ok || id != "" {
		t.Errorf("Expected currentLeaderID to be empty types.NodeID, got %v (type %T)", currentLeader, currentLeader)
	}
}

func TestServer_SubmitRaftProposal_ContextCancellation(t *testing.T) {
	server := createTestServer(t)
	server.raftNode = &mockRaft{isLeader: true} // Ensure it thinks it's leader

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond) // Very short timeout
	defer cancel()

	cmd := types.Command{Op: types.OperationAcquire, LockID: "test"}

	// Make proposalTracker.Track block until context is cancelled
	server.proposalTracker = &mockProposalTracker{
		trackFunc: func(p *types.PendingProposal) error {
			// Simulate Track itself respecting context, or just don't send to ResultCh
			<-p.Context.Done() // Wait for client context to be done
			return p.Context.Err()
		},
	}

	_, err := server.submitRaftProposal(ctx, cmd)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}

func TestServer_HealthStatusToProto_UptimeNotSet(t *testing.T) {
	server := createTestServer(t)
	server.serverStartTime = time.Time{} // Force zero time
	server.lockManager = nil             // Simulate lock manager not yet ready

	protoStatus := server.healthStatusToProto(true)

	if protoStatus.Uptime.AsDuration() != 0 {
		t.Errorf("Expected 0 uptime duration, got %v", protoStatus.Uptime.AsDuration())
	}
	if protoStatus.CurrentActiveLocks != 0 {
		t.Error("Expected 0 active locks when lockManager is nil")
	}
	if protoStatus.CurrentTotalWaiters != 0 {
		t.Error("Expected 0 total waiters when lockManager is nil")
	}
}
