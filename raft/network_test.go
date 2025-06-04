package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft/proto"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setupNetworkManagerWithOpts(t *testing.T, id types.NodeID, selfAddr string, peers map[types.NodeID]PeerConfig, opts GRPCNetworkManagerOptions, customClock Clock, customMetrics Metrics) (*gRPCNetworkManager, *mockRPCHandler, *atomic.Bool, Clock) {
	t.Helper()
	isShutdown := &atomic.Bool{}
	handler := &mockRPCHandler{}
	clockToUse := customClock
	if clockToUse == nil {
		clockToUse = newControlledMockClock()
	}
	metricsToUse := customMetrics
	if metricsToUse == nil {
		metricsToUse = NewNoOpMetrics()
	}

	var finalAddr string
	var tempListener net.Listener
	var err error

	if selfAddr == "" || selfAddr == "localhost:0" {
		tempListener, err = net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("setupNetworkManagerWithOpts: Failed to find an available port for self: %v", err)
		}
		finalAddr = tempListener.Addr().String()
		tempListener.Close()
	} else {
		finalAddr = selfAddr
	}

	if peers == nil {
		peers = make(map[types.NodeID]PeerConfig)
	}
	peers[id] = PeerConfig{ID: id, Address: finalAddr}

	nm, err := NewGRPCNetworkManager(
		id,
		finalAddr,
		peers,
		handler,
		isShutdown,
		logger.NewNoOpLogger(),
		metricsToUse,
		clockToUse,
		opts,
	)
	if err != nil {
		t.Fatalf("setupNetworkManagerWithOpts: NewGRPCNetworkManager failed for ID %s: %v", id, err)
	}
	return nm, handler, isShutdown, clockToUse
}

type controlledMockClock struct {
	currentTime time.Time
	mu          sync.Mutex
	timers      []*controlledTimer
}

type controlledTimer struct {
	ch        chan time.Time
	expiresAt time.Time
	clock     *controlledMockClock
	active    bool
	mu        sync.Mutex
}

func newControlledMockClock() *controlledMockClock {
	return &controlledMockClock{
		currentTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (c *controlledMockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentTime
}

func (c *controlledMockClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

func (c *controlledMockClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &controlledTimer{
		ch:        make(chan time.Time, 1),
		expiresAt: c.currentTime.Add(d),
		clock:     c,
		active:    true,
	}
	c.timers = append(c.timers, timer)
	return timer.ch
}
func (c *controlledMockClock) NewTicker(d time.Duration) Ticker {
	return &mockTicker{C: make(chan time.Time)}
}
func (c *controlledMockClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &controlledTimer{
		ch:        make(chan time.Time, 1),
		expiresAt: c.currentTime.Add(d),
		clock:     c,
		active:    true,
	}
	c.timers = append(c.timers, timer)
	return timer
}

func (c *controlledMockClock) Sleep(d time.Duration) {
	c.mu.Lock()
	c.currentTime = c.currentTime.Add(d)
	c.mu.Unlock()
	time.Sleep(d)
}

func (c *controlledMockClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.currentTime = c.currentTime.Add(d)
	var activeTimers []*controlledTimer
	for _, timer := range c.timers {
		timer.mu.Lock()
		if timer.active && !c.currentTime.Before(timer.expiresAt) {
			select {
			case timer.ch <- timer.expiresAt:
			default:
			}
			timer.active = false
		}
		if timer.active {
			activeTimers = append(activeTimers, timer)
		}
		timer.mu.Unlock()
	}
	c.timers = activeTimers
	c.mu.Unlock()
}

func (ct *controlledTimer) Chan() <-chan time.Time { return ct.ch }
func (ct *controlledTimer) Stop() bool {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if ct.active {
		ct.active = false
		return true
	}
	return false
}
func (ct *controlledTimer) Reset(d time.Duration) bool {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.expiresAt = ct.clock.Now().Add(d)
	ct.active = true
	select {
	case <-ct.ch:
	default:
	}
	return true
}

func setupDefaultNetworkManager(t *testing.T, id types.NodeID, addr string, peers map[types.NodeID]PeerConfig) (*gRPCNetworkManager, *mockRPCHandler, *atomic.Bool, *controlledMockClock) {
	opts := GRPCNetworkManagerOptions{
		DialTimeout:        100 * time.Millisecond,
		ServerStartTimeout: 1 * time.Second,
		KeepaliveTime:      1 * time.Second,
		KeepaliveTimeout:   500 * time.Millisecond,
	}
	nm, handler, shutdownFlag, clock := setupNetworkManagerWithOpts(t, id, addr, peers, opts, newControlledMockClock(), newMockMetrics())
	return nm, handler, shutdownFlag, clock.(*controlledMockClock)
}

func TestNewGRPCNetworkManager(t *testing.T) {
	type testCase struct {
		name             string
		id               types.NodeID
		addr             string
		peers            map[types.NodeID]PeerConfig
		rpcHandler       RPCHandler
		isShutdown       *atomic.Bool
		logger           logger.Logger
		metrics          Metrics
		clock            Clock
		opts             GRPCNetworkManagerOptions
		expectError      bool
		errorMsgContains string
		checkFields      func(t *testing.T, nm *gRPCNetworkManager, tc testCase)
	}

	tests := []testCase{
		{
			name: "Valid configuration",
			id:   "node1",
			addr: "localhost:8080",
			peers: map[types.NodeID]PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
				"node2": {ID: "node2", Address: "localhost:8081"},
			},
			rpcHandler:  &mockRPCHandler{},
			isShutdown:  &atomic.Bool{},
			logger:      logger.NewNoOpLogger(),
			metrics:     NewNoOpMetrics(),
			clock:       newMockClock(),
			opts:        GRPCNetworkManagerOptions{},
			expectError: false,
			checkFields: func(t *testing.T, nm *gRPCNetworkManager, tc testCase) {
				testutil.AssertEqual(t, tc.id, nm.id, "ID mismatch")
				testutil.AssertEqual(t, tc.addr, nm.localAddr, "Address mismatch")
				testutil.AssertNotNil(t, nm.rpcHandler, "RPC Handler should be set")
				testutil.AssertEqual(t, tc.isShutdown, nm.isShutdown, "isShutdown mismatch")
				testutil.AssertNotNil(t, nm.logger, "Logger should be set")
				testutil.AssertNotNil(t, nm.metrics, "Metrics should be set")
				testutil.AssertNotNil(t, nm.clock, "Clock should be set")
				testutil.AssertNotNil(t, nm.peers, "Peers should be set")
				testutil.AssertEqual(t, len(tc.peers), len(nm.peers), "Peer count mismatch")
				testutil.AssertNotNil(t, nm.peerClients, "PeerClients should be initialized")
				testutil.AssertNotNil(t, nm.server, "gRPC server should be created")
				testutil.AssertNotNil(t, nm.serverReady, "serverReady channel should be created")

				defaults := DefaultGRPCNetworkManagerOptions()
				if nm.opts.MaxRecvMsgSize != defaults.MaxRecvMsgSize {
					t.Errorf("Expected default MaxRecvMsgSize %d, got %d", defaults.MaxRecvMsgSize, nm.opts.MaxRecvMsgSize)
				}
			},
		},
		{
			name:             "Empty node ID",
			id:               "",
			addr:             "localhost:8080",
			peers:            map[types.NodeID]PeerConfig{},
			rpcHandler:       &mockRPCHandler{},
			isShutdown:       &atomic.Bool{},
			logger:           logger.NewNoOpLogger(),
			metrics:          NewNoOpMetrics(),
			clock:            newMockClock(),
			opts:             GRPCNetworkManagerOptions{},
			expectError:      true,
			errorMsgContains: "node ID cannot be empty",
		},
		{
			name:             "Empty address",
			id:               "node1",
			addr:             "",
			peers:            map[types.NodeID]PeerConfig{},
			rpcHandler:       &mockRPCHandler{},
			isShutdown:       &atomic.Bool{},
			logger:           logger.NewNoOpLogger(),
			metrics:          NewNoOpMetrics(),
			clock:            newMockClock(),
			opts:             GRPCNetworkManagerOptions{},
			expectError:      true,
			errorMsgContains: "local address cannot be empty",
		},
		{
			name: "Empty peer ID",
			id:   "node1",
			addr: "localhost:8080",
			peers: map[types.NodeID]PeerConfig{
				"":      {ID: "", Address: "localhost:8081"},
				"node1": {ID: "node1", Address: "localhost:8080"},
			},
			rpcHandler:       &mockRPCHandler{},
			isShutdown:       &atomic.Bool{},
			logger:           logger.NewNoOpLogger(),
			metrics:          NewNoOpMetrics(),
			clock:            newMockClock(),
			opts:             GRPCNetworkManagerOptions{},
			expectError:      true,
			errorMsgContains: "peer ID cannot be empty",
		},
		{
			name: "Empty peer address",
			id:   "node1",
			addr: "localhost:8080",
			peers: map[types.NodeID]PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
				"node2": {ID: "node2", Address: ""},
			},
			rpcHandler:       &mockRPCHandler{},
			isShutdown:       &atomic.Bool{},
			logger:           logger.NewNoOpLogger(),
			metrics:          NewNoOpMetrics(),
			clock:            newMockClock(),
			opts:             GRPCNetworkManagerOptions{},
			expectError:      true,
			errorMsgContains: "address for peer node2 cannot be empty",
		},
		{
			name:             "Nil RPC handler",
			id:               "node1",
			addr:             "localhost:8080",
			peers:            map[types.NodeID]PeerConfig{},
			rpcHandler:       nil,
			isShutdown:       &atomic.Bool{},
			logger:           logger.NewNoOpLogger(),
			metrics:          NewNoOpMetrics(),
			clock:            newMockClock(),
			opts:             GRPCNetworkManagerOptions{},
			expectError:      true,
			errorMsgContains: "RPC handler cannot be nil",
		},
		{
			name:             "Nil shutdown flag",
			id:               "node1",
			addr:             "localhost:8080",
			peers:            map[types.NodeID]PeerConfig{},
			rpcHandler:       &mockRPCHandler{},
			isShutdown:       nil,
			logger:           logger.NewNoOpLogger(),
			metrics:          NewNoOpMetrics(),
			clock:            newMockClock(),
			opts:             GRPCNetworkManagerOptions{},
			expectError:      true,
			errorMsgContains: "shutdown flag cannot be nil",
		},
		{
			name:             "Nil logger",
			id:               "node1",
			addr:             "localhost:8080",
			peers:            map[types.NodeID]PeerConfig{},
			rpcHandler:       &mockRPCHandler{},
			isShutdown:       &atomic.Bool{},
			logger:           nil,
			metrics:          NewNoOpMetrics(),
			clock:            newMockClock(),
			opts:             GRPCNetworkManagerOptions{},
			expectError:      true,
			errorMsgContains: "logger cannot be nil",
		},
		{
			name:             "Nil clock",
			id:               "node1",
			addr:             "localhost:8080",
			peers:            map[types.NodeID]PeerConfig{},
			rpcHandler:       &mockRPCHandler{},
			isShutdown:       &atomic.Bool{},
			logger:           logger.NewNoOpLogger(),
			metrics:          NewNoOpMetrics(),
			clock:            nil,
			opts:             GRPCNetworkManagerOptions{},
			expectError:      true,
			errorMsgContains: "clock cannot be nil",
		},
		{
			name: "Nil metrics provided",
			id:   "node1",
			addr: "localhost:8080",
			peers: map[types.NodeID]PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
			},
			rpcHandler:  &mockRPCHandler{},
			isShutdown:  &atomic.Bool{},
			logger:      logger.NewNoOpLogger(),
			metrics:     nil,
			clock:       newMockClock(),
			opts:        GRPCNetworkManagerOptions{},
			expectError: false,
			checkFields: func(t *testing.T, nm *gRPCNetworkManager, tc testCase) {
				if _, ok := nm.metrics.(*noOpMetrics); !ok {
					t.Errorf("Expected nm.metrics to be *noOpMetrics when nil is provided, got %T", nm.metrics)
				}
			},
		},
		{
			name: "Self peer with different address in config",
			id:   "node1",
			addr: "localhost:8080",
			peers: map[types.NodeID]PeerConfig{
				"node1": {ID: "node1", Address: "localhost:9999"},
				"node2": {ID: "node2", Address: "localhost:8081"},
			},
			rpcHandler:  &mockRPCHandler{},
			isShutdown:  &atomic.Bool{},
			logger:      logger.NewNoOpLogger(),
			metrics:     NewNoOpMetrics(),
			clock:       newMockClock(),
			opts:        GRPCNetworkManagerOptions{},
			expectError: false,
			checkFields: func(t *testing.T, nm *gRPCNetworkManager, tc testCase) {
				testutil.AssertEqual(t, tc.addr, nm.localAddr, "Local address should be the one passed to constructor, not from peers map")
			},
		},
		{
			name: "Partial gRPC options provided, others default",
			id:   "node1",
			addr: "localhost:8080",
			peers: map[types.NodeID]PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
			},
			rpcHandler: &mockRPCHandler{},
			isShutdown: &atomic.Bool{},
			logger:     logger.NewNoOpLogger(),
			metrics:    NewNoOpMetrics(),
			clock:      newMockClock(),
			opts: GRPCNetworkManagerOptions{
				MaxRecvMsgSize: 2048 * 1024,
				DialTimeout:    3 * time.Second,
			},
			expectError: false,
			checkFields: func(t *testing.T, nm *gRPCNetworkManager, tc testCase) {
				defaults := DefaultGRPCNetworkManagerOptions()
				testutil.AssertEqual(t, 2048*1024, nm.opts.MaxRecvMsgSize, "MaxRecvMsgSize mismatch")
				testutil.AssertEqual(t, 3*time.Second, nm.opts.DialTimeout, "DialTimeout mismatch")
				testutil.AssertEqual(t, defaults.KeepaliveTime, nm.opts.KeepaliveTime, "KeepaliveTime should be default")
				testutil.AssertEqual(t, defaults.MaxSendMsgSize, nm.opts.MaxSendMsgSize, "MaxSendMsgSize should be default")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			currentLogger := tc.logger
			if currentLogger == nil && tc.name != "Nil logger" {
				currentLogger = logger.NewNoOpLogger()
			}
			currentMetrics := tc.metrics
			if currentMetrics == nil && tc.name != "Nil metrics provided" {
				currentMetrics = NewNoOpMetrics()
			}
			currentClock := tc.clock
			if currentClock == nil && tc.name != "Nil clock" {
				currentClock = newMockClock()
			}
			currentRPCHandler := tc.rpcHandler
			if currentRPCHandler == nil && tc.name != "Nil RPC handler" {
				currentRPCHandler = &mockRPCHandler{}
			}
			currentIsShutdown := tc.isShutdown
			if currentIsShutdown == nil && tc.name != "Nil shutdown flag" {
				currentIsShutdown = &atomic.Bool{}
			}

			nm, err := NewGRPCNetworkManager(
				tc.id,
				tc.addr,
				tc.peers,
				currentRPCHandler,
				currentIsShutdown,
				currentLogger,
				currentMetrics,
				currentClock,
				tc.opts,
			)

			if tc.expectError {
				testutil.AssertError(t, err, "Expected error but got nil")
				if tc.errorMsgContains != "" {
					testutil.AssertContains(t, err.Error(), tc.errorMsgContains, "Error message mismatch")
				}
				testutil.AssertTrue(t, nm == nil, "Expected nil NetworkManager on error")
			} else {
				testutil.AssertNoError(t, err, "Unexpected error")
				testutil.AssertTrue(t, nm != nil, "Expected non-nil NetworkManager")
				if tc.checkFields != nil {
					tc.checkFields(t, nm, tc)
				} else {
					testutil.AssertEqual(t, tc.id, nm.id, "ID mismatch")
					testutil.AssertEqual(t, tc.addr, nm.localAddr, "Address mismatch")
				}
			}
		})
	}
}

func TestNetworkManager_Successful_Start_Stop_Lifecycle(t *testing.T) {
	nm, _, _, _ := setupDefaultNetworkManager(t, "node1", "localhost:0", nil)

	err := nm.Start()
	testutil.AssertNoError(t, err, "NetworkManager.Start() failed on initial start")
	testutil.AssertTrue(t, nm.serverStarted.Load(), "Server should be marked as started after successful Start()")
	testutil.AssertNotNil(t, nm.listener, "Listener should be non-nil after successful Start()")

	select {
	case <-nm.serverReady:
	case <-time.After(nm.opts.ServerStartTimeout + 500*time.Millisecond):
		t.Fatalf("Timeout waiting for server to become ready after initial Start()")
	}

	err = nm.Start()
	testutil.AssertNoError(t, err, "NetworkManager.Start() should be idempotent")

	err = nm.Stop()
	testutil.AssertNoError(t, err, "NetworkManager.Stop() failed")
	testutil.AssertTrue(t, nm.isShutdown.Load(), "Shutdown flag should be true after Stop()")

	err = nm.Stop()
	testutil.AssertError(t, err, "Duplicate Stop() should return an error")
	if err != nil {
		testutil.AssertContains(t, err.Error(), "stop already called")
	}
}

func mockNMForHandler(t *testing.T) *gRPCNetworkManager {
	t.Helper()
	isShutdown := &atomic.Bool{}
	clk := newMockClock()
	met := NewNoOpMetrics()

	return &gRPCNetworkManager{
		id:          types.NodeID("mock-server-nm-id"),
		localAddr:   "mock-server-addr",
		isShutdown:  isShutdown,
		logger:      logger.NewNoOpLogger().WithComponent("mock-nm-for-handler"),
		metrics:     met,
		clock:       clk,
		opts:        DefaultGRPCNetworkManagerOptions(),
		peers:       make(map[types.NodeID]PeerConfig),
		peerClients: make(map[types.NodeID]*peerConnection),
		serverReady: make(chan struct{}),
	}
}

func setupPeerServer(t *testing.T, addr string, actualRPCHandler RPCHandler) (actualAddr string, cleanupFunc func()) {
	t.Helper()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to listen on %s: %v", addr, err)
	}
	actualAddr = l.Addr().String()

	s := grpc.NewServer()

	mockNmInstance := mockNMForHandler(t)

	proto.RegisterRaftServer(s, &grpcServerHandler{
		nm:         mockNmInstance,
		rpcHandler: actualRPCHandler,
		logger:     logger.NewNoOpLogger().WithComponent("peer-grpc-server"),
	})

	serverErrCh := make(chan error, 1)
	go func() {
		if errServe := s.Serve(l); errServe != nil && !errors.Is(errServe, grpc.ErrServerStopped) {
			serverErrCh <- fmt.Errorf("peer gRPC server error: %w", errServe)
		}
		close(serverErrCh)
	}()

	cleanupFunc = func() {
		s.GracefulStop()
		l.Close()
		select {
		case errServe, ok := <-serverErrCh:
			if ok && errServe != nil {
				t.Logf("Peer server goroutine exited with error: %v", errServe)
			} else {
				t.Logf("Peer server goroutine exited cleanly.")
			}
		case <-time.After(1 * time.Second):
			t.Logf("Timeout waiting for peer server goroutine to exit.")
		}
	}
	return actualAddr, cleanupFunc
}

func TestNetworkManager_SendRequestVote(t *testing.T) {
	nm1, _, _, clock1 := setupDefaultNetworkManager(t, "node1", "localhost:0", nil)
	err := nm1.Start()
	testutil.AssertNoError(t, err)
	defer nm1.Stop()

	peerNodeID := types.NodeID("peer1")
	peerRPCHandler := &mockRPCHandler{}
	_, peerCleanup := setupPeerServer(t, "localhost:0", peerRPCHandler)
	defer peerCleanup()

	tempPeerListener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Cannot get ephemeral port for peer: %v", err)
	}
	peerActualAddr := tempPeerListener.Addr().String()
	tempPeerListener.Close()

	_, peerCleanupNew := setupPeerServer(t, peerActualAddr, peerRPCHandler)
	defer peerCleanupNew()

	nm1.mu.Lock()
	nm1.peers[peerNodeID] = PeerConfig{ID: peerNodeID, Address: peerActualAddr}
	nm1.mu.Unlock()

	defaultArgs := &types.RequestVoteArgs{Term: 1, CandidateID: "node1", LastLogIndex: 0, LastLogTerm: 0}

	tests := []struct {
		name        string
		setupPeer   func()
		args        *types.RequestVoteArgs
		expectReply *types.RequestVoteReply
		expectErr   error
		targetNode  types.NodeID
	}{
		{
			name: "Success",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.requestVoteFunc = func(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
					return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
				}
			},
			args:        defaultArgs,
			expectReply: &types.RequestVoteReply{Term: 1, VoteGranted: true},
			expectErr:   nil,
			targetNode:  peerNodeID,
		},
		{
			name: "RPC Error - Unavailable",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.SetRequestVoteError(status.Error(codes.Unavailable, "peer shut down"))
			},
			args:        defaultArgs,
			expectReply: nil,
			expectErr:   status.Error(codes.Unavailable, "peer shut down"),
			targetNode:  peerNodeID,
		},
		{
			name: "RPC Error - Timeout",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.requestVoteFunc = func(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
					select {
					case <-time.After(300 * time.Millisecond):
						return &types.RequestVoteReply{Term: args.Term, VoteGranted: false}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			},
			args:        defaultArgs,
			expectReply: nil,
			expectErr:   ErrTimeout,
			targetNode:  peerNodeID,
		},
		{
			name: "Target node not in config",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
			},
			args:        defaultArgs,
			expectReply: nil,
			expectErr:   ErrPeerNotFound,
			targetNode:  "unknownNode",
		},
		{
			name: "NM Shutdown",
			setupPeer: func() {
				nm1.isShutdown.Store(true)
			},
			args:        defaultArgs,
			expectReply: nil,
			expectErr:   ErrShuttingDown,
			targetNode:  peerNodeID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalShutdownState := nm1.isShutdown.Load()
			if tc.name == "NM Shutdown" {
				nm1.isShutdown.Store(true)
				defer nm1.isShutdown.Store(originalShutdownState)
			} else {
				nm1.isShutdown.Store(false)
			}

			if tc.setupPeer != nil {
				tc.setupPeer()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()

			if errors.Is(tc.expectErr, ErrTimeout) {
				go func() {
					time.Sleep(50 * time.Millisecond)
					clock1.Advance(200 * time.Millisecond)
				}()
			}

			reply, err := nm1.SendRequestVote(ctx, tc.targetNode, tc.args)

			if tc.expectErr != nil {
				testutil.AssertError(t, err, "Expected an error for %s", tc.name)
				if !errors.Is(err, tc.expectErr) {
					s, ok := status.FromError(tc.expectErr)
					if ok {
						sActual, okActual := status.FromError(err)
						if !okActual || s.Code() != sActual.Code() {
							t.Errorf("[%s] Expected error %v (or code %s), got %v (or code %s)", tc.name, tc.expectErr, s.Code(), err, sActual.Code())
						}
					} else {
						t.Errorf("[%s] Expected error type %T, got %T (%v)", tc.name, tc.expectErr, err, err)
					}
				}
			} else {
				testutil.AssertNoError(t, err, "Unexpected error for %s: %v", tc.name, err)
			}

			if tc.expectReply != nil {
				testutil.AssertEqual(t, tc.expectReply.Term, reply.Term, "[%s] Reply Term mismatch", tc.name)
				testutil.AssertEqual(t, tc.expectReply.VoteGranted, reply.VoteGranted, "[%s] Reply VoteGranted mismatch", tc.name)
			} else if err == nil && reply != nil {
			}
		})
	}
}

func TestNetworkManager_SendAppendEntries(t *testing.T) {
	nm1, _, _, _ := setupDefaultNetworkManager(t, "node1-ae", "localhost:0", nil)
	err := nm1.Start()
	testutil.AssertNoError(t, err)
	defer nm1.Stop()

	peerNodeID := types.NodeID("peer1-ae")
	peerRPCHandler := &mockRPCHandler{}
	peerActualAddr, peerCleanup := setupPeerServer(t, "localhost:0", peerRPCHandler)
	defer peerCleanup()

	nm1.mu.Lock()
	nm1.peers[peerNodeID] = PeerConfig{ID: peerNodeID, Address: peerActualAddr}
	nm1.mu.Unlock()

	baseArgs := &types.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "node1-ae",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	}

	tests := []struct {
		name        string
		setupPeer   func()
		args        *types.AppendEntriesArgs
		expectReply *types.AppendEntriesReply
		expectErr   error
		targetNode  types.NodeID
	}{
		{
			name: "SuccessHeartbeat",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.appendEntriesFunc = func(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
					return &types.AppendEntriesReply{Term: args.Term, Success: true, MatchIndex: args.PrevLogIndex}, nil
				}
			},
			args:        func() *types.AppendEntriesArgs { a := *baseArgs; a.Entries = []types.LogEntry{}; return &a }(),
			expectReply: &types.AppendEntriesReply{Term: 1, Success: true, MatchIndex: 0},
			expectErr:   nil,
			targetNode:  peerNodeID,
		},
		{
			name: "SuccessWithEntries",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.appendEntriesFunc = func(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
					matchIdx := args.PrevLogIndex + types.Index(len(args.Entries))
					return &types.AppendEntriesReply{Term: args.Term, Success: true, MatchIndex: matchIdx}, nil
				}
			},
			args: func() *types.AppendEntriesArgs {
				a := *baseArgs
				a.Entries = []types.LogEntry{{Term: 1, Index: 1, Command: []byte("cmd1")}}
				return &a
			}(),
			expectReply: &types.AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1},
			expectErr:   nil,
			targetNode:  peerNodeID,
		},
		{
			name: "RPC Error - Unavailable",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.SetAppendEntriesError(status.Error(codes.Unavailable, "peer is unavailable"))
			},
			args:        func() *types.AppendEntriesArgs { a := *baseArgs; return &a }(),
			expectReply: nil,
			// This relies on grpcServerHandler propagating the gRPC error correctly
			expectErr:  status.Error(codes.Unavailable, "peer is unavailable"),
			targetNode: peerNodeID,
		},
		{
			name: "RPC Error - Timeout (client-side)",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.appendEntriesFunc = func(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
					select {
					case <-time.After(300 * time.Millisecond): // Longer than client context timeout
						return &types.AppendEntriesReply{Term: args.Term, Success: false}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			},
			args:        func() *types.AppendEntriesArgs { a := *baseArgs; return &a }(),
			expectReply: nil,
			expectErr:   ErrTimeout, // formatGRPCError should convert context.DeadlineExceeded
			targetNode:  peerNodeID,
		},
		{
			name:        "PeerNotFound",
			setupPeer:   func() { peerRPCHandler.ResetErrors() },
			args:        func() *types.AppendEntriesArgs { a := *baseArgs; return &a }(),
			expectReply: nil,
			expectErr:   ErrPeerNotFound,
			targetNode:  "unknownPeerAE",
		},
		{
			name: "NM Shutdown",
			setupPeer: func() {
				nm1.isShutdown.Store(true) // Shutdown the sender
			},
			args:        func() *types.AppendEntriesArgs { a := *baseArgs; return &a }(),
			expectReply: nil,
			expectErr:   ErrShuttingDown,
			targetNode:  peerNodeID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalShutdownState := nm1.isShutdown.Load()
			if strings.Contains(tc.name, "NM Shutdown") {
				nm1.isShutdown.Store(true)
				defer nm1.isShutdown.Store(originalShutdownState)
			} else {
				nm1.isShutdown.Store(false)
			}

			tc.setupPeer()

			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()

			reply, err := nm1.SendAppendEntries(ctx, tc.targetNode, tc.args)

			if tc.expectErr != nil {
				testutil.AssertError(t, err, "[%s] Expected an error", tc.name)
				if !errors.Is(err, tc.expectErr) {
					sExpected, okExpected := status.FromError(tc.expectErr)
					sActual, okActual := status.FromError(err)
					if !(okExpected && okActual && sExpected.Code() == sActual.Code()) {
						t.Errorf("[%s] Expected error %v (or code %v), got %v (or code %v)", tc.name, tc.expectErr, sExpected.Code(), err, sActual.Code())
					}
				}
			} else {
				testutil.AssertNoError(t, err, "[%s] Unexpected error: %v", tc.name, err)
			}

			if tc.expectReply != nil {
				testutil.AssertNotNil(t, reply, "[%s] Expected a reply", tc.name)
				if reply != nil { // Guard against nil panic if AssertNotNil fails
					testutil.AssertEqual(t, tc.expectReply.Term, reply.Term, "[%s] Reply Term mismatch", tc.name)
					testutil.AssertEqual(t, tc.expectReply.Success, reply.Success, "[%s] Reply Success mismatch", tc.name)
					if tc.expectReply.Success {
						testutil.AssertEqual(t, tc.expectReply.MatchIndex, reply.MatchIndex, "[%s] Reply MatchIndex mismatch", tc.name)
					}
				}
			}
		})
	}
}

func TestNetworkManager_SendInstallSnapshot(t *testing.T) {
	nm1, _, _, _ := setupDefaultNetworkManager(t, "node1-is", "localhost:0", nil)
	err := nm1.Start()
	testutil.AssertNoError(t, err)
	defer nm1.Stop()

	peerNodeID := types.NodeID("peer1-is")
	peerRPCHandler := &mockRPCHandler{}
	peerActualAddr, peerCleanup := setupPeerServer(t, "localhost:0", peerRPCHandler)
	defer peerCleanup()

	nm1.mu.Lock()
	nm1.peers[peerNodeID] = PeerConfig{ID: peerNodeID, Address: peerActualAddr}
	nm1.mu.Unlock()

	defaultSnapshotArgs := &types.InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node1-is",
		LastIncludedIndex: 10,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot_data"),
	}

	tests := []struct {
		name        string
		setupPeer   func()
		args        *types.InstallSnapshotArgs
		expectReply *types.InstallSnapshotReply
		expectErr   error
		targetNode  types.NodeID
	}{
		{
			name: "Success",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.installSnapshotFunc = func(ctx context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
					return &types.InstallSnapshotReply{Term: args.Term}, nil
				}
			},
			args:        defaultSnapshotArgs,
			expectReply: &types.InstallSnapshotReply{Term: 1},
			expectErr:   nil,
			targetNode:  peerNodeID,
		},
		{
			name: "RPC Error - Unavailable",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.SetInstallSnapshotError(status.Error(codes.Unavailable, "peer unavailable for snapshot"))
			},
			args:        defaultSnapshotArgs,
			expectReply: nil,
			expectErr:   status.Error(codes.Unavailable, "peer unavailable for snapshot"),
			targetNode:  peerNodeID,
		},
		{
			name: "RPC Error - Timeout (client-side)",
			setupPeer: func() {
				peerRPCHandler.ResetErrors()
				peerRPCHandler.installSnapshotFunc = func(ctx context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
					select {
					case <-time.After(300 * time.Millisecond): // Longer than client context timeout
						return &types.InstallSnapshotReply{Term: args.Term}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			},
			args:        defaultSnapshotArgs,
			expectReply: nil,
			expectErr:   ErrTimeout,
			targetNode:  peerNodeID,
		},
		{
			name:        "PeerNotFound",
			setupPeer:   func() { peerRPCHandler.ResetErrors() },
			args:        defaultSnapshotArgs,
			expectReply: nil,
			expectErr:   ErrPeerNotFound,
			targetNode:  "unknownPeerIS",
		},
		{
			name: "NM Shutdown",
			setupPeer: func() {
				nm1.isShutdown.Store(true) // Shutdown the sender
			},
			args:        defaultSnapshotArgs,
			expectReply: nil,
			expectErr:   ErrShuttingDown,
			targetNode:  peerNodeID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalShutdownState := nm1.isShutdown.Load()
			if strings.Contains(tc.name, "NM Shutdown") {
				nm1.isShutdown.Store(true)
				defer nm1.isShutdown.Store(originalShutdownState)
			} else {
				nm1.isShutdown.Store(false)
			}

			tc.setupPeer()

			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()

			reply, err := nm1.SendInstallSnapshot(ctx, tc.targetNode, tc.args)

			if tc.expectErr != nil {
				testutil.AssertError(t, err, "[%s] Expected an error", tc.name)
				if !errors.Is(err, tc.expectErr) {
					sExpected, okExpected := status.FromError(tc.expectErr)
					sActual, okActual := status.FromError(err)
					if !(okExpected && okActual && sExpected.Code() == sActual.Code()) {
						t.Errorf("[%s] Expected error %v (or code %v), got %v (or code %v)", tc.name, tc.expectErr, sExpected.Code(), err, sActual.Code())
					}
				}
			} else {
				testutil.AssertNoError(t, err, "[%s] Unexpected error: %v", tc.name, err)
			}

			if tc.expectReply != nil {
				testutil.AssertNotNil(t, reply, "[%s] Expected a reply", tc.name)
				if reply != nil {
					testutil.AssertEqual(t, tc.expectReply.Term, reply.Term, "[%s] Reply Term mismatch", tc.name)
				}
			}
		})
	}
}

func TestNetworkManager_PeerStatus(t *testing.T) {
	nm1, _, _, _ := setupDefaultNetworkManager(t, "node1", "localhost:0", nil)
	err := nm1.Start()
	testutil.AssertNoError(t, err)
	defer nm1.Stop()

	peerNodeID := types.NodeID("peer1_status")
	peerRPCHandler := &mockRPCHandler{}

	tempPeerListener, _ := net.Listen("tcp", "localhost:0")
	peerActualAddr := tempPeerListener.Addr().String()
	tempPeerListener.Close()
	_, peerCleanup := setupPeerServer(t, peerActualAddr, peerRPCHandler)
	defer peerCleanup()

	nm1.mu.Lock()
	nm1.peers[peerNodeID] = PeerConfig{ID: peerNodeID, Address: peerActualAddr}
	nm1.mu.Unlock()

	_, _ = nm1.SendRequestVote(context.Background(), peerNodeID, &types.RequestVoteArgs{Term: 1}) // Make a call to establish connection state

	status, err := nm1.PeerStatus(peerNodeID)
	testutil.AssertNoError(t, err)
	testutil.AssertTrue(t, status.Connected, "Expected peer to be connected")
	testutil.AssertNoError(t, status.LastError)

	nm1.mu.Lock()

	if pc, exists := nm1.peerClients[peerNodeID]; exists {
		pc.connected.Store(false)
		pc.lastError = errors.New("simulated connection failure")
	} else {
		t.Logf("Peer %s not found in peerClients for direct error simulation, might affect test accuracy", peerNodeID)
	}
	nm1.mu.Unlock()

	status, err = nm1.PeerStatus(peerNodeID)
	testutil.AssertNoError(t, err)
	testutil.AssertFalse(t, status.Connected, "Expected peer to be not connected after simulated failure")
	testutil.AssertError(t, status.LastError, "Expected lastError to be set")
	testutil.AssertContains(t, status.LastError.Error(), "simulated connection failure")

	status, err = nm1.PeerStatus("unknownPeer")
	testutil.AssertErrorIs(t, err, ErrPeerNotFound)
	testutil.AssertFalse(t, status.Connected)

	nm1.isShutdown.Store(true)
	status, err = nm1.PeerStatus(peerNodeID)
	testutil.AssertErrorIs(t, err, ErrShuttingDown)
	nm1.isShutdown.Store(false)
}

func TestNetworkManager_LocalAddr(t *testing.T) {
	nm, _, _, _ := setupDefaultNetworkManager(t, "node1", "localhost:0", nil)

	addr := nm.LocalAddr()
	isLocalhostAddr := (strings.HasPrefix(addr, "127.0.0.1:") || strings.HasPrefix(addr, "[::1]:") || strings.HasPrefix(addr, "localhost:"))
	testutil.AssertTrue(t, addr != "" && isLocalhostAddr, "Expected a localhost-like address (e.g., 127.0.0.1:port, [::1]:port, or localhost:port) before start, got: %s", addr)

	err := nm.Start()
	testutil.AssertNoError(t, err)

	addrAfterStart := nm.LocalAddr()
	testutil.AssertTrue(t, addrAfterStart != "", "Expected non-empty local address after start")
	testutil.AssertTrue(t, strings.HasPrefix(addrAfterStart, "127.0.0.1:") || strings.HasPrefix(addrAfterStart, "[::1]:") || strings.Contains(addrAfterStart, "localhost:"), "Expected localhost address, got %s", addrAfterStart)

	originalListenerAddr := nm.listener.Addr().String()
	testutil.AssertEqual(t, originalListenerAddr, addrAfterStart, "LocalAddr should return listener's address after start")

	nm.Stop()

	addrAfterStop := nm.LocalAddr()

	testutil.AssertEqual(t, originalListenerAddr, addrAfterStop, "LocalAddr should still return originally resolved address after stop, got %s", addrAfterStop)
}
