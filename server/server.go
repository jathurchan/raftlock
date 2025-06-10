package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"maps"

	"github.com/jathurchan/raftlock/lock"
	"github.com/jathurchan/raftlock/logger"
	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// raftLockServer implements the RaftLockServer interface and the gRPC service.
type raftLockServer struct {
	pb.UnimplementedRaftLockServer // Embeds unimplemented gRPC service for forward compatibility.

	config RaftLockServerConfig

	raftNode    raft.Raft
	lockManager lock.LockManager
	storage     storage.Storage
	networkMgr  raft.NetworkManager

	grpcServer *grpc.Server // gRPC server instance.
	listener   net.Listener // Network listener for accepting client connections.

	proposalTracker ProposalTracker
	validator       RequestValidator
	connManager     ConnectionManager
	rateLimiter     RateLimiter

	state           atomic.Value  // ServerOperationalState: current operational state of the server.
	isLeader        atomic.Bool   // Indicates if this node is currently the Raft leader.
	currentLeaderID atomic.Value  // types.NodeID: ID of the current Raft leader.
	lastKnownTerm   atomic.Uint64 // Most recently observed Raft term.
	lastCommitIndex atomic.Uint64 // Index of the last committed log entry.

	stopCh          chan struct{}  // Channel used to signal server shutdown.
	taskWg          sync.WaitGroup // WaitGroup to track and await shutdown of background tasks.
	shutdownStarted atomic.Bool    // Indicates if the shutdown process has begun.

	logger     logger.Logger
	metrics    ServerMetrics
	clock      raft.Clock
	serializer lock.Serializer

	activeRequests   atomic.Int64  // Tracks the number of active requests being handled.
	requestSemaphore chan struct{} // Semaphore to limit concurrent request processing.

	serverStartTime time.Time // Timestamp of when the server instance was created
}

// NewRaftLockServer creates a new RaftLockServer instance using the provided configuration.
func NewRaftLockServer(config RaftLockServerConfig) (RaftLockServer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid server configuration: %w", err)
	}

	lg := config.Logger
	if lg == nil {
		lg = logger.NewNoOpLogger()
	}
	mt := config.Metrics
	if mt == nil {
		mt = NewNoOpServerMetrics()
	}
	clk := config.Clock
	if clk == nil {
		clk = raft.NewStandardClock()
	}
	sz := config.Serializer
	if sz == nil {
		sz = &lock.JSONSerializer{}
	}
	server := &raftLockServer{
		config:           config,
		stopCh:           make(chan struct{}),
		logger:           lg.WithComponent("server").WithNodeID(config.NodeID),
		metrics:          mt,
		clock:            clk,
		serializer:       sz,
		requestSemaphore: make(chan struct{}, config.MaxConcurrentReqs),
		serverStartTime:  clk.Now(),
	}

	server.state.Store(ServerStateStarting)
	server.currentLeaderID.Store(types.NodeID(""))

	server.validator = NewRequestValidator(server.logger)
	server.connManager = NewConnectionManager(server.metrics, server.logger, server.clock)

	if config.EnableRateLimit {
		server.rateLimiter = NewTokenBucketRateLimiter(
			config.RateLimit,
			config.RateLimitBurst,
			config.RateLimitWindow,
			server.logger,
		)
	}

	server.proposalTracker = NewProposalTracker(
		server.logger,
		WithMaxPendingAge(DefaultProposalMaxPendingAge),
		WithCleanupInterval(DefaultProposalCleanupInterval),
		WithClock(server.clock),
	)

	server.logger.Infow("RaftLockServer initialized",
		"listenAddress", config.ListenAddress,
		"peerCount", len(config.Peers)-1,
		"dataDir", config.DataDir)

	return server, nil
}

// Start initializes and starts all server components.
func (s *raftLockServer) Start(ctx context.Context) error {
	currentState := s.state.Load().(ServerOperationalState)
	if currentState != ServerStateStarting {
		if currentState == ServerStateRunning {
			s.logger.Warnw("Start called on already running server")
			return ErrServerAlreadyStarted
		}
		return fmt.Errorf("cannot start server in state %s", currentState)
	}

	s.logger.Infow("Starting RaftLockServer")
	startTime := s.clock.Now()

	if err := s.initializeStorage(); err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}

	if err := s.initializeLockManager(); err != nil {
		return fmt.Errorf("failed to initialize lock manager: %w", err)
	}

	if err := s.initializeRaftNode(); err != nil {
		return fmt.Errorf("failed to initialize Raft node: %w", err)
	}

	if err := s.initializeNetworkManager(); err != nil {
		return fmt.Errorf("failed to initialize network manager: %w", err)
	}
	s.raftNode.SetNetworkManager(s.networkMgr)

	if err := s.raftNode.Start(); err != nil {
		s.cleanup()
		return fmt.Errorf("failed to start Raft node: %w", err)
	}

	if err := s.initializeAndStartGRPCServer(); err != nil {
		s.cleanup()
		return fmt.Errorf("failed to initialize gRPC server: %w", err)
	}

	s.startBackgroundTasks()
	s.state.Store(ServerStateRunning)

	startupDuration := s.clock.Since(startTime)
	s.logger.Infow("RaftLockServer started successfully",
		"listenAddress", s.listener.Addr().String(),
		"startupDuration", startupDuration)

	return nil
}

// initializeStorage sets up the persistent storage backend.
func (s *raftLockServer) initializeStorage() error {
	s.logger.Infow("Initializing storage", "dataDir", s.config.DataDir)

	storageCfg := storage.StorageConfig{
		Dir: s.config.DataDir,
	}

	options := storage.DefaultFileStorageOptions()
	options.AutoTruncateOnSnapshot = true

	st, err := storage.NewFileStorageWithOptions(storageCfg, options, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	s.storage = st
	s.logger.Infow("Storage initialized")
	return nil
}

// initializeLockManager sets up the lock manager.
func (s *raftLockServer) initializeLockManager() error {
	s.logger.Infow("Initializing lock manager")

	s.lockManager = lock.NewLockManager(
		lock.WithLogger(s.logger),
		lock.WithMetrics(lock.NewNoOpMetrics()),
		lock.WithClock(s.clock),
		lock.WithDefaultTTL(lock.DefaultLockTTL),
		lock.WithMaxTTL(MaxLockTTL),
		lock.WithMaxWaiters(lock.DefaultMaxWaiters),
		lock.WithTickInterval(DefaultLockManagerTickInterval),
		lock.WithMaxWaiters(lock.DefaultMaxWaiters),
		lock.WithPriorityQueue(true),
		lock.WithSerializer(&lock.JSONSerializer{}),
		lock.WithCache(true),
		lock.WithCacheTTL(lock.DefaultCacheTTL),
		lock.WithCacheSize(lock.DefaultCacheSize),
		lock.WithMaxLocks(lock.DefaultMaxLocks),
	)

	s.logger.Infow("Lock manager initialized")
	return nil
}

// initializeRaftNode sets up and builds the Raft node with all necessary dependencies.
func (s *raftLockServer) initializeRaftNode() error {
	s.logger.Infow("Initializing Raft node")

	raftCfg := s.config.RaftConfig
	if raftCfg.ID == "" {
		raftCfg.ID = s.config.NodeID
	}
	if len(raftCfg.Peers) == 0 {
		raftCfg.Peers = s.config.Peers
	}

	raftBuilder := raft.NewRaftBuilder().
		WithConfig(raftCfg).
		WithApplier(s.lockManager).
		WithLogger(s.logger).
		WithMetrics(raft.NewNoOpMetrics()).
		WithClock(s.clock).
		WithRand(raft.NewStandardRand()).
		WithStorage(s.storage)

	var err error
	s.raftNode, err = raftBuilder.Build()
	if err != nil {
		return fmt.Errorf("failed to build Raft node: %w", err)
	}

	s.logger.Infow("Raft node initialized")
	return nil
}

// initializeNetworkManager sets up the network manager used for peer communication.
func (s *raftLockServer) initializeNetworkManager() error {
	s.logger.Infow("Initializing Raft network manager")

	if s.raftNode == nil {
		return fmt.Errorf("RaftNode must be initialized before NetworkManager")
	}

	peerCfg, ok := s.config.Peers[s.config.NodeID]
	if !ok || peerCfg.Address == "" {
		return fmt.Errorf("missing or invalid peer config for NodeID %q", s.config.NodeID)
	}

	options := raft.DefaultGRPCNetworkManagerOptions()
	options.MaxRecvMsgSize = raft.DefaultMaxRecvMsgSize

	nm, err := raft.NewGRPCNetworkManager(
		s.config.NodeID,
		peerCfg.Address,
		s.config.Peers,
		s.raftNode,
		&s.shutdownStarted,
		s.logger,
		raft.NewNoOpMetrics(),
		s.clock,
		options,
	)
	if err != nil {
		return fmt.Errorf("failed to create Raft NetworkManager: %w", err)
	}

	s.networkMgr = nm
	s.logger.Infow("Raft NetworkManager initialized", "listenAddress", peerCfg.Address)
	return nil
}

// initializeAndStartGRPCServer sets up the gRPC server and begins serving requests.
func (s *raftLockServer) initializeAndStartGRPCServer() error {
	s.logger.Infow("Initializing gRPC server", "address", s.config.ListenAddress)

	var err error
	s.listener, err = net.Listen("tcp", s.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.ListenAddress, err)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.config.MaxRequestSize),
		grpc.MaxSendMsgSize(s.config.MaxResponseSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    DefaultGRPCKeepaliveTime,
			Timeout: DefaultGRPCKeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             DefaultGRPCMinClientPingInterval,
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(s.unaryInterceptor),
	}

	s.grpcServer = grpc.NewServer(opts...)
	pb.RegisterRaftLockServer(s.grpcServer, s)

	s.taskWg.Add(1)
	go func() {
		defer s.taskWg.Done()
		s.logger.Infow("gRPC server listening", "address", s.listener.Addr().String())

		if err := s.grpcServer.Serve(s.listener); err != nil &&
			!errors.Is(err, grpc.ErrServerStopped) &&
			!s.shutdownStarted.Load() {
			s.logger.Errorw("gRPC server error", "error", err)
		} else {
			s.logger.Infow("gRPC server stopped")
		}
	}()

	return nil
}

// unaryInterceptor applies common server behavior for all unary gRPC requests:
// state checks, rate limiting, concurrency control, connection tracking, and metrics.
func (s *raftLockServer) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	startTime := s.clock.Now()
	method := extractMethodName(info.FullMethod)

	s.trackConcurrentRequestStart(method)
	defer s.trackConcurrentRequestEnd(method)

	if err := s.checkServerState(); err != nil {
		s.metrics.IncrServerError(method, ErrorTypeRaftUnavailable)
		return nil, s.errorToGRPCStatus(err)
	}

	if err := s.applyRateLimitIfEnabled(ctx, method); err != nil {
		return nil, err
	}

	if err := s.acquireRequestSlot(ctx, method); err != nil {
		return nil, err
	}
	defer s.releaseRequestSlot()

	s.trackClientConnection(ctx)

	ctx = s.applyRequestTimeout(ctx)
	resp, err := handler(ctx, req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = status.Error(codes.DeadlineExceeded, "request timeout")
		} else if errors.Is(err, context.Canceled) {
			err = status.Error(codes.Canceled, "request canceled")
		}
	}
	s.observeRequestOutcome(method, startTime, err)

	return resp, err
}

// trackConcurrentRequestStart increments request counters for active tracking.
func (s *raftLockServer) trackConcurrentRequestStart(method string) {
	s.activeRequests.Add(1)
	s.metrics.IncrConcurrentRequests(method, 1)
}

// trackConcurrentRequestEnd decrements counters for active request tracking.
func (s *raftLockServer) trackConcurrentRequestEnd(method string) {
	s.activeRequests.Add(-1)
	s.metrics.IncrConcurrentRequests(method, -1)
}

// applyRateLimitIfEnabled checks and enforces rate limiting.
func (s *raftLockServer) applyRateLimitIfEnabled(ctx context.Context, method string) error {
	if s.rateLimiter != nil && !s.rateLimiter.Allow() {
		s.metrics.IncrServerError(method, ErrorTypeRateLimit)
		s.logger.Debugw("Request rate limited", "method", method, "remote", s.getRemoteAddr(ctx))
		return status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}
	return nil
}

// acquireRequestSlot blocks until a request slot is available or context is cancelled.
func (s *raftLockServer) acquireRequestSlot(ctx context.Context, method string) error {
	select {
	case s.requestSemaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		s.metrics.IncrServerError(method, ErrorTypeTimeout)
		return status.Error(codes.DeadlineExceeded, "request timeout")
	}
}

// releaseRequestSlot releases a previously acquired concurrency slot.
func (s *raftLockServer) releaseRequestSlot() {
	<-s.requestSemaphore
}

// trackClientConnection logs request origin for usage tracking or rate limiting.
func (s *raftLockServer) trackClientConnection(ctx context.Context) {
	if remote := s.getRemoteAddr(ctx); remote != "" {
		s.connManager.OnRequest(remote)
	}
}

// applyRequestTimeout wraps the context with a timeout if configured.
func (s *raftLockServer) applyRequestTimeout(ctx context.Context) context.Context {
	if s.config.RequestTimeout <= 0 {
		return ctx
	}
	newCtx, cancel := context.WithTimeout(ctx, s.config.RequestTimeout)
	go func() {
		<-newCtx.Done()
		cancel()
	}()
	return newCtx
}

// observeRequestOutcome logs and reports metrics about the request.
func (s *raftLockServer) observeRequestOutcome(method string, start time.Time, err error) {
	latency := s.clock.Since(start)
	s.metrics.ObserveRequestLatency(method, latency)
	s.metrics.IncrGRPCRequest(method, err == nil)

	if err != nil {
		s.logger.Debugw("Request failed", "method", method, "error", err, "latency", latency)
	} else {
		s.logger.Debugw("Request completed", "method", method, "latency", latency)
	}
}

// extractMethodName parses the gRPC method name from its fully-qualified path.
// For example, "/package.Service/Method" → "Method".
func extractMethodName(fullMethod string) string {
	parts := strings.Split(fullMethod, "/")
	if len(parts) >= 2 {
		return parts[len(parts)-1]
	}
	return fullMethod
}

// checkServerState returns an error if the server is not in a running state.
// It prevents requests from being processed during startup or shutdown.
func (s *raftLockServer) checkServerState() error {
	currentState := s.state.Load().(ServerOperationalState)
	switch currentState {
	case ServerStateStarting:
		return ErrServerNotStarted
	case ServerStateStopping, ServerStateStopped:
		return ErrServerStopped
	case ServerStateRunning:
		return nil
	default:
		return fmt.Errorf("unknown server state: %s", currentState)
	}
}

// getRemoteAddr extracts the remote peer address from the request context.
// Returns an empty string if unavailable.
func (s *raftLockServer) getRemoteAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return ""
}

// errorToGRPCStatus converts a server-side error into a gRPC-compatible status error.
// It maps domain-specific error codes to appropriate gRPC codes.
func (s *raftLockServer) errorToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}

	protoError := ErrorToProtoError(err)
	if protoError == nil {
		return status.Error(codes.Internal, "internal server error")
	}

	var grpcCode codes.Code
	switch protoError.Code {
	case pb.ErrorCode_INVALID_ARGUMENT:
		grpcCode = codes.InvalidArgument
	case pb.ErrorCode_LOCK_HELD:
		grpcCode = codes.ResourceExhausted
	case pb.ErrorCode_LOCK_NOT_FOUND:
		grpcCode = codes.NotFound
	case pb.ErrorCode_NOT_LEADER:
		grpcCode = codes.FailedPrecondition
	case pb.ErrorCode_NO_LEADER:
		grpcCode = codes.Unavailable
	case pb.ErrorCode_UNAVAILABLE:
		grpcCode = codes.Unavailable
	case pb.ErrorCode_RATE_LIMITED:
		grpcCode = codes.ResourceExhausted
	case pb.ErrorCode_TIMEOUT:
		grpcCode = codes.DeadlineExceeded
	case pb.ErrorCode_INTERNAL_ERROR: // Fallback
		grpcCode = codes.Internal
	default: // Default for any unmapped pb.ErrorCodes
		s.logger.Warnw("Unmapped pb.ErrorCode to gRPC code", "pbErrorCode", protoError.Code.String())
		grpcCode = codes.Internal
	}

	st := status.New(grpcCode, protoError.Message)

	if len(protoError.Details) > 0 {
		st, _ = st.WithDetails(&pb.ErrorDetail{
			Code:    protoError.Code,
			Message: protoError.Message,
			Details: protoError.Details,
		})
	}

	return st.Err()
}

// cleanup gracefully shuts down all server components during shutdown or failure recovery.
// It ensures each subsystem is stopped in the proper order, logging any errors encountered.
func (s *raftLockServer) cleanup() {
	if s.raftNode != nil {
		raftCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		if err := s.raftNode.Stop(raftCtx); err != nil {
			s.logger.Warnw("Error stopping Raft node", "error", err)
		}
		cancel()
	}

	if s.networkMgr != nil {
		if err := s.networkMgr.Stop(); err != nil {
			s.logger.Warnw("Error stopping network manager", "error", err)
		}
	}

	if s.proposalTracker != nil {
		if err := s.proposalTracker.Close(); err != nil {
			s.logger.Warnw("Error closing proposal tracker", "error", err)
		}
	}

	if s.lockManager != nil {
		if err := s.lockManager.Close(); err != nil {
			s.logger.Warnw("Error closing lock manager", "error", err)
		}
	}

	if s.storage != nil {
		if err := s.storage.Close(); err != nil {
			s.logger.Warnw("Error closing storage", "error", err)
		}
	}

	if s.listener != nil { // Close the gRPC listener (client-facing endpoint)
		if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.logger.Warnw("Error closing listener", "error", err)
		}
	}
}

// startBackgroundTasks launches all long-running goroutines required by the server.
// Each task is tracked in the WaitGroup and exits cleanly on server shutdown.
func (s *raftLockServer) startBackgroundTasks() {
	s.runBackgroundTask("Raft tick loop", s.runRaftTickLoop)
	s.runBackgroundTask("Lock manager tick loop", s.runLockManagerTickLoop)
	s.runBackgroundTask("Leader change monitor", s.runLeaderChangeMonitor)
	s.runBackgroundTask("Apply message processor", s.runApplyMessageProcessor)
	s.runBackgroundTask("Health check loop", s.runHealthCheckLoop)

	s.logger.Infow("Background tasks started")
}

// runBackgroundTask wraps a background task with WaitGroup tracking and logging.
func (s *raftLockServer) runBackgroundTask(name string, fn func()) {
	s.taskWg.Add(1)
	go func() {
		defer s.taskWg.Done()
		s.logger.Infow(name + " started")
		fn()
		s.logger.Infow(name + " stopped")
	}()
}

// runRaftTickLoop periodically invokes Raft's Tick method to drive elections and heartbeats.
func (s *raftLockServer) runRaftTickLoop() {
	ticker := s.clock.NewTicker(DefaultRaftTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRaftTickInterval/2)
			if s.raftNode != nil {
				s.raftNode.Tick(ctx)
			}
			cancel()
		case <-s.stopCh:
			return
		}
	}
}

// runLockManagerTickLoop periodically calls the LockManager's Tick to expire stale locks.
func (s *raftLockServer) runLockManagerTickLoop() {
	ticker := s.clock.NewTicker(DefaultLockManagerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			ctx, cancel := context.WithTimeout(context.Background(), DefaultLockManagerTickInterval/2)
			if s.lockManager != nil {
				expired := s.lockManager.Tick(ctx)
				if expired > 0 {
					s.logger.Debugw("Lock manager tick completed", "expiredLocks", expired)
				}
			}
			cancel()
		case <-s.stopCh:
			return
		}
	}
}

// runLeaderChangeMonitor watches Raft's leader change channel and reacts to updates.
func (s *raftLockServer) runLeaderChangeMonitor() {
	if s.raftNode == nil {
		s.logger.Errorw("Leader change monitor: RaftNode is nil")
		return
	}

	leaderCh := s.raftNode.LeaderChangeChannel()
	if leaderCh == nil {
		s.logger.Errorw("Leader change monitor: LeaderChangeChannel is nil")
		return
	}

	for {
		select {
		case newLeaderID, ok := <-leaderCh:
			if !ok {
				s.handleLeaderChange(types.NodeID(""))
				return
			}
			s.handleLeaderChange(newLeaderID)
		case <-s.stopCh:
			return
		}
	}
}

// handleLeaderChange updates the server's leadership state when a new Raft leader is elected.
// It adjusts internal flags, logs the change, and updates metrics accordingly.
func (s *raftLockServer) handleLeaderChange(newLeaderID types.NodeID) {
	oldLeaderID := s.currentLeaderID.Swap(newLeaderID).(types.NodeID)

	wasLeader := s.isLeader.Load()
	isNowLeader := newLeaderID == s.config.NodeID
	s.isLeader.Store(isNowLeader)

	// Only log and update metrics if leadership actually changed.
	if wasLeader != isNowLeader || oldLeaderID != newLeaderID {
		s.logger.Infow("Leadership change detected",
			"oldLeader", oldLeaderID,
			"newLeader", newLeaderID,
			"wasLeader", wasLeader,
			"isNowLeader", isNowLeader)

		term, _ := s.raftNode.GetState()
		s.metrics.SetRaftTerm(term)
		s.metrics.SetServerState(isNowLeader, s.isHealthy())
	}
}

// isHealthy reports whether the server is in a running state and able to serve requests.
func (s *raftLockServer) isHealthy() bool {
	currentState := s.state.Load().(ServerOperationalState)
	return currentState == ServerStateRunning
}

// runApplyMessageProcessor processes committed Raft entries and resolves proposals.
func (s *raftLockServer) runApplyMessageProcessor() {
	if s.raftNode == nil {
		s.logger.Errorw("Apply message processor: RaftNode is nil")
		return
	}

	applyCh := s.raftNode.ApplyChannel()
	if applyCh == nil {
		s.logger.Errorw("Apply message processor: ApplyChannel is nil")
		return
	}
	s.logger.Infow("Apply message processor started")

	for {
		select {
		case msg, ok := <-applyCh:
			if !ok {
				s.logger.Infow("Apply channel closed, processor stopping.")
				return
			}
			s.handleApplyMessage(msg)
		case <-s.stopCh:
			s.logger.Infow("Apply message processor stopped")
			return
		}
	}
}

// handleApplyMessage processes a committed and applied Raft log entry or snapshot.
//
// The lockManager has already applied the command internally (via Raft integration).
// This method is responsible for handling post-application coordination, such as
// proposal resolution, metric updates, and logging.
func (s *raftLockServer) handleApplyMessage(applyMsg types.ApplyMsg) {
	s.metrics.SetRaftCommitIndex(applyMsg.CommandIndex)

	switch {
	case applyMsg.CommandValid:
		s.lastCommitIndex.Store(uint64(applyMsg.CommandIndex))

		s.proposalTracker.HandleAppliedCommand(applyMsg)

		logFields := []any{
			"index", applyMsg.CommandIndex,
			"term", applyMsg.CommandTerm,
		}
		if applyMsg.CommandResultError != nil {
			logFields = append(logFields, "applyError", applyMsg.CommandResultError)
			s.logger.Warnw("Applied command from Raft resulted in an operational error", logFields...)
		} else {
			s.logger.Debugw("Applied command committed via Raft", logFields...)
		}

	case applyMsg.SnapshotValid:
		s.lastCommitIndex.Store(uint64(applyMsg.SnapshotIndex))

		s.proposalTracker.HandleSnapshotApplied(applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)

		s.logger.Infow("Applied snapshot committed via Raft",
			"index", applyMsg.SnapshotIndex,
			"term", applyMsg.SnapshotTerm)
	}
}

// runHealthCheckLoop periodically evaluates the server's health status.
func (s *raftLockServer) runHealthCheckLoop() {
	if s.config.HealthCheckInterval <= 0 {
		s.logger.Infow("Health check loop disabled")
		return
	}

	ticker := s.clock.NewTicker(s.config.HealthCheckInterval)
	defer ticker.Stop()

	s.logger.Infow("Health check loop started", "interval", s.config.HealthCheckInterval)

	for {
		select {
		case <-ticker.Chan():
			isHealthy := s.performHealthCheck()
			s.metrics.IncrHealthCheck(isHealthy)
			s.metrics.SetServerState(s.isLeader.Load(), isHealthy)
		case <-s.stopCh:
			s.logger.Infow("Health check loop stopped")
			return
		}
	}
}

// Stop gracefully shuts down the RaftLockServer, terminating all background tasks,
// closing resources, and stopping the gRPC server. This method is idempotent.
func (s *raftLockServer) Stop(ctx context.Context) error {
	if !s.shutdownStarted.CompareAndSwap(false, true) {
		s.logger.Infow("Stop already in progress or completed")
		return nil
	}

	s.logger.Infow("Initiating RaftLockServer shutdown")
	startTime := s.clock.Now()

	s.state.Store(ServerStateStopping)
	close(s.stopCh)

	if s.grpcServer != nil {
		s.logger.Infow("Shutting down gRPC server")
		s.grpcServer.GracefulStop()
	}

	bgTasksDone := make(chan struct{})
	go func() {
		s.taskWg.Wait()
		close(bgTasksDone)
	}()

	select {
	case <-bgTasksDone:
		s.logger.Infow("All background tasks shut down cleanly")
	case <-ctx.Done():
		s.logger.Warnw("Timeout waiting for background tasks; forcing shutdown")
		if s.grpcServer != nil {
			s.grpcServer.Stop() // Force immediate shutdown
		}
	}

	s.cleanup()
	s.state.Store(ServerStateStopped)

	duration := s.clock.Since(startTime)
	s.logger.Infow("RaftLockServer shutdown complete", "duration", duration)
	return nil
}

// IsLeader returns whether this node is currently the Raft leader.
func (s *raftLockServer) IsLeader() bool {
	return s.isLeader.Load()
}

// GetLeaderAddress returns the address of the current Raft leader.
func (s *raftLockServer) GetLeaderAddress() string {
	leaderID := s.currentLeaderID.Load().(types.NodeID)
	if leaderID == "" {
		return ""
	}

	if peerConfig, exists := s.config.Peers[leaderID]; exists {
		return peerConfig.Address
	}
	return ""
}

// GetNodeID returns the unique Raft node ID of this server.
func (s *raftLockServer) GetNodeID() string {
	return string(s.config.NodeID)
}

// Metrics returns the server metrics interface.
func (s *raftLockServer) Metrics() ServerMetrics {
	return s.metrics
}

// Acquire attempts to acquire a distributed lock via Raft consensus.
func (s *raftLockServer) Acquire(ctx context.Context, req *pb.AcquireRequest) (*pb.AcquireResponse, error) {
	s.logger.Debugw("Received Acquire request", "lockId", req.LockId, "clientId", req.ClientId)

	if err := s.validateAcquireRequest(req); err != nil {
		return nil, err
	}

	if err := s.requireLeader(MethodAcquire); err != nil {
		s.logger.Warnw("Not leader", "lockId", req.LockId, "clientId", req.ClientId, "error", err)
		return nil, s.errorToGRPCStatus(err)
	}

	ttl, err := s.resolveTTL(req)
	if err != nil {
		return nil, err
	}

	waitTimeout, err := s.resolveAcquireWaitTimeout(req)
	if err != nil {
		return nil, err
	}

	cmd := s.buildAcquireCommand(req, ttl, waitTimeout)

	result, err := s.submitAndProcessProposal(ctx, MethodAcquire, cmd, s.parseAcquireProposal)
	if err != nil {
		return s.handleAcquireError(err, req)
	}

	return s.buildAcquireResponse(result, req)
}

// validateAcquireRequest checks the format of the AcquireRequest.
func (s *raftLockServer) validateAcquireRequest(req *pb.AcquireRequest) error {
	if err := s.validator.ValidateAcquireRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodAcquire, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid Acquire request", "error", err, "lockId", req.LockId, "clientId", req.ClientId)
		return s.errorToGRPCStatus(err)
	}
	return nil
}

// resolveTTL parses and validates the TTL value from the request.
func (s *raftLockServer) resolveTTL(req *pb.AcquireRequest) (time.Duration, error) {
	ttl := lock.DefaultLockTTL

	if req.Ttl == nil || !req.Ttl.IsValid() {
		return ttl, nil
	}
	parsed := req.Ttl.AsDuration()

	if parsed <= 0 || parsed < lock.MinLockTTL || parsed > lock.MaxLockTTL {
		err := NewValidationError("ttl", parsed.String(), fmt.Sprintf(ErrMsgInvalidTTL, lock.MinLockTTL, lock.MaxLockTTL))
		s.metrics.IncrValidationError(MethodAcquire, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid TTL", "error", err, "lockId", req.LockId)
		return 0, s.errorToGRPCStatus(err)
	}
	return parsed, nil
}

// resolveAcquireWaitTimeout parses and validates the WaitTimeout field if Wait=true.
func (s *raftLockServer) resolveAcquireWaitTimeout(req *pb.AcquireRequest) (time.Duration, error) {
	if !req.Wait {
		return 0, nil
	}
	timeout := lock.DefaultWaitQueueTimeout

	if req.WaitTimeout == nil || !req.WaitTimeout.IsValid() {
		return timeout, nil
	}
	parsed := req.WaitTimeout.AsDuration()

	if parsed < lock.MinWaitQueueTimeout || parsed > lock.MaxWaitQueueTimeout {
		err := NewValidationError("wait_timeout", parsed.String(), fmt.Sprintf(ErrMsgInvalidTimeout, lock.MinWaitQueueTimeout, lock.MaxWaitQueueTimeout))
		s.metrics.IncrValidationError(MethodAcquire, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid WaitTimeout", "error", err, "lockId", req.LockId)
		return 0, s.errorToGRPCStatus(err)
	}
	return parsed, nil
}

// buildAcquireCommand constructs a Raft command from the request and resolved durations.
func (s *raftLockServer) buildAcquireCommand(req *pb.AcquireRequest, ttl, waitTimeout time.Duration) types.Command {
	return types.Command{
		Op:          types.OperationAcquire,
		LockID:      types.LockID(req.LockId),
		ClientID:    types.ClientID(req.ClientId),
		TTL:         ttl.Milliseconds(),
		Priority:    int(req.Priority),
		Wait:        req.Wait,
		WaitTimeout: waitTimeout.Milliseconds(),
		RequestID:   req.RequestId,
	}
}

// parseAcquireProposal extracts *LockInfo from the result of a proposal.
func (s *raftLockServer) parseAcquireProposal(data any) (any, error) {
	if data == nil {
		return nil, nil
	}
	lockInfo, ok := data.(*types.LockInfo)
	if !ok {
		s.logger.Errorw("Unexpected data type", "type", fmt.Sprintf("%T", data))
		return nil, fmt.Errorf("unexpected result type from acquire proposal")
	}
	return lockInfo, nil
}

// handleAcquireError maps proposal errors to AcquireResponse or gRPC errors.
func (s *raftLockServer) handleAcquireError(err error, req *pb.AcquireRequest) (*pb.AcquireResponse, error) {
	s.logger.Warnw("Acquire failed", "lockId", req.LockId, "clientId", req.ClientId, "error", err)

	// If not waiting and lock is held, return backoff advice
	if errors.Is(err, lock.ErrLockHeld) && !req.Wait {
		info, _ := s.lockManager.GetLockInfo(context.Background(), types.LockID(req.LockId))
		return &pb.AcquireResponse{
			Acquired:      false,
			Error:         ErrorToProtoError(err),
			BackoffAdvice: s.lockInfoToBackoffAdvice(info),
		}, nil
	}

	return nil, s.errorToGRPCStatus(err)
}

// buildAcquireResponse constructs the final response from LockInfo or enqueue fallback.
func (s *raftLockServer) buildAcquireResponse(data any, req *pb.AcquireRequest) (*pb.AcquireResponse, error) {
	if info, ok := data.(*types.LockInfo); ok && info != nil {
		s.logger.Infow("Lock acquired", "lockId", req.LockId, "clientId", req.ClientId, "version", info.Version)
		return &pb.AcquireResponse{
			Acquired: true,
			Lock:     s.lockInfoToProtoLock(info),
		}, nil
	}

	if req.Wait {
		s.logger.Infow("Client enqueued", "lockId", req.LockId, "clientId", req.ClientId)
		info, _ := s.lockManager.GetLockInfo(context.Background(), types.LockID(req.LockId))

		return &pb.AcquireResponse{
			Acquired:              false, // Not immediately acquired, but enqueued
			Error:                 ErrorToProtoError(fmt.Errorf("enqueued for lock %s", req.LockId)),
			QueuePosition:         int32(info.WaiterCount),
			EstimatedWaitDuration: durationpb.New(s.calculateBackoffDuration(info) * time.Duration(info.WaiterCount)),
		}, nil
	}

	// Should never reach here unless internal logic breaks
	s.logger.Errorw("Unexpected state post-acquire", "lockId", req.LockId, "clientId", req.ClientId)
	return nil, s.errorToGRPCStatus(NewServerError(MethodAcquire, nil, "unexpected server state"))
}

// lockInfoToProtoLock converts an internal LockInfo structure to its protobuf representation.
func (s *raftLockServer) lockInfoToProtoLock(info *types.LockInfo) *pb.Lock {
	if info == nil {
		return nil
	}

	pbLock := &pb.Lock{
		LockId:     string(info.LockID),
		OwnerId:    string(info.OwnerID),
		Version:    int64(info.Version),
		AcquiredAt: timestamppb.New(info.AcquiredAt),
		ExpiresAt:  timestamppb.New(info.ExpiresAt),
		Metadata:   make(map[string]string, len(info.Metadata)),
	}

	maps.Copy(pbLock.Metadata, info.Metadata)

	return pbLock
}

// Release handles a lock release request via the Raft consensus mechanism.
func (s *raftLockServer) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.ReleaseResponse, error) {
	s.logger.Debugw("Received Release request", "lockId", req.LockId, "clientId", req.ClientId, "version", req.Version)
	if err := s.validator.ValidateReleaseRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodRelease, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid Release request", "error", err, "request", req)
		return nil, s.errorToGRPCStatus(err)
	}

	if err := s.requireLeader(MethodRelease); err != nil {
		s.logger.Warnw("Release failed: not leader", "lockId", req.LockId, "clientId", req.ClientId, "error", err)
		return nil, s.errorToGRPCStatus(err)
	}

	cmd := types.Command{
		Op:       types.OperationRelease,
		LockID:   types.LockID(req.LockId),
		ClientID: types.ClientID(req.ClientId),
		Version:  types.Index(req.Version),
	}

	parsedData, err := s.submitAndProcessProposal(ctx, MethodRelease, cmd,
		func(data any) (any, error) {
			waiterPromoted, ok := data.(bool)
			if !ok {
				s.logger.Infow("Release proposal returned non-boolean data for waiterPromoted, assuming false", "type", fmt.Sprintf("%T", data))
				return false, nil
			}
			return waiterPromoted, nil
		},
	)

	if err != nil {
		s.logger.Warnw("Release proposal processing failed", "lockId", req.LockId, "clientId", req.ClientId, "error", err)
		return nil, s.errorToGRPCStatus(err)
	}

	waiterPromoted := false
	if promotion, ok := parsedData.(bool); ok {
		waiterPromoted = promotion
	}

	s.logger.Infow("Lock released successfully", "lockId", req.LockId, "clientId", req.ClientId, "waiterPromoted", waiterPromoted)
	return &pb.ReleaseResponse{
		Released:       true,
		WaiterPromoted: waiterPromoted,
	}, nil
}

// Renew handles a lock renewal request via the Raft consensus mechanism.
func (s *raftLockServer) Renew(ctx context.Context, req *pb.RenewRequest) (*pb.RenewResponse, error) {
	if err := s.validator.ValidateRenewRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodRenew, ErrorTypeInvalidFormat)
		return nil, s.errorToGRPCStatus(err)
	}

	if err := s.requireLeader(MethodRenew); err != nil {
		return nil, s.errorToGRPCStatus(err)
	}

	ttl := lock.DefaultLockTTL
	if req.NewTtl != nil && req.NewTtl.IsValid() && req.NewTtl.AsDuration() > 0 {
		ttl = req.NewTtl.AsDuration()
	}

	cmd := types.Command{
		Op:       types.OperationRenew,
		LockID:   types.LockID(req.LockId),
		ClientID: types.ClientID(req.ClientId),
		Version:  types.Index(req.Version),
		TTL:      ttl.Milliseconds(),
	}

	result, err := s.submitRaftProposal(ctx, cmd)
	if err != nil {
		return nil, s.errorToGRPCStatus(err)
	}
	if !result.Success {
		return nil, s.errorToGRPCStatus(result.Error)
	}

	lockInfo, ok := result.Data.(*types.LockInfo)
	if !ok {
		s.logger.Errorw("Renew: unexpected proposal result type", "type", fmt.Sprintf("%T", result.Data))
		return nil, s.errorToGRPCStatus(fmt.Errorf("internal error: unexpected proposal result type"))
	}

	return &pb.RenewResponse{
		Renewed: true,
		Lock:    s.lockInfoToProtoLock(lockInfo),
	}, nil
}

// GetLockInfo returns metadata about a lock using a linearizable Raft read.
func (s *raftLockServer) GetLockInfo(ctx context.Context, req *pb.GetLockInfoRequest) (*pb.GetLockInfoResponse, error) {
	s.logger.Debugw("Received GetLockInfo request", "lockId", req.LockId, "includeWaiters", req.IncludeWaiters)
	if err := s.validator.ValidateGetLockInfoRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodGetLockInfo, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid GetLockInfo request", "error", err, "request", req)
		return nil, s.errorToGRPCStatus(err)
	}

	readIndex, err := s.getReadIndex(ctx, types.LockID(req.LockId))
	if err != nil {
		s.logger.Warnw("GetLockInfo: getReadIndex failed", "lockId", req.LockId, "error", err)
		return nil, s.errorToGRPCStatus(err)
	}

	if err := s.waitForCommitIndex(ctx, readIndex, types.LockID(req.LockId)); err != nil {
		s.logger.Warnw("GetLockInfo: waitForCommitIndex failed", "lockId", req.LockId, "readIndex", readIndex, "error", err)
		return nil, s.errorToGRPCStatus(err)
	}

	lockInfo, err := s.lockManager.GetLockInfo(ctx, types.LockID(req.LockId))
	if err != nil {
		if errors.Is(err, lock.ErrLockNotFound) {
			s.logger.Infow("Lock not found for GetLockInfo", "lockId", req.LockId)
			return &pb.GetLockInfoResponse{
				LockInfo: nil,
				Error:    ErrorToProtoError(err),
			}, nil
		}
		s.logger.Errorw("Failed to get lock info from lock manager", "lockId", req.LockId, "error", err)
		s.metrics.IncrServerError(MethodGetLockInfo, ErrorTypeInternalError)
		return nil, s.errorToGRPCStatus(fmt.Errorf("failed to retrieve lock info: %w", err))
	}

	pbLockInfo := s.lockInfoToProto(lockInfo)
	if !req.IncludeWaiters { // Filter out waiter details if not requested
		if pbLockInfo != nil {
			pbLockInfo.WaitersInfo = nil
		}
	}

	s.logger.Debugw("Lock info retrieved successfully", "lockId", req.LockId, "ownerId", lockInfo.OwnerID)
	s.metrics.IncrGRPCRequest(MethodGetLockInfo, true)
	return &pb.GetLockInfoResponse{
		LockInfo: pbLockInfo,
	}, nil
}

// getReadIndex issues a linearizable ReadIndex request via Raft.
func (s *raftLockServer) getReadIndex(ctx context.Context, lockID types.LockID) (types.Index, error) {
	readCtx, cancel := context.WithTimeout(ctx, s.config.RequestTimeout)
	defer cancel()

	index, err := s.raftNode.ReadIndex(readCtx)
	if err != nil {
		s.logger.Warnw("ReadIndex failed", "lockId", lockID, "error", err)

		if errors.Is(err, raft.ErrNotLeader) {
			s.metrics.IncrServerError(MethodGetLockInfo, ErrorTypeRaftUnavailable)
			return 0, s.errorToGRPCStatus(ErrNotLeader)
		}

		s.metrics.IncrServerError(MethodGetLockInfo, ErrorTypeRaftUnavailable)
		return 0, s.errorToGRPCStatus(fmt.Errorf("read index failed: %w", err))
	}

	s.logger.Debugw("ReadIndex OK", "lockId", lockID, "index", index)
	return index, nil
}

// waitForCommitIndex blocks until the local state machine has applied up to the given index.
func (s *raftLockServer) waitForCommitIndex(ctx context.Context, targetIndex types.Index, lockID types.LockID) error {
	waitCtx, cancel := context.WithTimeout(ctx, s.config.RequestTimeout) // TODO: fix timeout
	defer cancel()

	for s.lastCommitIndex.Load() < uint64(targetIndex) {
		select {
		case <-waitCtx.Done():
			s.logger.Warnw("timeout waiting for state to catch up",
				"lockId", lockID,
				"expectedIndex", targetIndex,
				"currentIndex", s.lastCommitIndex.Load(),
				"error", waitCtx.Err())
			s.metrics.IncrServerError(MethodGetLockInfo, ErrorTypeTimeout)
			return status.Errorf(codes.Unavailable,
				"state not ready (expected index %d, current %d)", targetIndex, s.lastCommitIndex.Load())

		case <-s.stopCh:
			s.logger.Infow("server shutting down during GetLockInfo", "lockId", lockID)
			return status.Error(codes.Unavailable, "server is shutting down")

		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	s.logger.Debugw("state caught up for GetLockInfo",
		"lockId", lockID,
		"index", targetIndex,
		"current", s.lastCommitIndex.Load())

	return nil
}

// GetLocks returns a paginated list of locks that match the provided filter.
func (s *raftLockServer) GetLocks(ctx context.Context, req *pb.GetLocksRequest) (*pb.GetLocksResponse, error) {
	s.logger.Debugw("Received GetLocks request", "filter", req.Filter, "limit", req.Limit, "offset", req.Offset)
	if err := s.validator.ValidateGetLocksRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodGetLocks, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid GetLocks request", "error", err, "request", req)
		return nil, s.errorToGRPCStatus(err)
	}

	filter := s.protoFilterToLockFilter(req.Filter)

	limit := DefaultPageLimit
	if req.Limit > 0 && req.Limit <= MaxPageLimit {
		limit = int(req.Limit)
	} else if req.Limit > MaxPageLimit {
		limit = MaxPageLimit
	}

	offset := 0
	if req.Offset > 0 {
		offset = int(req.Offset)
	}

	locks, total, err := s.lockManager.GetLocks(ctx, filter, limit, offset)
	if err != nil {
		s.logger.Errorw("Failed to get locks from lock manager", "error", err, "filter", req.Filter)
		s.metrics.IncrServerError(MethodGetLocks, ErrorTypeInternalError)
		return nil, s.errorToGRPCStatus(fmt.Errorf("failed to retrieve locks: %w", err))
	}

	protoLocks := make([]*pb.LockInfo, len(locks))
	for i, lockInfo := range locks {
		pl := s.lockInfoToProto(lockInfo)
		if !req.IncludeWaiters {
			if pl != nil {
				pl.WaitersInfo = nil
			}
		}
		protoLocks[i] = pl
	}

	hasMore := (offset + len(locks)) < total

	s.logger.Debugw("GetLocks successful", "returnedCount", len(protoLocks), "totalMatching", total, "hasMore", hasMore)
	s.metrics.IncrGRPCRequest(MethodGetLocks, true)
	return &pb.GetLocksResponse{
		Locks:               protoLocks,
		TotalMatchingFilter: int32(total),
		HasMore:             hasMore,
	}, nil
}

// protoFilterToLockFilter converts a protobuf LockFilter to an internal LockFilter function.
func (s *raftLockServer) protoFilterToLockFilter(pf *pb.LockFilter) lock.LockFilter {
	if pf == nil {
		return lock.FilterAll
	}

	return func(li *types.LockInfo) bool {
		// Match Lock ID pattern
		if pf.LockIdPattern != "" && !strings.Contains(string(li.LockID), pf.LockIdPattern) {
			return false
		}

		// Match Owner ID pattern
		if pf.OwnerIdPattern != "" && !strings.Contains(string(li.OwnerID), pf.OwnerIdPattern) {
			return false
		}

		// Match held status
		if pf.OnlyHeld && li.OwnerID == "" {
			return false
		}

		// Match expiration window
		if pf.ExpiresBefore != nil && li.ExpiresAt.After(pf.ExpiresBefore.AsTime()) {
			return false
		}
		if pf.ExpiresAfter != nil && li.ExpiresAt.Before(pf.ExpiresAfter.AsTime()) {
			return false
		}

		// Match metadata key-value pairs
		for key, expected := range pf.MetadataFilter {
			actual, ok := li.Metadata[key]
			if !ok || actual != expected {
				return false
			}
		}

		return true
	}
}

// lockInfoToProto converts internal LockInfo to its protobuf representation.
func (s *raftLockServer) lockInfoToProto(info *types.LockInfo) *pb.LockInfo {
	if info == nil {
		return nil
	}

	return &pb.LockInfo{
		LockId:         string(info.LockID),
		OwnerId:        string(info.OwnerID),
		Version:        int64(info.Version),
		AcquiredAt:     timestamppb.New(info.AcquiredAt),
		ExpiresAt:      timestamppb.New(info.ExpiresAt),
		WaiterCount:    int32(info.WaiterCount),
		Metadata:       cloneStringMap(info.Metadata),
		WaitersInfo:    convertWaiters(info.WaitersInfo),
		LastModifiedAt: timestamppb.New(info.LastModified),
	}
}

// convertWaiters maps a slice of internal WaiterInfo to protobuf WaiterInfo.
func convertWaiters(waiters []types.WaiterInfo) []*pb.WaiterInfo {
	if len(waiters) == 0 {
		return nil
	}

	out := make([]*pb.WaiterInfo, len(waiters))
	for i, w := range waiters {
		out[i] = &pb.WaiterInfo{
			ClientId:   string(w.ClientID),
			EnqueuedAt: timestamppb.New(w.EnqueuedAt),
			TimeoutAt:  timestamppb.New(w.TimeoutAt),
			Priority:   int32(w.Priority),
			Position:   int32(w.Position),
		}
	}
	return out
}

// cloneStringMap copies a map[string]string to avoid shared references.
func cloneStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)
	return out
}

// EnqueueWaiter adds a client to the wait queue for a lock via Raft.
// The client is enqueued with a version (fencing token), optional priority, and timeout.
func (s *raftLockServer) EnqueueWaiter(ctx context.Context, req *pb.EnqueueWaiterRequest) (*pb.EnqueueWaiterResponse, error) {
	s.logger.Debugw("Received EnqueueWaiter request", "lockId", req.LockId, "clientId", req.ClientId)

	if err := s.validator.ValidateEnqueueWaiterRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodEnqueueWaiter, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid EnqueueWaiter request", "error", err, "request", req)
		return nil, s.errorToGRPCStatus(err)
	}

	if err := s.requireLeader(MethodEnqueueWaiter); err != nil {
		s.logger.Warnw("EnqueueWaiter failed: not leader", "lockId", req.LockId, "clientId", req.ClientId, "error", err)
		return nil, s.errorToGRPCStatus(err)
	}

	timeout := s.resolveEnqueueWaitTimeout(req.Timeout)

	cmd := types.Command{
		Op:       types.OperationEnqueueWaiter,
		LockID:   types.LockID(req.LockId),
		ClientID: types.ClientID(req.ClientId),
		Version:  types.Index(req.Version),
		Priority: int(req.Priority),
		Timeout:  timeout.Milliseconds(),
	}

	result, err := s.submitAndProcessProposal(ctx, MethodEnqueueWaiter, cmd, parseEnqueuePositionResult)
	if err != nil {
		s.logger.Warnw("EnqueueWaiter proposal processing failed", "lockId", req.LockId, "clientId", req.ClientId, "error", err)
		return nil, s.errorToGRPCStatus(err)
	}
	position := result.(int)

	estimated := s.estimateEnqueueWaitDuration(types.LockID(req.LockId), position)

	s.logger.Infow("Client enqueued successfully", "lockId", req.LockId, "clientId", req.ClientId, "position", position)
	s.metrics.IncrGRPCRequest(MethodEnqueueWaiter, true)

	return &pb.EnqueueWaiterResponse{
		Enqueued:              true,
		Position:              int32(position),
		EstimatedWaitDuration: durationpb.New(estimated),
	}, nil
}

// resolveEnqueueWaitTimeout validates and resolves the effective wait timeout from the request.
func (s *raftLockServer) resolveEnqueueWaitTimeout(d *durationpb.Duration) time.Duration {
	if d != nil && d.IsValid() {
		timeout := d.AsDuration()
		if timeout >= lock.MinWaitQueueTimeout && timeout <= lock.MaxWaitQueueTimeout {
			return timeout
		}
		s.metrics.IncrValidationError(MethodEnqueueWaiter, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid wait timeout, using default", "requestedTimeout", timeout)
	}
	return lock.DefaultWaitQueueTimeout
}

// parseEnqueuePositionResult extracts an integer position from Raft proposal response.
func parseEnqueuePositionResult(data any) (any, error) {
	pos, ok := data.(int)
	if !ok {
		return 0, fmt.Errorf("internal error: unexpected result type for enqueue position: %T", data)
	}
	return pos, nil
}

// estimateEnqueueWaitDuration estimates wait time based on current lock state and queue position.
func (s *raftLockServer) estimateEnqueueWaitDuration(lockID types.LockID, position int) time.Duration {
	info, err := s.lockManager.GetLockInfo(context.Background(), lockID)
	if err != nil || info == nil {
		s.logger.Debugw("Failed to get lock info for wait estimate", "lockId", lockID, "error", err)
		return 0
	}

	if info.OwnerID == "" && info.WaiterCount == 0 {
		return 0
	}

	base := s.calculateBackoffDuration(info)
	numAhead := 0
	for _, w := range info.WaitersInfo {
		if w.Position < position {
			numAhead++
		}
	}
	if info.OwnerID != "" {
		numAhead++
	}
	return base * time.Duration(numAhead+1)
}

// CancelWait removes a client from a lock's wait queue via Raft.
func (s *raftLockServer) CancelWait(ctx context.Context, req *pb.CancelWaitRequest) (*pb.CancelWaitResponse, error) {
	s.logger.Debugw("Received CancelWait request", "lockId", req.LockId, "clientId", req.ClientId, "version", req.Version)
	if err := s.validator.ValidateCancelWaitRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodCancelWait, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid CancelWait request", "error", err, "request", req)
		return nil, s.errorToGRPCStatus(err)
	}

	if err := s.requireLeader(MethodCancelWait); err != nil {
		s.logger.Warnw("CancelWait failed: not leader", "lockId", req.LockId, "clientId", req.ClientId, "error", err)
		return nil, s.errorToGRPCStatus(err)
	}

	cmd := types.Command{
		Op:       types.OperationCancelWait,
		LockID:   types.LockID(req.LockId),
		ClientID: types.ClientID(req.ClientId),
		Version:  types.Index(req.Version),
	}

	parsedData, err := s.submitAndProcessProposal(ctx, MethodCancelWait, cmd,
		func(data any) (any, error) {
			cancelled, ok := data.(bool)
			if !ok {
				s.logger.Errorw("CancelWait: submitAndProcessProposal returned unexpected data type", "type", fmt.Sprintf("%T", data))
				return false, fmt.Errorf("internal error: unexpected result type for cancel wait")
			}
			return cancelled, nil
		},
	)

	if err != nil {
		s.logger.Warnw("CancelWait proposal processing failed", "lockId", req.LockId, "clientId", req.ClientId, "error", err)
		return nil, s.errorToGRPCStatus(err)
	}

	cancelled := parsedData.(bool)
	if !cancelled {
		s.logger.Infow("Wait cancellation processed by LockManager, but client was not found or not removed", "lockId", req.LockId, "clientId", req.ClientId)
		return &pb.CancelWaitResponse{
			Cancelled: false,
			Error:     ErrorToProtoError(lock.ErrNotWaiting),
		}, nil
	}

	s.logger.Infow("Wait cancelled successfully", "lockId", req.LockId, "clientId", req.ClientId)
	s.metrics.IncrGRPCRequest(MethodCancelWait, true)
	return &pb.CancelWaitResponse{Cancelled: true}, nil
}

// GetBackoffAdvice provides a client with backoff guidance based on current lock state.
func (s *raftLockServer) GetBackoffAdvice(ctx context.Context, req *pb.BackoffAdviceRequest) (*pb.BackoffAdviceResponse, error) {
	s.logger.Debugw("Received GetBackoffAdvice request", "lockId", req.LockId)
	if err := s.validator.ValidateBackoffAdviceRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodGetBackoffAdvice, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid GetBackoffAdvice request", "error", err, "request", req)
		return nil, s.errorToGRPCStatus(err)
	}

	// This is a local read. For linearizable backoff advice, ReadIndex would be needed.
	lockInfo, err := s.lockManager.GetLockInfo(ctx, types.LockID(req.LockId))
	if err != nil && !errors.Is(err, lock.ErrLockNotFound) {
		s.logger.Errorw("Failed to get lock info for backoff advice", "lockId", req.LockId, "error", err)
		s.metrics.IncrServerError(MethodGetBackoffAdvice, ErrorTypeInternalError)

		return &pb.BackoffAdviceResponse{
			Advice: &pb.BackoffAdvice{
				InitialBackoff: durationpb.New(100 * time.Millisecond),
				MaxBackoff:     durationpb.New(5 * time.Second),
				Multiplier:     2.0,
				JitterFactor:   0.1,
			},
		}, nil
	}

	advice := s.lockInfoToBackoffAdvice(lockInfo)

	s.logger.Debugw("GetBackoffAdvice successful", "lockId", req.LockId)
	s.metrics.IncrGRPCRequest(MethodGetBackoffAdvice, true)
	return &pb.BackoffAdviceResponse{
		Advice: &pb.BackoffAdvice{
			InitialBackoff: advice.InitialBackoff,
			MaxBackoff:     advice.MaxBackoff,
			Multiplier:     advice.Multiplier,
			JitterFactor:   advice.JitterFactor,
		},
	}, nil
}

// GetStatus returns a snapshot of Raft and server status.
func (s *raftLockServer) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	if err := s.validator.ValidateGetStatusRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodGetStatus, ErrorTypeInvalidFormat)
		return nil, s.errorToGRPCStatus(err)
	}

	raftStatus := s.raftNode.Status()

	status := &pb.RaftStatus{
		NodeId:      string(s.config.NodeID),
		LeaderId:    string(s.currentLeaderID.Load().(types.NodeID)),
		Term:        uint64(raftStatus.Term),
		CommitIndex: uint64(raftStatus.CommitIndex),
		LastApplied: uint64(raftStatus.LastApplied),
	}

	return &pb.GetStatusResponse{RaftStatus: status}, nil
}

// Health reports whether the server is currently serving requests.
func (s *raftLockServer) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	s.logger.Debugw("Received Health request", "serviceName", req.ServiceName)

	if err := s.validator.ValidateHealthRequest(req); err != nil {
		s.metrics.IncrValidationError(MethodHealth, ErrorTypeInvalidFormat)
		s.logger.Warnw("Invalid Health request", "error", err, "request", req)
		return nil, s.errorToGRPCStatus(err)
	}

	isHealthy := s.performHealthCheck()
	s.metrics.IncrHealthCheck(isHealthy)

	var overallStatus pb.HealthStatus_ServingStatus
	var topLevelMessage string

	if isHealthy {
		overallStatus = pb.HealthStatus_SERVING
		topLevelMessage = "Server is healthy and ready to serve requests."
	} else {
		overallStatus = pb.HealthStatus_NOT_SERVING
		topLevelMessage = "Server is unhealthy or not currently serving requests. Check HealthInfo for details."
	}

	detailedHealthInfo := s.healthStatusToProto(isHealthy)

	s.logger.Infow("Health check processed", "isHealthy", isHealthy, "reportedStatus", overallStatus)

	return &pb.HealthResponse{
		Status:     overallStatus,
		Message:    topLevelMessage,
		HealthInfo: detailedHealthInfo,
	}, nil
}

// performHealthCheck runs a basic health check against core subsystems.
func (s *raftLockServer) performHealthCheck() bool {
	currentState := s.state.Load().(ServerOperationalState)
	if currentState != ServerStateRunning {
		s.logger.Warnw("Health check: Server not in running state", "state", currentState)
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.HealthCheckTimeout)
	defer cancel()

	if s.raftNode == nil {
		s.logger.Warnw("Health check: Raft node is nil")
		return false
	}
	raftStatus := s.raftNode.Status()
	if raftStatus.ID != s.config.NodeID {
		s.logger.Warnw("Health check failed: Raft node ID mismatch",
			"expected", s.config.NodeID, "actual", raftStatus.ID)
		return false
	}

	if s.lockManager == nil {
		s.logger.Warnw("Health check: Lock manager is nil")
		return false
	}
	_, err := s.lockManager.GetLockInfo(ctx, types.LockID("__health_check_internal__"))
	if err != nil && !errors.Is(err, lock.ErrLockNotFound) {
		s.logger.Warnw("Health check failed: Lock manager GetLockInfo error", "error", err)
		return false
	}

	if s.listener == nil {
		s.logger.Warnw("Health check: gRPC listener is nil (server might not be fully started or stopped)")
		return false
	}

	s.logger.Debugw("Health check passed")
	return true
}

// healthStatusToProto builds a HealthStatus protobuf message with server health and Raft metadata.
func (s *raftLockServer) healthStatusToProto(isHealthy bool) *pb.HealthStatus {
	status := pb.HealthStatus_SERVING
	msg := "Server is healthy"
	if !isHealthy {
		status = pb.HealthStatus_NOT_SERVING
		msg = "Server is unhealthy"
	}
	var uptimeDuration time.Duration
	if !s.serverStartTime.IsZero() {
		uptimeDuration = s.clock.Since(s.serverStartTime)
	} else {
		uptimeDuration = 0
		s.logger.Debugw("healthStatusToProto: serverStartTime not set, uptime will be zero or placeholder.")
	}

	var activeLocks, totalWaiters int32

	if s.lockManager != nil {
		if count, err := s.lockManager.GetActiveLockCount(context.Background()); err == nil {
			activeLocks = int32(count)
		} else {
			s.logger.Warnw("Failed to get active lock count for health status", "error", err)
		}
		if count, err := s.lockManager.GetTotalWaiterCount(context.Background()); err == nil {
			totalWaiters = int32(count)
		} else {
			s.logger.Warnw("Failed to get total waiter count for health status", "error", err)
		}
	}

	return &pb.HealthStatus{
		Status:              status,
		Message:             msg,
		IsRaftLeader:        s.isLeader.Load(),
		RaftLeaderAddress:   s.GetLeaderAddress(),
		RaftTerm:            s.lastKnownTerm.Load(),
		RaftLastApplied:     s.lastCommitIndex.Load(), // Using commit index as proxy
		CurrentActiveLocks:  activeLocks,
		CurrentTotalWaiters: totalWaiters,
		Uptime:              durationpb.New(uptimeDuration),
		LastHealthCheckAt:   timestamppb.New(s.clock.Now()),
	}
}

// submitAndProcessProposal submits a command to Raft, waits for it to be applied,
// and returns the result (optionally parsed).
func (s *raftLockServer) submitAndProcessProposal(
	ctx context.Context,
	method string,
	cmd types.Command,
	resultParser func(data any) (any, error),
) (any, error) {
	s.logger.Debugw("Submitting Raft proposal", "method", method, "lockId", cmd.LockID, "clientId", cmd.ClientID, "op", cmd.Op)

	proposalResult, err := s.submitRaftProposal(ctx, cmd)
	if err != nil {
		s.logger.Warnw("Raft proposal submission failed", "method", method, "lockId", cmd.LockID, "error", err)
		return nil, err
	}

	if !proposalResult.Success {
		s.logger.Warnw("Raft proposal applied with failure", "method", method, "lockId", cmd.LockID, "proposalError", proposalResult.Error)
		s.metrics.IncrServerError(method, ErrorTypeInternalError)
		return nil, proposalResult.Error
	}

	s.logger.Debugw("Raft proposal applied successfully", "method", method, "lockId", cmd.LockID)

	if resultParser == nil {
		return proposalResult.Data, nil
	}
	return resultParser(proposalResult.Data)
}

// submitRaftProposal serializes the given command, proposes it to the Raft cluster,
// tracks the proposal, and waits for its result or cancellation.
func (s *raftLockServer) submitRaftProposal(ctx context.Context, cmd types.Command) (*types.ProposalResult, error) {
	startTime := s.clock.Now()

	cmdData, err := s.serializer.EncodeCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("submitRaftProposal: failed to marshal command: %w", err)
	}

	index, term, isLeader, err := s.raftNode.Propose(ctx, cmdData)
	if err != nil {
		s.metrics.IncrRaftProposal(cmd.Op, false)
		s.metrics.ObserveRaftProposalLatency(cmd.Op, s.clock.Since(startTime))
		return nil, fmt.Errorf("submitRaftProposal: Raft propose failed: %w", err)
	}

	if !isLeader {
		s.metrics.IncrRaftProposal(cmd.Op, false)
		return nil, s.requireLeader(string(cmd.Op))
	}

	proposalID := types.ProposalID(fmt.Sprintf("%d-%d", term, index))
	resultCh := make(chan types.ProposalResult, 1)

	proposal := &types.PendingProposal{
		ID:        proposalID,
		Index:     index,
		Term:      term,
		Operation: cmd.Op,
		StartTime: startTime,
		ResultCh:  resultCh,
		Context:   ctx,
		Command:   cmdData,
		ClientID:  cmd.ClientID,
		LockID:    cmd.LockID,
	}

	if err := s.proposalTracker.Track(proposal); err != nil {
		s.metrics.IncrRaftProposal(cmd.Op, false)
		return nil, fmt.Errorf("submitRaftProposal: failed to track proposal: %w", err)
	}

	select {
	case result := <-resultCh:
		s.metrics.IncrRaftProposal(cmd.Op, result.Success)
		s.metrics.ObserveRaftProposalLatency(cmd.Op, result.Duration)
		return &result, nil

	case <-ctx.Done():
		s.proposalTracker.ClientCancel(proposalID, ctx.Err())
		s.metrics.IncrRaftProposal(cmd.Op, false)
		s.metrics.ObserveRaftProposalLatency(cmd.Op, s.clock.Since(startTime))
		return nil, fmt.Errorf("submitRaftProposal: context cancelled: %w", ctx.Err())
	}
}

// requireLeader ensures that this server is currently the Raft leader.
func (s *raftLockServer) requireLeader(method string) error {
	if s.isLeader.Load() {
		return nil
	}

	s.metrics.IncrLeaderRedirect(method)

	leaderID := s.currentLeaderID.Load().(types.NodeID)
	leaderAddr := s.GetLeaderAddress()

	if leaderAddr == "" {
		s.logger.Warnw("RequireLeader check failed: not leader and leader address is unknown", "method", method)
		return ErrNoLeader
	}

	s.logger.Infow("RequireLeader check failed: not leader, redirecting", "method", method, "leaderAddress", leaderAddr, "leaderID", leaderID)
	return NewLeaderRedirectError(leaderAddr, string(leaderID))
}

// lockInfoToBackoffAdvice returns retry guidance when a lock is currently held.
// It's used when Acquire fails with ErrLockHeld and wait=false.
func (s *raftLockServer) lockInfoToBackoffAdvice(info *types.LockInfo) *pb.BackoffAdvice {
	if info == nil || info.OwnerID == "" {
		return &pb.BackoffAdvice{
			InitialBackoff: durationpb.New(DefaultBackoffInitial),
			MaxBackoff:     durationpb.New(DefaultBackoffMax),
			Multiplier:     DefaultBackoffMultiplier,
			JitterFactor:   DefaultBackoffJitterFactor,
		}
	}

	estimated := s.calculateBackoffDuration(info)

	return &pb.BackoffAdvice{
		InitialBackoff: durationpb.New(estimated / 4),
		MaxBackoff:     durationpb.New(estimated * 2),
		Multiplier:     DefaultBackoffMultiplier,
		JitterFactor:   DefaultBackoffJitterFactor,
	}
}

// calculateBackoffDuration estimates how long a client should wait before retrying a lock acquisition.
func (s *raftLockServer) calculateBackoffDuration(info *types.LockInfo) time.Duration {
	if info.OwnerID == "" {
		// No current owner — no need to back off
		return 0
	}

	now := s.clock.Now()
	remainingTTL := info.ExpiresAt.Sub(now)
	if remainingTTL <= 0 {
		// Already expired — safe to retry
		return 0
	}

	// Increase backoff based on number of waiters
	queueMultiplier := 1.0 + float64(info.WaiterCount)*0.1
	backoff := min(time.Duration(float64(remainingTTL)*0.5*queueMultiplier), MaxBackoffAdvice)
	if backoff < MinBackoffAdvice && remainingTTL > MinBackoffAdvice {
		backoff = MinBackoffAdvice
	}

	return backoff
}
