package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft/proto"
	"github.com/jathurchan/raftlock/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// NetworkManager represents the RPC communication layer between Raft peers.
//
// It handles connection management, serialization, context cancellation,
// and efficient transfer of large payloads (e.g., snapshots).
//
// Peer membership is fixed at construction. Configuration must be provided
// at initialization, not via methods like Start.
//
// Implementations must be safe for concurrent use.
type NetworkManager interface {
	// Start activates the network layer and begins listening for peer RPCs.
	// Must succeed before participating in Raft consensus.
	Start() error

	// Stop gracefully shuts down the network layer, closes connections,
	// and releases resources. Blocks until shutdown completes.
	Stop() error

	// SendRequestVote sends a RequestVote RPC to a target peer.
	SendRequestVote(
		ctx context.Context,
		target types.NodeID,
		args *types.RequestVoteArgs,
	) (*types.RequestVoteReply, error)

	// SendAppendEntries sends an AppendEntries RPC (log replication or heartbeat) to a target peer.
	SendAppendEntries(
		ctx context.Context,
		target types.NodeID,
		args *types.AppendEntriesArgs,
	) (*types.AppendEntriesReply, error)

	// SendInstallSnapshot efficiently streams or chunks snapshot data to a target peer.
	SendInstallSnapshot(
		ctx context.Context,
		target types.NodeID,
		args *types.InstallSnapshotArgs,
	) (*types.InstallSnapshotReply, error)

	// PeerStatus returns health and connection info for a known peer.
	PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error)

	// LocalAddr returns the local address (e.g., "host:port") or empty string if unavailable.
	LocalAddr() string

	// ResetConnection explicitly closes and re-establishes the connection to a peer.
	// This is useful when the connection is suspected to be in a bad state.
	ResetConnection(ctx context.Context, peerID types.NodeID) error
}

// gRPCNetworkManager implements the NetworkManager interface using gRPC
// for Raft peer-to-peer communication. It manages server lifecycle,
// client connections to peers, and marshaling/unmarshaling of RPC messages.
type gRPCNetworkManager struct {
	mu         sync.RWMutex // Protects access to peerClients map
	isShutdown *atomic.Bool // Flag indicating if the manager is shutting down

	id         types.NodeID // ID of the local node
	localAddr  string       // Address the local gRPC server listens on
	rpcHandler RPCHandler   // Handler for incoming Raft RPCs

	logger  logger.Logger
	metrics Metrics
	clock   Clock

	opts GRPCNetworkManagerOptions

	peers       map[types.NodeID]PeerConfig      // Static configuration of peers
	peerClients map[types.NodeID]*peerConnection // Active connections to peers
	server      *grpc.Server                     // The gRPC server instance
	listener    net.Listener                     // Network listener for the server

	serverStarted atomic.Bool   // Tracks if Serve() has been called
	serverReady   chan struct{} // Signals when the server goroutine is ready
	stopOnce      sync.Once     // Ensures Stop logic runs only once
}

// peerConnection encapsulates the state for a gRPC connection to a single Raft peer.
type peerConnection struct {
	mu         sync.Mutex // Protects conn and client fields during reset
	id         types.NodeID
	addr       string
	client     proto.RaftClient
	conn       *grpc.ClientConn
	pendingOps atomic.Int32
	lastError  error
	lastActive time.Time
	connected  atomic.Bool
}

// GRPCNetworkManagerOptions configures a gRPCNetworkManager.
type GRPCNetworkManagerOptions struct {
	MaxRecvMsgSize     int           // Maximum gRPC receive message size in bytes
	MaxSendMsgSize     int           // Maximum gRPC send message size in bytes
	DialTimeout        time.Duration // Timeout for establishing a connection to a peer
	ServerStartTimeout time.Duration // Max time to wait for the gRPC server to start listening
	KeepaliveTime      time.Duration // Client/Server: Ping interval when idle
	KeepaliveTimeout   time.Duration // Client/Server: Timeout for waiting for ping ack

	ServerMaxConnectionIdle     time.Duration // Server: Max duration a connection can be idle before closing
	ServerMaxConnectionAge      time.Duration // Server: Max duration a connection may exist before graceful close
	ServerMaxConnectionAgeGrace time.Duration // Server: Time to allow RPCs to complete after graceful close signal
}

// DefaultGRPCNetworkManagerOptions provides reasonable default configuration values
// using constants defined in constants.go.
func DefaultGRPCNetworkManagerOptions() GRPCNetworkManagerOptions {
	return GRPCNetworkManagerOptions{
		MaxRecvMsgSize:              DefaultMaxRecvMsgSize,
		MaxSendMsgSize:              DefaultMaxSendMsgSize,
		DialTimeout:                 DefaultDialTimeout,
		ServerStartTimeout:          DefaultServerStartTimeout,
		KeepaliveTime:               DefaultKeepaliveTime,
		KeepaliveTimeout:            DefaultKeepaliveTimeout,
		ServerMaxConnectionIdle:     DefaultServerMaxConnectionIdle,
		ServerMaxConnectionAge:      DefaultServerMaxConnectionAge,
		ServerMaxConnectionAgeGrace: DefaultServerMaxConnectionAgeGrace,
	}
}

// NewGRPCNetworkManager creates a new GRPCNetworkManager.
func NewGRPCNetworkManager(
	id types.NodeID,
	addr string,
	peers map[types.NodeID]PeerConfig,
	rpcHandler RPCHandler,
	isShutdown *atomic.Bool,
	logger logger.Logger,
	metrics Metrics,
	clock Clock,
	opts GRPCNetworkManagerOptions,
) (*gRPCNetworkManager, error) {
	if id == "" {
		return nil, errors.New("node ID cannot be empty")
	}
	if addr == "" {
		return nil, errors.New("local address cannot be empty")
	}
	if rpcHandler == nil {
		return nil, errors.New("RPC handler cannot be nil")
	}
	if isShutdown == nil {
		return nil, errors.New("shutdown flag cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}
	if metrics == nil {
		metrics = NewNoOpMetrics()
		logger.Warnw("No metrics implementation provided, using no-op metrics.")
	}
	if clock == nil {
		return nil, errors.New("clock cannot be nil")
	}
	for peerID, peerCfg := range peers {
		if peerID == "" {
			return nil, errors.New("peer ID cannot be empty")
		}
		if peerCfg.Address == "" {
			return nil, fmt.Errorf("address for peer %s cannot be empty", peerID)
		}
		if peerID == id && peerCfg.Address != addr {
			logger.Warnw(
				"Peer configuration for self has different address",
				"self_id",
				id,
				"configured_addr",
				peerCfg.Address,
				"local_addr",
				addr,
			)
		}
	}

	defaults := DefaultGRPCNetworkManagerOptions()
	if opts.MaxRecvMsgSize <= 0 {
		opts.MaxRecvMsgSize = defaults.MaxRecvMsgSize
	}
	if opts.MaxSendMsgSize <= 0 {
		opts.MaxSendMsgSize = defaults.MaxSendMsgSize
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = defaults.DialTimeout
	}
	if opts.ServerStartTimeout <= 0 {
		opts.ServerStartTimeout = defaults.ServerStartTimeout
	}
	if opts.KeepaliveTime <= 0 {
		opts.KeepaliveTime = defaults.KeepaliveTime
	}
	if opts.KeepaliveTimeout <= 0 {
		opts.KeepaliveTimeout = defaults.KeepaliveTimeout
	}
	if opts.ServerMaxConnectionIdle <= 0 {
		opts.ServerMaxConnectionIdle = defaults.ServerMaxConnectionIdle
	}
	if opts.ServerMaxConnectionAge <= 0 {
		opts.ServerMaxConnectionAge = defaults.ServerMaxConnectionAge
	}
	if opts.ServerMaxConnectionAgeGrace <= 0 {
		opts.ServerMaxConnectionAgeGrace = defaults.ServerMaxConnectionAgeGrace
	}

	serverKeepaliveParams := keepalive.ServerParameters{
		MaxConnectionIdle:     opts.ServerMaxConnectionIdle,
		MaxConnectionAge:      opts.ServerMaxConnectionAge,
		MaxConnectionAgeGrace: opts.ServerMaxConnectionAgeGrace,
		Time:                  opts.KeepaliveTime,
		Timeout:               opts.KeepaliveTimeout,
	}
	minClientPingInterval := max(opts.KeepaliveTime/2, time.Second)
	enforcementPolicy := keepalive.EnforcementPolicy{
		MinTime:             minClientPingInterval, // Disallow pings more frequent than roughly half the keepalive time
		PermitWithoutStream: true,                  // Allow pings even when there are no active streams
	}

	server := grpc.NewServer(
		grpc.KeepaliveParams(serverKeepaliveParams),
		grpc.KeepaliveEnforcementPolicy(enforcementPolicy),
		grpc.MaxRecvMsgSize(opts.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(opts.MaxSendMsgSize),
	)

	nm := &gRPCNetworkManager{
		id:          id,
		localAddr:   addr,
		rpcHandler:  rpcHandler,
		isShutdown:  isShutdown,
		logger:      logger.WithComponent("network"),
		metrics:     metrics,
		clock:       clock,
		opts:        opts,
		peers:       peers,
		peerClients: make(map[types.NodeID]*peerConnection),
		server:      server,
		serverReady: make(chan struct{}),
	}

	proto.RegisterRaftServer(server, &grpcServerHandler{
		nm:         nm,
		rpcHandler: rpcHandler,
		logger:     logger.WithComponent("grpc-server"),
	})

	nm.logger.Infow("gRPC Network Manager created", "id", id, "address", addr, "options", opts)
	return nm, nil
}

// Start initializes the network manager by starting its local gRPC server.
func (nm *gRPCNetworkManager) Start() error {
	nm.logger.Infow("Starting gRPC network manager", "address", nm.localAddr)

	if nm.isShutdown.Load() {
		nm.logger.Warnw("Start called but manager is shut down")
		return ErrShuttingDown
	}

	if nm.serverStarted.Load() {
		nm.logger.Warnw("Start called but server already started")
		return nil
	}

	if nm.listener == nil {
		l, err := net.Listen("tcp", nm.localAddr)
		if err != nil {
			nm.logger.Errorw(
				"Failed to listen on local address",
				"address",
				nm.localAddr,
				"error",
				err,
			)
			return fmt.Errorf("failed to listen on %s: %w", nm.localAddr, err)
		}
		nm.listener = l
	}

	actualAddr := nm.listener.Addr().String()
	nm.logger.Infow("gRPC server listener started", "address", actualAddr)

	go func() {
		nm.logger.Infow("gRPC server starting to serve requests", "address", actualAddr)
		nm.serverStarted.Store(true)
		close(nm.serverReady)

		if err := nm.server.Serve(nm.listener); err != nil {
			if !nm.isShutdown.Load() && !errors.Is(err, grpc.ErrServerStopped) &&
				!errors.Is(err, net.ErrClosed) {
				nm.logger.Errorw(
					"gRPC server encountered an error",
					"address", actualAddr,
					"error", err,
				)
			} else {
				nm.logger.Infow("gRPC server stopped serving", "address", actualAddr)
			}
		}
	}()

	select {
	case <-nm.serverReady:
		nm.logger.Infow("gRPC server goroutine is running", "address", actualAddr)
	case <-nm.clock.After(nm.opts.ServerStartTimeout):
		_ = nm.listener.Close()
		return fmt.Errorf(
			"timeout waiting for gRPC server to start listening after %v",
			nm.opts.ServerStartTimeout,
		)
	}

	nm.logger.Infow("Network manager's server started successfully", "self_id", nm.id)
	return nil
}

// Stop gracefully shuts down the network manager.
func (nm *gRPCNetworkManager) Stop() error {
	err := fmt.Errorf("stop already called")
	nm.stopOnce.Do(func() {
		err = nm.stopInternal()
	})
	return err
}

func (nm *gRPCNetworkManager) stopInternal() error {
	nm.logger.Infow("Stopping network manager...")

	if !nm.isShutdown.Load() {
		nm.logger.Warnw("Stop called but isShutdown flag was not set; setting it now.")
		nm.isShutdown.Store(true)
	}

	if nm.listener != nil {
		addr := nm.listener.Addr().String()
		nm.logger.Debugw("Closing server listener", "address", addr)
		if cerr := nm.listener.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
			nm.logger.Warnw("Error closing listener", "address", addr, "error", cerr)
		}
	}

	nm.mu.RLock()
	peerIDs := make([]types.NodeID, 0, len(nm.peerClients))
	for id := range nm.peerClients {
		peerIDs = append(peerIDs, id)
	}
	nm.mu.RUnlock()

	var wg sync.WaitGroup
	for _, id := range peerIDs {
		wg.Add(1)
		go func(peerID types.NodeID) {
			defer wg.Done()
			nm.mu.Lock()
			pc, exists := nm.peerClients[peerID]
			connToClose := pc.conn // To check conn existence after getting lock
			nm.mu.Unlock()

			if exists && connToClose != nil {
				nm.logger.Debugw("Closing client connection to peer", "peer_id", peerID)
				if cerr := connToClose.Close(); cerr != nil {
					nm.logger.Warnw(
						"Error closing connection to peer",
						"peer_id",
						peerID,
						"error",
						cerr,
					)
				}
			}
		}(id)
	}
	wg.Wait()
	nm.logger.Debugw("Finished closing client connections")

	if nm.server != nil && nm.serverStarted.Load() {
		nm.logger.Debugw("Initiating graceful stop of gRPC server")
		nm.server.GracefulStop() // Waits for active RPCs
		nm.logger.Debugw("gRPC server gracefully stopped")
	}

	nm.logger.Infow("Network manager stopped")
	return nil
}

// SendRequestVote sends a RequestVote RPC to a target peer.
func (nm *gRPCNetworkManager) SendRequestVote(
	ctx context.Context,
	target types.NodeID,
	args *types.RequestVoteArgs,
) (*types.RequestVoteReply, error) {
	if nm.isShutdown.Load() {
		return nil, ErrShuttingDown
	}

	client, err := nm.getOrCreatePeerClient(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("SendRequestVote: failed to get client for peer %s: %w", target, err)
	}

	startTime := nm.clock.Now()
	client.pendingOps.Add(1)
	defer client.pendingOps.Add(-1)

	req := &proto.RequestVoteRequest{
		Term:         uint64(args.Term),
		CandidateId:  string(args.CandidateID),
		LastLogIndex: uint64(args.LastLogIndex),
		LastLogTerm:  uint64(args.LastLogTerm),
		IsPreVote:    args.IsPreVote,
	}

	nm.logger.Debugw(
		"Sending RequestVote RPC",
		"target",
		target,
		"term",
		args.Term,
		"is_prevote",
		args.IsPreVote,
	)

	resp, err := client.client.RequestVote(ctx, req)
	latency := nm.clock.Since(startTime)
	nm.metrics.ObserveHistogram(
		"grpc_client_rpc_latency_seconds",
		latency.Seconds(),
		"rpc",
		"RequestVote",
		"peer_id",
		string(target),
	)

	if err != nil {
		nm.logger.Debugw(
			"RequestVote RPC failed",
			"target",
			target,
			"term",
			args.Term,
			"error",
			err,
			"latency",
			latency,
		)
		client.lastError = err
		client.connected.Store(false)
		nm.metrics.IncCounter(
			"grpc_client_rpc_failures_total",
			"rpc",
			"RequestVote",
			"peer_id",
			string(target),
		)
		return nil, formatGRPCError(err)
	}

	client.lastActive = nm.clock.Now()
	client.connected.Store(true)
	client.lastError = nil

	reply := &types.RequestVoteReply{
		Term:        types.Term(resp.Term),
		VoteGranted: resp.VoteGranted,
	}

	nm.logger.Debugw(
		"RequestVote RPC succeeded",
		"target",
		target,
		"term",
		args.Term,
		"vote_granted",
		reply.VoteGranted,
		"reply_term",
		reply.Term,
		"latency",
		latency,
	)
	nm.metrics.IncCounter(
		"grpc_client_rpc_success_total",
		"rpc",
		"RequestVote",
		"peer_id",
		string(target),
	)

	return reply, nil
}

// SendAppendEntries sends an AppendEntries RPC to a target peer.
func (nm *gRPCNetworkManager) SendAppendEntries(
	ctx context.Context,
	target types.NodeID,
	args *types.AppendEntriesArgs,
) (*types.AppendEntriesReply, error) {
	if nm.isShutdown.Load() {
		return nil, ErrShuttingDown
	}

	client, err := nm.getOrCreatePeerClient(ctx, target)
	if err != nil {
		return nil, fmt.Errorf(
			"SendAppendEntries: failed to get client for peer %s: %w",
			target,
			err,
		)
	}

	startTime := nm.clock.Now()
	client.pendingOps.Add(1)
	defer client.pendingOps.Add(-1)

	isHeartbeat := len(args.Entries) == 0
	rpcType := "AppendEntries"
	if isHeartbeat {
		rpcType = "Heartbeat"
	}

	entries := make([]*proto.LogEntry, len(args.Entries))
	var totalDataSize int64
	for i, entry := range args.Entries {
		entries[i] = &proto.LogEntry{
			Term:    uint64(entry.Term),
			Index:   uint64(entry.Index),
			Command: entry.Command,
		}
		totalDataSize += int64(len(entry.Command))
	}

	req := &proto.AppendEntriesRequest{
		Term:         uint64(args.Term),
		LeaderId:     string(args.LeaderID),
		PrevLogIndex: uint64(args.PrevLogIndex),
		PrevLogTerm:  uint64(args.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: uint64(args.LeaderCommit),
	}

	if !isHeartbeat {
		nm.logger.Debugw(
			"Sending AppendEntries RPC",
			"target",
			target,
			"term",
			args.Term,
			"entries_count",
			len(args.Entries),
			"prev_idx",
			args.PrevLogIndex,
			"prev_term",
			args.PrevLogTerm,
		)
	} else {
		nm.logger.Debugw("Sending Heartbeat", "target", target, "term", args.Term, "prev_idx", args.PrevLogIndex, "prev_term", args.PrevLogTerm)
	}

	resp, err := client.client.AppendEntries(ctx, req)
	latency := nm.clock.Since(startTime)
	nm.metrics.ObserveHistogram(
		"grpc_client_rpc_latency_seconds",
		latency.Seconds(),
		"rpc",
		rpcType,
		"peer_id",
		string(target),
	)

	if err != nil {
		logLevel := nm.logger.Debugw
		if !isHeartbeat {
			logLevel = nm.logger.Warnw
		}
		logLevel(
			rpcType+" RPC failed",
			"target",
			target,
			"term",
			args.Term,
			"error",
			err,
			"latency",
			latency,
		)
		client.lastError = err
		client.connected.Store(false)
		nm.metrics.IncCounter(
			"grpc_client_rpc_failures_total",
			"rpc",
			rpcType,
			"peer_id",
			string(target),
		)
		return nil, formatGRPCError(err)
	}

	client.lastActive = nm.clock.Now()
	client.connected.Store(true)
	client.lastError = nil

	reply := &types.AppendEntriesReply{
		Term:          types.Term(resp.Term),
		Success:       resp.Success,
		ConflictIndex: types.Index(resp.ConflictIndex),
		ConflictTerm:  types.Term(resp.ConflictTerm),
		MatchIndex:    types.Index(resp.MatchIndex),
	}

	logFields := []any{
		"target", target, "reply_term", reply.Term, "success", reply.Success, "latency", latency,
	}
	if !isHeartbeat || !reply.Success {
		logFields = append(
			logFields,
			"term",
			args.Term,
			"entries_count",
			len(args.Entries),
			"reply_conflict_idx",
			reply.ConflictIndex,
			"reply_conflict_term",
			reply.ConflictTerm,
			"reply_match_idx",
			reply.MatchIndex,
		)
		nm.logger.Debugw(rpcType+" RPC response", logFields...)
	} else {
		nm.logger.Debugw(rpcType+" successful", logFields...)
	}
	nm.metrics.IncCounter(
		"grpc_client_rpc_success_total",
		"rpc",
		rpcType,
		"peer_id",
		string(target),
	)
	if !isHeartbeat {
		nm.metrics.AddCounter(
			"grpc_client_sent_bytes_total",
			float64(totalDataSize),
			"rpc",
			rpcType,
			"peer_id",
			string(target),
		)
		nm.metrics.AddCounter(
			"grpc_client_sent_entries_total",
			float64(len(entries)),
			"rpc",
			rpcType,
			"peer_id",
			string(target),
		)
	}

	return reply, nil
}

// SendInstallSnapshot sends an InstallSnapshot RPC to a target peer.
func (nm *gRPCNetworkManager) SendInstallSnapshot(
	ctx context.Context,
	target types.NodeID,
	args *types.InstallSnapshotArgs,
) (*types.InstallSnapshotReply, error) {
	if nm.isShutdown.Load() {
		return nil, ErrShuttingDown
	}

	client, err := nm.getOrCreatePeerClient(ctx, target)
	if err != nil {
		return nil, fmt.Errorf(
			"SendInstallSnapshot: failed to get client for peer %s: %w",
			target,
			err,
		)
	}

	startTime := nm.clock.Now()
	client.pendingOps.Add(1)
	defer client.pendingOps.Add(-1)

	req := &proto.InstallSnapshotRequest{
		Term:              uint64(args.Term),
		LeaderId:          string(args.LeaderID),
		LastIncludedIndex: uint64(args.LastIncludedIndex),
		LastIncludedTerm:  uint64(args.LastIncludedTerm),
		Data:              args.Data,
	}
	dataSize := len(args.Data)

	nm.logger.Infow(
		"Sending InstallSnapshot RPC",
		"target",
		target,
		"term",
		args.Term,
		"last_included_index",
		args.LastIncludedIndex,
		"last_included_term",
		args.LastIncludedTerm,
		"data_size",
		dataSize,
	)

	resp, err := client.client.InstallSnapshot(ctx, req)
	latency := nm.clock.Since(startTime)
	nm.metrics.ObserveHistogram(
		"grpc_client_rpc_latency_seconds",
		latency.Seconds(),
		"rpc",
		"InstallSnapshot",
		"peer_id",
		string(target),
	)

	if err != nil {
		nm.logger.Warnw(
			"InstallSnapshot RPC failed",
			"target",
			target,
			"term",
			args.Term,
			"error",
			err,
			"latency",
			latency,
		)
		client.lastError = err
		client.connected.Store(false)
		nm.metrics.IncCounter(
			"grpc_client_rpc_failures_total",
			"rpc",
			"InstallSnapshot",
			"peer_id",
			string(target),
		)
		return nil, formatGRPCError(err)
	}

	client.lastActive = nm.clock.Now()
	client.connected.Store(true)
	client.lastError = nil

	reply := &types.InstallSnapshotReply{
		Term: types.Term(resp.Term),
	}

	nm.logger.Infow(
		"InstallSnapshot RPC succeeded",
		"target",
		target,
		"term",
		args.Term,
		"reply_term",
		reply.Term,
		"latency",
		latency,
	)
	nm.metrics.IncCounter(
		"grpc_client_rpc_success_total",
		"rpc",
		"InstallSnapshot",
		"peer_id",
		string(target),
	)
	nm.metrics.AddCounter(
		"grpc_client_sent_bytes_total",
		float64(dataSize),
		"rpc",
		"InstallSnapshot",
		"peer_id",
		string(target),
	)

	return reply, nil
}

// PeerStatus returns health and connection info for a known peer.
func (nm *gRPCNetworkManager) PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error) {
	if nm.isShutdown.Load() {
		return types.PeerConnectionStatus{}, ErrShuttingDown
	}

	nm.mu.RLock()
	defer nm.mu.RUnlock()

	client, exists := nm.peerClients[peer]
	if !exists {
		if _, cfgExists := nm.peers[peer]; !cfgExists {
			return types.PeerConnectionStatus{}, ErrPeerNotFound
		}
		return types.PeerConnectionStatus{Connected: false}, nil
	}

	return types.PeerConnectionStatus{
		Connected:   client.connected.Load(),
		LastError:   client.lastError,
		LastActive:  client.lastActive,
		PendingRPCs: int(client.pendingOps.Load()),
	}, nil
}

// LocalAddr returns the network address the manager's server is listening on.
func (nm *gRPCNetworkManager) LocalAddr() string {
	if nm.listener != nil {
		return nm.listener.Addr().String()
	}
	return nm.localAddr
}

// formatGRPCError converts gRPC status errors into more specific Raft or context errors.
func formatGRPCError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrTimeout
		}
		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}
		return fmt.Errorf("network error: %w", err)
	}

	switch st.Code() {
	case codes.DeadlineExceeded:
		return ErrTimeout
	case codes.Canceled:
		return context.Canceled
	case codes.Unavailable:
		return fmt.Errorf("peer unavailable: %w", err)
	case codes.Aborted:
		return ErrShuttingDown
	default:
		return err
	}
}

// grpcServerHandler implements the proto.RaftServer interface.
type grpcServerHandler struct {
	proto.UnimplementedRaftServer
	nm         *gRPCNetworkManager
	rpcHandler RPCHandler
	logger     logger.Logger
}

// RequestVote handles incoming RequestVote RPCs.
func (h *grpcServerHandler) RequestVote(
	ctx context.Context,
	req *proto.RequestVoteRequest,
) (*proto.RequestVoteResponse, error) {
	rpc := "RequestVote"
	if h.nm.isShutdown.Load() {
		h.logger.Debugw("Rejecting RPC: server shutting down", "rpc", rpc, "from", req.CandidateId)
		h.nm.metrics.IncCounter("grpc_server_rpc_rejected_total", "rpc", rpc, "reason", "shutdown")
		return nil, status.Error(codes.Aborted, "server shutting down")
	}

	startTime := h.nm.clock.Now()

	args := &types.RequestVoteArgs{
		Term:         types.Term(req.Term),
		CandidateID:  types.NodeID(req.CandidateId),
		LastLogIndex: types.Index(req.LastLogIndex),
		LastLogTerm:  types.Term(req.LastLogTerm),
		IsPreVote:    req.IsPreVote,
	}

	h.logger.Debugw("Received RPC", "rpc", rpc,
		"from", args.CandidateID, "term", args.Term, "is_prevote", args.IsPreVote,
		"last_log_index", args.LastLogIndex, "last_log_term", args.LastLogTerm)
	h.nm.metrics.IncCounter("grpc_server_rpc_received_total", "rpc", rpc)

	reply, err := h.rpcHandler.RequestVote(ctx, args)
	latency := h.nm.clock.Since(startTime)
	h.nm.metrics.ObserveHistogram("grpc_server_rpc_latency_seconds", latency.Seconds(), "rpc", rpc)

	if err != nil {
		h.logger.Warnw(
			"Internal handler failed for RPC",
			"rpc",
			rpc,
			"error",
			err,
			"from",
			args.CandidateID,
		)
		h.nm.metrics.IncCounter("grpc_server_rpc_handler_errors_total", "rpc", rpc)

		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, "handler deadline exceeded")
		}
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, "handler context canceled")
		}

		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("internal raft handler error: %v", err),
		)
	}

	resp := &proto.RequestVoteResponse{
		Term:        uint64(reply.Term),
		VoteGranted: reply.VoteGranted,
	}

	h.logger.Debugw(
		"Sending RPC reply",
		"rpc",
		rpc,
		"to",
		args.CandidateID,
		"term",
		resp.Term,
		"granted",
		resp.VoteGranted,
	)
	h.nm.metrics.IncCounter("grpc_server_rpc_success_total", "rpc", rpc)
	return resp, nil
}

// AppendEntries handles incoming AppendEntries RPCs.
func (h *grpcServerHandler) AppendEntries(
	ctx context.Context,
	req *proto.AppendEntriesRequest,
) (*proto.AppendEntriesResponse, error) {
	isHeartbeat := len(req.Entries) == 0
	rpcType := "AppendEntries"
	if isHeartbeat {
		rpcType = "Heartbeat"
	}

	if h.nm.isShutdown.Load() {
		if h.logger != nil {
			h.logger.Debugw(
				"Rejecting RPC: server shutting down",
				"rpc",
				rpcType,
				"from",
				req.LeaderId,
			)
		}
		if h.nm != nil && h.nm.metrics != nil {
			h.nm.metrics.IncCounter(
				"grpc_server_rpc_rejected_total",
				"rpc",
				rpcType,
				"reason",
				"shutdown",
			)
		}
		return nil, status.Error(codes.Aborted, "server shutting down")
	}

	startTime := h.nm.clock.Now()

	entries := make([]types.LogEntry, len(req.Entries))
	var totalDataSize int64
	for i, e := range req.Entries {
		entries[i] = types.LogEntry{
			Term:    types.Term(e.Term),
			Index:   types.Index(e.Index),
			Command: e.Command,
		}
		totalDataSize += int64(len(e.Command))
	}

	args := &types.AppendEntriesArgs{
		Term:         types.Term(req.Term),
		LeaderID:     types.NodeID(req.LeaderId),
		PrevLogIndex: types.Index(req.PrevLogIndex),
		PrevLogTerm:  types.Term(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: types.Index(req.LeaderCommit),
	}

	logFields := []any{"rpc", rpcType, "from", args.LeaderID, "term", args.Term}
	if !isHeartbeat {
		logFields = append(logFields,
			"entries_count", len(entries), "prev_log_index", args.PrevLogIndex,
			"prev_log_term", args.PrevLogTerm, "leader_commit", args.LeaderCommit)
	}
	h.logger.Debugw("Received RPC", logFields...)
	h.nm.metrics.IncCounter("grpc_server_rpc_received_total", "rpc", rpcType)
	if !isHeartbeat {
		h.nm.metrics.AddCounter(
			"grpc_server_received_bytes_total",
			float64(totalDataSize),
			"rpc",
			rpcType,
		)
		h.nm.metrics.AddCounter(
			"grpc_server_received_entries_total",
			float64(len(entries)),
			"rpc",
			rpcType,
		)
	}

	reply, err := h.rpcHandler.AppendEntries(ctx, args)
	latency := h.nm.clock.Since(startTime)
	h.nm.metrics.ObserveHistogram(
		"grpc_server_rpc_latency_seconds",
		latency.Seconds(),
		"rpc",
		rpcType,
	)

	if err != nil {
		h.logger.Warnw(
			"Internal handler failed for RPC",
			"rpc",
			rpcType,
			"error",
			err,
			"from",
			args.LeaderID,
		)
		h.nm.metrics.IncCounter("grpc_server_rpc_handler_errors_total", "rpc", rpcType)

		if s, ok := status.FromError(err); ok {
			return nil, s.Err()
		}
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("internal raft handler error: %v", err),
		)
	}

	resp := &proto.AppendEntriesResponse{
		Term:          uint64(reply.Term),
		Success:       reply.Success,
		ConflictIndex: uint64(reply.ConflictIndex),
		ConflictTerm:  uint64(reply.ConflictTerm),
		MatchIndex:    uint64(reply.MatchIndex),
	}

	replyLogFields := []any{
		"rpc",
		rpcType,
		"to",
		args.LeaderID,
		"term",
		resp.Term,
		"success",
		resp.Success,
	}
	if !isHeartbeat || !resp.Success {
		replyLogFields = append(
			replyLogFields,
			"conflict_idx",
			resp.ConflictIndex,
			"conflict_term",
			resp.ConflictTerm,
			"match_idx",
			resp.MatchIndex,
		)
	}
	h.logger.Debugw("Sending RPC reply", replyLogFields...)
	h.nm.metrics.IncCounter("grpc_server_rpc_success_total", "rpc", rpcType)

	return resp, nil
}

// InstallSnapshot handles incoming InstallSnapshot RPCs.
func (h *grpcServerHandler) InstallSnapshot(
	ctx context.Context,
	req *proto.InstallSnapshotRequest,
) (*proto.InstallSnapshotResponse, error) {
	rpc := "InstallSnapshot"
	if h.nm.isShutdown.Load() {
		h.logger.Debugw("Rejecting RPC: server shutting down", "rpc", rpc, "from", req.LeaderId)
		h.nm.metrics.IncCounter("grpc_server_rpc_rejected_total", "rpc", rpc, "reason", "shutdown")
		return nil, status.Error(codes.Aborted, "server shutting down")
	}

	startTime := h.nm.clock.Now()
	dataSize := len(req.Data)

	args := &types.InstallSnapshotArgs{
		Term:              types.Term(req.Term),
		LeaderID:          types.NodeID(req.LeaderId),
		LastIncludedIndex: types.Index(req.LastIncludedIndex),
		LastIncludedTerm:  types.Term(req.LastIncludedTerm),
		Data:              req.Data,
	}

	h.logger.Infow(
		"Received RPC",
		"rpc",
		rpc,
		"from",
		args.LeaderID,
		"term",
		args.Term,
		"last_included_index",
		args.LastIncludedIndex,
		"last_included_term",
		args.LastIncludedTerm,
		"data_size",
		dataSize,
	)
	h.nm.metrics.IncCounter("grpc_server_rpc_received_total", "rpc", rpc)
	h.nm.metrics.AddCounter("grpc_server_received_bytes_total", float64(dataSize), "rpc", rpc)

	reply, err := h.rpcHandler.InstallSnapshot(ctx, args)
	latency := h.nm.clock.Since(startTime)
	h.nm.metrics.ObserveHistogram("grpc_server_rpc_latency_seconds", latency.Seconds(), "rpc", rpc)

	if err != nil {
		h.logger.Warnw(
			"Internal handler failed for RPC",
			"rpc",
			rpc,
			"error",
			err,
			"from",
			args.LeaderID,
		)
		h.nm.metrics.IncCounter("grpc_server_rpc_handler_errors_total", "rpc", rpc)
		if s, ok := status.FromError(err); ok {
			return nil, s.Err()
		}
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("internal raft handler error: %v", err),
		)
	}

	resp := &proto.InstallSnapshotResponse{
		Term: uint64(reply.Term),
	}

	h.logger.Infow("Sending RPC reply", "rpc", rpc, "to", args.LeaderID, "term", resp.Term)
	h.nm.metrics.IncCounter("grpc_server_rpc_success_total", "rpc", rpc)

	return resp, nil
}

// ResetConnection forcefully closes the current gRPC connection to a peer
// and attempts to establish a new one. This is useful for recovering from
// persistent network errors where the gRPC client's internal reconnect
// logic may not be sufficient.
func (nm *gRPCNetworkManager) ResetConnection(ctx context.Context, peerID types.NodeID) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	pc, exists := nm.peerClients[peerID]
	if !exists {
		// If no connection state exists, create it. This will trigger a connection attempt.
		_, err := nm.getOrCreatePeerClientLocked(ctx, peerID)
		return err
	}

	nm.logger.Infow("Explicitly resetting connection to peer", "peer_id", peerID)

	// Close existing connection if it exists
	pc.mu.Lock()
	if pc.conn != nil {
		_ = pc.conn.Close()
		pc.conn = nil
	}
	pc.connected.Store(false)
	pc.mu.Unlock()

	// Attempt to reconnect immediately
	return nm.connectToPeerLocked(ctx, pc)
}

// getOrCreatePeerClient retrieves an active client connection, creating and
// connecting it if one doesn't exist or if the existing one is disconnected.
func (nm *gRPCNetworkManager) getOrCreatePeerClient(
	ctx context.Context,
	peerID types.NodeID,
) (*peerConnection, error) {
	nm.mu.RLock()
	client, exists := nm.peerClients[peerID]
	nm.mu.RUnlock()

	if exists && client.connected.Load() {
		return client, nil
	}

	nm.mu.Lock()
	defer nm.mu.Unlock()
	return nm.getOrCreatePeerClientLocked(ctx, peerID)
}

// getOrCreatePeerClientLocked is the lock-held version of getOrCreatePeerClient.
func (nm *gRPCNetworkManager) getOrCreatePeerClientLocked(
	ctx context.Context,
	peerID types.NodeID,
) (*peerConnection, error) {
	client, exists := nm.peerClients[peerID]
	if exists && client.connected.Load() {
		return client, nil // Another goroutine connected it
	}

	peerCfg, ok := nm.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("peer %s not found in configuration: %w", peerID, ErrPeerNotFound)
	}

	if !exists {
		client = &peerConnection{id: peerID, addr: peerCfg.Address}
		nm.peerClients[peerID] = client
	}

	if err := nm.connectToPeerLocked(ctx, client); err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %w", peerID, err)
	}

	return client, nil
}

// connectToPeerLocked establishes a gRPC connection. Assumes nm.mu is held.
func (nm *gRPCNetworkManager) connectToPeerLocked(
	ctx context.Context,
	client *peerConnection,
) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.conn != nil {
		_ = client.conn.Close()
	}

	dialKeepaliveParams := keepalive.ClientParameters{
		Time:                nm.opts.KeepaliveTime,
		Timeout:             nm.opts.KeepaliveTimeout,
		PermitWithoutStream: true,
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		), // TODO: Replace with secure credentials
		grpc.WithKeepaliveParams(dialKeepaliveParams),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		}),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"pick_first"}`),
	}

	nm.logger.Debugw("Attempting to connect to peer",
		"peer_id", client.id, "address", client.addr, "timeout", nm.opts.DialTimeout)

	conn, err := grpc.NewClient(client.addr, dialOpts...)
	if err != nil {
		client.lastError = err
		client.connected.Store(false)
		nm.logger.Warnw("Failed to connect to peer",
			"peer_id", client.id, "address", client.addr, "error", err)
		return fmt.Errorf("failed to connect to peer %s at %s: %w", client.id, client.addr, err)
	}

	client.conn = conn
	client.client = proto.NewRaftClient(conn)
	client.connected.Store(true)
	client.lastError = nil
	client.lastActive = nm.clock.Now()
	nm.logger.Infow("Successfully connected to peer", "peer_id", client.id, "address", client.addr)
	return nil
}
