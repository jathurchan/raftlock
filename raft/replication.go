package raft

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

type ReplicationManager interface {
	// SetNetworkManager allows late injection of the network manager after the Raft node is built.
	SetNetworkManager(nm NetworkManager)

	// Tick advances internal timers, sending heartbeats if needed.
	// Called only when this node is the leader.
	Tick(ctx context.Context)

	// InitializeLeaderState resets peer replication tracking (e.g., nextIndex and matchIndex)
	// based on the current log, and prepares the node to function as a leader.
	InitializeLeaderState()

	// Propose submits a client command to be appended to the Raft log and replicated.
	// If the node is the leader, it appends the entry and initiates replication.
	// Returns the index and term of the new entry, a boolean indicating leadership status,
	// and an error if the proposal fails (e.g., ErrNotLeader, storage failure).
	Propose(
		ctx context.Context,
		command []byte,
	) (index types.Index, term types.Term, isLeader bool, err error)

	// SendHeartbeats dispatches AppendEntries RPCs with no log entries to all peers
	// to assert leadership and maintain authority by resetting their election timers.
	// Should only be called by the leader.
	SendHeartbeats(ctx context.Context)

	// MaybeAdvanceCommitIndex determines the highest log index replicated to a quorum
	// and advances the commit index if it is safe to do so.
	// Only entries from the current leader term are eligible for commitment.
	// Should only be called by the leader.
	MaybeAdvanceCommitIndex()

	// HasValidLease returns whether the leader still holds a valid lease (if lease-based
	// reads are enabled), ensuring safe servicing of linearizable reads.
	// Should only be called by the leader.
	HasValidLease(ctx context.Context) bool

	// VerifyLeadershipAndGetCommitIndex verifies leadership by reaching a quorum,
	// and returns the current commit index for use in ReadIndex-based linearizable reads.
	// Should only be called by the leader.
	VerifyLeadershipAndGetCommitIndex(ctx context.Context) (types.Index, error)

	// HandleAppendEntries processes an incoming AppendEntries RPC from the leader.
	// Called by the RPC handler when this node is acting as a follower or candidate.
	HandleAppendEntries(
		ctx context.Context,
		args *types.AppendEntriesArgs,
	) (*types.AppendEntriesReply, error)

	// GetPeerReplicationStatus returns the current replication progress for each peer,
	// primarily used for metrics, monitoring, or debugging. Requires leader role.
	// Caller must hold the appropriate lock.
	GetPeerReplicationStatusUnsafe() map[types.NodeID]types.PeerState

	// ReplicateToPeer initiates a log replication attempt to the specified peer,
	// either by sending AppendEntries or triggering a snapshot if necessary.
	// This method is primarily exposed for testing or advanced usage. Requires leader role.
	ReplicateToPeer(ctx context.Context, peerID types.NodeID, isHeartbeat bool)

	// UpdatePeerAfterSnapshotSend updates replication tracking for a peer
	// after a snapshot is successfully sent, typically advancing nextIndex. Requires leader role.
	UpdatePeerAfterSnapshotSend(peerID types.NodeID, snapshotIndex types.Index)

	// SetPeerSnapshotInProgress marks whether a snapshot is currently in progress
	// for a given peer, preventing redundant snapshot attempts. Requires leader role.
	SetPeerSnapshotInProgress(peerID types.NodeID, inProgress bool)

	// Stop shuts down background tasks and releases resources.
	Stop()
}

// ReplicationManagerDeps encapsulates dependencies for creating a replicationManager.
type ReplicationManagerDeps struct {
	Mu             *sync.RWMutex
	ID             types.NodeID
	Peers          map[types.NodeID]PeerConfig
	QuorumSize     int
	Cfg            Config
	StateMgr       StateManager
	LogMgr         LogManager
	SnapshotMgr    SnapshotManager
	NetworkMgr     NetworkManager
	Metrics        Metrics
	Logger         logger.Logger
	Clock          Clock
	IsShutdownFlag *atomic.Bool
	ApplyNotifyCh  chan struct{}
}

// validateReplicationManagerDeps checks that all required dependencies are set.
// Returns an error describing the first missing or invalid field.
func validateReplicationManagerDeps(deps ReplicationManagerDeps) error {
	check := func(cond bool, fieldName string) error {
		if !cond {
			return fmt.Errorf("ReplicationManagerDeps: %s is required", fieldName)
		}
		return nil
	}
	checkPtr := func(ptr any, fieldName string) error {
		v := reflect.ValueOf(ptr)
		if v.Kind() == reflect.Ptr && v.IsNil() {
			return fmt.Errorf("ReplicationManagerDeps: %s is required", fieldName)
		}
		return nil
	}

	if err := checkPtr(deps.Mu, "Mu"); err != nil {
		return err
	}
	if err := check(deps.ID != "", "ID"); err != nil {
		return err
	}
	if err := checkPtr(deps.StateMgr, "StateMgr"); err != nil {
		return err
	}
	if err := checkPtr(deps.LogMgr, "LogMgr"); err != nil {
		return err
	}
	if err := checkPtr(deps.SnapshotMgr, "SnapshotMgr"); err != nil {
		return err
	}
	if err := checkPtr(deps.Metrics, "Metrics"); err != nil {
		return err
	}
	if err := checkPtr(deps.Logger, "Logger"); err != nil {
		return err
	}
	if err := checkPtr(deps.Clock, "Clock"); err != nil {
		return err
	}
	if err := checkPtr(deps.ApplyNotifyCh, "ApplyNotifyCh"); err != nil {
		return err
	}
	if err := checkPtr(deps.IsShutdownFlag, "IsShutdownFlag"); err != nil {
		return err
	}
	if deps.QuorumSize <= 0 {
		return fmt.Errorf("ReplicationManagerDeps: QuorumSize must be > 0, got %d", deps.QuorumSize)
	}

	if deps.Peers == nil {
		return fmt.Errorf("ReplicationManagerDeps: Peers map is required (can be empty)")
	}
	return nil
}

// replicationManager is the concrete implementation of the ReplicationManager interface.
type replicationManager struct {
	mu         *sync.RWMutex // Raft's mutex protecting state fields
	isShutdown *atomic.Bool  // Shared flag indicating Raft shutdown

	id         types.NodeID                // ID of the local Raft node.
	peers      map[types.NodeID]PeerConfig // Configuration of peer nodes (READ-ONLY)
	quorumSize int                         // Number of votes needed to win an election (majority)
	cfg        Config

	stateMgr    StateManager
	logMgr      LogManager
	snapshotMgr SnapshotManager
	networkMgr  NetworkManager
	metrics     Metrics
	logger      logger.Logger
	clock       Clock

	// Only valid when role is Leader
	peerStates       map[types.NodeID]*types.PeerState // Tracks per-peer replication progress
	heartbeatElapsed int                               // Ticks since last heartbeat broadcast

	// Guarded by mu
	leaderLeaseEnabled  bool          // Whether leader lease-based reads are enabled
	leaseExpiry         time.Time     // Timestamp when the current lease expires
	leaseDuration       time.Duration // Duration of the leader lease
	lastQuorumHeartbeat time.Time     // Time of last successful quorum heartbeat

	// Signaling pipeline:
	// replication → notifyCommitCh → commit check → applyNotifyCh → state machine
	applyNotifyCh  chan struct{} // Signals Raft core to apply newly committed log entries
	notifyCommitCh chan struct{} // Triggers commit index evaluation after replication events
}

// NewReplicationManager constructs and initializes a new ReplicationManager instance.
func NewReplicationManager(deps ReplicationManagerDeps) (ReplicationManager, error) {
	if err := validateReplicationManagerDeps(deps); err != nil {
		return nil, fmt.Errorf("invalid replication manager dependencies: %w", err)
	}

	log := deps.Logger.WithComponent("replication")

	rm := &replicationManager{
		mu:                 deps.Mu,
		isShutdown:         deps.IsShutdownFlag,
		id:                 deps.ID,
		peers:              deps.Peers,
		quorumSize:         deps.QuorumSize,
		cfg:                deps.Cfg,
		networkMgr:         deps.NetworkMgr,
		stateMgr:           deps.StateMgr,
		logMgr:             deps.LogMgr,
		snapshotMgr:        deps.SnapshotMgr,
		metrics:            deps.Metrics,
		logger:             log,
		clock:              deps.Clock,
		applyNotifyCh:      deps.ApplyNotifyCh,
		notifyCommitCh:     make(chan struct{}, 5),
		peerStates:         make(map[types.NodeID]*types.PeerState),
		leaderLeaseEnabled: deps.Cfg.FeatureFlags.EnableLeaderLease,
	}

	for peerID := range rm.peers {
		if peerID != rm.id {
			rm.peerStates[peerID] = &types.PeerState{}
		}
	}

	if rm.leaderLeaseEnabled {
		electionTicks := rm.cfg.Options.ElectionTickCount
		if electionTicks <= 0 {
			log.Warnw(
				"Using default ElectionTickCount for lease calculation",
				"configured",
				electionTicks,
				"default",
				DefaultElectionTickCount,
			)
			electionTicks = DefaultElectionTickCount
		}

		rm.leaseDuration = time.Duration(electionTicks) * NominalTickInterval / 2

		log.Infow("Leader lease enabled",
			"leaseDuration", rm.leaseDuration,
			"basedOnElectionTicks", electionTicks)
	}

	log.Infow("Replication manager initialized",
		"peerCount", len(rm.peers)-1,
		"quorumSize", rm.quorumSize,
		"leaderLease", rm.leaderLeaseEnabled)

	return rm, nil
}

// SetNetworkManager injects the network manager dependency into the replication manager.
func (s *replicationManager) SetNetworkManager(nm NetworkManager) {
	s.networkMgr = nm
}

// Tick drives periodic replication tasks for the leader.
func (rm *replicationManager) Tick(ctx context.Context) {
	if rm.isShutdown.Load() {
		return
	}

	term, role, _ := rm.stateMgr.GetState()
	if role != types.RoleLeader {
		return
	}

	// Check for commit advancement first (this might spawn heartbeats)
	rm.MaybeAdvanceCommitIndex()

	// Drain any pending commit notifications
	commitCheckNeeded := false
	for {
		select {
		case <-rm.notifyCommitCh:
			commitCheckNeeded = true
		default:
			goto drainComplete
		}
	}

drainComplete:
	if commitCheckNeeded {
		rm.MaybeAdvanceCommitIndex()
	}

	// Handle regular heartbeat timing
	rm.mu.Lock()
	rm.heartbeatElapsed++
	elapsed := rm.heartbeatElapsed
	interval := rm.getHeartbeatInterval()

	shouldSendHeartbeats := elapsed >= interval
	if shouldSendHeartbeats {
		rm.heartbeatElapsed = 0
	}
	rm.mu.Unlock()

	if shouldSendHeartbeats {
		rm.logger.Debugw("Heartbeat interval reached, sending heartbeats",
			"elapsedTicks", elapsed,
			"intervalTicks", interval,
			"term", term)
		rm.SendHeartbeats(ctx)
	}
}

// getHeartbeatInterval returns the configured or default heartbeat tick interval.
func (rm *replicationManager) getHeartbeatInterval() int {
	interval := rm.cfg.Options.HeartbeatTickCount
	if interval <= 0 {
		return DefaultHeartbeatTickCount
	}
	return interval
}

// InitializeLeaderState resets peer replication state when this node becomes leader.
func (rm *replicationManager) InitializeLeaderState() {
	term, role, leaderID := rm.stateMgr.GetState()

	if role != types.RoleLeader || leaderID != rm.id {
		rm.logger.Warnw("InitializeLeaderState called but node is not leader",
			"currentRole", role.String(), "term", term)
		return
	}

	rm.mu.Lock()

	// Reset peer tracking
	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()
	rm.initializeAllPeerStatesLocked(lastLogIndex)

	// Initialize leader lease if enabled
	if rm.leaderLeaseEnabled {
		rm.leaseExpiry = rm.clock.Now().Add(rm.leaseDuration)
		rm.logger.Infow("Leader lease initialized",
			"term", term, "expiresAt", rm.leaseExpiry.Format(time.RFC3339Nano),
			"duration", rm.leaseDuration)
	}

	rm.mu.Unlock()

	rm.logger.Infow("Leader state initialized",
		"term", term, "lastLogIndex", lastLogIndex, "peerCount", len(rm.peers)-1)

	// CRITICAL: Send immediate heartbeats to assert leadership
	go rm.sendImmediateLeadershipHeartbeats(term)
}

// initializeAllPeerStatesLocked sets initial next/match indices for all followers.
// Assumes rm.mu Lock is held.
func (rm *replicationManager) initializeAllPeerStatesLocked(lastLogIndex types.Index) {
	now := rm.clock.Now()
	initialNextIndex := lastLogIndex + 1

	for peerID := range rm.peerStates {
		rm.peerStates[peerID] = &types.PeerState{
			NextIndex:          initialNextIndex,
			MatchIndex:         0, // Follower starts with matchIndex 0
			IsActive:           true,
			LastActive:         now, // Assume active initially
			SnapshotInProgress: false,
		}
		rm.logger.Debugw("Initialized peer state for leader",
			"peerID", peerID,
			"nextIndex", initialNextIndex)
	}
}

func (rm *replicationManager) sendImmediateLeadershipHeartbeats(term types.Term) {
	// Validate we're still leader
	currentTerm, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id || currentTerm != term {
		rm.logger.Warnw("Cannot send leadership heartbeats - lost leadership",
			"originalTerm", term, "currentTerm", currentTerm,
			"role", role.String())
		return
	}

	rm.logger.Infow("Sending immediate leadership assertion heartbeats",
		"term", term, "leaderID", rm.id)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	commitIndex := rm.stateMgr.GetCommitIndex()
	var wg sync.WaitGroup

	// Send heartbeats to all peers concurrently
	for peerID := range rm.peers {
		if peerID == rm.id {
			continue
		}

		wg.Add(1)
		go func(peer types.NodeID) {
			defer wg.Done()

			heartbeatCtx, heartbeatCancel := context.WithTimeout(ctx, 1*time.Second)
			defer heartbeatCancel()

			rm.logger.Infow("Sending leadership assertion heartbeat",
				"peer", peer, "term", term, "commitIndex", commitIndex)

			rm.replicateToPeerInternal(heartbeatCtx, peer, term, commitIndex, true)
		}(peerID)
	}

	// Wait for all heartbeats to complete
	wg.Wait()
	rm.logger.Infow("Leadership assertion heartbeats completed", "term", term)
}

// initializeLeaderLeaseLocked sets the lease expiry and marks the start of the lease period.
// Assumes rm.mu Lock is held.
func (rm *replicationManager) initializeLeaderLeaseLocked(term types.Term) {
	now := rm.clock.Now()
	rm.leaseExpiry = now.Add(rm.leaseDuration)
	rm.lastQuorumHeartbeat = now // Lease starts now

	rm.logger.Infow("Leader lease initialized",
		"term", term,
		"expiresAt", rm.leaseExpiry.Format(time.RFC3339Nano),
		"duration", rm.leaseDuration)
}

// SendHeartbeats broadcasts heartbeat AppendEntries RPCs to all peers.
// It is a no-op if the node is not the current leader.
func (rm *replicationManager) SendHeartbeats(ctx context.Context) {
	if rm.isShutdown.Load() {
		return
	}

	term, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id {
		rm.logger.Debugw("Cannot send heartbeats - not leader",
			"role", role.String(), "leaderID", leaderID)
		return
	}

	commitIndex := rm.stateMgr.GetCommitIndex()

	rm.logger.Debugw("Sending heartbeats to all peers",
		"term", term, "commitIndex", commitIndex, "peerCount", len(rm.peers)-1)

	// Send heartbeats concurrently to all peers
	for peerID := range rm.peers {
		if peerID == rm.id {
			continue
		}

		go func(peer types.NodeID) {
			heartbeatCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			rm.replicateToPeerInternal(heartbeatCtx, peer, term, commitIndex, true)
		}(peerID)
	}
}

// replicateToPeerInternal handles log replication or heartbeat transmission to a specific peer.
// This is the core replication logic, called asynchronously.
func (rm *replicationManager) replicateToPeerInternal(
	ctx context.Context,
	peerID types.NodeID,
	term types.Term,
	commitIndex types.Index,
	isHeartbeat bool,
) {
	if rm.isShutdown.Load() {
		rm.logger.Debugw("replicateToPeerInternal aborted: shutdown",
			"peer", peerID, "term", term, "isHeartbeat", isHeartbeat)
		return
	}

	if ctx.Err() != nil {
		rm.logger.Debugw("replicateToPeerInternal aborted: context cancelled",
			"peer", peerID, "term", term, "isHeartbeat", isHeartbeat, "contextErr", ctx.Err())
		return
	}

	rm.logger.Infow("replicateToPeerInternal starting",
		"peer", peerID, "term", term, "commitIndex", commitIndex, "isHeartbeat", isHeartbeat)

	// Verify we're still the leader for this term
	currentTerm, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id || currentTerm != term {
		rm.logger.Warnw("replicateToPeerInternal aborted: leadership lost or term changed",
			"peer", peerID, "originalTerm", term, "currentTerm", currentTerm,
			"role", role.String(), "leaderID", leaderID, "expectedLeaderID", rm.id)
		return
	}

	// Get peer state safely with minimal lock scope
	nextIndex, snapshotInProgress := rm.getPeerNextIndexAndSnapshotStatus(peerID)
	if nextIndex == 0 {
		rm.logger.Errorw("Cannot replicate: peer state not found", "peerID", peerID)
		return
	}

	// Get log state (thread-safe operations)
	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()
	firstLogIndex := rm.logMgr.GetFirstIndex()

	rm.logger.Infow("Got replication info",
		"peer", peerID, "nextIndex", nextIndex, "snapshotInProgress", snapshotInProgress,
		"lastLogIndex", lastLogIndex, "firstLogIndex", firstLogIndex, "isHeartbeat", isHeartbeat)

	// Check if snapshot is needed
	if nextIndex < firstLogIndex {
		if snapshotInProgress {
			rm.logger.Debugw("Snapshot send already in progress for peer",
				"peerID", peerID, "nextIndex", nextIndex, "firstLogIndex", firstLogIndex)
			return
		}
		rm.logger.Infow("Snapshot needed for peer",
			"peerID", peerID, "nextIndex", nextIndex, "firstLogIndex", firstLogIndex)
		rm.initiateSnapshotIfNeeded(peerID, term)
		return
	}

	// For non-heartbeat calls, check if we actually need to replicate
	if !isHeartbeat && nextIndex > lastLogIndex {
		rm.logger.Infow("Peer is up-to-date, no replication needed",
			"peerID", peerID, "nextIndex", nextIndex, "lastLogIndex", lastLogIndex)
		return
	}

	// Get previous log info for consistency check
	prevLogIndex, prevLogTerm, prevLogOk := rm.getPrevLogInfoSafe(ctx, peerID, nextIndex, term)
	if !prevLogOk {
		rm.logger.Warnw("Failed to get prev log info, aborting replication",
			"peer", peerID, "nextIndex", nextIndex, "term", term)
		return
	}

	rm.logger.Debugw("Got prev log info",
		"peer", peerID, "prevLogIndex", prevLogIndex, "prevLogTerm", prevLogTerm, "nextIndex", nextIndex)

	// Get entries to send (if not a heartbeat)
	var entries []types.LogEntry
	var entriesErr error

	if !isHeartbeat && nextIndex <= lastLogIndex {
		rm.logger.Infow("Getting entries for peer",
			"peer", peerID, "nextIndex", nextIndex, "lastLogIndex", lastLogIndex,
			"expectedEntries", lastLogIndex-nextIndex+1)

		entries, entriesErr = rm.getEntriesForPeerSafe(ctx, peerID, nextIndex, lastLogIndex, term)
		if entriesErr != nil {
			rm.logger.Warnw("Failed to get entries for peer",
				"peer", peerID, "nextIndex", nextIndex, "lastLogIndex", lastLogIndex, "error", entriesErr)
			return
		}

		if entries != nil && len(entries) > 0 {
			rm.logger.Infow("Successfully got entries for peer",
				"peer", peerID, "entriesCount", len(entries), "nextIndex", nextIndex,
				"lastLogIndex", lastLogIndex, "firstEntryIndex", entries[0].Index,
				"lastEntryIndex", entries[len(entries)-1].Index)
		}
	} else {
		rm.logger.Debugw("No entries needed for this replication",
			"peer", peerID, "isHeartbeat", isHeartbeat, "nextIndex", nextIndex, "lastLogIndex", lastLogIndex)
	}

	// Build AppendEntries RPC arguments
	args := rm.buildAppendEntriesArgs(term, commitIndex, prevLogIndex, prevLogTerm, entries)

	// Log the replication attempt
	if len(entries) > 0 {
		rm.logger.Infow("Sending AppendEntries with entries to peer",
			"peer", peerID, "entriesCount", len(entries), "firstEntry", entries[0].Index,
			"lastEntry", entries[len(entries)-1].Index, "prevLogIndex", prevLogIndex,
			"prevLogTerm", prevLogTerm, "commitIndex", commitIndex, "term", term)
	} else {
		rm.logger.Debugw("Sending heartbeat to peer",
			"peer", peerID, "prevLogIndex", prevLogIndex, "prevLogTerm", prevLogTerm,
			"commitIndex", commitIndex, "term", term)
	}

	// Send the RPC with timeout
	rpcCtx, cancel := context.WithTimeout(ctx, rm.getAppendEntriesTimeout(isHeartbeat))
	defer cancel()

	startTime := rm.clock.Now()
	reply, err := rm.networkMgr.SendAppendEntries(rpcCtx, peerID, args)
	rpcLatency := rm.clock.Since(startTime)

	// Handle the response
	if err != nil {
		rm.logger.Warnw("AppendEntries RPC failed",
			"peer", peerID, "error", err, "isHeartbeat", isHeartbeat,
			"entriesCount", len(entries), "latency", rpcLatency)
		rm.handleAppendError(peerID, term, err, isHeartbeat, rpcLatency)
	} else {
		rm.logger.Infow("AppendEntries RPC succeeded",
			"peer", peerID, "success", reply.Success, "replyTerm", reply.Term,
			"isHeartbeat", isHeartbeat, "entriesCount", len(entries), "latency", rpcLatency)
		rm.handleAppendReply(peerID, term, args, reply, isHeartbeat, rpcLatency)
	}

	rm.logger.Infow("replicateToPeerInternal completed",
		"peer", peerID, "term", term, "isHeartbeat", isHeartbeat,
		"entriesCount", len(entries), "success", err == nil && reply != nil && reply.Success)
}

// getPeerNextIndexAndSnapshotStatus safely gets peer replication state with minimal lock scope
func (rm *replicationManager) getPeerNextIndexAndSnapshotStatus(peerID types.NodeID) (nextIndex types.Index, snapshotInProgress bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	state, exists := rm.peerStates[peerID]
	if !exists {
		return 0, false
	}
	return state.NextIndex, state.SnapshotInProgress
}

// getReplicationInfo safely gets all replication info needed without holding locks for too long
func (rm *replicationManager) getReplicationInfo(peerID types.NodeID) (nextIndex types.Index, snapshotInProgress bool, lastLogIndex, firstLogIndex types.Index) {
	// Get peer state
	rm.mu.RLock()
	state, exists := rm.peerStates[peerID]
	if !exists {
		rm.mu.RUnlock()
		return 0, false, 0, 0
	}
	nextIndex = state.NextIndex
	snapshotInProgress = state.SnapshotInProgress
	rm.mu.RUnlock()

	// Get log indices (these methods are thread-safe)
	lastLogIndex = rm.logMgr.GetLastIndexUnsafe()
	firstLogIndex = rm.logMgr.GetFirstIndex()

	return nextIndex, snapshotInProgress, lastLogIndex, firstLogIndex
}

// getEntriesForPeerSafe retrieves entries without holding locks for too long
func (rm *replicationManager) getEntriesForPeerSafe(
	ctx context.Context,
	peerID types.NodeID,
	nextIndex, lastLogIndex types.Index,
	term types.Term,
) ([]types.LogEntry, error) {
	rm.logger.Infow("getEntriesForPeerSafeNew called",
		"peer", peerID, "nextIndex", nextIndex, "lastLogIndex", lastLogIndex, "term", term)

	// Determine batch size
	maxEntries := rm.cfg.Options.MaxLogEntriesPerRequest
	if maxEntries <= 0 {
		maxEntries = 100 // Default reasonable batch size
	}

	endIndex := min(nextIndex+types.Index(maxEntries), lastLogIndex+1)

	rm.logger.Infow("getEntriesForPeerSafeNew range calculated",
		"peer", peerID, "nextIndex", nextIndex, "endIndex", endIndex,
		"maxEntries", maxEntries, "lastLogIndex", lastLogIndex, "requestedRange", endIndex-nextIndex)

	if nextIndex >= endIndex {
		rm.logger.Infow("getEntriesForPeerSafeNew: no entries needed (nextIndex >= endIndex)",
			"peer", peerID, "nextIndex", nextIndex, "endIndex", endIndex)
		return []types.LogEntry{}, nil // Return empty slice, not nil
	}

	// Use a reasonable timeout for fetching entries
	fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	rm.logger.Infow("getEntriesForPeerSafeNew: calling logMgr.GetEntries",
		"peer", peerID, "start", nextIndex, "end", endIndex, "expectedCount", endIndex-nextIndex)

	entries, err := rm.logMgr.GetEntries(fetchCtx, nextIndex, endIndex)

	rm.logger.Infow("getEntriesForPeerSafeNew: logMgr.GetEntries returned",
		"peer", peerID, "entriesCount", len(entries), "error", err,
		"requestedStart", nextIndex, "requestedEnd", endIndex)

	if err != nil {
		if errors.Is(err, ErrCompacted) {
			rm.logger.Infow("Required log entries compacted, initiating snapshot",
				"peerID", peerID, "requestedStart", nextIndex, "requestedEnd", endIndex)
			rm.startSnapshotForPeer(peerID, term)
			return nil, fmt.Errorf("log entries compacted: %w", err)
		}

		rm.logger.Warnw("Failed to get log entries for peer",
			"peerID", peerID, "start", nextIndex, "end", endIndex, "error", err)
		rm.metrics.ObservePeerReplication(peerID, false, ReplicationResultFailed)
		return nil, fmt.Errorf("failed to get log entries: %w", err)
	}

	if len(entries) == 0 && endIndex > nextIndex {
		rm.logger.Warnw("LogManager.GetEntries returned empty slice for non-empty range",
			"peerID", peerID, "nextIndex", nextIndex, "endIndex", endIndex, "expectedCount", endIndex-nextIndex)
		return nil, fmt.Errorf("log manager returned empty entries for range [%d, %d)", nextIndex, endIndex)
	}

	// Validate the returned entries
	if len(entries) > 0 {
		expectedFirstIndex := nextIndex
		actualFirstIndex := entries[0].Index
		actualLastIndex := entries[len(entries)-1].Index

		if actualFirstIndex != expectedFirstIndex {
			rm.logger.Errorw("GetEntries returned entries with wrong starting index",
				"peerID", peerID, "expectedFirstIndex", expectedFirstIndex,
				"actualFirstIndex", actualFirstIndex, "entriesCount", len(entries))
			return nil, fmt.Errorf("entries validation failed: expected first index %d, got %d",
				expectedFirstIndex, actualFirstIndex)
		}

		// Check that entries are contiguous
		for i := 1; i < len(entries); i++ {
			if entries[i].Index != entries[i-1].Index+1 {
				rm.logger.Errorw("GetEntries returned non-contiguous entries",
					"peerID", peerID, "entryAt", i-1, "index", entries[i-1].Index,
					"nextEntryAt", i, "index", entries[i].Index)
				return nil, fmt.Errorf("non-contiguous entries: index %d followed by %d",
					entries[i-1].Index, entries[i].Index)
			}
		}

		rm.logger.Infow("getEntriesForPeerSafeNew: entries validated successfully",
			"peer", peerID, "entriesCount", len(entries), "firstIndex", actualFirstIndex,
			"lastIndex", actualLastIndex, "expectedFirstIndex", expectedFirstIndex)
	}

	rm.logger.Infow("getEntriesForPeerSafeNew: returning entries",
		"peer", peerID, "entriesCount", len(entries),
		"firstIndex", func() types.Index {
			if len(entries) > 0 {
				return entries[0].Index
			}
			return 0
		}(),
		"lastIndex", func() types.Index {
			if len(entries) > 0 {
				return entries[len(entries)-1].Index
			}
			return 0
		}())

	return entries, nil
}

// getAppendEntriesTimeout returns appropriate timeout based on operation type
func (rm *replicationManager) getAppendEntriesTimeout(isHeartbeat bool) time.Duration {
	if isHeartbeat {
		return 1 * time.Second // Short timeout for heartbeats
	}
	return 3 * time.Second // Longer timeout for actual replication
}

// getPeerReplicationState safely returns the peer's nextIndex and snapshotInProgress status.
func (rm *replicationManager) getPeerReplicationState(
	peerID types.NodeID,
) (nextIndex types.Index, snapshotInProgress bool, ok bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	state, exists := rm.peerStates[peerID]
	if !exists {
		return 0, false, false
	}
	return state.NextIndex, state.SnapshotInProgress, true
}

// initiateSnapshotIfNeeded attempts to start a snapshot transfer if one isn't already running.
func (rm *replicationManager) initiateSnapshotIfNeeded(
	peerID types.NodeID,
	term types.Term,
) {
	if rm.startSnapshotIfNotInProgress(peerID, term) {
		rm.logger.Infow("Initiating snapshot transfer due to log gap",
			"peerID", peerID)
	}
}

// getPrevLogInfo returns the index and term of the entry preceding nextIndex.
// Returns ok=false if info cannot be retrieved (e.g., compacted).
func (rm *replicationManager) getPrevLogInfoSafe(
	ctx context.Context,
	peerID types.NodeID,
	nextIndex types.Index,
	term types.Term,
) (prevLogIndex types.Index, prevLogTerm types.Term, ok bool) {
	if nextIndex <= 1 {
		return 0, 0, true
	}

	prevLogIndex = nextIndex - 1

	// Use a short timeout for getting the term
	fetchCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	rm.logger.Debugw("getPrevLogInfoSafe: fetching term for prevLogIndex",
		"peer", peerID, "prevLogIndex", prevLogIndex)

	prevLogTerm, err := rm.logMgr.GetTerm(fetchCtx, prevLogIndex)
	if err != nil {
		if errors.Is(err, ErrCompacted) {
			rm.logger.Infow("Previous log entry compacted, initiating snapshot",
				"peerID", peerID, "prevLogIndex", prevLogIndex, "nextIndex", nextIndex)
			rm.startSnapshotForPeer(peerID, term)
		} else {
			rm.logger.Warnw("Failed to get term for prevLogIndex",
				"peerID", peerID, "prevLogIndex", prevLogIndex, "error", err)
		}
		return 0, 0, false
	}

	rm.logger.Debugw("getPrevLogInfoSafe: successfully got prevLog info",
		"peer", peerID, "prevLogIndex", prevLogIndex, "prevLogTerm", prevLogTerm)

	return prevLogIndex, prevLogTerm, true
}

// getEntriesForPeer retrieves a batch of log entries [nextIndex, lastLogIndex] for a peer.
// Handles log compaction by triggering snapshots. Returns nil if entries cannot be fetched or snapshot is needed.
func (rm *replicationManager) getEntriesForPeer(
	ctx context.Context,
	peerID types.NodeID,
	nextIndex, lastLogIndex types.Index,
	term types.Term,
) []types.LogEntry {
	rm.logger.Infow("getEntriesForPeer called",
		"peer", peerID,
		"nextIndex", nextIndex,
		"lastLogIndex", lastLogIndex,
		"term", term)

	maxEntries := rm.cfg.Options.MaxLogEntriesPerRequest
	if maxEntries <= 0 {
		maxEntries = DefaultMaxLogEntriesPerRequest
	}

	endIndex := min(nextIndex+types.Index(maxEntries), lastLogIndex+1)

	rm.logger.Infow("getEntriesForPeer range calculated",
		"peer", peerID,
		"nextIndex", nextIndex,
		"endIndex", endIndex,
		"maxEntries", maxEntries,
		"lastLogIndex", lastLogIndex)

	if nextIndex >= endIndex {
		rm.logger.Infow("getEntriesForPeer: no entries needed (nextIndex >= endIndex)",
			"peer", peerID,
			"nextIndex", nextIndex,
			"endIndex", endIndex)
		return nil
	}

	fetchCtx, cancel := context.WithTimeout(ctx, defaultLogFetchTimeout)
	defer cancel()

	rm.logger.Infow("getEntriesForPeer: about to call logMgr.GetEntries",
		"peer", peerID,
		"start", nextIndex,
		"end", endIndex)

	entries, err := rm.logMgr.GetEntries(fetchCtx, nextIndex, endIndex)

	rm.logger.Infow("getEntriesForPeer: logMgr.GetEntries returned",
		"peer", peerID,
		"entriesCount", len(entries),
		"error", err)

	if err != nil {
		if errors.Is(err, ErrCompacted) {
			rm.logger.Infow("Required log entries compacted, initiating snapshot",
				"peerID", peerID,
				"requestedStart", nextIndex,
				"requestedEnd", endIndex)

			rm.startSnapshotForPeer(peerID, term)
		} else {
			rm.logger.Warnw("Failed to get log entries for peer",
				"peerID", peerID,
				"start", nextIndex,
				"end", endIndex,
				"error", err)
			rm.metrics.ObservePeerReplication(peerID, false, ReplicationResultFailed)
		}
		return nil
	}

	if len(entries) == 0 && endIndex > nextIndex {
		rm.logger.Errorw("LogManager.GetEntries returned empty slice for non-empty range",
			"peerID", peerID,
			"nextIndex", nextIndex,
			"endIndex", endIndex)
		rm.metrics.ObservePeerReplication(peerID, false, ReplicationResultFailed)
		return nil
	}

	rm.logger.Infow("getEntriesForPeer: returning entries",
		"peer", peerID,
		"entriesCount", len(entries))

	return entries
}

// startSnapshotForPeer initiates a snapshot transfer due to log compaction or inconsistency.
func (rm *replicationManager) startSnapshotForPeer(peerID types.NodeID, term types.Term) {
	if rm.startSnapshotIfNotInProgress(peerID, term) {
		rm.logger.Infow("Starting snapshot due to log compaction or error",
			"peerID", peerID,
			"term", term)
	}
}

// startSnapshotIfNotInProgress is a shared helper that checks and sets the snapshot in-progress flag,
// then starts the snapshot send in a separate goroutine if not already running.
// Returns true if a snapshot was initiated, false if skipped.
func (rm *replicationManager) startSnapshotIfNotInProgress(
	peerID types.NodeID,
	term types.Term,
) bool {
	rm.mu.Lock()
	peerState, exists := rm.peerStates[peerID]
	if !exists {
		rm.mu.Unlock()
		rm.logger.Errorw("Cannot initiate snapshot: peer state not found", "peerID", peerID)
		return false
	}

	if peerState.SnapshotInProgress {
		rm.mu.Unlock()
		rm.logger.Debugw("Snapshot initiation skipped: already in progress", "peerID", peerID)
		return false
	}

	peerState.SnapshotInProgress = true
	rm.mu.Unlock()

	go rm.snapshotMgr.SendSnapshot(context.Background(), peerID, term)
	return true
}

// buildAppendEntriesArgs constructs the RPC request payload for AppendEntries.
func (rm *replicationManager) buildAppendEntriesArgs(
	term types.Term,
	commitIndex, prevLogIndex types.Index,
	prevLogTerm types.Term,
	entries []types.LogEntry,
) *types.AppendEntriesArgs {
	return &types.AppendEntriesArgs{
		Term:         term,
		LeaderID:     rm.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
}

// logReplicationAttempt emits debug logs for either a heartbeat or log entry transmission.
func (rm *replicationManager) logReplicationAttempt(
	peerID types.NodeID,
	isHeartbeat bool,
	entries []types.LogEntry,
	prevLogIndex types.Index,
) {
	if isHeartbeat || len(entries) == 0 {
		rm.logger.Debugw("Sending heartbeat", "to", peerID, "prevLogIndex", prevLogIndex)
	} else {
		rm.logger.Debugw("Sending entries",
			"to", peerID,
			"count", len(entries),
			"firstIndex", entries[0].Index,
			"lastIndex", entries[len(entries)-1].Index,
			"prevLogIndex", prevLogIndex)
	}
}

// handleAppendError processes errors encountered during AppendEntries RPCs.
func (rm *replicationManager) handleAppendError(
	peerID types.NodeID,
	term types.Term,
	err error,
	isHeartbeat bool,
	latency time.Duration,
) {
	level := rm.logger.Warnw
	if errors.Is(err, context.Canceled) {
		level = rm.logger.Debugw
	}

	level("AppendEntries RPC failed",
		"peerID", peerID,
		"term", term,
		"error", err,
		"isHeartbeat", isHeartbeat,
		"latency", latency)

	rm.mu.Lock()
	if peerState, exists := rm.peerStates[peerID]; exists {
		peerState.IsActive = false
	}
	rm.mu.Unlock()

	if isHeartbeat {
		rm.metrics.ObserveHeartbeat(peerID, false, latency)
	} else {
		rm.metrics.ObservePeerReplication(peerID, false, ReplicationResultFailed)
	}
}

// handleAppendReply processes AppendEntries replies from peers.
func (rm *replicationManager) handleAppendReply(
	peerID types.NodeID,
	term types.Term,
	sentArgs *types.AppendEntriesArgs,
	reply *types.AppendEntriesReply,
	isHeartbeat bool,
	rpcLatency time.Duration,
) {
	rm.logger.Debugw("Received AppendEntries reply",
		"peer", peerID,
		"term", term,
		"success", reply.Success,
		"replyTerm", reply.Term,
		"isHeartbeat", isHeartbeat,
		"latency", rpcLatency)

	// Pass an empty NodeID as the leader hint, because a reply from a follower
	// does not signify a new leader. This prevents the state manager from getting
	// confused and logging misleading warnings.
	steppedDown, previousLocalTerm := rm.stateMgr.CheckTermAndStepDown(
		context.Background(),
		reply.Term,
		"",
	)

	if steppedDown {
		rm.logger.Infow("Stepped down due to higher term in AppendEntries reply",
			"peerID", peerID,
			"replyTerm", reply.Term,
			"previousLocalTerm", previousLocalTerm)
		if isHeartbeat {
			rm.metrics.ObserveHeartbeat(peerID, false, rpcLatency)
		} else {
			rm.metrics.ObservePeerReplication(peerID, false, ReplicationResultStaleTerm)
		}
		return
	}

	if term != previousLocalTerm {
		rm.logger.Debugw("Ignoring stale AppendEntries reply for different term",
			"peerID", peerID,
			"requestTerm", term,
			"currentTerm", previousLocalTerm,
			"replyTerm", reply.Term)
		return
	}

	if reply.Success {
		rm.mu.Lock()
		updated := rm.updatePeerStateOnSuccessLocked(peerID, sentArgs, isHeartbeat, rpcLatency)
		rm.mu.Unlock()

		if updated {
			// Trigger commit check asynchronously to avoid blocking
			go rm.MaybeAdvanceCommitIndex()
		}
	} else {
		rm.handleLogInconsistency(peerID, term, reply, isHeartbeat, rpcLatency)
	}
}

// / updatePeerStateOnSuccessLocked updates peer state assuming the caller holds the lock.
// It returns true if the peer's matchIndex was advanced.
func (rm *replicationManager) updatePeerStateOnSuccessLocked(
	peerID types.NodeID,
	sentArgs *types.AppendEntriesArgs,
	isHeartbeat bool,
	rpcLatency time.Duration,
) bool {
	peerState, exists := rm.peerStates[peerID]
	if !exists {
		rm.logger.Errorw("Peer state not found during successful append handling", "peerID", peerID)
		return false
	}

	peerState.IsActive = true
	peerState.LastActive = rm.clock.Now()

	var matchIndexAdvanced bool

	// If entries were sent, success means the peer's log now matches up to the last new entry.
	if len(sentArgs.Entries) > 0 {
		newMatchIndex := sentArgs.Entries[len(sentArgs.Entries)-1].Index
		if newMatchIndex > peerState.MatchIndex {
			peerState.MatchIndex = newMatchIndex
			peerState.NextIndex = newMatchIndex + 1
			matchIndexAdvanced = true
		}
	} else {
		// If it was a heartbeat, success means the peer's log is consistent up to PrevLogIndex.
		// The leader's view of the follower's state should reflect this.
		if sentArgs.PrevLogIndex > peerState.MatchIndex {
			peerState.MatchIndex = sentArgs.PrevLogIndex
			matchIndexAdvanced = true
		}

		// Also ensure nextIndex is at least one greater than the now-confirmed matchIndex.
		if peerState.NextIndex <= peerState.MatchIndex {
			peerState.NextIndex = peerState.MatchIndex + 1
		}
	}

	if matchIndexAdvanced {
		rm.logger.Debugw("Updated peer replication state",
			"peerID", peerID,
			"newMatchIndex", peerState.MatchIndex,
			"newNextIndex", peerState.NextIndex)
	}

	if isHeartbeat {
		rm.metrics.ObserveHeartbeat(peerID, true, rpcLatency)
		rm.updateLeaderLeaseOnQuorum()
	} else {
		rm.metrics.ObservePeerReplication(peerID, true, ReplicationResultSuccess)
	}

	return matchIndexAdvanced
}

// updateLeaderLeaseOnQuorum checks if a quorum of heartbeats has been received recently
// and updates the lease expiry if so. This is a more robust way than updating on every single heartbeat ack.
func (rm *replicationManager) updateLeaderLeaseOnQuorum() {
	if !rm.leaderLeaseEnabled {
		return
	}

	now := rm.clock.Now()
	activeCount := 1 // Leader is always active

	for _, state := range rm.peerStates {
		if state.IsActive && now.Sub(state.LastActive) < rm.leaseDuration {
			activeCount++
		}
	}

	if activeCount >= rm.quorumSize {
		rm.leaseExpiry = now.Add(rm.leaseDuration)
		rm.lastQuorumHeartbeat = now
		rm.logger.Debugw("Leader lease renewed by quorum",
			"activeCount", activeCount,
			"quorumSize", rm.quorumSize,
			"newExpiry", rm.leaseExpiry.Format(time.RFC3339Nano))
	}
}

// updateLeaderLease refreshes the leader lease based on the current time.
// Called after confirming activity (e.g., successful heartbeat reply).
func (rm *replicationManager) updateLeaderLease() {
	if !rm.leaderLeaseEnabled {
		return
	}

	now := rm.clock.Now()
	newExpiry := now.Add(rm.leaseDuration)

	expiryExtended := false
	if newExpiry.After(rm.leaseExpiry) {
		rm.leaseExpiry = newExpiry
		expiryExtended = true
	}

	rm.lastQuorumHeartbeat = now

	if expiryExtended {
		rm.logger.Debugw("Leader lease expiry extended",
			"now", now.Format(time.RFC3339Nano),
			"newExpiresAt", rm.leaseExpiry.Format(time.RFC3339Nano),
			"lastQuorumHeartbeat", rm.lastQuorumHeartbeat.Format(time.RFC3339Nano),
			"duration", rm.leaseDuration)
	} else {
		rm.logger.Debugw("Leader lease quorum heartbeat updated",
			"now", now.Format(time.RFC3339Nano),
			"currentLeaseExpiry", rm.leaseExpiry.Format(time.RFC3339Nano),
			"lastQuorumHeartbeat", rm.lastQuorumHeartbeat.Format(time.RFC3339Nano),
			"duration", rm.leaseDuration)
	}
}

// triggerCommitCheck attempts to send a non-blocking signal to trigger MaybeAdvanceCommitIndex.
func (rm *replicationManager) triggerCommitCheck() {
	if rm.isShutdown.Load() {
		return
	}

	go func() {
		// Small delay to let replication settle
		time.Sleep(10 * time.Millisecond)
		rm.MaybeAdvanceCommitIndex()
	}()
}

// handleLogInconsistency adjusts a peer's nextIndex in response to a failed AppendEntries reply.
func (rm *replicationManager) handleLogInconsistency(
	peerID types.NodeID,
	term types.Term,
	reply *types.AppendEntriesReply,
	isHeartbeat bool,
	rpcLatency time.Duration,
) {
	rm.logger.Warnw("Log inconsistency detected from peer reply",
		"peerID", peerID,
		"term", term,
		"replyConflictIndex", reply.ConflictIndex,
		"replyConflictTerm", reply.ConflictTerm,
		"replyMatchIndexHint", reply.MatchIndex)

	rm.mu.Lock()
	defer rm.mu.Unlock()

	peerState, exists := rm.peerStates[peerID]
	if !exists {
		rm.logger.Errorw("Peer state not found during log inconsistency handling", "peerID", peerID)
		return
	}

	peerState.IsActive = true
	peerState.LastActive = rm.clock.Now()

	if reply.ConflictTerm > 0 {
		conflictTermIndex, findErr := rm.findLastLogEntryWithTerm(reply.ConflictTerm)
		if findErr == nil && conflictTermIndex < peerState.NextIndex {
			peerState.NextIndex = max(1, conflictTermIndex+1)
		} else {
			peerState.NextIndex = max(1, reply.ConflictIndex)
		}
	} else if reply.ConflictIndex > 0 {
		peerState.NextIndex = max(1, reply.ConflictIndex)
	} else {
		peerState.NextIndex = max(1, peerState.NextIndex-1)
	}

	if reply.MatchIndex > 0 && reply.MatchIndex > peerState.MatchIndex {
		peerState.MatchIndex = reply.MatchIndex
	}

	rm.logger.Debugw("Adjusted peer state due to log inconsistency",
		"peerID", peerID,
		"newNextIndex", peerState.NextIndex,
		"newMatchIndex", peerState.MatchIndex)

	if isHeartbeat {
		rm.metrics.ObserveHeartbeat(peerID, false, rpcLatency)
	} else {
		rm.metrics.ObservePeerReplication(peerID, false, ReplicationResultLogMismatch)
	}
}

// findLastLogEntryWithTerm searches the leader's log backwards for the last entry matching the given term.
// Used in the log rollback optimization. Returns the index if found, or an error.
func (rm *replicationManager) findLastLogEntryWithTerm(term types.Term) (types.Index, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultLogFetchTimeout)
	defer cancel()

	index, err := rm.logMgr.FindLastEntryWithTermUnsafe(ctx, term, 0)
	if err != nil {
		return 0, fmt.Errorf("failed searching log for term %d: %w", term, err)
	}

	return index, nil
}

// MaybeAdvanceCommitIndex evaluates if the leader can safely advance the commit index.
func (rm *replicationManager) MaybeAdvanceCommitIndex() {
	if rm.isShutdown.Load() {
		return
	}
	rm.mu.Lock()
	shouldSendHeartbeats := rm.maybeAdvanceCommitIndexLocked()
	rm.mu.Unlock()

	if shouldSendHeartbeats {
		go rm.SendHeartbeats(context.Background())
	}
}

// maybeAdvanceCommitIndexLocked is the internal implementation that assumes the caller holds the lock.
func (rm *replicationManager) maybeAdvanceCommitIndexLocked() bool {
	currentTerm, role, leaderID := rm.stateMgr.GetStateUnsafe()
	if role != types.RoleLeader || leaderID != rm.id {
		return false
	}

	oldCommitIndex := rm.stateMgr.GetCommitIndexUnsafe()
	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()

	if oldCommitIndex >= lastLogIndex {
		return false
	}

	matchIndices := rm.collectMatchIndicesLocked(lastLogIndex)
	potentialCommitIndex := rm.calculateQuorumMatchIndex(matchIndices)

	if potentialCommitIndex <= oldCommitIndex {
		return false
	}

	ok, err := rm.isEntryFromCurrentTerm(potentialCommitIndex, currentTerm)
	if err != nil {
		rm.logger.Warnw("Failed to check term for potential commit index",
			"index", potentialCommitIndex, "term", currentTerm, "error", err)
		return false
	}

	if !ok {
		return false
	}

	if rm.stateMgr.UpdateCommitIndexUnsafe(potentialCommitIndex) {
		rm.logger.Infow("Commit index advanced",
			"oldCommitIndex", oldCommitIndex,
			"newCommitIndex", potentialCommitIndex,
			"term", currentTerm,
			"matchIndices", matchIndices)

		rm.triggerApplyNotify()
		return true
	}

	return false
}

// collectMatchIndicesLocked gathers match indices from all peers plus the leader's own last index.
func (rm *replicationManager) collectMatchIndicesLocked(leaderLastIndex types.Index) []types.Index {
	matchIndices := make([]types.Index, 0, len(rm.peers))

	// The leader's own log is always considered matched up to its last index.
	matchIndices = append(matchIndices, leaderLastIndex)

	// Correctly append the MatchIndex from each peer's state.
	for _, peerState := range rm.peerStates {
		matchIndices = append(matchIndices, peerState.MatchIndex)
	}

	return matchIndices
}

// calculateQuorumMatchIndex finds the highest index replicated on a quorum of nodes.
func (rm *replicationManager) calculateQuorumMatchIndex(matchIndices []types.Index) types.Index {
	if len(matchIndices) < rm.quorumSize {
		return 0
	}
	sort.Slice(matchIndices, func(i, j int) bool {
		return matchIndices[i] > matchIndices[j]
	})
	return matchIndices[rm.quorumSize-1]
}

// isEntryFromCurrentTerm checks if the log entry at the given index was created in the current leader term.
// Returns false and logs details if not, or if an error occurs.
func (rm *replicationManager) isEntryFromCurrentTerm(
	index types.Index,
	currentTerm types.Term,
) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTermFetchTimeout)
	defer cancel()

	entryTerm, err := rm.logMgr.GetTerm(ctx, index)
	if err != nil {
		return false, fmt.Errorf("failed to get term for index %d: %w", index, err)
	}

	if entryTerm != currentTerm {
		rm.logger.Debugw("Cannot commit index: entry term mismatch",
			"index", index,
			"entryTerm", entryTerm,
			"currentLeaderTerm", currentTerm)
		return false, nil
	}

	return true, nil
}

// triggerApplyNotify attempts to notify the apply loop non-blockingly.
func (rm *replicationManager) triggerApplyNotify() {
	if rm.isShutdown.Load() {
		return
	}

	// Send notification asynchronously to avoid blocking the caller while holding locks
	go func() {
		select {
		case rm.applyNotifyCh <- struct{}{}:
			rm.logger.Debugw("Apply notification sent")
		case <-time.After(100 * time.Millisecond):
			rm.logger.Warnw("Apply notification timeout, potentially delayed")
			rm.metrics.ObserveApplyNotificationDropped()
		}
	}()
}

// Propose appends a command to the log if leader and starts replication.
func (rm *replicationManager) Propose(
	ctx context.Context,
	command []byte,
) (index types.Index, term types.Term, isLeader bool, err error) {
	if rm.isShutdown.Load() {
		return 0, 0, false, ErrShuttingDown
	}

	// STEP 1: Validate leadership immediately
	term, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id {
		rm.metrics.ObserveProposal(false, ProposalResultNotLeader)
		return 0, 0, false, ErrNotLeader
	}

	rm.logger.Infow("Starting proposal process",
		"term", term,
		"commandSize", len(command),
		"role", role.String(),
		"leaderID", leaderID)

	// STEP 2: Prepare log entry
	lastIndex := rm.logMgr.GetLastIndexUnsafe()
	newIndex := lastIndex + 1

	entry := types.LogEntry{
		Term:    term,
		Index:   newIndex,
		Command: command,
	}

	// STEP 3: Append to local log with leadership re-validation
	appendCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	rm.logger.Debugw("Appending entry to local log",
		"index", newIndex, "term", term, "commandSize", len(command))

	if err = rm.logMgr.AppendEntries(appendCtx, []types.LogEntry{entry}); err != nil {
		rm.logger.Errorw("Failed to append proposed entry to local log",
			"term", term, "index", newIndex, "error", err)
		rm.metrics.ObserveProposal(false, ProposalResultLogAppendFailed)
		return 0, term, true, fmt.Errorf("local log append failed: %w", err)
	}

	// STEP 4: Re-validate leadership after log append
	currentTerm, currentRole, currentLeaderID := rm.stateMgr.GetState()
	if currentRole != types.RoleLeader || currentLeaderID != rm.id || currentTerm != term {
		rm.logger.Warnw("Lost leadership during proposal, but entry was appended",
			"originalTerm", term, "currentTerm", currentTerm,
			"originalRole", role.String(), "currentRole", currentRole.String(),
			"index", newIndex)

		// Entry was appended but we lost leadership - still return success
		// The new leader will handle replication
		rm.metrics.ObserveProposal(true, ProposalResultNotLeader)
		return newIndex, term, false, nil
	}

	// STEP 5: Verify append was successful
	verifyLastIndex := rm.logMgr.GetLastIndexUnsafe()
	if verifyLastIndex != newIndex {
		rm.logger.Errorw("Log append verification failed",
			"expectedIndex", newIndex, "actualIndex", verifyLastIndex)
		rm.metrics.ObserveProposal(false, ProposalResultLogAppendFailed)
		return 0, term, true, fmt.Errorf("log append verification failed: expected %d, got %d",
			newIndex, verifyLastIndex)
	}

	rm.logger.Infow("Proposed command appended to local log successfully",
		"term", term, "index", newIndex, "commandSize", len(command))

	rm.metrics.ObserveProposal(true, ProposalResultSuccess)

	// STEP 6: Start immediate replication with leadership assertion
	go rm.startImmediateReplication(term, newIndex)

	return newIndex, term, true, nil
}

func (rm *replicationManager) startImmediateReplication(term types.Term, newIndex types.Index) {
	// Validate we're still leader
	currentTerm, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id || currentTerm != term {
		rm.logger.Warnw("Cannot start replication - lost leadership",
			"originalTerm", term, "currentTerm", currentTerm,
			"role", role.String(), "leaderID", leaderID)
		return
	}

	rm.logger.Infow("Starting immediate replication after proposal",
		"term", term, "index", newIndex)

	// Create replication context
	replicationCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Replicate to all peers immediately
	rm.replicateToAllPeersImmediate(replicationCtx, term)

	// Trigger commit check after a brief delay for replication to start
	time.Sleep(50 * time.Millisecond)
	rm.triggerCommitCheck()
}

// replicateToAllPeers initiates replication to all known peers.
// Called after a leader appends a new entry.
func (rm *replicationManager) replicateToAllPeers(ctx context.Context, term types.Term) {
	if rm.isShutdown.Load() || ctx.Err() != nil {
		rm.logger.Warnw("replicateToAllPeers aborted: shutdown or context cancelled",
			"term", term,
			"shutdown", rm.isShutdown.Load(),
			"contextErr", ctx.Err())
		return
	}

	currentTerm, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id || currentTerm != term {
		rm.logger.Warnw("replicateToAllPeers aborted: leadership lost or term changed",
			"originalTerm", term,
			"currentTerm", currentTerm,
			"role", role.String(),
			"leaderID", leaderID,
			"expectedLeaderID", rm.id)
		return
	}

	commitIndex := rm.stateMgr.GetCommitIndex()
	peerCount := len(rm.peers) - 1

	firstIndex, lastIndex, lastTerm := rm.logMgr.GetLogStateForDebugging()

	rm.logger.Infow("Starting replication to all peers",
		"term", term,
		"commitIndex", commitIndex,
		"peerCount", peerCount,
		"logFirstIndex", firstIndex,
		"logLastIndex", lastIndex,
		"logLastTerm", lastTerm,
		"peers", rm.getAllPeerIDs())

	if lastIndex == 0 {
		rm.logger.Warnw("No entries to replicate - log is empty",
			"term", term,
			"lastIndex", lastIndex)
		return
	}

	replicationCount := 0

	for peerID := range rm.peers {
		if peerID == rm.id {
			continue
		}

		replicationCount++
		rm.logger.Infow("Launching replication goroutine for peer",
			"peer", peerID,
			"term", term,
			"commitIndex", commitIndex,
			"lastLogIndex", lastIndex,
			"replicationAttempt", replicationCount)

		go func(peer types.NodeID, attemptNum int) {
			replicationCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			rm.logger.Infow("Starting replication goroutine",
				"peer", peer,
				"term", term,
				"attemptNum", attemptNum,
				"lastLogIndex", lastIndex)

			rm.replicateToPeerInternal(replicationCtx, peer, term, commitIndex, false)

			rm.logger.Infow("Completed replication goroutine",
				"peer", peer,
				"term", term,
				"attemptNum", attemptNum)
		}(peerID, replicationCount)
	}

	rm.logger.Infow("All replication goroutines launched",
		"term", term,
		"totalAttempts", replicationCount,
		"logLastIndex", lastIndex)
}

func (rm *replicationManager) replicateToAllPeersImmediate(ctx context.Context, term types.Term) {
	if rm.isShutdown.Load() || ctx.Err() != nil {
		return
	}

	// Validate leadership
	currentTerm, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id || currentTerm != term {
		rm.logger.Warnw("Cannot replicate - lost leadership",
			"originalTerm", term, "currentTerm", currentTerm,
			"role", role.String())
		return
	}

	commitIndex := rm.stateMgr.GetCommitIndex()
	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()

	if lastLogIndex == 0 {
		rm.logger.Debugw("No entries to replicate - log is empty", "term", term)
		return
	}

	rm.logger.Infow("Starting immediate replication to all peers",
		"term", term, "commitIndex", commitIndex, "lastLogIndex", lastLogIndex,
		"peerCount", len(rm.peers)-1)

	var wg sync.WaitGroup

	// Start replication to all peers
	for peerID := range rm.peers {
		if peerID == rm.id {
			continue
		}

		wg.Add(1)
		go func(peer types.NodeID) {
			defer wg.Done()

			replicationCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			rm.logger.Debugw("Starting immediate replication to peer",
				"peer", peer, "term", term, "lastLogIndex", lastLogIndex)

			rm.replicateToPeerInternal(replicationCtx, peer, term, commitIndex, false)

			rm.logger.Debugw("Immediate replication completed",
				"peer", peer, "term", term)
		}(peerID)
	}

	// Don't wait for all replications - return immediately for responsiveness
	go func() {
		wg.Wait()
		rm.logger.Infow("All immediate replications completed", "term", term)
	}()
}

// Helper method to get all peer IDs for logging
func (rm *replicationManager) getAllPeerIDs() []types.NodeID {
	var peerIDs []types.NodeID
	for peerID := range rm.peers {
		if peerID != rm.id {
			peerIDs = append(peerIDs, peerID)
		}
	}
	return peerIDs
}

// HasValidLease checks if the leader lease is currently valid.
func (rm *replicationManager) HasValidLease(ctx context.Context) bool {
	if !rm.leaderLeaseEnabled {
		return false
	}

	_, role, _ := rm.stateMgr.GetState()
	if role != types.RoleLeader {
		return false
	}

	rm.mu.RLock()
	expiry := rm.leaseExpiry
	rm.mu.RUnlock()

	now := rm.clock.Now()
	isValid := now.Before(expiry)

	if !isValid {
		rm.logger.Debugw("Leader lease check failed: lease expired",
			"now", now.Format(time.RFC3339Nano),
			"leaseExpiry", expiry.Format(time.RFC3339Nano))
	}

	return isValid
}

// VerifyLeadershipAndGetCommitIndex implements the ReadIndex protocol.
func (rm *replicationManager) VerifyLeadershipAndGetCommitIndex(
	ctx context.Context,
) (types.Index, error) {
	if rm.isShutdown.Load() {
		return 0, ErrShuttingDown
	}

	term, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id {
		return 0, ErrNotLeader
	}

	if rm.leaderLeaseEnabled && rm.HasValidLease(ctx) {
		commitIndex := rm.stateMgr.GetCommitIndex()
		rm.logger.Debugw("ReadIndex served via leader lease",
			"term", term, "commitIndex", commitIndex)
		rm.metrics.ObserveReadIndex(true, "lease")
		return commitIndex, nil
	}

	rm.logger.Debugw("ReadIndex requires quorum check", "term", term)
	quorumCtx, cancel := context.WithTimeout(ctx, defaultReadIndexTimeout)
	defer cancel()

	if ok := rm.verifyLeadershipViaQuorum(quorumCtx, term); !ok {
		rm.logger.Warnw("ReadIndex quorum check failed",
			"term", term, "quorumSize", rm.quorumSize)
		rm.metrics.ObserveReadIndex(false, "quorum_check")
		if quorumCtx.Err() == context.DeadlineExceeded {
			return 0, fmt.Errorf("read index quorum check timed out: %w", ErrTimeout)
		}
		return 0, fmt.Errorf("read index quorum verification failed: %w", ErrQuorumUnreachable)
	}

	commitIndex := rm.stateMgr.GetCommitIndex()
	rm.logger.Debugw("ReadIndex quorum check successful",
		"term", term, "commitIndex", commitIndex)
	rm.metrics.ObserveReadIndex(true, "quorum_check")
	return commitIndex, nil
}

// verifyLeadershipViaQuorum performs a quorum check using lightweight heartbeats.
func (rm *replicationManager) verifyLeadershipViaQuorum(ctx context.Context, term types.Term) bool {
	responseCh := make(chan bool, len(rm.peers)-1)
	var wg sync.WaitGroup

	successCount := 1

	for peerID := range rm.peers {
		if peerID == rm.id {
			continue
		}

		wg.Add(1)
		go rm.sendReadIndexHeartbeat(ctx, &wg, responseCh, peerID, term)
	}

	go func() {
		wg.Wait()
		close(responseCh)
	}()

	for {
		select {
		case ok, chanOpen := <-responseCh:
			if !chanOpen {
				return successCount >= rm.quorumSize
			}
			if ok {
				successCount++
				if successCount >= rm.quorumSize {
					return true
				}
			}
		case <-ctx.Done():
			return false
		}
	}
}

// sendReadIndexHeartbeat sends a minimal AppendEntries RPC for ReadIndex verification.
func (rm *replicationManager) sendReadIndexHeartbeat(
	ctx context.Context,
	wg *sync.WaitGroup,
	responseCh chan<- bool,
	peerID types.NodeID,
	term types.Term,
) {
	defer wg.Done()

	args := &types.AppendEntriesArgs{
		Term:         term,
		LeaderID:     rm.id,
		LeaderCommit: rm.stateMgr.GetCommitIndex(),
	}

	reply, err := rm.networkMgr.SendAppendEntries(ctx, peerID, args)

	select {
	case <-ctx.Done():
		responseCh <- false
	default:
		if err != nil || reply.Term > term {
			responseCh <- false
			return
		}
		responseCh <- true
	}
}

// HandleAppendEntries processes an incoming AppendEntries RPC from a leader.
func (rm *replicationManager) HandleAppendEntries(
	ctx context.Context,
	args *types.AppendEntriesArgs,
) (*types.AppendEntriesReply, error) {
	if rm.isShutdown.Load() {
		return nil, ErrShuttingDown
	}

	// Check for context cancellation early
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	rm.logger.Debugw("HandleAppendEntries called",
		"from", args.LeaderID,
		"term", args.Term,
		"prevLogIndex", args.PrevLogIndex,
		"prevLogTerm", args.PrevLogTerm,
		"entriesCount", len(args.Entries),
		"leaderCommit", args.LeaderCommit)

	// Step down if we see a higher term
	steppedDown, _ := rm.stateMgr.CheckTermAndStepDown(
		ctx, // Use the provided context
		args.Term,
		args.LeaderID,
	)

	currentTerm, currentRole, _ := rm.stateMgr.GetState()
	reply := &types.AppendEntriesReply{Term: currentTerm, Success: false}

	// Reject if term is too old
	if args.Term < currentTerm {
		rm.logger.Debugw("Rejecting AppendEntries: term too old",
			"argsTerm", args.Term, "currentTerm", currentTerm)
		return reply, nil
	}

	// Become follower if we stepped down or need to
	if steppedDown || (currentRole != types.RoleFollower && args.Term == currentTerm) {
		rm.stateMgr.BecomeFollower(ctx, currentTerm, args.LeaderID)
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Double-check term after acquiring lock
	lockedCurrentTerm, _, _ := rm.stateMgr.GetStateUnsafe()
	if args.Term < lockedCurrentTerm {
		reply.Term = lockedCurrentTerm
		rm.logger.Debugw("Rejecting AppendEntries after lock: term too old",
			"argsTerm", args.Term, "lockedCurrentTerm", lockedCurrentTerm)
		return reply, nil
	}
	reply.Term = lockedCurrentTerm

	// Check log consistency
	consistent, conflictIndex, conflictTerm := rm.checkLogConsistencyLocked(
		ctx, // Use provided context instead of background
		args.PrevLogIndex,
		args.PrevLogTerm,
	)

	if !consistent {
		reply.Success = false
		reply.ConflictIndex = conflictIndex
		reply.ConflictTerm = conflictTerm
		rm.logger.Debugw("AppendEntries rejected: log inconsistency",
			"conflictIndex", conflictIndex, "conflictTerm", conflictTerm)
		return reply, nil
	}

	// Process entries
	lastNewIndex, err := rm.processEntriesLocked(ctx, args.Entries, args.PrevLogIndex)
	if err != nil {
		reply.Success = false
		rm.logger.Warnw("AppendEntries failed: error processing entries",
			"error", err, "prevLogIndex", args.PrevLogIndex)
		return reply, fmt.Errorf("failed to process entries: %w", err)
	}

	// Update commit index
	rm.updateFollowerCommitIndexLocked(args.LeaderCommit, lastNewIndex)
	rm.recordAppendEntriesMetrics(args.Entries)

	reply.Success = true
	reply.MatchIndex = lastNewIndex

	rm.logger.Debugw("AppendEntries succeeded",
		"lastNewIndex", lastNewIndex, "leaderCommit", args.LeaderCommit)

	return reply, nil
}

// checkLogConsistencyLocked checks if the local log matches the leader's expectation.
// Assumes rm.mu lock is held.
func (rm *replicationManager) checkLogConsistencyLocked(
	ctx context.Context,
	prevLogIndex types.Index,
	prevLogTerm types.Term,
) (consistent bool, conflictIndex types.Index, conflictTerm types.Term) {
	if prevLogIndex == 0 { // The base case
		return true, 0, 0
	}

	localLastIndex := rm.logMgr.GetLastIndexUnsafe()
	if prevLogIndex > localLastIndex {
		// Our log is shorter than the leader's
		rm.logger.Debugw("Log consistency check failed: prevLogIndex > localLastIndex",
			"prevLogIndex", prevLogIndex, "localLastIndex", localLastIndex)
		return false, localLastIndex + 1, 0
	}

	// Use shorter timeout to avoid blocking
	fetchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	localTermAtPrev, err := rm.logMgr.GetTermUnsafe(fetchCtx, prevLogIndex)
	if err != nil {
		if errors.Is(err, ErrCompacted) {
			// The entry was compacted. Tell the leader our first index.
			return false, rm.logMgr.GetFirstIndex(), 0
		}
		// For any other error, report a conflict at that index
		rm.logger.Warnw("Log consistency check failed: could not get term for prevLogIndex",
			"prevLogIndex", prevLogIndex, "error", err)
		return false, prevLogIndex, 0
	}

	if localTermAtPrev != prevLogTerm {
		// The terms don't match. This is a definitive conflict.
		rm.logger.Debugw("Log consistency check failed: term mismatch",
			"prevLogIndex", prevLogIndex, "localTerm", localTermAtPrev, "leaderTerm", prevLogTerm)

		// Try to find the first index of the conflicting term
		firstIdxInConflictTerm, findErr := rm.logMgr.FindFirstIndexInTermUnsafe(
			fetchCtx, // Use the same short timeout
			localTermAtPrev,
			prevLogIndex,
		)
		if findErr != nil {
			// If we can't find the start of the conflicting term, fall back
			return false, prevLogIndex, localTermAtPrev
		}
		return false, firstIdxInConflictTerm, localTermAtPrev
	}

	// If we get here, the log is consistent up to prevLogIndex
	return true, 0, 0
}

// processEntriesLocked handles appending/truncating entries received from the leader.
// Returns the index of the last entry processed/appended.
// Assumes rm.mu lock is held.
func (rm *replicationManager) processEntriesLocked(
	ctx context.Context,
	entries []types.LogEntry,
	prevLogIndex types.Index,
) (types.Index, error) {
	if len(entries) == 0 {
		return prevLogIndex, nil
	}

	localLastIndex := rm.logMgr.GetLastIndexUnsafe()

	// Use a reasonable timeout for processing
	rm.logger.Infow("DEBUG: Creating process context",
		"timeout", 5*time.Second, "entriesCount", len(entries))
	processCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Find the first non-matching entry
	for i, entry := range entries {
		// Check for context cancellation
		if processCtx.Err() != nil {
			return rm.logMgr.GetLastIndexUnsafe(), processCtx.Err()
		}

		// If the entry index is past our log, append from here
		if entry.Index > localLastIndex {
			if err := rm.appendNewEntriesLocked(processCtx, entries[i:]); err != nil {
				return rm.logMgr.GetLastIndexUnsafe(), err
			}
			return rm.logMgr.GetLastIndexUnsafe(), nil
		}

		localTerm, err := rm.logMgr.GetTermUnsafe(processCtx, entry.Index)
		// If term lookup fails or terms mismatch, we must truncate from this index
		if err != nil || localTerm != entry.Term {
			rm.logger.Infow("Log conflict detected, truncating suffix", "from_index", entry.Index)

			if err := rm.truncateLogSuffixLocked(processCtx, entry.Index); err != nil {
				return 0, err
			}

			// After truncation, append all the new entries
			if err := rm.appendNewEntriesLocked(processCtx, entries[i:]); err != nil {
				return rm.logMgr.GetLastIndexUnsafe(), err
			}
			return rm.logMgr.GetLastIndexUnsafe(), nil
		}
	}

	// If the loop completes, all incoming entries matched existing ones
	return entries[len(entries)-1].Index, nil
}

// filterEntriesToAppend returns the sub-slice of entries starting from startIndex.
func (rm *replicationManager) filterEntriesToAppend(
	allEntries []types.LogEntry,
	startIndex types.Index,
) []types.LogEntry {
	for i, entry := range allEntries {
		if entry.Index >= startIndex {
			return allEntries[i:]
		}
	}
	return nil
}

// truncateLogSuffixLocked truncates the log from the specified index onward.
// Assumes rm.mu lock is held.
func (rm *replicationManager) truncateLogSuffixLocked(
	ctx context.Context,
	fromIndex types.Index,
) error {
	return rm.logMgr.TruncateSuffixUnsafe(ctx, fromIndex)
}

// appendNewEntriesLocked appends the given entries to the local log.
// Assumes rm.mu lock is held.
func (rm *replicationManager) appendNewEntriesLocked(
	ctx context.Context,
	entries []types.LogEntry,
) error {
	return rm.logMgr.AppendEntriesUnsafe(ctx, entries)
}

// updateFollowerCommitIndexLocked updates the follower's commit index based on the leader's commit.
// Assumes rm.mu lock is held.
func (rm *replicationManager) updateFollowerCommitIndexLocked(
	leaderCommit, lastProcessedIndex types.Index,
) {
	oldCommitIndex := rm.stateMgr.GetCommitIndexUnsafe()
	newCommitIndex := min(leaderCommit, lastProcessedIndex)

	rm.logger.Debugw("Follower commit index update",
		"leaderCommit", leaderCommit,
		"lastProcessedIndex", lastProcessedIndex,
		"oldCommitIndex", oldCommitIndex,
		"newCommitIndex", newCommitIndex)

	if newCommitIndex > oldCommitIndex {
		if rm.stateMgr.UpdateCommitIndexUnsafe(newCommitIndex) {
			rm.logger.Infow("Follower commit index advanced",
				"oldCommitIndex", oldCommitIndex,
				"newCommitIndex", newCommitIndex)

			rm.triggerApplyNotify()
		}
	}
}

// recordAppendEntriesMetrics tracks metrics for received AppendEntries RPCs.
func (rm *replicationManager) recordAppendEntriesMetrics(entries []types.LogEntry) {
	if len(entries) == 0 {
		rm.metrics.ObserveAppendEntriesHeartbeat()
	} else {
		rm.metrics.ObserveAppendEntriesReplication()
		rm.metrics.ObserveEntriesReceived(len(entries))
		var totalBytes int
		for _, e := range entries {
			totalBytes += len(e.Command)
		}
		rm.metrics.ObserveCommandBytesReceived(totalBytes)
	}
}

// GetPeerReplicationStatus returns a snapshot of replication status for all peers.
// Requires leader role. Returns an empty map if not leader or if shutdown.
// Caller must hold rm.mu.
func (rm *replicationManager) GetPeerReplicationStatusUnsafe() map[types.NodeID]types.PeerState {
	statusMap := make(map[types.NodeID]types.PeerState)
	if rm.isShutdown.Load() {
		return statusMap
	}

	_, role, _ := rm.stateMgr.GetStateUnsafe()
	if role != types.RoleLeader {
		return statusMap
	}

	leaderLastIndex := rm.logMgr.GetLastIndexUnsafe()

	for peerID, pState := range rm.peerStates {
		statusMap[peerID] = types.PeerState{
			NextIndex:          pState.NextIndex,
			MatchIndex:         pState.MatchIndex,
			IsActive:           pState.IsActive,
			LastActive:         pState.LastActive,
			SnapshotInProgress: pState.SnapshotInProgress,
			ReplicationLag:     leaderLastIndex - pState.MatchIndex,
		}
	}

	return statusMap
}

// ReplicateToPeer triggers replication to a specific peer. No-op if not leader.
func (rm *replicationManager) ReplicateToPeer(
	ctx context.Context,
	peerID types.NodeID,
	isHeartbeat bool,
) {
	if rm.isShutdown.Load() || ctx.Err() != nil {
		return
	}

	term, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id {
		return
	}

	commitIndex := rm.stateMgr.GetCommitIndex()
	go rm.replicateToPeerInternal(ctx, peerID, term, commitIndex, isHeartbeat)
}

// UpdatePeerAfterSnapshotSend updates peer state after a snapshot is successfully sent.
func (rm *replicationManager) UpdatePeerAfterSnapshotSend(
	peerID types.NodeID,
	snapshotIndex types.Index,
) {
	if rm.isShutdown.Load() {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	peerState, exists := rm.peerStates[peerID]
	if !exists {
		return
	}

	if snapshotIndex > peerState.MatchIndex {
		peerState.MatchIndex = snapshotIndex
	}
	peerState.NextIndex = snapshotIndex + 1
	peerState.SnapshotInProgress = false
	peerState.IsActive = true
	peerState.LastActive = rm.clock.Now()

	go rm.triggerCommitCheck()
}

// SetPeerSnapshotInProgress sets the snapshot-in-progress flag for a peer.
func (rm *replicationManager) SetPeerSnapshotInProgress(peerID types.NodeID, inProgress bool) {
	if rm.isShutdown.Load() {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	if peerState, exists := rm.peerStates[peerID]; exists {
		peerState.SnapshotInProgress = inProgress
	}
}

// Stop cleans up resources used by the replication manager.
func (rm *replicationManager) Stop() {
	rm.logger.Infow("Stopping replication manager...")

	if !rm.isShutdown.Swap(true) {
		close(rm.notifyCommitCh)
	}

	rm.logger.Infow("Replication manager stopped")
}
