package raft

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

type ReplicationManager interface {
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
	Propose(ctx context.Context, command []byte) (index types.Index, term types.Term, isLeader bool, err error)

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
	HandleAppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error)

	// GetPeerReplicationStatus returns the current replication progress for each peer,
	// primarily used for metrics, monitoring, or debugging. Requires leader role.
	GetPeerReplicationStatus() map[types.NodeID]types.PeerState

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
	ApplyNotifyCh  chan<- struct{} // Channel to notify the core Raft loop to apply entries
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
		if ptr == nil {
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
	if err := checkPtr(deps.NetworkMgr, "NetworkMgr"); err != nil {
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
	applyNotifyCh  chan<- struct{} // Signals Raft core to apply newly committed log entries
	notifyCommitCh chan struct{}   // Triggers commit index evaluation after replication events
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
		notifyCommitCh:     make(chan struct{}, 1),
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
			log.Warnw("Using default ElectionTickCount for lease calculation", "configured", electionTicks, "default", DefaultElectionTickCount)
			electionTicks = DefaultElectionTickCount
		}

		rm.leaseDuration = time.Duration(electionTicks) * nominalTickInterval / 2

		log.Infow("Leader lease enabled",
			"leaseDuration", rm.leaseDuration,
			"basedOnElectionTicks", electionTicks)
	}

	log.Infow("Replication manager initialized",
		"peerCount", len(rm.peers)-1, // Exclude self
		"quorumSize", rm.quorumSize,
		"leaderLease", rm.leaderLeaseEnabled)

	return rm, nil
}

// Tick drives periodic replication tasks for the leader.
func (rm *replicationManager) Tick(ctx context.Context) {
	if rm.isShutdown.Load() {
		return
	}

	term, role, _ := rm.stateMgr.GetState()
	if role != types.RoleLeader {
		return // Only leaders perform replication tasks.
	}

	select {
	case <-rm.notifyCommitCh:
		rm.MaybeAdvanceCommitIndex()
	default:
	}

	rm.mu.Lock()
	rm.heartbeatElapsed++
	elapsed := rm.heartbeatElapsed
	interval := rm.getHeartbeatInterval()

	shouldSendHeartbeats := elapsed >= interval
	if shouldSendHeartbeats {
		rm.heartbeatElapsed = 0 // Reset timer
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
	term, role, _ := rm.stateMgr.GetState()
	if role != types.RoleLeader {
		rm.logger.Warnw("InitializeLeaderState called but node is not leader",
			"currentRole", role.String(), "term", term)
		return
	}

	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.initializeAllPeerStatesLocked(lastLogIndex)
	rm.heartbeatElapsed = 0 // Reset heartbeat timer

	if rm.leaderLeaseEnabled {
		rm.initializeLeaderLeaseLocked(term)
	}

	rm.logger.Infow("Leader state initialized",
		"term", term,
		"lastLogIndex", lastLogIndex,
		"peerCount", len(rm.peerStates)) // peerStates map excludes self
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
	if rm.isShutdown.Load() || ctx.Err() != nil {
		return
	}

	term, role, leaderID := rm.stateMgr.GetState()
	commitIndex := rm.stateMgr.GetCommitIndex()

	if role != types.RoleLeader || leaderID != rm.id {
		rm.logger.Debugw("SendHeartbeats skipped: node is not leader",
			"term", term, "role", role.String(), "leaderID", leaderID)
		return
	}

	rm.logger.Debugw("Broadcasting heartbeats",
		"term", term,
		"commitIndex", commitIndex,
		"peerCount", len(rm.peers)-1)
	rm.metrics.ObserveHeartbeatSent()

	for peerID := range rm.peers {
		if peerID == rm.id {
			continue // Skip self
		}

		// Using context.Background() here to prevent cancellation if the original Tick context expires,
		// allowing heartbeats to potentially complete.
		go rm.replicateToPeerInternal(context.Background(), peerID, term, commitIndex, true)
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
	if rm.isShutdown.Load() || ctx.Err() != nil {
		return
	}

	nextIndex, snapshotInProgress, ok := rm.getPeerReplicationState(peerID)
	if !ok {
		rm.logger.Errorw("Cannot replicate: peer state not found", "peerID", peerID)
		return // Should not happen
	}

	firstLogIndex := rm.logMgr.GetFirstIndex()
	if nextIndex < firstLogIndex {
		if snapshotInProgress {
			rm.logger.Debugw("Snapshot send already in progress for peer", "peerID", peerID, "nextIndex", nextIndex)
			return
		}
		rm.initiateSnapshotIfNeeded(ctx, peerID, nextIndex, term, firstLogIndex)
		return // subsequent replication handled after snapshot completion
	}

	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()

	if !isHeartbeat && nextIndex > lastLogIndex {
		rm.logger.Debugw("Peer is up-to-date, no replication needed", "peerID", peerID, "nextIndex", nextIndex, "lastLogIndex", lastLogIndex)
		return
	}

	prevLogIndex, prevLogTerm, ok := rm.getPrevLogInfo(ctx, peerID, nextIndex, term)
	if !ok {
		return
	}

	var entries []types.LogEntry
	if !isHeartbeat && nextIndex <= lastLogIndex {
		entries = rm.getEntriesForPeer(ctx, peerID, nextIndex, lastLogIndex, term)
		if entries == nil && nextIndex <= lastLogIndex {
			rm.logger.Debugw("Failed to get entries, possibly due to compaction triggering snapshot", "peerID", peerID, "nextIndex", nextIndex)
			return
		}
	}

	args := rm.buildAppendEntriesArgs(term, commitIndex, prevLogIndex, prevLogTerm, entries)
	rm.logReplicationAttempt(peerID, isHeartbeat, entries, prevLogIndex)

	rpcCtx, cancel := context.WithTimeout(ctx, defaultAppendEntriesTimeout)
	defer cancel()

	startTime := rm.clock.Now()
	reply, err := rm.networkMgr.SendAppendEntries(rpcCtx, peerID, args)
	rpcLatency := rm.clock.Now().Sub(startTime)

	if err != nil {
		rm.handleAppendError(peerID, term, err, isHeartbeat, rpcLatency)
	} else {
		rm.handleAppendReply(peerID, term, args, reply, isHeartbeat, rpcLatency)
	}
}

// getPeerReplicationState safely returns the peer's nextIndex and snapshotInProgress status.
func (rm *replicationManager) getPeerReplicationState(peerID types.NodeID) (nextIndex types.Index, snapshotInProgress bool, ok bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	state, exists := rm.peerStates[peerID]
	if !exists {
		return 0, false, false
	}
	return state.NextIndex, state.SnapshotInProgress, true
}

// initiateSnapshotIfNeeded attempts to start a snapshot transfer if one isn't already running.
func (rm *replicationManager) initiateSnapshotIfNeeded(ctx context.Context, peerID types.NodeID, peerNextIndex types.Index, term types.Term, localFirstIndex types.Index) {
	rm.mu.Lock()
	peerState, exists := rm.peerStates[peerID]
	if !exists {
		rm.mu.Unlock()
		rm.logger.Errorw("Cannot initiate snapshot: peer state not found", "peerID", peerID)
		return
	}

	if peerState.SnapshotInProgress {
		rm.mu.Unlock()
		rm.logger.Debugw("Snapshot initiation skipped: already in progress", "peerID", peerID)
		return
	}

	peerState.SnapshotInProgress = true
	rm.mu.Unlock()

	rm.logger.Infow("Initiating snapshot transfer due to log gap",
		"peerID", peerID,
		"peerNextIndex", peerNextIndex,
		"localFirstIndex", localFirstIndex)

	// Using context.Background() so snapshot send isn't tied to the Tick context lifetime.
	go rm.snapshotMgr.SendSnapshot(context.Background(), peerID, term)
}

// getPrevLogInfo returns the index and term of the entry preceding nextIndex.
// Returns ok=false if info cannot be retrieved (e.g., compacted).
func (rm *replicationManager) getPrevLogInfo(ctx context.Context, peerID types.NodeID, nextIndex types.Index, term types.Term) (prevLogIndex types.Index, prevLogTerm types.Term, ok bool) {
	if nextIndex <= 1 {
		return 0, 0, true
	}

	prevLogIndex = nextIndex - 1
	fetchCtx, cancel := context.WithTimeout(ctx, defaultTermFetchTimeout)
	defer cancel()

	prevLogTerm, err := rm.logMgr.GetTerm(fetchCtx, prevLogIndex)
	if err != nil {
		if errors.Is(err, ErrCompacted) {
			rm.logger.Infow("Previous log entry compacted, initiating snapshot",
				"peerID", peerID,
				"prevLogIndex", prevLogIndex,
				"nextIndex", nextIndex)

			rm.startSnapshotForPeer(peerID, term)
		} else {
			rm.logger.Warnw("Failed to get term for prevLogIndex",
				"peerID", peerID,
				"prevLogIndex", prevLogIndex,
				"error", err)
		}
		return 0, 0, false
	}
	return prevLogIndex, prevLogTerm, true
}

// getEntriesIfNeeded returns a slice of log entries for replication if it's not a heartbeat.
// Returns nil if no entries are required or an error occurred.
func (rm *replicationManager) getEntriesIfNeeded(ctx context.Context, peerID types.NodeID, isHeartbeat bool, nextIndex, lastLogIndex types.Index, term types.Term) []types.LogEntry {
	if isHeartbeat || nextIndex > lastLogIndex {
		return nil
	}
	return rm.getEntriesForPeer(ctx, peerID, nextIndex, lastLogIndex, term)
}

// getEntriesForPeer retrieves a batch of log entries [nextIndex, lastLogIndex] for a peer.
// Handles log compaction by triggering snapshots. Returns nil if entries cannot be fetched or snapshot is needed.
func (rm *replicationManager) getEntriesForPeer(ctx context.Context, peerID types.NodeID, nextIndex, lastLogIndex types.Index, term types.Term) []types.LogEntry {
	maxEntries := rm.cfg.Options.MaxLogEntriesPerRequest
	if maxEntries <= 0 {
		maxEntries = DefaultMaxLogEntriesPerRequest
	}

	endIndex := nextIndex + types.Index(maxEntries)
	if endIndex > lastLogIndex+1 {
		endIndex = lastLogIndex + 1
	}

	if nextIndex >= endIndex {
		rm.logger.Warnw("Calculated empty range for getEntriesForPeer", "peerID", peerID, "nextIndex", nextIndex, "endIndex", endIndex)
		return nil // Should not happen
	}

	fetchCtx, cancel := context.WithTimeout(ctx, defaultLogFetchTimeout)
	defer cancel()

	entries, err := rm.logMgr.GetEntries(fetchCtx, nextIndex, endIndex)
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

	return entries
}

// startSnapshotForPeer initiates a snapshot transfer if one isn't already in progress.
// Helper function used when log entries/terms are found to be compacted.
func (rm *replicationManager) startSnapshotForPeer(peerID types.NodeID, term types.Term) {
	rm.mu.Lock()
	peerState, exists := rm.peerStates[peerID]
	if !exists {
		rm.mu.Unlock()
		rm.logger.Errorw("Cannot start snapshot: peer state not found", "peerID", peerID)
		return
	}

	if peerState.SnapshotInProgress {
		rm.mu.Unlock()
		rm.logger.Debugw("Snapshot start skipped: already in progress", "peerID", peerID)
		return
	}

	peerState.SnapshotInProgress = true
	rm.mu.Unlock()

	rm.logger.Infow("Starting snapshot due to log compaction or error",
		"peerID", peerID,
		"term", term)

	go rm.snapshotMgr.SendSnapshot(context.Background(), peerID, term)
}

// buildAppendEntriesArgs constructs the RPC request payload for AppendEntries.
func (rm *replicationManager) buildAppendEntriesArgs(term types.Term, commitIndex, prevLogIndex types.Index, prevLogTerm types.Term, entries []types.LogEntry) *types.AppendEntriesArgs {
	return &types.AppendEntriesArgs{
		Term:         term,
		LeaderID:     rm.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries, // empty for heartbeats
		LeaderCommit: commitIndex,
	}
}

// logReplicationAttempt emits debug logs for either a heartbeat or log entry transmission.
func (rm *replicationManager) logReplicationAttempt(peerID types.NodeID, isHeartbeat bool, entries []types.LogEntry, prevLogIndex types.Index) {
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
	errType := "unknown"
	level := rm.logger.Warnw

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		errType = "timeout"
	case errors.Is(err, context.Canceled):
		errType = "canceled"
		level = rm.logger.Debugw
	case errors.Is(err, ErrPeerNotFound):
		errType = "peer_not_found"
		level = rm.logger.Errorw
	default:
		level = rm.logger.Errorw // Treat unexpected errors more seriously
	}

	level("AppendEntries RPC failed",
		"peerID", peerID,
		"term", term,
		"error", err,
		"errorType", errType,
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
	steppedDown, previousLocalTerm := rm.stateMgr.CheckTermAndStepDown(context.Background(), reply.Term, peerID)

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
		peerState, exists := rm.peerStates[peerID]
		if exists {
			now := rm.clock.Now()
			peerState.IsActive = true
			peerState.LastActive = now

			if len(sentArgs.Entries) > 0 {
				lastIndexSent := sentArgs.Entries[len(sentArgs.Entries)-1].Index
				if lastIndexSent > peerState.MatchIndex {
					peerState.MatchIndex = lastIndexSent
				}
				peerState.NextIndex = peerState.MatchIndex + 1
			} else { // successful heartbeat
				expectedNext := sentArgs.PrevLogIndex + 1
				if expectedNext > peerState.NextIndex {
					// Avoid regressing nextIndex if other entries were replicated between heartbeats
					peerState.NextIndex = expectedNext
				}
				// Can happen if heartbeats succeed while entries fail/are slow
				if sentArgs.PrevLogIndex > peerState.MatchIndex {
					peerState.MatchIndex = sentArgs.PrevLogIndex
				}
			}
			rm.logger.Debugw("AppendEntries success: updated peer state",
				"peerID", peerID, "matchIndex", peerState.MatchIndex, "nextIndex", peerState.NextIndex)
		} else {
			rm.logger.Errorw("Peer state not found during successful append reply handling", "peerID", peerID)
		}
		rm.mu.Unlock()

		if isHeartbeat && rm.leaderLeaseEnabled {
			rm.updateLeaderLeaseOnQuorum()
		}

		if isHeartbeat {
			rm.metrics.ObserveHeartbeat(peerID, true, rpcLatency)
		} else if len(sentArgs.Entries) > 0 {
			rm.metrics.ObservePeerReplication(peerID, true, ReplicationResultSuccess)
		}

		rm.triggerCommitCheck()

	} else {
		rm.handleLogInconsistency(peerID, term, reply, isHeartbeat, rpcLatency)
	}
}

// updateLeaderLeaseOnQuorum checks if a quorum of heartbeats has been received recently
// and updates the lease expiry if so. This is a more robust way than updating on every single heartbeat ack.
func (rm *replicationManager) updateLeaderLeaseOnQuorum() {
	// TODO: Implement updateLeaderLeaseOnQuorum
	// The Raft paper suggests confirming with a quorum within the election timeout.
	// Keeping the simpler version for now: updating leaseExpiry on every successful
	// heartbeat response from a peer.
	rm.updateLeaderLease()
}

// updateLeaderLease refreshes the leader lease based on the current time.
// Called after confirming activity (e.g., successful heartbeat reply).
func (rm *replicationManager) updateLeaderLease() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.leaderLeaseEnabled {
		return
	}

	now := rm.clock.Now()
	newExpiry := now.Add(rm.leaseDuration)

	if newExpiry.After(rm.leaseExpiry) {
		rm.leaseExpiry = newExpiry
		rm.lastQuorumHeartbeat = now

		rm.logger.Debugw("Leader lease extended",
			"now", now.Format(time.RFC3339Nano),
			"newExpiresAt", rm.leaseExpiry.Format(time.RFC3339Nano),
			"duration", rm.leaseDuration)
	}
}

// triggerCommitCheck attempts to send a non-blocking signal to trigger MaybeAdvanceCommitIndex.
func (rm *replicationManager) triggerCommitCheck() {
	select {
	case rm.notifyCommitCh <- struct{}{}:
		rm.logger.Debugw("Commit check triggered")
	default:
		rm.logger.Debugw("Commit check already pending (notification channel full)")
	}
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
		if findErr == nil {
			if conflictTermIndex >= peerState.NextIndex {
				rm.logger.Debugw("Conflict term optimization skipped: found index not less than nextIndex", "peerID", peerID, "foundIndex", conflictTermIndex, "nextIndex", peerState.NextIndex)
				peerState.NextIndex = max(1, reply.ConflictIndex)
			} else {
				peerState.NextIndex = max(1, conflictTermIndex+1)
				rm.logger.Infow("Log inconsistency: Rolling back using ConflictTerm", "peerID", peerID, "conflictTerm", reply.ConflictTerm, "setNextIndex", peerState.NextIndex)
			}
		} else {
			peerState.NextIndex = max(1, reply.ConflictIndex)
			rm.logger.Infow("Log inconsistency: Rolling back using ConflictIndex (ConflictTerm not found locally)", "peerID", peerID, "conflictIndex", reply.ConflictIndex, "setNextIndex", peerState.NextIndex)
		}
	} else if reply.ConflictIndex > 0 {
		peerState.NextIndex = max(1, reply.ConflictIndex)
		rm.logger.Infow("Log inconsistency: Rolling back using ConflictIndex", "peerID", peerID, "conflictIndex", reply.ConflictIndex, "setNextIndex", peerState.NextIndex)
	} else {
		peerState.NextIndex = max(1, peerState.NextIndex-1)
		rm.logger.Infow("Log inconsistency: No conflict info, decrementing nextIndex", "peerID", peerID, "setNextIndex", peerState.NextIndex)
	}

	// Can happen if the inconsistency reply also carries info about the last matching entry.
	if reply.MatchIndex > 0 && reply.MatchIndex > peerState.MatchIndex {
		peerState.MatchIndex = reply.MatchIndex
		rm.logger.Debugw("Updated peer MatchIndex based on hint in failed reply", "peerID", peerID, "newMatchIndex", peerState.MatchIndex)
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
	searchHint := types.Index(0) // Start search from the very end

	ctx, cancel := context.WithTimeout(context.Background(), defaultLogFetchTimeout)
	defer cancel()

	index, err := rm.logMgr.FindLastEntryWithTermUnsafe(ctx, term, searchHint)

	if err != nil {
		if errors.Is(err, ErrNotFound) {
			rm.logger.Debugw("LogManager could not find last entry for term", "term", term)
			return 0, fmt.Errorf("term %d not found in local log: %w", term, err)
		}

		rm.logger.Warnw("Error searching log for last entry with term", "term", term, "error", err)
		return 0, fmt.Errorf("failed searching log for term %d: %w", term, err)
	}

	return index, nil
}

// MaybeAdvanceCommitIndex evaluates if the leader can safely advance the commit index.
func (rm *replicationManager) MaybeAdvanceCommitIndex() {
	if rm.isShutdown.Load() {
		return
	}

	currentTerm, role, _ := rm.stateMgr.GetState()
	if role != types.RoleLeader {
		return // Only leaders advance commit index this way
	}

	oldCommitIndex := rm.stateMgr.GetCommitIndex()
	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()

	if oldCommitIndex == lastLogIndex {
		return
	}

	matchIndices := rm.collectMatchIndices(lastLogIndex)
	potentialCommitIndex := rm.calculateQuorumMatchIndex(matchIndices)

	if potentialCommitIndex <= oldCommitIndex {
		return
	}

	if ok, err := rm.isEntryFromCurrentTerm(potentialCommitIndex, currentTerm); !ok {
		if err != nil {
			rm.logger.Warnw("Failed to check term for potential commit index",
				"index", potentialCommitIndex, "term", currentTerm, "error", err)
		}
		return
	}

	if rm.stateMgr.UpdateCommitIndex(potentialCommitIndex) {
		rm.logger.Infow("Commit index advanced",
			"oldCommitIndex", oldCommitIndex,
			"newCommitIndex", potentialCommitIndex,
			"term", currentTerm)

		rm.triggerApplyNotify()
	}
}

// collectMatchIndices gathers match indices from all peers plus the leader's own last index.
func (rm *replicationManager) collectMatchIndices(leaderLastIndex types.Index) []types.Index {
	matchIndices := make([]types.Index, 0, len(rm.peers))

	matchIndices = append(matchIndices, leaderLastIndex)

	rm.mu.RLock()
	for peerID, state := range rm.peerStates {
		if peerID != rm.id {
			matchIndices = append(matchIndices, state.MatchIndex)
		}
	}
	rm.mu.RUnlock()

	return matchIndices
}

// calculateQuorumMatchIndex finds the highest index replicated on a quorum of nodes.
func (rm *replicationManager) calculateQuorumMatchIndex(matchIndices []types.Index) types.Index {
	if len(matchIndices) == 0 {
		return 0
	}

	sort.Slice(matchIndices, func(i, j int) bool { // Sort in ascending order
		return matchIndices[i] < matchIndices[j]
	})

	// The index at position `len(matchIndices) - quorumSize` (0-based)
	// is the highest index replicated on at least `quorumSize` nodes.
	quorumPos := len(matchIndices) - rm.quorumSize
	if quorumPos < 0 {
		rm.logger.Errorw("Invalid quorum calculation", "matchIndicesCount", len(matchIndices), "quorumSize", rm.quorumSize)
		return 0
	}

	return matchIndices[quorumPos]
}

// isEntryFromCurrentTerm checks if the log entry at the given index was created in the current leader term.
// Returns false and logs details if not, or if an error occurs.
func (rm *replicationManager) isEntryFromCurrentTerm(index types.Index, currentTerm types.Term) (bool, error) {
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
	select {
	case rm.applyNotifyCh <- struct{}{}:
		rm.logger.Debugw("Apply notification sent")
	default:
		// Apply loop slow or blocked?
		rm.logger.Warnw("Apply notification channel full; apply task potentially delayed")
		rm.metrics.ObserveApplyNotificationDropped()
	}
}

// Propose appends a command to the log if leader and starts replication.
func (rm *replicationManager) Propose(ctx context.Context, command []byte) (index types.Index, term types.Term, isLeader bool, err error) {
	if rm.isShutdown.Load() {
		return 0, 0, false, ErrShuttingDown
	}

	term, role, leaderID := rm.stateMgr.GetState()

	if role != types.RoleLeader || leaderID != rm.id { // Only leaders can propose
		rm.metrics.ObserveProposal(false, ProposalResultNotLeader)
		return 0, 0, false, ErrNotLeader
	}

	lastIndex := rm.logMgr.GetLastIndexUnsafe()
	newIndex := lastIndex + 1
	entry := types.LogEntry{
		Term:    term,
		Index:   newIndex,
		Command: command,
	}

	appendCtx, cancel := context.WithTimeout(ctx, defaultLogFetchTimeout)
	defer cancel()

	if err = rm.logMgr.AppendEntries(appendCtx, []types.LogEntry{entry}); err != nil {
		rm.logger.Errorw("Failed to append proposed entry to local log",
			"term", term, "index", newIndex, "error", err)
		rm.metrics.ObserveProposal(false, ProposalResultLogAppendFailed)
		return 0, term, true, fmt.Errorf("local log append failed: %w", err)
	}

	rm.logger.Debugw("Appended proposed command to local log",
		"term", term, "index", newIndex, "commandSize", len(command))
	rm.metrics.ObserveProposal(true, ProposalResultSuccess)

	// Using context.Background() so replication continues even if proposer context cancels.
	go rm.replicateToAllPeers(context.Background(), term)

	return newIndex, term, true, nil
}

// replicateToAllPeers initiates replication to all known peers.
// Called after a leader appends a new entry.
func (rm *replicationManager) replicateToAllPeers(ctx context.Context, term types.Term) {
	if rm.isShutdown.Load() || ctx.Err() != nil {
		return
	}

	currentTerm, role, leaderID := rm.stateMgr.GetState() // Might have changed since Propose started
	if role != types.RoleLeader || leaderID != rm.id || currentTerm != term {
		rm.logger.Debugw("replicateToAllPeers aborted: leadership lost or term changed",
			"originalTerm", term, "currentTerm", currentTerm, "role", role.String())
		return
	}

	commitIndex := rm.stateMgr.GetCommitIndex()
	rm.logger.Debugw("Initiating replication to all peers",
		"term", term, "commitIndex", commitIndex)

	for peerID := range rm.peers {
		if peerID == rm.id {
			continue // Skip self
		}
		go rm.replicateToPeerInternal(ctx, peerID, term, commitIndex, false)
	}
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
	enabled := rm.leaderLeaseEnabled
	rm.mu.RUnlock()

	if !enabled { // Check again under lock
		return false
	}

	now := rm.clock.Now()
	isValid := now.Before(expiry)

	if !isValid {
		rm.logger.Debugw("Leader lease check failed: lease expired",
			"now", now.Format(time.RFC3339Nano),
			"leaseExpiry", expiry.Format(time.RFC3339Nano))
	} else {
		rm.logger.Debugw("Leader lease check success: lease valid",
			"now", now.Format(time.RFC3339Nano),
			"leaseExpiry", expiry.Format(time.RFC3339Nano))
	}

	return isValid
}

// VerifyLeadershipAndGetCommitIndex implements the ReadIndex protocol.
func (rm *replicationManager) VerifyLeadershipAndGetCommitIndex(ctx context.Context) (types.Index, error) {
	if rm.isShutdown.Load() {
		return 0, ErrShuttingDown
	}

	term, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id {
		return 0, ErrNotLeader
	}

	if rm.leaderLeaseEnabled && rm.HasValidLease(ctx) { // Fast path: Leader Lease
		commitIndex := rm.stateMgr.GetCommitIndex()

		rm.logger.Debugw("ReadIndex served via leader lease",
			"term", term, "commitIndex", commitIndex)
		rm.metrics.ObserveReadIndex(true, "lease")

		return commitIndex, nil
	}

	// Slow path: Quorum Check
	rm.logger.Debugw("ReadIndex requires quorum check (lease disabled or expired)", "term", term)
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
	responseCh := make(chan bool, len(rm.peers)-1) // number of peers (excluding self)
	var wg sync.WaitGroup

	successCount := 1 // Leader counts as one success vote for itself

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
		rm.logger.Debugw("ReadIndex quorum check: all heartbeats completed or timed out")
	}()

	for {
		select {
		case ok, chanOpen := <-responseCh:
			if !chanOpen {
				rm.logger.Debugw("ReadIndex quorum check: response channel closed", "successCount", successCount, "quorumSize", rm.quorumSize)
				return successCount >= rm.quorumSize
			}
			if ok {
				successCount++
				rm.logger.Debugw("ReadIndex quorum check: received successful heartbeat ack", "successCount", successCount)
				if successCount >= rm.quorumSize {
					rm.logger.Debugw("ReadIndex quorum check: quorum reached", "successCount", successCount, "quorumSize", rm.quorumSize)
					return true
				}
			}
		case <-ctx.Done():
			rm.logger.Warnw("ReadIndex quorum check aborted due to context error",
				"error", ctx.Err(), "successCount", successCount, "quorumSize", rm.quorumSize)
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
		// PrevLogIndex, PrevLogTerm, Entries are zero/nil
	}

	reply, err := rm.networkMgr.SendAppendEntries(ctx, peerID, args)

	select {
	case <-ctx.Done():
		rm.logger.Debugw("ReadIndex heartbeat context done before sending result", "peerID", peerID, "error", ctx.Err())
		responseCh <- false
	default:
		if err != nil {
			rm.logger.Debugw("ReadIndex heartbeat RPC failed", "peerID", peerID, "term", term, "error", err)
			responseCh <- false
			return
		}

		if reply.Term > term {
			rm.logger.Infow("ReadIndex heartbeat rejected: peer has higher term",
				"peerID", peerID, "localTerm", term, "replyTerm", reply.Term)
			responseCh <- false
			// Note: CheckTermAndStepDown will handle leader stepping down if needed.
			return
		}

		responseCh <- true
	}
}

// HandleAppendEntries processes an incoming AppendEntries RPC from a leader.
func (rm *replicationManager) HandleAppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
	if rm.isShutdown.Load() {
		return nil, ErrShuttingDown
	}

	// Using context.Background() for state persistence during step down
	steppedDown, _ := rm.stateMgr.CheckTermAndStepDown(context.Background(), args.Term, args.LeaderID)

	currentTerm, currentRole, _ := rm.stateMgr.GetState() // Get potentially updated state
	reply := &types.AppendEntriesReply{Term: currentTerm, Success: false}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < currentTerm {
		rm.logger.Debugw("Rejected AppendEntries: Stale term",
			"rpcTerm", args.Term, "currentTerm", currentTerm, "fromLeader", args.LeaderID)
		return reply, nil
	}

	// If we stepped down, ensure we are now a follower. Reset leader hint.
	if steppedDown {
		rm.logger.Infow("Stepped down to follower due to AppendEntries RPC", "newTerm", currentTerm, "leaderHint", args.LeaderID)
	} else if currentRole != types.RoleFollower && args.Term == currentTerm {
		rm.logger.Infow("Received AppendEntries for current term while not Follower; becoming Follower",
			"term", currentTerm, "previousRole", currentRole, "leaderHint", args.LeaderID)

		rm.stateMgr.BecomeFollower(context.Background(), currentTerm, args.LeaderID) // Using background context for internal state change
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	lockedCurrentTerm, _, _ := rm.stateMgr.GetStateUnsafe() // Re-check term under lock, might have changed again
	if args.Term < lockedCurrentTerm {
		rm.logger.Debugw("Rejected AppendEntries under lock: Stale term discovered after lock acquisition",
			"rpcTerm", args.Term, "currentTerm", lockedCurrentTerm, "fromLeader", args.LeaderID)

		reply.Term = lockedCurrentTerm // Update reply term
		return reply, nil
	}
	reply.Term = lockedCurrentTerm // Ensure reply term is the latest

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	consistent, conflictIndex, conflictTerm := rm.checkLogConsistencyLocked(ctx, args.PrevLogIndex, args.PrevLogTerm)
	if !consistent {
		reply.Success = false
		reply.ConflictIndex = conflictIndex
		reply.ConflictTerm = conflictTerm
		rm.logger.Warnw("Rejected AppendEntries: Log inconsistency detected",
			"prevLogIndex", args.PrevLogIndex, "prevLogTerm", args.PrevLogTerm,
			"conflictIndex", conflictIndex, "conflictTerm", conflictTerm,
			"fromLeader", args.LeaderID)
		return reply, nil
	}

	// 3. If existing entry conflicts with new one (same index, different terms), delete existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	lastNewIndex, err := rm.processEntriesLocked(ctx, args.Entries, args.PrevLogIndex)
	if err != nil {
		rm.logger.Errorw("Failed to process log entries from leader",
			"term", args.Term, "leaderID", args.LeaderID, "prevLogIndex", args.PrevLogIndex, "error", err)
		reply.Success = false

		return reply, fmt.Errorf("failed to process/append entries: %w", err)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	rm.updateFollowerCommitIndexLocked(args.LeaderCommit, lastNewIndex)

	rm.recordAppendEntriesMetrics(args.Entries)
	reply.Success = true
	reply.MatchIndex = lastNewIndex

	return reply, nil
}

// checkLogConsistencyLocked checks if the local log matches the leader's expectation.
// Assumes rm.mu lock is held.
func (rm *replicationManager) checkLogConsistencyLocked(
	ctx context.Context,
	prevLogIndex types.Index,
	prevLogTerm types.Term,
) (consistent bool, conflictIndex types.Index, conflictTerm types.Term) {

	localLastIndex := rm.logMgr.GetLastIndexUnsafe()

	if prevLogIndex == 0 { // Leader sends AppendEntries for index 0 (immediately after start or snapshot)
		return true, 0, 0
	}

	if prevLogIndex > localLastIndex {
		rm.logger.Debugw("Log inconsistency: prevLogIndex exceeds local log",
			"prevLogIndex", prevLogIndex, "localLastIndex", localLastIndex)
		return false, localLastIndex + 1, 0
	}

	fetchCtx, cancel := context.WithTimeout(ctx, defaultTermFetchTimeout)
	defer cancel()

	localTermAtPrev, err := rm.logMgr.GetTermUnsafe(fetchCtx, prevLogIndex)
	if err != nil {
		if errors.Is(err, ErrCompacted) {
			firstIndex := rm.logMgr.GetFirstIndex()
			rm.logger.Infow("Log inconsistency: prevLogIndex has been compacted locally",
				"prevLogIndex", prevLogIndex, "firstAvailableIndex", firstIndex)

			return false, firstIndex, 0
		}

		rm.logger.Errorw("Failed to fetch local term at prevLogIndex",
			"prevLogIndex", prevLogIndex, "error", err)

		return false, prevLogIndex, 0
	}

	if localTermAtPrev != prevLogTerm {
		rm.logger.Warnw("Log inconsistency: term mismatch at prevLogIndex",
			"prevLogIndex", prevLogIndex,
			"localTerm", localTermAtPrev,
			"leaderTerm", prevLogTerm)

		firstIdxInConflictTerm, findErr := rm.logMgr.FindFirstIndexInTermUnsafe(ctx, localTermAtPrev, prevLogIndex)
		if findErr != nil {
			rm.logger.Warnw("Failed to find first index in conflicting term, using simpler hint",
				"conflictingTerm", localTermAtPrev, "searchUpToIndex", prevLogIndex, "error", findErr)
			return false, prevLogIndex, localTermAtPrev
		}

		return false, firstIdxInConflictTerm, localTermAtPrev
	}

	return true, 0, 0
}

// processEntriesLocked handles appending/truncating entries received from the leader.
// Returns the index of the last entry processed/appended.
// Assumes rm.mu lock is held.
func (rm *replicationManager) processEntriesLocked(ctx context.Context, entries []types.LogEntry, prevLogIndex types.Index) (types.Index, error) {
	localLastIndex := rm.logMgr.GetLastIndexUnsafe()

	if len(entries) == 0 { // heartbeat?
		return prevLogIndex, nil
	}

	firstNewEntryIndex := entries[0].Index

	conflictIndex := types.Index(0) // 0 means no conflict found yet
	for _, entry := range entries {
		localIndex := entry.Index
		if localIndex > localLastIndex {
			break
		}

		fetchCtx, cancel := context.WithTimeout(ctx, defaultTermFetchTimeout)
		localTerm, err := rm.logMgr.GetTermUnsafe(fetchCtx, localIndex)
		cancel()

		if err != nil {
			rm.logger.Warnw("Error checking local term during entry processing, assuming conflict",
				"index", localIndex, "error", err)
			conflictIndex = localIndex
			break
		}

		if localTerm != entry.Term {
			conflictIndex = localIndex
			rm.logger.Infow("Log conflict detected during entry processing",
				"index", conflictIndex, "localTerm", localTerm, "leaderTerm", entry.Term)
			break
		}

		firstNewEntryIndex = entry.Index + 1
	}

	if conflictIndex > 0 {
		if err := rm.truncateLogSuffixLocked(ctx, conflictIndex); err != nil {
			return 0, fmt.Errorf("failed to truncate conflicting suffix at index %d: %w", conflictIndex, err)
		}
		localLastIndex = conflictIndex - 1
	}

	entriesToAppend := rm.filterEntriesToAppend(entries, firstNewEntryIndex)

	if len(entriesToAppend) > 0 {
		if err := rm.appendNewEntriesLocked(ctx, entriesToAppend); err != nil {
			return localLastIndex, fmt.Errorf("failed to append new entries: %w", err)
		}
		localLastIndex = entriesToAppend[len(entriesToAppend)-1].Index
	}

	return localLastIndex, nil
}

// filterEntriesToAppend returns the sub-slice of entries starting from startIndex.
func (rm *replicationManager) filterEntriesToAppend(allEntries []types.LogEntry, startIndex types.Index) []types.LogEntry {
	for i, entry := range allEntries {
		if entry.Index >= startIndex {
			return allEntries[i:]
		}
	}
	return nil
}

// truncateLogSuffixLocked truncates the log from the specified index onward.
// Assumes rm.mu lock is held.
func (rm *replicationManager) truncateLogSuffixLocked(ctx context.Context, fromIndex types.Index) error {
	truncateCtx, cancel := context.WithTimeout(ctx, defaultLogFetchTimeout)
	defer cancel()

	rm.logger.Infow("Truncating local log suffix due to conflict", "fromIndex", fromIndex)

	if err := rm.logMgr.TruncateSuffix(truncateCtx, fromIndex); err != nil {
		rm.logger.Errorw("Failed to truncate local log suffix", "fromIndex", fromIndex, "error", err)
		return fmt.Errorf("logMgr.TruncateSuffix failed: %w", err)
	}
	return nil
}

// appendNewEntriesLocked appends the given entries to the local log.
// Assumes rm.mu lock is held.
func (rm *replicationManager) appendNewEntriesLocked(ctx context.Context, entries []types.LogEntry) error {
	appendCtx, cancel := context.WithTimeout(ctx, defaultLogFetchTimeout)
	defer cancel()

	rm.logger.Infow("Appending entries received from leader",
		"count", len(entries),
		"firstIndex", entries[0].Index,
		"lastIndex", entries[len(entries)-1].Index)

	if err := rm.logMgr.AppendEntries(appendCtx, entries); err != nil {
		rm.logger.Errorw("Failed to append new entries", "count", len(entries), "error", err)
		return fmt.Errorf("logMgr.AppendEntries failed: %w", err)
	}
	return nil
}

// updateFollowerCommitIndexLocked updates the follower's commit index based on the leader's commit.
// Assumes rm.mu lock is held.
func (rm *replicationManager) updateFollowerCommitIndexLocked(leaderCommit, lastProcessedIndex types.Index) {
	oldCommitIndex := rm.stateMgr.GetCommitIndexUnsafe()

	newCommitIndex := min(leaderCommit, lastProcessedIndex)

	if newCommitIndex > oldCommitIndex {
		if rm.stateMgr.UpdateCommitIndexUnsafe(newCommitIndex) {
			rm.logger.Debugw("Follower commit index advanced",
				"oldCommitIndex", oldCommitIndex,
				"newCommitIndex", newCommitIndex,
				"leaderCommit", leaderCommit,
				"lastProcessedIndex", lastProcessedIndex)

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
func (rm *replicationManager) GetPeerReplicationStatus() map[types.NodeID]types.PeerState {
	statusMap := make(map[types.NodeID]types.PeerState)

	if rm.isShutdown.Load() {
		return statusMap
	}

	_, role, _ := rm.stateMgr.GetState()
	if role != types.RoleLeader {
		return statusMap // Only leader tracks peer status
	}

	leaderLastIndex := rm.logMgr.GetLastIndexUnsafe()

	rm.mu.RLock()
	defer rm.mu.RUnlock()

	for peerID, pState := range rm.peerStates {
		var lag types.Index
		if leaderLastIndex > pState.MatchIndex {
			lag = leaderLastIndex - pState.MatchIndex
		}

		statusMap[peerID] = types.PeerState{
			NextIndex:          pState.NextIndex,
			MatchIndex:         pState.MatchIndex,
			IsActive:           pState.IsActive,
			LastActive:         pState.LastActive,
			SnapshotInProgress: pState.SnapshotInProgress,
			ReplicationLag:     lag,
		}
	}

	return statusMap
}

// ReplicateToPeer triggers replication to a specific peer. No-op if not leader.
func (rm *replicationManager) ReplicateToPeer(ctx context.Context, peerID types.NodeID, isHeartbeat bool) {
	if rm.isShutdown.Load() || ctx.Err() != nil {
		return
	}

	term, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id {
		rm.logger.Debugw("ReplicateToPeer skipped: not leader",
			"peerID", peerID, "role", role.String(), "term", term)
		return
	}

	commitIndex := rm.stateMgr.GetCommitIndex()

	go rm.replicateToPeerInternal(ctx, peerID, term, commitIndex, isHeartbeat)
}

// UpdatePeerAfterSnapshotSend updates peer state after a snapshot is successfully sent.
func (rm *replicationManager) UpdatePeerAfterSnapshotSend(peerID types.NodeID, snapshotIndex types.Index) {
	if rm.isShutdown.Load() {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	peerState, exists := rm.peerStates[peerID]
	if !exists {
		rm.logger.Warnw("Cannot update peer after snapshot: peer state not found", "peerID", peerID)
		return
	}

	if snapshotIndex > peerState.MatchIndex {
		peerState.MatchIndex = snapshotIndex
	}

	peerState.NextIndex = snapshotIndex + 1
	peerState.SnapshotInProgress = false
	peerState.IsActive = true
	peerState.LastActive = rm.clock.Now()

	rm.logger.Infow("Updated peer state after successful snapshot send",
		"peerID", peerID,
		"snapshotIndex", snapshotIndex,
		"newMatchIndex", peerState.MatchIndex,
		"newNextIndex", peerState.NextIndex)

	go rm.triggerCommitCheck()
}

// SetPeerSnapshotInProgress sets the snapshot-in-progress flag for a peer.
func (rm *replicationManager) SetPeerSnapshotInProgress(peerID types.NodeID, inProgress bool) {
	if rm.isShutdown.Load() {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.setPeerSnapshotInProgressLocked(peerID, inProgress)
}

// setPeerSnapshotInProgressLocked sets the flag, assuming caller holds write lock.
func (rm *replicationManager) setPeerSnapshotInProgressLocked(peerID types.NodeID, inProgress bool) {
	peerState, exists := rm.peerStates[peerID]
	if !exists {
		rm.logger.Warnw("Peer state not found while setting snapshot in progress flag", "peerID", peerID)
		return
	}

	if peerState.SnapshotInProgress != inProgress {
		previousState := peerState.SnapshotInProgress
		peerState.SnapshotInProgress = inProgress
		rm.logger.Debugw("Updated snapshot in progress flag for peer",
			"peerID", peerID,
			"newValue", inProgress,
			"previousValue", previousState)
	}
}

// Stop cleans up resources used by the replication manager.
func (rm *replicationManager) Stop() {
	rm.logger.Infow("Stopping replication manager...")

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.peerStates = make(map[types.NodeID]*types.PeerState)
	rm.leaseExpiry = time.Time{}
	close(rm.notifyCommitCh)

	rm.logger.Infow("Replication manager stopped")
}
