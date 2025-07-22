package raft

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// ReplicationManager handles log replication between Raft peers.
// It manages peer state tracking, heartbeats, log consistency checks,
// and commit index advancement for leaders.
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

	// GetPeerReplicationStatusUnsafe returns the current replication progress for each peer,
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

// ReplicationManagerDeps encapsulates dependencies for creating a replicationManager
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

// validateReplicationManagerDeps checks that all required dependencies are set
func validateReplicationManagerDeps(deps ReplicationManagerDeps) error {
	check := func(cond bool, fieldName string) error {
		if !cond {
			return fmt.Errorf("ReplicationManagerDeps: %s is required", fieldName)
		}
		return nil
	}
	checkPtr := func(ptr any, fieldName string) error {
		v := reflect.ValueOf(ptr)
		switch v.Kind() {
		case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Interface, reflect.Slice:
			if v.IsNil() {
				return fmt.Errorf("ReplicationManagerDeps: %s is required", fieldName)
			}
		default:
			if ptr == nil {
				return fmt.Errorf("ReplicationManagerDeps: %s is required", fieldName)
			}
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

// replicationManager is the concrete implementation of the ReplicationManager interface
type replicationManager struct {
	mu         *sync.RWMutex // Shared Raft mutex protecting state fields
	isShutdown *atomic.Bool  // Shared flag indicating Raft shutdown

	id         types.NodeID                // ID of the local Raft node
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

	peerStates       map[types.NodeID]*types.PeerState // Tracks per-peer replication progress
	heartbeatElapsed int                               // Ticks since last heartbeat broadcast

	leaderLeaseEnabled  bool          // Whether leader lease-based reads are enabled
	leaseExpiry         time.Time     // Timestamp when the current lease expires
	leaseDuration       time.Duration // Duration of the leader lease
	lastQuorumHeartbeat time.Time     // Time of last successful quorum heartbeat

	applyNotifyCh  chan<- struct{} // Signals Raft core to apply newly committed log entries
	notifyCommitCh chan struct{}   // Triggers commit index evaluation after replication events

	triggerCommitCheck func()

	stopOnce     sync.Once
	stopCh       chan struct{}
	commitLoopWg sync.WaitGroup // Add this for commitLoop goroutine synchronization
}

// NewReplicationManager constructs and initializes a new ReplicationManager instance
func NewReplicationManager(deps ReplicationManagerDeps) (ReplicationManager, error) {
	if err := validateReplicationManagerDeps(deps); err != nil {
		return nil, err
	}

	rm := &replicationManager{
		mu:         deps.Mu,
		isShutdown: deps.IsShutdownFlag,
		id:         deps.ID,
		peers:      deps.Peers,
		quorumSize: deps.QuorumSize,
		cfg:        deps.Cfg,

		stateMgr:    deps.StateMgr,
		logMgr:      deps.LogMgr,
		snapshotMgr: deps.SnapshotMgr,
		networkMgr:  deps.NetworkMgr,
		metrics:     deps.Metrics,
		logger:      deps.Logger,
		clock:       deps.Clock,

		peerStates:       make(map[types.NodeID]*types.PeerState),
		heartbeatElapsed: 0,

		leaderLeaseEnabled: deps.Cfg.FeatureFlags.EnableLeaderLease,

		applyNotifyCh:  deps.ApplyNotifyCh,
		notifyCommitCh: make(chan struct{}, commitNotifyChannelSize),
		stopCh:         make(chan struct{}),
		commitLoopWg:   sync.WaitGroup{}, // Initialize WaitGroup
	}

	rm.triggerCommitCheck = rm.defaultTriggerCommitCheck

	if rm.leaderLeaseEnabled {
		electionTicks := rm.cfg.Options.ElectionTickCount
		if electionTicks <= 0 {
			rm.logger.Warnw(
				"Using default ElectionTickCount for lease calculation",
				"configured",
				electionTicks,
				"default",
				DefaultElectionTickCount,
			)
			electionTicks = DefaultElectionTickCount
		}

		rm.leaseDuration = time.Duration(electionTicks) * NominalTickInterval / 2

		rm.logger.Infow("Leader lease enabled",
			"leaseDuration", rm.leaseDuration,
			"basedOnElectionTicks", electionTicks)
	}

	rm.logger.Infow("Replication manager initialized",
		"peerCount", len(deps.Peers)-1, // Exclude self
		"quorumSize", deps.QuorumSize,
		"leaderLease", rm.leaderLeaseEnabled)

	go rm.commitCheckLoop()

	return rm, nil
}

// SetNetworkManager allows injection of the network manager dependency
func (rm *replicationManager) SetNetworkManager(nm NetworkManager) {
	rm.networkMgr = nm
}

// Tick advances internal timers, sending heartbeats if needed
func (rm *replicationManager) Tick(ctx context.Context) {
	if rm.isShutdown.Load() {
		return
	}

	term, role, leaderID := rm.stateMgr.GetState()
	if role != types.RoleLeader || leaderID != rm.id {
		return // Only leaders tick for heartbeats
	}

	rm.mu.Lock()
	rm.heartbeatElapsed++
	shouldSendHeartbeat := rm.heartbeatElapsed >= rm.cfg.Options.HeartbeatTickCount
	if shouldSendHeartbeat {
		rm.heartbeatElapsed = 0
	}
	rm.mu.Unlock()

	if shouldSendHeartbeat {
		rm.logger.Debugw("Heartbeat timer elapsed, sending heartbeats",
			"term", term, "heartbeatTicks", rm.cfg.Options.HeartbeatTickCount)
		rm.SendHeartbeats(ctx)
	}
}

// InitializeLeaderState resets peer replication tracking based on current log
func (rm *replicationManager) InitializeLeaderState() {
	if rm.isShutdown.Load() {
		return
	}

	rm.logger.Infow("Initializing leader state for replication",
		"nodeID", rm.id,
		"peerCount", len(rm.peers)-1)

	// Get current log state using unsafe methods for better performance
	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()

	// Initialize peer states for all peers (excluding self)
	rm.peerStates = make(map[types.NodeID]*types.PeerState)
	for peerID := range rm.peers {
		if peerID == rm.id {
			continue // Skip self
		}

		rm.peerStates[peerID] = &types.PeerState{
			NextIndex:          lastLogIndex + 1, // Next log entry to send
			MatchIndex:         0,                // Highest log entry known to be replicated
			IsActive:           false,            // Will be set to true on first successful contact
			LastActive:         time.Time{},      // Will be updated on first contact
			SnapshotInProgress: false,            // No snapshots in progress initially
			ReplicationLag:     lastLogIndex,     // Initially fully behind
		}

		rm.logger.Debugw("Initialized peer state",
			"peerID", peerID,
			"nextIndex", lastLogIndex+1,
			"matchIndex", 0)
	}

	// Reset heartbeat tracking
	rm.heartbeatElapsed = 0

	// Initialize leader lease if enabled
	if rm.leaderLeaseEnabled {
		rm.leaseExpiry = time.Time{} // No lease initially
		rm.lastQuorumHeartbeat = time.Time{}
	}

	rm.logger.Infow("Leader state initialized successfully",
		"lastLogIndex", lastLogIndex,
		"peersInitialized", len(rm.peerStates))
}

// Propose submits a client command to be appended to the Raft log and replicated
func (rm *replicationManager) Propose(
	ctx context.Context,
	command []byte,
) (index types.Index, term types.Term, isLeader bool, err error) {
	if rm.isShutdown.Load() {
		return 0, 0, false, ErrShuttingDown
	}

	currentTerm, role, leaderID := rm.stateMgr.GetStateUnsafe()
	if role != types.RoleLeader || leaderID != rm.id {
		rm.logger.Debugw("Propose rejected: not leader",
			"role", role.String(),
			"term", currentTerm,
			"leaderID", leaderID)
		return 0, currentTerm, false, ErrNotLeader
	}

	// Get current log state to determine the next index
	lastIndex := rm.logMgr.GetLastIndexUnsafe()
	nextIndex := lastIndex + 1

	// Create the log entry
	entry := types.LogEntry{
		Index:   nextIndex,
		Term:    currentTerm,
		Command: command,
	}

	appendCtx, cancel := context.WithTimeout(ctx, defaultLogFetchTimeout)
	defer cancel()

	err = rm.logMgr.AppendEntriesUnsafe(appendCtx, []types.LogEntry{entry})
	if err != nil {
		rm.logger.Errorw("Failed to append proposed entry",
			"error", err,
			"index", nextIndex,
			"term", currentTerm)
		return 0, currentTerm, true, fmt.Errorf("failed to append entry: %w", err)
	}

	rm.logger.Debugw("Proposed entry appended to log",
		"index", nextIndex,
		"term", currentTerm,
		"commandSize", len(command))

	go rm.replicateToAllPeers(currentTerm, false)

	return nextIndex, currentTerm, true, nil
}

// SendHeartbeats dispatches AppendEntries RPCs with no log entries to all peers
func (rm *replicationManager) SendHeartbeats(ctx context.Context) {
	if rm.isShutdown.Load() {
		return
	}

	term, role, leaderID := rm.stateMgr.GetStateUnsafe()
	if role != types.RoleLeader || leaderID != rm.id {
		return // Only leaders send heartbeats
	}

	rm.logger.Debugw("Sending heartbeats to all peers", "term", term)
	go rm.replicateToAllPeers(term, true)
}

// HasValidLease returns whether the leader still holds a valid lease
func (rm *replicationManager) HasValidLease(ctx context.Context) bool {
	if rm.isShutdown.Load() || !rm.leaderLeaseEnabled {
		return false
	}

	rm.mu.RLock()
	defer rm.mu.RUnlock()

	now := rm.clock.Now()
	hasValidLease := now.Before(rm.leaseExpiry)

	rm.logger.Debugw("Checking leader lease validity",
		"now", now,
		"leaseExpiry", rm.leaseExpiry,
		"hasValidLease", hasValidLease)

	return hasValidLease
}

// VerifyLeadershipAndGetCommitIndex verifies leadership by reaching a quorum
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

	// If we have a valid lease, we can return immediately
	if rm.HasValidLease(ctx) {
		commitIndex := rm.stateMgr.GetCommitIndex()
		rm.logger.Debugw("Using leader lease for ReadIndex",
			"commitIndex", commitIndex,
			"term", term)
		return commitIndex, nil
	}

	// Otherwise, we need to verify leadership with a quorum
	rm.logger.Debugw("Verifying leadership with quorum for ReadIndex", "term", term)

	// Send heartbeats and wait for quorum responses
	successCount := rm.sendHeartbeatsAndCountResponses(ctx, term)

	if successCount >= rm.quorumSize-1 { // -1 because we count ourselves
		commitIndex := rm.stateMgr.GetCommitIndex()

		// Update lease if enabled
		if rm.leaderLeaseEnabled {
			rm.mu.Lock()
			rm.updateLeaderLease()
			rm.mu.Unlock()
		}

		rm.logger.Debugw("Leadership verified with quorum",
			"successCount", successCount,
			"quorumNeeded", rm.quorumSize-1,
			"commitIndex", commitIndex)

		return commitIndex, nil
	}

	rm.logger.Warnw("Failed to verify leadership with quorum",
		"successCount", successCount,
		"quorumNeeded", rm.quorumSize-1)

	return 0, fmt.Errorf("failed to verify leadership: only %d/%d peers responded",
		successCount, rm.quorumSize-1)
}

// sendHeartbeatsAndCountResponses sends heartbeats and waits for responses
func (rm *replicationManager) sendHeartbeatsAndCountResponses(
	ctx context.Context,
	term types.Term,
) int {
	rm.mu.RLock()
	peerIDs := make([]types.NodeID, 0, len(rm.peerStates))
	for peerID := range rm.peerStates {
		peerIDs = append(peerIDs, peerID)
	}
	rm.mu.RUnlock()

	if len(peerIDs) == 0 {
		return 0
	}

	commitIndex := rm.stateMgr.GetCommitIndex()
	responseCh := make(chan bool, len(peerIDs))

	// Send heartbeats concurrently
	for _, peerID := range peerIDs {
		go func(peer types.NodeID) {
			success := rm.sendSingleHeartbeatAndWait(ctx, peer, term, commitIndex)
			responseCh <- success
		}(peerID)
	}

	// Count successful responses with timeout
	successCount := 0
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultAppendEntriesTimeout)
	defer cancel()

	for i := 0; i < len(peerIDs); i++ {
		select {
		case success := <-responseCh:
			if success {
				successCount++
			}
		case <-timeoutCtx.Done():
			rm.logger.Debugw("Timeout waiting for heartbeat responses",
				"responsesReceived", i,
				"totalPeers", len(peerIDs))
			return successCount
		}
	}

	return successCount
}

// sendSingleHeartbeatAndWait sends a heartbeat to one peer and waits for response
func (rm *replicationManager) sendSingleHeartbeatAndWait(
	ctx context.Context,
	peerID types.NodeID,
	term types.Term,
	commitIndex types.Index,
) bool {
	// Get peer state safely
	nextIndex, _, ok := rm.getPeerReplicationState(peerID)
	if !ok {
		return false
	}

	// Get previous log info
	prevLogIndex, prevLogTerm, ok := rm.getPrevLogInfo(ctx, peerID, nextIndex, term)
	if !ok {
		return false
	}

	// Build heartbeat (empty entries)
	args := rm.buildAppendEntriesArgs(term, commitIndex, prevLogIndex, prevLogTerm, nil)

	rpcCtx, cancel := context.WithTimeout(ctx, defaultAppendEntriesTimeout)
	defer cancel()

	reply, err := rm.networkMgr.SendAppendEntries(rpcCtx, peerID, args)
	if err != nil {
		rm.logger.Debugw("Heartbeat failed for leadership verification",
			"peerID", peerID,
			"error", err)
		return false
	}

	// Check for higher term
	if reply.Term > term {
		rm.logger.Infow("Higher term detected during leadership verification",
			"currentTerm", term,
			"replyTerm", reply.Term,
			"peerID", peerID)
		go func() {
			stepCtx, stepCancel := context.WithTimeout(
				context.Background(),
				defaultTermFetchTimeout,
			)
			defer stepCancel()
			rm.stateMgr.CheckTermAndStepDown(stepCtx, reply.Term, peerID)
		}()
		return false
	}

	return reply.Success
}

// commitCheckLoop processes commit index advancement requests in background
func (rm *replicationManager) commitCheckLoop() {
	rm.commitLoopWg.Add(1)       // Increment counter when loop starts
	defer rm.commitLoopWg.Done() // Decrement counter when loop exits

	defer rm.logger.Debugw("Commit check loop terminated")

	for {
		select {
		case <-rm.notifyCommitCh:
			if !rm.isShutdown.Load() {
				rm.MaybeAdvanceCommitIndex()
			}
		case <-rm.stopCh:
			return // Exit loop
		}
	}
}

// HandleAppendEntries processes incoming AppendEntries RPCs from leaders
func (rm *replicationManager) HandleAppendEntries(
	ctx context.Context,
	args *types.AppendEntriesArgs,
) (*types.AppendEntriesReply, error) {
	if rm.isShutdown.Load() {
		rm.logger.Debugw("Rejecting AppendEntries: shutting down",
			"fromLeader", args.LeaderID)
		return nil, ErrShuttingDown
	}

	rm.logger.Debugw("Processing AppendEntries RPC",
		"fromLeader", args.LeaderID,
		"term", args.Term,
		"prevLogIndex", args.PrevLogIndex,
		"prevLogTerm", args.PrevLogTerm,
		"entriesCount", len(args.Entries),
		"leaderCommit", args.LeaderCommit)

	// Step down if we see a higher term
	steppedDown, previousTerm := rm.stateMgr.CheckTermAndStepDown(
		context.Background(), // Use background context for persistence
		args.Term,
		args.LeaderID,
	)

	if steppedDown {
		rm.logger.Infow("Stepped down due to higher term in AppendEntries",
			"previousTerm", previousTerm,
			"newTerm", args.Term,
			"fromLeader", args.LeaderID)
	}

	// Get current state after potential step down
	currentTerm, currentRole, _ := rm.stateMgr.GetState()
	reply := &types.AppendEntriesReply{Term: currentTerm, Success: false}

	// Reject if term is stale
	if args.Term < currentTerm {
		rm.logger.Debugw("Rejected AppendEntries: stale term",
			"rpcTerm", args.Term,
			"currentTerm", currentTerm,
			"fromLeader", args.LeaderID)
		return reply, nil
	}

	// Ensure we're a follower for current term
	if currentRole != types.RoleFollower && args.Term == currentTerm {
		rm.logger.Infow("Received AppendEntries for current term while not follower; transitioning",
			"term", currentTerm,
			"previousRole", currentRole,
			"leaderHint", args.LeaderID)
		rm.stateMgr.BecomeFollower(context.Background(), currentTerm, args.LeaderID)
	}

	// Process the request under lock to prevent races
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Double-check term under lock
	lockedTerm, _, _ := rm.stateMgr.GetStateUnsafe()
	if args.Term < lockedTerm {
		rm.logger.Debugw("Rejected AppendEntries under lock: stale term",
			"rpcTerm", args.Term,
			"lockedTerm", lockedTerm,
			"fromLeader", args.LeaderID)
		reply.Term = lockedTerm
		return reply, nil
	}
	reply.Term = lockedTerm

	// Check log consistency
	consistent, conflictIndex, conflictTerm := rm.checkLogConsistencyLocked(
		ctx, args.PrevLogIndex, args.PrevLogTerm)
	if !consistent {
		reply.Success = false
		reply.ConflictIndex = conflictIndex
		reply.ConflictTerm = conflictTerm

		rm.logger.Warnw("AppendEntries rejected: log inconsistency",
			"prevLogIndex", args.PrevLogIndex,
			"prevLogTerm", args.PrevLogTerm,
			"conflictIndex", conflictIndex,
			"conflictTerm", conflictTerm,
			"fromLeader", args.LeaderID)

		// Include match index hint if possible
		if conflictIndex > 1 {
			matchCtx, cancel := context.WithTimeout(ctx, defaultTermFetchTimeout)
			defer cancel()
			if term, err := rm.logMgr.GetTermUnsafe(matchCtx, conflictIndex-1); err == nil {
				reply.MatchIndex = conflictIndex - 1
				rm.logger.Debugw("Providing match index hint",
					"matchIndex", reply.MatchIndex,
					"matchTerm", term)
			}
		}

		rm.recordAppendEntriesMetrics(args.Entries)
		return reply, nil
	}

	// Handle conflicting entries and append new ones
	var lastProcessedIndex types.Index = args.PrevLogIndex

	if len(args.Entries) > 0 {
		processed, err := rm.processEntriesLocked(ctx, args.Entries, args.PrevLogIndex)
		if err != nil {
			rm.logger.Errorw("Failed to process entries",
				"entriesCount", len(args.Entries),
				"prevLogIndex", args.PrevLogIndex,
				"error", err)
			return reply, err
		}
		lastProcessedIndex = processed
	}

	// Update commit index based on leader's commit
	rm.updateFollowerCommitIndexLocked(args.LeaderCommit, lastProcessedIndex)

	// Success!
	reply.Success = true
	rm.recordAppendEntriesMetrics(args.Entries)

	rm.logger.Debugw("AppendEntries processed successfully",
		"entriesProcessed", len(args.Entries),
		"lastProcessedIndex", lastProcessedIndex,
		"commitIndex", rm.stateMgr.GetCommitIndexUnsafe())

	return reply, nil
}

// checkLogConsistencyLocked verifies log consistency at prevLogIndex
// Returns (consistent, conflictIndex, conflictTerm)
func (rm *replicationManager) checkLogConsistencyLocked(
	ctx context.Context,
	prevLogIndex types.Index,
	prevLogTerm types.Term,
) (bool, types.Index, types.Term) {
	// If prevLogIndex is 0, it's always consistent (before any entries)
	if prevLogIndex == 0 {
		return true, 0, 0
	}

	lastIndex := rm.logMgr.GetLastIndexUnsafe()

	// If we don't have the previous entry, we're inconsistent
	if prevLogIndex > lastIndex {
		rm.logger.Debugw("Log consistency check: missing previous entry",
			"prevLogIndex", prevLogIndex,
			"lastLogIndex", lastIndex)
		return false, lastIndex + 1, 0
	}

	// Check if the term matches
	termCtx, cancel := context.WithTimeout(ctx, defaultTermFetchTimeout)
	defer cancel()

	actualTerm, err := rm.logMgr.GetTermUnsafe(termCtx, prevLogIndex)
	if err != nil {
		rm.logger.Warnw("Failed to get term for consistency check",
			"prevLogIndex", prevLogIndex,
			"error", err)

		// If we can't read the term, assume inconsistency and provide fallback
		if errors.Is(err, ErrCompacted) {
			return false, 1, 0 // Start from beginning if compacted
		}
		return false, prevLogIndex, 0
	}

	if actualTerm != prevLogTerm {
		rm.logger.Debugw("Log consistency check: term mismatch",
			"prevLogIndex", prevLogIndex,
			"expectedTerm", prevLogTerm,
			"actualTerm", actualTerm)
		return false, prevLogIndex, actualTerm
	}

	return true, 0, 0
}

// processEntriesLocked handles conflicting entries and appends new ones
// Returns the last processed index
func (rm *replicationManager) processEntriesLocked(
	ctx context.Context,
	entries []types.LogEntry,
	prevLogIndex types.Index,
) (types.Index, error) {
	if len(entries) == 0 {
		return prevLogIndex, nil
	}

	firstNewIndex := prevLogIndex + 1
	lastIndex := rm.logMgr.GetLastIndexUnsafe()

	rm.logger.Debugw("Processing entries",
		"entriesCount", len(entries),
		"firstNewIndex", firstNewIndex,
		"lastLogIndex", lastIndex,
		"firstEntryIndex", entries[0].Index,
		"lastEntryIndex", entries[len(entries)-1].Index)

	// Find where logs diverge
	var conflictIndex types.Index = 0
	for i, entry := range entries {
		currentIndex := firstNewIndex + types.Index(i)

		if currentIndex > lastIndex {
			// We don't have this entry, so append all remaining
			conflictIndex = currentIndex
			break
		}

		// Check if existing entry conflicts
		termCtx, cancel := context.WithTimeout(ctx, defaultTermFetchTimeout)
		existingTerm, err := rm.logMgr.GetTermUnsafe(termCtx, currentIndex)
		cancel()

		if err != nil {
			rm.logger.Warnw("Failed to get term during entry processing",
				"index", currentIndex,
				"error", err)
			conflictIndex = currentIndex
			break
		}

		if existingTerm != entry.Term {
			rm.logger.Infow("Found conflicting entry, will truncate",
				"index", currentIndex,
				"existingTerm", existingTerm,
				"newEntryTerm", entry.Term)
			conflictIndex = currentIndex
			break
		}
	}

	// If we found conflicts, truncate the log from that point
	if conflictIndex > 0 && conflictIndex <= lastIndex {
		truncateCtx, cancel := context.WithTimeout(ctx, defaultLogFetchTimeout)
		defer cancel()

		if err := rm.logMgr.TruncateSuffixUnsafe(truncateCtx, conflictIndex); err != nil {
			return prevLogIndex, fmt.Errorf(
				"failed to truncate log from index %d: %w",
				conflictIndex,
				err,
			)
		}

		rm.logger.Infow("Truncated conflicting entries",
			"fromIndex", conflictIndex,
			"previousLastIndex", lastIndex)
	}

	// Append new entries
	if conflictIndex > 0 {
		conflictEntryIndex := int(conflictIndex - firstNewIndex)
		entriesToAppend := entries[conflictEntryIndex:]

		if len(entriesToAppend) > 0 {
			if err := rm.appendNewEntriesLocked(ctx, entriesToAppend); err != nil {
				return prevLogIndex, fmt.Errorf("failed to append new entries: %w", err)
			}

			rm.logger.Infow("Appended new entries",
				"count", len(entriesToAppend),
				"firstIndex", entriesToAppend[0].Index,
				"lastIndex", entriesToAppend[len(entriesToAppend)-1].Index)
		}
	}

	return entries[len(entries)-1].Index, nil
}

// appendNewEntriesLocked appends entries to the log
func (rm *replicationManager) appendNewEntriesLocked(
	ctx context.Context,
	entries []types.LogEntry,
) error {
	appendCtx, cancel := context.WithTimeout(ctx, defaultLogFetchTimeout)
	defer cancel()

	rm.logger.Debugw("Appending entries from leader",
		"count", len(entries),
		"firstIndex", entries[0].Index,
		"lastIndex", entries[len(entries)-1].Index)

	if err := rm.logMgr.AppendEntriesUnsafe(appendCtx, entries); err != nil {
		rm.logger.Errorw("Failed to append entries", "count", len(entries), "error", err)
		return fmt.Errorf("logMgr.AppendEntriesUnsafe failed: %w", err)
	}
	return nil
}

// updateFollowerCommitIndexLocked updates commit index based on leader's commit
func (rm *replicationManager) updateFollowerCommitIndexLocked(
	leaderCommit, lastProcessedIndex types.Index,
) {
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

// recordAppendEntriesMetrics tracks metrics for AppendEntries RPCs
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

// replicateToAllPeers starts replication/heartbeat to all peers concurrently
func (rm *replicationManager) replicateToAllPeers(term types.Term, isHeartbeat bool) {
	if rm.isShutdown.Load() {
		return
	}

	peerIDs := make([]types.NodeID, 0, len(rm.peerStates))
	for peerID := range rm.peerStates {
		peerIDs = append(peerIDs, peerID)
	}

	commitIndex := rm.stateMgr.GetCommitIndex()

	rm.logger.Debugw("Starting replication to peers",
		"peerCount", len(peerIDs),
		"term", term,
		"commitIndex", commitIndex,
		"isHeartbeat", isHeartbeat)

	// Start replication to each peer concurrently
	for _, peerID := range peerIDs {
		go rm.replicateToPeerInternal(context.Background(), peerID, term, commitIndex, isHeartbeat)
	}
}

// replicateToPeerInternal handles replication or heartbeat to a specific peer
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

	rm.logger.Debugw("Starting replication to peer",
		"peerID", peerID,
		"term", term,
		"commitIndex", commitIndex,
		"isHeartbeat", isHeartbeat)

	// Get peer state safely
	nextIndex, snapshotInProgress, ok := rm.getPeerReplicationState(peerID)
	if !ok {
		rm.logger.Errorw("Cannot replicate: peer state not found", "peerID", peerID)
		return
	}

	// Check if we need to send a snapshot
	firstLogIndex := rm.logMgr.GetFirstIndex()
	if nextIndex < firstLogIndex {
		if snapshotInProgress {
			rm.logger.Debugw("Snapshot already in progress for peer",
				"peerID", peerID,
				"nextIndex", nextIndex,
				"firstLogIndex", firstLogIndex)
			return
		}
		rm.initiateSnapshotIfNeeded(peerID, nextIndex, term, firstLogIndex)
		return
	}

	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()

	// For non-heartbeats, check if peer is up-to-date
	if !isHeartbeat && nextIndex > lastLogIndex {
		rm.logger.Debugw("Peer is up-to-date, no replication needed",
			"peerID", peerID,
			"nextIndex", nextIndex,
			"lastLogIndex", lastLogIndex)
		return
	}

	// Get previous log info for consistency check
	prevLogIndex, prevLogTerm, ok := rm.getPrevLogInfo(ctx, peerID, nextIndex, term)
	if !ok {
		return
	}

	// Get entries to send (if not heartbeat)
	var entries []types.LogEntry
	if !isHeartbeat && nextIndex <= lastLogIndex {
		entries = rm.getEntriesForPeer(ctx, peerID, nextIndex, lastLogIndex, term)
		if entries == nil && nextIndex <= lastLogIndex {
			rm.logger.Debugw("Failed to get entries, possibly due to compaction",
				"peerID", peerID,
				"nextIndex", nextIndex,
				"lastLogIndex", lastLogIndex)
			return
		}
	}

	// Build and send AppendEntries RPC
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

// getPeerReplicationState safely returns peer's nextIndex and snapshot status
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

// initiateSnapshotIfNeeded starts snapshot if peer is too far behind
func (rm *replicationManager) initiateSnapshotIfNeeded(
	peerID types.NodeID,
	nextIndex types.Index,
	term types.Term,
	firstLogIndex types.Index,
) {
	rm.logger.Infow("Peer is behind compacted log, initiating snapshot",
		"peerID", peerID,
		"nextIndex", nextIndex,
		"firstLogIndex", firstLogIndex)

	rm.SetPeerSnapshotInProgress(peerID, true)
	go rm.snapshotMgr.SendSnapshot(context.Background(), peerID, term)
}

// getPrevLogInfo gets previous log index and term for consistency check
func (rm *replicationManager) getPrevLogInfo(
	ctx context.Context,
	peerID types.NodeID,
	nextIndex types.Index,
	term types.Term,
) (prevLogIndex types.Index, prevLogTerm types.Term, ok bool) {
	prevLogIndex = nextIndex - 1
	if prevLogIndex == 0 {
		return 0, 0, true // No previous entry
	}

	termCtx, cancel := context.WithTimeout(ctx, defaultTermFetchTimeout)
	defer cancel()

	var err error
	prevLogTerm, err = rm.logMgr.GetTermUnsafe(termCtx, prevLogIndex)
	if err != nil {
		rm.logger.Warnw("Failed to get previous log term",
			"peerID", peerID,
			"prevLogIndex", prevLogIndex,
			"error", err)

		if errors.Is(err, ErrCompacted) {
			// Log was compacted, trigger snapshot
			rm.logger.Infow("Previous log entry compacted, will initiate snapshot",
				"peerID", peerID,
				"prevLogIndex", prevLogIndex)
			go rm.initiateSnapshotIfNeeded(peerID, nextIndex, term, rm.logMgr.GetFirstIndex())
		}
		return 0, 0, false
	}

	return prevLogIndex, prevLogTerm, true
}

// getEntriesForPeer retrieves log entries to send to a peer
func (rm *replicationManager) getEntriesForPeer(
	ctx context.Context,
	peerID types.NodeID,
	nextIndex, lastLogIndex types.Index,
	term types.Term,
) []types.LogEntry {
	// Limit the number of entries per batch for performance
	maxEntries := rm.cfg.Options.MaxLogEntriesPerRequest
	if maxEntries <= 0 {
		maxEntries = 100 // Default batch size
	}

	endIndex := min(lastLogIndex+1, nextIndex+types.Index(maxEntries))

	entriesCtx, cancel := context.WithTimeout(ctx, defaultLogFetchTimeout)
	defer cancel()

	entries, err := rm.logMgr.GetEntriesUnsafe(entriesCtx, nextIndex, endIndex)
	if err != nil {
		rm.logger.Warnw("Failed to get entries for peer",
			"peerID", peerID,
			"nextIndex", nextIndex,
			"endIndex", endIndex,
			"error", err)

		if errors.Is(err, ErrCompacted) {
			// Entries were compacted, need snapshot
			rm.logger.Infow("Entries compacted, will initiate snapshot",
				"peerID", peerID,
				"nextIndex", nextIndex)
			go rm.initiateSnapshotIfNeeded(peerID, nextIndex, term, rm.logMgr.GetFirstIndex())
		}
		return nil
	}

	rm.logger.Debugw("Retrieved entries for peer",
		"peerID", peerID,
		"count", len(entries),
		"startIndex", nextIndex,
		"endIndex", endIndex)

	return entries
}

// buildAppendEntriesArgs constructs AppendEntries RPC arguments
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

// logReplicationAttempt logs debug information about replication attempts
func (rm *replicationManager) logReplicationAttempt(
	peerID types.NodeID,
	isHeartbeat bool,
	entries []types.LogEntry,
	prevLogIndex types.Index,
) {
	if isHeartbeat || len(entries) == 0 {
		rm.logger.Debugw("Sending heartbeat",
			"to", peerID,
			"prevLogIndex", prevLogIndex)
	} else {
		rm.logger.Debugw("Sending log entries",
			"to", peerID,
			"count", len(entries),
			"firstIndex", entries[0].Index,
			"lastIndex", entries[len(entries)-1].Index,
			"prevLogIndex", prevLogIndex)
	}
}

// handleAppendError processes errors from AppendEntries RPCs
func (rm *replicationManager) handleAppendError(
	peerID types.NodeID,
	term types.Term,
	err error,
	isHeartbeat bool,
	latency time.Duration,
) {
	var errType string
	logLevel := "warn"

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		errType = "timeout"
	case errors.Is(err, context.Canceled):
		errType = "canceled"
		logLevel = "debug" // Less severe
	case errors.Is(err, ErrPeerNotFound):
		errType = "peer_not_found"
		logLevel = "error"
	case errors.Is(err, ErrShuttingDown):
		errType = "shutting_down"
		logLevel = "debug"
	default:
		errType = "network_error"
		logLevel = "error"
	}

	// Log with appropriate level
	logArgs := []any{
		"AppendEntries RPC failed",
		"peerID", peerID,
		"term", term,
		"error", err,
		"errorType", errType,
		"isHeartbeat", isHeartbeat,
		"latency", latency,
	}

	switch logLevel {
	case "debug":
		rm.logger.Debugw(logArgs[0].(string), logArgs[1:]...)
	case "warn":
		rm.logger.Warnw(logArgs[0].(string), logArgs[1:]...)
	case "error":
		rm.logger.Errorw(logArgs[0].(string), logArgs[1:]...)
	}

	// Update peer state and handle consecutive failures
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if peerState, exists := rm.peerStates[peerID]; exists {
		peerState.IsActive = false
		peerState.ConsecutiveFailures++

		// If we have multiple consecutive failures, try to reset the connection
		if peerState.ConsecutiveFailures > 3 { // Threshold for resetting connection
			rm.logger.Warnw(
				"Multiple consecutive failures to peer, requesting connection reset",
				"peerID",
				peerID,
				"failures",
				peerState.ConsecutiveFailures,
			)
			// Reset the counter to avoid spamming reset requests
			peerState.ConsecutiveFailures = 0
			// Use a background context for the reset attempt so it doesn't block this goroutine
			go func() {
				if err := rm.networkMgr.ResetConnection(context.Background(), peerID); err != nil {
					rm.logger.Warnw("Failed to reset connection", "peerID", peerID, "error", err)
				}
			}()
		}
	}

	// Record metrics
	if isHeartbeat {
		rm.metrics.ObserveHeartbeat(peerID, false, latency)
	} else {
		rm.metrics.ObservePeerReplication(peerID, false, ReplicationResultFailed)
	}
}

// handleAppendReply processes successful AppendEntries replies
func (rm *replicationManager) handleAppendReply(
	peerID types.NodeID,
	term types.Term,
	args *types.AppendEntriesArgs,
	reply *types.AppendEntriesReply,
	isHeartbeat bool,
	rpcLatency time.Duration,
) {
	rm.logger.Debugw("Received AppendEntries reply",
		"peerID", peerID,
		"term", term,
		"replyTerm", reply.Term,
		"success", reply.Success,
		"isHeartbeat", isHeartbeat,
		"latency", rpcLatency)

	// Check for higher term
	if reply.Term > term {
		rm.logger.Infow("Detected higher term in AppendEntries reply, stepping down",
			"currentTerm", term,
			"replyTerm", reply.Term,
			"peerID", peerID)

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTermFetchTimeout)
			defer cancel()
			rm.stateMgr.CheckTermAndStepDown(ctx, reply.Term, peerID)
		}()
		return
	}

	// Handle the reply based on success
	if reply.Success {
		rm.mu.Lock()

		if peerState, exists := rm.peerStates[peerID]; exists {
			// Reset failure counter on any successful reply
			peerState.ConsecutiveFailures = 0
			peerState.IsActive = true
			peerState.LastActive = rm.clock.Now()

			if len(args.Entries) > 0 {
				// Update replication indices
				lastEntryIndex := args.Entries[len(args.Entries)-1].Index
				if lastEntryIndex > peerState.MatchIndex {
					oldMatchIndex := peerState.MatchIndex
					peerState.MatchIndex = lastEntryIndex
					peerState.NextIndex = lastEntryIndex + 1

					rm.logger.Debugw("Updated peer replication state",
						"peerID", peerID,
						"oldMatchIndex", oldMatchIndex,
						"newMatchIndex", peerState.MatchIndex,
						"newNextIndex", peerState.NextIndex,
						"entriesReplicated", len(args.Entries))

					// Update replication lag
					leaderLastIndex := rm.logMgr.GetLastIndexUnsafe()
					if leaderLastIndex > peerState.MatchIndex {
						peerState.ReplicationLag = leaderLastIndex - peerState.MatchIndex
					} else {
						peerState.ReplicationLag = 0
					}

					// Trigger commit check asynchronously
					go rm.triggerCommitCheck()
				}
			}

			// Record metrics
			if isHeartbeat {
				rm.metrics.ObserveHeartbeat(peerID, true, rpcLatency)

				// Update leader lease if enabled
				if rm.leaderLeaseEnabled {
					rm.updateLeaderLease()
				}
			} else {
				rm.metrics.ObservePeerReplication(peerID, true, ReplicationResultSuccess)
			}
		}
		rm.mu.Unlock()
	} else {
		rm.logger.Warnw("AppendEntries rejected by peer",
			"peerID", peerID,
			"term", term,
			"prevLogIndex", args.PrevLogIndex,
			"prevLogTerm", args.PrevLogTerm,
			"conflictIndex", reply.ConflictIndex,
			"conflictTerm", reply.ConflictTerm,
			"matchIndexHint", reply.MatchIndex)

		rm.mu.Lock()
		if peerState, exists := rm.peerStates[peerID]; exists {
			peerState.ConsecutiveFailures = 0
			peerState.LastActive = rm.clock.Now()
			peerState.IsActive = true

			// Handle log inconsistency using conflict information
			rm.handleLogInconsistency(peerState, peerID, reply)

			// Record metrics
			if isHeartbeat {
				rm.metrics.ObserveHeartbeat(peerID, false, rpcLatency)
			} else {
				rm.metrics.ObservePeerReplication(peerID, false, ReplicationResultLogMismatch)
			}
		}
		rm.mu.Unlock()
	}
}

// handleLogInconsistency adjusts peer's nextIndex based on conflict information
func (rm *replicationManager) handleLogInconsistency(
	peerState *types.PeerState,
	peerID types.NodeID,
	reply *types.AppendEntriesReply,
) {
	oldNextIndex := peerState.NextIndex

	// Use optimized log rollback if conflict term is provided
	if reply.ConflictTerm > 0 {
		// Look for the last entry with the conflicting term in our log
		if conflictTermIndex, err := rm.findLastLogEntryWithTerm(reply.ConflictTerm); err == nil {
			// We have entries with the conflict term
			if conflictTermIndex >= peerState.NextIndex {
				// The found index is not helpful, use conflict index
				peerState.NextIndex = max(1, reply.ConflictIndex)
				rm.logger.Debugw("Conflict term optimization not applicable, using conflict index",
					"peerID", peerID,
					"conflictTerm", reply.ConflictTerm,
					"foundIndex", conflictTermIndex,
					"currentNextIndex", oldNextIndex,
					"newNextIndex", peerState.NextIndex)
			} else {
				// Set nextIndex to the entry after our last entry with that term
				peerState.NextIndex = max(1, conflictTermIndex+1)
				rm.logger.Infow("Applied conflict term optimization",
					"peerID", peerID,
					"conflictTerm", reply.ConflictTerm,
					"conflictTermLastIndex", conflictTermIndex,
					"newNextIndex", peerState.NextIndex)
			}
		} else {
			// We don't have any entries with the conflict term, use conflict index
			peerState.NextIndex = max(1, reply.ConflictIndex)
			rm.logger.Infow("Conflict term not found locally, using conflict index",
				"peerID", peerID,
				"conflictTerm", reply.ConflictTerm,
				"conflictIndex", reply.ConflictIndex,
				"newNextIndex", peerState.NextIndex)
		}
	} else if reply.ConflictIndex > 0 {
		// No conflict term provided, use conflict index directly
		peerState.NextIndex = max(1, reply.ConflictIndex)
		rm.logger.Infow("Using conflict index for rollback",
			"peerID", peerID,
			"conflictIndex", reply.ConflictIndex,
			"newNextIndex", peerState.NextIndex)
	} else {
		// No conflict information, decrement nextIndex conservatively
		peerState.NextIndex = max(1, peerState.NextIndex-1)
		rm.logger.Infow("No conflict info, decrementing nextIndex",
			"peerID", peerID,
			"oldNextIndex", oldNextIndex,
			"newNextIndex", peerState.NextIndex)
	}

	// Update match index if peer provided a hint
	if reply.MatchIndex > 0 && reply.MatchIndex > peerState.MatchIndex {
		oldMatchIndex := peerState.MatchIndex
		peerState.MatchIndex = reply.MatchIndex
		rm.logger.Debugw("Updated match index from hint",
			"peerID", peerID,
			"oldMatchIndex", oldMatchIndex,
			"newMatchIndex", peerState.MatchIndex)
	}

	rm.logger.Debugw("Adjusted peer state for log inconsistency",
		"peerID", peerID,
		"oldNextIndex", oldNextIndex,
		"newNextIndex", peerState.NextIndex,
		"matchIndex", peerState.MatchIndex)
}

// findLastLogEntryWithTerm searches for the last entry with the given term
func (rm *replicationManager) findLastLogEntryWithTerm(term types.Term) (types.Index, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultLogFetchTimeout)
	defer cancel()

	index, err := rm.logMgr.FindLastEntryWithTermUnsafe(ctx, term, 0)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			rm.logger.Debugw("Term not found in local log", "term", term)
			return 0, fmt.Errorf("term %d not found: %w", term, err)
		}
		rm.logger.Warnw("Error searching for term", "term", term, "error", err)
		return 0, fmt.Errorf("search failed for term %d: %w", term, err)
	}

	rm.logger.Debugw("Found last entry with term", "term", term, "index", index)
	return index, nil
}

// updateLeaderLease updates the leader lease expiry time
func (rm *replicationManager) updateLeaderLease() {
	if !rm.leaderLeaseEnabled {
		return
	}

	now := rm.clock.Now()
	rm.lastQuorumHeartbeat = now
	rm.leaseExpiry = now.Add(rm.leaseDuration)

	rm.logger.Debugw("Updated leader lease",
		"leaseExpiry", rm.leaseExpiry,
		"leaseDuration", rm.leaseDuration)
}

// MaybeAdvanceCommitIndex evaluates and potentially advances the commit index
func (rm *replicationManager) MaybeAdvanceCommitIndex() {
	if rm.isShutdown.Load() {
		return
	}

	currentTerm, role, _ := rm.stateMgr.GetState()
	if role != types.RoleLeader {
		return // Only leaders advance commit index
	}

	oldCommitIndex := rm.stateMgr.GetCommitIndex()
	lastLogIndex := rm.logMgr.GetLastIndexUnsafe()

	if oldCommitIndex >= lastLogIndex {
		return // No new entries to commit
	}

	rm.logger.Debugw("Evaluating commit index advancement",
		"currentTerm", currentTerm,
		"oldCommitIndex", oldCommitIndex,
		"lastLogIndex", lastLogIndex)

	// Collect match indices from all nodes (including leader)
	matchIndices := rm.collectMatchIndices(lastLogIndex)
	potentialCommitIndex := rm.calculateQuorumMatchIndex(matchIndices)

	if potentialCommitIndex <= oldCommitIndex {
		rm.logger.Debugw("No commit advancement possible",
			"potentialCommitIndex", potentialCommitIndex,
			"oldCommitIndex", oldCommitIndex)
		return
	}

	// Verify the entry is from the current leader term (Raft safety requirement)
	if ok, err := rm.isEntryFromCurrentTerm(potentialCommitIndex, currentTerm); !ok {
		if err != nil {
			rm.logger.Warnw("Failed to verify entry term for commit",
				"index", potentialCommitIndex,
				"term", currentTerm,
				"error", err)
		} else {
			rm.logger.Debugw("Cannot commit entry from previous term",
				"index", potentialCommitIndex,
				"currentTerm", currentTerm)
		}
		return
	}

	// Advance commit index
	if rm.stateMgr.UpdateCommitIndex(potentialCommitIndex) {
		rm.logger.Infow("Commit index advanced",
			"oldCommitIndex", oldCommitIndex,
			"newCommitIndex", potentialCommitIndex,
			"term", currentTerm,
			"quorumMatchIndices", matchIndices)

		rm.triggerApplyNotify()
		rm.metrics.ObserveCommitIndex(potentialCommitIndex - oldCommitIndex)
	}
}

// collectMatchIndices gathers match indices from all peers plus leader's log
func (rm *replicationManager) collectMatchIndices(leaderLastIndex types.Index) []types.Index {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	matchIndices := make([]types.Index, 0, len(rm.peers))

	// Add leader's last index
	matchIndices = append(matchIndices, leaderLastIndex)

	// Add peer match indices
	for peerID, state := range rm.peerStates {
		if peerID != rm.id {
			matchIndices = append(matchIndices, state.MatchIndex)
			rm.logger.Debugw("Collected match index",
				"peerID", peerID,
				"matchIndex", state.MatchIndex,
				"isActive", state.IsActive)
		}
	}

	return matchIndices
}

// calculateQuorumMatchIndex finds the highest index replicated on a quorum
func (rm *replicationManager) calculateQuorumMatchIndex(matchIndices []types.Index) types.Index {
	if len(matchIndices) == 0 {
		return 0
	}

	// Sort in ascending order
	slices.Sort(matchIndices)

	// The index at position (len - quorumSize) is the highest index
	// replicated on at least quorumSize nodes
	quorumPos := len(matchIndices) - rm.quorumSize
	if quorumPos < 0 {
		rm.logger.Errorw("Invalid quorum calculation",
			"matchIndicesCount", len(matchIndices),
			"quorumSize", rm.quorumSize,
			"matchIndices", matchIndices)
		return 0
	}

	quorumIndex := matchIndices[quorumPos]

	rm.logger.Debugw("Calculated quorum match index",
		"matchIndices", matchIndices,
		"quorumSize", rm.quorumSize,
		"quorumIndex", quorumIndex)

	return quorumIndex
}

// isEntryFromCurrentTerm checks if entry at index is from current leader term
func (rm *replicationManager) isEntryFromCurrentTerm(
	index types.Index,
	currentTerm types.Term,
) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTermFetchTimeout)
	defer cancel()

	entryTerm, err := rm.logMgr.GetTermUnsafe(ctx, index)
	if err != nil {
		return false, fmt.Errorf("failed to get term for index %d: %w", index, err)
	}

	if entryTerm != currentTerm {
		rm.logger.Debugw("Entry not from current term",
			"index", index,
			"entryTerm", entryTerm,
			"currentTerm", currentTerm)
		return false, nil
	}

	return true, nil
}

// triggerApplyNotify notifies the apply loop about new committed entries
func (rm *replicationManager) triggerApplyNotify() {
	select {
	case rm.applyNotifyCh <- struct{}{}:
		rm.logger.Debugw("Apply notification sent")
	default:
		rm.logger.Warnw("Apply notification channel full, apply loop may be slow")
		rm.metrics.ObserveApplyNotificationDropped()
	}
}

// defaultTriggerCommitCheck triggers a commit index evaluation
func (rm *replicationManager) defaultTriggerCommitCheck() {
	// Check for shutdown first
	select {
	case <-rm.stopCh:
		rm.logger.Debugw("Commit check skipped: replication manager is stopping")
		return
	default:
	}

	// Only try to send if not shutting down
	if rm.isShutdown.Load() {
		return
	}

	select {
	case rm.notifyCommitCh <- struct{}{}:
		rm.logger.Debugw("Commit check triggered")
		rm.metrics.ObserveCommitCheckTriggered()
	default:
		rm.logger.Debugw("Commit check already pending")
		rm.metrics.ObserveCommitCheckPending()
	}
}

// GetPeerReplicationStatusUnsafe returns replication status for all peers
// Caller must hold rm.mu lock
func (rm *replicationManager) GetPeerReplicationStatusUnsafe() map[types.NodeID]types.PeerState {
	statusMap := make(map[types.NodeID]types.PeerState)

	if rm.isShutdown.Load() {
		return statusMap
	}

	_, role, _ := rm.stateMgr.GetStateUnsafe()
	if role != types.RoleLeader {
		return statusMap // Only leader tracks peer status
	}

	leaderLastIndex := rm.logMgr.GetLastIndexUnsafe()

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

// ReplicateToPeer initiates replication to a specific peer
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
		rm.logger.Debugw("ReplicateToPeer skipped: not leader",
			"peerID", peerID,
			"role", role.String(),
			"term", term)
		return
	}

	commitIndex := rm.stateMgr.GetCommitIndex()
	go rm.replicateToPeerInternal(ctx, peerID, term, commitIndex, isHeartbeat)
}

// UpdatePeerAfterSnapshotSend updates peer state after snapshot completion
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
		rm.logger.Warnw("Cannot update peer after snapshot: peer state not found", "peerID", peerID)
		return
	}

	oldMatchIndex := peerState.MatchIndex
	oldNextIndex := peerState.NextIndex

	// Update indices based on snapshot
	if snapshotIndex > peerState.MatchIndex {
		peerState.MatchIndex = snapshotIndex
	}
	peerState.NextIndex = snapshotIndex + 1
	peerState.SnapshotInProgress = false
	peerState.IsActive = true
	peerState.LastActive = rm.clock.Now()

	// Update replication lag
	leaderLastIndex := rm.logMgr.GetLastIndexUnsafe()
	if leaderLastIndex > peerState.MatchIndex {
		peerState.ReplicationLag = leaderLastIndex - peerState.MatchIndex
	} else {
		peerState.ReplicationLag = 0
	}

	rm.logger.Infow("Updated peer state after successful snapshot",
		"peerID", peerID,
		"snapshotIndex", snapshotIndex,
		"oldMatchIndex", oldMatchIndex,
		"newMatchIndex", peerState.MatchIndex,
		"oldNextIndex", oldNextIndex,
		"newNextIndex", peerState.NextIndex,
		"replicationLag", peerState.ReplicationLag)

	// Trigger commit check since peer state changed
	go rm.triggerCommitCheck()
}

// SetPeerSnapshotInProgress marks snapshot progress for a peer
func (rm *replicationManager) SetPeerSnapshotInProgress(peerID types.NodeID, inProgress bool) {
	if rm.isShutdown.Load() {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.setPeerSnapshotInProgressLocked(peerID, inProgress)
}

// setPeerSnapshotInProgressLocked sets snapshot flag with lock already held
func (rm *replicationManager) setPeerSnapshotInProgressLocked(
	peerID types.NodeID,
	inProgress bool,
) {
	peerState, exists := rm.peerStates[peerID]
	if !exists {
		rm.logger.Warnw("Peer state not found while setting snapshot flag",
			"peerID", peerID,
			"inProgress", inProgress)
		return
	}

	if peerState.SnapshotInProgress != inProgress {
		previousState := peerState.SnapshotInProgress
		peerState.SnapshotInProgress = inProgress
		rm.logger.Debugw("Updated snapshot in progress flag",
			"peerID", peerID,
			"previousState", previousState,
			"newState", inProgress)
	}
}

// Stop shuts down background tasks and releases resources
func (rm *replicationManager) Stop() {
	rm.stopOnce.Do(func() {
		rm.logger.Infow("Stopping replication manager", "nodeID", rm.id)

		// Signal shutdown to background goroutines
		close(rm.stopCh)

		// Wait for commitCheckLoop to exit
		rm.commitLoopWg.Wait() // Wait for the commitCheckLoop goroutine to finish

		// Clean up state under lock
		rm.mu.Lock()
		defer rm.mu.Unlock()

		// Mark as shutdown if not already done
		if !rm.isShutdown.Load() {
			rm.logger.Debugw("Setting shutdown flag", "nodeID", rm.id)
		}

		// Close commit notification channel
		// Draining explicitly here is less critical now that we wait for the loop,
		// but it doesn't hurt.
		select {
		case <-rm.notifyCommitCh:
			// Drain any pending notifications
		default:
		}
		close(rm.notifyCommitCh)

		// Clear peer states
		if len(rm.peerStates) > 0 {
			rm.logger.Debugw("Clearing peer states", "peerCount", len(rm.peerStates))
			rm.peerStates = make(map[types.NodeID]*types.PeerState)
		}

		// Clear leader lease state
		rm.leaseExpiry = time.Time{}
		rm.lastQuorumHeartbeat = time.Time{}

		rm.logger.Infow("Replication manager stopped successfully", "nodeID", rm.id)
	})
}
