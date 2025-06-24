package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// raftNode implements the Raft consensus protocol.
//
// It coordinates a distributed state machine by managing logs, elections,
// replication, and snapshots across a cluster of peers. The implementation
// uses external ticking - the embedding application must call Tick()
// periodically to drive elections, heartbeats, and other time-based events.
//
// raftNode is thread-safe for all public methods.
type raftNode struct {
	id types.NodeID // Node's unique identifier

	mu         *sync.RWMutex // Guards shared state
	isShutdown atomic.Bool   // True if node is shutting down
	isLeader   atomic.Bool   // Cached leadership flag for fast checking

	stateMgr       StateManager
	logMgr         LogManager
	electionMgr    ElectionManager
	snapshotMgr    SnapshotManager
	replicationMgr ReplicationManager

	networkMgrSet atomic.Bool
	networkMgr    NetworkManager // For peer-to-peer RPC communication

	applier Applier // For applying committed entries to state machine
	logger  logger.Logger
	metrics Metrics
	clock   Clock

	stopCh          chan struct{}       // Signals global shutdown
	applyCh         chan types.ApplyMsg // Delivers committed entries/snapshots
	applyNotifyCh   chan struct{}       // Signals new entries are committable
	leaderChangeCh  chan types.NodeID   // Notifies of leader changes
	applyLoopStopCh chan struct{}       // Stops apply loop specifically
	applyLoopDoneCh chan struct{}       // Apply loop signals completion
	forceApplyCh    chan struct{}       // For forced apply triggers

	applyEntryTimeout   time.Duration
	fetchEntriesTimeout time.Duration
}

// SetNetworkManager injects the network manager dependency into the replication manager.
// Must be called before Start().
func (r *raftNode) SetNetworkManager(nm NetworkManager) {
	if !r.networkMgrSet.CompareAndSwap(false, true) {
		r.logger.Warnw("SetNetworkManager called more than once; ignoring")
		return
	}

	r.networkMgr = nm

	if r.snapshotMgr != nil {
		r.snapshotMgr.SetNetworkManager(nm)
	}
	if r.replicationMgr != nil {
		r.replicationMgr.SetNetworkManager(nm)
	}
	if r.electionMgr != nil {
		r.electionMgr.SetNetworkManager(nm)
	}
}

// Start initializes the Raft node and launches background tasks.
func (r *raftNode) Start() error {
	if r.networkMgr == nil {
		return fmt.Errorf("network manager must be set before starting raft node")
	}

	if r.isShutdown.Load() {
		r.logger.Warnw("Start called on shutting down node")
		return ErrShuttingDown
	}

	r.logger.Infow("Starting Raft node", "id", r.id)
	r.metrics.IncCounter("raft_node_start_total")
	startTime := r.clock.Now()

	if err := r.networkMgr.Start(); err != nil {
		r.logger.Errorw("Failed to start network manager", "error", err)
		r.metrics.IncCounter("raft_node_start_failures", "component", "network")
		return fmt.Errorf("network initialization failed: %w", err)
	}

	initCtx := context.Background()

	if err := r.stateMgr.Initialize(initCtx); err != nil {
		r.logger.Errorw("Failed to initialize state manager", "error", err)
		_ = r.networkMgr.Stop()
		r.metrics.IncCounter("raft_node_start_failures", "component", "state")
		return fmt.Errorf("state manager initialization failed: %w", err)
	}

	if err := r.logMgr.Initialize(initCtx); err != nil {
		r.logger.Errorw("Failed to initialize log manager", "error", err)
		_ = r.networkMgr.Stop()
		r.metrics.IncCounter("raft_node_start_failures", "component", "log")
		return fmt.Errorf("log manager initialization failed: %w", err)
	}

	if err := r.snapshotMgr.Initialize(initCtx); err != nil {
		r.logger.Errorw("Failed to initialize snapshot manager", "error", err)
		_ = r.networkMgr.Stop()
		r.metrics.IncCounter("raft_node_start_failures", "component", "snapshot")
		return fmt.Errorf("snapshot manager initialization failed: %w", err)
	}

	if err := r.electionMgr.Initialize(initCtx); err != nil {
		r.logger.Errorw("Failed to initialize election manager", "error", err)
		_ = r.networkMgr.Stop()
		r.metrics.IncCounter("raft_node_start_failures", "component", "election")
		return fmt.Errorf("election manager initialization failed: %w", err)
	}

	go r.runApplyLoop()
	go r.handleLeaderChanges()

	startupDuration := r.clock.Since(startTime)
	r.metrics.ObserveHistogram("raft_node_startup_duration_seconds", startupDuration.Seconds())
	r.logger.Infow("Raft node started successfully",
		"id", r.id,
		"startup_duration", startupDuration.String(),
		"note", "Application must call Tick() periodically")

	return nil
}

// Stop gracefully shuts down the Raft node.
func (r *raftNode) Stop(ctx context.Context) error {
	startTime := r.clock.Now()

	if !r.isShutdown.CompareAndSwap(false, true) {
		r.logger.Infow("Stop called on already stopping/stopped node", "id", r.id)
		return r.waitForApplyLoop(ctx)
	}

	r.logger.Infow("Initiating Raft node shutdown", "id", r.id)
	r.metrics.IncCounter("raft_node_stop_total")

	r.initiateShutdown()
	r.stopManagers()

	if err := r.waitForApplyLoop(ctx); err != nil {
		r.logger.Warnw("Timeout waiting for apply loop to stop", "error", err)
		r.metrics.ObserveComponentStopTimeout("apply_loop")
		return err
	}

	if err := r.stopNetworkManager(); err != nil {
		r.logger.Warnw("Error stopping network manager", "error", err)
		r.metrics.IncCounter("raft_node_stop_failures", "component", "network")
	}

	close(r.applyCh)

	shutdownDuration := r.clock.Since(startTime)
	r.metrics.ObserveHistogram("raft_node_shutdown_duration_seconds", shutdownDuration.Seconds())
	r.logger.Infow("Raft node stopped successfully",
		"id", r.id,
		"shutdown_duration", shutdownDuration.String())
	return nil
}

// initiateShutdown signals internal components to begin shutdown.
func (r *raftNode) initiateShutdown() {
	close(r.stopCh)
	close(r.applyLoopStopCh)
}

// stopManagers stops all Raft subsystems except the network manager.
func (r *raftNode) stopManagers() {
	r.logger.Debugw("Stopping Raft managers")

	r.replicationMgr.Stop()
	r.snapshotMgr.Stop()
	r.electionMgr.Stop()
	r.logMgr.Stop()
	r.stateMgr.Stop()

	r.logger.Debugw("All managers stopped")
}

// waitForApplyLoop waits for the apply loop to finish or the context to expire.
func (r *raftNode) waitForApplyLoop(ctx context.Context) error {
	select {
	case <-r.applyLoopDoneCh:
		r.logger.Debugw("Apply loop finished")
		return nil
	case <-ctx.Done():
		r.logger.Warnw("Context expired while waiting for apply loop", "error", ctx.Err())
		return ctx.Err()
	}
}

// stopNetworkManager stops the network manager gracefully.
func (r *raftNode) stopNetworkManager() error {
	r.logger.Debugw("Stopping network manager")
	err := r.networkMgr.Stop()

	if err != nil && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

// Tick advances the logical clock, driving time-dependent operations.
func (r *raftNode) Tick(ctx context.Context) {
	if r.isShutdown.Load() {
		return
	}

	r.metrics.IncCounter("raft_ticks_total")
	term, role, leaderID := r.stateMgr.GetState()

	r.logger.Debugw("Processing tick",
		"term", term,
		"role", role.String(),
		"leaderID", leaderID)

	// Use internal context to avoid cancellation issues
	internalCtx := context.Background()

	// Handle different roles appropriately
	switch role {
	case types.RoleLeader:
		// Leaders handle replication and heartbeats
		r.replicationMgr.Tick(internalCtx)

		// Ensure we're still leader after tick
		if currentTerm, currentRole, _ := r.stateMgr.GetState(); currentRole != types.RoleLeader || currentTerm != term {
			r.logger.Infow("Lost leadership during tick",
				"originalTerm", term, "currentTerm", currentTerm, "currentRole", currentRole.String())
		}

	case types.RoleFollower, types.RoleCandidate:
		// Non-leaders handle election timeouts
		r.electionMgr.Tick(internalCtx)

	default:
		r.logger.Warnw("Unknown role during tick", "role", role.String())
	}

	// Always tick snapshot manager
	r.snapshotMgr.Tick(internalCtx)
	r.metrics.ObserveTick(role)
}

// Propose submits a new command to be replicated.
func (r *raftNode) Propose(
	ctx context.Context,
	command []byte,
) (types.Index, types.Term, bool, error) {
	startTime := r.clock.Now()
	r.metrics.IncCounter("raft_propose_total")

	// Validate state
	if r.isShutdown.Load() {
		r.metrics.IncCounter("raft_propose_rejected", "reason", "shutdown")
		return 0, 0, false, ErrShuttingDown
	}

	if len(command) == 0 {
		r.metrics.IncCounter("raft_propose_rejected", "reason", "empty_command")
		return 0, 0, false, errors.New("command cannot be empty")
	}

	// Check leadership before attempting proposal
	term, role, leaderID := r.stateMgr.GetState()
	if role != types.RoleLeader {
		r.logger.Infow("Proposal rejected: not leader",
			"commandSize", len(command),
			"currentRole", role.String(),
			"leaderID", leaderID)
		r.metrics.IncCounter("raft_propose_rejected", "reason", "not_leader")
		return 0, term, false, ErrNotLeader
	}

	// Attempt the proposal
	index, proposalTerm, isLeader, err := r.replicationMgr.Propose(ctx, command)

	// Handle results
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			r.logger.Infow("Proposal rejected: lost leadership during proposal",
				"commandSize", len(command),
				"originalTerm", term,
				"proposalTerm", proposalTerm)
			r.metrics.IncCounter("raft_propose_rejected", "reason", "leadership_lost")
		} else {
			r.logger.Warnw("Proposal failed",
				"commandSize", len(command),
				"error", err)
			r.metrics.IncCounter("raft_propose_failed", "reason", "error")
		}
		return index, proposalTerm, isLeader, err
	}

	// Success metrics and logging
	latency := r.clock.Since(startTime)
	r.logger.Infow("Command proposed successfully",
		"index", index,
		"term", proposalTerm,
		"commandSize", len(command),
		"latency", latency)

	r.metrics.ObserveHistogram("raft_propose_latency_seconds", latency.Seconds())
	r.metrics.IncCounter("raft_propose_success_total")
	r.metrics.AddCounter("raft_propose_bytes_total", float64(len(command)))

	return index, proposalTerm, true, nil
}

// ReadIndex implements linearizable reads.
func (r *raftNode) ReadIndex(ctx context.Context) (types.Index, error) {
	startTime := r.clock.Now()
	r.metrics.IncCounter("raft_read_index_total")

	if r.isShutdown.Load() {
		r.metrics.IncCounter("raft_read_index_rejected", "reason", "shutdown")
		return 0, ErrShuttingDown
	}

	index, err := r.replicationMgr.VerifyLeadershipAndGetCommitIndex(ctx)
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			r.logger.Infow("ReadIndex rejected: not leader",
				"leaderID", r.GetLeaderID())
			r.metrics.IncCounter("raft_read_index_rejected", "reason", "not_leader")
		} else {
			r.logger.Warnw("ReadIndex failed", "error", err)
			r.metrics.IncCounter("raft_read_index_failed", "reason", "error")
		}
		return 0, err
	}

	latency := r.clock.Since(startTime)
	r.logger.Debugw("ReadIndex successful",
		"commitIndex", index,
		"latency", latency)

	r.metrics.ObserveHistogram("raft_read_index_latency_seconds", latency.Seconds())
	r.metrics.IncCounter("raft_read_index_success_total")

	return index, nil
}

// Status returns a snapshot of the Raft node's state.
func (r *raftNode) Status() types.RaftStatus {
	term, role, leaderID := r.stateMgr.GetState()
	commitIndex := r.stateMgr.GetCommitIndex()
	lastApplied := r.stateMgr.GetLastApplied()

	lastLogIndex := r.logMgr.GetLastIndexUnsafe()

	return types.RaftStatus{
		ID:            r.id,
		Term:          term,
		Role:          role,
		LeaderID:      leaderID,
		CommitIndex:   commitIndex,
		LastApplied:   lastApplied,
		LastLogIndex:  lastLogIndex,
		SnapshotIndex: 0, // Implement based on your snapshot logic
		SnapshotTerm:  0, // Implement based on your snapshot logic
	}
}

// GetState returns the current term and leadership status
func (r *raftNode) GetState() (types.Term, bool) {
	term, role, _ := r.stateMgr.GetState()
	isLeader := (role == types.RoleLeader)

	// Update cached value
	r.isLeader.Store(isLeader)

	return term, isLeader
}

// GetLeaderID returns the ID of the current leader if known
func (r *raftNode) GetLeaderID() types.NodeID {
	_, _, leaderID := r.stateMgr.GetState()
	return leaderID
}

// GetCommitIndex returns the highest committed log index
func (r *raftNode) GetCommitIndex() types.Index {
	return r.stateMgr.GetCommitIndex()
}

// ApplyChannel returns the channel for delivering committed entries
func (r *raftNode) ApplyChannel() <-chan types.ApplyMsg {
	return r.applyCh
}

// LeaderChangeChannel returns the channel for leader change notifications
func (r *raftNode) LeaderChangeChannel() <-chan types.NodeID {
	return r.leaderChangeCh
}

// RequestVote handles vote requests from candidates
func (r *raftNode) RequestVote(
	ctx context.Context,
	args *types.RequestVoteArgs,
) (*types.RequestVoteReply, error) {
	r.metrics.IncCounter("raft_request_vote_received_total",
		"is_prevote", fmt.Sprintf("%t", args.IsPreVote),
		"candidate", string(args.CandidateID))

	if r.isShutdown.Load() {
		term, _, _ := r.stateMgr.GetState()
		r.metrics.IncCounter("raft_request_vote_rejected", "reason", "shutdown")
		return &types.RequestVoteReply{Term: term, VoteGranted: false}, ErrShuttingDown
	}

	startTime := r.clock.Now()
	reply, err := r.electionMgr.HandleRequestVote(ctx, args)
	if err != nil {
		r.metrics.IncCounter("raft_request_vote_errors_total")
		return reply, err
	}

	latency := r.clock.Since(startTime)
	r.metrics.ObserveHistogram("raft_request_vote_latency_seconds", latency.Seconds())

	if reply.VoteGranted {
		r.metrics.IncCounter("raft_votes_granted_total",
			"term", fmt.Sprintf("%d", reply.Term),
			"candidate", string(args.CandidateID))
	} else {
		r.metrics.IncCounter("raft_votes_denied_total",
			"term", fmt.Sprintf("%d", reply.Term),
			"candidate", string(args.CandidateID))
	}

	return reply, nil
}

// AppendEntries handles log replication RPCs from the leader
func (r *raftNode) AppendEntries(
	ctx context.Context,
	args *types.AppendEntriesArgs,
) (*types.AppendEntriesReply, error) {
	if r.isShutdown.Load() {
		currentTerm, _, _ := r.stateMgr.GetState()
		return &types.AppendEntriesReply{
			Term:    currentTerm,
			Success: false,
		}, ErrShuttingDown
	}

	startTime := r.clock.Now()
	isHeartbeat := len(args.Entries) == 0

	r.logger.Debugw("AppendEntries received",
		"from", args.LeaderID,
		"term", args.Term,
		"prevLogIndex", args.PrevLogIndex,
		"prevLogTerm", args.PrevLogTerm,
		"entriesCount", len(args.Entries),
		"leaderCommit", args.LeaderCommit,
		"isHeartbeat", isHeartbeat)

	// CRITICAL: Reset election timer immediately on valid AppendEntries
	// This prevents unnecessary elections
	currentTerm, _, _ := r.stateMgr.GetState()

	// Reset timer if this is from a leader in current or higher term
	if args.Term >= currentTerm && args.LeaderID != unknownNodeID {
		r.electionMgr.ResetTimerOnHeartbeat()
	}

	// Handle the AppendEntries through replication manager
	reply, err := r.replicationMgr.HandleAppendEntries(ctx, args)

	latency := r.clock.Since(startTime)

	if err != nil {
		r.logger.Warnw("AppendEntries handling failed",
			"from", args.LeaderID, "term", args.Term, "error", err, "latency", latency)
		r.metrics.IncCounter("raft_append_entries_errors", "reason", "handler_error")
		return reply, err
	}

	// Log the result
	if reply.Success {
		if !isHeartbeat {
			r.logger.Debugw("AppendEntries succeeded",
				"from", args.LeaderID, "term", args.Term, "entriesCount", len(args.Entries),
				"latency", latency)
		}
		r.metrics.IncCounter("raft_append_entries_success", "type", getAppendEntriesType(isHeartbeat))
	} else {
		r.logger.Debugw("AppendEntries rejected",
			"from", args.LeaderID, "term", args.Term, "replyTerm", reply.Term,
			"entriesCount", len(args.Entries), "latency", latency)
		r.metrics.IncCounter("raft_append_entries_rejected", "reason", "consistency_check")
	}

	r.metrics.ObserveHistogram("raft_append_entries_latency_seconds", latency.Seconds())
	return reply, nil
}

// Helper function to classify AppendEntries types
func getAppendEntriesType(isHeartbeat bool) string {
	if isHeartbeat {
		return "heartbeat"
	}
	return "replication"
}

// InstallSnapshot handles snapshot transfers from the leader
func (r *raftNode) InstallSnapshot(
	ctx context.Context,
	args *types.InstallSnapshotArgs,
) (*types.InstallSnapshotReply, error) {
	r.metrics.IncCounter("raft_install_snapshot_received_total")
	r.metrics.AddCounter("raft_install_snapshot_bytes_received", float64(len(args.Data)))

	if r.isShutdown.Load() {
		term, _, _ := r.stateMgr.GetState()
		r.metrics.IncCounter("raft_install_snapshot_rejected", "reason", "shutdown")
		return &types.InstallSnapshotReply{Term: term}, ErrShuttingDown
	}

	currentTerm, _, _ := r.stateMgr.GetState()
	if args.Term >= currentTerm {
		r.electionMgr.ResetTimerOnHeartbeat()
	}

	startTime := r.clock.Now()
	reply, err := r.snapshotMgr.HandleInstallSnapshot(ctx, args)

	latency := r.clock.Since(startTime)
	r.metrics.ObserveHistogram("raft_install_snapshot_latency_seconds", latency.Seconds())

	if reply != nil {
		r.metrics.IncCounter("raft_install_snapshot_success_total")
	}

	return reply, err
}

// runApplyLoop applies committed log entries to the state machine.
func (r *raftNode) runApplyLoop() {
	defer close(r.applyLoopDoneCh)
	r.logger.Infow("Apply loop started")
	r.metrics.IncCounter("raft_apply_loop_started_total")

	applyTicker := r.clock.NewTicker(100 * time.Millisecond)
	defer applyTicker.Stop()

	for {
		select {
		case <-r.applyNotifyCh:
			r.logger.Debugw("Apply notification received")
			r.applyCommittedEntries()

		case <-r.forceApplyCh:
			// Handle forced apply triggers
			r.logger.Debugw("Force apply triggered")
			r.applyCommittedEntries()

		case <-applyTicker.Chan():
			// Periodic apply check to catch any missed notifications
			r.applyCommittedEntries()

		case <-r.applyLoopStopCh:
			r.logger.Infow("Apply loop stopping")
			r.metrics.ObserveApplyLoopStopped("stop_signal")
			return

		case <-r.stopCh:
			r.logger.Infow("Apply loop stopping due to global shutdown")
			r.metrics.ObserveApplyLoopStopped("global_shutdown")
			return
		}
	}
}

// applyCommittedEntries applies log entries from lastApplied+1 to commitIndex.
func (r *raftNode) applyCommittedEntries() {
	r.mu.RLock()
	commitIdx := r.stateMgr.GetCommitIndexUnsafe()
	lastAppliedIdx := r.stateMgr.GetLastAppliedUnsafe()
	r.mu.RUnlock()

	r.logger.Debugw("Apply check",
		"commitIndex", commitIdx,
		"lastApplied", lastAppliedIdx)

	if lastAppliedIdx >= commitIdx {
		return
	}

	nextApplyIdx := lastAppliedIdx + 1
	endIdx := commitIdx + 1

	r.logger.Infow("Applying committed entries",
		"nextApplyIdx", nextApplyIdx,
		"endIdx", endIdx,
		"count", endIdx-nextApplyIdx)

	// Fetch entries without holding the main lock.
	entries, err := r.fetchEntries(nextApplyIdx, endIdx)
	if err != nil {
		r.logger.Errorw("Apply loop failed to fetch entries",
			"error", err,
			"start", nextApplyIdx,
			"end", endIdx)
		return
	}

	if len(entries) == 0 {
		r.logger.Debugw("No entries to apply")
		return
	}

	r.logger.Infow("Fetched entries for application",
		"count", len(entries),
		"firstIndex", entries[0].Index,
		"lastIndex", entries[len(entries)-1].Index)

	// Apply entries to state machine (this can be slow).
	r.applyEntries(entries)

	// Finally, update the lastApplied index under the main lock.
	r.mu.Lock()
	r.stateMgr.UpdateLastApplied(entries[len(entries)-1].Index)
	r.mu.Unlock()

	r.logger.Infow("Applied entries successfully",
		"count", len(entries),
		"newLastApplied", entries[len(entries)-1].Index)
}

// fetchEntries retrieves the log entries for the specified range.
func (r *raftNode) fetchEntries(start, end types.Index) ([]types.LogEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.fetchEntriesTimeout)
	defer cancel()

	entries, err := r.logMgr.GetEntries(ctx, start, end)
	if err != nil {
		if errors.Is(err, ErrCompacted) {
			r.logger.Infow("Entries needed for apply are compacted; waiting for snapshot",
				"nextApplyIndex", start,
				"commitIndex", end-1)
		} else {
			r.logger.Errorw("Failed to get entries for applying",
				"start", start,
				"end", end,
				"error", err)
		}
		return nil, err
	}

	if len(entries) == 0 && end > start {
		r.logger.Errorw("Log manager returned no entries for valid range",
			"start", start,
			"end", end)
		return nil, errors.New("empty entry batch")
	}

	return entries, nil
}

// applyEntries applies each entry to the state machine and updates state.
func (r *raftNode) applyEntries(entries []types.LogEntry) {
	for _, entry := range entries {
		if r.isShutdown.Load() {
			r.logger.Infow("Aborting apply due to shutdown")
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), r.applyEntryTimeout)
		resultData, opErr := r.applier.Apply(ctx, entry.Index, entry.Command)
		cancel()

		r.sendApplyMsg(types.ApplyMsg{
			CommandValid:       true,
			Command:            entry.Command,
			CommandIndex:       entry.Index,
			CommandTerm:        entry.Term,
			CommandResultData:  resultData,
			CommandResultError: opErr,
		})
	}
}

// sendApplyMsg safely sends apply notifications to clients.
func (r *raftNode) sendApplyMsg(msg types.ApplyMsg) {
	// Add a shutdown check to prevent sending on a closed channel during teardown.
	if r.isShutdown.Load() {
		r.logger.Debugw("Discarded apply notification due to shutdown", "index", msg.CommandIndex)
		return
	}

	// Use a non-blocking send with timeout
	select {
	case r.applyCh <- msg:
		// Message sent successfully.
		r.logger.Debugw("Sent apply notification", "index", msg.CommandIndex)
	case <-r.stopCh:
		// Node is shutting down, just discard.
		r.logger.Debugw("Discarded apply notification due to shutdown", "index", msg.CommandIndex)
	case <-time.After(100 * time.Millisecond):
		// If the channel is full after 100ms, log it and move on
		r.logger.Warnw("Apply channel full, dropping notification after timeout", "index", msg.CommandIndex)
		r.metrics.ObserveApplyNotificationDropped()
	}
}

// handleLeaderChanges processes leader change events
func (r *raftNode) handleLeaderChanges() {
	r.logger.Infow("Leader change handler started")
	defer r.logger.Infow("Leader change handler stopped")

	for {
		select {
		case newLeader := <-r.leaderChangeCh:
			r.handleLeaderChange(newLeader)
		case <-r.stopCh:
			return
		}
	}
}

func (r *raftNode) handleLeaderChange(newLeader types.NodeID) {
	term, role, _ := r.stateMgr.GetState()

	r.logger.Infow("Leader change detected",
		"newLeader", newLeader,
		"currentRole", role.String(),
		"term", term)

	// Update cached leadership status
	isLeader := (role == types.RoleLeader && newLeader == r.id)
	r.isLeader.Store(isLeader)

	// Handle leadership transitions
	if isLeader {
		r.logger.Infow("Became leader, initializing leader state",
			"term", term, "nodeID", r.id)

		// Initialize leader state should already be called by election manager
		// But we can add additional coordination here if needed

		// Start sending heartbeats immediately
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			r.replicationMgr.SendHeartbeats(ctx)
		}()

		r.metrics.ObserveLeaderChange(newLeader, term)
	} else if role == types.RoleFollower && newLeader != unknownNodeID {
		r.logger.Infow("Following new leader",
			"leader", newLeader, "term", term)
		r.metrics.ObserveLeaderChange(newLeader, term)
	}
}

func (r *raftNode) TriggerApply() {
	if r.isShutdown.Load() {
		return
	}

	select {
	case r.forceApplyCh <- struct{}{}:
		r.logger.Debugw("Force apply notification sent")
	default:
		r.logger.Debugw("Force apply notification channel full")
	}
}
