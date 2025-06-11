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
	term, role, _ := r.stateMgr.GetState()

	r.logger.Debugw("Processing tick",
		"term", term,
		"role", role.String())

	internalCtx := context.Background()

	if role != types.RoleLeader {
		r.electionMgr.Tick(internalCtx)
	} else {
		r.replicationMgr.Tick(internalCtx)
	}
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

	if r.isShutdown.Load() {
		r.metrics.IncCounter("raft_propose_rejected", "reason", "shutdown")
		return 0, 0, false, ErrShuttingDown
	}

	if len(command) == 0 {
		r.metrics.IncCounter("raft_propose_rejected", "reason", "empty_command")
		return 0, 0, false, errors.New("command cannot be empty")
	}

	index, term, isLeader, err := r.replicationMgr.Propose(ctx, command)
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			r.logger.Infow("Proposal rejected: not leader",
				"commandSize", len(command),
				"leaderID", r.GetLeaderID())
			r.metrics.IncCounter("raft_propose_rejected", "reason", "not_leader")
		} else {
			r.logger.Warnw("Proposal failed",
				"commandSize", len(command),
				"error", err)
			r.metrics.IncCounter("raft_propose_failed", "reason", "error")
		}
		return 0, 0, isLeader, err
	}

	latency := r.clock.Since(startTime)
	r.logger.Infow("Command proposed successfully",
		"index", index,
		"term", term,
		"commandSize", len(command),
		"latency", latency)

	r.metrics.ObserveHistogram("raft_propose_latency_seconds", latency.Seconds())
	r.metrics.IncCounter("raft_propose_success_total")
	r.metrics.AddCounter("raft_propose_bytes_total", float64(len(command)))

	return index, term, true, nil
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
	r.mu.RLock()
	defer r.mu.RUnlock()

	term, role, leaderID := r.stateMgr.GetStateUnsafe()
	lastLogIndex := r.logMgr.GetLastIndexUnsafe()
	lastLogTerm := r.logMgr.GetLastTermUnsafe()
	commitIndex := r.stateMgr.GetCommitIndexUnsafe()
	lastApplied := r.stateMgr.GetLastAppliedUnsafe()
	snapMeta := r.snapshotMgr.GetSnapshotMetadataUnsafe()

	var replicationStatus map[types.NodeID]types.PeerState
	if role == types.RoleLeader {
		replicationStatus = r.replicationMgr.GetPeerReplicationStatusUnsafe()
	} else {
		replicationStatus = make(map[types.NodeID]types.PeerState)
	}

	r.metrics.IncCounter("raft_status_total")

	return types.RaftStatus{
		ID:            r.id,
		Role:          role,
		Term:          term,
		LeaderID:      leaderID,
		LastLogIndex:  lastLogIndex,
		LastLogTerm:   lastLogTerm,
		CommitIndex:   commitIndex,
		LastApplied:   lastApplied,
		SnapshotIndex: snapMeta.LastIncludedIndex,
		SnapshotTerm:  snapMeta.LastIncludedTerm,
		Replication:   replicationStatus,
	}
}

// GetState returns the current term and leadership status
func (r *raftNode) GetState() (types.Term, bool) {
	term, role, _ := r.stateMgr.GetState()
	isLeader := role == types.RoleLeader
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
	isHeartbeat := len(args.Entries) == 0

	if isHeartbeat {
		r.metrics.IncCounter("raft_heartbeat_received_total")
	} else {
		r.metrics.ObserveAppendEntriesRejected("log_inconsistency")
		r.metrics.AddCounter("raft_append_entries_entry_count", float64(len(args.Entries)))
	}

	if r.isShutdown.Load() {
		term, _, _ := r.stateMgr.GetState()
		r.metrics.IncCounter("raft_append_entries_rejected", "reason", "shutdown")
		return &types.AppendEntriesReply{Term: term, Success: false}, ErrShuttingDown
	}

	startTime := r.clock.Now()
	reply, err := r.replicationMgr.HandleAppendEntries(ctx, args)

	if err == nil {
		currentTerm, _, _ := r.stateMgr.GetState()
		if args.Term >= currentTerm {
			r.electionMgr.ResetTimerOnHeartbeat()
		}
	} else {
		r.metrics.IncCounter("raft_append_entries_errors_total")
	}

	latency := r.clock.Since(startTime)
	r.metrics.ObserveHistogram("raft_append_entries_latency_seconds", latency.Seconds())

	if reply != nil {
		if reply.Success {
			r.metrics.IncCounter("raft_append_entries_success_total")
		} else {
			r.metrics.ObserveAppendEntriesRejected("log_inconsistency")
		}
	}

	return reply, err
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

	startTime := r.clock.Now()
	reply, err := r.snapshotMgr.HandleInstallSnapshot(ctx, args)

	if err == nil {
		currentTerm, _, _ := r.stateMgr.GetState()
		if args.Term >= currentTerm {
			r.electionMgr.ResetTimerOnHeartbeat()
		}
	} else {
		r.metrics.IncCounter("raft_install_snapshot_errors_total")
	}

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

	for {
		select {
		case <-r.applyNotifyCh:
			r.applyCommittedEntries()
		case <-r.applyLoopStopCh:
			r.logger.Infow("Apply loop stopping")
			r.metrics.ObserveApplyLoopStopped("stop_signal")
			select {
			case <-r.applyNotifyCh:
				r.applyCommittedEntries()
			default:
			}
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
	batchStartTime := r.clock.Now()
	entriesApplied := 0
	bytesApplied := 0

	for !r.isShutdown.Load() {
		commitIdx, lastAppliedIdx := r.getCommitAndLastApplied()

		if lastAppliedIdx >= commitIdx {
			break
		}

		nextApplyIdx, endIdx := r.determineApplyRange(lastAppliedIdx, commitIdx)

		fetchStart := r.clock.Now()
		entries, err := r.fetchEntries(nextApplyIdx, endIdx)
		fetchLatency := r.clock.Since(fetchStart)
		r.metrics.ObserveHistogram("raft_apply_fetch_latency_seconds", fetchLatency.Seconds())

		if err != nil || len(entries) == 0 {
			if err != nil {
				r.metrics.IncCounter("raft_apply_fetch_errors_total")
			}
			break
		}

		applyStart := r.clock.Now()
		r.applyEntries(entries)
		applyLatency := r.clock.Since(applyStart)
		r.metrics.ObserveHistogram("raft_apply_batch_latency_seconds", applyLatency.Seconds())

		entriesApplied += len(entries)
		for _, entry := range entries {
			bytesApplied += len(entry.Command)
		}
	}

	if entriesApplied > 0 {
		batchLatency := r.clock.Since(batchStartTime)
		r.logger.Infow("Applied batch of entries",
			"count", entriesApplied,
			"bytes", bytesApplied,
			"latency", batchLatency)

		r.metrics.AddCounter("raft_entries_applied_total", float64(entriesApplied))
		r.metrics.AddCounter("raft_bytes_applied_total", float64(bytesApplied))
		r.metrics.ObserveHistogram("raft_apply_total_latency_seconds", batchLatency.Seconds())
	}
}

// getCommitAndLastApplied returns the current commit and last applied indices safely.
func (r *raftNode) getCommitAndLastApplied() (commitIdx, lastAppliedIdx types.Index) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stateMgr.GetCommitIndexUnsafe(), r.stateMgr.GetLastAppliedUnsafe()
}

// determineApplyRange calculates the next index and capped end index for batch processing.
func (r *raftNode) determineApplyRange(
	lastAppliedIdx, commitIdx types.Index,
) (types.Index, types.Index) {
	nextApplyIdx := lastAppliedIdx + 1

	batchLimit := types.Index(DefaultMaxApplyBatchSize)
	if batchLimit <= 0 {
		batchLimit = 10
	}

	endIdx := nextApplyIdx + batchLimit
	if endIdx > commitIdx+1 {
		endIdx = commitIdx + 1
	}

	return nextApplyIdx, endIdx
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

		applyStart := r.clock.Now()
		resultData, opErr := r.applier.Apply(ctx, entry.Index, entry.Command)
		applyLatency := r.clock.Since(applyStart)
		cancel()

		r.metrics.ObserveHistogram("raft_apply_entry_latency_seconds", applyLatency.Seconds())

		if opErr != nil {
			r.logger.Errorw("Failed to apply committed entry to state machine",
				"index", entry.Index,
				"term", entry.Term,
				"error", opErr) // specific error from the lockManager
			r.metrics.IncCounter("raft_apply_entry_errors_total")
		}

		r.stateMgr.UpdateLastApplied(entry.Index)
		r.sendApplyMsg(types.ApplyMsg{
			CommandValid:       true,
			Command:            entry.Command,
			CommandIndex:       entry.Index,
			CommandTerm:        entry.Term,
			CommandResultData:  resultData,
			CommandResultError: opErr,
		})

		if opErr == nil {
			r.metrics.IncCounter("raft_apply_entry_success_total")
		}
	}
}

// sendApplyMsg safely sends apply notifications to clients.
func (r *raftNode) sendApplyMsg(msg types.ApplyMsg) {
	select {
	case r.applyCh <- msg:
		r.logger.Debugw("Sent apply notification", "index", msg.CommandIndex)
	case <-r.stopCh:
		r.logger.Debugw("Discarded apply notification due to shutdown", "index", msg.CommandIndex)
	default:
		r.logger.Warnw("Apply channel full, dropping notification", "index", msg.CommandIndex)
		r.metrics.ObserveApplyNotificationDropped()
	}
}

// handleLeaderChanges processes leader change events
func (r *raftNode) handleLeaderChanges() {
	r.logger.Infow("Leader change handler started")
	r.metrics.IncCounter("raft_leader_change_handler_started_total")

	defer func() {
		r.logger.Infow("Leader change handler stopped")
		r.metrics.IncCounter("raft_leader_change_handler_stopped_total")
	}()

	for {
		select {
		case newLeader, ok := <-r.leaderChangeCh:
			if !ok {
				r.logger.Infow("Leader change channel closed")
				r.isLeader.Store(false)
				return
			}
			r.logger.Infow("Leader change detected", "newLeader", newLeader)

			isNowLeader := newLeader == r.id
			r.isLeader.Store(isNowLeader)

			if isNowLeader {
				r.metrics.IncCounter("raft_became_leader_total")
			} else if newLeader != "" {
				r.metrics.IncCounter("raft_leader_changed_total", "new_leader", string(newLeader))
			} else {
				r.metrics.IncCounter("raft_leader_unknown_total")
			}

		case <-r.stopCh:
			r.isLeader.Store(false)
			return
		}
	}
}
