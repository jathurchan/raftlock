package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

// PeerStateUpdater defines an abstraction used by SnapshotManager to update
// the replication state of individual peers following snapshot operations.
// This decouples snapshot logic from the concrete replication manager implementation.
type PeerStateUpdater interface {
	// UpdatePeerAfterSnapshotSend updates the replication progress for the given peer
	// after a snapshot has been successfully sent. Typically, this sets the peer's
	// nextIndex and matchIndex to reflect the snapshot's last included index.
	UpdatePeerAfterSnapshotSend(peerID types.NodeID, snapshotIndex types.Index)

	// SetPeerSnapshotInProgress sets the snapshot-in-progress flag for the specified peer.
	// This helps prevent concurrent snapshot operations targeting the same follower.
	SetPeerSnapshotInProgress(peerID types.NodeID, inProgress bool)
}

// SnapshotManager defines the interface for managing Raft snapshot operations.
// It is responsible for snapshot creation, restoration, persistence, log truncation,
// and transferring snapshots to other Raft peers.
type SnapshotManager interface {
	// Initialize loads the latest snapshot from persistent storage.
	// If the snapshot is newer than the current applied state, it restores the
	// state machine from the snapshot. It also truncates the log prefix up to the
	// snapshot index if applicable.
	Initialize(ctx context.Context) error

	// Tick performs periodic snapshot-related maintenance.
	// Typically called on each Raft tick, it checks whether the snapshot threshold
	// has been reached and initiates snapshot creation if needed.
	Tick(ctx context.Context)

	// HandleInstallSnapshot processes an incoming InstallSnapshot RPC from a leader.
	// It persists the received snapshot, restores the state machine, updates the
	// local Raft state, and truncates the log to align with the snapshot.
	HandleInstallSnapshot(ctx context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error)

	// SendSnapshot sends the current snapshot to the specified follower node.
	// This is invoked when the follower is too far behind in the log to be caught up
	// through normal log replication. The method handles loading, sending, and
	// processing the response to the InstallSnapshot RPC.
	SendSnapshot(ctx context.Context, targetID types.NodeID, term types.Term)

	// GetSnapshotMetadata returns the metadata (last included index and term)
	// of the most recent snapshot that has been successfully saved or restored.
	GetSnapshotMetadata() types.SnapshotMetadata

	// Stop performs any necessary cleanup or shutdown logic for the snapshot manager.
	Stop()
}

// SnapshotManagerDeps defines the dependencies required to instantiate a SnapshotManager.
type SnapshotManagerDeps struct {
	Mu                *sync.RWMutex
	ID                types.NodeID
	Config            Config
	Storage           storage.Storage
	Applier           Applier
	PeerStateUpdater  PeerStateUpdater
	StateMgr          StateManager
	LogMgr            LogManager
	NetworkMgr        NetworkManager
	Metrics           Metrics
	Logger            logger.Logger
	Clock             Clock
	IsShutdown        *atomic.Bool
	NotifyCommitCheck func() // Callback to trigger commit check
}

// validateSnapshotManagerDeps checks that all required dependencies are non-nil or valid.
// Returns an error describing the first missing dependency.
func validateSnapshotManagerDeps(deps SnapshotManagerDeps) error {
	switch {
	case deps.Mu == nil:
		return fmt.Errorf("SnapshotManagerDeps: Mu is required")
	case deps.ID == "":
		return fmt.Errorf("SnapshotManagerDeps: ID is required")
	case deps.Storage == nil:
		return fmt.Errorf("SnapshotManagerDeps: Storage is required")
	case deps.Applier == nil:
		return fmt.Errorf("SnapshotManagerDeps: Applier is required")
	case deps.LogMgr == nil:
		return fmt.Errorf("SnapshotManagerDeps: LogMgr is required")
	case deps.NetworkMgr == nil:
		return fmt.Errorf("SnapshotManagerDeps: NetworkMgr is required")
	case deps.StateMgr == nil:
		return fmt.Errorf("SnapshotManagerDeps: StateMgr is required")
	case deps.PeerStateUpdater == nil:
		return fmt.Errorf("SnapshotManagerDeps: PeerStateUpdater is required")
	case deps.Metrics == nil:
		return fmt.Errorf("SnapshotManagerDeps: Metrics is required")
	case deps.Logger == nil:
		return fmt.Errorf("SnapshotManagerDeps: Logger is required")
	case deps.Clock == nil:
		return fmt.Errorf("SnapshotManagerDeps: Clock is required")
	case deps.IsShutdown == nil:
		return fmt.Errorf("SnapshotManagerDeps: IsShutdown flag is required")
	case deps.NotifyCommitCheck == nil:
		return fmt.Errorf("SnapshotManagerDeps: NotifyCommitCheck callback is required")
	default:
		return nil
	}
}

// snapshotManager is a concrete implementation of the SnapshotManager interface.
type snapshotManager struct {
	mu            *sync.RWMutex  // Raft's mutex protecting state fields
	isShutdown    *atomic.Bool   // Shared flag indicating Raft shutdown
	snapshotOpsWg sync.WaitGroup // Wait group for in-flight snapshot operations

	id  types.NodeID // ID of the local Raft node.
	cfg Config

	storage          storage.Storage
	applier          Applier
	peerStateUpdater PeerStateUpdater
	stateMgr         StateManager
	logMgr           LogManager
	networkMgr       NetworkManager
	metrics          Metrics
	logger           logger.Logger
	clock            Clock

	notifyCommitCheck func()

	lastSnapshotIndex types.Index // Last snapshot index (protected by mu)
	lastSnapshotTerm  types.Term  // Last snapshot term (protected by mu)

	snapshotCreateInProgress atomic.Bool  // Prevents concurrent local snapshot creation
	snapshotOpsCounter       atomic.Int32 // Counts all active snapshot ops (create, send, handle) for graceful shutdown
}

// NewSnapshotManager initializes and returns a new SnapshotManager instance.
func NewSnapshotManager(deps SnapshotManagerDeps) (SnapshotManager, error) {
	if err := validateSnapshotManagerDeps(deps); err != nil {
		return nil, fmt.Errorf("invalid snapshot manager dependencies: %w", err)
	}

	sm := &snapshotManager{
		mu:                deps.Mu,
		id:                deps.ID,
		cfg:               deps.Config,
		storage:           deps.Storage,
		applier:           deps.Applier,
		logMgr:            deps.LogMgr,
		stateMgr:          deps.StateMgr,
		peerStateUpdater:  deps.PeerStateUpdater,
		metrics:           deps.Metrics,
		logger:            deps.Logger.WithComponent("snapshot"),
		networkMgr:        deps.NetworkMgr,
		clock:             deps.Clock,
		isShutdown:        deps.IsShutdown,
		notifyCommitCheck: deps.NotifyCommitCheck,

		lastSnapshotIndex: 0,
		lastSnapshotTerm:  0,
	}

	sm.logger.Infow("Snapshot manager initialized",
		"snapshotThreshold", sm.cfg.Options.SnapshotThreshold,
		"logCompactionMinEntries", sm.cfg.Options.LogCompactionMinEntries)

	return sm, nil
}

// Initialize loads the most recent snapshot, restores state if necessary, and truncates the log.
// Called once during Raft node startup.
func (sm *snapshotManager) Initialize(ctx context.Context) error {
	if sm.isShutdownOrContextDone(ctx, "Initialize") {
		return ErrShuttingDown
	}
	sm.logger.Infow("Starting snapshot manager initialization")

	meta, data, err := sm.loadSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to load snapshot during initialization: %w", err)
	}

	sm.updateSnapshotMetadata(meta)

	if err := sm.maybeRestoreState(ctx, meta, data); err != nil {
		return fmt.Errorf("failed to restore state during initialization: %w", err)
	}

	sm.maybeTruncateLog(ctx, meta.LastIncludedIndex)

	sm.logger.Infow("Snapshot manager initialization complete",
		"lastSnapshotIndex", meta.LastIncludedIndex,
		"lastSnapshotTerm", meta.LastIncludedTerm,
	)
	return nil
}

// loadSnapshot attempts to load the latest snapshot from persistent storage.
// Handles ErrNoSnapshot and ErrCorruptedSnapshot gracefully.
func (sm *snapshotManager) loadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	meta, data, err := sm.storage.LoadSnapshot(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrNoSnapshot) || errors.Is(err, storage.ErrCorruptedSnapshot) {
			sm.logger.Infow("No valid snapshot found in storage; initializing with zero state", "error", err)
			return types.SnapshotMetadata{}, nil, nil
		}

		sm.logger.Errorw("Failed to load snapshot from storage", "error", err)
		return types.SnapshotMetadata{}, nil, fmt.Errorf("storage.LoadSnapshot failed: %w", err)
	}

	sm.logger.Infow("Loaded snapshot metadata from storage",
		"index", meta.LastIncludedIndex,
		"term", meta.LastIncludedTerm,
		"dataSize", len(data),
	)

	return meta, data, nil
}

// maybeRestoreState restores the state machine if the loaded snapshot is newer than the last applied index.
// Ensures commit index is at least the snapshot index otherwise.
func (sm *snapshotManager) maybeRestoreState(ctx context.Context, meta types.SnapshotMetadata, data []byte) error {
	lastApplied := sm.stateMgr.GetLastApplied()

	if meta.LastIncludedIndex > lastApplied {
		sm.logger.Infow("Snapshot is newer than applied state; restoring state machine",
			"snapshotIndex", meta.LastIncludedIndex,
			"snapshotTerm", meta.LastIncludedTerm,
			"lastApplied", lastApplied,
		)

		if err := sm.restoreSnapshot(ctx, meta, data); err != nil {
			return fmt.Errorf("restoreSnapshot failed: %w", err)
		}
		sm.logger.Infow("State machine restored successfully from snapshot")
	} else {
		sm.logger.Infow("Snapshot is not ahead of applied state; skipping restore, ensuring commit index",
			"snapshotIndex", meta.LastIncludedIndex,
			"lastApplied", lastApplied,
			"currentCommitIndex", sm.stateMgr.GetCommitIndex(),
		)

		updated := sm.stateMgr.UpdateCommitIndex(meta.LastIncludedIndex)
		if updated {
			sm.logger.Infow("Updated commit index to match snapshot index", "commitIndex", meta.LastIncludedIndex)
		}
	}
	return nil
}

// maybeTruncateLog attempts to truncate the log prefix up to the snapshot index + 1.
// This is best-effort; failure is logged but not fatal.
func (sm *snapshotManager) maybeTruncateLog(ctx context.Context, snapshotIndex types.Index) {
	if snapshotIndex == 0 {
		return
	}

	truncateBeforeIndex := snapshotIndex + 1
	sm.logger.Infow("Attempting log truncation based on loaded snapshot",
		"snapshotIndex", snapshotIndex,
		"truncateBeforeIndex", truncateBeforeIndex,
	)

	truncateCtx, cancel := context.WithTimeout(ctx, defaultSnapshotLogTruncateTimeout)
	defer cancel()

	if err := sm.logMgr.TruncatePrefix(truncateCtx, truncateBeforeIndex); err != nil {
		sm.logger.Warnw("Log truncation failed (best effort)", "error", err, "truncateBeforeIndex", truncateBeforeIndex)
		return
	}

	sm.logger.Infow("Log truncated successfully based on loaded snapshot", "truncatedBeforeIndex", truncateBeforeIndex)
}

// Tick checks if a new snapshot should be created based on the configured threshold.
// Called periodically by the main Raft loop.
func (sm *snapshotManager) Tick(ctx context.Context) {
	if sm.isShutdownOrContextDone(ctx, "Tick") {
		return
	}

	threshold := sm.cfg.Options.SnapshotThreshold
	if threshold <= 0 {
		return
	}

	if sm.snapshotCreateInProgress.Load() {
		sm.logger.Debugw("Snapshot creation already in progress; skipping check")
		return
	}

	sm.mu.RLock()
	lastSnapshotIdx := sm.lastSnapshotIndex
	lastSnapshotTerm := sm.lastSnapshotTerm
	sm.mu.RUnlock()

	_, role, _ := sm.stateMgr.GetState()

	lastApplied := sm.stateMgr.GetLastApplied()
	if lastApplied <= lastSnapshotIdx || (lastApplied-lastSnapshotIdx) < types.Index(threshold) {
		return
	}

	sm.logger.Infow("Snapshot threshold reached, initiating background snapshot creation",
		"lastApplied", lastApplied,
		"lastSnapshotIndex", lastSnapshotIdx,
		"lastSnapshotTerm", lastSnapshotTerm,
		"entriesSinceSnapshot", lastApplied-lastSnapshotIdx,
		"threshold", threshold,
		"role", role,
	)

	sm.snapshotOpsWg.Add(1)
	sm.snapshotOpsCounter.Add(1)

	go func() {
		defer sm.snapshotOpsWg.Done()
		defer sm.snapshotOpsCounter.Add(-1)

		sm.createSnapshot(context.Background())
	}()
}

// createSnapshot orchestrates the process of creating a new snapshot.
// This runs in a background goroutine.
func (sm *snapshotManager) createSnapshot(ctx context.Context) {
	if !sm.snapshotCreateInProgress.CompareAndSwap(false, true) {
		sm.logger.Debugw("Snapshot creation skipped: another creation process started concurrently")
		sm.metrics.ObserveSnapshot(SnapshotActionCapture, SnapshotStatusFailure,
			"reason", string(SnapshotReasonInProgress))
		return
	}
	defer sm.snapshotCreateInProgress.Store(false)

	if sm.isShutdownOrContextDone(ctx, "createSnapshot") {
		sm.logger.Infow("Snapshot creation aborted: shutdown signal received")
		sm.metrics.ObserveSnapshot(SnapshotActionCapture, SnapshotStatusFailure,
			"reason", string(SnapshotReasonShutdown))
		return
	}

	sm.logger.Infow("Beginning snapshot creation process")
	start := sm.clock.Now()

	metadata, data, err := sm.captureSnapshot(ctx)
	if err != nil {
		return
	}
	if metadata.LastIncludedIndex == 0 && len(data) == 0 {
		sm.logger.Infow("Applier returned empty snapshot; skipping persistence and finalization")
		sm.metrics.ObserveSnapshot(SnapshotActionCapture, SnapshotStatusFailure,
			"reason", string(SnapshotReasonEmptySnapshot))
		return
	}

	if err := sm.persistSnapshot(ctx, metadata, data); err != nil {
		return
	}

	sm.finalizeSnapshot(metadata, data, start)
	sm.maybeTriggerLogCompaction(ctx, metadata)
	sm.logger.Infow("Snapshot creation process completed", "index", metadata.LastIncludedIndex, "term", metadata.LastIncludedTerm)
}

// captureSnapshot gets the current state from the Applier and necessary metadata.
func (sm *snapshotManager) captureSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	snapCtx, cancel := context.WithTimeout(ctx, defaultSnapshotCaptureTimeout)
	defer cancel()
	lastAppliedIdx, data, err := sm.applier.Snapshot(snapCtx)
	if err != nil {
		sm.logger.Errorw("Failed to capture snapshot data from applier", "error", err)
		sm.metrics.ObserveSnapshot(SnapshotActionCapture, SnapshotStatusFailure,
			"reason", string(SnapshotReasonApplierError))
		return types.SnapshotMetadata{}, nil, fmt.Errorf("applier.Snapshot failed: %w", err)
	}

	if lastAppliedIdx == 0 {
		if len(data) == 0 {
			sm.logger.Infow("Applier returned zero index and empty data, interpreting as empty snapshot")
			return types.SnapshotMetadata{}, nil, nil
		}
		sm.logger.Warnw("Applier returned snapshot with index 0 but non-empty data", "dataSize", len(data))
	}

	termCtx, cancelTerm := context.WithTimeout(ctx, defaultSnapshotLogTermTimeout)
	defer cancelTerm()
	lastAppliedTerm, err := sm.logMgr.GetTerm(termCtx, lastAppliedIdx)
	if err != nil {
		sm.logger.Errorw("Failed to resolve term for snapshot index",
			"index", lastAppliedIdx,
			"error", err,
		)
		sm.metrics.ObserveSnapshot(SnapshotActionCapture, SnapshotStatusFailure,
			"reason", string(SnapshotReasonLogTermError))
		return types.SnapshotMetadata{}, nil, fmt.Errorf("logMgr.GetTerm failed for index %d: %w", lastAppliedIdx, err)
	}

	meta := types.SnapshotMetadata{
		LastIncludedIndex: lastAppliedIdx,
		LastIncludedTerm:  lastAppliedTerm,
	}
	sm.logger.Debugw("Snapshot captured successfully", "index", meta.LastIncludedIndex, "term", meta.LastIncludedTerm, "size", len(data))
	return meta, data, nil
}

// persistSnapshot writes the captured snapshot data and metadata to stable storage.
func (sm *snapshotManager) persistSnapshot(ctx context.Context, meta types.SnapshotMetadata, data []byte) error {
	saveCtx, cancel := context.WithTimeout(ctx, defaultSnapshotPersistTimeout)
	defer cancel()

	sm.logger.Debugw("Persisting snapshot to storage", "index", meta.LastIncludedIndex, "term", meta.LastIncludedTerm, "size", len(data))

	if err := sm.storage.SaveSnapshot(saveCtx, meta, data); err != nil {
		sm.logger.Errorw("Failed to persist snapshot to storage", "error", err, "index", meta.LastIncludedIndex)
		sm.metrics.ObserveSnapshot(SnapshotActionPersist, SnapshotStatusFailure,
			"reason", string(SnapshotReasonStorageError))
		return fmt.Errorf("storage.SaveSnapshot failed: %w", err)
	}

	sm.logger.Infow("Snapshot persisted successfully", "index", meta.LastIncludedIndex, "term", meta.LastIncludedTerm)
	sm.metrics.ObserveSnapshot(SnapshotActionPersist, SnapshotStatusSuccess)
	return nil
}

// finalizeSnapshot updates the snapshot manager's internal metadata after successful persistence.
func (sm *snapshotManager) finalizeSnapshot(meta types.SnapshotMetadata, data []byte, start time.Time) {
	sm.mu.Lock()
	prevIndex := sm.lastSnapshotIndex
	prevTerm := sm.lastSnapshotTerm
	sm.lastSnapshotIndex = meta.LastIncludedIndex
	sm.lastSnapshotTerm = meta.LastIncludedTerm
	sm.mu.Unlock()

	duration := sm.clock.Now().Sub(start)
	sm.logger.Infow("Snapshot creation finalized, internal state updated",
		"index", meta.LastIncludedIndex,
		"term", meta.LastIncludedTerm,
		"sizeBytes", len(data),
		"previousIndex", prevIndex,
		"previousTerm", prevTerm,
		"duration", duration,
	)

	sm.metrics.ObserveSnapshot(SnapshotActionFinalize, SnapshotStatusSuccess,
		"sizeBytes", fmt.Sprintf("%d", len(data)))
}

// maybeTriggerLogCompaction potentially truncates the log prefix after a snapshot.
// This runs after the snapshot is successfully created and finalized.
func (sm *snapshotManager) maybeTriggerLogCompaction(ctx context.Context, snapshotMeta types.SnapshotMetadata) {
	minEntries := sm.cfg.Options.LogCompactionMinEntries
	if minEntries <= 0 {
		sm.logger.Debugw("Log compaction skipped: disabled (LogCompactionMinEntries <= 0)")
		return
	}

	snapshotIndex := snapshotMeta.LastIncludedIndex
	sm.mu.RLock()
	lastLogIndex := sm.logMgr.GetLastIndexUnsafe()
	sm.mu.RUnlock()

	var entriesAfterSnapshot uint64
	if lastLogIndex >= snapshotIndex {
		entriesAfterSnapshot = uint64(lastLogIndex - snapshotIndex)
	}

	if entriesAfterSnapshot < uint64(minEntries) {
		sm.logger.Debugw("Log compaction skipped: not enough entries after snapshot",
			"snapshotIndex", snapshotIndex,
			"lastLogIndex", lastLogIndex,
			"entriesAfterSnapshot", entriesAfterSnapshot,
			"minRequired", minEntries,
		)
		return
	}

	sm.logger.Infow("Threshold met, triggering log compaction after snapshot",
		"snapshotIndex", snapshotIndex,
		"lastLogIndex", lastLogIndex,
		"entriesAfterSnapshot", entriesAfterSnapshot,
		"minRequired", minEntries,
	)

	truncateBeforeIndex := snapshotIndex + 1
	truncateCtx, cancel := context.WithTimeout(ctx, defaultSnapshotLogTruncateTimeout)
	defer cancel()

	if err := sm.logMgr.TruncatePrefix(truncateCtx, truncateBeforeIndex); err != nil {
		sm.logger.Warnw("Log compaction failed after snapshot (best effort)",
			"truncateBeforeIndex", truncateBeforeIndex,
			"error", err,
		)
	} else {
		sm.logger.Infow("Log successfully compacted after snapshot",
			"truncatedBeforeIndex", truncateBeforeIndex,
		)
	}
}

// GetSnapshotMetadata returns the metadata of the most recently completed snapshot.
// Safe for concurrent access.
func (sm *snapshotManager) GetSnapshotMetadata() types.SnapshotMetadata {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return types.SnapshotMetadata{
		LastIncludedIndex: sm.lastSnapshotIndex,
		LastIncludedTerm:  sm.lastSnapshotTerm,
	}
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC from a leader.
// This runs in the RPC handler goroutine.
func (sm *snapshotManager) HandleInstallSnapshot(ctx context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) { //
	if sm.isShutdownOrContextDone(ctx, "HandleInstallSnapshot") {
		return nil, ErrShuttingDown
	}

	sm.snapshotOpsCounter.Add(1)
	sm.snapshotOpsWg.Add(1)
	defer sm.snapshotOpsCounter.Add(-1)
	defer sm.snapshotOpsWg.Done()

	start := sm.clock.Now()
	sm.logger.Infow("Received InstallSnapshot RPC",
		"from", args.LeaderID,
		"term", args.Term,
		"snapshotIndex", args.LastIncludedIndex,
		"snapshotTerm", args.LastIncludedTerm,
	)

	reply, err := sm.processInstallSnapshot(ctx, args)
	if err != nil {
		sm.logger.Warnw("Failed to process InstallSnapshot RPC", "error", err, "from", args.LeaderID)
		return nil, err
	}

	sm.logger.Infow("InstallSnapshot RPC processed successfully",
		"from", args.LeaderID,
		"term", args.Term,
		"processedIndex", args.LastIncludedIndex,
		"duration", sm.clock.Now().Sub(start),
	)
	sm.metrics.ObserveSnapshot(SnapshotActionReceive, SnapshotStatusSuccess)
	return reply, nil
}

// processInstallSnapshot contains the core logic for handling an InstallSnapshot RPC.
func (sm *snapshotManager) processInstallSnapshot(ctx context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) { //
	steppedDown, _ := sm.stateMgr.CheckTermAndStepDown(ctx, args.Term, args.LeaderID)
	currentTerm, currentRole, _ := sm.stateMgr.GetState()
	reply := &types.InstallSnapshotReply{Term: currentTerm}

	if args.Term < currentTerm {
		sm.logger.Warnw("Rejecting InstallSnapshot: Stale RPC term",
			"rpcTerm", args.Term, "currentTerm", currentTerm, "from", args.LeaderID)
		sm.metrics.ObserveSnapshot(SnapshotActionReceive, SnapshotStatusFailure, "reason", string(SnapshotReasonStaleTerm))
		return reply, nil
	}

	if steppedDown || currentRole != types.RoleFollower {
		sm.logger.Infow("Becoming follower due to InstallSnapshot RPC", "term", currentTerm, "leader", args.LeaderID, "previousRole", currentRole)
		sm.stateMgr.BecomeFollower(ctx, currentTerm, args.LeaderID)
		currentTerm, _, _ = sm.stateMgr.GetState()
		reply.Term = currentTerm
	}

	incomingMeta := types.SnapshotMetadata{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
	}
	if sm.isStaleSnapshot(incomingMeta) {
		sm.logger.Infow("Ignoring stale InstallSnapshot RPC: index/term is not newer than current snapshot",
			"receivedIndex", incomingMeta.LastIncludedIndex,
			"receivedTerm", incomingMeta.LastIncludedTerm,
			"currentIndex", sm.lastSnapshotIndex,
			"currentTerm", sm.lastSnapshotTerm,
			"from", args.LeaderID,
		)
		return reply, nil
	}

	if err := sm.saveSnapshotToStorage(ctx, incomingMeta, args.Data); err != nil {
		return nil, fmt.Errorf("failed to save incoming snapshot to storage: %w", err)
	}

	if err := sm.restoreSnapshotToStateMachine(ctx, incomingMeta, args.Data); err != nil {
		return nil, fmt.Errorf("failed to restore state machine from incoming snapshot: %w", err)
	}

	sm.updateSnapshotMetadata(incomingMeta)
	sm.truncateLogAfterSnapshot(ctx, incomingMeta.LastIncludedIndex)

	return reply, nil
}

// isStaleSnapshot checks if the incoming snapshot metadata is older than or equal to
// the currently held snapshot metadata. Requires RLock on sm.mu.
func (sm *snapshotManager) isStaleSnapshot(incoming types.SnapshotMetadata) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if incoming.LastIncludedIndex < sm.lastSnapshotIndex {
		return true
	}
	if incoming.LastIncludedIndex == sm.lastSnapshotIndex && incoming.LastIncludedTerm <= sm.lastSnapshotTerm {
		return true
	}
	return false
}

// saveSnapshotToStorage persists the received snapshot.
func (sm *snapshotManager) saveSnapshotToStorage(ctx context.Context, meta types.SnapshotMetadata, data []byte) error {
	saveCtx, cancel := context.WithTimeout(ctx, defaultSnapshotPersistTimeout)
	defer cancel()

	sm.logger.Debugw("Saving received snapshot to local storage", "index", meta.LastIncludedIndex, "term", meta.LastIncludedTerm, "size", len(data))

	if err := sm.storage.SaveSnapshot(saveCtx, meta, data); err != nil {
		sm.logger.Errorw("Failed to persist received snapshot", "error", err, "index", meta.LastIncludedIndex)
		sm.metrics.ObserveSnapshot(SnapshotActionReceive, SnapshotStatusFailure, "reason", string(SnapshotReasonStorageError))
		return fmt.Errorf("snapshot save failed: %w", err)
	}

	sm.logger.Infow("Received snapshot saved successfully", "index", meta.LastIncludedIndex, "term", meta.LastIncludedTerm)
	return nil
}

// restoreSnapshotToStateMachine applies the snapshot to the state machine via the Applier.
func (sm *snapshotManager) restoreSnapshotToStateMachine(ctx context.Context, meta types.SnapshotMetadata, data []byte) error {
	restoreCtx, cancel := context.WithTimeout(ctx, defaultSnapshotRestoreTimeout)
	defer cancel()

	sm.logger.Debugw("Restoring state machine from received snapshot", "index", meta.LastIncludedIndex, "term", meta.LastIncludedTerm)

	if err := sm.restoreSnapshot(restoreCtx, meta, data); err != nil {
		sm.metrics.ObserveSnapshot(SnapshotActionReceive, SnapshotStatusFailure, "reason", string(SnapshotReasonRestoreError))
		return fmt.Errorf("snapshot restore failed: %w", err)
	}

	sm.logger.Infow("State machine restored successfully from received snapshot", "index", meta.LastIncludedIndex, "term", meta.LastIncludedTerm)
	return nil
}

// truncateLogAfterSnapshot attempts log truncation after successfully installing a snapshot. Best effort.
func (sm *snapshotManager) truncateLogAfterSnapshot(ctx context.Context, snapshotIndex types.Index) {
	if snapshotIndex == 0 {
		return
	}
	truncateBeforeIndex := snapshotIndex + 1
	sm.logger.Debugw("Attempting log truncation after installing snapshot", "snapshotIndex", snapshotIndex, "truncateBeforeIndex", truncateBeforeIndex)

	truncateCtx, cancel := context.WithTimeout(ctx, defaultSnapshotLogTruncateTimeout)
	defer cancel()

	if err := sm.logMgr.TruncatePrefix(truncateCtx, truncateBeforeIndex); err != nil {
		sm.logger.Warnw("Log truncation failed after snapshot installation (best effort)", "error", err, "truncateBeforeIndex", truncateBeforeIndex)
	} else {
		sm.logger.Infow("Log truncated successfully after snapshot installation", "truncatedBeforeIndex", truncateBeforeIndex)
	}
}

// SendSnapshot sends the current snapshot to a follower that is lagging too far behind.
// This runs in a leader's goroutine dedicated to a specific follower.
func (sm *snapshotManager) SendSnapshot(ctx context.Context, targetID types.NodeID, term types.Term) {
	if sm.isShutdownOrContextDone(ctx, "SendSnapshot") {
		sm.logger.Debugw("SendSnapshot aborted: shutdown or context done", "peer", targetID)
		return
	}

	sm.snapshotOpsCounter.Add(1)
	sm.snapshotOpsWg.Add(1)
	defer sm.snapshotOpsCounter.Add(-1)
	defer sm.snapshotOpsWg.Done()

	sm.peerStateUpdater.SetPeerSnapshotInProgress(targetID, true)
	defer sm.peerStateUpdater.SetPeerSnapshotInProgress(targetID, false)

	start := sm.clock.Now()
	sm.logger.Infow("Starting snapshot send process",
		"peer", targetID,
		"term", term,
	)

	meta, data, err := sm.loadSnapshotForSending(ctx, targetID)
	if err != nil {
		sm.logger.Errorw("Aborting snapshot send: failed to load snapshot", "peer", targetID, "error", err)
		return
	}
	if meta.LastIncludedIndex == 0 {
		sm.logger.Warnw("Aborting snapshot send: loaded snapshot is empty", "peer", targetID)
		sm.metrics.ObserveSnapshot(SnapshotActionSend, SnapshotStatusFailure, "peer", string(targetID), "reason", "empty_snapshot_loaded")
		return
	}

	if !sm.validateLeadershipBeforeSend(targetID, term) {
		return
	}

	args := &types.InstallSnapshotArgs{
		Term:              term,
		LeaderID:          sm.id,
		LastIncludedIndex: meta.LastIncludedIndex,
		LastIncludedTerm:  meta.LastIncludedTerm,
		Data:              data,
	}

	reply, err := sm.sendInstallSnapshotRPC(ctx, targetID, args)
	if err != nil {
		sm.logger.Errorw("Aborting snapshot send: RPC failed", "peer", targetID, "error", err)
		return
	}

	sm.processSnapshotReply(ctx, targetID, term, meta, reply, start)

	sm.logger.Infow("Snapshot send process finished", "peer", targetID, "snapshotIndex", meta.LastIncludedIndex)
}

// loadSnapshotForSending loads snapshot data from storage for sending to a peer.
func (sm *snapshotManager) loadSnapshotForSending(ctx context.Context, targetID types.NodeID) (types.SnapshotMetadata, []byte, error) {
	loadCtx, cancel := context.WithTimeout(ctx, defaultSnapshotLoadTimeout)
	defer cancel()

	sm.logger.Debugw("Loading snapshot from storage for sending", "peer", targetID)

	meta, data, err := sm.storage.LoadSnapshot(loadCtx)
	if err != nil {
		sm.logger.Errorw("Failed to load snapshot for sending", "peer", targetID, "error", err)
		sm.metrics.ObserveSnapshot(SnapshotActionSend, SnapshotStatusFailure,
			"peer", string(targetID), "reason", "storage_load_error")
		return types.SnapshotMetadata{}, nil, fmt.Errorf("storage.LoadSnapshot failed: %w", err)
	}

	sm.logger.Debugw("Snapshot loaded successfully for sending", "peer", targetID, "index", meta.LastIncludedIndex, "term", meta.LastIncludedTerm, "size", len(data))
	return meta, data, nil
}

// validateLeadershipBeforeSend checks if the node is still the leader in the expected term.
func (sm *snapshotManager) validateLeadershipBeforeSend(targetID types.NodeID, expectedTerm types.Term) bool {
	currentTerm, role, _ := sm.stateMgr.GetState()

	if role != types.RoleLeader || currentTerm != expectedTerm {
		sm.logger.Infow("Aborting snapshot send: leadership lost or term changed before RPC",
			"peer", targetID,
			"expectedTerm", expectedTerm,
			"currentTerm", currentTerm,
			"currentRole", role.String(),
		)
		sm.metrics.ObserveSnapshot(SnapshotActionSend, SnapshotStatusFailure,
			"peer", string(targetID), "reason", "not_leader_or_term_changed")
		return false
	}

	sm.logger.Debugw("Leadership validated before sending snapshot", "peer", targetID, "term", currentTerm)
	return true
}

// sendInstallSnapshotRPC performs the network call to send the snapshot.
func (sm *snapshotManager) sendInstallSnapshotRPC(ctx context.Context, peer types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
	sendCtx, cancel := context.WithTimeout(ctx, defaultSnapshotSendRPCTimeout)
	defer cancel()

	sm.logger.Debugw("Sending InstallSnapshot RPC",
		"peer", peer,
		"term", args.Term,
		"index", args.LastIncludedIndex,
		"term", args.LastIncludedTerm,
	)

	reply, err := sm.networkMgr.SendInstallSnapshot(sendCtx, peer, args)
	if err != nil {
		level := sm.logger.Warnw
		reason := "rpc_error"
		if errors.Is(err, context.DeadlineExceeded) {
			reason = "rpc_timeout"
		} else if errors.Is(err, context.Canceled) {
			level = sm.logger.Debugw // Context cancellation might be expected on shutdown
			reason = "rpc_canceled"
		}
		level("InstallSnapshot RPC failed",
			"peer", peer,
			"error", err,
			"index", args.LastIncludedIndex,
			"reason", reason,
		)
		sm.metrics.ObserveSnapshot(SnapshotActionSend, SnapshotStatusFailure,
			"peer", string(peer), "reason", reason)
		return nil, fmt.Errorf("SendInstallSnapshot RPC failed: %w", err)
	}

	sm.logger.Debugw("InstallSnapshot RPC successful", "peer", peer, "replyTerm", reply.Term)
	return reply, nil
}

// processSnapshotReply handles the response from a follower after sending a snapshot.
func (sm *snapshotManager) processSnapshotReply(
	ctx context.Context,
	peer types.NodeID,
	sentTerm types.Term,
	meta types.SnapshotMetadata,
	reply *types.InstallSnapshotReply,
	start time.Time,
) {
	steppedDown, _ := sm.stateMgr.CheckTermAndStepDown(context.Background(), reply.Term, peer)
	if steppedDown {
		sm.logger.Infow("Stepped down after sending snapshot: follower responded with higher term",
			"peer", peer,
			"replyTerm", reply.Term,
			"localTerm", sentTerm,
		)
		sm.metrics.ObserveSnapshot(SnapshotActionSend, SnapshotStatusFailure,
			"peer", string(peer), "reason", "higher_term_reply")
		return
	}

	sm.logger.Infow("Snapshot successfully acknowledged by follower",
		"peer", peer,
		"index", meta.LastIncludedIndex,
		"term", meta.LastIncludedTerm,
		"duration", sm.clock.Now().Sub(start),
	)

	sm.peerStateUpdater.UpdatePeerAfterSnapshotSend(peer, meta.LastIncludedIndex)

	sm.notifyCommitCheck()

	sm.metrics.ObserveSnapshot(SnapshotActionSend, SnapshotStatusSuccess,
		"peer", string(peer))
}

// Stop gracefully waits for ongoing snapshot operations to complete.
func (sm *snapshotManager) Stop() {
	sm.logger.Infow("Stopping snapshot manager...")

	activeOps := sm.snapshotOpsCounter.Load()
	if activeOps == 0 {
		sm.logger.Infow("No active snapshot operations. Snapshot manager stopped cleanly.")
		return
	}

	sm.logger.Infow("Waiting for active snapshot operations to complete...",
		"activeOperations", activeOps)

	waitCh := make(chan struct{})
	timer := sm.clock.NewTimer(defaultSnapshotStopTimeout)
	defer timer.Stop()

	go func() {
		sm.snapshotOpsWg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		sm.logger.Infow("All snapshot operations completed cleanly.")
	case <-timer.Chan(): //
		remaining := sm.snapshotOpsCounter.Load()
		sm.logger.Warnw("Timeout waiting for snapshot operations to finish",
			"timeout", defaultSnapshotStopTimeout,
			"remainingOperations", remaining)
	}

	sm.logger.Infow("Snapshot manager stopped.")
}

// restoreSnapshot applies snapshot data to the Applier and updates Raft state.
func (sm *snapshotManager) restoreSnapshot(ctx context.Context, meta types.SnapshotMetadata, data []byte) error {
	if sm.isShutdownOrContextDone(ctx, "restoreSnapshot") {
		return ErrShuttingDown
	}

	sm.logger.Infow("Restoring state machine from snapshot",
		"index", meta.LastIncludedIndex,
		"term", meta.LastIncludedTerm,
		"sizeBytes", len(data),
	)
	start := sm.clock.Now()

	restoreCtx, cancel := context.WithTimeout(ctx, defaultSnapshotRestoreTimeout)
	defer cancel()

	if err := sm.applier.RestoreSnapshot(restoreCtx, meta.LastIncludedIndex, meta.LastIncludedTerm, data); err != nil {
		sm.logger.Errorw("Applier failed to restore snapshot", "error", err, "index", meta.LastIncludedIndex)
		sm.metrics.ObserveSnapshot(SnapshotActionApply, SnapshotStatusFailure,
			"reason", string(SnapshotReasonApplierRestoreFailed))
		return fmt.Errorf("applier.RestoreSnapshot failed: %w", err)
	}

	sm.stateMgr.UpdateLastApplied(meta.LastIncludedIndex)
	sm.stateMgr.UpdateCommitIndex(meta.LastIncludedIndex)

	duration := sm.clock.Now().Sub(start)
	sm.logger.Infow("Snapshot restored and Raft state updated successfully",
		"index", meta.LastIncludedIndex,
		"term", meta.LastIncludedTerm,
		"newCommitIndex", meta.LastIncludedIndex,
		"newLastApplied", meta.LastIncludedIndex,
		"duration", duration,
	)

	sm.metrics.ObserveSnapshot(SnapshotActionApply, SnapshotStatusSuccess)
	return nil
}

// updateSnapshotMetadata updates the manager's internal record of the latest snapshot.
// Must be called with sm.mu write lock held.
func (sm *snapshotManager) updateSnapshotMetadata(meta types.SnapshotMetadata) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if meta.LastIncludedIndex > sm.lastSnapshotIndex ||
		(meta.LastIncludedIndex == sm.lastSnapshotIndex && meta.LastIncludedTerm > sm.lastSnapshotTerm) {

		sm.logger.Debugw("Updating internal snapshot metadata",
			"oldIndex", sm.lastSnapshotIndex, "oldTerm", sm.lastSnapshotTerm,
			"newIndex", meta.LastIncludedIndex, "newTerm", meta.LastIncludedTerm)
		sm.lastSnapshotIndex = meta.LastIncludedIndex
		sm.lastSnapshotTerm = meta.LastIncludedTerm
	} else {
		sm.logger.Debugw("Skipping update of internal snapshot metadata; incoming is not newer",
			"currentIndex", sm.lastSnapshotIndex, "currentTerm", sm.lastSnapshotTerm,
			"incomingIndex", meta.LastIncludedIndex, "incomingTerm", meta.LastIncludedTerm)
	}
}

// isShutdownOrContextDone checks if the node is shutting down or the context is done.
// Logs a debug message if true.
func (sm *snapshotManager) isShutdownOrContextDone(ctx context.Context, operationName string) bool {
	if sm.isShutdown.Load() {
		sm.logger.Debugw(fmt.Sprintf("%s aborted: node is shutting down", operationName))
		return true
	}
	if err := ctx.Err(); err != nil {
		sm.logger.Debugw(fmt.Sprintf("%s aborted: context done", operationName), "error", err)
		return true
	}
	return false
}
