package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
	NotifyCommitCheck func()
}

// validateSnapshotManagerDeps verifies that all required dependencies are present.
func validateSnapshotManagerDeps(deps SnapshotManagerDeps) error {
	if deps.ID == "" {
		return errors.New("missing Node ID")
	}
	if deps.Storage == nil {
		return errors.New("missing Storage dependency")
	}
	if deps.Applier == nil {
		return errors.New("missing Applier dependency")
	}
	if deps.LogMgr == nil {
		return errors.New("missing LogManager dependency")
	}
	if deps.StateMgr == nil {
		return errors.New("missing StateManager dependency")
	}
	if deps.PeerStateUpdater == nil {
		return errors.New("missing PeerStateUpdater dependency")
	}
	if deps.Metrics == nil {
		return errors.New("missing Metrics dependency")
	}
	if deps.Logger == nil {
		return errors.New("missing Logger dependency")
	}
	if deps.NetworkMgr == nil {
		return errors.New("missing NetworkManager dependency")
	}
	if deps.Clock == nil {
		return errors.New("missing Clock dependency")
	}
	if deps.IsShutdown == nil {
		return errors.New("missing IsShutdown flag")
	}
	if deps.NotifyCommitCheck == nil {
		return errors.New("missing NotifyCommitCheck function")
	}
	return nil
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

	snapshotInProgress atomic.Bool // Whether a local snapshot is in progress
	lastSnapshotIndex  types.Index // Last snapshot index (protected by mu)
	lastSnapshotTerm   types.Term  // Last snapshot term (protected by mu)

	snapshotOpsCounter atomic.Int32 // Tracks active snapshot operations
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

// Initialize loads the last snapshot, restores state if needed, and truncates the log prefix.
func (sm *snapshotManager) Initialize(ctx context.Context) error {
	panic("not implemented")
}

// Tick checks if conditions are met to create a new snapshot.
func (sm *snapshotManager) Tick(ctx context.Context) {
	panic("not implemented")
}

// GetSnapshotMetadata returns the index and term of the last known snapshot. Thread-safe.
func (sm *snapshotManager) GetSnapshotMetadata() types.SnapshotMetadata {
	panic("not implemented")
}

// HandleInstallSnapshot processes an incoming snapshot from the leader.
func (sm *snapshotManager) HandleInstallSnapshot(ctx context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
	panic("not implemented")
}

// SendSnapshot sends the latest snapshot to a follower.
// Typically called by replication logic when a follower is too far behind.
func (sm *snapshotManager) SendSnapshot(ctx context.Context, targetID types.NodeID, term types.Term) {
	panic("not implemented")
}

// Stop signals the snapshot manager to clean up and gracefully waits for any ongoing operations.
func (sm *snapshotManager) Stop() {
	panic("not implemented")
}
