package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// ReplicationManager manages log replication, heartbeats, commit progression,
// and peer state. It drives Raft’s leader behavior.
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
	SendHeartbeats(ctx context.Context)

	// MaybeAdvanceCommitIndex determines the highest log index replicated to a quorum
	// and advances the commit index if it is safe to do so.
	// Only entries from the current leader term are eligible for commitment.
	MaybeAdvanceCommitIndex()

	// HasValidLease returns whether the leader still holds a valid lease (if lease-based
	// reads are enabled), ensuring safe servicing of linearizable reads.
	HasValidLease(ctx context.Context) bool

	// VerifyLeadershipAndGetCommitIndex verifies leadership by reaching a quorum,
	// and returns the current commit index for use in ReadIndex-based linearizable reads.
	VerifyLeadershipAndGetCommitIndex(ctx context.Context) (types.Index, error)

	// HandleAppendEntries processes an incoming AppendEntries RPC from the leader.
	// Called by the RPC handler when this node is acting as a follower.
	HandleAppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error)

	// GetPeerReplicationStatus returns the current replication progress for each peer,
	// primarily used for metrics, monitoring, or debugging.
	GetPeerReplicationStatus() map[types.NodeID]types.PeerState

	// ReplicateToPeer initiates a log replication attempt to the specified peer,
	// either by sending AppendEntries or triggering a snapshot if necessary.
	// This method is primarily exposed for testing or advanced usage.
	ReplicateToPeer(ctx context.Context, peerID types.NodeID, isHeartbeat bool)

	// UpdatePeerAfterSnapshotSend updates replication tracking for a peer
	// after a snapshot is successfully sent, typically advancing nextIndex.
	UpdatePeerAfterSnapshotSend(peerID types.NodeID, snapshotIndex types.Index)

	// SetPeerSnapshotInProgress marks whether a snapshot is currently in progress
	// for a given peer, preventing redundant snapshot attempts.
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
	ApplyNotifyCh  chan<- struct{}
}

// validateReplicationManagerDeps checks that all required dependencies are set.
// Returns an error describing the first missing or invalid field.
func validateReplicationManagerDeps(deps ReplicationManagerDeps) error {
	switch {
	case deps.Mu == nil:
		return fmt.Errorf("ReplicationManagerDeps: Mu is required")
	case deps.ID == "":
		return fmt.Errorf("ReplicationManagerDeps: ID is required")
	case deps.NetworkMgr == nil:
		return fmt.Errorf("ReplicationManagerDeps: NetworkMgr is required")
	case deps.StateMgr == nil:
		return fmt.Errorf("ReplicationManagerDeps: StateMgr is required")
	case deps.LogMgr == nil:
		return fmt.Errorf("ReplicationManagerDeps: LogMgr is required")
	case deps.SnapshotMgr == nil:
		return fmt.Errorf("ReplicationManagerDeps: SnapshotMgr is required")
	case deps.Metrics == nil:
		return fmt.Errorf("ReplicationManagerDeps: Metrics is required")
	case deps.Logger == nil:
		return fmt.Errorf("ReplicationManagerDeps: Logger is required")
	case deps.Clock == nil:
		return fmt.Errorf("ReplicationManagerDeps: Clock is required")
	case deps.ApplyNotifyCh == nil:
		return fmt.Errorf("ReplicationManagerDeps: ApplyNotifyCh is required")
	case deps.IsShutdownFlag == nil:
		return fmt.Errorf("ReplicationManagerDeps: IsShutdownFlag is required")
	case deps.QuorumSize <= 0:
		return fmt.Errorf("ReplicationManagerDeps: QuorumSize must be > 0, got %d", deps.QuorumSize)
	default:
		return nil
	}
}

// replicationManager is the concrete implementation of the ReplicationManager interface.
type replicationManager struct {
	mu         *sync.RWMutex // Raft's mutex protecting state fields
	isShutdown *atomic.Bool  // Shared flag indicating Raft shutdown

	id         types.NodeID                // ID of the local Raft node.
	peers      map[types.NodeID]PeerConfig // Configuration of peer nodes
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

	// Signaling pipeline:
	// replication → notifyCommitCh → commit check → applyNotifyCh → state machine
	applyNotifyCh  chan<- struct{} // Signals Raft core to apply newly committed log entries
	notifyCommitCh chan struct{}   // Triggers commit index evaluation after replication events

	leaderLease         bool          // Whether leader lease-based reads are enabled
	leaseExpiry         time.Time     // Timestamp when the current lease expires
	leaseDuration       time.Duration // Duration of the leader lease
	lastQuorumHeartbeat time.Time     // Time of last successful quorum heartbeat
}
