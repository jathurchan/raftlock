package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

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
