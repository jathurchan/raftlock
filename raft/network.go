package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

// Network represents the RPC communication layer between Raft peers.
//
// It handles connection management, serialization, context cancellation,
// and efficient transfer of large payloads (e.g., snapshots).
//
// Peer membership is fixed at construction. Configuration must be provided
// at initialization, not via methods like Start.
//
// Implementations must be safe for concurrent use.
type Network interface {
	// Start activates the network layer and begins listening for peer RPCs.
	// Must succeed before participating in Raft consensus.
	Start() error

	// Stop gracefully shuts down the network layer, closes connections,
	// and releases resources. Blocks until shutdown completes.
	Stop() error

	// SendRequestVote sends a RequestVote RPC to a target peer.
	SendRequestVote(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error)

	// SendAppendEntries sends an AppendEntries RPC (log replication or heartbeat) to a target peer.
	SendAppendEntries(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error)

	// SendInstallSnapshot efficiently streams or chunks snapshot data to a target peer.
	SendInstallSnapshot(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error)

	// PeerStatus returns health and connection info for a known peer.
	PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error)

	// LocalAddr returns the local address (e.g., "host:port") or empty string if unavailable.
	LocalAddr() string
}
