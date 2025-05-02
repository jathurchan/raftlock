package raft

import (
	"context"

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
	GetPeerReplicationStatus() map[types.NodeID]types.PeerReplicationStatus

	// ReplicateToPeer initiates a log replication attempt to the specified peer,
	// either by sending AppendEntries or triggering a snapshot if necessary.
	// This method is primarily exposed for testing or advanced usage.
	ReplicateToPeer(ctx context.Context, peerID types.NodeID, isHeartbeat bool)

	// Stop shuts down background tasks and releases resources.
	Stop()
}
