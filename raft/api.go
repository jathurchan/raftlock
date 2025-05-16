package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

// Raft defines the primary application-facing interface for interacting with a Raft node.
//
// It provides lifecycle control, command proposal, state observation, and committed entry delivery.
// Peer-to-peer RPCs are handled separately via PeerNetwork and RPCHandler interfaces.
//
// The Raft implementation expects the application to drive logical time progression
// by calling Tick() periodically at a configured interval ("tick interval").
type Raft interface {
	rpcHandler

	// Start initializes the Raft node and launches background tasks such as elections, replication, and applying entries.
	// Must complete successfully before other methods are called.
	Start() error

	// Stop gracefully shuts down the Raft node.
	// Attempts to complete in-progress operations, flush state if necessary, and release resources.
	// Blocks until shutdown completes or the context expires.
	Stop(ctx context.Context) error

	// Tick advances the Raft node's logical clock by one tick.
	// Tick must be called externally at a regular interval to drive elections, heartbeats, and maintenance operations.
	Tick(ctx context.Context)

	// Propose submits a new command to be replicated across the Raft cluster.
	//
	// If the node is the leader, the command is appended locally and replication begins.
	// If not the leader, returns (isLeader = false, err = ErrNotLeader).
	//
	// Success indicates the proposal was locally accepted; commitment is signaled later via ApplyChannel().
	//
	// Errors:
	//   - ErrNotLeader if the node is not leader.
	//   - ErrTimeout if the proposal could not be processed in time.
	//   - ErrShuttingDown if the node is shutting down.
	Propose(ctx context.Context, command []byte) (index types.Index, term types.Term, isLeader bool, err error)

	// ReadIndex issues a linearizable read request, returning the commit index at the time the request is processed.
	//
	// The caller must ensure the application's state machine reflects at least the returned commit index
	// before reading any application state for linearizability.
	//
	// Errors:
	//   - ErrNotLeader if the node is not leader.
	//   - ErrTimeout if the operation times out.
	//   - ErrShuttingDown if the node is shutting down.
	ReadIndex(ctx context.Context) (committedIndex types.Index, err error)

	// Status returns a snapshot of the Raft node’s internal state for monitoring and diagnostics.
	Status() types.RaftStatus

	// GetState returns the current term and whether the node believes it is the leader.
	// Useful for lightweight leadership checks without fetching full status.
	GetState() (term types.Term, isLeader bool)

	// GetLeaderID returns the NodeID of the known leader, or an empty NodeID if unknown.
	// Leadership information may be stale if an election is ongoing.
	GetLeaderID() types.NodeID

	// GetCommitIndex returns the highest log entry index known to be committed.
	// The application’s state machine will eventually reflect at least this index.
	GetCommitIndex() types.Index

	// ApplyChannel returns a read-only channel delivering committed log entries and snapshots.
	//
	// Each ApplyMsg signals either a committed command (CommandValid = true)
	// or a snapshot being installed (SnapshotValid = true).
	//
	// The channel must be continuously drained to avoid blocking Raft's internal processing.
	// It remains open during normal operation and is closed only after Stop() completes.
	ApplyChannel() <-chan types.ApplyMsg

	// LeaderChangeChannel returns a read-only channel delivering leader change notifications.
	//
	// A notification is sent when the node learns about a new leader or when leadership is lost.
	// The notification carries the NodeID of the new leader, or an empty NodeID if there is no known leader.
	//
	// The channel remains open during normal operation and is closed after Stop() completes.
	LeaderChangeChannel() <-chan types.NodeID
}

// rpcHandler defines how a Raft node processes incoming RPCs from peers.
// It controls the node’s behavior when interacting with candidates and leaders.
type rpcHandler interface {
	// RequestVote handles a RequestVote RPC from a candidate.
	// It checks whether the candidate’s term is at least as recent as the receiver’s term
	// and whether the candidate’s log is sufficiently up-to-date to grant a vote.
	// The context must be respected for deadlines and cancellations.
	// Returns a reply indicating whether the vote was granted, the receiver’s current term, or an error.
	RequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteReply, error)

	// AppendEntries handles an AppendEntries RPC, typically sent by the leader.
	// It verifies the leader’s term, checks log consistency at PrevLogIndex and PrevLogTerm,
	// appends any new entries, and advances the commit index based on LeaderCommit.
	// This RPC also acts as a heartbeat when no entries are present.
	// The context must be respected for deadlines and cancellations.
	// Returns a reply indicating whether the entries were successfully appended, the receiver’s term,
	// and conflict information if applicable, or an error if the operation failed.
	AppendEntries(ctx context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error)

	// InstallSnapshot handles an InstallSnapshot RPC, typically sent by the leader.
	// It installs a snapshot on the receiver, replacing existing log entries covered by the snapshot,
	// and bringing the follower up to date if it has fallen too far behind.
	// The context must be respected for deadlines and cancellations.
	// Returns a reply containing the receiver’s current term, or an error if the snapshot installation failed.
	InstallSnapshot(ctx context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error)
}
