package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

// RPCHandler defines how a Raft node processes incoming RPCs from peers.
// It controls the node’s behavior when interacting with candidates and leaders.
type RPCHandler interface {
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
