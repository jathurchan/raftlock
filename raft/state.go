package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

// StateManager defines the interface for managing Raft node state.
// It handles term changes, role transitions, vote tracking, and log index updates.
type StateManager interface {
	// Initialize loads the persisted state from storage.
	Initialize(ctx context.Context) error

	// GetState returns the current term, role, and leader ID.
	GetState() (types.Term, types.NodeRole, types.NodeID)

	// BecomeCandidate transitions the node to candidate state.
	// Returns true if the transition was successful.
	BecomeCandidate(ctx context.Context) bool

	// BecomeLeader transitions the node to leader state.
	// Returns true if the transition was successful.
	BecomeLeader() bool

	// BecomeFollower transitions the node to follower state with the given leader ID.
	BecomeFollower(leaderID types.NodeID)

	// CheckTermAndStepDown compares the given term with the local term.
	// If the given term is higher, steps down and updates state.
	// Returns whether a step-down occurred and the previous term.
	CheckTermAndStepDown(ctx context.Context, rpcTerm types.Term, rpcLeader types.NodeID) (steppedDown bool, previousTerm types.Term)

	// GrantVote attempts to grant a vote to the given candidate for the specified term.
	// Returns true if the vote was granted.
	GrantVote(ctx context.Context, candidateID types.NodeID, term types.Term) bool

	// UpdateCommitIndex sets the commit index if the new index is higher.
	// Returns true if the index was updated.
	UpdateCommitIndex(newCommitIndex types.Index) bool

	// UpdateLastApplied sets the last applied index if the new index is higher.
	// Returns true if the index was updated.
	UpdateLastApplied(newLastApplied types.Index) bool

	// GetCommitIndex returns the current commit index.
	GetCommitIndex() types.Index

	// GetLastApplied returns the index of the last applied log entry.
	GetLastApplied() types.Index

	// Stop shuts down the state manager and releases resources.
	Stop()
}
