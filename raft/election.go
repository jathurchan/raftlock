package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

// ElectionManager defines the interface for managing leader elections
// in the Raft consensus algorithm. It handles election timeouts,
// vote requests and responses, and transitions to the leader role.
type ElectionManager interface {
	// Initialize prepares the election manager for operation,
	// setting the initial randomized election timeout.
	Initialize() error

	// Tick advances the election timer by one logical tick.
	// If the election timeout expires, it triggers a new election or pre-vote.
	// This should be invoked periodically by the main Raft loop.
	Tick(ctx context.Context)

	// ResetTimerOnHeartbeat resets the election timer upon receiving
	// a valid heartbeat (AppendEntries RPC) from the current leader.
	// This prevents unnecessary elections while a leader is active.
	ResetTimerOnHeartbeat()

	// HandleRequestVote processes an incoming RequestVote RPC from a candidate
	// and returns a reply indicating whether the vote was granted, based on
	// term, log freshness, and prior votes.
	HandleRequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteReply, error)

	// Stop performs any necessary cleanup when shutting down the election manager.
	Stop()
}
