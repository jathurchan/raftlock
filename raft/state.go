package raft

import "slices"

// RaftState represents the role a server plays in the Raft consensus algorithm.
// At any given time, each server in the Raft cluster is in one of three states.
type RaftState int

const (
	// Follower is the default state of a Raft server when it starts up.
	// - Followers do not initiate actions on their own and only respond to requests from other servers.
	// - If a Follower receives a heartbeat (`AppendEntries` RPC) from a valid Leader, it resets its election timeout.
	// - Else, it transitions to Candidate and starts a new election.
	Follower RaftState = iota

	// Candidate is the state a server enters when it times out without hearing from a Leader.
	// - A Candidate starts a new election by incrementing its term and sending `RequestVote` RPCs to other servers.
	// - If it receives votes from the majority of servers, it transitions to Leader.
	// - If it discovers an existing Leader (via an `AppendEntries` RPC) or detects a higher term in another server's message,
	//   it transitions back to Follower.
	// - If it times out without a majority vote, it starts a new election cycle.
	Candidate

	// Leader is the state a server enters after winning an election.
	// - A Leader sends periodic heartbeats (`AppendEntries` RPCs) to maintain authority over Followers.
	// - It handles client requests and replicates log entries across the cluster.
	// - If it detects a higher term in another server's message, it steps down and transitions back to Follower.
	Leader
)

// String helps with making state values more readable in logs and debug output.
func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// IsValid checks if the state is one of the valid Raft states.
func (s RaftState) IsValid() bool {
	return s == Follower || s == Candidate || s == Leader
}

// transitions maps the valid state transitions in the Raft algorithm.
var transitions = map[RaftState][]RaftState{
	Follower:  {Candidate},
	Candidate: {Follower, Candidate, Leader},
	Leader:    {Leader},
}

// CanTransitionTo checks if a transition from the current state to the target state is valid.
func (s RaftState) CanTransitionTo(target RaftState) bool {
	validTargets, exists := transitions[s]
	if !exists {
		return false
	}

	return slices.Contains(validTargets, target)
}
