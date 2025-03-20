package raft

import "slices"

// RaftRole represents the role a server plays in the Raft consensus algorithm.
// At any given time, each server in the Raft cluster is in one of three roles.
type RaftRole int

const (
	// Follower is the default role of a Raft server when it starts up.
	// - Followers do not initiate actions on their own and only respond to requests from other servers.
	// - If a Follower receives a heartbeat (`AppendEntries` RPC) from a valid Leader, it resets its election timeout.
	// - Else, it transitions to Candidate and starts a new election.
	Follower RaftRole = iota

	// Candidate is the role a server enters when it times out without hearing from a Leader.
	// - A Candidate starts a new election by incrementing its term and sending `RequestVote` RPCs to other servers.
	// - If it receives votes from the majority of servers, it transitions to Leader.
	// - If it discovers an existing Leader (via an `AppendEntries` RPC) or detects a higher term in another server's message,
	//   it transitions back to Follower.
	// - If it times out without a majority vote, it starts a new election cycle.
	Candidate

	// Leader is the role a server enters after winning an election.
	// - A Leader sends periodic heartbeats (`AppendEntries` RPCs) to maintain authority over Followers.
	// - It handles client requests and replicates log entries across the cluster.
	// - If it detects a higher term in another server's message, it steps down and transitions back to Follower.
	Leader
)

// String helps with making role values more readable in logs and debug output.
func (rr RaftRole) String() string {
	switch rr {
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

// IsValid checks if the role is one of the valid Raft roles.
func (rr RaftRole) IsValid() bool {
	return rr == Follower || rr == Candidate || rr == Leader
}

// transitions maps the valid role transitions in the Raft algorithm.
var transitions = map[RaftRole][]RaftRole{
	Follower:  {Candidate},
	Candidate: {Follower, Candidate, Leader},
	Leader:    {Leader, Follower},
}

// CanTransitionTo checks if a transition from the current role to the target role is valid.
func (rr RaftRole) CanTransitionTo(target RaftRole) bool {
	validTargets, exists := transitions[rr]
	if !exists {
		return false
	}

	return slices.Contains(validTargets, target)
}
