package types

import "slices"

// String helps with making role values more readable in logs and debug output.
func (rr NodeRole) String() string {
	switch rr {
	case RoleFollower:
		return "Follower"
	case RoleCandidate:
		return "Candidate"
	case RoleLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// IsValid checks if the role is one of the valid Raft roles.
func (rr NodeRole) IsValid() bool {
	return rr == RoleFollower || rr == RoleCandidate || rr == RoleLeader
}

// transitions maps the valid role transitions in the Raft algorithm.
var transitions = map[NodeRole][]NodeRole{
	RoleFollower:  {RoleCandidate},
	RoleCandidate: {RoleFollower, RoleCandidate, RoleLeader},
	RoleLeader:    {RoleLeader, RoleFollower},
}

// CanTransitionTo checks if a transition from the current role to the target role is valid.
func (rr NodeRole) CanTransitionTo(target NodeRole) bool {
	validTargets, exists := transitions[rr]
	if !exists {
		return false
	}

	return slices.Contains(validTargets, target)
}
