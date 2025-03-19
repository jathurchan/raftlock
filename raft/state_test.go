package raft

import (
	"testing"
)

func TestStateString(t *testing.T) {
	tests := []struct {
		name     string
		state    RaftState
		expected string
	}{
		{"follower", Follower, "Follower"},
		{"candidate", Candidate, "Candidate"},
		{"leader", Leader, "Leader"},
		{"invalid_state", RaftState(3), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res := tt.state.String(); res != tt.expected {
				t.Errorf("State.String() = %v, while expected %v", res, tt.expected)
			}
		})
	}
}

func TestStateIsValid(t *testing.T) {
	tests := []struct {
		name     string
		state    RaftState
		expected bool
	}{
		{"follower is valid", Follower, true},
		{"candidate is valid", Candidate, true},
		{"leader is valid", Leader, true},
		{"out of range state is invalid", RaftState(3), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res := tt.state.IsValid(); res != tt.expected {
				t.Errorf("State.String() = %v, while expected %v", res, tt.expected)
			}
		})
	}
}

func TestStateCanTransitionTo(t *testing.T) {
	tests := []struct {
		name         string
		currentState RaftState
		targetState  RaftState
		expected     bool
	}{
		// Valid transitions
		{"follower to candidate", Follower, Candidate, true},
		{"candidate to follower", Candidate, Follower, true},
		{"candidate to candidate (new election)", Candidate, Candidate, true},
		{"candidate to leader", Candidate, Leader, true},
		{"leader to follower", Leader, Leader, true},

		// Invalid transitions
		{"follower to leader", Follower, Leader, false},
		{"follower to follower", Follower, Follower, false},
		{"leader to candidate", Leader, Candidate, false},
		{"invalid state to any", RaftState(3), Follower, false},
		{"any to invalid state", Follower, RaftState(3), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res := tt.currentState.CanTransitionTo(tt.targetState); res != tt.expected {
				t.Errorf("State.CanTransitionTo(%v) = %v, while expected %v", tt.targetState, res, tt.expected)
			}
		})
	}
}
