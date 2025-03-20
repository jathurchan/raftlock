package raft

import (
	"testing"
)

func TestRoleString(t *testing.T) {
	tests := []struct {
		name     string
		role     RaftRole
		expected string
	}{
		{"follower", Follower, "Follower"},
		{"candidate", Candidate, "Candidate"},
		{"leader", Leader, "Leader"},
		{"invalid_role", RaftRole(3), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res := tt.role.String(); res != tt.expected {
				t.Errorf("Role.String() = %v, while expected %v", res, tt.expected)
			}
		})
	}
}

func TestRoleIsValid(t *testing.T) {
	tests := []struct {
		name     string
		role     RaftRole
		expected bool
	}{
		{"follower is valid", Follower, true},
		{"candidate is valid", Candidate, true},
		{"leader is valid", Leader, true},
		{"out of range role is invalid", RaftRole(3), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res := tt.role.IsValid(); res != tt.expected {
				t.Errorf("Role.IsValid() = %v, while expected %v", res, tt.expected)
			}
		})
	}
}

func TestRoleCanTransitionTo(t *testing.T) {
	tests := []struct {
		name        string
		currentRole RaftRole
		targetRole  RaftRole
		expected    bool
	}{
		// Valid transitions
		{"follower to candidate", Follower, Candidate, true},
		{"candidate to follower", Candidate, Follower, true},
		{"candidate to candidate (new election)", Candidate, Candidate, true},
		{"candidate to leader", Candidate, Leader, true},
		{"leader to follower", Leader, Follower, true},

		// Invalid transitions
		{"follower to leader", Follower, Leader, false},
		{"follower to follower", Follower, Follower, false},
		{"leader to candidate", Leader, Candidate, false},
		{"invalid role to any", RaftRole(3), Follower, false},
		{"any to invalid role", Follower, RaftRole(3), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res := tt.currentRole.CanTransitionTo(tt.targetRole); res != tt.expected {
				t.Errorf("Role.CanTransitionTo(%v) = %v, while expected %v", tt.targetRole, res, tt.expected)
			}
		})
	}
}
