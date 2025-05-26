package types

import (
	"slices"
	"testing"
)

func TestNodeRole_String(t *testing.T) {
	tests := []struct {
		name     string
		role     NodeRole
		expected string
	}{
		{
			name:     "RoleFollower returns Follower",
			role:     RoleFollower,
			expected: "Follower",
		},
		{
			name:     "RoleCandidate returns Candidate",
			role:     RoleCandidate,
			expected: "Candidate",
		},
		{
			name:     "RoleLeader returns Leader",
			role:     RoleLeader,
			expected: "Leader",
		},
		{
			name:     "Invalid role returns Unknown",
			role:     NodeRole(99), // Invalid role value
			expected: "Unknown",
		},
		{
			name:     "Negative role returns Unknown",
			role:     NodeRole(-1), // Invalid negative role
			expected: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.role.String()
			if result != tt.expected {
				t.Errorf("NodeRole.String() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestNodeRole_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		role     NodeRole
		expected bool
	}{
		{
			name:     "RoleFollower is valid",
			role:     RoleFollower,
			expected: true,
		},
		{
			name:     "RoleCandidate is valid",
			role:     RoleCandidate,
			expected: true,
		},
		{
			name:     "RoleLeader is valid",
			role:     RoleLeader,
			expected: true,
		},
		{
			name:     "Invalid role value is not valid",
			role:     NodeRole(99),
			expected: false,
		},
		{
			name:     "Negative role value is not valid",
			role:     NodeRole(-1),
			expected: false,
		},
		{
			name:     "Role value 3 is not valid",
			role:     NodeRole(3),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.role.IsValid()
			if result != tt.expected {
				t.Errorf("NodeRole.IsValid() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestNodeRole_CanTransitionTo(t *testing.T) {
	tests := []struct {
		name     string
		from     NodeRole
		to       NodeRole
		expected bool
	}{
		// Valid transitions from RoleFollower
		{
			name:     "Follower can transition to Candidate",
			from:     RoleFollower,
			to:       RoleCandidate,
			expected: true,
		},
		{
			name:     "Follower cannot transition to Leader",
			from:     RoleFollower,
			to:       RoleLeader,
			expected: false,
		},
		{
			name:     "Follower cannot stay as Follower",
			from:     RoleFollower,
			to:       RoleFollower,
			expected: false,
		},

		// Valid transitions from RoleCandidate
		{
			name:     "Candidate can transition to Follower",
			from:     RoleCandidate,
			to:       RoleFollower,
			expected: true,
		},
		{
			name:     "Candidate can transition to Leader",
			from:     RoleCandidate,
			to:       RoleLeader,
			expected: true,
		},
		{
			name:     "Candidate can stay as Candidate",
			from:     RoleCandidate,
			to:       RoleCandidate,
			expected: true,
		},

		// Valid transitions from RoleLeader
		{
			name:     "Leader can transition to Follower",
			from:     RoleLeader,
			to:       RoleFollower,
			expected: true,
		},
		{
			name:     "Leader can stay as Leader",
			from:     RoleLeader,
			to:       RoleLeader,
			expected: true,
		},
		{
			name:     "Leader cannot transition to Candidate",
			from:     RoleLeader,
			to:       RoleCandidate,
			expected: false,
		},

		// Invalid transitions from invalid roles
		{
			name:     "Invalid role cannot transition to any valid role",
			from:     NodeRole(99),
			to:       RoleFollower,
			expected: false,
		},
		{
			name:     "Valid role cannot transition to invalid role",
			from:     RoleFollower,
			to:       NodeRole(99),
			expected: false,
		},
		{
			name:     "Invalid role cannot transition to invalid role",
			from:     NodeRole(99),
			to:       NodeRole(88),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.from.CanTransitionTo(tt.to)
			if result != tt.expected {
				t.Errorf("NodeRole.CanTransitionTo() from %v to %v = %v, expected %v",
					tt.from, tt.to, result, tt.expected)
			}
		})
	}
}

func TestTransitionsMap(t *testing.T) {
	validRoles := []NodeRole{RoleFollower, RoleCandidate, RoleLeader}

	for _, role := range validRoles {
		if _, exists := transitions[role]; !exists {
			t.Errorf("transitions map missing entry for role %v", role)
		}
	}

	followerTransitions := transitions[RoleFollower]
	if len(followerTransitions) != 1 || followerTransitions[0] != RoleCandidate {
		t.Errorf("Follower should only transition to Candidate, got %v", followerTransitions)
	}

	candidateTransitions := transitions[RoleCandidate]
	expectedCandidateTransitions := []NodeRole{RoleFollower, RoleCandidate, RoleLeader}
	if len(candidateTransitions) != 3 {
		t.Errorf("Candidate should have 3 possible transitions, got %d", len(candidateTransitions))
	}
	for _, expected := range expectedCandidateTransitions {
		found := slices.Contains(candidateTransitions, expected)
		if !found {
			t.Errorf("Candidate transitions missing %v", expected)
		}
	}

	leaderTransitions := transitions[RoleLeader]
	expectedLeaderTransitions := []NodeRole{RoleLeader, RoleFollower}
	if len(leaderTransitions) != 2 {
		t.Errorf("Leader should have 2 possible transitions, got %d", len(leaderTransitions))
	}
	for _, expected := range expectedLeaderTransitions {
		found := slices.Contains(leaderTransitions, expected)
		if !found {
			t.Errorf("Leader transitions missing %v", expected)
		}
	}
}

func TestNodeRoleConstants(t *testing.T) {
	if RoleFollower != 0 {
		t.Errorf("RoleFollower should be 0, got %d", RoleFollower)
	}
	if RoleCandidate != 1 {
		t.Errorf("RoleCandidate should be 1, got %d", RoleCandidate)
	}
	if RoleLeader != 2 {
		t.Errorf("RoleLeader should be 2, got %d", RoleLeader)
	}
}
