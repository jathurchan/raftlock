package raft

import "testing"

func TestRaftFeatureFlags_WithExplicitFlags(t *testing.T) {
	t.Run("sets explicitlySet flag", func(t *testing.T) {
		flags := FeatureFlags{
			EnableReadIndex:   true,
			EnableLeaderLease: false,
			PreVoteEnabled:    true,
			explicitlySet:     false, // explicitly initialize as false
		}

		if flags.explicitlySet {
			t.Errorf("explicitlySet should initially be false")
		}

		flags = flags.WithExplicitFlags()

		if !flags.explicitlySet {
			t.Errorf("explicitlySet should be true after calling WithExplicitFlags")
		}
	})

	t.Run("supports method chaining", func(t *testing.T) {
		flags := &FeatureFlags{
			EnableReadIndex:   true,
			EnableLeaderLease: false,
			PreVoteEnabled:    true,
		}

		returnedFlags := flags.WithExplicitFlags()

		if !returnedFlags.explicitlySet {
			t.Errorf("explicitlySet should be true in the returned flags")
		}
	})

	t.Run("preserves other fields", func(t *testing.T) {
		flags := &FeatureFlags{
			EnableReadIndex:   true,
			EnableLeaderLease: false,
			PreVoteEnabled:    true,
			explicitlySet:     false,
		}

		flags.WithExplicitFlags()

		if !flags.EnableReadIndex {
			t.Errorf("EnableReadIndex should remain true")
		}
		if flags.EnableLeaderLease {
			t.Errorf("EnableLeaderLease should remain false")
		}
		if !flags.PreVoteEnabled {
			t.Errorf("PreVoteEnabled should remain true")
		}
	})
}
