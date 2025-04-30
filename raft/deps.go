package raft

import (
	"fmt"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
)

// Dependencies bundles the external components required by a Raft node instance.
type Dependencies struct {
	// Storage persists Raft log entries, state, and snapshots.
	Storage storage.Storage

	// Network handles RPC communication between Raft peers.
	Network NetworkManager

	// Applier applies committed entries to the user state machine and handles snapshots.
	Applier Applier

	// Logger provides structured logging.
	Logger logger.Logger

	// Metrics records operational metrics.
	Metrics Metrics

	// Clock provides an abstraction for time-related operations (Now, Sleep, Tickers, Timers).
	Clock Clock

	// Rand provides an abstraction for random number generation.
	Rand Rand
}

// Validate checks that all required dependencies are provided.
// Optional dependencies (Logger, Metrics) may be nil.
func (d *Dependencies) Validate() error {
	if d == nil {
		return fmt.Errorf("%w: dependencies struct cannot be nil", ErrMissingDependencies)
	}
	if d.Storage == nil {
		return fmt.Errorf("%w: Storage dependency cannot be nil", ErrMissingDependencies)
	}
	if d.Network == nil {
		return fmt.Errorf("%w: Network dependency cannot be nil", ErrMissingDependencies)
	}
	if d.Applier == nil {
		return fmt.Errorf("%w: Applier dependency cannot be nil", ErrMissingDependencies)
	}
	if d.Clock == nil {
		return fmt.Errorf("%w: Clock dependency cannot be nil", ErrMissingDependencies)
	}
	if d.Rand == nil {
		return fmt.Errorf("%w: Rand dependency cannot be nil", ErrMissingDependencies)
	}
	return nil
}
