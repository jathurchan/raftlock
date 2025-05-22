package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

// Applier defines how Raft applies committed log entries to an application's state machine
// and manages state snapshots. It bridges the consensus layer with application logic.
//
// Implementations must be:
//   - Deterministic: Same input yields identical results across nodes.
//   - Idempotent: Reapplying a command has no side effects.
//   - Thread-safe: Safe for concurrent calls from multiple goroutines.
//   - Cancellable: Context-aware for deadlines and cancellations.
type Applier interface {
	// Apply applies a committed log entry at the given index.
	//
	// Called after the entry is committed by Raft. The implementation must:
	//   - Decode the command
	//   - Apply it to the state machine
	//   - Track the latest applied index
	//   - Handle duplicates safely (important during recovery)
	//
	// The context may carry deadlines or cancellations. Implementations should
	// still attempt to apply the command, as skipping a committed entry may
	// lead to divergence.
	//
	// Returns an error only for unrecoverable failures.
	Apply(ctx context.Context, index types.Index, command []byte) (err error)

	// Snapshot returns a serialized, point-in-time snapshot of application state.
	//
	// Used by Raft for log compaction. Must:
	//   - Capture a consistent state view
	//   - Serialize the state and include integrity metadata
	//   - Record the highest included log index
	//
	// Must be atomic with respect to Apply calls—either block Apply or
	// guarantee the snapshot includes all changes up to a known index.
	//
	// Context should be respected for cancellation or timeouts.
	//
	// Returns the highest log index included and the snapshot data.
	Snapshot(ctx context.Context) (lastAppliedIndex types.Index, snapshotData []byte, err error)

	// RestoreSnapshot restores state from a previously created snapshot.
	//
	// Called when recovering or syncing a lagging node. Must:
	//   - Validate and deserialize the snapshot
	//   - Replace all existing state
	//   - Update internal tracking to match the snapshot's index and term
	//
	// After restoration, Apply must ignore entries at or below lastIncludedIndex.
	// Partial restores must be avoided—best effort should be made even if the
	// context expires.
	//
	// This is critical for correctness and must be implemented carefully.
	RestoreSnapshot(ctx context.Context, lastIncludedIndex types.Index, lastIncludedTerm types.Term, snapshotData []byte) error
}
