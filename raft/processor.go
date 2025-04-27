package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

// Processor defines the interface for application-specific state machine logic,
// connecting the Raft consensus module to the system being replicated.
//
// Implementations must be deterministic and safe for concurrent use.
type Processor interface {
	// Apply applies a committed log entry at the given index.
	// It is invoked by the Raft layer after an entry has been safely committed.
	//
	// Apply should be idempotent: reapplying the same entry must not affect the state.
	//
	// err must be non-nil only if a fatal, unrecoverable error occurs.
	Apply(ctx context.Context, index types.Index, command []byte) (err error)

	// Snapshot captures a consistent point-in-time view of the application's state.
	// It is periodically invoked by the Raft layer for log compaction.
	//
	// The operation must be safe for concurrent execution and respect context cancellation
	// if snapshot generation is slow or blocking.
	//
	// Returns the highest log index included in the snapshot, the serialized snapshot data,
	// and an optional error.
	Snapshot(ctx context.Context) (lastAppliedIndex types.Index, snapshotData []byte, err error)

	// RestoreSnapshot restores the application state from a previously generated snapshot.
	// It is used when a node falls behind and needs to catch up efficiently.
	//
	// The implementation must fully replace any existing state with the provided snapshot,
	// aligning internal tracking (e.g., lastAppliedIndex) with the given lastIncludedIndex and lastIncludedTerm.
	RestoreSnapshot(ctx context.Context, lastIncludedIndex types.Index, lastIncludedTerm types.Term, snapshotData []byte) error
}
