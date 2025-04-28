package raft

import (
	"context"

	"github.com/jathurchan/raftlock/types"
)

// Applier defines how the Raft consensus module applies committed log entries
// and manages snapshots by delegating to an external component.
//
// Implementations must be deterministic, idempotent, and safe for concurrent use.
type Applier interface {
	// Apply applies a committed log entry at the given index.
	// Invoked by the Raft layer after an entry has been safely committed.
	//
	// Apply must be idempotent: applying the same entry multiple times must not change the state.
	// Returns an error only for fatal, unrecoverable conditions.
	Apply(ctx context.Context, index types.Index, command []byte) (err error)

	// Snapshot captures a consistent point-in-time view of the application's state.
	// Periodically invoked by the Raft layer for log compaction.
	//
	// It must be safe for concurrent use and support cancellation via the context.
	// Returns the highest log index included in the snapshot, the serialized snapshot data,
	// and an optional error.
	Snapshot(ctx context.Context) (lastAppliedIndex types.Index, snapshotData []byte, err error)

	// RestoreSnapshot restores the application state from a previously generated snapshot.
	// Called when a node must efficiently catch up to the cluster state.
	//
	// The implementation must fully replace any existing state with the provided snapshot,
	// aligning internal tracking (e.g., lastAppliedIndex) with the given lastIncludedIndex and lastIncludedTerm.
	RestoreSnapshot(ctx context.Context, lastIncludedIndex types.Index, lastIncludedTerm types.Term, snapshotData []byte) error
}
