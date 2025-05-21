package lock

import (
	"time"

	"github.com/jathurchan/raftlock/types"
)

// lockState encapsulates the current state of a specific lock.
type lockState struct {
	// Unique identifier of the lock.
	lockID types.LockID

	// Identifier of the client currently holding the lock. Empty if unowned.
	owner types.ClientID

	// Raft log index when the lock was last acquired or renewed.
	version types.Index

	// Timestamp when the lock was acquired.
	acquiredAt time.Time

	// Timestamp when the lock is set to expire automatically.
	expiresAt time.Time

	// Optional key-value metadata associated with the lock.
	metadata map[string]string

	// Timestamp of the last modification to the lock's state.
	lastModified time.Time
}
