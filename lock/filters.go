package lock

import (
	"time"

	"github.com/jathurchan/raftlock/types"
)

// LockFilter defines a function that determines whether a given lock
// satisfies specific criteria. Used in GetLocks queries.
type LockFilter func(*types.LockInfo) bool

var (
	// FilterByOwner returns a LockFilter that matches locks held by the specified clientID.
	FilterByOwner = func(clientID types.ClientID) LockFilter {
		return func(lock *types.LockInfo) bool {
			return lock.OwnerID == clientID
		}
	}

	// FilterExpiringSoon returns a LockFilter that matches locks set to expire
	// within the specified duration from the current time.
	FilterExpiringSoon = func(within time.Duration) LockFilter {
		return func(lock *types.LockInfo) bool {
			// Ensure ExpiresAt is not the zero value and the lock is actually expiring soon.
			return !lock.ExpiresAt.IsZero() && time.Until(lock.ExpiresAt) <= within
		}
	}

	// FilterAll is a LockFilter that matches all locks unconditionally.
	FilterAll LockFilter = func(*types.LockInfo) bool {
		return true
	}
)
