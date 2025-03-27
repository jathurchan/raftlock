package raft

import "fmt"

// RaftOptions defines configuration options for a Raft instance.
// These options control timing, storage, and behavior parameters.
type RaftOptions struct {
	// ElectionTickCount is the number of ticks that must elapse before
	// triggering an election if no heartbeat is received from the leader.
	// This is a logical time value, not wall-clock time.
	// Default: 10 ticks
	ElectionTickCount int

	// HeartbeatTickCount is the number of ticks that must elapse before
	// a leader sends heartbeats to followers.
	// This is a logical time value, not wall-clock time.
	// Default: 1 tick
	HeartbeatTickCount int

	// ElectionRandomizationFactor determines the range of randomization added to
	// election timeouts to avoid split votes. A value of 0.2 means the actual
	// election timeout will be between ElectionTickCount and
	// ElectionTickCount * (1 + ElectionRandomizationFactor).
	// Default: 0.2 (20% randomization)
	ElectionRandomizationFactor float64

	// MaxLogEntriesPerRequest limits the number of log entries sent in a single
	// AppendEntries RPC. This helps with network efficiency and batching.
	// Default: 100 entries
	MaxLogEntriesPerRequest int

	// SnapshotThreshold is the number of log entries that must exist after the
	// last snapshot before considering creating a new snapshot.
	// Default: 10000 entries
	SnapshotThreshold int

	// StorageSyncDelay controls how often (in ticks) to force persistent storage sync.
	// A value of 1 means sync on every tick, higher values reduce I/O pressure.
	// Default: 5 ticks
	StorageSyncDelay int

	// LogCompactionMinEntries is the minimum number of entries that must exist
	// in the log before considering log compaction.
	// Default: 5000 entries
	LogCompactionMinEntries int

	// ApplyTickCount is how often (in ticks) to check for and apply committed entries.
	// Default: 1 tick
	ApplyTickCount int
}

// DefaultRaftOptions returns a RaftOptions instance initialized with default values.
func DefaultRaftOptions() RaftOptions {
	return RaftOptions{
		ElectionTickCount:           10,
		HeartbeatTickCount:          1,
		ElectionRandomizationFactor: 0.2,
		MaxLogEntriesPerRequest:     100,
		SnapshotThreshold:           10000,
		StorageSyncDelay:            5,
		LogCompactionMinEntries:     5000,
		ApplyTickCount:              1,
	}
}

// WithElectionTicks creates a copy of RaftOptions with the specified election tick count.
func (o RaftOptions) WithElectionTicks(ticks int) RaftOptions {
	if ticks <= 0 {
		panic("ElectionTickCount must be positive")
	}
	o.ElectionTickCount = ticks
	return o
}

// WithHeartbeatTicks creates a copy of RaftOptions with the specified heartbeat tick count.
func (o RaftOptions) WithHeartbeatTicks(ticks int) RaftOptions {
	if ticks <= 0 {
		panic("HeartbeatTickCount must be positive")
	}
	o.HeartbeatTickCount = ticks
	return o
}

// WithRandomizationFactor creates a copy of RaftOptions with the specified election randomization factor.
func (o RaftOptions) WithRandomizationFactor(factor float64) RaftOptions {
	if factor < 0 {
		panic("ElectionRandomizationFactor must be non-negative")
	}
	o.ElectionRandomizationFactor = factor
	return o
}

// WithMaxEntriesPerRequest creates a copy of RaftOptions with the specified max entries per request.
func (o RaftOptions) WithMaxEntriesPerRequest(max int) RaftOptions {
	if max <= 0 {
		panic("MaxLogEntriesPerRequest must be positive")
	}
	o.MaxLogEntriesPerRequest = max
	return o
}

// WithSnapshotThreshold creates a copy of RaftOptions with the specified snapshot threshold.
func (o RaftOptions) WithSnapshotThreshold(threshold int) RaftOptions {
	if threshold <= 0 {
		panic("SnapshotThreshold must be positive")
	}
	o.SnapshotThreshold = threshold
	return o
}

// WithStorageSyncDelay creates a copy of RaftOptions with the specified storage sync delay.
func (o RaftOptions) WithStorageSyncDelay(delay int) RaftOptions {
	if delay <= 0 {
		panic("StorageSyncDelay must be positive")
	}
	o.StorageSyncDelay = delay
	return o
}

// WithLogCompactionMinEntries creates a copy of RaftOptions with the specified log compaction minimum entries.
func (o RaftOptions) WithLogCompactionMinEntries(min int) RaftOptions {
	if min <= 0 {
		panic("LogCompactionMinEntries must be positive")
	}
	o.LogCompactionMinEntries = min
	return o
}

// WithApplyTickCount creates a copy of RaftOptions with the specified apply tick count.
func (o RaftOptions) WithApplyTickCount(ticks int) RaftOptions {
	if ticks <= 0 {
		panic("ApplyTickCount must be positive")
	}
	o.ApplyTickCount = ticks
	return o
}

// Validate checks if the options are valid and returns an error if not.
func (o RaftOptions) Validate() error {
	if o.HeartbeatTickCount <= 0 {
		return fmt.Errorf("HeartbeatTickCount must be positive, got %d", o.HeartbeatTickCount)
	}
	minElectionTicks := o.HeartbeatTickCount * 3
	if o.ElectionTickCount < minElectionTicks {
		return fmt.Errorf("ElectionTickCount (%d) should be at least 3x HeartbeatTickCount (%d)",
			o.ElectionTickCount, o.HeartbeatTickCount)
	}
	if o.ElectionRandomizationFactor < 0 || o.ElectionRandomizationFactor > 1 {
		return fmt.Errorf("ElectionRandomizationFactor must be in [0.0, 1.0], got %f", o.ElectionRandomizationFactor)
	}
	if o.MaxLogEntriesPerRequest <= 0 {
		return fmt.Errorf("MaxLogEntriesPerRequest must be positive, got %d", o.MaxLogEntriesPerRequest)
	}
	if o.SnapshotThreshold < 1000 {
		return fmt.Errorf("SnapshotThreshold (%d) is very low; may lead to frequent snapshots", o.SnapshotThreshold)
	}
	if o.StorageSyncDelay <= 0 {
		return fmt.Errorf("StorageSyncDelay must be positive, got %d", o.StorageSyncDelay)
	}
	if o.StorageSyncDelay > 100 {
		return fmt.Errorf("StorageSyncDelay (%d) is unusually high; it may risk durability", o.StorageSyncDelay)
	}
	if o.LogCompactionMinEntries <= 0 {
		return fmt.Errorf("LogCompactionMinEntries must be positive, got %d", o.LogCompactionMinEntries)
	}
	if o.ApplyTickCount <= 0 {
		return fmt.Errorf("ApplyTickCount must be positive, got %d", o.ApplyTickCount)
	}

	// Ensure heartbeat interval is less than election timeout
	if o.HeartbeatTickCount >= o.ElectionTickCount {
		return fmt.Errorf("HeartbeatTickCount (%d) must be less than ElectionTickCount (%d)",
			o.HeartbeatTickCount, o.ElectionTickCount)
	}

	return nil
}
