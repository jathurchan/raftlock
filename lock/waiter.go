package lock

import (
	"time"

	"github.com/jathurchan/raftlock/types"
)

// waiter represents a client that is waiting to acquire a lock.
type waiter struct {
	clientID  types.ClientID // Unique identifier of the client.
	enqueued  time.Time      // Timestamp when the client was added to the wait queue.
	timeoutAt time.Time      // Deadline after which the wait attempt is considered timed out.
	priority  int            // Priority of the waiter; higher values indicate higher priority.
	version   types.Index    // Raft log index at which the wait was registered.
	index     int            // Position of the waiter in the heap (used by heap.Interface).
	notifyCh  chan struct{}  // Channel used to signal the client when the lock is acquired, cancelled, or timed out.
}

// waitQueue is a priority queue for waiters requesting a lock.
// Waiters are ordered by priority (higher first), then by enqueue time (earlier first).
type waitQueue []*waiter

// Len returns the number of waiters in the queue.
func (wq waitQueue) Len() int { return len(wq) }

// Less determines the ordering of two waiters.
// Higher-priority waiters come first; among equal priorities, earlier enqueue time wins.
func (wq waitQueue) Less(i, j int) bool {
	if wq[i].priority != wq[j].priority {
		return wq[i].priority > wq[j].priority
	}
	return wq[i].enqueued.Before(wq[j].enqueued)
}

// Swap swaps two waiters in the queue and updates their index fields.
func (wq waitQueue) Swap(i, j int) {
	wq[i], wq[j] = wq[j], wq[i]
	wq[i].index = i
	wq[j].index = j
}

// Push adds a new waiter to the priority queue and assigns its index.
func (wq *waitQueue) Push(x interface{}) {
	n := len(*wq)
	item := x.(*waiter)
	item.index = n
	*wq = append(*wq, item)
}

// Pop removes and returns the last waiter in the queue.
// Also clears references to help with garbage collection.
func (wq *waitQueue) Pop() any {
	old := *wq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak.
	item.index = -1 // Mark as removed.
	*wq = old[0 : n-1]
	return item
}
