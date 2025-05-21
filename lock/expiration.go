package lock

import (
	"time"

	"github.com/jathurchan/raftlock/types"
)

// expirationItem represents a lock tracked for expiration within the expiration heap.
type expirationItem struct {
	// Identifier of the lock being tracked.
	lockID types.LockID

	// Scheduled expiration time of the lock.
	expiresAt time.Time

	// Position of the item in the heap (used by heap.Interface).
	index int
}

// expirationHeap is a min-heap of expirationItems, sorted by their expiresAt time.
// It allows efficient access to the lock that is closest to expiration.
// Implements `heap.Interface`
type expirationHeap []*expirationItem

// Len returns the number of items in the expiration heap.
func (h expirationHeap) Len() int { return len(h) }

// Less compares two expirationItems and returns true if the item at index i expires before the item at index j.
func (h expirationHeap) Less(i, j int) bool {
	return h[i].expiresAt.Before(h[j].expiresAt)
}

// Swap swaps the expirationItems at indices i and j, and updates their index fields.
func (h expirationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push adds a new expirationItem to the heap and assigns its index.
func (h *expirationHeap) Push(x any) {
	n := len(*h)
	item := x.(*expirationItem)
	item.index = n
	*h = append(*h, item)
}

// Pop removes and returns the expirationItem with the latest expiration time.
// Also clears references to help with garbage collection.
func (h *expirationHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak.
	item.index = -1 // Mark as removed.
	*h = old[0 : n-1]
	return item
}
