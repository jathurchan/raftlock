package lock

import (
	"container/heap"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func TestLockExpiration_ExpirationHeapBasicOperations(t *testing.T) {
	h := &expirationHeap{}
	heap.Init(h)

	testutil.AssertEqual(t, 0, h.Len(), "Empty heap should have length 0")

	now := time.Now()
	item1 := &expirationItem{lockID: "lock1", expiresAt: now.Add(3 * time.Minute)}
	item2 := &expirationItem{lockID: "lock2", expiresAt: now.Add(1 * time.Minute)}
	item3 := &expirationItem{lockID: "lock3", expiresAt: now.Add(2 * time.Minute)}

	heap.Push(h, item1)
	heap.Push(h, item2)
	heap.Push(h, item3)

	testutil.AssertEqual(t, 3, h.Len(), "Heap should have 3 items")

	popped := heap.Pop(h).(*expirationItem)
	testutil.AssertEqual(
		t,
		types.LockID("lock2"),
		popped.lockID,
		"First item should be the one with earliest expiry",
	)
	testutil.AssertEqual(t, -1, popped.index, "Popped item should have index = -1")

	popped = heap.Pop(h).(*expirationItem)
	testutil.AssertEqual(
		t,
		types.LockID("lock3"),
		popped.lockID,
		"Second item should have the second earliest expiry",
	)

	popped = heap.Pop(h).(*expirationItem)
	testutil.AssertEqual(
		t,
		types.LockID("lock1"),
		popped.lockID,
		"Last item should have the latest expiry",
	)

	testutil.AssertEqual(t, 0, h.Len(), "Heap should be empty after all pops")
}

func TestLockExpiration_ExpirationHeapUpdateOperation(t *testing.T) {
	now := time.Now()
	h := &expirationHeap{}
	heap.Init(h)

	item1 := &expirationItem{lockID: "lock1", expiresAt: now.Add(3 * time.Minute)}
	item2 := &expirationItem{lockID: "lock2", expiresAt: now.Add(1 * time.Minute)}
	item3 := &expirationItem{lockID: "lock3", expiresAt: now.Add(2 * time.Minute)}

	heap.Push(h, item1)
	heap.Push(h, item2)
	heap.Push(h, item3)

	for i, item := range *h {
		if item.lockID == "lock3" {
			item.expiresAt = now.Add(30 * time.Second)
			heap.Fix(h, i)
			break
		}
	}

	popped := heap.Pop(h).(*expirationItem)
	testutil.AssertEqual(
		t,
		types.LockID("lock3"),
		popped.lockID,
		"Updated lock3 should now be first to expire",
	)

	popped = heap.Pop(h).(*expirationItem)
	testutil.AssertEqual(t, types.LockID("lock2"), popped.lockID, "lock2 should be next")

	popped = heap.Pop(h).(*expirationItem)
	testutil.AssertEqual(t, types.LockID("lock1"), popped.lockID, "lock1 should be last")
}

func TestLockExpiration_ExpirationHeapIndexTracking(t *testing.T) {
	h := &expirationHeap{}

	item1 := &expirationItem{lockID: "lock1", expiresAt: time.Now().Add(time.Minute)}
	item2 := &expirationItem{lockID: "lock2", expiresAt: time.Now().Add(2 * time.Minute)}

	h.Push(item1)
	testutil.AssertEqual(t, 0, item1.index, "First item should have index 0")

	h.Push(item2)
	testutil.AssertEqual(t, 1, item2.index, "Second item should have index 1")

	h.Swap(0, 1)
	testutil.AssertEqual(t, 1, item1.index, "After swap, first item should have index 1")
	testutil.AssertEqual(t, 0, item2.index, "After swap, second item should have index 0")

	popped := h.Pop().(*expirationItem)
	testutil.AssertEqual(t, -1, popped.index, "Popped item should have index -1")
}
