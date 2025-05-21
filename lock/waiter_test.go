package lock

import (
	"container/heap"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/types"
)

func TestLockWaiter_WaiterQueue(t *testing.T) {
	wq := waitQueue{}

	if wq.Len() != 0 {
		t.Errorf("Empty queue should have length 0, got %d", wq.Len())
	}

	w1 := &waiter{clientID: "client1", index: 0}
	w2 := &waiter{clientID: "client2", index: 1}

	wq = waitQueue{w1, w2}

	if wq.Len() != 2 {
		t.Errorf("Queue length should be 2, got %d", wq.Len())
	}

	wq.Swap(0, 1)
	if wq[0] != w2 || wq[1] != w1 {
		t.Errorf("Swap failed to exchange elements")
	}
	if w1.index != 1 || w2.index != 0 {
		t.Errorf("Swap failed to update indices")
	}

	w3 := &waiter{clientID: "client3"}
	wq.Push(w3)
	if wq.Len() != 3 || wq[2] != w3 || w3.index != 2 {
		t.Errorf("Push failed to add element")
	}

	popped := wq.Pop()
	if popped != w3 || w3.index != -1 || wq.Len() != 2 {
		t.Errorf("Pop failed to remove correct element")
	}
}

func TestLockWaiter_WaitQueuePriority(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		queue    waitQueue
		i        int
		j        int
		expected bool
	}{
		{
			name: "Higher priority before lower",
			queue: waitQueue{
				&waiter{priority: 10},
				&waiter{priority: 5},
			},
			i: 0, j: 1, expected: true,
		},
		{
			name: "Same priority, earlier enqueue before later",
			queue: waitQueue{
				&waiter{priority: 5, enqueued: now.Add(-time.Hour)},
				&waiter{priority: 5, enqueued: now},
			},
			i: 0, j: 1, expected: true,
		},
		{
			name: "Lower priority after higher, regardless of time",
			queue: waitQueue{
				&waiter{priority: 5, enqueued: now.Add(-time.Hour)},
				&waiter{priority: 10, enqueued: now},
			},
			i: 0, j: 1, expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.queue.Less(tt.i, tt.j); result != tt.expected {
				t.Errorf("waitQueue.Less(%d, %d) = %v, want %v", tt.i, tt.j, result, tt.expected)
			}
		})
	}
}

func TestLockWaiter_HeapOperations(t *testing.T) {
	wq := &waitQueue{}
	heap.Init(wq)

	now := time.Now()
	heap.Push(wq, &waiter{clientID: "low", priority: 1, enqueued: now})
	heap.Push(wq, &waiter{clientID: "high", priority: 10, enqueued: now})
	heap.Push(wq, &waiter{clientID: "medium", priority: 5, enqueued: now})

	expected := []types.ClientID{"high", "medium", "low"}
	for i, exp := range expected {
		if wq.Len() == 0 {
			t.Fatalf("Queue empty after %d pops, expected %d items", i, len(expected))
		}

		got := heap.Pop(wq).(*waiter)
		if got.clientID != exp {
			t.Errorf("Pop %d: got %s, want %s", i, got.clientID, exp)
		}
	}

	if wq.Len() != 0 {
		t.Errorf("Queue should be empty after all pops")
	}
}
