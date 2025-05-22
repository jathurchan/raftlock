package lock

import (
	"testing"
	"time"

	"github.com/jathurchan/raftlock/types"
)

func TestLockFilters_FilterByOwner(t *testing.T) {
	clientA := types.ClientID("client-A")
	clientB := types.ClientID("client-B")

	locks := []*types.LockInfo{
		{LockID: "lock-1", OwnerID: clientA},
		{LockID: "lock-2", OwnerID: clientB},
		{LockID: "lock-3", OwnerID: ""},
	}

	filterA := FilterByOwner(clientA)
	if !filterA(locks[0]) || filterA(locks[1]) || filterA(locks[2]) {
		t.Errorf("FilterByOwner(%s) returned incorrect results", clientA)
	}

	emptyFilter := FilterByOwner("")
	if !emptyFilter(locks[2]) || emptyFilter(locks[0]) {
		t.Errorf("FilterByOwner('') returned incorrect results")
	}
}

func TestLockFilters_FilterExpiringSoon(t *testing.T) {
	now := time.Now()

	locks := []*types.LockInfo{
		{LockID: "expired", ExpiresAt: now.Add(-1 * time.Minute)},
		{LockID: "expiring", ExpiresAt: now.Add(5 * time.Second)},
		{LockID: "future", ExpiresAt: now.Add(1 * time.Hour)},
		{LockID: "no-expiry", ExpiresAt: time.Time{}},
	}

	tests := []struct {
		name     string
		window   time.Duration
		expected []bool
	}{
		{"10s window", 10 * time.Second, []bool{true, true, false, false}},
		{"1h window", 1 * time.Hour, []bool{true, true, true, false}},
		{"0s window", 0 * time.Second, []bool{true, false, false, false}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filter := FilterExpiringSoon(tc.window)
			for i, lock := range locks {
				result := filter(lock)
				if result != tc.expected[i] {
					t.Errorf("FilterExpiringSoon(%v) returned %v for lock %s, expected %v",
						tc.window, result, lock.LockID, tc.expected[i])
				}
			}
		})
	}
}

func TestLockFilters_FilterAll(t *testing.T) {
	locks := []*types.LockInfo{
		{},
		{LockID: "held", OwnerID: "client-1"},
		{LockID: "expired", ExpiresAt: time.Now().Add(-1 * time.Hour)},
	}

	for _, lock := range locks {
		if !FilterAll(lock) {
			t.Errorf("FilterAll returned false for lock %v", lock)
		}
	}
}
