package lock

import (
	"testing"
	"time"

	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func TestLockSerializer_Json_EncodeCommand(t *testing.T) {
	tests := []struct {
		name      string
		command   types.Command
		expectErr bool
	}{
		{
			name: "acquire command with all fields",
			command: types.Command{
				Op:          types.OperationAcquire,
				LockID:      "lock1",
				ClientID:    "client1",
				TTL:         30000,
				Version:     42,
				Priority:    10,
				Timeout:     5000,
				Wait:        true,
				WaitTimeout: 60000,
				RequestID:   "req123",
				Metadata:    map[string]string{"key": "value", "env": "test"},
			},
			expectErr: false,
		},
		{
			name: "release command minimal fields",
			command: types.Command{
				Op:       types.OperationRelease,
				LockID:   "lock2",
				ClientID: "client2",
				Version:  100,
			},
			expectErr: false,
		},
		{
			name: "renew command",
			command: types.Command{
				Op:       types.OperationRenew,
				LockID:   "lock3",
				ClientID: "client3",
				TTL:      45000,
				Version:  200,
			},
			expectErr: false,
		},
		{
			name: "enqueue waiter command",
			command: types.Command{
				Op:          types.OperationEnqueueWaiter,
				LockID:      "lock4",
				ClientID:    "client4",
				Priority:    5,
				WaitTimeout: 30000,
			},
			expectErr: false,
		},
		{
			name: "cancel wait command",
			command: types.Command{
				Op:       types.OperationCancelWait,
				LockID:   "lock5",
				ClientID: "client5",
			},
			expectErr: false,
		},
		{
			name: "command with empty strings",
			command: types.Command{
				Op:       types.OperationAcquire,
				LockID:   "",
				ClientID: "",
				TTL:      0,
			},
			expectErr: false,
		},
	}

	s := &JSONSerializer{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := s.EncodeCommand(tc.command)

			if tc.expectErr {
				testutil.AssertError(t, err, "Expected an error encoding command")
			} else {
				testutil.AssertNoError(t, err, "Unexpected error encoding command")
				testutil.AssertTrue(t, len(data) > 0, "Encoded data should not be empty")

				decoded, decodeErr := s.DecodeCommand(data)
				testutil.AssertNoError(t, decodeErr, "Failed to decode encoded command")
				testutil.AssertEqual(t, tc.command, decoded, "Round-trip encoding/decoding failed")
			}
		})
	}
}

func TestLockSerializer_Json_DecodeCommand(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		expected  types.Command
		expectErr bool
	}{
		{
			name: "valid acquire command",
			input: []byte(
				`{"op":"acquire","lock_id":"lock1","client_id":"client1","ttl":30000,"version":42}`,
			),
			expected: types.Command{
				Op:       types.OperationAcquire,
				LockID:   "lock1",
				ClientID: "client1",
				TTL:      30000,
				Version:  42,
			},
			expectErr: false,
		},
		{
			name: "valid release command",
			input: []byte(
				`{"op":"release","lock_id":"lock1","client_id":"client1","version":42}`,
			),
			expected: types.Command{
				Op:       types.OperationRelease,
				LockID:   "lock1",
				ClientID: "client1",
				Version:  42,
			},
			expectErr: false,
		},
		{
			name: "valid renew command with priority",
			input: []byte(
				`{"op":"renew","lock_id":"lock1","client_id":"client1","ttl":60000,"version":42,"priority":10}`,
			),
			expected: types.Command{
				Op:       types.OperationRenew,
				LockID:   "lock1",
				ClientID: "client1",
				TTL:      60000,
				Version:  42,
				Priority: 10,
			},
			expectErr: false,
		},
		{
			name:      "invalid json",
			input:     []byte(`{"op":"acquire","lock_id":}`), // Malformed JSON
			expectErr: true,
		},
		{
			name:      "empty data",
			input:     []byte{},
			expectErr: true,
		},
	}

	s := &JSONSerializer{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := s.DecodeCommand(tc.input)

			if tc.expectErr {
				testutil.AssertError(t, err, "Expected an error for invalid input")
			} else {
				testutil.AssertNoError(t, err, "Unexpected error decoding valid command")
				testutil.AssertEqual(t, tc.expected, cmd, "Decoded command does not match expected")
			}
		})
	}
}

func TestLockSerializer_Json_EncodeSnapshot(t *testing.T) {
	now := time.Now().Round(time.Millisecond)
	snapshot := lockSnapshot{
		LastAppliedIndex: 42,
		Locks: map[types.LockID]*lockState{
			"lock1": {
				LockID:       "lock1",
				Owner:        "client1",
				Version:      42,
				AcquiredAt:   now,
				ExpiresAt:    now.Add(30 * time.Second),
				Metadata:     map[string]string{"key": "value"},
				LastModified: now,
			},
		},
		Waiters: map[types.LockID][]*waiter{
			"lock2": {
				{
					clientID:  "client2",
					enqueued:  now,
					timeoutAt: now.Add(60 * time.Second),
					priority:  10,
					version:   43,
					index:     0,
				},
			},
		},
		Version: 1,
	}

	s := &JSONSerializer{}

	data, err := s.EncodeSnapshot(snapshot)
	testutil.AssertNoError(t, err, "Failed to encode snapshot")
	testutil.AssertTrue(t, len(data) > 0, "Encoded data should not be empty")

	decoded, err := s.DecodeSnapshot(data)
	testutil.AssertNoError(t, err, "Failed to decode the encoded snapshot")

	testutil.AssertEqual(
		t,
		snapshot.LastAppliedIndex,
		decoded.LastAppliedIndex,
		"LastAppliedIndex mismatch",
	)
	testutil.AssertEqual(t, snapshot.Version, decoded.Version, "Version mismatch")

	testutil.AssertEqual(t, len(snapshot.Locks), len(decoded.Locks), "Number of locks mismatch")
	for id, lock := range snapshot.Locks {
		decodedLock, ok := decoded.Locks[id]
		testutil.AssertTrue(t, ok, "Lock %s not found in decoded snapshot", id)
		testutil.AssertEqual(t, lock.LockID, decodedLock.LockID, "Lock ID mismatch")
		testutil.AssertEqual(t, lock.Owner, decodedLock.Owner, "Lock owner mismatch")
		testutil.AssertEqual(t, lock.Version, decodedLock.Version, "Lock version mismatch")
		testutil.AssertEqual(
			t,
			lock.Metadata["key"],
			decodedLock.Metadata["key"],
			"Lock metadata mismatch",
		)
	}
}

func TestJsonSerializer_DecodeSnapshot(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		expectErr bool
	}{
		{
			name: "valid snapshot",
			input: []byte(
				`{"last_applied_index":42,"locks":{"lock1":{"lockID":"lock1","owner":"client1","version":42,"metadata":{}}},"waiters":{},"version":1}`,
			),
			expectErr: false,
		},
		{
			name:      "invalid json",
			input:     []byte(`{"last_applied_index":42,"locks":}`), // Malformed JSON
			expectErr: true,
		},
		{
			name:      "empty data",
			input:     []byte{},
			expectErr: true,
		},
	}

	s := &JSONSerializer{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := s.DecodeSnapshot(tc.input)

			if tc.expectErr {
				testutil.AssertError(t, err, "Expected an error for invalid input")
			} else {
				testutil.AssertNoError(t, err, "Unexpected error decoding valid snapshot")

				if !tc.expectErr {
					testutil.AssertEqual(t, types.Index(42), snapshot.LastAppliedIndex, "LastAppliedIndex mismatch")
					testutil.AssertEqual(t, 1, snapshot.Version, "Version mismatch")
					testutil.AssertEqual(t, 1, len(snapshot.Locks), "Expected 1 lock")
					lock, ok := snapshot.Locks["lock1"]
					testutil.AssertTrue(t, ok, "Expected lock 'lock1' to exist")
					testutil.AssertEqual(t, types.LockID("lock1"), lock.LockID, "Lock ID mismatch")
					testutil.AssertEqual(t, types.ClientID("client1"), lock.Owner, "Lock owner mismatch")
				}
			}
		})
	}
}
