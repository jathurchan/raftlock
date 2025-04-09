package storage

import (
	"encoding/binary"
	"testing"

	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

// Sample data for tests
var (
	sampleEntry = types.LogEntry{
		Index:   42,
		Term:    3,
		Command: []byte("test command"),
	}

	sampleMetadata = logMetadata{
		FirstIndex: 10,
		LastIndex:  20,
	}

	sampleState = types.PersistentState{
		CurrentTerm: 5,
		VotedFor:    "node2",
	}

	sampleSnapshotMetadata = types.SnapshotMetadata{
		LastIncludedIndex: 15,
		LastIncludedTerm:  2,
	}
)

// TestJsonSerializer tests all methods of the jsonSerializer
func TestJsonSerializer(t *testing.T) {
	serializer := newJsonSerializer()
	testSerializer(t, serializer)
}

// TestBinarySerializer tests all methods of the binarySerializer
func TestBinarySerializer(t *testing.T) {
	serializer := newBinarySerializer()
	testSerializer(t, serializer)

	// Additional binary-specific tests
	testBinaryLogEntryRoundTrip(t, serializer)
	testBinaryLogEntryInvalidData(t, serializer)
}

// testSerializer tests all serializer methods with both implementations
func testSerializer(t *testing.T, s serializer) {
	// Test LogEntry serialization/deserialization
	data, err := s.MarshalLogEntry(sampleEntry)
	testutil.AssertNoError(t, err, "MarshalLogEntry should not error")

	entry, err := s.UnmarshalLogEntry(data)
	testutil.AssertNoError(t, err, "UnmarshalLogEntry should not error")
	testutil.AssertEqual(t, sampleEntry, entry, "LogEntry should be equal after round trip")

	// Test Metadata serialization/deserialization
	metaData, err := s.MarshalMetadata(sampleMetadata)
	testutil.AssertNoError(t, err, "MarshalMetadata should not error")

	metadata, err := s.UnmarshalMetadata(metaData)
	testutil.AssertNoError(t, err, "UnmarshalMetadata should not error")
	testutil.AssertEqual(t, sampleMetadata, metadata, "Metadata should be equal after round trip")

	// Test State serialization/deserialization
	stateData, err := s.MarshalState(sampleState)
	testutil.AssertNoError(t, err, "MarshalState should not error")

	state, err := s.UnmarshalState(stateData)
	testutil.AssertNoError(t, err, "UnmarshalState should not error")
	testutil.AssertEqual(t, sampleState, state, "State should be equal after round trip")

	// Test SnapshotMetadata serialization/deserialization
	snapshotData, err := s.MarshalSnapshotMetadata(sampleSnapshotMetadata)
	testutil.AssertNoError(t, err, "MarshalSnapshotMetadata should not error")

	snapshot, err := s.UnmarshalSnapshotMetadata(snapshotData)
	testutil.AssertNoError(t, err, "UnmarshalSnapshotMetadata should not error")
	testutil.AssertEqual(t, sampleSnapshotMetadata, snapshot, "SnapshotMetadata should be equal after round trip")
}

// Test invalid JSON for each type in the jsonSerializer
func TestJsonSerializerInvalidData(t *testing.T) {
	s := newJsonSerializer()
	invalidJson := []byte("{invalid json]")

	// Test all unmarshal methods with invalid data
	_, err := s.UnmarshalLogEntry(invalidJson)
	testutil.AssertError(t, err, "UnmarshalLogEntry should error with invalid data")

	_, err = s.UnmarshalMetadata(invalidJson)
	testutil.AssertError(t, err, "UnmarshalMetadata should error with invalid data")

	_, err = s.UnmarshalState(invalidJson)
	testutil.AssertError(t, err, "UnmarshalState should error with invalid data")

	_, err = s.UnmarshalSnapshotMetadata(invalidJson)
	testutil.AssertError(t, err, "UnmarshalSnapshotMetadata should error with invalid data")
}

// Test specific binary serializer edge cases
func testBinaryLogEntryRoundTrip(t *testing.T, s binarySerializer) {
	// Test with empty command
	emptyCommandEntry := types.LogEntry{
		Index:   42,
		Term:    3,
		Command: []byte{},
	}

	data, err := s.MarshalLogEntry(emptyCommandEntry)
	testutil.AssertNoError(t, err, "MarshalLogEntry should not error with empty command")

	entry, err := s.UnmarshalLogEntry(data)
	testutil.AssertNoError(t, err, "UnmarshalLogEntry should not error with empty command")
	testutil.AssertEqual(t, emptyCommandEntry, entry, "Empty command entry should be equal after round trip")

	// Test with large command
	largeCommandEntry := types.LogEntry{
		Index:   99,
		Term:    5,
		Command: make([]byte, 1024), // 1KB command
	}

	data, err = s.MarshalLogEntry(largeCommandEntry)
	testutil.AssertNoError(t, err, "MarshalLogEntry should not error with large command")

	entry, err = s.UnmarshalLogEntry(data)
	testutil.AssertNoError(t, err, "UnmarshalLogEntry should not error with large command")
	testutil.AssertEqual(t, largeCommandEntry, entry, "Large command entry should be equal after round trip")
}

func testBinaryLogEntryInvalidData(t *testing.T, s binarySerializer) {
	// Test with too short data
	tooShortData := make([]byte, headerSize-1)
	_, err := s.UnmarshalLogEntry(tooShortData)
	testutil.AssertError(t, err, "UnmarshalLogEntry should error with too short data")
	testutil.AssertContains(t, err.Error(), "data too short", "Error message should indicate data is too short")

	// Test with inconsistent command length
	inconsistentData := make([]byte, headerSize+10) // Enough space for header + some data
	// Set command length to a value greater than the actual data
	binary.BigEndian.PutUint64(inconsistentData[indexSize+termSize:headerSize], 100)

	_, err = s.UnmarshalLogEntry(inconsistentData)
	testutil.AssertError(t, err, "UnmarshalLogEntry should error with inconsistent command length")
	testutil.AssertContains(t, err.Error(), "command length mismatch", "Error message should indicate length mismatch")
}
