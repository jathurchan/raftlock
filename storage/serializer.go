package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/jathurchan/raftlock/types"
)

// serializer defines methods for encoding and decoding log entries, metadata,
// persistent state, and snapshot metadata.
type serializer interface {
	// MarshalLogEntry serializes a LogEntry into a byte slice.
	MarshalLogEntry(entry types.LogEntry) ([]byte, error)

	// UnmarshalLogEntry deserializes a byte slice into a LogEntry.
	UnmarshalLogEntry(data []byte) (types.LogEntry, error)

	// MarshalMetadata serializes log metadata into a byte slice.
	MarshalMetadata(metadata logMetadata) ([]byte, error)

	// UnmarshalMetadata deserializes a byte slice into log metadata.
	UnmarshalMetadata(data []byte) (logMetadata, error)

	// MarshalState serializes the persistent state of the node into a byte slice.
	MarshalState(state types.PersistentState) ([]byte, error)

	// UnmarshalState deserializes a byte slice into the node’s persistent state.
	UnmarshalState(data []byte) (types.PersistentState, error)

	// MarshalSnapshotMetadata serializes snapshot metadata into a byte slice.
	MarshalSnapshotMetadata(metadata types.SnapshotMetadata) ([]byte, error)

	// UnmarshalSnapshotMetadata deserializes a byte slice into snapshot metadata.
	UnmarshalSnapshotMetadata(data []byte) (types.SnapshotMetadata, error)
}

// jsonSerializer implements Serializer using JSON encoding
type jsonSerializer struct{}

// MarshalLogEntry serializes a log entry using JSON
func (s jsonSerializer) MarshalLogEntry(entry types.LogEntry) ([]byte, error) {
	return json.Marshal(entry)
}

// UnmarshalLogEntry deserializes a log entry using JSON
func (s jsonSerializer) UnmarshalLogEntry(data []byte) (types.LogEntry, error) {
	var entry types.LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return types.LogEntry{}, err
	}
	return entry, nil
}

// MarshalMetadata serializes metadata using JSON
func (s jsonSerializer) MarshalMetadata(metadata logMetadata) ([]byte, error) {
	return json.Marshal(metadata)
}

// UnmarshalMetadata deserializes metadata using JSON
func (s jsonSerializer) UnmarshalMetadata(data []byte) (logMetadata, error) {
	var metadata logMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return logMetadata{}, err
	}
	return metadata, nil
}

// MarshalState serializes persistent state using JSON
func (s jsonSerializer) MarshalState(state types.PersistentState) ([]byte, error) {
	return json.Marshal(state)
}

// UnmarshalState deserializes persistent state using JSON
func (s jsonSerializer) UnmarshalState(data []byte) (types.PersistentState, error) {
	var state types.PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return types.PersistentState{}, err
	}
	return state, nil
}

// MarshalSnapshotMetadata serializes snapshot metadata using JSON
func (s jsonSerializer) MarshalSnapshotMetadata(metadata types.SnapshotMetadata) ([]byte, error) {
	return json.Marshal(metadata)
}

// UnmarshalSnapshotMetadata deserializes snapshot metadata using JSON
func (s jsonSerializer) UnmarshalSnapshotMetadata(data []byte) (types.SnapshotMetadata, error) {
	var metadata types.SnapshotMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return types.SnapshotMetadata{}, err
	}
	return metadata, nil
}

// binarySerializer implements Serializer using binary encoding for log entries
// and JSON for other objects
type binarySerializer struct {
	jsonSerializer // Embed JSON serializer for non-log entry objects
}

// MarshalLogEntry serializes a log entry into binary format
func (s binarySerializer) MarshalLogEntry(entry types.LogEntry) ([]byte, error) {
	// Get command length
	commandLen := len(entry.Command)

	// Calculate total size: header + command
	totalSize := headerSize + commandLen

	// Allocate buffer
	data := make([]byte, totalSize)

	// Write index
	binary.BigEndian.PutUint64(data[0:indexSize], uint64(entry.Index))

	// Write term
	binary.BigEndian.PutUint64(data[indexSize:indexSize+termSize], uint64(entry.Term))

	// Write command length
	binary.BigEndian.PutUint64(
		data[indexSize+termSize:headerSize],
		uint64(commandLen))

	// Write command data
	copy(data[headerSize:], entry.Command)

	return data, nil
}

// UnmarshalLogEntry deserializes a log entry from binary format
func (s binarySerializer) UnmarshalLogEntry(data []byte) (types.LogEntry, error) {
	if len(data) < headerSize {
		return types.LogEntry{}, fmt.Errorf("data too short for binary log entry")
	}

	// Extract header fields
	index := types.Index(binary.BigEndian.Uint64(data[0:indexSize]))
	term := types.Term(binary.BigEndian.Uint64(data[indexSize : indexSize+termSize]))
	commandLen := binary.BigEndian.Uint64(data[indexSize+termSize : headerSize])

	// Validate command length
	if uint64(len(data)-headerSize) != commandLen {
		return types.LogEntry{}, fmt.Errorf(
			"command length mismatch: header says %d, actual is %d",
			commandLen, len(data)-headerSize)
	}

	// Extract command
	command := make([]byte, commandLen)
	copy(command, data[headerSize:])

	return types.LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}, nil
}

// newJsonSerializer creates a new JsonSerializer
func newJsonSerializer() jsonSerializer {
	return jsonSerializer{}
}

// newBinarySerializer creates a new BinarySerializer
func newBinarySerializer() binarySerializer {
	return binarySerializer{}
}
