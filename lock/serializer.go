package lock

import (
	"encoding/json"

	"github.com/jathurchan/raftlock/types"
)

// Serializer defines the interface for encoding and decoding data.
type Serializer interface {
	// EncodeCommand marshals a types.Command into a byte slice.
	EncodeCommand(cmd types.Command) ([]byte, error)

	// DecodeCommand unmarshals a byte slice into a types.Command.
	DecodeCommand(data []byte) (types.Command, error)

	// EncodeSnapshot serializes a lockSnapshot into a byte slice.
	EncodeSnapshot(snapshot lockSnapshot) ([]byte, error)

	// DecodeSnapshot deserializes a byte slice into a lockSnapshot.
	DecodeSnapshot(data []byte) (lockSnapshot, error)
}

// JSONSerializer implements the serializer interface using JSON encoding.
type JSONSerializer struct{}

// EncodeCommand marshals a Raft command.
func (s *JSONSerializer) EncodeCommand(cmd types.Command) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeCommand unmarshals a Raft command.
func (s *JSONSerializer) DecodeCommand(cmdData []byte) (types.Command, error) {
	var cmd types.Command
	err := json.Unmarshal(cmdData, &cmd)
	return cmd, err
}

// EncodeSnapshot marshals a snapshot of the lock manager state.
func (s *JSONSerializer) EncodeSnapshot(snapshot lockSnapshot) ([]byte, error) {
	return json.Marshal(snapshot)
}

// DecodeSnapshot deserializes a byte slice into a lockSnapshot.
func (s *JSONSerializer) DecodeSnapshot(data []byte) (lockSnapshot, error) {
	var snapshot lockSnapshot
	err := json.Unmarshal(data, &snapshot)
	return snapshot, err
}
