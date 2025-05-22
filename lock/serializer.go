package lock

import (
	"encoding/json"

	"github.com/jathurchan/raftlock/types"
)

// serializer defines the interface for encoding and decoding data.
type serializer interface {
	// DecodeCommand unmarshals a byte slice into a types.Command.
	DecodeCommand(data []byte) (types.Command, error)

	// EncodeSnapshot serializes a lockSnapshot into a byte slice.
	EncodeSnapshot(snapshot lockSnapshot) ([]byte, error)

	// DecodeSnapshot deserializes a byte slice into a lockSnapshot.
	DecodeSnapshot(data []byte) (lockSnapshot, error)
}

// jsonSerializer implements the serializer interface using JSON encoding.
type jsonSerializer struct{}

// DecodeCommand unmarshals a Raft command.
func (s *jsonSerializer) DecodeCommand(cmdData []byte) (types.Command, error) {
	var cmd types.Command
	err := json.Unmarshal(cmdData, &cmd)
	return cmd, err
}

// EncodeSnapshot marshals a snapshot of the lock manager state.
func (s *jsonSerializer) EncodeSnapshot(snapshot lockSnapshot) ([]byte, error) {
	return json.Marshal(snapshot)
}

// DecodeSnapshot deserializes a byte slice into a lockSnapshot.
func (s *jsonSerializer) DecodeSnapshot(data []byte) (lockSnapshot, error) {
	var snapshot lockSnapshot
	err := json.Unmarshal(data, &snapshot)
	return snapshot, err
}
