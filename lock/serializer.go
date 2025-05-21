package lock

import (
	"encoding/json"

	"github.com/jathurchan/raftlock/types"
)

// serializer defines the interface for encoding and decoding data.
type serializer interface {
	// Decode converts the raw byte slice into a types.Command.
	// It returns an error if the data is malformed or invalid.
	Decode(cmdData []byte) (types.Command, error)
}

// jsonSerializer implements the serializer interface using JSON encoding.
type jsonSerializer struct{}

// Decode unmarshals a JSON-encoded byte slice into a types.Command object.
func (s *jsonSerializer) Decode(cmdData []byte) (types.Command, error) {
	var cmd types.Command
	err := json.Unmarshal(cmdData, &cmd)
	return cmd, err
}
