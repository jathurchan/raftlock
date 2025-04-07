package storage

import "fmt"

// storageStatus represents the current status of the storage
type storageStatus int

const (
	// storageStatusUnknown is the initial status
	storageStatusUnknown storageStatus = iota

	// storageStatusInitializing indicates the storage is initializing
	storageStatusInitializing

	// storageStatusRecovering indicates the storage is in recovery mode
	storageStatusRecovering

	// storageStatusReady indicates the storage is ready for use
	storageStatusReady

	// storageStatusCorrupted indicates the storage has detected corruption
	storageStatusCorrupted

	// storageStatusClosed indicates the storage has been closed
	storageStatusClosed
)

// string returns a string representation of the storage status
func (s storageStatus) string() string {
	switch s {
	case storageStatusUnknown:
		return "Unknown"
	case storageStatusInitializing:
		return "Initializing"
	case storageStatusRecovering:
		return "Recovering"
	case storageStatusReady:
		return "Ready"
	case storageStatusCorrupted:
		return "Corrupted"
	case storageStatusClosed:
		return "Closed"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}
