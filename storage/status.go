package storage

import "fmt"

// StorageStatus represents the current status of the storage
type StorageStatus int

const (
	// StorageStatusUnknown is the initial status
	StorageStatusUnknown StorageStatus = iota

	// StorageStatusInitializing indicates the storage is initializing
	StorageStatusInitializing

	// StorageStatusRecovering indicates the storage is in recovery mode
	StorageStatusRecovering

	// StorageStatusReady indicates the storage is ready for use
	StorageStatusReady

	// StorageStatusCorrupted indicates the storage has detected corruption
	StorageStatusCorrupted

	// StorageStatusClosed indicates the storage has been closed
	StorageStatusClosed
)

// String returns a string representation of the storage status
func (s StorageStatus) String() string {
	switch s {
	case StorageStatusUnknown:
		return "Unknown"
	case StorageStatusInitializing:
		return "Initializing"
	case StorageStatusRecovering:
		return "Recovering"
	case StorageStatusReady:
		return "Ready"
	case StorageStatusCorrupted:
		return "Corrupted"
	case StorageStatusClosed:
		return "Closed"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}
