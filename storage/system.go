package storage

import (
	"os"
	"time"
)

// systemInfo defines an interface for retrieving basic process and system information.
type systemInfo interface {
	// PID returns the current process ID.
	PID() int

	// Hostname returns the hostname of the machine.
	// Returns "unknown" if the hostname cannot be determined.
	Hostname() string

	// NowUnixMilli returns the current Unix time in milliseconds.
	NowUnixMilli() int64
}

// HostnameResolver is a function type for hostname resolution.
type hostnameResolver func() (string, error)

// defaultSystemInfo provides a default implementation of the systemInfo interface
// using OS-specific calls.
type defaultSystemInfo struct {
	hostnameResolver hostnameResolver
}

// NewSystemInfo creates a new SystemInfo instance with the default OS implementation.
func NewSystemInfo() systemInfo {
	return NewSystemInfoWithResolver(os.Hostname)
}

// NewSystemInfoWithResolver creates a new SystemInfo instance with a custom hostname resolver.
// This is primarily useful for testing or specialized environments.
func NewSystemInfoWithResolver(resolver hostnameResolver) systemInfo {
	if resolver == nil {
		resolver = os.Hostname
	}

	return &defaultSystemInfo{
		hostnameResolver: resolver,
	}
}

// PID returns the current process ID.
func (d *defaultSystemInfo) PID() int {
	return os.Getpid()
}

// Hostname returns the system's hostname.
// If hostname resolution fails, it returns "unknown".
func (d *defaultSystemInfo) Hostname() string {
	hostname, err := d.hostnameResolver()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// NowUnixMilli returns the current time in milliseconds since the Unix epoch.
func (d *defaultSystemInfo) NowUnixMilli() int64 {
	return time.Now().UnixMilli()
}
