package storage

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestSystemInfo(t *testing.T) {
	// Test default constructor
	t.Run("NewSystemInfo", func(t *testing.T) {
		info := NewSystemInfo()
		if info == nil {
			t.Fatal("NewSystemInfo() returned nil")
		}

		// Verify implementation type
		_, ok := info.(*defaultSystemInfo)
		if !ok {
			t.Errorf("Wrong implementation type: got %T, want *defaultSystemInfo", info)
		}
	})

	// Test constructor with custom resolver
	t.Run("NewSystemInfoWithResolver", func(t *testing.T) {
		// Test with custom resolver
		customResolver := func() (string, error) {
			return "test-host", nil
		}
		info := NewSystemInfoWithResolver(customResolver)
		if info == nil {
			t.Fatal("NewSystemInfoWithResolver(custom) returned nil")
		}

		// Test with nil resolver (should default to os.Hostname)
		nilInfo := NewSystemInfoWithResolver(nil)
		if nilInfo == nil {
			t.Fatal("NewSystemInfoWithResolver(nil) returned nil")
		}
	})

	// Test PID functionality
	t.Run("PID", func(t *testing.T) {
		info := NewSystemInfo()
		pid := info.PID()

		// PID should be positive
		if pid <= 0 {
			t.Errorf("PID() returned non-positive value: %d", pid)
		}

		// PID should match os.Getpid()
		expectedPid := os.Getpid()
		if pid != expectedPid {
			t.Errorf("PID() = %d, want %d", pid, expectedPid)
		}
	})

	// Test Hostname functionality
	t.Run("Hostname", func(t *testing.T) {
		// Test successful hostname resolution
		t.Run("Success", func(t *testing.T) {
			expectedHostname := "test-hostname"
			info := NewSystemInfoWithResolver(func() (string, error) {
				return expectedHostname, nil
			})

			hostname := info.Hostname()
			if hostname != expectedHostname {
				t.Errorf("Hostname() = %q, want %q", hostname, expectedHostname)
			}
		})

		// Test hostname resolution failure
		t.Run("Failure", func(t *testing.T) {
			info := NewSystemInfoWithResolver(func() (string, error) {
				return "", errors.New("hostname error")
			})

			hostname := info.Hostname()
			if hostname != "unknown" {
				t.Errorf("Hostname() on error = %q, want \"unknown\"", hostname)
			}
		})
	})

	// Test timestamp functionality
	t.Run("NowUnixMilli", func(t *testing.T) {
		info := NewSystemInfo()

		// Get time before and after the call
		before := time.Now().UnixMilli()
		ts := info.NowUnixMilli()
		after := time.Now().Add(5 * time.Millisecond).UnixMilli() // Allow small buffer

		// Timestamp should be positive
		if ts <= 0 {
			t.Errorf("NowUnixMilli() returned non-positive value: %d", ts)
		}

		// Timestamp should be within the expected range
		if ts < before || ts > after {
			t.Errorf("NowUnixMilli() = %d, want value between %d and %d", ts, before, after)
		}

		// Subsequent call should return a greater or equal value
		ts2 := info.NowUnixMilli()
		if ts2 < ts {
			t.Errorf("Second NowUnixMilli() call = %d, which is less than first call = %d", ts2, ts)
		}
	})
}
