package storage

import "testing"

func TestStorageStatus_String(t *testing.T) {
	tests := []struct {
		name   string
		status storageStatus
		want   string
	}{
		{
			name:   "Unknown Status",
			status: storageStatusUnknown,
			want:   "Unknown",
		},
		{
			name:   "Initializing Status",
			status: storageStatusInitializing,
			want:   "Initializing",
		},
		{
			name:   "Recovering Status",
			status: storageStatusRecovering,
			want:   "Recovering",
		},
		{
			name:   "Ready Status",
			status: storageStatusReady,
			want:   "Ready",
		},
		{
			name:   "Corrupted Status",
			status: storageStatusCorrupted,
			want:   "Corrupted",
		},
		{
			name:   "Closed Status",
			status: storageStatusClosed,
			want:   "Closed",
		},
		{
			name:   "Invalid Status",
			status: storageStatus(99), // An invalid status value
			want:   "Status(99)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.string(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
