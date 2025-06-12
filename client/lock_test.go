package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/testutil"
)

type lockHandleTestSuite struct {
	mockClient *mockLockClient
	handle     LockHandle
	lockID     string
	clientID   string
	ctx        context.Context
}

func setupLockHandleTest(t *testing.T) *lockHandleTestSuite {
	mockClient := &mockLockClient{}
	lockID := "test-lock-id"
	clientID := "test-client-id"

	handle, err := NewLockHandle(mockClient, lockID, clientID)
	testutil.RequireNoError(t, err)

	return &lockHandleTestSuite{
		mockClient: mockClient,
		handle:     handle,
		lockID:     lockID,
		clientID:   clientID,
		ctx:        context.Background(),
	}
}

func (s *lockHandleTestSuite) createLock(version int64) *Lock {
	now := time.Now()
	return &Lock{
		LockID:     s.lockID,
		OwnerID:    s.clientID,
		Version:    version,
		AcquiredAt: now,
		ExpiresAt:  now.Add(30 * time.Second),
		Metadata:   map[string]string{"test": "metadata"},
	}
}

func (s *lockHandleTestSuite) setLockState(lock *Lock) {
	handleImpl := s.handle.(*lockHandle)
	handleImpl.mu.Lock()
	handleImpl.lock = lock
	handleImpl.mu.Unlock()
}

func (s *lockHandleTestSuite) setClosed(closed bool) {
	handleImpl := s.handle.(*lockHandle)
	handleImpl.mu.Lock()
	handleImpl.closed = closed
	handleImpl.mu.Unlock()
}

func TestNewLockHandle(t *testing.T) {
	tests := []struct {
		name        string
		client      RaftLockClient
		lockID      string
		clientID    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid_parameters",
			client:      &mockLockClient{},
			lockID:      "valid-lock",
			clientID:    "valid-client",
			expectError: false,
		},
		{
			name:        "nil_client",
			client:      nil,
			lockID:      "valid-lock",
			clientID:    "valid-client",
			expectError: true,
			errorMsg:    "client cannot be nil",
		},
		{
			name:        "empty_lock_id",
			client:      &mockLockClient{},
			lockID:      "",
			clientID:    "valid-client",
			expectError: true,
			errorMsg:    "lockID cannot be empty",
		},
		{
			name:        "empty_client_id",
			client:      &mockLockClient{},
			lockID:      "valid-lock",
			clientID:    "",
			expectError: true,
			errorMsg:    "clientID cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle, err := NewLockHandle(tt.client, tt.lockID, tt.clientID)

			if tt.expectError {
				testutil.AssertError(t, err)
				testutil.AssertContains(t, err.Error(), tt.errorMsg)
				testutil.AssertNil(t, handle)
			} else {
				testutil.AssertNoError(t, err)
				testutil.AssertNotNil(t, handle)

				testutil.AssertFalse(t, handle.IsHeld())
				testutil.AssertNil(t, handle.Lock())
			}
		})
	}
}

func TestLockHandle_Acquire(t *testing.T) {
	t.Run("successful_acquire", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		ttl := 30 * time.Second
		expectedLock := suite.createLock(1)

		suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
			testutil.AssertEqual(t, suite.lockID, req.LockID)
			testutil.AssertEqual(t, suite.clientID, req.ClientID)
			testutil.AssertEqual(t, ttl, req.TTL)
			testutil.AssertFalse(t, req.Wait)

			return &AcquireResult{
				Acquired: true,
				Lock:     expectedLock,
			}, nil
		}

		err := suite.handle.Acquire(suite.ctx, ttl, false)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, suite.mockClient.GetAcquireCallCount())

		actualLock := suite.handle.Lock()
		testutil.AssertNotNil(t, actualLock)
		testutil.AssertEqual(t, expectedLock.LockID, actualLock.LockID)
		testutil.AssertEqual(t, expectedLock.Version, actualLock.Version)
	})

	t.Run("acquire_with_wait", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		ttl := 45 * time.Second
		expectedLock := suite.createLock(2)

		suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
			testutil.AssertTrue(t, req.Wait)
			testutil.AssertEqual(t, ttl, req.TTL)
			return &AcquireResult{
				Acquired: true,
				Lock:     expectedLock,
			}, nil
		}

		err := suite.handle.Acquire(suite.ctx, ttl, true)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, suite.mockClient.GetAcquireCallCount())
	})

	t.Run("acquire_fails_lock_held", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
			return &AcquireResult{
				Acquired: false,
				Error: &ErrorDetail{
					Code: pb.ErrorCode_LOCK_HELD,
				},
			}, nil
		}

		err := suite.handle.Acquire(suite.ctx, 30*time.Second, false)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrLockHeld)
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, suite.mockClient.GetAcquireCallCount())
	})

	t.Run("acquire_fails_network_error", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		networkErr := errors.New("connection timeout")

		suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
			return nil, networkErr
		}

		err := suite.handle.Acquire(suite.ctx, 30*time.Second, false)

		testutil.AssertError(t, err)
		testutil.AssertEqual(t, networkErr, err)
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, suite.mockClient.GetAcquireCallCount())
	})

	t.Run("acquire_on_closed_handle", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		suite.setClosed(true)

		err := suite.handle.Acquire(suite.ctx, 30*time.Second, false)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrClientClosed)
		testutil.AssertEqual(t, 0, suite.mockClient.GetAcquireCallCount())
	})

	t.Run("acquire_not_acquired_without_error_detail", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
			return &AcquireResult{
				Acquired: false,
				Error:    nil, // No error detail provided
			}, nil
		}

		err := suite.handle.Acquire(suite.ctx, 30*time.Second, false)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrLockHeld) // Default error when not acquired
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, suite.mockClient.GetAcquireCallCount())
	})
}

func TestLockHandle_Release(t *testing.T) {
	t.Run("successful_release", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			testutil.AssertEqual(t, suite.lockID, req.LockID)
			testutil.AssertEqual(t, suite.clientID, req.ClientID)
			testutil.AssertEqual(t, lock.Version, req.Version)

			return &ReleaseResult{
				Released:       true,
				WaiterPromoted: false,
			}, nil
		}

		err := suite.handle.Release(suite.ctx)

		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertNil(t, suite.handle.Lock())
		testutil.AssertEqual(t, 1, suite.mockClient.GetReleaseCallCount())
	})

	t.Run("release_fails_not_owner", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			return &ReleaseResult{
				Released: false,
				Error: &ErrorDetail{
					Code: pb.ErrorCode_NOT_LOCK_OWNER,
				},
			}, nil
		}

		err := suite.handle.Release(suite.ctx)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotLockOwner)
		testutil.AssertEqual(t, 1, suite.mockClient.GetReleaseCallCount())
	})

	t.Run("release_without_lock", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		err := suite.handle.Release(suite.ctx)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotLockOwner)
		testutil.AssertEqual(t, 0, suite.mockClient.GetReleaseCallCount())
	})

	t.Run("release_on_closed_handle", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		suite.setClosed(true)

		err := suite.handle.Release(suite.ctx)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrClientClosed)
		testutil.AssertEqual(t, 0, suite.mockClient.GetReleaseCallCount())
	})

	t.Run("release_network_error", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)
		networkErr := errors.New("network failure")

		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			return nil, networkErr
		}

		err := suite.handle.Release(suite.ctx)

		testutil.AssertError(t, err)
		testutil.AssertEqual(t, networkErr, err)
		testutil.AssertEqual(t, 1, suite.mockClient.GetReleaseCallCount())
	})

	t.Run("release_not_released_without_error_detail", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			return &ReleaseResult{
				Released: false,
				Error:    nil,
			}, nil
		}

		err := suite.handle.Release(suite.ctx)

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to release lock")
		testutil.AssertEqual(t, 1, suite.mockClient.GetReleaseCallCount())
	})
}

func TestLockHandle_Renew(t *testing.T) {
	t.Run("successful_renew", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		originalLock := suite.createLock(1)
		suite.setLockState(originalLock)

		newTTL := 60 * time.Second
		renewedLock := suite.createLock(2) // New version
		renewedLock.ExpiresAt = time.Now().Add(newTTL)

		suite.mockClient.renewFunc = func(ctx context.Context, req *RenewRequest) (*RenewResult, error) {
			testutil.AssertEqual(t, suite.lockID, req.LockID)
			testutil.AssertEqual(t, suite.clientID, req.ClientID)
			testutil.AssertEqual(t, originalLock.Version, req.Version)
			testutil.AssertEqual(t, newTTL, req.NewTTL)

			return &RenewResult{
				Renewed: true,
				Lock:    renewedLock,
			}, nil
		}

		err := suite.handle.Renew(suite.ctx, newTTL)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, suite.mockClient.GetRenewCallCount())

		currentLock := suite.handle.Lock()
		testutil.AssertEqual(t, renewedLock.Version, currentLock.Version)
		testutil.AssertEqual(t, renewedLock.ExpiresAt, currentLock.ExpiresAt)
	})

	t.Run("renew_without_lock", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		err := suite.handle.Renew(suite.ctx, 60*time.Second)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrNotLockOwner)
		testutil.AssertEqual(t, 0, suite.mockClient.GetRenewCallCount())
	})

	t.Run("renew_on_closed_handle", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		suite.setClosed(true)

		err := suite.handle.Renew(suite.ctx, 60*time.Second)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrClientClosed)
		testutil.AssertEqual(t, 0, suite.mockClient.GetRenewCallCount())
	})

	t.Run("renew_fails_version_mismatch", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		suite.mockClient.renewFunc = func(ctx context.Context, req *RenewRequest) (*RenewResult, error) {
			return &RenewResult{
				Renewed: false,
				Error: &ErrorDetail{
					Code: pb.ErrorCode_VERSION_MISMATCH,
				},
			}, nil
		}

		err := suite.handle.Renew(suite.ctx, 60*time.Second)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrVersionMismatch)
		testutil.AssertEqual(t, 1, suite.mockClient.GetRenewCallCount())
	})

	t.Run("renew_not_renewed_without_error_detail", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		suite.mockClient.renewFunc = func(ctx context.Context, req *RenewRequest) (*RenewResult, error) {
			return &RenewResult{
				Renewed: false,
				Error:   nil,
			}, nil
		}

		err := suite.handle.Renew(suite.ctx, 60*time.Second)

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to renew lock")
		testutil.AssertEqual(t, 1, suite.mockClient.GetRenewCallCount())
	})
}

func TestLockHandle_IsHeld(t *testing.T) {
	t.Run("not_held_initially", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		testutil.AssertFalse(t, suite.handle.IsHeld())
	})

	t.Run("held_after_setting_lock", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		testutil.AssertTrue(t, suite.handle.IsHeld())
	})

	t.Run("not_held_when_closed", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)
		suite.setClosed(true)

		testutil.AssertFalse(t, suite.handle.IsHeld())
	})

	t.Run("not_held_after_clearing_lock", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		testutil.AssertTrue(t, suite.handle.IsHeld())

		suite.setLockState(nil)
		testutil.AssertFalse(t, suite.handle.IsHeld())
	})
}

func TestLockHandle_Lock(t *testing.T) {
	t.Run("returns_nil_when_no_lock", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		testutil.AssertNil(t, suite.handle.Lock())
	})

	t.Run("returns_nil_when_closed", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)
		suite.setClosed(true)

		testutil.AssertNil(t, suite.handle.Lock())
	})

	t.Run("returns_copy_of_lock_data", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		originalLock := suite.createLock(1)
		suite.setLockState(originalLock)

		returnedLock := suite.handle.Lock()

		if returnedLock == originalLock {
			t.Error("Expected different instances, got same pointer")
		}
		testutil.AssertEqual(t, originalLock.LockID, returnedLock.LockID)
		testutil.AssertEqual(t, originalLock.OwnerID, returnedLock.OwnerID)
		testutil.AssertEqual(t, originalLock.Version, returnedLock.Version)
		testutil.AssertEqual(t, originalLock.AcquiredAt, returnedLock.AcquiredAt)
		testutil.AssertEqual(t, originalLock.ExpiresAt, returnedLock.ExpiresAt)
	})

	t.Run("metadata_deep_copy", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		originalMetadata := map[string]string{
			"env":     "production",
			"service": "api-server",
		}
		lock := &Lock{
			LockID:   suite.lockID,
			OwnerID:  suite.clientID,
			Version:  1,
			Metadata: originalMetadata,
		}
		suite.setLockState(lock)

		returnedLock := suite.handle.Lock()

		if &originalMetadata == &returnedLock.Metadata {
			t.Error("Expected different map instances")
		}
		testutil.AssertEqual(t, originalMetadata, returnedLock.Metadata)

		returnedLock.Metadata["modified"] = "true"
		if _, exists := originalMetadata["modified"]; exists {
			t.Error("Original metadata was modified")
		}
	})

	t.Run("handles_nil_metadata", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := &Lock{
			LockID:   suite.lockID,
			OwnerID:  suite.clientID,
			Version:  1,
			Metadata: nil,
		}
		suite.setLockState(lock)

		returnedLock := suite.handle.Lock()

		testutil.AssertNotNil(t, returnedLock)
		testutil.AssertNil(t, returnedLock.Metadata)
	})

	t.Run("multiple_calls_return_independent_copies", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		lock1 := suite.handle.Lock()
		lock2 := suite.handle.Lock()

		if lock1 == lock2 {
			t.Error("Expected different instances")
		}
		if &lock1.Metadata == &lock2.Metadata {
			t.Error("Expected different metadata map instances")
		}

		testutil.AssertEqual(t, lock1.LockID, lock2.LockID)
		testutil.AssertEqual(t, lock1.Metadata, lock2.Metadata)

		lock1.Metadata["new"] = "value"
		if _, exists := lock2.Metadata["new"]; exists {
			t.Error("Modification affected other copy")
		}
	})
}

func TestLockHandle_Close(t *testing.T) {
	t.Run("close_without_lock", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		err := suite.handle.Close(suite.ctx)

		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 0, suite.mockClient.GetReleaseCallCount())

		err = suite.handle.Close(suite.ctx)
		testutil.AssertNoError(t, err)
	})

	t.Run("close_with_held_lock", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		releaseCallCount := 0
		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			releaseCallCount++
			// Verify the request parameters
			testutil.AssertEqual(t, suite.lockID, req.LockID)
			testutil.AssertEqual(t, suite.clientID, req.ClientID)
			testutil.AssertEqual(t, lock.Version, req.Version)

			if ctx == suite.ctx {
				t.Error("Expected background context with timeout, got the passed context")
			}

			return &ReleaseResult{Released: true}, nil
		}

		err := suite.handle.Close(suite.ctx)

		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, releaseCallCount)
	})

	t.Run("close_ignores_release_errors", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			return nil, errors.New("release failed")
		}

		err := suite.handle.Close(suite.ctx)

		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, suite.mockClient.GetReleaseCallCount())
	})

	t.Run("subsequent_operations_fail_after_close", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		err := suite.handle.Close(suite.ctx)
		testutil.RequireNoError(t, err)

		err = suite.handle.Acquire(suite.ctx, 30*time.Second, false)
		testutil.AssertErrorIs(t, err, ErrClientClosed)

		err = suite.handle.Release(suite.ctx)
		testutil.AssertErrorIs(t, err, ErrClientClosed)

		err = suite.handle.Renew(suite.ctx, 30*time.Second)
		testutil.AssertErrorIs(t, err, ErrClientClosed)

		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertNil(t, suite.handle.Lock())
	})
}

func TestLockHandle_StateTransitions(t *testing.T) {
	t.Run("full_lifecycle", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		// Initial state
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertNil(t, suite.handle.Lock())

		// Acquire
		acquiredLock := suite.createLock(1)
		suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
			return &AcquireResult{
				Acquired: true,
				Lock:     acquiredLock,
			}, nil
		}

		err := suite.handle.Acquire(suite.ctx, 30*time.Second, false)
		testutil.RequireNoError(t, err)
		testutil.AssertTrue(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, int64(1), suite.handle.Lock().Version)

		// Renew
		renewedLock := suite.createLock(2)
		suite.mockClient.renewFunc = func(ctx context.Context, req *RenewRequest) (*RenewResult, error) {
			return &RenewResult{
				Renewed: true,
				Lock:    renewedLock,
			}, nil
		}

		err = suite.handle.Renew(suite.ctx, 60*time.Second)
		testutil.RequireNoError(t, err)
		testutil.AssertTrue(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, int64(2), suite.handle.Lock().Version)

		// Release
		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			return &ReleaseResult{
				Released: true,
			}, nil
		}

		err = suite.handle.Release(suite.ctx)
		testutil.RequireNoError(t, err)
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertNil(t, suite.handle.Lock())

		// Verify call counts
		testutil.AssertEqual(t, 1, suite.mockClient.GetAcquireCallCount())
		testutil.AssertEqual(t, 1, suite.mockClient.GetRenewCallCount())
		testutil.AssertEqual(t, 1, suite.mockClient.GetReleaseCallCount())
	})
}

func TestLockHandle_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent_read_operations", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		const numGoroutines = 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for range numGoroutines {
			go func() {
				defer wg.Done()
				_ = suite.handle.IsHeld()
				_ = suite.handle.Lock()
			}()
		}

		wg.Wait()

		// Verify state is still consistent
		testutil.AssertTrue(t, suite.handle.IsHeld())
		testutil.AssertNotNil(t, suite.handle.Lock())
	})

	t.Run("concurrent_close_operations", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		const numGoroutines = 10
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		errors := make([]error, numGoroutines)

		for i := range numGoroutines {
			go func(index int) {
				defer wg.Done()
				errors[index] = suite.handle.Close(suite.ctx)
			}(i)
		}

		wg.Wait()

		for i, err := range errors {
			testutil.AssertNoError(t, err, "Close operation %d should succeed", i)
		}

		testutil.AssertFalse(t, suite.handle.IsHeld())
	})
}

func TestLockHandle_ContextHandling(t *testing.T) {
	t.Run("acquire_with_cancelled_context", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
			return nil, context.Canceled
		}

		err := suite.handle.Acquire(ctx, 30*time.Second, false)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, suite.mockClient.GetAcquireCallCount())
	})

	t.Run("release_with_timeout_context", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()

		time.Sleep(2 * time.Millisecond)

		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			return nil, context.DeadlineExceeded
		}

		err := suite.handle.Release(ctx)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.DeadlineExceeded)
		testutil.AssertEqual(t, 1, suite.mockClient.GetReleaseCallCount())
	})

	t.Run("renew_respects_context", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		ctx, cancel := context.WithCancel(context.Background())

		suite.mockClient.renewFunc = func(ctx context.Context, req *RenewRequest) (*RenewResult, error) {
			cancel()
			return nil, context.Canceled
		}

		err := suite.handle.Renew(ctx, 60*time.Second)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
		testutil.AssertEqual(t, 1, suite.mockClient.GetRenewCallCount())
	})
}

func TestLockHandle_ErrorMapping(t *testing.T) {
	errorMappingTests := []struct {
		name          string
		operation     string
		errorCode     pb.ErrorCode
		expectedError error
	}{
		{
			name:          "lock_held_error",
			operation:     "acquire",
			errorCode:     pb.ErrorCode_LOCK_HELD,
			expectedError: ErrLockHeld,
		},
		{
			name:          "not_lock_owner_error",
			operation:     "release",
			errorCode:     pb.ErrorCode_NOT_LOCK_OWNER,
			expectedError: ErrNotLockOwner,
		},
		{
			name:          "version_mismatch_error",
			operation:     "renew",
			errorCode:     pb.ErrorCode_VERSION_MISMATCH,
			expectedError: ErrVersionMismatch,
		},
		{
			name:          "lock_not_found_error",
			operation:     "release",
			errorCode:     pb.ErrorCode_LOCK_NOT_FOUND,
			expectedError: ErrLockNotFound,
		},
		{
			name:          "invalid_ttl_error",
			operation:     "renew",
			errorCode:     pb.ErrorCode_INVALID_TTL,
			expectedError: ErrInvalidTTL,
		},
	}

	for _, tc := range errorMappingTests {
		t.Run(tc.name, func(t *testing.T) {
			suite := setupLockHandleTest(t)

			if tc.operation != "acquire" {
				lock := suite.createLock(1)
				suite.setLockState(lock)
			}

			errorDetail := &ErrorDetail{
				Code:    tc.errorCode,
				Message: tc.expectedError.Error(),
			}

			switch tc.operation {
			case "acquire":
				suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
					return &AcquireResult{
						Acquired: false,
						Error:    errorDetail,
					}, nil
				}

				err := suite.handle.Acquire(suite.ctx, 30*time.Second, false)
				testutil.AssertErrorIs(t, err, tc.expectedError)

			case "release":
				suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
					return &ReleaseResult{
						Released: false,
						Error:    errorDetail,
					}, nil
				}

				err := suite.handle.Release(suite.ctx)
				testutil.AssertErrorIs(t, err, tc.expectedError)

			case "renew":
				suite.mockClient.renewFunc = func(ctx context.Context, req *RenewRequest) (*RenewResult, error) {
					return &RenewResult{
						Renewed: false,
						Error:   errorDetail,
					}, nil
				}

				err := suite.handle.Renew(suite.ctx, 30*time.Second)
				testutil.AssertErrorIs(t, err, tc.expectedError)
			}
		})
	}
}

func TestLockHandle_CloseTimeout(t *testing.T) {
	t.Run("close_uses_background_context_with_timeout", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		contextChecked := false
		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			_, hasDeadline := ctx.Deadline()
			if hasDeadline && ctx.Err() == nil {
				contextChecked = true
			}
			return &ReleaseResult{Released: true}, nil
		}

		err := suite.handle.Close(cancelledCtx)

		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, suite.handle.IsHeld())
		testutil.AssertTrue(t, contextChecked, "Expected background context with timeout")
	})

	t.Run("close_timeout_behavior", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		timeoutOccurred := false
		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			select {
			case <-time.After(releaseTimeout + time.Second):
				return &ReleaseResult{Released: true}, nil
			case <-ctx.Done():
				timeoutOccurred = true
				return nil, ctx.Err()
			}
		}

		start := time.Now()
		err := suite.handle.Close(suite.ctx)
		duration := time.Since(start)

		testutil.AssertNoError(t, err)
		testutil.AssertFalse(t, suite.handle.IsHeld())

		if duration > releaseTimeout+500*time.Millisecond {
			t.Errorf("Close took too long: %v, expected around %v", duration, releaseTimeout)
		}

		testutil.AssertTrue(t, timeoutOccurred, "Expected timeout to occur")
	})
}

func TestLockHandle_EdgeCases(t *testing.T) {
	t.Run("acquire_twice_without_release", func(t *testing.T) {
		suite := setupLockHandleTest(t)

		// First acquire
		lock1 := suite.createLock(1)
		callCount := 0
		suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
			callCount++
			if callCount == 1 {
				return &AcquireResult{
					Acquired: true,
					Lock:     lock1,
				}, nil
			}
			// Second acquire should replace the first lock
			lock2 := suite.createLock(2)
			return &AcquireResult{
				Acquired: true,
				Lock:     lock2,
			}, nil
		}

		err := suite.handle.Acquire(suite.ctx, 30*time.Second, false)
		testutil.RequireNoError(t, err)
		testutil.AssertTrue(t, suite.handle.IsHeld())

		err = suite.handle.Acquire(suite.ctx, 45*time.Second, false)
		testutil.RequireNoError(t, err)
		testutil.AssertTrue(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, int64(2), suite.handle.Lock().Version)

		testutil.AssertEqual(t, 2, suite.mockClient.GetAcquireCallCount())
	})

	t.Run("release_twice", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		// First release
		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			return &ReleaseResult{
				Released: true,
			}, nil
		}

		err := suite.handle.Release(suite.ctx)
		testutil.RequireNoError(t, err)
		testutil.AssertFalse(t, suite.handle.IsHeld())

		// Second release should fail with ErrNotLockOwner
		err = suite.handle.Release(suite.ctx)
		testutil.AssertErrorIs(t, err, ErrNotLockOwner)

		testutil.AssertEqual(t, 1, suite.mockClient.GetReleaseCallCount())
	})

	t.Run("renew_after_release", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		// Release first
		suite.mockClient.releaseFunc = func(ctx context.Context, req *ReleaseRequest) (*ReleaseResult, error) {
			return &ReleaseResult{
				Released: true,
			}, nil
		}

		err := suite.handle.Release(suite.ctx)
		testutil.RequireNoError(t, err)

		// Try to renew after release
		err = suite.handle.Renew(suite.ctx, 60*time.Second)
		testutil.AssertErrorIs(t, err, ErrNotLockOwner)

		testutil.AssertEqual(t, 1, suite.mockClient.GetReleaseCallCount())
		testutil.AssertEqual(t, 0, suite.mockClient.GetRenewCallCount())
	})

	t.Run("zero_ttl_acquire", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)

		suite.mockClient.acquireFunc = func(ctx context.Context, req *AcquireRequest) (*AcquireResult, error) {
			testutil.AssertEqual(t, time.Duration(0), req.TTL)
			return &AcquireResult{
				Acquired: true,
				Lock:     lock,
			}, nil
		}

		err := suite.handle.Acquire(suite.ctx, 0, false)

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, suite.handle.IsHeld())
		testutil.AssertEqual(t, 1, suite.mockClient.GetAcquireCallCount())
	})

	t.Run("negative_ttl_renew", func(t *testing.T) {
		suite := setupLockHandleTest(t)
		lock := suite.createLock(1)
		suite.setLockState(lock)

		renewedLock := suite.createLock(2)
		suite.mockClient.renewFunc = func(ctx context.Context, req *RenewRequest) (*RenewResult, error) {
			if req.NewTTL < 0 {
				return &RenewResult{
					Renewed: true,
					Lock:    renewedLock,
				}, nil
			}
			return &RenewResult{Renewed: false}, nil
		}

		err := suite.handle.Renew(suite.ctx, -30*time.Second)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 1, suite.mockClient.GetRenewCallCount())
	})
}
