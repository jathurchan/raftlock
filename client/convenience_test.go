package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/testutil"
)

// setupDoWithLockTest provides a consistent setup for DoWithLock tests,
// returning mock components and a cleanup function to restore monkey-patched constructors.
func setupDoWithLockTest(t *testing.T) (*mockLockClient, *mockLockHandle, func()) {
	t.Helper()

	mockClient := &mockLockClient{}
	mockHandle := newMockLockHandle()

	originalNewLockHandle := NewLockHandleFunc
	NewLockHandleFunc = func(client RaftLockClient, lockID, clientID string) (LockHandle, error) {
		testutil.AssertEqual(t, mockClient, client, "Mismatched client instance")
		testutil.AssertEqual(t, "test-lock", lockID, "Mismatched lockID")
		testutil.AssertEqual(t, "test-client", clientID, "Mismatched clientID")
		return mockHandle, nil
	}

	cleanup := func() {
		NewLockHandleFunc = originalNewLockHandle
	}

	return mockClient, mockHandle, cleanup
}

// TestDoWithLock encapsulates all test cases for the DoWithLock convenience function.
func TestDoWithLock(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockClient, _, cleanup := setupDoWithLockTest(t)
		defer cleanup()

		fnExecuted := false
		err := DoWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, func(ctx context.Context) error {
			fnExecuted = true
			return nil
		})

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, fnExecuted, "Expected the wrapped function to be executed")
	})

	t.Run("HandleCreationFailure", func(t *testing.T) {
		mockClient := &mockLockClient{}
		expectedErr := errors.New("handle creation failed")

		originalNewLockHandle := NewLockHandleFunc
		defer func() { NewLockHandleFunc = originalNewLockHandle }()
		NewLockHandleFunc = func(client RaftLockClient, lockID, clientID string) (LockHandle, error) {
			return nil, expectedErr
		}

		fnExecuted := false
		err := DoWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, func(ctx context.Context) error {
			fnExecuted = true
			return nil
		})

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to create lock handle")
		testutil.AssertFalse(t, fnExecuted, "Function should not execute if handle creation fails")
	})

	t.Run("AcquireFailure", func(t *testing.T) {
		mockClient, mockHandle, cleanup := setupDoWithLockTest(t)
		defer cleanup()

		expectedErr := errors.New("acquire failed")
		mockHandle.acquireErr = expectedErr

		fnExecuted := false
		err := DoWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, func(ctx context.Context) error {
			fnExecuted = true
			return nil
		})

		testutil.AssertErrorIs(t, err, expectedErr)
		testutil.AssertFalse(t, fnExecuted, "Function should not execute if acquire fails")
	})

	t.Run("FunctionError", func(t *testing.T) {
		mockClient, _, cleanup := setupDoWithLockTest(t)
		defer cleanup()

		expectedErr := errors.New("function execution failed")
		err := DoWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, func(ctx context.Context) error {
			return expectedErr
		})

		testutil.AssertErrorIs(t, err, expectedErr)
	})

	t.Run("CloseErrorAfterSuccess", func(t *testing.T) {
		mockClient, mockHandle, cleanup := setupDoWithLockTest(t)
		defer cleanup()

		closeErr := errors.New("close failed")
		mockHandle.closeErr = closeErr

		err := DoWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, func(ctx context.Context) error {
			return nil
		})

		testutil.AssertErrorIs(t, err, closeErr)
	})

	t.Run("CloseErrorAfterFunctionError", func(t *testing.T) {
		mockClient, mockHandle, cleanup := setupDoWithLockTest(t)
		defer cleanup()

		fnErr := errors.New("function failed")
		mockHandle.closeErr = errors.New("close failed")

		err := DoWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, func(ctx context.Context) error {
			return fnErr
		})

		testutil.AssertErrorIs(t, err, fnErr)
	})
}

// setupRunWithLockTest provides a consistent setup for RunWithLock tests.
func setupRunWithLockTest(t *testing.T) (*mockLockClient, *mockLockHandle, *mockAutoRenewer, func()) {
	t.Helper()

	mockClient := &mockLockClient{}
	mockHandle := newMockLockHandle()
	mockRenewer := &mockAutoRenewer{}

	originalNewLockHandle := NewLockHandleFunc
	NewLockHandleFunc = func(client RaftLockClient, lockID, clientID string) (LockHandle, error) {
		return mockHandle, nil
	}

	originalNewAutoRenewer := NewAutoRenewerFunc
	NewAutoRenewerFunc = func(handle LockHandle, interval, ttl time.Duration, opts ...AutoRenewerOption) (AutoRenewer, error) {
		return mockRenewer, nil
	}

	cleanup := func() {
		NewLockHandleFunc = originalNewLockHandle
		NewAutoRenewerFunc = originalNewAutoRenewer
	}

	return mockClient, mockHandle, mockRenewer, cleanup
}

// TestRunWithLock encapsulates all test cases for the RunWithLock convenience function.
func TestRunWithLock(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockClient, _, mockRenewer, cleanup := setupRunWithLockTest(t)
		defer cleanup()

		fnExecuted := false
		err := RunWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, 10*time.Second, func(ctx context.Context) error {
			fnExecuted = true
			return nil
		})

		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, fnExecuted, "Function should have been executed")
		testutil.AssertTrue(t, mockRenewer.startCalled, "Renewer.Start should have been called")
		testutil.AssertTrue(t, mockRenewer.stopCalled, "Renewer.Stop should have been called")
	})

	t.Run("NewAutoRenewerFailure", func(t *testing.T) {
		mockClient := &mockLockClient{}
		expectedErr := errors.New("renewer creation failed")

		originalNewLockHandle := NewLockHandleFunc
		NewLockHandleFunc = func(client RaftLockClient, lockID, clientID string) (LockHandle, error) {
			return newMockLockHandle(), nil
		}
		defer func() { NewLockHandleFunc = originalNewLockHandle }()

		originalNewAutoRenewer := NewAutoRenewerFunc
		NewAutoRenewerFunc = func(handle LockHandle, interval, ttl time.Duration, opts ...AutoRenewerOption) (AutoRenewer, error) {
			return nil, expectedErr
		}
		defer func() { NewAutoRenewerFunc = originalNewAutoRenewer }()

		fnExecuted := false
		err := RunWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, 10*time.Second, func(ctx context.Context) error {
			fnExecuted = true
			return nil
		})

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed to create auto-renewer")
		testutil.AssertFalse(t, fnExecuted, "Function should not execute if renewer creation fails")
	})

	t.Run("FunctionErrorTakesPrecedenceOverStopError", func(t *testing.T) {
		mockClient, _, mockRenewer, cleanup := setupRunWithLockTest(t)
		defer cleanup()

		fnErr := errors.New("function failed")
		mockRenewer.stopErr = errors.New("stop failed")

		err := RunWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, 10*time.Second, func(ctx context.Context) error {
			return fnErr
		})

		testutil.AssertErrorIs(t, err, fnErr)
		testutil.AssertTrue(t, mockRenewer.startCalled, "Renewer.Start should have been called")
		testutil.AssertTrue(t, mockRenewer.stopCalled, "Renewer.Stop should have been called")
	})

	t.Run("StopErrorIsReturned", func(t *testing.T) {
		mockClient, _, mockRenewer, cleanup := setupRunWithLockTest(t)
		defer cleanup()

		stopErr := errors.New("stop failed")
		mockRenewer.stopErr = stopErr

		err := RunWithLock(context.Background(), mockClient, "test-lock", "test-client", 30*time.Second, 10*time.Second, func(ctx context.Context) error {
			return nil
		})

		testutil.AssertErrorIs(t, err, stopErr)
	})
}
