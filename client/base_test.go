package client

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"slices"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newStatusError(code pb.ErrorCode, msg string, details map[string]string) error {
	st, _ := status.New(codes.Unknown, msg).WithDetails(&pb.ErrorDetail{
		Code:    code,
		Message: msg,
		Details: details,
	})
	return st.Err()
}

func TestNewBaseClient(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		config := DefaultClientConfig()
		config.Endpoints = []string{"localhost:8080"}
		client, err := newBaseClient(config)
		testutil.RequireNoError(t, err)
		testutil.AssertNotNil(t, client)
		testutil.AssertNotNil(t, client.(*baseClientImpl).metrics)
	})

	t.Run("no endpoints", func(t *testing.T) {
		config := DefaultClientConfig()
		config.Endpoints = nil
		_, err := newBaseClient(config)
		testutil.AssertError(t, err)
		if err != nil {
			testutil.AssertEqual(t, "at least one endpoint must be provided", err.Error())
		}
	})

	t.Run("metrics disabled", func(t *testing.T) {
		config := DefaultClientConfig()
		config.Endpoints = []string{"localhost:8080"}
		config.EnableMetrics = false
		client, err := newBaseClient(config)
		testutil.RequireNoError(t, err)
		testutil.AssertEqual(t, reflect.TypeOf(&noOpMetrics{}), reflect.TypeOf(client.(*baseClientImpl).getMetrics()))
	})
}

func TestBaseClient_Close(t *testing.T) {
	client, _, _, _ := setupTestClient(DefaultClientConfig())

	err := client.close()
	testutil.RequireNoError(t, err)
	testutil.AssertFalse(t, client.isConnected())
	testutil.AssertTrue(t, client.closed.Load())

	err = client.close()
	testutil.AssertError(t, err)
	testutil.AssertEqual(t, ErrClientClosed, err)

	err = client.executeWithRetry(context.Background(), "test", func(ctx context.Context, c pb.RaftLockClient) error {
		return nil
	})
	testutil.AssertEqual(t, ErrClientClosed, err)
}

func TestBaseClient_ExecuteWithRetry(t *testing.T) {
	setupMockedClient := func(config Config) (*baseClientImpl, *mockConnector, *mockClock, *mockRand) {
		client, connector, clock, rand := setupTestClient(config)

		// Override tryEndpoint to avoid real gRPC connections
		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			return fn(ctx, nil)
		}

		return client, connector, clock, rand
	}

	t.Run("SuccessFirstTry", func(t *testing.T) {
		client, _, _, _ := setupMockedClient(DefaultClientConfig())
		var callCount int

		opFunc := func(ctx context.Context, c pb.RaftLockClient) error {
			callCount++
			return nil
		}

		err := client.executeWithRetry(context.Background(), "testOp", opFunc)

		testutil.RequireNoError(t, err)
		testutil.AssertEqual(t, 1, callCount)
		testutil.AssertEqual(t, uint64(1), client.metrics.GetSuccessCount("testOp"))
	})

	t.Run("SuccessAfterRetry", func(t *testing.T) {
		client, _, clock, _ := setupMockedClient(DefaultClientConfig())
		var callCount atomic.Int32
		retryableErr := newStatusError(pb.ErrorCode_UNAVAILABLE, "server unavailable", nil)

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			count := callCount.Add(1)
			if count < 3 {
				return retryableErr // Fail first 2 attempts
			}
			return fn(ctx, nil) // Succeed on 3rd attempt
		}

		done := make(chan error, 1)
		go func() {
			err := client.executeWithRetry(context.Background(), "testOp", func(ctx context.Context, c pb.RaftLockClient) error {
				return nil
			})
			done <- err
		}()

		for i := range 2 {
			select {
			case <-clock.waiter:
				backoff := client.calculateBackoff(i + 1)
				clock.Advance(backoff + time.Millisecond)

				// Give the retry goroutine a chance to process
				time.Sleep(time.Millisecond)
			case err := <-done:
				// If we complete early, that's fine too
				testutil.RequireNoError(t, err)
				goto checkResults
			case <-time.After(1 * time.Second):
				t.Fatalf("Timeout waiting for retry %d", i+1)
			}
		}

		select {
		case err := <-done:
			testutil.RequireNoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for final success")
		}

	checkResults: // Early success: exit retry loop and proceed to assertions to avoid blocking on nonexistent timers.
		testutil.AssertEqual(t, int32(3), callCount.Load())
		testutil.AssertEqual(t, uint64(1), client.metrics.GetSuccessCount("testOp"))
		testutil.AssertTrue(t, client.metrics.GetRetryCount("testOp") >= 1, "Expected at least 1 retry")
	})

	t.Run("FailureAfterMaxRetries", func(t *testing.T) {
		config := DefaultClientConfig()
		config.RetryPolicy.MaxRetries = 2
		client, _, clock, _ := setupMockedClient(config)

		var callCount atomic.Int32
		retryableErr := newStatusError(pb.ErrorCode_TIMEOUT, "operation timed out", nil)

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			callCount.Add(1)
			return retryableErr
		}

		done := make(chan error, 1)
		go func() {
			err := client.executeWithRetry(context.Background(), "testOp", func(ctx context.Context, c pb.RaftLockClient) error {
				t.Error("Operation function should not be called when tryEndpoint fails")
				return nil
			})
			done <- err
		}()

		// Let the initial attempt happen
		time.Sleep(10 * time.Millisecond)

		// Process each retry by waiting for timer and advancing clock
		retriesProcessed := 0
		for retriesProcessed < config.RetryPolicy.MaxRetries {
			select {
			case <-clock.waiter:
				backoff := client.calculateBackoff(retriesProcessed + 1)
				clock.Advance(backoff + time.Millisecond)
				retriesProcessed++
				// Give the retry goroutine time to execute
				time.Sleep(10 * time.Millisecond)

			case err := <-done:
				// Operation completed (should be failure)
				testutil.AssertError(t, err)
				testutil.AssertContains(t, err.Error(), "failed after")
				goto validateResults

			case <-time.After(2 * time.Second):
				t.Fatalf("Timeout waiting for retry %d", retriesProcessed+1)
			}
		}

		select {
		case err := <-done:
			testutil.AssertError(t, err)
			testutil.AssertContains(t, err.Error(), "failed after")
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for final failure result")
		}

	validateResults: // Operation failed before all retries were processed; exit loop and validate results.
		// Since all endpoints fail, it tries all endpoints in each attempt
		expectedMinCalls := int32(config.RetryPolicy.MaxRetries+1) * int32(len(client.endpoints))
		actualCalls := callCount.Load()

		testutil.AssertTrue(t, actualCalls >= expectedMinCalls,
			"Expected at least %d calls (trying all endpoints in each attempt), got %d",
			expectedMinCalls, actualCalls)

		testutil.AssertEqual(t, uint64(1), client.metrics.GetFailureCount("testOp"))
		testutil.AssertEqual(t, uint64(config.RetryPolicy.MaxRetries), client.metrics.GetRetryCount("testOp"))
	})

	t.Run("LeaderRedirectSuccess", func(t *testing.T) {
		client, _, _, _ := setupMockedClient(DefaultClientConfig())
		client.setCurrentLeader("endpoint1")

		notLeaderErr := newStatusError(pb.ErrorCode_NOT_LEADER, "not leader", map[string]string{"leader_address": "endpoint2"})
		var calls []string

		// Current logic in base.go:
		// 1. tryOperation calls tryEndpoint with current leader (endpoint1)
		// 2. Gets NOT_LEADER, clears current leader, then tries all endpoints
		// 3. handleLeaderRedirect processes the error and sets new leader
		// 4. Retry loop continues and calls tryOperation again with new leader

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			calls = append(calls, endpoint)

			if endpoint == "endpoint1" {
				// First call to original leader returns NOT_LEADER
				return notLeaderErr
			} else if endpoint == "endpoint2" {
				// Call to redirected leader succeeds
				return fn(ctx, nil)
			} else {
				// Calls to other endpoints in the endpoint list (if any)
				return notLeaderErr // Also return NOT_LEADER for other endpoints
			}
		}

		err := client.executeWithRetry(context.Background(), "testOp", func(ctx context.Context, c pb.RaftLockClient) error {
			return nil
		})

		testutil.RequireNoError(t, err)

		t.Logf("Call sequence: %v", calls)
		t.Logf("Final leader: %s", client.getCurrentLeader())

		testutil.AssertEqual(t, "endpoint2", client.getCurrentLeader())
		testutil.AssertEqual(t, uint64(1), client.metrics.GetSuccessCount("testOp"))

		foundEndpoint2 := slices.Contains(calls, "endpoint2")
		testutil.AssertTrue(t, foundEndpoint2, "endpoint2 should have been called")
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		client, _, _, _ := setupMockedClient(DefaultClientConfig())
		retryableErr := newStatusError(pb.ErrorCode_UNAVAILABLE, "server unavailable", nil)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return retryableErr
		}

		opFunc := func(ctx context.Context, c pb.RaftLockClient) error {
			return retryableErr
		}

		err := client.executeWithRetry(ctx, "testOp", opFunc)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})

	t.Run("NonRetryableError", func(t *testing.T) {
		client, _, _, _ := setupMockedClient(DefaultClientConfig())
		nonRetryableErr := newStatusError(pb.ErrorCode_INVALID_ARGUMENT, "invalid argument", nil)
		var callCount int

		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			callCount++
			return nonRetryableErr
		}

		opFunc := func(ctx context.Context, c pb.RaftLockClient) error {
			return nil
		}

		err := client.executeWithRetry(context.Background(), "testOp", opFunc)

		testutil.AssertError(t, err)
		// Should try all endpoints once, but no retries
		testutil.AssertEqual(t, len(client.endpoints), callCount)
		testutil.AssertEqual(t, uint64(1), client.metrics.GetFailureCount("testOp"))
		testutil.AssertEqual(t, uint64(0), client.metrics.GetRetryCount("testOp"))
	})

	t.Run("AllEndpointsUnavailable", func(t *testing.T) {
		config := DefaultClientConfig()
		config.RetryPolicy.MaxRetries = 1
		config.Endpoints = []string{"endpoint1", "endpoint2", "endpoint3"}
		client, _, clock, _ := setupMockedClient(config)

		unavailableErr := newStatusError(pb.ErrorCode_UNAVAILABLE, "server unavailable", nil)
		var endpoints []string
		var mu sync.Mutex

		// Record each endpoint the client attempts and always return an unavailable error.
		client.tryEndpointFunc = func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
			mu.Lock()
			endpoints = append(endpoints, endpoint)
			mu.Unlock()
			return unavailableErr
		}

		done := make(chan error, 1)
		go func() {
			err := client.executeWithRetry(context.Background(), "testOp", func(ctx context.Context, c pb.RaftLockClient) error {
				return nil
			})
			done <- err
		}()

		// Handle the retry trigger by advancing the mock clock
		select {
		case <-clock.waiter:
			clock.Advance(client.calculateBackoff(1) + time.Millisecond)
			time.Sleep(time.Millisecond)
		case err := <-done:
			testutil.AssertError(t, err)
			goto validate
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for retry")
		}

		// Wait for the operation to complete (expected failure)
		select {
		case err := <-done:
			testutil.AssertError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for final result")
		}

	validate: // Proceed with result verification
		mu.Lock()
		finalEndpoints := make([]string, len(endpoints))
		copy(finalEndpoints, endpoints)
		mu.Unlock()

		// Should try all endpoints twice (initial + 1 retry)
		expectedTotal := len(config.Endpoints) * 2
		testutil.AssertEqual(t, expectedTotal, len(finalEndpoints))

		// Verify that each endpoint was tried exactly twice
		for _, expectedEndpoint := range config.Endpoints {
			count := 0
			for _, actual := range finalEndpoints {
				if actual == expectedEndpoint {
					count++
				}
			}
			testutil.AssertEqual(t, 2, count, "Expected endpoint %s to be tried 2 times", expectedEndpoint)
		}

		testutil.AssertEqual(t, uint64(1), client.metrics.GetFailureCount("testOp"))
		testutil.AssertEqual(t, uint64(1), client.metrics.GetRetryCount("testOp"))
	})
}

func TestBaseClient_CalculateBackoff(t *testing.T) {
	config := DefaultClientConfig()
	config.RetryPolicy = RetryPolicy{
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFactor:      0.1,
	}
	client, _, _, rand := setupTestClient(config)

	t.Run("no jitter", func(t *testing.T) {
		client.config.RetryPolicy.JitterFactor = 0
		testutil.AssertEqual(t, 100*time.Millisecond, client.calculateBackoff(1))
		testutil.AssertEqual(t, 200*time.Millisecond, client.calculateBackoff(2))
		testutil.AssertEqual(t, 1600*time.Millisecond, client.calculateBackoff(5))
		testutil.AssertEqual(t, 2*time.Second, client.calculateBackoff(6))
	})

	t.Run("with jitter", func(t *testing.T) {
		client.config.RetryPolicy.JitterFactor = 0.1

		rand.setFloat64s(0.5)
		testutil.AssertEqual(t, 200*time.Millisecond, client.calculateBackoff(2))

		rand.setFloat64s(1.0)
		testutil.AssertEqual(t, 220*time.Millisecond, client.calculateBackoff(2))

		rand.setFloat64s(0.0)
		testutil.AssertEqual(t, 180*time.Millisecond, client.calculateBackoff(2))
	})
}

func TestBaseClient_SetRetryPolicy(t *testing.T) {
	t.Run("UpdateRetryPolicy", func(t *testing.T) {
		client, _, _, _ := setupTestClient(DefaultClientConfig())

		newPolicy := RetryPolicy{
			MaxRetries:        5,
			InitialBackoff:    500 * time.Millisecond,
			MaxBackoff:        10 * time.Second,
			BackoffMultiplier: 3.0,
			JitterFactor:      0.2,
			RetryableErrors:   []pb.ErrorCode{pb.ErrorCode_UNAVAILABLE, pb.ErrorCode_TIMEOUT},
		}

		client.setRetryPolicy(newPolicy)

		client.mu.RLock()
		actualPolicy := client.config.RetryPolicy
		client.mu.RUnlock()

		testutil.AssertEqual(t, newPolicy.MaxRetries, actualPolicy.MaxRetries)
		testutil.AssertEqual(t, newPolicy.InitialBackoff, actualPolicy.InitialBackoff)
		testutil.AssertEqual(t, newPolicy.MaxBackoff, actualPolicy.MaxBackoff)
		testutil.AssertEqual(t, newPolicy.BackoffMultiplier, actualPolicy.BackoffMultiplier)
		testutil.AssertEqual(t, newPolicy.JitterFactor, actualPolicy.JitterFactor)
		testutil.AssertEqual(t, len(newPolicy.RetryableErrors), len(actualPolicy.RetryableErrors))
	})

	t.Run("ConcurrentUpdate", func(t *testing.T) {
		client, _, _, _ := setupTestClient(DefaultClientConfig())

		done := make(chan bool, 10)

		for i := range 10 {
			go func(maxRetries int) {
				policy := RetryPolicy{
					MaxRetries:        maxRetries,
					InitialBackoff:    100 * time.Millisecond,
					MaxBackoff:        5 * time.Second,
					BackoffMultiplier: 2.0,
					JitterFactor:      0.1,
					RetryableErrors:   []pb.ErrorCode{pb.ErrorCode_UNAVAILABLE},
				}
				client.setRetryPolicy(policy)
				done <- true
			}(i)
		}

		// Wait for all updates to complete
		for range 10 {
			<-done
		}

		client.mu.RLock()
		finalPolicy := client.config.RetryPolicy
		client.mu.RUnlock()

		testutil.AssertTrue(t, finalPolicy.MaxRetries >= 0 && finalPolicy.MaxRetries < 10)
	})
}

func TestBaseClient_SetRand(t *testing.T) {
	t.Run("SetCustomRand", func(t *testing.T) {
		client, _, _, _ := setupTestClient(DefaultClientConfig())

		customRand := newMockRand()
		customRand.setFloat64s(0.75)

		client.setRand(customRand)

		testutil.AssertEqual(t, customRand, client.rand)

		backoff := client.calculateBackoff(1)

		// With JitterFactor 0.1 and Float64() returning 0.75:
		// jitter = (0.75*2 - 1) * 0.1 * backoff = 0.5 * 0.1 * backoff
		// So final backoff should be backoff * 1.05
		expectedBase := float64(client.config.RetryPolicy.InitialBackoff)
		jitter := (0.75*2 - 1) * client.config.RetryPolicy.JitterFactor * expectedBase
		expected := time.Duration(expectedBase + jitter)

		testutil.AssertEqual(t, expected, backoff)
	})

	t.Run("SetNilRand", func(t *testing.T) {
		client, _, _, _ := setupTestClient(DefaultClientConfig())

		client.setRand(nil)
		testutil.AssertNil(t, client.rand)
	})
}

func TestBaseClient_BuildDialOptions(t *testing.T) {
	t.Run("DefaultOptions", func(t *testing.T) {
		config := DefaultClientConfig()
		client, _, _, _ := setupTestClient(config)

		opts := client.buildDialOptions()

		// Should have at least 3 options: credentials, keepalive, call options
		testutil.AssertTrue(t, len(opts) >= 3)

		// Verify options are set (we can't easily inspect the actual values,
		// but we can verify the options are created without panicking)
		testutil.AssertNotNil(t, opts)
	})

	t.Run("CustomConfig", func(t *testing.T) {
		config := DefaultClientConfig()
		config.MaxMessageSize = 1024 * 1024
		config.KeepAlive.Time = 30 * time.Second
		config.KeepAlive.Timeout = 5 * time.Second
		config.KeepAlive.PermitWithoutStream = true

		client, _, _, _ := setupTestClient(config)

		opts := client.buildDialOptions()
		testutil.AssertTrue(t, len(opts) >= 3)

		opts2 := client.buildDialOptions()
		testutil.AssertEqual(t, len(opts), len(opts2))
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		client, _, _, _ := setupTestClient(DefaultClientConfig())

		results := make(chan []grpc.DialOption, 5)

		for range 5 {
			go func() {
				opts := client.buildDialOptions()
				results <- opts
			}()
		}

		for range 5 {
			opts := <-results
			testutil.AssertTrue(t, len(opts) >= 3)
		}
	})
}

func TestBaseClient_GetMetrics(t *testing.T) {
	t.Run("MetricsEnabled", func(t *testing.T) {
		config := DefaultClientConfig()
		config.EnableMetrics = true
		client, _, _, _ := setupTestClient(config)

		metrics := client.getMetrics()
		testutil.AssertNotNil(t, metrics)
	})

	t.Run("MetricsDisabled", func(t *testing.T) {
		config := DefaultClientConfig()
		config.EnableMetrics = false
		client, _, _, _ := setupTestClient(config)

		metrics := client.getMetrics()
		testutil.AssertNotNil(t, metrics)
	})
}

// TestBaseClientImpl_GetConnection tests the logic for obtaining a gRPC connection.
func TestBaseClientImpl_GetConnection(t *testing.T) {
	expectedConn := &grpc.ClientConn{}

	testCases := []struct {
		name      string
		hostToGet string
		setup     func(t *testing.T, client *baseClientImpl, m *mockConnector)
		check     func(t *testing.T, conn *grpc.ClientConn, err error, client *baseClientImpl)
	}{
		{
			name:      "should connect and cache a new connection successfully",
			hostToGet: "host1",
			setup: func(t *testing.T, client *baseClientImpl, m *mockConnector) {
				m.GetConnectionFunc = func(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
					if endpoint == "host1" {
						return expectedConn, nil
					}
					return nil, errors.New("mock received unexpected host")
				}
			},
			check: func(t *testing.T, conn *grpc.ClientConn, err error, client *baseClientImpl) {
				testutil.RequireNoError(t, err)
				testutil.AssertEqual(t, expectedConn, conn, "should return the new connection")
				testutil.AssertEqual(t, expectedConn, client.conns["host1"], "connection should be cached")
			},
		},
		{
			name:      "should return an error if host is unavailable",
			hostToGet: "host1",
			setup: func(t *testing.T, client *baseClientImpl, m *mockConnector) {
				m.GetConnectionFunc = func(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
					return nil, errors.New("connection failed")
				}
			},
			check: func(t *testing.T, conn *grpc.ClientConn, err error, client *baseClientImpl) {
				testutil.AssertError(t, err)
				testutil.AssertNil(t, conn)
				if err != nil {
					testutil.AssertContains(t, err.Error(), "connection failed")
				}
				_, exists := client.conns["host1"]
				testutil.AssertFalse(t, exists, "failed connection should not be cached")
			},
		},
		{
			name:      "should return a cached connection without creating a new one",
			hostToGet: "host1",
			setup: func(t *testing.T, client *baseClientImpl, m *mockConnector) {
				client.conns["host1"] = expectedConn
				m.GetConnectionFunc = func(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
					t.Fatal("connector.GetConnection should not be called when a connection is cached")
					return nil, nil
				}
			},
			check: func(t *testing.T, conn *grpc.ClientConn, err error, client *baseClientImpl) {
				testutil.AssertNoError(t, err)
				testutil.AssertEqual(t, expectedConn, conn, "should return the cached connection")
			},
		},
		{
			name:      "should return an error if the client is closed",
			hostToGet: "host1",
			setup: func(t *testing.T, client *baseClientImpl, m *mockConnector) {
				client.closed.Store(true)
			},
			check: func(t *testing.T, conn *grpc.ClientConn, err error, client *baseClientImpl) {
				testutil.AssertErrorIs(t, err, ErrClientClosed)
				testutil.AssertNil(t, conn)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			connector := &mockConnector{}
			client := &baseClientImpl{
				conns:     make(map[string]*grpc.ClientConn),
				endpoints: []string{"host1", "host2", "host3"},
				connector: connector,
				closed:    atomic.Bool{},
			}

			if tc.setup != nil {
				tc.setup(t, client, connector)
			}

			conn, err := client.getConnection(tc.hostToGet)

			if tc.check != nil {
				tc.check(t, conn, err, client)
			}
		})
	}
}

func TestBaseClientImpl_TryEndpoint(t *testing.T) {
	mockConn := &grpc.ClientConn{}
	errConnectionFailed := errors.New("connection failed")
	errOperationFailed := errors.New("operation failed")

	testCases := []struct {
		name      string
		endpoint  string
		setup     func(t *testing.T, client *baseClientImpl, m *mockConnector)
		operation func(ctx context.Context, client pb.RaftLockClient) error
		check     func(t *testing.T, err error)
	}{
		{
			name:     "should succeed if connection and operation are successful",
			endpoint: "host1",
			setup: func(t *testing.T, client *baseClientImpl, m *mockConnector) {
				m.GetConnectionFunc = func(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
					return mockConn, nil
				}
			},
			operation: func(ctx context.Context, client pb.RaftLockClient) error {
				return nil // Simulate a successful gRPC call
			},
			check: func(t *testing.T, err error) {
				testutil.AssertNoError(t, err)
			},
		},
		{
			name:     "should fail if getConnection returns an error",
			endpoint: "host1",
			setup: func(t *testing.T, client *baseClientImpl, m *mockConnector) {
				m.GetConnectionFunc = func(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
					return nil, errConnectionFailed
				}
			},
			operation: func(ctx context.Context, client pb.RaftLockClient) error {
				t.Fatal("operation should not have been called if getConnection failed")
				return nil
			},
			check: func(t *testing.T, err error) {
				testutil.AssertError(t, err)
				testutil.AssertErrorIs(t, err, errConnectionFailed)
			},
		},
		{
			name:     "should fail if the operation returns an error",
			endpoint: "host1",
			setup: func(t *testing.T, client *baseClientImpl, m *mockConnector) {
				m.GetConnectionFunc = func(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
					return mockConn, nil
				}
			},
			operation: func(ctx context.Context, client pb.RaftLockClient) error {
				return errOperationFailed // Simulate a failed gRPC call
			},
			check: func(t *testing.T, err error) {
				testutil.AssertErrorIs(t, err, errOperationFailed)
			},
		},
		{
			name:     "should time out if the operation takes too long",
			endpoint: "host1",
			setup: func(t *testing.T, client *baseClientImpl, m *mockConnector) {
				client.config.RequestTimeout = 10 * time.Millisecond
				m.GetConnectionFunc = func(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
					return mockConn, nil
				}
			},
			operation: func(ctx context.Context, client pb.RaftLockClient) error {
				// Simulate work that takes longer than the timeout.
				const workDuration = 50 * time.Millisecond

				select {
				case <-time.After(workDuration):
					// This case means the work "finished" without a timeout, which shouldn't happen in this test.
					return nil
				case <-ctx.Done():
					// This case is hit when the context is canceled or times out.
					// Return the context's error to properly simulate a timeout.
					return ctx.Err()
				}
			},
			check: func(t *testing.T, err error) {
				testutil.AssertErrorIs(t, err, context.DeadlineExceeded)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			connector := &mockConnector{}
			client := &baseClientImpl{
				config:    Config{},
				conns:     make(map[string]*grpc.ClientConn),
				connector: connector,
				closed:    atomic.Bool{},
			}
			if tc.setup != nil {
				tc.setup(t, client, connector)
			}

			err := client.tryEndpoint(context.Background(), tc.endpoint, tc.operation)

			if tc.check != nil {
				tc.check(t, err)
			}
		})
	}
}
