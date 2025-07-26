package server

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/lock"
	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

func createValidPeers() map[types.NodeID]raft.PeerConfig {
	return map[types.NodeID]raft.PeerConfig{
		"node-1": {ID: "node-1", Address: "localhost:8001"},
		"node-2": {ID: "node-2", Address: "localhost:8002"},
		"node-3": {ID: "node-3", Address: "localhost:8003"},
	}
}

func assertBuilderChaining(
	t *testing.T,
	original, returned *RaftLockServerBuilder,
	methodName string,
) {
	t.Helper()
	if returned != original {
		t.Errorf("%s should return the same builder instance for method chaining", methodName)
	}
}

func TestNewRaftLockServerBuilder(t *testing.T) {
	t.Run("creates builder with defaults", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		if builder == nil {
			t.Fatal("NewRaftLockServerBuilder() returned nil")
		}

		if builder.config.ListenAddress != DefaultListenAddress {
			t.Errorf("expected default listen address %q, got %q",
				DefaultListenAddress, builder.config.ListenAddress)
		}

		if builder.config.RequestTimeout != DefaultRequestTimeout {
			t.Errorf("expected default request timeout %v, got %v",
				DefaultRequestTimeout, builder.config.RequestTimeout)
		}

		if builder.config.MaxConcurrentReqs != DefaultMaxConcurrentRequests {
			t.Errorf("expected default max concurrent requests %d, got %d",
				DefaultMaxConcurrentRequests, builder.config.MaxConcurrentReqs)
		}
	})

	t.Run("initializes flags as false", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		tests := []struct {
			name string
			flag bool
		}{
			{"hasNodeID", builder.hasNodeID},
			{"hasListenAddr", builder.hasListenAddr},
			{"hasPeers", builder.hasPeers},
			{"hasDataDir", builder.hasDataDir},
		}

		for _, tt := range tests {
			if tt.flag {
				t.Errorf("expected %s to be false initially", tt.name)
			}
		}
	})
}

func TestRaftLockServerBuilder_WithNodeID(t *testing.T) {
	tests := []struct {
		name   string
		nodeID types.NodeID
	}{
		{"simple node ID", "node-1"},
		{"node ID with hyphens", "test-node-123"},
		{"node ID with underscores", "test_node_456"},
		{"empty node ID", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockServerBuilder()

			result := builder.WithNodeID(tt.nodeID)

			assertBuilderChaining(t, builder, result, "WithNodeID")

			if builder.config.NodeID != tt.nodeID {
				t.Errorf("expected NodeID %q, got %q", tt.nodeID, builder.config.NodeID)
			}

			if !builder.hasNodeID {
				t.Error("expected hasNodeID flag to be true after setting NodeID")
			}
		})
	}
}

func TestRaftLockServerBuilder_WithListenAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{"localhost with port", "localhost:9090"},
		{"IP with port", "127.0.0.1:8080"},
		{"all interfaces", "0.0.0.0:8080"},
		{"hostname with port", "myhost:3000"},
		{"empty address", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockServerBuilder()

			result := builder.WithListenAddress(tt.address)

			assertBuilderChaining(t, builder, result, "WithListenAddress")

			if builder.config.ListenAddress != tt.address {
				t.Errorf(
					"expected ListenAddress %q, got %q",
					tt.address,
					builder.config.ListenAddress,
				)
			}

			if !builder.hasListenAddr {
				t.Error("expected hasListenAddr flag to be true after setting address")
			}
		})
	}
}

func TestRaftLockServerBuilder_WithPeers(t *testing.T) {
	t.Run("sets peers correctly", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		peers := createValidPeers()

		result := builder.WithPeers(peers)

		assertBuilderChaining(t, builder, result, "WithPeers")

		if len(builder.config.Peers) != len(peers) {
			t.Errorf("expected %d peers, got %d", len(peers), len(builder.config.Peers))
		}

		for id, expectedCfg := range peers {
			actualCfg, exists := builder.config.Peers[id]
			if !exists {
				t.Errorf("peer %q not found in builder config", id)
				continue
			}
			if actualCfg != expectedCfg {
				t.Errorf("peer %q: expected config %+v, got %+v", id, expectedCfg, actualCfg)
			}
		}

		if !builder.hasPeers {
			t.Error("expected hasPeers flag to be true after setting peers")
		}
	})

	t.Run("handles empty peers map", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		emptyPeers := make(map[types.NodeID]raft.PeerConfig)

		builder.WithPeers(emptyPeers)

		if len(builder.config.Peers) != 0 {
			t.Errorf("expected 0 peers, got %d", len(builder.config.Peers))
		}

		if !builder.hasPeers {
			t.Error("expected hasPeers flag to be true even with empty peers map")
		}
	})

	t.Run("handles nil peers map", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		builder.WithPeers(nil)

		if builder.config.Peers != nil {
			t.Error("expected Peers to be nil when nil is passed")
		}

		if !builder.hasPeers {
			t.Error("expected hasPeers flag to be true even with nil peers")
		}
	})
}

func TestRaftLockServerBuilder_WithDataDir(t *testing.T) {
	tests := []struct {
		name    string
		dataDir string
	}{
		{"absolute path", "/tmp/raftlock-test"},
		{"relative path", "./data"},
		{"current directory", "."},
		{"empty path", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockServerBuilder()

			result := builder.WithDataDir(tt.dataDir)

			assertBuilderChaining(t, builder, result, "WithDataDir")

			if builder.config.DataDir != tt.dataDir {
				t.Errorf("expected DataDir %q, got %q", tt.dataDir, builder.config.DataDir)
			}

			if !builder.hasDataDir {
				t.Error("expected hasDataDir flag to be true after setting data directory")
			}
		})
	}
}

func TestRaftLockServerBuilder_WithRaftConfig(t *testing.T) {
	t.Run("sets custom raft config", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		customConfig := raft.Config{
			ID: "custom-node",
			Options: raft.Options{
				ElectionTickCount:       25,
				HeartbeatTickCount:      5,
				MaxLogEntriesPerRequest: 500,
			},
			FeatureFlags: raft.FeatureFlags{
				EnableReadIndex:   true,
				EnableLeaderLease: false,
			},
		}

		result := builder.WithRaftConfig(customConfig)

		assertBuilderChaining(t, builder, result, "WithRaftConfig")

		if builder.config.RaftConfig.ID != customConfig.ID {
			t.Errorf(
				"expected RaftConfig.ID %q, got %q",
				customConfig.ID,
				builder.config.RaftConfig.ID,
			)
		}

		if builder.config.RaftConfig.Options.ElectionTickCount != customConfig.Options.ElectionTickCount {
			t.Errorf(
				"expected ElectionTickCount %d, got %d",
				customConfig.Options.ElectionTickCount,
				builder.config.RaftConfig.Options.ElectionTickCount,
			)
		}

		if builder.config.RaftConfig.FeatureFlags.EnableReadIndex != customConfig.FeatureFlags.EnableReadIndex {
			t.Errorf(
				"expected EnableReadIndex %v, got %v",
				customConfig.FeatureFlags.EnableReadIndex,
				builder.config.RaftConfig.FeatureFlags.EnableReadIndex,
			)
		}
	})
}

func TestRaftLockServerBuilder_WithTimeouts(t *testing.T) {
	t.Run("sets all timeouts", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		requestTimeout := 45 * time.Second
		shutdownTimeout := 15 * time.Second
		redirectTimeout := 8 * time.Second

		result := builder.WithTimeouts(requestTimeout, shutdownTimeout, redirectTimeout)

		assertBuilderChaining(t, builder, result, "WithTimeouts")

		if builder.config.RequestTimeout != requestTimeout {
			t.Errorf(
				"expected RequestTimeout %v, got %v",
				requestTimeout,
				builder.config.RequestTimeout,
			)
		}
		if builder.config.ShutdownTimeout != shutdownTimeout {
			t.Errorf(
				"expected ShutdownTimeout %v, got %v",
				shutdownTimeout,
				builder.config.ShutdownTimeout,
			)
		}
		if builder.config.RedirectTimeout != redirectTimeout {
			t.Errorf(
				"expected RedirectTimeout %v, got %v",
				redirectTimeout,
				builder.config.RedirectTimeout,
			)
		}
	})

	t.Run("preserves defaults for zero values", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		originalRequest := builder.config.RequestTimeout
		originalShutdown := builder.config.ShutdownTimeout
		originalRedirect := builder.config.RedirectTimeout

		builder.WithTimeouts(0, 0, 0)

		if builder.config.RequestTimeout != originalRequest {
			t.Error("expected RequestTimeout to remain unchanged for zero value")
		}
		if builder.config.ShutdownTimeout != originalShutdown {
			t.Error("expected ShutdownTimeout to remain unchanged for zero value")
		}
		if builder.config.RedirectTimeout != originalRedirect {
			t.Error("expected RedirectTimeout to remain unchanged for zero value")
		}
	})

	t.Run("preserves defaults for negative values", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		originalRequest := builder.config.RequestTimeout

		builder.WithTimeouts(-5*time.Second, 10*time.Second, 5*time.Second)

		if builder.config.RequestTimeout != originalRequest {
			t.Error("expected RequestTimeout to remain unchanged for negative value")
		}
		if builder.config.ShutdownTimeout != 10*time.Second {
			t.Error("expected positive ShutdownTimeout to be set")
		}
	})
}

func TestRaftLockServerBuilder_WithLimits(t *testing.T) {
	t.Run("sets all limits", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		maxRequestSize := 8 * 1024 * 1024
		maxResponseSize := 16 * 1024 * 1024
		maxConcurrentReqs := 2000

		result := builder.WithLimits(maxRequestSize, maxResponseSize, maxConcurrentReqs)

		assertBuilderChaining(t, builder, result, "WithLimits")

		if builder.config.MaxRequestSize != maxRequestSize {
			t.Errorf(
				"expected MaxRequestSize %d, got %d",
				maxRequestSize,
				builder.config.MaxRequestSize,
			)
		}
		if builder.config.MaxResponseSize != maxResponseSize {
			t.Errorf(
				"expected MaxResponseSize %d, got %d",
				maxResponseSize,
				builder.config.MaxResponseSize,
			)
		}
		if builder.config.MaxConcurrentReqs != maxConcurrentReqs {
			t.Errorf(
				"expected MaxConcurrentReqs %d, got %d",
				maxConcurrentReqs,
				builder.config.MaxConcurrentReqs,
			)
		}
	})

	t.Run("preserves defaults for zero and negative values", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		originalRequest := builder.config.MaxRequestSize
		originalResponse := builder.config.MaxResponseSize
		originalConcurrent := builder.config.MaxConcurrentReqs

		builder.WithLimits(0, -100, 0)

		if builder.config.MaxRequestSize != originalRequest {
			t.Error("expected MaxRequestSize to remain unchanged for zero value")
		}
		if builder.config.MaxResponseSize != originalResponse {
			t.Error("expected MaxResponseSize to remain unchanged for negative value")
		}
		if builder.config.MaxConcurrentReqs != originalConcurrent {
			t.Error("expected MaxConcurrentReqs to remain unchanged for zero value")
		}
	})
}

func TestRaftLockServerBuilder_WithRateLimit(t *testing.T) {
	t.Run("enables rate limiting with custom values", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		rateLimit := 200
		burst := 300
		window := 2 * time.Second

		result := builder.WithRateLimit(true, rateLimit, burst, window)

		assertBuilderChaining(t, builder, result, "WithRateLimit")

		if !builder.config.EnableRateLimit {
			t.Error("expected EnableRateLimit to be true")
		}
		if builder.config.RateLimit != rateLimit {
			t.Errorf("expected RateLimit %d, got %d", rateLimit, builder.config.RateLimit)
		}
		if builder.config.RateLimitBurst != burst {
			t.Errorf("expected RateLimitBurst %d, got %d", burst, builder.config.RateLimitBurst)
		}
		if builder.config.RateLimitWindow != window {
			t.Errorf("expected RateLimitWindow %v, got %v", window, builder.config.RateLimitWindow)
		}
	})

	t.Run("disables rate limiting", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		builder.WithRateLimit(true, 100, 200, time.Second)

		result := builder.WithRateLimit(false, 500, 600, 5*time.Second)

		assertBuilderChaining(t, builder, result, "WithRateLimit")

		if builder.config.EnableRateLimit {
			t.Error("expected EnableRateLimit to be false")
		}
	})

	t.Run("preserves defaults for zero values when enabled", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		originalLimit := builder.config.RateLimit
		originalBurst := builder.config.RateLimitBurst
		originalWindow := builder.config.RateLimitWindow

		builder.WithRateLimit(true, 0, 0, 0)

		if !builder.config.EnableRateLimit {
			t.Error("expected EnableRateLimit to be true")
		}
		if builder.config.RateLimit != originalLimit {
			t.Error("expected RateLimit to remain unchanged for zero value")
		}
		if builder.config.RateLimitBurst != originalBurst {
			t.Error("expected RateLimitBurst to remain unchanged for zero value")
		}
		if builder.config.RateLimitWindow != originalWindow {
			t.Error("expected RateLimitWindow to remain unchanged for zero value")
		}
	})
}

func TestRaftLockServerBuilder_WithHealthCheck(t *testing.T) {
	t.Run("sets health check values", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		interval := 45 * time.Second
		timeout := 8 * time.Second

		result := builder.WithHealthCheck(interval, timeout)

		assertBuilderChaining(t, builder, result, "WithHealthCheck")

		if builder.config.HealthCheckInterval != interval {
			t.Errorf(
				"expected HealthCheckInterval %v, got %v",
				interval,
				builder.config.HealthCheckInterval,
			)
		}
		if builder.config.HealthCheckTimeout != timeout {
			t.Errorf(
				"expected HealthCheckTimeout %v, got %v",
				timeout,
				builder.config.HealthCheckTimeout,
			)
		}
	})

	t.Run("preserves defaults for zero values", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		originalInterval := builder.config.HealthCheckInterval
		originalTimeout := builder.config.HealthCheckTimeout

		builder.WithHealthCheck(0, 0)

		if builder.config.HealthCheckInterval != originalInterval {
			t.Error("expected HealthCheckInterval to remain unchanged for zero value")
		}
		if builder.config.HealthCheckTimeout != originalTimeout {
			t.Error("expected HealthCheckTimeout to remain unchanged for zero value")
		}
	})
}

func TestRaftLockServerBuilder_WithLogger(t *testing.T) {
	t.Run("sets custom logger", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		customLogger := logger.NewNoOpLogger()

		result := builder.WithLogger(customLogger)

		assertBuilderChaining(t, builder, result, "WithLogger")

		if builder.config.Logger != customLogger {
			t.Error("expected logger to be set to the provided custom logger")
		}
	})

	t.Run("accepts nil logger", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		builder.WithLogger(nil)

		if builder.config.Logger != nil {
			t.Error("expected logger to be nil when nil is provided")
		}
	})
}

func TestRaftLockServerBuilder_WithMetrics(t *testing.T) {
	t.Run("sets custom metrics", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		customMetrics := NewNoOpServerMetrics()

		result := builder.WithMetrics(customMetrics)

		assertBuilderChaining(t, builder, result, "WithMetrics")

		if builder.config.Metrics != customMetrics {
			t.Error("expected metrics to be set to the provided custom metrics")
		}
	})

	t.Run("accepts nil metrics", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		builder.WithMetrics(nil)

		if builder.config.Metrics != nil {
			t.Error("expected metrics to be nil when nil is provided")
		}
	})
}

func TestRaftLockServerBuilder_WithSerializer(t *testing.T) {
	t.Run("sets custom serializer", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		customSerializer := &lock.JSONSerializer{}

		result := builder.WithSerializer(customSerializer)

		assertBuilderChaining(t, builder, result, "WithSerializer")

		if builder.config.Serializer != customSerializer {
			t.Error("expected serializer to be set to the provided custom serializer")
		}
	})

	t.Run("accepts nil serializer", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		builder.WithSerializer(nil)

		if builder.config.Serializer != nil {
			t.Error("expected serializer to be nil when nil is provided")
		}
	})
}

func TestRaftLockServerBuilder_WithLeaderRedirect(t *testing.T) {
	t.Run("enables leader redirect", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		result := builder.WithLeaderRedirect(true)

		assertBuilderChaining(t, builder, result, "WithLeaderRedirect")

		if !builder.config.EnableLeaderRedirect {
			t.Error("expected EnableLeaderRedirect to be true")
		}
	})

	t.Run("disables leader redirect", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		result := builder.WithLeaderRedirect(false)

		assertBuilderChaining(t, builder, result, "WithLeaderRedirect")

		if builder.config.EnableLeaderRedirect {
			t.Error("expected EnableLeaderRedirect to be false")
		}
	})
}

func TestRaftLockServerBuilder_prepareConfig(t *testing.T) {
	t.Run("sets RaftConfig.ID from NodeID", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		nodeID := types.NodeID("test-node")

		builder.WithNodeID(nodeID)
		builder.prepareConfig()

		if builder.config.RaftConfig.ID != nodeID {
			t.Errorf(
				"expected RaftConfig.ID to be set to %q, got %q",
				nodeID,
				builder.config.RaftConfig.ID,
			)
		}
	})

	t.Run("preserves existing RaftConfig.ID if already set", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		nodeID := types.NodeID("test-node")
		existingID := types.NodeID("existing-id")

		builder.WithNodeID(nodeID)
		builder.config.RaftConfig.ID = existingID
		builder.prepareConfig()

		if builder.config.RaftConfig.ID != existingID {
			t.Errorf(
				"expected RaftConfig.ID to remain %q, got %q",
				existingID,
				builder.config.RaftConfig.ID,
			)
		}
	})

	t.Run("copies peers to RaftConfig.Peers", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		peers := createValidPeers()

		builder.WithPeers(peers)
		builder.prepareConfig()

		if len(builder.config.RaftConfig.Peers) != len(peers) {
			t.Errorf("expected %d peers in RaftConfig.Peers, got %d",
				len(peers), len(builder.config.RaftConfig.Peers))
		}

		for id, cfg := range peers {
			if builder.config.RaftConfig.Peers[id] != cfg {
				t.Errorf("peer %q not properly copied to RaftConfig.Peers", id)
			}
		}
	})

	t.Run("preserves existing RaftConfig.Peers if already set", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		peers := createValidPeers()
		existingPeers := map[types.NodeID]raft.PeerConfig{
			"existing": {ID: "existing", Address: "localhost:9999"},
		}

		builder.WithPeers(peers)
		builder.config.RaftConfig.Peers = existingPeers
		builder.prepareConfig()

		if len(builder.config.RaftConfig.Peers) == len(peers) {
			t.Error("expected existing RaftConfig.Peers to be preserved")
		}
	})

	t.Run("sets default dependencies", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()

		builder.config.Logger = nil
		builder.config.Metrics = nil
		builder.config.Clock = nil
		builder.config.Serializer = nil

		builder.prepareConfig()

		if builder.config.Logger == nil {
			t.Error("expected Logger to be set to default")
		}
		if builder.config.Metrics == nil {
			t.Error("expected Metrics to be set to default")
		}
		if builder.config.Clock == nil {
			t.Error("expected Clock to be set to default")
		}
		if builder.config.Serializer == nil {
			t.Error("expected Serializer to be set to default")
		}
	})

	t.Run("auto-sets listen address from peer config", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		nodeID := types.NodeID("test-node")
		expectedAddr := "localhost:9090"
		peers := map[types.NodeID]raft.PeerConfig{
			nodeID: {ID: nodeID, Address: expectedAddr},
		}

		builder.WithNodeID(nodeID)
		builder.WithPeers(peers)
		// Don't explicitly set listen address

		builder.prepareConfig()

		if builder.config.ListenAddress != expectedAddr {
			t.Errorf("expected ListenAddress to be auto-set to %q, got %q",
				expectedAddr, builder.config.ListenAddress)
		}
	})

	t.Run("preserves explicit listen address", func(t *testing.T) {
		builder := NewRaftLockServerBuilder()
		nodeID := types.NodeID("test-node")
		explicitAddr := "localhost:7777"
		peerAddr := "localhost:8888"
		peers := map[types.NodeID]raft.PeerConfig{
			nodeID: {ID: nodeID, Address: peerAddr},
		}

		builder.WithNodeID(nodeID)
		builder.WithListenAddress(explicitAddr)
		builder.WithPeers(peers)

		builder.prepareConfig()

		if builder.config.ListenAddress != explicitAddr {
			t.Errorf("expected explicit ListenAddress %q to be preserved, got %q",
				explicitAddr, builder.config.ListenAddress)
		}
	})
}

func TestRaftLockServerBuilder_Build(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		tests := []struct {
			name          string
			setup         func(*RaftLockServerBuilder)
			expectedError string
		}{
			{
				name: "missing NodeID",
				setup: func(b *RaftLockServerBuilder) {
					b.WithDataDir("/tmp/test").WithPeers(createValidPeers())
				},
				expectedError: "NodeID must be set using WithNodeID",
			},
			{
				name: "missing DataDir",
				setup: func(b *RaftLockServerBuilder) {
					b.WithNodeID("test").WithPeers(createValidPeers())
				},
				expectedError: "DataDir must be set using WithDataDir",
			},
			{
				name: "missing Peers",
				setup: func(b *RaftLockServerBuilder) {
					b.WithNodeID("test").WithDataDir("/tmp/test")
				},
				expectedError: "Peers must be set using WithPeers",
			},
			{
				name: "nil Peers",
				setup: func(b *RaftLockServerBuilder) {
					b.WithNodeID("test").WithDataDir("/tmp/test").WithPeers(nil)
				},
				expectedError: "Peers must be set using WithPeers",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				builder := NewRaftLockServerBuilder()
				tt.setup(builder)

				server, err := builder.Build()

				if server != nil {
					t.Error("expected nil server when required field is missing")
				}
				if err == nil {
					t.Error("expected error when required field is missing")
				}
				if err != nil && !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("expected error containing %q, got %q", tt.expectedError, err.Error())
				}
			})
		}
	})
}

func TestRaftLockServerQuickBuild(t *testing.T) {
	t.Run("creates server with minimal config", func(t *testing.T) {
		nodeID := types.NodeID("quick-node")
		listenAddr := "localhost:7777"
		peers := map[types.NodeID]raft.PeerConfig{
			nodeID: {ID: nodeID, Address: listenAddr},
		}
		dataDir := "/tmp/quick-test"

		server, err := RaftLockServerQuickBuild(nodeID, listenAddr, peers, dataDir)

		if err != nil {
			raftLockServerConfigError := &RaftLockServerConfigError{}
			if errors.As(err, &raftLockServerConfigError) {
				t.Errorf("expected no validation error in QuickBuild, got: %v", err)
			}

			t.Logf("QuickBuild failed with non-validation error (expected): %v", err)
		} else if server == nil {
			t.Error("expected non-nil server when no error returned")
		}
	})

	t.Run("validates required parameters", func(t *testing.T) {
		tests := []struct {
			name      string
			nodeID    types.NodeID
			addr      string
			peers     map[types.NodeID]raft.PeerConfig
			dataDir   string
			expectErr bool
		}{
			{
				name:   "empty node ID",
				nodeID: "",
				addr:   "localhost:8080",
				peers: map[types.NodeID]raft.PeerConfig{
					"test": {ID: "test", Address: "localhost:8080"},
				},
				dataDir:   "/tmp/test",
				expectErr: true,
			},
			{
				name:   "empty data dir",
				nodeID: "test",
				addr:   "localhost:8080",
				peers: map[types.NodeID]raft.PeerConfig{
					"test": {ID: "test", Address: "localhost:8080"},
				},
				dataDir:   "",
				expectErr: true,
			},
			{
				name:      "nil peers",
				nodeID:    "test",
				addr:      "localhost:8080",
				peers:     nil,
				dataDir:   "/tmp/test",
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				server, err := RaftLockServerQuickBuild(tt.nodeID, tt.addr, tt.peers, tt.dataDir)

				if tt.expectErr {
					if err == nil {
						t.Error("expected error for invalid parameters")
					}
					if server != nil {
						t.Error("expected nil server for invalid parameters")
					}
				}
			})
		}
	})
}

func TestRaftLockServerBuilder_MethodChaining(t *testing.T) {
	t.Run("supports fluent interface", func(t *testing.T) {
		nodeID := types.NodeID("chain-node")
		dataDir := "/tmp/chain-test"
		peers := map[types.NodeID]raft.PeerConfig{
			nodeID: {ID: nodeID, Address: "localhost:8888"},
		}

		builder := NewRaftLockServerBuilder().
			WithNodeID(nodeID).
			WithListenAddress("localhost:8888").
			WithPeers(peers).
			WithDataDir(dataDir).
			WithTimeouts(30*time.Second, 10*time.Second, 5*time.Second).
			WithLimits(1024*1024, 2*1024*1024, 500).
			WithRateLimit(true, 50, 100, time.Second).
			WithHealthCheck(20*time.Second, 3*time.Second).
			WithLogger(logger.NewNoOpLogger()).
			WithMetrics(NewNoOpServerMetrics()).
			WithSerializer(&lock.JSONSerializer{}).
			WithLeaderRedirect(true)

		if builder == nil {
			t.Fatal("expected chaining to return non-nil builder")
		}

		if builder.config.NodeID != nodeID {
			t.Error("chaining did not preserve NodeID")
		}
		if builder.config.DataDir != dataDir {
			t.Error("chaining did not preserve DataDir")
		}
		if !builder.config.EnableRateLimit {
			t.Error("chaining did not preserve EnableRateLimit")
		}
		if !builder.config.EnableLeaderRedirect {
			t.Error("chaining did not preserve EnableLeaderRedirect")
		}
		if builder.config.RequestTimeout != 30*time.Second {
			t.Error("chaining did not preserve RequestTimeout")
		}
	})
}
