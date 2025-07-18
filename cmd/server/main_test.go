package main

import (
	"flag"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/server"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name        string
		peersStr    string
		expected    map[types.NodeID]raft.PeerConfig
		expectError bool
		errorMsg    string
	}{
		{
			name:     "empty string",
			peersStr: "",
			expected: make(map[types.NodeID]raft.PeerConfig),
		},
		{
			name:     "single peer",
			peersStr: "node1=localhost:8080",
			expected: map[types.NodeID]raft.PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
			},
		},
		{
			name:     "multiple peers",
			peersStr: "node1=localhost:8080,node2=localhost:8081,node3=localhost:8082",
			expected: map[types.NodeID]raft.PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
				"node2": {ID: "node2", Address: "localhost:8081"},
				"node3": {ID: "node3", Address: "localhost:8082"},
			},
		},
		{
			name:     "peers with whitespace",
			peersStr: " node1 = localhost:8080 , node2 = localhost:8081 ",
			expected: map[types.NodeID]raft.PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
				"node2": {ID: "node2", Address: "localhost:8081"},
			},
		},
		{
			name:     "IPv6 addresses",
			peersStr: "node1=[::1]:8080,node2=[2001:db8::1]:8081",
			expected: map[types.NodeID]raft.PeerConfig{
				"node1": {ID: "node1", Address: "[::1]:8080"},
				"node2": {ID: "node2", Address: "[2001:db8::1]:8081"},
			},
		},
		{
			name:     "domains and IPs mixed",
			peersStr: "node1=example.com:8080,node2=127.0.0.1:8081,node3=192.168.1.100:8082",
			expected: map[types.NodeID]raft.PeerConfig{
				"node1": {ID: "node1", Address: "example.com:8080"},
				"node2": {ID: "node2", Address: "127.0.0.1:8081"},
				"node3": {ID: "node3", Address: "192.168.1.100:8082"},
			},
		},
		{
			name:        "invalid format - missing equals",
			peersStr:    "node1-localhost:8080",
			expectError: true,
			errorMsg:    "invalid peer format",
		},
		{
			name:        "invalid format - too many equals",
			peersStr:    "node1=localhost:8080=extra",
			expectError: true,
			errorMsg:    "invalid peer format",
		},
		{
			name:        "invalid format - empty node ID",
			peersStr:    "=localhost:8080",
			expectError: true,
			errorMsg:    "invalid peer entry",
		},
		{
			name:        "invalid format - empty address",
			peersStr:    "node1=",
			expectError: true,
			errorMsg:    "invalid peer entry",
		},
		{
			name:        "invalid format - only whitespace",
			peersStr:    "   =   ",
			expectError: true,
			errorMsg:    "invalid peer entry",
		},
		{
			name:        "mixed valid and invalid",
			peersStr:    "node1=localhost:8080,invalid-entry",
			expectError: true,
			errorMsg:    "invalid peer format",
		},
		{
			name:        "duplicate node IDs",
			peersStr:    "node1=localhost:8080,node1=localhost:8081",
			expectError: false, // Last entry wins
			expected: map[types.NodeID]raft.PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8081"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parsePeers(tt.peersStr)

			if tt.expectError {
				testutil.AssertError(t, err, "Expected error for input: %s", tt.peersStr)
				if tt.errorMsg != "" {
					testutil.AssertContains(
						t,
						err.Error(),
						tt.errorMsg,
						"Error message should contain expected text",
					)
				}
				return
			}

			testutil.AssertNoError(t, err, "Unexpected error for input: %s", tt.peersStr)
			testutil.AssertEqual(t, tt.expected, result, "Peer configuration mismatch")

			for nodeID, config := range result {
				testutil.AssertEqual(t, nodeID, config.ID, "Peer ID should match map key")
			}
		})
	}
}

func TestFormatPeers(t *testing.T) {
	tests := []struct {
		name     string
		peers    map[types.NodeID]raft.PeerConfig
		expected int // Expected number of formatted strings
	}{
		{
			name:     "empty peers",
			peers:    make(map[types.NodeID]raft.PeerConfig),
			expected: 0,
		},
		{
			name: "single peer",
			peers: map[types.NodeID]raft.PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
			},
			expected: 1,
		},
		{
			name: "multiple peers",
			peers: map[types.NodeID]raft.PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
				"node2": {ID: "node2", Address: "localhost:8081"},
				"node3": {ID: "node3", Address: "localhost:8082"},
			},
			expected: 3,
		},
		{
			name: "peers with special characters",
			peers: map[types.NodeID]raft.PeerConfig{
				"node-1": {ID: "node-1", Address: "127.0.0.1:8080"},
				"node_2": {ID: "node_2", Address: "192.168.1.1:9090"},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatPeers(tt.peers)

			testutil.AssertLen(t, result, tt.expected, "Length mismatch")

			resultMap := make(map[string]bool)
			for _, entry := range result {
				resultMap[entry] = true

				parts := strings.Split(entry, "=")
				testutil.AssertLen(t, parts, 2, "Each entry should have exactly one '=' character")
				testutil.AssertTrue(t, parts[0] != "", "Node ID should not be empty")
				testutil.AssertTrue(t, parts[1] != "", "Address should not be empty")
			}

			for nodeID, config := range tt.peers {
				expected := string(nodeID) + "=" + config.Address
				testutil.AssertTrue(t, resultMap[expected], "Should contain entry: %s", expected)
			}
		})
	}
}

func TestParseAndValidateFlags_Version(t *testing.T) {
	testWithFlags(t, []string{"--version"}, func(t *testing.T) {
		cfg, err := parseAndValidateFlags()
		testutil.AssertNoError(t, err, "Should not error with --version flag")
		testutil.AssertTrue(t, cfg.ShowVersion, "ShowVersion should be true")
	})
}

func TestParseAndValidateFlags_RequiredFields(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "missing node-id",
			args:        []string{"--peers", "node1=localhost:8080", "--data-dir", "/tmp/data"},
			expectError: true,
			errorMsg:    "--node-id is required",
		},
		{
			name:        "missing peers",
			args:        []string{"--node-id", "node1", "--data-dir", "/tmp/data"},
			expectError: true,
			errorMsg:    "--peers is required",
		},
		{
			name: "empty node-id",
			args: []string{
				"--node-id",
				"",
				"--peers",
				"node1=localhost:8080",
				"--data-dir",
				"/tmp/data",
			},
			expectError: true,
			errorMsg:    "--node-id is required",
		},
		{
			name:        "empty peers",
			args:        []string{"--node-id", "node1", "--peers", "", "--data-dir", "/tmp/data"},
			expectError: true,
			errorMsg:    "--peers is required",
		},
		{
			name: "explicit empty data-dir",
			args: []string{
				"--node-id",
				"node1",
				"--peers",
				"node1=localhost:8080",
				"--data-dir",
				"",
			},
			expectError: true,
			errorMsg:    "--data-dir is required",
		},
		{
			name: "invalid peer format",
			args: []string{
				"--node-id",
				"node1",
				"--peers",
				"invalid-format",
				"--data-dir",
				"/tmp/data",
			},
			expectError: true,
			errorMsg:    "invalid peer configuration",
		},
		{
			name: "node not in peers list",
			args: []string{
				"--node-id",
				"node1",
				"--peers",
				"node2=localhost:8081,node3=localhost:8082",
				"--data-dir",
				"/tmp/data",
			},
			expectError: true,
			errorMsg:    "current node-id \"node1\" must be included in the peers list",
		},
		{
			name: "all required fields present",
			args: []string{
				"--node-id",
				"node1",
				"--peers",
				"node1=localhost:8080",
				"--data-dir",
				"/tmp/data",
			},
		},
		{
			name: "using default data-dir",
			args: []string{"--node-id", "node1", "--peers", "node1=localhost:8080"},
		},
		{
			name: "multiple valid peers",
			args: []string{
				"--node-id",
				"node1",
				"--peers",
				"node1=localhost:8080,node2=localhost:8081,node3=localhost:8082",
				"--data-dir",
				"/tmp/data",
			},
		},
		{
			name: "complex node IDs",
			args: []string{
				"--node-id",
				"raft-node-001",
				"--peers",
				"raft-node-001=127.0.0.1:8080,raft-node-002=127.0.0.1:8081",
				"--data-dir",
				"/tmp/data",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testWithFlags(t, tt.args, func(t *testing.T) {
				cfg, err := parseAndValidateFlags()

				if tt.expectError {
					testutil.AssertError(t, err, "Expected error for test case: %s", tt.name)
					testutil.AssertContains(
						t,
						err.Error(),
						tt.errorMsg,
						"Error message should contain expected text",
					)
					return
				}

				testutil.AssertNoError(t, err, "Unexpected error for test case: %s", tt.name)
				testutil.AssertNotNil(t, cfg, "Config should not be nil")

				if !cfg.ShowVersion {
					testutil.AssertNotNil(
						t,
						cfg.ServerConfig.Peers,
						"Peers should be parsed and set",
					)
					testutil.AssertTrue(
						t,
						len(cfg.ServerConfig.Peers) > 0,
						"Should have at least one peer",
					)

					_, exists := cfg.ServerConfig.Peers[cfg.ServerConfig.NodeID]
					testutil.AssertTrue(t, exists, "Current node should be in peers list")

					for nodeID, peerConfig := range cfg.ServerConfig.Peers {
						testutil.AssertEqual(
							t,
							nodeID,
							peerConfig.ID,
							"Peer ID should match map key",
						)
						testutil.AssertTrue(
							t,
							peerConfig.Address != "",
							"Peer address should not be empty",
						)
					}
				}
			})
		})
	}
}

func TestParseAndValidateFlags_Defaults(t *testing.T) {
	testWithFlags(
		t,
		[]string{"--node-id", "node1", "--peers", "node1=localhost:8080"},
		func(t *testing.T) {
			cfg, err := parseAndValidateFlags()
			testutil.AssertNoError(t, err, "Should not error with valid required fields")

			testutil.AssertEqual(
				t,
				"0.0.0.0:8080",
				cfg.ServerConfig.ListenAddress,
				"Default listen address",
			)
			testutil.AssertEqual(t, "./data", cfg.ServerConfig.DataDir, "Default data dir")
			testutil.AssertEqual(
				t,
				4*1024*1024,
				cfg.ServerConfig.MaxRequestSize,
				"Default max request size",
			)
			testutil.AssertEqual(
				t,
				1000,
				cfg.ServerConfig.MaxConcurrentReqs,
				"Default max concurrent requests",
			)
			testutil.AssertEqual(
				t,
				30*time.Second,
				cfg.ServerConfig.RequestTimeout,
				"Default request timeout",
			)
			testutil.AssertEqual(
				t,
				30*time.Second,
				cfg.ServerConfig.ShutdownTimeout,
				"Default shutdown timeout",
			)

			testutil.AssertEqual(t, "info", cfg.LogLevel, "Default log level")
			testutil.AssertFalse(t, cfg.EnablePprof, "Default pprof setting")
			testutil.AssertFalse(t, cfg.ShowVersion, "Default version setting")

			testutil.AssertEqual(
				t,
				1,
				cfg.ServerConfig.RaftConfig.Options.HeartbeatTickCount,
				"Default heartbeat ticks",
			)
			testutil.AssertEqual(
				t,
				24,
				cfg.ServerConfig.RaftConfig.Options.ElectionTickCount,
				"Default election ticks",
			)
			testutil.AssertEqual(
				t,
				10000,
				cfg.ServerConfig.RaftConfig.Options.SnapshotThreshold,
				"Default snapshot threshold",
			)

			testutil.AssertFalse(t, cfg.ServerConfig.EnableRateLimit, "Default rate limit enabled")
			testutil.AssertEqual(t, 100, cfg.ServerConfig.RateLimit, "Default rate limit RPS")
			testutil.AssertEqual(
				t,
				200,
				cfg.ServerConfig.RateLimitBurst,
				"Default rate limit burst",
			)
		},
	)
}

func TestParseAndValidateFlags_CustomValues(t *testing.T) {
	customArgs := []string{
		"--node-id", "custom-node",
		"--listen", "127.0.0.1:9999",
		"--peers", "custom-node=127.0.0.1:9999,node2=localhost:8081",
		"--data-dir", "/custom/data",
		"--log-level", "debug",
		"--pprof",
		"--max-request-size", "8388608", // 8MB
		"--max-concurrent", "2000",
		"--request-timeout", "60s",
		"--shutdown-timeout", "45s",
		"--rate-limit",
		"--rate-limit-rps", "200",
		"--rate-limit-burst", "400",
		"--heartbeat-ticks", "2",
		"--election-ticks", "20",
		"--snapshot-threshold", "20000",
	}

	testWithFlags(t, customArgs, func(t *testing.T) {
		cfg, err := parseAndValidateFlags()
		testutil.AssertNoError(t, err, "Should not error with custom values")

		testutil.AssertEqual(
			t,
			types.NodeID("custom-node"),
			cfg.ServerConfig.NodeID,
			"Custom node ID",
		)
		testutil.AssertEqual(
			t,
			"127.0.0.1:9999",
			cfg.ServerConfig.ListenAddress,
			"Custom listen address",
		)
		testutil.AssertEqual(t, "/custom/data", cfg.ServerConfig.DataDir, "Custom data dir")
		testutil.AssertEqual(t, "debug", cfg.LogLevel, "Custom log level")
		testutil.AssertTrue(t, cfg.EnablePprof, "Custom pprof setting")
		testutil.AssertEqual(t, 8388608, cfg.ServerConfig.MaxRequestSize, "Custom max request size")
		testutil.AssertEqual(
			t,
			2000,
			cfg.ServerConfig.MaxConcurrentReqs,
			"Custom max concurrent requests",
		)
		testutil.AssertEqual(
			t,
			60*time.Second,
			cfg.ServerConfig.RequestTimeout,
			"Custom request timeout",
		)
		testutil.AssertEqual(
			t,
			45*time.Second,
			cfg.ServerConfig.ShutdownTimeout,
			"Custom shutdown timeout",
		)
		testutil.AssertTrue(t, cfg.ServerConfig.EnableRateLimit, "Custom rate limit enabled")
		testutil.AssertEqual(t, 200, cfg.ServerConfig.RateLimit, "Custom rate limit RPS")
		testutil.AssertEqual(t, 400, cfg.ServerConfig.RateLimitBurst, "Custom rate limit burst")
		testutil.AssertEqual(
			t,
			2,
			cfg.ServerConfig.RaftConfig.Options.HeartbeatTickCount,
			"Custom heartbeat ticks",
		)
		testutil.AssertEqual(
			t,
			20,
			cfg.ServerConfig.RaftConfig.Options.ElectionTickCount,
			"Custom election ticks",
		)
		testutil.AssertEqual(
			t,
			20000,
			cfg.ServerConfig.RaftConfig.Options.SnapshotThreshold,
			"Custom snapshot threshold",
		)

		testutil.AssertLen(t, cfg.ServerConfig.Peers, 2, "Should have 2 peers")
		testutil.AssertEqual(
			t,
			"127.0.0.1:9999",
			cfg.ServerConfig.Peers["custom-node"].Address,
			"Custom node address",
		)
		testutil.AssertEqual(
			t,
			"localhost:8081",
			cfg.ServerConfig.Peers["node2"].Address,
			"Node2 address",
		)
	})
}

func TestCreateLogger(t *testing.T) {
	tests := []struct {
		name     string
		logLevel string
	}{
		{"debug level", "debug"},
		{"info level", "info"},
		{"warn level", "warn"},
		{"error level", "error"},
		{"invalid level", "invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := createLogger(tt.logLevel)
			testutil.AssertNotNil(t, logger, "Logger should not be nil")

			logger.Infow("Test log message", "level", tt.logLevel)
		})
	}
}

func TestBuildServer(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *AppConfig
		validate func(t *testing.T, srv server.RaftLockServer, cfg *AppConfig)
	}{
		{
			name: "valid minimal configuration",
			cfg: &AppConfig{
				ServerConfig: createValidServerConfig(
					"node1",
					createTempDir(t),
					map[types.NodeID]raft.PeerConfig{
						"node1": {ID: "node1", Address: "localhost:8080"},
					},
				),
			},
			validate: func(t *testing.T, srv server.RaftLockServer, cfg *AppConfig) {
				testutil.AssertEqual(t, string(cfg.ServerConfig.NodeID), srv.GetNodeID())
				testutil.AssertFalse(t, srv.IsLeader()) // Should not be leader initially
			},
		},
		{
			name: "valid multi-node configuration",
			cfg: &AppConfig{
				ServerConfig: createValidServerConfig(
					"node1",
					createTempDir(t),
					map[types.NodeID]raft.PeerConfig{
						"node1": {ID: "node1", Address: "localhost:8080"},
						"node2": {ID: "node2", Address: "localhost:8081"},
						"node3": {ID: "node3", Address: "localhost:8082"},
					},
				),
			},
			validate: func(t *testing.T, srv server.RaftLockServer, cfg *AppConfig) {
				testutil.AssertEqual(t, string(cfg.ServerConfig.NodeID), srv.GetNodeID())
				testutil.AssertNotNil(t, srv.Metrics())
			},
		},
		{
			name: "configuration with rate limiting",
			cfg: func() *AppConfig {
				cfg := &AppConfig{
					ServerConfig: createValidServerConfig(
						"node1",
						createTempDir(t),
						map[types.NodeID]raft.PeerConfig{
							"node1": {ID: "node1", Address: "localhost:8080"},
						},
					),
				}
				cfg.ServerConfig.EnableRateLimit = true
				cfg.ServerConfig.RateLimit = 50
				cfg.ServerConfig.RateLimitBurst = 100
				return cfg
			}(),
			validate: func(t *testing.T, srv server.RaftLockServer, cfg *AppConfig) {
				testutil.AssertEqual(t, string(cfg.ServerConfig.NodeID), srv.GetNodeID())
			},
		},
		{
			name: "configuration with custom timeouts",
			cfg: func() *AppConfig {
				cfg := &AppConfig{
					ServerConfig: createValidServerConfig(
						"node1",
						createTempDir(t),
						map[types.NodeID]raft.PeerConfig{
							"node1": {ID: "node1", Address: "localhost:8080"},
						},
					),
				}
				cfg.ServerConfig.RequestTimeout = 45 * time.Second
				cfg.ServerConfig.ShutdownTimeout = 20 * time.Second
				cfg.ServerConfig.RedirectTimeout = 10 * time.Second
				return cfg
			}(),
			validate: func(t *testing.T, srv server.RaftLockServer, cfg *AppConfig) {
				testutil.AssertEqual(t, string(cfg.ServerConfig.NodeID), srv.GetNodeID())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := createLogger("info")

			server, err := buildServer(tt.cfg, logger)

			testutil.AssertNoError(t, err, "Unexpected error for test case: %s", tt.name)
			testutil.AssertNotNil(t, server, "Server should not be nil")

			if tt.validate != nil {
				tt.validate(t, server, tt.cfg)
			}
		})
	}
}

func TestBuildServer_ValidationErrors(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *AppConfig
		errorMsg string
	}{
		{
			name: "empty node ID",
			cfg: &AppConfig{
				ServerConfig: func() server.RaftLockServerConfig {
					cfg := createValidServerConfig(
						"",
						createTempDir(t),
						map[types.NodeID]raft.PeerConfig{
							"node1": {ID: "node1", Address: "localhost:8080"},
						},
					)
					return cfg
				}(),
			},
			errorMsg: "NodeID cannot be empty",
		},
		{
			name: "empty data directory",
			cfg: &AppConfig{
				ServerConfig: createValidServerConfig("node1", "", map[types.NodeID]raft.PeerConfig{
					"node1": {ID: "node1", Address: "localhost:8080"},
				}),
			},
			errorMsg: "DataDir cannot be empty",
		},
		{
			name: "nil peers",
			cfg: &AppConfig{
				ServerConfig: createValidServerConfig("node1", createTempDir(t), nil),
			},
			errorMsg: "Peers must be set using WithPeers",
		},
		{
			name: "empty peers",
			cfg: &AppConfig{
				ServerConfig: createValidServerConfig(
					"node1",
					createTempDir(t),
					map[types.NodeID]raft.PeerConfig{},
				),
			},
			errorMsg: "Peers must include an entry for NodeID",
		},
		{
			name: "node not in peers",
			cfg: &AppConfig{
				ServerConfig: createValidServerConfig(
					"node1",
					createTempDir(t),
					map[types.NodeID]raft.PeerConfig{
						"node2": {ID: "node2", Address: "localhost:8081"},
					},
				),
			},
			errorMsg: "Peers must include an entry for NodeID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := createLogger("info")

			server, err := buildServer(tt.cfg, logger)

			testutil.AssertError(t, err, "Expected error for test case: %s", tt.name)
			testutil.AssertContains(
				t,
				err.Error(),
				tt.errorMsg,
				"Error message should contain expected text",
			)
			testutil.AssertNil(t, server, "Server should be nil on error")
		})
	}
}

func TestIntegration_FlagParsingToServerBuilding(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		validate func(t *testing.T, cfg *AppConfig, srv server.RaftLockServer)
	}{
		{
			name: "single node cluster",
			args: []string{
				"--node-id", "node1",
				"--peers", "node1=localhost:8080",
				"--data-dir", createTempDir(t),
			},
			validate: func(t *testing.T, cfg *AppConfig, srv server.RaftLockServer) {
				testutil.AssertEqual(t, "node1", srv.GetNodeID())
				testutil.AssertLen(t, cfg.ServerConfig.Peers, 1)
			},
		},
		{
			name: "three node cluster",
			args: []string{
				"--node-id", "node2",
				"--peers", "node1=localhost:8080,node2=localhost:8081,node3=localhost:8082",
				"--data-dir", createTempDir(t),
				"--listen", "localhost:8081",
			},
			validate: func(t *testing.T, cfg *AppConfig, srv server.RaftLockServer) {
				testutil.AssertEqual(t, "node2", srv.GetNodeID())
				testutil.AssertLen(t, cfg.ServerConfig.Peers, 3)
				testutil.AssertEqual(t, "localhost:8081", cfg.ServerConfig.ListenAddress)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testWithFlags(t, tt.args, func(t *testing.T) {
				cfg, err := parseAndValidateFlags()
				testutil.AssertNoError(t, err, "Flag parsing should succeed")
				testutil.AssertNotNil(t, cfg, "Config should not be nil")

				logger := createLogger("info")
				server, err := buildServer(cfg, logger)
				testutil.AssertNoError(t, err, "Server building should succeed")
				testutil.AssertNotNil(t, server, "Server should not be nil")

				if tt.validate != nil {
					tt.validate(t, cfg, server)
				}
			})
		})
	}
}

func TestRun_VersionFlag(t *testing.T) {
	testWithFlags(t, []string{"--version"}, func(t *testing.T) {
		err := run()
		testutil.AssertNoError(t, err, "run() should succeed with --version flag")
	})
}

func TestRun_InvalidFlags(t *testing.T) {
	testWithFlags(t, []string{"--node-id", "node1"}, func(t *testing.T) {
		err := run()
		testutil.AssertError(t, err, "run() should fail with missing required flags")
		testutil.AssertContains(
			t,
			err.Error(),
			"configuration error",
			"Error should mention configuration",
		)
	})
}

func TestWaitForShutdown(t *testing.T) {
	t.Run("signal channel setup", func(t *testing.T) {
		logger := createLogger("info")

		done := make(chan struct{})
		go func() {
			defer close(done)
			// This would normally block, but we'll interrupt it quickly
			waitForShutdown(logger)
		}()

		// Give it a moment to set up signal handling
		time.Sleep(10 * time.Millisecond)

		// Send SIGTERM to ourselves to trigger the shutdown
		process := os.Process{Pid: os.Getpid()}
		err := process.Signal(os.Interrupt)
		testutil.AssertNoError(t, err, "Should be able to send signal")

		select {
		case <-done:
			// Success - the signal was handled
		case <-time.After(1 * time.Second):
			t.Error("waitForShutdown did not respond to signal in time")
		}
	})
}

func TestRun_Integration(t *testing.T) {
	tempDir := createTempDir(t)
	args := []string{
		"--node-id", "test-node",
		"--peers", "test-node=localhost:8080",
		"--data-dir", tempDir,
	}

	testWithFlags(t, args, func(t *testing.T) {
		cfg, err := parseAndValidateFlags()
		testutil.AssertNoError(t, err, "Flag parsing should succeed")

		logger := createLogger("info")
		server, err := buildServer(cfg, logger)
		testutil.AssertNoError(t, err, "Server building should succeed")
		testutil.AssertNotNil(t, server, "Server should be created")

		err = gracefulShutdown(server, 5*time.Second, logger)
		testutil.AssertNoError(t, err, "Graceful shutdown should succeed on unstarted server")
	})
}

func TestConstants(t *testing.T) {
	testutil.AssertEqual(t, "RaftLock Server", AppName, "App name should be correct")
	testutil.AssertEqual(t, "v1.0.0", AppVersion, "App version should be correct")
	testutil.AssertTrue(t, AppDesc != "", "App description should not be empty")
	testutil.AssertContains(
		t,
		AppDesc,
		"distributed lock",
		"Description should mention distributed lock",
	)
	testutil.AssertContains(t, AppDesc, "Raft", "Description should mention Raft")
}

func testWithFlags(t *testing.T, args []string, testFunc func(*testing.T)) {
	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	}()

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = append([]string{"test"}, args...)

	testFunc(t)
}

func createValidServerConfig(
	nodeID string,
	dataDir string,
	peers map[types.NodeID]raft.PeerConfig,
) server.RaftLockServerConfig {
	cfg := server.DefaultRaftLockServerConfig()
	cfg.NodeID = types.NodeID(nodeID)
	cfg.DataDir = dataDir
	cfg.Peers = peers
	cfg.ListenAddress = "localhost:8080"
	cfg.ClientAPIAddress = "127.0.0.1:9090"
	cfg.RequestTimeout = 30 * time.Second
	cfg.ShutdownTimeout = 30 * time.Second
	cfg.MaxRequestSize = 4 * 1024 * 1024
	cfg.MaxResponseSize = 4 * 1024 * 1024
	cfg.MaxConcurrentReqs = 1000

	cfg.RaftConfig = raft.Config{
		ID:    types.NodeID(nodeID),
		Peers: peers,
		Options: raft.Options{
			HeartbeatTickCount: 1,
			ElectionTickCount:  10,
			SnapshotThreshold:  10000,
		},
	}

	return cfg
}

func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "raftlock-test-*")
	testutil.RequireNoError(t, err, "Failed to create temp directory")

	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("Failed to clean up temp directory %s: %v", dir, err)
		}
	})

	return dir
}

func TestFormatPeers_Roundtrip(t *testing.T) {
	tests := []struct {
		name     string
		peersStr string
	}{
		{
			name:     "single peer",
			peersStr: "node1=localhost:8080",
		},
		{
			name:     "multiple peers",
			peersStr: "node1=localhost:8080,node2=localhost:8081,node3=localhost:8082",
		},
		{
			name:     "complex addresses",
			peersStr: "node1=[::1]:8080,node2=example.com:443,node3=192.168.1.1:9999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peers, err := parsePeers(tt.peersStr)
			testutil.AssertNoError(t, err, "Parsing should succeed")

			formatted := formatPeers(peers)

			rejoined := strings.Join(formatted, ",")
			reparsed, err := parsePeers(rejoined)
			testutil.AssertNoError(t, err, "Reparsing should succeed")

			testutil.AssertTrue(
				t,
				reflect.DeepEqual(peers, reparsed),
				"Roundtrip should preserve peer configuration\nOriginal: %v\nReparsed: %v",
				peers,
				reparsed,
			)
		})
	}
}
