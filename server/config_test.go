package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/lock"
	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

func TestDefaultRaftLockServerConfig(t *testing.T) {
	config := DefaultRaftLockServerConfig()

	tests := []struct {
		name     string
		actual   any
		expected any
	}{
		{"ListenAddress", config.ListenAddress, "0.0.0.0:8080"},
		{"RequestTimeout", config.RequestTimeout, DefaultRequestTimeout},
		{"ShutdownTimeout", config.ShutdownTimeout, DefaultShutdownTimeout},
		{"MaxRequestSize", config.MaxRequestSize, DefaultMaxRequestSize},
		{"MaxResponseSize", config.MaxResponseSize, DefaultMaxResponseSize},
		{"MaxConcurrentReqs", config.MaxConcurrentReqs, DefaultMaxConcurrentRequests},
		{"EnableRateLimit", config.EnableRateLimit, false},
		{"RateLimit", config.RateLimit, DefaultRateLimit},
		{"RateLimitBurst", config.RateLimitBurst, DefaultRateLimitBurst},
		{"RateLimitWindow", config.RateLimitWindow, DefaultRateLimitWindow},
		{"HealthCheckInterval", config.HealthCheckInterval, DefaultHealthCheckInterval},
		{"HealthCheckTimeout", config.HealthCheckTimeout, DefaultHealthCheckTimeout},
		{"EnableLeaderRedirect", config.EnableLeaderRedirect, true},
		{"RedirectTimeout", config.RedirectTimeout, DefaultRedirectTimeout},
		{"ClientAPIAddress", config.ClientAPIAddress, "0.0.0.0:8090"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.actual != tt.expected {
				t.Errorf("Expected %s to be %v, got %v", tt.name, tt.expected, tt.actual)
			}
		})
	}

	// Test Peers initialization
	if config.Peers == nil {
		t.Fatal("Expected Peers to be initialized, got nil")
	}
	if len(config.Peers) != 0 {
		t.Errorf("Expected empty Peers map, got %d entries", len(config.Peers))
	}

	// Test Raft configuration defaults
	raftTests := []struct {
		name     string
		actual   any
		expected any
	}{
		{
			"ElectionTickCount",
			config.RaftConfig.Options.ElectionTickCount,
			raft.DefaultElectionTickCount,
		},
		{
			"HeartbeatTickCount",
			config.RaftConfig.Options.HeartbeatTickCount,
			raft.DefaultHeartbeatTickCount,
		},
		{
			"ElectionRandomizationFactor",
			config.RaftConfig.Options.ElectionRandomizationFactor,
			raft.DefaultElectionRandomizationFactor,
		},
		{
			"MaxLogEntriesPerRequest",
			config.RaftConfig.Options.MaxLogEntriesPerRequest,
			raft.DefaultMaxLogEntriesPerRequest,
		},
		{
			"SnapshotThreshold",
			config.RaftConfig.Options.SnapshotThreshold,
			raft.DefaultSnapshotThreshold,
		},
		{
			"EnableReadIndex",
			config.RaftConfig.FeatureFlags.EnableReadIndex,
			raft.DefaultEnableReadIndex,
		},
		{
			"EnableLeaderLease",
			config.RaftConfig.FeatureFlags.EnableLeaderLease,
			raft.DefaultEnableLeaderLease,
		},
		{
			"MaxApplyBatchSize",
			config.RaftConfig.TuningParams.MaxApplyBatchSize,
			raft.DefaultMaxApplyBatchSize,
		},
		{
			"MaxSnapshotChunkSize",
			config.RaftConfig.TuningParams.MaxSnapshotChunkSize,
			raft.DefaultMaxSnapshotChunkSize,
		},
	}

	for _, tt := range raftTests {
		t.Run("Raft_"+tt.name, func(t *testing.T) {
			if tt.actual != tt.expected {
				t.Errorf("Expected Raft %s to be %v, got %v", tt.name, tt.expected, tt.actual)
			}
		})
	}

	dependencyTests := []struct {
		name      string
		value     any
		typeCheck func(any) bool
	}{
		{"Logger", config.Logger, func(v any) bool { return v != nil }},
		{"Metrics", config.Metrics, func(v any) bool { return v != nil }},
		{"Clock", config.Clock, func(v any) bool { return v != nil }},
		{"Serializer", config.Serializer, func(v any) bool {
			_, ok := v.(*lock.JSONSerializer)
			return ok
		}},
	}

	for _, tt := range dependencyTests {
		t.Run("Dependency_"+tt.name, func(t *testing.T) {
			if !tt.typeCheck(tt.value) {
				t.Errorf("Expected %s to be properly initialized with correct type", tt.name)
			}
		})
	}
}

func TestRaftLockServerConfig_Validate_ValidConfig(t *testing.T) {
	config := createValidConfig()
	config.ClientAPIAddress = "localhost:8090"

	if err := config.Validate(); err != nil {
		t.Errorf("Expected valid config to pass validation, got error: %v", err)
	}
}

func TestRaftLockServerConfig_Validate_RequiredFields(t *testing.T) {
	tests := []struct {
		name           string
		modifyConfig   func(*RaftLockServerConfig)
		expectedErrMsg string
	}{
		{
			name:           "empty NodeID",
			modifyConfig:   func(c *RaftLockServerConfig) { c.NodeID = "" },
			expectedErrMsg: "NodeID cannot be empty",
		},
		{
			name:           "empty ListenAddress",
			modifyConfig:   func(c *RaftLockServerConfig) { c.ListenAddress = "" },
			expectedErrMsg: "ListenAddress cannot be empty",
		},
		{
			name:           "empty ClientAPIAddress",
			modifyConfig:   func(c *RaftLockServerConfig) { c.ClientAPIAddress = "" },
			expectedErrMsg: "ClientAPIAddress cannot be empty",
		},
		{
			name:           "empty DataDir",
			modifyConfig:   func(c *RaftLockServerConfig) { c.DataDir = "" },
			expectedErrMsg: "DataDir cannot be empty",
		},
		{
			name:           "nil Peers",
			modifyConfig:   func(c *RaftLockServerConfig) { c.Peers = nil },
			expectedErrMsg: "Peers map cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createValidConfig()
			config.ClientAPIAddress = "localhost:8090"
			tt.modifyConfig(&config)

			err := config.Validate()
			assertValidationError(t, err, tt.expectedErrMsg)
		})
	}
}

func TestRaftLockServerConfig_Validate_PeerValidation(t *testing.T) {
	tests := []struct {
		name           string
		modifyConfig   func(*RaftLockServerConfig)
		expectedErrMsg string
	}{
		{
			name: "NodeID not in Peers",
			modifyConfig: func(c *RaftLockServerConfig) {
				c.NodeID = "missing-node"
			},
			expectedErrMsg: `Peers must include an entry for NodeID "missing-node"`,
		},
		{
			name: "empty peer ID",
			modifyConfig: func(c *RaftLockServerConfig) {
				c.Peers[""] = raft.PeerConfig{Address: "localhost:8081"}
			},
			expectedErrMsg: "Peer ID cannot be empty",
		},
		{
			name: "empty peer address",
			modifyConfig: func(c *RaftLockServerConfig) {
				c.Peers["node3"] = raft.PeerConfig{Address: ""}
			},
			expectedErrMsg: `Peer "node3" must have a non-empty address`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createValidConfig()
			config.ClientAPIAddress = "localhost:8090"
			tt.modifyConfig(&config)

			err := config.Validate()
			assertValidationError(t, err, tt.expectedErrMsg)
		})
	}
}

func TestRaftLockServerConfig_Validate_RaftConfigValidation(t *testing.T) {
	t.Run("RaftConfig.ID mismatch", func(t *testing.T) {
		config := createValidConfig()
		config.ClientAPIAddress = "localhost:8090"
		config.RaftConfig.ID = "different-id"

		err := config.Validate()
		assertValidationError(t, err, "RaftConfig.ID (different-id) must match NodeID (node1)")
	})

	t.Run("empty RaftConfig.ID allowed", func(t *testing.T) {
		config := createValidConfig()
		config.ClientAPIAddress = "localhost:8090"
		config.RaftConfig.ID = ""

		if err := config.Validate(); err != nil {
			t.Errorf("Expected empty RaftConfig.ID to be allowed, got error: %v", err)
		}
	})
}

func TestRaftLockServerConfig_Validate_TimeoutValues(t *testing.T) {
	tests := []struct {
		name         string
		modifyConfig func(*RaftLockServerConfig)
		errorMsg     string
	}{
		{
			name:         "zero RequestTimeout",
			modifyConfig: func(c *RaftLockServerConfig) { c.RequestTimeout = 0 },
			errorMsg:     "RequestTimeout must be positive",
		},
		{
			name:         "negative RequestTimeout",
			modifyConfig: func(c *RaftLockServerConfig) { c.RequestTimeout = -time.Second },
			errorMsg:     "RequestTimeout must be positive",
		},
		{
			name:         "zero ShutdownTimeout",
			modifyConfig: func(c *RaftLockServerConfig) { c.ShutdownTimeout = 0 },
			errorMsg:     "ShutdownTimeout must be positive",
		},
		{
			name:         "zero HealthCheckInterval",
			modifyConfig: func(c *RaftLockServerConfig) { c.HealthCheckInterval = 0 },
			errorMsg:     "HealthCheckInterval must be positive",
		},
		{
			name:         "zero HealthCheckTimeout",
			modifyConfig: func(c *RaftLockServerConfig) { c.HealthCheckTimeout = 0 },
			errorMsg:     "HealthCheckTimeout must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createValidConfig()
			config.ClientAPIAddress = "localhost:8090"
			tt.modifyConfig(&config)

			err := config.Validate()
			assertValidationError(t, err, tt.errorMsg)
		})
	}
}

func TestRaftLockServerConfig_Validate_IntegerValues(t *testing.T) {
	tests := []struct {
		name         string
		modifyConfig func(*RaftLockServerConfig)
		errorMsg     string
	}{
		{
			name:         "zero MaxRequestSize",
			modifyConfig: func(c *RaftLockServerConfig) { c.MaxRequestSize = 0 },
			errorMsg:     "MaxRequestSize must be positive",
		},
		{
			name:         "negative MaxRequestSize",
			modifyConfig: func(c *RaftLockServerConfig) { c.MaxRequestSize = -1 },
			errorMsg:     "MaxRequestSize must be positive",
		},
		{
			name:         "zero MaxResponseSize",
			modifyConfig: func(c *RaftLockServerConfig) { c.MaxResponseSize = 0 },
			errorMsg:     "MaxResponseSize must be positive",
		},
		{
			name:         "zero MaxConcurrentReqs",
			modifyConfig: func(c *RaftLockServerConfig) { c.MaxConcurrentReqs = 0 },
			errorMsg:     "MaxConcurrentReqs must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createValidConfig()
			config.ClientAPIAddress = "localhost:8090"
			tt.modifyConfig(&config)

			err := config.Validate()
			assertValidationError(t, err, tt.errorMsg)
		})
	}
}

func TestRaftLockServerConfig_Validate_RateLimiting(t *testing.T) {
	t.Run("disabled rate limiting ignores invalid values", func(t *testing.T) {
		config := createValidConfig()
		config.ClientAPIAddress = "localhost:8090"
		config.EnableRateLimit = false
		config.RateLimit = 0
		config.RateLimitBurst = 0
		config.RateLimitWindow = 0

		if err := config.Validate(); err != nil {
			t.Errorf("Expected disabled rate limiting to ignore invalid values, got error: %v", err)
		}
	})

	rateLimitTests := []struct {
		name         string
		modifyConfig func(*RaftLockServerConfig)
		errorMsg     string
	}{
		{
			name: "zero RateLimit",
			modifyConfig: func(c *RaftLockServerConfig) {
				c.EnableRateLimit = true
				c.RateLimit = 0
			},
			errorMsg: "RateLimit must be positive",
		},
		{
			name: "zero RateLimitBurst",
			modifyConfig: func(c *RaftLockServerConfig) {
				c.EnableRateLimit = true
				c.RateLimitBurst = 0
			},
			errorMsg: "RateLimitBurst must be positive",
		},
		{
			name: "zero RateLimitWindow",
			modifyConfig: func(c *RaftLockServerConfig) {
				c.EnableRateLimit = true
				c.RateLimitWindow = 0
			},
			errorMsg: "RateLimitWindow must be positive",
		},
	}

	for _, tt := range rateLimitTests {
		t.Run(tt.name, func(t *testing.T) {
			config := createValidConfig()
			config.ClientAPIAddress = "localhost:8090"
			tt.modifyConfig(&config)

			err := config.Validate()
			assertValidationError(t, err, tt.errorMsg)
		})
	}

	t.Run("valid rate limiting configuration", func(t *testing.T) {
		config := createValidConfig()
		config.ClientAPIAddress = "localhost:8090"
		config.EnableRateLimit = true
		config.RateLimit = 100
		config.RateLimitBurst = 200
		config.RateLimitWindow = time.Second

		if err := config.Validate(); err != nil {
			t.Errorf("Expected valid rate limiting config to pass, got error: %v", err)
		}
	})
}

func TestRaftLockServerConfig_Validate_LeaderRedirect(t *testing.T) {
	t.Run("disabled leader redirect ignores invalid timeout", func(t *testing.T) {
		config := createValidConfig()
		config.ClientAPIAddress = "localhost:8090"
		config.EnableLeaderRedirect = false
		config.RedirectTimeout = 0

		if err := config.Validate(); err != nil {
			t.Errorf(
				"Expected disabled leader redirect to ignore invalid timeout, got error: %v",
				err,
			)
		}
	})

	t.Run("enabled leader redirect requires valid timeout", func(t *testing.T) {
		config := createValidConfig()
		config.ClientAPIAddress = "localhost:8090"
		config.EnableLeaderRedirect = true
		config.RedirectTimeout = 0

		err := config.Validate()
		assertValidationError(t, err, "RedirectTimeout must be positive")
	})

	t.Run("valid leader redirect configuration", func(t *testing.T) {
		config := createValidConfig()
		config.ClientAPIAddress = "localhost:8090"
		config.EnableLeaderRedirect = true
		config.RedirectTimeout = 5 * time.Second

		if err := config.Validate(); err != nil {
			t.Errorf("Expected valid leader redirect config to pass, got error: %v", err)
		}
	})
}

func TestRaftLockServerConfig_Validate_EdgeCases(t *testing.T) {
	t.Run("minimal valid configuration", func(t *testing.T) {
		config := RaftLockServerConfig{
			NodeID:           "test-node",
			ListenAddress:    "localhost:8080",
			ClientAPIAddress: "localhost:8090",
			Peers: map[types.NodeID]raft.PeerConfig{
				"test-node": {Address: "localhost:8080"},
			},
			DataDir:              "/tmp/test",
			RequestTimeout:       time.Millisecond,
			ShutdownTimeout:      time.Millisecond,
			MaxRequestSize:       1,
			MaxResponseSize:      1,
			MaxConcurrentReqs:    1,
			HealthCheckInterval:  time.Millisecond,
			HealthCheckTimeout:   time.Millisecond,
			EnableLeaderRedirect: false,
			EnableRateLimit:      false,
		}

		if err := config.Validate(); err != nil {
			t.Errorf("Expected minimal valid config to pass, got error: %v", err)
		}
	})

	t.Run("configuration with many peers", func(t *testing.T) {
		config := createValidConfig()
		config.ClientAPIAddress = "localhost:8090"

		// Add many peers
		for i := 3; i <= 10; i++ {
			nodeID := types.NodeID(fmt.Sprintf("node%d", i))
			config.Peers[nodeID] = raft.PeerConfig{
				ID:      nodeID,
				Address: fmt.Sprintf("localhost:808%d", i),
			}
		}

		if err := config.Validate(); err != nil {
			t.Errorf("Expected config with many peers to pass, got error: %v", err)
		}
	})
}

func TestRaftLockServerConfigError(t *testing.T) {
	t.Run("error creation and message", func(t *testing.T) {
		msg := "test error message"
		err := NewRaftLockServerConfigError(msg)

		if err.Message != msg {
			t.Errorf("Expected Message to be %q, got %q", msg, err.Message)
		}

		expectedError := "server config error: " + msg
		if err.Error() != expectedError {
			t.Errorf("Expected Error() to return %q, got %q", expectedError, err.Error())
		}
	})

	t.Run("implements error interface", func(t *testing.T) {
		var err error = NewRaftLockServerConfigError("test")

		// This verifies the error interface is implemented
		errorMsg := err.Error()
		if !strings.Contains(errorMsg, "test") {
			t.Errorf("Expected error message to contain 'test', got %q", errorMsg)
		}
	})

	t.Run("empty message", func(t *testing.T) {
		err := NewRaftLockServerConfigError("")
		expected := "server config error: "
		if err.Error() != expected {
			t.Errorf("Expected error with empty message to be %q, got %q", expected, err.Error())
		}
	})
}

func createValidConfig() RaftLockServerConfig {
	return RaftLockServerConfig{
		NodeID:           "node1",
		ListenAddress:    "localhost:8080",
		ClientAPIAddress: "localhost:8090",
		Peers: map[types.NodeID]raft.PeerConfig{
			"node1": {ID: "node1", Address: "localhost:8080"},
			"node2": {ID: "node2", Address: "localhost:8081"},
		},
		RaftConfig: raft.Config{
			ID: "node1",
			Peers: map[types.NodeID]raft.PeerConfig{
				"node1": {ID: "node1", Address: "localhost:8080"},
				"node2": {ID: "node2", Address: "localhost:8081"},
			},
		},
		DataDir:              "/tmp/raftlock-test",
		RequestTimeout:       30 * time.Second,
		ShutdownTimeout:      10 * time.Second,
		MaxRequestSize:       1024 * 1024,
		MaxResponseSize:      1024 * 1024,
		MaxConcurrentReqs:    100,
		EnableRateLimit:      false,
		RateLimit:            100,
		RateLimitBurst:       200,
		RateLimitWindow:      time.Second,
		Logger:               logger.NewNoOpLogger(),
		Metrics:              NewNoOpServerMetrics(),
		Clock:                raft.NewStandardClock(),
		Serializer:           &lock.JSONSerializer{},
		HealthCheckInterval:  30 * time.Second,
		HealthCheckTimeout:   5 * time.Second,
		EnableLeaderRedirect: true,
		RedirectTimeout:      5 * time.Second,
	}
}

func assertValidationError(t *testing.T, err error, expectedMsg string) {
	t.Helper()

	if err == nil {
		t.Fatal("Expected validation error, got nil")
	}

	configErr, ok := err.(*RaftLockServerConfigError)
	if !ok {
		t.Fatalf("Expected *RaftLockServerConfigError, got %T: %v", err, err)
	}

	if !strings.Contains(configErr.Error(), expectedMsg) {
		t.Errorf("Expected error to contain %q, got %q", expectedMsg, configErr.Error())
	}
}
