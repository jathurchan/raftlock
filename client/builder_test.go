package client

import (
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
)

func TestNewRaftLockClientBuilder(t *testing.T) {
	tests := []struct {
		name            string
		endpoints       []string
		wantHasEndpoint bool
	}{
		{
			name:            "with endpoints",
			endpoints:       []string{"localhost:8080", "localhost:8081"},
			wantHasEndpoint: true,
		},
		{
			name:            "empty endpoints",
			endpoints:       []string{},
			wantHasEndpoint: false,
		},
		{
			name:            "nil endpoints",
			endpoints:       nil,
			wantHasEndpoint: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder(tt.endpoints)

			if builder == nil {
				t.Fatal("NewRaftLockClientBuilder returned nil")
			}

			if builder.hasEndpoint != tt.wantHasEndpoint {
				t.Errorf("hasEndpoint = %v, want %v", builder.hasEndpoint, tt.wantHasEndpoint)
			}

			if builder.config.DialTimeout == 0 {
				t.Error("Expected default config to be initialized with non-zero DialTimeout")
			}

			if len(tt.endpoints) > 0 {
				if len(builder.config.Endpoints) != len(tt.endpoints) {
					t.Errorf("config.Endpoints length = %d, want %d", len(builder.config.Endpoints), len(tt.endpoints))
				}
				for i, ep := range tt.endpoints {
					if builder.config.Endpoints[i] != ep {
						t.Errorf("config.Endpoints[%d] = %s, want %s", i, builder.config.Endpoints[i], ep)
					}
				}
			}
		})
	}
}

func TestRaftLockClientBuilder_WithEndpoints(t *testing.T) {
	tests := []struct {
		name            string
		initial         []string
		new             []string
		wantHasEndpoint bool
	}{
		{
			name:            "set new endpoints",
			initial:         []string{"old:8080"},
			new:             []string{"new:8080", "new:8081"},
			wantHasEndpoint: true,
		},
		{
			name:            "clear endpoints",
			initial:         []string{"old:8080"},
			new:             []string{},
			wantHasEndpoint: false,
		},
		{
			name:            "nil endpoints",
			initial:         []string{"old:8080"},
			new:             nil,
			wantHasEndpoint: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder(tt.initial)
			result := builder.WithEndpoints(tt.new)

			if result != builder {
				t.Error("WithEndpoints should return the same builder instance")
			}

			if builder.hasEndpoint != tt.wantHasEndpoint {
				t.Errorf("hasEndpoint = %v, want %v", builder.hasEndpoint, tt.wantHasEndpoint)
			}

			if len(builder.config.Endpoints) != len(tt.new) {
				t.Errorf("config.Endpoints length = %d, want %d", len(builder.config.Endpoints), len(tt.new))
			}
			for i, ep := range tt.new {
				if builder.config.Endpoints[i] != ep {
					t.Errorf("config.Endpoints[%d] = %s, want %s", i, builder.config.Endpoints[i], ep)
				}
			}
		})
	}
}

func TestRaftLockClientBuilder_WithTimeouts(t *testing.T) {
	tests := []struct {
		name           string
		dialTimeout    time.Duration
		requestTimeout time.Duration
		wantDial       time.Duration
		wantRequest    time.Duration
	}{
		{
			name:           "positive timeouts",
			dialTimeout:    2 * time.Second,
			requestTimeout: 5 * time.Second,
			wantDial:       2 * time.Second,
			wantRequest:    5 * time.Second,
		},
		{
			name:           "zero timeouts - should not update",
			dialTimeout:    0,
			requestTimeout: 0,
			wantDial:       defaultDialTimeout,
			wantRequest:    defaultRequestTimeout,
		},
		{
			name:           "negative timeouts - should not update",
			dialTimeout:    -1 * time.Second,
			requestTimeout: -1 * time.Second,
			wantDial:       defaultDialTimeout,
			wantRequest:    defaultRequestTimeout,
		},
		{
			name:           "mixed timeouts",
			dialTimeout:    3 * time.Second,
			requestTimeout: 0,
			wantDial:       3 * time.Second,
			wantRequest:    defaultRequestTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder([]string{"localhost:8080"})
			result := builder.WithTimeouts(tt.dialTimeout, tt.requestTimeout)

			if result != builder {
				t.Error("WithTimeouts should return the same builder instance")
			}

			if builder.config.DialTimeout != tt.wantDial {
				t.Errorf("DialTimeout = %v, want %v", builder.config.DialTimeout, tt.wantDial)
			}

			if builder.config.RequestTimeout != tt.wantRequest {
				t.Errorf("RequestTimeout = %v, want %v", builder.config.RequestTimeout, tt.wantRequest)
			}
		})
	}
}

func TestRaftLockClientBuilder_WithKeepAlive(t *testing.T) {
	tests := []struct {
		name                string
		time                time.Duration
		timeout             time.Duration
		permitWithoutStream bool
	}{
		{
			name:                "standard keepalive",
			time:                30 * time.Second,
			timeout:             5 * time.Second,
			permitWithoutStream: true,
		},
		{
			name:                "zero values",
			time:                0,
			timeout:             0,
			permitWithoutStream: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder([]string{"localhost:8080"})
			result := builder.WithKeepAlive(tt.time, tt.timeout, tt.permitWithoutStream)

			if result != builder {
				t.Error("WithKeepAlive should return the same builder instance")
			}

			if builder.config.KeepAlive.Time != tt.time {
				t.Errorf("KeepAlive.Time = %v, want %v", builder.config.KeepAlive.Time, tt.time)
			}

			if builder.config.KeepAlive.Timeout != tt.timeout {
				t.Errorf("KeepAlive.Timeout = %v, want %v", builder.config.KeepAlive.Timeout, tt.timeout)
			}

			if builder.config.KeepAlive.PermitWithoutStream != tt.permitWithoutStream {
				t.Errorf("KeepAlive.PermitWithoutStream = %v, want %v", builder.config.KeepAlive.PermitWithoutStream, tt.permitWithoutStream)
			}
		})
	}
}

func TestRaftLockClientBuilder_WithRetryPolicy(t *testing.T) {
	customPolicy := RetryPolicy{
		MaxRetries:        5,
		InitialBackoff:    200 * time.Millisecond,
		MaxBackoff:        10 * time.Second,
		BackoffMultiplier: 1.5,
		JitterFactor:      0.2,
		RetryableErrors:   []pb.ErrorCode{pb.ErrorCode_TIMEOUT},
	}

	builder := NewRaftLockClientBuilder([]string{"localhost:8080"})
	result := builder.WithRetryPolicy(customPolicy)

	if result != builder {
		t.Error("WithRetryPolicy should return the same builder instance")
	}

	if builder.config.RetryPolicy.MaxRetries != customPolicy.MaxRetries {
		t.Errorf("RetryPolicy.MaxRetries = %d, want %d", builder.config.RetryPolicy.MaxRetries, customPolicy.MaxRetries)
	}

	if builder.config.RetryPolicy.InitialBackoff != customPolicy.InitialBackoff {
		t.Errorf("RetryPolicy.InitialBackoff = %v, want %v", builder.config.RetryPolicy.InitialBackoff, customPolicy.InitialBackoff)
	}

	if builder.config.RetryPolicy.MaxBackoff != customPolicy.MaxBackoff {
		t.Errorf("RetryPolicy.MaxBackoff = %v, want %v", builder.config.RetryPolicy.MaxBackoff, customPolicy.MaxBackoff)
	}

	if builder.config.RetryPolicy.BackoffMultiplier != customPolicy.BackoffMultiplier {
		t.Errorf("RetryPolicy.BackoffMultiplier = %f, want %f", builder.config.RetryPolicy.BackoffMultiplier, customPolicy.BackoffMultiplier)
	}

	if builder.config.RetryPolicy.JitterFactor != customPolicy.JitterFactor {
		t.Errorf("RetryPolicy.JitterFactor = %f, want %f", builder.config.RetryPolicy.JitterFactor, customPolicy.JitterFactor)
	}

	if len(builder.config.RetryPolicy.RetryableErrors) != len(customPolicy.RetryableErrors) {
		t.Errorf("RetryPolicy.RetryableErrors length = %d, want %d", len(builder.config.RetryPolicy.RetryableErrors), len(customPolicy.RetryableErrors))
	}
}

func TestRaftLockClientBuilder_WithRetryOptions(t *testing.T) {
	tests := []struct {
		name               string
		maxRetries         int
		initialBackoff     time.Duration
		maxBackoff         time.Duration
		multiplier         float64
		wantMaxRetries     int
		wantInitialBackoff time.Duration
		wantMaxBackoff     time.Duration
		wantMultiplier     float64
	}{
		{
			name:               "all positive values",
			maxRetries:         10,
			initialBackoff:     500 * time.Millisecond,
			maxBackoff:         30 * time.Second,
			multiplier:         3.0,
			wantMaxRetries:     10,
			wantInitialBackoff: 500 * time.Millisecond,
			wantMaxBackoff:     30 * time.Second,
			wantMultiplier:     3.0,
		},
		{
			name:               "zero/negative values - should not update",
			maxRetries:         -1,
			initialBackoff:     0,
			maxBackoff:         0,
			multiplier:         0,
			wantMaxRetries:     defaultMaxRetries,
			wantInitialBackoff: defaultInitialBackoff,
			wantMaxBackoff:     defaultMaxBackoff,
			wantMultiplier:     defaultBackoffMultiplier,
		},
		{
			name:               "zero retries allowed",
			maxRetries:         0,
			initialBackoff:     100 * time.Millisecond,
			maxBackoff:         5 * time.Second,
			multiplier:         2.0,
			wantMaxRetries:     0,
			wantInitialBackoff: 100 * time.Millisecond,
			wantMaxBackoff:     5 * time.Second,
			wantMultiplier:     2.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder([]string{"localhost:8080"})
			result := builder.WithRetryOptions(tt.maxRetries, tt.initialBackoff, tt.maxBackoff, tt.multiplier)

			if result != builder {
				t.Error("WithRetryOptions should return the same builder instance")
			}

			if builder.config.RetryPolicy.MaxRetries != tt.wantMaxRetries {
				t.Errorf("RetryPolicy.MaxRetries = %d, want %d", builder.config.RetryPolicy.MaxRetries, tt.wantMaxRetries)
			}

			if builder.config.RetryPolicy.InitialBackoff != tt.wantInitialBackoff {
				t.Errorf("RetryPolicy.InitialBackoff = %v, want %v", builder.config.RetryPolicy.InitialBackoff, tt.wantInitialBackoff)
			}

			if builder.config.RetryPolicy.MaxBackoff != tt.wantMaxBackoff {
				t.Errorf("RetryPolicy.MaxBackoff = %v, want %v", builder.config.RetryPolicy.MaxBackoff, tt.wantMaxBackoff)
			}

			if builder.config.RetryPolicy.BackoffMultiplier != tt.wantMultiplier {
				t.Errorf("RetryPolicy.BackoffMultiplier = %f, want %f", builder.config.RetryPolicy.BackoffMultiplier, tt.wantMultiplier)
			}
		})
	}
}

func TestRaftLockClientBuilder_WithRetryableErrors(t *testing.T) {
	tests := []struct {
		name       string
		codes      []pb.ErrorCode
		wantLength int
	}{
		{
			name:       "multiple error codes",
			codes:      []pb.ErrorCode{pb.ErrorCode_TIMEOUT, pb.ErrorCode_UNAVAILABLE, pb.ErrorCode_NOT_LEADER},
			wantLength: 3,
		},
		{
			name:       "single error code",
			codes:      []pb.ErrorCode{pb.ErrorCode_TIMEOUT},
			wantLength: 1,
		},
		{
			name:       "empty slice - should clear errors",
			codes:      []pb.ErrorCode{},
			wantLength: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder([]string{"localhost:8080"})

			builder.config.RetryPolicy.RetryableErrors = []pb.ErrorCode{pb.ErrorCode_INTERNAL_ERROR}

			result := builder.WithRetryableErrors(tt.codes...)

			if result != builder {
				t.Error("WithRetryableErrors should return the same builder instance")
			}

			if len(builder.config.RetryPolicy.RetryableErrors) != tt.wantLength {
				t.Errorf("RetryableErrors length = %d, want %d", len(builder.config.RetryPolicy.RetryableErrors), tt.wantLength)
			}

			for i, code := range tt.codes {
				if builder.config.RetryPolicy.RetryableErrors[i] != code {
					t.Errorf("RetryableErrors[%d] = %v, want %v", i, builder.config.RetryPolicy.RetryableErrors[i], code)
				}
			}
		})
	}
}

func TestRaftLockClientBuilder_WithMetrics(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
	}{
		{
			name:    "enable metrics",
			enabled: true,
		},
		{
			name:    "disable metrics",
			enabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder([]string{"localhost:8080"})
			result := builder.WithMetrics(tt.enabled)

			if result != builder {
				t.Error("WithMetrics should return the same builder instance")
			}

			if builder.config.EnableMetrics != tt.enabled {
				t.Errorf("EnableMetrics = %v, want %v", builder.config.EnableMetrics, tt.enabled)
			}
		})
	}
}

func TestRaftLockClientBuilder_WithMaxMessageSize(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		wantSize int
	}{
		{
			name:     "positive size",
			size:     1024 * 1024,
			wantSize: 1024 * 1024,
		},
		{
			name:     "zero size - should not update",
			size:     0,
			wantSize: defaultMaxMessageSize,
		},
		{
			name:     "negative size - should not update",
			size:     -1,
			wantSize: defaultMaxMessageSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder([]string{"localhost:8080"})
			result := builder.WithMaxMessageSize(tt.size)

			if result != builder {
				t.Error("WithMaxMessageSize should return the same builder instance")
			}

			if builder.config.MaxMessageSize != tt.wantSize {
				t.Errorf("MaxMessageSize = %d, want %d", builder.config.MaxMessageSize, tt.wantSize)
			}
		})
	}
}

func TestRaftLockClientBuilder_validate(t *testing.T) {
	tests := []struct {
		name        string
		hasEndpoint bool
		endpoints   []string
		wantError   bool
	}{
		{
			name:        "valid config with endpoints",
			hasEndpoint: true,
			endpoints:   []string{"localhost:8080"},
			wantError:   false,
		},
		{
			name:        "no endpoints",
			hasEndpoint: false,
			endpoints:   []string{},
			wantError:   true,
		},
		{
			name:        "hasEndpoint false but endpoints provided",
			hasEndpoint: false,
			endpoints:   []string{"localhost:8080"},
			wantError:   true,
		},
		{
			name:        "hasEndpoint true but no endpoints",
			hasEndpoint: true,
			endpoints:   []string{},
			wantError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := &RaftLockClientBuilder{
				config:      DefaultClientConfig(),
				hasEndpoint: tt.hasEndpoint,
			}
			builder.config.Endpoints = tt.endpoints

			err := builder.validate()

			if tt.wantError && err == nil {
				t.Error("validate() should return an error")
			}

			if !tt.wantError && err != nil {
				t.Errorf("validate() should not return an error, got: %v", err)
			}

			if tt.wantError && err != nil {
				expectedMsg := "builder: at least one endpoint must be set"
				if err.Error() != expectedMsg {
					t.Errorf("validate() error = %q, want %q", err.Error(), expectedMsg)
				}
			}
		})
	}
}

func TestRaftLockClientBuilder_Build(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "valid build",
			endpoints: []string{"localhost:8080"},
			wantError: false,
		},
		{
			name:      "no endpoints",
			endpoints: []string{},
			wantError: true,
			errorMsg:  "builder: at least one endpoint must be set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder(tt.endpoints)
			client, err := builder.Build()

			if tt.wantError {
				if err == nil {
					t.Error("Build() should return an error")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("Build() error = %q, want %q", err.Error(), tt.errorMsg)
				}
				if client != nil {
					t.Error("Build() should return nil client on error")
				}
			} else {
				if err != nil {
					t.Errorf("Build() should not return an error, got: %v", err)
				}
				if client == nil {
					t.Error("Build() should return a valid client")
				}
			}
		})
	}
}

func TestRaftLockClientBuilder_BuildAdmin(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "valid build",
			endpoints: []string{"localhost:8080"},
			wantError: false,
		},
		{
			name:      "no endpoints",
			endpoints: []string{},
			wantError: true,
			errorMsg:  "builder: at least one endpoint must be set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder(tt.endpoints)
			client, err := builder.BuildAdmin()

			if tt.wantError {
				if err == nil {
					t.Error("BuildAdmin() should return an error")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("BuildAdmin() error = %q, want %q", err.Error(), tt.errorMsg)
				}
				if client != nil {
					t.Error("BuildAdmin() should return nil client on error")
				}
			} else {
				if err != nil {
					t.Errorf("BuildAdmin() should not return an error, got: %v", err)
				}
				if client == nil {
					t.Error("BuildAdmin() should return a valid client")
				}
			}
		})
	}
}

func TestRaftLockClientBuilder_BuildAdvanced(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "valid build",
			endpoints: []string{"localhost:8080"},
			wantError: false,
		},
		{
			name:      "no endpoints",
			endpoints: []string{},
			wantError: true,
			errorMsg:  "builder: at least one endpoint must be set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewRaftLockClientBuilder(tt.endpoints)
			client, err := builder.BuildAdvanced()

			if tt.wantError {
				if err == nil {
					t.Error("BuildAdvanced() should return an error")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("BuildAdvanced() error = %q, want %q", err.Error(), tt.errorMsg)
				}
				if client != nil {
					t.Error("BuildAdvanced() should return nil client on error")
				}
			} else {
				if err != nil {
					t.Errorf("BuildAdvanced() should not return an error, got: %v", err)
				}
				if client == nil {
					t.Error("BuildAdvanced() should return a valid client")
				}
			}
		})
	}
}

func TestRaftLockClientBuilder_MethodChaining(t *testing.T) {
	endpoints := []string{"localhost:8080", "localhost:8081"}

	builder := NewRaftLockClientBuilder(endpoints).
		WithTimeouts(2*time.Second, 5*time.Second).
		WithKeepAlive(30*time.Second, 5*time.Second, true).
		WithRetryOptions(5, 200*time.Millisecond, 10*time.Second, 2.0).
		WithRetryableErrors(pb.ErrorCode_TIMEOUT, pb.ErrorCode_UNAVAILABLE).
		WithMetrics(true).
		WithMaxMessageSize(8 * 1024 * 1024)

	if builder == nil {
		t.Fatal("Method chaining should return a valid builder")
	}

	if len(builder.config.Endpoints) != 2 {
		t.Errorf("Endpoints length = %d, want 2", len(builder.config.Endpoints))
	}

	if builder.config.DialTimeout != 2*time.Second {
		t.Errorf("DialTimeout = %v, want %v", builder.config.DialTimeout, 2*time.Second)
	}

	if builder.config.RequestTimeout != 5*time.Second {
		t.Errorf("RequestTimeout = %v, want %v", builder.config.RequestTimeout, 5*time.Second)
	}

	if builder.config.KeepAlive.Time != 30*time.Second {
		t.Errorf("KeepAlive.Time = %v, want %v", builder.config.KeepAlive.Time, 30*time.Second)
	}

	if builder.config.RetryPolicy.MaxRetries != 5 {
		t.Errorf("RetryPolicy.MaxRetries = %d, want 5", builder.config.RetryPolicy.MaxRetries)
	}

	if len(builder.config.RetryPolicy.RetryableErrors) != 2 {
		t.Errorf("RetryableErrors length = %d, want 2", len(builder.config.RetryPolicy.RetryableErrors))
	}

	if !builder.config.EnableMetrics {
		t.Error("EnableMetrics should be true")
	}

	if builder.config.MaxMessageSize != 8*1024*1024 {
		t.Errorf("MaxMessageSize = %d, want %d", builder.config.MaxMessageSize, 8*1024*1024)
	}

	client, err := builder.Build()
	if err != nil {
		t.Errorf("Build() after method chaining failed: %v", err)
	}
	if client == nil {
		t.Error("Build() should return a valid client after method chaining")
	}
}
