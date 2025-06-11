package client

import (
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
)

func TestDefaultClientConfig(t *testing.T) {
	config := DefaultClientConfig()

	if config.DialTimeout != defaultDialTimeout {
		t.Errorf("Expected DialTimeout to be %v, got %v", defaultDialTimeout, config.DialTimeout)
	}

	if config.RequestTimeout != defaultRequestTimeout {
		t.Errorf("Expected RequestTimeout to be %v, got %v", defaultRequestTimeout, config.RequestTimeout)
	}

	if config.KeepAlive.Time != defaultKeepAliveTime {
		t.Errorf("Expected KeepAlive.Time to be %v, got %v", defaultKeepAliveTime, config.KeepAlive.Time)
	}

	if config.KeepAlive.Timeout != defaultKeepAliveTimeout {
		t.Errorf("Expected KeepAlive.Timeout to be %v, got %v", defaultKeepAliveTimeout, config.KeepAlive.Timeout)
	}

	if config.KeepAlive.PermitWithoutStream != defaultPermitWithoutStream {
		t.Errorf("Expected KeepAlive.PermitWithoutStream to be %v, got %v", defaultPermitWithoutStream, config.KeepAlive.PermitWithoutStream)
	}

	if config.EnableMetrics != defaultEnableMetrics {
		t.Errorf("Expected EnableMetrics to be %v, got %v", defaultEnableMetrics, config.EnableMetrics)
	}

	if config.MaxMessageSize != defaultMaxMessageSize {
		t.Errorf("Expected MaxMessageSize to be %v, got %v", defaultMaxMessageSize, config.MaxMessageSize)
	}

	if config.RetryPolicy.MaxRetries != defaultMaxRetries {
		t.Errorf("Expected RetryPolicy.MaxRetries to be %v, got %v", defaultMaxRetries, config.RetryPolicy.MaxRetries)
	}

	if len(config.Endpoints) != 0 {
		t.Errorf("Expected Endpoints to be empty by default, got %v", config.Endpoints)
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	policy := DefaultRetryPolicy()

	if policy.MaxRetries != defaultMaxRetries {
		t.Errorf("Expected MaxRetries to be %v, got %v", defaultMaxRetries, policy.MaxRetries)
	}

	if policy.InitialBackoff != defaultInitialBackoff {
		t.Errorf("Expected InitialBackoff to be %v, got %v", defaultInitialBackoff, policy.InitialBackoff)
	}

	if policy.MaxBackoff != defaultMaxBackoff {
		t.Errorf("Expected MaxBackoff to be %v, got %v", defaultMaxBackoff, policy.MaxBackoff)
	}

	if policy.BackoffMultiplier != defaultBackoffMultiplier {
		t.Errorf("Expected BackoffMultiplier to be %v, got %v", defaultBackoffMultiplier, policy.BackoffMultiplier)
	}

	if policy.JitterFactor != defaultJitterFactor {
		t.Errorf("Expected JitterFactor to be %v, got %v", defaultJitterFactor, policy.JitterFactor)
	}

	expectedErrors := []pb.ErrorCode{
		pb.ErrorCode_NO_LEADER,
		pb.ErrorCode_NOT_LEADER,
		pb.ErrorCode_UNAVAILABLE,
		pb.ErrorCode_TIMEOUT,
	}

	if len(policy.RetryableErrors) != len(expectedErrors) {
		t.Errorf("Expected %d retryable errors, got %d", len(expectedErrors), len(policy.RetryableErrors))
	}

	errorMap := make(map[pb.ErrorCode]bool)
	for _, code := range policy.RetryableErrors {
		errorMap[code] = true
	}

	for _, expectedCode := range expectedErrors {
		if !errorMap[expectedCode] {
			t.Errorf("Expected error code %v to be in retryable errors", expectedCode)
		}
	}
}

func TestKeepAliveConfig(t *testing.T) {
	tests := []struct {
		name     string
		config   KeepAliveConfig
		expected KeepAliveConfig
	}{
		{
			name: "default values",
			config: KeepAliveConfig{
				Time:                defaultKeepAliveTime,
				Timeout:             defaultKeepAliveTimeout,
				PermitWithoutStream: defaultPermitWithoutStream,
			},
			expected: KeepAliveConfig{
				Time:                30 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		},
		{
			name: "custom values",
			config: KeepAliveConfig{
				Time:                10 * time.Second,
				Timeout:             2 * time.Second,
				PermitWithoutStream: false,
			},
			expected: KeepAliveConfig{
				Time:                10 * time.Second,
				Timeout:             2 * time.Second,
				PermitWithoutStream: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.config.Time != tt.expected.Time {
				t.Errorf("Expected Time to be %v, got %v", tt.expected.Time, tt.config.Time)
			}
			if tt.config.Timeout != tt.expected.Timeout {
				t.Errorf("Expected Timeout to be %v, got %v", tt.expected.Timeout, tt.config.Timeout)
			}
			if tt.config.PermitWithoutStream != tt.expected.PermitWithoutStream {
				t.Errorf("Expected PermitWithoutStream to be %v, got %v", tt.expected.PermitWithoutStream, tt.config.PermitWithoutStream)
			}
		})
	}
}

func TestRetryPolicy(t *testing.T) {
	tests := []struct {
		name     string
		policy   RetryPolicy
		expected RetryPolicy
	}{
		{
			name: "zero values",
			policy: RetryPolicy{
				MaxRetries:        0,
				InitialBackoff:    0,
				MaxBackoff:        0,
				BackoffMultiplier: 0,
				JitterFactor:      0,
				RetryableErrors:   nil,
			},
			expected: RetryPolicy{
				MaxRetries:        0,
				InitialBackoff:    0,
				MaxBackoff:        0,
				BackoffMultiplier: 0,
				JitterFactor:      0,
				RetryableErrors:   nil,
			},
		},
		{
			name: "custom values",
			policy: RetryPolicy{
				MaxRetries:        5,
				InitialBackoff:    200 * time.Millisecond,
				MaxBackoff:        10 * time.Second,
				BackoffMultiplier: 1.5,
				JitterFactor:      0.2,
				RetryableErrors: []pb.ErrorCode{
					pb.ErrorCode_TIMEOUT,
					pb.ErrorCode_UNAVAILABLE,
				},
			},
			expected: RetryPolicy{
				MaxRetries:        5,
				InitialBackoff:    200 * time.Millisecond,
				MaxBackoff:        10 * time.Second,
				BackoffMultiplier: 1.5,
				JitterFactor:      0.2,
				RetryableErrors: []pb.ErrorCode{
					pb.ErrorCode_TIMEOUT,
					pb.ErrorCode_UNAVAILABLE,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.policy.MaxRetries != tt.expected.MaxRetries {
				t.Errorf("Expected MaxRetries to be %v, got %v", tt.expected.MaxRetries, tt.policy.MaxRetries)
			}
			if tt.policy.InitialBackoff != tt.expected.InitialBackoff {
				t.Errorf("Expected InitialBackoff to be %v, got %v", tt.expected.InitialBackoff, tt.policy.InitialBackoff)
			}
			if tt.policy.MaxBackoff != tt.expected.MaxBackoff {
				t.Errorf("Expected MaxBackoff to be %v, got %v", tt.expected.MaxBackoff, tt.policy.MaxBackoff)
			}
			if tt.policy.BackoffMultiplier != tt.expected.BackoffMultiplier {
				t.Errorf("Expected BackoffMultiplier to be %v, got %v", tt.expected.BackoffMultiplier, tt.policy.BackoffMultiplier)
			}
			if tt.policy.JitterFactor != tt.expected.JitterFactor {
				t.Errorf("Expected JitterFactor to be %v, got %v", tt.expected.JitterFactor, tt.policy.JitterFactor)
			}
			if len(tt.policy.RetryableErrors) != len(tt.expected.RetryableErrors) {
				t.Errorf("Expected %d retryable errors, got %d", len(tt.expected.RetryableErrors), len(tt.policy.RetryableErrors))
			}
		})
	}
}

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected Config
	}{
		{
			name:   "empty config",
			config: Config{},
			expected: Config{
				Endpoints:      nil,
				DialTimeout:    0,
				RequestTimeout: 0,
				KeepAlive:      KeepAliveConfig{},
				RetryPolicy:    RetryPolicy{},
				EnableMetrics:  false,
				MaxMessageSize: 0,
			},
		},
		{
			name: "fully configured",
			config: Config{
				Endpoints:      []string{"localhost:8080", "localhost:8081"},
				DialTimeout:    10 * time.Second,
				RequestTimeout: 60 * time.Second,
				KeepAlive: KeepAliveConfig{
					Time:                15 * time.Second,
					Timeout:             3 * time.Second,
					PermitWithoutStream: false,
				},
				RetryPolicy: RetryPolicy{
					MaxRetries:        2,
					InitialBackoff:    50 * time.Millisecond,
					MaxBackoff:        2 * time.Second,
					BackoffMultiplier: 1.8,
					JitterFactor:      0.15,
					RetryableErrors: []pb.ErrorCode{
						pb.ErrorCode_TIMEOUT,
					},
				},
				EnableMetrics:  true,
				MaxMessageSize: 8 * 1024 * 1024,
			},
			expected: Config{
				Endpoints:      []string{"localhost:8080", "localhost:8081"},
				DialTimeout:    10 * time.Second,
				RequestTimeout: 60 * time.Second,
				KeepAlive: KeepAliveConfig{
					Time:                15 * time.Second,
					Timeout:             3 * time.Second,
					PermitWithoutStream: false,
				},
				RetryPolicy: RetryPolicy{
					MaxRetries:        2,
					InitialBackoff:    50 * time.Millisecond,
					MaxBackoff:        2 * time.Second,
					BackoffMultiplier: 1.8,
					JitterFactor:      0.15,
					RetryableErrors: []pb.ErrorCode{
						pb.ErrorCode_TIMEOUT,
					},
				},
				EnableMetrics:  true,
				MaxMessageSize: 8 * 1024 * 1024,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.config.Endpoints) != len(tt.expected.Endpoints) {
				t.Errorf("Expected %d endpoints, got %d", len(tt.expected.Endpoints), len(tt.config.Endpoints))
			}
			for i, endpoint := range tt.config.Endpoints {
				if i < len(tt.expected.Endpoints) && endpoint != tt.expected.Endpoints[i] {
					t.Errorf("Expected endpoint[%d] to be %v, got %v", i, tt.expected.Endpoints[i], endpoint)
				}
			}

			if tt.config.DialTimeout != tt.expected.DialTimeout {
				t.Errorf("Expected DialTimeout to be %v, got %v", tt.expected.DialTimeout, tt.config.DialTimeout)
			}
			if tt.config.RequestTimeout != tt.expected.RequestTimeout {
				t.Errorf("Expected RequestTimeout to be %v, got %v", tt.expected.RequestTimeout, tt.config.RequestTimeout)
			}

			if tt.config.EnableMetrics != tt.expected.EnableMetrics {
				t.Errorf("Expected EnableMetrics to be %v, got %v", tt.expected.EnableMetrics, tt.config.EnableMetrics)
			}
			if tt.config.MaxMessageSize != tt.expected.MaxMessageSize {
				t.Errorf("Expected MaxMessageSize to be %v, got %v", tt.expected.MaxMessageSize, tt.config.MaxMessageSize)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	if defaultDialTimeout != 5*time.Second {
		t.Errorf("Expected defaultDialTimeout to be 5s, got %v", defaultDialTimeout)
	}

	if defaultRequestTimeout != 30*time.Second {
		t.Errorf("Expected defaultRequestTimeout to be 30s, got %v", defaultRequestTimeout)
	}

	if defaultKeepAliveTime != 30*time.Second {
		t.Errorf("Expected defaultKeepAliveTime to be 30s, got %v", defaultKeepAliveTime)
	}

	if defaultKeepAliveTimeout != 5*time.Second {
		t.Errorf("Expected defaultKeepAliveTimeout to be 5s, got %v", defaultKeepAliveTimeout)
	}

	if defaultPermitWithoutStream != true {
		t.Errorf("Expected defaultPermitWithoutStream to be true, got %v", defaultPermitWithoutStream)
	}

	if defaultEnableMetrics != true {
		t.Errorf("Expected defaultEnableMetrics to be true, got %v", defaultEnableMetrics)
	}

	if defaultMaxMessageSize != 16*1024*1024 {
		t.Errorf("Expected defaultMaxMessageSize to be 16MB, got %v", defaultMaxMessageSize)
	}

	if defaultMaxRetries != 3 {
		t.Errorf("Expected defaultMaxRetries to be 3, got %v", defaultMaxRetries)
	}

	if defaultInitialBackoff != 100*time.Millisecond {
		t.Errorf("Expected defaultInitialBackoff to be 100ms, got %v", defaultInitialBackoff)
	}

	if defaultMaxBackoff != 5*time.Second {
		t.Errorf("Expected defaultMaxBackoff to be 5s, got %v", defaultMaxBackoff)
	}

	if defaultBackoffMultiplier != 2.0 {
		t.Errorf("Expected defaultBackoffMultiplier to be 2.0, got %v", defaultBackoffMultiplier)
	}

	if defaultJitterFactor != 0.1 {
		t.Errorf("Expected defaultJitterFactor to be 0.1, got %v", defaultJitterFactor)
	}
}

func TestDefaultRetryPolicyErrorCodes(t *testing.T) {
	policy := DefaultRetryPolicy()

	errorCodeSet := make(map[pb.ErrorCode]bool)
	for _, code := range policy.RetryableErrors {
		errorCodeSet[code] = true
	}

	requiredCodes := []pb.ErrorCode{
		pb.ErrorCode_NO_LEADER,
		pb.ErrorCode_NOT_LEADER,
		pb.ErrorCode_UNAVAILABLE,
		pb.ErrorCode_TIMEOUT,
	}

	for _, code := range requiredCodes {
		if !errorCodeSet[code] {
			t.Errorf("Expected error code %v to be in default retryable errors", code)
		}
	}

	if len(policy.RetryableErrors) != len(requiredCodes) {
		t.Errorf("Expected exactly %d retryable error codes, got %d", len(requiredCodes), len(policy.RetryableErrors))
	}
}

func TestConfigComposition(t *testing.T) {
	config := DefaultClientConfig()
	defaultPolicy := DefaultRetryPolicy()

	if config.RetryPolicy.MaxRetries != defaultPolicy.MaxRetries {
		t.Errorf("DefaultClientConfig should use DefaultRetryPolicy MaxRetries")
	}

	if config.RetryPolicy.InitialBackoff != defaultPolicy.InitialBackoff {
		t.Errorf("DefaultClientConfig should use DefaultRetryPolicy InitialBackoff")
	}

	if len(config.RetryPolicy.RetryableErrors) != len(defaultPolicy.RetryableErrors) {
		t.Errorf("DefaultClientConfig should use DefaultRetryPolicy RetryableErrors")
	}
}
