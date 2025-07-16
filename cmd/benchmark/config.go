package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jathurchan/raftlock/server"
)

const (
	benchmarkVersion = "v1.0.0"
	goVersion        = "go1.23.0"
	build            = "production"
)

// Config holds all benchmark configuration with comprehensive validation
type Config struct {
	// ServerAddrs lists the addresses of Raft servers in host:port format.
	// A minimum of three is required for fault tolerance testing.
	ServerAddrs []string `json:"server_addrs" yaml:"server_addrs"`

	// ContainerNames contains the Docker container names mapped to the servers.
	// Required if Docker is enabled.
	ContainerNames []string `json:"container_names" yaml:"container_names"`

	// UseDocker specifies whether to use Docker to simulate faults.
	UseDocker bool `json:"use_docker" yaml:"use_docker"`

	// UncontestedOps is the number of operations to run with no contention.
	UncontestedOps int `json:"uncontested_ops" yaml:"uncontested_ops"`

	// ContentionOps is the number of operations to run under contention.
	ContentionOps int `json:"contention_ops" yaml:"contention_ops"`

	// MaxWorkers is the maximum number of concurrent workers performing operations.
	MaxWorkers int `json:"max_workers" yaml:"max_workers"`

	// FaultTestDuration sets the total duration of the fault tolerance test.
	FaultTestDuration time.Duration `json:"fault_test_duration" yaml:"fault_test_duration"`

	// FailureDelay is the delay before simulating a node failure.
	FailureDelay time.Duration `json:"failure_delay" yaml:"failure_delay"`

	// RecoveryDelay is the delay before simulating node recovery.
	RecoveryDelay time.Duration `json:"recovery_delay" yaml:"recovery_delay"`

	// OperationTimeout defines the timeout for each individual operation.
	OperationTimeout time.Duration `json:"operation_timeout" yaml:"operation_timeout"`

	// InterOperationDelay is the delay between consecutive operations.
	InterOperationDelay time.Duration `json:"inter_operation_delay" yaml:"inter_operation_delay"`

	// MaxRetries is the maximum number of retry attempts for a failed operation.
	MaxRetries int `json:"max_retries" yaml:"max_retries"`

	// BaseBackoff is the initial backoff duration used between retries.
	BaseBackoff time.Duration `json:"base_backoff" yaml:"base_backoff"`

	// MaxBackoff is the maximum backoff duration used between retries.
	MaxBackoff time.Duration `json:"max_backoff" yaml:"max_backoff"`

	// BackoffMultiplier controls the exponential growth of backoff durations.
	BackoffMultiplier float64 `json:"backoff_multiplier" yaml:"backoff_multiplier"`

	// JitterFactor adds randomness to retry backoffs to avoid thundering herd issues.
	JitterFactor float64 `json:"jitter_factor" yaml:"jitter_factor"`

	// Verbose enables detailed logging output.
	Verbose bool `json:"verbose" yaml:"verbose"`

	// OutputFormat specifies the output format: "text", "json", or "yaml".
	OutputFormat string `json:"output_format" yaml:"output_format"`

	// OutputFile is the file path to write output to. Uses stdout if empty.
	OutputFile string `json:"output_file" yaml:"output_file"`

	// GenerateReport enables generation of a detailed benchmark report.
	GenerateReport bool `json:"generate_report" yaml:"generate_report"`

	// ConnectionPoolSize sets the size of the gRPC connection pool per server.
	ConnectionPoolSize int `json:"connection_pool_size" yaml:"connection_pool_size"`

	// HealthCheckTimeout is the timeout for server health checks.
	HealthCheckTimeout time.Duration `json:"health_check_timeout" yaml:"health_check_timeout"`

	// WarmupDuration is the time allowed for the cluster to stabilize before testing begins.
	WarmupDuration time.Duration `json:"warmup_duration" yaml:"warmup_duration"`
}

// DefaultConfig returns a Config instance with default values.
func DefaultConfig() *Config {
	return &Config{
		ServerAddrs:         []string{"localhost:8080", "localhost:8089", "localhost:8090"},
		ContainerNames:      []string{"raftlock-node1", "raftlock-node2", "raftlock-node3"},
		UseDocker:           true,
		UncontestedOps:      1000,
		ContentionOps:       2000,
		MaxWorkers:          100,
		FaultTestDuration:   3 * time.Minute,
		FailureDelay:        45 * time.Second,
		RecoveryDelay:       60 * time.Second,
		OperationTimeout:    5 * time.Second,
		InterOperationDelay: 10 * time.Millisecond,
		MaxRetries:          5,
		BaseBackoff:         50 * time.Millisecond,
		MaxBackoff:          2 * time.Second,
		BackoffMultiplier:   1.8,
		JitterFactor:        0.2,
		Verbose:             false,
		OutputFormat:        "text",
		GenerateReport:      true,
		ConnectionPoolSize:  10,
		HealthCheckTimeout:  10 * time.Second,
		WarmupDuration:      30 * time.Second,
	}
}

// Validate checks all config fields for logical consistency and acceptable ranges.
func (c *Config) Validate() error {
	if len(c.ServerAddrs) == 0 {
		return errors.New("at least one server address is required")
	}

	if len(c.ServerAddrs) < 3 {
		return errors.New("at least 3 servers required for fault tolerance testing")
	}

	for i, addr := range c.ServerAddrs {
		if err := server.ValidateAddress(addr); err != nil {
			return fmt.Errorf("invalid server address %d (%s): %w", i, addr, err)
		}
	}

	if c.UseDocker && len(c.ContainerNames) != len(c.ServerAddrs) {
		return errors.New("container names count must match server addresses when using Docker")
	}

	if c.UncontestedOps <= 0 {
		return errors.New("uncontested operations must be positive")
	}
	if c.UncontestedOps > 100000 {
		return errors.New(
			"uncontested operations should not exceed 100,000 for reasonable test duration",
		)
	}

	if c.ContentionOps <= 0 {
		return errors.New("contention operations must be positive")
	}
	if c.ContentionOps > 50000 {
		return errors.New(
			"contention operations should not exceed 50,000 for reasonable test duration",
		)
	}

	if c.MaxWorkers <= 0 {
		return errors.New("max workers must be positive")
	}
	if c.MaxWorkers > 1000 {
		return errors.New("max workers should not exceed 1000 to avoid resource exhaustion")
	}

	if c.OperationTimeout <= 0 {
		return errors.New("operation timeout must be positive")
	}
	if c.OperationTimeout > 1*time.Minute {
		return errors.New("operation timeout should not exceed 1 minute")
	}

	if c.FaultTestDuration <= 0 {
		return errors.New("fault test duration must be positive")
	}
	if c.FaultTestDuration < c.RecoveryDelay {
		return errors.New("fault test duration must be greater than recovery delay")
	}

	if c.FailureDelay <= 0 || c.RecoveryDelay <= 0 {
		return errors.New("failure and recovery delays must be positive")
	}
	if c.FailureDelay >= c.RecoveryDelay {
		return errors.New("failure delay must be less than recovery delay")
	}

	if c.MaxRetries < 0 {
		return errors.New("max retries cannot be negative")
	}
	if c.MaxRetries > 20 {
		return errors.New("max retries should not exceed 20")
	}

	if c.JitterFactor < 0.0 || c.JitterFactor > 1.0 {
		return errors.New("jitter factor must be between 0.0 and 1.0")
	}

	if c.BackoffMultiplier <= 1.0 {
		return errors.New("backoff multiplier must be greater than 1.0")
	}

	if c.BaseBackoff <= 0 || c.MaxBackoff <= 0 {
		return errors.New("backoff durations must be positive")
	}
	if c.BaseBackoff >= c.MaxBackoff {
		return errors.New("base backoff must be less than max backoff")
	}

	validFormats := map[string]bool{"text": true, "json": true, "yaml": true}
	if !validFormats[c.OutputFormat] {
		return fmt.Errorf("invalid output format: %s (must be text, json, or yaml)", c.OutputFormat)
	}

	if c.ConnectionPoolSize <= 0 {
		return errors.New("connection pool size must be positive")
	}
	if c.ConnectionPoolSize > 100 {
		return errors.New("connection pool size should not exceed 100")
	}

	if c.HealthCheckTimeout <= 0 {
		return errors.New("health check timeout must be positive")
	}

	return nil
}

// parseConfig reads flags, applies defaults, and returns a validated Config.
func parseConfig() (*Config, error) {
	cfg := DefaultConfig()

	var (
		serverAddrsStr  string
		containerPrefix string
		showHelp        bool
		showVersion     bool
	)

	flag.BoolVar(&showHelp, "help", false, "Show help message")
	flag.BoolVar(&showHelp, "h", false, "Show help message (shorthand)")
	flag.BoolVar(&showVersion, "version", false, "Show version information")

	flag.StringVar(&serverAddrsStr, "servers", strings.Join(cfg.ServerAddrs, ","),
		"Comma-separated server addresses (host:port format)")
	flag.StringVar(&containerPrefix, "container-prefix", "raftlock",
		"Docker container name prefix")
	flag.BoolVar(&cfg.UseDocker, "use-docker", cfg.UseDocker,
		"Enable Docker for fault simulation")

	flag.IntVar(&cfg.UncontestedOps, "uncontested-ops", cfg.UncontestedOps,
		"Number of operations for uncontested latency test")
	flag.IntVar(&cfg.ContentionOps, "contention-ops", cfg.ContentionOps,
		"Number of operations per contention level")
	flag.IntVar(&cfg.MaxWorkers, "max-workers", cfg.MaxWorkers,
		"Maximum number of concurrent workers")

	flag.DurationVar(&cfg.FaultTestDuration, "fault-duration", cfg.FaultTestDuration,
		"Total duration for fault tolerance test")
	flag.DurationVar(&cfg.FailureDelay, "failure-delay", cfg.FailureDelay,
		"Time before simulating node failure")
	flag.DurationVar(&cfg.RecoveryDelay, "recovery-delay", cfg.RecoveryDelay,
		"Time before starting node recovery")
	flag.DurationVar(&cfg.OperationTimeout, "op-timeout", cfg.OperationTimeout,
		"Timeout for individual operations")
	flag.DurationVar(&cfg.InterOperationDelay, "inter-op-delay", cfg.InterOperationDelay,
		"Delay between consecutive operations")

	flag.IntVar(&cfg.MaxRetries, "max-retries", cfg.MaxRetries,
		"Maximum number of retries per operation")
	flag.DurationVar(&cfg.BaseBackoff, "base-backoff", cfg.BaseBackoff,
		"Base backoff duration for retries")
	flag.DurationVar(&cfg.MaxBackoff, "max-backoff", cfg.MaxBackoff,
		"Maximum backoff duration for retries")
	flag.Float64Var(&cfg.BackoffMultiplier, "backoff-multiplier", cfg.BackoffMultiplier,
		"Exponential backoff multiplier")
	flag.Float64Var(&cfg.JitterFactor, "jitter-factor", cfg.JitterFactor,
		"Jitter factor for backoff randomization (0.0-1.0)")

	flag.BoolVar(&cfg.Verbose, "verbose", cfg.Verbose,
		"Enable verbose logging output")
	flag.StringVar(&cfg.OutputFormat, "output-format", cfg.OutputFormat,
		"Output format: text, json, or yaml")
	flag.StringVar(&cfg.OutputFile, "output-file", cfg.OutputFile,
		"Output file path (stdout if empty)")
	flag.BoolVar(&cfg.GenerateReport, "generate-report", cfg.GenerateReport,
		"Generate detailed benchmark report")

	flag.IntVar(&cfg.ConnectionPoolSize, "pool-size", cfg.ConnectionPoolSize,
		"gRPC connection pool size per server")
	flag.DurationVar(&cfg.HealthCheckTimeout, "health-timeout", cfg.HealthCheckTimeout,
		"Health check timeout duration")
	flag.DurationVar(&cfg.WarmupDuration, "warmup", cfg.WarmupDuration,
		"Cluster warmup duration before tests")

	flag.Parse()

	if showHelp {
		printUsage()
		os.Exit(0)
	}

	if showVersion {
		printVersion()
		os.Exit(0)
	}

	cfg.ServerAddrs = parseServerAddresses(serverAddrsStr)

	cfg.ContainerNames = generateContainerNames(containerPrefix, len(cfg.ServerAddrs))

	return cfg, nil
}

// parseServerAddresses splits a comma-separated string into individual addresses.
func parseServerAddresses(addrsStr string) []string {
	if addrsStr == "" {
		return nil
	}

	addrs := strings.Split(addrsStr, ",")
	result := make([]string, 0, len(addrs))

	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			result = append(result, addr)
		}
	}

	return result
}

// generateContainerNames builds container names using the given prefix.
func generateContainerNames(prefix string, count int) []string {
	names := make([]string, count)
	for i := range count {
		names[i] = fmt.Sprintf("%s-node%d", prefix, i+1)
	}
	return names
}

// printUsage shows full usage instructions and examples.
func printUsage() {
	fmt.Printf(`RaftLock Benchmark Suite

USAGE:
    benchmark [OPTIONS]

OPTIONS:
    -h, --help                     Show this help message
    --version                      Show version information

  Server Configuration:
    --servers STRING              Comma-separated server addresses (default: localhost:8081,localhost:8082,localhost:8083)
    --container-prefix STRING     Docker container name prefix (default: raftlock)
    --use-docker BOOL             Enable Docker for fault simulation (default: true)

  Benchmark Parameters:
    --uncontested-ops INT         Operations for uncontested latency test (default: 1000)
    --contention-ops INT          Operations per contention level (default: 2000)
    --max-workers INT             Maximum concurrent workers (default: 100)
    --fault-duration DURATION     Fault tolerance test duration (default: 3m)
    --failure-delay DURATION      Time before simulating failure (default: 45s)
    --recovery-delay DURATION     Time before recovery (default: 60s)
    --op-timeout DURATION         Per-operation timeout (default: 5s)
    --inter-op-delay DURATION     Delay between operations (default: 10ms)

  Retry Configuration:
    --max-retries INT             Maximum retries per operation (default: 5)
    --base-backoff DURATION       Base backoff duration (default: 50ms)
    --max-backoff DURATION        Maximum backoff duration (default: 2s)
    --backoff-multiplier FLOAT    Backoff multiplier (default: 1.8)
    --jitter-factor FLOAT         Jitter factor 0.0-1.0 (default: 0.2)

  Output Configuration:
    --verbose                     Enable verbose logging
    --output-format STRING        Output format: text|json|yaml (default: text)
    --output-file STRING          Output file path (default: stdout)
    --generate-report             Generate detailed report (default: true)

  Performance Tuning:
    --pool-size INT               Connection pool size per server (default: 10)
    --health-timeout DURATION     Health check timeout (default: 10s)
    --warmup DURATION             Warmup duration (default: 30s)

EXAMPLES:
    # Basic benchmark with defaults
    benchmark

    # Custom server configuration
    benchmark --servers "node1:8080,node2:8080,node3:8080" --verbose

    # High-load benchmark
    benchmark --max-workers 500 --contention-ops 10000 --uncontested-ops 5000

    # JSON output to file
    benchmark --output-format json --output-file results.json

    # Quick test without Docker
    benchmark --use-docker=false --fault-duration 1m --warmup 10s
`)
}

func printVersion() {
	fmt.Printf("RaftLock Benchmark Suite %s\n", benchmarkVersion)
	fmt.Printf("Go version: %s\n", goVersion)
	fmt.Printf("Build: %s\n", build)
}
