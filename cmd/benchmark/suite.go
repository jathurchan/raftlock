package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/jathurchan/raftlock/cmd/benchmark/internal"
	"github.com/jathurchan/raftlock/logger"
)

// newBenchmarkSuite initializes and returns a fully configured BenchmarkSuite.
func newBenchmarkSuite(config *Config) (*BenchmarkSuite, error) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()
	logLevel := "info"
	if config.Verbose {
		logLevel = "debug"
	}

	suite := &BenchmarkSuite{
		config:    config,
		logger:    logger.NewStdLogger(logLevel),
		clientID:  fmt.Sprintf("benchmark-suite-%d-%d", time.Now().Unix(), r.Intn(10000)),
		results:   &BenchmarkResults{StartTime: start},
		metrics:   internal.NewMetricsCollector(),
		startTime: start,
	}

	suite.results.SystemInfo = &SystemInfo{
		BenchmarkVersion: benchmarkVersion,
		StartTimestamp:   suite.startTime,
		TestEnvironment:  "automated-benchmark",
		ClusterSize:      len(config.ServerAddrs),
		ClientVersion:    "raftlock-client-v1.0",
		GoVersion:        runtime.Version(),
		Platform:         fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		ConfigSummary:    suite.generateConfigSummary(),
	}

	suite.results.SuiteConfig = config

	suite.dockerMgr = internal.NewDockerManager(
		config.UseDocker,
		config.ContainerNames,
		10*time.Second,
	)

	if err := suite.initializeClients(); err != nil {
		return nil, WrapError("initialization", "client_setup", err)
	}

	suite.logger.Infow("✅ Benchmark suite initialized successfully")
	return suite, nil
}

// generateConfigSummary returns a summary of key configuration values for reporting.
func (s *BenchmarkSuite) generateConfigSummary() map[string]string {
	return map[string]string{
		"servers":          fmt.Sprintf("%d servers", len(s.config.ServerAddrs)),
		"uncontested_ops":  fmt.Sprintf("%d", s.config.UncontestedOps),
		"contention_ops":   fmt.Sprintf("%d", s.config.ContentionOps),
		"max_workers":      fmt.Sprintf("%d", s.config.MaxWorkers),
		"fault_duration":   s.config.FaultTestDuration.String(),
		"use_docker":       fmt.Sprintf("%t", s.config.UseDocker),
		"output_format":    s.config.OutputFormat,
		"connection_pools": fmt.Sprintf("%d per server", s.config.ConnectionPoolSize),
	}
}

// initializeClients sets up client connections and performs a cluster health check.
func (s *BenchmarkSuite) initializeClients() error {
	s.logger.Infow("Initializing client connections to %d servers...", len(s.config.ServerAddrs))

	s.clients = newClientPool(s.config.ServerAddrs, s.config.ConnectionPoolSize)
	if err := s.clients.connect(); err != nil {
		return NewConnectionError("cluster", "initial_connect", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.HealthCheckTimeout)
	defer cancel()

	if err := s.clients.validateClusterHealth(ctx); err != nil {
		return WrapErrorWithContext("initialization", "health_check", err, map[string]interface{}{
			"timeout": s.config.HealthCheckTimeout.String(),
			"servers": s.config.ServerAddrs,
		})
	}

	s.logger.Infow("✅ Cluster health validated: %d/%d nodes healthy",
		s.clients.healthyNodes, len(s.config.ServerAddrs))
	return nil
}

// runWithContext runs all benchmark phases with context and cancellation support.
func (s *BenchmarkSuite) runWithContext(ctx context.Context) error {
	if err := s.performPreflightChecks(ctx); err != nil {
		return WrapError("preflight", "validation", err)
	}

	if s.config.WarmupDuration > 0 {
		s.logger.Infow("🔥 Warming up cluster for %v...", s.config.WarmupDuration)
		if err := s.warmupCluster(ctx); err != nil {
			return WrapError("warmup", "cluster_warmup", err)
		}
	}

	s.logger.Infow("⏳ Allowing cluster stabilization...")
	select {
	case <-time.After(10 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	s.logger.Infow("\n🎯 EXECUTING COMPREHENSIVE PERFORMANCE BENCHMARKS")
	s.logger.Infow("=================================================")

	benchmarks := []struct {
		name        string
		description string
		fn          func(context.Context) error
		required    bool
	}{
		{
			"uncontested",
			"Uncontested Latency Assessment",
			s.runUncontestedBenchmarkWithContext,
			true,
		},
		{
			"contention",
			"Contention Performance Analysis",
			s.runContentionBenchmarkWithContext,
			true,
		},
		{
			"fault_tolerance",
			"Fault Tolerance & Recovery Validation",
			s.runFaultToleranceBenchmarkWithContext,
			s.config.UseDocker,
		},
	}

	for i, benchmark := range benchmarks {
		if !benchmark.required {
			s.logger.Infow(
				"⏭️  Skipping %s (not required in current configuration)",
				benchmark.description,
			)
			continue
		}

		s.logger.Infow("\n📊 Benchmark %d/%d: %s", i+1, len(benchmarks), benchmark.description)
		s.logger.Infow(strings.Repeat("=", 60))

		if err := benchmark.fn(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return WrapErrorWithContext("execution", benchmark.name, err, map[string]interface{}{
				"benchmark_index":  i + 1,
				"total_benchmarks": len(benchmarks),
			})
		}

		if i < len(benchmarks)-1 {
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	s.logger.Infow("\n🎯 GENERATING COMPREHENSIVE ANALYSIS")
	s.logger.Infow("====================================")

	s.results.EndTime = time.Now()
	s.results.TotalDuration = s.results.EndTime.Sub(s.results.StartTime).String()
	s.results.SystemInfo.EndTimestamp = s.results.EndTime

	s.generateBenchmarkSummary()

	if s.config.GenerateReport {
		if err := s.generateAndOutputReport(); err != nil {
			return WrapError("reporting", "generate_output", err)
		}
	}

	return nil
}

// performPreflightChecks verifies environment setup before execution.
func (s *BenchmarkSuite) performPreflightChecks(ctx context.Context) error {
	s.logger.Infow("🔍 Performing pre-flight checks...")

	if s.config.UseDocker {
		if err := s.dockerMgr.VerifyDockerAvailable(ctx); err != nil {
			return WrapError("preflight", "docker_verification", err)
		}
		if err := s.dockerMgr.VerifyContainersExist(ctx); err != nil {
			return WrapError("preflight", "container_verification", err)
		}
		s.logger.Infow("✅ Docker environment verified")
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := s.clients.validateClusterHealth(ctx); err != nil {
		return WrapError("preflight", "cluster_health", err)
	}

	leaderClient, leaderAddr := s.clients.findLeader()
	if leaderClient == nil {
		return WrapError("preflight", "leader_discovery", ErrLeaderNotFound)
	}
	s.logger.Infow("✅ Leader identified at %s", leaderAddr)

	s.logger.Infow("✅ All pre-flight checks passed")
	return nil
}

// warmupCluster performs light operations to stabilize the system before benchmarks.
func (s *BenchmarkSuite) warmupCluster(ctx context.Context) error {
	warmupCtx, cancel := context.WithTimeout(ctx, s.config.WarmupDuration)
	defer cancel()

	warmupOps := 100
	successfulOps := 0
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for i := 0; i < warmupOps; i++ {
		select {
		case <-warmupCtx.Done():
			if warmupCtx.Err() == context.DeadlineExceeded {
				break
			}
			return warmupCtx.Err()
		case <-ticker.C:
			lockID := fmt.Sprintf("warmup-lock-%d", i)
			client := s.clients.getRoundRobin()
			if client == nil {
				continue
			}
			if s.performSingleOperation(client, lockID, s.clientID, 1*time.Second) {
				successfulOps++
			}
		}
	}

	successRate := float64(successfulOps) / float64(warmupOps) * 100
	if successRate < 80 {
		return WrapErrorWithContext("warmup", "low_success_rate",
			fmt.Errorf("warmup success rate too low: %.1f%%", successRate),
			map[string]interface{}{
				"successful_ops": successfulOps,
				"total_ops":      warmupOps,
				"success_rate":   successRate,
			})
	}

	s.logger.Infow("✅ Cluster warmup completed (%d/%d operations successful, %.1f%%)",
		successfulOps, warmupOps, successRate)
	return nil
}

// runUncontestedBenchmarkWithContext runs the uncontested benchmark phase.
func (s *BenchmarkSuite) runUncontestedBenchmarkWithContext(ctx context.Context) error {
	benchCtx := NewBenchmarkContext(ctx, s, "uncontested")
	return s.runUncontestedBenchmark(benchCtx)
}

// runContentionBenchmarkWithContext runs the contention benchmark phase.
func (s *BenchmarkSuite) runContentionBenchmarkWithContext(ctx context.Context) error {
	benchCtx := NewBenchmarkContext(ctx, s, "contention")
	return s.runContentionBenchmark(benchCtx)
}

// runFaultToleranceBenchmarkWithContext runs the fault tolerance benchmark phase.
func (s *BenchmarkSuite) runFaultToleranceBenchmarkWithContext(ctx context.Context) error {
	benchCtx := NewBenchmarkContext(ctx, s, "fault_tolerance")
	return s.runFaultToleranceBenchmark(benchCtx)
}

// generateAndOutputReport creates the final report and writes it to the configured output.
func (s *BenchmarkSuite) generateAndOutputReport() error {
	reporter, writer, err := NewReporter(s.config)
	if err != nil {
		return fmt.Errorf("failed to create reporter: %w", err)
	}
	defer func() {
		if writer != nil && writer != os.Stdout {
			_ = writer.Close()
		}
	}()

	if err := reporter.Generate(s.results); err != nil {
		return fmt.Errorf("failed to generate report: %w", err)
	}

	if s.config.OutputFile != "" {
		s.logger.Infow("📄 Report written to %s", s.config.OutputFile)
	}

	return nil
}

// cleanup releases resources and prints final metrics summary.
func (s *BenchmarkSuite) cleanup() {
	s.logger.Infow("🧹 Cleaning up benchmark resources...")

	if s.clients != nil {
		s.clients.close()
	}

	if s.metrics != nil {
		summary := s.metrics.GetMetricsSummary()
		s.logger.Infow("📊 Final metrics summary:")
		s.logger.Infow("   Total operations: %d", summary.TotalOperations)
		s.logger.Infow("   Total duration: %v", summary.TotalDuration)
		if len(summary.OperationCounts) > 0 {
			s.logger.Infow("   Operation breakdown:")
			for op, count := range summary.OperationCounts {
				errorRate := summary.ErrorRates[op]
				avgLatency := summary.AvgLatencies[op]
				s.logger.Infow("     %s: %d ops (%.2f%% errors, %v avg)",
					op, count, errorRate*100, avgLatency)
			}
		}
	}

	s.logger.Infow("✅ Cleanup completed")
}

// printBanner logs a summary banner showing the test configuration.
func (s *BenchmarkSuite) printBanner() {
	s.logger.Infow("🚀 RaftLock Comprehensive Performance & Resilience Benchmark Suite")
	s.logger.Infow("==================================================================")
	s.logger.Infow("Version: 3.0.0 | Focus: Production Readiness Assessment")
	s.logger.Infow("")

	s.logger.Infow("📋 Configuration Summary:")
	s.logger.Infow("  Cluster: %d servers (%v)", len(s.config.ServerAddrs), s.config.ServerAddrs)
	s.logger.Infow("  Client ID: %s", s.clientID)
	s.logger.Infow("  Docker Simulation: %t", s.config.UseDocker)
	s.logger.Infow("")

	s.logger.Infow("🎯 Test Parameters:")
	s.logger.Infow("  • Uncontested Operations: %s", formatNumber(s.config.UncontestedOps))
	s.logger.Infow("  • Contention Operations: %s per level", formatNumber(s.config.ContentionOps))
	s.logger.Infow("  • Maximum Workers: %s", formatNumber(s.config.MaxWorkers))
	s.logger.Infow("  • Fault Test Duration: %v", s.config.FaultTestDuration)
	s.logger.Infow("  • Operation Timeout: %v", s.config.OperationTimeout)
	s.logger.Infow("")

	s.logger.Infow("⚙️  Advanced Settings:")
	s.logger.Infow("  • Connection Pools: %d per server", s.config.ConnectionPoolSize)
	s.logger.Infow("  • Max Retries: %d", s.config.MaxRetries)
	s.logger.Infow(
		"  • Backoff: %v - %v (×%.1f)",
		s.config.BaseBackoff,
		s.config.MaxBackoff,
		s.config.BackoffMultiplier,
	)
	s.logger.Infow("  • Output: %s format", s.config.OutputFormat)
	if s.config.OutputFile != "" {
		s.logger.Infow("  • Output File: %s", s.config.OutputFile)
	}
	s.logger.Infow("")
}

// formatNumber formats integers with unit suffixes for display (e.g., 1.2K, 3.4M).
func formatNumber(n int) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}
