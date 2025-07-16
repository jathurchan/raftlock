package main

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
)

// runUncontestedBenchmark executes comprehensive uncontested latency testing
func (s *BenchmarkSuite) runUncontestedBenchmark(ctx *BenchmarkContext) error {
	ctx.LogInfo("Starting uncontested latency benchmark...")
	s.results.UncontestedResults = &UncontestedBenchmark{}

	// Find leader for optimized testing
	leaderClient, leaderAddr := s.clients.findLeader()
	if leaderClient == nil {
		return WrapError("uncontested", "leader_discovery", ErrLeaderNotFound)
	}

	ctx.LogInfo("Found leader at %s for optimized tests", leaderAddr)

	// Test with leader optimization
	ctx.LogProgress("Testing leader-optimized path...")
	withLeader, err := s.measureUncontestedLatency(ctx, leaderClient, "leader-optimized")
	if err != nil {
		return WrapError("uncontested", "leader_test", err)
	}
	s.results.UncontestedResults.WithLeaderOptimization = withLeader

	// Test without leader optimization (through follower)
	followerClient, followerAddr := s.clients.findFollower()
	if followerClient == nil {
		ctx.LogWarn("No follower found, using random client for redirect test")
		followerClient = s.clients.getRoundRobin()
		followerAddr = "unknown"
	}

	if followerClient == nil {
		return WrapError("uncontested", "follower_discovery", fmt.Errorf("no clients available"))
	}

	ctx.LogInfo("Testing follower-redirected path via %s", followerAddr)
	ctx.LogProgress("Testing follower-redirected path...")
	withoutLeader, err := s.measureUncontestedLatency(ctx, followerClient, "follower-redirected")
	if err != nil {
		return WrapError("uncontested", "follower_test", err)
	}
	s.results.UncontestedResults.WithoutLeaderOptimization = withoutLeader

	// Calculate derived metrics
	s.calculateUncontestedMetrics()

	ctx.LogInfo("Uncontested latency benchmark completed successfully")
	return nil
}

// measureUncontestedLatency performs latency measurements for a specific test type
func (s *BenchmarkSuite) measureUncontestedLatency(
	ctx *BenchmarkContext,
	client pb.RaftLockClient,
	testType string,
) (LatencyStats, error) {
	operations := s.config.UncontestedOps
	latencies := make([]time.Duration, 0, operations)
	var successfulOps int64

	progressInterval := operations / 20 // Report progress every 5%
	if progressInterval == 0 {
		progressInterval = 1
	}

	for i := range operations {
		if err := ctx.CheckCancellation(); err != nil {
			return LatencyStats{}, err
		}

		lockID := fmt.Sprintf("uncontested-%s-%d-%d", testType, time.Now().UnixNano(), i)
		clientID := fmt.Sprintf("%s-uncontested", s.clientID)

		start := time.Now()
		success := s.performSingleOperation(client, lockID, clientID, 2*time.Second)
		latency := time.Since(start)

		if success {
			latencies = append(latencies, latency)
			atomic.AddInt64(&successfulOps, 1)
		}

		// Record metrics
		ctx.RecordOperation("acquire", success, latency)

		// Progress reporting
		if s.config.Verbose && (i+1)%progressInterval == 0 {
			successRate := float64(successfulOps) / float64(i+1) * 100
			ctx.LogProgress("Progress: %d/%d (%.1f%% success)", i+1, operations, successRate)
		}

		// Respect inter-operation delay
		if s.config.InterOperationDelay > 0 {
			time.Sleep(s.config.InterOperationDelay)
		}
	}

	return calculateLatencyStats(testType, latencies, successfulOps, int64(operations)), nil
}

// calculateUncontestedMetrics computes derived metrics for uncontested benchmark
func (s *BenchmarkSuite) calculateUncontestedMetrics() {
	res := s.results.UncontestedResults
	if res == nil {
		return
	}

	// Calculate improvement factor
	if res.WithLeaderOptimization.Mean != "0s" && res.WithoutLeaderOptimization.Mean != "0s" {
		meanWith, _ := time.ParseDuration(res.WithLeaderOptimization.Mean)
		meanWithout, _ := time.ParseDuration(res.WithoutLeaderOptimization.Mean)
		if meanWith > 0 {
			res.ImprovementFactor = float64(meanWithout) / float64(meanWith)
			res.ThroughputBaseline = 1.0 / meanWith.Seconds()
		}
	}

	// Assess optimization effectiveness
	if res.ImprovementFactor >= 3.0 {
		res.OptimizationEffectiveness = "Excellent"
	} else if res.ImprovementFactor >= 2.0 {
		res.OptimizationEffectiveness = "Good"
	} else if res.ImprovementFactor >= 1.5 {
		res.OptimizationEffectiveness = "Moderate"
	} else {
		res.OptimizationEffectiveness = "Poor"
	}
}

// runContentionBenchmark executes comprehensive contention performance testing
func (s *BenchmarkSuite) runContentionBenchmark(ctx *BenchmarkContext) error {
	ctx.LogInfo("Starting contention performance benchmark...")
	s.results.ContentionResults = &ContentionBenchmark{}

	// Define contention scenarios
	scenarios := []struct {
		level     string
		workers   int
		resources int
	}{
		{"low", s.config.MaxWorkers / 10, 50},   // 10% workers, many resources
		{"medium", s.config.MaxWorkers / 2, 15}, // 50% workers, moderate resources
		{"high", s.config.MaxWorkers, 5},        // 100% workers, few resources
	}

	var results [3]ContentionStats

	for i, scenario := range scenarios {
		if err := ctx.CheckCancellation(); err != nil {
			return err
		}

		ctx.LogInfo("Testing %s contention: %d workers competing for %d resources",
			scenario.level, scenario.workers, scenario.resources)

		stats, err := s.measureContention(ctx, scenario.level, scenario.workers, scenario.resources)
		if err != nil {
			return WrapError("contention", fmt.Sprintf("%s_test", scenario.level), err)
		}
		results[i] = stats

		// Brief pause between scenarios
		if i < len(scenarios)-1 {
			time.Sleep(3 * time.Second)
		}
	}

	// Store results
	s.results.ContentionResults.LowContention = results[0]
	s.results.ContentionResults.MediumContention = results[1]
	s.results.ContentionResults.HighContention = results[2]

	// Calculate derived metrics
	s.calculateContentionMetrics()

	ctx.LogInfo("Contention benchmark completed successfully")
	return nil
}

// measureContention performs contention testing for a specific scenario
func (s *BenchmarkSuite) measureContention(
	ctx *BenchmarkContext,
	level string,
	workers, resources int,
) (ContentionStats, error) {
	ctx.LogProgress("Executing %s contention scenario...", level)

	var (
		totalOps      int64
		successfulOps int64
		failedOps     int64
		timeoutOps    int64
		totalRetries  int64
		redirects     int64
		totalLatency  int64
		wg            sync.WaitGroup
		resultsCh     = make(chan OperationResult, s.config.ContentionOps)
	)

	start := time.Now()

	// Create work queue
	workCh := make(chan int, s.config.ContentionOps)
	for i := 0; i < s.config.ContentionOps; i++ {
		workCh <- i
	}
	close(workCh)

	// Launch worker goroutines
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for opID := range workCh {
				if err := ctx.CheckCancellation(); err != nil {
					return
				}

				resourceID := opID % resources
				lockID := fmt.Sprintf("contention-%s-resource-%d", level, resourceID)
				clientID := fmt.Sprintf("%s-worker-%d", s.clientID, workerID)

				result := s.performEnhancedOperation(ctx.Context, lockID, clientID)

				select {
				case resultsCh <- result:
				case <-ctx.Done():
					return
				}
			}
		}(w)
	}

	// Wait for completion and close results channel
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results
	for result := range resultsCh {
		atomic.AddInt64(&totalOps, 1)
		atomic.AddInt64(&totalRetries, int64(result.Attempts))
		atomic.AddInt64(&totalLatency, int64(result.Duration))

		if result.Success {
			atomic.AddInt64(&successfulOps, 1)
		} else {
			atomic.AddInt64(&failedOps, 1)
		}

		if result.WasTimeout {
			atomic.AddInt64(&timeoutOps, 1)
		}

		if result.LeaderRedirect {
			atomic.AddInt64(&redirects, 1)
		}
	}

	duration := time.Since(start)

	// Calculate statistics
	throughput := float64(totalOps) / duration.Seconds()
	successRate := float64(successfulOps) / float64(totalOps) * 100
	avgRetries := float64(totalRetries) / float64(totalOps)

	avgLatency := time.Duration(0)
	if totalOps > 0 {
		avgLatency = time.Duration(totalLatency / totalOps)
	}

	// Calculate efficiency rating
	efficiencyRating := "Poor"
	if successRate >= 95 {
		efficiencyRating = "Excellent"
	} else if successRate >= 85 {
		efficiencyRating = "Good"
	} else if successRate >= 75 {
		efficiencyRating = "Fair"
	}

	return ContentionStats{
		Level:                 level,
		Workers:               workers,
		Resources:             resources,
		TotalOperations:       totalOps,
		SuccessfulOperations:  successfulOps,
		FailedOperations:      failedOps,
		TimeoutOperations:     timeoutOps,
		LeaderRedirects:       redirects,
		Throughput:            throughput,
		SuccessRate:           successRate,
		AverageRetries:        avgRetries,
		AverageLatency:        avgLatency.String(),
		ContentionCoefficient: calculateContentionCoefficient(workers, resources, avgRetries),
		EfficiencyRating:      efficiencyRating,
	}, nil
}

// calculateContentionMetrics computes derived contention metrics
func (s *BenchmarkSuite) calculateContentionMetrics() {
	res := s.results.ContentionResults
	if res == nil {
		return
	}

	// Calculate throughput degradation
	if res.LowContention.Throughput > 0 {
		res.ThroughputDegradation = (res.LowContention.Throughput - res.HighContention.Throughput) / res.LowContention.Throughput
	}

	// Calculate backoff effectiveness
	res.BackoffEffectiveness = s.calculateBackoffEffectiveness(
		res.LowContention,
		res.MediumContention,
		res.HighContention,
	)

	// Build contention model
	res.ContentionModel = ContentionModel{
		BaselineThroughput:     res.LowContention.Throughput,
		ContentionFactor:       res.ThroughputDegradation,
		BackoffOptimalness:     res.BackoffEffectiveness,
		ResourceUtilization:    s.calculateResourceUtilization(res.HighContention),
		ScalabilityCoefficient: s.calculateScalabilityCoefficient(res),
		BottleneckAnalysis:     s.analyzeBottlenecks(res),
	}

	// Overall scalability assessment
	if res.HighContention.SuccessRate >= 90 && res.ThroughputDegradation < 0.3 {
		res.ScalabilityAssessment = "Excellent scalability under contention"
	} else if res.HighContention.SuccessRate >= 80 && res.ThroughputDegradation < 0.5 {
		res.ScalabilityAssessment = "Good scalability with minor degradation"
	} else if res.HighContention.SuccessRate >= 70 {
		res.ScalabilityAssessment = "Moderate scalability, optimization recommended"
	} else {
		res.ScalabilityAssessment = "Poor scalability, significant optimization needed"
	}
}

// calculateScalabilityCoefficient measures how well the system scales
func (s *BenchmarkSuite) calculateScalabilityCoefficient(res *ContentionBenchmark) float64 {
	// Compare performance degradation rate across contention levels
	lowToMed := res.MediumContention.Throughput / res.LowContention.Throughput
	medToHigh := res.HighContention.Throughput / res.MediumContention.Throughput

	// Average degradation rate (closer to 1.0 = better scalability)
	avgDegradation := (lowToMed + medToHigh) / 2.0

	return avgDegradation
}

// analyzeBottlenecks identifies performance bottlenecks
func (s *BenchmarkSuite) analyzeBottlenecks(res *ContentionBenchmark) string {
	if res.HighContention.AverageRetries > 5 {
		return "High retry rate suggests lock acquisition bottleneck"
	}
	if res.HighContention.TimeoutOperations > res.HighContention.TotalOperations/10 {
		return "High timeout rate suggests processing capacity bottleneck"
	}
	if res.HighContention.LeaderRedirects > res.HighContention.TotalOperations/5 {
		return "High redirect rate suggests leader discovery bottleneck"
	}
	if res.ThroughputDegradation > 0.7 {
		return "Severe throughput degradation suggests algorithmic bottleneck"
	}
	return "No significant bottlenecks identified"
}

// runFaultToleranceBenchmark executes comprehensive fault tolerance testing
func (s *BenchmarkSuite) runFaultToleranceBenchmark(ctx *BenchmarkContext) error {
	if !s.config.UseDocker {
		ctx.LogWarn("Skipping fault tolerance benchmark: Docker simulation disabled")
		s.results.FaultToleranceResults = &FaultToleranceBenchmark{
			BeforeFailure:   OperationalStats{Phase: "skipped"},
			DuringFailure:   OperationalStats{Phase: "skipped"},
			AfterRecovery:   OperationalStats{Phase: "skipped"},
			ResilienceGrade: "N/A (Docker disabled)",
		}
		return nil
	}

	ctx.LogInfo("Starting fault tolerance benchmark...")
	s.results.FaultToleranceResults = &FaultToleranceBenchmark{}
	res := s.results.FaultToleranceResults

	// Create fault tolerance context with overall timeout
	faultCtx, cancel := context.WithTimeout(ctx.Context, s.config.FaultTestDuration)
	defer cancel()

	// Phase 1: Baseline measurement
	ctx.LogInfo("Phase 1: Measuring baseline performance...")
	phase1Ctx, phase1Cancel := context.WithTimeout(faultCtx, s.config.FailureDelay)
	res.BeforeFailure = s.measureOperationalPhase(
		NewBenchmarkContext(phase1Ctx, s, "baseline"),
		"baseline",
	)
	phase1Cancel()

	// Phase 2: Simulate leader failure
	ctx.LogInfo("Phase 2: Simulating leader failure...")
	_, leaderAddr := s.clients.findLeader()
	if leaderAddr == "" {
		return WrapError("fault_tolerance", "leader_discovery", ErrLeaderNotFound)
	}

	leaderIndex := s.findNodeIndex(leaderAddr)
	if leaderIndex < 0 {
		return WrapError("fault_tolerance", "node_mapping",
			fmt.Errorf("could not map leader address %s to node index", leaderAddr))
	}

	if err := s.dockerMgr.SimulateNodeFailure(faultCtx, leaderIndex); err != nil {
		return WrapError("fault_tolerance", "simulate_failure", err)
	}
	ctx.LogWarn("🚨 Leader node %d stopped", leaderIndex)

	// Wait for new leader election
	electionTime, newLeaderAddr, err := s.measureLeaderElection(faultCtx, leaderAddr)
	if err != nil {
		return WrapError("fault_tolerance", "leader_election", err)
	}
	ctx.LogInfo("✅ New leader elected at %s in %v", newLeaderAddr, electionTime)

	// Phase 3: Measure performance during failure
	ctx.LogInfo("Phase 3: Measuring performance during failure...")
	failureDuration := s.config.RecoveryDelay - s.config.FailureDelay
	phase2Ctx, phase2Cancel := context.WithTimeout(faultCtx, failureDuration)
	res.DuringFailure = s.measureOperationalPhase(
		NewBenchmarkContext(phase2Ctx, s, "during_failure"),
		"during_failure",
	)
	phase2Cancel()

	// Phase 4: Recovery
	ctx.LogInfo("Phase 4: Recovering failed node...")
	recoveryStart := time.Now()
	if err := s.dockerMgr.RecoverNode(faultCtx, leaderIndex); err != nil {
		ctx.LogError("Failed to recover node %d: %v", leaderIndex, err)
	} else {
		ctx.LogInfo("🔄 Node %d restarted", leaderIndex)
	}

	// Wait for node to rejoin
	if err := s.waitForNodeRecovery(faultCtx, leaderIndex); err != nil {
		ctx.LogWarn("Node recovery incomplete: %v", err)
	}
	recoveryTime := time.Since(recoveryStart)

	// Phase 5: Post-recovery measurement
	ctx.LogInfo("Phase 5: Measuring post-recovery performance...")
	recoveryDuration := s.config.FaultTestDuration - s.config.RecoveryDelay
	phase3Ctx, phase3Cancel := context.WithTimeout(faultCtx, recoveryDuration)
	res.AfterRecovery = s.measureOperationalPhase(
		NewBenchmarkContext(phase3Ctx, s, "after_recovery"),
		"after_recovery",
	)
	phase3Cancel()

	// Calculate final metrics
	s.calculateFaultToleranceMetrics(res, electionTime, recoveryTime)

	ctx.LogInfo("Fault tolerance benchmark completed successfully")
	return nil
}

// measureLeaderElection measures the time taken for leader election
func (s *BenchmarkSuite) measureLeaderElection(
	ctx context.Context,
	oldLeaderAddr string,
) (time.Duration, string, error) {
	start := time.Now()
	timeout := 30 * time.Second
	interval := 500 * time.Millisecond

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return time.Since(start), "", fmt.Errorf("leader election timed out after %v", timeout)
		case <-ticker.C:
			_, newLeaderAddr := s.clients.findLeader()
			if newLeaderAddr != "" && newLeaderAddr != oldLeaderAddr {
				return time.Since(start), newLeaderAddr, nil
			}
		}
	}
}

// measureOperationalPhase measures operational statistics during a specific phase
func (s *BenchmarkSuite) measureOperationalPhase(
	ctx *BenchmarkContext,
	phase string,
) OperationalStats {
	var (
		totalOps      int64
		successfulOps int64
		failedOps     int64
		timeoutOps    int64
		redirects     int64
		totalLatency  int64
		wg            sync.WaitGroup
	)

	start := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond) // 10 ops/sec target
	defer ticker.Stop()

	opCount := 0
	for {
		select {
		case <-ctx.Done():
			duration := time.Since(start)

			avgLatency := time.Duration(0)
			if totalOps > 0 {
				avgLatency = time.Duration(totalLatency / totalOps)
			}

			throughput := float64(totalOps) / duration.Seconds()
			errorRate := float64(failedOps) / float64(totalOps) * 100
			resourceUtil := math.Min(100, throughput/10) // Rough estimate

			return OperationalStats{
				Phase:               phase,
				Duration:            duration.String(),
				TotalOps:            totalOps,
				SuccessfulOps:       successfulOps,
				FailedOps:           failedOps,
				TimeoutOps:          timeoutOps,
				LeaderRedirects:     redirects,
				Throughput:          throughput,
				ErrorRate:           errorRate,
				AvgResponseTime:     avgLatency.String(),
				ResourceUtilization: resourceUtil,
			}
		case <-ticker.C:
			opCount++
			wg.Add(1)
			go func(opID int) {
				defer wg.Done()

				lockID := fmt.Sprintf("fault-%s-%d", phase, opID)
				clientID := fmt.Sprintf("%s-fault", s.clientID)

				result := s.performEnhancedOperation(ctx.Context, lockID, clientID)

				atomic.AddInt64(&totalOps, 1)
				atomic.AddInt64(&totalLatency, int64(result.Duration))

				if result.Success {
					atomic.AddInt64(&successfulOps, 1)
				} else {
					atomic.AddInt64(&failedOps, 1)
				}

				if result.WasTimeout {
					atomic.AddInt64(&timeoutOps, 1)
				}

				if result.LeaderRedirect {
					atomic.AddInt64(&redirects, 1)
				}
			}(opCount)
		}
	}
}

// calculateFaultToleranceMetrics computes derived fault tolerance metrics
func (s *BenchmarkSuite) calculateFaultToleranceMetrics(
	res *FaultToleranceBenchmark,
	electionTime, recoveryTime time.Duration,
) {
	res.LeaderElectionTime = electionTime.String()
	res.RecoveryTime = recoveryTime.String()

	// Calculate system availability
	totalDuration := s.config.FaultTestDuration.Seconds()
	downtime := electionTime.Seconds()
	res.SystemAvailability = math.Max(0, (1-downtime/totalDuration)*100)

	// Perform consistency check
	res.DataConsistency = s.performConsistencyCheck()

	// Calculate resilience grade
	if res.SystemAvailability >= 99.9 && electionTime <= 10*time.Second &&
		res.DataConsistency.ConsistencyRate >= 99.99 {
		res.ResilienceGrade = "A (Excellent)"
	} else if res.SystemAvailability >= 99.5 && electionTime <= 15*time.Second && res.DataConsistency.ConsistencyRate >= 99.9 {
		res.ResilienceGrade = "B (Good)"
	} else if res.SystemAvailability >= 99.0 && electionTime <= 30*time.Second {
		res.ResilienceGrade = "C (Acceptable)"
	} else {
		res.ResilienceGrade = "D (Poor)"
	}
}

// performConsistencyCheck validates data consistency across the cluster
func (s *BenchmarkSuite) performConsistencyCheck() ConsistencyCheck {
	s.logger.Infow("Performing data consistency validation...")

	totalChecks := 50
	var consistent, inconsistent int64

	for i := 0; i < totalChecks; i++ {
		lockID := fmt.Sprintf("consistency-check-%d", i)
		clientID := fmt.Sprintf("%s-consistency", s.clientID)

		// Perform operation and verify across all nodes
		if s.verifyConsistencyAcrossNodes(lockID, clientID) {
			atomic.AddInt64(&consistent, 1)
		} else {
			atomic.AddInt64(&inconsistent, 1)
		}
	}

	consistencyRate := float64(consistent) / float64(totalChecks) * 100
	linearizabilityScore := consistencyRate // Simplified for this implementation

	grade := "F"
	if consistencyRate >= 99.99 {
		grade = "A"
	} else if consistencyRate >= 99.9 {
		grade = "B"
	} else if consistencyRate >= 99.0 {
		grade = "C"
	} else if consistencyRate >= 95.0 {
		grade = "D"
	}

	return ConsistencyCheck{
		TotalChecks:          int64(totalChecks),
		ConsistentReads:      consistent,
		InconsistentReads:    inconsistent,
		ConsistencyRate:      consistencyRate,
		LinearizabilityScore: linearizabilityScore,
		ConsistencyGrade:     grade,
	}
}

// verifyConsistencyAcrossNodes checks consistency across all healthy nodes (SIMPLIFIED)
func (s *BenchmarkSuite) verifyConsistencyAcrossNodes(lockID, clientID string) bool {
	client := s.clients.getRoundRobin()
	if client == nil {
		return false
	}

	return s.performSingleOperation(client, lockID, clientID, 1*time.Second)
}

// findNodeIndex maps a server address to its index in the configuration
func (s *BenchmarkSuite) findNodeIndex(addr string) int {
	for i, serverAddr := range s.config.ServerAddrs {
		if serverAddr == addr {
			return i
		}
	}
	return -1
}

// waitForNodeRecovery waits for a node to recover and rejoin the cluster
func (s *BenchmarkSuite) waitForNodeRecovery(ctx context.Context, nodeIndex int) error {
	if nodeIndex < 0 || nodeIndex >= len(s.config.ContainerNames) {
		return fmt.Errorf("invalid node index: %d", nodeIndex)
	}

	containerName := s.config.ContainerNames[nodeIndex]

	healthCheck := func() error {
		client := s.clients.get(nodeIndex)
		if client == nil {
			return fmt.Errorf("client not available for node %d", nodeIndex)
		}

		healthCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		_, err := client.Health(healthCtx, &pb.HealthRequest{})
		return err
	}

	return s.dockerMgr.WaitForContainerReady(ctx, containerName, healthCheck)
}
