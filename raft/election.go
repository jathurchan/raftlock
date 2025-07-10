package raft

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

// ElectionState represents the current state of the election process
type ElectionState int32

const (
	ElectionStateIdle ElectionState = iota
	ElectionStateVoting
)

// SplitVoteDetector tracks election patterns to detect and recover from split votes
type SplitVoteDetector struct {
	mu               sync.RWMutex
	consecutiveFails int
	lastFailTime     time.Time
	termHistory      []types.Term
	maxHistorySize   int
	electionAttempts int
}

// LeaderInitializer defines the interface for actions that must be taken
// when a node transitions to the leader role in a Raft cluster.
type LeaderInitializer interface {
	// InitializeLeaderState prepares the internal replication state for leadership.
	// This typically involves resetting tracking structures such as nextIndex and matchIndex
	// for all followers.
	InitializeLeaderState()

	// SendHeartbeats sends initial AppendEntries RPCs (heartbeats) to all follower nodes.
	// This serves to establish authority as the new leader and prevent election timeouts.
	// Should only be called by the leader.
	SendHeartbeats(ctx context.Context)
}

// ElectionManager defines the interface for managing leader elections
// in the Raft consensus algorithm. It handles election timeouts,
// vote requests and responses, and transitions to the leader role.
type ElectionManager interface {
	// SetNetworkManager allows late injection of the network manager after the Raft node is built.
	// Must be called before Start().
	SetNetworkManager(nm NetworkManager)

	// Initialize prepares the election manager for operation,
	// setting the initial randomized election timeout.
	Initialize(ctx context.Context) error

	// Tick advances the election timer by one logical tick.
	// If the election timeout expires, it triggers a new election or pre-vote.
	// This should be invoked periodically by the main Raft loop.
	Tick(ctx context.Context)

	// ResetTimerOnHeartbeat resets the election timer upon receiving
	// a valid heartbeat (AppendEntries RPC) from the current leader.
	// This prevents unnecessary elections while a leader is active.
	ResetTimerOnHeartbeat()

	// HandleRequestVote processes an incoming RequestVote RPC from a candidate
	// and returns a reply indicating whether the vote was granted, based on
	// term, log freshness, and prior votes.
	HandleRequestVote(
		ctx context.Context,
		args *types.RequestVoteArgs,
	) (*types.RequestVoteReply, error)

	// Stop performs any necessary cleanup when shutting down the election manager.
	Stop()
}

// electionManager is the concrete implementation of ElectionManager.
// It coordinates leader election in the Raft consensus algorithm,
// with enhanced split vote prevention and recovery mechanisms.
type electionManager struct {
	id         types.NodeID
	peers      map[types.NodeID]PeerConfig // All known peer nodes
	quorumSize int                         // Minimum votes required to achieve quorum

	mu         *sync.RWMutex // Protects shared election timing state
	isShutdown *atomic.Bool  // Signals whether the election manager is shutting down

	stateMgr          StateManager
	logMgr            LogManager
	networkMgr        NetworkManager
	leaderInitializer LeaderInitializer
	metrics           Metrics
	logger            logger.Logger
	rand              Rand
	clock             Clock // Use the clock interface for time operations

	electionTickCount   int     // Base number of ticks before an election timeout
	randomizationFactor float64 // Randomization percentage to avoid election collisions

	electionElapsed  int // Ticks elapsed since last heartbeat
	randomizedPeriod int // Randomized timeout threshold for current cycle

	voteMu        sync.RWMutex          // To isolate vote tracking from mu and reduce contention
	votesReceived map[types.NodeID]bool // Tracks which peers have granted votes
	voteCount     int                   // Number of votes granted in the current term
	voteTerm      types.Term            // Term in which votes are currently being collected

	electionState    atomic.Int32  // Current election phase (idle, pre-voting, voting)
	electionInFlight atomic.Bool   // Ensures only one election runs at a time
	lastElectionTime atomic.Int64  // Unix timestamp of last election attempt
	electionCount    atomic.Uint64 // Total number of elections triggered
	concurrentOps    atomic.Int32  // Number of active operations (e.g., ticks, RPCs)

	handleElectionTimeout func(ctx context.Context) // Handles election timeout logic
	startElection         func(ctx context.Context) // Begins a real election

	timerMu sync.Mutex // Serializes access to pre-vote/election delay timers to prevent overlapping triggers

	electionStartDelay time.Duration // Random delay before starting election after pre-vote

	splitVoteDetector *SplitVoteDetector // Tracks and recovers from split votes
	nodeBasedSeed     int64              // Deterministic seed based on node ID

	stopOnce sync.Once     // Ensures Stop() is only executed once
	stopCh   chan struct{} // Closed to signal shutdown to background workers
}

// ElectionManagerDeps encapsulates all dependencies
type ElectionManagerDeps struct {
	ID                types.NodeID
	Peers             map[types.NodeID]PeerConfig
	QuorumSize        int
	Mu                *sync.RWMutex
	IsShutdown        *atomic.Bool
	StateMgr          StateManager
	LogMgr            LogManager
	NetworkMgr        NetworkManager
	LeaderInitializer LeaderInitializer
	Metrics           Metrics
	Logger            logger.Logger
	Rand              Rand
	Clock             Clock // Added clock dependency
	Config            Config
}

// NewElectionManager creates a new election manager with enhanced split vote prevention
func NewElectionManager(deps ElectionManagerDeps) (ElectionManager, error) {
	if err := validateElectionManagerDeps(deps); err != nil {
		return nil, fmt.Errorf("invalid election manager dependencies: %w", err)
	}

	log := deps.Logger.WithComponent("election")
	opts := deps.Config.Options

	// Calculate node-based deterministic seed
	nodeBasedSeed := int64(0)
	for _, b := range []byte(string(deps.ID)) {
		nodeBasedSeed = nodeBasedSeed*31 + int64(b)
	}

	em := &electionManager{
		id:         deps.ID,
		peers:      deps.Peers,
		quorumSize: deps.QuorumSize,
		isShutdown: deps.IsShutdown,

		mu:                deps.Mu,
		stateMgr:          deps.StateMgr,
		logMgr:            deps.LogMgr,
		networkMgr:        deps.NetworkMgr,
		leaderInitializer: deps.LeaderInitializer,
		metrics:           deps.Metrics,
		logger:            log,
		rand:              deps.Rand,
		clock:             deps.Clock,

		electionTickCount:   opts.ElectionTickCount,
		randomizationFactor: opts.ElectionRandomizationFactor,

		electionStartDelay: time.Duration(nodeBasedSeed%100)*time.Millisecond +
			time.Duration(deps.Rand.IntN(100))*time.Millisecond,
		nodeBasedSeed: nodeBasedSeed,

		votesReceived: make(map[types.NodeID]bool),

		stopCh: make(chan struct{}),

		splitVoteDetector: &SplitVoteDetector{
			maxHistorySize: 10,
			termHistory:    make([]types.Term, 0, 10),
		},
	}

	em.electionState.Store(int32(ElectionStateIdle))

	em.handleElectionTimeout = em.defaultHandleElectionTimeout
	em.startElection = em.defaultStartElection

	em.applyDefaults()
	em.resetElectionTimeoutPeriod()

	log.Infow("Election manager initialized with enhanced split vote prevention",
		"nodeID", em.id,
		"electionTickCount", em.electionTickCount,
		"randomizationFactor", em.randomizationFactor,
		"quorumSize", em.quorumSize,
		"peerCount", len(em.peers)-1,
		"nodeBasedSeed", em.nodeBasedSeed)

	return em, nil
}

// SetNetworkManager sets the network manager for the election manager.
func (em *electionManager) SetNetworkManager(nm NetworkManager) {
	em.networkMgr = nm
	em.logger.Debugw("Network manager set", "nodeID", em.id)
}

// Initialize sets up the election manager's state and timers.
func (em *electionManager) Initialize(ctx context.Context) error {
	if em.isShutdown.Load() {
		return ErrShuttingDown
	}

	if em.networkMgr == nil {
		return errors.New("network manager must be set before initialization")
	}

	em.resetElectionTimeoutPeriod()

	em.logger.Infow("Election manager initialized",
		"nodeID", em.id,
		"randomizedPeriod", em.randomizedPeriod)

	return nil
}

// validateElectionManagerDeps ensures all required dependencies are provided
func validateElectionManagerDeps(deps ElectionManagerDeps) error {
	if deps.ID == "" {
		return errors.New("ID must be provided")
	}
	if deps.QuorumSize <= 0 {
		return fmt.Errorf("quorum size must be positive (got %d)", deps.QuorumSize)
	}
	if deps.Mu == nil {
		return errors.New("mutex (Mu) must not be nil")
	}
	if deps.IsShutdown == nil {
		return errors.New("shutdown flag (IsShutdown) must not be nil")
	}
	if deps.StateMgr == nil {
		return errors.New("state manager (StateMgr) must not be nil")
	}
	if deps.LogMgr == nil {
		return errors.New("log manager (LogMgr) must not be nil")
	}
	if deps.LeaderInitializer == nil {
		return errors.New("leader initializer (LeaderInitializer) must not be nil")
	}
	if deps.Metrics == nil {
		return errors.New("metrics implementation (Metrics) must not be nil")
	}
	if deps.Logger == nil {
		return errors.New("logger implementation (Logger) must not be nil")
	}
	if deps.Rand == nil {
		return errors.New("random number generator (Rand) must not be nil")
	}
	if deps.Clock == nil {
		return errors.New("clock implementation (Clock) must not be nil")
	}
	return nil
}

// applyDefaults sets reasonable defaults for configuration parameters
func (em *electionManager) applyDefaults() {
	if em.electionTickCount <= 0 {
		em.logger.Warnw("Invalid ElectionTickCount, using enhanced default",
			"provided", em.electionTickCount,
			"default", DefaultElectionTickCount)
		em.electionTickCount = DefaultElectionTickCount
	}

	// Enhanced validation for cluster size
	minElectionTicks := len(em.peers) * 8 // 8 ticks per node minimum
	if em.electionTickCount < minElectionTicks {
		em.logger.Warnw("ElectionTickCount may be too low for cluster size",
			"provided", em.electionTickCount,
			"clusterSize", len(em.peers),
			"recommended", minElectionTicks)
	}

	if em.electionTickCount <= DefaultHeartbeatTickCount {
		em.logger.Warnw("ElectionTickCount should be greater than HeartbeatTickCount",
			"electionTicks", em.electionTickCount,
			"heartbeatTicks", DefaultHeartbeatTickCount)
	}

	if em.randomizationFactor < 1.0 {
		em.logger.Warnw(
			"ElectionRandomizationFactor should be at least 1.0 for good split vote prevention",
			"provided",
			em.randomizationFactor,
			"recommended",
			DefaultElectionRandomizationFactor,
		)
		em.randomizationFactor = DefaultElectionRandomizationFactor
	}
}

// Enhanced randomization algorithm with exponential distribution
func (em *electionManager) resetElectionTimeoutPeriod() {
	em.timerMu.Lock()
	defer em.timerMu.Unlock()

	min := float64(em.electionTickCount)
	max := min * (1.0 + em.randomizationFactor*2.0)

	// Use exponential randomization for better distribution
	randomFactor := em.rand.Float64()
	exponentialFactor := math.Pow(randomFactor, 2.0)
	randomized := min + exponentialFactor*(max-min)

	// Add cluster-size-based jitter
	clusterSizeJitter := float64(len(em.peers)) * em.rand.Float64() * 2.0
	randomized += clusterSizeJitter

	// Add node-specific deterministic component
	var nodeComponentValue float64
	if len(em.peers) > 0 { // Ensure there are peers before performing modulo operation
		nodeComponentValue = float64((em.nodeBasedSeed%int64(len(em.peers)))+1) * 2.0
	} else {
		// If there are no peers (empty map), this component should not involve division by zero.
		// Setting it to 0.0 is a sensible default for a cluster without other peers.
		nodeComponentValue = 0.0
	}
	randomized += nodeComponentValue

	em.randomizedPeriod = int(randomized)
	em.electionElapsed = 0

	em.logger.Debugw("Election timeout period reset with enhanced randomization",
		"nodeID", em.id,
		"randomizedPeriod", em.randomizedPeriod,
		"elapsed", em.electionElapsed,
		"baseMin", min,
		"baseMax", max,
		"clusterSizeJitter", clusterSizeJitter,
		"nodeComponent", nodeComponentValue)
}

// ResetTimerOnHeartbeat resets the election timeout when a heartbeat is received.
func (em *electionManager) ResetTimerOnHeartbeat() {
	if em.isShutdown.Load() {
		return
	}

	em.resetElectionTimeoutPeriod()

	em.logger.Debugw("Election timeout period reset on heartbeat",
		"nodeID", em.id,
		"randomizedPeriod", em.randomizedPeriod,
		"elapsed", em.electionElapsed)
}

// Tick advances the election timer. If it expires, a new election may start.
func (em *electionManager) Tick(ctx context.Context) {
	if em.isShutdown.Load() {
		return
	}

	select {
	case <-em.stopCh:
		return
	case <-ctx.Done():
		return
	default:
	}

	if em.concurrentOps.Add(1) > maxConcurrentElections {
		em.concurrentOps.Add(-1)
		em.logger.Debugw("Skipping tick: too many concurrent operations", "nodeID", em.id)
		return
	}
	defer em.concurrentOps.Add(-1)

	shouldTriggerElection := func() bool {
		em.mu.RLock()
		_, role, _ := em.stateMgr.GetState()
		em.mu.RUnlock()

		if role != types.RoleFollower {
			return false
		}

		em.timerMu.Lock()
		defer em.timerMu.Unlock()

		em.electionElapsed++
		return em.electionElapsed >= em.randomizedPeriod
	}()

	if shouldTriggerElection {
		em.handleElectionTimeout(ctx)
	}
}

// Enhanced election timeout handling with split vote detection and recovery
func (em *electionManager) defaultHandleElectionTimeout(ctx context.Context) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Election timeout ignored: node shutting down", "nodeID", em.id)
		return
	}

	if !em.electionInFlight.CompareAndSwap(false, true) {
		em.logger.Debugw("Election timeout ignored: election already in progress", "nodeID", em.id)
		return
	}
	defer em.electionInFlight.Store(false)

	now := em.clock.Now().UnixMilli()
	lastElection := em.lastElectionTime.Load()
	electionCount := em.electionCount.Load()

	// Be more aggressive with timing when pre-vote is enabled
	minIntervalBase := int64(minElectionIntervalBase.Milliseconds())

	minInterval := minIntervalBase + int64(electionCount*25)
	maxInterval := int64(maxElectionBackoff.Milliseconds())
	if minInterval > maxInterval {
		minInterval = maxInterval
	}

	if now-lastElection < minInterval {
		em.logger.Debugw("Election timeout ignored: too soon after last election",
			"nodeID", em.id,
			"timeSince", now-lastElection,
			"minInterval", minInterval,
			"electionCount", electionCount,
			"unit", "ms")
		em.resetElectionTimeoutPeriod()
		return
	}

	currentTerm, role, _ := em.stateMgr.GetState()
	em.logger.Debugw("Handling election timeout",
		"nodeID", em.id,
		"currentTerm", currentTerm,
		"role", role.String(),
		"electionCount", electionCount)

	if role != types.RoleFollower {
		em.logger.Debugw("Not starting election: not a follower",
			"nodeID", em.id, "role", role.String())
		em.resetElectionTimeoutPeriod()
		return
	}

	currentState := ElectionState(em.electionState.Load())
	if currentState != ElectionStateIdle {
		em.logger.Debugw("Election timeout ignored: election already in progress",
			"nodeID", em.id, "currentState", currentState)
		em.resetElectionTimeoutPeriod()
		return
	}

	em.lastElectionTime.Store(now)
	em.electionCount.Add(1)

	em.trackElectionAttempt(currentTerm)

	em.electionState.Store(int32(ElectionStateVoting))
	em.startElection(ctx)
}

func (em *electionManager) trackElectionAttempt(currentTerm types.Term) {
	em.splitVoteDetector.mu.Lock()
	defer em.splitVoteDetector.mu.Unlock()

	em.splitVoteDetector.termHistory = append(em.splitVoteDetector.termHistory, currentTerm)
	if len(em.splitVoteDetector.termHistory) > em.splitVoteDetector.maxHistorySize {
		em.splitVoteDetector.termHistory = em.splitVoteDetector.termHistory[1:]
	}

	em.splitVoteDetector.electionAttempts++

	if em.splitVoteDetector.electionAttempts > 10 {
		em.splitVoteDetector.electionAttempts = 5 // Partial reset
	}
}

func (em *electionManager) resetElectionState(reason string) {
	em.logger.Infow("Resetting election state",
		"nodeID", em.id,
		"reason", reason)

	em.electionState.Store(int32(ElectionStateIdle))
	em.electionInFlight.Store(false)

	em.resetVoteTracking(0)

	em.resetElectionTimeoutPeriod()
}

func (em *electionManager) processVoteReply(
	fromPeerID types.NodeID,
	voteTerm types.Term,
	originatorTerm types.Term,
	reply *types.RequestVoteReply,
) {
	if em.isShutdown.Load() {
		return
	}

	currentState := ElectionState(em.electionState.Load())
	if currentState != ElectionStateVoting {
		em.logger.Debugw("Ignoring vote reply: no longer in voting state",
			"from", fromPeerID, "nodeID", em.id, "currentState", currentState)
		return
	}

	if reply.Term > originatorTerm {
		em.logger.Infow("Received higher term in vote reply, stepping down",
			"from", fromPeerID,
			"replyTerm", reply.Term,
			"currentTerm", originatorTerm,
			"nodeID", em.id)

		ctx, cancel := context.WithTimeout(context.Background(), stateTransitionTimeout)
		defer cancel()
		em.stateMgr.CheckTermAndStepDown(ctx, reply.Term, fromPeerID)
		em.resetElectionState("higher term in vote reply")
		em.recordElectionFailure()
		return
	}

	em.logger.Debugw("Received vote reply",
		"from", fromPeerID,
		"granted", reply.VoteGranted,
		"term", reply.Term,
		"nodeID", em.id)

	if !reply.VoteGranted {
		em.splitVoteDetector.mu.Lock()
		em.splitVoteDetector.electionAttempts++
		em.splitVoteDetector.mu.Unlock()

		em.logger.Debugw("Vote rejected, incrementing election attempts",
			"from", fromPeerID,
			"nodeID", em.id,
			"totalAttempts", em.splitVoteDetector.electionAttempts)
	}

	em.recordVote(fromPeerID, reply.VoteGranted)

	if em.hasQuorum() {
		em.logger.Infow("Vote quorum achieved, becoming leader",
			"nodeID", em.id,
			"term", voteTerm,
			"voteCount", em.voteCount,
			"quorumSize", em.quorumSize)

		ctx, cancel := context.WithTimeout(context.Background(), stateTransitionTimeout)
		defer cancel()
		em.becomeLeader(ctx, voteTerm)
	}
}

// Enhanced real election with improved error handling
func (em *electionManager) defaultStartElection(ctx context.Context) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Election aborted: node shutting down", "nodeID", em.id)
		return
	}

	candidateCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	em.logger.Infow("Starting election - DETAILED",
		"nodeID", em.id)

	if !em.stateMgr.BecomeCandidate(candidateCtx, ElectionReasonTimeout) {
		em.logger.Warnw("Failed to become candidate - ELECTION ABORTED",
			"nodeID", em.id)
		em.resetElectionState("failed to become candidate")
		em.recordElectionFailure()
		return
	}

	newTerm, role, _ := em.stateMgr.GetState()
	if role != types.RoleCandidate {
		em.logger.Warnw("Failed to transition to candidate role - ELECTION ABORTED",
			"nodeID", em.id,
			"actualRole", role.String(),
			"expectedRole", "Candidate")
		em.resetElectionState("failed to transition to candidate")
		em.recordElectionFailure()
		return
	}

	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()

	em.logger.Infow("Election started successfully - DETAILED",
		"nodeID", em.id,
		"term", newTerm,
		"lastLogIndex", lastIndex,
		"lastLogTerm", lastTerm,
		"quorumSize", em.quorumSize,
		"peerCount", len(em.peers))

	em.resetVoteTracking(newTerm)

	em.logger.Infow("Recording self-vote",
		"nodeID", em.id,
		"term", newTerm)
	em.recordVote(em.id, true)

	args := &types.RequestVoteArgs{
		Term:         newTerm,
		CandidateID:  em.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		IsPreVote:    false,
	}

	em.logger.Infow("Broadcasting vote requests to all peers",
		"nodeID", em.id,
		"term", newTerm,
		"peerCount", len(em.peers)-1)

	em.broadcastVoteRequests(ctx, args)

	alreadyHasQuorum := em.hasQuorum()
	if alreadyHasQuorum {
		em.logger.Infow("Already have quorum after self-vote, becoming leader immediately",
			"nodeID", em.id,
			"term", newTerm)

		if em.electionInFlight.CompareAndSwap(false, true) {
			go func() {
				defer em.electionInFlight.Store(false)
				em.becomeLeader(ctx, newTerm)
			}()
		}
	} else {
		em.logger.Infow("Waiting for vote replies to achieve quorum",
			"nodeID", em.id,
			"term", newTerm,
			"currentVotes", em.voteCount,
			"neededVotes", em.quorumSize)
	}
}

// Record election failure for split vote detection
func (em *electionManager) recordElectionFailure() {
	em.splitVoteDetector.mu.Lock()
	defer em.splitVoteDetector.mu.Unlock()

	// TEMPORARY DEBUG LOGS:
	em.logger.Debugw(
		"recordElectionFailure: (Inside lock) BEFORE increment",
		"attempts",
		em.splitVoteDetector.electionAttempts,
	)

	em.splitVoteDetector.consecutiveFails++
	em.splitVoteDetector.lastFailTime = em.clock.Now()
	em.splitVoteDetector.electionAttempts++ // This is the critical line that increments

	// TEMPORARY DEBUG LOGS:
	em.logger.Debugw(
		"recordElectionFailure: (Inside lock) AFTER increment",
		"attempts",
		em.splitVoteDetector.electionAttempts,
	)

	if em.splitVoteDetector.electionAttempts > 10 {
		// TEMPORARY DEBUG LOGS:
		em.logger.Debugw(
			"recordElectionFailure: (Inside lock) Attempts > 10, resetting...",
			"attempts_before_reset",
			em.splitVoteDetector.electionAttempts,
		)
		em.splitVoteDetector.electionAttempts = 5
		// TEMPORARY DEBUG LOGS:
		em.logger.Debugw(
			"recordElectionFailure: (Inside lock) Attempts AFTER reset",
			"attempts_after_reset",
			em.splitVoteDetector.electionAttempts,
		)
	}

	em.logger.Infow("Election failure recorded",
		"nodeID", em.id,
		"consecutiveFails", em.splitVoteDetector.consecutiveFails,
		"finalAttempts_in_func", em.splitVoteDetector.electionAttempts)
}

// becomeLeader transitions to leader role with enhanced logging
func (em *electionManager) becomeLeader(ctx context.Context, term types.Term) {
	currentTerm, role, _ := em.stateMgr.GetState() // Get current state early

	if em.isShutdown.Load() {
		em.logger.Debugw("Cannot become leader: node shutting down", "nodeID", em.id)
		// If shutting down, the node should step down to follower
		em.stateMgr.BecomeFollower(
			context.Background(),
			currentTerm,
			unknownNodeID,
		) // Use currentTerm
		em.resetElectionState("shutting down")
		return
	}

	em.logger.Infow("ATTEMPTING TO BECOME LEADER",
		"nodeID", em.id,
		"expectedTerm", term,
		"currentTerm", currentTerm,
		"currentRole", role.String())

	if term != currentTerm {
		em.logger.Warnw("Cannot become leader: term changed during election",
			"expectedTerm", term,
			"currentTerm", currentTerm,
			"nodeID", em.id)
		// Step down to Follower using the current (authoritative) term
		em.stateMgr.BecomeFollower(context.Background(), currentTerm, unknownNodeID)
		em.resetElectionState("term changed")
		return
	}

	if role != types.RoleCandidate {
		em.logger.Warnw("Cannot become leader: no longer a candidate",
			"nodeID", em.id,
			"role", role.String(),
			"term", currentTerm)
		// Step down to Follower using the current (authoritative) term
		em.stateMgr.BecomeFollower(context.Background(), currentTerm, unknownNodeID)
		em.resetElectionState("not a candidate")
		return
	}

	leaderCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	if !em.stateMgr.BecomeLeader(leaderCtx) {
		em.logger.Errorw("FAILED TO TRANSITION TO LEADER STATE",
			"nodeID", em.id,
			"term", currentTerm)
		// Step down to Follower using the current (authoritative) term
		em.stateMgr.BecomeFollower(context.Background(), currentTerm, unknownNodeID)
		em.resetElectionState("failed to become leader")
		em.recordElectionFailure()
		return
	}

	em.logger.Infow("✅ SUCCESSFULLY BECAME LEADER ✅",
		"nodeID", em.id,
		"term", currentTerm,
		"electionCount", em.electionCount.Load())

	em.splitVoteDetector.mu.Lock()
	em.splitVoteDetector.consecutiveFails = 0
	em.splitVoteDetector.lastFailTime = time.Time{}
	em.splitVoteDetector.electionAttempts = 0
	em.splitVoteDetector.mu.Unlock()

	em.leaderInitializer.InitializeLeaderState()

	go func() {
		heartbeatCtx, cancel := context.WithTimeout(context.Background(), electionManagerOpTimeout)
		defer cancel()
		em.leaderInitializer.SendHeartbeats(heartbeatCtx)
	}()

	em.resetElectionState("became leader")

	em.logger.Infow("Leader transition completed successfully",
		"nodeID", em.id,
		"term", currentTerm)
}

// resetVoteTracking clears real vote tracking state for a new term
func (em *electionManager) resetVoteTracking(term types.Term) {
	em.voteMu.Lock()
	defer em.voteMu.Unlock()

	for k := range em.votesReceived {
		delete(em.votesReceived, k)
	}
	em.voteCount = 0
	em.voteTerm = term

	em.logger.Debugw("Real vote tracking reset",
		"nodeID", em.id,
		"term", term,
		"quorumSize", em.quorumSize)
}

// recordVote records a real vote response
func (em *electionManager) recordVote(from types.NodeID, granted bool) {
	if !granted {
		em.logger.Debugw("Vote not granted, not recording",
			"from", from,
			"nodeID", em.id,
			"term", em.voteTerm)
		return
	}

	em.voteMu.Lock()
	defer em.voteMu.Unlock() // Corrected from em.mu.Unlock()

	if em.votesReceived[from] {
		em.logger.Debugw("Vote already recorded from this peer",
			"from", from,
			"nodeID", em.id,
			"term", em.voteTerm)
		return
	}

	em.logger.Debugw(
		"recordVote: BEFORE increment",
		"from",
		from,
		"nodeID",
		em.id,
		"voteCount_before",
		em.voteCount,
		"term",
		em.voteTerm,
	)
	em.votesReceived[from] = true
	em.voteCount++
	em.logger.Debugw(
		"recordVote: AFTER increment",
		"from",
		from,
		"nodeID",
		em.id,
		"voteCount_after",
		em.voteCount,
		"term",
		em.voteTerm,
	)

	em.logger.Infow("Real vote recorded - DETAILED",
		"from", from,
		"nodeID", em.id,
		"voteCount", em.voteCount,
		"quorumSize", em.quorumSize,
		"hasQuorum", em.voteCount >= em.quorumSize,
		"term", em.voteTerm,
		"allVoters", em.getVotersList())
}

// hasQuorum checks if we have enough votes to become leader with enhanced logging
func (em *electionManager) hasQuorum() bool {
	em.voteMu.RLock()
	defer em.voteMu.RUnlock()

	hasIt := em.voteCount >= em.quorumSize

	em.logger.Infow("Quorum check - DETAILED",
		"nodeID", em.id,
		"voteCount", em.voteCount,
		"quorumSize", em.quorumSize,
		"hasQuorum", hasIt,
		"term", em.voteTerm,
		"allVoters", em.getVotersListUnsafe())

	return hasIt
}

// Helper to get list of all voters for debugging
func (em *electionManager) getVotersList() []types.NodeID {
	var voters []types.NodeID
	for voterID := range em.votesReceived {
		voters = append(voters, voterID)
	}
	return voters
}

// Helper for unsafe voter list access
func (em *electionManager) getVotersListUnsafe() []types.NodeID {
	var voters []types.NodeID
	for voterID := range em.votesReceived {
		voters = append(voters, voterID)
	}
	return voters
}

// broadcastVoteRequests sends vote requests to all peers with enhanced concurrency control
func (em *electionManager) broadcastVoteRequests(
	ctx context.Context,
	args *types.RequestVoteArgs,
) {
	requestType := "vote"

	em.logger.Debugw("Broadcasting vote requests",
		"type", requestType,
		"term", args.Term,
		"nodeID", em.id,
		"peerCount", len(em.peers)-1)

	const maxWorkers = 10
	semaphore := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup

	for peerID := range em.peers {
		if peerID == em.id {
			continue // Skip self
		}

		wg.Add(1)
		go func(targetPeerID types.NodeID) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				return
			}

			rpcCtx, cancel := context.WithTimeout(context.Background(), voteRequestTimeout)
			defer cancel()

			em.sendVoteRequest(rpcCtx, targetPeerID, args, args.Term)
		}(peerID)
	}

	go func() {
		wg.Wait()
		em.logger.Debugw("Finished broadcasting vote requests",
			"type", requestType,
			"term", args.Term,
			"nodeID", em.id)
	}()
}

// sendVoteRequest sends a regular vote request with improved error handling
func (em *electionManager) sendVoteRequest(
	ctx context.Context,
	peer types.NodeID,
	args *types.RequestVoteArgs,
	currentTerm types.Term,
) {
	requestCtx, cancel := context.WithTimeout(ctx, voteRequestTimeout)
	defer cancel()

	em.logger.Debugw("Sending vote request",
		"to", peer,
		"term", args.Term,
		"nodeID", em.id)

	reply, err := em.networkMgr.SendRequestVote(requestCtx, peer, args)
	if err != nil {
		em.logger.Warnw("Vote request failed - NETWORK ERROR",
			"nodeID", em.id,
			"to", peer,
			"term", args.Term,
			"error", err)

		em.splitVoteDetector.mu.Lock()
		em.splitVoteDetector.electionAttempts++
		em.splitVoteDetector.mu.Unlock()

		return
	}

	em.logger.Debugw("Vote request succeeded",
		"to", peer,
		"term", args.Term,
		"granted", reply.VoteGranted,
		"replyTerm", reply.Term,
		"nodeID", em.id)

	em.processVoteReply(peer, args.Term, currentTerm, reply)
}

// HandleRequestVote processes an incoming vote request.
func (em *electionManager) HandleRequestVote(
	ctx context.Context,
	args *types.RequestVoteArgs,
) (*types.RequestVoteReply, error) {
	if err := ctx.Err(); err != nil {
		em.logger.Debugw("Rejecting vote request: context cancelled",
			"from", args.CandidateID,
			"error", err,
			"nodeID", em.id)
		term, _, _ := em.stateMgr.GetState()
		return &types.RequestVoteReply{Term: term, VoteGranted: false}, err
	}

	if em.isShutdown.Load() {
		term, _, _ := em.stateMgr.GetState()
		em.logger.Debugw("Rejecting vote request: node shutting down",
			"from", args.CandidateID,
			"nodeID", em.id)
		return &types.RequestVoteReply{Term: term, VoteGranted: false}, ErrShuttingDown
	}

	reply := &types.RequestVoteReply{VoteGranted: false}
	requestType := "vote"

	em.logger.Debugw("Processing vote request",
		"type", requestType,
		"from", args.CandidateID,
		"term", args.Term,
		"nodeID", em.id)

	steppedDown, termBeforeCheck := em.stateMgr.CheckTermAndStepDown(
		ctx,
		args.Term,
		args.CandidateID,
	)
	if steppedDown {
		em.logger.Infow("Stepped down due to higher term in vote request",
			"from", args.CandidateID,
			"newTerm", args.Term,
			"previousTerm", termBeforeCheck,
			"nodeID", em.id)
		// reset the election state when stepping down
		em.resetElectionState("higher term in vote request")
	}

	currentTerm, currentRole, _ := em.stateMgr.GetState()
	votedFor := em.stateMgr.GetVotedFor()
	reply.Term = currentTerm

	if args.Term < currentTerm {
		em.logger.Debugw("Rejecting vote request: outdated term",
			"from", args.CandidateID,
			"requestTerm", args.Term,
			"currentTerm", currentTerm,
			"nodeID", em.id)
		return reply, nil
	}

	if currentRole == types.RoleCandidate && args.Term > currentTerm {
		em.logger.Infow("Candidate stepping down for higher term vote request",
			"from", args.CandidateID,
			"requestTerm", args.Term,
			"currentTerm", currentTerm,
			"nodeID", em.id)
		currentTerm, _, _ = em.stateMgr.GetState()
		votedFor = em.stateMgr.GetVotedFor()
		reply.Term = currentTerm
	}

	if votedFor != "" && votedFor != args.CandidateID {
		em.logger.Debugw("Rejecting vote request: already voted",
			"from", args.CandidateID,
			"term", args.Term,
			"votedFor", votedFor,
			"nodeID", em.id)
		return reply, nil
	}

	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()
	logUpToDate := em.isLogUpToDate(args.LastLogTerm, args.LastLogIndex, lastTerm, lastIndex)

	if !logUpToDate {
		em.logger.Debugw("Rejecting vote request: log not up-to-date",
			"from", args.CandidateID,
			"candidateLastTerm", args.LastLogTerm,
			"candidateLastIndex", args.LastLogIndex,
			"ourLastTerm", lastTerm,
			"ourLastIndex", lastIndex,
			"nodeID", em.id)
		return reply, nil
	}

	em.logger.Infow("Granting vote",
		"to", args.CandidateID,
		"term", args.Term,
		"nodeID", em.id)

	if !em.stateMgr.GrantVote(ctx, args.CandidateID, args.Term) {
		em.logger.Debugw("Vote not granted based on Raft rules",
			"to", args.CandidateID,
			"term", args.Term,
			"nodeID", em.id,
			"reason", "state manager denied vote")
		return reply, nil
	}

	reply.VoteGranted = true
	em.ResetTimerOnHeartbeat()

	return reply, nil
}

// isLogUpToDate checks if the candidate's log is at least as up-to-date as ours
func (em *electionManager) isLogUpToDate(
	candidateLastTerm types.Term,
	candidateLastIndex types.Index,
	ourLastTerm types.Term,
	ourLastIndex types.Index,
) bool {
	// Log is up-to-date if:
	// 1. Candidate's last term is higher, OR
	// 2. Terms are equal but candidate's index is >= ours
	return candidateLastTerm > ourLastTerm ||
		(candidateLastTerm == ourLastTerm && candidateLastIndex >= ourLastIndex)
}

// Stop stops the election manager and waits for any ongoing operations to finish.
func (em *electionManager) Stop() {
	em.stopOnce.Do(func() {
		em.logger.Infow("Stopping election manager", "nodeID", em.id)

		// Signal all background operations to stop.
		close(em.stopCh)

		// Use a real-time timer as a safeguard against deadlock during shutdown.
		// This is crucial because the main Raft logic might be paused, and we don't
		// want to rely on the mock clock in tests for shutdown.
		timeout := time.NewTimer(2 * time.Second)
		defer timeout.Stop()

		// Periodically check if all operations have completed.
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timeout.C:
				em.logger.Warnw("Timeout waiting for election operations to complete",
					"nodeID", em.id,
					"remainingOps", em.concurrentOps.Load())
				return
			case <-ticker.C:
				// If there are no more concurrent operations, shutdown is clean.
				if em.concurrentOps.Load() == 0 {
					em.logger.Infow("Election manager stopped cleanly", "nodeID", em.id)
					return
				}
			}
		}
	})
}
