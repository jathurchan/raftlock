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
	ElectionStatePreVoting
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
	SendHeartbeats(ctx context.Context)
}

// ElectionManager defines the interface for managing leader elections
// in the Raft consensus algorithm. It handles election timeouts,
// vote requests and responses, and transitions to the leader role.
type ElectionManager interface {
	// SetNetworkManager allows late injection of the network manager after the Raft node is built.
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
	enablePreVote       bool    // Whether to perform a pre-vote phase before a real election

	electionElapsed  int // Ticks elapsed since last heartbeat
	randomizedPeriod int // Randomized timeout threshold for current cycle

	preVoteMu        sync.RWMutex          // Protects pre-vote tracking
	preVotesReceived map[types.NodeID]bool // Tracks pre-vote responses
	preVoteCount     int                   // Pre-vote count
	preVoteTerm      types.Term            // Term for pre-vote tracking

	voteMu        sync.RWMutex          // To isolate vote tracking from mu and reduce contention
	votesReceived map[types.NodeID]bool // Tracks which peers have granted votes
	voteCount     int                   // Number of votes granted in the current term
	voteTerm      types.Term            // Term in which votes are currently being collected

	electionState    atomic.Int32  // Current election phase (idle, pre-voting, voting)
	electionInFlight atomic.Bool   // Ensures only one election runs at a time
	lastElectionTime atomic.Int64  // Unix timestamp of last election attempt
	electionCount    atomic.Uint64 // Total number of elections triggered
	concurrentOps    atomic.Int32  // Number of active operations (e.g., ticks, RPCs)

	startPreVote          func(ctx context.Context) // Begins the pre-vote phase
	handleElectionTimeout func(ctx context.Context) // Handles election timeout logic
	startElection         func(ctx context.Context) // Begins a real election

	timerMu sync.Mutex // Serializes access to pre-vote/election delay timers to prevent overlapping triggers

	preVoteSuccessTime atomic.Int64  // Timestamp when pre-vote succeeded
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
	features := deps.Config.FeatureFlags

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
		enablePreVote:       features.PreVoteEnabled,

		electionStartDelay: time.Duration(nodeBasedSeed%100)*time.Millisecond +
			time.Duration(deps.Rand.IntN(100))*time.Millisecond,
		nodeBasedSeed: nodeBasedSeed,

		preVotesReceived: make(map[types.NodeID]bool),
		votesReceived:    make(map[types.NodeID]bool),

		stopCh: make(chan struct{}),

		splitVoteDetector: &SplitVoteDetector{
			maxHistorySize: 10,
			termHistory:    make([]types.Term, 0, 10),
		},
	}

	em.electionState.Store(int32(ElectionStateIdle))

	em.startPreVote = em.defaultStartPreVote
	em.handleElectionTimeout = em.defaultHandleElectionTimeout
	em.startElection = em.defaultStartElection

	em.applyDefaults()
	em.resetElectionTimeoutPeriod()

	log.Infow("Election manager initialized with enhanced split vote prevention",
		"nodeID", em.id,
		"preVoteEnabled", em.enablePreVote,
		"electionTickCount", em.electionTickCount,
		"randomizationFactor", em.randomizationFactor,
		"quorumSize", em.quorumSize,
		"peerCount", len(em.peers)-1,
		"nodeBasedSeed", em.nodeBasedSeed)

	return em, nil
}

// SetNetworkManager allows late injection of the network manager
func (em *electionManager) SetNetworkManager(nm NetworkManager) {
	em.networkMgr = nm
	em.logger.Debugw("Network manager set", "nodeID", em.id)
}

// Initialize prepares the election manager for operation
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
		em.logger.Warnw("ElectionRandomizationFactor should be at least 1.0 for good split vote prevention",
			"provided", em.randomizationFactor,
			"recommended", DefaultElectionRandomizationFactor)
		em.randomizationFactor = DefaultElectionRandomizationFactor
	}
}

// Enhanced randomization algorithm with exponential distribution
func (em *electionManager) resetElectionTimeoutPeriod() {
	em.timerMu.Lock()
	defer em.timerMu.Unlock()

	min := float64(em.electionTickCount)
	// Increase the randomization factor to make split votes less likely.
	// A larger range of possible timeout values reduces the chance of simultaneous timeouts.
	max := min * (1.0 + em.randomizationFactor*2.0) // Increased randomization

	// Use exponential randomization for better distribution
	randomFactor := em.rand.Float64()
	exponentialFactor := math.Pow(randomFactor, 2.0) // Square for exponential distribution
	randomized := min + exponentialFactor*(max-min)

	// Add cluster-size-based jitter
	clusterSizeJitter := float64(len(em.peers)) * em.rand.Float64() * 2.0
	randomized += clusterSizeJitter

	// Add node-specific deterministic component
	nodeComponent := float64((em.nodeBasedSeed%int64(len(em.peers)))+1) * 2.0
	randomized += nodeComponent

	em.randomizedPeriod = int(randomized)
	em.electionElapsed = 0

	em.logger.Debugw("Election timeout period reset with enhanced randomization",
		"nodeID", em.id,
		"randomizedPeriod", em.randomizedPeriod,
		"elapsed", em.electionElapsed,
		"baseMin", min,
		"baseMax", max,
		"clusterSizeJitter", clusterSizeJitter,
		"nodeComponent", nodeComponent)
}

// ResetTimerOnHeartbeat resets the election timer when a heartbeat is received
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

// Tick advances the election timer and triggers elections when necessary
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

	now := em.clock.Now().UnixMilli() // Using the clock interface
	lastElection := em.lastElectionTime.Load()
	electionCount := em.electionCount.Load()

	minInterval := int64(minElectionIntervalBase.Milliseconds()) + int64(electionCount*25) // Reduced from 50
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
		return
	}

	if em.detectSplitVote() {
		em.handleSplitVoteRecovery(ctx)
		em.resetElectionTimeoutPeriod()
		return
	}

	em.lastElectionTime.Store(now)
	em.electionCount.Add(1)

	em.trackElectionAttempt(currentTerm)

	if em.enablePreVote {
		em.electionState.Store(int32(ElectionStatePreVoting))
		em.startPreVote(ctx)
	} else {
		em.electionState.Store(int32(ElectionStateVoting))
		em.startElection(ctx)
	}
}

func (em *electionManager) detectSplitVote() bool {
	em.splitVoteDetector.mu.RLock()
	defer em.splitVoteDetector.mu.RUnlock()

	currentTerm, _, _ := em.stateMgr.GetState()
	electionCount := em.electionCount.Load()

	if em.splitVoteDetector.consecutiveFails >= splitVoteDetectionThreshold {
		timeSinceLastFail := em.clock.Since(em.splitVoteDetector.lastFailTime) // Using the clock interface
		if timeSinceLastFail < 30*time.Second {
			em.logger.Infow("Split vote detected: consecutive failures",
				"nodeID", em.id,
				"consecutiveFails", em.splitVoteDetector.consecutiveFails,
				"timeSinceLastFail", timeSinceLastFail)
			return true
		}
	}

	if len(em.splitVoteDetector.termHistory) >= 2 { // Reduced from 3 to 2
		recentTerms := em.splitVoteDetector.termHistory[len(em.splitVoteDetector.termHistory)-2:]
		if recentTerms[1]-recentTerms[0] >= 1 && currentTerm > 0 { // Any term progression in current election
			em.logger.Infow("Split vote detected: rapid term progression",
				"nodeID", em.id,
				"termProgression", recentTerms[1]-recentTerms[0],
				"currentTerm", currentTerm)
			return true
		}
	}

	if em.splitVoteDetector.electionAttempts >= 3 { // Reduced from 5 to 3
		em.logger.Infow("Split vote detected: too many election attempts",
			"nodeID", em.id,
			"electionAttempts", em.splitVoteDetector.electionAttempts)
		return true
	}

	if electionCount >= 2 {
		currentState := ElectionState(em.electionState.Load())
		if currentState == ElectionStateVoting {
			em.logger.Infow("Split vote detected: stuck in voting state",
				"nodeID", em.id,
				"electionCount", electionCount,
				"currentState", currentState)
			return true
		}
	}

	return false
}

func (em *electionManager) handleSplitVoteRecovery(ctx context.Context) {
	em.logger.Infow("Split vote detected, applying recovery mechanism", "nodeID", em.id)

	currentTerm, _, _ := em.stateMgr.GetState()
	electionCount := em.electionCount.Load()

	nodeSpecificDelay := time.Duration((em.nodeBasedSeed%500)+250) * time.Millisecond // Reduced range
	termBasedDelay := time.Duration(currentTerm*50) * time.Millisecond                // Reduced multiplier
	randomDelay := time.Duration(em.rand.IntN(1000)+500) * time.Millisecond           // Reduced range
	clusterDelay := time.Duration(len(em.peers)*50) * time.Millisecond                // Reduced multiplier

	backoffMultiplier := math.Min(float64(electionCount), 4.0) // Cap at 4x
	exponentialDelay := time.Duration(float64(splitVoteRecoveryDelay) * backoffMultiplier)

	totalRecoveryDelay := nodeSpecificDelay + termBasedDelay + randomDelay + clusterDelay + exponentialDelay

	// Lower cap for faster recovery
	if totalRecoveryDelay > maxSplitVoteBackoff {
		totalRecoveryDelay = maxSplitVoteBackoff
	}

	em.logger.Infow("Applying split vote recovery delay",
		"nodeID", em.id,
		"nodeSpecificDelay", nodeSpecificDelay,
		"termBasedDelay", termBasedDelay,
		"randomDelay", randomDelay,
		"clusterDelay", clusterDelay,
		"exponentialDelay", exponentialDelay,
		"totalDelay", totalRecoveryDelay,
		"electionCount", electionCount)

	timer := em.clock.NewTimer(totalRecoveryDelay) // Using the clock interface
	defer timer.Stop()

	select {
	case <-timer.Chan():
	case <-ctx.Done():
		em.logger.Debugw("Split vote recovery cancelled due to context", "nodeID", em.id)
		return
	case <-em.stopCh:
		em.logger.Debugw("Split vote recovery cancelled due to shutdown", "nodeID", em.id)
		return
	}

	em.splitVoteDetector.mu.Lock()
	em.splitVoteDetector.consecutiveFails = 0
	em.splitVoteDetector.lastFailTime = time.Time{}
	em.splitVoteDetector.electionAttempts = 0
	em.splitVoteDetector.mu.Unlock()

	em.logger.Infow("Split vote recovery completed", "nodeID", em.id)
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
	em.resetPreVoteTracking(0)

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

// Enhanced pre-vote phase with improved error handling
func (em *electionManager) defaultStartPreVote(ctx context.Context) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Pre-vote aborted: node shutting down", "nodeID", em.id)
		return
	}

	currentTerm, role, _ := em.stateMgr.GetState()

	if role != types.RoleFollower {
		em.logger.Debugw("Pre-vote aborted: no longer a follower",
			"nodeID", em.id, "role", role.String())
		em.resetElectionState("not a follower")
		return
	}

	preVoteTerm := currentTerm + 1
	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()

	em.logger.Infow("Starting pre-vote phase",
		"nodeID", em.id,
		"currentTerm", currentTerm,
		"preVoteTerm", preVoteTerm,
		"lastLogIndex", lastIndex,
		"lastLogTerm", lastTerm)

	em.resetPreVoteTracking(preVoteTerm)

	args := &types.RequestVoteArgs{
		Term:         preVoteTerm,
		CandidateID:  em.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		IsPreVote:    true,
	}

	em.broadcastVoteRequests(ctx, args, true, currentTerm)
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

	em.broadcastVoteRequests(ctx, args, false, newTerm)

	if em.hasQuorum() {
		em.logger.Infow("Already have quorum after self-vote, becoming leader immediately",
			"nodeID", em.id,
			"term", newTerm)

		em.electionInFlight.Store(true)
		go func() {
			defer em.electionInFlight.Store(false)
			em.becomeLeader(ctx, newTerm)
		}()
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

	em.splitVoteDetector.consecutiveFails++
	em.splitVoteDetector.lastFailTime = em.clock.Now() // Using the clock interface

	em.logger.Infow("Election failure recorded", // Changed from Debugw to Infow for visibility
		"nodeID", em.id,
		"consecutiveFails", em.splitVoteDetector.consecutiveFails)
}

// Enhanced election delay with adaptive backoff
func (em *electionManager) startElectionWithDelay() {
	totalDelay := em.calculateAdaptiveDelay()

	// Cap maximum delay
	if totalDelay > 5*time.Second {
		totalDelay = 5 * time.Second
	}

	timer := em.clock.NewTimer(totalDelay)
	defer timer.Stop()

	select {
	case <-timer.Chan(): // This channel will now correctly receive a value when the mock clock advances.
	case <-em.stopCh:
		em.logger.Debugw("Election start cancelled: node shutting down", "nodeID", em.id)
		return
	}

	if em.isShutdown.Load() {
		return
	}

	currentState := ElectionState(em.electionState.Load())
	if currentState != ElectionStatePreVoting {
		em.logger.Debugw("Cancelling delayed election: no longer in pre-voting state",
			"nodeID", em.id, "currentState", currentState)
		return
	}

	_, role, _ := em.stateMgr.GetState()
	if role != types.RoleFollower {
		em.logger.Debugw("Cancelling delayed election: no longer a follower",
			"nodeID", em.id, "role", role.String())
		em.resetElectionState("not a follower")
		return
	}

	currentLeader, _, hasLeader := em.stateMgr.GetLeaderInfo()
	if hasLeader && currentLeader != "" {
		em.logger.Debugw("Cancelling delayed election: leader detected",
			"nodeID", em.id, "leader", currentLeader)
		em.resetElectionState("leader detected")
		return
	}

	// Proceed with election
	ctx, cancel := context.WithTimeout(context.Background(), electionManagerOpTimeout)
	defer cancel()
	em.startElection(ctx)
}

// Helper to encapsulate delay calculation
func (em *electionManager) calculateAdaptiveDelay() time.Duration {
	electionCount := em.electionCount.Load()
	baseDelay := em.electionStartDelay

	if electionCount > 1 {
		backoffMultiplier := math.Min(float64(electionCount), 8.0)
		baseDelay = time.Duration(float64(baseDelay) * backoffMultiplier)
	}

	additionalJitter := time.Duration(em.rand.IntN(200)+100) * time.Millisecond
	clusterDelay := time.Duration(len(em.peers)*(em.rand.IntN(100)+50)) * time.Millisecond
	nodeDelay := time.Duration((em.nodeBasedSeed%int64(len(em.peers)))*50) * time.Millisecond
	totalDelay := baseDelay + additionalJitter + clusterDelay + nodeDelay

	em.logger.Debugw("Starting election with adaptive delay",
		"nodeID", em.id,
		"electionCount", electionCount,
		"baseDelay", baseDelay,
		"additionalJitter", additionalJitter,
		"clusterDelay", clusterDelay,
		"nodeDelay", nodeDelay,
		"totalDelay", totalDelay)

	if totalDelay > 5*time.Second {
		totalDelay = 5 * time.Second
	}
	return totalDelay
}

// Enhanced pre-vote reply processing with better delay calculation
func (em *electionManager) processPreVoteReply(
	fromPeerID types.NodeID,
	preVoteTerm types.Term,
	originatorTerm types.Term,
	reply *types.RequestVoteReply,
) {
	if em.isShutdown.Load() {
		return
	}

	currentState := ElectionState(em.electionState.Load())
	if currentState != ElectionStatePreVoting {
		em.logger.Debugw("Ignoring pre-vote reply: no longer in pre-voting state",
			"from", fromPeerID, "nodeID", em.id, "currentState", currentState)
		return
	}

	if reply.Term > originatorTerm {
		em.logger.Infow("Received higher term in pre-vote reply, stepping down",
			"from", fromPeerID,
			"replyTerm", reply.Term,
			"currentTerm", originatorTerm,
			"nodeID", em.id)

		ctx, cancel := context.WithTimeout(context.Background(), stateTransitionTimeout)
		defer cancel()
		em.stateMgr.CheckTermAndStepDown(ctx, reply.Term, fromPeerID)
		em.resetElectionState("higher term in pre-vote reply")
		return
	}

	em.logger.Debugw("Received pre-vote reply",
		"from", fromPeerID,
		"granted", reply.VoteGranted,
		"term", reply.Term,
		"nodeID", em.id)

	em.recordPreVote(fromPeerID, reply.VoteGranted)

	if em.hasPreVoteQuorum() {
		baseDelay := em.electionStartDelay
		clusterFactor := time.Duration(len(em.peers)) * time.Duration(em.rand.IntN(100)+50) * time.Millisecond
		electionCount := em.electionCount.Load()
		historyDelay := time.Duration(electionCount*50) * time.Millisecond
		nodeDelay := time.Duration((em.nodeBasedSeed%100)+50) * time.Millisecond
		totalDelay := baseDelay + clusterFactor + historyDelay + nodeDelay

		if totalDelay > 3*time.Second {
			totalDelay = 3 * time.Second
		}

		em.logger.Infow("Pre-vote succeeded, starting real election after enhanced delay",
			"nodeID", em.id,
			"preVoteTerm", preVoteTerm,
			"baseDelay", baseDelay,
			"clusterFactor", clusterFactor,
			"historyDelay", historyDelay,
			"nodeDelay", nodeDelay,
			"totalDelay", totalDelay)

		em.preVoteSuccessTime.Store(em.clock.Now().UnixNano()) // Using the clock interface
		em.electionStartDelay = totalDelay

		go em.startElectionWithDelay()
	}
}

// becomeLeader transitions to leader role with enhanced logging
func (em *electionManager) becomeLeader(ctx context.Context, term types.Term) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Cannot become leader: node shutting down", "nodeID", em.id)
		em.resetElectionState("shutting down")
		return
	}

	currentTerm, role, _ := em.stateMgr.GetState()

	em.logger.Infow("ATTEMPTING TO BECOME LEADER - DETAILED",
		"nodeID", em.id,
		"expectedTerm", term,
		"currentTerm", currentTerm,
		"currentRole", role.String())

	if term != currentTerm {
		em.logger.Warnw("Cannot become leader: term changed during election",
			"expectedTerm", term,
			"currentTerm", currentTerm,
			"nodeID", em.id)
		em.resetElectionState("term changed")
		return
	}

	if role != types.RoleCandidate {
		em.logger.Warnw("Cannot become leader: no longer a candidate",
			"nodeID", em.id,
			"role", role.String(),
			"term", currentTerm)
		em.resetElectionState("not a candidate")
		return
	}

	em.logger.Infow("Calling stateMgr.BecomeLeader()...",
		"nodeID", em.id,
		"term", currentTerm)

	leaderCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	if !em.stateMgr.BecomeLeader(leaderCtx) {
		em.logger.Errorw("FAILED TO TRANSITION TO LEADER STATE",
			"nodeID", em.id,
			"term", currentTerm)
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

	em.logger.Infow("Initializing leader state...",
		"nodeID", em.id,
		"term", currentTerm)

	em.leaderInitializer.InitializeLeaderState()

	em.logger.Infow("Sending initial heartbeats...",
		"nodeID", em.id,
		"term", currentTerm)

	em.leaderInitializer.SendHeartbeats(leaderCtx)

	em.resetElectionState("became leader")

	em.logger.Infow("Leader transition completed successfully",
		"nodeID", em.id,
		"term", currentTerm)
}

// resetPreVoteTracking clears pre-vote tracking state
func (em *electionManager) resetPreVoteTracking(term types.Term) {
	em.preVoteMu.Lock()
	defer em.preVoteMu.Unlock()

	for k := range em.preVotesReceived {
		delete(em.preVotesReceived, k)
	}
	em.preVoteCount = 0
	em.preVoteTerm = term

	em.logger.Debugw("Pre-vote tracking reset",
		"nodeID", em.id,
		"term", term,
		"quorumSize", em.quorumSize)
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

// recordPreVote records a pre-vote response
func (em *electionManager) recordPreVote(from types.NodeID, granted bool) {
	if !granted {
		em.logger.Debugw("Pre-vote not granted, not recording",
			"from", from,
			"nodeID", em.id,
			"term", em.preVoteTerm)
		return
	}

	em.preVoteMu.Lock()
	defer em.preVoteMu.Unlock()

	if em.preVotesReceived[from] {
		em.logger.Debugw("Pre-vote already recorded from this peer",
			"from", from,
			"nodeID", em.id,
			"term", em.preVoteTerm)
		return
	}

	em.preVotesReceived[from] = true
	em.preVoteCount++

	em.logger.Infow("Pre-vote recorded - DETAILED",
		"from", from,
		"nodeID", em.id,
		"preVoteCount", em.preVoteCount,
		"quorumSize", em.quorumSize,
		"hasQuorum", em.preVoteCount >= em.quorumSize,
		"term", em.preVoteTerm,
		"allPreVoters", em.getPreVotersList())
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
	defer em.voteMu.Unlock()

	if em.votesReceived[from] {
		em.logger.Debugw("Vote already recorded from this peer",
			"from", from,
			"nodeID", em.id,
			"term", em.voteTerm)
		return
	}

	em.votesReceived[from] = true
	em.voteCount++

	em.logger.Infow("Real vote recorded - DETAILED",
		"from", from,
		"nodeID", em.id,
		"voteCount", em.voteCount,
		"quorumSize", em.quorumSize,
		"hasQuorum", em.voteCount >= em.quorumSize,
		"term", em.voteTerm,
		"allVoters", em.getVotersList())
}

// hasPreVoteQuorum checks if we have enough pre-votes
func (em *electionManager) hasPreVoteQuorum() bool {
	em.preVoteMu.RLock()
	defer em.preVoteMu.RUnlock()

	hasIt := em.preVoteCount >= em.quorumSize

	em.logger.Infow("Pre-vote quorum check - DETAILED",
		"nodeID", em.id,
		"preVoteCount", em.preVoteCount,
		"quorumSize", em.quorumSize,
		"hasQuorum", hasIt,
		"term", em.preVoteTerm,
		"allPreVoters", em.getPreVotersListUnsafe())

	return hasIt
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

// Helper functions for voter lists
func (em *electionManager) getPreVotersList() []types.NodeID {
	var voters []types.NodeID
	for voterID := range em.preVotesReceived {
		voters = append(voters, voterID)
	}
	return voters
}

func (em *electionManager) getPreVotersListUnsafe() []types.NodeID {
	var voters []types.NodeID
	for voterID := range em.preVotesReceived {
		voters = append(voters, voterID)
	}
	return voters
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
	isPreVote bool,
	originatorTerm types.Term,
) {
	requestType := "vote"
	if isPreVote {
		requestType = "pre-vote"
	}

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

			if isPreVote {
				em.sendPreVoteRequest(rpcCtx, targetPeerID, args, originatorTerm)
			} else {
				em.sendVoteRequest(rpcCtx, targetPeerID, args, false, args.Term)
			}
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

// sendPreVoteRequest sends a pre-vote request with improved error handling
func (em *electionManager) sendPreVoteRequest(
	ctx context.Context,
	peerID types.NodeID,
	args *types.RequestVoteArgs,
	originatorTerm types.Term,
) {
	reply, err := em.networkMgr.SendRequestVote(ctx, peerID, args)
	if err != nil {
		em.logger.Debugw("Pre-vote request failed",
			"to", peerID,
			"preVoteTerm", args.Term,
			"error", err,
			"nodeID", em.id)
		return
	}

	em.processPreVoteReply(peerID, args.Term, originatorTerm, reply)
}

// sendVoteRequest sends a regular vote request with improved error handling
func (em *electionManager) sendVoteRequest(
	ctx context.Context,
	peer types.NodeID,
	args *types.RequestVoteArgs,
	isPreVote bool,
	currentTerm types.Term,
) {
	requestCtx, cancel := context.WithTimeout(ctx, voteRequestTimeout)
	defer cancel()

	reply, err := em.networkMgr.SendRequestVote(requestCtx, peer, args)
	if err != nil {
		em.logger.Warnw("Vote request failed - NETWORK ERROR",
			"nodeID", em.id,
			"to", peer,
			"term", args.Term,
			"error", err)

		if !isPreVote {
			em.splitVoteDetector.mu.Lock()
			em.splitVoteDetector.electionAttempts++
			em.splitVoteDetector.mu.Unlock()
		}
		return
	}

	if isPreVote {
		em.processPreVoteReply(peer, args.Term, currentTerm, reply)
	} else {
		em.processVoteReply(peer, args.Term, currentTerm, reply)
	}
}

// Enhanced vote request handling with improved validation
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
	if args.IsPreVote {
		requestType = "pre-vote"
	}

	em.logger.Debugw("Processing vote request",
		"type", requestType,
		"from", args.CandidateID,
		"term", args.Term,
		"nodeID", em.id)

	if !args.IsPreVote {
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
	}

	currentTerm, currentRole, votedFor := em.stateMgr.GetState()
	reply.Term = currentTerm

	if args.Term < currentTerm {
		em.logger.Debugw("Rejecting vote request: outdated term",
			"from", args.CandidateID,
			"requestTerm", args.Term,
			"currentTerm", currentTerm,
			"nodeID", em.id)
		return reply, nil
	}

	if args.IsPreVote {
		return em.handlePreVoteRequest(args, currentTerm, currentRole)
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
		em.logger.Errorw("Failed to grant vote",
			"to", args.CandidateID,
			"term", args.Term,
			"nodeID", em.id)
		return reply, errors.New("failed to grant vote")
	}

	reply.VoteGranted = true
	em.ResetTimerOnHeartbeat()

	return reply, nil
}

// handlePreVoteRequest handles pre-vote requests with special logic
func (em *electionManager) handlePreVoteRequest(
	args *types.RequestVoteArgs,
	currentTerm types.Term,
	currentRole types.NodeRole,
) (*types.RequestVoteReply, error) {
	reply := &types.RequestVoteReply{
		Term:        currentTerm,
		VoteGranted: false,
	}

	if currentRole == types.RoleFollower {
		currentLeader, _, hasLeader := em.stateMgr.GetLeaderInfo()
		if hasLeader && currentLeader != "" {
			em.logger.Debugw("Rejecting pre-vote: have current leader",
				"from", args.CandidateID,
				"currentLeader", currentLeader,
				"nodeID", em.id)
			return reply, nil
		}
	}

	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()
	logUpToDate := em.isLogUpToDate(args.LastLogTerm, args.LastLogIndex, lastTerm, lastIndex)

	if !logUpToDate {
		em.logger.Debugw("Rejecting pre-vote: log not up-to-date",
			"from", args.CandidateID,
			"candidateLastTerm", args.LastLogTerm,
			"candidateLastIndex", args.LastLogIndex,
			"ourLastTerm", lastTerm,
			"ourLastIndex", lastIndex,
			"nodeID", em.id)
		return reply, nil
	}

	em.logger.Debugw("Granting pre-vote",
		"to", args.CandidateID,
		"term", args.Term,
		"nodeID", em.id)

	reply.VoteGranted = true
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

// Stop performs any necessary cleanup when shutting down the election manager
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
