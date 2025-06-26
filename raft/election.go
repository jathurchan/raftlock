package raft

import (
	"context"
	"errors"
	"fmt"
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
// with careful structuring to avoid deadlocks and maintain responsiveness.
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

	electionTickCount   int     // Base number of ticks before an election timeout
	randomizationFactor float64 // Randomization percentage to avoid election collisions
	enablePreVote       bool    // Whether to perform a pre-vote phase before a real election

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

	startPreVote          func(ctx context.Context) // Begins the pre-vote phase
	handleElectionTimeout func(ctx context.Context) // Handles election timeout logic
	startElection         func(ctx context.Context) // Begins a real election

	timerMu sync.Mutex // Serializes access to pre-vote/election delay timers to prevent overlapping triggers

	preVoteSuccessTime atomic.Int64  // Timestamp when pre-vote succeeded
	electionStartDelay time.Duration // Random delay before starting election after pre-vote

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
	Config            Config
}

// NewElectionManager creates a new election manager with improved deadlock prevention
func NewElectionManager(deps ElectionManagerDeps) (ElectionManager, error) {
	if err := validateElectionManagerDeps(deps); err != nil {
		return nil, fmt.Errorf("invalid election manager dependencies: %w", err)
	}

	log := deps.Logger.WithComponent("election")
	opts := deps.Config.Options
	features := deps.Config.FeatureFlags

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

		electionTickCount:   opts.ElectionTickCount,
		randomizationFactor: opts.ElectionRandomizationFactor,
		enablePreVote:       features.PreVoteEnabled,

		// Initialize with a small random delay (0-50ms)
		electionStartDelay: time.Duration(deps.Rand.IntN(50)) * time.Millisecond,

		votesReceived: make(map[types.NodeID]bool),
		stopCh:        make(chan struct{}),
	}

	em.electionState.Store(int32(ElectionStateIdle))

	em.startPreVote = em.defaultStartPreVote
	em.handleElectionTimeout = em.defaultHandleElectionTimeout
	em.startElection = em.defaultStartElection

	em.applyDefaults()
	em.resetElectionTimeoutPeriod()

	log.Infow("Election manager initialized",
		"nodeID", em.id,
		"preVoteEnabled", em.enablePreVote,
		"electionTickCount", em.electionTickCount,
		"randomizationFactor", em.randomizationFactor,
		"quorumSize", em.quorumSize,
		"peerCount", len(em.peers)-1)

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
	return nil
}

// applyDefaults sets reasonable defaults for configuration parameters
func (em *electionManager) applyDefaults() {
	if em.electionTickCount <= 0 {
		em.logger.Warnw("Invalid ElectionTickCount, using default",
			"provided", em.electionTickCount,
			"default", DefaultElectionTickCount)
		em.electionTickCount = DefaultElectionTickCount
	}
	if em.electionTickCount <= DefaultHeartbeatTickCount {
		em.logger.Warnw("ElectionTickCount should be greater than HeartbeatTickCount",
			"electionTicks", em.electionTickCount,
			"heartbeatTicks", DefaultHeartbeatTickCount)
	}
	if em.randomizationFactor < 0.0 || em.randomizationFactor > 1.0 {
		em.logger.Warnw("Invalid ElectionRandomizationFactor, using default",
			"provided", em.randomizationFactor,
			"default", DefaultElectionRandomizationFactor)
		em.randomizationFactor = DefaultElectionRandomizationFactor
	}
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

// ResetTimerOnHeartbeat resets the election timer when a heartbeat is received
func (em *electionManager) ResetTimerOnHeartbeat() {
	if em.isShutdown.Load() {
		return
	}

	em.timerMu.Lock()
	defer em.timerMu.Unlock()

	min := float64(em.electionTickCount)
	max := min * (1.0 + em.randomizationFactor)
	randomized := min + em.rand.Float64()*(max-min)

	em.randomizedPeriod = int(randomized)
	em.electionElapsed = 0

	em.logger.Debugw("Election timeout period reset",
		"nodeID", em.id,
		"randomizedPeriod", em.randomizedPeriod,
		"elapsed", em.electionElapsed)
}

// resetElectionTimeoutPeriod calculates a new randomized election timeout
// Must be called with em.mu.Lock() held
func (em *electionManager) resetElectionTimeoutPeriod() {
	em.timerMu.Lock()
	defer em.timerMu.Unlock()

	min := float64(em.electionTickCount)
	max := min * (1.0 + em.randomizationFactor)
	randomized := min + em.rand.Float64()*(max-min)

	em.randomizedPeriod = int(randomized)
	em.electionElapsed = 0

	em.logger.Debugw("Election timeout period reset",
		"nodeID", em.id,
		"randomizedPeriod", em.randomizedPeriod,
		"elapsed", em.electionElapsed)
}

// defaultHandleElectionTimeout handles election timeouts with improved state management
func (em *electionManager) defaultHandleElectionTimeout(ctx context.Context) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Election timeout ignored: node shutting down", "nodeID", em.id)
		return
	}

	// Prevent multiple concurrent elections using atomic operation
	if !em.electionInFlight.CompareAndSwap(false, true) {
		em.logger.Debugw("Election timeout ignored: election already in progress", "nodeID", em.id)
		return
	}
	defer em.electionInFlight.Store(false)

	now := time.Now().UnixMilli()
	lastElection := em.lastElectionTime.Load()
	if now-lastElection < 100 { // Minimum 100ms between elections
		em.logger.Debugw("Election timeout ignored: too soon after last election",
			"nodeID", em.id, "timeSince", now-lastElection, "unit", "ms")
		return
	}

	currentTerm, role, _ := em.stateMgr.GetState()
	em.logger.Debugw("Handling election timeout",
		"nodeID", em.id,
		"currentTerm", currentTerm,
		"role", role.String())

	// Only followers should start elections
	if role != types.RoleFollower {
		em.logger.Debugw("Not starting election: not a follower",
			"nodeID", em.id, "role", role.String())
		return
	}

	em.lastElectionTime.Store(now)
	em.electionCount.Add(1)

	if em.enablePreVote {
		em.electionState.Store(int32(ElectionStatePreVoting))
		em.startPreVote(ctx)
	} else {
		em.electionState.Store(int32(ElectionStateVoting))
		em.startElection(ctx)
	}
}

// defaultStartPreVote initiates the pre-vote phase with improved error handling
func (em *electionManager) defaultStartPreVote(ctx context.Context) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Pre-vote aborted: node shutting down", "nodeID", em.id)
		return
	}

	currentTerm, role, _ := em.stateMgr.GetState()

	if role != types.RoleFollower {
		em.logger.Debugw("Pre-vote aborted: no longer a follower",
			"nodeID", em.id, "role", role.String())
		em.electionState.Store(int32(ElectionStateIdle))
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

	em.resetVoteTracking(preVoteTerm)

	args := &types.RequestVoteArgs{
		Term:         preVoteTerm,
		CandidateID:  em.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		IsPreVote:    true,
	}

	em.broadcastVoteRequests(ctx, args, true, currentTerm)
}

// defaultStartElection initiates a regular election with improved error handling
func (em *electionManager) defaultStartElection(ctx context.Context) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Election aborted: node shutting down", "nodeID", em.id)
		return
	}

	candidateCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	if !em.stateMgr.BecomeCandidate(candidateCtx, ElectionReasonTimeout) {
		em.logger.Debugw("Failed to become candidate", "nodeID", em.id)
		em.electionState.Store(int32(ElectionStateIdle))
		return
	}

	newTerm, role, _ := em.stateMgr.GetState()
	if role != types.RoleCandidate {
		em.logger.Warnw("Failed to transition to candidate role",
			"nodeID", em.id, "actualRole", role.String())
		em.electionState.Store(int32(ElectionStateIdle))
		return
	}

	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()

	em.logger.Infow("Starting election",
		"nodeID", em.id,
		"term", newTerm,
		"lastLogIndex", lastIndex,
		"lastLogTerm", lastTerm)

	em.resetVoteTracking(newTerm)
	em.recordVote(em.id, true)

	args := &types.RequestVoteArgs{
		Term:         newTerm,
		CandidateID:  em.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		IsPreVote:    false,
	}

	em.broadcastVoteRequests(ctx, args, false, newTerm)

	if em.hasQuorum() {
		em.becomeLeader(ctx, newTerm)
	}
}

// resetVoteTracking clears vote tracking state for a new term
func (em *electionManager) resetVoteTracking(term types.Term) {
	em.voteMu.Lock()
	defer em.voteMu.Unlock()

	for k := range em.votesReceived {
		delete(em.votesReceived, k)
	}
	em.voteCount = 0
	em.voteTerm = term

	em.logger.Debugw("Vote tracking reset",
		"nodeID", em.id,
		"term", term,
		"quorumSize", em.quorumSize)
}

// recordVote records a vote and checks for quorum
func (em *electionManager) recordVote(from types.NodeID, granted bool) {
	if !granted {
		return
	}

	em.voteMu.Lock()
	defer em.voteMu.Unlock()

	if em.votesReceived[from] {
		return
	}

	em.votesReceived[from] = true
	em.voteCount++

	em.logger.Debugw("Vote recorded",
		"from", from,
		"nodeID", em.id,
		"voteCount", em.voteCount,
		"quorumSize", em.quorumSize,
		"term", em.voteTerm)
}

// hasQuorum checks if we have enough votes to become leader
func (em *electionManager) hasQuorum() bool {
	em.voteMu.RLock()
	defer em.voteMu.RUnlock()
	return em.voteCount >= em.quorumSize
}

// broadcastVoteRequests sends vote requests to all peers
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
				em.sendVoteRequest(rpcCtx, targetPeerID, args)
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
	peerID types.NodeID,
	args *types.RequestVoteArgs,
) {
	reply, err := em.networkMgr.SendRequestVote(ctx, peerID, args)
	if err != nil {
		em.logger.Debugw("Vote request failed",
			"to", peerID,
			"term", args.Term,
			"error", err,
			"nodeID", em.id)
		return
	}

	em.processVoteReply(peerID, reply)
}

// processPreVoteReply handles pre-vote responses with improved validation
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
		em.electionState.Store(int32(ElectionStateIdle))
		return
	}

	em.logger.Debugw("Received pre-vote reply",
		"from", fromPeerID,
		"granted", reply.VoteGranted,
		"term", reply.Term,
		"nodeID", em.id)

	em.recordVote(fromPeerID, reply.VoteGranted)

	if em.hasQuorum() {
		em.logger.Infow("Pre-vote succeeded, starting real election after delay",
			"nodeID", em.id,
			"preVoteTerm", preVoteTerm,
			"delay", em.electionStartDelay)

		em.preVoteSuccessTime.Store(time.Now().UnixNano())

		go em.startElectionWithDelay()
	}
}

func (em *electionManager) startElectionWithDelay() {
	delay := em.electionStartDelay + time.Duration(em.rand.IntN(25))*time.Millisecond

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
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
		em.electionState.Store(int32(ElectionStateIdle))
		return
	}

	currentLeader, _, hasLeader := em.stateMgr.GetLeaderInfo()
	if hasLeader && currentLeader != "" {
		em.logger.Debugw("Cancelling delayed election: leader exists",
			"nodeID", em.id, "leader", currentLeader)
		em.electionState.Store(int32(ElectionStateIdle))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), electionManagerOpTimeout)
	defer cancel()

	em.electionState.Store(int32(ElectionStateVoting))
	em.startElection(ctx)
}

// processVoteReply handles regular vote responses
func (em *electionManager) processVoteReply(
	fromPeerID types.NodeID,
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

	currentTerm, role, _ := em.stateMgr.GetState()

	if reply.Term > currentTerm {
		ctx, cancel := context.WithTimeout(context.Background(), stateTransitionTimeout)
		defer cancel()
		em.handleHigherTermDetected(ctx, reply.Term, fromPeerID)
		return
	}

	if role != types.RoleCandidate {
		em.logger.Debugw("Ignoring vote reply: no longer a candidate",
			"from", fromPeerID, "nodeID", em.id, "role", role.String())
		return
	}

	em.logger.Debugw("Received vote reply",
		"from", fromPeerID,
		"granted", reply.VoteGranted,
		"term", reply.Term,
		"nodeID", em.id)

	em.recordVote(fromPeerID, reply.VoteGranted)

	if em.hasQuorum() {
		ctx, cancel := context.WithTimeout(context.Background(), electionManagerOpTimeout)
		defer cancel()
		em.becomeLeader(ctx, currentTerm)
	}
}

func (em *electionManager) handleHigherTermDetected(
	ctx context.Context,
	higherTerm types.Term,
	fromNode types.NodeID,
) {
	em.logger.Infow("Higher term detected, stepping down and cleaning up election state",
		"nodeID", em.id,
		"currentTerm", em.getCurrentTerm(),
		"higherTerm", higherTerm,
		"from", fromNode)

	em.electionState.Store(int32(ElectionStateIdle))
	em.electionInFlight.Store(false)

	em.voteMu.Lock()
	for k := range em.votesReceived {
		delete(em.votesReceived, k)
	}
	em.voteCount = 0
	em.voteMu.Unlock()

	em.stateMgr.CheckTermAndStepDown(ctx, higherTerm, fromNode)
}

func (em *electionManager) getCurrentTerm() types.Term {
	term, _, _ := em.stateMgr.GetState()
	return term
}

// becomeLeader transitions the node to leader state
func (em *electionManager) becomeLeader(ctx context.Context, term types.Term) {
	if em.isShutdown.Load() {
		return
	}

	currentTerm, role, _ := em.stateMgr.GetState()
	if term != currentTerm {
		em.logger.Warnw("Cannot become leader: term changed",
			"expectedTerm", term,
			"currentTerm", currentTerm,
			"nodeID", em.id)
		em.electionState.Store(int32(ElectionStateIdle))
		return
	}

	if role != types.RoleCandidate {
		em.logger.Debugw("Cannot become leader: no longer a candidate",
			"nodeID", em.id,
			"role", role.String(),
			"term", currentTerm)
		em.electionState.Store(int32(ElectionStateIdle))
		return
	}

	leaderCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	if !em.stateMgr.BecomeLeader(leaderCtx) {
		em.logger.Warnw("Failed to transition to leader state",
			"nodeID", em.id,
			"term", currentTerm)
		em.electionState.Store(int32(ElectionStateIdle))
		return
	}

	em.logger.Infow("Successfully became leader",
		"nodeID", em.id,
		"term", currentTerm,
		"electionCount", em.electionCount.Load())

	em.leaderInitializer.InitializeLeaderState()

	em.leaderInitializer.SendHeartbeats(leaderCtx)

	em.electionState.Store(int32(ElectionStateIdle))
}

// HandleRequestVote processes incoming vote requests with improved validation
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
			em.electionState.Store(int32(ElectionStateIdle))
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

// Stop performs cleanup when shutting down the election manager
func (em *electionManager) Stop() {
	em.stopOnce.Do(func() {
		em.logger.Infow("Stopping election manager", "nodeID", em.id)

		close(em.stopCh)

		timeout := time.NewTimer(2 * time.Second)
		defer timeout.Stop()

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
				if em.concurrentOps.Load() == 0 {
					em.logger.Infow("Election manager stopped cleanly", "nodeID", em.id)
					return
				}
			}
		}
	})
}
