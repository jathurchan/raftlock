package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
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
	HandleRequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteReply, error)

	// Stop performs any necessary cleanup when shutting down the election manager.
	Stop()
}

// electionManager implements the ElectionManager interface. It handles Raft election logic,
// including managing election timeouts, initiating pre-vote or full election rounds,
// processing vote requests and replies from peers, and coordinating state transitions.
type electionManager struct {
	mu         *sync.RWMutex // Raft's mutex protecting state fields
	isShutdown *atomic.Bool  // Shared flag indicating Raft shutdown

	id         types.NodeID                // ID of the local Raft node.
	peers      map[types.NodeID]PeerConfig // Configuration of peer nodes
	quorumSize int                         // Number of votes needed to win an election (majority)

	leaderInitializer LeaderInitializer
	stateMgr          StateManager
	logMgr            LogManager
	networkMgr        NetworkManager
	metrics           Metrics
	logger            logger.Logger
	rand              Rand

	electionTickCount   int     // Number of votes needed to win an election (majority)
	randomizationFactor float64 // Factor (0.0 to 1.0) to add jitter to election timeout
	enablePreVote       bool    // Whether to use the PreVote optimization

	electionElapsed  int // Ticks elapsed since last heartbeat or election start (protected by mu)
	randomizedPeriod int // Current randomized election timeout period in ticks (protected by mu)

	votesReceived map[types.NodeID]bool // Tracks votes received in the current election round (term specific)
	voteMu        sync.Mutex            // Dedicated mutex for votesReceived map

	startPreVote          func(ctx context.Context)
	handleElectionTimeout func(ctx context.Context)
	startElection         func(ctx context.Context)
}

// ElectionManagerDeps encapsulates the external dependencies required to construct an ElectionManager.
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

// NewElectionManager initializes and returns a new ElectionManager instance using the provided dependencies.
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

		votesReceived: make(map[types.NodeID]bool),
	}

	em.startPreVote = em.defaultStartPreVote
	em.handleElectionTimeout = em.defaultHandleElectionTimeout
	em.startElection = em.defaultStartElection

	em.applyDefaults()

	log.Infow("Election manager initialized",
		"preVoteEnabled", em.enablePreVote,
		"electionTickCount", em.electionTickCount,
		"randomizationFactor", em.randomizationFactor,
		"quorumSize", em.quorumSize,
		"peerCount", len(em.peers)-1, // Exclude self
	)

	return em, nil
}

// validateElectionManagerDeps ensures all required dependencies for ElectionManager are provided and valid.
func validateElectionManagerDeps(deps ElectionManagerDeps) error {
	if deps.ID == unknownNodeID {
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
	if deps.NetworkMgr == nil {
		return errors.New("network manager (NetworkMgr) must not be nil")
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

// applyDefaults assigns default values to election configuration parameters if unset or invalid.
func (em *electionManager) applyDefaults() {
	if em.electionTickCount <= 0 {
		em.logger.Warnw("Invalid ElectionTickCount, using default", "provided", em.electionTickCount, "default", DefaultElectionTickCount)
		em.electionTickCount = DefaultElectionTickCount
	}
	if em.electionTickCount <= DefaultHeartbeatTickCount {
		em.logger.Warnw("ElectionTickCount should be greater than HeartbeatTickCount", "electionTicks", em.electionTickCount, "heartbeatTicks", DefaultHeartbeatTickCount)
	}
	if em.randomizationFactor < 0.0 || em.randomizationFactor > 1.0 {
		em.logger.Warnw("Invalid ElectionRandomizationFactor, using default", "provided", em.randomizationFactor, "default", DefaultElectionRandomizationFact)
		em.randomizationFactor = DefaultElectionRandomizationFact
	}
}

// Initialize sets up the election manager, primarily by calculating the initial randomized election timeout.
func (em *electionManager) Initialize(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		em.logger.Warnw("Initialization aborted due to context error", "error", err)
		return err
	}

	if em.isShutdown.Load() {
		em.logger.Warnw("Initialization aborted: node is shutting down")
		return ErrShuttingDown
	}

	term, role, _ := em.stateMgr.GetState()
	if term == 0 && role != types.RoleFollower {
		err := fmt.Errorf("invalid initial state: term=%d, role=%s, expected RoleFollower at term 0", term, role)
		em.logger.Errorw("Initialization failed: inconsistent initial state", "error", err)
		return err
	}

	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()

	randPeriod := em.resetElectionTimeoutPeriod()

	em.logger.Infow("Election manager initialized",
		"term", term,
		"role", role.String(),
		"lastLogIndex", lastIndex,
		"lastLogTerm", lastTerm,
		"initialRandomizedTimeoutTicks", randPeriod,
	)

	return nil
}

// Tick advances the election timer. If the timeout is reached for a non-leader,
// it triggers the election process (pre-vote or full election).
func (em *electionManager) Tick(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		em.logger.Debugw("Tick skipped due to context error", "error", err, "nodeID", em.id)
		return
	}

	if em.isShutdown.Load() {
		return
	}

	term, role, _ := em.stateMgr.GetState()
	if role == types.RoleLeader {
		return // Leaders don't participate in election timeouts
	}

	em.mu.Lock()
	em.electionElapsed++
	elapsed := em.electionElapsed
	timeoutThreshold := em.randomizedPeriod
	em.mu.Unlock()

	if elapsed < timeoutThreshold {
		return
	}

	em.logger.Infow("Election timeout detected",
		"elapsedTicks", elapsed,
		"timeoutThreshold", timeoutThreshold,
		"nodeID", em.id,
	)

	em.metrics.ObserveElectionElapsed(em.id, term, elapsed)

	em.resetElectionTimeoutPeriod()
	go em.handleElectionTimeout(ctx)
}

// defaultHandleElectionTimeout determines whether to start a pre-vote or a full election
// based on configuration and current state. Called asynchronously after a timeout.
func (em *electionManager) defaultHandleElectionTimeout(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		em.logger.Debugw("Election timeout handling aborted: parent context cancelled", "error", err, "nodeID", em.id)
		return
	}
	if em.isShutdown.Load() {
		em.logger.Debugw("Election timeout handling aborted: node is shutting down", "nodeID", em.id)
		return
	}

	opCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	_, role, _ := em.stateMgr.GetState() // Check role again
	if role == types.RoleLeader {
		em.logger.Infow("Election timeout handling skipped: node became leader", "nodeID", em.id)
		return
	}

	if em.enablePreVote {
		em.logger.Infow("Election timeout: initiating pre-vote phase", "nodeID", em.id)
		em.startPreVote(opCtx)
	} else {
		em.logger.Infow("Election timeout: initiating election", "nodeID", em.id)
		em.startElection(opCtx)
	}
}

// defaultStartPreVote initiates the pre-vote phase. It sends pre-vote requests to peers
// without incrementing the term, aiming to gauge likelihood of winning a real election.
func (em *electionManager) defaultStartPreVote(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		em.logger.Debugw("Pre-vote start aborted: context cancelled", "error", err, "nodeID", em.id)
		return
	}
	if em.isShutdown.Load() {
		em.logger.Debugw("Pre-vote aborted: node is shutting down", "nodeID", em.id)
		return
	}

	term, role, _ := em.stateMgr.GetState()
	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()

	if role == types.RoleLeader { // Check role again
		em.logger.Infow("Pre-vote skipped: node is already the leader", "term", term, "nodeID", em.id)
		return
	}

	preVoteTerm := term + 1

	em.logger.Infow("Starting pre-vote phase",
		"nodeID", em.id,
		"currentTerm", term,
		"preVoteTerm", preVoteTerm,
		"lastLogIndex", lastIndex,
		"lastLogTerm", lastTerm,
	)

	em.metrics.ObserveElectionStart(preVoteTerm, ElectionReasonPreVote)

	em.resetVotesReceived()

	args := &types.RequestVoteArgs{
		Term:         preVoteTerm,
		CandidateID:  em.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		IsPreVote:    true,
	}

	em.broadcastVoteRequests(ctx, args, term, true)
}

// defaultStartElection initiates a full election. It transitions the node to Candidate,
// increments the term, votes for itself, persists state, and sends RequestVote RPCs.
func (em *electionManager) defaultStartElection(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		em.logger.Debugw("Election start aborted: context cancelled", "error", err, "nodeID", em.id)
		return
	}
	if em.isShutdown.Load() {
		em.logger.Debugw("Election aborted: node is shutting down", "nodeID", em.id)
		return
	}

	persistCtx, cancelPersist := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancelPersist()

	if !em.stateMgr.BecomeCandidate(persistCtx, ElectionReasonTimeout) {
		em.logger.Warnw("Election aborted: failed to transition to Candidate state", "nodeID", em.id)
		return
	}

	currentTerm, role, _ := em.stateMgr.GetState()
	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()

	if role != types.RoleCandidate {
		em.logger.Errorw("Election logic error: node role is not Candidate after successful BecomeCandidate call",
			"nodeID", em.id, "role", role.String(), "term", currentTerm)
		return
	}

	em.logger.Infow("Starting election",
		"nodeID", em.id,
		"term", currentTerm,
	)

	if len(em.peers) <= 1 {
		em.logger.Infow("Single-node cluster: becoming leader immediately", "term", currentTerm, "nodeID", em.id)
		go em.becomeLeaderIfWon(ctx)
		return
	}

	em.resetVotesReceived()

	args := &types.RequestVoteArgs{
		Term:         currentTerm,
		CandidateID:  em.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		IsPreVote:    false,
	}

	em.broadcastVoteRequests(ctx, args, currentTerm, false)
}

// broadcastVoteRequests sends RequestVote or PreVote RPCs to all configured peers in parallel.
func (em *electionManager) broadcastVoteRequests(
	ctx context.Context,
	args *types.RequestVoteArgs,
	originatorTerm types.Term,
	isPreVote bool,
) {
	em.logger.Debugw("Broadcasting requests", "type", requestTypeLabel(isPreVote), "term", args.Term, "nodeID", em.id)

	var wg sync.WaitGroup
	peerCount := 0

	for peerID := range em.peers {
		if peerID == em.id {
			continue // Skip self
		}

		peerCount++
		wg.Add(1)

		go func(targetPeerID types.NodeID) {
			defer wg.Done()
			rpcCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
			defer cancel()

			if isPreVote {
				em.sendPreVoteRequest(rpcCtx, targetPeerID, args, originatorTerm)
			} else {
				em.sendVoteRequest(rpcCtx, targetPeerID, args)
			}
		}(peerID)
	}

	go func(term types.Term, reqType string, count int) {
		wg.Wait()
		em.logger.Debugw("Finished broadcasting requests",
			"type", reqType,
			"term", term,
			"peerCount", count,
			"nodeID", em.id,
		)
	}(args.Term, requestTypeLabel(isPreVote), peerCount)
}

// sendPreVoteRequest sends a single pre-vote RPC to a peer and handles the response.
func (em *electionManager) sendPreVoteRequest(
	ctx context.Context,
	targetPeerID types.NodeID,
	args *types.RequestVoteArgs,
	originatorTerm types.Term,
) {
	em.logger.Debugw("Sending pre-vote request",
		"to", targetPeerID,
		"preVoteTerm", args.Term,
		"lastLogIndex", args.LastLogIndex,
		"lastLogTerm", args.LastLogTerm,
		"nodeID", em.id,
	)

	reply, err := em.networkMgr.SendRequestVote(ctx, targetPeerID, args)

	if err != nil {
		if errors.Is(err, context.Canceled) {
			em.logger.Debugw("Pre-vote request canceled", "to", targetPeerID, "nodeID", em.id)
		} else if errors.Is(err, context.DeadlineExceeded) {
			em.logger.Warnw("Pre-vote request timed out", "to", targetPeerID, "timeout", ctx.Err(), "nodeID", em.id)
		} else {
			em.logger.Warnw("Pre-vote request failed", "to", targetPeerID, "error", err, "nodeID", em.id)
		}
		return // Do not process reply if RPC failed
	}

	em.processPreVoteReply(targetPeerID, args.Term, originatorTerm, reply)
}

// sendVoteRequest sends a single regular RequestVote RPC to a peer and handles the response.
func (em *electionManager) sendVoteRequest(ctx context.Context, targetPeerID types.NodeID, args *types.RequestVoteArgs) {
	em.logger.Debugw("Sending vote request",
		"to", targetPeerID,
		"term", args.Term,
		"lastLogIndex", args.LastLogIndex,
		"lastLogTerm", args.LastLogTerm,
		"nodeID", em.id)

	reply, err := em.networkMgr.SendRequestVote(ctx, targetPeerID, args)

	if err != nil {
		if errors.Is(err, context.Canceled) {
			em.logger.Debugw("Vote request canceled", "to", targetPeerID, "nodeID", em.id)
		} else if errors.Is(err, context.DeadlineExceeded) {
			em.logger.Warnw("Vote request timed out", "to", targetPeerID, "timeout", ctx.Err(), "nodeID", em.id)
		} else {
			em.logger.Warnw("Vote request failed", "to", targetPeerID, "error", err, "nodeID", em.id)
		}
		return // Do not process reply if RPC failed
	}

	// Process the received reply
	em.processVoteReply(ctx, targetPeerID, args.Term, reply)
}

// processPreVoteReply handles the response from a pre-vote request.
func (em *electionManager) processPreVoteReply(
	fromPeerID types.NodeID,
	preVoteTerm types.Term,
	originatorTerm types.Term,
	reply *types.RequestVoteReply,
) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Ignoring pre-vote reply: node is shutting down", "from", fromPeerID, "nodeID", em.id)
		return
	}

	if reply.Term > preVoteTerm {
		em.logger.Debugw("Ignoring higher-term pre-vote reply without stepping down",
			"from", fromPeerID, "replyTerm", reply.Term, "localTerm", preVoteTerm)
		return
	}

	currentTerm, _, _ := em.stateMgr.GetState()
	if currentTerm != originatorTerm {
		em.logger.Debugw("Ignoring stale pre-vote reply: local term changed since pre-vote started",
			"from", fromPeerID,
			"replyTerm", reply.Term,
			"preVoteTerm", preVoteTerm,
			"originatorTerm", originatorTerm,
			"currentTerm", currentTerm,
			"nodeID", em.id)
		return
	}

	if reply.VoteGranted {
		em.logger.Debugw("Pre-vote granted", "from", fromPeerID, "preVoteTerm", preVoteTerm, "nodeID", em.id)
		quorumAchieved := em.recordVoteAndCheckQuorum(fromPeerID, preVoteTerm)

		if quorumAchieved {
			em.logger.Infow("Pre-vote quorum achieved, starting election",
				"preVoteTerm", preVoteTerm,
				"quorumSize", em.quorumSize,
				"nodeID", em.id)
			go em.startElection(context.Background())
		}
	} else {
		em.logger.Debugw("Pre-vote denied", "from", fromPeerID, "preVoteTerm", preVoteTerm, "replyTerm", reply.Term, "nodeID", em.id)
	}
}

// processVoteReply handles the response from a regular RequestVote RPC.
func (em *electionManager) processVoteReply(
	ctx context.Context,
	fromPeerID types.NodeID,
	electionTerm types.Term,
	reply *types.RequestVoteReply,
) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Ignoring vote reply: node is shutting down", "from", fromPeerID, "nodeID", em.id)
		return
	}

	steppedDown, localTermBeforeStepDown := em.stateMgr.CheckTermAndStepDown(ctx, reply.Term, fromPeerID) // Leader hint might be inaccurate here
	if steppedDown {
		em.logger.Infow("Stepped down due to higher term in vote reply",
			"from", fromPeerID, "replyTerm", reply.Term, "localTermBefore", localTermBeforeStepDown, "nodeID", em.id)
		return
	}

	currentTerm, currentRole, _ := em.stateMgr.GetState()
	if currentRole != types.RoleCandidate || currentTerm != electionTerm {
		em.logger.Debugw("Ignoring stale or irrelevant vote reply",
			"from", fromPeerID,
			"replyTerm", reply.Term,
			"electionTerm", electionTerm,
			"currentTerm", currentTerm,
			"currentRole", currentRole.String(),
			"nodeID", em.id)
		return
	}

	if reply.VoteGranted {
		em.logger.Debugw("Vote granted", "from", fromPeerID, "term", electionTerm, "nodeID", em.id)
		quorumAchieved := em.recordVoteAndCheckQuorum(fromPeerID, electionTerm)

		if quorumAchieved {
			em.logger.Infow("Election quorum achieved, becoming leader",
				"term", electionTerm,
				"quorumSize", em.quorumSize,
				"nodeID", em.id)
			go em.becomeLeaderIfWon(context.Background())
		}
	} else {
		em.logger.Debugw("Vote denied", "from", fromPeerID, "term", electionTerm, "replyTerm", reply.Term, "nodeID", em.id)
	}
}

// recordVoteAndCheckQuorum registers a granted vote from a peer for the given term.
// Returns true if quorum is achieved after recording this vote.
func (em *electionManager) recordVoteAndCheckQuorum(fromPeerID types.NodeID, term types.Term) bool {
	em.voteMu.Lock()
	defer em.voteMu.Unlock()

	if _, exists := em.votesReceived[fromPeerID]; exists {
		// Might happen with retries or duplicate network messages.
		em.logger.Debugw("Duplicate vote received or already recorded",
			"from", fromPeerID, "term", term, "nodeID", em.id)
		return false // Quorum state doesn't change
	}

	em.votesReceived[fromPeerID] = true
	voteCount := len(em.votesReceived)
	quorumReached := voteCount >= em.quorumSize

	em.logger.Debugw("Vote recorded",
		"from", fromPeerID,
		"term", term,
		"nodeID", em.id,
		"totalVotes", voteCount,
		"quorumSize", em.quorumSize,
		"quorumReached", quorumReached)

	return quorumReached
}

// ResetTimerOnHeartbeat resets the election timer in response to a valid heartbeat
// or AppendEntries RPC from the current leader. This prevents unnecessary elections.
func (em *electionManager) ResetTimerOnHeartbeat() {
	if em.isShutdown.Load() {
		em.logger.Debugw("Election timer reset skipped: node is shutting down")
		return
	}

	em.logger.Debugw("Resetting election timer due to heartbeat/AppendEntries", "nodeID", em.id)
	em.resetElectionTimeoutPeriod()
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (em *electionManager) HandleRequestVote(ctx context.Context, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
	if err := ctx.Err(); err != nil {
		em.logger.Debugw("Rejecting vote request: context cancelled", "from", args.CandidateID, "error", err, "nodeID", em.id)
		term, _, _ := em.stateMgr.GetState()
		return &types.RequestVoteReply{Term: term, VoteGranted: false}, err
	}

	if em.isShutdown.Load() {
		term, _, _ := em.stateMgr.GetState()
		em.logger.Debugw("Rejecting vote request: node is shutting down", "from", args.CandidateID, "nodeID", em.id)
		return &types.RequestVoteReply{Term: term, VoteGranted: false}, ErrShuttingDown
	}

	reply := &types.RequestVoteReply{VoteGranted: false}

	_, termBeforeCheck := em.stateMgr.CheckTermAndStepDown(ctx, args.Term, args.CandidateID)

	currentTerm, _, _ := em.stateMgr.GetState()
	reply.Term = currentTerm

	logType := requestTypeLabel(args.IsPreVote)

	if args.Term < termBeforeCheck {
		em.logger.Debugw(fmt.Sprintf("%s rejected: requester term %d lower than local term %d", logType, args.Term, termBeforeCheck),
			"from", args.CandidateID, "nodeID", em.id)
		return reply, nil // VoteGranted remains false
	}

	localLastIndex, localLastTerm := em.logMgr.GetConsistentLastState()

	if !isLogUpToDate(args.LastLogTerm, args.LastLogIndex, localLastTerm, localLastIndex) {
		em.logger.Debugw(fmt.Sprintf("%s rejected: candidate log is not up-to-date", logType),
			"from", args.CandidateID, "reqTerm", args.Term, "nodeID", em.id,
			"candidateLastIdx", args.LastLogIndex, "candidateLastTerm", args.LastLogTerm,
			"localLastIdx", localLastIndex, "localLastTerm", localLastTerm)
		return reply, nil // VoteGranted remains false
	}

	if args.IsPreVote {
		reply.VoteGranted = true
		em.logger.Debugw("Pre-vote granted", "to", args.CandidateID, "preVoteTerm", args.Term, "nodeID", em.id)
		return reply, nil
	}

	persistCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	if em.stateMgr.GrantVote(persistCtx, args.CandidateID, args.Term) {
		reply.VoteGranted = true
		em.logger.Infow("Vote granted", "to", args.CandidateID, "term", args.Term, "nodeID", em.id)
		em.ResetTimerOnHeartbeat()
	} else {
		em.logger.Infow("Vote denied by state manager", "to", args.CandidateID, "term", args.Term, "nodeID", em.id)
	}

	return reply, nil
}

// isLogUpToDate returns true if the candidate's log term is greater than the local term,
// or if terms are equal and the candidate's index is at least as large.
func isLogUpToDate(candidateTerm types.Term, candidateIndex types.Index, localTerm types.Term, localIndex types.Index) bool {
	return candidateTerm > localTerm || (candidateTerm == localTerm && candidateIndex >= localIndex)
}

// Stop signals the election manager to shut down.
func (em *electionManager) Stop() {
	em.logger.Infow("Election manager stopping...", "nodeID", em.id)
}

// resetElectionTimeoutPeriod safely resets the randomized election timeout period
// and returns the new randomized period.
func (em *electionManager) resetElectionTimeoutPeriod() int {
	em.mu.Lock()
	defer em.mu.Unlock()

	return em.resetElectionTimeoutPeriodLocked()
}

// resetElectionTimeoutPeriodLocked recalculates and sets a new randomized election timeout.
// Must be called with em.mu held. Returns the new randomized period.
func (em *electionManager) resetElectionTimeoutPeriodLocked() int {
	em.electionElapsed = 0

	baseTimeout := em.electionTickCount
	maxJitter := int(float64(baseTimeout) * em.randomizationFactor)

	em.randomizedPeriod = baseTimeout

	if maxJitter > 0 {
		em.randomizedPeriod += em.rand.IntN(maxJitter + 1)
	}

	em.logger.Debugw("Election timeout recalculated",
		"randomizedPeriod", em.randomizedPeriod,
		"baseTimeout", baseTimeout,
		"jitterRange", maxJitter)

	return em.randomizedPeriod
}

// resetVotesReceived clears any prior vote tracking and records a self-vote.
// This is used at the start of a new pre-vote or election round.
// It is thread-safe and must be called before broadcasting vote requests.
func (em *electionManager) resetVotesReceived() {
	em.voteMu.Lock()
	defer em.voteMu.Unlock()

	em.votesReceived = map[types.NodeID]bool{
		em.id: true,
	}

	em.logger.Debugw("Vote tracking reset with self-vote", "nodeID", em.id)
}

// requestTypeLabel returns a human-readable label for logging.
func requestTypeLabel(isPreVote bool) string {
	if isPreVote {
		return "pre-vote"
	}
	return "vote"
}

// becomeLeaderIfWon transitions the node to leader if it is still a candidate.
// This is called after vote quorum is confirmed.
func (em *electionManager) becomeLeaderIfWon(ctx context.Context) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Leader transition aborted: node is shutting down")
		return
	}

	term, role, _ := em.stateMgr.GetState()
	if role != types.RoleCandidate {
		em.logger.Infow("Leader transition skipped: node is no longer a candidate",
			"term", term,
			"currentRole", role.String())
		return
	}

	em.logger.Infow("Election won — transitioning to leader",
		"term", term)

	persistCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	if !em.stateMgr.BecomeLeader(persistCtx) {
		em.logger.Errorw("Failed to transition to leader after winning election",
			"term", term)
		return
	}

	em.leaderInitializer.InitializeLeaderState()
	go em.leaderInitializer.SendHeartbeats(context.Background())
}
