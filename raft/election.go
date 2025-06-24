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

// ElectionState represents the current state of election process
type ElectionState int

const (
	ElectionStateIdle ElectionState = iota
	ElectionStatePreVoting
	ElectionStateVoting
	ElectionStateWon
)

func (s ElectionState) String() string {
	switch s {
	case ElectionStateIdle:
		return "Idle"
	case ElectionStatePreVoting:
		return "PreVoting"
	case ElectionStateVoting:
		return "Voting"
	case ElectionStateWon:
		return "Won"
	default:
		return "Unknown"
	}
}

// VoteRecord tracks votes for a specific term and election type
type VoteRecord struct {
	term       types.Term
	isPreVote  bool
	votes      map[types.NodeID]bool
	quorumSize int
	hasQuorum  bool
	createdAt  time.Time
}

func newVoteRecord(term types.Term, isPreVote bool, quorumSize int, selfID types.NodeID) *VoteRecord {
	votes := make(map[types.NodeID]bool)
	votes[selfID] = true // Self-vote

	return &VoteRecord{
		term:       term,
		isPreVote:  isPreVote,
		votes:      votes,
		quorumSize: quorumSize,
		hasQuorum:  len(votes) >= quorumSize,
		createdAt:  time.Now(),
	}
}

func (vr *VoteRecord) addVote(nodeID types.NodeID) bool {
	if vr.hasQuorum {
		return true // Already achieved quorum
	}

	if _, exists := vr.votes[nodeID]; exists {
		return vr.hasQuorum // Duplicate vote
	}

	vr.votes[nodeID] = true
	vr.hasQuorum = len(vr.votes) >= vr.quorumSize
	return vr.hasQuorum
}

func (vr *VoteRecord) getVoteCount() int {
	return len(vr.votes)
}

// electionManager implements the ElectionManager interface. It handles Raft election logic,
// including managing election timeouts, initiating pre-vote or full election rounds,
// processing vote requests and replies from peers, and coordinating state transitions.
type electionManager struct {
	id            types.NodeID                // ID of the local Raft node.
	peers         map[types.NodeID]PeerConfig // Configuration of peer nodes
	quorumSize    int                         // Number of votes needed to win an election (majority)
	enablePreVote bool                        // Whether to use the PreVote optimization

	isShutdown *atomic.Bool // Shared flag indicating Raft shutdown

	stateMgr          StateManager
	logMgr            LogManager
	networkMgr        NetworkManager
	leaderInitializer LeaderInitializer

	electionTickCount   int     // Number of votes needed to win an election (majority)
	randomizationFactor float64 // Factor (0.0 to 1.0) to add jitter to election timeout
	minElectionTimeout  int
	maxElectionTimeout  int

	electionMu      sync.RWMutex // To protect fields below
	electionState   ElectionState
	currentRecord   *VoteRecord
	electionElapsed int // Ticks elapsed since last heartbeat or election start
	electionTimeout int // Current randomized election timeout period in ticks
	lastHeartbeat   time.Time

	// Atomics for fast access
	lastActivity atomic.Int64 // Unix timestamp

	// Channels for coordinating election process
	stopElectionCh chan struct{}

	// For preventing concurrent elections
	electionInProgress atomic.Bool

	cancelPreviousElection context.CancelFunc

	metrics Metrics
	logger  logger.Logger
	rand    Rand
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

	em := &electionManager{
		id:         deps.ID,
		peers:      deps.Peers,
		quorumSize: deps.QuorumSize,
		isShutdown: deps.IsShutdown,

		stateMgr:          deps.StateMgr,
		logMgr:            deps.LogMgr,
		networkMgr:        deps.NetworkMgr,
		leaderInitializer: deps.LeaderInitializer,
		metrics:           deps.Metrics,
		logger:            deps.Logger.WithComponent("election"),
		rand:              deps.Rand,

		electionTickCount:   getElectionTickCount(deps.Config),
		randomizationFactor: getRandomizationFactor(deps.Config),
		enablePreVote:       getPreVoteEnabled(deps.Config),

		electionState:          ElectionStateIdle,
		stopElectionCh:         make(chan struct{}),
		cancelPreviousElection: nil,
	}

	em.calculateTimeoutBounds()
	em.resetElectionTimeoutLocked()

	return em, nil
}

func (em *electionManager) calculateTimeoutBounds() {
	base := em.electionTickCount
	jitter := int(float64(base) * em.randomizationFactor)

	em.minElectionTimeout = base
	em.maxElectionTimeout = base + jitter*3

	if em.maxElectionTimeout-em.minElectionTimeout < base {
		em.maxElectionTimeout = em.minElectionTimeout + base
	}

	em.logger.Infow("Election timeout bounds calculated",
		"nodeID", em.id,
		"base", base,
		"min", em.minElectionTimeout,
		"max", em.maxElectionTimeout,
		"jitterFactor", em.randomizationFactor)
}

// SetNetworkManager injects the network manager dependency into the election manager.
func (s *electionManager) SetNetworkManager(nm NetworkManager) {
	s.networkMgr = nm
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
	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()

	em.electionMu.Lock()
	em.electionState = ElectionStateIdle
	em.resetElectionTimeoutLocked()
	em.electionMu.Unlock()

	em.lastActivity.Store(time.Now().Unix())

	em.logger.Infow("Election manager initialized",
		"nodeID", em.id,
		"term", term,
		"role", role.String(),
		"lastLogIndex", lastIndex,
		"lastLogTerm", lastTerm,
		"quorumSize", em.quorumSize,
		"preVoteEnabled", em.enablePreVote,
		"electionTimeout", fmt.Sprintf("[%d,%d]", em.minElectionTimeout, em.maxElectionTimeout))

	return nil
}

// Tick advances the election timer. If the timeout is reached for a non-leader,
// it triggers the election process (pre-vote or full election).
func (em *electionManager) Tick(ctx context.Context) {
	if em.isShutdown.Load() || ctx.Err() != nil {
		return
	}

	_, role, _ := em.stateMgr.GetState()
	if role == types.RoleLeader {
		return // Leaders don't need election timeouts
	}

	em.electionMu.Lock()
	em.electionElapsed++
	elapsed := em.electionElapsed
	timeout := em.electionTimeout
	state := em.electionState
	em.electionMu.Unlock()

	if elapsed >= timeout {
		em.handleElectionTimeout(ctx, role, state)
	}
}

// handleElectionTimeout determines whether to start a pre-vote or a full election
// based on configuration and current state. Called asynchronously after a timeout.
func (em *electionManager) handleElectionTimeout(ctx context.Context, role types.NodeRole, currentState ElectionState) {
	// Prevent concurrent election attempts
	if !em.electionInProgress.CompareAndSwap(false, true) {
		em.logger.Debugw("Election timeout ignored: election already in progress", "nodeID", em.id)
		return
	}

	if em.isShutdown.Load() {
		em.electionInProgress.Store(false)
		return
	}

	// Double-check role hasn't changed
	_, role, _ = em.stateMgr.GetState()
	if role == types.RoleLeader {
		em.logger.Debugw("Election timeout ignored: became leader", "nodeID", em.id)
		em.electionInProgress.Store(false)
		return
	}

	em.logger.Infow("Election timeout detected",
		"nodeID", em.id,
		"elapsed", em.electionElapsed,
		"timeout", em.electionTimeout,
		"state", currentState.String())

	em.resetElectionTimeout()

	if em.enablePreVote && currentState == ElectionStateIdle {
		go func() {
			defer em.electionInProgress.Store(false)
			em.startPreVotePhase(ctx)
		}()
	} else {
		go func() {
			defer em.electionInProgress.Store(false)
			em.startElection(ctx, false)
		}()
	}
}

// startPreVotePhase initiates the pre-vote phase. It sends pre-vote requests to peers
// without incrementing the term, aiming to gauge likelihood of winning a real election.
func (em *electionManager) startPreVotePhase(ctx context.Context) {
	if em.isShutdown.Load() {
		em.logger.Debugw("Pre-vote aborted: node is shutting down", "nodeID", em.id)
		return
	}

	term, role, _ := em.stateMgr.GetState()
	if role == types.RoleLeader {
		em.logger.Debugw("Pre-vote skipped: node is leader", "nodeID", em.id)
		return
	}

	preVoteTerm := term + 1
	lastIndex, lastTerm := em.logMgr.GetConsistentLastState()

	em.electionMu.Lock()
	if em.electionState != ElectionStateIdle {
		em.electionMu.Unlock()
		em.logger.Debugw("Pre-vote skipped: not in idle state",
			"nodeID", em.id, "state", em.electionState.String())
		return
	}

	if em.cancelPreviousElection != nil {
		em.cancelPreviousElection()
		em.cancelPreviousElection = nil
	}

	em.electionState = ElectionStatePreVoting
	em.currentRecord = newVoteRecord(preVoteTerm, true, em.quorumSize, em.id)
	em.electionMu.Unlock()

	em.logger.Infow("Starting pre-vote phase",
		"nodeID", em.id,
		"currentTerm", term,
		"preVoteTerm", preVoteTerm,
		"lastLogIndex", lastIndex,
		"lastLogTerm", lastTerm)

	args := &types.RequestVoteArgs{
		Term:         preVoteTerm,
		CandidateID:  em.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		IsPreVote:    true,
	}

	em.broadcastVoteRequests(ctx, args, true)
}

// startElection initiates a full election. It transitions the node to Candidate,
// increments the term, votes for itself, persists state, and sends RequestVote RPCs.
func (em *electionManager) startElection(ctx context.Context, isPreVote bool) {
	if em.isShutdown.Load() || ctx.Err() != nil {
		return
	}

	currentTerm, currentRole, _ := em.stateMgr.GetState()

	// Determine target term
	targetTerm := currentTerm + 1
	if isPreVote {
		// Pre-vote uses hypothetical next term but doesn't increment local term
		targetTerm = currentTerm + 1
	}

	em.electionMu.Lock()

	// Cancel any previous election
	if em.cancelPreviousElection != nil {
		em.cancelPreviousElection()
	}

	// Set up new election context
	electionCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	em.cancelPreviousElection = cancel

	// Update election state
	if isPreVote {
		em.electionState = ElectionStatePreVoting
	} else {
		em.electionState = ElectionStateVoting
	}

	// Create vote record
	em.currentRecord = newVoteRecord(targetTerm, isPreVote, em.quorumSize, em.id)

	electionType := "election"
	if isPreVote {
		electionType = "pre-vote"
	}

	em.logger.Infow("Starting "+electionType,
		"nodeID", em.id, "currentTerm", currentTerm, "targetTerm", targetTerm,
		"currentRole", currentRole.String())

	em.electionMu.Unlock()

	// For real elections, become candidate first
	if !isPreVote {
		if !em.stateMgr.BecomeCandidateForTerm(electionCtx, targetTerm) {
			em.logger.Warnw("Failed to become candidate", "nodeID", em.id, "term", targetTerm)
			em.electionMu.Lock()
			em.resetElectionState()
			em.electionMu.Unlock()
			cancel()
			return
		}
	}

	// Send vote requests to all peers
	em.sendVoteRequests(electionCtx, targetTerm, isPreVote)
}

// Helper method to reset election state
func (em *electionManager) resetElectionState() {
	em.electionState = ElectionStateIdle
	em.currentRecord = nil
	if em.cancelPreviousElection != nil {
		em.cancelPreviousElection()
		em.cancelPreviousElection = nil
	}
}

// Helper method to send vote requests
func (em *electionManager) sendVoteRequests(ctx context.Context, term types.Term, isPreVote bool) {
	lastLogIndex, lastLogTerm := em.logMgr.GetConsistentLastState()

	// Create vote request
	voteReq := &types.RequestVoteArgs{
		Term:         term,
		CandidateID:  em.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		IsPreVote:    isPreVote,
	}

	// Send to all peers concurrently
	for peerID, _ := range em.peers {
		if peerID == em.id {
			continue
		}

		go func(peer types.NodeID) {
			requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			reply, err := em.networkMgr.SendRequestVote(requestCtx, peer, voteReq)
			if err != nil {
				em.logger.Debugw("Vote request failed",
					"nodeID", em.id, "peer", peer, "term", term,
					"isPreVote", isPreVote, "error", err)
				return
			}

			em.processVoteReply(requestCtx, peer, reply, isPreVote)
		}(peerID)
	}
}

// broadcastVoteRequests sends RequestVote or PreVote RPCs to all configured peers in parallel.
func (em *electionManager) broadcastVoteRequests(
	ctx context.Context,
	args *types.RequestVoteArgs,
	isPreVote bool,
) {
	requestType := "vote"
	if isPreVote {
		requestType = "pre-vote"
	}

	em.logger.Debugw("Broadcasting vote requests",
		"nodeID", em.id,
		"type", requestType,
		"term", args.Term,
		"peerCount", len(em.peers)-1)

	broadcastCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	em.electionMu.Lock()
	if em.cancelPreviousElection != nil {
		em.cancelPreviousElection()
	}
	em.cancelPreviousElection = cancel
	em.electionMu.Unlock()

	var wg sync.WaitGroup
	for peerID := range em.peers {
		if peerID == em.id {
			continue
		}

		wg.Add(1)
		go func(targetPeer types.NodeID) {
			defer wg.Done()
			em.sendVoteRequest(broadcastCtx, targetPeer, args)
		}(peerID)
	}

	// Don't use defer cancel() here - let it be cancelled by next election or timeout
	go func() {
		wg.Wait()
		em.logger.Debugw("All vote requests completed",
			"nodeID", em.id,
			"type", requestType,
			"term", args.Term)
	}()

	go func() {
		wg.Wait()
		em.logger.Debugw("All vote requests completed",
			"nodeID", em.id,
			"type", requestType,
			"term", args.Term)
	}()
}

// sendVoteRequest sends a single RequestVote RPC to a peer and handles the response.
func (em *electionManager) sendVoteRequest(
	ctx context.Context,
	targetPeer types.NodeID,
	args *types.RequestVoteArgs,
) {
	requestType := "vote"
	if args.IsPreVote {
		requestType = "pre-vote"
	}

	select {
	case <-ctx.Done():
		em.logger.Debugw("Vote request cancelled before RPC",
			"nodeID", em.id,
			"to", targetPeer,
			"type", requestType,
			"error", ctx.Err())
		return
	default:
		// Continue
	}

	rpcCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	em.logger.Debugw("Sending vote request",
		"nodeID", em.id,
		"to", targetPeer,
		"type", requestType,
		"term", args.Term)

	reply, err := em.networkMgr.SendRequestVote(rpcCtx, targetPeer, args)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			em.logger.Debugw("Vote request cancelled during RPC",
				"nodeID", em.id,
				"to", targetPeer,
				"type", requestType)
		} else if !errors.Is(err, context.DeadlineExceeded) {
			em.logger.Warnw("Vote request failed",
				"nodeID", em.id,
				"to", targetPeer,
				"type", requestType,
				"error", err)
		}
		return
	}

	// Check for cancellation before processing reply
	select {
	case <-ctx.Done():
		em.logger.Debugw("Vote request cancelled before processing reply",
			"nodeID", em.id,
			"to", targetPeer,
			"type", requestType)
		return
	default:
		// Continue with processing
	}

	if args.IsPreVote {
		em.processPreVoteReply(ctx, targetPeer, args.Term, reply)
	} else {
		em.processVoteReply(ctx, targetPeer, reply, false)
	}
}

// processPreVoteReply handles the response from a pre-vote request.
func (em *electionManager) processPreVoteReply(
	ctx context.Context, fromPeer types.NodeID, preVoteTerm types.Term, reply *types.RequestVoteReply,
) {
	if em.isShutdown.Load() {
		return
	}

	em.logger.Debugw("Pre-vote reply received",
		"nodeID", em.id,
		"from", fromPeer,
		"preVoteTerm", preVoteTerm,
		"granted", reply.VoteGranted,
		"replyTerm", reply.Term)

	// Check for higher term
	if reply.Term > preVoteTerm {
		em.stateMgr.CheckTermAndStepDown(ctx, reply.Term, fromPeer)
		return
	}

	if !reply.VoteGranted {
		return
	}

	em.electionMu.Lock()
	defer em.electionMu.Unlock()

	// Validate this is still the active pre-vote
	if em.electionState != ElectionStatePreVoting ||
		em.currentRecord == nil ||
		em.currentRecord.term != preVoteTerm ||
		!em.currentRecord.isPreVote {
		em.logger.Debugw("Ignoring stale pre-vote reply",
			"nodeID", em.id,
			"from", fromPeer,
			"state", em.electionState.String())
		return
	}

	quorumAchieved := em.currentRecord.addVote(fromPeer)

	em.logger.Debugw("Pre-vote recorded",
		"nodeID", em.id,
		"from", fromPeer,
		"votes", em.currentRecord.getVoteCount(),
		"quorumSize", em.quorumSize,
		"quorumAchieved", quorumAchieved)

	if quorumAchieved {
		em.logger.Infow("Pre-vote quorum achieved",
			"nodeID", em.id,
			"preVoteTerm", preVoteTerm,
			"votes", em.currentRecord.getVoteCount(),
			"quorumSize", em.quorumSize)

		// Reset to idle state and start real election
		em.electionState = ElectionStateIdle
		em.currentRecord = nil

		// Start the real election for this term
		go em.startElection(context.Background(), true)
	}
}

// processVoteReply handles the response from a regular RequestVote RPC.
func (em *electionManager) processVoteReply(
	ctx context.Context,
	from types.NodeID,
	reply *types.RequestVoteReply,
	isPreVote bool,
) {
	if em.isShutdown.Load() || ctx.Err() != nil {
		return
	}

	em.electionMu.Lock()
	defer em.electionMu.Unlock()

	// Verify we're still in the correct election state
	expectedState := ElectionStateVoting
	if isPreVote {
		expectedState = ElectionStatePreVoting
	}

	if em.electionState != expectedState || em.currentRecord == nil {
		em.logger.Debugw("Vote reply received but not in expected state",
			"nodeID", em.id, "from", from, "isPreVote", isPreVote,
			"currentState", em.electionState.String(), "expectedState", expectedState.String())
		return
	}

	// Check if this reply is for the current election term
	if em.currentRecord.term != reply.Term {
		em.logger.Debugw("Vote reply for wrong term",
			"nodeID", em.id, "from", from, "replyTerm", reply.Term,
			"expectedTerm", em.currentRecord.term)
		return
	}

	// Handle higher term in reply - step down
	currentTerm, _, _ := em.stateMgr.GetState()
	if reply.Term > currentTerm {
		em.logger.Infow("Stepped down due to higher term in vote reply",
			"nodeID", em.id, "from", from, "replyTerm", reply.Term)

		em.stateMgr.CheckTermAndStepDown(ctx, reply.Term, unknownNodeID)
		em.resetElectionState()
		return
	}

	// Process the vote
	if reply.VoteGranted {
		achievedQuorum := em.currentRecord.addVote(from)

		em.logger.Debugw("Vote granted",
			"nodeID", em.id, "from", from, "term", reply.Term,
			"votes", em.currentRecord.getVoteCount(), "quorumSize", em.quorumSize,
			"quorum", achievedQuorum, "isPreVote", isPreVote)

		if achievedQuorum {
			if isPreVote {
				// Pre-vote quorum achieved, start real election
				em.logger.Infow("Pre-vote quorum achieved",
					"nodeID", em.id, "preVoteTerm", em.currentRecord.term,
					"votes", em.currentRecord.getVoteCount(), "quorumSize", em.quorumSize)

				// Start real election immediately
				go func() {
					electionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					em.startElection(electionCtx, false) // real election
				}()
			} else {
				// Real election won - become leader immediately
				em.electionState = ElectionStateWon

				em.logger.Infow("Election won!",
					"nodeID", em.id, "term", em.currentRecord.term,
					"votes", em.currentRecord.getVoteCount(), "quorumSize", em.quorumSize)

				// Complete election atomically in a separate goroutine to avoid deadlock
				go func() {
					term := em.currentRecord.term
					if em.completeElectionAtomically(ctx, term) {
						em.metrics.ObserveElectionElapsed(em.id, em.currentRecord.term, em.electionElapsed)
					}
				}()
			}
		}
	} else {
		em.logger.Debugw("Vote denied",
			"nodeID", em.id, "from", from, "term", reply.Term, "isPreVote", isPreVote)
	}
}

func (em *electionManager) becomeLeader(ctx context.Context) {
	if em.isShutdown.Load() {
		return
	}

	term, role, _ := em.stateMgr.GetState()
	if role != types.RoleCandidate {
		em.logger.Warnw("Cannot become leader: not in candidate state",
			"nodeID", em.id,
			"role", role.String(),
			"term", term)
		return
	}

	persistCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	if !em.stateMgr.BecomeLeader(persistCtx) {
		em.logger.Errorw("Failed to transition to leader",
			"nodeID", em.id,
			"term", term)
		return
	}

	em.logger.Infow("Successfully became leader",
		"nodeID", em.id,
		"term", term)

	em.resetElectionState()

	em.leaderInitializer.InitializeLeaderState()
	go em.leaderInitializer.SendHeartbeats(context.Background())
}

// ResetTimerOnHeartbeat resets the election timer in response to a valid heartbeat
// or AppendEntries RPC from the current leader. This prevents unnecessary elections.
func (em *electionManager) ResetTimerOnHeartbeat() {
	if em.isShutdown.Load() {
		return
	}

	em.lastActivity.Store(time.Now().Unix())

	em.electionMu.Lock()
	defer em.electionMu.Unlock()

	em.resetElectionTimeoutLocked()
	em.lastHeartbeat = time.Now()

	if em.cancelPreviousElection != nil {
		em.logger.Debugw("Cancelling election due to heartbeat", "nodeID", em.id)
		em.cancelPreviousElection()
		em.cancelPreviousElection = nil
	}

	// Reset election state completely
	if em.electionState != ElectionStateIdle {
		em.logger.Debugw("Resetting election state due to heartbeat",
			"nodeID", em.id, "previousState", em.electionState.String())
		em.resetElectionState()
	}

	em.logger.Debugw("Election timer reset due to heartbeat", "nodeID", em.id)
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (em *electionManager) HandleRequestVote(
	ctx context.Context,
	args *types.RequestVoteArgs,
) (*types.RequestVoteReply, error) {
	if em.isShutdown.Load() {
		term, _, _ := em.stateMgr.GetState()
		return &types.RequestVoteReply{Term: term, VoteGranted: false}, ErrShuttingDown
	}

	if ctx.Err() != nil {
		term, _, _ := em.stateMgr.GetState()
		return &types.RequestVoteReply{Term: term, VoteGranted: false}, ctx.Err()
	}

	requestType := "vote"
	if args.IsPreVote {
		requestType = "pre-vote"
	}

	em.logger.Debugw("Vote request received",
		"nodeID", em.id,
		"from", args.CandidateID,
		"type", requestType,
		"term", args.Term,
		"lastLogIndex", args.LastLogIndex,
		"lastLogTerm", args.LastLogTerm)

	// Get current state BEFORE any term checks for pre-votes
	currentTerm, currentRole, _ := em.stateMgr.GetState()

	// Handle pre-vote requests differently - they should NOT cause stepping down
	if args.IsPreVote {
		reply := &types.RequestVoteReply{Term: currentTerm, VoteGranted: false}

		// Pre-vote basic checks (don't update term)
		if args.Term <= currentTerm {
			em.logger.Debugw("Pre-vote rejected: term not higher than current",
				"nodeID", em.id,
				"from", args.CandidateID,
				"preVoteTerm", args.Term,
				"currentTerm", currentTerm)
			return reply, nil
		}

		// Check log up-to-date requirement for pre-vote
		localLastIndex, localLastTerm := em.logMgr.GetConsistentLastState()
		if !isLogUpToDate(args.LastLogTerm, args.LastLogIndex, localLastTerm, localLastIndex) {
			em.logger.Debugw("Pre-vote rejected: candidate log not up-to-date",
				"nodeID", em.id,
				"from", args.CandidateID,
				"candidateLastTerm", args.LastLogTerm,
				"candidateLastIndex", args.LastLogIndex,
				"localLastTerm", localLastTerm,
				"localLastIndex", localLastIndex)
			return reply, nil
		}

		// Grant pre-vote if checks pass (no term update, no persistence)
		reply.VoteGranted = true
		em.logger.Debugw("Pre-vote granted",
			"nodeID", em.id,
			"to", args.CandidateID,
			"preVoteTerm", args.Term)

		em.ResetTimerOnHeartbeat()

		return reply, nil
	}

	reply := &types.RequestVoteReply{Term: currentTerm, VoteGranted: false}

	// Step 1: Check if request term is too low (reject immediately)
	if args.Term < currentTerm {
		em.logger.Debugw("Vote request rejected: term too low",
			"nodeID", em.id,
			"from", args.CandidateID,
			"requestTerm", args.Term,
			"currentTerm", currentTerm)
		return reply, nil
	}

	// Step 2: Handle higher term - step down and update state
	if args.Term > currentTerm {
		// Step down to the higher term
		steppedDown, _ := em.stateMgr.CheckTermAndStepDown(ctx, args.Term, args.CandidateID)
		if steppedDown {
			em.logger.Debugw("Stepped down to higher term",
				"nodeID", em.id,
				"from", args.CandidateID,
				"oldTerm", currentTerm,
				"newTerm", args.Term)

			// Update our reply term and current state
			currentTerm = args.Term
			reply.Term = currentTerm
			// After stepping down, we're now a follower in the new term
		}
	}

	// Step 3: Handle same term case - DO NOT automatically step down if we're a candidate
	if args.Term == currentTerm {
		// For same-term requests, candidates should NOT step down
		// Only check if this is from a leader (AppendEntries would have LeaderID set appropriately)
		// Vote requests from other candidates should not cause stepping down

		em.logger.Debugw("Same term vote request received",
			"nodeID", em.id,
			"from", args.CandidateID,
			"currentRole", currentRole.String(),
			"term", currentTerm)

		// Don't call CheckTermAndStepDown for same-term vote requests from candidates
		// This allows candidates to coexist and compete properly
	}

	// Step 4: Check log up-to-date requirement
	localLastIndex, localLastTerm := em.logMgr.GetConsistentLastState()
	if !isLogUpToDate(args.LastLogTerm, args.LastLogIndex, localLastTerm, localLastIndex) {
		em.logger.Debugw("Vote request rejected: candidate log not up-to-date",
			"nodeID", em.id,
			"from", args.CandidateID,
			"candidateLastTerm", args.LastLogTerm,
			"candidateLastIndex", args.LastLogIndex,
			"localLastTerm", localLastTerm,
			"localLastIndex", localLastIndex)
		return reply, nil
	}

	// Step 5: Try to grant the vote through state manager
	persistCtx, cancel := context.WithTimeout(ctx, electionManagerOpTimeout)
	defer cancel()

	if em.stateMgr.GrantVote(persistCtx, args.CandidateID, args.Term) {
		reply.VoteGranted = true
		em.ResetTimerOnHeartbeat() // Reset election timer when granting vote

		em.logger.Infow("Vote granted",
			"nodeID", em.id,
			"to", args.CandidateID,
			"term", args.Term)
	} else {
		em.logger.Debugw("Vote denied by state manager",
			"nodeID", em.id,
			"to", args.CandidateID,
			"term", args.Term)
	}

	return reply, nil
}

func (em *electionManager) resetElectionTimeout() {
	em.electionMu.Lock()
	defer em.electionMu.Unlock()
	em.resetElectionTimeoutLocked()
}

func (em *electionManager) resetElectionTimeoutLocked() {
	em.electionElapsed = 0

	// Use improved randomization with better spread
	jitterRange := em.maxElectionTimeout - em.minElectionTimeout
	if jitterRange <= 0 {
		em.electionTimeout = em.minElectionTimeout
	} else {
		em.electionTimeout = em.minElectionTimeout + em.rand.IntN(jitterRange)
	}

	em.logger.Debugw("Election timeout reset",
		"nodeID", em.id,
		"newTimeout", em.electionTimeout,
		"range", fmt.Sprintf("[%d,%d]", em.minElectionTimeout, em.maxElectionTimeout))
}

// Stop signals the election manager to shut down.
func (em *electionManager) Stop() {
	em.logger.Infow("Improved election manager stopping", "nodeID", em.id)

	em.electionMu.Lock()
	if em.cancelPreviousElection != nil {
		em.logger.Debugw("Cancelling election during shutdown", "nodeID", em.id)
		em.cancelPreviousElection()
		em.cancelPreviousElection = nil
	}
	em.electionMu.Unlock()

	close(em.stopElectionCh)
	em.resetElectionState()
}

func getElectionTickCount(config Config) int {
	if config.Options.ElectionTickCount > 0 {
		return config.Options.ElectionTickCount
	}
	return DefaultElectionTickCount
}

func getRandomizationFactor(config Config) float64 {
	if config.Options.ElectionRandomizationFactor >= 0.0 && config.Options.ElectionRandomizationFactor <= 1.0 {
		return config.Options.ElectionRandomizationFactor
	}
	return DefaultElectionRandomizationFactor
}

func getPreVoteEnabled(config Config) bool {
	return config.FeatureFlags.PreVoteEnabled
}

// isLogUpToDate returns true if the candidate's log term is greater than the local term,
// or if terms are equal and the candidate's index is at least as large.
func isLogUpToDate(
	candidateTerm types.Term,
	candidateIndex types.Index,
	localTerm types.Term,
	localIndex types.Index,
) bool {
	return candidateTerm > localTerm || (candidateTerm == localTerm && candidateIndex >= localIndex)
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

func (em *electionManager) completeElectionAtomically(ctx context.Context, term types.Term) bool {
	em.electionMu.Lock()
	defer em.electionMu.Unlock()

	// Double-check we're still in the winning state
	if em.electionState != ElectionStateWon || em.currentRecord == nil {
		em.logger.Warnw("Election completion called but not in winning state",
			"nodeID", em.id, "state", em.electionState.String())
		return false
	}

	// Verify the term hasn't changed
	currentTerm, currentRole, _ := em.stateMgr.GetState()
	if currentTerm != term || currentRole != types.RoleCandidate {
		em.logger.Warnw("Election completion aborted - state changed",
			"nodeID", em.id, "expectedTerm", term, "currentTerm", currentTerm,
			"currentRole", currentRole.String())
		return false
	}

	em.logger.Infow("Successfully became leader", "nodeID", em.id, "term", term)

	// ATOMIC STEP 1: Become leader in state manager
	if !em.stateMgr.BecomeLeader(ctx) {
		em.logger.Errorw("Failed to become leader in state manager",
			"nodeID", em.id, "term", term)
		return false
	}

	// ATOMIC STEP 2: Initialize leader state immediately
	em.leaderInitializer.InitializeLeaderState()

	// ATOMIC STEP 3: Send immediate heartbeats to assert authority
	// This prevents other nodes from starting elections
	go func() {
		assertCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		em.logger.Infow("Sending immediate leadership assertion heartbeats",
			"nodeID", em.id, "term", term)

		// Send heartbeats immediately to assert leadership
		em.leaderInitializer.SendHeartbeats(assertCtx)
	}()

	// Reset election state
	em.electionState = ElectionStateIdle
	em.currentRecord = nil
	if em.cancelPreviousElection != nil {
		em.cancelPreviousElection()
		em.cancelPreviousElection = nil
	}

	return true
}
