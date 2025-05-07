package raft

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

// RaftNodeBuilder facilitates the construction of a RaftNode with appropriate defaults.
type RaftNodeBuilder struct {
	id                  types.NodeID
	config              Config
	peers               map[types.NodeID]PeerConfig
	applier             Applier
	networkMgr          NetworkManager
	logger              logger.Logger
	metrics             Metrics
	clock               Clock
	rand                Rand
	storage             storage.Storage
	applyEntryTimeout   time.Duration
	fetchEntriesTimeout time.Duration
}

// NewRaftNodeBuilder creates a new builder for RaftNode construction.
func NewRaftNodeBuilder(id types.NodeID) *RaftNodeBuilder {
	return &RaftNodeBuilder{
		id:                  id,
		peers:               make(map[types.NodeID]PeerConfig),
		applyEntryTimeout:   defaultApplyEntryTimeout,
		fetchEntriesTimeout: defaultFetchEntriesTimeout,
	}
}

// WithConfig sets the configuration for the Raft node.
func (b *RaftNodeBuilder) WithConfig(config Config) *RaftNodeBuilder {
	b.config = config
	return b
}

// WithPeers sets the peer configuration map.
func (b *RaftNodeBuilder) WithPeers(peers map[types.NodeID]PeerConfig) *RaftNodeBuilder {
	b.peers = peers
	return b
}

// WithApplier sets the state machine applier.
func (b *RaftNodeBuilder) WithApplier(applier Applier) *RaftNodeBuilder {
	b.applier = applier
	return b
}

// WithNetworkManager sets the network manager for peer communication.
func (b *RaftNodeBuilder) WithNetworkManager(networkMgr NetworkManager) *RaftNodeBuilder {
	b.networkMgr = networkMgr
	return b
}

// WithLogger sets the logger for the Raft node.
func (b *RaftNodeBuilder) WithLogger(logger logger.Logger) *RaftNodeBuilder {
	b.logger = logger
	return b
}

// WithMetrics sets the metrics implementation.
func (b *RaftNodeBuilder) WithMetrics(metrics Metrics) *RaftNodeBuilder {
	b.metrics = metrics
	return b
}

// WithClock sets the clock implementation.
func (b *RaftNodeBuilder) WithClock(clock Clock) *RaftNodeBuilder {
	b.clock = clock
	return b
}

// WithRand sets the random number generator.
func (b *RaftNodeBuilder) WithRand(rand Rand) *RaftNodeBuilder {
	b.rand = rand
	return b
}

// WithStorage sets the storage implementation.
func (b *RaftNodeBuilder) WithStorage(storage storage.Storage) *RaftNodeBuilder {
	b.storage = storage
	return b
}

// WithApplyEntryTimeout sets the timeout for applying a single entry.
func (b *RaftNodeBuilder) WithApplyEntryTimeout(timeout time.Duration) *RaftNodeBuilder {
	b.applyEntryTimeout = timeout
	return b
}

// WithFetchEntriesTimeout sets the timeout for fetching log entries.
func (b *RaftNodeBuilder) WithFetchEntriesTimeout(timeout time.Duration) *RaftNodeBuilder {
	b.fetchEntriesTimeout = timeout
	return b
}

// Build constructs a RaftNode with the configured values.
// Returns an error if required components are missing or invalid.
func (b *RaftNodeBuilder) Build() (Raft, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	b.setDefaults()

	deps := b.createDependencies()
	if err := deps.Validate(); err != nil {
		return nil, fmt.Errorf("invalid dependencies: %w", err)
	}

	log := b.logger.WithNodeID(b.id).WithComponent("raft")
	mu := &sync.RWMutex{}
	shutdownFlag := &atomic.Bool{}

	node := &raftNode{
		id:                  b.id,
		mu:                  mu,
		logger:              log,
		clock:               b.clock,
		applier:             b.applier,
		networkMgr:          b.networkMgr,
		metrics:             b.metrics,
		stopCh:              make(chan struct{}),
		applyCh:             make(chan types.ApplyMsg, b.config.TuningParams.MaxApplyBatchSize*2),
		applyNotifyCh:       make(chan struct{}, 1),
		leaderChangeCh:      make(chan types.NodeID, 1),
		applyLoopStopCh:     make(chan struct{}),
		applyLoopDoneCh:     make(chan struct{}),
		applyEntryTimeout:   b.config.Options.ApplyEntryTimeout,
		fetchEntriesTimeout: b.config.Options.FetchEntriesTimeout,
	}

	stateMgr := NewStateManager(mu, shutdownFlag, deps, b.id, node.leaderChangeCh)
	logMgr := NewLogManager(mu, shutdownFlag, deps, b.id)

	node.stateMgr = stateMgr
	node.logMgr = logMgr

	quorum := calculateQuorumSize(len(b.peers))
	if quorum == 0 && len(b.peers) > 0 {
		log.Warnw("Quorum size is 0 for a multi-node cluster; check configuration", "peerCount", len(b.peers))
	}

	snapshotDeps := SnapshotManagerDeps{
		Mu:                mu,
		ID:                b.id,
		Config:            b.config,
		Storage:           b.storage,
		Applier:           b.applier,
		PeerStateUpdater:  NoOpReplicationStateUpdater{},
		StateMgr:          stateMgr,
		LogMgr:            logMgr,
		NetworkMgr:        b.networkMgr,
		Metrics:           b.metrics,
		Logger:            log,
		Clock:             b.clock,
		IsShutdown:        shutdownFlag,
		NotifyCommitCheck: func() {},
	}
	var err error
	node.snapshotMgr, err = NewSnapshotManager(snapshotDeps)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize snapshot manager: %w", err)
	}

	replicationDeps := ReplicationManagerDeps{
		Mu:             mu,
		ID:             b.id,
		Peers:          b.peers,
		QuorumSize:     quorum,
		Cfg:            b.config,
		StateMgr:       stateMgr,
		LogMgr:         logMgr,
		SnapshotMgr:    node.snapshotMgr,
		NetworkMgr:     b.networkMgr,
		Metrics:        b.metrics,
		Logger:         log,
		Clock:          b.clock,
		IsShutdownFlag: shutdownFlag,
		ApplyNotifyCh:  node.applyNotifyCh,
	}
	node.replicationMgr, err = NewReplicationManager(replicationDeps)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize replication manager: %w", err)
	}
	node.snapshotMgr.SetReplicationStateUpdater(node.replicationMgr)

	electionDeps := ElectionManagerDeps{
		ID:                b.id,
		Peers:             b.peers,
		QuorumSize:        quorum,
		Mu:                mu,
		IsShutdown:        shutdownFlag,
		StateMgr:          stateMgr,
		LogMgr:            logMgr,
		NetworkMgr:        b.networkMgr,
		LeaderInitializer: node.replicationMgr,
		Metrics:           b.metrics,
		Logger:            log,
		Rand:              b.rand,
		Config:            b.config,
	}
	node.electionMgr, err = NewElectionManager(electionDeps)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize election manager: %w", err)
	}

	log.Infow("RaftNode initialized", "id", b.id, "peerCount", len(b.peers), "quorumSize", quorum)
	return node, nil
}

// validate checks that all required components are provided in the builder.
// Returns an error if any essential field is missing.
func (b *RaftNodeBuilder) validate() error {
	if b.id == "" {
		return errors.New("node ID cannot be empty")
	}
	if b.applier == nil {
		return errors.New("applier cannot be nil")
	}
	if b.networkMgr == nil {
		return errors.New("network manager cannot be nil")
	}
	if b.logger == nil {
		return errors.New("logger cannot be nil")
	}
	return nil
}

// setDefaults assigns default implementations for optional components.
func (b *RaftNodeBuilder) setDefaults() {
	if b.metrics == nil {
		b.metrics = NewNoOpMetrics()
		b.logger.Infow("Using no-op metrics implementation")
	}
	if b.clock == nil {
		b.clock = NewStandardClock()
		b.logger.Infow("Using default clock implementation")
	}
	if b.rand == nil {
		b.rand = NewStandardRand()
		b.logger.Infow("Using default random number generator")
	}
}

// createDependencies constructs a Dependencies struct using the values from the builder.
func (b *RaftNodeBuilder) createDependencies() Dependencies {
	return Dependencies{
		Storage: b.storage,
		Network: b.networkMgr,
		Applier: b.applier,
		Logger:  b.logger,
		Metrics: b.metrics,
		Clock:   b.clock,
		Rand:    b.rand,
	}
}

// calculateQuorumSize returns the minimum number of nodes for majority consensus.
func calculateQuorumSize(clusterSize int) int {
	if clusterSize <= 0 {
		return 0
	}
	return (clusterSize / 2) + 1
}
