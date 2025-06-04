package raft

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

// RaftBuilder facilitates the construction of a Raft with appropriate defaults.
type RaftBuilder struct {
	config  Config
	applier Applier
	logger  logger.Logger
	metrics Metrics
	clock   Clock
	rand    Rand
	storage storage.Storage
}

// NewRaftBuilder creates a new builder for Raft construction.
func NewRaftBuilder() *RaftBuilder {
	return &RaftBuilder{}
}

// WithConfig sets the configuration for the Raft node.
// This is a primary method for providing node ID, peer info, and options.
func (b *RaftBuilder) WithConfig(config Config) *RaftBuilder {
	b.config = config
	return b
}

// WithApplier sets the state machine applier.
func (b *RaftBuilder) WithApplier(applier Applier) *RaftBuilder {
	b.applier = applier
	return b
}

// WithLogger sets the logger for the Raft node.
func (b *RaftBuilder) WithLogger(logger logger.Logger) *RaftBuilder {
	b.logger = logger
	return b
}

// WithMetrics sets the metrics implementation.
func (b *RaftBuilder) WithMetrics(metrics Metrics) *RaftBuilder {
	b.metrics = metrics
	return b
}

// WithClock sets the clock implementation.
func (b *RaftBuilder) WithClock(clock Clock) *RaftBuilder {
	b.clock = clock
	return b
}

// WithRand sets the random number generator.
func (b *RaftBuilder) WithRand(rand Rand) *RaftBuilder {
	b.rand = rand
	return b
}

// WithStorage sets the storage implementation.
func (b *RaftBuilder) WithStorage(storage storage.Storage) *RaftBuilder {
	b.storage = storage
	return b
}

// Build constructs a Raft with the configured values.
// Returns an error if required components are missing or invalid.
func (b *RaftBuilder) Build() (Raft, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	b.setDefaults()

	deps := b.createDependencies()
	if err := deps.Validate(); err != nil {
		return nil, fmt.Errorf("invalid dependencies: %w", err)
	}

	log := b.logger.WithNodeID(b.config.ID).WithComponent("raft")
	mu := &sync.RWMutex{}
	shutdownFlag := &atomic.Bool{}

	applyTimeout := b.config.Options.ApplyEntryTimeout
	if applyTimeout <= 0 {
		applyTimeout = DefaultApplyEntryTimeout
	}
	fetchTimeout := b.config.Options.FetchEntriesTimeout
	if fetchTimeout <= 0 {
		fetchTimeout = DefaultFetchEntriesTimeout
	}

	maxApplyBatchSize := b.config.TuningParams.MaxApplyBatchSize
	if maxApplyBatchSize <= 0 {
		maxApplyBatchSize = DefaultMaxApplyBatchSize
	}

	node := &raftNode{
		id:                  b.config.ID,
		mu:                  mu,
		logger:              log,
		clock:               b.clock,
		applier:             b.applier,
		metrics:             b.metrics,
		stopCh:              make(chan struct{}),
		applyCh:             make(chan types.ApplyMsg, maxApplyBatchSize*2),
		applyNotifyCh:       make(chan struct{}, 1),
		leaderChangeCh:      make(chan types.NodeID, 1), // Buffered to prevent blocking
		applyLoopStopCh:     make(chan struct{}),
		applyLoopDoneCh:     make(chan struct{}),
		applyEntryTimeout:   applyTimeout,
		fetchEntriesTimeout: fetchTimeout,
	}

	stateMgr := NewStateManager(mu, shutdownFlag, deps, b.config.ID, node.leaderChangeCh)
	logMgr := NewLogManager(mu, shutdownFlag, deps, b.config.ID)

	node.stateMgr = stateMgr
	node.logMgr = logMgr

	quorum := calculateQuorumSize(len(b.config.Peers))
	if quorum == 0 && len(b.config.Peers) > 0 {
		log.Warnw("Quorum size is 0 for a multi-node cluster; check configuration", "peerCount", len(b.config.Peers))
	}

	snapshotDeps := SnapshotManagerDeps{
		Mu:                mu,
		ID:                b.config.ID,
		Config:            b.config,
		Storage:           b.storage,
		Applier:           b.applier,
		PeerStateUpdater:  NoOpReplicationStateUpdater{}, // Will be updated later
		StateMgr:          stateMgr,
		LogMgr:            logMgr,
		Metrics:           b.metrics,
		Logger:            log,
		Clock:             b.clock,
		IsShutdown:        shutdownFlag,
		NotifyCommitCheck: func() {}, // Will be updated later
	}
	var err error
	node.snapshotMgr, err = NewSnapshotManager(snapshotDeps)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize snapshot manager: %w", err)
	}

	replicationDeps := ReplicationManagerDeps{
		Mu:             mu,
		ID:             b.config.ID,
		Peers:          b.config.Peers,
		QuorumSize:     quorum,
		Cfg:            b.config,
		StateMgr:       stateMgr,
		LogMgr:         logMgr,
		SnapshotMgr:    node.snapshotMgr,
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
		ID:                b.config.ID,
		Peers:             b.config.Peers,
		QuorumSize:        quorum,
		Mu:                mu,
		IsShutdown:        shutdownFlag,
		StateMgr:          stateMgr,
		LogMgr:            logMgr,
		LeaderInitializer: node.replicationMgr, // replicationMgr implements LeaderInitializer
		Metrics:           b.metrics,
		Logger:            log,
		Rand:              b.rand,
		Config:            b.config,
	}
	node.electionMgr, err = NewElectionManager(electionDeps)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize election manager: %w", err)
	}

	log.Infow("Raft initialized", "id", b.config.ID, "peerCount", len(b.config.Peers), "quorumSize", quorum)
	return node, nil
}

// validate checks that all required components are provided in the builder.
// Returns an error if any essential field is missing.
func (b *RaftBuilder) validate() error {
	if b.config.ID == "" {
		return errors.New("config (including node ID) must be provided using WithConfig()")
	}
	if b.applier == nil {
		return errors.New("applier cannot be nil")
	}
	if b.logger == nil {
		return errors.New("logger cannot be nil (will be defaulted if not set before Build)")
	}
	if b.storage == nil {
		return errors.New("storage cannot be nil")
	}
	return nil
}

// setDefaults assigns default implementations for optional components.
// Logger is also defaulted here if nil.
func (b *RaftBuilder) setDefaults() {
	if b.logger == nil {
		b.logger = logger.NewNoOpLogger()
	}
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
func (b *RaftBuilder) createDependencies() Dependencies {
	return Dependencies{
		Storage: b.storage,
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
