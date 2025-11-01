package xconsensus

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/compose-network/specs/compose"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// coordinator implements the Coordinator interface
type coordinator struct {
	config      Config
	callbackMgr *CallbackManager
	metrics     MetricsRecorder
	log         zerolog.Logger

	// Track committed xTs already sent with a block to avoid duplicates
	sentMu  sync.Mutex
	sentMap map[string]bool

	// Lifecycle management
	started      atomic.Bool
	stopped      atomic.Bool
	stopCh       chan struct{}
	wg           sync.WaitGroup
	shutdownOnce sync.Once
}

func (c *coordinator) RecordDecision(xtID compose.InstanceID, decision bool) error {
	//c.callbackMgr.InvokeVote()
	return nil
}

func (c *coordinator) RecordMailboxMessage(circMessage *sbcpproto.MailboxMessage) error {
	//TODO implement me
	panic("implement me")
}

// NewConsensusCoord creates a new coordinator instanceproto
func NewConsensusCoord(log zerolog.Logger, config Config) Coordinator {
	return newWithMetrics(log, config, NewMetrics())
}

// newWithMetrics creates a new coordinator instanceproto with custom metrics recorder
// TODO: check best practices for metrics recorder
func newWithMetrics(log zerolog.Logger, config Config, metrics MetricsRecorder) Coordinator {
	logger := log.With().
		Str("component", "consensus-coordinator").
		Str("node_id", config.NodeID).
		Logger()

	return &coordinator{
		config:      config,
		callbackMgr: NewCallbackManager(30*time.Second, logger),
		metrics:     metrics,
		log:         logger,
		sentMap:     make(map[string]bool),
	}
}

// StartInstance initiates a new 2PC transaction
func (c *coordinator) StartInstance(ctx context.Context, from string, instance *sbcpproto.StartInstance) error {
	xtRequest := instance.GetXtRequest()
	chains := getChainIDs(xtRequest)

	if len(chains) == 0 {
		return fmt.Errorf("no participating chains found")
	}

	// Timeout only for leader; followers rely on the SP decision
	c.metrics.RecordTransactionStarted(len(chains))

	c.log.Info().
		Str("instance_id", hex.EncodeToString(instance.InstanceId)).
		Int("participating_chains", len(chains)).
		Dur("timeout", c.config.Timeout).
		Msg("Started 2PC instanceproto")

	// Invoke start callback
	c.callbackMgr.InvokeStart(ctx, from, instance)

	return nil
}

func getChainIDs(request *sbcpproto.XTRequest) []uint64 {
	chainIDs := make([]uint64, 0)
	for _, tx := range request.TransactionRequests {
		chainIDs = append(chainIDs, tx.ChainId)
	}
	return chainIDs
}

// SetStartCallback sets the start callback
func (c *coordinator) SetStartCallback(fn StartFn) {
	c.callbackMgr.SetStartCallback(fn)
}

// SetVoteCallback sets the vote callback
func (c *coordinator) SetVoteCallback(fn VoteFn) {
	c.callbackMgr.SetVoteCallback(fn)
}

func (c *coordinator) SetMailboxMsgCallback(fn MailboxMsgFn) {
	c.callbackMgr.SetMailboxMsgCallback(fn)
}

func (c *coordinator) SetDecisionCallback(fn DecisionFn) {
	c.callbackMgr.SetDecisionCallback(fn)
}

// SetBlockCallback sets the block callback
func (c *coordinator) SetBlockCallback(fn BlockFn) {
	c.callbackMgr.SetBlockCallback(fn)
}

// handleTimeout handles transaction timeout
func (c *coordinator) handleTimeout(xtID *pb.XtID) {

}

// Start initializes and starts the coordinator
func (c *coordinator) Start(ctx context.Context) error {
	if c.started.Load() {
		return fmt.Errorf("coordinator already started")
	}

	c.started.Store(true)
	c.stopCh = make(chan struct{})

	c.log.Info().
		Str("node_id", c.config.NodeID).
		Msg("ConsensusCoord coordinator starting")

	c.log.Info().Msg("ConsensusCoord coordinator started successfully")
	return nil
}

// Stop gracefully stops the coordinator
func (c *coordinator) Stop(ctx context.Context) error {
	if c.stopped.Load() {
		return nil
	}

	c.log.Info().Msg("ConsensusCoord coordinator stopping...")
	c.stopped.Store(true)

	if c.stopCh != nil {
		close(c.stopCh)
	}

	return nil
}

// Stopped returns true if the coordinator has been stopped
func (c *coordinator) Stopped() bool {
	return c.stopped.Load()
}
