package consensus

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
)

// coordinator implements the Coordinator interface
type coordinator struct {
	config       Config
	stateManager *StateManager
	callbackMgr  *CallbackManager
	metrics      MetricsRecorder
	log          zerolog.Logger

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

// New creates a new coordinator instance
func New(log zerolog.Logger, config Config) Coordinator {
	return NewWithMetrics(log, config, NewMetrics())
}

// NewWithMetrics creates a new coordinator instance with custom metrics recorder
// TODO: check best practices for metrics recorder
func NewWithMetrics(log zerolog.Logger, config Config, metrics MetricsRecorder) Coordinator {
	logger := log.With().
		Str("component", "consensus-coordinator").
		Str("role", config.Role.String()).
		Str("node_id", config.NodeID).
		Logger()

	return &coordinator{
		config:       config,
		stateManager: NewStateManager(),
		callbackMgr:  NewCallbackManager(30*time.Second, logger),
		metrics:      metrics,
		log:          logger,
		sentMap:      make(map[string]bool),
	}
}

// OnBlockCommitted selects committed xTs not yet sent and invokes block callback.
// Used by execution-integrated path (geth types.Block).
func (c *coordinator) OnBlockCommitted(ctx context.Context, block *types.Block) error {
	// Gather committed xTs that haven't been sent yet
	active := c.stateManager.GetAllActiveIDs()
	xtIDs := make([]*pb.XtID, 0)

	for _, id := range active {
		state, ok := c.stateManager.GetState(id)
		if !ok {
			continue
		}
		if state.GetDecision() != StateCommit {
			continue
		}
		idStr := id.Hex()
		c.sentMu.Lock()
		already := c.sentMap[idStr]
		c.sentMu.Unlock()
		if already {
			continue
		}
		xtIDs = append(xtIDs, id)
	}

	if len(xtIDs) == 0 {
		return nil
	}

	// Invoke block callback
	c.callbackMgr.InvokeBlock(ctx, block, xtIDs)

	// Mark as sent
	c.sentMu.Lock()
	for _, id := range xtIDs {
		c.sentMap[id.Hex()] = true
	}
	c.sentMu.Unlock()

	c.log.Info().
		Int("xt_count", len(xtIDs)).
		Str("block_hash", block.Hash().Hex()).
		Msg("OnBlockCommitted sent committed xTs")

	return nil
}

// OnL2BlockCommitted marks included xTs from a pb.L2Block as sent in consensus state.
// Used by SBCP sequencer path (no geth types.Block available).
func (c *coordinator) OnL2BlockCommitted(ctx context.Context, block *pb.L2Block) error {
	if block == nil || len(block.IncludedXts) == 0 {
		return nil
	}
	c.sentMu.Lock()
	for _, xt := range block.IncludedXts {
		c.sentMap[fmt.Sprintf("%x", xt)] = true
	}
	c.sentMu.Unlock()
	c.log.Info().
		Int("xt_count", len(block.IncludedXts)).
		Uint64("slot", block.Slot).
		Msg("OnL2BlockCommitted marked committed xTs")
	return nil
}

// StartTransaction initiates a new 2PC transaction
func (c *coordinator) StartTransaction(ctx context.Context, from string, xtReq *pb.XTRequest) error {
	xtID, err := xtReq.XtID()
	if err != nil {
		return fmt.Errorf("failed to generate xtID: %w", err)
	}

	chains := xtReq.ChainIDs()
	if len(chains) == 0 {
		return fmt.Errorf("no participating chains found")
	}

	state, err := c.stateManager.AddState(xtID, xtReq, chains)
	if err != nil {
		return err
	}

	// Timeout only for leader; followers rely on the SP decision
	if c.config.Role == Leader {
		state.Timer = time.AfterFunc(c.config.Timeout, func() {
			c.handleTimeout(xtID)
		})
	}

	c.metrics.RecordTransactionStarted(len(chains))

	c.log.Info().
		Str("xt_id", xtID.Hex()).
		Int("participating_chains", len(chains)).
		Dur("timeout", c.config.Timeout).
		Msg("Started 2PC transaction")

	// Invoke start callback
	c.callbackMgr.InvokeStart(ctx, from, xtReq)

	return nil
}

// RecordVote processes a vote from a participant
func (c *coordinator) RecordVote(xtID *pb.XtID, chainID string, vote bool) (DecisionState, error) {
	state, exists := c.stateManager.GetState(xtID)
	if !exists {
		return StateUndecided, fmt.Errorf("transaction %s not found", xtID.Hex())
	}

	if state.GetDecision() != StateUndecided {
		return state.GetDecision(), nil
	}

	// Check if chain is a participant
	state.mu.RLock()
	_, isParticipant := state.ParticipatingChains[chainID]
	state.mu.RUnlock()

	if !isParticipant {
		return StateUndecided, fmt.Errorf("chain %s not participating in transaction %s", chainID, xtID.Hex())
	}

	// Add vote atomically
	if !state.AddVote(chainID, vote) {
		return StateUndecided, fmt.Errorf("chain %s already voted for transaction %s", chainID, xtID.Hex())
	}

	voteLatency := time.Since(state.StartTime)
	c.metrics.RecordVote(chainID, vote, voteLatency)

	c.log.Info().
		Str("xt_id", xtID.Hex()).
		Str("chain", chainID).
		Bool("vote", vote).
		Int("votes_recorded", state.GetVoteCount()).
		Int("votes_required", state.GetParticipantCount()).
		Msg("Recorded vote")

	// Handle abort immediately
	if !vote {
		return c.handleAbort(xtID, state), nil
	}

	// Check for commit (leader only)
	if c.config.Role == Leader {
		if state.GetVoteCount() == state.GetParticipantCount() {
			return c.handleCommit(xtID, state), nil
		}
	} else {
		// Follower broadcasts vote
		c.callbackMgr.InvokeVote(xtID, vote, voteLatency)
	}

	return StateUndecided, nil
}

// RecordDecision processes a decision (for followers)
func (c *coordinator) RecordDecision(xtID *pb.XtID, decision bool) error {
	if c.config.Role != Follower {
		return fmt.Errorf("only followers can record decisions, current role: %s", c.config.Role)
	}

	state, exists := c.stateManager.GetState(xtID)
	if !exists {
		c.log.Debug().
			Str("xt_id", xtID.Hex()).
			Bool("decision", decision).
			Msg("Received decision for unknown transaction")
		return nil
	}

	if state.GetDecision() != StateUndecided {
		c.log.Debug().
			Str("xt_id", xtID.Hex()).
			Bool("decision", decision).
			Msg("Received decision for already decided transaction")
		return nil
	}

	// Set decision
	if decision {
		state.SetDecision(StateCommit)
	} else {
		state.SetDecision(StateAbort)
	}

	// Stop timer
	if state.Timer != nil {
		state.Timer.Stop()
	}

	duration := time.Since(state.StartTime)
	c.metrics.RecordTransactionCompleted(state.GetDecision().String(), duration)

	c.log.Info().
		Str("xt_id", xtID.Hex()).
		Bool("decision", decision).
		Dur("duration", duration).
		Msg("Recorded decision")

	// Invoke decision callback so sequencer can transition state and process queued transactions
	c.callbackMgr.InvokeDecision(xtID, decision, duration)

	// Schedule cleanup
	time.AfterFunc(5*time.Minute, func() {
		c.stateManager.RemoveState(xtID)
	})

	return nil
}

// GetTransactionState returns the current state of a transaction
func (c *coordinator) GetTransactionState(xtID *pb.XtID) (DecisionState, error) {
	state, exists := c.stateManager.GetState(xtID)
	if !exists {
		return StateUndecided, fmt.Errorf("transaction %s not found", xtID.Hex())
	}

	return state.GetDecision(), nil
}

// GetActiveTransactions returns all active transaction IDs
func (c *coordinator) GetActiveTransactions() []*pb.XtID {
	return c.stateManager.GetAllActiveIDs()
}

// GetState retrieves a transaction state
func (c *coordinator) GetState(xtID *pb.XtID) (*TwoPCState, bool) {
	return c.stateManager.GetState(xtID)
}

// RecordCIRCMessage records a CIRC message for a transaction
func (c *coordinator) RecordCIRCMessage(circMessage *pb.CIRCMessage) error {
	xtID := circMessage.XtId
	state, exists := c.stateManager.GetState(xtID)
	if !exists {
		return fmt.Errorf("transaction %s not found", xtID.Hex())
	}

	sourceChainID := ChainKeyBytes(circMessage.SourceChain)

	state.mu.Lock()
	defer state.mu.Unlock()

	if _, isParticipant := state.ParticipatingChains[sourceChainID]; !isParticipant {
		return fmt.Errorf("chain %s not participating in transaction %s", sourceChainID, xtID.Hex())
	}

	// Add message to queue
	messages, ok := state.CIRCMessages[sourceChainID]
	if !ok {
		messages = make([]*pb.CIRCMessage, 0)
	}
	messages = append(messages, circMessage)
	state.CIRCMessages[sourceChainID] = messages

	sourceChainIDInt := new(big.Int).SetBytes(circMessage.SourceChain)
	c.log.Info().
		Str("xt_id", xtID.Hex()).
		Str("chain_id", sourceChainIDInt.String()).
		Msg("Recorded CIRC message")

	return nil
}

// ConsumeCIRCMessage consumes a CIRC message from the queue
func (c *coordinator) ConsumeCIRCMessage(xtID *pb.XtID, sourceChainID string) (*pb.CIRCMessage, error) {
	state, exists := c.stateManager.GetState(xtID)
	if !exists {
		return nil, fmt.Errorf("transaction %s not found", xtID.Hex())
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if _, isParticipant := state.ParticipatingChains[sourceChainID]; !isParticipant {
		return nil, fmt.Errorf("chain %s not participating in transaction %s", sourceChainID, xtID.Hex())
	}

	messages, ok := state.CIRCMessages[sourceChainID]
	if !ok || len(messages) == 0 {
		return nil, fmt.Errorf("no messages available for chain %s in transaction %s", sourceChainID, xtID.Hex())
	}

	// Pop first message
	message := messages[0]
	state.CIRCMessages[sourceChainID] = messages[1:]

	if len(state.CIRCMessages[sourceChainID]) == 0 {
		delete(state.CIRCMessages, sourceChainID)
	}

	return message, nil
}

// SetStartCallback sets the start callback
func (c *coordinator) SetStartCallback(fn StartFn) {
	c.callbackMgr.SetStartCallback(fn)
}

// SetVoteCallback sets the vote callback
func (c *coordinator) SetVoteCallback(fn VoteFn) {
	c.callbackMgr.SetVoteCallback(fn)
}

// SetDecisionCallback sets the decision callback
func (c *coordinator) SetDecisionCallback(fn DecisionFn) {
	c.callbackMgr.SetDecisionCallback(fn)
}

// SetBlockCallback sets the block callback
func (c *coordinator) SetBlockCallback(fn BlockFn) {
	c.callbackMgr.SetBlockCallback(fn)
}

// handleCommit handles a commit decision
func (c *coordinator) handleCommit(xtID *pb.XtID, state *TwoPCState) DecisionState {
	state.SetDecision(StateCommit)

	if state.Timer != nil {
		state.Timer.Stop()
	}

	duration := time.Since(state.StartTime)
	c.metrics.RecordTransactionCompleted(StateCommit.String(), duration)

	c.callbackMgr.InvokeDecision(xtID, true, duration)

	// Schedule cleanup
	time.AfterFunc(5*time.Minute, func() {
		c.stateManager.RemoveState(xtID)
	})

	return StateCommit
}

// handleAbort handles an abort decision
func (c *coordinator) handleAbort(xtID *pb.XtID, state *TwoPCState) DecisionState {
	state.SetDecision(StateAbort)

	if state.Timer != nil {
		state.Timer.Stop()
	}

	duration := time.Since(state.StartTime)
	c.metrics.RecordTransactionCompleted(StateAbort.String(), duration)

	if c.config.Role == Leader {
		c.callbackMgr.InvokeDecision(xtID, false, duration)
	} else {
		c.callbackMgr.InvokeVote(xtID, false, duration)
	}

	// Schedule cleanup
	time.AfterFunc(5*time.Minute, func() {
		c.stateManager.RemoveState(xtID)
	})

	return StateAbort
}

// handleTimeout handles transaction timeout
func (c *coordinator) handleTimeout(xtID *pb.XtID) {
	state, exists := c.stateManager.GetState(xtID)
	if !exists {
		return
	}

	if state.GetDecision() == StateUndecided {
		c.log.Warn().
			Str("xt_id", xtID.Hex()).
			Dur("timeout", c.config.Timeout).
			Msg("Transaction timed out")

		c.metrics.RecordTimeout()
		c.handleAbort(xtID, state)
	}
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
		Str("role", c.config.Role.String()).
		Msg("Consensus coordinator starting")

	c.log.Info().Msg("Consensus coordinator started successfully")
	return nil
}

// Stop gracefully stops the coordinator
func (c *coordinator) Stop(ctx context.Context) error {
	if c.stopped.Load() {
		return nil
	}

	c.log.Info().Msg("Consensus coordinator stopping...")
	c.stopped.Store(true)

	if c.stopCh != nil {
		close(c.stopCh)
	}

	done := make(chan struct{})

	go func() {
		c.wg.Wait()
		c.shutdownOnce.Do(func() {
			c.stateManager.Shutdown()
		})
		close(done)
	}()

	select {
	case <-done:
		c.log.Info().Msg("Consensus coordinator stopped gracefully")
		return nil
	case <-ctx.Done():
		c.log.Warn().Msg("Consensus coordinator stop timed out, forcing shutdown")
		c.shutdownOnce.Do(func() {
			c.stateManager.Shutdown()
		})
		return ctx.Err()
	}
}

// Stopped returns true if the coordinator has been stopped
func (c *coordinator) Stopped() bool {
	return c.stopped.Load()
}
