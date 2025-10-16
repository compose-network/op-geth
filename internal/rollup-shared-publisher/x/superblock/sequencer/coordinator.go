package sequencer

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/consensus"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/superblock/protocol"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/transport"
	"github.com/rs/zerolog"
)

// SequencerCoordinator coordinates sequencer SBCP operations
type SequencerCoordinator struct {
	mu      sync.RWMutex
	config  Config
	chainID []byte
	log     zerolog.Logger

	// Core components
	stateMachine   *StateMachine
	blockBuilder   *BlockBuilder
	messageRouter  *MessageRouter
	scpIntegration *SCPIntegration

	// Dependencies
	consensusCoord consensus.Coordinator
	transport      transport.Client

	// Miner integration (SDK)
	minerNotifier MinerNotifier
	callbacks     CoordinatorCallbacks

	// Current slot context
	currentSlot    uint64
	currentRequest *pb.L2BlockRequest

	// Runtime state
	running bool
	stopCh  chan struct{}

	// Queue StartSC messages that arrive while an SCP instance is active
	// TODO: rethink
	pendingStartSCs []struct {
		from  string
		start *pb.StartSC
	}
}

// NewSequencerCoordinator creates a new sequencer coordinator
func NewSequencerCoordinator(
	baseConsensus consensus.Coordinator,
	config Config,
	transport transport.Client,
	log zerolog.Logger,
) *SequencerCoordinator {
	coordinator := &SequencerCoordinator{
		config:         config,
		chainID:        config.ChainID,
		log:            log.With().Str("component", "sequencer.coordinator").Logger(),
		consensusCoord: baseConsensus,
		transport:      transport,
		stopCh:         make(chan struct{}),
	}

	// Initialize state machine with callback
	coordinator.stateMachine = NewStateMachine(
		config.ChainID,
		log,
		coordinator.onStateChange,
	)

	// Initialize block builder
	coordinator.blockBuilder = NewBlockBuilder(config.ChainID, log)

	// Initialize SCP integration
	coordinator.scpIntegration = NewSCPIntegration(
		config.ChainID,
		baseConsensus,
		coordinator.stateMachine,
		log,
		coordinator.blockBuilder,
	)

	// Initialize protocol handlers
	sbcpMessageHandler := NewSBCPHandler(coordinator, log)
	sbcpHandler := protocol.NewHandler(sbcpMessageHandler, protocol.NewBasicValidator(), log)
	scpHandler := consensus.NewProtocolHandler(baseConsensus, log)

	// Initialize message router with protocol handlers
	coordinator.messageRouter = NewMessageRouter(sbcpHandler, scpHandler, log)

	// Bind consensus decision callback directly to the coordinator so lifecycle is unified
	// and external callers (e.g., SDK hosts) don't need to forward decisions.
	if baseConsensus != nil {
		baseConsensus.SetDecisionCallback(coordinator.handleConsensusDecision)
	}

	return coordinator
}

// Start starts the sequencer coordinator
func (sc *SequencerCoordinator) Start(ctx context.Context) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.running {
		return fmt.Errorf("coordinator already running")
	}

	sc.log.Info().Msg("Starting sequencer coordinator")

	if err := sc.consensusCoord.Start(ctx); err != nil {
		return fmt.Errorf("failed to start consensus coordinator: %w", err)
	}

	sc.running = true

	sc.log.Info().
		Str("chain_id", fmt.Sprintf("%x", sc.chainID)).
		Str("state", sc.stateMachine.GetCurrentState().String()).
		Msg("Sequencer coordinator started")

	return nil
}

// Stop stops the sequencer coordinator
func (sc *SequencerCoordinator) Stop(ctx context.Context) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.running {
		return nil
	}

	sc.log.Info().Msg("Stopping sequencer coordinator")

	close(sc.stopCh)

	if err := sc.consensusCoord.Stop(ctx); err != nil {
		sc.log.Warn().Err(err).Msg("Failed to stop consensus coordinator gracefully")
	}

	sc.running = false

	sc.log.Info().Msg("Sequencer coordinator stopped")
	return nil
}

// HandleMessage routes messages through the message router
func (sc *SequencerCoordinator) HandleMessage(ctx context.Context, from string, msg *pb.Message) error {
	return sc.messageRouter.Route(ctx, from, msg)
}

func (sc *SequencerCoordinator) handleStartSlot(startSlot *pb.StartSlot) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.log.Info().
		Uint64("slot", startSlot.Slot).
		Uint64("superblock_number", startSlot.NextSuperblockNumber).
		Int("l2_requests", len(startSlot.L2BlocksRequest)).
		Str("chain_id", fmt.Sprintf("%x", sc.chainID)).
		Msg("Received StartSlot")

	// StartSlot messages should always be processed regardless of current state
	// This handles SP crashes/restarts where slot numbers may reset or rollback
	prevSlot := atomic.LoadUint64(&sc.currentSlot)
	if prevSlot > 0 && prevSlot > startSlot.Slot {
		sc.log.Info().
			Uint64("current_slot", prevSlot).
			Uint64("new_slot", startSlot.Slot).
			Msg("Processing StartSlot with lower slot number - likely SP restart/rollback")
	}

	// Find our L2BlockRequest
	var ourRequest *pb.L2BlockRequest
	for _, req := range startSlot.L2BlocksRequest {
		if bytes.Equal(req.ChainId, sc.chainID) {
			ourRequest = req
			break
		}
	}

	atomic.StoreUint64(&sc.currentSlot, startSlot.Slot)

	if ourRequest == nil {
		// Collect SP-provided chain IDs for diagnostics
		spChains := make([]string, 0, len(startSlot.L2BlocksRequest))
		for _, r := range startSlot.L2BlocksRequest {
			spChains = append(spChains, fmt.Sprintf("%x", r.ChainId))
		}

		sc.log.Info().
			Uint64("slot", startSlot.Slot).
			Str("our_chain_id", fmt.Sprintf("%x", sc.chainID)).
			Strs("sp_chain_ids", spChains).
			Msg("Not participating in this slot (no matching ChainID)")
		return nil
	}

	sc.log.Info().
		Uint64("block_number", ourRequest.BlockNumber).
		Str("parent_hash", fmt.Sprintf("%x", ourRequest.ParentHash)).
		Msg("Participating in slot - starting block building")

	sc.currentRequest = ourRequest

	// Start block builder
	if err := sc.blockBuilder.StartSlot(startSlot.Slot, ourRequest); err != nil {
		return fmt.Errorf("failed to start block builder: %w", err)
	}

	// Reset SCP per-slot tracking
	sc.scpIntegration.ResetForSlot(startSlot.Slot)

	// StartSlot messages should be processed regardless of the current state to handle
	// publisher crashes and restarts. Reset to Waiting first if needed, then proceed to Building-Free.
	curr := sc.stateMachine.GetCurrentState()
	if curr != StateWaiting {
		_ = sc.stateMachine.TransitionTo(StateWaiting, startSlot.Slot, "reset by StartSlot")
	}
	if err := sc.stateMachine.TransitionTo(StateBuildingFree, startSlot.Slot, "received StartSlot"); err != nil {
		return err
	}

	// Notify miner about slot start for block building coordination
	if sc.minerNotifier != nil {
		if err := sc.minerNotifier.NotifySlotStart(startSlot); err != nil {
			sc.log.Error().Err(err).Msg("Failed to notify miner of slot start")
		}
	}

	return nil
}

// handleRollBackAndStartSlot processes rollback + slot restart instructions from SP
func (sc *SequencerCoordinator) handleRollBackAndStartSlot(
	from string,
	rb *pb.RollBackAndStartSlot,
) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.log.Warn().
		Str("from", from).
		Uint64("slot", rb.CurrentSlot).
		Uint64("superblock_number", rb.NextSuperblockNumber).
		Int("l2_requests", len(rb.L2BlocksRequest)).
		Msg("Handling RollBackAndStartSlot - resetting draft and state")

	// Record the slot value so later SBCP messages see the right slot, even if we don't participate
	atomic.StoreUint64(&sc.currentSlot, rb.CurrentSlot)

	// Find our L2BlockRequest
	var ourRequest *pb.L2BlockRequest
	for _, req := range rb.L2BlocksRequest {
		if bytes.Equal(req.ChainId, sc.chainID) {
			ourRequest = req
			break
		}
	}

	if ourRequest == nil {
		// Collect SP-provided chain IDs for diagnostics
		spChains := make([]string, 0, len(rb.L2BlocksRequest))
		for _, r := range rb.L2BlocksRequest {
			spChains = append(spChains, fmt.Sprintf("%x", r.ChainId))
		}

		sc.log.Info().
			Uint64("slot", rb.CurrentSlot).
			Str("our_chain_id", fmt.Sprintf("%x", sc.chainID)).
			Strs("sp_chain_ids", spChains).
			Msg("Not participating in this rollback slot (no matching ChainID)")
		return nil
	}

	sc.currentRequest = ourRequest

	// Reset builder and start new draft from requested parent
	sc.blockBuilder.Reset()
	if err := sc.blockBuilder.StartSlot(rb.CurrentSlot, ourRequest); err != nil {
		return fmt.Errorf("failed to start block builder on rollback: %w", err)
	}

	// Reset SCP per-slot tracking
	sc.scpIntegration.ResetForSlot(rb.CurrentSlot)

	// Transition to Building-Free regardless of previous state
	if sc.stateMachine.GetCurrentState() != StateWaiting {
		_ = sc.stateMachine.TransitionTo(StateWaiting, rb.CurrentSlot, "reset by RollBackAndStartSlot")
	}
	if err := sc.stateMachine.TransitionTo(StateBuildingFree,
		rb.CurrentSlot,
		"received RollBackAndStartSlot"); err != nil {
		return err
	}

	// Notify miner about slot start for block building coordination (optional)
	if sc.minerNotifier != nil {
		// Synthesize a StartSlot-like structure for notifier
		startSlot := &pb.StartSlot{
			Slot:                 rb.CurrentSlot,
			NextSuperblockNumber: rb.NextSuperblockNumber,
			LastSuperblockHash:   rb.LastSuperblockHash,
			L2BlocksRequest:      rb.L2BlocksRequest,
		}
		if err := sc.minerNotifier.NotifySlotStart(startSlot); err != nil {
			sc.log.Error().Err(err).Msg("Failed to notify miner after rollback")
		}
	}

	return nil
}

func (sc *SequencerCoordinator) handleStartSC(
	ctx context.Context,
	from string,
	startSC *pb.StartSC) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if startSC.Slot != sc.currentSlot {
		sc.log.Warn().
			Uint64("msg_slot", startSC.Slot).
			Uint64("current_slot", sc.currentSlot).
			Msg("StartSC for wrong slot")
		return nil
	}

	if sc.stateMachine.GetCurrentState() != StateBuildingFree {
		// Queue for later processing once the current SCP instance completes
		sc.pendingStartSCs = append(sc.pendingStartSCs, struct {
			from  string
			start *pb.StartSC
		}{from: from, start: startSC})
		sc.log.Warn().
			Str("state", sc.stateMachine.GetCurrentState().String()).
			Int("queued", len(sc.pendingStartSCs)).
			Msg("StartSC received while locked; queued for later")
		return nil
	}

	// Enforce StartSC ordering: previous instance must be decided and sequence must be monotonic
	if sc.scpIntegration.GetActiveCount() > 0 {
		sc.pendingStartSCs = append(sc.pendingStartSCs, struct {
			from  string
			start *pb.StartSC
		}{from: from, start: startSC})
		sc.log.Warn().
			Uint64("sequence", startSC.XtSequenceNumber).
			Int("queued", len(sc.pendingStartSCs)).
			Msg("StartSC queued: previous instance undecided")
		return nil
	}

	requiredSeq := uint64(0)
	if lastSeq, ok := sc.scpIntegration.GetLastDecidedSequenceNumber(); ok {
		requiredSeq = lastSeq + 1
	}
	if startSC.XtSequenceNumber != requiredSeq {
		sc.log.Warn().
			Uint64("got_seq", startSC.XtSequenceNumber).
			Uint64("required_seq", requiredSeq).
			Msg("StartSC ignored: non-monotonic sequence")
		return nil
	}

	xtID := &pb.XtID{Hash: startSC.XtId}

	sc.log.Info().
		Str("xt_id", xtID.Hex()).
		Uint64("sequence", startSC.XtSequenceNumber).
		Msg("Starting SCP for cross-chain transaction")

	// Transition to Building-Locked
	if err := sc.stateMachine.TransitionTo(
		StateBuildingLocked,
		startSC.Slot,
		fmt.Sprintf("StartSC seq=%d", startSC.XtSequenceNumber),
	); err != nil {
		return err
	}

	// Handle SCP integration
	if err := sc.scpIntegration.HandleStartSC(ctx, startSC); err != nil {
		return err
	}

	// Extract our transactions
	myTxs := sc.extractMyTransactions(startSC.XtRequest)

	var voteResult = true

	if sc.callbacks.SimulateAndVote != nil && len(myTxs) > 0 {
		success, err := sc.callbacks.SimulateAndVote(ctx, startSC.XtRequest, xtID)
		if err != nil {
			sc.log.Error().Err(err).Str("xt_id", xtID.Hex()).Msg("Simulation failed")
			voteResult = false
		} else {
			voteResult = success
		}
	} else if len(myTxs) > 0 {
		// TODO: handle this case
		sc.log.Warn().
			Str("xt_id", xtID.Hex()).
			Msg("No simulation callback configured, voting true blindly")
	}

	// Send vote based on a simulation result
	vote := &pb.Vote{
		SenderChainId: sc.chainID,
		XtId:          xtID,
		Vote:          voteResult,
	}

	msg := &pb.Message{
		SenderId: fmt.Sprintf("seq-%x", sc.chainID),
		Payload:  &pb.Message_Vote{Vote: vote},
	}

	if err := sc.transport.Send(ctx, msg); err != nil {
		sc.log.Error().Err(err).Msg("Failed to send vote to SP")
		return err
	}

	sc.log.Info().
		Str("xt_id", xtID.Hex()).
		Bool("vote", voteResult).
		Msg("Sent vote to SP based on simulation")

	return nil
}

// Helper to extract our transactions
func (sc *SequencerCoordinator) extractMyTransactions(xtReq *pb.XTRequest) [][]byte {
	myTxs := make([][]byte, 0)

	for _, txReq := range xtReq.Transactions {
		if bytes.Equal(txReq.ChainId, sc.chainID) {
			myTxs = append(myTxs, txReq.Transaction...)
		}
	}

	return myTxs
}

//nolint:unparam,gocyclo // in progress
func (sc *SequencerCoordinator) handleRequestSeal(ctx context.Context, from string, requestSeal *pb.RequestSeal) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if requestSeal.Slot < sc.currentSlot {
		sc.log.Warn().
			Uint64("msg_slot", requestSeal.Slot).
			Uint64("current_slot", sc.currentSlot).
			Msg("RequestSeal for old slot")
		return nil
	}

	// If RequestSeal is for a newer slot, update our slot tracking
	// This can happen if we missed StartSlot messages or during startup
	if requestSeal.Slot > sc.currentSlot {
		sc.log.Info().
			Uint64("msg_slot", requestSeal.Slot).
			Uint64("current_slot", sc.currentSlot).
			Msg("Updating slot from RequestSeal message")
		atomic.StoreUint64(&sc.currentSlot, requestSeal.Slot)
	}

	// If we are not participating in this slot (no current request/draft), ignore the seal politely
	if sc.currentRequest == nil {
		sc.log.Info().
			Uint64("slot", requestSeal.Slot).
			Int("included_xts", len(requestSeal.IncludedXts)).
			Msg("Ignoring RequestSeal: not participating in this slot")
		return nil
	}

	sc.log.Info().
		Uint64("slot", requestSeal.Slot).
		Int("included_xts", len(requestSeal.IncludedXts)).
		Msg("Received RequestSeal - sealing block")

	// Verify superset: includedxTs(draft.scpInstances) ⊆ RequestSeal.IncludedXts
	// Build set from RequestSeal
	includedSet := make(map[string]struct{}, len(requestSeal.IncludedXts))
	for _, id := range requestSeal.IncludedXts {
		includedSet[fmt.Sprintf("%x", id)] = struct{}{}
	}
	// Compare with locally decided-included XTs
	for _, idHex := range sc.scpIntegration.GetIncludedXTsHex() {
		if _, ok := includedSet[idHex]; !ok {
			sc.log.Warn().
				Str("missing_xt", idHex).
				Msg("RequestSeal ignored: superset check failed")
			return nil
		}
	}

	// Handle any stuck SCP instances
	activeContexts := sc.scpIntegration.GetActiveContexts()
	for xtIDStr, scpCtx := range activeContexts {
		if scpCtx.Decision == nil {
			// Force decide based on inclusion
			decision := false
			for _, xtBytes := range requestSeal.IncludedXts {
				if xtIDStr == fmt.Sprintf("%x", xtBytes) {
					decision = true
					break
				}
			}

			sc.log.Info().
				Str("xt_id", xtIDStr).
				Bool("decision", decision).
				Msg("Force deciding stuck SCP instance")

			if err := sc.scpIntegration.HandleDecision(scpCtx.XtID, decision); err != nil {
				sc.log.Error().Err(err).Str("xt_id", xtIDStr).Msg("Failed to force decide")
			}
		}
	}

	// Clear queued StartSC messages: RequestSeal marks the end of the SCP phase for this slot.
	// Messages that were queued but not yet processed arrived too late (after the seal cutover
	// time) to be included in this slot. The publisher tracks attempted transactions and will
	// re-queue them for the next slot if this slot fails. The sequencer should not carry over
	// queued messages across slot boundaries.
	if len(sc.pendingStartSCs) > 0 {
		sc.log.Info().
			Int("dropped_count", len(sc.pendingStartSCs)).
			Uint64("slot", requestSeal.Slot).
			Msg("Clearing queued StartSC messages at RequestSeal (too late for this slot)")
		sc.pendingStartSCs = nil
	}

	// Notify miner to seal current block
	if sc.minerNotifier != nil {
		if err := sc.minerNotifier.NotifyRequestSeal(ctx, requestSeal); err != nil {
			sc.log.Error().Err(err).Msg("Failed to notify miner of request seal")
		}
	}

	// Transition to Submission if not already there; real block sealing/submission
	// will be handled by the miner and EthAPIBackend once the block is actually built.
	if sc.stateMachine.GetCurrentState() != StateSubmission {
		if err := sc.stateMachine.TransitionTo(
			StateSubmission,
			requestSeal.Slot,
			"received RequestSeal",
		); err != nil {
			return err
		}
	} else {
		sc.log.Debug().
			Uint64("slot", requestSeal.Slot).
			Msg("RequestSeal received while already in Submission; idempotent no-op")
	}
	return nil
}

// sealAndSubmitBlock seals the current block and submits to SP
//
//nolint:unused // in progress
func (sc *SequencerCoordinator) sealAndSubmitBlock(ctx context.Context, includedXTs [][]byte) error {
	// Build L2 block
	l2Block, err := sc.blockBuilder.SealBlock(includedXTs)
	if err != nil {
		return fmt.Errorf("failed to seal block: %w", err)
	}

	sc.log.Info().
		Uint64("slot", l2Block.Slot).
		Uint64("block_number", l2Block.BlockNumber).
		Str("block_hash", fmt.Sprintf("%x", l2Block.BlockHash)).
		Int("included_xts", len(l2Block.IncludedXts)).
		Msg("Submitting L2 block to SP")

	// Submit to SP
	msg := &pb.Message{
		SenderId: fmt.Sprintf("seq-%x", sc.chainID),
		Payload:  &pb.Message_L2Block{L2Block: l2Block},
	}

	if err := sc.transport.Send(ctx, msg); err != nil {
		sc.log.Error().Err(err).Msg("Failed to submit L2 block")
		return err
	}

	// Transition back to Waiting
	if err := sc.stateMachine.TransitionTo(
		StateWaiting,
		l2Block.Slot,
		"L2 block submitted",
	); err != nil {
		return err
	}

	// Reset block builder for next slot
	sc.blockBuilder.Reset()

	sc.log.Info().
		Uint64("slot", l2Block.Slot).
		Msg("L2 block submitted successfully")

	return nil
}

// onStateChange handles actions on state transitions
func (sc *SequencerCoordinator) onStateChange(from, to State, slot uint64, reason string) {
	// Handle state-specific actions
	switch to {
	case StateBuildingFree:
		// Ready to accept local transactions
		sc.log.Debug().Msg("Ready to accept local transactions")

	case StateBuildingLocked:
		// SCP in progress - no local tx acceptance
		sc.log.Debug().Msg("SCP in progress - blocking local transactions")

	case StateSubmission:
		// Block sealing in progress
		sc.log.Debug().Msg("Block sealing in progress")

	case StateWaiting:
		// Waiting for next slot
		sc.log.Debug().Msg("Waiting for next slot")
	}

	// Notify miner about state changes
	if sc.minerNotifier != nil {
		if err := sc.minerNotifier.NotifyStateChange(from, to, slot); err != nil {
			sc.log.Error().Err(err).Msg("Failed to notify miner of state change")
		}
	}
}

// Interface implementations

// Consensus returns the underlying consensus coordinator
func (sc *SequencerCoordinator) Consensus() consensus.Coordinator {
	return sc.consensusCoord
}

func (sc *SequencerCoordinator) GetCurrentSlot() uint64 {
	return atomic.LoadUint64(&sc.currentSlot)
}

func (sc *SequencerCoordinator) GetState() State {
	return sc.stateMachine.GetCurrentState()
}

func (sc *SequencerCoordinator) GetStats() map[string]interface{} {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	stats := map[string]interface{}{
		"running":       sc.running,
		"chain_id":      fmt.Sprintf("%x", sc.chainID),
		"current_slot":  atomic.LoadUint64(&sc.currentSlot),
		"current_state": sc.stateMachine.GetCurrentState().String(),
		"transitions":   len(sc.stateMachine.GetTransitions()),
	}

	// Add block builder stats
	if sc.blockBuilder != nil {
		builderStats := sc.blockBuilder.GetDraftStats()
		stats["block_builder"] = builderStats
	}

	// Add SCP stats
	if sc.scpIntegration != nil {
		activeContexts := sc.scpIntegration.GetActiveContexts()
		stats["active_scp_instances"] = len(activeContexts)
	}

	// Add message router stats
	if sc.messageRouter != nil {
		routerStats := sc.messageRouter.GetStats()
		stats["message_router"] = routerStats
	}

	return stats
}

// GetActiveSCPInstanceCount returns the number of active SCP instances
func (sc *SequencerCoordinator) GetActiveSCPInstanceCount() int {
	if sc.scpIntegration != nil {
		return sc.scpIntegration.GetActiveCount()
	}
	return 0
}

// BlockLifecycleManager implementation

// OnBlockBuildingStart is called when block building starts
// TODO: rethink lock, it blocks ethapi engine
func (sc *SequencerCoordinator) OnBlockBuildingStart(ctx context.Context, slot uint64) error {
	active := 0
	if sc.scpIntegration != nil {
		active = sc.scpIntegration.GetActiveCount()
	}
	state := "unknown"
	if sc.stateMachine != nil {
		state = sc.stateMachine.GetCurrentState().String()
	}
	sc.log.Info().
		Uint64("slot", slot).
		Str("state", state).
		Int("active_scp_instances", active).
		Msg("Block building started")

	if active > 0 {
		sc.log.Info().
			Uint64("slot", slot).
			Msg("Building with in-flight SCP instances")
	}
	return nil
}

// OnBlockBuildingComplete is called when block building completes
func (sc *SequencerCoordinator) OnBlockBuildingComplete(ctx context.Context, block *pb.L2Block, success bool) error {
	if success && block != nil {
		sc.log.Info().
			Uint64("slot", block.Slot).
			Uint64("block_number", block.BlockNumber).
			Str("block_hash", fmt.Sprintf("%x", block.BlockHash)).
			Msg("Block building completed successfully")

		// TODO: we should call Consensus().OnL2BlockCommitted here

		// Transition back to Waiting after successful sealing
		if sc.stateMachine.GetCurrentState() == StateSubmission {
			if err := sc.stateMachine.TransitionTo(StateWaiting, block.Slot, "L2 block submitted"); err != nil {
				sc.log.Error().Err(err).Msg("Failed to transition to Waiting after block submission")
			}
		}

		// Reset block builder for the next slot
		if sc.blockBuilder != nil {
			sc.blockBuilder.Reset()
		}
	} else {
		sc.log.Warn().
			Bool("success", success).
			Msg("Block building completed with issues")
	}
	return nil
}

// handleConsensusDecision processes the final decision from the consensus layer for a cross-chain
// transaction. It updates the block builder state and manages transaction lifecycle based on whether
// the transaction was committed (decision=true) or aborted (decision=false).
//
// For committed transactions, the block builder includes them in the draft block. For aborted
// transactions, they are immediately removed from both the block builder and the execution layer's
// pending pool to ensure they cannot be included in any block. This guarantees atomic inclusion
// semantics: transactions are either fully included or fully excluded, with no partial states.
//
// After processing the decision, if the coordinator has returned to Building-Free state and there
// are queued cross-chain transactions waiting, the next one is automatically started.
func (sc *SequencerCoordinator) handleConsensusDecision(ctx context.Context, xtID *pb.XtID, decision bool) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.log.Info().
		Str("xt_id", xtID.Hex()).
		Bool("decision", decision).
		Msg("Processing consensus decision at coordinator")

	// HandleDecision is idempotent - if RequestSeal already processed this, it's a no-op
	if err := sc.scpIntegration.HandleDecision(xtID, decision); err != nil {
		// If context not found, it means RequestSeal already handled this decision
		sc.log.Debug().
			Err(err).
			Str("xt_id", xtID.Hex()).
			Msg("SCP context already processed (likely by RequestSeal)")
		return nil
	}

	// For aborted transactions, immediately invoke cleanup callback to remove from pending pool.
	// This ensures the transaction cannot be committed in blocks built before RequestSeal arrives.
	if !decision && sc.callbacks.CleanupAbortedTransaction != nil {
		if err := sc.callbacks.CleanupAbortedTransaction(ctx, xtID); err != nil {
			sc.log.Warn().Err(err).Str("xt_id", xtID.Hex()).Msg("Cleanup callback failed for aborted transaction")
		}
	}

	// If we returned to Building-Free and have queued StartSCs, process the next one
	if sc.stateMachine.GetCurrentState() == StateBuildingFree && len(sc.pendingStartSCs) > 0 {
		next := sc.pendingStartSCs[0]
		sc.pendingStartSCs = sc.pendingStartSCs[1:]
		sc.log.Info().
			Int("remaining", len(sc.pendingStartSCs)).
			Uint64("slot", sc.currentSlot).
			Msg("Starting next queued StartSC after decision")
		// Drop lock while invoking handler to avoid deadlocks and allow nested transitions
		sc.mu.Unlock()
		defer sc.mu.Lock()
		return sc.handleStartSC(ctx, next.from, next.start)
	}

	return nil
}

// PrepareTransactionsForBlock prepares transactions for block inclusion
func (sc *SequencerCoordinator) PrepareTransactionsForBlock(ctx context.Context, slot uint64) error {
	if slot != sc.currentSlot {
		return fmt.Errorf("preparing for wrong slot: current=%d, requested=%d", sc.currentSlot, slot)
	}

	sc.log.Debug().
		Uint64("slot", slot).
		Msg("Preparing transactions for block")

	// Prepare any coordination transactions through block builder
	if sc.blockBuilder != nil {
		// BlockBuilder should handle coordination transaction preparation
		sc.log.Debug().Msg("Block builder preparation delegated")
	}

	return nil
}

// GetOrderedTransactionsForBlock returns transactions in correct order for block
func (sc *SequencerCoordinator) GetOrderedTransactionsForBlock(ctx context.Context) ([]*pb.TransactionRequest, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// Delegate to block builder for transaction ordering
	if sc.blockBuilder != nil {
		sc.log.Debug().Msg("Getting ordered transactions from block builder")
		// This would need to be implemented in BlockBuilder
		// For now, return empty slice
		return []*pb.TransactionRequest{}, nil
	}

	return []*pb.TransactionRequest{}, nil
}

// CallbackManager implementation

// SetCallbacks sets the coordinator callbacks
func (sc *SequencerCoordinator) SetCallbacks(callbacks CoordinatorCallbacks) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.callbacks = callbacks
	sc.log.Debug().Msg("Coordinator callbacks set")
}

// SetMinerNotifier sets the miner notifier
func (sc *SequencerCoordinator) SetMinerNotifier(notifier MinerNotifier) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.minerNotifier = notifier
	sc.log.Debug().Msg("Miner notifier set")
}

// WrapCoordinator wraps an existing consensus coordinator with SBCP functionality
func WrapCoordinator(
	baseConsensus consensus.Coordinator,
	config Config,
	transport transport.Client,
	log zerolog.Logger,
) (Coordinator, error) {
	return NewSequencerCoordinator(baseConsensus, config, transport, log), nil
}
