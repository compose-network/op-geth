package sequencer

import (
	"context"
	"fmt"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	"github.com/ethereum/go-ethereum/internal/xconsensus"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	xprotocol "github.com/ethereum/go-ethereum/internal/xsuperblock/protocol"
	"github.com/ethereum/go-ethereum/internal/xtransport"
	"sync"

	"github.com/rs/zerolog"
)

// SequencerCoordinator coordinates sequencer SBCP operations
type SequencerCoordinator struct {
	mu      sync.RWMutex
	config  Config
	chainID []byte
	log     zerolog.Logger

	messageRouter  *MessageRouter
	scpIntegration *SCPIntegration

	// Dependencies
	consensusCoord xconsensus.Coordinator
	transport      xtransport.Client

	// Miner integration (SDK)
	minerNotifier MinerNotifier
	callbacks     CoordinatorCallbacks

	// Current slot context
	currentSlot uint64

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
	consensusCoord xconsensus.Coordinator,
	config Config,
	transport xtransport.Client,
	log zerolog.Logger,
) *SequencerCoordinator {
	coordinator := &SequencerCoordinator{
		config:         config,
		chainID:        config.ChainID,
		log:            log.With().Str("component", "sequencer.coordinator").Logger(),
		consensusCoord: consensusCoord,
		transport:      transport,
		stopCh:         make(chan struct{}),
	}

	// Initialize SCP integration
	coordinator.scpIntegration = NewSCPIntegration(
		config.ChainID,
		consensusCoord,
		log,
	)

	// Initialize protocol handlers
	sbcpHandler := xprotocol.NewSBCPHandler(xprotocol.NewBasicValidator(), log)
	scpHandler := xconsensus.NewSCPHandler(consensusCoord, log)

	// Initialize message router with protocol handlers
	coordinator.messageRouter = NewMessageRouter(sbcpHandler, scpHandler, log)

	// Bind consensus decision callback directly to the coordinator so lifecycle is unified
	// and external callers (e.g., SDK hosts) don't need to forward decisions.
	if consensusCoord != nil {
		consensusCoord.SetDecisionCallback(coordinator.handleConsensusDecision)
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
func (sc *SequencerCoordinator) HandleMessage(ctx context.Context, from string, msg *sbcpproto.Message) error {
	return sc.messageRouter.Route(ctx, from, msg)
}

// Helper to extract our transactions
//func (sc *SequencerCoordinator) extractMyTransactions(xtReq *sbcp.XTRequest) [][]byte {
//	myTxs := make([][]byte, 0)
//
//	for _, txReq := range xtReq.Transactions {
//		if bytes.Equal(txReq.ChainId, sc.chainID) {
//			myTxs = append(myTxs, txReq.Transaction...)
//		}
//	}
//
//	return myTxs
//}

// sealAndSubmitBlock seals the current block and submits to SP
//

// Interface implementations

// Consensus returns the underlying consensus coordinator
func (sc *SequencerCoordinator) Consensus() xconsensus.Coordinator {
	return sc.consensusCoord
}

// OnBlockBuildingStart is called when block building begins for a slot
func (sc *SequencerCoordinator) OnBlockBuildingStart(ctx context.Context, slot uint64) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.log.Debug().
		Uint64("slot", slot).
		Msg("Block building started")

	// TODO: Implement block building start logic
	return nil
}

// OnBlockBuildingComplete is called when block building completes for a slot
func (sc *SequencerCoordinator) OnBlockBuildingComplete(ctx context.Context, block *pb.L2Block, success bool) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.log.Debug().
		Bool("success", success).
		Msg("Block building completed")

	// TODO: Implement block building completion logic
	return nil
}

// TransactionManager implementation

// PrepareTransactionsForBlock prepares transactions for inclusion in a block
func (sc *SequencerCoordinator) PrepareTransactionsForBlock(ctx context.Context, slot uint64) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.log.Debug().
		Uint64("slot", slot).
		Msg("Preparing transactions for block")

	// TODO: Implement transaction preparation logic
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

	return nil
}

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
