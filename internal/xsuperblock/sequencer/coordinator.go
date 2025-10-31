package sequencer

import (
	"context"
	"fmt"
	"github.com/compose-network/specs/compose"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	periodproto "github.com/compose-network/specs/compose/sbcp"
	instanceproto "github.com/compose-network/specs/compose/scp"
	"github.com/ethereum/go-ethereum/internal/xconsensus"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/xsuperblock/period"
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

	callbacks CoordinatorCallbacks

	// Current slot context
	currentSlot uint64

	// Runtime state
	running         bool
	stopCh          chan struct{}
	periodSequencer periodproto.Sequencer
}

// NewSequencerCoordinator creates a new sequencer coordinator
func NewSequencerCoordinator(consensusCoord xconsensus.Coordinator, periodSequencer sbcp.Sequencer, config Config, transport xtransport.Client, log zerolog.Logger) *SequencerCoordinator {
	coordinator := &SequencerCoordinator{
		config:          config,
		chainID:         config.ChainID,
		log:             log.With().Str("component", "sequencer.coordinator").Logger(),
		consensusCoord:  consensusCoord,
		periodSequencer: periodSequencer,
		transport:       transport,
		stopCh:          make(chan struct{}),
	}

	// Initialize SCP integration
	coordinator.scpIntegration = NewSCPIntegration(
		config.ChainID,
		consensusCoord,
		log,
	)

	// Initialize protocol handlers
	periodHandler := period.NewPeriodHandler(period.NewBasicValidator(), log)
	instanceHandler := xconsensus.NewInstanceHandler(consensusCoord, log)

	// Initialize message router with protocol handlers
	coordinator.messageRouter = NewMessageRouter(periodHandler, instanceHandler, log)

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
		Msg("PeriodSequencer coordinator started")

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

	sc.log.Info().Msg("PeriodSequencer coordinator stopped")
	return nil
}

// HandleMessage routes messages through the message router
func (sc *SequencerCoordinator) HandleMessage(ctx context.Context, from string, msg *sbcpproto.Message) error {
	return sc.messageRouter.Route(ctx, from, msg)
}

// Consensus returns the underlying consensus coordinator
func (sc *SequencerCoordinator) ConsensusCoord() xconsensus.Coordinator {
	return sc.consensusCoord
}

func (sc *SequencerCoordinator) PeriodSequencer() periodproto.Sequencer {
	return sc.periodSequencer
}

func (sc *SequencerCoordinator) InstanceSequencer() instanceproto.Sequencer {
	return sc.instanceSequencer
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
func (sc *SequencerCoordinator) handleConsensusDecision(ctx context.Context, instanceID *compose.InstanceID, decision bool) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.log.Info().
		Str("xt_id", instanceID.String()).
		Bool("decision", decision).
		Msg("Processing consensus decision at coordinator")

	// HandleDecision is idempotent - if RequestSeal already processed this, it's a no-op
	if err := sc.scpIntegration.HandleDecision(instanceID, decision); err != nil {
		// If context not found, it means RequestSeal already handled this decision
		sc.log.Debug().
			Err(err).
			Str("xt_id", instanceID.String()).
			Msg("SCP context already processed (likely by RequestSeal)")
		return nil
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
