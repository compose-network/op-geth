package sequencer

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/consensus"
	"github.com/rs/zerolog"
)

type SCPContext struct {
	XtID           *pb.XtID
	Request        *pb.XTRequest
	Slot           uint64
	SequenceNumber uint64
	MyTransactions [][]byte
	Decision       *bool
}

// DecidedXT tracks the final decision for an XT in a slot
type DecidedXT struct {
	XtIDBytes []byte // raw xtID bytes
	Included  bool   // true=included, false=aborted
}

type SCPIntegration struct {
	mu           sync.RWMutex
	chainID      []byte
	consensus    consensus.Coordinator
	stateMachine *StateMachine
	log          zerolog.Logger

	activeContexts map[string]*SCPContext // xtID -> context

	// per-slot tracked state
	decidedXTs map[string]*DecidedXT // hex xtID -> decision state for this slot
	// last decided sequence number for monotonic StartSC enforcement
	lastDecidedSeq    uint64
	hasLastDecidedSeq bool
	currentSlot       uint64
	blockBuilder      *BlockBuilder
}

func NewSCPIntegration(
	chainID []byte,
	consensus consensus.Coordinator,
	stateMachine *StateMachine,
	log zerolog.Logger,
	builder *BlockBuilder,
) *SCPIntegration {
	return &SCPIntegration{
		chainID:        chainID,
		consensus:      consensus,
		stateMachine:   stateMachine,
		log:            log.With().Str("component", "scp_integration").Logger(),
		activeContexts: make(map[string]*SCPContext),
		decidedXTs:     make(map[string]*DecidedXT),
		blockBuilder:   builder,
	}
}

func (si *SCPIntegration) HandleStartSC(ctx context.Context, startSC *pb.StartSC) error {
	xtID := &pb.XtID{Hash: startSC.XtId}
	xtIDStr := xtID.Hex()

	si.mu.Lock()
	defer si.mu.Unlock()

	// Ensure local consensus state exists for this xT so CIRC
	// messages can be recorded/consumed by the sequencer's coordinator
	if err := si.consensus.StartTransaction(ctx, "sequencer", startSC.XtRequest); err != nil {
		// Do not fail the flow – log and continue to avoid blocking SBCP.
		// CIRC Record/Consume will clearly error if state is missing.
		si.log.Error().
			Err(err).
			Str("xt_id", xtIDStr).
			Msg("Failed to start local 2PC state for StartSC")
	} else {
		si.log.Debug().
			Str("xt_id", xtIDStr).
			Msg("Initialized local 2PC state for StartSC")
	}

	// Create SCP context
	scpCtx := &SCPContext{
		XtID:           xtID,
		Request:        startSC.XtRequest,
		Slot:           startSC.Slot,
		SequenceNumber: startSC.XtSequenceNumber,
		MyTransactions: si.extractMyTransactions(startSC.XtRequest),
	}

	si.activeContexts[xtIDStr] = scpCtx

	si.log.Info().
		Str("xt_id", xtIDStr).
		Uint64("sequence", startSC.XtSequenceNumber).
		Int("my_txs", len(scpCtx.MyTransactions)).
		Msg("Started SCP context")

	return nil
}

func (si *SCPIntegration) HandleDecision(xtID *pb.XtID, decision bool) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	xtIDStr := xtID.Hex()

	scpCtx, exists := si.activeContexts[xtIDStr]
	if !exists {
		return fmt.Errorf("no SCP context found for xt_id %s", xtIDStr)
	}

	scpCtx.Decision = &decision

	si.log.Info().
		Str("xt_id", xtIDStr).
		Bool("decision", decision).
		Msg("SCP decision received")

	// Update block builder with decision for our chain's txs
	if si.blockBuilder != nil {
		if decision {
			_ = si.blockBuilder.AddSCPTransactions(xtIDStr, scpCtx.MyTransactions, true)
		} else {
			_ = si.blockBuilder.AddSCPTransactions(xtIDStr, nil, false)
		}
	}

	// Track decision for superset check and re-pooling prevention
	si.decidedXTs[xtIDStr] = &DecidedXT{
		XtIDBytes: scpCtx.XtID.Hash,
		Included:  decision,
	}

	// Clean up context after decision
	delete(si.activeContexts, xtIDStr)

	// If we were the last SCP instance, transition back to Free
	if len(si.activeContexts) == 0 && si.stateMachine.GetCurrentState() == StateBuildingLocked {
		// update last decided sequence for ordering enforcement
		si.lastDecidedSeq = scpCtx.SequenceNumber
		si.hasLastDecidedSeq = true
		return si.stateMachine.TransitionTo(StateBuildingFree, si.stateMachine.GetCurrentSlot(), "SCP completed")
	}

	return nil
}

func (si *SCPIntegration) extractMyTransactions(xtReq *pb.XTRequest) [][]byte {
	myTxs := make([][]byte, 0)

	for _, txReq := range xtReq.Transactions {
		if len(txReq.ChainId) == len(si.chainID) {
			match := true
			for i := range si.chainID {
				if txReq.ChainId[i] != si.chainID[i] {
					match = false
					break
				}
			}
			if match {
				myTxs = append(myTxs, txReq.Transaction...)
			}
		}
	}

	return myTxs
}

func (si *SCPIntegration) GetActiveContexts() map[string]*SCPContext {
	si.mu.RLock()
	defer si.mu.RUnlock()

	result := make(map[string]*SCPContext)
	for k, v := range si.activeContexts {
		result[k] = v
	}

	return result
}

// ResetForSlot clears per-slot SCP tracking
func (si *SCPIntegration) ResetForSlot(slot uint64) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.currentSlot = slot
	si.activeContexts = make(map[string]*SCPContext)
	si.decidedXTs = make(map[string]*DecidedXT)
	si.hasLastDecidedSeq = false
}

// GetIncludedXTsHex returns hex-encoded xtIDs decided to include in current slot
func (si *SCPIntegration) GetIncludedXTsHex() []string {
	si.mu.RLock()
	defer si.mu.RUnlock()
	out := make([]string, 0)
	for k, decided := range si.decidedXTs {
		if decided.Included {
			out = append(out, k)
		}
	}
	return out
}

// ShouldRejectXt returns true if the XT was decided against and should be rejected
func (si *SCPIntegration) ShouldRejectXt(xtID string) bool {
	si.mu.RLock()
	defer si.mu.RUnlock()
	decided, exists := si.decidedXTs[xtID]
	return exists && !decided.Included
}

// GetLastDecidedSequenceNumber returns the last decided sequence and whether it exists
func (si *SCPIntegration) GetLastDecidedSequenceNumber() (uint64, bool) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.lastDecidedSeq, si.hasLastDecidedSeq
}

// GetActiveCount returns the number of in-flight SCP instances
func (si *SCPIntegration) GetActiveCount() int {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return len(si.activeContexts)
}
