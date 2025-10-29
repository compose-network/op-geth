package sequencer

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/internal/xconsensus"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"sync"

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

type SCPIntegration struct {
	mu        sync.RWMutex
	chainID   []byte
	consensus xconsensus.Coordinator
	log       zerolog.Logger

	activeContexts map[string]*SCPContext // xtID -> context

	// per-slot tracked state
	includedXTs map[string][]byte // hex xtID -> raw xtID bytes for this slot (decided=true)
	// last decided sequence number for monotonic StartSC enforcement
	lastDecidedSeq    uint64
	hasLastDecidedSeq bool
	currentSlot       uint64
}

func NewSCPIntegration(
	chainID []byte,
	consensus xconsensus.Coordinator,
	log zerolog.Logger,
) *SCPIntegration {
	return &SCPIntegration{
		chainID:        chainID,
		consensus:      consensus,
		log:            log.With().Str("component", "scp_integration").Logger(),
		activeContexts: make(map[string]*SCPContext),
		includedXTs:    make(map[string][]byte),
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

	// Track included XTs for superset check
	if decision {
		si.includedXTs[xtIDStr] = scpCtx.XtID.Hash
	} else {
		delete(si.includedXTs, xtIDStr)
	}

	// Clean up context after decision
	delete(si.activeContexts, xtIDStr)

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
	si.includedXTs = make(map[string][]byte)
	si.hasLastDecidedSeq = false
}

// GetIncludedXTsHex returns hex-encoded xtIDs decided to include in current slot
func (si *SCPIntegration) GetIncludedXTsHex() []string {
	si.mu.RLock()
	defer si.mu.RUnlock()
	out := make([]string, 0, len(si.includedXTs))
	for k := range si.includedXTs {
		out = append(out, k)
	}
	return out
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
