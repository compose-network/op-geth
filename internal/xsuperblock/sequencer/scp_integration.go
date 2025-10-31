package sequencer

import (
	"fmt"
	"github.com/compose-network/specs/compose"
	"github.com/ethereum/go-ethereum/internal/xconsensus"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"sync"

	"github.com/rs/zerolog"
)

type SCPContext struct {
	XtID           *pb.XtID
	Request        *pb.XTRequest
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

func (si *SCPIntegration) HandleDecision(xtID *compose.InstanceID, decision bool) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	xtIDStr := xtID.String()

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
