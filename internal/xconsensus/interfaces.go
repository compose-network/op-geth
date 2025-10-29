package xconsensus

import (
	"context"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

// Coordinator defines the consensus coordinator interface
type Coordinator interface {
	// Transaction lifecycle
	StartTransaction(ctx context.Context, from string, xtReq *pb.XTRequest) error
	RecordVote(xtID *pb.XtID, chainID string, vote bool) (DecisionState, error)
	RecordDecision(xtID *pb.XtID, decision bool) error
	GetTransactionState(xtID *pb.XtID) (DecisionState, error)
	GetActiveTransactions() []*pb.XtID
	GetState(xtID *pb.XtID) (*TwoPCState, bool)

	// CIRC message handling
	RecordCIRCMessage(circMessage *pb.CIRCMessage) error
	ConsumeCIRCMessage(xtID *pb.XtID, sourceChainID string) (*pb.CIRCMessage, error)

	// Callbacks
	SetStartCallback(fn StartFn)
	SetVoteCallback(fn VoteFn)
	SetDecisionCallback(fn DecisionFn)
	SetBlockCallback(fn BlockFn)

	// OnL2BlockCommitted is called by sequencer SBCP path when a pb.L2Block is sealed and submitted
	OnL2BlockCommitted(ctx context.Context, block *pb.L2Block) error

	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Callback function types
type StartFn func(ctx context.Context, from string, xtReq *pb.XTRequest) error
type VoteFn func(ctx context.Context, xtID *pb.XtID, vote bool) error
type DecisionFn func(ctx context.Context, xtID *pb.XtID, decision bool) error

// BlockFn sends a block plus committed xTs to the SP layer
type BlockFn func(ctx context.Context, block *types.Block, xtIDs []*pb.XtID) error

// Config holds coordinator configuration
type Config struct {
	NodeID   string
	IsLeader bool
	Timeout  time.Duration
	Role     Role
}

// DefaultConfig returns sensible defaults
func DefaultConfig(nodeID string) Config {
	return Config{
		NodeID:   nodeID,
		IsLeader: true,
		Timeout:  time.Minute,
		Role:     Leader,
	}
}
