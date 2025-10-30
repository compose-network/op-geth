package xconsensus

import (
	"context"
	"github.com/compose-network/specs/compose"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

// Coordinator defines the consensus coordinator interface
type Coordinator interface {
	// Transaction lifecycle
	StartTransaction(ctx context.Context, from string, xtReq *sbcpproto.XTRequest) error
	RecordDecision(xtID compose.InstanceID, decision bool) error

	RecordMailboxMessage(circMessage *sbcpproto.MailboxMessage) error

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
	NodeID  string
	Timeout time.Duration
}

// DefaultConfig returns sensible defaults
func DefaultConfig(nodeID string) Config {
	return Config{
		NodeID:  nodeID,
		Timeout: time.Minute,
	}
}
