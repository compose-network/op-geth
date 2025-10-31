package xconsensus

import (
	"context"
	"github.com/compose-network/specs/compose"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

// Coordinator defines the consensus coordinator interface
type Coordinator interface {
	// Instance lifecycle
	StartInstance(ctx context.Context, from string, instance *sbcpproto.StartInstance) error
	RecordDecision(xtID compose.InstanceID, decision bool) error
	RecordMailboxMessage(circMessage *sbcpproto.MailboxMessage) error

	// Callbacks
	SetStartCallback(fn StartFn)
	SetVoteCallback(fn VoteFn)
	SetDecisionCallback(fn DecisionFn)
	SetBlockCallback(fn BlockFn)

	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Callback function types
type StartFn func(ctx context.Context, from string, instance *sbcpproto.StartInstance) error
type VoteFn func(ctx context.Context, instanceID *compose.InstanceID, vote bool) error
type DecisionFn func(ctx context.Context, xtID *compose.InstanceID, decision bool) error

// BlockFn sends a block plus committed xTs to the SP layer
type BlockFn func(ctx context.Context, block *types.Block, xtIDs []*compose.InstanceID) error

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
