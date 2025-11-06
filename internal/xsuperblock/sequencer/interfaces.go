package sequencer

import (
	"context"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	periodproto "github.com/compose-network/specs/compose/sbcp"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/xconsensus"
	"github.com/ethereum/go-ethereum/internal/xconsensus/instanceproto"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
)

// CoordinatorCallbacks defines callback functions for cross-component communication
type CoordinatorCallbacks struct {
	SendCIRC func(ctx context.Context, circ *pb.CIRCMessage) error
	// SimulateAndVote runs local-chain simulation for the provided XT request
	// and returns whether the local transactions are ready to commit (vote=true)
	// or not (vote=false). This callback is used by the coordinator during
	// StartSC handling and is implemented by the host SDK (e.g., geth backend).
	SimulateAndVote func(ctx context.Context, xtReq *pb.XTRequest, xtID *pb.XtID) (bool, error)
	// CleanupAbortedTransaction is called when an SCP instanceproto decides to abort,
	// allowing the execution layer to immediately remove staged transactions from
	// its pending pool. This ensures atomic exclude behavior when blocks are built
	// before RequestSeal arrives.
	CleanupAbortedTransaction func(ctx context.Context, xtID *pb.XtID) error
}

// BlockLifecycleManager handles block building lifecycle events
type BlockLifecycleManager interface {
	OnBlockBuildingStart(ctx context.Context, slot uint64) error
	OnBlockBuildingComplete(ctx context.Context, block *types.Block, success bool) error
}

// TransactionManager handles transaction preparation and ordering
type TransactionManager interface {
	PrepareTransactionsForBlock(ctx context.Context, slot uint64) error
	//GetOrderedTransactionsForBlock(ctx context.Context) ([]*pb.TransactionRequest, error)
}

// CallbackManager handles callback registration and miner notifications
type CallbackManager interface {
	SetCallbacks(callbacks CoordinatorCallbacks)
}

// Coordinator defines the sequencer coordinator interface
type Coordinator interface {
	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	// Message handling
	HandleMessage(ctx context.Context, from string, msg *sbcpproto.Message) error

	// Consensus access
	ConsensusCoord() xconsensus.Coordinator
	PeriodSequencer() periodproto.Sequencer
	InstanceSequencer() instanceproto.Sequencer

	// SDK access
	BlockLifecycleManager
	TransactionManager
	CallbackManager
}

// MessageRouterInterface for routing messages
type MessageRouterInterface interface {
	Route(ctx context.Context, from string, msg *sbcpproto.Message) error
}
