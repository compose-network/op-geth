package sequencer

import (
	"context"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	"github.com/ethereum/go-ethereum/internal/xconsensus"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
)

// MinerNotifier defines the interface for notifying miner about sequencer events
type MinerNotifier interface {
	NotifySlotStart(startSlot *pb.StartSlot) error
	NotifyRequestSeal(ctx context.Context, requestSeal *pb.RequestSeal) error
}

// CoordinatorCallbacks defines callback functions for cross-component communication
type CoordinatorCallbacks struct {
	SendCIRC func(ctx context.Context, circ *pb.CIRCMessage) error
	// SimulateAndVote runs local-chain simulation for the provided XT request
	// and returns whether the local transactions are ready to commit (vote=true)
	// or not (vote=false). This callback is used by the coordinator during
	// StartSC handling and is implemented by the host SDK (e.g., geth backend).
	SimulateAndVote func(ctx context.Context, xtReq *pb.XTRequest, xtID *pb.XtID) (bool, error)
	// CleanupAbortedTransaction is called when an SCP instance decides to abort,
	// allowing the execution layer to immediately remove staged transactions from
	// its pending pool. This ensures atomic exclude behavior when blocks are built
	// before RequestSeal arrives.
	CleanupAbortedTransaction func(ctx context.Context, xtID *pb.XtID) error
}

// BlockLifecycleManager handles block building lifecycle events
type BlockLifecycleManager interface {
	OnBlockBuildingStart(ctx context.Context, slot uint64) error
	OnBlockBuildingComplete(ctx context.Context, block *pb.L2Block, success bool) error
}

// TransactionManager handles transaction preparation and ordering
type TransactionManager interface {
	PrepareTransactionsForBlock(ctx context.Context, slot uint64) error
	//GetOrderedTransactionsForBlock(ctx context.Context) ([]*pb.TransactionRequest, error)
}

// CallbackManager handles callback registration and miner notifications
type CallbackManager interface {
	SetCallbacks(callbacks CoordinatorCallbacks)
	SetMinerNotifier(notifier MinerNotifier)
}

// Coordinator defines the sequencer coordinator interface
type Coordinator interface {
	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	// Message handling
	HandleMessage(ctx context.Context, from string, msg *sbcpproto.Message) error

	// Consensus access
	Consensus() xconsensus.Coordinator

	// SDK access
	BlockLifecycleManager
	TransactionManager
	CallbackManager
}

// MessageRouterInterface for routing messages
type MessageRouterInterface interface {
	Route(ctx context.Context, from string, msg *sbcpproto.Message) error
}

// SCPIntegrationInterface for SCP coordination
type SCPIntegrationInterface interface {
	HandleStartSC(ctx context.Context, startSC *pb.StartSC) error
	HandleDecision(xtID *pb.XtID, decision bool) error
	GetActiveContexts() map[string]*SCPContext
	ResetForSlot(slot uint64)
	GetIncludedXTsHex() []string
	GetLastDecidedSequenceNumber() (uint64, bool)
	GetActiveCount() int
}
