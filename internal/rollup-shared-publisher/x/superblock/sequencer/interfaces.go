package sequencer

import (
	"context"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/consensus"
)

// MinerNotifier defines the interface for notifying miner about sequencer events
type MinerNotifier interface {
	NotifySlotStart(startSlot *pb.StartSlot) error
	NotifyRequestSeal(ctx context.Context, requestSeal *pb.RequestSeal) error
	NotifyStateChange(from, to State, slot uint64) error
}

// CoordinatorCallbacks defines callback functions for cross-component communication
type CoordinatorCallbacks struct {
	SendCIRC func(ctx context.Context, circ *pb.CIRCMessage) error
	// SimulateAndVote runs local-chain simulation for the provided XT request
	// and returns whether the local transactions are ready to commit (vote=true)
	// or not (vote=false). This callback is used by the coordinator during
	// StartSC handling and is implemented by the host SDK (e.g., geth backend).
	SimulateAndVote func(ctx context.Context, xtReq *pb.XTRequest, xtID *pb.XtID) (bool, error)
}

// BlockLifecycleManager handles block building lifecycle events
type BlockLifecycleManager interface {
	OnBlockBuildingStart(ctx context.Context, slot uint64) error
	OnBlockBuildingComplete(ctx context.Context, block *pb.L2Block, success bool) error
}

// TransactionManager handles transaction preparation and ordering
type TransactionManager interface {
	PrepareTransactionsForBlock(ctx context.Context, slot uint64) error
	GetOrderedTransactionsForBlock(ctx context.Context) ([]*pb.TransactionRequest, error)
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
	HandleMessage(ctx context.Context, from string, msg *pb.Message) error

	// State queries
	GetCurrentSlot() uint64
	GetState() State
	GetStats() map[string]interface{}
	GetActiveSCPInstanceCount() int

	// Consensus access
	Consensus() consensus.Coordinator

	// SDK access
	BlockLifecycleManager
	TransactionManager
	CallbackManager
}

// BlockBuilderInterface for L2 block construction
type BlockBuilderInterface interface {
	StartSlot(slot uint64, request *pb.L2BlockRequest) error
	AddLocalTransaction(tx []byte) error
	AddSCPTransactions(xtID string, txs [][]byte, decision bool) error
	AddCIRCMessage(msg *pb.CIRCMessage) error
	SealBlock(includedXTs [][]byte) (*pb.L2Block, error)
	GetDraftStats() map[string]interface{}
	Reset()
}

// StateMachineInterface for sequencer FSM
type StateMachineInterface interface {
	GetCurrentState() State
	GetCurrentSlot() uint64
	TransitionTo(newState State, slot uint64, reason string) error
	GetTransitions() []StateTransition
	Reset()
}

// MessageRouterInterface for routing messages
type MessageRouterInterface interface {
	Route(ctx context.Context, from string, msg *pb.Message) error
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
