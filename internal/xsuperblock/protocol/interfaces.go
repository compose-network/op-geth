package protocol

import (
	"context"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
)

// Handler defines the interface for SBCP protocol message handling
type Handler interface {
	// Handle processes SBCP protocol messages
	Handle(ctx context.Context, from string, msg *pb.Message) error

	// CanHandle returns true if this handler can process the message
	CanHandle(msg *pb.Message) bool

	// GetProtocolName returns the protocol name for logging/debugging
	GetProtocolName() string
}

// MessageHandler defines handlers for specific SBCP message types
type MessageHandler interface {
	// TODO: handle specific messages like StartPeriod
}

// Validator defines message validation interface
type Validator interface {
}
