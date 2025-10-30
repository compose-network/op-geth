package protocol

import (
	"context"

	sbcpproto "github.com/compose-network/specs/compose/proto"
)

// Handler defines the interface for SBCP protocol message handling
type Handler interface {
	// Handle processes SBCP protocol messages
	Handle(ctx context.Context, from string, msg *sbcpproto.Message) error

	// CanHandle returns true if this sbcpHandler can process the message
	CanHandle(msg *sbcpproto.Message) bool

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
