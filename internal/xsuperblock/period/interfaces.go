package period

import (
	"context"

	sbcpproto "github.com/compose-network/specs/compose/proto"
)

// PeriodHandler defines the interface for SBCP protocol message handling
type PeriodHandler interface {
	// Handle processes SBCP protocol messages
	Handle(ctx context.Context, from string, msg *sbcpproto.Message) error

	// CanHandle returns true if this periodHandler can process the message
	CanHandle(msg *sbcpproto.Message) bool
}

// MessageHandler defines handlers for specific SBCP message types
type MessageHandler interface {
	OnStartPeriod(ctx context.Context, from string, msg *sbcpproto.StartPeriod) error
	OnRollback(ctx context.Context, from string, msg *sbcpproto.Rollback) error
}

// Validator defines message validation interface
type Validator interface {
	ValidateStartPeriod(msg *sbcpproto.StartPeriod) error
	ValidateRollback(msg *sbcpproto.Rollback) error
}
