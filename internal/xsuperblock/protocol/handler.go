package protocol

import (
	"context"
	"fmt"

	sbcpproto "github.com/compose-network/specs/compose/proto"

	"github.com/rs/zerolog"
)

// sbcpHandler implements the SBCP protocol Handler interface
type sbcpHandler struct {
	validator Validator
	log       zerolog.Logger
}

// NewSBCPHandler creates a new SBCP protocol sbcpHandler
func NewSBCPHandler(validator Validator, log zerolog.Logger) Handler {
	return &sbcpHandler{
		validator: validator,
		log:       log.With().Str("protocol", "SBCP").Logger(),
	}
}

// CanHandle returns true if this sbcpHandler can process the message.
func (h *sbcpHandler) CanHandle(msg *sbcpproto.Message) bool {
	return IsSBCPMessage(msg)
}

// GetProtocolName returns the protocol name.
func (h *sbcpHandler) GetProtocolName() string {
	return "SBCP"
}

// Handle processes SBCP protocol messages
func (h *sbcpHandler) Handle(ctx context.Context, from string, msg *sbcpproto.Message) error {
	msgType, ok := ClassifyMessage(msg)
	if !ok {
		return fmt.Errorf("invalid or unsupported SBCP message from %s", from)
	}

	h.log.Debug().
		Str("from", from).
		Str("message_type", msgType.String()).
		Msg("Handling SBCP message")

	if h.validator == nil {
		return h.handleMessage(ctx, from, msgType, msg)
	}

	if err := h.validateMessage(msgType, msg); err != nil {
		return fmt.Errorf("validation failed for %s from %s: %w", msgType, from, err)
	}

	return h.handleMessage(ctx, from, msgType, msg)
}

// validateMessage validates the message based on its type
func (h *sbcpHandler) validateMessage(msgType MessageType, msg *sbcpproto.Message) error {
	return fmt.Errorf("no validator for message type %s", msgType)
}

// handleMessage routes the message to the appropriate sbcpHandler
func (h *sbcpHandler) handleMessage(ctx context.Context, from string, msgType MessageType, msg *sbcpproto.Message) error {
	return fmt.Errorf("no sbcpHandler for message type %s", msgType)
}
