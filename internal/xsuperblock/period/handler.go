package period

import (
	"context"
	"fmt"

	sbcpproto "github.com/compose-network/specs/compose/proto"

	"github.com/rs/zerolog"
)

// periodHandler implements the SBCP protocol PeriodHandler interface
type periodHandler struct {
	validator Validator
	log       zerolog.Logger
}

// NewPeriodHandler creates a new SBCP protocol periodHandler
func NewPeriodHandler(validator Validator, log zerolog.Logger) PeriodHandler {
	return &periodHandler{
		validator: validator,
		log:       log.With().Str("protocol", "SBCP").Logger(),
	}
}

// CanHandle returns true if this periodHandler can process the message.
func (h *periodHandler) CanHandle(msg *sbcpproto.Message) bool {
	return IsPeriodProtoMessage(msg)
}

// Handle processes SBCP protocol messages
func (h *periodHandler) Handle(ctx context.Context, from string, msg *sbcpproto.Message) error {
	msgType, ok := ClassifyMessage(msg)
	if !ok {
		return fmt.Errorf("invalid or unsupported SBCP message from %s", from)
	}

	h.log.Debug().
		Str("from", from).
		Str("message_type", msgType.String()).
		Msg("Handling SBCP message")

	if h.validator != nil {
		if err := h.validateMessage(msgType, msg); err != nil {
			return fmt.Errorf("validation failed for %s from %s: %w", msgType, from, err)
		}
	}

	return h.handleMessage(ctx, from, msgType, msg)
}

// validateMessage validates the message based on its type
func (h *periodHandler) validateMessage(msgType MessageType, msg *sbcpproto.Message) error {
	return fmt.Errorf("no validator for message type %s", msgType)
}

// handleMessage routes the message to the appropriate periodHandler
func (h *periodHandler) handleMessage(ctx context.Context, from string, msgType MessageType, msg *sbcpproto.Message) error {
	return fmt.Errorf("no periodHandler for message type %s", msgType)
}
