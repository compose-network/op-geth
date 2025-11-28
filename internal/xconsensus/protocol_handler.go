package xconsensus

import (
	"context"
	"fmt"

	"github.com/compose-network/specs/compose"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	"github.com/rs/zerolog"
)

// InstanceHandler defines the interface for instanceproto protocol message handling
type InstanceHandler interface {
	// Handle processes SCP protocol messages
	Handle(ctx context.Context, from string, msg *sbcpproto.Message) error

	// CanHandle returns true if this handler can process the message
	CanHandle(msg *sbcpproto.Message) bool
}

// instanceHandler implements the SCP protocol handler
type instanceHandler struct {
	coordinator Coordinator
	log         zerolog.Logger
}

// NewInstanceHandler creates a new SCP protocol handler
func NewInstanceHandler(coordinator Coordinator, log zerolog.Logger) InstanceHandler {
	return &instanceHandler{
		coordinator: coordinator,
		log:         log.With().Str("protocol", "SCP").Logger(),
	}
}

// Handle processes Instance protocol messages
func (h *instanceHandler) Handle(ctx context.Context, from string, msg *sbcpproto.Message) error {
	msgType := ClassifyMessage(msg)
	if msgType == MsgUnknown {
		return fmt.Errorf("unknown SCP message type from %s", from)
	}

	h.log.Debug().
		Str("from", from).
		Str("message_type", msgType.String()).
		Msg("Handling SCP message")

	switch msgType {
	case MsgStartInstance:
		return h.coordinator.StartInstance(ctx, from, msg.GetStartInstance())

	case MsgMailboxMessage:
		mailboxMsg := msg.GetMailboxMessage()
		return h.coordinator.RecordMailboxMessage(mailboxMsg)

	case MsgDecided:
		decided := msg.GetDecided()
		instanceID, err := BytesToInstanceID(decided.InstanceId)
		if err != nil {
			return err
		}
		return h.coordinator.RecordDecision(instanceID, decided.Decision)

	default:
		return fmt.Errorf("unhandled SCP message type %s from %s", msgType.String(), from)
	}
}

func BytesToInstanceID(b []byte) (compose.InstanceID, error) {
	var id compose.InstanceID
	if len(b) != len(id) {
		return id, fmt.Errorf("invalid length: got %d, want %d", len(b), len(id))
	}
	copy(id[:], b)
	return id, nil
}

// CanHandle returns true if this handler can process the message
func (h *instanceHandler) CanHandle(msg *sbcpproto.Message) bool {
	return IsInstanceProtoMessage(msg)
}
