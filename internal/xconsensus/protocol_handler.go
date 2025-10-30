package xconsensus

import (
	"context"
	"fmt"
	"github.com/compose-network/specs/compose"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	"github.com/rs/zerolog"
)

// SCPHandler defines the interface for SCP protocol message handling
// SCPHandler defines the interface for SCP protocol message handling
type SCPHandler interface {
	// Handle processes SCP protocol messages
	Handle(ctx context.Context, from string, msg *sbcpproto.Message) error

	// CanHandle returns true if this handler can process the message
	CanHandle(msg *sbcpproto.Message) bool

	// GetProtocolName returns the protocol name for logging/debugging
	GetProtocolName() string
}

// scpHandler implements the SCP protocol handler
type scpHandler struct {
	coordinator Coordinator
	log         zerolog.Logger
}

// NewSCPHandler creates a new SCP protocol handler
func NewSCPHandler(coordinator Coordinator, log zerolog.Logger) SCPHandler {
	return &scpHandler{
		coordinator: coordinator,
		log:         log.With().Str("protocol", "SCP").Logger(),
	}
}

// Handle processes SCP protocol messages
func (h *scpHandler) Handle(ctx context.Context, from string, msg *sbcpproto.Message) error {
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
		xtReq := msg.GetXtRequest()
		return h.coordinator.StartTransaction(ctx, from, xtReq)
	case MsgDecided:
		decided := msg.GetDecided()
		instanceID, err := BytesToInstanceID(decided.InstanceId)
		if err != nil {
			return err
		}
		return h.coordinator.RecordDecision(instanceID, decided.Decision)

	case MsgMailboxMessage:
		mailboxMsg := msg.GetMailboxMessage()
		return h.coordinator.RecordMailboxMessage(mailboxMsg)

	case MsgUnknown:
		return fmt.Errorf("unhandled SCP message type %s from %s", msgType.String(), from)

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
func (h *scpHandler) CanHandle(msg *sbcpproto.Message) bool {
	return IsSCPMessage(msg)
}

// GetProtocolName returns the protocol name for logging/debugging
func (h *scpHandler) GetProtocolName() string {
	return "SCP"
}
