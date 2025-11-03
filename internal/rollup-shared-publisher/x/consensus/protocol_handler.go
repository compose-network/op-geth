package consensus

import (
	"context"
	"fmt"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/rs/zerolog"
)

// ProtocolHandler defines the interface for SCP protocol message handling
type ProtocolHandler interface {
	// Handle processes SCP protocol messages
	Handle(ctx context.Context, from string, msg *pb.Message) error

	// CanHandle returns true if this handler can process the message
	CanHandle(msg *pb.Message) bool

	// GetProtocolName returns the protocol name for logging/debugging
	GetProtocolName() string
}

// protocolHandler implements the SCP protocol handler
type protocolHandler struct {
	coordinator Coordinator
	log         zerolog.Logger
}

// NewProtocolHandler creates a new SCP protocol handler
func NewProtocolHandler(coordinator Coordinator, log zerolog.Logger) ProtocolHandler {
	return &protocolHandler{
		coordinator: coordinator,
		log:         log.With().Str("protocol", "SCP").Logger(),
	}
}

// Handle processes SCP protocol messages
func (h *protocolHandler) Handle(ctx context.Context, from string, msg *pb.Message) error {
	msgType := ClassifyMessage(msg)
	if msgType == MsgUnknown {
		return fmt.Errorf("unknown SCP message type from %s", from)
	}

	h.log.Debug().
		Str("from", from).
		Str("message_type", msgType.String()).
		Msg("Handling SCP message")

	switch msgType {
	case MsgXTRequest:
		xtReq := msg.GetXtRequest()
		return h.coordinator.StartTransaction(ctx, from, xtReq)

	case MsgVote:
		vote := msg.GetVote()
		chainID := ChainKeyBytes(vote.SenderChainId)
		_, err := h.coordinator.RecordVote(vote.XtId, chainID, vote.Vote)
		return err

	case MsgDecided:
		decided := msg.GetDecided()
		return h.coordinator.RecordDecision(decided.XtId, decided.Decision)

	case MsgCIRCMessage:
		circMsg := msg.GetCircMessage()
		return h.coordinator.RecordCIRCMessage(circMsg)

	case MsgUnknown:
		return fmt.Errorf("unhandled SCP message type %s from %s", msgType.String(), from)

	default:
		return fmt.Errorf("unhandled SCP message type %s from %s", msgType.String(), from)
	}
}

// CanHandle returns true if this handler can process the message
func (h *protocolHandler) CanHandle(msg *pb.Message) bool {
	return IsSCPMessage(msg)
}

// GetProtocolName returns the protocol name for logging/debugging
func (h *protocolHandler) GetProtocolName() string {
	return "SCP"
}
