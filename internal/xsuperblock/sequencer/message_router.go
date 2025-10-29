package sequencer

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/internal/xconsensus"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/xsuperblock/protocol"
	"time"

	"github.com/rs/zerolog"
)

type MessageRouter struct {
	sbcpHandler protocol.Handler
	scpHandler  xconsensus.ProtocolHandler
	log         zerolog.Logger

	// Metrics
	routingStats map[ProtocolType]int64
}

func NewMessageRouter(
	sbcpHandler protocol.Handler,
	scpHandler xconsensus.ProtocolHandler,
	log zerolog.Logger,
) *MessageRouter {
	return &MessageRouter{
		sbcpHandler:  sbcpHandler,
		scpHandler:   scpHandler,
		log:          log.With().Str("component", "message_router").Logger(),
		routingStats: make(map[ProtocolType]int64),
	}
}

func (mr *MessageRouter) Route(ctx context.Context, from string, msg *pb.Message) error {
	start := time.Now()

	// Classify the protocol
	protocolType := ClassifyProtocol(msg)
	if protocolType == ProtocolUnknown {
		return fmt.Errorf("unknown protocol for message from %s", from)
	}

	// Update metrics
	mr.routingStats[protocolType]++

	// Route to the appropriate handler
	var err error
	switch protocolType {
	case ProtocolSBCP:
		if !mr.sbcpHandler.CanHandle(msg) {
			return fmt.Errorf("SBCP handler cannot process message from %s", from)
		}

		mr.log.Debug().
			Str("from", from).
			Str("protocol", protocolType.String()).
			Str("message_type", GetMessageTypeString(msg)).
			Msg("Routing to SBCP handler")

		err = mr.sbcpHandler.Handle(ctx, from, msg)

	case ProtocolSCP:
		if !mr.scpHandler.CanHandle(msg) {
			return fmt.Errorf("SCP handler cannot process message from %s", from)
		}

		mr.log.Debug().
			Str("from", from).
			Str("protocol", protocolType.String()).
			Str("message_type", GetMessageTypeString(msg)).
			Msg("Routing to SCP handler")

		err = mr.scpHandler.Handle(ctx, from, msg)

	case ProtocolUnknown:
		return fmt.Errorf("no handler available for protocol %s from %s", protocolType.String(), from)

	default:
		return fmt.Errorf("no handler available for protocol %s from %s", protocolType.String(), from)
	}

	// Log processing time
	duration := time.Since(start)
	if err != nil {
		mr.log.Error().
			Err(err).
			Str("from", from).
			Str("protocol", protocolType.String()).
			Str("message_type", GetMessageTypeString(msg)).
			Dur("duration", duration).
			Msg("Message routing failed")
	} else {
		mr.log.Debug().
			Str("from", from).
			Str("protocol", protocolType.String()).
			Str("message_type", GetMessageTypeString(msg)).
			Dur("duration", duration).
			Msg("Message routed successfully")
	}

	return err
}

// GetStats returns routing statistics
func (mr *MessageRouter) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"sbcp_handler_available": mr.sbcpHandler != nil,
		"scp_handler_available":  mr.scpHandler != nil,
		"routing_counts":         map[string]int64{},
	}

	// Add routing counts
	routingCounts := make(map[string]int64)
	for protocol, count := range mr.routingStats {
		routingCounts[protocol.String()] = count
	}
	stats["routing_counts"] = routingCounts

	return stats
}

// Reset clears routing statistics
func (mr *MessageRouter) Reset() {
	mr.routingStats = make(map[ProtocolType]int64)
	mr.log.Debug().Msg("Message router statistics reset")
}
