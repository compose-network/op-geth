package sequencer

import (
	"context"
	"fmt"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	"github.com/ethereum/go-ethereum/internal/xsuperblock/period"
	"time"

	"github.com/ethereum/go-ethereum/internal/xconsensus"
	"github.com/rs/zerolog"
)

type MessageRouter struct {
	periodHandler   period.PeriodHandler
	instanceHandler xconsensus.InstanceHandler
	log             zerolog.Logger
}

func NewMessageRouter(
	periodHandler period.PeriodHandler,
	instanceHandler xconsensus.InstanceHandler,
	log zerolog.Logger,
) *MessageRouter {
	return &MessageRouter{
		periodHandler:   periodHandler,
		instanceHandler: instanceHandler,
		log:             log.With().Str("component", "message_router").Logger(),
	}
}

func (mr *MessageRouter) Route(ctx context.Context, from string, msg *sbcpproto.Message) error {
	start := time.Now()

	protocolType := ClassifyProtocol(msg)
	if protocolType == ProtocolUnknown {
		return fmt.Errorf("unknown protocol for message from %s", from)
	}

	mr.log.Debug().
		Str("from", from).
		Str("protocol", protocolType.String()).
		Str("message_type", LogMessageTypeString(msg)).
		Msgf("Routing to %s handler", protocolType)

	var err error
	switch protocolType {
	case PeriodProtocol:
		if !mr.periodHandler.CanHandle(msg) {
			return fmt.Errorf("Period proto handler cannot process message from %s", from)
		}
		err = mr.periodHandler.Handle(ctx, from, msg)

	case InstanceProtocol:
		if !mr.instanceHandler.CanHandle(msg) {
			return fmt.Errorf("Instance proto handler cannot process message from %s", from)
		}
		err = mr.instanceHandler.Handle(ctx, from, msg)

	default:
		return fmt.Errorf("no handler available for protocol %s from %s", protocolType.String(), from)
	}

	duration := time.Since(start)
	if err != nil {
		mr.log.Error().
			Err(err).
			Str("from", from).
			Str("protocol", protocolType.String()).
			Str("message_type", LogMessageTypeString(msg)).
			Dur("duration", duration).
			Msg("Message routing failed")
	} else {
		mr.log.Debug().
			Str("from", from).
			Str("protocol", protocolType.String()).
			Str("message_type", LogMessageTypeString(msg)).
			Dur("duration", duration).
			Msg("Message routed successfully")
	}

	return err
}
