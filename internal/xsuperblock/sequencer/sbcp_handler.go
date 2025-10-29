package sequencer

import (
	"github.com/ethereum/go-ethereum/internal/xsuperblock/protocol"
	"github.com/rs/zerolog"
)

type sbcpHandler struct {
	coordinator *SequencerCoordinator
	log         zerolog.Logger
}

// NewSBCPHandler creates a new SBCP message handler
func NewSBCPHandler(coordinator *SequencerCoordinator, log zerolog.Logger) protocol.MessageHandler {
	return &sbcpHandler{
		coordinator: coordinator,
		log:         log.With().Str("component", "sbcp_handler").Logger(),
	}
}
