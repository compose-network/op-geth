package consensus

import (
	"math/big"
	"sync"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/rs/zerolog"
)

// circMessageBuffer stores CIRC messages that arrive before transaction state exists
type circMessageBuffer struct {
	mu     sync.RWMutex
	buffer map[string][]*pb.CIRCMessage
	log    zerolog.Logger
}

// newCircMessageBuffer creates a new CIRC message buffer
func newCircMessageBuffer(log zerolog.Logger) *circMessageBuffer {
	return &circMessageBuffer{
		buffer: make(map[string][]*pb.CIRCMessage),
		log:    log.With().Str("component", "circ-buffer").Logger(),
	}
}

// Add buffers a CIRC message for later processing
func (b *circMessageBuffer) Add(circMessage *pb.CIRCMessage) {
	xtIDHex := circMessage.XtId.Hex()

	b.mu.Lock()
	b.buffer[xtIDHex] = append(b.buffer[xtIDHex], circMessage)
	bufferedCount := len(b.buffer[xtIDHex])
	b.mu.Unlock()

	b.log.Debug().
		Str("xt_id", xtIDHex).
		Int("buffered_count", bufferedCount).
		Msg("Buffered CIRC message")
}

// Flush retrieves and removes all buffered messages for a transaction
func (b *circMessageBuffer) Flush(xtID *pb.XtID) []*pb.CIRCMessage {
	xtIDHex := xtID.Hex()

	b.mu.Lock()
	buffered := b.buffer[xtIDHex]
	delete(b.buffer, xtIDHex)
	b.mu.Unlock()

	if len(buffered) > 0 {
		b.log.Info().
			Str("xt_id", xtIDHex).
			Int("flushed_count", len(buffered)).
			Msg("Flushed buffered CIRC messages")
	}

	return buffered
}

// ProcessBuffered flushes and processes buffered CIRC messages into transaction state
func (b *circMessageBuffer) ProcessBuffered(xtID *pb.XtID, state *TwoPCState, log zerolog.Logger) {
	buffered := b.Flush(xtID)
	if len(buffered) == 0 {
		return
	}

	xtIDHex := xtID.Hex()

	for _, msg := range buffered {
		sourceChainID := ChainKeyBytes(msg.SourceChain)

		state.mu.Lock()
		if _, isParticipant := state.ParticipatingChains[sourceChainID]; isParticipant {
			messages := state.CIRCMessages[sourceChainID]
			state.CIRCMessages[sourceChainID] = append(messages, msg)

			sourceChainIDInt := new(big.Int).SetBytes(msg.SourceChain)
			log.Info().
				Str("xt_id", xtIDHex).
				Str("chain_id", sourceChainIDInt.String()).
				Msg("Recorded buffered CIRC message")
		}
		state.mu.Unlock()
	}
}
