package xbootstrap

import (
	"context"
	"errors"

	"github.com/compose-network/specs/compose"
	"github.com/compose-network/specs/compose/proto"
	"github.com/ethereum/go-ethereum/internal/xtransport"
	"github.com/rs/zerolog"
)

// Messanger ships SBCP-side messages from the sequencer to the Shared Publisher.
// It keeps a handle to the SP client so Sequencer calls can forward traffic
// without the higher layers wiring every call manually.
type Messanger struct {
	client xtransport.Client
	log    zerolog.Logger
}

func NewMessanger(log zerolog.Logger) *Messanger {
	return &Messanger{log: log.With().Str("component", "messenger").Logger()}
}

// SetClient is called by the bootstrapper once the SP client has been created.
func (m *Messanger) SetClient(c xtransport.Client) {
	m.client = c
}

// SendProof forwards a per-rollup proof to the Shared Publisher.
func (m *Messanger) SendProof(ctx context.Context, periodID compose.PeriodID, superblockNumber compose.SuperblockNumber, proof []byte) error {
	if m.client == nil {
		m.log.Warn().Uint64("period_id", uint64(periodID)).Msg("dropping proof: SP client not set")
		return errors.New("SP client not set")
	}

	protoReq := &proto.Proof{
		PeriodId:         uint64(periodID),
		SuperblockNumber: uint64(superblockNumber),
		ProofData:        proof,
	}

	return m.client.Send(ctx, &proto.Message{
		Payload: &proto.Message_Proof{
			Proof: protoReq,
		},
	})
}

// ForwardRequest forwards a user XTRequest to the Shared Publisher.
func (m *Messanger) ForwardRequest(ctx context.Context, request compose.XTRequest) error {
	if m.client == nil {
		m.log.Warn().Msg("dropping XTRequest: SP client not set")
		return errors.New("SP client not set")
	}

	protoReq := &proto.XTRequest{
		TransactionRequests: make([]*proto.TransactionRequest, 0, len(request.Transactions)),
	}
	for _, tr := range request.Transactions {
		p := &proto.TransactionRequest{
			ChainId:     uint64(tr.ChainID),
			Transaction: make([][]byte, len(tr.Transactions)),
		}
		for i, tx := range tr.Transactions {
			p.Transaction[i] = append(p.Transaction[i], tx...)
		}
		protoReq.TransactionRequests = append(protoReq.TransactionRequests, p)
	}

	return m.client.Send(ctx, &proto.Message{
		Payload: &proto.Message_XtRequest{
			XtRequest: protoReq,
		},
	})
}
