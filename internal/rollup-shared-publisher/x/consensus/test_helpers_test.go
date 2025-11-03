package consensus

import (
	"context"
	"io"
	"testing"
	"time"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/rs/zerolog"
)

func newTestCoordinator(t *testing.T, role Role) *coordinator {
	t.Helper()

	logger := zerolog.New(io.Discard)
	cfg := Config{
		NodeID:  "test-node",
		Timeout: 50 * time.Millisecond,
		Role:    role,
	}

	coordIface := NewWithMetrics(logger, cfg, NewNoOpMetrics())
	coord := coordIface.(*coordinator)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if err := coord.Stop(ctx); err != nil {
			t.Fatalf("failed to stop coordinator: %v", err)
		}
	})

	return coord
}

func buildXTRequest(chainIDs ...[]byte) *pb.XTRequest {
	txs := make([]*pb.TransactionRequest, 0, len(chainIDs))
	for idx, id := range chainIDs {
		if len(id) == 0 {
			continue
		}
		chainCopy := append([]byte(nil), id...)
		txs = append(txs, &pb.TransactionRequest{
			ChainId:     chainCopy,
			Transaction: [][]byte{[]byte{byte(idx)}},
		})
	}
	return &pb.XTRequest{Transactions: txs}
}
