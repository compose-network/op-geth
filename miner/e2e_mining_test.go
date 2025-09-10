package miner

import (
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/consensus/misc/eip1559"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/stretchr/testify/require"
)

var daFootprintGasScalar uint16 = 200

// TestEndToEndMiningAndExecution builds a block via the miner from txpool
// transactions and then imports the block into the chain, asserting that
// execution (via StateProcessor through InsertChain) succeeds.
func TestEndToEndMiningAndExecution(t *testing.T) {
	requireTxGas := func(t *testing.T, block *types.Block, receipts []*types.Receipt) {
		var txGas uint64
		for _, receipt := range receipts {
			txGas += receipt.GasUsed
		}
		require.Equal(t, txGas, block.GasUsed(), "total tx gas used should be equal to block gas used")
	}

	requireDAFootprint := func(t *testing.T, block *types.Block, receipts []*types.Receipt) {
		var (
			txGas       uint64
			daFootprint uint64
		)
		txs := block.Transactions()
		for i, receipt := range receipts {
			txGas += receipt.GasUsed
			if txs[i].IsDepositTx() {
				continue
			}
			daFootprint += txs[i].RollupCostData().EstimatedDASize().Uint64() * uint64(daFootprintGasScalar)
		}
		require.Less(t, txGas, block.GasUsed(), "total tx gas used must be smaller than block gas used")
		require.Equal(t, daFootprint, block.GasUsed(), "total DA footprint used should be equal to block gas used")
	}
	t.Run("jovian", func(t *testing.T) {
		testMineAndExecute(t, 17, jovianConfig(), func(t *testing.T, block *types.Block, receipts []*types.Receipt) {
			requireDAFootprint(t, block, receipts)
		})
	})
	t.Run("holocene", func(t *testing.T) {
		testMineAndExecute(t, 20, holoceneConfig(), func(t *testing.T, block *types.Block, receipts []*types.Receipt) {
			requireTxGas(t, block, receipts)
		})
	})
}

func testMineAndExecute(t *testing.T, numTxs uint64, cfg *params.ChainConfig, assertFn func(t *testing.T, block *types.Block, receipts []*types.Receipt)) {
	db := rawdb.NewMemoryDatabase()
	w, b := newTestWorker(t, cfg, beacon.New(ethash.NewFaker()), db, 0)

	// Start from nonce 1 to avoid colliding with the preloaded pending tx.
	txs := genTxs(1, numTxs)

	// Add to txpool for the miner to pick up.
	if errs := b.txPool.Add(txs, false); len(errs) > 0 {
		for _, err := range errs {
			require.NoError(t, err, "failed adding tx to pool")
		}
	}

	data := make([]byte, 178)
	binary.BigEndian.PutUint16(data[176:178], daFootprintGasScalar)

	genParams := &generateParams{
		parentHash:           b.chain.CurrentBlock().Hash(),
		timestamp:            b.chain.CurrentBlock().Time + 12,
		withdrawals:          types.Withdrawals{},
		beaconRoot:           new(common.Hash),
		gasLimit:             ptr(uint64(1e6)), // Small gas limit to easily fill block
		eip1559Params:        eip1559.EncodeHolocene1559Params(250, 6),
		daFootprintGasScalar: &daFootprintGasScalar,
		txs:                  types.Transactions{types.NewTx(&types.DepositTx{Data: data})},
	}
	if cfg.IsJovian(b.chain.CurrentBlock().Time) {
		genParams.minBaseFee = new(uint64)
	}
	r := w.generateWork(genParams, false)
	require.NoError(t, r.err, "block generation failed")
	require.NotNil(t, r.block, "no block generated")

	assertFn(t, r.block, r.receipts)

	// We expect all our transactions to be included.
	totalExpected := 1 + len(pendingTxs) + len(txs)
	require.Equal(t, totalExpected, len(r.block.Transactions()), "unexpected tx count in block")

	// Import the block into the chain, which executes it via StateProcessor.
	_, err := b.chain.InsertChain(types.Blocks{r.block})
	require.NoError(t, err, "block import/execution failed")
}

func ptr[T any](v T) *T {
	return &v
}
