package miner

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/clique"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/txpool/legacypool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"

	"github.com/stretchr/testify/require"
)

func createnMiner(t *testing.T, jovian bool) (*Miner, *ecdsa.PrivateKey, common.Address) {
	// Create Ethash config with interop enabled
	config := Config{
		PendingFeeRecipient:                   common.HexToAddress("123456789"),
		RollupTransactionConditionalRateLimit: params.TransactionConditionalMaxCost,
	}

	// Create chainConfig with interop enabled
	chainDB := rawdb.NewMemoryDatabase()
	triedb := triedb.NewDatabase(chainDB, nil)

	// Create test keys that will have funds in genesis
	testBankKey, _ := crypto.GenerateKey()
	testBankAddress := crypto.PubkeyToAddress(testBankKey.PublicKey)

	gasLimit := uint64(12_000_000)
	genesis := minerTestGenesisBlock(15, gasLimit, testBankAddress)

	// Enable jovian by setting JovianTime to 0
	if jovian {
		genesis.Config.JovianTime = new(uint64)
		*genesis.Config.JovianTime = 0
	}

	chainConfig, _, _, err := core.SetupGenesisBlock(chainDB, triedb, genesis)
	if err != nil {
		t.Fatalf("can't create new chain config: %v", err)
	}

	// Create consensus engine
	engine := clique.New(chainConfig.Clique, chainDB)

	// Create Ethereum backend
	bc, err := core.NewBlockChain(chainDB, nil, genesis, nil, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatalf("can't create new chain %v", err)
	}

	statedb, _ := state.New(bc.Genesis().Root(), bc.StateCache())
	blockchain := &testBlockChain{bc.Genesis().Root(), chainConfig, statedb, 10000000, new(event.Feed)}

	pool := legacypool.New(legacypool.DefaultConfig, blockchain)
	txpool, _ := txpool.New(legacypool.DefaultConfig.PriceLimit, blockchain, []txpool.SubPool{pool}, nil)

	// Create mock backend with interop support
	backend := NewMockBackend(bc, txpool, false, nil)

	miner := New(backend, config, engine)
	return miner, testBankKey, testBankAddress
}

func createTestTransaction(t *testing.T, key *ecdsa.PrivateKey, nonce uint64, data []byte, config *params.ChainConfig) *types.Transaction {
	signer := types.LatestSigner(config)

	tx, err := types.SignTx(types.NewTransaction(
		nonce,
		common.HexToAddress("0x1234567890123456789012345678901234567890"),
		big.NewInt(1000),
		500000, // High gas limit to accommodate large calldata
		big.NewInt(params.InitialBaseFee),
		data,
	), signer, key)

	if err != nil {
		t.Fatalf("Failed to sign transaction: %v", err)
	}

	return tx
}

func highEntropyCalldata(t *testing.T) []byte {
	calldata := make([]byte, 1500)
	for i := range calldata {
		calldata[i] = byte(i % 256)
	}
	return calldata
}

// TestCallDataFootprint runs a complete jovian transaction test
func TestCallDataFootprint(t *testing.T) {

	nonJovianMiner, nonJovianKey, _ := createnMiner(t, false)
	jovianMiner, jovianKey, _ := createnMiner(t, true)

	jovianTxs := make([]*types.Transaction, 0, 10)
	nonJovianTxs := make([]*types.Transaction, 0, 10)

	for i := 0; i < 10; i++ {
		jovianTx := createTestTransaction(t, jovianKey, uint64(i), highEntropyCalldata(t), jovianMiner.chainConfig)
		jovianTxs = append(jovianTxs, jovianTx)
		nonJovianTx := createTestTransaction(t, nonJovianKey, uint64(i), highEntropyCalldata(t), nonJovianMiner.chainConfig)
		nonJovianTxs = append(nonJovianTxs, nonJovianTx)
	}

	// Add the transaction to the pool
	errs := jovianMiner.txpool.Add(jovianTxs, false)
	if len(errs) > 0 && errs[0] != nil {
		t.Fatalf("Failed to add transaction to pool: %v", errs[0])
	}

	// Add the transaction to the pool
	errs = nonJovianMiner.txpool.Add(nonJovianTxs, false)
	if len(errs) > 0 && errs[0] != nil {
		t.Fatalf("Failed to add transaction to pool: %v", errs[0])
	}

	// Request block generation with RPC context (required for interop check)
	timestamp := uint64(5)
	jovianResult := jovianMiner.generateWork(&generateParams{
		parentHash: jovianMiner.chain.CurrentBlock().Hash(),
		timestamp:  timestamp,
		random:     common.HexToHash("0xcafebabe"),
		noTxs:      false,
		forceTime:  true,
		rpcCtx:     context.Background(),
	}, false)
	nonJovianResult := nonJovianMiner.generateWork(&generateParams{
		parentHash: nonJovianMiner.chain.CurrentBlock().Hash(),
		timestamp:  timestamp,
		random:     common.HexToHash("0xcafebabe"),
		noTxs:      false,
		forceTime:  true,
		rpcCtx:     context.Background(),
	}, false)

	require.Greater(t, jovianResult.block.GasUsed(), nonJovianResult.block.GasUsed())

}
