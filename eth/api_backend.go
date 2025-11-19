// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"

	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	rollupv1 "github.com/compose-network/publisher/proto/rollup/v1"
	spconsensus "github.com/compose-network/publisher/x/consensus"
	"github.com/compose-network/publisher/x/superblock/sequencer"
	"github.com/compose-network/publisher/x/transport"
	"github.com/ethereum/go-ethereum/core/ssv"
	"github.com/ethereum/go-ethereum/eth/tracers/native"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/filtermaps"
	"github.com/ethereum/go-ethereum/core/history"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/txpool/locals"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/gasprice"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
)

// xtIDFromCtx extracts the xtID from context, if present, else returns empty string.
func xtIDFromCtx(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v := ctx.Value("xtID"); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// ctxWithXtID attaches the xtID hex string to context for downstream logging.
func ctxWithXtID(ctx context.Context, xtID *rollupv1.XtID) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if xtID == nil {
		return ctx
	}
	return context.WithValue(ctx, "xtID", xtID.Hex())
}

// reasonForGrep maps common error message patterns to a stable, greppable reason key.
// It intentionally prefers more specific patterns before generic ones.
func reasonForGrep(err error) string {
	if err == nil {
		return "ok"
	}
	e := err.Error()
	switch {
	case strings.Contains(e, "replacement transaction underpriced"):
		return "replacement"
	case strings.Contains(e, "nonce too high"):
		return "nonce_too_high"
	case strings.Contains(e, "nonce too low"):
		return "nonce_too_low"
	case strings.Contains(e, "already known"):
		return "already_known"
	case strings.Contains(e, "insufficient funds"):
		return "insufficient_funds"
	case strings.Contains(e, "underpriced"):
		return "underpriced"
	case strings.Contains(e, "evicted"):
		return "evicted"
	default:
		return "other"
	}
}

// EthAPIBackend implements ethapi.Backend and tracers.Backend for full nodes
type EthAPIBackend struct {
	extRPCEnabled       bool
	allowUnprotectedTxs bool
	disableTxPool       bool
	eth                 *Ethereum
	gpo                 *gasprice.Oracle

	// SSV: Shared publisher + SBCP coordinator integration
	spClient         transport.Client
	coordinator      sequencer.Coordinator
	sequencerClients map[string]transport.Client
	sequencerKey     *ecdsa.PrivateKey
	sequencerAddress common.Address
	coordinatorKey   *ecdsa.PrivateKey
	coordinatorAddr  common.Address
	mailboxAddresses []common.Address
	mailboxByChainID map[uint64]common.Address

	// SSV: Sequencer transaction management
	sequencerTxMutex sync.RWMutex
	txTracker        *sequencerTxTracker
	putInboxPool     *putInboxTxPool

	// SSV: Track last RequestSeal inclusion list for SBCP
	rsMutex                 sync.RWMutex
	lastRequestSealIncluded [][]byte
	lastRequestSealSlot     uint64

	// SSV: Store built blocks to send after RequestSeal
	pendingBlockMutex sync.RWMutex
	pendingBlocks     []*types.Block
	pendingBlockSlot  uint64
}

// xtOutcome represents the final outcome of a cross-rollup transaction (XT) from
// the execution client's point of view. It is used to drive centralized cleanup
// and txpool rejection policy.
type xtOutcome int

const (
	xtOutcomeDelivered xtOutcome = iota
	xtOutcomeAborted
	xtOutcomeNotIncluded
	xtOutcomeCleared
)

func (o xtOutcome) String() string {
	switch o {
	case xtOutcomeDelivered:
		return "delivered"
	case xtOutcomeAborted:
		return "aborted"
	case xtOutcomeNotIncluded:
		return "not_included"
	case xtOutcomeCleared:
		return "cleared"
	default:
		return "unknown"
	}
}

// isXtAborted queries the sequencer coordinator (protocol layer) to determine
// if an XT should be rejected. The coordinator is the authoritative source for
// XT decisions made during consensus.
func (b *EthAPIBackend) isXtAborted(key string) bool {
	if key == "" || b.coordinator == nil {
		return false
	}
	return b.coordinator.ShouldRejectXt(key)
}

type sequencerTxKind int

const (
	sequencerTxOriginal sequencerTxKind = iota
	sequencerTxPutInbox
)

// ChainConfig returns the active chain configuration.
func (b *EthAPIBackend) ChainConfig() *params.ChainConfig {
	return b.eth.blockchain.Config()
}

func (b *EthAPIBackend) CurrentBlock() *types.Header {
	return b.eth.blockchain.CurrentBlock()
}

func (b *EthAPIBackend) SetHead(number uint64) {
	b.eth.handler.downloader.Cancel()
	b.eth.blockchain.SetHead(number)
}

func (b *EthAPIBackend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	// Pending block is only known by the miner
	if number == rpc.PendingBlockNumber {
		block, _, _ := b.eth.miner.Pending(ctx)
		if block == nil {
			return nil, errors.New("pending block is not available")
		}
		return block.Header(), nil
	}
	// Otherwise resolve and return the block
	if number == rpc.LatestBlockNumber {
		return b.eth.blockchain.CurrentBlock(), nil
	}
	if number == rpc.FinalizedBlockNumber {
		block := b.eth.blockchain.CurrentFinalBlock()
		if block == nil {
			return nil, errors.New("finalized block not found")
		}
		return block, nil
	}
	if number == rpc.SafeBlockNumber {
		block := b.eth.blockchain.CurrentSafeBlock()
		if block == nil {
			return nil, errors.New("safe block not found")
		}
		return block, nil
	}
	var bn uint64
	if number == rpc.EarliestBlockNumber {
		bn = b.HistoryPruningCutoff()
	} else {
		bn = uint64(number)
	}
	return b.eth.blockchain.GetHeaderByNumber(bn), nil
}

func (b *EthAPIBackend) HeaderByNumberOrHash(
	ctx context.Context,
	blockNrOrHash rpc.BlockNumberOrHash,
) (*types.Header, error) {
	if blockNr, ok := blockNrOrHash.Number(); ok {
		return b.HeaderByNumber(ctx, blockNr)
	}
	if hash, ok := blockNrOrHash.Hash(); ok {
		header := b.eth.blockchain.GetHeaderByHash(hash)
		if header == nil {
			return nil, errors.New("header for hash not found")
		}
		if blockNrOrHash.RequireCanonical && b.eth.blockchain.GetCanonicalHash(header.Number.Uint64()) != hash {
			return nil, errors.New("hash is not currently canonical")
		}
		return header, nil
	}
	return nil, errors.New("invalid arguments; neither block nor hash specified")
}

func (b *EthAPIBackend) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	return b.eth.blockchain.GetHeaderByHash(hash), nil
}

func (b *EthAPIBackend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	// Pending block is only known by the miner
	if number == rpc.PendingBlockNumber {
		block, _, _ := b.eth.miner.Pending(context.Background())
		if block == nil {
			return nil, errors.New("pending block is not available")
		}
		return block, nil
	}
	// Otherwise resolve and return the block
	if number == rpc.LatestBlockNumber {
		header := b.eth.blockchain.CurrentBlock()
		return b.eth.blockchain.GetBlock(header.Hash(), header.Number.Uint64()), nil
	}
	if number == rpc.FinalizedBlockNumber {
		header := b.eth.blockchain.CurrentFinalBlock()
		if header == nil {
			return nil, errors.New("finalized block not found")
		}
		return b.eth.blockchain.GetBlock(header.Hash(), header.Number.Uint64()), nil
	}
	if number == rpc.SafeBlockNumber {
		header := b.eth.blockchain.CurrentSafeBlock()
		if header == nil {
			return nil, errors.New("safe block not found")
		}
		return b.eth.blockchain.GetBlock(header.Hash(), header.Number.Uint64()), nil
	}
	bn := uint64(number) // the resolved number
	if number == rpc.EarliestBlockNumber {
		bn = b.HistoryPruningCutoff()
	}
	block := b.eth.blockchain.GetBlockByNumber(bn)
	if block == nil && bn < b.HistoryPruningCutoff() {
		return nil, &history.PrunedHistoryError{}
	}
	return block, nil
}

func (b *EthAPIBackend) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	number := b.eth.blockchain.GetBlockNumber(hash)
	if number == nil {
		return nil, nil
	}
	block := b.eth.blockchain.GetBlock(hash, *number)
	if block == nil && *number < b.HistoryPruningCutoff() {
		return nil, &history.PrunedHistoryError{}
	}
	return block, nil
}

// GetBody returns body of a block. It does not resolve special block numbers.
func (b *EthAPIBackend) GetBody(ctx context.Context, hash common.Hash, number rpc.BlockNumber) (*types.Body, error) {
	if number < 0 || hash == (common.Hash{}) {
		return nil, errors.New("invalid arguments; expect hash and no special block numbers")
	}
	body := b.eth.blockchain.GetBody(hash)
	if body == nil {
		if uint64(number) < b.HistoryPruningCutoff() {
			return nil, &history.PrunedHistoryError{}
		}
		return nil, errors.New("block body not found")
	}
	return body, nil
}

func (b *EthAPIBackend) BlockByNumberOrHash(
	ctx context.Context,
	blockNrOrHash rpc.BlockNumberOrHash,
) (*types.Block, error) {
	if blockNr, ok := blockNrOrHash.Number(); ok {
		return b.BlockByNumber(ctx, blockNr)
	}
	if hash, ok := blockNrOrHash.Hash(); ok {
		header := b.eth.blockchain.GetHeaderByHash(hash)
		if header == nil {
			// Return 'null' and no error if block is not found.
			// This behavior is required by RPC spec.
			return nil, nil
		}
		if blockNrOrHash.RequireCanonical && b.eth.blockchain.GetCanonicalHash(header.Number.Uint64()) != hash {
			return nil, errors.New("hash is not currently canonical")
		}
		block := b.eth.blockchain.GetBlock(hash, header.Number.Uint64())
		if block == nil {
			if header.Number.Uint64() < b.HistoryPruningCutoff() {
				return nil, &history.PrunedHistoryError{}
			}
			return nil, errors.New("header found, but block body is missing")
		}
		return block, nil
	}
	return nil, errors.New("invalid arguments; neither block nor hash specified")
}

func (b *EthAPIBackend) Pending() (*types.Block, types.Receipts, *state.StateDB) {
	return b.eth.miner.Pending(context.Background())
}

func (b *EthAPIBackend) StateAndHeaderByNumber(
	ctx context.Context,
	number rpc.BlockNumber,
) (*state.StateDB, *types.Header, error) {
	// Pending state is only known by the miner
	if number == rpc.PendingBlockNumber {
		block, _, state := b.eth.miner.Pending(ctx)
		if block != nil && state != nil {
			//state.TxIndex() == 1
			//sequencerBalance := state.GetBalance(common.HexToAddress("0x0f10aF865F68F5aA1dDB7c5b5A1a0f396232C6Be"))
			//fmt.Println("[AFTER] Sequencer balance: ", sequencerBalance.String())
			return state, block.Header(), nil
		} else {
			number = rpc.LatestBlockNumber // fall back to latest state
		}
	}
	// Otherwise resolve the block number and return its state
	header, err := b.HeaderByNumber(ctx, number)
	if err != nil {
		return nil, nil, err
	}
	if header == nil {
		return nil, nil, fmt.Errorf("header %w", ethereum.NotFound)
	}
	stateDb, err := b.eth.BlockChain().StateAt(header.Root)
	if err != nil {
		stateDb, err = b.eth.BlockChain().HistoricState(header.Root)
		if err != nil {
			return nil, nil, err
		}
	}
	return stateDb, header, nil
}

func (b *EthAPIBackend) StateAndHeaderByNumberOrHash(
	ctx context.Context,
	blockNrOrHash rpc.BlockNumberOrHash,
) (*state.StateDB, *types.Header, error) {
	if blockNr, ok := blockNrOrHash.Number(); ok {
		return b.StateAndHeaderByNumber(ctx, blockNr)
	}
	if hash, ok := blockNrOrHash.Hash(); ok {
		header, err := b.HeaderByHash(ctx, hash)
		if err != nil {
			return nil, nil, err
		}
		if header == nil {
			return nil, nil, fmt.Errorf("header for hash %w", ethereum.NotFound)
		}
		if blockNrOrHash.RequireCanonical && b.eth.blockchain.GetCanonicalHash(header.Number.Uint64()) != hash {
			return nil, nil, errors.New("hash is not currently canonical")
		}
		stateDb, err := b.eth.BlockChain().StateAt(header.Root)
		if err != nil {
			stateDb, err = b.eth.BlockChain().HistoricState(header.Root)
			if err != nil {
				return nil, nil, err
			}
		}
		return stateDb, header, nil
	}
	return nil, nil, errors.New("invalid arguments; neither block nor hash specified")
}

func (b *EthAPIBackend) HistoryPruningCutoff() uint64 {
	bn, _ := b.eth.blockchain.HistoryPruningCutoff()
	return bn
}

func (b *EthAPIBackend) GetReceipts(ctx context.Context, hash common.Hash) (types.Receipts, error) {
	return b.eth.blockchain.GetReceiptsByHash(hash), nil
}

func (b *EthAPIBackend) GetCanonicalReceipt(tx *types.Transaction, blockHash common.Hash, blockNumber, blockIndex uint64) (*types.Receipt, error) {
	return b.eth.blockchain.GetCanonicalReceipt(tx, blockHash, blockNumber, blockIndex)
}

func (b *EthAPIBackend) GetLogs(ctx context.Context, hash common.Hash, number uint64) ([][]*types.Log, error) {
	return rawdb.ReadLogs(b.eth.chainDb, hash, number), nil
}

func (b *EthAPIBackend) GetEVM(
	ctx context.Context,
	state *state.StateDB,
	header *types.Header,
	vmConfig *vm.Config,
	blockCtx *vm.BlockContext,
) *vm.EVM {
	if vmConfig == nil {
		vmConfig = b.eth.blockchain.GetVMConfig()
	}
	var context vm.BlockContext
	if blockCtx != nil {
		context = *blockCtx
	} else {
		context = core.NewEVMBlockContext(header, b.eth.BlockChain(), nil, b.eth.blockchain.Config(), state)
	}
	return vm.NewEVM(context, state, b.ChainConfig(), *vmConfig)
}

func (b *EthAPIBackend) SubscribeRemovedLogsEvent(ch chan<- core.RemovedLogsEvent) event.Subscription {
	return b.eth.BlockChain().SubscribeRemovedLogsEvent(ch)
}

func (b *EthAPIBackend) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
	return b.eth.BlockChain().SubscribeChainEvent(ch)
}

func (b *EthAPIBackend) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	return b.eth.BlockChain().SubscribeChainHeadEvent(ch)
}

func (b *EthAPIBackend) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
	return b.eth.BlockChain().SubscribeLogsEvent(ch)
}

func (b *EthAPIBackend) SendTx(ctx context.Context, signedTx *types.Transaction) error {
	if b.ChainConfig().IsOptimism() && signedTx.Type() == types.BlobTxType {
		return types.ErrTxTypeNotSupported
	}

	// OP-Stack: forward to remote sequencer RPC
	if b.eth.seqRPCService != nil {
		data, err := signedTx.MarshalBinary()
		if err != nil {
			return err
		}
		if err := b.eth.seqRPCService.CallContext(ctx, nil, "eth_sendRawTransaction", hexutil.Encode(data)); err != nil {
			return err
		}
	}
	if b.disableTxPool {
		return nil
	}

	// Retain tx in local tx pool after forwarding, for local RPC usage.
	err := b.sendTx(ctx, signedTx)
	if err != nil && b.eth.seqRPCService != nil {
		log.Warn(
			"successfully sent tx to sequencer, but failed to persist in local tx pool",
			"err",
			err,
			"tx",
			signedTx.Hash(),
		)
		return nil
	}
	return err
}

func (b *EthAPIBackend) sendTx(ctx context.Context, signedTx *types.Transaction) error {
	err := b.eth.txPool.Add([]*types.Transaction{signedTx}, false)[0]

	// If the local transaction tracker is not configured, returns whatever
	// returned from the txpool.
	if b.eth.localTxTracker == nil {
		return err
	}
	// If the transaction fails with an error indicating it is invalid, or if there is
	// very little chance it will be accepted later (e.g., the gas price is below the
	// configured minimum, or the sender has insufficient funds to cover the cost),
	// propagate the error to the user.
	if err != nil && !locals.IsTemporaryReject(err) {
		return err
	}
	// No error will be returned to user if the transaction fails with a temporary
	// error and might be accepted later (e.g., the transaction pool is full).
	// Locally submitted transactions will be resubmitted later via the local tracker.
	b.eth.localTxTracker.Track(signedTx)

	var from common.Address
	if signer := types.LatestSignerForChainID(b.ChainConfig().ChainID); signer != nil {
		if s, err := types.Sender(signer, signedTx); err == nil {
			from = s
		}
	}
	kindStr := "unknown"
	xt := ""
	b.sequencerTxMutex.RLock()
	if b.txTracker != nil {
		if record := b.txTracker.record(signedTx.Hash()); record != nil {
			if record.kind == sequencerTxPutInbox {
				kindStr = "putInbox"
			} else if record.kind == sequencerTxOriginal {
				kindStr = "original"
			}
			xt = record.xtID
		}
	}
	b.sequencerTxMutex.RUnlock()
	log.Info("[SSV] Tracked local tx for resubmission",
		"txHash", signedTx.Hash().Hex(),
		"from", from.Hex(),
		"nonce", signedTx.Nonce(),
		"kind", kindStr,
		"xtID", xt,
	)
	return nil
}

func (b *EthAPIBackend) GetPoolTransactions() (types.Transactions, error) {
	pending := b.eth.txPool.Pending(txpool.PendingFilter{})
	var txs types.Transactions
	for _, batch := range pending {
		for _, lazy := range batch {
			if tx := lazy.Resolve(); tx != nil {
				txs = append(txs, tx)
			}
		}
	}
	return txs, nil
}

func (b *EthAPIBackend) GetPoolTransaction(hash common.Hash) *types.Transaction {
	return b.eth.txPool.Get(hash)
}

// GetCanonicalTransaction retrieves the lookup along with the transaction itself
// associate with the given transaction hash.
//
// A null will be returned if the transaction is not found. The transaction is not
// existent from the node's perspective. This can be due to the transaction indexer
// not being finished. The caller must explicitly check the indexer progress.
//
// Notably, only the transaction in the canonical chain is visible.
func (b *EthAPIBackend) GetCanonicalTransaction(
	txHash common.Hash,
) (bool, *types.Transaction, common.Hash, uint64, uint64) {
	lookup, tx := b.eth.blockchain.GetCanonicalTransaction(txHash)
	if lookup == nil || tx == nil {
		return false, nil, common.Hash{}, 0, 0
	}
	return true, tx, lookup.BlockHash, lookup.BlockIndex, lookup.Index
}

// TxIndexDone returns true if the transaction indexer has finished indexing.
func (b *EthAPIBackend) TxIndexDone() bool {
	return b.eth.blockchain.TxIndexDone()
}

func (b *EthAPIBackend) GetPoolNonce(ctx context.Context, addr common.Address) (uint64, error) {
	nonce := b.eth.txPool.PoolNonce(addr)
	pend, queued := b.eth.txPool.ContentFrom(addr)
	log.Info("[SSV] GetPoolNonce snapshot",
		"addr", addr.Hex(),
		"nonce", nonce,
		"pending_count", len(pend),
		"queued_count", len(queued),
	)
	return nonce, nil
}

func (b *EthAPIBackend) Stats() (runnable int, blocked int) {
	return b.eth.txPool.Stats()
}

func (b *EthAPIBackend) TxPoolContent() (map[common.Address][]*types.Transaction, map[common.Address][]*types.Transaction) {
	return b.eth.txPool.Content()
}

func (b *EthAPIBackend) TxPoolContentFrom(addr common.Address) ([]*types.Transaction, []*types.Transaction) {
	return b.eth.txPool.ContentFrom(addr)
}

func (b *EthAPIBackend) TxPool() *txpool.TxPool {
	return b.eth.txPool
}

func (b *EthAPIBackend) SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription {
	return b.eth.txPool.SubscribeTransactions(ch, true)
}

func (b *EthAPIBackend) SyncProgress(ctx context.Context) ethereum.SyncProgress {
	prog := b.eth.Downloader().Progress()
	if txProg, err := b.eth.blockchain.TxIndexProgress(); err == nil {
		prog.TxIndexFinishedBlocks = txProg.Indexed
		prog.TxIndexRemainingBlocks = txProg.Remaining
	}
	remain, err := b.eth.blockchain.StateIndexProgress()
	if err == nil {
		prog.StateIndexRemaining = remain
	}
	return prog
}

func (b *EthAPIBackend) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	return b.gpo.SuggestTipCap(ctx)
}

func (b *EthAPIBackend) FeeHistory(
	ctx context.Context,
	blockCount uint64,
	lastBlock rpc.BlockNumber,
	rewardPercentiles []float64,
) (firstBlock *big.Int, reward [][]*big.Int, baseFee []*big.Int, gasUsedRatio []float64, baseFeePerBlobGas []*big.Int, blobGasUsedRatio []float64, err error) {
	return b.gpo.FeeHistory(ctx, blockCount, lastBlock, rewardPercentiles)
}

func (b *EthAPIBackend) BlobBaseFee(ctx context.Context) *big.Int {
	if excess := b.CurrentHeader().ExcessBlobGas; excess != nil {
		return eip4844.CalcBlobFee(b.ChainConfig(), b.CurrentHeader())
	}
	return nil
}

func (b *EthAPIBackend) ChainDb() ethdb.Database {
	return b.eth.ChainDb()
}

func (b *EthAPIBackend) AccountManager() *accounts.Manager {
	return b.eth.AccountManager()
}

func (b *EthAPIBackend) ExtRPCEnabled() bool {
	return b.extRPCEnabled
}

func (b *EthAPIBackend) UnprotectedAllowed() bool {
	return b.allowUnprotectedTxs
}

func (b *EthAPIBackend) RPCGasCap() uint64 {
	return b.eth.config.RPCGasCap
}

func (b *EthAPIBackend) RPCEVMTimeout() time.Duration {
	return b.eth.config.RPCEVMTimeout
}

func (b *EthAPIBackend) RPCTxFeeCap() float64 {
	return b.eth.config.RPCTxFeeCap
}

func (b *EthAPIBackend) CurrentView() *filtermaps.ChainView {
	head := b.eth.blockchain.CurrentBlock()
	if head == nil {
		return nil
	}
	return filtermaps.NewChainView(b.eth.blockchain, head.Number.Uint64(), head.Hash())
}

func (b *EthAPIBackend) NewMatcherBackend() filtermaps.MatcherBackend {
	return b.eth.filterMaps.NewMatcherBackend()
}

func (b *EthAPIBackend) Engine() consensus.Engine {
	return b.eth.engine
}

func (b *EthAPIBackend) CurrentHeader() *types.Header {
	return b.eth.blockchain.CurrentHeader()
}

func (b *EthAPIBackend) StateAtBlock(
	ctx context.Context,
	block *types.Block,
	reexec uint64,
	base *state.StateDB,
	readOnly bool,
	preferDisk bool,
) (*state.StateDB, tracers.StateReleaseFunc, error) {
	return b.eth.stateAtBlock(ctx, block, reexec, base, readOnly, preferDisk)
}

func (b *EthAPIBackend) StateAtTransaction(
	ctx context.Context,
	block *types.Block,
	txIndex int,
	reexec uint64,
) (*types.Transaction, vm.BlockContext, *state.StateDB, tracers.StateReleaseFunc, error) {
	return b.eth.stateAtTransaction(ctx, block, txIndex, reexec)
}

func (b *EthAPIBackend) HistoricalRPCService() *rpc.Client {
	return b.eth.historicalRPCService
}

func (b *EthAPIBackend) Genesis() *types.Block {
	return b.eth.blockchain.Genesis()
}

// HandleSPMessage processes messages received from the shared publisher.
// SSV
func (b *EthAPIBackend) HandleSPMessage(ctx context.Context, msg *rollupv1.Message) ([]common.Hash, error) {
	if b.coordinator == nil {
		return nil, fmt.Errorf("coordinator not configured")
	}

	// If this call originates from local RPC (SendXTransaction) we set ctx value "forward".
	// Forward XTRequest to the SP over transport instead of handling locally.
	if forward, _ := ctx.Value("forward").(bool); forward {
		switch msg.Payload.(type) {
		case *rollupv1.Message_XtRequest:
			if b.spClient == nil {
				return nil, fmt.Errorf("shared publisher client not configured")
			}
			if msg.SenderId == "" {
				msg.SenderId = b.ChainConfig().ChainID.String()
			}
			if err := b.spClient.Send(ctx, msg); err != nil {
				return nil, fmt.Errorf("failed to forward XTRequest to shared publisher: %w", err)
			}
			return nil, nil
		}
	}

	// Default path: route inbound messages (from SP or peers) to the SBCP coordinator
	if err := b.coordinator.HandleMessage(ctx, msg.SenderId, msg); err != nil {
		return nil, fmt.Errorf("coordinator failed to handle %T: %w", msg.Payload, err)
	}
	return nil, nil
}

func successfulAll(coordinationStates []*SimulationState) bool {
	for _, s := range coordinationStates {
		// checking if any transaction reverted or requires processing CIRCMessage
		if !s.Success || len(s.Dependencies) > 0 {
			return false
		}
	}

	return true
}

// handleSequencerMessage processes messages received from sequencer clients (peer-to-peer).
// SSV
func (b *EthAPIBackend) handleSequencerMessage(
	ctx context.Context,
	chainID string,
	msg *rollupv1.Message,
) ([]common.Hash, error) {
	if b.coordinator == nil {
		return nil, fmt.Errorf("coordinator not configured for sequencer message from chainID %s", chainID)
	}

	log.Debug(
		"[SSV] Handling message from sequencer",
		"chainID",
		chainID,
		"senderID",
		msg.SenderId,
		"type",
		fmt.Sprintf("%T", msg.Payload),
	)

	if err := b.coordinator.HandleMessage(ctx, msg.SenderId, msg); err != nil {
		log.Error("[SSV] Failed to handle message from sequencer", "chainID", chainID, "err", err)
		return nil, fmt.Errorf("coordinator failed to handle %T from sequencer %s: %w", msg.Payload, chainID, err)
	}

	return nil, nil
}

// StartCallbackFn returns a function that can be used to send transaction bundles to the shared publisher.
// SSV
func (b *EthAPIBackend) StartCallbackFn(chainID *big.Int) spconsensus.StartFn {
	_ = chainID
	return func(ctx context.Context, from string, xtReq *rollupv1.XTRequest) error {
		log.Warn("[SSV] Suppressing StartCallback XTRequest forward (SBCP-only)", "from", from)
		return nil
	}
}

// VoteCallbackFn returns a function that can be used to send votes for cross-chain transactions.
// SSV
func (b *EthAPIBackend) VoteCallbackFn(chainID *big.Int) spconsensus.VoteFn {
	return func(ctx context.Context, xtID *rollupv1.XtID, vote bool) error {
		msgVote := &rollupv1.Message_Vote{
			Vote: &rollupv1.Vote{
				Vote:          vote,
				XtId:          xtID,
				SenderChainId: chainID.Bytes(),
			},
		}

		spMsg := &rollupv1.Message{
			SenderId: chainID.String(),
			Payload:  msgVote,
		}
		return b.spClient.Send(ctx, spMsg)
	}
}

func (b *EthAPIBackend) SimulateTransaction(
	ctx context.Context,
	tx *types.Transaction,
	blockNrOrHash rpc.BlockNumberOrHash,
) (*ssv.SSVTraceResult, error) {
	timer := time.Now()
	defer func() {
		log.Info("[SSV] Simulated transaction with SSV trace", "txHash", tx.Hash().Hex(), "duration", time.Since(timer))
	}()

	ctx = context.WithValue(ctx, "simulation", true)

	// stateDB should have clear() and putInbox() in its state
	stateDB, header, err := b.StateAndHeaderByNumberOrHash(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}

	stateDB.Finalise(true)
	snapshot := stateDB.Snapshot()
	defer stateDB.RevertToSnapshot(snapshot)

	signer := types.MakeSigner(b.ChainConfig(), header.Number, header.Time)
	msg, err := core.TransactionToMessage(tx, signer, header.BaseFee)
	if err != nil {
		return nil, err
	}

	// For speculative simulation during SBCP, skip nonce checks to allow
	// simulating transactions submitted in parallel batches with future nonces.
	// The actual nonce validation will happen during real block execution.
	msg.SkipNonceChecks = true

	blockContext := core.NewEVMBlockContext(header, b.eth.blockchain, nil, b.ChainConfig(), stateDB)

	if ctx.Value("simulation") != nil {
		stagingVMConfig := vm.Config{}
		if b.eth.blockchain.GetVMConfig() != nil {
			stagingVMConfig = *b.eth.blockchain.GetVMConfig()
		}
		stagingVMConfig.Tracer = nil
		stagingVMConfig.EnablePreimageRecording = true

		stagingEVM := vm.NewEVM(blockContext, stateDB, b.ChainConfig(), stagingVMConfig)

		for _, staged := range b.GetPendingPutInboxTxs() {
			if staged == nil || staged.Hash() == tx.Hash() {
				continue
			}

			stageMsg, err := core.TransactionToMessage(staged, signer, header.BaseFee)
			if err != nil {
				continue
			}
			stageMsg.SkipNonceChecks = true

			stageGasPool := new(core.GasPool).AddGas(header.GasLimit)
			stateDB.SetTxContext(staged.Hash(), stateDB.TxIndex()+1)
			if _, err := core.ApplyMessage(stagingEVM, stageMsg, stageGasPool); err != nil {
				continue
			}
			stateDB.Finalise(true)
		}
	}

	mailboxAddresses := b.GetMailboxAddresses()
	tracer := native.NewSSVTracer(mailboxAddresses)

	vmConfig := vm.Config{}
	if b.eth.blockchain.GetVMConfig() != nil {
		vmConfig = *b.eth.blockchain.GetVMConfig()
	}
	vmConfig.Tracer = tracer.Hooks()
	vmConfig.EnablePreimageRecording = true

	evm := vm.NewEVM(blockContext, stateDB, b.ChainConfig(), vmConfig)

	stateDB.SetTxContext(tx.Hash(), stateDB.TxIndex()+1)

	gasPool := new(core.GasPool).AddGas(header.GasLimit)
	result, err := core.ApplyMessage(evm, msg, gasPool)
	if err != nil {
		xtKey := xtIDFromCtx(ctx)
		reason := reasonForGrep(err)
		log.Error("[SSV] EVM execution failed during simulation - REASON: evm_apply_message_error",
			"txHash", tx.Hash().Hex(),
			"error", err,
			"failure_reason", "evm_apply_message_error",
			"xtID", xtKey,
			"reason", reason,
		)

		// If it's a nonce mismatch, add a structured snapshot to aid parallel-tx debugging
		if reason == "nonce_too_high" || reason == "nonce_too_low" {
			// Gather sender/account info
			sender := msg.From
			poolNonce := b.eth.txPool.PoolNonce(sender)
			stateNonce := stateDB.GetNonce(sender)
			pend, queued := b.eth.txPool.ContentFrom(sender)
			const maxDump = 16
			pNonces := make([]uint64, 0, maxDump)
			qNonces := make([]uint64, 0, maxDump)
			for i := 0; i < len(pend) && i < maxDump; i++ {
				pNonces = append(pNonces, pend[i].Nonce())
			}
			for i := 0; i < len(queued) && i < maxDump; i++ {
				qNonces = append(qNonces, queued[i].Nonce())
			}
			log.Info("[SSV] Nonce mismatch snapshot during simulation",
				"txHash", tx.Hash().Hex(),
				"sender", sender.Hex(),
				"txNonce", tx.Nonce(),
				"poolNonce", poolNonce,
				"stateNonce", stateNonce,
				"pending_count", len(pend),
				"queued_count", len(queued),
				"pending_nonces", pNonces,
				"queued_nonces", qNonces,
				"xtID", xtKey,
				"reason", reason,
			)
		}
		return nil, err
	}

	traceResult := tracer.GetTraceResult()
	traceResult.ExecutionResult = result

	return traceResult, nil
}

// SubmitSequencerTransaction submits a transaction with a priority flag.
// SSV
func (b *EthAPIBackend) SubmitSequencerTransaction(ctx context.Context, tx *types.Transaction, isPutInbox bool) error {
	// Try to add sender to the log context
	var sender common.Address
	if signer := types.LatestSignerForChainID(b.ChainConfig().ChainID); signer != nil {
		if s, err := types.Sender(signer, tx); err == nil {
			sender = s
		}
	}
	xtKey := xtIDFromCtx(ctx)
	log.Info("[SSV] SubmitSequencerTransaction",
		"txHash", tx.Hash().Hex(),
		"nonce", tx.Nonce(),
		"isPutInbox", isPutInbox,
		"from", sender.Hex(),
		"xtID", xtKey,
	)
	if err := b.validateSequencerTransaction(tx); err != nil {
		log.Error("[SSV] Sequencer transaction validation failed", "err", err, "txHash", tx.Hash().Hex())
		return fmt.Errorf("sequencer transaction validation failed: %w", err)
	}

	// Stage the transaction but defer txpool injection until block building.
	var finalTx *types.Transaction
	if isPutInbox {
		signedTx, err := b.AddPendingPutInboxTx(ctx, tx)
		if err != nil {
			log.Error("[SSV] Failed to add putInbox transaction to pool",
				"err", err,
				"originalHash", tx.Hash().Hex(),
				"xtID", xtKey)
			return fmt.Errorf("failed to add putInbox transaction: %w", err)
		}
		finalTx = signedTx
	} else {
		b.sequencerTxMutex.Lock()
		b.addSequencerEntryLocked(tx, sequencerTxOriginal)
		b.sequencerTxMutex.Unlock()
		finalTx = tx
	}

	log.Info("[SSV] Staged sequencer transaction (deferred pool injection)",
		"txHash", finalTx.Hash().Hex(),
		"nonce", finalTx.Nonce(),
		"isPutInbox", isPutInbox,
		"from", sender.Hex(),
		"xtID", xtKey,
	)
	return nil
}

// ConfigureMailboxes sets the mailbox contract addresses for known rollups.
// SSV
func (b *EthAPIBackend) ConfigureMailboxes(raw map[uint64]string) error {
	if len(raw) == 0 {
		b.mailboxAddresses = nil
		b.mailboxByChainID = nil
		native.ReplaceChainIDToMailbox(nil)
		return nil
	}

	ordered := make([]uint64, 0, len(raw))
	for chainID := range raw {
		ordered = append(ordered, chainID)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })

	addresses := make([]common.Address, 0, len(ordered))
	mailboxMap := make(map[uint64]common.Address, len(ordered))

	for _, chainID := range ordered {
		addrStr := strings.TrimSpace(raw[chainID])
		if addrStr == "" {
			mailboxMap[chainID] = common.Address{}
			addresses = append(addresses, common.Address{})
			log.Warn("[SSV] Mailbox address not configured", "chainID", chainID)
			continue
		}
		if !common.IsHexAddress(addrStr) {
			return fmt.Errorf("invalid mailbox address %q for chain %d", addrStr, chainID)
		}
		addr := common.HexToAddress(addrStr)
		mailboxMap[chainID] = addr
		addresses = append(addresses, addr)
	}

	b.mailboxAddresses = addresses
	b.mailboxByChainID = mailboxMap
	native.ReplaceChainIDToMailbox(mailboxMap)
	return nil
}

// GetMailboxAddresses returns the list of mailbox contract addresses to watch.
// SSV
func (b *EthAPIBackend) GetMailboxAddresses() []common.Address {
	if len(b.mailboxAddresses) == 0 {
		return nil
	}
	out := make([]common.Address, len(b.mailboxAddresses))
	copy(out, b.mailboxAddresses)
	return out
}

func (b *EthAPIBackend) GetMailboxAddressFromChainID(chainID uint64) common.Address {
	if b.mailboxByChainID == nil {
		return common.Address{}
	}
	return b.mailboxByChainID[chainID]
}

// AddPendingPutInboxTx adds a putInbox transaction to the pool with atomic nonce assignment.
// SSV
func (b *EthAPIBackend) AddPendingPutInboxTx(ctx context.Context, tx *types.Transaction) (*types.Transaction, error) {
	xtID := xtIDFromCtx(ctx)

	b.sequencerTxMutex.Lock()
	tracker := b.ensureTrackerLocked()
	sequence := tracker.nextSeq
	tracker.nextSeq++
	b.sequencerTxMutex.Unlock()

	signedTx, err := b.putInboxPool.add(tx, xtID, sequence)
	if err != nil {
		log.Error("[SSV] Failed to add putInbox tx to pool",
			"err", err,
			"xtID", xtID,
			"originalHash", tx.Hash().Hex())
		return nil, err
	}

	pending, committed, nextNonce := b.putInboxPool.stats()
	log.Info("[SSV] Added putInbox transaction to pool",
		"xtID", xtID,
		"signedHash", signedTx.Hash().Hex(),
		"nonce", signedTx.Nonce(),
		"pending", pending,
		"committed", committed,
		"nextNonce", nextNonce)

	if miner := b.eth.miner; miner != nil {
		miner.InvalidatePendingCache()
	}
	return signedTx, nil
}

func (b *EthAPIBackend) GetPendingPutInboxTxs() []*types.Transaction {
	if b.putInboxPool == nil {
		return nil
	}
	return b.putInboxPool.getPending()
}

// ClearSequencerTransactionsAfterBlock clears all pending sequencer transactions after block creation
// SSV
func (b *EthAPIBackend) ClearSequencerTransactionsAfterBlock() {
	if b.coordinator == nil {
		log.Info("[SSV] Clearing transactions - non-SBCP mode")
		b.clearAllSequencerTransactions()
		return
	}

	currentState := b.coordinator.GetState()
	slot := b.coordinator.GetCurrentSlot()

	log.Info("[SSV] Transaction clearing request",
		"state", currentState.String(),
		"slot", slot,
		"pending", len(b.GetPendingPutInboxTxs())+len(b.GetPendingOriginalTxs()))

	switch currentState {
	case sequencer.StateBuildingFree, sequencer.StateBuildingLocked:
		// Preserve transactions during these states:
		// - BuildingLocked: SCP coordination in progress
		// - BuildingFree: Transactions ready, waiting for block inclusion
		// Actual clearing happens in OnBlockBuildingComplete after commitment
		log.Info("[SSV] Preserving transactions during coordination")
		return
	default:
		log.Info("[SSV] Clearing transactions")
		b.clearAllSequencerTransactions()
	}
}

// clearAllSequencerTransactions performs the actual clearing of transactions
// SSV
func (b *EthAPIBackend) clearAllSequencerTransactions() {
	// First, finalize all XTs that are currently tracked, so XT-specific cleanup
	// (including txpool rejection) goes through the centralized path.
	xtIDs := make(map[string]struct{})

	b.sequencerTxMutex.RLock()
	if b.txTracker != nil {
		for xtID := range b.txTracker.allXTIDsLocked() {
			if xtID != "" {
				xtIDs[xtID] = struct{}{}
			}
		}
	}
	b.sequencerTxMutex.RUnlock()

	if b.putInboxPool != nil {
		entries := b.putInboxPool.pendingEntries()
		for _, entry := range entries {
			if entry.xtID != "" {
				xtIDs[entry.xtID] = struct{}{}
			}
		}
	}

	totalPutRemoved := 0
	totalOrigRemoved := 0
	for xtID := range xtIDs {
		removedPut, removedOrig := b.finalizeXt(xtID, xtOutcomeCleared)
		totalPutRemoved += removedPut
		totalOrigRemoved += removedOrig
	}

	// As a safety net, clear any remaining sequencer records that may not be
	// associated with an xtID (should be rare). This preserves the behaviour
	// of the previous implementation while routing XT-tagged txs through
	// finalizeXt.
	b.sequencerTxMutex.Lock()
	tracker := b.ensureTrackerLocked()
	remainingPut, remainingOrig, remainingRecords := tracker.clearAll()
	b.sequencerTxMutex.Unlock()

	poolCleared := 0
	if b.putInboxPool != nil {
		poolCleared = b.putInboxPool.clearAll()
	}

	txHashesToReject := make([]common.Hash, 0, len(remainingRecords))
	for _, record := range remainingRecords {
		if record != nil && record.tx != nil {
			b.logSequencerRemoval(record, "clear_all")
			txHashesToReject = append(txHashesToReject, record.tx.Hash())
		}
	}

	for _, hash := range txHashesToReject {
		if tx := b.eth.txPool.Get(hash); tx != nil {
			tx.SetRejected()
			log.Info("[SSV] Marked cleared tx as rejected in txpool",
				"txHash", hash.Hex())
		}
	}

	log.Info("[SSV] Cleared sequencer transactions",
		"xtCount", len(xtIDs),
		"putInbox", totalPutRemoved+remainingPut,
		"original", totalOrigRemoved+remainingOrig,
		"poolCleared", poolCleared,
		"rejectedInPool", len(txHashesToReject))

	if miner := b.eth.miner; miner != nil && (totalPutRemoved+remainingPut > 0 || totalOrigRemoved+remainingOrig > 0) {
		miner.InvalidatePendingCache()
	}
}

// PrepareSequencerTransactionsForBlock prepares sequencer transactions for inclusion in a new block
// SSV
func (b *EthAPIBackend) PrepareSequencerTransactionsForBlock(ctx context.Context) error {
	if b.coordinator == nil {
		return nil
	}

	currentState := b.coordinator.GetState()
	currentSlot := b.coordinator.GetCurrentSlot()

	// During active SCP coordination, notify coordinator
	if currentState == sequencer.StateBuildingLocked {
		if err := b.coordinator.PrepareTransactionsForBlock(ctx, currentSlot); err != nil {
			log.Warn("[SSV] Coordinator failed to prepare transactions", "err", err)
		}
	}

	return nil
}

func (b *EthAPIBackend) prepareAndInjectSequencerTransactions(ctx context.Context) error {
	if b.putInboxPool == nil {
		return nil
	}

	if err := b.putInboxPool.inject(); err != nil {
		log.Error("[SSV] Failed to inject putInbox transactions from pool", "err", err)
		return err
	}

	b.sequencerTxMutex.RLock()
	var originalTxs []*types.Transaction
	if b.txTracker != nil {
		originalTxs = b.txTracker.transactionsByKind(sequencerTxOriginal)
	}
	b.sequencerTxMutex.RUnlock()

	if len(originalTxs) > 0 {
		log.Info("[SSV] Injecting original transactions into pool", "count", len(originalTxs))

		for _, tx := range originalTxs {
			if err := b.sendTx(ctx, tx); err != nil {
				reason := reasonForGrep(err)
				log.Warn("[SSV] Failed to inject original tx",
					"err", err,
					"txHash", tx.Hash().Hex(),
					"nonce", tx.Nonce(),
					"reason", reason)
			} else {
				log.Info("[SSV] Injected original tx",
					"txHash", tx.Hash().Hex(),
					"nonce", tx.Nonce())
			}
		}
	}

	return nil
}

// GetOrderedTransactionsForBlock returns only sequencer-managed transactions in
// the correct order for block inclusion. Normal mempool transactions are
// included by the miner after this list, and must not be returned here.
// SSV
func (b *EthAPIBackend) GetOrderedTransactionsForBlock(ctx context.Context) (types.Transactions, error) {
	if b.coordinator == nil {
		txs, _, _ := b.assembleSequencerBundle()
		return txs, nil
	}

	currentState := b.coordinator.GetState()

	switch currentState {
	case sequencer.StateBuildingLocked:
		// During coordination, exclude cross-chain txs - they'll be included after decision
		return types.Transactions{}, nil
	case sequencer.StateBuildingFree, sequencer.StateSubmission:
		// Ensure pool + tracker transactions are injected before ordering for block inclusion.
		if err := b.prepareAndInjectSequencerTransactions(ctx); err != nil {
			log.Error("[SSV] Failed to prepare sequencer transactions", "err", err)
		}

		// During Submission state, get the RequestSeal inclusion list to filter transactions
		var requestSealIncluded map[string]bool
		if currentState == sequencer.StateSubmission {
			b.rsMutex.RLock()
			if len(b.lastRequestSealIncluded) > 0 {
				requestSealIncluded = make(map[string]bool, len(b.lastRequestSealIncluded))
				for _, xt := range b.lastRequestSealIncluded {
					requestSealIncluded[hex.EncodeToString(xt)] = true
				}
			}
			b.rsMutex.RUnlock()

			if len(requestSealIncluded) > 0 {
				log.Info("[SSV] Filtering block transactions based on RequestSeal inclusion list",
					"state", currentState.String(),
					"includedCount", len(requestSealIncluded))
			}
		}

		var poolEntries []putInboxTxEntry
		if b.putInboxPool != nil {
			poolEntries = b.putInboxPool.pendingEntries()
		}
		poolTxs := make(types.Transactions, 0, len(poolEntries))
		poolRecords := make([]sequencerBundleEntry, 0, len(poolEntries))
		skippedNonIncluded := 0
		for _, entry := range poolEntries {
			if entry.xtID != "" && b.isXtAborted(entry.xtID) {
				log.Info("[SSV] Skipping aborted putInbox tx in pool",
					"xtID", entry.xtID,
					"hash", entry.tx.Hash().Hex(),
					"nonce", entry.tx.Nonce())
				continue
			}

			// During Submission, only include XTs that are in RequestSeal list
			if requestSealIncluded != nil && entry.xtID != "" {
				if !requestSealIncluded[entry.xtID] {
					log.Debug("[SSV] Skipping non-included putInbox tx (not in RequestSeal)",
						"xtID", entry.xtID,
						"hash", entry.tx.Hash().Hex(),
						"state", currentState.String())
					skippedNonIncluded++
					continue
				}
			}

			poolTxs = append(poolTxs, entry.tx)
			status := sequencerTxStatusStaged
			if entry.committed {
				status = sequencerTxStatusCommitted
			}
			poolRecords = append(poolRecords, sequencerBundleEntry{
				tx:     entry.tx,
				xtID:   entry.xtID,
				kind:   sequencerTxPutInbox,
				status: status,
			})
		}

		// After SCP completes (BuildingFree) or during submission, include ready transactions.
		originalTxs, orderedRecords, pendingBundles := b.assembleSequencerBundle()
		combinedTxs := make(types.Transactions, 0, len(poolTxs)+len(originalTxs))
		combinedTxs = append(combinedTxs, poolTxs...)
		combinedTxs = append(combinedTxs, originalTxs...)
		combinedRecords := append(poolRecords, orderedRecords...)

		if skippedNonIncluded > 0 {
			log.Info("[SSV] Filtered non-included XTs from block",
				"state", currentState.String(),
				"skippedCount", skippedNonIncluded)
		}

		if len(combinedTxs) > 0 {
			var putCount int
			if b.putInboxPool != nil {
				pending, _, _ := b.putInboxPool.stats()
				putCount = pending
			}
			b.sequencerTxMutex.RLock()
			origCount := b.countEntriesByKindLocked(sequencerTxOriginal)
			b.sequencerTxMutex.RUnlock()

			signer := types.LatestSignerForChainID(b.ChainConfig().ChainID)
			for i, info := range combinedRecords {
				tx := info.tx
				if tx == nil {
					continue
				}
				kind := info.kind.String()
				from := common.Address{}
				if signer != nil {
					if s, err := types.Sender(signer, tx); err == nil {
						from = s
					}
				}
				log.Info("[SSV] Block order",
					"idx", i,
					"kind", kind,
					"status", info.status.String(),
					"from", from.Hex(),
					"nonce", tx.Nonce(),
					"xtID", info.xtID,
					"hash", tx.Hash().Hex(),
				)
			}
			log.Info("[SSV] GetOrderedTransactionsForBlock",
				"total", len(combinedTxs),
				"putInbox_pending", putCount,
				"original_pending", origCount,
			)
		} else {
			// No sequencer-managed transactions returned for block inclusion. If there
			// are still staged records, log a warning so we can correlate with any
			// missing-commit issues during analysis.
			b.sequencerTxMutex.RLock()
			if b.txTracker != nil {
				if pendingTotal := b.txTracker.pendingTotal(); pendingTotal > 0 {
					log.Warn("[SSV] No sequencer transactions ordered for block despite pending records",
						"pendingTotal", pendingTotal)
				}
			}
			b.sequencerTxMutex.RUnlock()
		}
		if len(pendingBundles) > 0 {
			for _, bundle := range pendingBundles {
				details := make([]string, 0, len(bundle.items))
				for _, rec := range bundle.items {
					details = append(details, fmt.Sprintf("%s/%s", rec.kind.String(), rec.status.String()))
				}
				log.Info("[SSV] Deferring XT until pair ready",
					"xtID", bundle.xtID,
					"details", details)
			}
		}
		return combinedTxs, nil
	default:
		return types.Transactions{}, nil
	}
}

func (b *EthAPIBackend) assembleSequencerBundle() (types.Transactions, []sequencerBundleEntry, []sequencerPendingBundle) {
	b.sequencerTxMutex.RLock()
	defer b.sequencerTxMutex.RUnlock()

	if b.txTracker == nil {
		return types.Transactions{}, nil, nil
	}

	ready, pending := b.txTracker.buildBundles()
	if len(ready) == 0 {
		return types.Transactions{}, ready, pending
	}

	// During Submission state, filter based on RequestSeal inclusion list
	var requestSealIncluded map[string]bool
	var currentState sequencer.State
	if b.coordinator != nil {
		currentState = b.coordinator.GetState()
	}
	if currentState == sequencer.StateSubmission {
		b.rsMutex.RLock()
		if len(b.lastRequestSealIncluded) > 0 {
			requestSealIncluded = make(map[string]bool, len(b.lastRequestSealIncluded))
			for _, xt := range b.lastRequestSealIncluded {
				requestSealIncluded[hex.EncodeToString(xt)] = true
			}
		}
		b.rsMutex.RUnlock()
	}

	txs := make(types.Transactions, 0, len(ready))
	for _, entry := range ready {
		if entry.tx == nil {
			continue
		}

		if entry.xtID != "" && b.isXtAborted(entry.xtID) {
			log.Info("[SSV] Skipping aborted XT in bundle assembly",
				"xtID", entry.xtID,
				"txHash", entry.tx.Hash().Hex(),
				"nonce", entry.tx.Nonce())
			continue
		}

		// During Submission, only include XTs that are in RequestSeal list
		if requestSealIncluded != nil && entry.xtID != "" {
			if !requestSealIncluded[entry.xtID] {
				log.Debug("[SSV] Skipping non-included original tx (not in RequestSeal)",
					"xtID", entry.xtID,
					"hash", entry.tx.Hash().Hex(),
					"state", currentState.String())
				continue
			}
		}

		txs = append(txs, entry.tx)
	}

	return txs, ready, pending
}

// buildSequencerOnlyList assembles only the sequencer-managed transactions preserving
// their insertion order. The insertion order is guaranteed to be correct because:
// 1. For each XT, putInbox transactions are created before original transactions
// 2. XTs are processed sequentially, maintaining their relative order
func (b *EthAPIBackend) buildSequencerOnlyList() types.Transactions {
	var combined types.Transactions
	if b.putInboxPool != nil {
		combined = append(combined, b.putInboxPool.getPending()...)
	}
	originals, _, _ := b.assembleSequencerBundle()
	combined = append(combined, originals...)
	return combined
}

// validateSequencerTransaction validates that a sequencer transaction is properly formed
// SSV
func (b *EthAPIBackend) validateSequencerTransaction(tx *types.Transaction) error {
	// Basic validation
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	if tx.To() == nil {
		return fmt.Errorf("sequencer transaction must have a destination")
	}

	// Check if it's targeting a mailbox address
	mailboxAddrs := b.GetMailboxAddresses()
	isMailboxTx := false
	for _, addr := range mailboxAddrs {
		if *tx.To() == addr {
			isMailboxTx = true
			break
		}
	}

	if !isMailboxTx {
		log.Warn("[SSV] Sequencer transaction not targeting mailbox",
			"to", tx.To().Hex(),
			"expected", mailboxAddrs)
	}

	// Validate gas limits
	if tx.Gas() < 21000 { // TODO: update this
		return fmt.Errorf("sequencer transaction gas too low: %d", tx.Gas())
	}

	if tx.Gas() > 1000000 { // TODO: update this
		log.Warn("[SSV] Sequencer transaction has high gas limit", "gas", tx.Gas())
	}

	log.Info("[SSV] Sequencer transaction validated",
		"txHash", tx.Hash().Hex(),
		"to", tx.To().Hex(),
		"gas", tx.Gas(),
		"gasPrice", tx.GasPrice())

	return nil
}

// OnBlockBuildingStart is called when block building starts
// SSV
func (b *EthAPIBackend) OnBlockBuildingStart(ctx context.Context) error {
	if b.coordinator != nil {
		slot := b.coordinator.GetCurrentSlot()
		coordState := b.coordinator.GetState().String()
		pendingPut := 0
		if b.putInboxPool != nil {
			pendingPut, _, _ = b.putInboxPool.stats()
		}
		b.sequencerTxMutex.RLock()
		origCount := b.countEntriesByKindLocked(sequencerTxOriginal)
		total := 0
		if b.txTracker != nil {
			total = b.txTracker.pendingTotal()
		}
		total += pendingPut
		b.sequencerTxMutex.RUnlock()
		log.Info("[SSV] OnBlockBuildingStart",
			"slot", slot,
			"state", coordState,
			"staged_total", total,
			"staged_putInbox", pendingPut,
			"staged_original", origCount,
		)
		_ = b.coordinator.OnBlockBuildingStart(ctx, slot)
	}

	return nil
}

// OnBlockBuildingComplete is called when block building completes.
// Blocks are stored for later submission but not sent immediately. Block submission to the
// shared publisher happens in NotifyRequestSeal after the seal request arrives.
// SSV
func (b *EthAPIBackend) OnBlockBuildingComplete(
	ctx context.Context,
	block *types.Block,
	success, simulation bool,
) error {
	if !success || block == nil {
		log.Warn("[SSV] Block build failed, clearing sequencer transactions")
		b.clearAllSequencerTransactions()
		return nil
	}
	if simulation {
		return nil
	}

	// Get slot
	slot := uint64(0)
	currentState := "unknown"
	if b.coordinator != nil {
		slot = b.coordinator.GetCurrentSlot()
		currentState = b.coordinator.GetState().String()
	}

	// Identify which cross-chain txs are in this block and mark them committed.
	b.sequencerTxMutex.Lock()
	tracker := b.ensureTrackerLocked()
	type pairCheck struct {
		putIndex      int
		originalIndex int
	}
	inBlockHashes := make([]common.Hash, 0)
	pairStatus := make(map[string]*pairCheck)
	for idx, tx := range block.Transactions() {
		hash := tx.Hash()
		var (
			record = tracker.record(hash)
			isPut  bool
			xtID   string
		)
		if record != nil {
			xtID = record.xtID
			isPut = record.kind == sequencerTxPutInbox
		} else if b.putInboxPool != nil {
			if entry := b.putInboxPool.entryByHash(hash); entry != nil {
				xtID = entry.xtID
				isPut = true
			}
		}
		if record != nil || isPut {
			inBlockHashes = append(inBlockHashes, hash)
			if xtID != "" {
				check, ok := pairStatus[xtID]
				if !ok {
					check = &pairCheck{putIndex: -1, originalIndex: -1}
				}
				if isPut && check.putIndex == -1 {
					check.putIndex = idx
				} else if !isPut && check.originalIndex == -1 {
					check.originalIndex = idx
				}
				pairStatus[xtID] = check
			}
		}
	}
	if len(inBlockHashes) > 0 {
		tracker.markCommitted(slot, block.NumberU64(), inBlockHashes)
		if b.putInboxPool != nil {
			b.putInboxPool.markCommitted(slot, block.NumberU64(), inBlockHashes)
		}
	}
	b.sequencerTxMutex.Unlock()

	crossChainTxHashes := make(map[common.Hash]bool, len(inBlockHashes))
	for _, hash := range inBlockHashes {
		crossChainTxHashes[hash] = true
	}

	for xtID, check := range pairStatus {
		hasPut := check.putIndex != -1
		hasOriginal := check.originalIndex != -1

		if hasPut && hasOriginal {
			if check.putIndex > check.originalIndex {
				log.Error("[SSV] Cross-chain pair out of order",
					"xtID", xtID,
					"putIndex", check.putIndex,
					"originalIndex", check.originalIndex,
					"block", block.NumberU64())
			}
		} else if hasPut {
			log.Error("[SSV] Incomplete cross-chain pair: putInbox without original",
				"xtID", xtID,
				"putIndex", check.putIndex,
				"block", block.NumberU64())
		} else if hasOriginal {
			log.Debug("[SSV] Bridge contract transaction (no putInbox)",
				"xtID", xtID,
				"originalIndex", check.originalIndex,
				"block", block.NumberU64())
		}
	}

	signer := types.LatestSignerForChainID(b.ChainConfig().ChainID)
	for i, tx := range block.Transactions() {
		if _, ok := crossChainTxHashes[tx.Hash()]; ok {
			from := common.Address{}
			if signer != nil {
				if s, err := types.Sender(signer, tx); err == nil {
					from = s
				}
			}
			log.Info("[SSV] Included sequencer tx",
				"block", block.NumberU64(),
				"idx", i,
				"hash", tx.Hash().Hex(),
				"from", from.Hex(),
				"nonce", tx.Nonce(),
			)
		}
	}

	if len(inBlockHashes) > 0 {
		log.Info("[SSV] Recorded committed sequencer transactions awaiting finalization",
			"slot", slot,
			"blockNumber", block.NumberU64(),
			"count", len(inBlockHashes),
			"state", currentState)
	}

	// Store block with automatic deduplication. Treat pendingBlocks as a stack keyed
	// by block number: newer payloads replace older ones, identical hashes are ignored.
	blockHash := block.Hash()
	blockNumber := block.NumberU64()

	b.pendingBlockMutex.Lock()
	filtered := make([]*types.Block, 0, len(b.pendingBlocks))
	isDuplicateHash := false
	for _, existingBlock := range b.pendingBlocks {
		switch {
		case existingBlock.Hash() == blockHash:
			isDuplicateHash = true
			filtered = append(filtered, existingBlock)
		case existingBlock.NumberU64() == blockNumber:
			// Drop older version for this block number.
		default:
			filtered = append(filtered, existingBlock)
		}
	}

	if isDuplicateHash {
		b.pendingBlocks = filtered
		totalStored := len(b.pendingBlocks)
		b.pendingBlockMutex.Unlock()

		log.Info("[SSV] Skipping duplicate block (identical hash already stored)",
			"slot", slot,
			"state", currentState,
			"blockNumber", blockNumber,
			"hash", blockHash.Hex(),
			"totalStored", totalStored)
		return nil
	}

	b.pendingBlocks = append(filtered, block)
	b.pendingBlockSlot = slot
	b.pendingBlockMutex.Unlock()

	// If RequestSeal already arrived for this slot, send immediately
	b.rsMutex.RLock()
	requestSealReady := b.lastRequestSealIncluded != nil && b.lastRequestSealSlot == slot
	b.rsMutex.RUnlock()

	if requestSealReady {
		if err := b.sendStoredL2Block(ctx); err != nil {
			log.Error("[SSV] Failed to send stored L2Blocks after block build", "err", err, "slot", slot)
		}
	}

	return nil
}

func (b *EthAPIBackend) clearCommittedSequencerTransactions(committed map[common.Hash]bool) {
	if len(committed) == 0 {
		return
	}

	// Determine which XTs are affected by the committed hashes by consulting both the
	// sequencer tracker and the putInbox pool.
	xtIDs := make(map[string]struct{})

	b.sequencerTxMutex.RLock()
	if b.txTracker != nil {
		for hash := range committed {
			if rec := b.txTracker.record(hash); rec != nil && rec.xtID != "" {
				xtIDs[rec.xtID] = struct{}{}
			}
		}
	}
	b.sequencerTxMutex.RUnlock()

	if b.putInboxPool != nil {
		entries := b.putInboxPool.pendingEntries()
		for _, entry := range entries {
			if entry.xtID == "" {
				continue
			}
			if committed[entry.tx.Hash()] {
				xtIDs[entry.xtID] = struct{}{}
			}
		}
	}

	if len(xtIDs) == 0 {
		log.Warn("[SSV] clearCommittedSequencerTransactions called with committed hashes but no xtIDs found")
		return
	}

	totalPutRemoved := 0
	totalOrigRemoved := 0
	for xtID := range xtIDs {
		removedPut, removedOrig := b.finalizeXt(xtID, xtOutcomeDelivered)
		totalPutRemoved += removedPut
		totalOrigRemoved += removedOrig
	}

	log.Info("[SSV] Cleared committed cross-chain txs after delivery",
		"xtCount", len(xtIDs),
		"putInboxRemoved", totalPutRemoved,
		"originalRemoved", totalOrigRemoved)
}

func (b *EthAPIBackend) GetPendingOriginalTxs() []*types.Transaction {
	return b.listTransactionsByKind(sequencerTxOriginal)
}

// reSimulateTransaction re-simulates a single transaction and checks for success
// SSV
func (b *EthAPIBackend) reSimulateTransaction(
	ctx context.Context,
	tx *types.Transaction,
	blockNrOrHash rpc.BlockNumberOrHash,
	xtID *rollupv1.XtID,
) (bool, error) {
	log.Info("[SSV] Re-simulating transaction",
		"txHash", tx.Hash().Hex(),
		"xtID", xtID.Hex())

	// Simulate with SSV tracing to detect mailbox interactions
	traceResult, err := b.SimulateTransaction(ctx, tx, blockNrOrHash)
	if err != nil {
		log.Error("[SSV] Transaction simulation with trace failed - REASON: simulation_trace_error",
			"txHash", tx.Hash().Hex(),
			"error", err,
			"xtID", xtID.Hex(),
			"failure_reason", "simulation_trace_error")
		return false, err
	}

	// Check if execution was successful
	if traceResult.ExecutionResult.Err != nil {
		log.Warn("[SSV] Transaction execution failed in re-simulation - REASON: execution_error",
			"txHash", tx.Hash().Hex(),
			"executionError", traceResult.ExecutionResult.Err,
			"xtID", xtID.Hex(),
			"failure_reason", "execution_error")
		return false, nil
	}

	// Validate that the transaction used reasonable gas (not failed silently)
	if traceResult.ExecutionResult.UsedGas == 0 {
		log.Warn("[SSV] Transaction used no gas, likely failed silently - REASON: zero_gas_used",
			"txHash", tx.Hash().Hex(),
			"xtID", xtID.Hex(),
			"failure_reason", "zero_gas_used")
		return false, nil
	}

	// Check that mailbox operations were traced (indicating they succeeded)
	if len(traceResult.Operations) == 0 {
		log.Warn("[SSV] No mailbox operations detected in re-simulation - REASON: no_mailbox_operations",
			"txHash", tx.Hash().Hex(),
			"xtID", xtID.Hex(),
			"failure_reason", "no_mailbox_operations")
		return false, nil
	}

	log.Info("[SSV] Transaction re-simulation successful",
		"txHash", tx.Hash().Hex(),
		"gasUsed", traceResult.ExecutionResult.UsedGas,
		"mailboxOps", len(traceResult.Operations),
		"xtID", xtID.Hex())

	return true, nil
}

func (b *EthAPIBackend) poolPayloadTx(
	ctx context.Context,
	tx *types.Transaction) {
	if xtKey := xtIDFromCtx(ctx); xtKey != "" {
		if b.isXtAborted(xtKey) {
			log.Info("[SSV] Skipping pool for aborted XT", "xtID", xtKey, "txHash", tx.Hash().Hex(), "nonce", tx.Nonce())
			return
		}
	}

	b.sequencerTxMutex.Lock()
	b.addSequencerEntryLocked(tx, sequencerTxOriginal)
	b.sequencerTxMutex.Unlock()

	xtKey := xtIDFromCtx(ctx)
	log.Info("[SSV] Staged original tx",
		"txHash", tx.Hash().Hex(),
		"nonce", tx.Nonce(),
		"xtID", xtKey,
	)

	if miner := b.eth.miner; miner != nil {
		miner.InvalidatePendingCache()
	}
}

// assignXtKeyToHash associates a transaction with its cross-chain transaction ID.
// This enables per-XT cleanup when transactions are aborted.
func (b *EthAPIBackend) assignXtKeyToHash(tx *types.Transaction, xtID *rollupv1.XtID) {
	if tx == nil || xtID == nil {
		log.Warn("[SSV] assignXtKeyToHash called with nil parameter", "txNil", tx == nil, "xtIDNil", xtID == nil)
		return
	}
	key := xtID.Hex()
	txHash := tx.Hash()

	b.sequencerTxMutex.Lock()
	tracker := b.ensureTrackerLocked()
	assigned := tracker.assignXtID(txHash, key)
	var slot uint64
	if b.coordinator != nil {
		slot = b.coordinator.GetCurrentSlot()
	}
	b.sequencerTxMutex.Unlock()

	if !assigned {
		log.Warn("[SSV] assignXtKeyToHash: transaction not staged",
			"txHash", txHash.Hex(),
			"xtID", key,
			"slot", slot)
		return
	}

	log.Info("[SSV] Assigned xtID to transaction",
		"txHash", txHash.Hex(),
		"xtID", key,
		"slot", slot)
}

func (b *EthAPIBackend) ensureTrackerLocked() *sequencerTxTracker {
	if b.txTracker == nil {
		b.txTracker = newSequencerTxTracker()
	}
	return b.txTracker
}

func (b *EthAPIBackend) countEntriesByKindLocked(kind sequencerTxKind) int {
	if b.txTracker == nil {
		return 0
	}
	return b.txTracker.countByKind(kind)
}

func (b *EthAPIBackend) pendingPutInboxCount() int {
	if b.putInboxPool == nil {
		return 0
	}
	pending, _, _ := b.putInboxPool.stats()
	return pending
}

func (b *EthAPIBackend) listTransactionsByKind(kind sequencerTxKind) []*types.Transaction {
	b.sequencerTxMutex.RLock()
	defer b.sequencerTxMutex.RUnlock()

	if b.txTracker == nil {
		return []*types.Transaction{}
	}
	return b.txTracker.transactionsByKind(kind)
}

func (b *EthAPIBackend) addSequencerEntryLocked(tx *types.Transaction, kind sequencerTxKind) {
	tracker := b.ensureTrackerLocked()

	slot := uint64(0)
	if b.coordinator != nil {
		slot = b.coordinator.GetCurrentSlot()
	}
	tracker.add(tx, kind, slot)
	record := tracker.record(tx.Hash())

	signer := types.LatestSignerForChainID(b.ChainConfig().ChainID)
	from := common.Address{}
	if record != nil && signer != nil {
		if s, err := types.Sender(signer, record.tx); err == nil {
			from = s
		}
	}
	kindStr := "original"
	if kind == sequencerTxPutInbox {
		kindStr = "putInbox"
	}
	putCount := b.pendingPutInboxCount()
	origCount := tracker.countByKind(sequencerTxOriginal)

	log.Info("[SSV] Staged add",
		"kind", kindStr,
		"from", from.Hex(),
		"nonce", tx.Nonce(),
		"hash", tx.Hash().Hex(),
		"pending_total", tracker.pendingTotal(),
		"pending_putInbox", putCount,
		"pending_original", origCount,
		"slot", slot,
	)
}

func (b *EthAPIBackend) logSequencerRemoval(record *sequencerTxRecord, reason string) {
	if record == nil || record.tx == nil {
		return
	}
	signer := types.LatestSignerForChainID(b.ChainConfig().ChainID)
	from := common.Address{}
	if signer != nil {
		if s, err := types.Sender(signer, record.tx); err == nil {
			from = s
		}
	}
	kind := "original"
	if record.kind == sequencerTxPutInbox {
		kind = "putInbox"
	}
	log.Info("[SSV] Staged remove",
		"reason", reason,
		"kind", kind,
		"status", record.status.String(),
		"from", from.Hex(),
		"nonce", record.tx.Nonce(),
		"hash", record.tx.Hash().Hex(),
		"xtID", record.xtID,
		"committedBlock", record.committedBlock,
		"committedSlot", record.committedSlot,
	)
}

// finalizeXt removes all staged transactions associated with the provided xtID and, depending
// on the outcome, may also mark those transactions as rejected in the txpool. This centralizes
// XT cleanup behaviour so that higher-level callers only need to decide on the XT outcome.
func (b *EthAPIBackend) finalizeXt(key string, outcome xtOutcome) (int, int) {
	if key == "" {
		return 0, 0
	}

	b.sequencerTxMutex.Lock()
	tracker := b.ensureTrackerLocked()
	removedPutInbox, removedOriginal, removedRecords := tracker.dropByXtID(key)
	b.sequencerTxMutex.Unlock()

	poolDropped := 0
	if b.putInboxPool != nil {
		poolDropped = b.putInboxPool.dropByXtID(key)
	}

	removedCommitted := 0
	txHashesToRemove := make([]common.Hash, 0, len(removedRecords))

	for _, record := range removedRecords {
		if record == nil || record.tx == nil {
			continue
		}
		if record.status == sequencerTxStatusCommitted {
			removedCommitted++
		}

		// Tag removal with the XT outcome for clearer diagnostics.
		b.logSequencerRemoval(record, outcome.String())
		txHashesToRemove = append(txHashesToRemove, record.tx.Hash())
	}

	if removedCommitted > 0 {
		log.Info("[SSV] Cleared committed markers for XT",
			"xtID", key,
			"count", removedCommitted,
			"outcome", outcome.String())
	}

	rejectedCount := 0
	for _, record := range removedRecords {
		if record == nil || record.tx == nil {
			continue
		}

		if b.shouldRejectRecordForOutcome(outcome, record) {
			if tx := b.eth.txPool.Get(record.tx.Hash()); tx != nil {
				tx.SetRejected()
				rejectedCount++
			}
		}
	}

	if removedPutInbox+removedOriginal+poolDropped > 0 {
		log.Info("[SSV] Dropped transactions for XT",
			"xtID", key,
			"putInbox", removedPutInbox,
			"original", removedOriginal,
			"poolDropped", poolDropped,
			"rejected", rejectedCount)

		if miner := b.eth.miner; miner != nil {
			miner.InvalidatePendingCache()
		}
	}

	return removedPutInbox, removedOriginal
}

// shouldRejectRecordForOutcome determines whether a sequencer transaction record should be
// marked as rejected in the txpool for a given XT outcome. This encodes the rejection policy
// in one place so call sites only need to choose the XT outcome.
func (b *EthAPIBackend) shouldRejectRecordForOutcome(outcome xtOutcome, record *sequencerTxRecord) bool {
	if record == nil || record.tx == nil {
		return false
	}

	switch outcome {
	case xtOutcomeDelivered, xtOutcomeCleared:
		// Once an XT has been delivered or we explicitly clear all sequencer state, we never
		// want its transactions to re-enter the pool.
		return true
	case xtOutcomeAborted, xtOutcomeNotIncluded:
		// For aborted / non-included XTs, we match the previous behaviour of dropTransactionsForXtKey:
		// reject putInbox transactions and any records that were already marked committed.
		return record.kind == sequencerTxPutInbox || record.status == sequencerTxStatusCommitted
	default:
		return false
	}
}

// SetSequencerCoordinator wires an SBCP sequencer coordinator, consensus callbacks, and SP client routing.
// SSV
func (b *EthAPIBackend) SetSequencerCoordinator(coord sequencer.Coordinator, sp transport.Client) {
	b.coordinator = coord
	b.spClient = sp

	if b.spClient != nil {
		b.spClient.SetHandler(b.HandleSPMessage)
	}

	// Set handlers for sequencer clients to receive CIRC messages
	for chainID, client := range b.sequencerClients {
		if client != nil {
			// Capture chainID in closure to avoid loop variable issues
			chainID := chainID
			client.SetHandler(func(ctx context.Context, msg *rollupv1.Message) ([]common.Hash, error) {
				return b.handleSequencerMessage(ctx, chainID, msg)
			})

			log.Info("[SSV] Sequencer client handler set", "peerChainID", chainID)
		}
	}

	if b.coordinator != nil {
		// Wire consensus callbacks for SCP → coordinator integration
		if b.coordinator.Consensus() != nil {
			chainID := b.ChainConfig().ChainID
			b.coordinator.Consensus().SetStartCallback(b.StartCallbackFn(chainID))
			b.coordinator.Consensus().SetVoteCallback(b.VoteCallbackFn(chainID))
		}

		// Register SBCP callbacks
		b.coordinator.SetCallbacks(sequencer.CoordinatorCallbacks{
			// For SBCP mode simulation during StartSC
			SimulateAndVote: b.simulateXTRequestForSBCP,
			// For immediate cleanup of aborted transactions
			CleanupAbortedTransaction: b.cleanupAbortedTransactionCallback,
		})

		// Set miner notifier and start
		b.coordinator.SetMinerNotifier(b)
	}
}

// NotifySlotStart notifies the backend when a new SBCP slot begins.
// SSV
func (b *EthAPIBackend) NotifySlotStart(startSlot *rollupv1.StartSlot) error {
	// Per-slot snapshot before any cleanup
	b.sequencerTxMutex.RLock()
	putInboxCount := b.pendingPutInboxCount()
	originalCount := b.countEntriesByKindLocked(sequencerTxOriginal)
	b.sequencerTxMutex.RUnlock()
	mailboxCount := len(b.mailboxAddresses)
	log.Info("[SSV] Notify miner: StartSlot",
		"slot", startSlot.Slot,
		"next_sb", startSlot.NextSuperblockNumber,
		"pending_putInbox", putInboxCount,
		"pending_original", originalCount,
		"mailboxes", mailboxCount,
	)

	// Clear any pending blocks from previous slot when new slot starts
	b.pendingBlockMutex.Lock()
	prevBlockCount := len(b.pendingBlocks)
	if prevBlockCount > 0 {
		log.Warn("[SSV] Clearing unsent blocks from previous slot",
			"prevSlot", b.pendingBlockSlot,
			"newSlot", startSlot.Slot,
			"blockCount", prevBlockCount)
	}
	b.pendingBlocks = nil
	b.pendingBlockSlot = startSlot.Slot
	b.pendingBlockMutex.Unlock()

	// Clear any lingering sequencer transactions from previous slot.
	b.sequencerTxMutex.Lock()
	tracker := b.ensureTrackerLocked()
	prevTxCount := tracker.pendingTotal()
	putInboxCount = b.pendingPutInboxCount()
	originalCount = tracker.countByKind(sequencerTxOriginal)
	b.sequencerTxMutex.Unlock()

	totalCarry := prevTxCount + putInboxCount
	if prevTxCount > 0 || putInboxCount > 0 {
		log.Info("[SSV] Carrying sequencer transactions into new slot",
			"slot", startSlot.Slot,
			"totalTxs", totalCarry,
			"putInbox", putInboxCount,
			"original", originalCount)
	}

	if miner := b.eth.miner; miner != nil {
		miner.InvalidatePendingCache()
	}

	return nil
}

// NotifyRequestSeal notifies the backend when RequestSeal is received from coordinator.
// SSV
func (b *EthAPIBackend) NotifyRequestSeal(ctx context.Context, requestSeal *rollupv1.RequestSeal) error {
	// Per-slot snapshot at RequestSeal
	putInboxCount := b.pendingPutInboxCount()
	b.sequencerTxMutex.RLock()
	originalCount := b.countEntriesByKindLocked(sequencerTxOriginal)
	b.sequencerTxMutex.RUnlock()
	mailboxCount := len(b.mailboxAddresses)
	log.Info("[SSV] Notify miner: RequestSeal",
		"slot", requestSeal.Slot,
		"included_xts", len(requestSeal.IncludedXts),
		"pending_putInbox", putInboxCount,
		"pending_original", originalCount,
		"mailboxes", mailboxCount,
	)

	// Clean up non-included transactions FIRST, before storing RequestSeal info
	// This ensures transactions rejected by SCP cannot be included in blocks built during Submission state
	included := make(map[string]struct{}, len(requestSeal.IncludedXts))
	for _, xt := range requestSeal.IncludedXts {
		included[hex.EncodeToString(xt)] = struct{}{}
	}

	// Collect ALL pending transaction keys
	b.sequencerTxMutex.RLock()
	pendingKeys := make(map[string]struct{})
	totalPending := 0
	if b.txTracker != nil {
		totalPending = b.txTracker.pendingTotal()
		for xtID := range b.txTracker.allXTIDsLocked() {
			if xtID != "" {
				pendingKeys[xtID] = struct{}{}
			}
		}
	}
	if b.putInboxPool != nil {
		poolEntries := b.putInboxPool.pendingEntries()
		totalPending += len(poolEntries)
		for _, entry := range poolEntries {
			if entry.xtID != "" {
				pendingKeys[entry.xtID] = struct{}{}
			}
		}
	}
	b.sequencerTxMutex.RUnlock()

	log.Info("[SSV] RequestSeal cleanup check",
		"slot", requestSeal.Slot,
		"totalPending", totalPending,
		"pendingWithXtID", len(pendingKeys),
		"includedXts", len(included))

	droppedNonIncluded := 0
	droppedAborted := 0

	for key := range pendingKeys {
		isAborted := b.isXtAborted(key)
		_, isIncluded := included[key]

		// If consensus marks this XT as aborted but it is also present in the
		// current RequestSeal.IncludedXts set, log an inconsistency. We still
		// prefer the inclusion signal here and keep the transaction.
		if isIncluded {
			if isAborted {
				log.Error("[SSV] Inconsistent XT state: aborted in consensus but present in RequestSeal.IncludedXts",
					"xtID", key,
					"slot", requestSeal.Slot)
			}
			log.Info("[SSV] Keeping transaction (included in RequestSeal)", "xtID", key)
			continue
		}

		// XT not included in RequestSeal - drop it.
		// If SP wants to retry, it will send a fresh StartSC in a future slot.
		outcome := xtOutcomeNotIncluded
		if isAborted {
			outcome = xtOutcomeAborted
		}
		removedPut, removedOriginal := b.finalizeXt(key, outcome)
		if removedPut+removedOriginal > 0 {
			if isAborted {
				droppedAborted += removedPut + removedOriginal
				log.Info("[SSV] Dropped aborted XT after RequestSeal",
					"xtID", key,
					"putInboxRemoved", removedPut,
					"originalRemoved", removedOriginal)
			} else {
				droppedNonIncluded += removedPut + removedOriginal
				log.Info("[SSV] Dropped non-included XT after RequestSeal",
					"xtID", key,
					"putInboxRemoved", removedPut,
					"originalRemoved", removedOriginal,
					"reason", "scp_incomplete_before_seal")
			}
		} else {
			log.Warn("[SSV] RequestSeal cleanup: no transactions found for xtID", "xtID", key)
		}
	}

	if droppedNonIncluded > 0 || droppedAborted > 0 {
		log.Info("[SSV] RequestSeal cleanup complete",
			"slot", requestSeal.Slot,
			"droppedNonIncluded", droppedNonIncluded,
			"droppedAborted", droppedAborted)
	}

	// Store RequestSeal info after cleanup
	b.rsMutex.Lock()
	b.lastRequestSealIncluded = make([][]byte, len(requestSeal.IncludedXts))
	for i, xt := range requestSeal.IncludedXts {
		// copy to avoid aliasing
		dup := make([]byte, len(xt))
		copy(dup, xt)
		b.lastRequestSealIncluded[i] = dup
	}
	b.lastRequestSealSlot = requestSeal.Slot
	b.rsMutex.Unlock()

	// Send ALL stored blocks if available
	b.pendingBlockMutex.RLock()
	hasStoredBlocks := len(b.pendingBlocks) > 0
	blockCount := len(b.pendingBlocks)
	b.pendingBlockMutex.RUnlock()

	if hasStoredBlocks {
		log.Info("[SSV] Sending stored blocks after RequestSeal", "slot", requestSeal.Slot, "blockCount", blockCount)
		if err := b.sendStoredL2Block(ctx); err != nil {
			log.Error("[SSV] Failed to send stored L2Blocks after RequestSeal", "err", err, "slot", requestSeal.Slot)
		}
	} else {
		log.Info("[SSV] RequestSeal received with no stored blocks yet (will build now)", "slot", requestSeal.Slot)
	}

	// If there are still ready pairs staged after sealing existing blocks, force a payload rebuild
	// so they are included before the next slot transition.
	pendingPairs := 0
	b.sequencerTxMutex.RLock()
	if b.txTracker != nil {
		pendingPairs = b.txTracker.readyPairCount()
	}
	b.sequencerTxMutex.RUnlock()
	if pendingPairs > 0 {
		if miner := b.eth.miner; miner != nil {
			log.Info("[SSV] Pending sequencer pairs remain after RequestSeal; triggering payload rebuild",
				"slot", requestSeal.Slot,
				"pendingPairs", pendingPairs)
			miner.InvalidatePendingCache()
		}
	}

	return nil
}

// NotifyStateChange notifies the miner of sequencer state changes
// SSV
func (b *EthAPIBackend) NotifyStateChange(from, to sequencer.State, slot uint64) error {
	log.Info("[SSV] SBCP state change", "from", from.String(), "to", to.String(), "slot", slot)

	// When SCP completes (Building-Locked → Building-Free), force miner to rebuild payload
	// with newly added SCP transactions. Without this, the payload remains stale and
	// RequestSeal seals a block without the SCP transactions.
	if from == sequencer.StateBuildingLocked && to == sequencer.StateBuildingFree {
		if miner := b.eth.miner; miner != nil {
			log.Info("[SSV] Forcing payload rebuild after SCP completion", "slot", slot)
			miner.InvalidatePendingCache()
		}
	}
	return nil
}

// cleanupAbortedTransactionCallback removes aborted cross-chain transactions from the pending pool
// immediately upon consensus decision. This ensures transactions rejected by the consensus layer
// cannot be included in blocks, maintaining atomic transaction semantics where transactions are
// either fully committed or fully excluded across all participating chains.
//
// This callback is invoked by the sequencer coordinator and complements the RequestSeal cleanup,
// providing early removal to handle the case where blocks are built before the seal message arrives.
// SSV
func (b *EthAPIBackend) cleanupAbortedTransactionCallback(_ context.Context, xtID *rollupv1.XtID) error {
	if xtID == nil {
		return nil
	}
	key := xtID.Hex()

	// Clear any pending blocks that contain the aborted transaction
	// This prevents sending stale blocks built before the abort decision
	b.pendingBlockMutex.Lock()
	if len(b.pendingBlocks) > 0 {
		// Check if any pending blocks contain transactions with this xtID
		needsClear := false
		b.sequencerTxMutex.RLock()
		if b.txTracker != nil {
			for _, block := range b.pendingBlocks {
				for _, tx := range block.Transactions() {
					if record := b.txTracker.record(tx.Hash()); record != nil && record.xtID == key {
						needsClear = true
						break
					}
				}
				if needsClear {
					break
				}
			}
		}
		b.sequencerTxMutex.RUnlock()

		if needsClear {
			clearedCount := len(b.pendingBlocks)
			b.pendingBlocks = nil
			log.Info("[SSV] Cleared pending blocks containing aborted transaction",
				"xtID", key,
				"blockCount", clearedCount)

			// Force miner to rebuild payload without the aborted transaction
			if miner := b.eth.miner; miner != nil {
				log.Info("[SSV] Forcing payload rebuild after clearing aborted blocks", "xtID", key)
				miner.InvalidatePendingCache()
			}
		}
	}
	b.pendingBlockMutex.Unlock()

	// Remove staged transactions
	removedPut, removedOriginal := b.finalizeXt(key, xtOutcomeAborted)
	if removedPut+removedOriginal > 0 {
		log.Info("[SSV] Dropped aborted transactions via callback",
			"xtID", key,
			"putInboxRemoved", removedPut,
			"originalRemoved", removedOriginal)
	}
	return nil
}

// sendStoredL2Block sends the stored block as L2Block message
// SSV
func (b *EthAPIBackend) sendStoredL2Block(ctx context.Context) error {
	b.pendingBlockMutex.Lock()
	blocks := make([]*types.Block, len(b.pendingBlocks))
	copy(blocks, b.pendingBlocks)
	slot := b.pendingBlockSlot
	// Clear after copying
	b.pendingBlocks = nil
	b.pendingBlockMutex.Unlock()

	if len(blocks) == 0 {
		return fmt.Errorf("no stored blocks to send")
	}

	// Get RequestSeal inclusion list
	b.rsMutex.RLock()
	requestSealIncluded := make([][]byte, len(b.lastRequestSealIncluded))
	for i := range b.lastRequestSealIncluded {
		dup := make([]byte, len(b.lastRequestSealIncluded[i]))
		copy(dup, b.lastRequestSealIncluded[i])
		requestSealIncluded[i] = dup
	}
	b.rsMutex.RUnlock()

	// Get committed cross-chain tx hashes (tracked during block building)
	crossChainTxHashes := make(map[common.Hash]bool)
	if b.txTracker != nil {
		b.sequencerTxMutex.RLock()
		for hash := range b.txTracker.committedHashSet() {
			crossChainTxHashes[hash] = true
		}
		b.sequencerTxMutex.RUnlock()
	}

	if len(requestSealIncluded) > 0 && len(crossChainTxHashes) == 0 {
		log.Warn("[SSV] RequestSeal has included XTs but no committed cross-chain hashes recorded",
			"slot", slot,
			"includedXts", len(requestSealIncluded))
	}

	log.Info("[SSV] Submitting L2 blocks to shared publisher",
		"slot", slot,
		"blockCount", len(blocks),
		"committedXTs", len(crossChainTxHashes))

	var lastL2Block *rollupv1.L2Block
	blocksWithXTs := 0

	// Send ALL blocks built during this slot
	for _, block := range blocks {
		// Determine IncludedXts by checking if block contains cross-chain txs
		// If block has any cross-chain txs, use RequestSeal list; otherwise empty
		var included [][]byte
		hasXTs := false
		for _, tx := range block.Transactions() {
			if crossChainTxHashes[tx.Hash()] {
				hasXTs = true
				break
			}
		}
		if hasXTs {
			included = requestSealIncluded
			blocksWithXTs++
		} else {
			included = [][]byte{}
		}
		// RLP encode the block
		var buf bytes.Buffer
		if err := block.EncodeRLP(&buf); err != nil {
			log.Error("[SSV] Failed to RLP encode block", "err", err, "blockHash", block.Hash().Hex())
			return err
		}

		l2 := &rollupv1.L2Block{
			Slot:            slot,
			ChainId:         b.ChainConfig().ChainID.Bytes(),
			BlockNumber:     block.NumberU64(),
			BlockHash:       block.Hash().Bytes(),
			ParentBlockHash: block.ParentHash().Bytes(),
			IncludedXts:     included,
			Block:           buf.Bytes(),
		}

		msg := &rollupv1.Message{
			SenderId: b.ChainConfig().ChainID.String(),
			Payload:  &rollupv1.Message_L2Block{L2Block: l2},
		}

		if err := b.spClient.Send(ctx, msg); err != nil {
			log.Error("[SSV] Failed to send L2Block to shared publisher", "err", err, "slot", slot)
			return err
		}

		// Mark included XTs as sent in consensus layer for EACH block with XTs
		// This is important so the consensus layer knows which XTs were committed
		if b.coordinator != nil && b.coordinator.Consensus() != nil && len(included) > 0 {
			if err := b.coordinator.Consensus().OnL2BlockCommitted(ctx, l2); err != nil {
				log.Warn("[SSV] Consensus OnL2BlockCommitted warning", "err", err, "slot", slot)
			}
		}

		lastL2Block = l2
	}

	log.Info("[SSV] Successfully submitted L2 blocks",
		"slot", slot,
		"totalBlocks", len(blocks),
		"blocksWithXTs", blocksWithXTs)

	if len(crossChainTxHashes) > 0 {
		b.clearCommittedSequencerTransactions(crossChainTxHashes)
	}

	// Call OnBlockBuildingComplete ONCE after all blocks sent (for state transition)
	// Use the last block (doesn't matter which one, just need to trigger the transition)
	if b.coordinator != nil && lastL2Block != nil {
		if err := b.coordinator.OnBlockBuildingComplete(ctx, lastL2Block, true); err != nil {
			log.Warn("[SSV] Coordinator OnBlockBuildingComplete warning", "err", err, "slot", slot)
		}
	}

	// After sending all blocks, reset RequestSeal state
	b.rsMutex.Lock()
	b.lastRequestSealIncluded = nil
	b.lastRequestSealSlot = 0
	b.rsMutex.Unlock()

	return nil
}

func (b *EthAPIBackend) simulateXTRequestForSBCP(
	ctx context.Context,
	xtReq *rollupv1.XTRequest,
	xtID *rollupv1.XtID,
) (bool, error) {
	log.Info("[SSV] Simulating XT request for SBCP",
		"xtID", xtID.Hex(),
		"chainID", b.ChainConfig().ChainID,
		"txCount", len(xtReq.Transactions))

	chainID := b.ChainConfig().ChainID

	// Extract local transactions
	localTxs := make([]*rollupv1.TransactionRequest, 0)
	for _, txReq := range xtReq.Transactions {
		txChainID := new(big.Int).SetBytes(txReq.ChainId)
		if txChainID.Cmp(chainID) == 0 {
			localTxs = append(localTxs, txReq)
		}
	}

	if len(localTxs) == 0 {
		log.Info("[SSV] No local transactions to simulate", "xtID", xtID.Hex())
		return true, nil
	}

	mailboxProcessor := NewMailboxProcessor(
		b.ChainConfig().ChainID.Uint64(),
		b.GetMailboxAddresses(),
		b.sequencerClients,
		b.coordinator,
		b.coordinatorKey,
		b.coordinatorAddr,
		b,
	)

	coordinationStates := make([]*SimulationState, 0)
	txDone := make(map[string]struct{})

	// Simulate each local transaction
	for _, txReq := range localTxs {
		for _, txBytes := range txReq.Transaction {
			tx := &types.Transaction{}
			if err := tx.UnmarshalBinary(txBytes); err != nil {
				return false, fmt.Errorf("failed to unmarshal transaction: %w", err)
			}

			traceResult, err := b.SimulateTransaction(ctxWithXtID(ctx, xtID), tx, rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber))
			if err != nil {
				return false, fmt.Errorf("failed to simulate transaction: %w", err)
			}

			simState, err := mailboxProcessor.AnalyzeTransaction(traceResult, nil, nil, tx)
			if err != nil {
				return false, fmt.Errorf("failed to analyze transaction: %w", err)
			}

			coordinationStates = append(coordinationStates, simState)
			log.Info("[SSV] Transaction analyzed",
				"txHash", tx.Hash().Hex(),
				"requiresCoordination", simState.RequiresCoordination(),
				"dependencies", len(simState.Dependencies),
				"outbound", len(simState.OutboundMessages))
		}
	}

	allSentMsgs := make([]CrossRollupMessage, 0)
	allFulfilledDeps := make([]CrossRollupDependency, 0)

	for _, simState := range coordinationStates {
		if !simState.RequiresCoordination() {
			continue
		}

		log.Info("[SSV] Transaction requires cross-rollup coordination",
			"txHash", simState.Tx.Hash().Hex(),
			"dependencies", len(simState.Dependencies),
			"outbound", len(simState.OutboundMessages))

		sentMsgs, fulfilledDeps, err := mailboxProcessor.handleCrossRollupCoordination(ctx, simState, xtID)
		if err != nil {
			return false, fmt.Errorf("failed to handle cross-rollup coordination: %w", err)
		}

		log.Info(
			"[SSV] Cross-rollup coordination completed",
			"xtID",
			xtID.Hex(),
			"sent",
			len(sentMsgs),
			"received",
			len(fulfilledDeps),
		)

		allSentMsgs = append(allSentMsgs, sentMsgs...)
		allFulfilledDeps = append(allFulfilledDeps, fulfilledDeps...)
	}

	// Create putInbox transactions for fulfilled dependencies
	if len(allFulfilledDeps) > 0 {
		log.Info("[SSV] Creating putInbox transactions for fulfilled dependencies", "count", len(allFulfilledDeps))

		poolNonce, err := b.GetPoolNonce(ctx, b.coordinatorAddr)
		if err != nil {
			return false, fmt.Errorf("failed to get nonce: %w", err)
		}

		for _, dep := range allFulfilledDeps {
			putInboxTx, err := mailboxProcessor.createPutInboxTx(dep, poolNonce)
			if err != nil {
				return false, fmt.Errorf("failed to create putInbox transaction: %w", err)
			}

			if err := b.SubmitSequencerTransaction(ctxWithXtID(ctx, xtID), putInboxTx, true); err != nil {
				return false, fmt.Errorf("failed to submit putInbox transaction: %w", err)
			}
			// xtID is tracked inside the putInbox pool entry mapped by the signed hash
			poolNonce++
		}

		// Re-simulate after putInbox to detect ACK messages that need to be sent
		for i, simState := range coordinationStates {
			traceResult, err := b.SimulateTransaction(ctxWithXtID(ctx, xtID), simState.Tx, rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber))
			if err != nil {
				continue
			}

			newSimState, err := mailboxProcessor.AnalyzeTransaction(
				traceResult,
				allSentMsgs,
				allFulfilledDeps,
				simState.Tx,
			)
			if err != nil {
				continue
			}

			log.Info("[SSV] Re-simulation mailbox state",
				"txHash", simState.Tx.Hash().Hex(),
				"success", newSimState.Success,
				"deps", len(newSimState.Dependencies),
			)
			coordinationStates[i] = newSimState
			log.Info(
				"[SSV] Re-simulation successful for transaction",
				"txHash",
				simState.Tx.Hash().Hex(),
				"xtID",
				xtID.Hex(),
			)

			// Send any new outbound messages detected in re-simulation
			for _, outMsg := range newSimState.OutboundMessages {
				log.Info("[SSV] Detected new outbound message in re-simulation",
					"xtID", xtID.Hex(),
					"srcChain", outMsg.SourceChainID,
					"destChain", outMsg.DestChainID,
					"sessionId", outMsg.SessionID,
					"label", string(outMsg.Label))

				if err := mailboxProcessor.sendCIRCMessage(ctx, &outMsg, xtID); err != nil {
					log.Error("[SSV] Failed to send CIRC message", "error", err, "xtID", xtID.Hex())
					continue
				}
			}

			if len(newSimState.OutboundMessages) > 0 {
				log.Info(
					"[SSV] Successfully sent CIRC messages after putInbox",
					"count",
					len(newSimState.OutboundMessages),
					"xtID",
					xtID.Hex(),
				)
			}

			// Pool transactions immediately when they become successful,
			// unless the XT has been aborted meanwhile.
			_, done := txDone[simState.Tx.Hash().Hex()]
			xtKey := xtID.Hex()
			if newSimState.Success && !done && len(newSimState.Dependencies) == 0 {
				if b.isXtAborted(xtKey) {
					log.Info("[SSV] Not pooling aborted XT after re-simulation", "xtID", xtKey, "hash", simState.Tx.Hash().Hex())
				} else {
					log.Info("[SSV] Pooling transaction after re-simulation", "hash", simState.Tx.Hash().Hex(), "xtID", xtKey)
					b.poolPayloadTx(ctxWithXtID(ctx, xtID), simState.Tx)
					b.assignXtKeyToHash(simState.Tx, xtID)
					txDone[simState.Tx.Hash().Hex()] = struct{}{}
				}
			}
		}
	}

	// Final check - pool any remaining successful transactions that weren't pooled yet,
	// unless the XT has been aborted.
	for _, simState := range coordinationStates {
		tx := simState.Tx
		_, done := txDone[tx.Hash().Hex()]
		xtKey := xtID.Hex()
		if simState.Success && !done && len(simState.Dependencies) == 0 {
			if b.isXtAborted(xtKey) {
				log.Info("[SSV] Not pooling aborted XT in final sweep", "xtID", xtKey, "hash", tx.Hash().Hex())
			} else {
				log.Info("[SSV] Pooling remaining successful transaction", "hash", tx.Hash().Hex(), "xtID", xtKey)
				b.poolPayloadTx(ctxWithXtID(ctx, xtID), tx)
				b.assignXtKeyToHash(tx, xtID)
				txDone[tx.Hash().Hex()] = struct{}{}
			}
		}
	}

	// Check if all transactions are successful
	allSuccessful := successfulAll(coordinationStates)
	log.Info(
		"[SSV] SBCP simulation completed",
		"xtID",
		xtID.Hex(),
		"allSuccessful",
		allSuccessful,
		"pooled_original_txs",
		len(b.GetPendingOriginalTxs()),
	)

	return allSuccessful, nil
}
