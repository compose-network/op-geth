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
	"errors"
	"fmt"

	rollupv1 "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/transport"

	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/ssv"
	"github.com/ethereum/go-ethereum/core/tracing"
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

	spconsensus "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/consensus"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/superblock/sequencer"
)

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
	pendingXTEntries []sequencerTxEntry
	pendingByHash    map[common.Hash]int // Maps transaction hash to index in pendingXTEntries for O(1) lookups

	// SSV: Track last RequestSeal inclusion list for SBCP
	rsMutex                 sync.RWMutex
	lastRequestSealIncluded [][]byte
	lastRequestSealSlot     uint64

	// SSV: Store built blocks to send after RequestSeal
	pendingBlockMutex sync.RWMutex
	pendingBlocks     []*types.Block
	pendingBlockSlot  uint64

	// SSV: Keep copy of committed transactions for SP submission
	// Transactions are staged in pendingXTEntries and cleared after block building,
	// but we need them to determine which blocks have XTs when sending to SP
	committedTxsMutex sync.RWMutex
	committedTxHashes map[common.Hash]bool // Hashes of txs that were committed in blocks during this slot
}

type sequencerTxKind int

const (
	sequencerTxOriginal sequencerTxKind = iota
	sequencerTxPutInbox
)

type sequencerTxEntry struct {
	tx   *types.Transaction
	xtID string // Cross-chain transaction ID this tx belongs to (hex-encoded)
	kind sequencerTxKind
}

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

func (b *EthAPIBackend) GetCanonicalReceipt(
	tx *types.Transaction,
	blockHash common.Hash,
	blockNumber, blockIndex uint64,
) (*types.Receipt, error) {
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
	return b.eth.txPool.PoolNonce(addr), nil
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

	mailboxAddresses := b.GetMailboxAddresses()
	tracer := native.NewSSVTracer(mailboxAddresses)

	vmConfig := vm.Config{}
	if b.eth.blockchain.GetVMConfig() != nil {
		vmConfig = *b.eth.blockchain.GetVMConfig()
	}
	vmConfig.Tracer = tracer.Hooks()
	vmConfig.EnablePreimageRecording = true

	blockContext := core.NewEVMBlockContext(header, b.eth.blockchain, nil, b.ChainConfig(), stateDB)

	evm := vm.NewEVM(blockContext, stateDB, b.ChainConfig(), vmConfig)

	if ctx.Value("simulation") != nil {
		for _, staged := range b.GetPendingPutInboxTxs() {
			if staged == nil || staged.Hash() == tx.Hash() {
				continue
			}

			stageMsg, err := core.TransactionToMessage(staged, signer, header.BaseFee)
			if err != nil {
				log.Warn("[SSV] Failed to build staged putInbox message", "txHash", staged.Hash(), "err", err)
				continue
			}

			stageGasPool := new(core.GasPool).AddGas(header.GasLimit)
			stateDB.SetTxContext(staged.Hash(), stateDB.TxIndex()+1)
			if wants := staged.Nonce(); stateDB.GetNonce(stageMsg.From) != wants {
				stateDB.SetNonce(stageMsg.From, wants, tracing.NonceChangeUnspecified)
			}
			if _, err := core.ApplyMessage(evm, stageMsg, stageGasPool); err != nil {
				log.Warn("[SSV] Failed to pre-apply putInbox transaction", "txHash", staged.Hash(), "err", err)
				continue
			}
			log.Info("[SSV] Pre-applied putInbox for simulation",
				"stagedHash", staged.Hash().Hex(),
			)
			stateDB.Finalise(true)
		}
	}

	stateDB.SetTxContext(tx.Hash(), stateDB.TxIndex()+1)

	gasPool := new(core.GasPool).AddGas(header.GasLimit)
	result, err := core.ApplyMessage(evm, msg, gasPool)
	if err != nil {
		log.Error("[SSV] EVM execution failed during simulation - REASON: evm_apply_message_error",
			"txHash", tx.Hash().Hex(),
			"error", err,
			"failure_reason", "evm_apply_message_error")
		return nil, err
	}

	traceResult := tracer.GetTraceResult()
	traceResult.ExecutionResult = result

	return traceResult, nil
}

// SubmitSequencerTransaction submits a transaction with a priority flag.
// SSV
func (b *EthAPIBackend) SubmitSequencerTransaction(ctx context.Context, tx *types.Transaction, isPutInbox bool) error {
	if err := b.validateSequencerTransaction(tx); err != nil {
		log.Error("[SSV] Sequencer transaction validation failed", "err", err, "txHash", tx.Hash().Hex())
		return fmt.Errorf("sequencer transaction validation failed: %w", err)
	}

	if isPutInbox {
		b.AddPendingPutInboxTx(tx)
	}

	// Always inject sequencer transactions into txpool since SubmitSequencerTransaction
	// is only called for real sequencer transactions that should be included in blocks
	if err := b.sendTx(ctx, tx); err != nil {
		log.Warn(
			"[SSV] Failed to inject sequencer tx into txpool (continuing with staged include)",
			"err",
			err,
			"txHash",
			tx.Hash().Hex(),
		)
	}
	return nil
}

// ConfigureMailboxes sets the mailbox contract addresses for known rollups.
// SSV
func (b *EthAPIBackend) ConfigureMailboxes(raw map[uint64]string) error {
	ordered := []uint64{native.RollupAChainID, native.RollupBChainID}
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

// AddPendingPutInboxTx adds a putInbox transaction to the pending list.
// SSV
func (b *EthAPIBackend) AddPendingPutInboxTx(tx *types.Transaction) {
	b.sequencerTxMutex.Lock()
	b.addSequencerEntryLocked(tx, sequencerTxPutInbox)
	putCount := b.countEntriesByKindLocked(sequencerTxPutInbox)
	b.sequencerTxMutex.Unlock()

	log.Info("[SSV] Added putInbox transaction to mempool",
		"txHash", tx.Hash().Hex(),
		"totalPending", putCount,
		"nonce", tx.Nonce(),
	)

	// Invalidate pending block cache since transaction state changed
	// This ensures fresh pending blocks reflect new sequencer transactions
	if miner := b.eth.miner; miner != nil {
		miner.InvalidatePendingCache()
	}
}

// GetPendingPutInboxTxs returns all pending putInbox transactions.
// SSV
func (b *EthAPIBackend) GetPendingPutInboxTxs() []*types.Transaction {
	return b.listTransactionsByKind(sequencerTxPutInbox)
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
	b.sequencerTxMutex.Lock()
	defer b.sequencerTxMutex.Unlock()

	putInboxCount := 0
	originalCount := 0
	for _, entry := range b.pendingXTEntries {
		if entry.kind == sequencerTxPutInbox {
			putInboxCount++
		} else {
			originalCount++
		}
	}

	b.pendingXTEntries = nil
	b.pendingByHash = make(map[common.Hash]int)

	log.Info("[SSV] Cleared sequencer transactions",
		"putInbox", putInboxCount,
		"original", originalCount)

	if miner := b.eth.miner; miner != nil && (putInboxCount > 0 || originalCount > 0) {
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

// GetOrderedTransactionsForBlock returns only sequencer-managed transactions in
// the correct order for block inclusion. Normal mempool transactions are
// included by the miner after this list, and must not be returned here.
// SSV
func (b *EthAPIBackend) GetOrderedTransactionsForBlock(ctx context.Context) (types.Transactions, error) {
	if b.coordinator == nil {
		// Non-SBCP mode: return sequencer-managed txs only; miner appends normals
		return b.buildSequencerOnlyList(), nil
	}

	currentState := b.coordinator.GetState()

	switch currentState {
	case sequencer.StateBuildingLocked:
		// During coordination, exclude cross-chain txs - they'll be included after decision
		return types.Transactions{}, nil
	case sequencer.StateBuildingFree, sequencer.StateSubmission:
		// After SCP completes (BuildingFree) or during final submission, include ready transactions
		// This ensures transactions are committed in the first possible block after simulation/decision
		return b.buildSequencerOnlyList(), nil
	default:
		return types.Transactions{}, nil
	}
}

// buildSequencerOnlyList assembles only the sequencer-managed transactions in the
// correct internal order: putInbox(), then the original user txs.
// Normal mempool transactions are not part of this list.
func (b *EthAPIBackend) buildSequencerOnlyList() types.Transactions {
	var orderedTxs types.Transactions

	for _, tx := range b.GetPendingPutInboxTxs() {
		orderedTxs = append(orderedTxs, tx)
	}
	for _, tx := range b.GetPendingOriginalTxs() {
		orderedTxs = append(orderedTxs, tx)
	}

	return orderedTxs
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

	log.Debug("[SSV] Sequencer transaction validated",
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
		_ = b.coordinator.OnBlockBuildingStart(ctx, b.coordinator.GetCurrentSlot())
	}

	return nil
}

// OnBlockBuildingComplete is called when block building completes
// Per SBCP spec: Always STORE all blocks, never send here. Sending happens in NotifyRequestSeal.
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

	// Get current cross-chain tx hashes BEFORE clearing
	b.sequencerTxMutex.RLock()
	crossChainTxHashes := make(map[common.Hash]bool, len(b.pendingXTEntries))
	for _, entry := range b.pendingXTEntries {
		crossChainTxHashes[entry.tx.Hash()] = true
	}
	b.sequencerTxMutex.RUnlock()

	// Identify which cross-chain txs are in this block
	txsToRemove := make(map[common.Hash]bool)
	for _, tx := range block.Transactions() {
		if crossChainTxHashes[tx.Hash()] {
			b.committedTxsMutex.Lock()
			b.committedTxHashes[tx.Hash()] = true
			b.committedTxsMutex.Unlock()
			txsToRemove[tx.Hash()] = true
		}
	}

	if len(txsToRemove) > 0 {
		b.clearCommittedSequencerTransactions(txsToRemove)
		log.Info("[SSV] Cleared committed sequencer transactions after block build",
			"slot", slot,
			"blockNumber", block.NumberU64(),
			"cleared", len(txsToRemove))
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

		log.Debug("[SSV] Skipping duplicate block (identical hash already stored)",
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

	removeSet := make(map[common.Hash]struct{}, len(committed))
	for hash := range committed {
		removeSet[hash] = struct{}{}
	}

	b.sequencerTxMutex.Lock()
	removedPutInbox, removedOriginal := b.removeEntriesByHashLocked(removeSet)
	b.sequencerTxMutex.Unlock()

	if removedPutInbox > 0 || removedOriginal > 0 {
		log.Debug("[SSV] Cleared committed cross-chain txs after delivery",
			"putInboxRemoved", removedPutInbox,
			"originalRemoved", removedOriginal)
	}
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
	log.Debug("[SSV] Re-simulating transaction",
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

	log.Debug("[SSV] Transaction re-simulation successful",
		"txHash", tx.Hash().Hex(),
		"gasUsed", traceResult.ExecutionResult.UsedGas,
		"mailboxOps", len(traceResult.Operations),
		"xtID", xtID.Hex())

	return true, nil
}

// waitForPutInboxTransactionsToBeProcessed waits for putInbox transactions to be included
// SSV
func (b *EthAPIBackend) waitForPutInboxTransactionsToBeProcessed() error {
	putInboxTxs := b.GetPendingPutInboxTxs()
	if len(putInboxTxs) == 0 {
		return nil
	}

	// Wait for transactions to be in txpool
	for _, tx := range putInboxTxs {
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)

		func() {
			defer ticker.Stop() // Now properly scoped to this transaction
			for {
				select {
				case <-timeout:
					log.Error("timed out waiting for putInbox transaction appearance in pool")
					return // This will trigger the defer and stop the ticker
				case <-ticker.C:
					if poolTx := b.GetPoolTransaction(tx.Hash()); poolTx != nil {
						log.Info("[SSV] found putInbox transaction in pool", "hash", tx.Hash().Hex())
						// small settling delay so miner refresh picks it up in pending view
						time.Sleep(450 * time.Millisecond)
						return // This will trigger the defer and stop the ticker
					}
				}
			}
		}()
	}

	return nil
}

func (b *EthAPIBackend) poolPayloadTx(tx *types.Transaction) {
	b.sequencerTxMutex.Lock()
	b.addSequencerEntryLocked(tx, sequencerTxOriginal)
	b.sequencerTxMutex.Unlock()

	if miner := b.eth.miner; miner != nil {
		miner.InvalidatePendingCache()
	}
}

// assignXtKeyToHash associates a transaction with its cross-chain transaction ID.
// This enables per-XT cleanup when transactions are aborted.
func (b *EthAPIBackend) assignXtKeyToHash(tx *types.Transaction, xtID *rollupv1.XtID) {
	if tx == nil || xtID == nil {
		return
	}
	key := hexutil.Encode(xtID.Hash)

	b.sequencerTxMutex.Lock()
	defer b.sequencerTxMutex.Unlock()

	if b.pendingByHash != nil {
		if idx, ok := b.pendingByHash[tx.Hash()]; ok {
			// Bounds check to prevent panic
			if idx >= 0 && idx < len(b.pendingXTEntries) {
				b.pendingXTEntries[idx].xtID = key
			}
		}
	}
}

func (b *EthAPIBackend) addSequencerEntryLocked(tx *types.Transaction, kind sequencerTxKind) {
	if b.pendingByHash == nil {
		b.pendingByHash = make(map[common.Hash]int)
	}
	hash := tx.Hash()
	if idx, exists := b.pendingByHash[hash]; exists {
		if kind == sequencerTxPutInbox {
			b.pendingXTEntries[idx].kind = sequencerTxPutInbox
		}
		return
	}
	b.pendingXTEntries = append(b.pendingXTEntries, sequencerTxEntry{tx: tx, kind: kind})
	b.pendingByHash[hash] = len(b.pendingXTEntries) - 1
}

func (b *EthAPIBackend) countEntriesByKindLocked(kind sequencerTxKind) int {
	count := 0
	for _, entry := range b.pendingXTEntries {
		if entry.kind == kind {
			count++
		}
	}
	return count
}

func (b *EthAPIBackend) listTransactionsByKind(kind sequencerTxKind) []*types.Transaction {
	b.sequencerTxMutex.RLock()
	defer b.sequencerTxMutex.RUnlock()

	result := make([]*types.Transaction, 0)
	for _, entry := range b.pendingXTEntries {
		if entry.kind == kind {
			result = append(result, entry.tx)
		}
	}
	return result
}

func (b *EthAPIBackend) rebuildPendingIndexLocked() {
	b.pendingByHash = make(map[common.Hash]int, len(b.pendingXTEntries))
	for i, entry := range b.pendingXTEntries {
		b.pendingByHash[entry.tx.Hash()] = i
	}
}

func (b *EthAPIBackend) removeEntriesMatchingLocked(predicate func(sequencerTxEntry) bool) (int, int) {
	if len(b.pendingXTEntries) == 0 {
		return 0, 0
	}

	removedPutInbox := 0
	removedOriginal := 0
	filtered := b.pendingXTEntries[:0]
	for _, entry := range b.pendingXTEntries {
		if predicate(entry) {
			if entry.kind == sequencerTxPutInbox {
				removedPutInbox++
			} else {
				removedOriginal++
			}
			continue
		}
		filtered = append(filtered, entry)
	}
	b.pendingXTEntries = filtered
	b.rebuildPendingIndexLocked()
	return removedPutInbox, removedOriginal
}

func (b *EthAPIBackend) removeEntriesByHashLocked(remove map[common.Hash]struct{}) (int, int) {
	if len(remove) == 0 {
		return 0, 0
	}
	return b.removeEntriesMatchingLocked(func(entry sequencerTxEntry) bool {
		_, ok := remove[entry.tx.Hash()]
		return ok
	})
}

func (b *EthAPIBackend) dropTransactionsForXtKey(key string) (int, int) {
	if key == "" {
		return 0, 0
	}

	b.sequencerTxMutex.Lock()
	removedPutInbox, removedOriginal := b.removeEntriesMatchingLocked(func(entry sequencerTxEntry) bool {
		return entry.xtID == key
	})
	b.sequencerTxMutex.Unlock()

	if removedPutInbox+removedOriginal > 0 {
		if miner := b.eth.miner; miner != nil {
			miner.InvalidatePendingCache()
		}
	}

	return removedPutInbox, removedOriginal
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
		})

		// Set miner notifier and start
		b.coordinator.SetMinerNotifier(b)
	}
}

// NotifySlotStart notifies the backend when a new SBCP slot begins.
// SSV
func (b *EthAPIBackend) NotifySlotStart(startSlot *rollupv1.StartSlot) error {
	log.Info("[SSV] Notify miner: StartSlot", "slot", startSlot.Slot, "next_sb", startSlot.NextSuperblockNumber)

	// Clear any pending blocks from previous slot when new slot starts
	b.pendingBlockMutex.Lock()
	if len(b.pendingBlocks) > 0 {
		log.Warn("[SSV] Clearing unsent blocks from previous slot",
			"prevSlot", b.pendingBlockSlot,
			"newSlot", startSlot.Slot,
			"blockCount", len(b.pendingBlocks))
	}
	b.pendingBlocks = nil
	b.pendingBlockSlot = startSlot.Slot
	b.pendingBlockMutex.Unlock()

	return nil
}

// NotifyRequestSeal notifies the backend when RequestSeal is received from coordinator.
// SSV
func (b *EthAPIBackend) NotifyRequestSeal(ctx context.Context, requestSeal *rollupv1.RequestSeal) error {
	log.Info("[SSV] Notify miner: RequestSeal", "slot", requestSeal.Slot, "included_xts", len(requestSeal.IncludedXts))

	// Store RequestSeal info first
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

	included := make(map[string]struct{}, len(requestSeal.IncludedXts))
	for _, xt := range requestSeal.IncludedXts {
		included[hexutil.Encode(xt)] = struct{}{}
	}

	b.sequencerTxMutex.RLock()
	pendingKeys := make(map[string]struct{})
	for _, entry := range b.pendingXTEntries {
		if entry.xtID != "" {
			pendingKeys[entry.xtID] = struct{}{}
		}
	}
	b.sequencerTxMutex.RUnlock()

	for key := range pendingKeys {
		if _, ok := included[key]; ok {
			continue
		}

		removedPut, removedOriginal := b.dropTransactionsForXtKey(key)
		if removedPut+removedOriginal > 0 {
			log.Info("[SSV] Dropped staged sequencer transactions after RequestSeal abort",
				"xtID", key,
				"putInboxRemoved", removedPut,
				"originalRemoved", removedOriginal)
		}
	}

	return nil
}

// NotifyStateChange notifies the miner of sequencer state changes
// SSV
func (b *EthAPIBackend) NotifyStateChange(from, to sequencer.State, slot uint64) error {
	log.Debug("[SSV] SBCP state change", "from", from.String(), "to", to.String(), "slot", slot)
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
	b.committedTxsMutex.RLock()
	crossChainTxHashes := make(map[common.Hash]bool)
	for hash := range b.committedTxHashes {
		crossChainTxHashes[hash] = true
	}
	b.committedTxsMutex.RUnlock()

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

	// Clear committed tx hashes for next slot
	b.committedTxsMutex.Lock()
	b.committedTxHashes = make(map[common.Hash]bool)
	b.committedTxsMutex.Unlock()

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

			traceResult, err := b.SimulateTransaction(ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber))
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

			// Pool successful transactions immediately
			_, done := txDone[tx.Hash().Hex()]
			if simState.Success && !done && len(simState.Dependencies) == 0 {
				log.Info("[SSV] Pooling successful transaction immediately", "hash", tx.Hash().Hex())
				b.poolPayloadTx(tx)
				b.assignXtKeyToHash(tx, xtID)
				txDone[tx.Hash().Hex()] = struct{}{}
			}
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

		nonce, err := b.GetPoolNonce(ctx, b.coordinatorAddr)
		if err != nil {
			return false, fmt.Errorf("failed to get nonce: %w", err)
		}
		log.Info(
			"[SSV] Using coordinator address for putInbox nonce",
			"coordinatorAddr",
			b.coordinatorAddr.Hex(),
			"nonce",
			nonce,
		)

		// Create putInbox transactions
		nextNonce := nonce
		for _, dep := range allFulfilledDeps {
			putInboxTx, err := mailboxProcessor.createPutInboxTx(dep, nextNonce)
			if err != nil {
				return false, fmt.Errorf("failed to create putInbox transaction: %w", err)
			}

			if err := b.SubmitSequencerTransaction(ctx, putInboxTx, true); err != nil {
				return false, fmt.Errorf("failed to submit putInbox transaction: %w", err)
			}
			b.assignXtKeyToHash(putInboxTx, xtID)

			nextNonce++
		}

		// Wait for putInbox transactions to be processed
		if err := b.waitForPutInboxTransactionsToBeProcessed(); err != nil {
			return false, fmt.Errorf("failed to wait for putInbox transactions: %w", err)
		}

		// Re-simulate after putInbox to detect ACK messages that need to be sent
		for i, simState := range coordinationStates {
			traceResult, err := b.SimulateTransaction(
				ctx,
				simState.Tx,
				rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber),
			)
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

			// Send any ACK messages detected in re-simulation
			for _, outMsg := range newSimState.OutboundMessages {
				log.Info("[SSV] Detected new ACK message in re-simulation",
					"xtID", xtID.Hex(),
					"srcChain", outMsg.SourceChainID,
					"destChain", outMsg.DestChainID,
					"sessionId", outMsg.SessionID,
					"label", string(outMsg.Label))

				if err := mailboxProcessor.sendCIRCMessage(ctx, &outMsg, xtID); err != nil {
					log.Error("[SSV] Failed to send ACK CIRC message", "error", err, "xtID", xtID.Hex())
					continue
				}
			}

			if len(newSimState.OutboundMessages) > 0 {
				log.Info(
					"[SSV] Successfully sent ACK CIRC messages after putInbox",
					"count",
					len(newSimState.OutboundMessages),
					"xtID",
					xtID.Hex(),
				)
			}

			// Pool transactions immediately when they become successful
			_, done := txDone[simState.Tx.Hash().Hex()]
			if newSimState.Success && !done && len(newSimState.Dependencies) == 0 {
				log.Info("[SSV] Pooling transaction after re-simulation", "hash", simState.Tx.Hash().Hex())
				b.poolPayloadTx(simState.Tx)
				b.assignXtKeyToHash(simState.Tx, xtID)
				txDone[simState.Tx.Hash().Hex()] = struct{}{}
			}
		}
	}

	// Final check - pool any remaining successful transactions that weren't pooled yet
	for _, simState := range coordinationStates {
		tx := simState.Tx
		_, done := txDone[tx.Hash().Hex()]
		if simState.Success && !done && len(simState.Dependencies) == 0 {
			log.Info("[SSV] Pooling remaining successful transaction", "hash", tx.Hash().Hex())
			b.poolPayloadTx(tx)
			b.assignXtKeyToHash(tx, xtID)
			txDone[tx.Hash().Hex()] = struct{}{}
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
