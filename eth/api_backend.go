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
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/compose-network/specs/compose"
	composeproto "github.com/compose-network/specs/compose/proto"
	instanceproto "github.com/compose-network/specs/compose/scp"
	spconsensus "github.com/ethereum/go-ethereum/internal/xconsensus"
	rollupv1 "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	xsequencer "github.com/ethereum/go-ethereum/internal/xsuperblock/sequencer"
	"github.com/ethereum/go-ethereum/internal/xtransport"

	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/ssv"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/eth/tracers/native"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
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

// EthAPIBackend implements ethapi.Backend and tracers.Backend for full nodes
type EthAPIBackend struct {
	extRPCEnabled       bool
	allowUnprotectedTxs bool
	disableTxPool       bool
	eth                 *Ethereum
	gpo                 *gasprice.Oracle

	// SSV: Shared publisher + SBCP coordinator integration
	spClient         xtransport.Client
	coordinator      xsequencer.Coordinator
	sequencerClients map[string]xtransport.Client
	sequencerKey     *ecdsa.PrivateKey
	sequencerAddress common.Address
	coordinatorKey   *ecdsa.PrivateKey
	coordinatorAddr  common.Address
	mailboxAddresses []common.Address
	mailboxByChainID map[uint64]common.Address

	// SSV: PeriodSequencer transaction management
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

	// SSV: Keep copy of committed transactions for SP submission
	// Transactions are staged in pendingXTEntries and cleared after block building,
	// but we need them to determine which blocks have XTs when sending to SP
	committedTxsMutex sync.RWMutex
	committedTxHashes map[common.Hash]bool // Hashes of txs that were committed in blocks during this slot

	// Overrides for testing purposes only
	// TODO refactor dependency injection with interfaces to avoid these
	chainConfigOverride         *params.ChainConfig
	chainContextOverride        core.ChainContext
	stateByNumberOverride       func(ctx context.Context, number rpc.BlockNumber) (*state.StateDB, *types.Header, error)
	stateByNumberOrHashOverride func(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*state.StateDB, *types.Header, error)
}

const defaultPutInboxGas = 500000

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
	if b.chainConfigOverride != nil {
		return b.chainConfigOverride
	}
	if b.eth != nil && b.eth.blockchain != nil {
		return b.eth.blockchain.Config()
	}
	return params.AllEthashProtocolChanges
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
	if b.stateByNumberOverride != nil {
		return b.stateByNumberOverride(ctx, number)
	}
	// Pending state is only known by the miner
	if number == rpc.PendingBlockNumber {
		block, _, state := b.eth.miner.Pending(ctx)
		if block != nil && state != nil {
			//state.TxIndex() == 1
			//sequencerBalance := state.GetBalance(common.HexToAddress("0x0f10aF865F68F5aA1dDB7c5b5A1a0f396232C6Be"))
			//fmt.Println("[AFTER] PeriodSequencer balance: ", sequencerBalance.String())
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
	if b.stateByNumberOrHashOverride != nil {
		return b.stateByNumberOrHashOverride(ctx, blockNrOrHash)
	}
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
func (b *EthAPIBackend) HandleSPMessage(ctx context.Context, msg *composeproto.Message) ([]common.Hash, error) {
	if b.coordinator == nil {
		return nil, fmt.Errorf("coordinator not configured")
	}

	// If this call originates from local RPC (SendXTransaction) we set ctx value "forward".
	// Forward XTRequest to the SP over transport instead of handling locally.
	if forward, _ := ctx.Value("forward").(bool); forward {
		switch msg.Payload.(type) {
		case *composeproto.Message_XtRequest:
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
	msg *composeproto.Message,
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
func (b *EthAPIBackend) StartCallbackFn() spconsensus.StartFn {
	return func(ctx context.Context, from string, instance *composeproto.StartInstance) error {
		if err := b.coordinator.PeriodSequencer().OnStartInstance(compose.InstanceID(instance.InstanceId), compose.PeriodID(instance.PeriodId), compose.SequenceNumber(instance.SequenceNumber)); err != nil {
			return err
		}
		return b.coordinator.InstanceSequencer().StartInstance(instance)
	}
}

func (b *EthAPIBackend) MailboxMsgCallbackFn() spconsensus.MailboxMsgFn {
	return func(ctx context.Context, instanceID *compose.InstanceID, mailboxMsg composeproto.MailboxMessage) error {
		if instanceID == nil {
			return fmt.Errorf("mailbox callback received nil instance ID")
		}
		if b.coordinator == nil || b.coordinator.InstanceSequencer() == nil {
			return fmt.Errorf("instance sequencer not configured")
		}

		specMsg, err := convertMailboxMessage(&mailboxMsg)
		if err != nil {
			return fmt.Errorf("failed to convert mailbox message: %w", err)
		}

		return b.coordinator.InstanceSequencer().ProcessMailboxMessage(*instanceID, &specMsg)
	}
}

func (b *EthAPIBackend) DecisionCallbackFn() spconsensus.DecisionFn {
	return func(ctx context.Context, instanceID *compose.InstanceID, decision bool) error {
		if instanceID == nil {
			return fmt.Errorf("decision callback received nil instance ID")
		}
		if b.coordinator == nil || b.coordinator.InstanceSequencer() == nil {
			return fmt.Errorf("instance sequencer not configured")
		}

		if err := b.coordinator.PeriodSequencer().OnDecidedInstance(*instanceID); err != nil {
			return err
		}

		return b.coordinator.InstanceSequencer().Decide(*instanceID, decision)
	}
}

// VoteCallbackFn returns a function that can be used to send votes for cross-chain transactions.
// SSV
func (b *EthAPIBackend) VoteCallbackFn(chainID *big.Int) spconsensus.VoteFn {
	return func(ctx context.Context, instanceID *compose.InstanceID, vote bool) error {
		msgVote := &composeproto.Message_Vote{
			Vote: &composeproto.Vote{
				Vote:       vote,
				InstanceId: instanceID[:],
				ChainId:    chainID.Uint64(),
			},
		}

		spMsg := &composeproto.Message{
			SenderId: chainID.String(),
			Payload:  msgVote,
		}
		return b.spClient.Send(ctx, spMsg)
	}
}

func convertMailboxMessage(msg *composeproto.MailboxMessage) (instanceproto.MailboxMessage, error) {
	var specMsg instanceproto.MailboxMessage

	source, err := bytesToEthAddress(msg.Source)
	if err != nil {
		return specMsg, fmt.Errorf("invalid mailbox sender: %w", err)
	}

	receiver, err := bytesToEthAddress(msg.Receiver)
	if err != nil {
		return specMsg, fmt.Errorf("invalid mailbox receiver: %w", err)
	}

	header := instanceproto.MailboxMessageHeader{
		SourceChainID: compose.ChainID(msg.SourceChain),
		DestChainID:   compose.ChainID(msg.DestinationChain),
		Sender:        source,
		Receiver:      receiver,
		SessionID:     compose.SessionID(msg.SessionId),
		Label:         msg.Label,
	}

	specMsg.MailboxMessageHeader = header
	specMsg.Data = mergeMailboxDataChunks(msg.Data)

	return specMsg, nil
}

func bytesToEthAddress(data []byte) (compose.EthAddress, error) {
	var addr compose.EthAddress
	if len(data) != len(addr) {
		return addr, fmt.Errorf("expected %d-byte address, got %d bytes", len(addr), len(data))
	}
	copy(addr[:], data)
	return addr, nil
}

func mergeMailboxDataChunks(chunks [][]byte) []byte {
	if len(chunks) == 0 {
		return nil
	}
	if len(chunks) == 1 {
		return append([]byte(nil), chunks[0]...)
	}

	total := 0
	for _, c := range chunks {
		total += len(c)
	}

	merged := make([]byte, 0, total)
	for _, c := range chunks {
		merged = append(merged, c...)
	}
	return merged
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

	blockContext := core.NewEVMBlockContext(header, b.eth.blockchain, nil, b.ChainConfig(), stateDB)

	// Pre-apply staging transactions without tracing
	if ctx.Value("simulation") != nil {
		// Create a clean VM config without tracer for staging transactions
		stagingVMConfig := vm.Config{}
		if b.eth.blockchain.GetVMConfig() != nil {
			stagingVMConfig = *b.eth.blockchain.GetVMConfig()
		}
		stagingVMConfig.Tracer = nil
		stagingVMConfig.EnablePreimageRecording = true

		stagingEVM := vm.NewEVM(blockContext, stateDB, b.ChainConfig(), stagingVMConfig)

		// Pre-apply putInbox transactions without tracing
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
			if _, err := core.ApplyMessage(stagingEVM, stageMsg, stageGasPool); err != nil {
				log.Warn("[SSV] Failed to pre-apply putInbox transaction", "txHash", staged.Hash(), "err", err)
				continue
			}
			log.Info("[SSV] Pre-applied putInbox for simulation",
				"stagedHash", staged.Hash().Hex(),
			)
			stateDB.Finalise(true)
		}

		// Pre-apply pending original transactions to ensure correct nonce sequencing.
		// Simulations must observe cumulative state from all pending transactions to prevent
		// nonce validation errors when transactions with dependent nonces are submitted rapidly.
		for _, staged := range b.GetPendingOriginalTxs() {
			if staged == nil || staged.Hash() == tx.Hash() {
				continue
			}

			stageMsg, err := core.TransactionToMessage(staged, signer, header.BaseFee)
			if err != nil {
				log.Warn("[SSV] Failed to build staged original message", "txHash", staged.Hash(), "err", err)
				continue
			}

			stageGasPool := new(core.GasPool).AddGas(header.GasLimit)
			stateDB.SetTxContext(staged.Hash(), stateDB.TxIndex()+1)
			if wants := staged.Nonce(); stateDB.GetNonce(stageMsg.From) != wants {
				stateDB.SetNonce(stageMsg.From, wants, tracing.NonceChangeUnspecified)
			}
			if _, err := core.ApplyMessage(stagingEVM, stageMsg, stageGasPool); err != nil {
				log.Warn("[SSV] Failed to pre-apply original transaction", "txHash", staged.Hash(), "err", err)
				continue
			}
			log.Info("[SSV] Pre-applied original tx for simulation",
				"stagedHash", staged.Hash().Hex(),
				"nonce", staged.Nonce(),
			)
			stateDB.Finalise(true)
		}
	}

	// Create fresh tracer and EVM for the actual transaction being simulated.
	// This ensures only the target transaction's mailbox operations are captured,
	// not any operations from pre-applied staging transactions.
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
		log.Error("[SSV] PeriodSequencer transaction validation failed", "err", err, "txHash", tx.Hash().Hex())
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

	log.Info("[SSV] Transaction clearing request",
		"pending", len(b.GetPendingPutInboxTxs())+len(b.GetPendingOriginalTxs()))

	//switch currentState {
	//case sequencer.StateBuildingFree, sequencer.StateBuildingLocked:
	// Preserve transactions during these states:
	// - BuildingLocked: SCP coordination in progress
	// - BuildingFree: Transactions ready, waiting for block inclusion
	// Actual clearing happens in OnBlockBuildingComplete after commitment
	//log.Info("[SSV] Preserving transactions during coordination")
	//return
	//default:
	//	log.Info("[SSV] Clearing transactions")
	b.clearAllSequencerTransactions()
	//}
}

// clearAllSequencerTransactions performs the actual clearing of transactions
// SSV
func (b *EthAPIBackend) clearAllSequencerTransactions() {
	b.sequencerTxMutex.Lock()

	putInboxCount := 0
	originalCount := 0
	txHashesToReject := make([]common.Hash, 0, len(b.pendingXTEntries))

	for _, entry := range b.pendingXTEntries {
		if entry.kind == sequencerTxPutInbox {
			putInboxCount++
		} else {
			originalCount++
		}
		txHashesToReject = append(txHashesToReject, entry.tx.Hash())
	}

	b.pendingXTEntries = nil
	b.pendingByHash = make(map[common.Hash]int)
	b.sequencerTxMutex.Unlock()

	// Remove from Ethereum txpool to prevent inclusion in future blocks.
	// Critical for transaction rejection scenarios: supervisor failsafe mode, conditional validation
	// failures, or bundle simulation errors during block construction.
	for _, hash := range txHashesToReject {
		if tx := b.eth.txPool.Get(hash); tx != nil {
			tx.SetRejected()
			log.Debug("[SSV] Marked cleared tx as rejected in txpool",
				"txHash", hash.Hex())
		}
	}

	log.Info("[SSV] Cleared sequencer transactions",
		"putInbox", putInboxCount,
		"original", originalCount,
		"rejectedInPool", len(txHashesToReject))

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

	// During active SCP coordination, notify coordinator
	//if currentState == sequencer.StateBuildingLocked {
	//	if err := b.coordinator.PrepareTransactionsForBlock(ctx, currentSlot); err != nil {
	//		log.Warn("[SSV] Coordinator failed to prepare transactions", "err", err)
	//	}
	//}

	return nil
}

// GetOrderedTransactionsForBlock returns only sequencer-managed transactions in
// the correct order for block inclusion. Normal mempool transactions are
// included by the miner after this list, and must not be returned here.
// SSV
func (b *EthAPIBackend) GetOrderedTransactionsForBlock(ctx context.Context) (types.Transactions, error) {
	return b.buildSequencerOnlyList(), nil
}

// buildSequencerOnlyList assembles only the sequencer-managed transactions preserving
// their insertion order. The insertion order is guaranteed to be correct because:
// 1. For each XT, putInbox transactions are created before original transactions
// 2. XTs are processed sequentially, maintaining their relative order
func (b *EthAPIBackend) buildSequencerOnlyList() types.Transactions {
	b.sequencerTxMutex.RLock()
	defer b.sequencerTxMutex.RUnlock()

	orderedTxs := make(types.Transactions, 0, len(b.pendingXTEntries))
	for _, entry := range b.pendingXTEntries {
		orderedTxs = append(orderedTxs, entry.tx)
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
		log.Warn("[SSV] PeriodSequencer transaction not targeting mailbox",
			"to", tx.To().Hex(),
			"expected", mailboxAddrs)
	}

	// Validate gas limits
	if tx.Gas() < 21000 { // TODO: update this
		return fmt.Errorf("sequencer transaction gas too low: %d", tx.Gas())
	}

	if tx.Gas() > 1000000 { // TODO: update this
		log.Warn("[SSV] PeriodSequencer transaction has high gas limit", "gas", tx.Gas())
	}

	log.Debug("[SSV] PeriodSequencer transaction validated",
		"txHash", tx.Hash().Hex(),
		"to", tx.To().Hex(),
		"gas", tx.Gas(),
		"gasPrice", tx.GasPrice())

	return nil
}

// OnBlockBuildingStart is called when block building starts
// SSV
func (b *EthAPIBackend) OnBlockBuildingStart(ctx context.Context, blockNumber uint64) error {
	return b.coordinator.OnBlockBuildingStart(ctx, blockNumber)
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
	return b.coordinator.OnBlockBuildingComplete(ctx, block, success)
}

// CanIncludeLocalTx returns whether local transactions can be included in the block
// SSV
func (b *EthAPIBackend) CanIncludeLocalTx() (bool, error) {
	if b.coordinator == nil || b.coordinator.PeriodSequencer() == nil {
		return false, fmt.Errorf("instance sequencer not configured")
	}
	return b.coordinator.PeriodSequencer().CanIncludeLocalTx()
}

func (b *EthAPIBackend) GetPendingOriginalTxs() []*types.Transaction {
	return b.listTransactionsByKind(sequencerTxOriginal)
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
		ticker := time.NewTicker(10 * time.Millisecond)

		func() {
			defer ticker.Stop()
			for {
				select {
				case <-timeout:
					log.Error("timed out waiting for putInbox transaction appearance in pool")
					return
				case <-ticker.C:
					if poolTx := b.GetPoolTransaction(tx.Hash()); poolTx != nil {
						log.Info("[SSV] found putInbox transaction in pool", "hash", tx.Hash().Hex())
						return
					}
				}
			}
		}()
	}

	return nil
}

func (b *EthAPIBackend) poolPayloadTx(
	ctx context.Context,
	tx *types.Transaction) {
	b.sequencerTxMutex.Lock()
	b.addSequencerEntryLocked(tx, sequencerTxOriginal)
	b.sequencerTxMutex.Unlock()

	// Add to Ethereum txpool for native nonce management.
	// Transactions are filtered from block building via skip logic until SCP coordination completes,
	// ensuring proper nonce sequencing while preventing premature inclusion.
	if err := b.sendTx(ctx, tx); err != nil {
		log.Warn("[SSV] Failed to add original tx to txpool for nonce management",
			"txHash", tx.Hash().Hex(), "err", err)
	} else {
		log.Info("[SSV] Pooled original tx to txpool", "txHash", tx.Hash().Hex(), "nonce", tx.Nonce())
	}

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
	key := hexutil.Encode(xtID.Hash)
	txHash := tx.Hash()

	b.sequencerTxMutex.Lock()
	defer b.sequencerTxMutex.Unlock()

	if b.pendingByHash == nil {
		log.Warn("[SSV] assignXtKeyToHash: pendingByHash is nil", "txHash", txHash.Hex(), "xtID", key)
		return
	}

	idx, ok := b.pendingByHash[txHash]
	if !ok {
		log.Warn("[SSV] assignXtKeyToHash: transaction not found in pendingByHash",
			"txHash", txHash.Hex(),
			"xtID", key,
			"pendingCount", len(b.pendingXTEntries))
		return
	}

	// Bounds check to prevent panic
	if idx < 0 || idx >= len(b.pendingXTEntries) {
		log.Error("[SSV] assignXtKeyToHash: index out of bounds",
			"txHash", txHash.Hex(),
			"xtID", key,
			"idx", idx,
			"pendingCount", len(b.pendingXTEntries))
		return
	}

	b.pendingXTEntries[idx].xtID = key
	log.Info("[SSV] Assigned xtID to transaction",
		"txHash", txHash.Hex(),
		"xtID", key,
		"idx", idx)
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

	result := make([]*types.Transaction, 0, len(b.pendingXTEntries)/2)
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

	// First, collect transaction hashes that will be removed
	b.sequencerTxMutex.Lock()
	txHashesToRemove := make([]common.Hash, 0)
	for _, entry := range b.pendingXTEntries {
		if entry.xtID == key {
			txHashesToRemove = append(txHashesToRemove, entry.tx.Hash())
		}
	}

	removedPutInbox, removedOriginal := b.removeEntriesMatchingLocked(func(entry sequencerTxEntry) bool {
		return entry.xtID == key
	})
	b.sequencerTxMutex.Unlock()

	// Remove from Ethereum txpool to prevent nonce gaps and maintain sequence integrity.
	// Marking as rejected triggers automatic removal of dependent transactions with higher nonces,
	// preventing invalid transaction chains from persisting in the pool.
	for _, hash := range txHashesToRemove {
		if tx := b.eth.txPool.Get(hash); tx != nil {
			// Mark as rejected so txpool removes it and any dependent transactions
			tx.SetRejected()
			log.Info("[SSV] Marked aborted tx as rejected in txpool",
				"txHash", hash.Hex(),
				"xtID", key,
				"nonce", tx.Nonce())
		}
	}

	if removedPutInbox+removedOriginal > 0 {
		if miner := b.eth.miner; miner != nil {
			miner.InvalidatePendingCache()
		}
	}

	return removedPutInbox, removedOriginal
}

// SetSequencerCoordinator wires an SBCP sequencer coordinator, consensus callbacks, and SP client routing.
// SSV
func (b *EthAPIBackend) SetSequencerCoordinator(coord xsequencer.Coordinator, sp xtransport.Client) {
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
			client.SetHandler(func(ctx context.Context, msg *composeproto.Message) ([]common.Hash, error) {
				return b.handleSequencerMessage(ctx, chainID, msg)
			})

			log.Info("[SSV] PeriodSequencer client handler set", "peerChainID", chainID)
		}
	}

	if b.coordinator != nil {
		// Wire consensus callbacks for SCP → coordinator integration
		if b.coordinator.ConsensusCoord() != nil {
			chainID := b.ChainConfig().ChainID
			b.coordinator.ConsensusCoord().SetStartCallback(b.StartCallbackFn())
			b.coordinator.ConsensusCoord().SetMailboxMsgCallback(b.MailboxMsgCallbackFn())
			b.coordinator.ConsensusCoord().SetDecisionCallback(b.DecisionCallbackFn())
			b.coordinator.ConsensusCoord().SetVoteCallback(b.VoteCallbackFn(chainID))
		}

		// Register SBCP callbacks
		b.coordinator.SetCallbacks(xsequencer.CoordinatorCallbacks{
			// For SBCP mode simulation during StartSC
			SimulateAndVote: b.simulateXTRequestForSBCP,
			// For immediate cleanup of aborted transactions
			CleanupAbortedTransaction: b.cleanupAbortedTransactionCallback,
		})

		// Set miner notifier and start
		//b.coordinator.SetMinerNotifier(b)
	}
}

// simulateSCPBundle performs the mailbox-aware EVM simulation required by the SCP instance
// sequencer. The simulation proceeds in three phases:
//  1. Apply all provided mailbox messages by invoking the local mailbox contract's putInbox
//     entrypoint. This reproduces the contract state that would exist if the messages had
//     been relayed on-chain prior to executing the bundle.
//  2. Execute the supplied transactions sequentially while tracing mailbox read/write calls.
//     Execution stops early if a mailbox read targets this chain without an available message,
//     returning the header that must be fulfilled before the bundle can succeed.
//  3. Collect any outbound mailbox writes emitted during successful execution. The simulation
//     operates on a copy of the state so it never mutates the underlying chain data.
func (b *EthAPIBackend) simulateSCPBundle(request instanceproto.SimulationRequest) (*instanceproto.MailboxMessageHeader, []instanceproto.MailboxMessage, error) {
	if len(request.Transactions) == 0 {
		return nil, nil, errors.New("no transactions to simulate")
	}

	ctx := context.Background()

	// Gets state DB and header of current pending block
	stateDBBase, header, err := b.StateAndHeaderByNumberOrHash(ctx, rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber))
	if err != nil {
		return nil, nil, fmt.Errorf("state lookup failed: %w", err)
	}

	// Finalize any prior state changes (removing dirty state)
	stateDBBase.Finalise(true)

	// The simulation request is made with a requirement on the state root it should be executed on top.
	// If there is a mismatch, abort early.
	if request.Snapshot != (compose.StateRoot{}) {
		expected := composeRootToHash(request.Snapshot)
		current := stateDBBase.IntermediateRoot(false)
		if current != expected {
			return nil, nil, fmt.Errorf("state root mismatch: pending=%s expected=%s", current.Hex(), expected.Hex())
		}
	}

	// Work on a copy so simulation does not disturb the original state
	stateDB := stateDBBase.Copy()

	// Retrieve mailbox addresses
	mailboxAddresses := b.GetMailboxAddresses()
	if len(mailboxAddresses) == 0 {
		return nil, nil, errors.New("mailbox addresses not configured")
	}

	// Produce the block and VM contexts
	vmBaseConfig := vm.Config{}
	if cfg := b.eth.blockchain.GetVMConfig(); cfg != nil {
		vmBaseConfig = *cfg
	}
	vmBaseConfig.EnablePreimageRecording = true
	blockContext := core.NewEVMBlockContext(header, b.chainContext(), nil, b.ChainConfig(), stateDB)

	// Run putInbox transactions
	fulfilled := make(map[string]struct{}, len(request.PutInboxMessages))
	for _, msg := range request.PutInboxMessages {
		if err := b.applyPutInboxMessage(blockContext, vmBaseConfig, stateDB, msg); err != nil {
			return nil, nil, fmt.Errorf("apply putInbox message: %w", err)
		}
		fulfilled[mailboxHeaderKey(msg.MailboxMessageHeader)] = struct{}{}
	}

	localChainID := b.ChainConfig().ChainID.Uint64()
	processor := &MailboxProcessor{
		chainID:          localChainID,
		mailboxAddresses: mailboxAddresses,
	}

	signer := types.MakeSigner(b.ChainConfig(), header.Number, header.Time)
	writeAccumulator := make([]instanceproto.MailboxMessage, 0)

	for idx, payload := range request.Transactions {
		if len(payload) == 0 {
			continue
		}

		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(payload); err != nil {
			return nil, nil, fmt.Errorf("decode transaction %d: %w", idx, err)
		}

		txSnapshot := stateDB.Snapshot()

		tracer := native.NewSSVTracer(mailboxAddresses)
		vmConfig := vmBaseConfig
		vmConfig.Tracer = tracer.Hooks()

		evm := vm.NewEVM(blockContext, stateDB, b.ChainConfig(), vmConfig)

		msg, err := core.TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			stateDB.RevertToSnapshot(txSnapshot)
			return nil, nil, fmt.Errorf("build message for tx %d: %w", idx, err)
		}

		stateDB.SetTxContext(tx.Hash(), stateDB.TxIndex()+1)
		gasPool := new(core.GasPool)
		gasPool.AddGas(header.GasLimit)

		execResult, execErr := core.ApplyMessage(evm, msg, gasPool)
		traceResult := tracer.GetTraceResult()
		if execResult != nil {
			traceResult.ExecutionResult = execResult
		} else {
			traceResult.ExecutionResult = &core.ExecutionResult{Err: execErr}
		}

		missing, writes, analyzeErr := b.analyzeMailboxTrace(processor, traceResult, fulfilled)
		if analyzeErr != nil {
			stateDB.RevertToSnapshot(txSnapshot)
			return nil, nil, fmt.Errorf("analyze tx %d: %w", idx, analyzeErr)
		}

		if missing != nil {
			stateDB.RevertToSnapshot(txSnapshot)
			accumulated := append(append([]instanceproto.MailboxMessage(nil), writeAccumulator...), writes...)
			return missing, accumulated, nil
		}

		if execErr != nil {
			stateDB.RevertToSnapshot(txSnapshot)
			return nil, nil, fmt.Errorf("transaction %d failed: %w", idx, execErr)
		}
		if execResult != nil && execResult.Failed() {
			stateDB.RevertToSnapshot(txSnapshot)
			if err := execResult.Unwrap(); err != nil {
				return nil, nil, fmt.Errorf("transaction %d reverted: %w", idx, err)
			}
			return nil, nil, fmt.Errorf("transaction %d reverted", idx)
		}

		stateDB.Finalise(true)
		writeAccumulator = append(writeAccumulator, writes...)
	}

	return nil, writeAccumulator, nil
}

func (b *EthAPIBackend) applyPutInboxMessage(blockCtx vm.BlockContext, baseVMConfig vm.Config, stateDB *state.StateDB, msg instanceproto.MailboxMessage) error {
	// Get mailbox address from local chain
	mailboxAddr := b.GetMailboxAddressFromChainID(b.ChainConfig().ChainID.Uint64())
	if (mailboxAddr == common.Address{}) {
		return errors.New("mailbox address not configured for local chain")
	}

	// Produce call data for putInbox invocation
	callData, err := buildPutInboxCalldata(msg)
	if err != nil {
		return fmt.Errorf("encode putInbox call: %w", err)
	}

	// Prepare EVM to execute putInbox
	vmConfig := baseVMConfig
	vmConfig.Tracer = nil
	evm := vm.NewEVM(blockCtx, stateDB, b.ChainConfig(), vmConfig)

	gasPool := new(core.GasPool)
	gasPool.AddGas(blockCtx.GasLimit)

	if (b.coordinatorAddr == common.Address{}) {
		return errors.New("coordinator address not configured")
	}
	if b.coordinatorKey == nil {
		return errors.New("coordinator key not configured")
	}

	nonce := stateDB.GetNonce(b.coordinatorAddr)

	txData := &types.DynamicFeeTx{
		ChainID:   b.ChainConfig().ChainID,
		Nonce:     nonce,
		GasTipCap: big.NewInt(1_000_000_000),
		GasFeeCap: big.NewInt(20_000_000_000),
		Gas:       defaultPutInboxGas,
		To:        &mailboxAddr,
		Value:     big.NewInt(0),
		Data:      callData,
	}

	tx := types.NewTx(txData)
	signedTx, err := types.SignTx(tx, types.NewLondonSigner(b.ChainConfig().ChainID), b.coordinatorKey)
	if err != nil {
		return fmt.Errorf("sign putInbox tx: %w", err)
	}

	signer := types.MakeSigner(b.ChainConfig(), blockCtx.BlockNumber, blockCtx.Time)
	message, err := core.TransactionToMessage(signedTx, signer, blockCtx.BaseFee)
	if err != nil {
		return fmt.Errorf("convert putInbox tx to message: %w", err)
	}

	stateDB.SetTxContext(signedTx.Hash(), stateDB.TxIndex()+1)

	result, err := core.ApplyMessage(evm, message, gasPool)
	if err != nil {
		return fmt.Errorf("putInbox execution failed: %w", err)
	}
	if result != nil && result.Failed() {
		if reason := result.Revert(); len(reason) > 0 {
			return fmt.Errorf("putInbox execution reverted: %x", reason)
		}
		return fmt.Errorf("putInbox execution reverted: %w", result.Err)
	}

	stateDB.Finalise(true)
	return nil
}

func (b *EthAPIBackend) analyzeMailboxTrace(mp *MailboxProcessor, trace *ssv.SSVTraceResult, fulfilled map[string]struct{}) (*instanceproto.MailboxMessageHeader, []instanceproto.MailboxMessage, error) {
	writes := make([]instanceproto.MailboxMessage, 0)
	var missing *instanceproto.MailboxMessageHeader

	for _, op := range trace.Operations {
		if !mp.isMailboxAddress(op.Address) {
			continue
		}
		if op.Type != vm.CALL && op.Type != vm.STATICCALL {
			continue
		}
		if len(op.CallData) < 4 {
			continue
		}

		call, err := mp.parseMailboxCall(op.CallData)
		if err != nil {
			log.Debug("[SSV] Unable to parse mailbox call during simulation", "err", err)
			continue
		}

		if call.IsRead && awaitRead(call, mp.chainID) {
			header, err := buildReadHeader(call, op.From)
			if err != nil {
				return nil, nil, err
			}
			key := mailboxHeaderKey(*header)
			if _, ok := fulfilled[key]; ok {
				delete(fulfilled, key)
				continue
			}
			if missing == nil {
				temp := *header
				missing = &temp
			}
			continue
		}

		if call.IsWrite && mustWrite(call, mp.chainID) {
			message, err := buildWriteMessage(call, op.From)
			if err != nil {
				return nil, nil, err
			}
			writes = append(writes, message)
		}
	}

	if missing != nil {
		return missing, writes, nil
	}

	return nil, writes, nil
}

func buildPutInboxCalldata(msg instanceproto.MailboxMessage) ([]byte, error) {
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		return nil, err
	}

	data, err := parsedABI.Pack(
		"putInbox",
		new(big.Int).SetUint64(uint64(msg.SourceChainID)),
		commonAddressFromCompose(msg.Sender),
		commonAddressFromCompose(msg.Receiver),
		new(big.Int).SetUint64(uint64(msg.SessionID)),
		[]byte(msg.Label),
		msg.Data,
	)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func buildReadHeader(call *MailboxCall, receiver common.Address) (*instanceproto.MailboxMessageHeader, error) {
	if call.ChainSrc == nil || !call.ChainSrc.IsUint64() {
		return nil, fmt.Errorf("invalid source chain id in read call")
	}
	if call.ChainDest == nil || !call.ChainDest.IsUint64() {
		return nil, fmt.Errorf("invalid destination chain id in read call")
	}
	if call.SessionId == nil || !call.SessionId.IsUint64() {
		return nil, fmt.Errorf("invalid session id in read call")
	}

	return &instanceproto.MailboxMessageHeader{
		SourceChainID: compose.ChainID(call.ChainSrc.Uint64()),
		DestChainID:   compose.ChainID(call.ChainDest.Uint64()),
		Sender:        composeAddressFromCommon(call.Sender),
		Receiver:      composeAddressFromCommon(receiver),
		SessionID:     compose.SessionID(call.SessionId.Uint64()),
		Label:         string(call.Label),
	}, nil
}

func buildWriteMessage(call *MailboxCall, sender common.Address) (instanceproto.MailboxMessage, error) {
	if call.ChainSrc == nil || !call.ChainSrc.IsUint64() {
		return instanceproto.MailboxMessage{}, fmt.Errorf("invalid source chain id in write call")
	}
	if call.ChainDest == nil || !call.ChainDest.IsUint64() {
		return instanceproto.MailboxMessage{}, fmt.Errorf("invalid destination chain id in write call")
	}
	if call.SessionId == nil || !call.SessionId.IsUint64() {
		return instanceproto.MailboxMessage{}, fmt.Errorf("invalid session id in write call")
	}

	header := instanceproto.MailboxMessageHeader{
		SourceChainID: compose.ChainID(call.ChainSrc.Uint64()),
		DestChainID:   compose.ChainID(call.ChainDest.Uint64()),
		Sender:        composeAddressFromCommon(sender),
		Receiver:      composeAddressFromCommon(call.Receiver),
		SessionID:     compose.SessionID(call.SessionId.Uint64()),
		Label:         string(call.Label),
	}

	data := append([]byte(nil), call.Data...)

	return instanceproto.MailboxMessage{
		MailboxMessageHeader: header,
		Data:                 data,
	}, nil
}

func mailboxHeaderKey(header instanceproto.MailboxMessageHeader) string {
	return fmt.Sprintf("%d|%d|%x|%x|%d|%s",
		header.SourceChainID,
		header.DestChainID,
		header.Sender[:],
		header.Receiver[:],
		header.SessionID,
		header.Label,
	)
}

func composeRootToHash(root compose.StateRoot) common.Hash {
	var h common.Hash
	copy(h[:], root[:])
	return h
}

func composeAddressFromCommon(addr common.Address) compose.EthAddress {
	var out compose.EthAddress
	copy(out[:], addr.Bytes())
	return out
}

func (b *EthAPIBackend) chainContext() core.ChainContext {
	if b.chainContextOverride != nil {
		return b.chainContextOverride
	}
	if b.eth != nil && b.eth.blockchain != nil {
		return b.eth.blockchain
	}
	return nil
}

func commonAddressFromCompose(addr compose.EthAddress) common.Address {
	return common.BytesToAddress(addr[:])
}

// NotifyStateChange notifies the miner of sequencer state changes
// SSV
//func (b *EthAPIBackend) NotifyStateChange(from, to sequencer.State, slot uint64) error {
//	log.Debug("[SSV] SBCP state change", "from", from.String(), "to", to.String(), "slot", slot)
//
//	// When SCP completes (Building-Locked → Building-Free), force miner to rebuild payload
//	// with newly added SCP transactions. Without this, the payload remains stale and
//	// RequestSeal seals a block without the SCP transactions.
//	if from == sequencer.StateBuildingLocked && to == sequencer.StateBuildingFree {
//		if miner := b.eth.miner; miner != nil {
//			log.Info("[SSV] Forcing payload rebuild after SCP completion", "slot", slot)
//			miner.InvalidatePendingCache()
//		}
//	}
//
//	return nil
//}

// cleanupAbortedTransactionCallback removes aborted cross-chain transactions from the pending pool
// immediately upon consensus decision. This ensures transactions rejected by the consensus layer
// cannot be included in blocks, maintaining atomic transaction semantics where transactions are
// either fully committed or fully excluded across all participating chains.
//
// This callback is invoked by the sequencer coordinator and complements the RequestSeal cleanup,
// providing early removal to handle the case where blocks are built before the seal message arrives.
// SSV
func (b *EthAPIBackend) cleanupAbortedTransactionCallback(ctx context.Context, xtID *rollupv1.XtID) error {
	if xtID == nil {
		return nil
	}
	key := hexutil.Encode(xtID.Hash)
	removedPut, removedOriginal := b.dropTransactionsForXtKey(key)
	if removedPut+removedOriginal > 0 {
		log.Info("[SSV] Dropped aborted transactions via callback",
			"xtID", key,
			"putInboxRemoved", removedPut,
			"originalRemoved", removedOriginal)
	}
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

		log.Info("[SSV] Skip legacy: mailboxProcessor.handleCrossRollupCoordination")
		//sentMsgs, fulfilledDeps, err := mailboxProcessor.handleCrossRollupCoordination(ctx, simState, xtID)
		//if err != nil {
		//	return false, fmt.Errorf("failed to handle cross-rollup coordination: %w", err)
		//}

		//log.Info(
		//	"[SSV] Cross-rollup coordination completed",
		//	"xtID",
		//	xtID.Hex(),
		//	"sent",
		//	len(sentMsgs),
		//	"received",
		//	len(fulfilledDeps),
		//)
		//
		//allSentMsgs = append(allSentMsgs, sentMsgs...)
		//allFulfilledDeps = append(allFulfilledDeps, fulfilledDeps...)
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

				log.Info("[SSV] Skip Legacy call: mailboxProcessor.sendCIRCMessage")
				//if err := mailboxProcessor.sendCIRCMessage(ctx, &outMsg, xtID); err != nil {
				//	log.Error("[SSV] Failed to send ACK CIRC message", "error", err, "xtID", xtID.Hex())
				//	continue
				//}
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
				b.poolPayloadTx(ctx, simState.Tx)
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
			b.poolPayloadTx(ctx, tx)
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
