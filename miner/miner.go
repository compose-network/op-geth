// Copyright 2014 The go-ethereum Authors
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

// Package miner implements Ethereum block creation and mining.
package miner

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/types/interoptypes"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
)

var (
	maxDATxSizeGauge    = metrics.NewRegisteredGauge("miner/maxDATxSize", nil)
	maxDABlockSizeGauge = metrics.NewRegisteredGauge("miner/maxDABlockSize", nil)
)

// Backend wraps all methods required for mining. Only full node is capable
// to offer all the functions here.
type Backend interface {
	BlockChain() *core.BlockChain
	TxPool() *txpool.TxPool
}
type BackendWithHistoricalState interface {
	StateAtBlock(ctx context.Context, block *types.Block, reexec uint64, base *state.StateDB, readOnly bool, preferDisk bool) (*state.StateDB, tracers.StateReleaseFunc, error)
}

type BackendWithInterop interface {
	CheckAccessList(ctx context.Context, inboxEntries []common.Hash, minSafety interoptypes.SafetyLevel, executingDescriptor interoptypes.ExecutingDescriptor) error

	// GetFailsafeEnabled reads the local failsafe status from the backend
	GetSupervisorFailsafe() bool

	// QueryFailsafe queries the supervisor over RPC for the failsafe status,
	// caches it in the backend, and returns the status.
	QueryFailsafe(ctx context.Context) (bool, error)
}

// SimulatedBundleStats describes a simulated bundle ready for block inclusion.
// SSV
type SimulatedBundleStats struct {
	BaseTxCount int
	BaseGasUsed uint64
	GasUsed     uint64
	Size        uint64
	TxCount     int
	Blobs       int
	BlobGasUsed uint64
}

// BackendWithSequencerTransactions defines the interface for sequencer transaction handling
// SSV
type BackendWithSequencerTransactions interface {
	// PrepareSequencerTransactionsForBlock prepares sequencer transactions for inclusion in a new block
	// SSV
	PrepareSequencerTransactionsForBlock(ctx context.Context) error

	// GetOrderedTransactionsForBlock returns sequencer-managed transactions in the
	// correct order for block inclusion. Normal mempool transactions are handled
	// by the miner and must NOT be included here.
	// SSV
	GetOrderedTransactionsForBlock(ctx context.Context) (types.Transactions, error)

	// GetPendingPutInboxTxs returns all pending put inbox transactions
	// SSV
	GetPendingPutInboxTxs() []*types.Transaction

	// GetPendingOriginalTxs returns all pending original transactions
	// SSV
	GetPendingOriginalTxs() []*types.Transaction

	// OnBlockBuildingStart is called when block building starts
	// SSV
	OnBlockBuildingStart(ctx context.Context, u uint64) error

	// OnBlockBuildingComplete is called when block building completes
	// SSV
	OnBlockBuildingComplete(ctx context.Context, block *types.Block, success bool, simulation bool) error
}

// Config is the configuration parameters of mining.
type Config struct {
	Etherbase           common.Address `toml:"-"`          // Deprecated
	PendingFeeRecipient common.Address `toml:"-"`          // Address for pending block rewards.
	ExtraData           hexutil.Bytes  `toml:",omitempty"` // Block extra data set by the miner
	GasCeil             uint64         // Target gas ceiling for mined blocks.
	GasPrice            *big.Int       // Minimum gas price for mining a transaction
	Recommit            time.Duration  // The time interval for miner to re-create mining work.

	RollupComputePendingBlock             bool // Compute the pending block from tx-pool, instead of copying the latest-block
	RollupTransactionConditionalRateLimit int  // Total number of conditional cost units allowed in a second

	EffectiveGasCeil uint64   // if non-zero, a gas ceiling to apply independent of the header's gaslimit value
	MaxDATxSize      *big.Int `toml:",omitempty"` // if non-nil, don't include any txs with data availability size larger than this in any built block
	MaxDABlockSize   *big.Int `toml:",omitempty"` // if non-nil, then don't build a block requiring more than this amount of total data availability
}

// DefaultConfig contains default settings for miner.
var DefaultConfig = Config{
	GasCeil:  45_000_000,
	GasPrice: big.NewInt(params.Wei),

	// The default recommit time is chosen as two seconds since
	// consensus-layer usually will wait a half slot of time(6s)
	// for payload generation. It should be enough for Geth to
	// run 3 rounds.
	Recommit: 2 * time.Second,
}

// Miner is the main object which takes care of submitting new work to consensus
// engine and gathering the sealing result.
type Miner struct {
	confMu      sync.RWMutex // The lock used to protect the config fields: GasCeil, GasTip and Extradata
	config      *Config
	chainConfig *params.ChainConfig
	engine      consensus.Engine
	txpool      *txpool.TxPool
	prio        []common.Address // A list of senders to prioritize
	chain       *core.BlockChain
	pending     *pending
	pendingMu   sync.Mutex // Lock protects the pending block

	backend    Backend
	backendAPI interface{}

	// SSV: expose in-flight state safely for simulation
	simStateMu sync.Mutex
	simEnv     *environment
	simReadyCh chan struct{}

	lifeCtxCancel context.CancelFunc
	lifeCtx       context.Context
}

// SimulationStateGuard holds the current in-flight state under lock and allows
// callers to either keep or restore it before unlocking.
type SimulationStateGuard struct {
	state   *state.StateDB
	header  *types.Header
	txCount int
	unlock  func()
	restore func()
	swap    func(*state.StateDB)
	apply   func(types.Transactions, []*types.Receipt, []*types.BlobTxSidecar, SimulatedBundleStats) error
}

func (g *SimulationStateGuard) State() *state.StateDB {
	if g == nil {
		return nil
	}
	return g.state
}

func (g *SimulationStateGuard) Header() *types.Header {
	if g == nil {
		return nil
	}
	return g.header
}

// TxCount returns the current in-flight transaction count.
// SSV
func (g *SimulationStateGuard) TxCount() int {
	if g == nil {
		return 0
	}
	return g.txCount
}

// UnlockState releases the guard. If success is false, the state is restored.
func (g *SimulationStateGuard) UnlockState(success bool) {
	if g == nil {
		return
	}
	if !success && g.restore != nil {
		g.restore()
	}
	if g.unlock != nil {
		g.unlock()
		g.unlock = nil
	}
}

// SwapState replaces the in-flight state with the provided state.
// SSV
func (g *SimulationStateGuard) SwapState(state *state.StateDB) {
	if g == nil || g.swap == nil || state == nil {
		return
	}
	g.swap(state)
}

// ApplySimulatedBundle appends a simulated bundle to the in-flight block.
// SSV
func (g *SimulationStateGuard) ApplySimulatedBundle(
	txs types.Transactions,
	receipts []*types.Receipt,
	sidecars []*types.BlobTxSidecar,
	stats SimulatedBundleStats,
) error {
	if g == nil || g.apply == nil {
		return errors.New("simulation guard cannot apply bundle")
	}
	return g.apply(txs, receipts, sidecars, stats)
}

// New creates a new miner with provided config.
func New(eth Backend, backendAPI interface{}, config Config, engine consensus.Engine) *Miner {
	ctx, cancel := context.WithCancel(context.Background())
	miner := &Miner{
		backend:     eth,
		backendAPI:  backendAPI,
		config:      &config,
		chainConfig: eth.BlockChain().Config(),
		engine:      engine,
		txpool:      eth.TxPool(),
		chain:       eth.BlockChain(),
		pending:     &pending{},
		// To interrupt background tasks that may be attached to external processes
		lifeCtxCancel: cancel,
		lifeCtx:       ctx,
	}
	miner.simReadyCh = make(chan struct{})

	// OP-Stack: Start background RPC polling
	miner.startBackgroundInteropFailsafeDetection()

	return miner
}

// OP-Stack: startBackgroundInteropFailsafeDetection starts a background goroutine that periodically
// calls the supervisor over RPC to check if the failsafe is enabled
func (miner *Miner) startBackgroundInteropFailsafeDetection() {
	backend, ok := miner.backend.(BackendWithInterop)
	if !ok {
		log.Warn("Miner backend does not implement BackendWithInterop, skipping interop failsafe detection")
		return
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		log.Info("Starting background interop failsafe detection", "interval", "1s")

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(miner.lifeCtx, 1*time.Second)
				defer cancel()
				backend.QueryFailsafe(ctx)
			case <-miner.lifeCtx.Done():
				log.Info("Stopping background RPC polling due to miner shutdown")
				return
			}
		}
	}()
}

// Pending returns the currently pending block and associated receipts, logs
// and statedb. The returned values can be nil in case the pending block is
// not initialized.
func (miner *Miner) Pending(ctx context.Context) (*types.Block, types.Receipts, *state.StateDB) {
	if miner.chainConfig.Optimism != nil && !miner.config.RollupComputePendingBlock {
		// For compatibility when not computing a pending block, we serve the latest block as "pending"
		headHeader := miner.chain.CurrentHeader()
		headBlock := miner.chain.GetBlock(headHeader.Hash(), headHeader.Number.Uint64())
		headReceipts := miner.chain.GetReceiptsByHash(headHeader.Hash())
		stateDB, err := miner.chain.StateAt(headHeader.Root)
		if err != nil {
			return nil, nil, nil
		}

		return headBlock, headReceipts, stateDB.Copy()
	}

	pending := miner.getPending(ctx)
	if pending == nil {
		return nil, nil, nil
	}

	return pending.block, pending.receipts, pending.stateDB.Copy()
}

// SetExtra sets the content used to initialize the block extra field.
func (miner *Miner) SetExtra(extra []byte) error {
	if uint64(len(extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra exceeds max length. %d > %v", len(extra), params.MaximumExtraDataSize)
	}
	miner.confMu.Lock()
	miner.config.ExtraData = extra
	miner.confMu.Unlock()
	return nil
}

// SetPrioAddresses sets a list of addresses to prioritize for transaction inclusion.
func (miner *Miner) SetPrioAddresses(prio []common.Address) {
	miner.confMu.Lock()
	miner.prio = prio
	miner.confMu.Unlock()
}

// SetGasCeil sets the gaslimit to strive for when mining blocks post 1559.
// For pre-1559 blocks, it sets the ceiling.
func (miner *Miner) SetGasCeil(ceil uint64) {
	miner.confMu.Lock()
	miner.config.GasCeil = ceil
	miner.confMu.Unlock()
}

// SetGasTip sets the minimum gas tip for inclusion.
func (miner *Miner) SetGasTip(tip *big.Int) error {
	miner.confMu.Lock()
	miner.config.GasPrice = tip
	miner.confMu.Unlock()
	return nil
}

// SetMaxDASize sets the maximum data availability size currently allowed for inclusion. 0 means no maximum.
func (miner *Miner) SetMaxDASize(maxTxSize, maxBlockSize *big.Int) {
	convertZeroToNil := func(v *big.Int) *big.Int {
		if v != nil && v.BitLen() == 0 {
			return nil
		}
		return v
	}
	convertNilToZero := func(v *big.Int) int64 {
		if v == nil {
			return 0
		}
		return v.Int64()
	}

	miner.confMu.Lock()
	miner.config.MaxDATxSize = convertZeroToNil(maxTxSize)
	miner.config.MaxDABlockSize = convertZeroToNil(maxBlockSize)
	miner.confMu.Unlock()

	maxDATxSizeGauge.Update(convertNilToZero(maxTxSize))
	maxDABlockSizeGauge.Update(convertNilToZero(maxBlockSize))
}

// BuildPayload builds the payload according to the provided parameters.
func (miner *Miner) BuildPayload(args *BuildPayloadArgs, witness bool) (*Payload, error) {
	return miner.buildPayload(args, witness)
}

// getPending retrieves the pending block based on the current head block.
// The result might be nil if pending generation is failed.
func (miner *Miner) getPending(ctx context.Context) *newPayloadResult {
	miner.pendingMu.Lock()
	defer miner.pendingMu.Unlock()
	return miner.getPendingLocked(ctx)
}

func (miner *Miner) getPendingLocked(ctx context.Context) *newPayloadResult {
	header := miner.chain.CurrentHeader()

	// Check simulation mode - for simulations, never use cache and don't update cache
	simulation, _ := ctx.Value("simulation").(bool)

	// For non-simulation requests, use cache if available
	if !simulation {
		if cached := miner.pending.resolve(header.Hash()); cached != nil {
			return cached
		}
	}
	var (
		timestamp  = uint64(time.Now().Unix())
		withdrawal types.Withdrawals
	)
	if miner.chainConfig.IsShanghai(new(big.Int).Add(header.Number, big.NewInt(1)), timestamp) {
		withdrawal = []*types.Withdrawal{}
	}
	var eip1559Params []byte
	if miner.chainConfig.IsHolocene(timestamp) {
		eip1559Params = miner.createHoloceneEIP1559Params(header, timestamp)
	}

	ret := miner.generateWork(&generateParams{
		timestamp:     timestamp,
		forceTime:     false,
		parentHash:    header.Hash(),
		coinbase:      miner.config.PendingFeeRecipient,
		random:        common.Hash{},
		withdrawals:   withdrawal,
		beaconRoot:    nil,
		noTxs:         false,
		eip1559Params: eip1559Params,
		simulation:    simulation,
	}, false) // we will never make a witness for a pending block
	if ret.err != nil {
		return nil
	}

	// Only cache non-simulation results
	if !simulation {
		miner.pending.update(header.Hash(), ret)
	}
	return ret
}

// PendingStateAndHeaderLocked returns the current pending state and header while
// holding the pending lock. The caller must invoke the returned unlock function.
func (miner *Miner) PendingStateAndHeaderLocked(ctx context.Context) (*state.StateDB, *types.Header, func(), error) {
	miner.pendingMu.Lock()
	unlock := func() {
		miner.pendingMu.Unlock()
	}

	pending := miner.getPendingLocked(ctx)
	if pending == nil || pending.stateDB == nil || pending.block == nil {
		unlock()
		return nil, nil, func() {}, errors.New("pending state unavailable")
	}

	return pending.stateDB, pending.block.Header(), unlock, nil
}

// CurrentBuildEnvLocked returns the in-flight block-building environment while holding
// the simulation lock. The caller must invoke the returned unlock function.
func (miner *Miner) CurrentBuildEnvLocked(ctx context.Context) (*environment, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		miner.simStateMu.Lock()
		if miner.simEnv != nil && miner.simEnv.state != nil && miner.simEnv.header != nil {
			return miner.simEnv, func() { miner.simStateMu.Unlock() }, nil
		}
		ch := miner.simReadyCh
		miner.simStateMu.Unlock()

		select {
		case <-ctx.Done():
			return nil, func() {}, ctx.Err()
		case <-ch:
		}
	}
}

// SimulationStateGuard returns a guard for the in-flight state. Call UnlockState(success)
// when done to either keep or restore the state.
func (miner *Miner) SimulationStateGuard(ctx context.Context) (*SimulationStateGuard, error) {
	env, unlock, err := miner.CurrentBuildEnvLocked(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := env.state.Snapshot()
	restore := func() {
		env.state.RevertToSnapshot(snapshot)
		if env.evm != nil {
			env.evm.StateDB = env.state
		}
	}
	return &SimulationStateGuard{
		state:   env.state,
		header:  env.header,
		txCount: env.tcount,
		unlock:  unlock,
		restore: restore,
		swap: func(state *state.StateDB) {
			env.state = state
			if env.evm != nil {
				env.evm.StateDB = state
			}
		},
		apply: func(txs types.Transactions, receipts []*types.Receipt, sidecars []*types.BlobTxSidecar, stats SimulatedBundleStats) error {
			return miner.applySimulatedBundle(env, txs, receipts, sidecars, stats)
		},
	}, nil
}

func (miner *Miner) createHoloceneEIP1559Params(parent *types.Header, timestamp uint64) []byte {
	params := make([]byte, 8)

	// Get the chain config values
	baseFeeChangeDenominator := miner.chainConfig.BaseFeeChangeDenominator(timestamp)
	elasticityMultiplier := miner.chainConfig.ElasticityMultiplier()

	binary.BigEndian.PutUint32(params[0:4], 8)
	// Last 4 bytes: ElasticityMultiplier
	binary.BigEndian.PutUint32(params[4:8], 2)

	log.Debug("Created Holocene EIP-1559 params",
		"baseFeeChangeDenominator", baseFeeChangeDenominator,
		"elasticityMultiplier", elasticityMultiplier,
		"paramsHex", common.Bytes2Hex(params),
	)

	return params
}

func (miner *Miner) Close() {
	miner.lifeCtxCancel()
}

// InvalidatePendingCache invalidates the pending block cache
// This should be called when transaction state changes that affect pending blocks
func (miner *Miner) InvalidatePendingCache() {
	miner.pendingMu.Lock()
	defer miner.pendingMu.Unlock()
	miner.pending = &pending{}
}
