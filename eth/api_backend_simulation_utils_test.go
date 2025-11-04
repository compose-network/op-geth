package eth

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"strings"
	"testing"

	"github.com/compose-network/specs/compose"
	instanceproto "github.com/compose-network/specs/compose/scp"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/holiman/uint256"
)

// Const addresses and ABIs

var (
	// Use legacy one if not updated by test (for testing only auxiliary functions)
	mailboxABIParsed, _ = abi.JSON(strings.NewReader(mailboxABI))

	pingPongABIParsed abi.ABI
)

var (
	mailboxContractAddr  = common.HexToAddress("0x1000000000000000000000000000000000000011")
	pingPongContractAddr = common.HexToAddress("0x2000000000000000000000000000000000000022")
)

// ========================================
// ETH API backend with overrides for testing.
// ========================================

type testAPIBackend struct {
	EthAPIBackend
	chainCfg   *params.ChainConfig
	testState  *state.StateDB
	testHeader *types.Header
	testChain  core.ChainContext
}

func (t *testAPIBackend) ChainConfig() *params.ChainConfig {
	if t.chainCfg != nil {
		return t.chainCfg
	}
	return t.EthAPIBackend.ChainConfig()
}

func (t *testAPIBackend) chainContext() core.ChainContext {
	if t.testChain != nil {
		return t.testChain
	}
	return t.EthAPIBackend.chainContext()
}

type testChainContext struct {
	header *types.Header
	cfg    *params.ChainConfig
	engine consensus.Engine
}

func (c *testChainContext) Engine() consensus.Engine {
	return c.engine
}

func (c *testChainContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	if c.header.Number.Uint64() == number {
		h := c.header.Hash()
		if h == hash || hash == (common.Hash{}) {
			return c.header
		}
	}
	return nil
}

func (c *testChainContext) Config() *params.ChainConfig {
	return c.cfg
}

// Main function for setting up an API backend and a brand-new state DB with the mailbox and PingPong contracts deployed.
func setupSimulationTestBackend(t *testing.T) (*testAPIBackend, func() *state.StateDB, core.ChainContext, *types.Header) {
	t.Helper()

	// Create coordinator account
	coordKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate coordinator key: %v", err)
	}
	coordAddr := crypto.PubkeyToAddress(coordKey.PublicKey)

	// State factory to create fresh state dbs for each test
	stateFactory := func() *state.StateDB {
		// New state db
		stateDB, err := state.New(common.Hash{}, state.NewDatabaseForTesting())
		if err != nil {
			t.Fatalf("failed to create state db: %v", err)
		}

		// Mailbox contract deployment
		bytecode, storage, abiInstance := LoadMailboxRuntimeForCoordinator(t, coordAddr)
		mailboxABIParsed = abiInstance
		stateDB.SetCode(mailboxContractAddr, bytecode)
		for key, value := range storage {
			stateDB.SetState(mailboxContractAddr, key, value)
		}
		// PingPong contract deployment
		pingCode, pingABI := LoadPingPongRuntime(t, mailboxContractAddr)
		pingPongABIParsed = pingABI
		stateDB.SetCode(pingPongContractAddr, pingCode)

		// Fund coordinator
		gasBudget := uint256.NewInt(defaultPutInboxGas)
		gasBudget.Mul(gasBudget, uint256.NewInt(20_000_000_000))
		gasBudget.Mul(gasBudget, uint256.NewInt(10))
		stateDB.AddBalance(coordAddr, gasBudget, tracing.BalanceChangeUnspecified)
		stateDB.Finalise(true)
		return stateDB
	}

	// Chain config with Shanghai activated and header
	chainCfg := *params.AllDevChainProtocolChanges
	chainCfg.ChainID = big.NewInt(99)

	header := &types.Header{
		Number:     big.NewInt(1),
		GasLimit:   30_000_000,
		Time:       1,
		BaseFee:    big.NewInt(1_000_000_000),
		Difficulty: big.NewInt(0),
	}
	if !chainCfg.IsShanghai(header.Number, header.Time) {
		t.Fatalf("test chain config missing Shanghai activation")
	}

	engine := ethash.NewFaker()
	chainCtx := &testChainContext{
		header: header,
		cfg:    &chainCfg,
		engine: engine,
	}

	backend := &testAPIBackend{
		chainCfg: &chainCfg,
	}

	chainDB := rawdb.NewMemoryDatabase()
	blockchainCfg := core.DefaultConfig()
	genesis := &core.Genesis{
		Config:     &chainCfg,
		Difficulty: header.Difficulty,
		GasLimit:   header.GasLimit,
		Timestamp:  header.Time,
		BaseFee:    header.BaseFee,
	}
	blockchain, err := core.NewBlockChain(chainDB, genesis, engine, blockchainCfg)
	if err != nil {
		t.Fatalf("failed to create blockchain: %v", err)
	}
	t.Cleanup(func() {
		blockchain.Stop()
		chainDB.Close()
	})

	backend.eth = &Ethereum{
		blockchain: blockchain,
		engine:     engine,
	}
	backend.eth.APIBackend = &backend.EthAPIBackend

	backend.mailboxAddresses = []common.Address{mailboxContractAddr}
	backend.mailboxByChainID = map[uint64]common.Address{chainCfg.ChainID.Uint64(): mailboxContractAddr}
	backend.coordinatorAddr = coordAddr
	backend.coordinatorKey = coordKey
	backend.chainConfigOverride = &chainCfg
	backend.chainContextOverride = chainCtx
	backend.stateByNumberOverride = func(context.Context, rpc.BlockNumber) (*state.StateDB, *types.Header, error) {
		return stateFactory(), header, nil
	}
	backend.stateByNumberOrHashOverride = func(context.Context, rpc.BlockNumberOrHash) (*state.StateDB, *types.Header, error) {
		return stateFactory(), header, nil
	}

	return backend, stateFactory, chainCtx, header
}

// ========================================
// Tx bundle execution
// ========================================

// Runs a bundle simulation, returning a read miss and write messages.
// If fails, also fails the test.
// So no error is expected here. Don't use this if you want to test error cases. Rather, use runBundleSimulationWithError.
func runBundleSimulation(t *testing.T, backend *testAPIBackend, stateDB *state.StateDB, header *types.Header, request instanceproto.SimulationRequest) (*instanceproto.MailboxMessageHeader, []instanceproto.MailboxMessage) {
	t.Helper()
	missing, writes, err := runBundleSimulationWithError(t, backend, stateDB, header, request)
	if err != nil {
		t.Fatalf("simulateSCPBundle returned error: %v", err)
	}
	return missing, writes
}

func setStateOverrides(backend *testAPIBackend, stateDB *state.StateDB, header *types.Header) {
	backend.stateByNumberOverride = func(context.Context, rpc.BlockNumber) (*state.StateDB, *types.Header, error) {
		return stateDB, header, nil
	}
	backend.stateByNumberOrHashOverride = func(context.Context, rpc.BlockNumberOrHash) (*state.StateDB, *types.Header, error) {
		return stateDB, header, nil
	}
}

// Runs a bundle simulation, returning all the output (read miss, writes, and error).
func runBundleSimulationWithError(t *testing.T, backend *testAPIBackend, stateDB *state.StateDB, header *types.Header, request instanceproto.SimulationRequest) (*instanceproto.MailboxMessageHeader, []instanceproto.MailboxMessage, error) {
	t.Helper()
	setStateOverrides(backend, stateDB, header)
	return backend.simulateSCPBundle(request)
}

// ========================================
// Testing account utilities
// ========================================

const (
	testAccountFundingWei = 20_000_000_000_000_000
	defaultReadGasLimit   = 150_000
	defaultWriteGasLimit  = defaultPutInboxGas
	pingPongCallGasLimit  = defaultPutInboxGas
)

type testAccount struct {
	key  *ecdsa.PrivateKey
	addr common.Address
}

// Creates a new testing account with funds
func newTestAccount(t *testing.T, state *state.StateDB) testAccount {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	return fundTestAccount(t, state, key)
}

// Funds a test account
func fundTestAccount(t *testing.T, state *state.StateDB, key *ecdsa.PrivateKey) testAccount {
	t.Helper()
	addr := crypto.PubkeyToAddress(key.PublicKey)
	state.AddBalance(addr, uint256.NewInt(testAccountFundingWei), tracing.BalanceChangeUnspecified)
	state.SetNonce(addr, 0, tracing.NonceChangeUnspecified)
	return testAccount{key: key, addr: addr}
}

func (a testAccount) Address() common.Address {
	return a.addr
}

// ========================================
// Sign mailbox and PingPong transactions util
// ========================================

func (a testAccount) SignReadTx(t *testing.T, backend *testAPIBackend, nonce uint64, chainSrc uint64, sender common.Address, sessionID uint64, label string) []byte {
	t.Helper()
	callData := encodeMailboxReadCall(t, chainSrc, sender, sessionID, label)
	return mustEncodeSignedTx(t, backend.ChainConfig().ChainID, a.key, nonce, backend.mailboxAddresses[0], callData, defaultReadGasLimit)
}

func (a testAccount) SignWriteTx(t *testing.T, backend *testAPIBackend, nonce uint64, destChain uint64, receiver common.Address, sessionID uint64, label string, payload []byte) []byte {
	t.Helper()
	callData := encodeMailboxWriteCall(t, destChain, receiver, sessionID, label, payload)
	return mustEncodeSignedTx(t, backend.ChainConfig().ChainID, a.key, nonce, backend.mailboxAddresses[0], callData, defaultWriteGasLimit)
}

func (a testAccount) SignPingTx(t *testing.T, backend *testAPIBackend, nonce uint64, otherChain uint64, pongSender, pingReceiver common.Address, sessionID uint64, payload []byte) []byte {
	t.Helper()
	callData := encodePingCall(t, otherChain, pongSender, pingReceiver, sessionID, payload)
	return mustEncodeSignedTx(t, backend.ChainConfig().ChainID, a.key, nonce, pingPongContractAddr, callData, pingPongCallGasLimit)
}

func (a testAccount) SignPongTx(t *testing.T, backend *testAPIBackend, nonce uint64, otherChain uint64, pingSender common.Address, sessionID uint64, payload []byte) []byte {
	t.Helper()
	callData := encodePongCall(t, otherChain, pingSender, sessionID, payload)
	return mustEncodeSignedTx(t, backend.ChainConfig().ChainID, a.key, nonce, pingPongContractAddr, callData, pingPongCallGasLimit)
}

// ========================================
// Output verification utilities
// ========================================

func assertStateUnchanged(t *testing.T, state *state.StateDB, expected common.Hash) {
	t.Helper()
	if after := state.IntermediateRoot(false); after != expected {
		t.Fatalf("state mutated during simulation")
	}
}

type missingExpectation struct {
	source   uint64
	dest     uint64
	session  uint64
	label    string
	receiver common.Address
}

func requireMissingHeader(t *testing.T, missing *instanceproto.MailboxMessageHeader, want missingExpectation, context string) {
	t.Helper()
	if missing == nil {
		if context == "" {
			context = "expected missing mailbox header"
		}
		t.Fatalf(context)
	}
	fail := func(format string, args ...interface{}) {
		if context != "" {
			format = context + ": " + format
		}
		t.Fatalf(format, args...)
	}
	if want.source != 0 && missing.SourceChainID != compose.ChainID(want.source) {
		fail("unexpected source chain: %d", missing.SourceChainID)
	}
	if want.dest != 0 && missing.DestChainID != compose.ChainID(want.dest) {
		fail("unexpected destination chain: %d", missing.DestChainID)
	}
	if want.session != 0 && missing.SessionID != compose.SessionID(want.session) {
		fail("unexpected session id: %d", missing.SessionID)
	}
	if want.label != "" && missing.Label != want.label {
		fail("unexpected label: %s", missing.Label)
	}
	if want.receiver != (common.Address{}) && commonAddressFromCompose(missing.Receiver) != want.receiver {
		fail("unexpected receiver: %s", commonAddressFromCompose(missing.Receiver).Hex())
	}
}

func requireNoMissing(t *testing.T, missing *instanceproto.MailboxMessageHeader, context string) {
	t.Helper()
	if missing != nil {
		if context == "" {
			context = "unexpected missing mailbox header"
		}
		t.Fatalf("%s: %+v", context, *missing)
	}
}

type writeExpectation struct {
	source   uint64
	dest     uint64
	session  uint64
	label    string
	data     []byte
	receiver common.Address
}

func requireWriteMessage(t *testing.T, got instanceproto.MailboxMessage, want writeExpectation, context string) {
	t.Helper()
	fail := func(format string, args ...interface{}) {
		if context != "" {
			format = context + ": " + format
		}
		t.Fatalf(format, args...)
	}
	if want.source != 0 && got.SourceChainID != compose.ChainID(want.source) {
		fail("unexpected source chain: %d", got.SourceChainID)
	}
	if want.dest != 0 && got.DestChainID != compose.ChainID(want.dest) {
		fail("unexpected destination chain: %d", got.DestChainID)
	}
	if want.session != 0 && got.SessionID != compose.SessionID(want.session) {
		fail("unexpected session id: %d", got.SessionID)
	}
	if want.label != "" && got.Label != want.label {
		fail("unexpected label: %s", got.Label)
	}
	if want.data != nil && !bytes.Equal(got.Data, want.data) {
		fail("unexpected payload: %x", got.Data)
	}
	if want.receiver != (common.Address{}) && commonAddressFromCompose(got.Receiver) != want.receiver {
		fail("unexpected receiver: %s", commonAddressFromCompose(got.Receiver).Hex())
	}
}

func hashToComposeRoot(h common.Hash) compose.StateRoot {
	var out compose.StateRoot
	copy(out[:], h[:])
	return out
}

// ========================================
// Call data encoding utilities
// ========================================

func encodeMailboxReadCall(t *testing.T, chainSrc uint64, sender common.Address, sessionID uint64, label string) []byte {
	t.Helper()
	//parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	//if err != nil {
	//	t.Fatalf("parse mailbox ABI: %v", err)
	//}
	parsedABI := mailboxABIParsed
	data, err := parsedABI.Pack(
		"read",
		new(big.Int).SetUint64(chainSrc),
		sender,
		new(big.Int).SetUint64(sessionID),
		[]byte(label),
	)
	if err != nil {
		t.Fatalf("pack read calldata: %v", err)
	}
	return data
}

func encodeMailboxWriteCall(t *testing.T, destChain uint64, receiver common.Address, sessionID uint64, label string, payload []byte) []byte {
	t.Helper()
	//parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	//if err != nil {
	//	t.Fatalf("parse mailbox ABI: %v", err)
	//}
	parsedABI := mailboxABIParsed
	data, err := parsedABI.Pack(
		"write",
		new(big.Int).SetUint64(destChain),
		receiver,
		new(big.Int).SetUint64(sessionID),
		[]byte(label),
		payload,
	)
	if err != nil {
		t.Fatalf("pack write calldata: %v", err)
	}
	return data
}

func encodePingCall(t *testing.T, otherChain uint64, pongSender, pingReceiver common.Address, sessionID uint64, payload []byte) []byte {
	t.Helper()
	if len(pingPongABIParsed.Methods) == 0 {
		t.Fatalf("pingpong ABI not initialized")
	}
	data, err := pingPongABIParsed.Pack(
		"ping",
		new(big.Int).SetUint64(otherChain),
		pongSender,
		pingReceiver,
		new(big.Int).SetUint64(sessionID),
		payload,
	)
	if err != nil {
		t.Fatalf("pack ping calldata: %v", err)
	}
	return data
}

func encodePongCall(t *testing.T, otherChain uint64, pingSender common.Address, sessionID uint64, payload []byte) []byte {
	t.Helper()
	if len(pingPongABIParsed.Methods) == 0 {
		t.Fatalf("pingpong ABI not initialized")
	}
	data, err := pingPongABIParsed.Pack(
		"pong",
		new(big.Int).SetUint64(otherChain),
		pingSender,
		new(big.Int).SetUint64(sessionID),
		payload,
	)
	if err != nil {
		t.Fatalf("pack pong calldata: %v", err)
	}
	return data
}

func mustEncodeSignedTx(t *testing.T, chainID *big.Int, key *ecdsa.PrivateKey, nonce uint64, to common.Address, data []byte, gasLimit uint64) []byte {
	t.Helper()

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     nonce,
		GasTipCap: big.NewInt(1_000_000_000),
		GasFeeCap: big.NewInt(20_000_000_000),
		Gas:       gasLimit,
		To:        &to,
		Value:     big.NewInt(0),
		Data:      data,
	})

	signed, err := types.SignTx(tx, types.NewLondonSigner(chainID), key)
	if err != nil {
		t.Fatalf("sign transaction: %v", err)
	}
	payload, err := signed.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal transaction: %v", err)
	}
	return payload
}
