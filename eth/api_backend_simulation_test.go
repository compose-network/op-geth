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
	"github.com/ethereum/go-ethereum/core/ssv"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/holiman/uint256"
)

// Tests analyzeMailboxTrace for missing reads and writes.
func TestAnalyzeMailboxTraceReturnsMissingRead(t *testing.T) {
	mailboxAddr := common.HexToAddress("0x1000000000000000000000000000000000000001")
	readerContract := common.HexToAddress("0x2000000000000000000000000000000000000002")
	sourceSender := common.HexToAddress("0x3000000000000000000000000000000000000003")

	processor := &MailboxProcessor{
		chainID:          10,
		mailboxAddresses: []common.Address{mailboxAddr},
	}

	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		t.Fatalf("parse mailbox ABI: %v", err)
	}

	// Prepare calldata for a read operation from chain 7, from sender, session ID 42, label "coord".
	sessionID := uint64(42)
	callData, err := parsedABI.Pack(
		"read",
		new(big.Int).SetUint64(7),
		sourceSender,
		new(big.Int).SetUint64(sessionID),
		[]byte("coord"),
	)
	if err != nil {
		t.Fatalf("pack read calldata: %v", err)
	}

	// Mock trace result with a STATICCALL to the mailbox's read function.
	trace := &ssv.SSVTraceResult{
		Operations: []ssv.SSVOperation{
			{
				Type:     vm.STATICCALL,
				Address:  mailboxAddr,
				From:     readerContract,
				CallData: callData,
			},
		},
		ExecutionResult: &core.ExecutionResult{},
	}

	// Run analye function
	backend := &EthAPIBackend{}
	missing, writes, err := backend.analyzeMailboxTrace(processor, trace, map[string]struct{}{})

	// Err should be nil
	if err != nil {
		t.Fatalf("analyzeMailboxTrace returned error: %v", err)
	}
	// Missing should have the msg we expect
	if missing == nil {
		t.Fatalf("expected missing mailbox header, got nil")
	}
	// No writes should be returned
	if writes != nil {
		t.Fatalf("expected no writes when read missing, got %d messages", len(writes))
	}
	// Validate missing header contents
	if missing.SourceChainID != compose.ChainID(7) || missing.DestChainID != compose.ChainID(10) {
		t.Fatalf("unexpected chains in missing header: %+v", *missing)
	}
	// Validate label and session ID
	if missing.Label != "coord" {
		t.Fatalf("unexpected label: %s", missing.Label)
	}
	if missing.SessionID != compose.SessionID(sessionID) {
		t.Fatalf("unexpected session ID: %d", missing.SessionID)
	}
}

// Test that analyzeMailboxTrace ignores reads that are already fulfilled.
func TestAnalyzeMailboxTraceRespectsFulfilledReads(t *testing.T) {
	mailboxAddr := common.HexToAddress("0x4000000000000000000000000000000000000004")
	readerContract := common.HexToAddress("0x5000000000000000000000000000000000000005")
	sourceSender := common.HexToAddress("0x6000000000000000000000000000000000000006")

	processor := &MailboxProcessor{
		chainID:          25,
		mailboxAddresses: []common.Address{mailboxAddr},
	}

	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		t.Fatalf("parse mailbox ABI: %v", err)
	}

	session := uint64(77)
	callData, err := parsedABI.Pack(
		"read",
		new(big.Int).SetUint64(12),
		sourceSender,
		new(big.Int).SetUint64(session),
		[]byte("token"),
	)
	if err != nil {
		t.Fatalf("pack read calldata: %v", err)
	}

	trace := &ssv.SSVTraceResult{
		Operations: []ssv.SSVOperation{{
			Type:     vm.STATICCALL,
			Address:  mailboxAddr,
			From:     readerContract,
			CallData: callData,
		}},
		ExecutionResult: &core.ExecutionResult{},
	}

	// Compute the key of a fulfilled read
	header, err := buildReadHeader(&MailboxCall{
		ChainSrc: new(big.Int).SetUint64(12),
		ChainDest: func() *big.Int {
			return new(big.Int).SetUint64(25)
		}(),
		Sender:    sourceSender,
		SessionId: new(big.Int).SetUint64(session),
		Label:     []byte("token"),
	}, readerContract)
	if err != nil {
		t.Fatalf("prepare header: %v", err)
	}
	key := mailboxHeaderKey(*header)

	// Run analyze function with the read marked as fulfilled
	backend := &EthAPIBackend{}
	missing, writes, err := backend.analyzeMailboxTrace(processor, trace, map[string]struct{}{key: {}})
	// Err should be nil
	if err != nil {
		t.Fatalf("analyzeMailboxTrace returned error: %v", err)
	}
	// Missing should be nil since read is fulfilled
	if missing != nil {
		t.Fatalf("expected fulfilled read to be ignored")
	}
	// No writes is expected
	if len(writes) != 0 {
		t.Fatalf("expected no writes in read-only trace, got %d", len(writes))
	}
}

// Test that analyzeMailboxTrace correctly extracts outbound messages from write calls.
func TestAnalyzeMailboxTraceCollectsWrites(t *testing.T) {
	mailboxAddr := common.HexToAddress("0x7000000000000000000000000000000000000007")
	senderContract := common.HexToAddress("0x8000000000000000000000000000000000000008")
	receiver := common.HexToAddress("0x9000000000000000000000000000000000000009")

	processor := &MailboxProcessor{
		chainID:          42,
		mailboxAddresses: []common.Address{mailboxAddr},
	}

	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		t.Fatalf("parse mailbox ABI: %v", err)
	}
	// Create a write call to chain 55, receiver, session ID 99, label "hello", payload "payload"
	callData, err := parsedABI.Pack(
		"write",
		new(big.Int).SetUint64(55),
		receiver,
		new(big.Int).SetUint64(99),
		[]byte("hello"),
		[]byte("payload"),
	)
	if err != nil {
		t.Fatalf("pack write calldata: %v", err)
	}

	trace := &ssv.SSVTraceResult{
		Operations: []ssv.SSVOperation{{
			Type:     vm.CALL,
			Address:  mailboxAddr,
			From:     senderContract,
			CallData: callData,
		}},
		ExecutionResult: &core.ExecutionResult{},
	}

	// Run analyze function
	backend := &EthAPIBackend{}
	missing, writes, err := backend.analyzeMailboxTrace(processor, trace, map[string]struct{}{})
	// Err should be nil
	if err != nil {
		t.Fatalf("analyzeMailboxTrace returned error: %v", err)
	}
	// No missing reads expected
	if missing != nil {
		t.Fatalf("expected no missing reads")
	}
	// One write expected
	if len(writes) != 1 {
		t.Fatalf("expected a single outbound message, got %d", len(writes))
	}
	// Validate contents of the write
	msg := writes[0]
	if msg.SourceChainID != compose.ChainID(42) || msg.DestChainID != compose.ChainID(55) {
		t.Fatalf("unexpected chains in outbound message: %+v", msg.MailboxMessageHeader)
	}
	if msg.Label != "hello" {
		t.Fatalf("unexpected label: %s", msg.Label)
	}
	if string(msg.Data) != "payload" {
		t.Fatalf("unexpected payload: %s", string(msg.Data))
	}
}

// Test that buildPutInboxCalldata constructs valid calldata for putInbox.
func TestBuildPutInboxCalldata(t *testing.T) {
	// Create a sample MailboxMessage
	message := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(3),
			DestChainID:   compose.ChainID(9),
			Sender:        composeAddressFromCommon(common.HexToAddress("0x1")),
			Receiver:      composeAddressFromCommon(common.HexToAddress("0x2")),
			SessionID:     compose.SessionID(11),
			Label:         "abc",
		},
		Data: []byte{0xaa, 0xbb},
	}

	// Build calldata
	data, err := buildPutInboxCalldata(message)
	if err != nil {
		t.Fatalf("buildPutInboxCalldata failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected non-empty calldata")
	}

	// Verify that calldata starts with putInbox selector
	parsedABI, _ := abi.JSON(strings.NewReader(mailboxABI))
	prefix := common.Bytes2Hex(parsedABI.Methods["putInbox"].ID)
	if !strings.HasPrefix(common.Bytes2Hex(data), prefix) {
		t.Fatalf("calldata does not start with putInbox selector")
	}

	// Validate other fields by unpacking
	values, err := parsedABI.Methods["putInbox"].Inputs.Unpack(data[4:])
	if err != nil {
		t.Fatalf("unpack calldata failed: %v", err)
	}
	if len(values) != 6 {
		t.Fatalf("unexpected argument count: %d", len(values))
	}

	if got := compose.ChainID(values[0].(*big.Int).Uint64()); got != message.SourceChainID {
		t.Fatalf("source chain mismatch: got %d want %d", got, message.SourceChainID)
	}
	if addr := values[1].(common.Address); addr != common.HexToAddress("0x1") {
		t.Fatalf("sender mismatch: got %s", addr.Hex())
	}
	if addr := values[2].(common.Address); addr != common.HexToAddress("0x2") {
		t.Fatalf("receiver mismatch: got %s", addr.Hex())
	}
	if got := compose.SessionID(values[3].(*big.Int).Uint64()); got != message.SessionID {
		t.Fatalf("session mismatch: got %d want %d", got, message.SessionID)
	}
	if label := string(values[4].([]byte)); label != message.Label {
		t.Fatalf("label mismatch: got %s want %s", label, message.Label)
	}
	if payload := values[5].([]byte); !bytes.Equal(payload, message.Data) {
		t.Fatalf("payload mismatch: got %x want %x", payload, message.Data)
	}
}

// Test that buildReadHeader validates required fields.
func TestBuildReadHeaderValidation(t *testing.T) {
	// Create a sample MailboxCall
	call := &MailboxCall{
		ChainSrc:  new(big.Int).SetUint64(1),
		ChainDest: new(big.Int).SetUint64(2),
		Sender:    common.HexToAddress("0x1"),
		SessionId: new(big.Int).SetUint64(3),
		Label:     []byte("lbl"),
	}

	// Build header
	header, err := buildReadHeader(call, common.HexToAddress("0x2"))
	if err != nil {
		t.Fatalf("buildReadHeader failed: %v", err)
	}
	// Validate source and dest chain IDs
	if header.SourceChainID != compose.ChainID(1) || header.DestChainID != compose.ChainID(2) {
		t.Fatalf("unexpected header chains: %+v", header)
	}

	// Confirm error when ChainSrc is missing
	call.ChainSrc = nil
	if _, err := buildReadHeader(call, common.Address{}); err == nil {
		t.Fatalf("expected error when chain source missing")
	}
}

// Test that buildWriteMessage validates required fields.
func TestBuildWriteMessageValidation(t *testing.T) {
	// Create a sample MailboxCall
	call := &MailboxCall{
		ChainSrc:              new(big.Int).SetUint64(5),
		ChainDest:             new(big.Int).SetUint64(6),
		Receiver:              common.HexToAddress("0x3"),
		SessionId:             new(big.Int).SetUint64(7),
		Label:                 []byte("msg"),
		Data:                  []byte("data"),
		ChainMessageRecipient: new(big.Int).SetUint64(6),
	}

	// Build message
	msg, err := buildWriteMessage(call, common.HexToAddress("0x4"))
	if err != nil {
		t.Fatalf("buildWriteMessage failed: %v", err)
	}
	// Validate source and dest chain IDs
	if msg.SourceChainID != compose.ChainID(5) || msg.DestChainID != compose.ChainID(6) {
		t.Fatalf("unexpected chains: %+v", msg.MailboxMessageHeader)
	}

	// Confirm error when ChainDest is missing
	call.ChainDest = nil
	if _, err := buildWriteMessage(call, common.Address{}); err == nil {
		t.Fatalf("expected error when chain destination missing")
	}
}

// Tests that applyPutInboxMessage correctly applies a putInbox message to state.
func TestApplyPutInboxMessage(t *testing.T) {
	backend, stateFactory, chainCtx, header := setupSimulationTestBackend(t)

	// Message for putInbox
	msg := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(10),
			DestChainID:   compose.ChainID(backend.chainCfg.ChainID.Uint64()),
			Sender:        composeAddressFromCommon(common.HexToAddress("0x5000000000000000000000000000000000000055")),
			Receiver:      composeAddressFromCommon(common.HexToAddress("0x2000000000000000000000000000000000000022")),
			SessionID:     compose.SessionID(123),
			Label:         "label",
		},
		Data: []byte("payload"),
	}

	stateCopy := stateFactory()
	blockCtx := core.NewEVMBlockContext(header, chainCtx, nil, backend.ChainConfig(), stateCopy)
	vmCfg := vm.Config{}

	if err := backend.applyPutInboxMessage(blockCtx, vmCfg, stateCopy, msg); err != nil {
		t.Fatalf("applyPutInboxMessage returned error: %v", err)
	}
	// Confirm nonce has been updated
	if nonce := stateCopy.GetNonce(backend.coordinatorAddr); nonce != 1 {
		t.Fatalf("expected coordinator nonce 1 after putInbox, got %d", nonce)
	}
}

func TestSimulateSCPBundle(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)

	// Message for putInbox
	msg := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(10),
			DestChainID:   compose.ChainID(backend.chainCfg.ChainID.Uint64()),
			Sender:        composeAddressFromCommon(common.HexToAddress("0x5000000000000000000000000000000000000055")),
			Receiver:      composeAddressFromCommon(common.HexToAddress("0x2000000000000000000000000000000000000022")),
			SessionID:     compose.SessionID(123),
			Label:         "label",
		},
		Data: []byte("payload"),
	}

	baseState := stateFactory()
	initialRoot := baseState.IntermediateRoot(false)

	request := instanceproto.SimulationRequest{
		PutInboxMessages: []instanceproto.MailboxMessage{msg},
		Transactions:     [][]byte{},
		Snapshot:         hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	if missing != nil {
		t.Fatalf("expected no missing mailbox header")
	}
	if len(writes) != 0 {
		t.Fatalf("expected no outbound mailbox messages, got %d", len(writes))
	}
	if after := baseState.IntermediateRoot(false); after != initialRoot {
		t.Fatalf("base state mutated during simulation")
	}
}

func TestSimulateSCPBundleDetectsMissingRead(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	// Create reader account (or contract)
	readerKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate reader key: %v", err)
	}
	readerAddr := crypto.PubkeyToAddress(readerKey.PublicKey)
	baseState.AddBalance(readerAddr, uint256.NewInt(5_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	baseState.SetNonce(readerAddr, 0, tracing.NonceChangeUnspecified)

	sessionID := uint64(77)
	label := []byte("read-test")
	readCallData := encodeMailboxReadCall(t, 12, readerAddr, sessionID, label)

	// Build request
	payload := mustEncodeSignedTx(t, backend.ChainConfig().ChainID, readerKey, 0, backend.mailboxAddresses[0], readCallData, 150_000)
	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{payload},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	// Run simulation
	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	// Read missing is expected
	if missing == nil {
		t.Fatalf("expected missing mailbox header from read transaction")
	}
	// Validate message fields
	if missing.SourceChainID != compose.ChainID(12) {
		t.Fatalf("unexpected source chain: %d", missing.SourceChainID)
	}
	if missing.DestChainID != compose.ChainID(backend.ChainConfig().ChainID.Uint64()) {
		t.Fatalf("unexpected destination chain: %d", missing.DestChainID)
	}
	if missing.SessionID != compose.SessionID(sessionID) {
		t.Fatalf("unexpected session id: %d", missing.SessionID)
	}
	if missing.Label != string(label) {
		t.Fatalf("unexpected label: %s", missing.Label)
	}
	if commonAddressFromCompose(missing.Receiver) != readerAddr {
		t.Fatalf("unexpected receiver: %s", commonAddressFromCompose(missing.Receiver).Hex())
	}
	if len(writes) != 0 {
		t.Fatalf("expected no outbound writes, got %d", len(writes))
	}
	if after := baseState.IntermediateRoot(false); after != initialRoot {
		t.Fatalf("base state mutated during simulation")
	}
}

func TestSimulateSCPBundleCollectsWrites(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	writerKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate writer key: %v", err)
	}
	writerAddr := crypto.PubkeyToAddress(writerKey.PublicKey)
	baseState.AddBalance(writerAddr, uint256.NewInt(5_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	baseState.SetNonce(writerAddr, 0, tracing.NonceChangeUnspecified)

	destChain := uint64(101)
	sessionID := uint64(55)
	label := []byte("write-test")
	payloadData := []byte("payload")
	writeCallData := encodeMailboxWriteCall(t, destChain, common.HexToAddress("0x3000000000000000000000000000000000000033"), sessionID, label, payloadData)

	payload := mustEncodeSignedTx(t, backend.ChainConfig().ChainID, writerKey, 0, backend.mailboxAddresses[0], writeCallData, 200_000)

	// Build request with write as main tx
	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{payload},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	if missing != nil {
		t.Fatalf("did not expect missing mailbox header")
	}
	if len(writes) != 1 {
		t.Fatalf("expected a single outbound write, got %d", len(writes))
	}

	out := writes[0]
	if out.SourceChainID != compose.ChainID(backend.ChainConfig().ChainID.Uint64()) {
		t.Fatalf("unexpected source chain: %d", out.SourceChainID)
	}
	if out.DestChainID != compose.ChainID(destChain) {
		t.Fatalf("unexpected destination chain: %d", out.DestChainID)
	}
	if out.SessionID != compose.SessionID(sessionID) {
		t.Fatalf("unexpected session id: %d", out.SessionID)
	}
	if out.Label != string(label) {
		t.Fatalf("unexpected label: %s", out.Label)
	}
	if !bytes.Equal(out.Data, payloadData) {
		t.Fatalf("unexpected payload: %x", out.Data)
	}

	if after := baseState.IntermediateRoot(false); after != initialRoot {
		t.Fatalf("base state mutated during simulation")
	}
}

// Tests the simulation of a bundle with two reads (as main txs).
// The result should be that only the first read miss is reported, as the second shouldn't even be reached.
func TestSimulateSCPBundleMultipleReads(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	chainID := backend.ChainConfig().ChainID.Uint64()

	keyOne, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key one: %v", err)
	}
	addrOne := crypto.PubkeyToAddress(keyOne.PublicKey)
	baseState.AddBalance(addrOne, uint256.NewInt(5_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	baseState.SetNonce(addrOne, 0, tracing.NonceChangeUnspecified)

	keyTwo, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key two: %v", err)
	}
	addrTwo := crypto.PubkeyToAddress(keyTwo.PublicKey)
	baseState.AddBalance(addrTwo, uint256.NewInt(5_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	baseState.SetNonce(addrTwo, 0, tracing.NonceChangeUnspecified)

	labelOne := []byte("read-one")
	labelTwo := []byte("read-two")

	// Create two read transactions
	readOne := mustEncodeSignedTx(
		t,
		backend.ChainConfig().ChainID,
		keyOne,
		0,
		backend.mailboxAddresses[0],
		encodeMailboxReadCall(t, 12, addrOne, 11, labelOne),
		150_000,
	)
	readTwo := mustEncodeSignedTx(
		t,
		backend.ChainConfig().ChainID,
		keyTwo,
		0,
		backend.mailboxAddresses[0],
		encodeMailboxReadCall(t, 13, addrTwo, 22, labelTwo),
		150_000,
	)

	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{readOne, readTwo},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	// Ensure only the first read miss is reported
	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	if missing == nil {
		t.Fatalf("expected missing mailbox header")
	}
	if missing.SourceChainID != compose.ChainID(12) {
		t.Fatalf("unexpected source chain: %d", missing.SourceChainID)
	}
	if missing.DestChainID != compose.ChainID(chainID) {
		t.Fatalf("unexpected destination chain: %d", missing.DestChainID)
	}
	if missing.SessionID != compose.SessionID(11) {
		t.Fatalf("unexpected session id: %d", missing.SessionID)
	}
	if missing.Label != string(labelOne) {
		t.Fatalf("unexpected label: %s", missing.Label)
	}
	if commonAddressFromCompose(missing.Receiver) != addrOne {
		t.Fatalf("unexpected receiver: %s", commonAddressFromCompose(missing.Receiver).Hex())
	}
	if len(writes) != 0 {
		t.Fatalf("expected no writes, got %d", len(writes))
	}
	if after := baseState.IntermediateRoot(false); after != initialRoot {
		t.Fatalf("base state mutated during simulation")
	}
}

// Tests the simulation of a bundle with two writes (as main txs).
// Both writes should be collected and returned. No read miss is expected.
func TestSimulateSCPBundleMultipleWrites(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	keyOne, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key one: %v", err)
	}
	addrOne := crypto.PubkeyToAddress(keyOne.PublicKey)
	baseState.AddBalance(addrOne, uint256.NewInt(5_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	baseState.SetNonce(addrOne, 0, tracing.NonceChangeUnspecified)

	keyTwo, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key two: %v", err)
	}
	addrTwo := crypto.PubkeyToAddress(keyTwo.PublicKey)
	baseState.AddBalance(addrTwo, uint256.NewInt(5_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	baseState.SetNonce(addrTwo, 0, tracing.NonceChangeUnspecified)

	writeOne := mustEncodeSignedTx(
		t,
		backend.ChainConfig().ChainID,
		keyOne,
		0,
		backend.mailboxAddresses[0],
		encodeMailboxWriteCall(
			t,
			201,
			common.HexToAddress("0x4000000000000000000000000000000000000044"),
			31,
			[]byte("write-one"),
			[]byte("payload-one"),
		),
		200_000,
	)

	writeTwo := mustEncodeSignedTx(
		t,
		backend.ChainConfig().ChainID,
		keyTwo,
		0,
		backend.mailboxAddresses[0],
		encodeMailboxWriteCall(
			t,
			202,
			common.HexToAddress("0x5000000000000000000000000000000000000055"),
			32,
			[]byte("write-two"),
			[]byte("payload-two"),
		),
		200_000,
	)

	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{writeOne, writeTwo},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	if missing != nil {
		t.Fatalf("unexpected missing mailbox header")
	}
	if len(writes) != 2 {
		t.Fatalf("expected 2 writes, got %d", len(writes))
	}

	localChain := compose.ChainID(backend.ChainConfig().ChainID.Uint64())

	first := writes[0]
	if first.SourceChainID != localChain {
		t.Fatalf("unexpected first source chain: %d", first.SourceChainID)
	}
	if first.DestChainID != compose.ChainID(201) {
		t.Fatalf("unexpected first dest chain: %d", first.DestChainID)
	}
	if first.SessionID != compose.SessionID(31) {
		t.Fatalf("unexpected first session: %d", first.SessionID)
	}
	if first.Label != "write-one" {
		t.Fatalf("unexpected first label: %s", first.Label)
	}
	if string(first.Data) != "payload-one" {
		t.Fatalf("unexpected first payload: %s", string(first.Data))
	}

	second := writes[1]
	if second.SourceChainID != localChain {
		t.Fatalf("unexpected second source chain: %d", second.SourceChainID)
	}
	if second.DestChainID != compose.ChainID(202) {
		t.Fatalf("unexpected second dest chain: %d", second.DestChainID)
	}
	if second.SessionID != compose.SessionID(32) {
		t.Fatalf("unexpected second session: %d", second.SessionID)
	}
	if second.Label != "write-two" {
		t.Fatalf("unexpected second label: %s", second.Label)
	}
	if string(second.Data) != "payload-two" {
		t.Fatalf("unexpected second payload: %s", string(second.Data))
	}

	if after := baseState.IntermediateRoot(false); after != initialRoot {
		t.Fatalf("base state mutated during simulation")
	}
}

// Tests a bundle with a putInbox message and a read that matches it.
// No missing read should be reported.
func TestSimulateSCPBundlePutInboxSatisfiedRead(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	readerKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate reader key: %v", err)
	}
	readerAddr := crypto.PubkeyToAddress(readerKey.PublicKey)
	baseState.AddBalance(readerAddr, uint256.NewInt(5_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	baseState.SetNonce(readerAddr, 0, tracing.NonceChangeUnspecified)

	sourceChain := uint64(321)
	sessionID := uint64(99)
	label := []byte("inbox-fulfilled")

	message := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(sourceChain),
			DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
			Sender:        composeAddressFromCommon(readerAddr),
			Receiver:      composeAddressFromCommon(readerAddr),
			SessionID:     compose.SessionID(sessionID),
			Label:         string(label),
		},
		Data: []byte("payload"),
	}

	readTx := mustEncodeSignedTx(
		t,
		backend.ChainConfig().ChainID,
		readerKey,
		0,
		backend.mailboxAddresses[0],
		encodeMailboxReadCall(t, sourceChain, readerAddr, sessionID, label),
		150_000,
	)

	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		PutInboxMessages: []instanceproto.MailboxMessage{message},
		Transactions:     [][]byte{readTx},
		Snapshot:         hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	if missing != nil {
		t.Fatalf("did not expect missing mailbox header")
	}
	if len(writes) != 0 {
		t.Fatalf("expected no outbound writes, got %d", len(writes))
	}
	if after := baseState.IntermediateRoot(false); after != initialRoot {
		t.Fatalf("base state mutated during simulation")
	}
}

// Tests a bundle with a putInbox message and a read that DOES NOT match it.
// A missing read should be reported.
func TestSimulateSCPBundlePutInboxDifferentRead(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	readerKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate reader key: %v", err)
	}
	readerAddr := crypto.PubkeyToAddress(readerKey.PublicKey)
	baseState.AddBalance(readerAddr, uint256.NewInt(5_000_000_000_000_000), tracing.BalanceChangeUnspecified)
	baseState.SetNonce(readerAddr, 0, tracing.NonceChangeUnspecified)

	fulfilledMsg := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(400),
			DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
			Sender:        composeAddressFromCommon(readerAddr),
			Receiver:      composeAddressFromCommon(readerAddr),
			SessionID:     compose.SessionID(10),
			Label:         "fulfilled",
		},
		Data: []byte("payload"),
	}

	readTx := mustEncodeSignedTx(
		t,
		backend.ChainConfig().ChainID,
		readerKey,
		0,
		backend.mailboxAddresses[0],
		encodeMailboxReadCall(t, 401, readerAddr, 11, []byte("different")),
		150_000,
	)

	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		PutInboxMessages: []instanceproto.MailboxMessage{fulfilledMsg},
		Transactions:     [][]byte{readTx},
		Snapshot:         hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	if missing == nil {
		t.Fatalf("expected missing mailbox header for unmatched read")
	}
	if missing.SessionID == compose.SessionID(10) && missing.Label == fulfilledMsg.Label {
		t.Fatalf("expected missing header to differ from fulfilled message")
	}
	if len(writes) != 0 {
		t.Fatalf("expected no outbound writes, got %d", len(writes))
	}
	if after := baseState.IntermediateRoot(false); after != initialRoot {
		t.Fatalf("base state mutated during simulation")
	}
}

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

func setupSimulationTestBackend(t *testing.T) (*testAPIBackend, func() *state.StateDB, core.ChainContext, *types.Header) {
	t.Helper()

	mailboxAddr := common.HexToAddress("0x1000000000000000000000000000000000000011")

	coordKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate coordinator key: %v", err)
	}
	coordAddr := crypto.PubkeyToAddress(coordKey.PublicKey)

	// State factory to create fresh state dbs for each test
	stateFactory := func() *state.StateDB {
		stateDB, err := state.New(common.Hash{}, state.NewDatabaseForTesting())
		if err != nil {
			t.Fatalf("failed to create state db: %v", err)
		}
		stateDB.SetCode(mailboxAddr, []byte{0x60, 0x00, 0x60, 0x00, 0xf3})
		gasBudget := uint256.NewInt(defaultPutInboxGas)
		gasBudget.Mul(gasBudget, uint256.NewInt(20_000_000_000))
		stateDB.AddBalance(coordAddr, gasBudget, tracing.BalanceChangeUnspecified)
		stateDB.Finalise(true)
		return stateDB
	}

	chainCfg := *params.AllEthashProtocolChanges
	chainCfg.ChainID = big.NewInt(99)

	header := &types.Header{
		Number:     big.NewInt(1),
		GasLimit:   30_000_000,
		Time:       1,
		BaseFee:    big.NewInt(1_000_000_000),
		Difficulty: big.NewInt(1),
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

	backend.mailboxAddresses = []common.Address{mailboxAddr}
	backend.mailboxByChainID = map[uint64]common.Address{chainCfg.ChainID.Uint64(): mailboxAddr}
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

func setStateOverrides(backend *testAPIBackend, stateDB *state.StateDB, header *types.Header) {
	backend.stateByNumberOverride = func(context.Context, rpc.BlockNumber) (*state.StateDB, *types.Header, error) {
		return stateDB, header, nil
	}
	backend.stateByNumberOrHashOverride = func(context.Context, rpc.BlockNumberOrHash) (*state.StateDB, *types.Header, error) {
		return stateDB, header, nil
	}
}

func runBundleSimulation(t *testing.T, backend *testAPIBackend, stateDB *state.StateDB, header *types.Header, request instanceproto.SimulationRequest) (*instanceproto.MailboxMessageHeader, []instanceproto.MailboxMessage) {
	t.Helper()
	setStateOverrides(backend, stateDB, header)
	missing, writes, err := backend.simulateSCPBundle(request)
	if err != nil {
		t.Fatalf("simulateSCPBundle returned error: %v", err)
	}
	return missing, writes
}

func hashToComposeRoot(h common.Hash) compose.StateRoot {
	var out compose.StateRoot
	copy(out[:], h[:])
	return out
}

func encodeMailboxReadCall(t *testing.T, chainSrc uint64, sender common.Address, sessionID uint64, label []byte) []byte {
	t.Helper()
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		t.Fatalf("parse mailbox ABI: %v", err)
	}
	data, err := parsedABI.Pack(
		"read",
		new(big.Int).SetUint64(chainSrc),
		sender,
		new(big.Int).SetUint64(sessionID),
		label,
	)
	if err != nil {
		t.Fatalf("pack read calldata: %v", err)
	}
	return data
}

func encodeMailboxWriteCall(t *testing.T, destChain uint64, receiver common.Address, sessionID uint64, label []byte, payload []byte) []byte {
	t.Helper()
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		t.Fatalf("parse mailbox ABI: %v", err)
	}
	data, err := parsedABI.Pack(
		"write",
		new(big.Int).SetUint64(destChain),
		receiver,
		new(big.Int).SetUint64(sessionID),
		label,
		payload,
	)
	if err != nil {
		t.Fatalf("pack write calldata: %v", err)
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
