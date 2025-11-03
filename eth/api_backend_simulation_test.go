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

	sessionID := uint64(42)
	callData := encodeMailboxReadCall(t, 7, sourceSender, sessionID, "coord")

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
	requireMissingHeader(t, missing, missingExpectation{
		source:   7,
		dest:     10,
		session:  sessionID,
		label:    "coord",
		receiver: readerContract,
	}, "expected missing mailbox header")
	// No writes should be returned
	if writes != nil {
		t.Fatalf("expected no writes when read missing, got %d messages", len(writes))
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

	session := uint64(77)
	callData := encodeMailboxReadCall(t, 12, sourceSender, session, "token")

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
	requireNoMissing(t, missing, "expected fulfilled read to be ignored")
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

	callData := encodeMailboxWriteCall(t, 55, receiver, 99, "hello", []byte("payload"))

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
	requireNoMissing(t, missing, "expected no missing reads")
	// One write expected
	if len(writes) != 1 {
		t.Fatalf("expected a single outbound message, got %d", len(writes))
	}
	requireWriteMessage(t, writes[0], writeExpectation{
		source:   42,
		dest:     55,
		session:  99,
		label:    "hello",
		data:     []byte("payload"),
		receiver: receiver,
	}, "unexpected outbound mailbox message")
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
	requireNoMissing(t, missing, "expected no missing mailbox header")
	if len(writes) != 0 {
		t.Fatalf("expected no outbound mailbox messages, got %d", len(writes))
	}
	assertStateUnchanged(t, baseState, initialRoot)
}

func TestSimulateSCPBundleDetectsMissingRead(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	// Create reader account (or contract)
	reader := newTestAccount(t, baseState)

	// Create read transaction
	sessionID := uint64(77)
	label := "read-test"
	payload := reader.SignReadTx(t, backend, 0, 12, reader.Address(), sessionID, label)
	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{payload},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	// Run simulation
	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	requireMissingHeader(t, missing, missingExpectation{
		source:   12,
		dest:     backend.ChainConfig().ChainID.Uint64(),
		session:  sessionID,
		label:    label,
		receiver: reader.Address(),
	}, "expected missing mailbox header from read transaction")
	if len(writes) != 0 {
		t.Fatalf("expected no outbound writes, got %d", len(writes))
	}
	assertStateUnchanged(t, baseState, initialRoot)
}

func TestSimulateSCPBundleCollectsWrites(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	writer := newTestAccount(t, baseState)

	destChain := uint64(101)
	sessionID := uint64(55)
	label := "write-test"
	payloadData := []byte("payload")
	payload := writer.SignWriteTx(t, backend, 0, destChain, common.HexToAddress("0x3000000000000000000000000000000000000033"), sessionID, label, payloadData)

	// Build request with write as main tx
	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{payload},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	requireNoMissing(t, missing, "did not expect missing mailbox header")
	if len(writes) != 1 {
		t.Fatalf("expected a single outbound write, got %d", len(writes))
	}

	requireWriteMessage(t, writes[0], writeExpectation{
		source:   backend.ChainConfig().ChainID.Uint64(),
		dest:     destChain,
		session:  sessionID,
		label:    label,
		data:     payloadData,
		receiver: common.HexToAddress("0x3000000000000000000000000000000000000033"),
	}, "unexpected write message")

	assertStateUnchanged(t, baseState, initialRoot)
}

// Tests the simulation of a bundle with two reads (as main txs).
// The result should be that only the first read miss is reported, as the second shouldn't even be reached.
func TestSimulateSCPBundleMultipleReads(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	chainID := backend.ChainConfig().ChainID.Uint64()

	accOne := newTestAccount(t, baseState)
	accTwo := newTestAccount(t, baseState)

	labelOne := "read-one"
	labelTwo := "read-two"

	readOne := accOne.SignReadTx(t, backend, 0, 12, accOne.Address(), 11, labelOne)
	readTwo := accTwo.SignReadTx(t, backend, 0, 13, accTwo.Address(), 22, labelTwo)

	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{readOne, readTwo},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	// Ensure only the first read miss is reported
	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	requireMissingHeader(t, missing, missingExpectation{
		source:   12,
		dest:     chainID,
		session:  11,
		label:    labelOne,
		receiver: accOne.Address(),
	}, "expected missing mailbox header")
	if len(writes) != 0 {
		t.Fatalf("expected no writes, got %d", len(writes))
	}
	assertStateUnchanged(t, baseState, initialRoot)
}

// Tests the simulation of a bundle with two writes (as main txs).
// Both writes should be collected and returned. No read miss is expected.
func TestSimulateSCPBundleMultipleWrites(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	accOne := newTestAccount(t, baseState)
	accTwo := newTestAccount(t, baseState)

	writeOne := accOne.SignWriteTx(t, backend, 0, 201, common.HexToAddress("0x4000000000000000000000000000000000000044"), 31, "write-one", []byte("payload-one"))

	writeTwo := accTwo.SignWriteTx(t, backend, 0, 202, common.HexToAddress("0x5000000000000000000000000000000000000055"), 32, "write-two", []byte("payload-two"))

	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{writeOne, writeTwo},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	requireNoMissing(t, missing, "unexpected missing mailbox header")
	if len(writes) != 2 {
		t.Fatalf("expected 2 writes, got %d", len(writes))
	}

	requireWriteMessage(t, writes[0], writeExpectation{
		source:  backend.ChainConfig().ChainID.Uint64(),
		dest:    201,
		session: 31,
		label:   "write-one",
		data:    []byte("payload-one"),
	}, "first write")
	requireWriteMessage(t, writes[1], writeExpectation{
		source:  backend.ChainConfig().ChainID.Uint64(),
		dest:    202,
		session: 32,
		label:   "write-two",
		data:    []byte("payload-two"),
	}, "second write")

	assertStateUnchanged(t, baseState, initialRoot)
}

// Tests a bundle with a putInbox message and a read that matches it.
// No missing read should be reported.
func TestSimulateSCPBundlePutInboxSatisfiedRead(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	reader := newTestAccount(t, baseState)

	sourceChain := uint64(321)
	sessionID := uint64(99)
	label := "inbox-fulfilled"

	message := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(sourceChain),
			DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
			Sender:        composeAddressFromCommon(reader.Address()),
			Receiver:      composeAddressFromCommon(reader.Address()),
			SessionID:     compose.SessionID(sessionID),
			Label:         label,
		},
		Data: []byte("payload"),
	}

	readTx := reader.SignReadTx(t, backend, 0, sourceChain, reader.Address(), sessionID, label)

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
	assertStateUnchanged(t, baseState, initialRoot)
}

// Tests a bundle with a putInbox message and a read that DOES NOT match it.
// A missing read should be reported.
func TestSimulateSCPBundlePutInboxDifferentRead(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	reader := newTestAccount(t, baseState)

	fulfilledMsg := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(400),
			DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
			Sender:        composeAddressFromCommon(reader.Address()),
			Receiver:      composeAddressFromCommon(reader.Address()),
			SessionID:     compose.SessionID(10),
			Label:         "fulfilled",
		},
		Data: []byte("payload"),
	}

	readTx := reader.SignReadTx(t, backend, 0, 401, reader.Address(), 11, "different")

	initialRoot := baseState.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		PutInboxMessages: []instanceproto.MailboxMessage{fulfilledMsg},
		Transactions:     [][]byte{readTx},
		Snapshot:         hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, baseState, header, request)
	requireMissingHeader(t, missing, missingExpectation{
		source:   401,
		dest:     backend.ChainConfig().ChainID.Uint64(),
		session:  11,
		label:    "different",
		receiver: reader.Address(),
	}, "expected missing mailbox header for unmatched read")
	if len(writes) != 0 {
		t.Fatalf("expected no outbound writes, got %d", len(writes))
	}
	assertStateUnchanged(t, baseState, initialRoot)
}

// Tests a bundle with two putInbox messages and two reads that match them,
// in varying orders. No missing read should be reported.
func TestSimulateSCPBundleMatchedReadsRegardlessOfOrder(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)

	remoteA := common.HexToAddress("0x7000000000000000000000000000000000000070")
	remoteB := common.HexToAddress("0x8000000000000000000000000000000000000080")

	buildMessages := func(state *state.StateDB) ([]instanceproto.MailboxMessage, [][]byte) {
		readerA := newTestAccount(t, state)
		readerB := newTestAccount(t, state)

		msgA := instanceproto.MailboxMessage{
			MailboxMessageHeader: instanceproto.MailboxMessageHeader{
				SourceChainID: compose.ChainID(501),
				DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
				Sender:        composeAddressFromCommon(remoteA),
				Receiver:      composeAddressFromCommon(readerA.Address()),
				SessionID:     compose.SessionID(41),
				Label:         "read-A",
			},
			Data: []byte("payload-A"),
		}
		msgB := instanceproto.MailboxMessage{
			MailboxMessageHeader: instanceproto.MailboxMessageHeader{
				SourceChainID: compose.ChainID(502),
				DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
				Sender:        composeAddressFromCommon(remoteB),
				Receiver:      composeAddressFromCommon(readerB.Address()),
				SessionID:     compose.SessionID(42),
				Label:         "read-B",
			},
			Data: []byte("payload-B"),
		}

		readATx := readerA.SignReadTx(t, backend, 0, 501, remoteA, 41, "read-A")
		readBTx := readerB.SignReadTx(t, backend, 0, 502, remoteB, 42, "read-B")

		return []instanceproto.MailboxMessage{msgA, msgB}, [][]byte{readATx, readBTx}
	}

	cases := []struct {
		name      string
		reorderMs func([]instanceproto.MailboxMessage) []instanceproto.MailboxMessage
		reorderTx func([][]byte) [][]byte
	}{
		{
			name: "matching order",
			reorderMs: func(msgs []instanceproto.MailboxMessage) []instanceproto.MailboxMessage {
				return msgs
			},
			reorderTx: func(txs [][]byte) [][]byte {
				return txs
			},
		},
		{
			name: "reads reverse order",
			reorderMs: func(msgs []instanceproto.MailboxMessage) []instanceproto.MailboxMessage {
				return msgs
			},
			reorderTx: func(txs [][]byte) [][]byte {
				return [][]byte{txs[1], txs[0]}
			},
		},
		{
			name: "putInbox reverse order",
			reorderMs: func(msgs []instanceproto.MailboxMessage) []instanceproto.MailboxMessage {
				return []instanceproto.MailboxMessage{msgs[1], msgs[0]}
			},
			reorderTx: func(txs [][]byte) [][]byte {
				return txs
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			state := stateFactory()
			msgs, txs := buildMessages(state)
			initialRoot := state.IntermediateRoot(false)
			request := instanceproto.SimulationRequest{
				PutInboxMessages: tc.reorderMs(msgs),
				Transactions:     tc.reorderTx(txs),
				Snapshot:         hashToComposeRoot(initialRoot),
			}

			missing, writes := runBundleSimulation(t, backend, state, header, request)
			requireNoMissing(t, missing, "unexpected missing mailbox header")
			if len(writes) != 0 {
				t.Fatalf("expected no outbound writes, got %d", len(writes))
			}
			assertStateUnchanged(t, state, initialRoot)
		})
	}
}

// Tests a bundle with a read that misses, followed by a write operation.
// The simulation should stop at the first read miss, and the write operation should not be applied.
func TestSimulateSCPBundleStopsBeforeWritesOnMissingRead(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	state := stateFactory()

	reader := newTestAccount(t, state)
	writer := newTestAccount(t, state)

	// PutInbox message that does not satisfy the first read
	putInboxMsg := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(777),
			DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
			Sender:        composeAddressFromCommon(common.HexToAddress("0x9100000000000000000000000000000000000091")),
			Receiver:      composeAddressFromCommon(reader.Address()),
			SessionID:     compose.SessionID(55),
			Label:         "read-A",
		},
		Data: []byte("payload-A"),
	}

	// Read that will miss
	readMissing := reader.SignReadTx(t, backend, 0, 888, common.HexToAddress("0x9200000000000000000000000000000000000092"), 10, "missing")

	// Write that should not be applied
	writeTx := writer.SignWriteTx(t, backend, 0, 999, common.HexToAddress("0x9300000000000000000000000000000000000093"), 66, "write-label", []byte("write-payload"))

	// Read that would be fulfilled if it were reached
	readFulfilled := reader.SignReadTx(t, backend, 1, 777, common.HexToAddress("0x9100000000000000000000000000000000000091"), 55, "read-A")

	// PutInbox (A). Transactions (ReadB, write, ReadA)
	// Expect to stop at ReadB missing. Neither write nor ReadA should be applied.
	initialRoot := state.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		PutInboxMessages: []instanceproto.MailboxMessage{putInboxMsg},
		Transactions:     [][]byte{readMissing, writeTx, readFulfilled},
		Snapshot:         hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, state, header, request)
	requireMissingHeader(t, missing, missingExpectation{
		source:  888,
		dest:    backend.ChainConfig().ChainID.Uint64(),
		session: 10,
		label:   "missing",
	}, "expected missing mailbox header from first read")
	if len(writes) != 0 {
		t.Fatalf("expected no writes when first tx fails, got %d", len(writes))
	}
	if nonce := state.GetNonce(writer.Address()); nonce != 0 {
		t.Fatalf("writer nonce mutated despite rollback")
	}
	assertStateUnchanged(t, state, initialRoot)
}

// Tests that a simulation with a snapshot that does not match the initial state root returns an error.
func TestSimulateSCPBundleSnapshotMismatchError(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	state := stateFactory()
	initialRoot := state.IntermediateRoot(false)

	var wrong compose.StateRoot
	copy(wrong[:], initialRoot[:])
	wrong[0] ^= 0xFF

	request := instanceproto.SimulationRequest{
		Snapshot: wrong,
	}

	_, _, err := runBundleSimulationWithError(t, backend, state, header, request)
	if err == nil {
		t.Fatalf("expected snapshot mismatch error")
	}
	if !strings.Contains(err.Error(), "state root mismatch") {
		t.Fatalf("unexpected error: %v", err)
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
		gasBudget.Mul(gasBudget, uint256.NewInt(10))
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

func runBundleSimulationWithError(t *testing.T, backend *testAPIBackend, stateDB *state.StateDB, header *types.Header, request instanceproto.SimulationRequest) (*instanceproto.MailboxMessageHeader, []instanceproto.MailboxMessage, error) {
	t.Helper()
	setStateOverrides(backend, stateDB, header)
	return backend.simulateSCPBundle(request)
}

const (
	testAccountFundingWei = 5_000_000_000_000_000
	defaultReadGasLimit   = 150_000
	defaultWriteGasLimit  = 200_000
)

type testAccount struct {
	key  *ecdsa.PrivateKey
	addr common.Address
}

func newTestAccount(t *testing.T, state *state.StateDB) testAccount {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	return fundTestAccount(t, state, key)
}

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

func encodeMailboxReadCall(t *testing.T, chainSrc uint64, sender common.Address, sessionID uint64, label string) []byte {
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
		[]byte(label),
	)
	if err != nil {
		t.Fatalf("pack read calldata: %v", err)
	}
	return data
}

func encodeMailboxWriteCall(t *testing.T, destChain uint64, receiver common.Address, sessionID uint64, label string, payload []byte) []byte {
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
		[]byte(label),
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
