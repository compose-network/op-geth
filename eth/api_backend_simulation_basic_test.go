package eth

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	"github.com/compose-network/specs/compose"
	instanceproto "github.com/compose-network/specs/compose/scp"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/ssv"
	"github.com/ethereum/go-ethereum/core/vm"
)

// Unit test for analyzeMailboxTrace function to ensure it correctly identifies missing reads.
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
	if len(writes) != 0 {
		t.Fatalf("expected no writes when read missing, got %d messages", len(writes))
	}
}

// Unit test for analyzeMailboxTrace function to ensure it respects fulfilled reads.
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

// Unit test for analyzeMailboxTrace function to ensure it correctly identifies writes.
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

// Unit test for buildPutInboxCalldata function to ensure it encodes correctly.
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
	//parsedABI, _ := abi.JSON(strings.NewReader(mailboxABI))
	parsedABI := mailboxABIParsed
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

// Unit test for buildReadHeader function to ensure it validates inputs.
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

// Unit test for buildWriteMessage function to ensure it validates inputs.
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

// Unit test for applyPutInboxMessage function to ensure it updates state correctly.
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
