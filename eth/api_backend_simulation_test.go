package eth

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	"github.com/compose-network/specs/compose"
	instanceproto "github.com/compose-network/specs/compose/scp"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/ssv"
	"github.com/ethereum/go-ethereum/core/vm"
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
