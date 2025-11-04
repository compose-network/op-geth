package eth

import (
	"strings"
	"testing"

	"github.com/compose-network/specs/compose"
	instanceproto "github.com/compose-network/specs/compose/scp"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
)

// Tests that the simulation of a bundle with no transactions returns an error
func TestSimulateSCPBundleWithNoTransaction(t *testing.T) {
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

	missing, writes, err := runBundleSimulationWithError(t, backend, baseState, header, request)
	// No transactions, so error is expected
	if err == nil {
		t.Fatalf("expected error for simulation with no transactions: %v", err)
	}
	// No read miss and writes should be reported
	requireNoMissing(t, missing, "expected no missing mailbox header")
	if len(writes) != 0 {
		t.Fatalf("expected no outbound mailbox messages, got %d", len(writes))
	}
	assertStateUnchanged(t, baseState, initialRoot)
}

// Tests that a read transaction with no corresponding inbox message is detected as missing
// Yes, note that we are using the read tx as the main one. Usually, another contract would call the read function.
// But, for testing purposes, it's pretty much the same to call the read directly.
// More general flows are tested in the PingPong tests.
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

// Tests that a write transaction results in an outbound mailbox message being recorded
func TestSimulateSCPBundleCollectsWrites(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	baseState := stateFactory()

	writer := newTestAccount(t, baseState)

	// Create write transaction
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

// Tests that, in a bundle with multiple reads, only the first missing read is reported
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

// Tests that multiple write transactions in a bundle are all recorded as outbound mailbox messages
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

// Tests for the case in which a tx raises an error (!= from read miss)
// We accomplish this using two identical write transactions.
// Due to the contract code, they have the same key and trying to write the second message will raise an error.
func TestSimulateSCPBundleDuplicateWritesError(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	state := stateFactory()

	// Create identical write transactions
	writer := newTestAccount(t, state)
	destChain := uint64(404)
	sessionID := uint64(1234)
	label := "duplicate-write"
	receiver := common.HexToAddress("0xdead00000000000000000000000000000000beef")
	payload := []byte("payload")

	writeOne := writer.SignWriteTx(t, backend, 0, destChain, receiver, sessionID, label, payload)
	writeTwo := writer.SignWriteTx(t, backend, 1, destChain, receiver, sessionID, label, payload)

	initialRoot := state.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{writeOne, writeTwo},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	missing, writes, err := runBundleSimulationWithError(t, backend, state, header, request)
	// Assert error
	if err == nil || !strings.Contains(err.Error(), "transaction 1 reverted") {
		t.Fatalf("expected duplicate write to revert, got missing=%v writes=%v err=%v", missing, writes, err)
	}
	// In case of error, no missing or writes should be reported
	if missing != nil {
		t.Fatalf("unexpected missing mailbox header: %+v", *missing)
	}
	if writes != nil {
		t.Fatalf("expected no outbound writes when bundle errors, got %d", len(writes))
	}
	assertStateUnchanged(t, state, initialRoot)
}

// Tests that a read transaction that is satisfied by a putInbox message results in no read miss
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

// Tests that a read transaction that does not match any putInbox message is still reported as missing
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

// Tests that multiple reads and putInbox messages are matched correctly regardless of order
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

// Tests a bundle with a {read miss, write, read fulfilled}.
// The output should be the first read miss only; the write transaction (2nd) shouldn't even be reached.
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

// Tests that a bundle with an incorrect snapshot root returns a snapshot mismatch error
func TestSimulateSCPBundleSnapshotMismatchError(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	state := stateFactory()
	initialRoot := state.IntermediateRoot(false)

	var wrong compose.StateRoot
	copy(wrong[:], initialRoot[:])
	wrong[0] ^= 0xFF

	// Create reader account (or contract)
	reader := newTestAccount(t, state)

	// Sample transaction
	payload := reader.SignReadTx(t, backend, 0, 12, reader.Address(), uint64(77), "read-test")

	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{payload},
		Snapshot:     wrong,
	}

	_, _, err := runBundleSimulationWithError(t, backend, state, header, request)
	if err == nil {
		t.Fatalf("expected snapshot mismatch error")
	}
	if !strings.Contains(err.Error(), "state root mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}
