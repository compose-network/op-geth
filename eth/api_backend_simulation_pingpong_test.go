package eth

import (
	"strings"
	"testing"

	"github.com/compose-network/specs/compose"
	instanceproto "github.com/compose-network/specs/compose/scp"
	"github.com/ethereum/go-ethereum/common"
)

// Tests a ping tx which should raise a "PONG" read miss and a "PING" write.
func TestSimulateSCPBundlePingMissingPong(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	state := stateFactory()

	caller := newTestAccount(t, state)
	otherChain := uint64(777)
	sessionID := uint64(99)
	pongSender := common.HexToAddress("0x7100000000000000000000000000000000000071")
	pingReceiver := common.HexToAddress("0x7200000000000000000000000000000000000072")
	payload := []byte("ping-data")

	tx := caller.SignPingTx(t, backend, 0, otherChain, pongSender, pingReceiver, sessionID, payload)

	initialRoot := state.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{tx},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, state, header, request)
	requireMissingHeader(t, missing, missingExpectation{
		source:   otherChain,
		dest:     backend.ChainConfig().ChainID.Uint64(),
		session:  sessionID,
		label:    "PONG",
		receiver: pingPongContractAddr,
	}, "expected missing pong reply")
	if len(writes) != 1 {
		t.Fatalf("expected ping write to be recorded, got %d", len(writes))
	}
	requireWriteMessage(t, writes[0], writeExpectation{
		source:   backend.ChainConfig().ChainID.Uint64(),
		dest:     otherChain,
		session:  sessionID,
		label:    "PING",
		data:     payload,
		receiver: pingReceiver,
	}, "unexpected ping write")
	assertStateUnchanged(t, state, initialRoot)
}

// Tests a pong tx which should raise a "PING" read miss and no writes.
func TestSimulateSCPBundlePongMissingPing(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	state := stateFactory()

	caller := newTestAccount(t, state)
	otherChain := uint64(888)
	sessionID := uint64(11)
	pingSender := common.HexToAddress("0x7300000000000000000000000000000000000073")
	payload := []byte("pong-data")

	tx := caller.SignPongTx(t, backend, 0, otherChain, pingSender, sessionID, payload)

	initialRoot := state.IntermediateRoot(false)
	request := instanceproto.SimulationRequest{
		Transactions: [][]byte{tx},
		Snapshot:     hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, state, header, request)
	requireMissingHeader(t, missing, missingExpectation{
		source:   otherChain,
		dest:     backend.ChainConfig().ChainID.Uint64(),
		session:  sessionID,
		label:    "PING",
		receiver: pingPongContractAddr,
	}, "expected missing ping message")
	if len(writes) != 0 {
		t.Fatalf("expected no writes before missing, got %d", len(writes))
	}
	assertStateUnchanged(t, state, initialRoot)
}

// Tests a ping tx with an inbound pong message which should satisfy the read miss and record a ping write.
func TestSimulateSCPBundlePingSatisfiedByPutInbox(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	state := stateFactory()

	caller := newTestAccount(t, state)
	otherChain := uint64(889)
	sessionID := uint64(12)
	pongSender := common.HexToAddress("0x7400000000000000000000000000000000000074")
	pingReceiver := common.HexToAddress("0x7500000000000000000000000000000000000075")
	payload := []byte("ping-to-remote")
	reply := []byte("pong-reply")

	tx := caller.SignPingTx(t, backend, 0, otherChain, pongSender, pingReceiver, sessionID, payload)

	initialRoot := state.IntermediateRoot(false)
	message := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(otherChain),
			DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
			Sender:        composeAddressFromCommon(pongSender),
			Receiver:      composeAddressFromCommon(pingPongContractAddr),
			SessionID:     compose.SessionID(sessionID),
			Label:         "PONG",
		},
		Data: reply,
	}
	request := instanceproto.SimulationRequest{
		PutInboxMessages: []instanceproto.MailboxMessage{message},
		Transactions:     [][]byte{tx},
		Snapshot:         hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, state, header, request)
	requireNoMissing(t, missing, "expected pong reply to satisfy ping")
	if len(writes) != 1 {
		t.Fatalf("expected ping write when pong satisfied, got %d", len(writes))
	}
	requireWriteMessage(t, writes[0], writeExpectation{
		source:   backend.ChainConfig().ChainID.Uint64(),
		dest:     otherChain,
		session:  sessionID,
		label:    "PING",
		data:     payload,
		receiver: pingReceiver,
	}, "unexpected ping write when satisfied")
	assertStateUnchanged(t, state, initialRoot)
}

// Tests a pong tx with an inbound ping message which should satisfy the read miss and record a pong write.
func TestSimulateSCPBundlePongSatisfiedByPutInbox(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	state := stateFactory()

	caller := newTestAccount(t, state)
	otherChain := uint64(890)
	sessionID := uint64(13)
	pingSender := common.HexToAddress("0x7600000000000000000000000000000000000076")
	payload := []byte("pong-payload")
	pingPayload := []byte("ping-from-remote")

	tx := caller.SignPongTx(t, backend, 0, otherChain, pingSender, sessionID, payload)

	initialRoot := state.IntermediateRoot(false)
	message := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(otherChain),
			DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
			Sender:        composeAddressFromCommon(pingSender),
			Receiver:      composeAddressFromCommon(pingPongContractAddr),
			SessionID:     compose.SessionID(sessionID),
			Label:         "PING",
		},
		Data: pingPayload,
	}
	request := instanceproto.SimulationRequest{
		PutInboxMessages: []instanceproto.MailboxMessage{message},
		Transactions:     [][]byte{tx},
		Snapshot:         hashToComposeRoot(initialRoot),
	}

	missing, writes := runBundleSimulation(t, backend, state, header, request)
	requireNoMissing(t, missing, "expected inbound ping to satisfy pong")
	if len(writes) != 1 {
		t.Fatalf("expected single pong write, got %d", len(writes))
	}
	requireWriteMessage(t, writes[0], writeExpectation{
		source:   backend.ChainConfig().ChainID.Uint64(),
		dest:     otherChain,
		session:  sessionID,
		label:    "PONG",
		data:     payload,
		receiver: pingPongContractAddr,
	}, "unexpected pong write when satisfied")
	assertStateUnchanged(t, state, initialRoot)
}

// Test case for transaction failure (!= from read miss)
// For that, we use: one "PONG" inbound msg, and two identical "PING" txs
// The first is satisfied.
// The second raises an error in its write operation because the message key is the same.
func TestSimulateSCPBundleDuplicatePingWriteError(t *testing.T) {
	backend, stateFactory, _, header := setupSimulationTestBackend(t)
	state := stateFactory()

	caller := newTestAccount(t, state)
	otherChain := uint64(891)
	sessionID := uint64(14)
	pongSender := common.HexToAddress("0x7700000000000000000000000000000000000077")
	pingReceiver := common.HexToAddress("0x7800000000000000000000000000000000000078")
	payload := []byte("ping-dup")

	txA := caller.SignPingTx(t, backend, 0, otherChain, pongSender, pingReceiver, sessionID, payload)
	txB := caller.SignPingTx(t, backend, 1, otherChain, pongSender, pingReceiver, sessionID, payload)

	initialRoot := state.IntermediateRoot(false)
	reply := instanceproto.MailboxMessage{
		MailboxMessageHeader: instanceproto.MailboxMessageHeader{
			SourceChainID: compose.ChainID(otherChain),
			DestChainID:   compose.ChainID(backend.ChainConfig().ChainID.Uint64()),
			Sender:        composeAddressFromCommon(pongSender),
			Receiver:      composeAddressFromCommon(pingPongContractAddr),
			SessionID:     compose.SessionID(sessionID),
			Label:         "PONG",
		},
		Data: []byte("pong-response"),
	}
	request := instanceproto.SimulationRequest{
		PutInboxMessages: []instanceproto.MailboxMessage{reply},
		Transactions:     [][]byte{txA, txB},
		Snapshot:         hashToComposeRoot(initialRoot),
	}

	missing, writes, err := runBundleSimulationWithError(t, backend, state, header, request)
	if err == nil || !strings.Contains(err.Error(), "transaction 1 reverted") {
		t.Fatalf("expected second ping to revert, got missing=%v writes=%v err=%v", missing, writes, err)
	}
	requireNoMissing(t, missing, "expected no missing header when pong provided")
	if writes != nil {
		t.Fatalf("expected no writes returned after revert, got %d", len(writes))
	}
	assertStateUnchanged(t, state, initialRoot)
}
