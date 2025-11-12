package eth

import (
	"testing"

	"math/big"

	rollupv1 "github.com/compose-network/publisher/proto/rollup/v1"
	"github.com/ethereum/go-ethereum/common"
)

func TestMatchCIRCToDependency_LabelAndReceiverMatch(t *testing.T) {
	dep := CrossRollupDependency{
		Receiver: common.HexToAddress("0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C"),
		Label:    []byte("SEND"),
	}

	circ := &rollupv1.CIRCMessage{
		Label:    "SEND",
		Receiver: [][]byte{common.HexToAddress("0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C").Bytes()},
		Data:     [][]byte{[]byte{0x01, 0x02}},
	}

	if !matchCIRCToDependency(dep, circ) {
		t.Fatalf("expected matchCIRCToDependency to return true for matching label/receiver")
	}
}

func TestMatchCIRCToDependency_LabelMismatch(t *testing.T) {
	dep := CrossRollupDependency{
		Receiver: common.HexToAddress("0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C"),
		Label:    []byte("SEND"),
	}

	circ := &rollupv1.CIRCMessage{
		Label:    "ACK SEND",
		Receiver: [][]byte{common.HexToAddress("0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C").Bytes()},
		Data:     [][]byte{[]byte("OK")},
	}

	if matchCIRCToDependency(dep, circ) {
		t.Fatalf("expected matchCIRCToDependency to return false for label mismatch")
	}
}

func TestMatchCIRCToDependency_ReceiverMismatch(t *testing.T) {
	dep := CrossRollupDependency{
		Receiver: common.HexToAddress("0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C"),
		Label:    []byte("SEND"),
	}

	circ := &rollupv1.CIRCMessage{
		Label:    "SEND",
		Receiver: [][]byte{common.HexToAddress("0x0000000000000000000000000000000000000001").Bytes()},
		Data:     [][]byte{[]byte{0x01, 0x02}},
	}

	if matchCIRCToDependency(dep, circ) {
		t.Fatalf("expected matchCIRCToDependency to return false for receiver mismatch")
	}
}

func TestMatchCIRCToDependency_SessionMatch(t *testing.T) {
	dep := CrossRollupDependency{
		Receiver:  common.HexToAddress("0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C"),
		Label:     []byte("SEND"),
		SessionID: big.NewInt(123456),
	}

	circ := &rollupv1.CIRCMessage{
		Label:     "SEND",
		Receiver:  [][]byte{dep.Receiver.Bytes()},
		SessionId: common.LeftPadBytes(dep.SessionID.Bytes(), 32),
		Data:      [][]byte{[]byte{0x01}},
	}

	if !matchCIRCToDependency(dep, circ) {
		t.Fatalf("expected match when session matches")
	}
}

func TestMatchCIRCToDependency_SessionMismatch(t *testing.T) {
	dep := CrossRollupDependency{
		Receiver:  common.HexToAddress("0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C"),
		Label:     []byte("SEND"),
		SessionID: big.NewInt(123456),
	}

	circ := &rollupv1.CIRCMessage{
		Label:     "SEND",
		Receiver:  [][]byte{dep.Receiver.Bytes()},
		SessionId: common.LeftPadBytes(big.NewInt(654321).Bytes(), 32),
		Data:      [][]byte{[]byte{0x01}},
	}

	if matchCIRCToDependency(dep, circ) {
		t.Fatalf("expected no match when session differs")
	}
}
