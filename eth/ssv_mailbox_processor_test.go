package eth

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	rollupv1 "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
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
