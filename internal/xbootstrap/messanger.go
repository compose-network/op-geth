package xbootstrap

import (
	"fmt"

	"github.com/compose-network/specs/compose"
	"github.com/compose-network/specs/compose/sbcp"
)

type SimpleMessanger struct{}

func NewSimpleMessanger() sbcp.SequencerMessenger {
	return &SimpleMessanger{}
}

func (sm *SimpleMessanger) ForwardRequest(request compose.XTRequest) {
	fmt.Printf("Unimplemented: Forwarding request %+v\n", request)
}

func (sm *SimpleMessanger) SendProof(periodID compose.PeriodID, superblockNumber compose.SuperblockNumber, proof []byte) {
	fmt.Printf("Unimplemented: Sending proof for superblock #%d in period #%d\n", superblockNumber, periodID)
}
