package xbootstrap

import (
	"fmt"

	"github.com/compose-network/specs/compose"
	"github.com/compose-network/specs/compose/sbcp"
)

type Messanger struct{}

func NewMessanger() sbcp.SequencerMessenger {
	return &Messanger{}
}

func (m *Messanger) SendProof(periodID compose.PeriodID, superblockNumber compose.SuperblockNumber, proof []byte) {
	fmt.Printf("Unimplemented: Sending proof for period %d, superblock %d: %x\n", periodID, superblockNumber, proof)
}

func (p *Messanger) ForwardRequest(request compose.XTRequest) {
	fmt.Printf("Unimplemented: Forwarding XT request: %+v\n", request)
}
