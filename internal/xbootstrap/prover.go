package xbootstrap

import (
	"fmt"

	"github.com/compose-network/specs/compose"
	"github.com/compose-network/specs/compose/sbcp"
)

type SimpleProver struct{}

func NewSimpleProver() sbcp.SequencerProver {
	return &SimpleProver{}
}

func (p *SimpleProver) RequestProofs(blockHeader *sbcp.BlockHeader, superblockNumber compose.SuperblockNumber) []byte {
	if blockHeader == nil {
		fmt.Printf("No sealed block for superblock #%d\n", superblockNumber)
	}

	fmt.Printf("Unimplemented: Requesting proofs for block #%d (hash: %s) at superblock #%d\n",
		blockHeader.Number, blockHeader.BlockHash, superblockNumber)

	return nil
}
