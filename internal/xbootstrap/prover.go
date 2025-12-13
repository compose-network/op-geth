package xbootstrap

import (
	"context"
	"fmt"

	"github.com/compose-network/specs/compose"
	"github.com/compose-network/specs/compose/sbcp"
)

type SimpleProver struct{}

func NewSimpleProver() sbcp.SequencerProver {
	return &SimpleProver{}
}

func (p *SimpleProver) RequestProofs(ctx context.Context, blockHeader *sbcp.BlockHeader, superblockNumber compose.SuperblockNumber) ([]byte, error) {
	if blockHeader == nil {
		fmt.Printf("No sealed block for superblock #%d\n", superblockNumber)
	}

	fmt.Printf("Unimplemented: Requesting proofs for block #%d (hash: %s) at superblock #%d\n",
		blockHeader.Number, blockHeader.BlockHash, superblockNumber)

	return nil, nil
}
