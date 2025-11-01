package xnetwork

import (
	"github.com/compose-network/specs/compose"
	instanceproto "github.com/compose-network/specs/compose/scp"
)

//
// type SequencerNetwork interface {
// 	SendMailboxMessage(recipient compose.ChainID, msg MailboxMessage)
// 	SendVote(vote bool)
// }

type sequencerNetwork struct {
}

func NewSequencerNetwork() instanceproto.SequencerNetwork {
	return &sequencerNetwork{}
}

func (sn *sequencerNetwork) SendMailboxMessage(recipient compose.ChainID, msg instanceproto.MailboxMessage) {
	return
}

func (sn *sequencerNetwork) SendVote(vote bool) {
	return
}
