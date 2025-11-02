package xnetwork

import (
	"context"
	"encoding/hex"
	"sync"

	"github.com/compose-network/specs/compose"
	composeproto "github.com/compose-network/specs/compose/proto"
	instanceproto "github.com/compose-network/specs/compose/scp"
	"github.com/rs/zerolog"
)

// VoteSender is a function used to deliver votes to the shared publisher.
type VoteSender func(ctx context.Context, instanceID compose.InstanceID, chainID compose.ChainID, vote bool) error

// MailboxSender sends mailbox messages to peer sequencers. The destination peer is inferred by the destination chain ID.
type MailboxSender func(ctx context.Context, msg *composeproto.MailboxMessage) error

// Network provides an adapter between the spec-level SequencerNetwork interface and the transports
// used by the host environment (SP client + sequencer P2P).
type Network struct {
	mu            sync.RWMutex
	ctx           context.Context
	log           zerolog.Logger
	chainID       compose.ChainID
	voteSender    VoteSender
	mailboxSender MailboxSender
}

// Ensure Network satisfies the spec interface so it can be passed directly into the instance runner.
var _ instanceproto.SequencerNetwork = (*Network)(nil)

func NewSequencerNetwork(ctx context.Context, chainID compose.ChainID, log zerolog.Logger) *Network {
	if ctx == nil {
		ctx = context.Background()
	}

	return &Network{
		ctx:     ctx,
		log:     log.With().Str("component", "sequencer-network").Logger(),
		chainID: chainID,
	}
}

func (n *Network) SetVoteSender(sender VoteSender) {
	n.mu.Lock()
	n.voteSender = sender
	n.mu.Unlock()
}

func (n *Network) SetMailboxSender(sender MailboxSender) {
	n.mu.Lock()
	n.mailboxSender = sender
	n.mu.Unlock()
}

// ForInstance returns a SequencerNetwork scoped to the provided instance.
func (n *Network) ForInstance(instance compose.Instance) instanceproto.SequencerNetwork {
	return &instanceNetwork{
		parent:     n,
		instanceID: instance.ID,
	}
}

// SendMailboxMessage aligns to the SequencerNetwork interface, but raises an error as the base
// adapter is being used directly without first binding to an instance.
func (n *Network) SendMailboxMessage(_ compose.ChainID, _ instanceproto.MailboxMessage) {
	n.log.Error().Msg("sequencer network used without binding to an instance; message discarded")
}

// SendVote aligns to the SequencerNetwork interface, but raises an error as the base
// adapter is being used directly without first binding to an instance.
func (n *Network) SendVote(bool) {
	n.log.Error().Msg("sequencer network used without binding to an instance; vote discarded")
}

// instanceNetwork wraps the base adapter with instance-specific metadata.
type instanceNetwork struct {
	parent     *Network
	instanceID compose.InstanceID
}

var _ instanceproto.SequencerNetwork = (*instanceNetwork)(nil)

// SendMailboxMessage converts the spec mailbox message into protobuf and sends it
// with the mailbox transport.
func (in *instanceNetwork) SendMailboxMessage(recipient compose.ChainID, msg instanceproto.MailboxMessage) {
	pbMsg := convertMailboxMessage(in.instanceID, msg)

	if pbMsg.DestinationChain != uint64(recipient) {
		in.parent.log.Warn().
			Uint64("header_dest", pbMsg.DestinationChain).
			Uint64("arg_dest", uint64(recipient)).
			Str("instance_id", hex.EncodeToString(in.instanceID[:])).
			Msg("mailbox destination mismatch detected")
	}

	in.parent.dispatchMailbox(pbMsg)
}

// SendVote forwards the vote to the shared publisher.
func (in *instanceNetwork) SendVote(vote bool) {
	in.parent.dispatchVote(in.instanceID, vote)
}

func (n *Network) dispatchVote(instanceID compose.InstanceID, vote bool) {
	sender, ctx := n.getVoteSender()
	chainID := n.getChainID()
	if sender == nil {
		n.log.Error().
			Str("instance_id", hex.EncodeToString(instanceID[:])).
			Bool("vote", vote).
			Msg("vote sender not configured; vote dropped")
		return
	}

	if err := sender(ctx, instanceID, chainID, vote); err != nil {
		n.log.Error().
			Err(err).
			Str("instance_id", hex.EncodeToString(instanceID[:])).
			Uint64("chain_id", uint64(chainID)).
			Bool("vote", vote).
			Msg("failed to send vote")
	}
}

func (n *Network) dispatchMailbox(msg *composeproto.MailboxMessage) {
	sender, ctx := n.getMailboxSender()
	if sender == nil {
		n.log.Error().
			Str("instance_id", hex.EncodeToString(msg.GetInstanceId())).
			Msg("mailbox sender not configured; message dropped")
		return
	}

	if err := sender(ctx, msg); err != nil {
		n.log.Error().
			Err(err).
			Str("instance_id", hex.EncodeToString(msg.GetInstanceId())).
			Msg("failed to dispatch mailbox message")
	}
}

func (n *Network) getVoteSender() (VoteSender, context.Context) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.voteSender, n.ctx
}

func (n *Network) getMailboxSender() (MailboxSender, context.Context) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.mailboxSender, n.ctx
}

func (n *Network) getChainID() compose.ChainID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.chainID
}

func convertMailboxMessage(instanceID compose.InstanceID, msg instanceproto.MailboxMessage) *composeproto.MailboxMessage {
	pb := &composeproto.MailboxMessage{
		InstanceId:       append([]byte(nil), instanceID[:]...),
		SourceChain:      uint64(msg.SourceChainID),
		DestinationChain: uint64(msg.DestChainID),
		SessionId:        uint64(msg.SessionID),
		Label:            msg.Label,
	}

	if len(msg.Data) > 0 {
		pb.Data = [][]byte{append([]byte(nil), msg.Data...)}
	}

	pb.Source = addressToBytes(msg.Sender)
	pb.Receiver = addressToBytes(msg.Receiver)

	return pb
}

func addressToBytes(addr compose.EthAddress) []byte {
	out := make([]byte, len(addr))
	copy(out, addr[:])
	return out
}
