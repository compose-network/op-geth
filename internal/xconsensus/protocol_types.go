package xconsensus

import (
	sbcpproto "github.com/compose-network/specs/compose/proto"
)

const unknownString = "Unknown"

// MessageType represents SCP protocol message types
type MessageType int

const (
	MsgUnknown        MessageType = iota
	MsgStartInstance  MessageType = iota
	MsgDecided                    // SP decision
	MsgMailboxMessage             // Inter-rollup communication
)

// String returns a human-readable message type name
func (t MessageType) String() string {
	switch t {
	case MsgUnknown:
		return unknownString
	case MsgStartInstance:
		return "StartInstance"
	case MsgDecided:
		return "Decided"
	case MsgMailboxMessage:
		return "MailboxMessage"
	}
	// Fallback for unrecognized values
	return unknownString
}

// IsValid returns true if a message type is valid
func (t MessageType) IsValid() bool {
	return t > MsgUnknown && t <= MsgMailboxMessage
}

// ClassifyMessage returns an SCP message type from a protobuf message
func ClassifyMessage(msg *sbcpproto.Message) MessageType {
	if msg == nil || msg.Payload == nil {
		return MsgUnknown
	}

	switch msg.Payload.(type) {
	case *sbcpproto.Message_StartInstance:
		return MsgStartInstance
	case *sbcpproto.Message_Decided:
		return MsgDecided
	case *sbcpproto.Message_MailboxMessage:
		return MsgMailboxMessage
	default:
		return MsgUnknown
	}
}

// IsSCPMessage returns true if the message belongs to SCP protocol
func IsSCPMessage(msg *sbcpproto.Message) bool {
	return ClassifyMessage(msg) != MsgUnknown
}
