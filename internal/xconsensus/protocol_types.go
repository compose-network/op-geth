package xconsensus

import (
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
)

const unknownString = "Unknown"

// MessageType represents SCP protocol message types
type MessageType int

const (
	MsgUnknown     MessageType = iota
	MsgXTRequest               // Cross-chain transaction request
	MsgVote                    // Sequencer vote
	MsgDecided                 // SP decision
	MsgCIRCMessage             // Inter-rollup communication
)

// String returns a human-readable message type name
func (t MessageType) String() string {
	switch t {
	case MsgUnknown:
		return unknownString
	case MsgXTRequest:
		return "XTRequest"
	case MsgVote:
		return "Vote"
	case MsgDecided:
		return "Decided"
	case MsgCIRCMessage:
		return "CIRCMessage"
	}
	// Fallback for unrecognized values
	return unknownString
}

// IsValid returns true if a message type is valid
func (t MessageType) IsValid() bool {
	return t > MsgUnknown && t <= MsgCIRCMessage
}

// ClassifyMessage returns an SCP message type from a protobuf message
func ClassifyMessage(msg *pb.Message) MessageType {
	if msg == nil || msg.Payload == nil {
		return MsgUnknown
	}

	switch msg.Payload.(type) {
	case *pb.Message_XtRequest:
		return MsgXTRequest
	case *pb.Message_Vote:
		return MsgVote
	case *pb.Message_Decided:
		return MsgDecided
	case *pb.Message_CircMessage:
		return MsgCIRCMessage
	default:
		return MsgUnknown
	}
}

// IsSCPMessage returns true if the message belongs to SCP protocol
func IsSCPMessage(msg *pb.Message) bool {
	return ClassifyMessage(msg) != MsgUnknown
}
