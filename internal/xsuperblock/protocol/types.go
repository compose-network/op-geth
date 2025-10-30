package protocol

import (
	"fmt"

	sbcpproto "github.com/compose-network/specs/compose/proto"
)

// MessageType represents SBCP protocol message types
type MessageType int

const (
	_ MessageType = iota
	StartPeriod
	Rollback
)

// String returns a human-readable message type name
func (t MessageType) String() string {
	names := map[MessageType]string{
		StartPeriod: "StartPeriod",
		Rollback:    "Rollback",
	}

	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", t)
}

// IsValid returns true if a message type is valid
func (t MessageType) IsValid() bool {
	switch t {
	case StartPeriod, Rollback:
		return true
	default:
		return false
	}
}

// ClassifyMessage returns SBCP message type from a protobuf message
func ClassifyMessage(msg *sbcpproto.Message) (MessageType, bool) {
	if msg == nil || msg.Payload == nil {
		return 0, false
	}

	switch msg.Payload.(type) {
	case *sbcpproto.Message_StartPeriod:
		return StartPeriod, true
	case *sbcpproto.Message_Rollback:
		return Rollback, true
	}

	return 0, false
}

// IsSBCPMessage returns true if the message belongs to SBCP protocol
func IsSBCPMessage(msg *sbcpproto.Message) bool {
	_, ok := ClassifyMessage(msg)
	return ok
}
