package protocol

import (
	"fmt"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
)

// MessageType represents SBCP protocol message types
type MessageType int

// String returns a human-readable message type name
func (t MessageType) String() string {
	names := map[MessageType]string{}

	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", t)
}

// IsValid returns true if a message type is valid
func (t MessageType) IsValid() bool {
	return false
}

// ClassifyMessage returns SBCP message type from a protobuf message
func ClassifyMessage(msg *pb.Message) (MessageType, bool) {
	if msg == nil || msg.Payload == nil {
		return 0, false
	}

	return 0, false
}

// IsSBCPMessage returns true if the message belongs to SBCP protocol
func IsSBCPMessage(msg *pb.Message) bool {
	_, ok := ClassifyMessage(msg)
	return ok
}
