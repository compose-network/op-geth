package sequencer

import (
	"github.com/ethereum/go-ethereum/internal/xconsensus"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/xsuperblock/protocol"
)

// ProtocolType represents the high-level protocol classification
type ProtocolType int

const (
	ProtocolUnknown ProtocolType = iota
	ProtocolSBCP                 // Superblock Construction Protocol
	ProtocolSCP                  // Synchronous Composability Protocol
)

const unknownString = "Unknown"

// String returns human-readable protocol name
func (p ProtocolType) String() string {
	switch p {
	case ProtocolUnknown:
		return unknownString
	case ProtocolSBCP:
		return "SBCP"
	case ProtocolSCP:
		return "SCP"
	}
	// Fallback for unrecognized values
	return unknownString
}

// IsValid returns true if a protocol type is valid
func (p ProtocolType) IsValid() bool {
	return p > ProtocolUnknown && p <= ProtocolSCP
}

// ClassifyProtocol determines which high-level protocol a message belongs to
func ClassifyProtocol(msg *pb.Message) ProtocolType {
	if msg == nil || msg.Payload == nil {
		return ProtocolUnknown
	}

	// Check SBCP first (superblock construction messages)
	if protocol.IsSBCPMessage(msg) {
		return ProtocolSBCP
	}

	// Check SCP (cross-chain consensus messages)
	if xconsensus.IsSCPMessage(msg) {
		return ProtocolSCP
	}

	return ProtocolUnknown
}

// GetMessageTypeString returns a formatted string for logging
func GetMessageTypeString(msg *pb.Message) string {
	protocolType := ClassifyProtocol(msg)

	switch protocolType {
	case ProtocolUnknown:
		return unknownString
	case ProtocolSBCP:
		msgType, ok := protocol.ClassifyMessage(msg)
		if !ok {
			return unknownString
		}

		return msgType.String()
	case ProtocolSCP:
		msgType := xconsensus.ClassifyMessage(msg)
		return msgType.String()
	}
	// Fallback for unrecognized values
	return unknownString
}

// IsProtocolMessage returns true if the message belongs to any known protocol
func IsProtocolMessage(msg *pb.Message) bool {
	return ClassifyProtocol(msg) != ProtocolUnknown
}
