package sequencer

import (
	sbcpproto "github.com/compose-network/specs/compose/proto"
	"github.com/ethereum/go-ethereum/internal/xconsensus"
	"github.com/ethereum/go-ethereum/internal/xsuperblock/period"
)

// ProtocolType represents the high-level protocol classification
type ProtocolType int

const (
	ProtocolUnknown  ProtocolType = iota
	PeriodProtocol                // Superblock Construction Protocol
	InstanceProtocol              // Synchronous Composability Protocol
)

const unknownString = "Unknown"

// String returns human-readable protocol name
func (p ProtocolType) String() string {
	switch p {
	case ProtocolUnknown:
		return unknownString
	case PeriodProtocol:
		return "PERIOD"
	case InstanceProtocol:
		return "INSTANCE"
	}
	// Fallback for unrecognized values
	return unknownString
}

// IsValid returns true if a protocol type is valid
func (p ProtocolType) IsValid() bool {
	return p > ProtocolUnknown && p <= InstanceProtocol
}

// ClassifyProtocol determines which high-level protocol a message belongs to
func ClassifyProtocol(msg *sbcpproto.Message) ProtocolType {
	if msg == nil || msg.Payload == nil {
		return ProtocolUnknown
	}

	// Check SBCP first (superblock construction messages)
	if period.IsPeriodProtoMessage(msg) {
		return PeriodProtocol
	}

	// Check SCP (cross-chain consensus messages)
	if xconsensus.IsInstanceProtoMessage(msg) {
		return InstanceProtocol
	}

	return ProtocolUnknown
}

func LogMessageTypeString(msg *sbcpproto.Message) string {
	protocolType := ClassifyProtocol(msg)

	switch protocolType {
	case ProtocolUnknown:
		return unknownString
	case PeriodProtocol:
		msgType, ok := period.ClassifyMessage(msg)
		if !ok {
			return unknownString
		}

		return msgType.String()
	case InstanceProtocol:
		msgType := xconsensus.ClassifyMessage(msg)
		return msgType.String()
	}
	// Fallback for unrecognized values
	return unknownString
}
