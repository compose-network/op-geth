package xconsensus

import (
	"encoding/hex"
	"math/big"
)

// ChainKeyBytes converts a raw chain-id byte slice into the canonical
// hex-encoded key used internally by the consensus coordinator.
func ChainKeyBytes(id []byte) string {
	if len(id) == 0 {
		return ""
	}
	return hex.EncodeToString(id)
}

// ChainKeyUint64 converts a numeric chain-id into the canonical
// hex-encoded key by first encoding the number as minimal-length,
// big-endian bytes.
func ChainKeyUint64(id uint64) string {
	if id == 0 {
		return "00"
	}
	return hex.EncodeToString(new(big.Int).SetUint64(id).Bytes())
}
