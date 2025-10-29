package rollupv1

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// XtID generates a unique transaction ID by computing the SHA256 hash of the marshaled XTRequest object.
func (xt *XTRequest) XtID() (*XtID, error) {
	data, err := proto.Marshal(xt)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal XTRequest: %w", err)
	}

	hash := sha256.Sum256(data)

	return &XtID{Hash: hash[:]}, nil
}

// ChainIDs extracts all unique chain IDs from the transactions in the XTRequest.
// It returns them as a map[string]struct{} where the keys are the hexadecimal representations of the chain IDs.
func (xt *XTRequest) ChainIDs() map[string]struct{} {
	chains := make(map[string]struct{})
	for _, tx := range xt.Transactions {
		if tx != nil && len(tx.ChainId) > 0 {
			chainID := hex.EncodeToString(tx.ChainId)
			chains[chainID] = struct{}{}
		}
	}
	return chains
}

// Hex returns the hexadecimal string representation of the XtID's SHA256 hash.
// If XtID or its Hash is nil, it returns an empty string.
func (id *XtID) Hex() string {
	if id == nil || id.Hash == nil {
		return ""
	}

	return hex.EncodeToString(id.Hash)
}
