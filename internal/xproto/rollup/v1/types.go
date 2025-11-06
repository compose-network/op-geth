package rollupv1

import (
	"encoding/hex"
)

// Hex returns the hexadecimal string representation of the XtID's SHA256 hash.
// If XtID or its Hash is nil, it returns an empty string.
func (id *XtID) Hex() string {
	if id == nil || id.Hash == nil {
		return ""
	}

	return hex.EncodeToString(id.Hash)
}
