package xauth

import (
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/crypto"
)

// ECDSAVerifier implements signature verification
type ECDSAVerifier struct{}

// NewECDSAVerifier creates a new verifier
func NewECDSAVerifier() *ECDSAVerifier {
	return &ECDSAVerifier{}
}

// Verify checks if signature is valid
func (v *ECDSAVerifier) Verify(data, signature, publicKey []byte) error {
	if len(signature) != 65 {
		return fmt.Errorf("invalid signature length: %d", len(signature))
	}

	var pubKey *ecdsa.PublicKey
	var err error

	switch len(publicKey) {
	case 33:
		// Compressed public key
		pubKey, err = crypto.DecompressPubkey(publicKey)
		if err != nil {
			return fmt.Errorf("failed to decompress public key: %w", err)
		}
	case 65:
		// Uncompressed public key
		pubKey, err = crypto.UnmarshalPubkey(publicKey)
		if err != nil {
			return fmt.Errorf("failed to unmarshal public key: %w", err)
		}
	default:
		return fmt.Errorf("invalid public key length: %d", len(publicKey))
	}

	hash := crypto.Keccak256Hash(data)

	sigPublicKey, err := crypto.SigToPub(hash.Bytes(), signature)
	if err != nil {
		return fmt.Errorf("failed to recover public key: %w", err)
	}

	recoveredBytes := crypto.CompressPubkey(sigPublicKey)
	expectedBytes := crypto.CompressPubkey(pubKey)

	if !bytes.Equal(recoveredBytes, expectedBytes) {
		return errors.New("signature verification failed")
	}

	return nil
}

// RecoverPublicKey recovers the public key from signature
func (v *ECDSAVerifier) RecoverPublicKey(data, signature []byte) ([]byte, error) {
	if len(signature) != 65 {
		return nil, fmt.Errorf("invalid signature length: %d", len(signature))
	}

	hash := crypto.Keccak256Hash(data)
	pubKey, err := crypto.SigToPub(hash.Bytes(), signature)
	if err != nil {
		return nil, fmt.Errorf("failed to recover public key: %w", err)
	}

	return crypto.CompressPubkey(pubKey), nil
}
