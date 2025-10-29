package xauth

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// ECDSASigner implements Signer using secp256k1
type ECDSASigner struct {
	privateKey *ecdsa.PrivateKey
	publicKey  []byte
	address    string
}

// NewECDSASigner creates a new signer from a private key
func NewECDSASigner(privateKey *ecdsa.PrivateKey) (*ECDSASigner, error) {
	if privateKey == nil {
		return nil, fmt.Errorf("private key cannot be nil")
	}

	publicKeyBytes := crypto.CompressPubkey(&privateKey.PublicKey)
	address := crypto.PubkeyToAddress(privateKey.PublicKey)

	return &ECDSASigner{
		privateKey: privateKey,
		publicKey:  publicKeyBytes,
		address:    address.Hex(),
	}, nil
}

// NewECDSASignerFromHex creates a signer from hex private key
func NewECDSASignerFromHex(hexKey string) (*ECDSASigner, error) {
	privateKey, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}
	return NewECDSASigner(privateKey)
}

// GenerateECDSASigner generates a new random signer
func GenerateECDSASigner() (*ECDSASigner, error) {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}
	return NewECDSASigner(privateKey)
}

// Sign signs data using ECDSA
func (s *ECDSASigner) Sign(data []byte) ([]byte, error) {
	hash := crypto.Keccak256Hash(data)
	signature, err := crypto.Sign(hash.Bytes(), s.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}
	return signature, nil
}

// PublicKeyBytes returns compressed public key
func (s *ECDSASigner) PublicKeyBytes() []byte {
	return s.publicKey
}

// Address returns Ethereum address
func (s *ECDSASigner) Address() string {
	return s.address
}

// PrivateKeyHex returns the private key as hex string
func (s *ECDSASigner) PrivateKeyHex() string {
	return common.Bytes2Hex(crypto.FromECDSA(s.privateKey))
}
