package xauth

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// manager implements the Manager interface
type authManager struct {
	privateKey *ecdsa.PrivateKey
	publicKey  []byte
	address    string

	mu          sync.RWMutex
	trustedKeys map[string][]byte // ID -> public key
	keyToID     map[string]string // public key hex -> ID
}

// NewManager creates a new auth manager
func NewAuthManager(privateKey *ecdsa.PrivateKey) Manager {
	if privateKey == nil {
		return nil
	}

	publicKey := crypto.CompressPubkey(&privateKey.PublicKey)
	address := crypto.PubkeyToAddress(privateKey.PublicKey)

	return &authManager{
		privateKey:  privateKey,
		publicKey:   publicKey,
		address:     address.Hex(),
		trustedKeys: make(map[string][]byte),
		keyToID:     make(map[string]string),
	}
}

// NewManagerFromHex creates manager from hex private key
func NewManagerFromHex(hexKey string) (Manager, error) {
	if hexKey == "" {
		return nil, nil // No auth
	}

	privateKey, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	return NewAuthManager(privateKey), nil
}

// GenerateManager generates a new manager with random key
func GenerateManager() (Manager, error) {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	return NewAuthManager(privateKey), nil
}

// Sign signs data using ECDSA
func (m *authManager) Sign(data []byte) ([]byte, error) {
	hash := crypto.Keccak256Hash(data)
	signature, err := crypto.Sign(hash.Bytes(), m.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}
	return signature, nil
}

// Verify verifies a signature
func (m *authManager) Verify(data, signature, publicKey []byte) error {
	if len(signature) != 65 {
		return fmt.Errorf("invalid signature length: %d", len(signature))
	}

	// Recover public key from signature
	hash := crypto.Keccak256Hash(data)
	sigPublicKey, err := crypto.SigToPub(hash.Bytes(), signature)
	if err != nil {
		return fmt.Errorf("failed to recover public key: %w", err)
	}

	// Compare with provided public key
	var providedKey *ecdsa.PublicKey
	switch len(publicKey) {
	case 33:
		providedKey, err = crypto.DecompressPubkey(publicKey)
	case 65:
		providedKey, err = crypto.UnmarshalPubkey(publicKey)
	default:
		return fmt.Errorf("invalid public key length: %d", len(publicKey))
	}

	if err != nil {
		return fmt.Errorf("failed to parse public key: %w", err)
	}

	recoveredBytes := crypto.CompressPubkey(sigPublicKey)
	providedBytes := crypto.CompressPubkey(providedKey)

	if !bytes.Equal(recoveredBytes, providedBytes) {
		return errors.New("signature verification failed")
	}

	return nil
}

// VerifyKnown verifies signature from a known entity
func (m *authManager) VerifyKnown(data, signature []byte) (string, error) {
	publicKey, err := m.RecoverPublicKey(data, signature)
	if err != nil {
		return "", err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	keyHex := hex.EncodeToString(publicKey)
	id, exists := m.keyToID[keyHex]
	if !exists {
		return "", errors.New("unknown public key")
	}

	return id, nil
}

// RecoverPublicKey recovers public key from signature
func (m *authManager) RecoverPublicKey(data, signature []byte) ([]byte, error) {
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

// AddTrustedKey adds a trusted public key
func (m *authManager) AddTrustedKey(id string, publicKey []byte) error {
	if id == "" {
		return errors.New("empty ID")
	}

	// Normalize to compressed format
	var compressed []byte
	switch len(publicKey) {
	case 33:
		compressed = publicKey
	case 65:
		pubKey, err := crypto.UnmarshalPubkey(publicKey)
		if err != nil {
			return fmt.Errorf("invalid public key: %w", err)
		}
		compressed = crypto.CompressPubkey(pubKey)
	default:
		return fmt.Errorf("invalid public key length: %d", len(publicKey))
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.trustedKeys[id] = compressed
	m.keyToID[hex.EncodeToString(compressed)] = id

	return nil
}

// RemoveTrustedKey removes a trusted key
func (m *authManager) RemoveTrustedKey(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	publicKey, exists := m.trustedKeys[id]
	if !exists {
		return fmt.Errorf("key not found for ID: %s", id)
	}

	delete(m.trustedKeys, id)
	delete(m.keyToID, hex.EncodeToString(publicKey))

	return nil
}

// IsTrusted checks if a public key is trusted
func (m *authManager) IsTrusted(publicKey []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keyHex := hex.EncodeToString(publicKey)
	_, exists := m.keyToID[keyHex]
	return exists
}

// PublicKeyBytes returns public key
func (m *authManager) PublicKeyBytes() []byte {
	return m.publicKey
}

// PublicKeyString returns public key as hex
func (m *authManager) PublicKeyString() string {
	return hex.EncodeToString(m.publicKey)
}

// Address returns Ethereum address
func (m *authManager) Address() string {
	return m.address
}

// GetPrivateKeyHex returns private key as hex (for saving)
func GetPrivateKeyHex(m Manager) string {
	if mgr, ok := m.(*authManager); ok {
		return common.Bytes2Hex(crypto.FromECDSA(mgr.privateKey))
	}
	return ""
}
