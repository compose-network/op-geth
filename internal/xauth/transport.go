package xauth

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"time"
)

// CreateHandshakeRequest creates a handshake request with signature
func CreateHandshakeRequest(signer Manager, clientID string) (*pb.HandshakeRequest, error) {
	timestamp := time.Now().UnixNano()

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(timestamp)) //nolint: gosec // Safe

	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	dataToSign := make([]byte, 0, len(timestampBytes)+len(nonce))
	dataToSign = append(dataToSign, timestampBytes...)
	dataToSign = append(dataToSign, nonce...)
	signature, err := signer.Sign(dataToSign)
	if err != nil {
		return nil, fmt.Errorf("failed to sign handshake: %w", err)
	}

	return &pb.HandshakeRequest{
		Timestamp: timestamp,
		PublicKey: signer.PublicKeyBytes(),
		Signature: signature,
		ClientId:  clientID,
		Nonce:     nonce,
	}, nil
}

// VerifyHandshakeRequest verifies a handshake request
func VerifyHandshakeRequest(req *pb.HandshakeRequest, verifier Verifier, maxClockDrift time.Duration) error {
	reqTime := time.Unix(0, req.Timestamp)
	now := time.Now()

	if reqTime.After(now.Add(maxClockDrift)) {
		return fmt.Errorf("handshake timestamp too far in future")
	}

	if reqTime.Before(now.Add(-maxClockDrift)) {
		return fmt.Errorf("handshake timestamp too old")
	}

	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(req.Timestamp)) //nolint: gosec // Safe

	dataToVerify := make([]byte, 0, len(timestampBytes)+len(req.Nonce))
	dataToVerify = append(dataToVerify, timestampBytes...)
	dataToVerify = append(dataToVerify, req.Nonce...)

	return verifier.Verify(dataToVerify, req.Signature, req.PublicKey)
}
