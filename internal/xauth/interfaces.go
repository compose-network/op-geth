package xauth

// Signer signs messages with ECDSA
type Signer interface {
	Sign(data []byte) ([]byte, error)
	PublicKeyBytes() []byte
	Address() string
}

// Verifier verifies ECDSA signatures
type Verifier interface {
	Verify(data, signature, publicKey []byte) error
	VerifyKnown(data, signature []byte) (string, error)
}

// Manager manages authentication and authorization
type Manager interface {
	// Sign signs data with the private key
	Sign(data []byte) (signature []byte, err error)

	// Verify verifies a signature against data and public key
	Verify(data, signature, publicKey []byte) error

	// VerifyKnown verifies signature from a known/trusted entity
	// Returns the ID of the signer if trusted, error otherwise
	VerifyKnown(data, signature []byte) (signerID string, err error)

	// RecoverPublicKey recovers public key from signature
	RecoverPublicKey(data, signature []byte) ([]byte, error)

	// AddTrustedKey adds a trusted public key with an identifier
	AddTrustedKey(id string, publicKey []byte) error

	// RemoveTrustedKey removes a trusted key
	RemoveTrustedKey(id string) error

	// IsTrusted checks if a public key is trusted
	IsTrusted(publicKey []byte) bool

	// PublicKeyBytes returns this manager's public key
	PublicKeyBytes() []byte

	// PublicKeyString returns this manager's public key as hex
	PublicKeyString() string

	// Address returns this manager's Ethereum address
	Address() string
}

// Config holds auth configuration
type Config struct {
	Enabled        bool              // Enable authentication
	PrivateKey     string            // Hex-encoded private key
	TrustedKeys    map[string]string // ID -> hex public key
	AllowUntrusted bool              // Allow connections from untrusted keys
	RequireAuth    bool              // Require all messages to be signed
}
