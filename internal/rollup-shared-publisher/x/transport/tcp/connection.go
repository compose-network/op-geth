package tcp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/auth"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/transport"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

// TimeoutConfig contains timeout settings for various connection operations
type TimeoutConfig struct {
	Handshake  time.Duration // Timeout for authentication handshake (default: 5s)
	Read       time.Duration // Timeout for read operations, also acts as idle timeout (default: 30s)
	Write      time.Duration // Timeout for write operations (default: 20s)
	Disconnect time.Duration // Expedited timeout for disconnect messages (default: 1s)
}

// DefaultTimeoutConfig returns production-ready timeout defaults
func DefaultTimeoutConfig() TimeoutConfig {
	return TimeoutConfig{
		Handshake:  5 * time.Second,
		Read:       30 * time.Second,
		Write:      20 * time.Second,
		Disconnect: 1 * time.Second,
	}
}

// connection implements transport.Connection with authentication
type connection struct {
	net.Conn
	id       string
	codec    *Codec
	log      zerolog.Logger
	timeouts TimeoutConfig

	// Authentication state
	mu              sync.RWMutex
	authenticated   bool
	authenticatedID string // Business identity after successful auth
	sessionID       string
	publicKey       []byte // Remote party's public key

	// Metadata
	info    transport.ConnectionInfo
	chainID string

	// Buffered I/O
	reader  *bufio.Reader
	writer  *bufio.Writer
	writeMu sync.Mutex

	// Metrics
	bytesRead    uint64
	bytesWritten uint64
}

// NewConnection creates a new connection wrapper
func NewConnection(netConn net.Conn, id string, codec *Codec, log zerolog.Logger) transport.Connection {
	return NewConnectionWithTimeouts(netConn, id, codec, log, DefaultTimeoutConfig())
}

// NewConnectionWithTimeouts creates a new connection wrapper with custom timeout configuration
func NewConnectionWithTimeouts(
	netConn net.Conn, id string, codec *Codec, log zerolog.Logger, timeouts TimeoutConfig,
) transport.Connection {
	now := time.Now()

	return &connection{
		Conn:     netConn,
		id:       id,
		codec:    codec,
		log:      log.With().Str("conn_id", id).Logger(),
		timeouts: timeouts,
		reader:   bufio.NewReaderSize(netConn, 16384),
		writer:   bufio.NewWriterSize(netConn, 16384),
		info: transport.ConnectionInfo{
			ID:          id,
			RemoteAddr:  netConn.RemoteAddr().String(),
			ConnectedAt: now,
			LastSeen:    now,
		},
	}
}

// PerformHandshake performs client-side authentication handshake
func (c *connection) PerformHandshake(signer auth.Manager, clientID string) error {
	req, err := auth.CreateHandshakeRequest(signer, clientID)
	if err != nil {
		return fmt.Errorf("failed to create handshake: %w", err)
	}

	if err := c.writeRawMessage(req); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}

	var resp pb.HandshakeResponse
	if err := c.readRawMessage(&resp); err != nil {
		return fmt.Errorf("failed to read handshake response: %w", err)
	}

	if !resp.Accepted {
		return fmt.Errorf("handshake rejected: %s", resp.Error)
	}

	c.mu.Lock()
	c.authenticated = true
	c.authenticatedID = clientID
	c.sessionID = resp.SessionId
	c.publicKey = signer.PublicKeyBytes()
	c.mu.Unlock()

	c.log.Info().
		Str("session_id", resp.SessionId).
		Msg("Handshake successful")

	return nil
}

// HandleHandshake handles server-side authentication handshake
func (c *connection) HandleHandshake(authManager auth.Manager) error {
	if err := c.SetReadDeadline(time.Now().Add(c.timeouts.Handshake)); err != nil {
		return err
	}
	defer c.SetReadDeadline(time.Time{})

	var req pb.HandshakeRequest
	if err := c.readRawMessage(&req); err != nil {
		return fmt.Errorf("failed to read handshake: %w", err)
	}

	maxClockDrift := 30 * time.Second
	if err := auth.VerifyHandshakeRequest(&req, authManager, maxClockDrift); err != nil {
		resp := &pb.HandshakeResponse{
			Accepted: false,
			Error:    err.Error(),
		}
		c.writeRawMessage(resp)
		return fmt.Errorf("handshake verification failed: %w", err)
	}

	// Check if public key is trusted - REJECT if not trusted
	if !authManager.IsTrusted(req.PublicKey) {
		resp := &pb.HandshakeResponse{
			Accepted: false,
			Error:    "untrusted public key - connection rejected",
		}
		c.writeRawMessage(resp)
		return fmt.Errorf("untrusted public key: %x", req.PublicKey[:8])
	}

	// Get the trusted identity by recreating signed data and verifying
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(req.Timestamp)) //nolint: gosec // Safe
	// Build signedData without reusing backing arrays
	signedData := make([]byte, 0, len(timestampBytes)+len(req.Nonce))
	signedData = append(signedData, timestampBytes...)
	signedData = append(signedData, req.Nonce...)

	verifiedID, err := authManager.VerifyKnown(signedData, req.Signature)
	if err != nil {
		c.log.Warn().Err(err).Msg("VerifyKnown failed for trusted key")
		verifiedID = fmt.Sprintf("trusted:%x", req.PublicKey[:8])
	}

	sessionID := fmt.Sprintf("%s-%d", verifiedID, time.Now().UnixNano())

	resp := &pb.HandshakeResponse{
		Accepted:  true,
		SessionId: sessionID,
	}

	if err := c.writeRawMessage(resp); err != nil {
		return fmt.Errorf("failed to send handshake response: %w", err)
	}

	c.mu.Lock()
	c.authenticated = true
	c.authenticatedID = verifiedID
	c.sessionID = sessionID
	c.publicKey = req.PublicKey
	c.mu.Unlock()

	c.log.Info().
		Str("session_id", sessionID).
		Str("client_id", req.ClientId).
		Msg("Client authenticated")

	return nil
}

// IsAuthenticated returns whether the connection is authenticated
func (c *connection) IsAuthenticated() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.authenticated
}

// GetAuthenticatedID returns the authenticated identity, empty if not authenticated
func (c *connection) GetAuthenticatedID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.authenticatedID
}

// GetSessionID returns the session ID
func (c *connection) GetSessionID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessionID
}

// ReadMessage reads a protobuf message
func (c *connection) ReadMessage() (*pb.Message, error) {
	if c.timeouts.Read > 0 {
		if err := c.SetReadDeadline(time.Now().Add(c.timeouts.Read)); err != nil {
			return nil, fmt.Errorf("failed to set read deadline: %w", err)
		}
	}

	var msg pb.Message
	if err := c.codec.ReadMessage(c.reader, &msg); err != nil {
		return nil, err
	}

	c.UpdateLastSeen()
	atomic.AddUint64(&c.bytesRead, 1)

	return &msg, nil
}

// WriteMessage writes a protobuf message
func (c *connection) WriteMessage(msg *pb.Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.timeouts.Write > 0 {
		if err := c.SetWriteDeadline(time.Now().Add(c.timeouts.Write)); err != nil {
			return fmt.Errorf("failed to set write deadline: %w", err)
		}
	}

	if err := c.codec.WriteMessage(c.writer, msg); err != nil {
		return err
	}

	if err := c.writer.Flush(); err != nil {
		return err
	}

	atomic.AddUint64(&c.bytesWritten, 1)
	return nil
}

// writeRawMessage writes any protobuf message (for handshake)
func (c *connection) writeRawMessage(msg proto.Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.codec.WriteMessage(c.writer, msg); err != nil {
		return err
	}

	return c.writer.Flush()
}

// readRawMessage reads any protobuf message.
func (c *connection) readRawMessage(msg proto.Message) error {
	return c.codec.ReadMessage(c.reader, msg)
}

// ID returns the connection ID.
func (c *connection) ID() string {
	return c.id
}

// Info returns connection information.
func (c *connection) Info() transport.ConnectionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info := c.info
	info.ChainID = c.chainID
	info.BytesRead = atomic.LoadUint64(&c.bytesRead)
	info.BytesWritten = atomic.LoadUint64(&c.bytesWritten)

	return info
}

// UpdateLastSeen updates the last seen timestamp.
func (c *connection) UpdateLastSeen() {
	c.mu.Lock()
	c.info.LastSeen = time.Now()
	c.mu.Unlock()
}

// SetChainID sets the chain ID for this connection.
func (c *connection) SetChainID(chainID string) {
	c.mu.Lock()
	c.chainID = chainID
	c.mu.Unlock()
}

// CloseWithReason sends a disconnect message with reason before closing the connection
func (c *connection) CloseWithReason(reason pb.DisconnectMessage_Reason, details string) error {
	c.log.Info().
		Str("reason", reason.String()).
		Str("details", details).
		Msg("Closing connection with reason")

	msg := &pb.Message{
		SenderId: c.id,
		Payload: &pb.Message_Disconnect{
			Disconnect: &pb.DisconnectMessage{
				Reason:  reason,
				Details: details,
			},
		},
	}

	c.writeMu.Lock()
	if c.timeouts.Disconnect > 0 {
		if err := c.SetWriteDeadline(time.Now().Add(c.timeouts.Disconnect)); err != nil {
			c.writeMu.Unlock()
			c.log.Warn().Err(err).Msg("Failed to set disconnect deadline")
			return c.Close()
		}
	}

	if err := c.codec.WriteMessage(c.writer, msg); err != nil {
		c.writeMu.Unlock()
		c.log.Warn().Err(err).Msg("Failed to send disconnect message")
		return c.Close()
	}

	if err := c.writer.Flush(); err != nil {
		c.writeMu.Unlock()
		c.log.Warn().Err(err).Msg("Failed to flush disconnect message")
		return c.Close()
	}
	c.writeMu.Unlock()

	return c.Close()
}
