package tcp

import (
	"context"
	"fmt"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	"github.com/ethereum/go-ethereum/internal/xauth"
	"github.com/ethereum/go-ethereum/internal/xtransport"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	pingInterval = 15 * time.Second
)

// ClientConfig contains client-specific configuration
type ClientConfig struct {
	ServerAddr      string
	ConnectTimeout  time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ReconnectDelay  time.Duration
	MaxMessageSize  int
	KeepAlive       bool
	KeepAlivePeriod time.Duration

	// Optional custom client identifier. If empty, a new uuid.NewV7() will be generated (fallback to uuid.New())
	ClientID string
}

// DefaultClientConfig returns sensible client defaults
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		ServerAddr:      "localhost:8080",
		ConnectTimeout:  10 * time.Second,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    10 * time.Second,
		ReconnectDelay:  5 * time.Second,
		MaxMessageSize:  10 * 1024 * 1024, // 10MB
		KeepAlive:       true,
		KeepAlivePeriod: 30 * time.Second,
	}
}

// client implements the xtransport.Client interface
type client struct {
	config      ClientConfig
	id          string
	handler     xtransport.ClientMessageHandler
	codec       *Codec
	authManager xauth.Manager // Optional authentication
	log         zerolog.Logger

	// Connection management
	conn          xtransport.Connection
	connected     atomic.Bool
	mu            sync.RWMutex
	reconnecting  atomic.Bool
	autoReconnect atomic.Bool

	// Ping/Pong for connection health
	pingRecv chan struct{}

	// Shutdown management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Metrics
	metrics *xtransport.Metrics
}

// NewClient creates a new TCP client instanceproto
func NewClient(config ClientConfig, log zerolog.Logger) xtransport.Client {
	id := config.ClientID
	if id == "" {
		if u, err := uuid.NewV7(); err == nil {
			id = u.String()
		} else {
			id = uuid.New().String()
		}
	}

	c := &client{
		config:   config,
		id:       id,
		codec:    NewCodec(config.MaxMessageSize),
		log:      log.With().Str("component", "tcp-client").Logger(),
		metrics:  xtransport.NewMetrics(id),
		pingRecv: make(chan struct{}, 16),
	}

	c.autoReconnect.Store(true)

	return c
}

// WithAuth adds authentication to the client
func (c *client) WithAuth(authManager xauth.Manager) xtransport.Client {
	c.authManager = authManager
	return c
}

// Connect establishes connection to the server
func (c *client) Connect(ctx context.Context, addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected.Load() {
		return fmt.Errorf("client already connected")
	}

	if addr != "" {
		c.config.ServerAddr = addr
	}

	if c.ctx == nil {
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}
	receiveCtx := c.ctx

	connCtx, cancel := context.WithTimeout(ctx, c.config.ConnectTimeout)
	defer cancel()

	dialer := &net.Dialer{
		Timeout:   c.config.ConnectTimeout,
		KeepAlive: c.config.KeepAlivePeriod,
	}

	netConn, err := dialer.DialContext(connCtx, "tcp", c.config.ServerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.config.ServerAddr, err)
	}

	// Configure TCP options
	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(c.config.KeepAlive)
		if c.config.KeepAlive {
			tcpConn.SetKeepAlivePeriod(c.config.KeepAlivePeriod)
		}
		tcpConn.SetNoDelay(true) // Low latency
	}

	// Create connection wrapper with same codec (includes auth if configured)
	c.conn = NewConnection(netConn, c.id, c.codec, c.log)

	if c.authManager != nil {
		if err := c.conn.PerformHandshake(c.authManager, c.id); err != nil {
			netConn.Close()
			return fmt.Errorf("handshake failed: %w", err)
		}
	}

	c.connected.Store(true)

	// Start receive loop
	c.wg.Add(1)
	go c.receiveLoop(receiveCtx)

	// Start ping loop for connection health monitoring
	c.wg.Add(1)
	go c.pingLoop(receiveCtx)

	// Start connection monitor for auto-reconnect
	c.wg.Add(1)
	go c.connectionMonitor(receiveCtx)

	c.metrics.RecordConnection("connected")

	logEntry := c.log.Info().
		Str("server", c.config.ServerAddr).
		Str("client_id", c.id)

	if c.authManager != nil {
		logEntry = logEntry.
			Str("address", c.authManager.Address()).
			Str("auth", "enabled")
	} else {
		logEntry = logEntry.Str("auth", "disabled")
	}

	logEntry.Msg("Connected to server")

	return nil
}

// Disconnect closes the connection
func (c *client) Disconnect(ctx context.Context) error {
	c.autoReconnect.Store(false)

	c.mu.Lock()
	if !c.connected.Load() {
		c.mu.Unlock()
		return fmt.Errorf("client not connected")
	}

	c.log.Info().Msg("Disconnecting from server")

	// Signal shutdown
	if c.cancel != nil {
		c.cancel()
		c.ctx = nil
		c.cancel = nil
	}

	// Close connection
	if c.conn != nil {
		c.conn.Close()
	}
	c.mu.Unlock() // Release lock BEFORE waiting for goroutines

	// Wait for goroutines with timeout
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.log.Info().Msg("Disconnected from server")
	case <-ctx.Done():
		c.log.Warn().Msg("Disconnect timeout")
		return ctx.Err()
	}

	c.connected.Store(false)
	c.metrics.RecordConnection("disconnected")
	return nil
}

// Send sends a message to the server
func (c *client) Send(ctx context.Context, msg *sbcpproto.Message) error {
	c.mu.RLock()
	conn := c.conn
	connected := c.connected.Load()
	c.mu.RUnlock()

	if !connected || conn == nil {
		return fmt.Errorf("client not connected")
	}

	// Set sender ID
	msg.SenderId = c.id

	// Set write timeout
	if c.config.WriteTimeout > 0 {
		deadline := time.Now().Add(c.config.WriteTimeout)
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("failed to set write deadline: %w", err)
		}
	}

	// Send message
	if err := conn.WriteMessage(msg); err != nil {
		c.log.Error().Err(err).Msg("Failed to send message")
		return fmt.Errorf("send failed: %w", err)
	}

	c.log.Debug().
		Str("msg_type", fmt.Sprintf("%T", msg.Payload)).
		Msg("Message sent")

	return nil
}

// SetHandler sets the message handler
func (c *client) SetHandler(handler xtransport.ClientMessageHandler) {
	c.handler = handler
}

// IsConnected returns connection status
func (c *client) IsConnected() bool {
	return c.connected.Load()
}

// GetID returns the client ID
func (c *client) GetID() string {
	return c.id
}

// receiveLoop reads messages from the server
//
// //nolint:gocyclo // under development
func (c *client) receiveLoop(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			c.log.Debug().Msg("Receive loop canceled")
			return
		default:
			// Set read timeout
			if c.config.ReadTimeout > 0 {
				deadline := time.Now().Add(c.config.ReadTimeout)
				if err := c.conn.SetReadDeadline(deadline); err != nil {
					c.handleConnectionLoss(err)
					return
				}
			}

			msg, err := c.conn.ReadMessage()
			if err != nil {
				if err == io.EOF {
					c.log.Debug().Msg("Server closed connection")
				} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
					if c.conn != nil {
						c.conn.UpdateLastSeen()
					}
					continue
				} else {
					c.log.Error().Err(err).Msg("Read error")
				}

				c.handleConnectionLoss(err)
				return
			}

			// Handle ping/pong messages
			switch msg.Payload.(type) {
			case *sbcpproto.Message_Ping:
				// Received ping, send pong
				c.log.Debug().Msg("Received ping, sending pong")
				select {
				case c.pingRecv <- struct{}{}:
				default:
				}
				continue
			case *sbcpproto.Message_Pong:
				// Received pong, connection is alive
				c.log.Debug().Msg("Received pong")
				continue
			}

			var verifiedID string
			if c.conn != nil {
				verifiedID = c.conn.GetAuthenticatedID()
			}

			c.log.Debug().
				Str("msg_type", fmt.Sprintf("%T", msg.Payload)).
				Str("sender_id", msg.SenderId).
				Str("verified_id", verifiedID).
				Msg("Message received")

			// Handle message
			if c.handler != nil {
				if _, err := c.handler(ctx, msg); err != nil {
					c.log.Error().
						Err(err).
						Str("msg_type", fmt.Sprintf("%T", msg.Payload)).
						Msg("Error handling message")
				}
			}
		}
	}
}

// pingLoop sends periodic pings to detect connection health
func (c *client) pingLoop(ctx context.Context) {
	defer c.wg.Done()

	ping := time.NewTimer(pingInterval)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			c.log.Debug().Msg("Ping loop canceled")
			return
		case <-ping.C:
			c.mu.RLock()
			conn := c.conn
			connected := c.connected.Load()
			c.mu.RUnlock()

			if !connected || conn == nil {
				return
			}

			// Send ping message
			pingMsg := &sbcpproto.Message{
				SenderId: c.id,
				Payload: &sbcpproto.Message_Ping{
					Ping: &sbcpproto.Ping{
						Timestamp: time.Now().UnixNano(),
					},
				},
			}

			if err := c.Send(ctx, pingMsg); err != nil {
				c.log.Error().Err(err).Msg("Failed to send ping")
				c.handleConnectionLoss(err)
				return
			}

			c.log.Debug().Msg("Ping sent")
			ping.Reset(pingInterval)

		case <-c.pingRecv:
			// Received ping from server, send pong
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				continue
			}

			pongMsg := &sbcpproto.Message{
				SenderId: c.id,
				Payload: &sbcpproto.Message_Pong{
					Pong: &sbcpproto.Pong{
						Timestamp: time.Now().UnixNano(),
					},
				},
			}

			if err := c.Send(ctx, pongMsg); err != nil {
				c.log.Debug().Err(err).Msg("Failed to send pong")
			}
		}
	}
}

// Reconnect attempts to reconnect to the server
func (c *client) Reconnect(ctx context.Context) error {
	c.log.Info().
		Dur("delay", c.config.ReconnectDelay).
		Msg("Attempting reconnect")

	// Disconnect first
	if err := c.Disconnect(ctx); err != nil {
		c.log.Warn().Err(err).Msg("Error during disconnect before reconnect")
	}

	// Wait before reconnecting
	select {
	case <-time.After(c.config.ReconnectDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := c.Connect(ctx, ""); err != nil {
		return err
	}

	c.autoReconnect.Store(true)
	return nil
}

// ConnectWithRetry connects with automatic retry logic using exponential backoff with jitter.
func (c *client) ConnectWithRetry(ctx context.Context, addr string, maxRetries int) error {
	var lastErr error
	delay := c.config.ReconnectDelay
	const maxDelay = time.Minute // Sensible max delay

	// For jitter. Using time.Now() is generally fine for this use case.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			jitter := time.Duration(rng.Int63n(int64(delay / 2)))
			waitTime := delay + jitter

			c.log.Info().
				Int("attempt", attempt+1).
				Int("max_retries", maxRetries+1).
				Dur("delay", waitTime).
				Msg("Retrying connection")

			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}

		if err := c.Connect(ctx, addr); err == nil {
			return nil
		} else {
			lastErr = err
			c.log.Warn().
				Err(err).
				Int("attempt", attempt+1).
				Msg("Connection failed")
		}
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", maxRetries+1, lastErr)
}

// GetStats returns client statistics
func (c *client) GetStats() map[string]interface{} {
	c.mu.RLock()
	connected := c.connected.Load()
	var connInfo xtransport.ConnectionInfo
	if c.conn != nil {
		connInfo = c.conn.Info()
	}
	c.mu.RUnlock()

	return map[string]interface{}{
		"client_id":     c.id,
		"connected":     connected,
		"server_addr":   c.config.ServerAddr,
		"bytes_read":    connInfo.BytesRead,
		"bytes_written": connInfo.BytesWritten,
		"connected_at":  connInfo.ConnectedAt,
		"last_seen":     connInfo.LastSeen,
	}
}

// SetReadTimeout sets the read timeout
func (c *client) SetReadTimeout(timeout time.Duration) {
	c.config.ReadTimeout = timeout
}

// SetWriteTimeout sets the write timeout
func (c *client) SetWriteTimeout(timeout time.Duration) {
	c.config.WriteTimeout = timeout
}

// SetReconnectDelay sets the reconnect delay
func (c *client) SetReconnectDelay(delay time.Duration) {
	c.config.ReconnectDelay = delay
}

// connectionMonitor monitors connection health and handles automatic reconnection
func (c *client) connectionMonitor(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.connected.Load() && c.autoReconnect.Load() {
				c.log.Info().Msg("Connection not active; scheduling reconnect")
				c.triggerReconnectNow()
			}
		}
	}
}

func (c *client) handleConnectionLoss(err error) {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

	wasConnected := c.connected.Swap(false)
	if wasConnected {
		c.metrics.RecordConnection("disconnected")
	}

	if !c.autoReconnect.Load() {
		c.log.Info().Err(err).Msg("Connection closed (auto-reconnect disabled)")
		return
	}

	c.log.Warn().Err(err).Msg("Connection to server lost")
	c.triggerReconnectNow()
}

func (c *client) triggerReconnectNow() {
	if !c.autoReconnect.Load() {
		return
	}
	if !c.reconnecting.CompareAndSwap(false, true) {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer c.reconnecting.Store(false)

		delay := c.config.ReconnectDelay
		if delay <= 0 {
			delay = time.Second
		}
		const maxDelay = time.Minute

		for c.autoReconnect.Load() {
			c.mu.RLock()
			ctx := c.ctx
			c.mu.RUnlock()

			if ctx == nil {
				c.log.Debug().Msg("Client context is nil, stopping reconnect loop")
				return
			}

			if err := c.Connect(ctx, ""); err == nil {
				c.log.Info().Msg("Successfully reconnected to server")
				return
			} else {
				if ctx.Err() != nil {
					c.log.Debug().Err(ctx.Err()).Msg("Reconnect stopped due to shutdown")
					return
				}
				c.log.Warn().
					Err(err).
					Dur("retry_in", delay).
					Msg("Reconnect attempt failed")
			}

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				c.log.Debug().Msg("Reconnect loop stopped during sleep")
				return
			}

			if delay < maxDelay {
				delay *= 2
				if delay > maxDelay {
					delay = maxDelay
				}
			}
		}
	}()
}
