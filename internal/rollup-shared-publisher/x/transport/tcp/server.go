package tcp

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/auth"
	"github.com/google/uuid"
	"github.com/rs/zerolog"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/transport"
)

// ConnectionThrottle prevents rapid reconnection attacks by throttling per-IP connections
type ConnectionThrottle struct {
	mu          sync.RWMutex
	attempts    map[string]time.Time
	throttleDur time.Duration
	log         zerolog.Logger
}

// NewConnectionThrottle creates a new connection throttle with the specified throttle duration
func NewConnectionThrottle(throttleDur time.Duration, log zerolog.Logger) *ConnectionThrottle {
	if throttleDur <= 0 {
		throttleDur = 30 * time.Second
	}

	return &ConnectionThrottle{
		attempts:    make(map[string]time.Time),
		throttleDur: throttleDur,
		log:         log.With().Str("component", "throttle").Logger(),
	}
}

// Check verifies if the connection from the given IP should be allowed
func (ct *ConnectionThrottle) Check(remoteAddr string) error {
	ip := extractIP(remoteAddr)
	if ip == "" {
		return nil
	}

	ct.mu.Lock()
	defer ct.mu.Unlock()

	if lastAttempt, exists := ct.attempts[ip]; exists {
		if time.Since(lastAttempt) < ct.throttleDur {
			ct.log.Warn().
				Str("ip", ip).
				Dur("throttle", ct.throttleDur).
				Msg("Connection throttled")
			return fmt.Errorf("too many connection attempts from %s", ip)
		}
	}

	ct.attempts[ip] = time.Now()
	return nil
}

// cleanup removes expired throttle entries
func (ct *ConnectionThrottle) cleanup() {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	now := time.Now()
	for ip, lastAttempt := range ct.attempts {
		if now.Sub(lastAttempt) > ct.throttleDur*2 {
			delete(ct.attempts, ip)
		}
	}

	ct.log.Debug().
		Int("remaining_entries", len(ct.attempts)).
		Msg("Cleaned up throttle entries")
}

// StartCleanup starts periodic cleanup of expired throttle entries
func (ct *ConnectionThrottle) StartCleanup(ctx context.Context) {
	ticker := time.NewTicker(ct.throttleDur)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ct.cleanup()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// extractIP extracts the IP address from a net.Addr string
func extractIP(remoteAddr string) string {
	if idx := strings.LastIndex(remoteAddr, ":"); idx != -1 {
		return remoteAddr[:idx]
	}
	return remoteAddr
}

// Server implements high-performance TCP server
type Server struct {
	config      transport.Config
	manager     *transport.Manager
	codec       *Codec
	authManager auth.Manager
	log         zerolog.Logger
	handler     transport.ServerMessageHandler

	listener net.Listener
	running  atomic.Bool
	wg       sync.WaitGroup

	// Performance optimizations
	listenerBacklog int
	acceptQueue     chan net.Conn
	workerPool      *WorkerPool
	throttle        *ConnectionThrottle
}

func DefaultServerConfig() transport.Config {
	cfg := transport.DefaultConfig()
	cfg.MaxConnections = 1024
	return cfg
}

// NewServer creates a new TCP server
func NewServer(config transport.Config, log zerolog.Logger) transport.Server {
	manager := transport.NewManager(config.MaxConnections, log)

	return &Server{
		config:          config,
		manager:         manager,
		codec:           NewCodec(config.MaxMessageSize),
		log:             log.With().Str("component", "tcp-server").Logger(),
		listenerBacklog: 128,
		acceptQueue:     make(chan net.Conn, 64),
		workerPool:      NewWorkerPool(8, log),
		throttle:        NewConnectionThrottle(30*time.Second, log),
	}
}

// WithAuth adds authentication to the server
func (s *Server) WithAuth(authManager auth.Manager) *Server {
	s.authManager = authManager
	return s
}

// Start starts the TCP server
func (s *Server) Start(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("server already running")
	}

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", s.config.ListenAddr)
	if err != nil {
		s.running.Store(false)
		return fmt.Errorf("failed to listen on %s: %w", s.config.ListenAddr, err)
	}
	s.listener = listener

	s.workerPool.Start(ctx)
	s.manager.StartHealthMonitoring(ctx, 30*time.Second)
	s.throttle.StartCleanup(ctx)

	s.wg.Add(2)
	go s.acceptLoop(ctx)
	go s.connectionWorker(ctx)

	s.log.Info().
		Str("addr", s.config.ListenAddr).
		Int("max_connections", s.config.MaxConnections).
		Msg("TCP server started")

	return nil
}

// Listen starts listening on the specified address
func (s *Server) Listen(ctx context.Context, addr string) error {
	s.config.ListenAddr = addr
	return s.Start(ctx)
}

// Stop gracefully stops the server
func (s *Server) Stop(ctx context.Context) error {
	if !s.running.CompareAndSwap(true, false) {
		return fmt.Errorf("server not running")
	}

	s.log.Info().Msg("Stopping TCP server")

	// Close listener to unblock Accept
	if s.listener != nil {
		s.listener.Close()
	}

	s.manager.StopHealthMonitoring()
	close(s.acceptQueue)

	for _, conn := range s.manager.GetAll() {
		conn.Close()
	}

	s.workerPool.Stop()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.log.Info().Msg("TCP server stopped gracefully")
		return nil
	case <-ctx.Done():
		s.log.Warn().Msg("TCP server stop timeout")
		return ctx.Err()
	}
}

// acceptLoop accepts new connections
func (s *Server) acceptLoop(ctx context.Context) {
	defer s.wg.Done()

	for s.running.Load() {
		select {
		case <-ctx.Done():
			return
		default:
			// Set accept deadline
			if tcpListener, ok := s.listener.(*net.TCPListener); ok {
				tcpListener.SetDeadline(time.Now().Add(time.Second))
			}

			netConn, err := s.listener.Accept()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				if s.running.Load() {
					s.log.Error().Err(err).Msg("Accept error")
				}
				return
			}

			// Queue connection for processing
			select {
			case s.acceptQueue <- netConn:
			default:
				// Queue full, reject connection
				s.log.Warn().Msg("Accept queue full, closing connection")
				netConn.Close()
			}
		}
	}
}

// connectionWorker processes queued connections
func (s *Server) connectionWorker(ctx context.Context) {
	defer s.wg.Done()

	for s.running.Load() {
		select {
		case <-ctx.Done():
			return
		case netConn, ok := <-s.acceptQueue:
			if !ok {
				return
			}
			if !s.running.Load() {
				netConn.Close()
				continue
			}

			// Check connection limit
			if s.manager.Count() >= s.config.MaxConnections {
				s.log.Warn().
					Int("current", s.manager.Count()).
					Int("max", s.config.MaxConnections).
					Msg("Connection limit reached")
				netConn.Close()
				continue
			}

			s.setupConnection(netConn)
		}
	}
}

// setupConnection configures and registers a new connection
func (s *Server) setupConnection(netConn net.Conn) {
	remoteAddr := netConn.RemoteAddr().String()

	// Check throttle before processing connection
	if err := s.throttle.Check(remoteAddr); err != nil {
		s.log.Warn().
			Str("remote_addr", remoteAddr).
			Err(err).
			Msg("Connection throttled, rejecting")

		connID := uuid.New().String()
		conn := NewConnection(netConn, connID, s.codec, s.log)
		if impl, ok := conn.(*connection); ok {
			impl.CloseWithReason(pb.DisconnectMessage_THROTTLED, "too many connection attempts")
		} else {
			netConn.Close()
		}
		return
	}

	// Configure TCP options
	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(s.config.KeepAlive)
		if s.config.KeepAlive {
			tcpConn.SetKeepAlivePeriod(s.config.KeepAlivePeriod)
		}
		tcpConn.SetNoDelay(true)
	}

	// Create connection with same codec (includes auth if configured)
	connID := uuid.New().String()
	conn := NewConnection(netConn, connID, s.codec, s.log)

	if s.authManager != nil {
		if impl, ok := conn.(*connection); ok {
			if err := impl.HandleHandshake(s.authManager); err != nil {
				s.log.Error().Err(err).Msg("Handshake failed")
				impl.CloseWithReason(pb.DisconnectMessage_AUTH_FAILURE, err.Error())
				return
			}
		} else {
			s.log.Error().Msg("Unexpected connection implementation; closing")
			conn.Close()
			return
		}
	}

	// Register connection
	if err := s.manager.Add(conn); err != nil {
		s.log.Error().Err(err).Msg("Failed to register connection")
		if impl, ok := conn.(*connection); ok {
			impl.CloseWithReason(pb.DisconnectMessage_TOO_MANY_PEERS, "connection limit reached")
		} else {
			netConn.Close()
		}
		return
	}

	// Submit to worker pool
	s.workerPool.Submit(&ConnectionTask{
		conn:    conn,
		handler: s.handler,
		manager: s.manager,
		log:     s.log,
	})
}

// SetHandler sets the message handler
func (s *Server) SetHandler(handler transport.ServerMessageHandler) {
	s.handler = handler
}

// Broadcast sends a message to all clients except excluded
func (s *Server) Broadcast(ctx context.Context, msg *pb.Message, excludeID string) error {
	// Override sender ID
	msg.SenderId = "publisher"

	data, err := s.codec.EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	return s.manager.Broadcast(ctx, data, excludeID)
}

// Send sends a message to a specific client
func (s *Server) Send(ctx context.Context, clientID string, msg *pb.Message) error {
	conn, exists := s.manager.Get(clientID)
	if !exists {
		return fmt.Errorf("client %s not found", clientID)
	}

	return conn.WriteMessage(msg)
}

// GetConnections returns all active connections
func (s *Server) GetConnections() []transport.ConnectionInfo {
	return s.manager.GetInfo()
}
