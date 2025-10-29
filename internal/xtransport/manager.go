package xtransport

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// Manager handles connection lifecycle and health monitoring
type Manager struct {
	mu          sync.RWMutex
	connections map[string]Connection
	metrics     *Metrics
	log         zerolog.Logger

	// Health monitoring
	healthTicker *time.Ticker
	stopHealth   chan struct{}

	// Connection limits
	maxConnections int
	activeCount    int64
}

// NewManager creates a new connection manager
func NewManager(maxConnections int, log zerolog.Logger) *Manager {
	if maxConnections > 0x7FFFFFFF {
		maxConnections = 0x7FFFFFFF
	}

	return &Manager{
		connections:    make(map[string]Connection),
		metrics:        NewMetrics("connection_manager"),
		log:            log.With().Str("component", "connection-manager").Logger(),
		maxConnections: maxConnections,
		stopHealth:     make(chan struct{}),
	}
}

// Add registers a new connection
func (m *Manager) Add(conn Connection) error {
	if atomic.LoadInt64(&m.activeCount) >= int64(m.maxConnections) {
		return ErrConnectionLimit
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.connections[conn.ID()] = conn
	atomic.AddInt64(&m.activeCount, 1)

	m.metrics.RecordConnection("added")
	m.log.Debug().
		Str("conn_id", conn.ID()).
		Int("total", len(m.connections)).
		Msg("Connection added")

	return nil
}

// Remove unregisters a connection
func (m *Manager) Remove(connID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if conn, exists := m.connections[connID]; exists {
		delete(m.connections, connID)
		atomic.AddInt64(&m.activeCount, -1)

		info := conn.Info()
		duration := time.Since(info.ConnectedAt)

		m.metrics.RecordConnection("removed")
		m.metrics.RecordConnectionDuration(duration)

		m.log.Debug().
			Str("conn_id", connID).
			Dur("duration", duration).
			Uint64("bytes_read", info.BytesRead).
			Uint64("bytes_written", info.BytesWritten).
			Msg("Connection removed")
	}
}

// Get retrieves a connection by ID
func (m *Manager) Get(connID string) (Connection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conn, exists := m.connections[connID]
	return conn, exists
}

// GetAll returns all active connections
func (m *Manager) GetAll() []Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conns := make([]Connection, 0, len(m.connections))
	for _, conn := range m.connections {
		conns = append(conns, conn)
	}
	return conns
}

// GetInfo returns connection info for all connections
func (m *Manager) GetInfo() []ConnectionInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	infos := make([]ConnectionInfo, 0, len(m.connections))
	for _, conn := range m.connections {
		infos = append(infos, conn.Info())
	}
	return infos
}

// StartHealthMonitoring begins health monitoring of connections
func (m *Manager) StartHealthMonitoring(ctx context.Context, interval time.Duration) {
	m.healthTicker = time.NewTicker(interval)

	go func() {
		defer m.healthTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stopHealth:
				return
			case <-m.healthTicker.C:
				m.checkHealth()
			}
		}
	}()
}

// StopHealthMonitoring stops health monitoring
func (m *Manager) StopHealthMonitoring() {
	close(m.stopHealth)
}

// checkHealth monitors connection health and removes stale connections
func (m *Manager) checkHealth() {
	m.mu.RLock()
	staleConns := make([]string, 0)
	now := time.Now()

	for id, conn := range m.connections {
		info := conn.Info()
		if now.Sub(info.LastSeen) > 5*time.Minute {
			staleConns = append(staleConns, id)
		}
	}
	m.mu.RUnlock()

	// Remove stale connections
	for _, id := range staleConns {
		if conn, exists := m.Get(id); exists {
			conn.Close()
			m.Remove(id)
			m.log.Warn().Str("conn_id", id).Msg("Removed stale connection")
		}
	}
}

// Count returns the number of active connections
func (m *Manager) Count() int {
	return int(atomic.LoadInt64(&m.activeCount))
}

// Broadcast sends a message to all connections except excluded
func (m *Manager) Broadcast(ctx context.Context, data []byte, excludeID string) error {
	start := time.Now()
	conns := m.GetAll()
	if len(conns) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(conns))
	sent := int32(0)

	for _, conn := range conns {
		if conn.ID() == excludeID {
			continue
		}

		wg.Add(1)
		go func(c Connection) {
			defer wg.Done()

			if _, err := c.Write(data); err != nil {
				select {
				case errCh <- err:
				default:
				}
				m.log.Error().
					Str("conn_id", c.ID()).
					Err(err).
					Msg("Broadcast write error")
			} else {
				atomic.AddInt32(&sent, 1)
			}
		}(conn)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.metrics.RecordBroadcast(int(sent), time.Since(start))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

var ErrConnectionLimit = fmt.Errorf("connection limit reached")
