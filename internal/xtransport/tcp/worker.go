package tcp

import (
	"context"
	"errors"
	"fmt"
	sbcpproto "github.com/compose-network/specs/compose/proto"
	"github.com/ethereum/go-ethereum/internal/xtransport"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/rs/zerolog"
)

// WorkerPool manages connection handling workers
type WorkerPool struct {
	workers   int
	taskQueue chan Task
	wg        sync.WaitGroup
	log       zerolog.Logger
}

// Task represents work to be done by a worker
type Task interface {
	Execute(ctx context.Context)
}

// ConnectionTask handles a single connection
type ConnectionTask struct {
	conn    xtransport.Connection
	handler xtransport.ServerMessageHandler
	manager *xtransport.Manager
	log     zerolog.Logger
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workers int, log zerolog.Logger) *WorkerPool {
	return &WorkerPool{
		workers:   workers,
		taskQueue: make(chan Task, workers*2), // Buffer for queuing
		log:       log.With().Str("component", "worker-pool").Logger(),
	}
}

// Start starts the worker pool
func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}

	wp.log.Info().Int("workers", wp.workers).Msg("Worker pool started")
}

// Stop stops the worker pool
func (wp *WorkerPool) Stop() {
	close(wp.taskQueue)
	wp.wg.Wait()
	wp.log.Info().Msg("Worker pool stopped")
}

// Submit submits a task to the worker pool
func (wp *WorkerPool) Submit(task Task) {
	select {
	case wp.taskQueue <- task:
	default:
		wp.log.Warn().Msg("Worker pool task queue full")
	}
}

// worker processes tasks from the queue
func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()

	log := wp.log.With().Int("worker_id", id).Logger()
	log.Debug().Msg("Worker started")

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-wp.taskQueue:
			if !ok {
				return
			}

			task.Execute(ctx)
		}
	}
}

// Execute handles a connection lifecycle
func (ct *ConnectionTask) Execute(ctx context.Context) {
	log := ct.log.With().Str("conn_id", ct.conn.ID()).Logger()
	log.Info().Msg("Handling new connection")

	defer func() {
		ct.conn.Close()
		ct.manager.Remove(ct.conn.ID())
		log.Info().Msg("Connection closed")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := ct.conn.ReadMessage()
			if err != nil {
				if err == io.EOF ||
					errors.Is(err, net.ErrClosed) ||
					strings.Contains(err.Error(), "closed network connection") {
					log.Debug().Msg("Connection closed")
					return
				}
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					continue
				}
				if ctx.Err() != nil {
					log.Debug().Msg("Context canceled; stopping connection handler")
					return
				}
				log.Error().Err(err).Msg("Read error")
				return
			}

			// Handle ping/pong messages at transport level
			switch payload := msg.Payload.(type) {
			case *sbcpproto.Message_Ping:
				// Received ping, send pong
				log.Debug().Msg("Received ping from client, sending pong")
				pongMsg := &sbcpproto.Message{
					SenderId: "server",
					Payload: &sbcpproto.Message_Pong{
						Pong: &sbcpproto.Pong{
							Timestamp: payload.Ping.Timestamp,
						},
					},
				}
				if err := ct.conn.WriteMessage(pongMsg); err != nil {
					log.Debug().Err(err).Msg("Failed to send pong")
				}
				continue
			case *sbcpproto.Message_Pong:
				// Received pong, connection is alive
				log.Debug().Msg("Received pong from client")
				continue
			}

			if ct.handler != nil {
				from := ct.conn.ID()
				if authID := ct.conn.GetAuthenticatedID(); authID != "" {
					from = authID
				}

				log.Info().
					Str("from", from).
					Str("sender_id", msg.SenderId).
					Str("msg_type", fmt.Sprintf("%T", msg.Payload)).
					Msg("Received message")

				if err := ct.handler(ctx, from, msg); err != nil {
					log.Error().Err(err).Msg("Handler error")
				}
			}
		}
	}
}
