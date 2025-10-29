package xtransport

import (
	metrics2 "github.com/ethereum/go-ethereum/internal/xmetrics"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds transport-level metrics
type Metrics struct {
	registry *metrics2.ComponentRegistry

	// Connection metrics
	ConnectionsTotal   *prometheus.CounterVec
	ConnectionsActive  prometheus.Gauge
	ConnectionDuration prometheus.Histogram

	// Message metrics
	MessagesTotal             *prometheus.CounterVec
	MessageSizeBytes          *prometheus.HistogramVec
	MessageProcessingDuration *prometheus.HistogramVec

	// Broadcast metrics
	BroadcastsTotal     prometheus.Counter
	BroadcastRecipients prometheus.Histogram
	BroadcastDuration   prometheus.Histogram

	// Performance metrics
	BufferUtilization   *prometheus.GaugeVec
	WorkerPoolQueueSize prometheus.Gauge
	ErrorsTotal         *prometheus.CounterVec
}

// NewMetrics creates transport metrics
func NewMetrics(suffix string) *Metrics {
	reg := metrics2.NewComponentRegistry("publisher", "transport_"+suffix)

	return &Metrics{
		registry: reg,

		ConnectionsTotal: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "connections_total",
			Help: "Total number of network connections",
		}, []string{"state"}),

		ConnectionsActive: reg.NewGauge(prometheus.GaugeOpts{
			Name: "connections_active",
			Help: "Number of active network connections",
		}),

		ConnectionDuration: reg.NewHistogram(prometheus.HistogramOpts{
			Name:    "connection_duration_seconds",
			Help:    "Duration of network connections",
			Buckets: metrics2.NetworkBuckets,
		}),

		MessagesTotal: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "messages_total",
			Help: "Total number of messages by type and direction",
		}, []string{"type", "direction"}),

		MessageSizeBytes: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "message_size_bytes",
			Help:    "Size of messages in bytes",
			Buckets: metrics2.SizeBuckets,
		}, []string{"type", "direction"}),

		MessageProcessingDuration: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "message_processing_duration_seconds",
			Help:    "Duration of message processing",
			Buckets: metrics2.DurationBuckets,
		}, []string{"type"}),

		BroadcastsTotal: reg.NewCounter(prometheus.CounterOpts{
			Name: "broadcasts_total",
			Help: "Total number of broadcast operations",
		}),

		BroadcastRecipients: reg.NewHistogram(prometheus.HistogramOpts{
			Name:    "broadcast_recipients_total",
			Help:    "Number of recipients per broadcast operation",
			Buckets: metrics2.CountBuckets,
		}),

		BroadcastDuration: reg.NewHistogram(prometheus.HistogramOpts{
			Name:    "broadcast_duration_seconds",
			Help:    "Duration of broadcast operations",
			Buckets: metrics2.DurationBuckets,
		}),

		BufferUtilization: reg.NewGaugeVec(prometheus.GaugeOpts{
			Name: "buffer_utilization_ratio",
			Help: "Buffer utilization ratio",
		}, []string{"type"}),

		WorkerPoolQueueSize: reg.NewGauge(prometheus.GaugeOpts{
			Name: "worker_pool_queue_size",
			Help: "Number of tasks in worker pool queue",
		}),

		ErrorsTotal: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "errors_total",
			Help: "Total number of transport errors",
		}, []string{"type", "operation"}),
	}
}

// RecordConnection records a connection event
func (m *Metrics) RecordConnection(state string) {
	m.ConnectionsTotal.WithLabelValues(state).Inc()

	switch state {
	case "added":
		m.ConnectionsActive.Inc()
	case "removed":
		m.ConnectionsActive.Dec()
	}
}

// RecordConnectionDuration records connection duration
func (m *Metrics) RecordConnectionDuration(duration time.Duration) {
	m.ConnectionDuration.Observe(duration.Seconds())
}

// RecordBroadcast records a broadcast operation
func (m *Metrics) RecordBroadcast(recipientCount int, duration time.Duration) {
	m.BroadcastsTotal.Inc()
	m.BroadcastRecipients.Observe(float64(recipientCount))
	m.BroadcastDuration.Observe(duration.Seconds())
}
