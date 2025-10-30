package xconsensus

import (
	"github.com/ethereum/go-ethereum/internal/xmetrics"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricsRecorder defines the interface for recording consensus metrics
type MetricsRecorder interface {
	RecordTransactionStarted(participantCount int)
	RecordTransactionCompleted(state string, duration time.Duration)
	RecordVote(chainID string, vote bool, latency time.Duration)
	RecordTimeout()
	RecordDecisionBroadcast(decision bool)
	RecordVoteBroadcast(vote bool)
}

// Metrics holds all consensus-level metrics
type Metrics struct {
	registry *xmetrics.ComponentRegistry

	TransactionsTotal          *prometheus.CounterVec
	ActiveTransactions         prometheus.Gauge
	Duration                   *prometheus.HistogramVec
	VotesReceived              *prometheus.CounterVec
	VoteLatency                *prometheus.HistogramVec
	Timeouts                   prometheus.Counter
	ParticipantsPerTransaction prometheus.Histogram
	DecisionsBroadcast         *prometheus.CounterVec
	VoteBroadcast              *prometheus.CounterVec

	// NewConsensusCoord performance metrics
	StateManagerSize  prometheus.Gauge
	CallbackLatency   *prometheus.HistogramVec
	CIRCMessagesTotal *prometheus.CounterVec
}

// Ensure Metrics implements MetricsRecorder
var _ MetricsRecorder = (*Metrics)(nil)

// NewMetrics creates consensus metrics
func NewMetrics() *Metrics {
	reg := xmetrics.NewComponentRegistry("publisher", "consensus")

	return &Metrics{
		registry: reg,

		TransactionsTotal: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "transactions_total",
			Help: "Total number of consensus transactions",
		}, []string{"state"}),

		ActiveTransactions: reg.NewGauge(prometheus.GaugeOpts{
			Name: "active_transactions",
			Help: "Number of active consensus transactions",
		}),

		Duration: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "duration_seconds",
			Help:    "Duration of consensus transactions",
			Buckets: xmetrics.ConsensusBuckets,
		}, []string{"state"}),

		VotesReceived: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "votes_received_total",
			Help: "Total number of votes received",
		}, []string{"chain_id", "vote"}),

		VoteLatency: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "vote_latency_seconds",
			Help:    "Latency from transaction start to vote received",
			Buckets: xmetrics.ConsensusBuckets,
		}, []string{"chain_id"}),

		Timeouts: reg.NewCounter(prometheus.CounterOpts{
			Name: "timeouts_total",
			Help: "Total number of transaction timeouts",
		}),

		ParticipantsPerTransaction: reg.NewHistogram(prometheus.HistogramOpts{
			Name:    "participants_per_transaction",
			Help:    "Number of participants per transaction",
			Buckets: xmetrics.CountBuckets,
		}),

		DecisionsBroadcast: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "decisions_broadcast_total",
			Help: "Total number of decisions broadcast",
		}, []string{"decision"}),

		VoteBroadcast: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "vote_broadcast_total",
			Help: "Total number of votes broadcast",
		}, []string{"vote"}),

		StateManagerSize: reg.NewGauge(prometheus.GaugeOpts{
			Name: "state_manager_size",
			Help: "Number of states in state manager",
		}),

		CallbackLatency: reg.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "callback_latency_seconds",
			Help:    "Latency of callback executions",
			Buckets: xmetrics.DurationBuckets,
		}, []string{"type"}),

		CIRCMessagesTotal: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "circ_messages_total",
			Help: "Total number of CIRC messages",
		}, []string{"operation"}),
	}
}

// RecordTransactionStarted records a transaction start
func (m *Metrics) RecordTransactionStarted(participantCount int) {
	m.TransactionsTotal.WithLabelValues("initiated").Inc()
	m.ActiveTransactions.Inc()
	m.ParticipantsPerTransaction.Observe(float64(participantCount))
}

// RecordTransactionCompleted records a transaction completion
func (m *Metrics) RecordTransactionCompleted(state string, duration time.Duration) {
	m.TransactionsTotal.WithLabelValues(state).Inc()
	m.Duration.WithLabelValues(state).Observe(duration.Seconds())
	m.ActiveTransactions.Dec()
}

// RecordVote records a vote received
func (m *Metrics) RecordVote(chainID string, vote bool, latency time.Duration) {
	voteStr := StateAbortStr
	if vote {
		voteStr = StateCommitStr
	}

	m.VotesReceived.WithLabelValues(chainID, voteStr).Inc()
	m.VoteLatency.WithLabelValues(chainID).Observe(latency.Seconds())
}

// RecordTimeout records a timeout
func (m *Metrics) RecordTimeout() {
	m.Timeouts.Inc()
	m.ActiveTransactions.Dec()
}

// RecordDecisionBroadcast records a decision broadcast
func (m *Metrics) RecordDecisionBroadcast(decision bool) {
	decisionStr := StateAbortStr
	if decision {
		decisionStr = StateCommitStr
	}
	m.DecisionsBroadcast.WithLabelValues(decisionStr).Inc()
}

// RecordVoteBroadcast records a vote broadcast
func (m *Metrics) RecordVoteBroadcast(vote bool) {
	voteStr := StateAbortStr
	if vote {
		voteStr = StateCommitStr
	}
	m.VoteBroadcast.WithLabelValues(voteStr).Inc()
}

// NoOpMetrics provides a no-op implementation of MetricsRecorder for testing
type NoOpMetrics struct{}

// Ensure NoOpMetrics implements MetricsRecorder
var _ MetricsRecorder = (*NoOpMetrics)(nil)

func (n *NoOpMetrics) RecordTransactionStarted(participantCount int)                   {}
func (n *NoOpMetrics) RecordTransactionCompleted(state string, duration time.Duration) {}
func (n *NoOpMetrics) RecordVote(chainID string, vote bool, latency time.Duration)     {}
func (n *NoOpMetrics) RecordTimeout()                                                  {}
func (n *NoOpMetrics) RecordDecisionBroadcast(decision bool)                           {}
func (n *NoOpMetrics) RecordVoteBroadcast(vote bool)                                   {}

// NewNoOpMetrics creates a new no-op metrics recorder for testing
func NewNoOpMetrics() MetricsRecorder {
	return &NoOpMetrics{}
}
