package xmetrics

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	registryOnce   sync.Once
	customRegistry *prometheus.Registry
)

// GetRegistry returns a process-wide custom registry for shared publisher metrics.
// It isolates this module's metrics from the global default registry to avoid
// duplicate registration panics when embedded in other binaries.
func GetRegistry() *prometheus.Registry {
	registryOnce.Do(func() {
		customRegistry = prometheus.NewRegistry()
	})
	return customRegistry
}

// ComponentRegistry manages metrics for a specific component.
type ComponentRegistry struct {
	namespace string
	subsystem string
	registry  *prometheus.Registry
}

// NewComponentRegistry creates a registry for a component.
func NewComponentRegistry(namespace, subsystem string) *ComponentRegistry {
	// Sanitize subsystem to ensure it's a valid Prometheus metric name
	// Replace hyphens with underscores (common for UUIDs)
	subsystem = strings.ReplaceAll(subsystem, "-", "_")

	return &ComponentRegistry{
		namespace: namespace,
		subsystem: subsystem,
		registry:  GetRegistry(),
	}
}

// NewCounterVec creates a new counter with proper naming.
func (r *ComponentRegistry) NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	opts.Namespace = r.namespace
	opts.Subsystem = r.subsystem
	return promauto.With(r.registry).NewCounterVec(opts, labelNames)
}

// NewCounter creates a new counter with proper naming.
func (r *ComponentRegistry) NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	opts.Namespace = r.namespace
	opts.Subsystem = r.subsystem
	return promauto.With(r.registry).NewCounter(opts)
}

// NewGauge creates a new gauge with proper naming.
func (r *ComponentRegistry) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	opts.Namespace = r.namespace
	opts.Subsystem = r.subsystem
	return promauto.With(r.registry).NewGauge(opts)
}

// NewGaugeVec creates a new gauge vector with proper naming.
func (r *ComponentRegistry) NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	opts.Namespace = r.namespace
	opts.Subsystem = r.subsystem
	return promauto.With(r.registry).NewGaugeVec(opts, labelNames)
}

// NewHistogram creates a new histogram with proper naming.
func (r *ComponentRegistry) NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	opts.Namespace = r.namespace
	opts.Subsystem = r.subsystem
	return promauto.With(r.registry).NewHistogram(opts)
}

// NewHistogramVec creates a new histogram vector with proper naming.
func (r *ComponentRegistry) NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string,
) *prometheus.HistogramVec {
	opts.Namespace = r.namespace
	opts.Subsystem = r.subsystem
	return promauto.With(r.registry).NewHistogramVec(opts, labelNames)
}
