package xmetrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Uptime is a gauge registered in the shared publisher custom registry.
var (
	Uptime = func() prometheus.Gauge {
		gauge := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "shared_publisher",
			Subsystem: "core",
			Name:      "uptime_seconds",
			Help:      "Uptime in seconds",
		})
		GetRegistry().MustRegister(gauge)
		return gauge
	}()
)
