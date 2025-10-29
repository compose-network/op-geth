package xmetrics

import (
	"context"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// RuntimeCollector collects runtime metrics.
type RuntimeCollector struct {
	goroutines *prometheus.Desc
	gcPause    *prometheus.Desc
	memAlloc   *prometheus.Desc
	memSys     *prometheus.Desc
}

// NewRuntimeCollector creates a new runtime collector.
func NewRuntimeCollector() *RuntimeCollector {
	return &RuntimeCollector{
		goroutines: prometheus.NewDesc(
			"publisher_runtime_goroutines",
			"Number of goroutines",
			nil, nil,
		),
		gcPause: prometheus.NewDesc(
			"publisher_runtime_gc_pause_seconds",
			"GC pause duration",
			nil, nil,
		),
		memAlloc: prometheus.NewDesc(
			"publisher_runtime_mem_alloc_bytes",
			"Allocated memory",
			nil, nil,
		),
		memSys: prometheus.NewDesc(
			"publisher_runtime_mem_sys_bytes",
			"System memory",
			nil, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *RuntimeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.goroutines
	ch <- c.gcPause
	ch <- c.memAlloc
	ch <- c.memSys
}

// Collect implements prometheus.Collector.
func (c *RuntimeCollector) Collect(ch chan<- prometheus.Metric) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ch <- prometheus.MustNewConstMetric(c.goroutines, prometheus.GaugeValue, float64(runtime.NumGoroutine()))
	ch <- prometheus.MustNewConstMetric(c.gcPause, prometheus.GaugeValue, float64(m.PauseNs[(m.NumGC+255)%256])/1e9)
	ch <- prometheus.MustNewConstMetric(c.memAlloc, prometheus.GaugeValue, float64(m.Alloc))
	ch <- prometheus.MustNewConstMetric(c.memSys, prometheus.GaugeValue, float64(m.Sys))
}

func StartPeriodicCollection(ctx context.Context, interval time.Duration, start time.Time) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			uptime := time.Since(start)
			Uptime.Set(uptime.Seconds())
		}
	}
}
