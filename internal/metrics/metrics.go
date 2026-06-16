// Package metrics defines all Prometheus metrics for the logateway gateway.
// It is a leaf package to avoid circular imports between sink, observability, and project.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal counts all HTTP requests by project, method, and status.
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_http_requests_total",
			Help: "Total number of HTTP requests handled.",
		},
		[]string{"project", "method", "status"},
	)

	// HTTPRequestDuration records HTTP request duration in seconds.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"project", "method", "status"},
	)

	// SinkDeliveriesTotal counts sink delivery attempts by sink name and status.
	SinkDeliveriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_sink_deliveries_total",
			Help: "Total number of sink delivery attempts.",
		},
		[]string{"sink", "status"},
	)

	// SinkRetriesTotal counts sink retry attempts.
	SinkRetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_sink_retries_total",
			Help: "Total number of sink retry attempts.",
		},
		[]string{"sink"},
	)

	// CircuitState reports whether the circuit breaker is open (1) or closed (0).
	CircuitState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gateway_circuit_state",
			Help: "Circuit breaker state: 0=closed, 1=open.",
		},
		[]string{"sink"},
	)

	// ChannelUsageRatio reports the current channel fill ratio per sink.
	ChannelUsageRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gateway_channel_usage_ratio",
			Help: "Current sink channel usage ratio (0.0 to 1.0).",
		},
		[]string{"sink"},
	)

	// RatelimitRejectsTotal counts rate-limit rejections by level (global/project).
	RatelimitRejectsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_ratelimit_rejects_total",
			Help: "Total number of rate-limit rejections.",
		},
		[]string{"level", "project"},
	)

	// PoolGoroutines reports the number of active goroutines in the ants pool.
	PoolGoroutines = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "gateway_pool_goroutines",
			Help: "Number of active goroutines in the ants pool.",
		},
	)

	// PoolCapacity reports the total capacity of the ants pool.
	PoolCapacity = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "gateway_pool_capacity",
			Help: "Total capacity of the ants pool.",
		},
	)
)

// RecordSinkSuccess records a successful sink delivery.
func RecordSinkSuccess(sinkName string) {
	SinkDeliveriesTotal.WithLabelValues(sinkName, "success").Inc()
}

// RecordSinkFailure records a failed sink delivery.
func RecordSinkFailure(sinkName string) {
	SinkDeliveriesTotal.WithLabelValues(sinkName, "failure").Inc()
}

// RecordSinkRetry records a sink retry attempt.
func RecordSinkRetry(sinkName string) {
	SinkRetriesTotal.WithLabelValues(sinkName).Inc()
}

// SetCircuitState sets the circuit breaker gauge.
func SetCircuitState(sinkName string, open bool) {
	v := 0.0
	if open {
		v = 1.0
	}
	CircuitState.WithLabelValues(sinkName).Set(v)
}

// SetChannelUsage sets the channel usage gauge.
func SetChannelUsage(sinkName string, ratio float64) {
	ChannelUsageRatio.WithLabelValues(sinkName).Set(ratio)
}
