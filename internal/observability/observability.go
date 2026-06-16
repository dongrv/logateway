// Package observability provides Prometheus metrics, health checks, and structured logging.
package observability

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dongrv/logateway/internal/config"
	"github.com/dongrv/logateway/internal/metrics"
	"github.com/dongrv/logateway/internal/project"
	"github.com/dongrv/logateway/internal/sink"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Probe is a function that checks the health of a component.
type Probe func() error

// HealthChecker periodically probes component health and caches results.
type HealthChecker struct {
	mu        sync.RWMutex
	statuses  map[string]bool
	probes    map[string]Probe
	lastCheck time.Time
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker() *HealthChecker {
	return &HealthChecker{
		statuses: make(map[string]bool),
		probes:   make(map[string]Probe),
	}
}

// Register adds a health probe.
func (h *HealthChecker) Register(name string, probe Probe) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.probes[name] = probe
}

// Run starts periodic health checking in a background goroutine.
func (h *HealthChecker) Run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[ERROR] health checker panic: %v", r)
			}
		}()
		for range ticker.C {
			h.checkAll()
		}
	}()
}

func (h *HealthChecker) checkAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for name, probe := range h.probes {
		if err := probe(); err != nil {
			h.statuses[name] = false
			log.Printf("[WARN] health check %s failed: %v", name, err)
		} else {
			h.statuses[name] = true
		}
	}
	h.lastCheck = time.Now()
}

// Status returns the cached health status of all components.
func (h *HealthChecker) Status() map[string]bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make(map[string]bool, len(h.statuses))
	for k, v := range h.statuses {
		result[k] = v
	}
	return result
}

// Healthy returns true if all registered components are healthy.
func (h *HealthChecker) Healthy() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, healthy := range h.statuses {
		if !healthy {
			return false
		}
	}
	return len(h.statuses) > 0
}

// RegisterHealthEndpoints registers /health, /ready, /metrics endpoints on the Gin engine.
func RegisterHealthEndpoints(r *gin.Engine, hc *HealthChecker, cfg *config.Manager, disp *project.Dispatcher) {
	r.GET("/health", func(c *gin.Context) {
		status := hc.Status()
		healthy := hc.Healthy()
		httpStatus := http.StatusOK
		if !healthy {
			httpStatus = http.StatusServiceUnavailable
		}
		c.JSON(httpStatus, gin.H{
			"status":     healthy,
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
			"components": status,
		})
	})

	r.GET("/ready", func(c *gin.Context) {
		if hc.Healthy() {
			c.JSON(http.StatusOK, gin.H{"ready": true})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ready": false})
		}
	})

	metricsPath := cfg.Get().Metrics.Path
	if metricsPath == "" {
		metricsPath = "/metrics"
	}
	r.GET(metricsPath, gin.WrapH(promhttp.Handler()))

	r.POST("/admin/config/reload", func(c *gin.Context) {
		if err := cfg.Reload(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "config reloaded"})
	})

	r.GET("/admin/pools", func(c *gin.Context) {
		statuses := disp.GetPoolStatus()
		c.JSON(http.StatusOK, statuses)
	})
}

// SinkProbe creates a health probe for a specific sink.
func SinkProbe(s sink.Sink) Probe {
	return func() error {
		return s.HealthCheck()
	}
}

// MetricsMiddleware returns a Gin middleware that records Prometheus metrics.
func MetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		method := c.Request.Method
		project := ""
		if p, exists := c.Get("project_name"); exists {
			if s, ok := p.(string); ok {
				project = s
			}
		}

		metrics.HTTPRequestsTotal.WithLabelValues(project, method, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(project, method, status).Observe(duration)
	}
}

// JSONLogEntry represents a structured log entry.
type JSONLogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
	TraceID   string `json:"trace_id,omitempty"`
	Project   string `json:"project,omitempty"`
	Error     string `json:"error,omitempty"`
}

// LogJSON writes a structured JSON log entry to stdout.
func LogJSON(level, msg, requestID, traceID, project, errStr string) {
	entry := JSONLogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level,
		Message:   msg,
		RequestID: requestID,
		TraceID:   traceID,
		Project:   project,
		Error:     errStr,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		// Fallback: log as plain text to avoid silent log loss
		log.Printf("[%s] %s request_id=%s trace_id=%s project=%s error=%s",
			level, msg, requestID, traceID, project, errStr)
		return
	}
	println(string(data))
}
