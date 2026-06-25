package ratelimit

import (
	"log"
	"net/http"
	"sync"

	"github.com/dongrv/logateway/internal/config"
	"github.com/dongrv/logateway/internal/message"
	"github.com/dongrv/logateway/internal/metrics"
	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

type Manager struct {
	cfg       *config.Manager
	mu        sync.RWMutex
	projectRL map[string]*projectLimiter
	globalRL  *rate.Limiter
}

type projectLimiter struct {
	qps int
	lim *rate.Limiter
}

func NewManager(cfg *config.Manager) *Manager {
	c := cfg.Get()
	m := &Manager{
		cfg:       cfg,
		projectRL: make(map[string]*projectLimiter),
		globalRL:  newLimiter(c.Server.GlobalRateLimit),
	}
	cfg.OnReload(func(c *config.Config) {
		m.updateGlobalLimiter(c.Server.GlobalRateLimit)
	})
	return m
}

func (m *Manager) GetOrCreateLimiter(project string, qps int) *rate.Limiter {
	qps = normalizeQPS(qps)
	m.mu.RLock()
	entry, ok := m.projectRL[project]
	m.mu.RUnlock()
	if ok && entry.qps == qps {
		return entry.lim
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.projectRL[project]; ok {
		if entry.qps != qps {
			entry.qps = qps
			entry.lim.SetLimit(rate.Limit(qps))
			entry.lim.SetBurst(qps * 2)
		}
		return entry.lim
	}
	lim := newLimiter(qps)
	m.projectRL[project] = &projectLimiter{qps: qps, lim: lim}
	return lim
}

func (m *Manager) updateGlobalLimiter(qps int) {
	qps = normalizeQPS(qps)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.globalRL.SetLimit(rate.Limit(qps))
	m.globalRL.SetBurst(qps * 2)
}

func (m *Manager) GlobalMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !m.globalRL.Allow() {
			metrics.RatelimitRejectsTotal.WithLabelValues("global", "").Inc()
			c.JSON(http.StatusTooManyRequests, message.UploadResponse{
				Code:    http.StatusTooManyRequests,
				Message: "global rate limit exceeded",
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

func (m *Manager) ProjectMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		raw, exists := c.Get("project_name")
		if !exists {
			c.Next()
			return
		}
		projectName, ok := raw.(string)
		if !ok {
			log.Printf("[ERROR] ratelimit: project_name is not a string, got %T", raw)
			c.Next()
			return
		}

		projCfg := m.cfg.GetProject(projectName)
		if projCfg == nil {
			c.Next()
			return
		}

		lim := m.GetOrCreateLimiter(projCfg.Name, projCfg.RateLimit)
		if !lim.Allow() {
			metrics.RatelimitRejectsTotal.WithLabelValues("project", projCfg.Name).Inc()
			c.JSON(http.StatusTooManyRequests, message.UploadResponse{
				Code:    http.StatusTooManyRequests,
				Message: "project rate limit exceeded",
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

func (m *Manager) GlobalLimit() bool {
	return m.globalRL.Allow()
}

func newLimiter(qps int) *rate.Limiter {
	qps = normalizeQPS(qps)
	return rate.NewLimiter(rate.Limit(qps), qps*2)
}

func normalizeQPS(qps int) int {
	if qps <= 0 {
		return 1000
	}
	return qps
}
