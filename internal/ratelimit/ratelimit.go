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
	projectRL map[string]*rate.Limiter
	globalRL  *rate.Limiter
}

func NewManager(cfg *config.Manager) *Manager {
	c := cfg.Get()
	return &Manager{
		cfg:       cfg,
		projectRL: make(map[string]*rate.Limiter),
		globalRL:  rate.NewLimiter(rate.Limit(c.Server.GlobalRateLimit), c.Server.GlobalRateLimit*2),
	}
}

func (m *Manager) GetOrCreateLimiter(project string, qps int) *rate.Limiter {
	m.mu.RLock()
	lim, ok := m.projectRL[project]
	m.mu.RUnlock()
	if ok {
		return lim
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if lim, ok := m.projectRL[project]; ok {
		return lim
	}
	if qps <= 0 {
		qps = 1000
	}
	lim = rate.NewLimiter(rate.Limit(float64(qps)), qps*2)
	m.projectRL[project] = lim
	return lim
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
