package ratelimit

import (
	"net/http"
	"sync"

	"github.com/dongrv/logateway/internal/config"
	"github.com/dongrv/logateway/internal/message"
	"github.com/dongrv/logateway/internal/metrics"
	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

// Manager manages rate limiters for projects and global traffic.
type Manager struct {
	cfg       *config.Manager
	mu        sync.RWMutex
	projectRL map[string]*rate.Limiter
	globalRL  *rate.Limiter
}

// NewManager creates a new rate limit manager.
func NewManager(cfg *config.Manager) *Manager {
	c := cfg.Get()
	return &Manager{
		cfg:       cfg,
		projectRL: make(map[string]*rate.Limiter),
		globalRL:  rate.NewLimiter(rate.Limit(c.Server.GlobalRateLimit), c.Server.GlobalRateLimit),
	}
}

// GetOrCreateLimiter returns a rate limiter for the given project, creating one if needed.
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
	lim = rate.NewLimiter(rate.Limit(qps), qps)
	m.projectRL[project] = lim
	return lim
}

// GlobalMiddleware returns a Gin middleware for global rate limiting.
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

// ProjectMiddleware returns a Gin middleware for project-level rate limiting.
func (m *Manager) ProjectMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		projectName, exists := c.Get("project_name")
		if !exists {
			c.Next()
			return
		}

		projCfg := m.cfg.GetProject(projectName.(string))
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

// GlobalLimit returns true if the global limiter allows the request.
func (m *Manager) GlobalLimit() bool {
	return m.globalRL.Allow()
}
