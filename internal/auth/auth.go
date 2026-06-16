// Package auth provides HMAC-SHA256 header-based authentication middleware for Gin.
package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dongrv/logateway/internal/config"
	"github.com/dongrv/logateway/internal/message"
	"github.com/gin-gonic/gin"
)

// AppKeyStore provides secret lookup for app keys.
type AppKeyStore interface {
	// GetSecret returns the secret for the given app key, or empty string if not found.
	GetSecret(appKey string) (string, error)
	// IsAuthorized checks if the app key has access to the given project.
	IsAuthorized(appKey, project string) (bool, error)
}

// Middleware is the Gin middleware for HMAC-SHA256 authentication.
type Middleware struct {
	cfg        *config.Manager
	keyStore   AppKeyStore
	nonceMu    sync.RWMutex
	nonceCache map[string]time.Time
	maxNonces  int
}

// NewMiddleware creates a new auth middleware.
func NewMiddleware(cfg *config.Manager, store AppKeyStore) (*Middleware, error) {
	authCfg := cfg.Get().Auth
	return &Middleware{
		cfg:        cfg,
		keyStore:   store,
		nonceCache: make(map[string]time.Time, authCfg.NonceCacheSize),
		maxNonces:  authCfg.NonceCacheSize,
	}, nil
}

// Handler returns the Gin handler function for authentication.
func (m *Middleware) Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		authCfg := m.cfg.Get().Auth
		if !authCfg.Enabled {
			c.Next()
			return
		}

		appKey := c.GetHeader("X-App-Key")
		timestampStr := c.GetHeader("X-Timestamp")
		nonce := c.GetHeader("X-Nonce")
		signature := c.GetHeader("X-Signature")

		// Read body for signature verification
		bodyBytes, err := io.ReadAll(c.Request.Body)
		c.Request.Body.Close()
		if err != nil {
			writeAuthError(c, http.StatusBadRequest, "failed to read request body")
			c.Abort()
			return
		}
		bodyStr := string(bodyBytes)
		// Re-set the body so downstream handlers can read it
		c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		// Use Set to store raw body for later use
		c.Set("raw_body", bodyBytes)

		// 1. Check AppKey exists
		if appKey == "" {
			writeAuthError(c, http.StatusUnauthorized, "missing X-App-Key header")
			c.Abort()
			return
		}

		secret, err := m.keyStore.GetSecret(appKey)
		if err != nil || secret == "" {
			writeAuthError(c, http.StatusUnauthorized, "invalid app key")
			c.Abort()
			return
		}

		// 2. Validate timestamp
		ts, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			writeAuthError(c, http.StatusUnauthorized, "invalid timestamp")
			c.Abort()
			return
		}
		now := time.Now().Unix()
		if abs(now-ts) > authCfg.TimestampWindow {
			writeAuthError(c, http.StatusUnauthorized, "timestamp out of window")
			c.Abort()
			return
		}

		// 3. Check nonce for replay protection
		if nonce == "" {
			writeAuthError(c, http.StatusUnauthorized, "missing nonce")
			c.Abort()
			return
		}
		if m.isNonceReplayed(nonce) {
			writeAuthError(c, http.StatusUnauthorized, "nonce already used")
			c.Abort()
			return
		}

		// 4. Verify signature
		expectedSig := computeHMAC(secret, bodyStr, timestampStr, nonce)
		if !hmac.Equal([]byte(expectedSig), []byte(signature)) {
			writeAuthError(c, http.StatusUnauthorized, "signature mismatch")
			c.Abort()
			return
		}

		// Cache the nonce
		m.cacheNonce(nonce)

		// Store app key for downstream use
		c.Set("app_key", appKey)
		c.Next()
	}
}

func (m *Middleware) isNonceReplayed(nonce string) bool {
	m.nonceMu.RLock()
	_, ok := m.nonceCache[nonce]
	m.nonceMu.RUnlock()
	return ok
}

func (m *Middleware) cacheNonce(nonce string) {
	m.nonceMu.Lock()
	defer m.nonceMu.Unlock()

	// Evict oldest entries if at capacity
	if len(m.nonceCache) >= m.maxNonces {
		for k := range m.nonceCache {
			delete(m.nonceCache, k)
			break // just evict one to make room
		}
	}
	m.nonceCache[nonce] = time.Now()
}

func computeHMAC(secret, body, timestamp, nonce string) string {
	data := body + timestamp + nonce
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

func writeAuthError(c *gin.Context, code int, msg string) {
	resp := message.UploadResponse{
		Code:    code,
		Message: msg,
	}
	c.JSON(code, resp)
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
