// Package auth provides HMAC-SHA256 header-based authentication middleware for Gin.
package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
}

// NewMiddleware creates a new auth middleware.
func NewMiddleware(cfg *config.Manager, store AppKeyStore) (*Middleware, error) {
	authCfg := cfg.Get().Auth
	return &Middleware{
		cfg:        cfg,
		keyStore:   store,
		nonceCache: make(map[string]time.Time, authCfg.NonceCacheSize),
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

		bodyBytes, ok := rawBodyFromContext(c)
		if !ok {
			writeAuthError(c, http.StatusBadRequest, "request body not available")
			c.Abort()
			return
		}

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

		// 3. Validate nonce presence. It is consumed atomically after the
		// signature succeeds, so concurrent replays cannot pass together.
		if nonce == "" {
			writeAuthError(c, http.StatusUnauthorized, "missing nonce")
			c.Abort()
			return
		}

		// 4. Verify signature
		if !verifyHMAC(secret, bodyBytes, timestampStr, nonce, signature) {
			writeAuthError(c, http.StatusUnauthorized, "signature mismatch")
			c.Abort()
			return
		}

		var peek struct {
			Project string `json:"project"`
		}
		if err := json.Unmarshal(bodyBytes, &peek); err == nil && peek.Project != "" {
			if err := authorizeProject(m.keyStore, appKey, peek.Project); err != nil {
				writeAuthError(c, http.StatusForbidden, "app key is not authorized for project")
				c.Abort()
				return
			}
		}

		if m.consumeNonce(appKey, nonce, authCfg.NonceTTLSeconds, authCfg.NonceCacheSize) {
			writeAuthError(c, http.StatusUnauthorized, "nonce already used")
			c.Abort()
			return
		}

		// Store app key for downstream use
		c.Set("app_key", appKey)
		c.Next()
	}
}

func (m *Middleware) consumeNonce(appKey, nonce string, ttlSeconds, maxNonces int) bool {
	if ttlSeconds <= 0 {
		ttlSeconds = 300
	}
	if maxNonces <= 0 {
		maxNonces = 100000
	}

	key := appKey + "\x00" + nonce
	now := time.Now()
	expireBefore := now.Add(-time.Duration(ttlSeconds) * time.Second)

	m.nonceMu.Lock()
	defer m.nonceMu.Unlock()

	for k, seenAt := range m.nonceCache {
		if seenAt.Before(expireBefore) {
			delete(m.nonceCache, k)
		}
	}

	if _, exists := m.nonceCache[key]; exists {
		return true
	}

	for len(m.nonceCache) >= maxNonces {
		var oldestKey string
		var oldestTime time.Time
		for k, seenAt := range m.nonceCache {
			if oldestKey == "" || seenAt.Before(oldestTime) {
				oldestKey = k
				oldestTime = seenAt
			}
		}
		if oldestKey == "" {
			break
		}
		delete(m.nonceCache, oldestKey)
	}

	m.nonceCache[key] = now
	return false
}

func computeHMAC(secret, body, timestamp, nonce string) string {
	data := append([]byte(body), timestamp...)
	data = append(data, nonce...)
	return hex.EncodeToString(computeHMACBytes(secret, data))
}

func verifyHMAC(secret string, body []byte, timestamp, nonce, signature string) bool {
	if signature == "" {
		return false
	}
	got, err := hex.DecodeString(signature)
	if err != nil {
		return false
	}
	data := make([]byte, 0, len(body)+len(timestamp)+len(nonce))
	data = append(data, body...)
	data = append(data, timestamp...)
	data = append(data, nonce...)
	expected := computeHMACBytes(secret, data)
	return hmac.Equal(expected, got)
}

func computeHMACBytes(secret string, data []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(data)
	return mac.Sum(nil)
}

func rawBodyFromContext(c *gin.Context) ([]byte, bool) {
	if b, exists := c.Get("raw_body"); exists {
		raw, ok := b.([]byte)
		return raw, ok
	}
	if c.Request.Body == nil {
		return nil, false
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(c.Request.Body); err != nil {
		return nil, false
	}
	raw := buf.Bytes()
	c.Request.Body.Close()
	c.Request.Body = io.NopCloser(bytes.NewReader(raw))
	c.Set("raw_body", raw)
	return raw, true
}

func authorizeProject(store AppKeyStore, appKey, project string) error {
	ok, err := store.IsAuthorized(appKey, project)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("app key %q is not authorized for project %q", appKey, project)
	}
	return nil
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
