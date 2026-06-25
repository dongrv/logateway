package auth

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dongrv/logateway/internal/config"
	"github.com/gin-gonic/gin"
)

type testKeyStore struct{}

func (testKeyStore) GetSecret(appKey string) (string, error) {
	if appKey == "app" {
		return "secret", nil
	}
	return "", nil
}

func (testKeyStore) IsAuthorized(_, _ string) (bool, error) { return true, nil }

func TestNonceConsumedAtomically(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfgPath := writeAuthConfig(t)
	cfgMgr, err := config.NewManager(cfgPath)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	defer cfgMgr.Close()

	mw, err := NewMiddleware(cfgMgr, testKeyStore{})
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		body, _ := c.GetRawData()
		c.Set("raw_body", body)
		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		c.Next()
	})
	router.Use(mw.Handler())
	router.POST("/", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	body := `{"project":"p","router":"r","data":{"k":"v"}}`
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := "same-nonce"
	sig := computeHMAC("secret", body, ts, nonce)

	var okCount atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
			req.Header.Set("X-App-Key", "app")
			req.Header.Set("X-Timestamp", ts)
			req.Header.Set("X-Nonce", nonce)
			req.Header.Set("X-Signature", sig)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code == http.StatusOK {
				okCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := okCount.Load(); got != 1 {
		t.Fatalf("successful requests = %d, want 1", got)
	}
}

func writeAuthConfig(t *testing.T) string {
	t.Helper()
	return writeTempConfig(t, `
auth:
  enabled: true
  timestamp_window: 300
  nonce_ttl_seconds: 300
  nonce_cache_size: 1000
`)
}

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
