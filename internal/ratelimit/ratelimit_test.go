package ratelimit

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/dongrv/logateway/internal/config"
)

func TestProjectLimiterUpdatesWhenQPSChanges(t *testing.T) {
	cfgMgr, err := config.NewManager(writeRateLimitConfig(t, 10))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	defer cfgMgr.Close()

	mgr := NewManager(cfgMgr)
	lim := mgr.GetOrCreateLimiter("p", 10)
	if got := lim.Burst(); got != 20 {
		t.Fatalf("initial burst = %d, want 20", got)
	}

	lim2 := mgr.GetOrCreateLimiter("p", 3)
	if lim2 != lim {
		t.Fatal("expected limiter to be updated in place")
	}
	if got := lim.Burst(); got != 6 {
		t.Fatalf("updated burst = %d, want 6", got)
	}
}

func writeRateLimitConfig(t *testing.T, global int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	body := []byte(`
server:
  global_rate_limit: ` + strconv.Itoa(global) + `
`)
	if err := os.WriteFile(path, body, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
