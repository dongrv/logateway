package project

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dongrv/logateway/internal/config"
	"github.com/dongrv/logateway/internal/message"
	"github.com/dongrv/logateway/internal/sink"
)

type captureSink struct {
	name string
	ch   chan json.RawMessage
}

func (s *captureSink) Name() string { return s.name }

func (s *captureSink) Send(_ context.Context, msg *message.Message) error {
	cp := make([]byte, len(msg.Data))
	copy(cp, msg.Data)
	s.ch <- cp
	return nil
}

func (s *captureSink) HealthCheck() error { return nil }
func (s *captureSink) Close() error       { return nil }

func TestDispatchStrictSkipsPipeline(t *testing.T) {
	cfgPath := writeDispatcherConfig(t)
	cfgMgr, err := config.NewManager(cfgPath)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	defer cfgMgr.Close()

	captured := make(chan json.RawMessage, 2)
	reg := sink.NewRegistry()
	reg.Register("capture", func(name string, _ map[string]interface{}) (sink.Sink, error) {
		return &captureSink{name: name, ch: captured}, nil
	})

	disp := NewDispatcher(cfgMgr, reg, nil, sink.BackpressureBlock)
	if err := disp.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	defer disp.Shutdown()

	normal := message.AcquireMessage()
	normal.Project = "p"
	normal.Router = "r"
	normal.Data = json.RawMessage(`{"k":"v"}`)
	normal.Timestamp = time.Now()
	if err := disp.Dispatch(normal); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	assertCapturedData(t, captured, true)

	replay := message.AcquireMessage()
	replay.Project = "p"
	replay.Router = "r"
	replay.Data = json.RawMessage(`{"k":"v"}`)
	replay.Timestamp = time.Now()
	if err := disp.DispatchStrict(replay); err != nil {
		t.Fatalf("dispatch strict: %v", err)
	}
	assertCapturedData(t, captured, false)
}

func TestSinkInstanceWorkersAndChannelSizeAreApplied(t *testing.T) {
	cfgPath := writeInstanceConfig(t)
	cfgMgr, err := config.NewManager(cfgPath)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	defer cfgMgr.Close()

	reg := sink.NewRegistry()
	reg.Register("capture", func(name string, _ map[string]interface{}) (sink.Sink, error) {
		return &captureSink{name: name, ch: make(chan json.RawMessage, 1)}, nil
	})

	disp := NewDispatcher(cfgMgr, reg, nil, sink.BackpressureBlock)
	if err := disp.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	defer disp.Shutdown()

	disp.mu.RLock()
	pools := disp.pools["p"]
	disp.mu.RUnlock()
	if len(pools) != 1 {
		t.Fatalf("pool count = %d, want 1", len(pools))
	}
	if got := pools[0].ChannelCapacity(); got != 7 {
		t.Fatalf("channel capacity = %d, want 7", got)
	}
	if got := pools[0].WorkerCount(); got != 3 {
		t.Fatalf("workers = %d, want 3", got)
	}
}

func assertCapturedData(t *testing.T, captured <-chan json.RawMessage, wantAdded bool) {
	t.Helper()
	select {
	case raw := <-captured:
		var data map[string]interface{}
		if err := json.Unmarshal(raw, &data); err != nil {
			t.Fatalf("unmarshal captured data: %v", err)
		}
		_, gotAdded := data["added"]
		if gotAdded != wantAdded {
			t.Fatalf("pipeline added field = %v, want %v (data=%s)", gotAdded, wantAdded, string(raw))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sink")
	}
}

func writeDispatcherConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	body := []byte(`
server:
  backpressure: block
projects:
  - name: p
    enabled: true
    sinks:
      - type: capture
        workers: 1
        channel_size: 4
    pipelines:
      - type: field_add
        config:
          fields:
            added: replay-should-skip-this
`)
	if err := os.WriteFile(path, body, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func writeInstanceConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	body := []byte(`
server:
  backpressure: block
sink_instances:
  capture_instance:
    type: capture
    workers: 3
    channel_size: 7
projects:
  - name: p
    enabled: true
    sinks:
      - instance: capture_instance
`)
	if err := os.WriteFile(path, body, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
