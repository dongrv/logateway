package sink

import (
	"context"
	"testing"
	"time"

	"github.com/dongrv/logateway/internal/message"
)

type panicOnceSink struct {
	name string
	ch   chan string
}

func (s *panicOnceSink) Name() string { return s.name }

func (s *panicOnceSink) Send(_ context.Context, msg *message.Message) error {
	if msg.RequestID == "panic" {
		panic("send panic")
	}
	s.ch <- msg.RequestID
	return nil
}

func (s *panicOnceSink) HealthCheck() error { return nil }
func (s *panicOnceSink) Close() error       { return nil }

func TestWorkerContinuesAfterMessagePanic(t *testing.T) {
	captured := make(chan string, 1)
	wp := NewWorkerPool(WorkerPoolConfig{
		Sink:         &panicOnceSink{name: "panic-once", ch: captured},
		Workers:      1,
		ChannelSize:  2,
		Backpressure: BackpressureDrop,
	})
	defer wp.Shutdown(2 * time.Second)

	panicMsg := message.AcquireMessage()
	panicMsg.RequestID = "panic"
	if err := wp.SubmitStrict(panicMsg); err != nil {
		t.Fatalf("submit panic message: %v", err)
	}

	okMsg := message.AcquireMessage()
	okMsg.RequestID = "ok"
	if err := wp.SubmitStrict(okMsg); err != nil {
		t.Fatalf("submit ok message: %v", err)
	}

	select {
	case got := <-captured:
		if got != "ok" {
			t.Fatalf("captured = %q, want ok", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not continue after panic")
	}
}
