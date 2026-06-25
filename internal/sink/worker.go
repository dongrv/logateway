package sink

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dongrv/logateway/internal/message"
	"github.com/dongrv/logateway/internal/metrics"
	"github.com/dongrv/logateway/internal/wal"
)

type Backpressure int

const (
	BackpressureDrop Backpressure = iota
	BackpressureBlock
	BackpressureFallback
)

type WorkerPool struct {
	sink    Sink
	workers int
	ch      chan *message.Message
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	mu          sync.RWMutex
	closed      bool
	circuitOpen bool
	failCount   int
	maxFails    int

	backpressure     Backpressure
	walWriter        *wal.Writer
	submitTimeout    time.Duration
	walFallbacks     atomic.Int64
	recoveryInterval time.Duration

	metricsStop  chan struct{}
	recoveryStop chan struct{}
	shutdownOnce sync.Once
	recoveryOnce sync.Once
}

type WorkerPoolConfig struct {
	Sink          Sink
	Workers       int
	ChannelSize   int
	MaxFails      int
	Backpressure  Backpressure
	WALWriter     *wal.Writer
	SubmitTimeout time.Duration
}

func NewWorkerPool(cfg WorkerPoolConfig) *WorkerPool {
	if cfg.Workers <= 0 {
		cfg.Workers = 16
	}
	if cfg.ChannelSize <= 0 {
		cfg.ChannelSize = 16384
	}
	if cfg.MaxFails <= 0 {
		cfg.MaxFails = 10
	}
	if cfg.SubmitTimeout <= 0 {
		cfg.SubmitTimeout = 100 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	wp := &WorkerPool{
		sink:             cfg.Sink,
		workers:          cfg.Workers,
		ch:               make(chan *message.Message, cfg.ChannelSize),
		ctx:              ctx,
		cancel:           cancel,
		maxFails:         cfg.MaxFails,
		backpressure:     cfg.Backpressure,
		walWriter:        cfg.WALWriter,
		submitTimeout:    cfg.SubmitTimeout,
		recoveryInterval: 15 * time.Second,
		metricsStop:      make(chan struct{}),
	}
	wp.start()
	go wp.reportMetrics()
	return wp
}

func (wp *WorkerPool) reportMetrics() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-wp.metricsStop:
			return
		case <-ticker.C:
			metrics.SetChannelUsage(wp.sink.Name(), wp.ChannelUsage())
			metrics.SetCircuitState(wp.sink.Name(), wp.CircuitOpen())
		}
	}
}

func (wp *WorkerPool) start() {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
}

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] sink worker panic: %v", r)
		}
	}()

	for {
		select {
		case <-wp.ctx.Done():
			for msg := range wp.ch {
				wp.processDrain(msg)
			}
			return
		case msg, ok := <-wp.ch:
			if !ok {
				return
			}
			wp.process(msg)
		}
	}
}

func (wp *WorkerPool) process(msg *message.Message) {
	wp.processWithContext(msg, wp.ctx)
}

// processDrain processes messages during shutdown with a background context
// so that the send timeout is not cancelled by the pool's own shutdown.
func (wp *WorkerPool) processDrain(msg *message.Message) {
	wp.processWithContext(msg, context.Background())
}

func (wp *WorkerPool) processWithContext(msg *message.Message, parentCtx context.Context) {
	wp.mu.RLock()
	open := wp.circuitOpen
	wp.mu.RUnlock()

	if open {
		wp.handleRejected(msg, "circuit_open")
		return
	}

	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			metrics.RecordSinkRetry(wp.sink.Name())
			backoff := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
			select {
			case <-wp.ctx.Done():
				wp.handleRejected(msg, "shutdown_interrupt")
				return
			case <-time.After(backoff):
			}
		}

		if err := wp.sink.Send(ctx, msg); err != nil {
			lastErr = err
			continue
		}
		metrics.RecordSinkSuccess(wp.sink.Name())
		wp.resetFailures()
		message.ReleaseMessage(msg)
		return
	}

	log.Printf("[ERROR] sink %s send failed after retries: %v, request_id=%s",
		wp.sink.Name(), lastErr, msg.RequestID)
	metrics.RecordSinkFailure(wp.sink.Name())
	wp.recordFailure()
	wp.handleRejected(msg, "send_failure")
}

func (wp *WorkerPool) handleRejected(msg *message.Message, reason string) {
	log.Printf("[WARN] sink %s message rejected: reason=%s request_id=%s bp=%d",
		wp.sink.Name(), reason, msg.RequestID, wp.backpressure)

	switch wp.backpressure {
	case BackpressureFallback:
		if wp.walWriter != nil {
			if err := wp.walWriter.WriteMessage(msg); err != nil {
				log.Printf("[ERROR] wal fallback write failed: %v, request_id=%s", err, msg.RequestID)
			} else {
				wp.walFallbacks.Add(1)
			}
		}
	}
	message.ReleaseMessage(msg)
}

func (wp *WorkerPool) recordFailure() {
	wp.mu.Lock()
	wp.failCount++
	if wp.failCount >= wp.maxFails && !wp.circuitOpen {
		wp.circuitOpen = true
		metrics.SetCircuitState(wp.sink.Name(), true)
		log.Printf("[ERROR] circuit breaker opened for sink %s", wp.sink.Name())
		wp.startRecovery()
	}
	wp.mu.Unlock()
}

func (wp *WorkerPool) resetFailures() {
	wp.mu.Lock()
	wp.failCount = 0
	if wp.circuitOpen {
		wp.circuitOpen = false
		metrics.SetCircuitState(wp.sink.Name(), false)
		log.Printf("[INFO] circuit breaker closed for sink %s", wp.sink.Name())
	}
	wp.mu.Unlock()
}

func (wp *WorkerPool) Submit(msg *message.Message) error {
	wp.mu.RLock()
	closed := wp.closed
	bp := wp.backpressure
	timeout := wp.submitTimeout
	wp.mu.RUnlock()

	if closed {
		return fmt.Errorf("worker pool closed")
	}

	select {
	case wp.ch <- msg:
		return nil
	default:
	}

	switch bp {
	case BackpressureBlock:
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case wp.ch <- msg:
			return nil
		case <-timer.C:
			wp.handleRejected(msg, "channel_full_timeout")
			return fmt.Errorf("worker pool channel full (block timeout)")
		}
	case BackpressureFallback:
		wp.handleRejected(msg, "channel_full")
		return nil
	default:
		wp.handleRejected(msg, "channel_full")
		return fmt.Errorf("worker pool channel full (dropped)")
	}
}

func (wp *WorkerPool) CircuitOpen() bool {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	return wp.circuitOpen
}

func (wp *WorkerPool) Name() string       { return wp.sink.Name() }
func (wp *WorkerPool) SinkInstance() Sink { return wp.sink }

func (wp *WorkerPool) ChannelUsage() float64 {
	return float64(len(wp.ch)) / float64(cap(wp.ch))
}

func (wp *WorkerPool) WALFallbackCount() int64 {
	return wp.walFallbacks.Load()
}

// SubmitStrict enqueues a message or returns an error — it never falls back
// to WAL or drops. Used by WAL replay to detect channel pressure and abort
// early, preserving the segment for the next cycle.
// On error, the message is released (same ownership semantics as Submit).
func (wp *WorkerPool) SubmitStrict(msg *message.Message) error {
	wp.mu.RLock()
	closed := wp.closed
	wp.mu.RUnlock()
	if closed {
		message.ReleaseMessage(msg)
		return fmt.Errorf("worker pool closed")
	}
	select {
	case wp.ch <- msg:
		return nil
	default:
		message.ReleaseMessage(msg)
		return fmt.Errorf("worker pool channel full")
	}
}

func (wp *WorkerPool) Shutdown(timeout time.Duration) error {
	var err error
	wp.shutdownOnce.Do(func() {
		wp.mu.Lock()
		wp.closed = true
		wp.mu.Unlock()

		// 1. Cancel context — wakes workers blocked in retry backoff
		wp.cancel()
		// 2. Stop metrics reporter
		close(wp.metricsStop)
		// 3. Close channel — workers' for-range exits after draining
		close(wp.ch)
		// 4. Stop recovery if running
		if wp.recoveryStop != nil {
			close(wp.recoveryStop)
		}

		done := make(chan struct{})
		go func() {
			wp.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			err = wp.sink.Close()
		case <-time.After(timeout):
			err = fmt.Errorf("shutdown timeout after %v", timeout)
		}
	})
	return err
}

// startRecovery launches a background health probe that periodically checks
// sink connectivity. When the sink becomes healthy again, the circuit breaker
// is closed automatically.
func (wp *WorkerPool) startRecovery() {
	wp.recoveryOnce.Do(func() {
		wp.recoveryStop = make(chan struct{})
		go wp.recoveryLoop()
		log.Printf("[INFO] circuit recovery probe started for sink %s", wp.sink.Name())
	})
}

func (wp *WorkerPool) recoveryLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] circuit recovery probe panic for sink %s: %v", wp.sink.Name(), r)
		}
		// Allow restarting recovery if circuit opens again
		wp.recoveryOnce = sync.Once{}
	}()

	ticker := time.NewTicker(wp.recoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wp.recoveryStop:
			return
		case <-ticker.C:
			if !wp.CircuitOpen() {
				return
			}
			if err := wp.sink.HealthCheck(); err != nil {
				log.Printf("[WARN] circuit recovery probe failed for sink %s: %v", wp.sink.Name(), err)
				continue
			}
			wp.resetFailures()
			log.Printf("[INFO] circuit breaker auto-recovered for sink %s", wp.sink.Name())
			return
		}
	}
}
