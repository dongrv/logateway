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

// Backpressure strategy determines behavior when the worker channel is full.
type Backpressure int

const (
	BackpressureDrop     Backpressure = iota // drop the message (default)
	BackpressureBlock                        // block with timeout
	BackpressureFallback                     // write to disk WAL
)

// WorkerPool manages a pool of goroutines that consume from a bounded channel
// and deliver messages to a Sink.
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

	backpressure  Backpressure
	walWriter     *wal.Writer
	submitTimeout time.Duration
	walFallbacks  atomic.Int64

	metricsStop  chan struct{}
	shutdownOnce sync.Once
}

// WorkerPoolConfig configures a WorkerPool.
type WorkerPoolConfig struct {
	Sink          Sink
	Workers       int
	ChannelSize   int
	MaxFails      int
	Backpressure  Backpressure
	WALWriter     *wal.Writer
	SubmitTimeout time.Duration // for "block" strategy
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(cfg WorkerPoolConfig) *WorkerPool {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.ChannelSize <= 0 {
		cfg.ChannelSize = 4096
	}
	if cfg.MaxFails <= 0 {
		cfg.MaxFails = 10
	}
	if cfg.SubmitTimeout <= 0 {
		cfg.SubmitTimeout = 100 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	wp := &WorkerPool{
		sink:          cfg.Sink,
		workers:       cfg.Workers,
		ch:            make(chan *message.Message, cfg.ChannelSize),
		ctx:           ctx,
		cancel:        cancel,
		maxFails:      cfg.MaxFails,
		backpressure:  cfg.Backpressure,
		walWriter:     cfg.WALWriter,
		submitTimeout: cfg.SubmitTimeout,
		metricsStop:   make(chan struct{}),
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
			for {
				select {
				case msg, ok := <-wp.ch:
					if !ok {
						return
					}
					wp.process(msg)
				default:
					return
				}
			}
		case msg, ok := <-wp.ch:
			if !ok {
				return
			}
			wp.process(msg)
		}
	}
}

func (wp *WorkerPool) process(msg *message.Message) {
	wp.mu.RLock()
	open := wp.circuitOpen
	wp.mu.RUnlock()

	if open {
		wp.handleRejected(msg, "circuit_open")
		return
	}

	ctx, cancel := context.WithTimeout(wp.ctx, 5*time.Second)
	defer cancel()

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			metrics.RecordSinkRetry(wp.sink.Name())
			backoff := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
			select {
			case <-wp.ctx.Done():
				message.ReleaseMessage(msg)
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

// handleRejected deals with a message that cannot be delivered to the sink.
// Strategy depends on the backpressure config: drop (default), or write to WAL.
func (wp *WorkerPool) handleRejected(msg *message.Message, reason string) {
	switch wp.backpressure {
	case BackpressureFallback:
		if wp.walWriter != nil {
			if err := wp.walWriter.WriteMessage(msg); err != nil {
				log.Printf("[ERROR] wal fallback write failed: %v, request_id=%s", err, msg.RequestID)
			} else {
				wp.walFallbacks.Add(1)
				log.Printf("[WARN] wal fallback: %s, request_id=%s", reason, msg.RequestID)
			}
		}
	default: // drop
		// already logged in process() or circuit open handler
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

// Submit enqueues a message for delivery. Returns error if the pool is closed,
// or if the channel is full and backpressure is "block" with timeout.
// For "drop" and "fallback" strategies, channel-full is not an error — the
// message is handled internally (dropped or written to WAL).
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

	// Channel full — apply backpressure strategy
	switch bp {
	case BackpressureBlock:
		// Block with timeout
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
		// Write to WAL and drop from memory
		wp.handleRejected(msg, "channel_full")
		return nil // success from caller's perspective (message is persisted)
	default: // BackpressureDrop
		wp.handleRejected(msg, "channel_full")
		return fmt.Errorf("worker pool channel full (dropped)")
	}
}

// CircuitOpen returns whether the circuit breaker is open.
func (wp *WorkerPool) CircuitOpen() bool {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	return wp.circuitOpen
}

// Name returns the underlying sink name.
func (wp *WorkerPool) Name() string {
	return wp.sink.Name()
}

// SinkInstance returns the underlying sink for health checks.
func (wp *WorkerPool) SinkInstance() Sink {
	return wp.sink
}

// ChannelUsage returns the current channel usage ratio (0.0 to 1.0).
func (wp *WorkerPool) ChannelUsage() float64 {
	return float64(len(wp.ch)) / float64(cap(wp.ch))
}

// WALFallbackCount returns the number of messages written to WAL fallback.
func (wp *WorkerPool) WALFallbackCount() int64 {
	return wp.walFallbacks.Load()
}

// Shutdown gracefully stops the worker pool.
// Safe to call multiple times; only the first call takes effect.
func (wp *WorkerPool) Shutdown(timeout time.Duration) error {
	var err error
	wp.shutdownOnce.Do(func() {
		wp.mu.Lock()
		wp.closed = true
		wp.mu.Unlock()

		wp.cancel()
		close(wp.metricsStop)
		close(wp.ch)

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
