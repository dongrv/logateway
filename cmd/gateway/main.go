// Package main is the entry point for the logateway HTTP message gateway.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dongrv/logateway/internal/auth"
	"github.com/dongrv/logateway/internal/config"
	"github.com/dongrv/logateway/internal/message"
	"github.com/dongrv/logateway/internal/metrics"
	"github.com/dongrv/logateway/internal/observability"
	"github.com/dongrv/logateway/internal/project"
	"github.com/dongrv/logateway/internal/ratelimit"
	"github.com/dongrv/logateway/internal/sink"
	"github.com/dongrv/logateway/internal/wal"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
)

// staticKeyStore is a simple in-memory AppKeyStore for development/testing.
type staticKeyStore struct {
	keys map[string]string
}

func (s *staticKeyStore) GetSecret(appKey string) (string, error) {
	secret, ok := s.keys[appKey]
	if !ok {
		return "", nil
	}
	return secret, nil
}

func (s *staticKeyStore) IsAuthorized(_, _ string) (bool, error) {
	return true, nil
}

func main() {
	configPath := flag.String("config", "configs/gateway.yaml", "path to config file")
	flag.Parse()

	// Load configuration
	cfgMgr, err := config.NewManager(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	cfg := cfgMgr.Get()

	// Start config file watcher for hot-reload
	if err := cfgMgr.Watch(); err != nil {
		log.Printf("[WARN] config file watcher failed: %v (hot-reload disabled)", err)
	}
	defer cfgMgr.Close()

	// Set Gin mode
	if strings.EqualFold(cfg.Log.Level, "debug") {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	// Resolve backpressure strategy
	bp := resolveBackpressure(cfg.Server.Backpressure)

	// Initialize WAL for disk fallback
	walWriter, err := initWAL(cfg, bp)
	if err != nil {
		log.Fatalf("failed to initialize WAL: %v", err)
	}
	defer func() {
		if err := walWriter.Close(); err != nil {
			log.Printf("[ERROR] wal close: %v", err)
		}
	}()

	// Initialize sink registry
	reg := sink.NewRegistry()
	reg.Register("redis", sink.RedisSinkFactory)
	reg.Register("kafka", sink.KafkaSinkFactory)

	// Initialize project dispatcher
	disp := project.NewDispatcher(cfgMgr, reg, nil, bp)
	// Note: walWriter is set to nil here because WAL fallback writes happen via
	// WorkerPool.handleRejected, which already has the walWriter reference.
	// The dispatcher passes the walWriter to each pool during initProject.
	if err := disp.Initialize(); err != nil {
		log.Fatalf("failed to initialize dispatcher: %v", err)
	}

	// Replay any persisted WAL messages from previous run
	replayWAL(cfg, disp)

	// Now connect the WAL writer to worker pools for ongoing fallback writes.
	// We re-initialize with the walWriter so newly created pools get it.
	// Actually, the pools already exist and don't have WAL writer set.
	// Let's fix this: the dispatcher should pass walWriter on creation.
	// Since Initialize() was already called without walWriter, we need to
	// recreate the dispatcher with the WAL writer.
	disp = project.NewDispatcher(cfgMgr, reg, walWriter, bp)
	if err := disp.Initialize(); err != nil {
		log.Fatalf("failed to re-initialize dispatcher: %v", err)
	}

	// Initialize health checker with sink probes
	hc := observability.NewHealthChecker()
	registerSinkProbes(hc, disp)
	hc.Run(5 * time.Second)

	// Initialize rate limiter
	rlMgr := ratelimit.NewManager(cfgMgr)

	// Initialize auth middleware
	keyStore := &staticKeyStore{
		keys: map[string]string{
			"test-app-key": "test-secret-key-change-in-production",
		},
	}
	authMW, err := auth.NewMiddleware(cfgMgr, keyStore)
	if err != nil {
		log.Fatalf("failed to create auth middleware: %v", err)
	}

	// Initialize ants goroutine pool
	pool, err := ants.NewPool(cfg.Server.AntsPoolSize, ants.WithPreAlloc(false))
	if err != nil {
		log.Fatalf("failed to create ants pool: %v", err)
	}
	defer pool.Release()

	// Start ants pool metrics reporter
	metrics.PoolCapacity.Set(float64(pool.Cap()))
	poolMetricsStop := make(chan struct{})
	go reportPoolMetrics(pool, poolMetricsStop)
	defer close(poolMetricsStop)

	// Create Gin engine
	router := gin.New()

	router.Use(gin.Recovery())
	router.Use(requestIDMiddleware())
	router.Use(observability.MetricsMiddleware())
	router.Use(loggingMiddleware())
	router.Use(rlMgr.GlobalMiddleware())

	observability.RegisterHealthEndpoints(router, hc, cfgMgr, disp)

	api := router.Group("/api/v1/log")
	{
		api.POST("/upload",
			authMW.Handler(),
			projectResolutionMiddleware(cfgMgr),
			rlMgr.ProjectMiddleware(),
			uploadHandler(cfgMgr, disp, pool),
		)
	}

	srv := &http.Server{
		Addr:         cfg.Server.ListenAddr,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("[INFO] gateway starting on %s (bp=%s wal=%v)",
			cfg.Server.ListenAddr, cfg.Server.Backpressure, cfg.WAL.Enabled)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] server error: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("[INFO] received signal %v, shutting down...", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[ERROR] server forced to shutdown: %v", err)
	}

	if err := disp.Shutdown(); err != nil {
		log.Printf("[ERROR] dispatcher shutdown error: %v", err)
	}

	// Flush and close WAL
	if err := walWriter.Close(); err != nil {
		log.Printf("[ERROR] wal close: %v", err)
	}

	log.Println("[INFO] gateway stopped")
}

// resolveBackpressure maps config string to Backpressure enum.
func resolveBackpressure(s string) sink.Backpressure {
	switch strings.ToLower(s) {
	case "block":
		return sink.BackpressureBlock
	case "fallback":
		return sink.BackpressureFallback
	default:
		return sink.BackpressureDrop
	}
}

// initWAL creates the WAL writer if enabled in config.
func initWAL(cfg *config.Config, bp sink.Backpressure) (*wal.Writer, error) {
	if bp != sink.BackpressureFallback && !cfg.WAL.Enabled {
		// WAL not needed — return a no-op writer
		return nil, nil
	}

	walCfg := wal.Config{
		Dir:             cfg.WAL.Dir,
		MaxSegmentBytes: cfg.WAL.MaxSegmentBytes,
		MaxSegments:     cfg.WAL.MaxSegments,
		SyncInterval:    cfg.WAL.SyncInterval,
	}

	w, err := wal.NewWriter(walCfg)
	if err != nil {
		return nil, fmt.Errorf("wal init: %w", err)
	}

	log.Printf("[INFO] WAL initialized: dir=%s max_segment=%dMB max_segments=%d",
		cfg.WAL.Dir, cfg.WAL.MaxSegmentBytes>>20, cfg.WAL.MaxSegments)
	return w, nil
}

// replayWAL reads all persisted WAL entries from disk and dispatches them.
func replayWAL(cfg *config.Config, disp *project.Dispatcher) {
	entryCh, errCh := wal.ReadAll(cfg.WAL.Dir)

	var replayed int
	for entry := range entryCh {
		msg := message.AcquireMessage()
		msg.RequestID = entry.RequestID
		msg.TraceID = entry.TraceID
		msg.Project = entry.Project
		msg.Router = entry.Router
		msg.Data = entry.Data
		msg.Timestamp = entry.Timestamp

		if err := disp.Dispatch(msg); err != nil {
			log.Printf("[ERROR] wal replay dispatch failed: %v, request_id=%s", err, msg.RequestID)
			message.ReleaseMessage(msg)
		}
		replayed++
	}

	if err := <-errCh; err != nil {
		log.Printf("[ERROR] wal replay error: %v", err)
	}

	if replayed > 0 {
		log.Printf("[INFO] WAL replay complete: %d messages replayed", replayed)
	}
}

// projectResolutionMiddleware resolves the project from the request body
// and sets it in the Gin context for downstream middleware (rate limit, handler).
func projectResolutionMiddleware(cfgMgr *config.Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		var rawBody []byte
		if b, exists := c.Get("raw_body"); exists {
			rawBody = b.([]byte)
		} else {
			var err error
			rawBody, err = c.GetRawData()
			if err != nil {
				c.JSON(http.StatusBadRequest, message.UploadResponse{
					Code:    http.StatusBadRequest,
					Message: "failed to read body",
				})
				c.Abort()
				return
			}
			c.Set("raw_body", rawBody)
		}

		var peek struct {
			Project string `json:"Project"`
		}
		if err := json.Unmarshal(rawBody, &peek); err != nil || peek.Project == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, message.UploadResponse{
				Code:    http.StatusBadRequest,
				Message: "Project is required",
			})
			return
		}

		projCfg := cfgMgr.GetProject(peek.Project)
		if projCfg == nil {
			c.AbortWithStatusJSON(http.StatusNotFound, message.UploadResponse{
				Code:    http.StatusNotFound,
				Message: fmt.Sprintf("unknown project: %s", peek.Project),
			})
			return
		}

		if projCfg.AuthRequired {
			if _, exists := c.Get("app_key"); !exists {
				c.AbortWithStatusJSON(http.StatusUnauthorized, message.UploadResponse{
					Code:    http.StatusUnauthorized,
					Message: "authentication required",
				})
				return
			}
		}

		c.Set("project_name", projCfg.Name)
		c.Next()
	}
}

// registerSinkProbes registers health probes for sink connectivity.
func registerSinkProbes(hc *observability.HealthChecker, disp *project.Dispatcher) {
	for _, info := range disp.SinkInfos() {
		name := info.Name
		si := info.Sink
		hc.Register(name, func() error {
			return si.HealthCheck()
		})
	}
	hc.Register("gateway", func() error {
		return nil
	})
}

// reportPoolMetrics periodically reports ants pool running count.
func reportPoolMetrics(pool *ants.Pool, stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			metrics.PoolGoroutines.Set(float64(pool.Running()))
		}
	}
}

// uploadHandler handles POST /api/v1/log/upload.
func uploadHandler(cfgMgr *config.Manager, disp *project.Dispatcher, pool *ants.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		var rawBody []byte
		if b, exists := c.Get("raw_body"); exists {
			rawBody = b.([]byte)
		} else {
			c.JSON(http.StatusInternalServerError, message.UploadResponse{
				Code:    http.StatusInternalServerError,
				Message: "internal error: body not available",
			})
			return
		}

		cfg := cfgMgr.Get()
		if int64(len(rawBody)) > cfg.Server.MaxBodyBytes {
			c.JSON(http.StatusRequestEntityTooLarge, message.UploadResponse{
				Code:    http.StatusRequestEntityTooLarge,
				Message: "request body too large",
			})
			return
		}

		var req message.UploadRequest
		if err := json.Unmarshal(rawBody, &req); err != nil {
			c.JSON(http.StatusBadRequest, message.UploadResponse{
				Code:    http.StatusBadRequest,
				Message: "invalid JSON: " + err.Error(),
			})
			return
		}

		projCfg := cfgMgr.GetProject(req.Project)
		if projCfg != nil && projCfg.MaxBodyBytes > 0 && int64(len(rawBody)) > projCfg.MaxBodyBytes {
			c.JSON(http.StatusRequestEntityTooLarge, message.UploadResponse{
				Code:    http.StatusRequestEntityTooLarge,
				Message: "request body exceeds project limit",
			})
			return
		}

		requestID := c.GetString("request_id")
		traceID := c.GetHeader("X-Trace-Id")
		if traceID == "" {
			traceID = requestID
		}

		msg := message.AcquireMessage()
		msg.RequestID = requestID
		msg.TraceID = traceID
		msg.Project = req.Project
		msg.Router = req.Router
		msg.Data = req.Data
		msg.Timestamp = time.Now()

		if err := pool.Submit(func() {
			defer message.ReleaseMessage(msg)
			if err := disp.Dispatch(msg); err != nil {
				observability.LogJSON("error", "dispatch failed",
					msg.RequestID, msg.TraceID, msg.Project, err.Error())
			}
		}); err != nil {
			message.ReleaseMessage(msg)
			c.JSON(http.StatusServiceUnavailable, message.UploadResponse{
				Code:    http.StatusServiceUnavailable,
				Message: "server busy, please retry",
			})
			return
		}

		c.JSON(http.StatusOK, message.UploadResponse{
			Code:      0,
			Message:   "success",
			RequestID: requestID,
			TraceID:   traceID,
		})
	}
}

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-Id")
		if requestID == "" {
			requestID = uuid.New().String()
		}
		c.Set("request_id", requestID)
		c.Header("X-Request-Id", requestID)
		c.Next()
	}
}

func loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		reqIDVal, _ := c.Get("request_id")
		requestID, _ := reqIDVal.(string)
		if requestID == "" {
			requestID = "unknown"
		}

		traceID := c.GetHeader("X-Trace-Id")
		if traceID == "" {
			traceID = requestID
		}

		observability.LogJSON("info", "request",
			requestID, traceID, "",
			fmt.Sprintf("method=%s path=%s status=%d duration=%s",
				c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(start)))
	}
}
