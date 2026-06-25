package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dongrv/logateway/internal/auth"
	"github.com/dongrv/logateway/internal/config"
	"github.com/dongrv/logateway/internal/logging"
	"github.com/dongrv/logateway/internal/message"
	"github.com/dongrv/logateway/internal/metrics"
	"github.com/dongrv/logateway/internal/observability"
	"github.com/dongrv/logateway/internal/project"
	"github.com/dongrv/logateway/internal/ratelimit"
	"github.com/dongrv/logateway/internal/sink"
	"github.com/dongrv/logateway/internal/wal"

	_ "net/http/pprof"

	"github.com/gin-gonic/gin"
	"github.com/panjf2000/ants/v2"
)

type Gateway struct {
	Config    *config.Manager
	Router    *gin.Engine
	server    *http.Server
	disp      *project.Dispatcher
	walWriter *wal.Writer
	pool      *ants.Pool
	hc        *observability.HealthChecker
	stopCh    chan struct{}
	stopHC    func()
	stopOnce  sync.Once
	closeOnce sync.Once
}

func New(cfgPath string) (*Gateway, error) {
	cfgMgr, err := config.NewManager(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	cfg := cfgMgr.Get()
	logging.Setup(cfg.Log.File.Dir, cfg.Log.Console.Enabled, cfg.Log.File.Levels)

	if err := cfgMgr.Watch(); err != nil {
		log.Printf("[WARN] config file watcher failed: %v (hot-reload disabled)", err)
	}

	if strings.EqualFold(cfg.Log.Level, "debug") {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	bp := resolveBackpressure(cfg.Server.Backpressure)

	walWriter, err := initWAL(cfg, bp)
	if err != nil {
		cfgMgr.Close()
		return nil, fmt.Errorf("wal: %w", err)
	}

	reg := sink.NewRegistry()
	reg.Register("redis", sink.RedisSinkFactory)
	reg.Register("kafka", sink.KafkaSinkFactory)

	disp := project.NewDispatcher(cfgMgr, reg, walWriter, bp)
	if err := disp.Initialize(); err != nil {
		if walWriter != nil {
			_ = walWriter.Close()
		}
		cfgMgr.Close()
		return nil, fmt.Errorf("dispatcher: %w", err)
	}

	// Start background WAL auto-replay so sealed segments get replayed
	// without requiring a restart.
	if walWriter != nil {
		walWriter.StartReplay(func(entry wal.Entry) error {
			if entry.Project == "" {
				return fmt.Errorf("empty project")
			}
			if delay, err := disp.ReplayBackoff(entry.Project); err != nil {
				return err
			} else if delay > 0 {
				time.Sleep(delay)
			}
			msg := messageFromWALEntry(entry)
			// DispatchStrict: channel-full returns error → segment preserved for retry
			if err := disp.DispatchStrict(msg); err != nil {
				log.Printf("[WARN] wal auto-replay dispatch failed (will retry): %v, request_id=%s", err, entry.RequestID)
				return err
			}
			return nil
		}, 5*time.Second)
	}

	pool, err := ants.NewPool(cfg.Server.AntsPoolSize, ants.WithPreAlloc(false))
	if err != nil {
		if walWriter != nil {
			_ = walWriter.Close()
		}
		_ = disp.Shutdown()
		cfgMgr.Close()
		return nil, fmt.Errorf("ants pool: %w", err)
	}
	metrics.PoolCapacity.Set(float64(pool.Cap()))

	hc := observability.NewHealthChecker()
	registerSinkProbes(hc, disp)
	router := buildRouter(cfgMgr, disp, pool, hc)

	return &Gateway{
		Config:    cfgMgr,
		Router:    router,
		disp:      disp,
		walWriter: walWriter,
		pool:      pool,
		hc:        hc,
		stopCh:    make(chan struct{}),
	}, nil
}

func (g *Gateway) Run() error {
	cfg := g.Config.Get()
	go g.reportMetrics()

	g.stopHC = g.hc.Run(5 * time.Second)
	defer func() {
		if g.stopHC != nil {
			g.stopHC()
		}
		g.stopMetrics()
	}()

	g.server = &http.Server{
		Addr:         cfg.Server.ListenAddr,
		Handler:      g.Router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("[INFO] gateway starting on %s (bp=%s wal=%v)",
			cfg.Server.ListenAddr, cfg.Server.Backpressure, cfg.WAL.Enabled)
		if err := g.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Printf("[INFO] received signal %v, shutting down...", sig)
	case err := <-serverErr:
		return fmt.Errorf("server listen: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := g.server.Shutdown(ctx); err != nil {
		log.Printf("[ERROR] server forced to shutdown: %v", err)
	}
	if err := g.disp.Shutdown(); err != nil {
		log.Printf("[ERROR] dispatcher shutdown error: %v", err)
	}
	if g.walWriter != nil {
		if err := g.walWriter.Close(); err != nil {
			log.Printf("[ERROR] wal close error: %v", err)
		}
	}
	return nil
}

func (g *Gateway) Close() {
	g.closeOnce.Do(func() {
		if g.stopHC != nil {
			g.stopHC()
		}
		g.stopMetrics()
		if g.pool != nil {
			g.pool.Release()
		}
		if g.walWriter != nil {
			g.walWriter.Close()
		}
		g.Config.Close()
		logging.Close()
	})
}

func (g *Gateway) stopMetrics() {
	g.stopOnce.Do(func() {
		close(g.stopCh)
	})
}

func (g *Gateway) reportMetrics() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			metrics.PoolGoroutines.Set(float64(g.pool.Running()))
		}
	}
}

func buildRouter(cfgMgr *config.Manager, disp *project.Dispatcher, pool *ants.Pool, hc *observability.HealthChecker) *gin.Engine {
	router := gin.New()
	rlMgr := ratelimit.NewManager(cfgMgr)

	router.Use(gin.Recovery())
	router.Use(RequestIDMiddleware())
	router.Use(observability.MetricsMiddleware())
	router.Use(LoggingMiddleware())
	router.Use(rlMgr.GlobalMiddleware())

	registerHealthEndpoints(router, cfgMgr, disp, hc)

	api := router.Group("/api/v1/log")
	{
		api.POST("/upload",
			BodyCacheMiddleware(cfgMgr),
			authMiddleware(cfgMgr),
			ProjectResolutionMiddleware(cfgMgr),
			rlMgr.ProjectMiddleware(),
			UploadHandler(cfgMgr, disp, pool),
		)
	}
	return router
}

func authMiddleware(cfgMgr *config.Manager) gin.HandlerFunc {
	keyStore := &staticKeyStore{
		keys: map[string]string{"test-app-key": "test-secret"},
	}
	mw, err := auth.NewMiddleware(cfgMgr, keyStore)
	if err != nil {
		return func(c *gin.Context) {
			log.Printf("[ERROR] auth middleware unavailable: %v", err)
			c.AbortWithStatusJSON(http.StatusInternalServerError, message.UploadResponse{
				Code:    http.StatusInternalServerError,
				Message: "authentication unavailable",
			})
		}
	}
	return mw.Handler()
}

func registerHealthEndpoints(r *gin.Engine, cfg *config.Manager, disp *project.Dispatcher, hc *observability.HealthChecker) {
	observability.RegisterHealthEndpoints(r, hc, cfg, disp)

	pprofCfg := cfg.Get().Pprof
	if pprofCfg.Enabled {
		pprofPath := pprofCfg.Path
		if pprofPath == "" {
			pprofPath = "/debug/pprof"
		}
		r.Any(pprofPath+"/*action", gin.WrapH(http.DefaultServeMux))
	}
}

func UploadHandler(cfgMgr *config.Manager, disp *project.Dispatcher, pool *ants.Pool) gin.HandlerFunc {
	return func(c *gin.Context) {
		var rawBody []byte
		if b, exists := c.Get("raw_body"); exists {
			var ok bool
			rawBody, ok = b.([]byte)
			if !ok {
				log.Printf("[ERROR] raw_body is not []byte, got %T", b)
				c.JSON(http.StatusInternalServerError, message.UploadResponse{
					Code: http.StatusInternalServerError, Message: "internal error",
				})
				return
			}
		} else {
			c.JSON(http.StatusInternalServerError, message.UploadResponse{
				Code: http.StatusInternalServerError, Message: "internal error: body not available",
			})
			return
		}

		cfg := cfgMgr.Get()
		if int64(len(rawBody)) > cfg.Server.MaxBodyBytes {
			c.JSON(http.StatusRequestEntityTooLarge, message.UploadResponse{
				Code: http.StatusRequestEntityTooLarge, Message: "request body too large",
			})
			return
		}

		var req message.UploadRequest
		if err := json.Unmarshal(rawBody, &req); err != nil {
			c.JSON(http.StatusBadRequest, message.UploadResponse{
				Code: http.StatusBadRequest, Message: "invalid JSON: " + err.Error(),
			})
			return
		}

		projCfg := cfgMgr.GetProject(req.Project)
		if projCfg != nil && projCfg.MaxBodyBytes > 0 && int64(len(rawBody)) > projCfg.MaxBodyBytes {
			c.JSON(http.StatusRequestEntityTooLarge, message.UploadResponse{
				Code: http.StatusRequestEntityTooLarge, Message: "request body exceeds project limit",
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
		msg.Env = cfg.Server.Env

		if err := pool.Submit(func() {
			if err := disp.Dispatch(msg); err != nil {
				observability.LogJSON("error", "dispatch failed",
					msg.RequestID, msg.TraceID, msg.Project, err.Error())
			}
		}); err != nil {
			message.ReleaseMessage(msg)
			c.JSON(http.StatusServiceUnavailable, message.UploadResponse{
				Code: http.StatusServiceUnavailable, Message: "server busy, please retry",
			})
			return
		}

		c.JSON(http.StatusOK, message.UploadResponse{
			Code: 0, Message: "success", RequestID: requestID, TraceID: traceID,
		})
	}
}

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

func initWAL(cfg *config.Config, bp sink.Backpressure) (*wal.Writer, error) {
	if bp != sink.BackpressureFallback && !cfg.WAL.Enabled {
		return nil, nil
	}
	walCfg := wal.Config{
		Dir: cfg.WAL.Dir, MaxSegmentBytes: cfg.WAL.MaxSegmentBytes,
		MaxSegments: cfg.WAL.MaxSegments, SyncInterval: cfg.WAL.SyncInterval,
	}
	w, err := wal.NewWriter(walCfg)
	if err != nil {
		return nil, fmt.Errorf("wal init: %w", err)
	}
	log.Printf("[INFO] WAL initialized: dir=%s max_segment=%dMB max_segments=%d",
		cfg.WAL.Dir, cfg.WAL.MaxSegmentBytes>>20, cfg.WAL.MaxSegments)
	return w, nil
}

func messageFromWALEntry(entry wal.Entry) *message.Message {
	msg := message.AcquireMessage()
	msg.RequestID = entry.RequestID
	msg.TraceID = entry.TraceID
	msg.Project = entry.Project
	msg.Router = entry.Router
	msg.Data = entry.Data
	msg.Timestamp = entry.Timestamp
	msg.Env = entry.Env
	return msg
}

func registerSinkProbes(hc *observability.HealthChecker, disp *project.Dispatcher) {
	for _, info := range disp.SinkInfos() {
		name, si := info.Name, info.Sink
		hc.Register(name, func() error { return si.HealthCheck() })
	}
	hc.Register("gateway", func() error { return nil })
}

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
