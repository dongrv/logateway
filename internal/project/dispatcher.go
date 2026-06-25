package project

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dongrv/logateway/internal/config"
	"github.com/dongrv/logateway/internal/message"
	"github.com/dongrv/logateway/internal/pipeline"
	"github.com/dongrv/logateway/internal/sink"
	"github.com/dongrv/logateway/internal/wal"
)

type SinkInfo struct {
	Name string
	Sink sink.Sink
}

type Dispatcher struct {
	cfg         *config.Manager
	reg         *sink.Registry
	pipelineReg *pipeline.Registry
	mu          sync.RWMutex
	pools       map[string][]*sink.WorkerPool
	walWriter   *wal.Writer
	bp          sink.Backpressure
}

func NewDispatcher(cfg *config.Manager, reg *sink.Registry, walWriter *wal.Writer, bp sink.Backpressure) *Dispatcher {
	return &Dispatcher{
		cfg:         cfg,
		reg:         reg,
		pipelineReg: pipeline.NewRegistry(),
		pools:       make(map[string][]*sink.WorkerPool),
		walWriter:   walWriter,
		bp:          bp,
	}
}

func (d *Dispatcher) PipelineRegistry() *pipeline.Registry { return d.pipelineReg }

func (d *Dispatcher) Initialize() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cfg := d.cfg.Get()
	for _, projCfg := range cfg.Projects {
		if !projCfg.Enabled {
			continue
		}
		if err := d.initProject(projCfg); err != nil {
			return fmt.Errorf("init project %s: %w", projCfg.Name, err)
		}
	}
	return nil
}

func (d *Dispatcher) initProject(projCfg config.ProjectConfig) error {
	var pools []*sink.WorkerPool
	for i, sr := range projCfg.Sinks {
		sinkType, mergedCfg := d.resolveSinkConfig(sr)
		sinkName := fmt.Sprintf("%s-%s-%d", projCfg.Name, sinkType, i)
		workers := sr.Workers
		if workers <= 0 {
			workers = readPoolInt(mergedCfg, "workers", 16)
		}
		channelSize := sr.ChannelSize
		if channelSize <= 0 {
			channelSize = readPoolInt(mergedCfg, "channel_size", 16384)
		}
		si, err := d.reg.Create(sinkType, sinkName, mergedCfg)
		if err != nil {
			return fmt.Errorf("create sink %s: %w", sinkName, err)
		}
		pool := sink.NewWorkerPool(sink.WorkerPoolConfig{
			Sink:          si,
			Workers:       workers,
			ChannelSize:   channelSize,
			MaxFails:      10,
			Backpressure:  d.bp,
			WALWriter:     d.walWriter,
			SubmitTimeout: 100 * time.Millisecond,
		})
		pools = append(pools, pool)
	}
	d.pools[projCfg.Name] = pools
	log.Printf("[INFO] initialized project %s with %d sinks (bp=%v)", projCfg.Name, len(pools), d.bp)
	return nil
}

func (d *Dispatcher) resolveSinkConfig(ref config.SinkRef) (string, map[string]interface{}) {
	sinkType := ref.Type
	if ref.Instance != "" {
		instances := d.cfg.Get().SinkInstances
		if inst, ok := instances[ref.Instance]; ok {
			sinkType = inst.Type
		} else {
			log.Printf("[WARN] sink instance %q not found, falling back to type %q", ref.Instance, ref.Type)
		}
	}
	if sinkType == "" {
		sinkType = "redis"
	}

	merged := d.globalSinkDefaults(sinkType)

	if ref.Instance != "" {
		instances := d.cfg.Get().SinkInstances
		if inst, ok := instances[ref.Instance]; ok {
			for k, v := range inst.Config {
				merged[k] = v
			}
		}
	}

	for k, v := range ref.Config {
		merged[k] = v
	}

	return sinkType, merged
}

func (d *Dispatcher) globalSinkDefaults(sinkType string) map[string]interface{} {
	cfg := d.cfg.Get()
	switch sinkType {
	case "redis":
		return map[string]interface{}{
			"addr":           cfg.Sinks.Redis.Addr,
			"password":       cfg.Sinks.Redis.Password,
			"db":             float64(cfg.Sinks.Redis.DB),
			"pool_size":      float64(cfg.Sinks.Redis.PoolSize),
			"min_idle_conns": float64(cfg.Sinks.Redis.MinIdleConns),
			"dial_timeout":   cfg.Sinks.Redis.DialTimeout.String(),
			"read_timeout":   cfg.Sinks.Redis.ReadTimeout.String(),
			"write_timeout":  cfg.Sinks.Redis.WriteTimeout.String(),
			"key":            cfg.Sinks.Redis.Key,
			"type":           cfg.Sinks.Redis.Type,
			"max_len":        float64(cfg.Sinks.Redis.MaxLen),
		}
	case "kafka":
		brokers := make([]interface{}, len(cfg.Sinks.Kafka.Brokers))
		for i, b := range cfg.Sinks.Kafka.Brokers {
			brokers[i] = b
		}
		return map[string]interface{}{
			"brokers":       brokers,
			"topic":         cfg.Sinks.Kafka.Topic,
			"partition_key": cfg.Sinks.Kafka.PartitionKey,
			"compression":   cfg.Sinks.Kafka.Compression,
			"batch_size":    float64(cfg.Sinks.Kafka.BatchSize),
			"batch_timeout": cfg.Sinks.Kafka.BatchTimeout.String(),
		}
	default:
		return make(map[string]interface{})
	}
}

func (d *Dispatcher) SinkInfos() []SinkInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var infos []SinkInfo
	for _, pools := range d.pools {
		for _, pool := range pools {
			infos = append(infos, SinkInfo{
				Name: pool.Name(),
				Sink: pool.SinkInstance(),
			})
		}
	}
	return infos
}

// Dispatch routes a message to all configured sinks for the project.
// Uses Submit (may fallback to WAL). Worker pools release the message.
func (d *Dispatcher) Dispatch(msg *message.Message) error {
	return d.dispatchImpl(msg, false)
}

// DispatchStrict is like Dispatch but uses SubmitStrict — it returns an
// error if any worker pool channel is full, without falling back to WAL.
// Used by WAL replay to detect backpressure and preserve segment for retry.
func (d *Dispatcher) DispatchStrict(msg *message.Message) error {
	return d.dispatchImpl(msg, true)
}

// dispatchImpl is the single dispatch implementation. When strict is true,
// SubmitStrict is used; otherwise Submit (with fallback/drop semantics).
// On error, the message is always released; callers must not release again.
func (d *Dispatcher) dispatchImpl(msg *message.Message, strict bool) error {
	if msg == nil {
		return fmt.Errorf("nil message")
	}
	if msg.Project == "" {
		message.ReleaseMessage(msg)
		return fmt.Errorf("empty project")
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.pools == nil {
		message.ReleaseMessage(msg)
		return fmt.Errorf("dispatcher pools not initialized")
	}
	pools, ok := d.pools[msg.Project]
	if !ok {
		message.ReleaseMessage(msg)
		return fmt.Errorf("no sinks configured for project %s", msg.Project)
	}

	projCfg := d.cfg.GetProject(msg.Project)
	if projCfg != nil && len(projCfg.Pipelines) > 0 {
		chain := d.buildPipelineChain(projCfg)
		var err error
		msg, err = chain.Process(msg)
		if err != nil {
			message.ReleaseMessage(msg)
			return fmt.Errorf("pipeline %s: %w", projCfg.Name, err)
		}
	}

	for i, pool := range pools {
		m := msg
		if i > 0 {
			m = copyMessage(msg)
		}
		var err error
		if strict {
			err = pool.SubmitStrict(m)
		} else {
			err = pool.Submit(m)
		}
		if err != nil {
			// Submit/SubmitStrict already released the message on error.
			// Release remaining copies (i>0) and the original (i==0).
			if i == 0 {
				message.ReleaseMessage(msg)
			}
			return fmt.Errorf("submit to sink pool: %w", err)
		}
	}
	return nil
}

func copyMessage(src *message.Message) *message.Message {
	dst := message.AcquireMessage()
	dst.RequestID = src.RequestID
	dst.TraceID = src.TraceID
	dst.Project = src.Project
	dst.Router = src.Router
	if len(src.Data) > 0 {
		dst.Data = make([]byte, len(src.Data))
		copy(dst.Data, src.Data)
	}
	dst.Timestamp = src.Timestamp
	dst.Env = src.Env
	for k, v := range src.Headers {
		dst.Headers[k] = v
	}
	return dst
}

func (d *Dispatcher) buildPipelineChain(projCfg *config.ProjectConfig) *pipeline.Chain {
	chain := pipeline.NewChain()
	for _, ref := range projCfg.Pipelines {
		proc, err := d.pipelineReg.Create(ref.Type, ref.Config)
		if err != nil {
			log.Printf("[WARN] skip pipeline %s for project %s: %v", ref.Type, projCfg.Name, err)
			continue
		}
		chain.Add(proc)
	}
	return chain
}

func (d *Dispatcher) Shutdown() error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for name, pools := range d.pools {
		for _, pool := range pools {
			log.Printf("[INFO] shutting down worker pool for project %s", name)
			if err := pool.Shutdown(10 * time.Second); err != nil {
				log.Printf("[WARN] shutdown pool error: %v", err)
			}
		}
	}
	return nil
}

func (d *Dispatcher) GetPoolStatus() map[string]float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	status := make(map[string]float64)
	for name, pools := range d.pools {
		var maxUsage float64
		for _, pool := range pools {
			if u := pool.ChannelUsage(); u > maxUsage {
				maxUsage = u
			}
		}
		status[name] = maxUsage
	}
	return status
}

func readPoolInt(cfg map[string]interface{}, key string, def int) int {
	if v, ok := cfg[key]; ok {
		switch val := v.(type) {
		case float64:
			return int(val)
		case int:
			return val
		}
	}
	return def
}
