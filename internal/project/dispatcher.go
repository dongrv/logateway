// Package project handles project resolution, message routing, and dispatching
// to the appropriate sink worker pools.
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

// SinkInfo exposes a sink instance for health checking.
type SinkInfo struct {
	Name string
	Sink sink.Sink
}

// Dispatcher resolves project configurations and routes messages to sink worker pools.
type Dispatcher struct {
	cfg         *config.Manager
	reg         *sink.Registry
	pipelineReg *pipeline.Registry
	mu          sync.RWMutex
	pools       map[string][]*sink.WorkerPool
	walWriter   *wal.Writer
	bp          sink.Backpressure
}

// NewDispatcher creates a new project dispatcher.
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

// PipelineRegistry returns the pipeline processor registry for custom processor registration.
func (d *Dispatcher) PipelineRegistry() *pipeline.Registry {
	return d.pipelineReg
}

// Initialize creates worker pools for all enabled projects.
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
		sinkName := fmt.Sprintf("%s-%s-%d", projCfg.Name, sr.Type, i)
		si, err := d.reg.Create(sr.Type, sinkName, sr.Config)
		if err != nil {
			return fmt.Errorf("create sink %s: %w", sinkName, err)
		}

		pool := sink.NewWorkerPool(sink.WorkerPoolConfig{
			Sink:          si,
			Workers:       4,
			ChannelSize:   4096,
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

// SinkInfos returns all sink instances for health check registration.
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

// Dispatch routes a message to all configured sinks for the project asynchronously.
func (d *Dispatcher) Dispatch(msg *message.Message) error {
	d.mu.RLock()
	pools, ok := d.pools[msg.Project]
	d.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no sinks configured for project %s", msg.Project)
	}

	// Run pipelines if configured
	projCfg := d.cfg.GetProject(msg.Project)
	if projCfg != nil && len(projCfg.Pipelines) > 0 {
		chain := d.buildPipelineChain(projCfg)
		var err error
		msg, err = chain.Process(msg)
		if err != nil {
			return fmt.Errorf("pipeline processing: %w", err)
		}
	}

	for _, pool := range pools {
		if err := pool.Submit(msg); err != nil {
			return fmt.Errorf("submit to sink pool: %w", err)
		}
	}
	return nil
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

// Shutdown gracefully shuts down all worker pools.
func (d *Dispatcher) Shutdown() error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	for name, pools := range d.pools {
		for _, pool := range pools {
			log.Printf("[INFO] shutting down worker pool for project %s", name)
			if err := pool.Shutdown(0); err != nil {
				log.Printf("[WARN] shutdown pool error: %v", err)
			}
		}
	}
	return nil
}

// GetPoolStatus returns a map of project -> channel usage ratios.
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
