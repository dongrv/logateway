// Package config manages gateway configuration loading, hot-reloading,
// and thread-safe access via atomic.Value.
package config

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

// Config is the top-level gateway configuration.
type Config struct {
	Server        ServerConfig                  `yaml:"server"`
	Auth          AuthConfig                    `yaml:"auth"`
	Projects      []ProjectConfig               `yaml:"projects"`
	Sinks         SinksConfig                   `yaml:"sinks"`
	SinkInstances map[string]SinkInstanceConfig `yaml:"sink_instances"`
	Pipeline      PipelineConfig                `yaml:"pipeline"`
	Log           LogConfig                     `yaml:"log"`
	Metrics       MetricsConfig                 `yaml:"metrics"`
	WAL           WALConfig                     `yaml:"wal"`
	Pprof         PprofConfig                   `yaml:"pprof"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	ListenAddr      string        `yaml:"listen_addr"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	MaxBodyBytes    int64         `yaml:"max_body_bytes"`
	MaxConnsPerIP   int           `yaml:"max_conns_per_ip"`
	GlobalRateLimit int           `yaml:"global_rate_limit"`
	AntsPoolSize    int           `yaml:"ants_pool_size"`
	Backpressure    string        `yaml:"backpressure"`
	IdleTimeout     time.Duration `yaml:"idle_timeout"`
	Env             string        `yaml:"env"`
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	Enabled         bool  `yaml:"enabled"`
	TimestampWindow int64 `yaml:"timestamp_window"`
	NonceTTLSeconds int   `yaml:"nonce_ttl_seconds"`
	NonceCacheSize  int   `yaml:"nonce_cache_size"`
}

// ProjectConfig defines a single project's configuration.
type ProjectConfig struct {
	Name         string        `yaml:"name"`
	Enabled      bool          `yaml:"enabled"`
	Sinks        []SinkRef     `yaml:"sinks"`
	RateLimit    int           `yaml:"rate_limit"`
	MaxBodyBytes int64         `yaml:"max_body_bytes"`
	AuthRequired bool          `yaml:"auth_required"`
	Pipelines    []PipelineRef `yaml:"pipelines"`
}

// SinkRef references a sink configuration for a project.
type SinkRef struct {
	Type        string                 `yaml:"type"`
	Instance    string                 `yaml:"instance"`
	Workers     int                    `yaml:"workers"`
	ChannelSize int                    `yaml:"channel_size"`
	Config      map[string]interface{} `yaml:"config"`
}

// SinkInstanceConfig defines a named, reusable sink instance.
type SinkInstanceConfig struct {
	Type        string                 `yaml:"type"`
	Workers     int                    `yaml:"workers"`
	ChannelSize int                    `yaml:"channel_size"`
	Config      map[string]interface{} `yaml:"config"`
}

// PipelineRef references a pipeline processor.
type PipelineRef struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

// SinksConfig holds predefined sink configurations used as global defaults.
type SinksConfig struct {
	Redis RedisSinkConfig `yaml:"redis"`
	Kafka KafkaSinkConfig `yaml:"kafka"`
}

// RedisSinkConfig is the Redis sink configuration.
type RedisSinkConfig struct {
	Addr         string        `yaml:"addr"`
	Password     string        `yaml:"password"`
	DB           int           `yaml:"db"`
	PoolSize     int           `yaml:"pool_size"`
	MinIdleConns int           `yaml:"min_idle_conns"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	Key          string        `yaml:"key"`
	Type         string        `yaml:"type"`
	MaxLen       int64         `yaml:"max_len"`
}

// KafkaSinkConfig is the Kafka sink configuration.
type KafkaSinkConfig struct {
	Brokers      []string      `yaml:"brokers"`
	Topic        string        `yaml:"topic"`
	PartitionKey string        `yaml:"partition_key"`
	Compression  string        `yaml:"compression"`
	BatchSize    int           `yaml:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`
}

// PipelineConfig holds pipeline-related settings.
type PipelineConfig struct {
	MaxDepth int `yaml:"max_depth"`
}

// LogConfig holds logging settings.
type LogConfig struct {
	Level   string         `yaml:"level"`
	Console ConsoleLogConf `yaml:"console"`
	File    FileLogConf    `yaml:"file"`
}

// ConsoleLogConf controls console logging output.
type ConsoleLogConf struct {
	Enabled bool   `yaml:"enabled"`
	Format  string `yaml:"format"`
}

// FileLogConf controls file-based logging output.
type FileLogConf struct {
	Enabled bool     `yaml:"enabled"`
	Dir     string   `yaml:"dir"`
	Levels  []string `yaml:"levels"`
	MaxAge  int      `yaml:"max_age"`
}

// MetricsConfig holds metrics settings.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
	Port    int    `yaml:"port"`
}

// PprofConfig controls Go pprof debugging endpoints.
type PprofConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

// WALConfig holds Write-Ahead Log settings for disk fallback.
type WALConfig struct {
	Enabled         bool          `yaml:"enabled"`
	Dir             string        `yaml:"dir"`
	MaxSegmentBytes int64         `yaml:"max_segment_bytes"`
	MaxSegments     int           `yaml:"max_segments"`
	SyncInterval    time.Duration `yaml:"sync_interval"`
}

// Manager manages configuration loading and hot-reloading.
type Manager struct {
	current   atomic.Value
	path      string
	watcher   *fsnotify.Watcher
	stopCh    chan struct{}
	closeOnce sync.Once
	reloadMu  sync.Mutex
	onReload  []func(*Config)
	watchMu   sync.Mutex
}

// NewManager creates a new config manager.
func NewManager(configPath string) (*Manager, error) {
	m := &Manager{
		path:   configPath,
		stopCh: make(chan struct{}),
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

// OnReload registers a callback invoked after each successful config reload.
func (m *Manager) OnReload(fn func(*Config)) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	m.onReload = append(m.onReload, fn)
}

func (m *Manager) load() error {
	data, err := os.ReadFile(m.path)
	if err != nil {
		return err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}
	m.applyDefaults(&cfg)

	m.reloadMu.Lock()
	callbacks := make([]func(*Config), len(m.onReload))
	copy(callbacks, m.onReload)
	m.reloadMu.Unlock()

	m.current.Store(&cfg)

	for _, fn := range callbacks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[ERROR] config reload callback panic: %v", r)
				}
			}()
			fn(&cfg)
		}()
	}
	return nil
}

// Reload re-reads the configuration file and updates the stored config.
func (m *Manager) Reload() error {
	return m.load()
}

// Watch starts watching the config file for changes using fsnotify.
func (m *Manager) Watch() error {
	m.watchMu.Lock()
	defer m.watchMu.Unlock()
	if m.watcher != nil {
		return nil
	}
	var err error
	m.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err := m.watcher.Add(m.path); err != nil {
		m.watcher.Close()
		m.watcher = nil
		return err
	}

	go m.watchLoop()
	log.Printf("[INFO] config watcher started for %s", m.path)
	return nil
}

func (m *Manager) watchLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] config watcher panic: %v", r)
		}
	}()

	var timer *time.Timer
	var timerCh <-chan time.Time

	for {
		select {
		case <-m.stopCh:
			if timer != nil {
				timer.Stop()
			}
			return
		case event, ok := <-m.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				if timer == nil {
					timer = time.NewTimer(500 * time.Millisecond)
					timerCh = timer.C
				} else {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(500 * time.Millisecond)
				}
			}
		case err, ok := <-m.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("[ERROR] config watcher error: %v", err)
		case <-timerCh:
			timer = nil
			timerCh = nil
			log.Printf("[INFO] config file changed, reloading...")
			if err := m.Reload(); err != nil {
				log.Printf("[ERROR] config reload failed: %v", err)
			} else {
				log.Printf("[INFO] config reloaded successfully")
			}
		}
	}
}

// Close stops the file watcher if active.
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		if m.watcher != nil {
			m.watcher.Close()
		}
		close(m.stopCh)
	})
}

// Get returns the current configuration (safe for concurrent use).
// The returned *Config must not be modified.
func (m *Manager) Get() *Config {
	return m.current.Load().(*Config)
}

// GetProject returns a project configuration by name, or nil if not found.
func (m *Manager) GetProject(name string) *ProjectConfig {
	cfg := m.Get()
	for i := range cfg.Projects {
		if cfg.Projects[i].Name == name && cfg.Projects[i].Enabled {
			return &cfg.Projects[i]
		}
	}
	return nil
}

func (m *Manager) applyDefaults(cfg *Config) {
	if cfg.Server.ListenAddr == "" {
		cfg.Server.ListenAddr = ":8080"
	}
	if cfg.Server.ReadTimeout == 0 {
		cfg.Server.ReadTimeout = 3 * time.Second
	}
	if cfg.Server.WriteTimeout == 0 {
		cfg.Server.WriteTimeout = 5 * time.Second
	}
	if cfg.Server.MaxBodyBytes == 0 {
		cfg.Server.MaxBodyBytes = 1 << 20
	}
	if cfg.Server.AntsPoolSize == 0 {
		cfg.Server.AntsPoolSize = 10000
	}
	if cfg.Auth.TimestampWindow == 0 {
		cfg.Auth.TimestampWindow = 300
	}
	if cfg.Auth.NonceTTLSeconds == 0 {
		cfg.Auth.NonceTTLSeconds = 300
	}
	if cfg.Auth.NonceCacheSize == 0 {
		cfg.Auth.NonceCacheSize = 100000
	}
	if cfg.Server.GlobalRateLimit <= 0 {
		cfg.Server.GlobalRateLimit = 20000
	}
	if cfg.Server.Backpressure == "" {
		cfg.Server.Backpressure = "drop"
	}
	if cfg.Server.IdleTimeout == 0 {
		cfg.Server.IdleTimeout = 120 * time.Second
	}
	if cfg.Log.File.Dir == "" {
		cfg.Log.File.Dir = "logs"
	}
	if cfg.WAL.Dir == "" {
		cfg.WAL.Dir = "data/wal"
	}
	if cfg.WAL.MaxSegmentBytes == 0 {
		cfg.WAL.MaxSegmentBytes = 64 << 20
	}
	if cfg.WAL.MaxSegments == 0 {
		cfg.WAL.MaxSegments = 10
	}
	if cfg.WAL.SyncInterval == 0 {
		cfg.WAL.SyncInterval = 100 * time.Millisecond
	}
	if cfg.Sinks.Redis.MinIdleConns == 0 {
		cfg.Sinks.Redis.MinIdleConns = 10
	}
	if cfg.Sinks.Redis.DialTimeout == 0 {
		cfg.Sinks.Redis.DialTimeout = 5 * time.Second
	}
	if cfg.Sinks.Redis.ReadTimeout == 0 {
		cfg.Sinks.Redis.ReadTimeout = 3 * time.Second
	}
	if cfg.Sinks.Redis.WriteTimeout == 0 {
		cfg.Sinks.Redis.WriteTimeout = 3 * time.Second
	}
	if cfg.Sinks.Redis.PoolSize == 0 {
		cfg.Sinks.Redis.PoolSize = 100
	}
}
