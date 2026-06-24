package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/dongrv/logateway/internal/message"
	"github.com/redis/go-redis/v9"
)

type RedisSink struct {
	name   string
	client *redis.Client
	key    string
	typ    string
	maxLen int64
}

type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Key          string
	Type         string
	MaxLen       int64
}

func NewRedisSink(name string, cfg RedisConfig) (*RedisSink, error) {
	if cfg.Type == "" {
		cfg.Type = "list"
	}
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
	return &RedisSink{
		name:   name,
		client: client,
		key:    cfg.Key,
		typ:    cfg.Type,
		maxLen: cfg.MaxLen,
	}, nil
}

func (s *RedisSink) Name() string { return s.name }

func (s *RedisSink) Send(ctx context.Context, msg *message.Message) error {
	envelope := buildEnvelope(msg)
	data, err := json.Marshal(envelope)
	message.ReleaseEnvelope(envelope)
	if err != nil {
		return fmt.Errorf("redis sink marshal: %w", err)
	}

	switch s.typ {
	case "stream":
		return s.client.XAdd(ctx, &redis.XAddArgs{
			Stream: s.key,
			MaxLen: s.maxLen,
			Approx: s.maxLen > 0,
			Values: map[string]interface{}{
				"data": string(data),
			},
		}).Err()
	default:
		return s.client.LPush(ctx, s.key, string(data)).Err()
	}
}

func (s *RedisSink) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.client.Ping(ctx).Err()
}

func (s *RedisSink) Close() error {
	return s.client.Close()
}

func RedisSinkFactory(name string, cfg map[string]interface{}) (Sink, error) {
	redisCfg := RedisConfig{Type: "list"}

	if v, ok := cfg["addr"].(string); ok {
		redisCfg.Addr = v
	} else if _, exists := cfg["addr"]; exists {
		log.Printf("[WARN] redis factory: addr has unexpected type %T", cfg["addr"])
	}
	if v, ok := cfg["password"].(string); ok {
		redisCfg.Password = v
	}
	if v, ok := cfg["key"].(string); ok {
		redisCfg.Key = v
	} else if _, exists := cfg["key"]; exists {
		log.Printf("[WARN] redis factory: key has unexpected type %T", cfg["key"])
	}
	if v, ok := cfg["type"].(string); ok {
		redisCfg.Type = v
	}

	redisCfg.PoolSize = intConfig(cfg, "pool_size", redisCfg.PoolSize)
	redisCfg.MinIdleConns = intConfig(cfg, "min_idle_conns", redisCfg.MinIdleConns)
	redisCfg.DB = intConfig(cfg, "db", redisCfg.DB)
	redisCfg.MaxLen = int64Config(cfg, "max_len", redisCfg.MaxLen)
	redisCfg.DialTimeout = durationConfig(cfg, "dial_timeout", redisCfg.DialTimeout)
	redisCfg.ReadTimeout = durationConfig(cfg, "read_timeout", redisCfg.ReadTimeout)
	redisCfg.WriteTimeout = durationConfig(cfg, "write_timeout", redisCfg.WriteTimeout)

	return NewRedisSink(name, redisCfg)
}

func intConfig(cfg map[string]interface{}, key string, def int) int {
	v, exists := cfg[key]
	if !exists {
		return def
	}
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	default:
		log.Printf("[WARN] config field %q has unexpected type %T, using default %d", key, v, def)
		return def
	}
}

func int64Config(cfg map[string]interface{}, key string, def int64) int64 {
	v, exists := cfg[key]
	if !exists {
		return def
	}
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	default:
		log.Printf("[WARN] config field %q has unexpected type %T, using default %d", key, v, def)
		return def
	}
}

func durationConfig(cfg map[string]interface{}, key string, def time.Duration) time.Duration {
	v, ok := cfg[key].(string)
	if !ok {
		if _, exists := cfg[key]; exists {
			log.Printf("[WARN] config field %q has unexpected type %T, using default %v", key, cfg[key], def)
		}
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("[WARN] config field %q has invalid duration %q: %v, using default %v", key, v, err, def)
		return def
	}
	return d
}

func buildEnvelope(msg *message.Message) *message.Envelope {
	env := message.AcquireEnvelope()
	env.GatewayMeta = message.GatewayMeta{
		RequestID:  msg.RequestID,
		TraceID:    msg.TraceID,
		ReceivedAt: msg.Timestamp,
		Env:        msg.Env,
	}
	env.Project = msg.Project
	env.Router = msg.Router
	env.Data = msg.Data
	return env
}
