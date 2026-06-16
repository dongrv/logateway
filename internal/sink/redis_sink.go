package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dongrv/logateway/internal/message"
	"github.com/redis/go-redis/v9"
)

// RedisSink delivers messages to Redis (List or Stream).
type RedisSink struct {
	name   string
	client *redis.Client
	key    string
	typ    string // "list" or "stream"
	maxLen int64
}

// RedisConfig holds the configuration for creating a RedisSink.
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
	Type         string // list or stream
	MaxLen       int64
}

// NewRedisSink creates a new Redis sink.
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

// Name returns the sink name.
func (s *RedisSink) Name() string {
	return s.name
}

// Send delivers a message to Redis.
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
	default: // list
		return s.client.LPush(ctx, s.key, string(data)).Err()
	}
}

// HealthCheck pings Redis.
func (s *RedisSink) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.client.Ping(ctx).Err()
}

// Close closes the Redis client connection pool.
func (s *RedisSink) Close() error {
	return s.client.Close()
}

// RedisSinkFactory creates RedisSink instances from config maps.
func RedisSinkFactory(name string, cfg map[string]interface{}) (Sink, error) {
	redisCfg := RedisConfig{
		Type: "list",
	}
	if v, ok := cfg["addr"].(string); ok {
		redisCfg.Addr = v
	}
	if v, ok := cfg["password"].(string); ok {
		redisCfg.Password = v
	}
	if v, ok := cfg["key"].(string); ok {
		redisCfg.Key = v
	}
	if v, ok := cfg["type"].(string); ok {
		redisCfg.Type = v
	}
	if v, ok := cfg["pool_size"].(float64); ok {
		redisCfg.PoolSize = int(v)
	}
	if v, ok := cfg["min_idle_conns"].(float64); ok {
		redisCfg.MinIdleConns = int(v)
	}
	if v, ok := cfg["max_len"].(float64); ok {
		redisCfg.MaxLen = int64(v)
	}
	if v, ok := cfg["db"].(float64); ok {
		redisCfg.DB = int(v)
	}
	if v, ok := cfg["dial_timeout"].(string); ok {
		d, _ := time.ParseDuration(v)
		redisCfg.DialTimeout = d
	}
	if v, ok := cfg["read_timeout"].(string); ok {
		d, _ := time.ParseDuration(v)
		redisCfg.ReadTimeout = d
	}
	if v, ok := cfg["write_timeout"].(string); ok {
		d, _ := time.ParseDuration(v)
		redisCfg.WriteTimeout = d
	}
	return NewRedisSink(name, redisCfg)
}

func buildEnvelope(msg *message.Message) *message.Envelope {
	env := message.AcquireEnvelope()
	env.GatewayMeta = message.GatewayMeta{
		RequestID:  msg.RequestID,
		TraceID:    msg.TraceID,
		ReceivedAt: msg.Timestamp,
	}
	env.Project = msg.Project
	env.Router = msg.Router
	env.Data = msg.Data
	return env
}
