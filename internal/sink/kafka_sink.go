package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/dongrv/logateway/internal/message"
	"github.com/segmentio/kafka-go"
)

type KafkaSink struct {
	name         string
	writer       *kafka.Writer
	partitionKey string
}

type KafkaConfig struct {
	Brokers      []string
	Topic        string
	PartitionKey string
	Compression  string
	BatchSize    int
	BatchTimeout time.Duration
}

func NewKafkaSink(name string, cfg KafkaConfig) (*KafkaSink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka topic required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 100 * time.Millisecond
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	switch cfg.Compression {
	case "gzip":
		writer.Compression = kafka.Gzip
	case "snappy":
		writer.Compression = kafka.Snappy
	case "lz4":
		writer.Compression = kafka.Lz4
	case "zstd":
		writer.Compression = kafka.Zstd
	}

	return &KafkaSink{
		name:         name,
		writer:       writer,
		partitionKey: cfg.PartitionKey,
	}, nil
}

func (s *KafkaSink) Name() string { return s.name }

func (s *KafkaSink) Send(ctx context.Context, msg *message.Message) error {
	envelope := buildEnvelope(msg)
	data, err := json.Marshal(envelope)
	message.ReleaseEnvelope(envelope)
	if err != nil {
		return fmt.Errorf("kafka sink marshal: %w", err)
	}

	key := s.partitionKeyBytes(msg)

	return s.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: data,
	})
}

func (s *KafkaSink) partitionKeyBytes(msg *message.Message) []byte {
	if len(msg.Data) == 0 {
		return []byte(msg.RequestID)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return []byte(msg.RequestID)
	}

	if s.partitionKey != "" {
		if v, ok := data[s.partitionKey]; ok {
			return []byte(fmt.Sprint(v))
		}
	}

	for _, field := range []string{"UID", "uid", "user_id", "UserID"} {
		if v, ok := data[field]; ok {
			return []byte(fmt.Sprint(v))
		}
	}

	return []byte(msg.RequestID)
}

func (s *KafkaSink) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	addr := s.writer.Addr.String()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("kafka broker unreachable: %w", err)
	}
	conn.Close()
	return nil
}

func (s *KafkaSink) Close() error {
	return s.writer.Close()
}

func KafkaSinkFactory(name string, rawCfg map[string]interface{}) (Sink, error) {
	cfg := KafkaConfig{
		BatchSize:    100,
		BatchTimeout: 100 * time.Millisecond,
	}
	if v, ok := rawCfg["brokers"].([]interface{}); ok {
		for _, b := range v {
			if s, ok := b.(string); ok {
				cfg.Brokers = append(cfg.Brokers, s)
			}
		}
	} else if _, exists := rawCfg["brokers"]; exists {
		log.Printf("[WARN] kafka factory: brokers has unexpected type %T", rawCfg["brokers"])
	}
	if v, ok := rawCfg["topic"].(string); ok {
		cfg.Topic = v
	} else if _, exists := rawCfg["topic"]; exists {
		log.Printf("[WARN] kafka factory: topic has unexpected type %T", rawCfg["topic"])
	}
	if v, ok := rawCfg["partition_key"].(string); ok {
		cfg.PartitionKey = v
	}
	if v, ok := rawCfg["compression"].(string); ok {
		cfg.Compression = v
	}
	cfg.BatchSize = intConfig(rawCfg, "batch_size", cfg.BatchSize)
	if v, ok := rawCfg["batch_timeout"].(string); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Printf("[WARN] kafka factory: invalid batch_timeout %q: %v", v, err)
		} else {
			cfg.BatchTimeout = d
		}
	}
	return NewKafkaSink(name, cfg)
}
