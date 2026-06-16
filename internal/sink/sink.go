// Package sink defines the Sink interface and provides worker pool management
// for asynchronous message delivery to downstream systems.
package sink

import (
	"context"
	"fmt"
	"sync"

	"github.com/dongrv/logateway/internal/message"
)

// Sink is the interface that all message delivery backends must implement.
type Sink interface {
	// Send delivers a message to the backend.
	Send(ctx context.Context, msg *message.Message) error
	// Name returns the unique name of this sink instance.
	Name() string
	// HealthCheck verifies connectivity to the backend.
	HealthCheck() error
	// Close gracefully shuts down the sink, flushing pending messages.
	Close() error
}

// Registry maintains a registry of named Sink factories.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

// Factory is a function that creates a Sink from a configuration map.
type Factory func(name string, cfg map[string]interface{}) (Sink, error)

// NewRegistry creates a new sink registry.
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]Factory),
	}
}

// Register adds a sink factory to the registry.
func (r *Registry) Register(sinkType string, factory Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[sinkType] = factory
}

// Create instantiates a sink by type name.
func (r *Registry) Create(sinkType, name string, cfg map[string]interface{}) (Sink, error) {
	r.mu.RLock()
	factory, ok := r.factories[sinkType]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown sink type: %s", sinkType)
	}
	return factory(name, cfg)
}
