// Package pipeline provides a chain of message processors that run before delivery.
package pipeline

import (
	"fmt"

	"github.com/dongrv/logateway/internal/message"
)

// Processor transforms a message before it is delivered to sinks.
// Returning an error may drop the message depending on pipeline configuration.
type Processor interface {
	Process(msg *message.Message) (*message.Message, error)
	Name() string
}

// Chain runs a series of processors sequentially.
type Chain struct {
	processors []Processor
}

// NewChain creates a new processor chain.
func NewChain(processors ...Processor) *Chain {
	return &Chain{processors: processors}
}

// Process runs all processors in order. If any processor returns an error,
// processing stops and the error is returned.
func (c *Chain) Process(msg *message.Message) (*message.Message, error) {
	var err error
	for _, p := range c.processors {
		msg, err = p.Process(msg)
		if err != nil {
			return nil, fmt.Errorf("pipeline %s: %w", p.Name(), err)
		}
	}
	return msg, nil
}

// Add appends a processor to the chain.
func (c *Chain) Add(p Processor) {
	c.processors = append(c.processors, p)
}

// Len returns the number of processors in the chain.
func (c *Chain) Len() int {
	return len(c.processors)
}
