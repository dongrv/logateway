// Package message defines the internal message structure and object pool
// for the logateway message gateway.
package message

import (
	"encoding/json"
	"sync"
	"time"
)

// Message is the internal representation of a log upload request after parsing.
type Message struct {
	RequestID string            `json:"request_id"`
	TraceID   string            `json:"trace_id"`
	Project   string            `json:"project"`
	Router    string            `json:"router"`
	Data      json.RawMessage   `json:"data"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
}

// GatewayMeta is embedded into the final delivery payload for downstream consumers.
type GatewayMeta struct {
	RequestID  string    `json:"request_id"`
	TraceID    string    `json:"trace_id"`
	ReceivedAt time.Time `json:"received_at"`
}

// Envelope is the final format delivered to sinks, wrapping the original payload
// with gateway metadata for compatibility with PHP consumers.
type Envelope struct {
	GatewayMeta GatewayMeta     `json:"_gateway_meta"`
	Project     string          `json:"Project"`
	Router      string          `json:"Router"`
	Data        json.RawMessage `json:"Data"`
}

// UploadRequest is the raw HTTP request body structure.
type UploadRequest struct {
	Project string          `json:"Project"`
	Router  string          `json:"Router"`
	Data    json.RawMessage `json:"Data"`
}

// UploadResponse is the standard HTTP response for the upload endpoint.
type UploadResponse struct {
	Code      int    `json:"Code"`
	Message   string `json:"Message"`
	RequestID string `json:"RequestId"`
	TraceID   string `json:"TraceId,omitempty"`
}

// messagePool is a sync.Pool for reusing Message objects to reduce GC pressure.
var messagePool = sync.Pool{
	New: func() interface{} {
		return &Message{
			Headers: make(map[string]string, 4),
		}
	},
}

// AcquireMessage gets a Message from the pool.
func AcquireMessage() *Message {
	return messagePool.Get().(*Message)
}

// ReleaseMessage returns a Message to the pool after resetting it.
func ReleaseMessage(msg *Message) {
	msg.RequestID = ""
	msg.TraceID = ""
	msg.Project = ""
	msg.Router = ""
	msg.Data = nil
	for k := range msg.Headers {
		delete(msg.Headers, k)
	}
	msg.Timestamp = time.Time{}
	messagePool.Put(msg)
}

// envelopePool is a sync.Pool for Envelope objects.
var envelopePool = sync.Pool{
	New: func() interface{} {
		return &Envelope{}
	},
}

// AcquireEnvelope gets an Envelope from the pool.
func AcquireEnvelope() *Envelope {
	return envelopePool.Get().(*Envelope)
}

// ReleaseEnvelope returns an Envelope to the pool.
func ReleaseEnvelope(env *Envelope) {
	env.GatewayMeta = GatewayMeta{}
	env.Project = ""
	env.Router = ""
	env.Data = nil
	envelopePool.Put(env)
}
