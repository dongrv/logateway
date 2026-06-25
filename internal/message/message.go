package message

import (
	"encoding/json"
	"sync"
	"time"
)

type Message struct {
	RequestID string            `json:"request_id"`
	TraceID   string            `json:"trace_id"`
	Project   string            `json:"project"`
	Router    string            `json:"router"`
	Data      json.RawMessage   `json:"data"`
	Env       string            `json:"env,omitempty"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
}

type GatewayMeta struct {
	RequestID  string    `json:"request_id"`
	TraceID    string    `json:"trace_id"`
	ReceivedAt time.Time `json:"received_at"`
	Env        string    `json:"env,omitempty"`
}

type Envelope struct {
	GatewayMeta GatewayMeta     `json:"_gateway_meta"`
	Project     string          `json:"project"`
	Router      string          `json:"router"`
	Data        json.RawMessage `json:"data"`
}

type UploadRequest struct {
	Project string          `json:"project"`
	Router  string          `json:"router"`
	Data    json.RawMessage `json:"data"`
}

type UploadResponse struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id,omitempty"`
}

var messagePool = sync.Pool{
	New: func() any {
		return &Message{Headers: make(map[string]string, 4)}
	},
}

func AcquireMessage() *Message {
	return messagePool.Get().(*Message)
}

func ReleaseMessage(msg *Message) {
	if msg == nil {
		return
	}
	msg.RequestID = ""
	msg.TraceID = ""
	msg.Project = ""
	msg.Router = ""
	msg.Data = nil
	msg.Env = ""
	for k := range msg.Headers {
		delete(msg.Headers, k)
	}
	msg.Timestamp = time.Time{}
	messagePool.Put(msg)
}

var envelopePool = sync.Pool{
	New: func() any {
		return &Envelope{}
	},
}

func AcquireEnvelope() *Envelope {
	return envelopePool.Get().(*Envelope)
}

func ReleaseEnvelope(env *Envelope) {
	env.GatewayMeta = GatewayMeta{}
	env.Project = ""
	env.Router = ""
	env.Data = nil
	envelopePool.Put(env)
}
