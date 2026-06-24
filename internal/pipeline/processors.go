// Package pipeline provides built-in message processors for field filtering,
// field addition, and data redaction.
package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dongrv/logateway/internal/message"
)

// ---------- FieldFilter (include/exclude) ----------

// FieldFilter keeps or removes specified fields from the Data payload.
type FieldFilter struct {
	fields []string
	mode   string // "include" or "exclude"
}

// NewFieldFilter creates a field filter processor.
func NewFieldFilter(mode string, fields []string) *FieldFilter {
	return &FieldFilter{mode: mode, fields: fields}
}

func (f *FieldFilter) Name() string { return "field_filter" }

func (f *FieldFilter) Process(msg *message.Message) (*message.Message, error) {
	if len(msg.Data) == 0 {
		return msg, nil
	}
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return msg, nil // non-object data, pass through unchanged
	}

	switch f.mode {
	case "include":
		filtered := make(map[string]interface{}, len(f.fields))
		for _, field := range f.fields {
			if v, ok := data[field]; ok {
				filtered[field] = v
			}
		}
		raw, err := json.Marshal(filtered)
		if err != nil {
			return nil, fmt.Errorf("field_filter marshal: %w", err)
		}
		msg.Data = raw
	case "exclude":
		for _, field := range f.fields {
			delete(data, field)
		}
		raw, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("field_filter marshal: %w", err)
		}
		msg.Data = raw
	}
	return msg, nil
}

// ---------- FieldAdd ----------

// FieldAdd adds fixed key-value pairs to the message Data.
type FieldAdd struct {
	fields map[string]interface{}
}

// NewFieldAdd creates a field-add processor.
func NewFieldAdd(fields map[string]interface{}) *FieldAdd {
	return &FieldAdd{fields: fields}
}

func (f *FieldAdd) Name() string { return "field_add" }

func (f *FieldAdd) Process(msg *message.Message) (*message.Message, error) {
	var data map[string]interface{}
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			data = make(map[string]interface{})
		}
	} else {
		data = make(map[string]interface{})
	}
	for k, v := range f.fields {
		data[k] = v
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("field_add marshal: %w", err)
	}
	msg.Data = raw
	return msg, nil
}

// ---------- FieldRedact ----------

// FieldRedact replaces sensitive field values with a mask string.
type FieldRedact struct {
	fields map[string]string
}

// NewFieldRedact creates a field redaction processor.
func NewFieldRedact(fields map[string]string) *FieldRedact {
	return &FieldRedact{fields: fields}
}

func (f *FieldRedact) Name() string { return "field_redact" }

func (f *FieldRedact) Process(msg *message.Message) (*message.Message, error) {
	if len(msg.Data) == 0 {
		return msg, nil
	}
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return msg, nil
	}
	for field, mask := range f.fields {
		if _, ok := data[field]; ok {
			data[field] = mask
		}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("field_redact marshal: %w", err)
	}
	msg.Data = raw
	return msg, nil
}

// ---------- Registry ----------

// Registry maintains a registry of named processor factories.
type Registry struct {
	factories map[string]Factory
}

// Factory creates a Processor from a configuration map.
type Factory func(cfg map[string]interface{}) (Processor, error)

// NewRegistry creates a new processor registry with built-in processors.
func NewRegistry() *Registry {
	r := &Registry{
		factories: make(map[string]Factory),
	}
	r.registerBuiltins()
	return r
}

func (r *Registry) registerBuiltins() {
	r.Register("field_filter", func(cfg map[string]interface{}) (Processor, error) {
		mode, ok := cfg["mode"].(string); if !ok { mode = "exclude" }
		if mode != "include" && mode != "exclude" {
			mode = "exclude"
		}
		var fields []string
		if raw, ok := cfg["fields"].([]interface{}); ok {
			for _, v := range raw {
				if s, ok := v.(string); ok {
					fields = append(fields, s)
				}
			}
		}
		return NewFieldFilter(mode, fields), nil
	})

	r.Register("field_add", func(cfg map[string]interface{}) (Processor, error) {
		fields := make(map[string]interface{})
		if raw, ok := cfg["fields"].(map[string]interface{}); ok {
			for k, v := range raw {
				fields[k] = v
			}
		}
		return NewFieldAdd(fields), nil
	})

	r.Register("field_redact", func(cfg map[string]interface{}) (Processor, error) {
		fields := make(map[string]string)
		if raw, ok := cfg["fields"].(map[string]interface{}); ok {
			for k, v := range raw {
				if s, ok := v.(string); ok {
					fields[k] = s
				}
			}
		}
		return NewFieldRedact(fields), nil
	})
}

// Register adds a processor factory. Not safe for concurrent use after initialization.
func (r *Registry) Register(name string, factory Factory) {
	r.factories[name] = factory
}

// Create instantiates a processor by type name.
func (r *Registry) Create(typ string, cfg map[string]interface{}) (Processor, error) {
	factory, ok := r.factories[typ]
	if !ok {
		return nil, fmt.Errorf("unknown pipeline processor: %s (available: %s)", typ, r.listAvailable())
	}
	return factory(cfg)
}

func (r *Registry) listAvailable() string {
	names := make([]string, 0, len(r.factories))
	for name := range r.factories {
		names = append(names, name)
	}
	return strings.Join(names, ", ")
}
