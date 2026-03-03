package pipeline

import (
	"context"
	"fmt"
	"time"
)

// --- Deserializer Stage ---

type Deserializer struct{}

func (d *Deserializer) Name() string { return "deserializer" }

func (d *Deserializer) Process(ctx context.Context, event *Event) (*Event, error) {
	if event.ID == "" {
		return nil, fmt.Errorf("event missing ID")
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	return event, nil
}

// --- Router Stage (filters by event type) ---

type Router struct {
	allowedTypes map[EventType]bool
}

func NewRouter(types ...EventType) *Router {
	m := make(map[EventType]bool, len(types))
	for _, t := range types {
		m[t] = true
	}
	return &Router{allowedTypes: m}
}

func (r *Router) Name() string { return "router" }

func (r *Router) Process(ctx context.Context, event *Event) (*Event, error) {
	if len(r.allowedTypes) == 0 {
		return event, nil // no filter, pass everything
	}
	if !r.allowedTypes[event.Type] {
		return nil, nil // filtered out
	}
	return event, nil
}

// --- Enricher Stage (joins with state store) ---

type Enricher struct {
	store StateStore
}

type StateStore interface {
	Get(key string) (interface{}, bool)
	Put(key string, value interface{})
}

func NewEnricher(store StateStore) *Enricher {
	return &Enricher{store: store}
}

func (e *Enricher) Name() string { return "enricher" }

func (e *Enricher) Process(ctx context.Context, event *Event) (*Event, error) {
	if userData, ok := e.store.Get("user:" + event.UserID); ok {
		if meta, ok := userData.(map[string]string); ok {
			if event.Payload.Action == "" {
				event.Payload.Action = meta["segment"]
			}
		}
	}
	return event, nil
}
