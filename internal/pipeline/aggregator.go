package pipeline

import (
	"context"
	"sync"
	"time"
)

// Window represents a time window for aggregation.
type Window struct {
	Start time.Time
	End   time.Time
}

func (w Window) Contains(t time.Time) bool {
	return !t.Before(w.Start) && t.Before(w.End)
}

// windowState holds the mutable state for a single window.
type windowState struct {
	eventType   EventType
	count       int64
	uniqueUsers map[string]bool
	totalAmount float64
	totalDurMs  int64
}

// Aggregator performs windowed aggregation of events.
type Aggregator struct {
	mu         sync.Mutex
	windowSize time.Duration
	windows    map[Window]*windowState
	sink       Sink
}

func NewAggregator(windowSize time.Duration, sink Sink) *Aggregator {
	return &Aggregator{
		windowSize: windowSize,
		windows:    make(map[Window]*windowState),
		sink:       sink,
	}
}

func (a *Aggregator) Name() string { return "aggregator" }

func (a *Aggregator) Process(ctx context.Context, event *Event) (*Event, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	w := a.windowFor(event.Timestamp)
	state, ok := a.windows[w]
	if !ok {
		state = &windowState{
			eventType:   event.Type,
			uniqueUsers: make(map[string]bool),
		}
		a.windows[w] = state
	}

	state.count++
	state.uniqueUsers[event.UserID] = true
	state.totalAmount += event.Payload.Amount
	state.totalDurMs += int64(event.Payload.Duration)

	return event, nil
}

func (a *Aggregator) windowFor(t time.Time) Window {
	start := t.Truncate(a.windowSize)
	return Window{Start: start, End: start.Add(a.windowSize)}
}

// FlushExpired emits completed windows to the sink and removes them.
func (a *Aggregator) FlushExpired(ctx context.Context) error {
	a.mu.Lock()

	now := time.Now()
	var toFlush []struct {
		w Window
		s *windowState
	}

	for w, s := range a.windows {
		if now.After(w.End) {
			toFlush = append(toFlush, struct {
				w Window
				s *windowState
			}{w, s})
			delete(a.windows, w)
		}
	}
	a.mu.Unlock()

	for _, item := range toFlush {
		avgDur := float64(0)
		if item.s.count > 0 {
			avgDur = float64(item.s.totalDurMs) / float64(item.s.count)
		}

		result := &AggregatedResult{
			WindowStart: item.w.Start,
			WindowEnd:   item.w.End,
			EventType:   item.s.eventType,
			Count:       item.s.count,
			UniqueUsers: int64(len(item.s.uniqueUsers)),
			TotalAmount: item.s.totalAmount,
			AvgDuration: avgDur,
		}

		if err := a.sink.Write(ctx, result); err != nil {
			return err
		}
	}

	return nil
}

// RunFlusher starts a background goroutine that periodically flushes expired windows.
// It also flushes the sink after each cycle to ensure buffered results are written.
func (a *Aggregator) RunFlusher(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown
			_ = a.FlushExpired(context.Background())
			_ = a.sink.Flush(context.Background())
			return
		case <-ticker.C:
			_ = a.FlushExpired(ctx)
			_ = a.sink.Flush(ctx)
		}
	}
}
