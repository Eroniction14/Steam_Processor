package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

// mockSink collects results for testing.
type mockSink struct {
	mu      sync.Mutex
	results []*AggregatedResult
}

func (s *mockSink) Name() string { return "mock" }
func (s *mockSink) Write(_ context.Context, r *AggregatedResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, r)
	return nil
}
func (s *mockSink) Flush(_ context.Context) error { return nil }
func (s *mockSink) Close() error                  { return nil }

func TestAggregator_CountsEvents(t *testing.T) {
	sink := &mockSink{}
	agg := NewAggregator(1*time.Minute, sink)

	ctx := context.Background()
	now := time.Now().Add(-2 * time.Minute) // in the past so window is expired

	for i := 0; i < 5; i++ {
		event := &Event{
			ID:        string(rune('a' + i)),
			Type:      EventPageView,
			UserID:    "user-1",
			Timestamp: now,
			Payload:   Payload{Page: "/home", Duration: 100},
		}
		if _, err := agg.Process(ctx, event); err != nil {
			t.Fatal(err)
		}
	}

	if err := agg.FlushExpired(ctx); err != nil {
		t.Fatal(err)
	}

	if len(sink.results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(sink.results))
	}

	r := sink.results[0]
	if r.Count != 5 {
		t.Errorf("expected count 5, got %d", r.Count)
	}
	if r.UniqueUsers != 1 {
		t.Errorf("expected 1 unique user, got %d", r.UniqueUsers)
	}
}

func TestAggregator_UniqueUsers(t *testing.T) {
	sink := &mockSink{}
	agg := NewAggregator(1*time.Minute, sink)
	ctx := context.Background()
	now := time.Now().Add(-2 * time.Minute)

	users := []string{"u1", "u2", "u3", "u2", "u1"}
	for i, uid := range users {
		event := &Event{
			ID:        string(rune('a' + i)),
			Type:      EventPageView,
			UserID:    uid,
			Timestamp: now,
		}
		agg.Process(ctx, event)
	}

	agg.FlushExpired(ctx)

	if len(sink.results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(sink.results))
	}
	if sink.results[0].UniqueUsers != 3 {
		t.Errorf("expected 3 unique users, got %d", sink.results[0].UniqueUsers)
	}
}

func TestAggregator_SeparateWindows(t *testing.T) {
	sink := &mockSink{}
	agg := NewAggregator(1*time.Minute, sink)
	ctx := context.Background()

	// Two events in different windows
	t1 := time.Date(2025, 1, 1, 12, 0, 30, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 12, 1, 30, 0, time.UTC)

	agg.Process(ctx, &Event{ID: "a", Type: EventPageView, UserID: "u1", Timestamp: t1})
	agg.Process(ctx, &Event{ID: "b", Type: EventPageView, UserID: "u2", Timestamp: t2})

	agg.FlushExpired(ctx)

	if len(sink.results) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(sink.results))
	}
}

func TestAggregator_AvgDuration(t *testing.T) {
	sink := &mockSink{}
	agg := NewAggregator(1*time.Minute, sink)
	ctx := context.Background()
	now := time.Now().Add(-2 * time.Minute)

	durations := []int{100, 200, 300}
	for i, d := range durations {
		agg.Process(ctx, &Event{
			ID:        string(rune('a' + i)),
			Type:      EventPageView,
			UserID:    "u1",
			Timestamp: now,
			Payload:   Payload{Duration: d},
		})
	}

	agg.FlushExpired(ctx)

	if len(sink.results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(sink.results))
	}
	if sink.results[0].AvgDuration != 200.0 {
		t.Errorf("expected avg duration 200, got %.1f", sink.results[0].AvgDuration)
	}
}
