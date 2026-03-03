package pipeline

import (
	"context"
	"testing"
)

func TestRouter_FiltersEvents(t *testing.T) {
	router := NewRouter(EventPageView)
	ctx := context.Background()

	// Should pass through
	pv := &Event{Type: EventPageView}
	result, err := router.Process(ctx, pv)
	if err != nil || result == nil {
		t.Error("page_view should pass through router")
	}

	// Should be filtered
	tx := &Event{Type: EventTransaction}
	result, err = router.Process(ctx, tx)
	if err != nil || result != nil {
		t.Error("transaction should be filtered out")
	}
}

func TestRouter_EmptyAllowsAll(t *testing.T) {
	router := NewRouter() // no types specified
	ctx := context.Background()

	event := &Event{Type: EventTransaction}
	result, err := router.Process(ctx, event)
	if err != nil || result == nil {
		t.Error("empty router should pass all events")
	}
}

func TestDeserializer_RejectsEmptyID(t *testing.T) {
	d := &Deserializer{}
	ctx := context.Background()

	_, err := d.Process(ctx, &Event{})
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestDeserializer_SetsTimestamp(t *testing.T) {
	d := &Deserializer{}
	ctx := context.Background()

	event := &Event{ID: "test-1"}
	result, err := d.Process(ctx, event)
	if err != nil {
		t.Fatal(err)
	}
	if result.Timestamp.IsZero() {
		t.Error("expected timestamp to be set")
	}
}

func TestPipeline_StageError(t *testing.T) {
	sink := &mockSink{}
	d := &Deserializer{}
	p := NewPipeline(sink, d)

	err := p.Process(context.Background(), &Event{}) // empty ID
	if err == nil {
		t.Fatal("expected stage error")
	}

	stageErr, ok := err.(*StageError)
	if !ok {
		t.Fatal("expected *StageError")
	}
	if stageErr.Stage != "deserializer" {
		t.Errorf("expected stage 'deserializer', got '%s'", stageErr.Stage)
	}
}

func TestPipeline_FilteredEventStopsProcessing(t *testing.T) {
	sink := &mockSink{}
	router := NewRouter(EventPageView) // only allows page_view
	d := &Deserializer{}
	p := NewPipeline(sink, d, router)

	// Transaction event — should be filtered by router, no error
	event := &Event{ID: "tx-1", Type: EventTransaction}
	err := p.Process(context.Background(), event)
	if err != nil {
		t.Errorf("filtered event should not produce an error, got: %v", err)
	}
}
