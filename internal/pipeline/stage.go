package pipeline

import "context"

// Stage represents a single processing step in the pipeline.
type Stage interface {
	Name() string
	Process(ctx context.Context, event *Event) (*Event, error)
}

// Sink consumes final results from the pipeline.
type Sink interface {
	Name() string
	Write(ctx context.Context, result *AggregatedResult) error
	Flush(ctx context.Context) error
	Close() error
}

// Pipeline chains stages together and feeds results to a sink.
type Pipeline struct {
	stages []Stage
	sink   Sink
}

func NewPipeline(sink Sink, stages ...Stage) *Pipeline {
	return &Pipeline{
		stages: stages,
		sink:   sink,
	}
}

func (p *Pipeline) Process(ctx context.Context, event *Event) error {
	current := event

	for _, stage := range p.stages {
		result, err := stage.Process(ctx, current)
		if err != nil {
			return &StageError{
				Stage: stage.Name(),
				Err:   err,
				Event: current,
			}
		}
		if result == nil {
			// Stage filtered out this event
			return nil
		}
		current = result
	}

	return nil
}

// StageError wraps errors with the stage that produced them.
type StageError struct {
	Stage string
	Err   error
	Event *Event
}

func (e *StageError) Error() string {
	return "stage " + e.Stage + ": " + e.Err.Error()
}

func (e *StageError) Unwrap() error {
	return e.Err
}
