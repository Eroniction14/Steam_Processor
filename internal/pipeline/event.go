package pipeline

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	EventPageView    EventType = "page_view"
	EventUserAction  EventType = "user_action"
	EventTransaction EventType = "transaction"
)

type Event struct {
	ID        string    `json:"id"`
	Type      EventType `json:"type"`
	UserID    string    `json:"user_id"`
	Timestamp time.Time `json:"timestamp"`
	Payload   Payload   `json:"payload"`
}

type Payload struct {
	Page     string  `json:"page,omitempty"`
	Action   string  `json:"action,omitempty"`
	Amount   float64 `json:"amount,omitempty"`
	Currency string  `json:"currency,omitempty"`
	Duration int     `json:"duration_ms,omitempty"`
}

func (e *Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func UnmarshalEvent(data []byte) (*Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

type AggregatedResult struct {
	WindowStart time.Time         `json:"window_start"`
	WindowEnd   time.Time         `json:"window_end"`
	EventType   EventType         `json:"event_type"`
	Count       int64             `json:"count"`
	UniqueUsers int64             `json:"unique_users"`
	TotalAmount float64           `json:"total_amount,omitempty"`
	AvgDuration float64           `json:"avg_duration_ms,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (r *AggregatedResult) Marshal() ([]byte, error) {
	return json.Marshal(r)
}
