package runlog

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

// Types contains the supported event types.
var Types = map[string]bool{
	"run.queued":     true,
	"build.started":  true,
	"build.failed":   true,
	"tests.finished": true,
	"run.finished":   true,
}

// Event is a run event with its storage envelope.
type Event struct {
	EventID       string         `json:"event_id" bson:"event_id"`
	RunID         string         `json:"run_id" bson:"run_id"`
	Type          string         `json:"type" bson:"type"`
	OccurredAt    time.Time      `json:"occurred_at" bson:"occurred_at"`
	Message       string         `json:"message" bson:"message"`
	Data          map[string]any `json:"data,omitempty" bson:"data,omitempty"`
	ProjectID     string         `json:"project_id,omitempty" bson:"project_id"`
	ReceivedAt    time.Time      `json:"received_at,omitempty" bson:"received_at"`
	SchemaVersion int            `json:"schema_version,omitempty" bson:"schema_version"`
}

// Validate checks client-provided event fields.
func (e Event) Validate() error {
	if len(e.EventID) == 0 || len(e.EventID) > 128 || len(e.RunID) == 0 || len(e.RunID) > 128 {
		return errors.New("event_id and run_id must contain 1..128 bytes")
	}
	if !Types[e.Type] {
		return errors.New("unknown event type")
	}
	if e.OccurredAt.IsZero() || e.OccurredAt.Year() < 1970 || e.OccurredAt.Year() > 9999 {
		return errors.New("invalid occurred_at")
	}
	if len(e.Message) > 8192 {
		return errors.New("message exceeds 8192 bytes")
	}
	return nil
}

// Cursor identifies a position within a filtered run history.
type Cursor struct {
	Time    time.Time `json:"t"`
	ID      string    `json:"id"`
	Project string    `json:"p"`
	Run     string    `json:"r"`
	Filter  string    `json:"f"`
}

// EncodeCursor encodes a cursor for use in a history request.
func EncodeCursor(c Cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor decodes and validates a history cursor.
func DecodeCursor(s string) (Cursor, error) {
	var c Cursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	if err == nil && (c.Time.IsZero() || c.ID == "") {
		err = errors.New("invalid cursor")
	}
	return c, err
}
