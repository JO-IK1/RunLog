package runlog

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func validEvent() Event {
	return Event{EventID: "e1", RunID: "r1", Type: "run.queued", OccurredAt: time.Now()}
}

func TestEventValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Event)
	}{
		{"missing ID", func(e *Event) { e.EventID = "" }},
		{"long run", func(e *Event) { e.RunID = strings.Repeat("x", 129) }},
		{"unknown type", func(e *Event) { e.Type = "unknown" }},
		{"missing time", func(e *Event) { e.OccurredAt = time.Time{} }},
		{"long message", func(e *Event) { e.Message = strings.Repeat("x", 8193) }},
	}
	if err := validEvent().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEvent()
			tt.change(&e)
			if e.Validate() == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestCursorIsolation(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/runs/r1/events?limit=1", nil)
	r.SetPathValue("run_id", "r1")
	_, filter, err := parseQuery(r, "p1")
	if err != nil {
		t.Fatal(err)
	}
	c := EncodeCursor(Cursor{
		Time:    time.Now().UTC().Truncate(time.Millisecond),
		ID:      "e1",
		Project: "p1",
		Run:     "r1",
		Filter:  filter,
	})
	r = httptest.NewRequest("GET", "/?cursor="+c, nil)
	r.SetPathValue("run_id", "r1")
	q, _, err := parseQuery(r, "p1")
	if err != nil || q.After.ID != "e1" {
		t.Fatalf("round trip: %v", err)
	}
	if _, _, err = parseQuery(r, "p2"); err == nil {
		t.Fatal("cursor accepted across projects")
	}
	r = httptest.NewRequest("GET", "/?cursor="+c+"&type=build.failed", nil)
	r.SetPathValue("run_id", "r1")
	if _, _, err = parseQuery(r, "p1"); err == nil {
		t.Fatal("cursor accepted with changed filter")
	}
}

func TestInvalidHistoryParameters(t *testing.T) {
	for _, query := range []string{
		"limit=0",
		"limit=101",
		"limit=no",
		"type=unknown",
		"from=no",
		"to=no",
		"from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z",
		"cursor=garbage",
	} {
		t.Run(query, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/?"+query, nil)
			r.SetPathValue("run_id", "r1")
			if _, _, err := parseQuery(r, "p1"); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestIngestRejectsInvalidRequestsBeforePublishing(t *testing.T) {
	api := &API{}
	tests := []struct {
		name, contentType, body string
		status                  int
	}{
		{"wrong content type", "text/plain", `{}`, 415},
		{"unknown field", "application/json", `{"project_id":"forged"}`, 400},
		{"multiple values", "application/json", `{} {}`, 400},
		{"broken JSON", "application/json", `{`, 400},
		{"missing fields", "application/json", `{}`, 400},
		{"oversized", "application/json", `{"message":"` + strings.Repeat("x", 70000) + `"}`, 413},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/events", strings.NewReader(tt.body))
			r.Header.Set("Content-Type", tt.contentType)
			w := httptest.NewRecorder()
			api.ingest(w, r, "p1")
			if w.Code != tt.status {
				t.Fatalf("got %d: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestHistoryBoundsRespectMongoPrecision(t *testing.T) {
	r := httptest.NewRequest("GET", "/?from=2026-11-14T10:00:00.123456Z&to=2026-11-14T10:00:00.125999Z", nil)
	r.SetPathValue("run_id", "r1")
	q, _, err := parseQuery(r, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if q.From.Nanosecond() != 124000000 || q.To.Nanosecond() != 125000000 {
		t.Fatalf("unexpected bounds: %s..%s", q.From, q.To)
	}
}
