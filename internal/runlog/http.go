package runlog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// API serves event ingestion, history, and health endpoints.
type API struct {
	PG    *pgxpool.Pool
	Store *Store
	Kafka *kgo.Client
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// Handler returns the HTTP handler for the API.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if a.PG.Ping(ctx) != nil || a.Kafka.Ping(ctx) != nil || a.Store.Client.Ping(ctx, nil) != nil {
			fail(w, http.StatusServiceUnavailable, "dependency unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.Handle("POST /v1/events", a.auth(a.ingest))
	mux.Handle("GET /v1/runs/{run_id}/events", a.auth(a.history))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		mux.ServeHTTP(w, r)
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}

func (a *API) auth(next func(http.ResponseWriter, *http.Request, string)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") || len(header) > 256 {
			fail(w, http.StatusUnauthorized, "bearer key required")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		project, err := Authenticate(ctx, a.PG, strings.TrimPrefix(header, "Bearer "))
		if errors.Is(err, pgx.ErrNoRows) {
			fail(w, http.StatusUnauthorized, "invalid key")
			return
		}
		if err != nil {
			slog.Error("authentication failed", "error", err)
			fail(w, http.StatusServiceUnavailable, "authentication unavailable")
			return
		}
		next(w, r, project)
	})
}

func (a *API) ingest(w http.ResponseWriter, r *http.Request, project string) {
	if ct := r.Header.Get("Content-Type"); strings.Split(ct, ";")[0] != "application/json" {
		fail(w, http.StatusUnsupportedMediaType, "application/json required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	// Input has no server-owned fields: clients cannot override project or receipt time.
	var in struct {
		EventID    string         `json:"event_id"`
		RunID      string         `json:"run_id"`
		Type       string         `json:"type"`
		OccurredAt time.Time      `json:"occurred_at"`
		Message    string         `json:"message"`
		Data       map[string]any `json:"data"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(&in)
	if err == nil {
		var extra any
		if dec.Decode(&extra) != io.EOF {
			err = errors.New("expected one JSON object")
		}
	}
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			fail(w, http.StatusRequestEntityTooLarge, "event exceeds 64 KiB")
		} else {
			fail(w, http.StatusBadRequest, "invalid JSON or unknown fields")
		}
		return
	}
	e := Event{
		EventID:       in.EventID,
		RunID:         in.RunID,
		Type:          in.Type,
		OccurredAt:    in.OccurredAt.UTC().Truncate(time.Millisecond),
		Message:       in.Message,
		Data:          in.Data,
		ProjectID:     project,
		ReceivedAt:    time.Now().UTC().Truncate(time.Millisecond),
		SchemaVersion: 1,
	}
	if err = e.Validate(); err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err = Publish(ctx, a.Kafka, e); err != nil {
		slog.Error("publish failed", "error", err)
		fail(w, http.StatusServiceUnavailable, "publication not confirmed; retry with the same event_id")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"event_id": e.EventID, "status": "accepted"})
}

func parseQuery(r *http.Request, project string) (Query, string, error) {
	values := r.URL.Query()
	q := Query{Project: project, Run: r.PathValue("run_id"), Type: values.Get("type"), Limit: 50}
	if q.Type != "" && !Types[q.Type] {
		return q, "", errors.New("unknown type")
	}
	if len(q.Run) == 0 || len(q.Run) > 128 {
		return q, "", errors.New("invalid run_id")
	}
	var err error
	if s := values.Get("limit"); s != "" {
		q.Limit, err = strconv.Atoi(s)
		if err != nil || q.Limit < 1 || q.Limit > 100 {
			return q, "", errors.New("limit must be 1..100")
		}
	}
	if s := values.Get("from"); s != "" {
		q.From, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return q, "", errors.New("invalid from")
		}
	}
	if s := values.Get("to"); s != "" {
		q.To, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return q, "", errors.New("invalid to")
		}
	}
	if !q.From.IsZero() && !q.To.IsZero() && q.From.After(q.To) {
		return q, "", errors.New("from must not exceed to")
	}
	// BSON dates have millisecond precision. Round the lower bound up so
	// fractional filters cannot include an event that occurred before from.
	if !q.From.IsZero() {
		floor := q.From.Truncate(time.Millisecond)
		if floor.Before(q.From) {
			floor = floor.Add(time.Millisecond)
		}
		q.From = floor
	}
	if !q.To.IsZero() {
		q.To = q.To.Truncate(time.Millisecond)
	}
	filter := q.Type + "|" + q.From.UTC().Format(time.RFC3339Nano) + "|" + q.To.UTC().Format(time.RFC3339Nano)
	if s := values.Get("cursor"); s != "" {
		if len(s) > 2048 {
			return q, "", errors.New("cursor too long")
		}
		c, e := DecodeCursor(s)
		if e != nil || c.Project != project || c.Run != q.Run || c.Filter != filter {
			return q, "", errors.New("invalid cursor for this query")
		}
		q.After = &c
	}
	return q, filter, nil
}

func (a *API) history(w http.ResponseWriter, r *http.Request, project string) {
	q, filter, err := parseQuery(r, project)
	if err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	events, err := a.Store.History(ctx, q)
	if err != nil {
		slog.Error("history failed", "error", err)
		fail(w, http.StatusServiceUnavailable, "history unavailable")
		return
	}
	next := ""
	if len(events) > q.Limit {
		events = events[:q.Limit]
		last := events[len(events)-1]
		next = EncodeCursor(Cursor{
			Time:    last.OccurredAt,
			ID:      last.EventID,
			Project: project,
			Run:     q.Run,
			Filter:  filter,
		})
	}
	writeJSON(w, http.StatusOK, struct {
		Events []Event `json:"events"`
		Next   string  `json:"next_cursor,omitempty"`
	}{events, next})
}
