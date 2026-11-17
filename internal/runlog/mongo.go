package runlog

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Store persists run events in MongoDB.
type Store struct {
	Client *mongo.Client
	Events *mongo.Collection
}

// OpenMongo connects to the configured MongoDB event store.
func OpenMongo(ctx context.Context, c Config) (*Store, error) {
	clientOptions := options.Client().
		ApplyURI(c.Mongo).
		SetBSONOptions(&options.BSONOptions{DefaultDocumentM: true})
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, err
	}
	if err = client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return nil, err
	}
	return &Store{
		Client: client,
		Events: client.Database(c.Database).Collection("events"),
	}, nil
}

// Indexes creates the indexes required for deduplication and history queries.
func (s *Store) Indexes(ctx context.Context) error {
	_, err := s.Events.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "project_id", Value: 1}, {Key: "event_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "project_id", Value: 1}, {Key: "run_id", Value: 1}, {Key: "occurred_at", Value: 1}, {Key: "event_id", Value: 1}}},
	})
	return err
}

// Save stores an event without overwriting an existing event with the same ID.
func (s *Store) Save(ctx context.Context, e Event) error {
	// First write wins, including after redelivery with a changed payload.
	_, err := s.Events.UpdateOne(ctx,
		bson.M{"project_id": e.ProjectID, "event_id": e.EventID},
		bson.M{"$setOnInsert": e},
		options.UpdateOne().SetUpsert(true),
	)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

// Query specifies filters and pagination for a run history.
type Query struct {
	Project string
	Run     string
	Type    string
	From    time.Time
	To      time.Time
	After   *Cursor
	Limit   int
}

// History returns matching events and up to one extra event for pagination.
func (s *Store) History(ctx context.Context, q Query) ([]Event, error) {
	f := bson.M{"project_id": q.Project, "run_id": q.Run}
	if q.Type != "" {
		f["type"] = q.Type
	}
	times := bson.M{}
	if !q.From.IsZero() {
		times["$gte"] = q.From
	}
	if !q.To.IsZero() {
		times["$lte"] = q.To
	}
	if len(times) > 0 {
		f["occurred_at"] = times
	}
	if q.After != nil {
		f["$or"] = bson.A{
			bson.M{"occurred_at": bson.M{"$gt": q.After.Time}},
			bson.M{"occurred_at": q.After.Time, "event_id": bson.M{"$gt": q.After.ID}},
		}
	}
	findOptions := options.Find().
		SetSort(bson.D{{Key: "occurred_at", Value: 1}, {Key: "event_id", Value: 1}}).
		SetLimit(int64(q.Limit + 1))
	cur, err := s.Events.Find(ctx, f, findOptions)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	events := []Event{}
	err = cur.All(ctx, &events)
	return events, err
}
