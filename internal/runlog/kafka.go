package runlog

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka creates a client configured for producing or consuming run events.
func Kafka(c Config, consumer bool) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(c.Brokers, ",")...),
		kgo.DefaultProduceTopic(c.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(8 * time.Second),
		kgo.MaxBufferedRecords(1000),
		kgo.MaxBufferedBytes(16 << 20),
	}
	if consumer {
		opts = append(opts,
			kgo.ConsumerGroup("runlog-storage-v1"),
			kgo.ConsumeTopics(c.Topic),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
		)
	}
	return kgo.NewClient(opts...)
}

type producer interface {
	TryProduce(context.Context, *kgo.Record, func(*kgo.Record, error))
}

// Publish submits an event and waits for delivery confirmation or cancellation.
func Publish(ctx context.Context, k producer, e Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	key, _ := json.Marshal([]string{e.ProjectID, e.RunID})
	// Idempotent delivery can outlive ctx when the broker's result is unknown.
	// Bound the HTTP wait independently; the event may still be delivered later.
	done := make(chan error, 1)
	k.TryProduce(ctx, &kgo.Record{Key: key, Value: b}, func(_ *kgo.Record, err error) {
		done <- err
	})
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Consume stores events and commits their offsets after successful writes.
func Consume(ctx context.Context, k *kgo.Client, s *Store) error {
	defer k.AllowRebalance()
	for ctx.Err() == nil {
		fetches := k.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			k.AllowRebalance()
			return fmt.Errorf("fetch: %v", errs)
		}
		for _, record := range fetches.Records() {
			var e Event
			if err := json.Unmarshal(record.Value, &e); err != nil {
				return fmt.Errorf("invalid message partition=%d offset=%d: %w", record.Partition, record.Offset, err)
			}
			if err := e.Validate(); err != nil {
				return err
			}
			if e.ProjectID == "" || e.SchemaVersion != 1 || e.ReceivedAt.IsZero() {
				return fmt.Errorf("invalid message envelope at offset %d", record.Offset)
			}
			delay := time.Second
			for {
				op, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := s.Save(op, e)
				cancel()
				if err == nil {
					break
				}
				slog.Error("mongo write failed; retrying", "error", err, "event_id", e.EventID)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(delay):
				}
				if delay < 8*time.Second {
					delay *= 2
				}
			}
			if err := k.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
		}
		k.AllowRebalance()
	}
	return nil
}
