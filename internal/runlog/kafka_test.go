package runlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type pendingProducer struct {
	callback func(*kgo.Record, error)
}

func (p *pendingProducer) TryProduce(_ context.Context, _ *kgo.Record, callback func(*kgo.Record, error)) {
	p.callback = callback
}

func TestPublicationDeadlineDoesNotWaitForBroker(t *testing.T) {
	p := &pendingProducer{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := Publish(ctx, p, validEvent()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	// A late broker response must not leave the Kafka callback blocked.
	done := make(chan struct{})
	go func() {
		p.callback(nil, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late callback blocked")
	}
}
