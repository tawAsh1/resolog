package resolog

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeBackend emits n events per source then closes the stream.
type fakeBackend struct{ n int }

func (b fakeBackend) Stream(ctx context.Context, src Source) (<-chan Event, error) {
	out := make(chan Event)
	go func() {
		defer close(out)
		for i := 0; i < b.n; i++ {
			select {
			case out <- Event{Source: src, Message: "x"}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// collectSink gathers every event it receives.
type collectSink struct {
	mu     sync.Mutex
	events []Event
}

func (s *collectSink) Consume(ctx context.Context, events <-chan Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			s.mu.Lock()
			s.events = append(s.events, ev)
			s.mu.Unlock()
		}
	}
}

// TestTailFansInAllSources checks that every source discovered is streamed and
// merged, and that Tail returns once historical streams drain.
func TestTailFansInAllSources(t *testing.T) {
	sources := make(chan Source, 3)
	for i := 0; i < 3; i++ {
		sources <- Source{Key: string(rune('a' + i)), LogGroup: "g"}
	}
	close(sources)

	sink := &collectSink{}
	res := Resolution{Sources: sources} // nil Done: ends when streams drain

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := Tail(ctx, res, fakeBackend{n: 4}, sink); err != nil {
		t.Fatalf("Tail returned error: %v", err)
	}

	if got, want := len(sink.events), 3*4; got != want {
		t.Fatalf("got %d events, want %d", got, want)
	}
}

// TestTailStopsOnDoneGrace checks that a terminal Done signal stops a
// never-ending (live) backend after the grace period.
func TestTailStopsOnDoneGrace(t *testing.T) {
	sources := make(chan Source, 1)
	sources <- Source{Key: "a", LogGroup: "g"}
	close(sources)

	done := make(chan struct{})
	close(done) // already terminal

	sink := &collectSink{}
	res := Resolution{Sources: sources, Done: done}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// liveBackend never closes its stream on its own; only Done's grace stops it.
	err := Tail(ctx, res, liveBackend{}, sink, WithGracePeriod(50*time.Millisecond))
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Tail did not stop promptly after Done grace: %v", elapsed)
	}
}

// liveBackend produces a stream that never ends until ctx is cancelled.
type liveBackend struct{}

func (liveBackend) Stream(ctx context.Context, src Source) (<-chan Event, error) {
	out := make(chan Event)
	go func() {
		defer close(out)
		<-ctx.Done()
	}()
	return out, nil
}
