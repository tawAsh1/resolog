package resolog

import (
	"context"
	"sort"
	"time"
)

// SortingSink buffers every event and, once the stream ends, emits them to Inner
// ordered by event time. Tail's fan-in interleaves sources by arrival, not by
// timestamp, so reviewing a finished resource (e.g. a completed Step Functions
// execution tailed with poll + --since) otherwise reads out of order across
// streams.
//
// It holds all events in memory and prints nothing until the stream closes, so
// it suits only bounded, historical tailing — not follow mode or the live
// backend, where the stream never ends. The CLI's --sort flag wires it and
// requires `--backend poll` without `-f`.
//
// If the context is cancelled (Ctrl-C) mid-fetch, it still flushes the ordered
// prefix it has buffered rather than discarding everything. Ordering is
// content-stable (see lessEvent), so the same events always print in the same
// order — golden-test friendly and free of spurious diff churn.
type SortingSink struct {
	Inner Sink
}

// Consume implements Sink.
func (s SortingSink) Consume(ctx context.Context, events <-chan Event) error {
	var buf []Event
	for {
		select {
		case <-ctx.Done():
			// Interrupted: still emit what we have, ordered, instead of nothing.
			// Flush with a fresh context so the already-cancelled ctx doesn't
			// abort the write immediately.
			_ = s.flush(context.Background(), buf)
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return s.flush(ctx, buf)
			}
			buf = append(buf, ev)
		}
	}
}

func (s SortingSink) flush(ctx context.Context, buf []Event) error {
	sort.SliceStable(buf, func(i, j int) bool { return lessEvent(buf[i], buf[j]) })
	out := make(chan Event, len(buf))
	for _, ev := range buf {
		out <- ev
	}
	close(out)
	return s.Inner.Consume(ctx, out)
}

// lessEvent is a content-stable total order: event time, then a stable per-stream
// key, then the message. It does not depend on goroutine arrival order, so a
// given set of events always sorts the same way across runs (stable golden
// tests, no churn when piped to diff). Truly identical lines compare equal,
// which is fine — their output bytes are identical regardless of position.
func lessEvent(a, b Event) bool {
	at, bt := eventTime(a), eventTime(b)
	if !at.Equal(bt) {
		return at.Before(bt)
	}
	if a.Source.Key != b.Source.Key {
		return a.Source.Key < b.Source.Key
	}
	return a.Message < b.Message
}

// eventTime is the time an event is ordered by: its reported timestamp, falling
// back to ingestion time when the former is absent.
func eventTime(e Event) time.Time {
	if !e.Timestamp.IsZero() {
		return e.Timestamp
	}
	return e.Ingestion
}
