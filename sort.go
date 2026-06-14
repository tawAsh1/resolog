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
type SortingSink struct {
	Inner Sink
}

// Consume implements Sink.
func (s SortingSink) Consume(ctx context.Context, events <-chan Event) error {
	var buf []Event
	for {
		select {
		case <-ctx.Done():
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
	sort.SliceStable(buf, func(i, j int) bool {
		return eventTime(buf[i]).Before(eventTime(buf[j]))
	})
	out := make(chan Event, len(buf))
	for _, ev := range buf {
		out <- ev
	}
	close(out)
	return s.Inner.Consume(ctx, out)
}

// eventTime is the time an event is ordered by: its reported timestamp, falling
// back to ingestion time when the former is absent.
func eventTime(e Event) time.Time {
	if !e.Timestamp.IsZero() {
		return e.Timestamp
	}
	return e.Ingestion
}
