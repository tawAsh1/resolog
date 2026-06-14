package resolog

import (
	"context"
	"testing"
	"time"
)

func TestSortingSinkOrdersByTime(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	in := make(chan Event, 4)
	// Arrival order deliberately scrambled across "streams".
	in <- Event{Timestamp: t0.Add(2 * time.Second), Message: "c", Source: Source{Key: "x"}}
	in <- Event{Timestamp: t0, Message: "a", Source: Source{Key: "y"}}
	in <- Event{Timestamp: t0.Add(3 * time.Second), Message: "d", Source: Source{Key: "x"}}
	in <- Event{Timestamp: t0.Add(1 * time.Second), Message: "b", Source: Source{Key: "y"}}
	close(in)

	sink := &collectSink{}
	if err := (SortingSink{Inner: sink}).Consume(context.Background(), in); err != nil {
		t.Fatal(err)
	}

	got := ""
	for _, e := range sink.events {
		got += e.Message
	}
	if got != "abcd" {
		t.Errorf("order = %q, want abcd", got)
	}
}

func TestSortingSinkFallsBackToIngestion(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	in := make(chan Event, 2)
	// No Timestamp; ordering must use Ingestion.
	in <- Event{Ingestion: t0.Add(time.Second), Message: "second"}
	in <- Event{Ingestion: t0, Message: "first"}
	close(in)

	sink := &collectSink{}
	if err := (SortingSink{Inner: sink}).Consume(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 2 || sink.events[0].Message != "first" {
		t.Errorf("got %+v, want first then second", sink.events)
	}
}
