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

// Equal timestamps must order deterministically by (key, message), independent
// of arrival order — so output is stable across runs.
func TestSortingSinkTieBreakIsContentStable(t *testing.T) {
	ts := time.Unix(1_700_000_000, 0)
	mk := func() chan Event {
		c := make(chan Event, 3)
		c <- Event{Timestamp: ts, Source: Source{Key: "b"}, Message: "m2"}
		c <- Event{Timestamp: ts, Source: Source{Key: "a"}, Message: "m9"}
		c <- Event{Timestamp: ts, Source: Source{Key: "a"}, Message: "m1"}
		close(c)
		return c
	}
	order := func() string {
		sink := &collectSink{}
		if err := (SortingSink{Inner: sink}).Consume(context.Background(), mk()); err != nil {
			t.Fatal(err)
		}
		s := ""
		for _, e := range sink.events {
			s += e.Source.Key + ":" + e.Message + " "
		}
		return s
	}
	want := "a:m1 a:m9 b:m2 "
	if got := order(); got != want {
		t.Errorf("order = %q, want %q", got, want)
	}
	if a, b := order(), order(); a != b {
		t.Errorf("not deterministic across runs: %q vs %q", a, b)
	}
}

// Exceeding Limit must error instead of buffering without bound.
func TestSortingSinkLimit(t *testing.T) {
	in := make(chan Event, 3)
	for i := 0; i < 3; i++ {
		in <- Event{Message: "x"}
	}
	close(in)
	err := (SortingSink{Inner: &collectSink{}, Limit: 2}).Consume(context.Background(), in)
	if err == nil {
		t.Fatal("expected an error when the buffer exceeds Limit")
	}
}

// On cancel mid-fetch, the ordered prefix already buffered must still be flushed.
func TestSortingSinkFlushesOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan Event) // unbuffered: a send returns only once Consume has read it
	sink := &collectSink{}
	done := make(chan error, 1)
	go func() { done <- (SortingSink{Inner: sink}).Consume(ctx, events) }()

	t0 := time.Unix(1_700_000_000, 0)
	events <- Event{Timestamp: t0.Add(time.Second), Message: "b"}
	events <- Event{Timestamp: t0, Message: "a"}
	cancel() // both events are now buffered; cancel before close

	if err := <-done; err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(sink.events) != 2 || sink.events[0].Message != "a" || sink.events[1].Message != "b" {
		t.Errorf("on cancel want sorted [a b], got %+v", sink.events)
	}
}
