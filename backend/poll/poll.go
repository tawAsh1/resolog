// Package poll is the historical Backend: it pages CloudWatch Logs with
// FilterLogEvents rather than tailing live. FilterLogEvents covers both shapes
// of a Source — a whole log group, or a single stream within it — so one code
// path serves every Resolver. Use it for resources that have already finished,
// or where StartLiveTail is unavailable.
package poll

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"

	"github.com/tawAsh1/resolog"
)

// API is the slice of the CloudWatch Logs client this backend needs, declared
// locally so the SDK stays mockable and the dependency is explicit. The real
// *cloudwatchlogs.Client satisfies it.
type API interface {
	FilterLogEvents(ctx context.Context, in *cwl.FilterLogEventsInput, optFns ...func(*cwl.Options)) (*cwl.FilterLogEventsOutput, error)
}

// Options configures a poll Backend.
type Options struct {
	// Follow keeps polling for new events after the initial backfill. When
	// false, Stream returns once the available history is exhausted.
	Follow bool

	// Interval is the gap between polls in follow mode. Defaults to 2s.
	Interval time.Duration

	// Since, if non-zero, is the earliest event time to fetch. Zero means "from
	// the beginning of what CloudWatch retains".
	Since time.Time
}

// Backend pages historical (and, in follow mode, ongoing) events for a source.
type Backend struct {
	api  API
	opts Options
}

// New builds a polling Backend.
func New(api API, opts Options) *Backend {
	if opts.Interval <= 0 {
		opts.Interval = 2 * time.Second
	}
	return &Backend{api: api, opts: opts}
}

// Stream implements resolog.Backend.
func (b *Backend) Stream(ctx context.Context, src resolog.Source) (<-chan resolog.Event, error) {
	out := make(chan resolog.Event)
	go func() {
		defer close(out)

		var startMillis int64
		if !b.opts.Since.IsZero() {
			startMillis = b.opts.Since.UnixMilli()
		}
		// boundary holds the event ids seen at exactly the last poll's max
		// timestamp, so the next poll (which starts at that same millisecond, to
		// avoid dropping same-ms events) can skip them without re-emitting.
		boundary := map[string]bool{}

		for {
			maxTs, nextBoundary, err := b.pollOnce(ctx, src, startMillis, boundary, out)
			if err != nil {
				return // ctx cancelled or API error
			}
			if !b.opts.Follow {
				return
			}
			if maxTs > startMillis {
				startMillis = maxTs
				boundary = nextBoundary
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(b.opts.Interval):
			}
		}
	}()
	return out, nil
}

// pollOnce pages all events from startMillis forward once, emitting each new
// one. It returns the max timestamp seen and the set of event ids at that
// timestamp (the next poll's boundary), so callers can advance without gaps or
// duplicates.
func (b *Backend) pollOnce(ctx context.Context, src resolog.Source, startMillis int64, skip map[string]bool, out chan<- resolog.Event) (maxTs int64, boundary map[string]bool, err error) {
	maxTs = startMillis
	boundary = map[string]bool{}

	in := &cwl.FilterLogEventsInput{LogGroupName: aws.String(src.LogGroup)}
	if src.LogStream != "" {
		in.LogStreamNames = []string{src.LogStream}
	}
	if startMillis > 0 {
		in.StartTime = aws.Int64(startMillis)
	}

	for {
		page, perr := b.api.FilterLogEvents(ctx, in)
		if perr != nil {
			return 0, nil, perr
		}
		for i := range page.Events {
			ev := page.Events[i]
			id := aws.ToString(ev.EventId)
			if skip[id] {
				continue
			}
			ts := aws.ToInt64(ev.Timestamp)
			if err := send(ctx, out, toEvent(src, ev)); err != nil {
				return 0, nil, err
			}
			switch {
			case ts > maxTs:
				maxTs = ts
				boundary = map[string]bool{id: true}
			case ts == maxTs:
				boundary[id] = true
			}
		}
		if page.NextToken == nil {
			return maxTs, boundary, nil
		}
		in.NextToken = page.NextToken
	}
}

// toEvent converts a CloudWatch FilteredLogEvent into a resolog.Event. For a
// group-wide source the per-event stream name is folded into the label so the
// renderer's gutter still distinguishes streams.
func toEvent(src resolog.Source, ev cwltypes.FilteredLogEvent) resolog.Event {
	if src.LogStream == "" {
		if s := aws.ToString(ev.LogStreamName); s != "" {
			src.Label = src.Label + " " + s
			src.Key = src.LogGroup + "\x00" + s
		}
	}
	return resolog.Event{
		Source:    src,
		Timestamp: time.UnixMilli(aws.ToInt64(ev.Timestamp)),
		Ingestion: time.UnixMilli(aws.ToInt64(ev.IngestionTime)),
		Message:   aws.ToString(ev.Message),
	}
}

func send(ctx context.Context, out chan<- resolog.Event, ev resolog.Event) error {
	select {
	case out <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
