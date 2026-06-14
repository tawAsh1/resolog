package resolog

import (
	"context"
	"sync"
	"time"
)

// DefaultGracePeriod is how long Tail keeps streaming after a Resolution
// reports Done, to catch log lines CloudWatch delivers late. See Resolution.Done.
const DefaultGracePeriod = 5 * time.Second

// Option configures Tail.
type Option func(*options)

type options struct {
	grace   time.Duration
	onError func(Source, error)
}

// WithGracePeriod overrides how long Tail keeps streaming after Done fires.
func WithGracePeriod(d time.Duration) Option {
	return func(o *options) { o.grace = d }
}

// WithErrorHandler registers a callback invoked when a Backend fails to open a
// stream for a source. Without it such errors are silent — which, with a live
// backend, looks indistinguishable from "no logs yet", so the CLI sets one.
func WithErrorHandler(f func(Source, error)) Option {
	return func(o *options) { o.onError = f }
}

// Tail is the core orchestrator. It reads sources out of res as they are
// discovered, opens a Backend stream for each, merges everything into one
// interleaved channel, and feeds that to sink. It returns when:
//
//   - all sources have ended (historical backends drain naturally), or
//   - res.Done fires and the grace period elapses (terminal resource), or
//   - ctx is cancelled (Ctrl-C), or
//   - sink.Consume returns.
//
// Completion is driven by res.Done (resource status), never by inferring that
// "the logs stopped" — that inference is wrong often enough to be useless.
func Tail(ctx context.Context, res Resolution, backend Backend, sink Sink, opts ...Option) error {
	o := options{grace: DefaultGracePeriod}
	for _, fn := range opts {
		fn(&o)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	merged := make(chan Event)

	// Sink runs in its own goroutine so we can return its error.
	sinkErr := make(chan error, 1)
	go func() { sinkErr <- sink.Consume(ctx, merged) }()

	// Terminal handling: once the resource is Done, allow a grace window for
	// late-arriving lines, then cancel everything. nil Done = never terminal.
	if res.Done != nil {
		go func() {
			select {
			case <-ctx.Done():
			case <-res.Done:
				select {
				case <-ctx.Done():
				case <-time.After(o.grace):
					cancel()
				}
			}
		}()
	}

	// Discovery + fan-in: spawn a streamer per source as it appears.
	var wg sync.WaitGroup
	go func() {
		defer func() {
			wg.Wait()
			close(merged)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case src, ok := <-res.Sources:
				if !ok {
					return // discovery complete; existing streamers finish via wg
				}
				wg.Add(1)
				go func(s Source) {
					defer wg.Done()
					streamOne(ctx, backend, s, merged, o.onError)
				}(src)
			}
		}
	}()

	err := <-sinkErr
	cancel()
	return err
}

// streamOne pumps one source's events into the merged channel until the backend
// stream closes or ctx is cancelled. A Stream error is reported via onError (if
// set) and drops just that source — other sources keep tailing.
func streamOne(ctx context.Context, backend Backend, src Source, out chan<- Event, onError func(Source, error)) {
	ch, err := backend.Stream(ctx, src)
	if err != nil {
		if onError != nil {
			onError(src, err)
		}
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}
}
