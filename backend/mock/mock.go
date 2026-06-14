// Package mock is a dependency-free Backend that emits synthetic events. It
// lets the CLI run end-to-end without AWS credentials and gives the seams a
// consumer to test against. Not for production use.
package mock

import (
	"context"
	"fmt"
	"time"

	"github.com/tawAsh1/resolog"
)

// Backend emits Lines fake events per source, spaced by Interval, then closes
// the stream (simulating a historical/exhausted source).
type Backend struct {
	Lines    int
	Interval time.Duration
}

// New returns a mock Backend with sensible demo defaults.
func New() *Backend {
	return &Backend{Lines: 5, Interval: 200 * time.Millisecond}
}

// Stream implements resolog.Backend.
func (b *Backend) Stream(ctx context.Context, src resolog.Source) (<-chan resolog.Event, error) {
	out := make(chan resolog.Event)
	go func() {
		defer close(out)
		for i := 0; i < b.Lines; i++ {
			select {
			case <-ctx.Done():
				return
			case <-time.After(b.Interval):
			}
			ev := resolog.Event{
				Source:    src,
				Timestamp: time.Now(),
				Ingestion: time.Now(),
				Message:   fmt.Sprintf("synthetic log line %d from %s", i+1, src.LogGroup),
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
