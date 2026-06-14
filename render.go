package resolog

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

// Renderer is the default Sink: a docker-compose-style interleaved printer that
// assigns each Source a stable color and prefixes every line with its label.
//
// It is intentionally simple (one line per event, no progress bar / paging).
// The richer TUI ("k9s for CloudWatch") is a later, separate Sink — keeping
// this one minimal preserves the small public surface the design calls for.
type Renderer struct {
	// Out is where lines are written. Defaults to os.Stdout when nil is passed
	// to NewRenderer.
	Out io.Writer

	// Color enables ANSI color. Callers typically set this from isatty.
	Color bool

	// ShowTime prepends each line with the event timestamp.
	ShowTime bool

	mu      sync.Mutex
	colors  map[string]int // Source.Key -> palette index
	nextHue int
	width   int // widest label seen, for gutter alignment
}

// palette is a set of readable ANSI 256-color foreground codes.
var palette = []int{39, 208, 41, 170, 220, 51, 198, 141, 81, 214}

// NewRenderer builds a Renderer writing to out (os.Stdout if nil).
func NewRenderer(out io.Writer, color, showTime bool) *Renderer {
	return &Renderer{Out: out, Color: color, ShowTime: showTime, colors: map[string]int{}}
}

// Consume implements Sink.
func (r *Renderer) Consume(ctx context.Context, events <-chan Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if err := r.print(ev); err != nil {
				return err
			}
		}
	}
}

func (r *Renderer) print(ev Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	label := ev.Source.Label
	if label == "" {
		label = ev.Source.Key
	}
	if len(label) > r.width {
		r.width = len(label)
	}
	gutter := fmt.Sprintf("%-*s", r.width, label)

	var line string
	switch {
	case r.Color:
		code := r.colorFor(ev.Source.Key)
		gutter = fmt.Sprintf("\x1b[38;5;%dm%s\x1b[0m", code, gutter)
	}

	if r.ShowTime {
		ts := ev.Timestamp
		if ts.IsZero() {
			ts = ev.Ingestion
		}
		line = fmt.Sprintf("%s | %s | %s\n", ts.Format(time.RFC3339), gutter, ev.Message)
	} else {
		line = fmt.Sprintf("%s | %s\n", gutter, ev.Message)
	}

	_, err := io.WriteString(r.Out, line)
	return err
}

func (r *Renderer) colorFor(key string) int {
	if c, ok := r.colors[key]; ok {
		return c
	}
	c := palette[r.nextHue%len(palette)]
	r.colors[key] = c
	r.nextHue++
	return c
}
