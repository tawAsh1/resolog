package resolog

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
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

	// MaxGutter caps the width (in runes) of the label gutter. Labels longer
	// than this are middle-ellipsized so a long resource name (e.g. a folded log
	// stream id) can't push every message off the right edge. 0 means no cap —
	// appropriate when output is piped, so downstream tools see full labels. The
	// CLI sets it from the terminal width.
	MaxGutter int

	mu      sync.Mutex
	colors  map[string]int // Source.Key -> palette index
	nextHue int
	width   int // widest label seen (runes), for gutter alignment
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
	if r.MaxGutter > 0 {
		label = truncateLabel(label, r.MaxGutter)
	}
	w := utf8.RuneCountInString(label)
	if w > r.width {
		r.width = w
	}
	// Pad by runes, not bytes: labels can contain multibyte glyphs (e.g. "λ").
	gutter := label + strings.Repeat(" ", r.width-w)

	if r.Color {
		gutter = fmt.Sprintf("\x1b[38;5;%dm%s\x1b[0m", r.colorFor(ev.Source.Key), gutter)
	}

	var line string
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

// truncateLabel shortens s to at most max runes, putting an ellipsis in the
// middle so both the distinguishing prefix (service path) and suffix (name /
// stream id) survive.
func truncateLabel(s string, max int) string {
	if max <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	if max == 1 {
		return "…"
	}
	keep := max - 1 // leave room for the ellipsis
	head := (keep + 1) / 2
	tail := keep - head
	return string(r[:head]) + "…" + string(r[len(r)-tail:])
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
