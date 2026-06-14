package resolog

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncateLabelNoChange(t *testing.T) {
	cases := []struct {
		in  string
		max int
	}{
		{"short", 10},             // fits under the cap
		{"/aws/lambda/ingest", 0}, // max 0 = no cap
		{"abcdefghij", 10},        // exactly the cap
	}
	for _, c := range cases {
		if got := truncateLabel(c.in, c.max); got != c.in {
			t.Errorf("truncateLabel(%q, %d) = %q, want unchanged", c.in, c.max, got)
		}
	}
}

func TestTruncateLabelMiddleEllipsis(t *testing.T) {
	in := "/aws/lambda/really-long-function-name"
	got := truncateLabel(in, 16)
	if n := utf8.RuneCountInString(got); n != 16 {
		t.Errorf("got %q (%d runes), want 16", got, n)
	}
	if !strings.Contains(got, "…") {
		t.Errorf("expected an ellipsis in %q", got)
	}
	if !strings.HasPrefix(got, "/aws/") || !strings.HasSuffix(got, "name") {
		t.Errorf("middle-ellipsis should keep both ends, got %q", got)
	}
}

func TestTruncateLabelMultibyte(t *testing.T) {
	// "λ " + long name; the λ counts as one rune.
	got := truncateLabel("λ very-long-lambda-function", 12)
	if utf8.RuneCountInString(got) != 12 {
		t.Errorf("got %q (%d runes), want 12", got, utf8.RuneCountInString(got))
	}
	if !strings.Contains(got, "…") {
		t.Errorf("expected ellipsis in %q", got)
	}
}

func TestRendererCapsGutter(t *testing.T) {
	var buf bytes.Buffer
	r := NewRenderer(&buf, false, false)
	r.MaxGutter = 10
	long := "/aws/lambda/some-really-long-name"
	ch := make(chan Event, 1)
	ch <- Event{Source: Source{Key: "k", Label: long}, Message: "hi"}
	close(ch)
	if err := r.Consume(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	gutter, _, _ := strings.Cut(out, " | ")
	if utf8.RuneCountInString(strings.TrimRight(gutter, " ")) > 10 {
		t.Errorf("gutter %q exceeds MaxGutter 10", gutter)
	}
	if !strings.Contains(out, "hi") {
		t.Errorf("message missing from %q", out)
	}
}
