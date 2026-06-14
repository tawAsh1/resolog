package poll

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"

	"github.com/tawAsh1/resolog"
)

// fakeAPI returns pre-canned pages on successive FilterLogEvents calls.
type fakeAPI struct {
	pages []*cwl.FilterLogEventsOutput
	n     int
}

func (f *fakeAPI) FilterLogEvents(ctx context.Context, in *cwl.FilterLogEventsInput, _ ...func(*cwl.Options)) (*cwl.FilterLogEventsOutput, error) {
	if f.n >= len(f.pages) {
		return &cwl.FilterLogEventsOutput{}, nil
	}
	p := f.pages[f.n]
	f.n++
	return p, nil
}

func ev(id, msg string, ts int64) cwltypes.FilteredLogEvent {
	return cwltypes.FilteredLogEvent{EventId: aws.String(id), Message: aws.String(msg), Timestamp: aws.Int64(ts), LogStreamName: aws.String("s")}
}

func collect(t *testing.T, ch <-chan resolog.Event) []resolog.Event {
	t.Helper()
	var got []resolog.Event
	timeout := time.After(2 * time.Second)
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, e)
		case <-timeout:
			t.Fatal("timed out collecting events")
		}
	}
}

// Non-follow: page through two pages via NextToken, then close.
func TestStreamHistoricalPaginates(t *testing.T) {
	api := &fakeAPI{pages: []*cwl.FilterLogEventsOutput{
		{Events: []cwltypes.FilteredLogEvent{ev("1", "a", 100), ev("2", "b", 200)}, NextToken: aws.String("t")},
		{Events: []cwltypes.FilteredLogEvent{ev("3", "c", 300)}},
	}}
	b := New(api, Options{})
	ch, err := b.Stream(context.Background(), resolog.Source{LogGroup: "g"})
	if err != nil {
		t.Fatal(err)
	}
	got := collect(t, ch)
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3", len(got))
	}
	if got[2].Message != "c" {
		t.Errorf("last message = %q, want c", got[2].Message)
	}
}

// Follow: the boundary event (same id, same ts) returned again on the next poll
// must be de-duplicated; a genuinely new event must pass through.
func TestStreamFollowDedupesBoundary(t *testing.T) {
	api := &fakeAPI{pages: []*cwl.FilterLogEventsOutput{
		{Events: []cwltypes.FilteredLogEvent{ev("1", "a", 100)}},                    // poll 1
		{Events: []cwltypes.FilteredLogEvent{ev("1", "a", 100), ev("2", "b", 100)}}, // poll 2: id 1 repeats
	}}
	b := New(api, Options{Follow: true, Interval: time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := b.Stream(ctx, resolog.Source{LogGroup: "g"})
	if err != nil {
		t.Fatal(err)
	}

	var got []resolog.Event
	timeout := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case e := <-ch:
			got = append(got, e)
		case <-timeout:
			cancel()
			t.Fatalf("got %d events, want 2 (boundary dedup failed?)", len(got))
		}
	}
	cancel()
	if got[0].Message != "a" || got[1].Message != "b" {
		t.Errorf("messages = %q,%q, want a,b", got[0].Message, got[1].Message)
	}
}
