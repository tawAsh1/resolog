// Package livetail is the real-time Backend, built on CloudWatch Logs
// StartLiveTail. It opens one Live Tail session per Source and converts the
// session's event stream into resolog.Events.
//
// StartLiveTail can tail several log groups in a single session (with filters),
// but resolog.Backend.Stream is called per Source, so this backend uses one
// session per source. That is simple and correct; batching many sources into
// fewer sessions (and paging past the per-session group cap, the same idea as
// batchkoi's >32-child paging) is a future optimization — see MaxGroupsPerSession.
package livetail

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"

	"github.com/tawAsh1/resolog"
)

// MaxGroupsPerSession is the StartLiveTail per-session log-group cap. Sources
// beyond this must be paged across multiple sessions once session batching is
// added.
//
// TODO: confirm against the current service limit before relying on it.
const MaxGroupsPerSession = 10

// API is the slice of the CloudWatch Logs client this backend needs, declared
// locally so the dependency is explicit. The real *cloudwatchlogs.Client
// satisfies it. (The returned event stream is a concrete SDK type, so this
// backend is exercised end-to-end rather than via a fake; the pure conversion
// logic is unit-tested separately.)
type API interface {
	StartLiveTail(ctx context.Context, in *cwl.StartLiveTailInput, optFns ...func(*cwl.Options)) (*cwl.StartLiveTailOutput, error)
}

// Backend tails sources live via StartLiveTail.
type Backend struct {
	api API
}

// New builds a live Backend from a CloudWatch Logs client.
func New(api API) *Backend { return &Backend{api: api} }

// Stream implements resolog.Backend. It opens a Live Tail session for src and
// relays its events until ctx is cancelled or the session ends.
func (b *Backend) Stream(ctx context.Context, src resolog.Source) (<-chan resolog.Event, error) {
	in := &cwl.StartLiveTailInput{LogGroupIdentifiers: []string{src.LogGroup}}
	if src.LogStream != "" {
		in.LogStreamNames = []string{src.LogStream}
	}
	out, err := b.api.StartLiveTail(ctx, in)
	if err != nil {
		return nil, err
	}

	events := make(chan resolog.Event)
	go func() {
		defer close(events)
		stream := out.GetStream()
		defer stream.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-stream.Events():
				if !ok {
					return // session ended; stream.Err() carries any cause
				}
				upd, isUpdate := msg.(*cwltypes.StartLiveTailResponseStreamMemberSessionUpdate)
				if !isUpdate {
					continue // SessionStart and any other frames carry no log lines
				}
				for i := range upd.Value.SessionResults {
					ev := convert(src, upd.Value.SessionResults[i])
					select {
					case events <- ev:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return events, nil
}

// convert turns a Live Tail log event into a resolog.Event. For a group-wide
// source the per-event stream name is folded into the label and key so the
// renderer's gutter still distinguishes streams.
func convert(src resolog.Source, e cwltypes.LiveTailSessionLogEvent) resolog.Event {
	if src.LogStream == "" {
		if s := aws.ToString(e.LogStreamName); s != "" {
			src.Label = src.Label + " " + s
			src.Key = src.LogGroup + "\x00" + s
		}
	}
	return resolog.Event{
		Source:    src,
		Timestamp: time.UnixMilli(aws.ToInt64(e.Timestamp)),
		Ingestion: time.UnixMilli(aws.ToInt64(e.IngestionTime)),
		Message:   aws.ToString(e.Message),
	}
}
