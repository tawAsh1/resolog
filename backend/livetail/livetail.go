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
	"fmt"
	"strings"
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
	api    API
	region string
	// account is the AWS account id that owns the log groups. region and account
	// are needed because StartLiveTail identifies log groups by ARN, but a Source
	// carries only the group name; see arnFor.
	account string
}

// New builds a live Backend from a CloudWatch Logs client. region and account
// (the AWS region and account id that own the log groups) are required: a Source
// names its log group, but StartLiveTail identifies log groups by ARN, so the
// backend rebuilds the ARN from these.
func New(api API, region, account string) *Backend {
	return &Backend{api: api, region: region, account: account}
}

// arnFor builds the log-group ARN StartLiveTail wants from a bare group name.
// The ARN must not end with ":*" (the StartLiveTail form), so this appends none.
func (b *Backend) arnFor(group string) (string, error) {
	if b.region == "" || b.account == "" {
		return "", fmt.Errorf("livetail: need AWS region and account to form a log-group ARN (have region=%q account=%q)", b.region, b.account)
	}
	return "arn:" + partitionForRegion(b.region) + ":logs:" + b.region + ":" + b.account + ":log-group:" + group, nil
}

// partitionForRegion maps a region to its ARN partition. The common commercial
// partition is "aws"; China and GovCloud have their own.
func partitionForRegion(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	default:
		return "aws"
	}
}

// Stream implements resolog.Backend. It opens a Live Tail session for src and
// relays its events until ctx is cancelled or the session ends.
func (b *Backend) Stream(ctx context.Context, src resolog.Source) (<-chan resolog.Event, error) {
	arn, err := b.arnFor(src.LogGroup)
	if err != nil {
		return nil, err
	}
	in := &cwl.StartLiveTailInput{LogGroupIdentifiers: []string{arn}}
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
