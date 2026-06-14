package livetail

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"

	"github.com/tawAsh1/resolog"
)

func TestConvertFoldsStreamForGroupWideSource(t *testing.T) {
	src := resolog.Source{Key: "g", Label: "mygroup", LogGroup: "g"} // no LogStream: group-wide
	e := cwltypes.LiveTailSessionLogEvent{
		Message:       aws.String("hello"),
		Timestamp:     aws.Int64(1700000000000),
		LogStreamName: aws.String("stream-7"),
	}
	got := convert(src, e)
	if got.Message != "hello" {
		t.Errorf("message = %q", got.Message)
	}
	if got.Source.Label != "mygroup stream-7" {
		t.Errorf("label = %q, want folded stream name", got.Source.Label)
	}
	if got.Source.Key == src.Key {
		t.Error("key should be specialized per stream for a group-wide source")
	}
	if got.Timestamp.UnixMilli() != 1700000000000 {
		t.Errorf("timestamp = %v", got.Timestamp)
	}
}

func TestConvertKeepsLabelForSingleStreamSource(t *testing.T) {
	src := resolog.Source{Key: "k", Label: "λ fn", LogGroup: "/aws/lambda/fn", LogStream: "2024/01/01/[$LATEST]abc"}
	e := cwltypes.LiveTailSessionLogEvent{Message: aws.String("x"), LogStreamName: aws.String("2024/01/01/[$LATEST]abc")}
	got := convert(src, e)
	if got.Source.Label != "λ fn" || got.Source.Key != "k" {
		t.Errorf("single-stream source should be untouched, got label=%q key=%q", got.Source.Label, got.Source.Key)
	}
}
