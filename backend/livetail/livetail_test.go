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

func TestArnForBuildsStartLiveTailARN(t *testing.T) {
	b := New(nil, "ap-northeast-1", "123456789012")
	got, err := b.arnFor("/aws/batch/job")
	if err != nil {
		t.Fatalf("arnFor: %v", err)
	}
	want := "arn:aws:logs:ap-northeast-1:123456789012:log-group:/aws/batch/job"
	if got != want {
		t.Errorf("arnFor = %q, want %q", got, want)
	}
	if got[len(got)-2:] == ":*" {
		t.Error("StartLiveTail ARN must not end with :*")
	}
}

func TestArnForPartitions(t *testing.T) {
	cases := map[string]string{
		"ap-northeast-1": "aws",
		"cn-north-1":     "aws-cn",
		"us-gov-west-1":  "aws-us-gov",
	}
	for region, part := range cases {
		got, err := New(nil, region, "111").arnFor("g")
		if err != nil {
			t.Fatalf("arnFor(%s): %v", region, err)
		}
		want := "arn:" + part + ":logs:" + region + ":111:log-group:g"
		if got != want {
			t.Errorf("region %s: arnFor = %q, want %q", region, got, want)
		}
	}
}

func TestArnForRequiresRegionAndAccount(t *testing.T) {
	if _, err := New(nil, "", "123").arnFor("g"); err == nil {
		t.Error("missing region should error")
	}
	if _, err := New(nil, "us-east-1", "").arnFor("g"); err == nil {
		t.Error("missing account should error")
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
