package batch

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsbatch "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"

	"github.com/tawAsh1/resolog"
)

// fakeAPI answers DescribeJobs from a map of jobId -> JobDetail.
type fakeAPI struct{ jobs map[string]types.JobDetail }

func (f fakeAPI) DescribeJobs(ctx context.Context, in *awsbatch.DescribeJobsInput, _ ...func(*awsbatch.Options)) (*awsbatch.DescribeJobsOutput, error) {
	var out awsbatch.DescribeJobsOutput
	for _, id := range in.Jobs {
		if j, ok := f.jobs[id]; ok {
			out.Jobs = append(out.Jobs, j)
		}
	}
	return &out, nil
}

func (f fakeAPI) ListJobs(ctx context.Context, in *awsbatch.ListJobsInput, _ ...func(*awsbatch.Options)) (*awsbatch.ListJobsOutput, error) {
	return &awsbatch.ListJobsOutput{}, nil
}

func job(id, name, stream string, status types.JobStatus) types.JobDetail {
	d := types.JobDetail{JobId: aws.String(id), JobName: aws.String(name), Status: status}
	if stream != "" {
		d.Container = &types.ContainerDetail{LogStreamName: aws.String(stream)}
	}
	return d
}

func drain(t *testing.T, res resolog.Resolution) []resolog.Source {
	t.Helper()
	var got []resolog.Source
	timeout := time.After(2 * time.Second)
	for {
		select {
		case s, ok := <-res.Sources:
			if !ok {
				return got
			}
			got = append(got, s)
		case <-timeout:
			t.Fatal("timed out draining sources")
		}
	}
}

func TestResolveSingleJob(t *testing.T) {
	api := fakeAPI{jobs: map[string]types.JobDetail{
		"j1": job("j1", "crunch", "crunch/default/abc", types.JobStatusSucceeded),
	}}
	res, err := New(api).Resolve(context.Background(), "j1")
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, res)
	if len(got) != 1 {
		t.Fatalf("got %d sources, want 1", len(got))
	}
	if got[0].LogGroup != DefaultLogGroup || got[0].LogStream != "crunch/default/abc" {
		t.Errorf("source = %+v", got[0])
	}
	select {
	case <-res.Done:
	case <-time.After(time.Second):
		t.Fatal("Done not closed for terminal job")
	}
}

func TestResolveArrayJobExpandsChildren(t *testing.T) {
	parent := types.JobDetail{
		JobId:           aws.String("p"),
		Status:          types.JobStatusSucceeded,
		ArrayProperties: &types.ArrayPropertiesDetail{Size: aws.Int32(2)},
	}
	child := func(idx int32, stream string) types.JobDetail {
		d := job("p:"+itoa(idx), "arr", stream, types.JobStatusSucceeded)
		d.ArrayProperties = &types.ArrayPropertiesDetail{Index: aws.Int32(idx)}
		return d
	}
	api := fakeAPI{jobs: map[string]types.JobDetail{
		"p":   parent,
		"p:0": child(0, "arr/default/c0"),
		"p:1": child(1, "arr/default/c1"),
	}}
	res, err := New(api).Resolve(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, res)
	if len(got) != 2 {
		t.Fatalf("got %d sources, want 2: %+v", len(got), got)
	}
	labels := map[string]bool{}
	for _, s := range got {
		labels[s.Label] = true
	}
	if !labels["batch[0]"] || !labels["batch[1]"] {
		t.Errorf("labels = %v, want batch[0] and batch[1]", labels)
	}
}

func TestResolveCustomLogGroup(t *testing.T) {
	d := job("j", "n", "stream-x", types.JobStatusSucceeded)
	d.Container.LogConfiguration = &types.LogConfiguration{
		LogDriver: types.LogDriverAwslogs,
		Options:   map[string]string{"awslogs-group": "/custom/group"},
	}
	api := fakeAPI{jobs: map[string]types.JobDetail{"j": d}}
	res, err := New(api).Resolve(context.Background(), "j")
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, res)
	if len(got) != 1 || got[0].LogGroup != "/custom/group" {
		t.Fatalf("got %+v, want log group /custom/group", got)
	}
}

func itoa(i int32) string { return string(rune('0' + i)) }
