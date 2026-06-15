package ecs

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"

	"github.com/tawAsh1/resolog"
)

// fakeAPI answers DescribeTasks from a map of task identifier -> Task, and
// DescribeTaskDefinition from a map of definition arn -> container defs.
type fakeAPI struct {
	tasks map[string]types.Task
	defs  map[string][]types.ContainerDefinition
	list  []string // task arns returned by ListTasks
}

func (f fakeAPI) DescribeTasks(_ context.Context, in *awsecs.DescribeTasksInput, _ ...func(*awsecs.Options)) (*awsecs.DescribeTasksOutput, error) {
	var out awsecs.DescribeTasksOutput
	for _, id := range in.Tasks {
		if t, ok := f.tasks[id]; ok {
			out.Tasks = append(out.Tasks, t)
		}
	}
	return &out, nil
}

func (f fakeAPI) DescribeTaskDefinition(_ context.Context, in *awsecs.DescribeTaskDefinitionInput, _ ...func(*awsecs.Options)) (*awsecs.DescribeTaskDefinitionOutput, error) {
	return &awsecs.DescribeTaskDefinitionOutput{
		TaskDefinition: &types.TaskDefinition{ContainerDefinitions: f.defs[aws.ToString(in.TaskDefinition)]},
	}, nil
}

func (f fakeAPI) ListTasks(_ context.Context, _ *awsecs.ListTasksInput, _ ...func(*awsecs.Options)) (*awsecs.ListTasksOutput, error) {
	return &awsecs.ListTasksOutput{TaskArns: f.list}, nil
}

func awslogs(group, prefix string) *types.LogConfiguration {
	opts := map[string]string{"awslogs-group": group}
	if prefix != "" {
		opts["awslogs-stream-prefix"] = prefix
	}
	return &types.LogConfiguration{LogDriver: types.LogDriverAwslogs, Options: opts}
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

func TestWithPollInterval(t *testing.T) {
	api := fakeAPI{}
	if r := New(api, WithPollInterval(7*time.Millisecond)); r.pollInterval != 7*time.Millisecond {
		t.Errorf("pollInterval = %v, want 7ms", r.pollInterval)
	}
	if r := New(api, WithPollInterval(0)); r.pollInterval != defaultPollInterval {
		t.Errorf("pollInterval = %v, want default %v", r.pollInterval, defaultPollInterval)
	}
}

func TestParseTaskRef(t *testing.T) {
	cases := []struct {
		ref, cluster, task string
	}{
		{"arn:aws:ecs:us-east-1:111:task/prod/abc123", "prod", "arn:aws:ecs:us-east-1:111:task/prod/abc123"},
		{"arn:aws:ecs:us-east-1:111:task/abc123", "default", "arn:aws:ecs:us-east-1:111:task/abc123"},
		{"staging/def456", "staging", "def456"},
		{"def456", "default", "def456"},
	}
	for _, c := range cases {
		gotCluster, gotTask := parseTaskRef(c.ref)
		if gotCluster != c.cluster || gotTask != c.task {
			t.Errorf("parseTaskRef(%q) = (%q, %q), want (%q, %q)", c.ref, gotCluster, gotTask, c.cluster, c.task)
		}
	}
}

func TestResolveStreamPrefix(t *testing.T) {
	const arn = "arn:aws:ecs:us-east-1:111:task/prod/abc123"
	api := fakeAPI{
		tasks: map[string]types.Task{
			arn: {
				TaskArn:           aws.String(arn),
				TaskDefinitionArn: aws.String("td"),
				LastStatus:        aws.String("STOPPED"),
				Containers:        []types.Container{{Name: aws.String("app")}},
			},
		},
		defs: map[string][]types.ContainerDefinition{
			"td": {{Name: aws.String("app"), LogConfiguration: awslogs("/ecs/my-svc", "ecs")}},
		},
	}
	res, err := New(api).Resolve(context.Background(), arn)
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, res)
	if len(got) != 1 {
		t.Fatalf("got %d sources, want 1", len(got))
	}
	// <prefix>/<container>/<taskID>
	if got[0].LogGroup != "/ecs/my-svc" || got[0].LogStream != "ecs/app/abc123" {
		t.Errorf("source = %+v", got[0])
	}
	select {
	case <-res.Done:
	case <-time.After(time.Second):
		t.Fatal("Done not closed for terminal task")
	}
}

// Without awslogs-stream-prefix the stream is the container's runtime id, which
// only appears once the container has started.
func TestResolveNoPrefixUsesRuntimeID(t *testing.T) {
	const arn = "arn:aws:ecs:us-east-1:111:task/prod/abc"
	api := fakeAPI{
		tasks: map[string]types.Task{
			arn: {
				TaskArn:           aws.String(arn),
				TaskDefinitionArn: aws.String("td"),
				LastStatus:        aws.String("STOPPED"),
				Containers:        []types.Container{{Name: aws.String("app"), RuntimeId: aws.String("docker-xyz")}},
			},
		},
		defs: map[string][]types.ContainerDefinition{
			"td": {{Name: aws.String("app"), LogConfiguration: awslogs("/ecs/g", "")}},
		},
	}
	res, err := New(api).Resolve(context.Background(), arn)
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, res)
	if len(got) != 1 || got[0].LogStream != "docker-xyz" {
		t.Fatalf("got %+v, want stream docker-xyz", got)
	}
}

// A container that does not use the awslogs driver yields no source.
func TestResolveSkipsNonAwslogs(t *testing.T) {
	const arn = "arn:aws:ecs:us-east-1:111:task/prod/abc"
	api := fakeAPI{
		tasks: map[string]types.Task{
			arn: {
				TaskArn:           aws.String(arn),
				TaskDefinitionArn: aws.String("td"),
				LastStatus:        aws.String("STOPPED"),
				Containers:        []types.Container{{Name: aws.String("sidecar"), RuntimeId: aws.String("r")}},
			},
		},
		defs: map[string][]types.ContainerDefinition{
			"td": {{Name: aws.String("sidecar"), LogConfiguration: &types.LogConfiguration{LogDriver: types.LogDriverFluentd}}},
		},
	}
	res, err := New(api).Resolve(context.Background(), arn)
	if err != nil {
		t.Fatal(err)
	}
	if got := drain(t, res); len(got) != 0 {
		t.Fatalf("got %d sources, want 0: %+v", len(got), got)
	}
}

func TestResolveNotFound(t *testing.T) {
	if _, err := New(fakeAPI{}).Resolve(context.Background(), "arn:aws:ecs:us-east-1:111:task/prod/missing"); err == nil {
		t.Fatal("expected error for missing task")
	}
}

func TestList(t *testing.T) {
	const arn = "arn:aws:ecs:us-east-1:111:task/prod/abc"
	api := fakeAPI{
		list: []string{arn},
		tasks: map[string]types.Task{
			arn: {
				TaskArn:    aws.String(arn),
				Group:      aws.String("service:my-svc"),
				LastStatus: aws.String("RUNNING"),
			},
		},
	}
	refs, err := New(api).List(context.Background(), "prod")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].Ref != arn || refs[0].Status != "RUNNING" {
		t.Fatalf("refs = %+v", refs)
	}
	if _, err := New(api).List(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty cluster filter")
	}
}
