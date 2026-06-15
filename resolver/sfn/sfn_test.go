package sfn

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/tawAsh1/resolog"
)

// fakeAPI returns a fixed status and a fixed, single-page history.
type fakeAPI struct {
	status types.ExecutionStatus
	events []types.HistoryEvent
}

func (f fakeAPI) DescribeExecution(ctx context.Context, in *awssfn.DescribeExecutionInput, _ ...func(*awssfn.Options)) (*awssfn.DescribeExecutionOutput, error) {
	return &awssfn.DescribeExecutionOutput{Status: f.status}, nil
}

func (f fakeAPI) GetExecutionHistory(ctx context.Context, in *awssfn.GetExecutionHistoryInput, _ ...func(*awssfn.Options)) (*awssfn.GetExecutionHistoryOutput, error) {
	return &awssfn.GetExecutionHistoryOutput{Events: f.events}, nil
}

func (f fakeAPI) ListExecutions(ctx context.Context, in *awssfn.ListExecutionsInput, _ ...func(*awssfn.Options)) (*awssfn.ListExecutionsOutput, error) {
	return &awssfn.ListExecutionsOutput{}, nil
}

// drain collects every source until the Resolution's Sources channel closes.
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

func TestResolveMapsLambdaAndBatch(t *testing.T) {
	events := []types.HistoryEvent{
		// Lambda, direct-ARN form.
		{LambdaFunctionScheduledEventDetails: &types.LambdaFunctionScheduledEventDetails{
			Resource: aws.String("arn:aws:lambda:us-east-1:123456789012:function:ingest:PROD"),
		}},
		// Lambda, lambda:invoke form.
		{TaskScheduledEventDetails: &types.TaskScheduledEventDetails{
			ResourceType: aws.String("lambda"),
			Resource:     aws.String("invoke"),
			Parameters:   aws.String(`{"FunctionName":"arn:aws:lambda:us-east-1:123456789012:function:transform","Payload":{}}`),
		}},
		// Batch (.sync) completed: log stream comes from the job output.
		{TaskSucceededEventDetails: &types.TaskSucceededEventDetails{
			ResourceType: aws.String("batch"),
			Resource:     aws.String("submitJob.sync"),
			Output:       aws.String(`{"JobId":"abc-123","JobName":"crunch","Container":{"LogStreamName":"crunch/default/abc123"}}`),
		}},
		// Duplicate of the first lambda — must be de-duplicated by key.
		{LambdaFunctionScheduledEventDetails: &types.LambdaFunctionScheduledEventDetails{
			Resource: aws.String("arn:aws:lambda:us-east-1:123456789012:function:ingest"),
		}},
	}

	r := New(fakeAPI{status: types.ExecutionStatusSucceeded, events: events})
	res, err := r.Resolve(context.Background(), "arn:aws:states:us-east-1:123456789012:execution:sm:exec")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	got := drain(t, res)
	byKey := map[string]resolog.Source{}
	for _, s := range got {
		byKey[s.Key] = s
	}

	if len(byKey) != 3 {
		t.Fatalf("got %d unique sources, want 3: %+v", len(byKey), got)
	}
	if s := byKey["lambda:ingest"]; s.LogGroup != "/aws/lambda/ingest" {
		t.Errorf("ingest log group = %q, want /aws/lambda/ingest", s.LogGroup)
	}
	if s := byKey["lambda:transform"]; s.LogGroup != "/aws/lambda/transform" {
		t.Errorf("transform log group = %q, want /aws/lambda/transform", s.LogGroup)
	}
	if s := byKey["batch:crunch/default/abc123"]; s.LogGroup != DefaultBatchLogGroup || s.LogStream != "crunch/default/abc123" {
		t.Errorf("batch source = %+v, want group %s stream crunch/default/abc123", s, DefaultBatchLogGroup)
	}

	// A terminal execution must close Done.
	select {
	case <-res.Done:
	case <-time.After(2 * time.Second):
		t.Fatal("Done not closed for a terminal execution")
	}
}

// fakeBatch is a stand-in delegate Resolver: it records the job id it was asked
// to resolve and returns one source for it.
type fakeBatch struct{ resolved chan string }

func (f *fakeBatch) Scheme() string { return "batch-job" }
func (f *fakeBatch) Resolve(ctx context.Context, ref string) (resolog.Resolution, error) {
	f.resolved <- ref
	src := make(chan resolog.Source, 1)
	src <- resolog.Source{Key: "batch:" + ref, Label: "batch/" + ref, LogGroup: "/aws/batch/job", LogStream: ref}
	close(src)
	return resolog.Resolution{Sources: src}, nil
}

func TestResolveDelegatesRunningBatch(t *testing.T) {
	events := []types.HistoryEvent{
		// A running Batch task: only TaskSubmitted (JobId), no completion output.
		{TaskSubmittedEventDetails: &types.TaskSubmittedEventDetails{
			ResourceType: aws.String("batch"),
			Resource:     aws.String("submitJob.sync"),
			Output:       aws.String(`{"JobId":"job-xyz","JobName":"crunch"}`),
		}},
	}
	fb := &fakeBatch{resolved: make(chan string, 1)}
	r := New(fakeAPI{status: types.ExecutionStatusSucceeded, events: events}, WithBatchResolver(fb))

	res, err := r.Resolve(context.Background(), "arn:...:execution:sm:exec")
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, res)

	if len(got) != 1 || got[0].Key != "batch:job-xyz" {
		t.Fatalf("got %+v, want one delegated batch source for job-xyz", got)
	}
	select {
	case jid := <-fb.resolved:
		if jid != "job-xyz" {
			t.Errorf("delegate resolved %q, want job-xyz", jid)
		}
	default:
		t.Error("delegate was not invoked")
	}
}

// fakeECS is a stand-in delegate Resolver for ECS: it records the task ARN it
// was asked to resolve and returns one source for it.
type fakeECS struct{ resolved chan string }

func (f *fakeECS) Scheme() string { return "ecs-task" }
func (f *fakeECS) Resolve(ctx context.Context, ref string) (resolog.Resolution, error) {
	f.resolved <- ref
	src := make(chan resolog.Source, 1)
	src <- resolog.Source{Key: "ecs:" + ref, Label: "ecs/app", LogGroup: "/ecs/g", LogStream: ref}
	close(src)
	return resolog.Resolution{Sources: src}, nil
}

func TestResolveDelegatesRunTask(t *testing.T) {
	const taskArn = "arn:aws:ecs:us-east-1:123:task/prod/abc"
	events := []types.HistoryEvent{
		// ecs:runTask.sync appears as a TaskSubmitted whose output is the
		// RunTask response carrying the task ARN.
		{TaskSubmittedEventDetails: &types.TaskSubmittedEventDetails{
			ResourceType: aws.String("ecs"),
			Resource:     aws.String("runTask.sync"),
			Output:       aws.String(`{"Tasks":[{"TaskArn":"` + taskArn + `"}]}`),
		}},
	}
	fe := &fakeECS{resolved: make(chan string, 1)}
	// A Batch delegate is also set, to prove ECS is still examined when the
	// Batch branch would otherwise consume the event.
	fb := &fakeBatch{resolved: make(chan string, 1)}
	r := New(fakeAPI{status: types.ExecutionStatusSucceeded, events: events},
		WithBatchResolver(fb), WithECSResolver(fe))

	res, err := r.Resolve(context.Background(), "arn:...:execution:sm:exec")
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, res)

	if len(got) != 1 || got[0].Key != "ecs:"+taskArn {
		t.Fatalf("got %+v, want one delegated ecs source for %s", got, taskArn)
	}
	select {
	case tarn := <-fe.resolved:
		if tarn != taskArn {
			t.Errorf("delegate resolved %q, want %q", tarn, taskArn)
		}
	default:
		t.Error("ecs delegate was not invoked")
	}
}

func TestLambdaName(t *testing.T) {
	cases := map[string]string{
		"arn:aws:lambda:us-east-1:123:function:Foo:PROD": "Foo",
		"arn:aws:lambda:us-east-1:123:function:Foo":      "Foo",
		"Foo:alias": "Foo",
		"Foo":       "Foo",
	}
	for in, want := range cases {
		if got := lambdaName(in); got != want {
			t.Errorf("lambdaName(%q) = %q, want %q", in, got, want)
		}
	}
}
