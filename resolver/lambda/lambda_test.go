package lambda

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/tawAsh1/resolog"
)

type fakeAPI struct {
	cfg   *awslambda.GetFunctionConfigurationOutput
	funcs []types.FunctionConfiguration
}

func (f fakeAPI) GetFunctionConfiguration(ctx context.Context, in *awslambda.GetFunctionConfigurationInput, _ ...func(*awslambda.Options)) (*awslambda.GetFunctionConfigurationOutput, error) {
	return f.cfg, nil
}

func (f fakeAPI) ListFunctions(ctx context.Context, in *awslambda.ListFunctionsInput, _ ...func(*awslambda.Options)) (*awslambda.ListFunctionsOutput, error) {
	return &awslambda.ListFunctionsOutput{Functions: f.funcs}, nil
}

func first(t *testing.T, res resolog.Resolution) resolog.Source {
	t.Helper()
	s, ok := <-res.Sources
	if !ok {
		t.Fatal("no source produced")
	}
	return s
}

func TestResolveDefaultGroup(t *testing.T) {
	api := fakeAPI{cfg: &awslambda.GetFunctionConfigurationOutput{FunctionName: aws.String("ingest")}}
	res, err := New(api).Resolve(context.Background(), "ingest")
	if err != nil {
		t.Fatal(err)
	}
	if s := first(t, res); s.LogGroup != "/aws/lambda/ingest" {
		t.Errorf("log group = %q, want /aws/lambda/ingest", s.LogGroup)
	}
}

func TestResolveCustomGroup(t *testing.T) {
	api := fakeAPI{cfg: &awslambda.GetFunctionConfigurationOutput{
		FunctionName:  aws.String("ingest"),
		LoggingConfig: &types.LoggingConfig{LogGroup: aws.String("/custom/lg")},
	}}
	res, err := New(api).Resolve(context.Background(), "ingest")
	if err != nil {
		t.Fatal(err)
	}
	if s := first(t, res); s.LogGroup != "/custom/lg" {
		t.Errorf("log group = %q, want /custom/lg", s.LogGroup)
	}
}

func TestListPrefixFilter(t *testing.T) {
	api := fakeAPI{funcs: []types.FunctionConfiguration{
		{FunctionName: aws.String("ingest-a")},
		{FunctionName: aws.String("ingest-b")},
		{FunctionName: aws.String("other")},
	}}
	refs, err := New(api).List(context.Background(), "ingest")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 {
		t.Fatalf("got %d refs, want 2: %+v", len(refs), refs)
	}
}
