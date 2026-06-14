// Package lambda resolves a Lambda function to its log group. By default that is
// the convention /aws/lambda/<name>, but Lambda's advanced logging controls let
// a function write to a custom group, so the resolver reads
// GetFunctionConfiguration to pick that up (and to confirm the function exists).
// It also implements resolog.Lister over ListFunctions.
package lambda

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/tawAsh1/resolog"
)

// Scheme is the reference scheme this Resolver handles. A reference is a
// function name or ARN: "lambda:<name>".
const Scheme = "lambda"

// API is the slice of the Lambda client this resolver needs. The real
// *lambda.Client satisfies it.
type API interface {
	GetFunctionConfiguration(ctx context.Context, in *awslambda.GetFunctionConfigurationInput, optFns ...func(*awslambda.Options)) (*awslambda.GetFunctionConfigurationOutput, error)
	ListFunctions(ctx context.Context, in *awslambda.ListFunctionsInput, optFns ...func(*awslambda.Options)) (*awslambda.ListFunctionsOutput, error)
}

// Resolver resolves Lambda functions into log sources.
type Resolver struct {
	api API
}

// New builds a lambda Resolver from a Lambda client.
func New(api API) *Resolver { return &Resolver{api: api} }

// Scheme implements resolog.Resolver.
func (r *Resolver) Scheme() string { return Scheme }

// Resolve implements resolog.Resolver. ref is a function name or ARN. A
// function's log group is never terminal, so Done is nil and following runs
// until the caller cancels.
func (r *Resolver) Resolve(ctx context.Context, ref string) (resolog.Resolution, error) {
	cfg, err := r.api.GetFunctionConfiguration(ctx, &awslambda.GetFunctionConfigurationInput{
		FunctionName: aws.String(ref),
	})
	if err != nil {
		return resolog.Resolution{}, fmt.Errorf("get function configuration: %w", err)
	}

	name := aws.ToString(cfg.FunctionName)
	group := "/aws/lambda/" + name
	if cfg.LoggingConfig != nil {
		if g := aws.ToString(cfg.LoggingConfig.LogGroup); g != "" {
			group = g
		}
	}

	sources := make(chan resolog.Source, 1)
	sources <- resolog.Source{
		Key:      "lambda:" + name,
		Label:    "λ " + name,
		LogGroup: group,
	}
	close(sources)
	return resolog.Resolution{Sources: sources, Done: nil}, nil
}

// List implements resolog.Lister, enumerating functions whose name starts with
// filter (empty filter lists all).
func (r *Resolver) List(ctx context.Context, filter string) ([]resolog.ResourceRef, error) {
	var refs []resolog.ResourceRef
	var marker *string
	for {
		out, err := r.api.ListFunctions(ctx, &awslambda.ListFunctionsInput{Marker: marker})
		if err != nil {
			return nil, err
		}
		for _, fn := range out.Functions {
			name := aws.ToString(fn.FunctionName)
			if filter != "" && !strings.HasPrefix(name, filter) {
				continue
			}
			refs = append(refs, resolog.ResourceRef{
				Scheme: Scheme,
				Ref:    name,
				Label:  name,
			})
		}
		if out.NextMarker == nil {
			return refs, nil
		}
		marker = out.NextMarker
	}
}
