// Package sfn is the flagship Resolver: hand it a Step Functions execution and
// it resolves every Lambda / Batch task that execution ran, streaming new
// sources as a running execution progresses and signalling Done when it reaches
// a terminal state.
//
// The mapping is derived from a single GetExecutionHistory walk — no extra
// per-task API calls — which is what makes "tail this execution" cheap:
//
//   - Lambda, direct-ARN form  -> LambdaFunctionScheduled.Resource
//   - Lambda, lambda:invoke    -> TaskScheduled(ResourceType=lambda).Parameters.FunctionName
//     both map to /aws/lambda/<name>.
//
// Batch tasks are handled one of two ways:
//
//   - With a delegate Resolver set (WithBatchResolver) the JobId from each Batch
//     TaskSubmitted is resolved through resolver/batch, which uses DescribeJobs
//     and therefore handles still-RUNNING jobs and array children too.
//   - Without one, only completed .sync Batch tasks are mapped, cheaply, from
//     Task{Succeeded}.Output.Container.LogStreamName — no extra API call.
package sfn

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/tawAsh1/resolog"
)

// Scheme is the reference scheme this Resolver handles. A reference is an
// execution ARN: "sfn-execution:arn:aws:states:...:execution:...".
const Scheme = "sfn-execution"

// DefaultBatchLogGroup is the log group AWS Batch writes to by default, used by
// the no-delegate cheap path. The delegate path derives the group itself.
const DefaultBatchLogGroup = "/aws/batch/job"

const defaultPollInterval = 3 * time.Second

// API is the slice of the Step Functions client this resolver needs. The real
// *sfn.Client satisfies it.
type API interface {
	DescribeExecution(ctx context.Context, in *awssfn.DescribeExecutionInput, optFns ...func(*awssfn.Options)) (*awssfn.DescribeExecutionOutput, error)
	GetExecutionHistory(ctx context.Context, in *awssfn.GetExecutionHistoryInput, optFns ...func(*awssfn.Options)) (*awssfn.GetExecutionHistoryOutput, error)
	ListExecutions(ctx context.Context, in *awssfn.ListExecutionsInput, optFns ...func(*awssfn.Options)) (*awssfn.ListExecutionsOutput, error)
}

// Option configures a Resolver.
type Option func(*Resolver)

// WithBatchResolver wires a resolver/batch-style delegate for Batch tasks. Its
// reference is a Batch job id. When set, Batch tasks are resolved through it
// (covering running jobs and array children) instead of the cheap completed-only
// history path.
func WithBatchResolver(batch resolog.Resolver) Option {
	return func(r *Resolver) { r.batch = batch }
}

// Resolver resolves Step Functions executions into their constituent log
// sources.
type Resolver struct {
	api          API
	batch        resolog.Resolver
	pollInterval time.Duration
}

// New builds an sfn Resolver from a Step Functions client.
func New(api API, opts ...Option) *Resolver {
	r := &Resolver{api: api, pollInterval: defaultPollInterval}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Scheme implements resolog.Resolver.
func (r *Resolver) Scheme() string { return Scheme }

// Resolve implements resolog.Resolver. ref is an execution ARN. It validates the
// execution up front, then discovers sources on a goroutine until the execution
// reaches a terminal state.
func (r *Resolver) Resolve(ctx context.Context, ref string) (resolog.Resolution, error) {
	desc, err := r.api.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{ExecutionArn: &ref})
	if err != nil {
		return resolog.Resolution{}, fmt.Errorf("describe execution: %w", err)
	}

	sources := make(chan resolog.Source)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var subwg sync.WaitGroup
		r.discover(ctx, ref, desc.Status, sources, &subwg)
		subwg.Wait() // let delegated sub-resolutions finish before closing
		close(sources)
	}()

	return resolog.Resolution{Sources: sources, Done: done}, nil
}

// discover sweeps the history, emitting new sources and (when a delegate is set)
// fanning in delegated Batch sub-resolutions, until the execution is terminal.
func (r *Resolver) discover(ctx context.Context, arn string, status types.ExecutionStatus, out chan<- resolog.Source, subwg *sync.WaitGroup) {
	seen := map[string]bool{}
	seenJobs := map[string]bool{}
	for {
		srcs, jobIDs, err := r.sweep(ctx, arn)
		if err != nil {
			return
		}
		for _, s := range srcs {
			if seen[s.Key] {
				continue
			}
			seen[s.Key] = true
			select {
			case out <- s:
			case <-ctx.Done():
				return
			}
		}
		for _, jid := range jobIDs {
			if seenJobs[jid] {
				continue
			}
			seenJobs[jid] = true
			subwg.Add(1)
			go func(id string) {
				defer subwg.Done()
				r.pumpDelegate(ctx, id, out)
			}(jid)
		}

		if status != types.ExecutionStatusRunning && status != types.ExecutionStatusPendingRedrive {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.pollInterval):
		}
		d, err := r.api.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{ExecutionArn: &arn})
		if err != nil {
			return
		}
		status = d.Status
	}
}

// pumpDelegate resolves a Batch job id through the delegate and forwards its
// sources into out. The delegate's Done is ignored; the execution's own Done
// governs shutdown.
func (r *Resolver) pumpDelegate(ctx context.Context, jobID string, out chan<- resolog.Source) {
	res, err := r.batch.Resolve(ctx, jobID)
	if err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case s, ok := <-res.Sources:
			if !ok {
				return
			}
			select {
			case out <- s:
			case <-ctx.Done():
				return
			}
		}
	}
}

// sweep pages the full execution history and maps it to sources (and, when a
// delegate is set, Batch job ids to resolve through it).
func (r *Resolver) sweep(ctx context.Context, arn string) (srcs []resolog.Source, jobIDs []string, err error) {
	var token *string
	includeData := true
	for {
		page, perr := r.api.GetExecutionHistory(ctx, &awssfn.GetExecutionHistoryInput{
			ExecutionArn:         &arn,
			IncludeExecutionData: &includeData,
			NextToken:            token,
		})
		if perr != nil {
			return nil, nil, perr
		}
		for i := range page.Events {
			e := page.Events[i]
			if s, ok := lambdaFromEvent(e); ok {
				srcs = append(srcs, s)
				continue
			}
			if r.batch != nil {
				if jid, ok := batchJobID(e); ok {
					jobIDs = append(jobIDs, jid)
				}
				continue
			}
			if s, ok := batchSourceFromEvent(e); ok {
				srcs = append(srcs, s)
			}
		}
		if page.NextToken == nil {
			return srcs, jobIDs, nil
		}
		token = page.NextToken
	}
}

// lambdaFromEvent maps either Lambda task form to a source.
func lambdaFromEvent(e types.HistoryEvent) (resolog.Source, bool) {
	switch {
	case e.LambdaFunctionScheduledEventDetails != nil:
		return lambdaSource(lambdaName(deref(e.LambdaFunctionScheduledEventDetails.Resource)))
	case e.TaskScheduledEventDetails != nil && deref(e.TaskScheduledEventDetails.ResourceType) == "lambda":
		var p struct {
			FunctionName string `json:"FunctionName"`
		}
		_ = json.Unmarshal([]byte(deref(e.TaskScheduledEventDetails.Parameters)), &p)
		return lambdaSource(lambdaName(p.FunctionName))
	}
	return resolog.Source{}, false
}

// batchJobID extracts a Batch job id from a TaskSubmitted event, for delegation.
func batchJobID(e types.HistoryEvent) (string, bool) {
	if e.TaskSubmittedEventDetails == nil || deref(e.TaskSubmittedEventDetails.ResourceType) != "batch" {
		return "", false
	}
	var o struct {
		JobId string `json:"JobId"`
	}
	if err := json.Unmarshal([]byte(deref(e.TaskSubmittedEventDetails.Output)), &o); err != nil || o.JobId == "" {
		return "", false
	}
	return o.JobId, true
}

// batchSourceFromEvent is the cheap, no-delegate path: a completed .sync Batch
// task's output carries the exact log stream.
func batchSourceFromEvent(e types.HistoryEvent) (resolog.Source, bool) {
	if e.TaskSucceededEventDetails == nil || deref(e.TaskSucceededEventDetails.ResourceType) != "batch" {
		return resolog.Source{}, false
	}
	return batchSource(deref(e.TaskSucceededEventDetails.Output))
}

func lambdaSource(name string) (resolog.Source, bool) {
	if name == "" {
		return resolog.Source{}, false
	}
	return resolog.Source{
		Key:      "lambda:" + name,
		Label:    "λ " + name,
		LogGroup: "/aws/lambda/" + name,
	}, true
}

// batchSource extracts the log stream from a completed .sync Batch task output
// (a DescribeJobs-shaped JSON blob).
func batchSource(output string) (resolog.Source, bool) {
	var job struct {
		JobId     string `json:"JobId"`
		JobName   string `json:"JobName"`
		Container struct {
			LogStreamName string `json:"LogStreamName"`
		} `json:"Container"`
	}
	if err := json.Unmarshal([]byte(output), &job); err != nil {
		return resolog.Source{}, false
	}
	if job.Container.LogStreamName == "" {
		return resolog.Source{}, false
	}
	label := job.JobName
	if label == "" {
		label = job.JobId
	}
	return resolog.Source{
		Key:       "batch:" + job.Container.LogStreamName,
		Label:     "batch/" + label,
		LogGroup:  DefaultBatchLogGroup,
		LogStream: job.Container.LogStreamName,
	}, true
}

// lambdaName normalizes a function reference (full ARN, partial ARN, or bare
// name, with or without a version/alias qualifier) to its bare function name.
func lambdaName(s string) string {
	if i := strings.Index(s, "function:"); i >= 0 {
		s = s[i+len("function:"):]
	}
	if i := strings.IndexByte(s, ':'); i >= 0 { // strip :version or :alias
		s = s[:i]
	}
	return s
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// List implements resolog.Lister. filter is a state-machine ARN whose executions
// are enumerated (most recent first). Resolve then takes one of the returned
// execution ARNs.
func (r *Resolver) List(ctx context.Context, filter string) ([]resolog.ResourceRef, error) {
	if filter == "" {
		return nil, fmt.Errorf("sfn list needs a state-machine ARN as the filter")
	}
	out, err := r.api.ListExecutions(ctx, &awssfn.ListExecutionsInput{StateMachineArn: &filter})
	if err != nil {
		return nil, err
	}
	refs := make([]resolog.ResourceRef, 0, len(out.Executions))
	for _, e := range out.Executions {
		ref := resolog.ResourceRef{
			Scheme: Scheme,
			Ref:    deref(e.ExecutionArn),
			Label:  deref(e.Name),
			Status: string(e.Status),
		}
		if e.StartDate != nil {
			ref.StartedAt = *e.StartDate
		}
		refs = append(refs, ref)
	}
	return refs, nil
}
