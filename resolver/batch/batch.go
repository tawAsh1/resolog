// Package batch resolves AWS Batch jobs into their CloudWatch log sources. This
// is the resolver batchkoi consumes (or supplies its own logic to): array jobs
// fan out into one Source per child index, mirroring batchkoi's colored,
// interleaved behavior. It is also what resolver/sfn delegates to for Batch
// tasks that are still running (where only a JobId, not a log stream, is known).
//
// Resolution is driven off DescribeJobs: a child's log stream
// (Container.LogStreamName) only exists once its container starts, so the
// resolver polls and emits each Source as it appears. Completion is driven off
// job status (SUCCEEDED/FAILED), never off log silence.
package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsbatch "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"

	"github.com/tawAsh1/resolog"
)

// Scheme is the reference scheme this Resolver handles. A reference is a Batch
// job id: "batch-job:<jobId>".
const Scheme = "batch-job"

// DefaultLogGroup is the log group AWS Batch writes to unless a job definition
// overrides it via an awslogs log configuration.
const DefaultLogGroup = "/aws/batch/job"

const (
	defaultPollInterval = 3 * time.Second
	describeChunk       = 100 // DescribeJobs accepts at most 100 ids per call
)

// API is the slice of the AWS Batch client this resolver needs, declared
// locally to keep the surface small and mockable. The real *batch.Client
// satisfies it.
type API interface {
	DescribeJobs(ctx context.Context, in *awsbatch.DescribeJobsInput, optFns ...func(*awsbatch.Options)) (*awsbatch.DescribeJobsOutput, error)
	ListJobs(ctx context.Context, in *awsbatch.ListJobsInput, optFns ...func(*awsbatch.Options)) (*awsbatch.ListJobsOutput, error)
}

// Resolver resolves Batch jobs into log sources.
type Resolver struct {
	api          API
	pollInterval time.Duration
}

// New builds a batch Resolver from an AWS Batch client.
func New(api API) *Resolver {
	return &Resolver{api: api, pollInterval: defaultPollInterval}
}

// Scheme implements resolog.Resolver.
func (r *Resolver) Scheme() string { return Scheme }

// Resolve implements resolog.Resolver. ref is a Batch job id. It validates the
// job up front, then discovers sources on a goroutine: for an array job it
// expands to one child per index; for any job it polls until the (parent)
// status is terminal.
func (r *Resolver) Resolve(ctx context.Context, ref string) (resolog.Resolution, error) {
	out, err := r.api.DescribeJobs(ctx, &awsbatch.DescribeJobsInput{Jobs: []string{ref}})
	if err != nil {
		return resolog.Resolution{}, fmt.Errorf("describe jobs: %w", err)
	}
	if len(out.Jobs) == 0 {
		return resolog.Resolution{}, fmt.Errorf("batch job %q not found", ref)
	}
	size := arraySize(out.Jobs[0])

	sources := make(chan resolog.Source)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(sources)

		seen := map[string]bool{}
		for {
			ids := []string{ref}
			for i := 0; i < size; i++ {
				ids = append(ids, fmt.Sprintf("%s:%d", ref, i))
			}
			jobs, err := r.describeAll(ctx, ids)
			if err != nil {
				return
			}

			var refStatus types.JobStatus
			for i := range jobs {
				j := jobs[i]
				if aws.ToString(j.JobId) == ref {
					refStatus = j.Status
					if size > 0 {
						continue // array parent has no container of its own
					}
				}
				s, ok := logSource(j)
				if !ok || seen[s.Key] {
					continue
				}
				seen[s.Key] = true
				select {
				case sources <- s:
				case <-ctx.Done():
					return
				}
			}

			if isTerminal(refStatus) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.pollInterval):
			}
		}
	}()

	return resolog.Resolution{Sources: sources, Done: done}, nil
}

// describeAll pages DescribeJobs across chunks of ids and concatenates results.
func (r *Resolver) describeAll(ctx context.Context, ids []string) ([]types.JobDetail, error) {
	var jobs []types.JobDetail
	for start := 0; start < len(ids); start += describeChunk {
		end := start + describeChunk
		if end > len(ids) {
			end = len(ids)
		}
		out, err := r.api.DescribeJobs(ctx, &awsbatch.DescribeJobsInput{Jobs: ids[start:end]})
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, out.Jobs...)
	}
	return jobs, nil
}

// logSource maps a job to its log source, if its container has a log stream yet.
func logSource(job types.JobDetail) (resolog.Source, bool) {
	c := job.Container
	if c == nil {
		return resolog.Source{}, false
	}
	stream := aws.ToString(c.LogStreamName)
	if stream == "" {
		return resolog.Source{}, false
	}

	group := DefaultLogGroup
	if c.LogConfiguration != nil && string(c.LogConfiguration.LogDriver) == "awslogs" {
		if g := c.LogConfiguration.Options["awslogs-group"]; g != "" {
			group = g
		}
	}

	label := "batch/" + aws.ToString(job.JobName)
	if job.ArrayProperties != nil && job.ArrayProperties.Index != nil {
		label = fmt.Sprintf("batch[%d]", *job.ArrayProperties.Index)
	}

	return resolog.Source{
		Key:       "batch:" + stream,
		Label:     label,
		LogGroup:  group,
		LogStream: stream,
	}, true
}

func arraySize(job types.JobDetail) int {
	if job.ArrayProperties != nil && job.ArrayProperties.Size != nil {
		return int(*job.ArrayProperties.Size)
	}
	return 0
}

func isTerminal(s types.JobStatus) bool {
	return s == types.JobStatusSucceeded || s == types.JobStatusFailed
}

// List implements resolog.Lister. filter is a job queue name; it returns the
// RUNNING jobs in that queue (the ones worth tailing). Resolve then takes one of
// the returned job ids.
func (r *Resolver) List(ctx context.Context, filter string) ([]resolog.ResourceRef, error) {
	if filter == "" {
		return nil, fmt.Errorf("batch list needs a job queue as the filter")
	}
	var refs []resolog.ResourceRef
	var token *string
	for {
		out, err := r.api.ListJobs(ctx, &awsbatch.ListJobsInput{
			JobQueue:  aws.String(filter),
			JobStatus: types.JobStatusRunning,
			NextToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, j := range out.JobSummaryList {
			ref := resolog.ResourceRef{
				Scheme: Scheme,
				Ref:    aws.ToString(j.JobId),
				Label:  aws.ToString(j.JobName),
				Status: string(j.Status),
			}
			if j.StartedAt != nil {
				ref.StartedAt = time.UnixMilli(*j.StartedAt)
			}
			refs = append(refs, ref)
		}
		if out.NextToken == nil {
			return refs, nil
		}
		token = out.NextToken
	}
}
