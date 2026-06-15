// Package ecs resolves an ECS task into its CloudWatch log sources, one per
// container that uses the awslogs driver. It works like resolver/batch, and is
// the resolver Step Functions uses for its ecs:runTask.sync integration.
//
// Unlike Batch, an ECS task detail does not carry the computed log stream name,
// so this resolver reads it from two places: the task definition supplies each
// container's awslogs configuration (group + stream prefix), and the running
// task supplies the per-container identity (the ECS task id, and the runtime id
// for the no-prefix case). The stream name is then:
//
//   - with awslogs-stream-prefix:  <prefix>/<container>/<taskID>
//   - without one:                 the container's runtime (docker) id
//
// Discovery polls DescribeTasks: a container's runtime id only exists once it
// starts, so sources are emitted as they appear. Completion is driven off task
// status (lastStatus == STOPPED), never off log silence.
package ecs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"

	"github.com/tawAsh1/resolog"
)

// Scheme is the reference scheme this Resolver handles. A reference is an ECS
// task ARN, a "<cluster>/<taskId>" pair, or a bare task id (cluster defaults to
// "default"): "ecs-task:<ref>".
const Scheme = "ecs-task"

const defaultPollInterval = 3 * time.Second

// API is the slice of the ECS client this resolver needs, declared locally to
// keep the surface small and mockable. The real *ecs.Client satisfies it.
type API interface {
	DescribeTasks(ctx context.Context, in *awsecs.DescribeTasksInput, optFns ...func(*awsecs.Options)) (*awsecs.DescribeTasksOutput, error)
	DescribeTaskDefinition(ctx context.Context, in *awsecs.DescribeTaskDefinitionInput, optFns ...func(*awsecs.Options)) (*awsecs.DescribeTaskDefinitionOutput, error)
	ListTasks(ctx context.Context, in *awsecs.ListTasksInput, optFns ...func(*awsecs.Options)) (*awsecs.ListTasksOutput, error)
}

// Resolver resolves ECS tasks into log sources.
type Resolver struct {
	api          API
	pollInterval time.Duration
}

// Option configures a Resolver.
type Option func(*Resolver)

// WithPollInterval sets how often a still-running task is re-polled for new
// container log streams. Values <= 0 are ignored. Defaults to 3s.
func WithPollInterval(d time.Duration) Option {
	return func(r *Resolver) {
		if d > 0 {
			r.pollInterval = d
		}
	}
}

// New builds an ecs Resolver from an ECS client.
func New(api API, opts ...Option) *Resolver {
	r := &Resolver{api: api, pollInterval: defaultPollInterval}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Scheme implements resolog.Resolver.
func (r *Resolver) Scheme() string { return Scheme }

// Resolve implements resolog.Resolver. ref is a task ARN, "<cluster>/<taskId>",
// or a bare task id. It validates the task up front, reads the task definition's
// log configuration once, then polls until the task status is terminal, emitting
// one Source per container as its log stream becomes addressable.
func (r *Resolver) Resolve(ctx context.Context, ref string) (resolog.Resolution, error) {
	cluster, task := parseTaskRef(ref)
	out, err := r.api.DescribeTasks(ctx, &awsecs.DescribeTasksInput{
		Cluster: aws.String(cluster),
		Tasks:   []string{task},
	})
	if err != nil {
		return resolog.Resolution{}, fmt.Errorf("describe tasks: %w", err)
	}
	if len(out.Tasks) == 0 {
		return resolog.Resolution{}, fmt.Errorf("ecs task %q not found in cluster %q", task, cluster)
	}
	defArn := aws.ToString(out.Tasks[0].TaskDefinitionArn)

	sources := make(chan resolog.Source)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(sources)

		logCfg, err := r.loadLogConfig(ctx, defArn)
		if err != nil {
			return
		}

		seen := map[string]bool{}
		for {
			dt, err := r.api.DescribeTasks(ctx, &awsecs.DescribeTasksInput{
				Cluster: aws.String(cluster),
				Tasks:   []string{task},
			})
			if err != nil || len(dt.Tasks) == 0 {
				return
			}
			t := dt.Tasks[0]

			for _, c := range t.Containers {
				s, ok := containerSource(t, c, logCfg)
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

			if aws.ToString(t.LastStatus) == "STOPPED" {
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

// loadLogConfig reads the task definition once and maps each container name to
// its log configuration. Containers without one are simply absent from the map.
func (r *Resolver) loadLogConfig(ctx context.Context, defArn string) (map[string]types.LogConfiguration, error) {
	out, err := r.api.DescribeTaskDefinition(ctx, &awsecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String(defArn),
	})
	if err != nil {
		return nil, err
	}
	m := map[string]types.LogConfiguration{}
	if out.TaskDefinition != nil {
		for _, cd := range out.TaskDefinition.ContainerDefinitions {
			if cd.LogConfiguration != nil {
				m[aws.ToString(cd.Name)] = *cd.LogConfiguration
			}
		}
	}
	return m, nil
}

// containerSource maps a running container to its log source, if it uses the
// awslogs driver and its stream is addressable yet.
func containerSource(t types.Task, c types.Container, logCfg map[string]types.LogConfiguration) (resolog.Source, bool) {
	name := aws.ToString(c.Name)
	cfg, ok := logCfg[name]
	if !ok || cfg.LogDriver != types.LogDriverAwslogs {
		return resolog.Source{}, false
	}
	group := cfg.Options["awslogs-group"]
	if group == "" {
		return resolog.Source{}, false
	}

	var stream string
	if prefix := cfg.Options["awslogs-stream-prefix"]; prefix != "" {
		// ECS composes the stream deterministically from the prefix, the
		// container name, and the task id — known as soon as the task exists.
		stream = prefix + "/" + name + "/" + taskID(aws.ToString(t.TaskArn))
	} else if rid := aws.ToString(c.RuntimeId); rid != "" {
		// No prefix: the awslogs driver defaults the stream to the container's
		// runtime (docker) id, which only exists once the container starts.
		stream = rid
	} else {
		return resolog.Source{}, false
	}

	return resolog.Source{
		Key:       "ecs:" + stream,
		Label:     "ecs/" + name,
		LogGroup:  group,
		LogStream: stream,
	}, true
}

// parseTaskRef splits a reference into the cluster and the task identifier to
// pass to DescribeTasks. A task ARN carries its cluster (new long-ARN format);
// a "<cluster>/<taskId>" pair states it explicitly; a bare id falls back to the
// "default" cluster.
func parseTaskRef(ref string) (cluster, task string) {
	switch {
	case strings.HasPrefix(ref, "arn:"):
		// arn:partition:ecs:region:account:task/<cluster>/<id> (new) or
		// arn:partition:ecs:region:account:task/<id> (old, no cluster).
		cluster = "default"
		if p := strings.SplitN(ref, ":", 6); len(p) == 6 {
			if seg := strings.Split(p[5], "/"); len(seg) == 3 {
				cluster = seg[1]
			}
		}
		return cluster, ref
	case strings.Contains(ref, "/"):
		i := strings.Index(ref, "/")
		return ref[:i], ref[i+1:]
	default:
		return "default", ref
	}
}

// taskID returns the trailing id segment of a task ARN (or id).
func taskID(taskArn string) string {
	if i := strings.LastIndex(taskArn, "/"); i >= 0 {
		return taskArn[i+1:]
	}
	return taskArn
}

// List implements resolog.Lister. filter is a cluster name; it returns the
// RUNNING tasks in that cluster (the ones worth tailing). Resolve then takes one
// of the returned task ARNs.
func (r *Resolver) List(ctx context.Context, filter string) ([]resolog.ResourceRef, error) {
	if filter == "" {
		return nil, fmt.Errorf("ecs list needs a cluster as the filter")
	}
	var refs []resolog.ResourceRef
	var token *string
	for {
		lt, err := r.api.ListTasks(ctx, &awsecs.ListTasksInput{
			Cluster:       aws.String(filter),
			DesiredStatus: types.DesiredStatusRunning,
			NextToken:     token,
		})
		if err != nil {
			return nil, err
		}
		if len(lt.TaskArns) > 0 {
			dt, err := r.api.DescribeTasks(ctx, &awsecs.DescribeTasksInput{
				Cluster: aws.String(filter),
				Tasks:   lt.TaskArns,
			})
			if err != nil {
				return nil, err
			}
			for _, t := range dt.Tasks {
				ref := resolog.ResourceRef{
					Scheme: Scheme,
					Ref:    aws.ToString(t.TaskArn),
					Label:  aws.ToString(t.Group),
					Status: aws.ToString(t.LastStatus),
				}
				if t.StartedAt != nil {
					ref.StartedAt = *t.StartedAt
				}
				refs = append(refs, ref)
			}
		}
		if lt.NextToken == nil {
			return refs, nil
		}
		token = lt.NextToken
	}
}
