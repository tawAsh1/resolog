// Package loggroup is the simplest Resolver: a reference IS a log group name
// (optionally "group:stream"), so resolution is purely lexical and needs no AWS
// client. An optional client (via WithClient) enables resolog.Lister over
// DescribeLogGroups for `resolog ls log-group <prefix>`.
package loggroup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"

	"github.com/tawAsh1/resolog"
)

// Scheme is the reference scheme this Resolver handles.
const Scheme = "log-group"

// API is the slice of the CloudWatch Logs client needed for listing. Resolve
// does not use it; only List does.
type API interface {
	DescribeLogGroups(ctx context.Context, in *cwl.DescribeLogGroupsInput, optFns ...func(*cwl.Options)) (*cwl.DescribeLogGroupsOutput, error)
}

// Resolver resolves bare log group references.
type Resolver struct {
	api API // optional; only required for List
}

// Option configures a Resolver.
type Option func(*Resolver)

// WithClient enables List by supplying a CloudWatch Logs client.
func WithClient(api API) Option {
	return func(r *Resolver) { r.api = api }
}

// New returns a loggroup Resolver. Without WithClient it resolves but cannot list.
func New(opts ...Option) *Resolver {
	r := &Resolver{}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Scheme implements resolog.Resolver.
func (r *Resolver) Scheme() string { return Scheme }

// Resolve turns "my-group" or "my-group:my-stream" into a single Source. The
// Resolution completes immediately (one source, no terminal state: a log group
// is never "done", so Done is nil and following runs until the caller cancels).
func (r *Resolver) Resolve(ctx context.Context, ref string) (resolog.Resolution, error) {
	group, stream := ref, ""
	if i := strings.Index(ref, ":"); i >= 0 {
		group, stream = ref[:i], ref[i+1:]
	}

	sources := make(chan resolog.Source, 1)
	sources <- resolog.Source{
		Key:       ref,
		Label:     group,
		LogGroup:  group,
		LogStream: stream,
	}
	close(sources)

	return resolog.Resolution{Sources: sources, Done: nil}, nil
}

// List implements resolog.Lister, enumerating log groups whose name starts with
// filter. Requires a client (WithClient).
func (r *Resolver) List(ctx context.Context, filter string) ([]resolog.ResourceRef, error) {
	if r.api == nil {
		return nil, fmt.Errorf("log-group list needs a client (loggroup.WithClient)")
	}
	in := &cwl.DescribeLogGroupsInput{}
	if filter != "" {
		in.LogGroupNamePrefix = aws.String(filter)
	}
	var refs []resolog.ResourceRef
	for {
		out, err := r.api.DescribeLogGroups(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, g := range out.LogGroups {
			name := aws.ToString(g.LogGroupName)
			ref := resolog.ResourceRef{Scheme: Scheme, Ref: name, Label: name}
			if g.CreationTime != nil {
				ref.StartedAt = time.UnixMilli(*g.CreationTime)
			}
			refs = append(refs, ref)
		}
		if out.NextToken == nil {
			return refs, nil
		}
		in.NextToken = out.NextToken
	}
}
