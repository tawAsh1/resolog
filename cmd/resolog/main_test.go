package main

import (
	"testing"

	"github.com/tawAsh1/resolog"
	"github.com/tawAsh1/resolog/resolver/batch"
	"github.com/tawAsh1/resolog/resolver/ecs"
	"github.com/tawAsh1/resolog/resolver/lambda"
	"github.com/tawAsh1/resolog/resolver/loggroup"
	"github.com/tawAsh1/resolog/resolver/sfn"
)

// resolvers is the set of known schemes, sans AWS clients — splitRef only needs
// the keys.
func testResolvers() map[string]resolog.Resolver {
	return map[string]resolog.Resolver{
		loggroup.Scheme: nil,
		batch.Scheme:    nil,
		sfn.Scheme:      nil,
		lambda.Scheme:   nil,
		ecs.Scheme:      nil,
	}
}

func TestSplitRef(t *testing.T) {
	cases := []struct {
		name, arg, scheme, ref string
	}{
		// Raw ARNs dispatch by service, ref keeps the ARN.
		{"ecs arn", "arn:aws:ecs:us-east-1:111:task/prod/abc", ecs.Scheme, "arn:aws:ecs:us-east-1:111:task/prod/abc"},
		{"sfn execution arn", "arn:aws:states:us-east-1:111:execution:sm:run", sfn.Scheme, "arn:aws:states:us-east-1:111:execution:sm:run"},
		{"lambda arn", "arn:aws:lambda:us-east-1:111:function:my-fn", lambda.Scheme, "arn:aws:lambda:us-east-1:111:function:my-fn"},
		{"lambda arn with qualifier", "arn:aws:lambda:us-east-1:111:function:my-fn:PROD", lambda.Scheme, "arn:aws:lambda:us-east-1:111:function:my-fn:PROD"},
		// Batch keys off the bare job id.
		{"batch arn", "arn:aws:batch:us-east-1:111:job/abc-123", batch.Scheme, "abc-123"},
		// Log-group ARN reduces to the bare group name.
		{"logs arn", "arn:aws:logs:us-east-1:111:log-group:/my/group:*", loggroup.Scheme, "/my/group"},
		// A state-machine ARN is not an execution; no resolver claims it, so it
		// falls through to the bare-log-group form.
		{"sfn state machine arn falls through", "arn:aws:states:us-east-1:111:stateMachine:sm", loggroup.Scheme, "arn:aws:states:us-east-1:111:stateMachine:sm"},
		// Explicit scheme shorthand still works.
		{"explicit scheme", "batch-job:abc-123", batch.Scheme, "abc-123"},
		{"ecs cluster/id shorthand", "ecs-task:prod/abc", ecs.Scheme, "prod/abc"},
		// Bare names.
		{"bare log group", "/aws/lambda/my-fn", loggroup.Scheme, "/aws/lambda/my-fn"},
		{"log-group with stream", "log-group:/my/group:my-stream", loggroup.Scheme, "/my/group:my-stream"},
	}
	rs := testResolvers()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			scheme, ref := splitRef(c.arg, rs)
			if scheme != c.scheme || ref != c.ref {
				t.Errorf("splitRef(%q) = (%q, %q), want (%q, %q)", c.arg, scheme, ref, c.scheme, c.ref)
			}
		})
	}
}
