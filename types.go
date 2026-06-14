package resolog

import (
	"context"
	"time"
)

// Source is a single resolved log stream to tail. A Resolver emits these as it
// discovers them; a Backend turns each one into a stream of Events.
type Source struct {
	// Key is a stable identifier used for color assignment and de-duplication.
	// It must be unique per logical stream within a run.
	Key string

	// Label is the short human-facing name shown in the output gutter,
	// e.g. "lambda/ingest" or "batch[3]".
	Label string

	// LogGroup is the CloudWatch Logs log group name.
	LogGroup string

	// LogStream optionally narrows to a single stream within the group.
	// Empty means "all streams in the group".
	LogStream string
}

// Event is one log line, tagged with the Source it came from.
type Event struct {
	Source    Source
	Timestamp time.Time // the event time reported by CloudWatch
	Message   string
	Ingestion time.Time // when CloudWatch ingested it (may lag Timestamp)
}

// Resolver maps a resource reference to the log sources it produces. Resolvers
// are the primary extension point: log-group, batch-job, sfn-execution, lambda…
//
// A Resolver is just a package that returns a New() value; consumers wire them
// explicitly. There is deliberately no global registry.
type Resolver interface {
	// Scheme is the reference prefix this Resolver handles, e.g.
	// "sfn-execution" or "batch-job". Used by CLI-level scheme dispatch.
	Scheme() string

	// Resolve turns a reference into a Resolution. ref is the scheme-stripped
	// remainder (an ARN, a name, a job id…). Resolve should return quickly;
	// ongoing discovery happens on the returned channels.
	Resolve(ctx context.Context, ref string) (Resolution, error)
}

// Resolution is the live result of resolving a reference. Sources may grow over
// time (a running Step Functions execution spawns tasks); Done reports that the
// underlying resource has reached a terminal state.
type Resolution struct {
	// Sources delivers log sources as they are discovered. It is closed once
	// discovery is complete (no more sources will ever appear).
	Sources <-chan Source

	// Done is closed when the resource reaches a terminal state (job SUCCEEDED,
	// execution FAILED…). It drives --follow shutdown. A nil Done means the
	// resource is never terminal (e.g. a bare log group), so following runs
	// until the caller cancels the context.
	//
	// Completion MUST be driven by resource status, never by "logs went quiet":
	// CloudWatch lags, and the last lines often arrive after the resource ends.
	// Tail applies a grace period after Done before stopping. Keeping this as a
	// signal (not a return value) prevents Batch/SFn status logic from leaking
	// into the tailer.
	Done <-chan struct{}
}

// Backend turns a Source into a stream of Events. Implementations: livetail
// (StartLiveTail, real-time), poll (GetLogEvents, historical), mock (demo).
type Backend interface {
	// Stream returns a channel of Events for src. The channel is closed when
	// the stream ends (historical exhausted, or ctx cancelled). A live backend
	// only ends on ctx cancellation.
	Stream(ctx context.Context, src Source) (<-chan Event, error)
}

// Sink consumes the merged, interleaved event stream. The default Sink is the
// renderer in this package; a consumer can supply its own (JSON, custom TUI…).
type Sink interface {
	// Consume reads events until the channel is closed or ctx is cancelled,
	// then returns. The error is propagated out of Tail.
	Consume(ctx context.Context, events <-chan Event) error
}

// ResourceRef is one entry from a Lister: a reference that could be tailed,
// plus enough metadata to show it in a picker.
type ResourceRef struct {
	Scheme    string // the Resolver scheme this ref belongs to
	Ref       string // the scheme-stripped reference, ready to pass to Resolve
	Label     string
	Status    string // e.g. "RUNNING", "SUCCEEDED" (resolver-defined)
	StartedAt time.Time
}

// Lister is the dual of a Resolver: where Resolve maps ref -> sources, List
// maps a kind -> the refs that exist. It is an optional, separate interface so
// a Resolver can implement it (or not); CLI code type-asserts for it to offer
// "list -> pick -> tail" in one breath.
//
// Cleanly enumerable kinds have an enumerate API (log groups, SFn executions,
// Batch jobs, Lambdas). EC2 does not ("does this instance even have logs?"),
// so it is deliberately not a List target.
type Lister interface {
	// List returns the refs matching filter (a prefix, status, etc. — the exact
	// meaning is resolver-defined).
	List(ctx context.Context, filter string) ([]ResourceRef, error)
}
