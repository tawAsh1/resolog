// Command resolog is the reference CLI for the resolog library. It demonstrates
// the intended wiring: a scheme dispatch table mapping reference prefixes to
// explicitly-composed Resolvers, a chosen Backend, and the default renderer
// Sink. The library itself has no registry — this map is the only dispatch
// layer, and it lives here in the consumer, not in the core.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsbatch "github.com/aws/aws-sdk-go-v2/service/batch"
	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	awsecs "github.com/aws/aws-sdk-go-v2/service/ecs"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	"golang.org/x/term"

	"github.com/tawAsh1/resolog"
	"github.com/tawAsh1/resolog/backend/livetail"
	"github.com/tawAsh1/resolog/backend/poll"
	"github.com/tawAsh1/resolog/resolver/batch"
	"github.com/tawAsh1/resolog/resolver/ecs"
	"github.com/tawAsh1/resolog/resolver/lambda"
	"github.com/tawAsh1/resolog/resolver/loggroup"
	"github.com/tawAsh1/resolog/resolver/sfn"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

// resolveVersion falls back to the module version from build info when no
// ldflags were set — `go install .../resolog/cmd/resolog@v0.1.0` embeds the
// version there, so those installs report it instead of "dev".
func resolveVersion() string {
	if version != "dev" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return version
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "resolog:", err)
		os.Exit(1)
	}
}

func run(argv []string) error {
	switch {
	case len(argv) > 0 && (argv[0] == "version" || argv[0] == "--version" || argv[0] == "-v"):
		fmt.Println("resolog", resolveVersion())
		return nil
	case len(argv) > 0 && argv[0] == "ls":
		return runLs(argv[1:])
	default:
		return runTail(argv)
	}
}

func runTail(argv []string) error {
	fs := flag.NewFlagSet("resolog", flag.ContinueOnError)
	backendName := fs.String("backend", "live", "backend: live|poll")
	follow := fs.Bool("f", false, "keep polling for new events (poll backend)")
	since := fs.Duration("since", 0, "only fetch events newer than this ago, e.g. 10m (poll backend)")
	until := fs.Duration("until", 0, "only fetch events older than this ago; pair with --since for a window (poll backend)")
	noColor := fs.Bool("no-color", false, "disable colored output")
	showTime := fs.Bool("t", false, "show timestamps")
	sortByTime := fs.Bool("sort", false, "buffer and print in time order across streams (needs --backend poll, no -f)")
	sortMax := fs.Int("sort-max", 1_000_000, "max events --sort buffers before erroring (0 = unlimited)")
	fs.Usage = usage(fs)
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("expected exactly one <ref> argument")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := loadConfig(ctx)
	resolvers := buildResolvers(cfg)

	scheme, ref := splitRef(fs.Arg(0), resolvers)
	resolver, ok := resolvers[scheme]
	if !ok {
		return fmt.Errorf("scheme %q is unavailable (AWS config may have failed to load)", scheme)
	}

	var sinceTime, untilTime time.Time
	if *since > 0 {
		sinceTime = time.Now().Add(-*since)
	}
	if *until > 0 {
		untilTime = time.Now().Add(-*until)
	}
	backend, err := buildBackend(*backendName, cfg, poll.Options{Follow: *follow, Since: sinceTime, Until: untilTime})
	if err != nil {
		return err
	}

	res, err := resolver.Resolve(ctx, ref)
	if err != nil {
		return err
	}

	renderer := resolog.NewRenderer(os.Stdout, !*noColor, *showTime)
	renderer.MaxGutter = gutterCap()

	var sink resolog.Sink = renderer
	if *sortByTime {
		if *backendName != "poll" || *follow {
			return errors.New("--sort requires --backend poll without -f (it buffers a finished resource, then prints in time order)")
		}
		sink = resolog.SortingSink{Inner: renderer, Limit: *sortMax}
	}

	onError := func(s resolog.Source, err error) {
		fmt.Fprintf(os.Stderr, "resolog: %s: %v\n", s.LogGroup, err)
	}
	return resolog.Tail(ctx, res, backend, sink, resolog.WithErrorHandler(onError))
}

// gutterCap returns the max width for the label gutter, based on the terminal.
// When stdout is not a terminal (piped), it returns 0 so labels are never
// truncated — downstream tools should see full names. Otherwise it caps the
// gutter at a third of the width, clamped to a sane range, so a long resource
// name can't push messages off the screen.
func gutterCap() int {
	fd := int(os.Stdout.Fd())
	if !term.IsTerminal(fd) {
		return 0
	}
	w, _, err := term.GetSize(fd)
	if err != nil || w <= 0 {
		return 0
	}
	cap := w / 3
	if cap < 12 {
		cap = 12
	}
	if cap > 40 {
		cap = 40
	}
	return cap
}

// runLs lists the refs a resolver can enumerate: `resolog ls <scheme> [filter]`.
func runLs(argv []string) error {
	if len(argv) < 1 {
		return errors.New("usage: resolog ls <scheme> [filter]")
	}
	scheme := argv[0]
	filter := ""
	if len(argv) > 1 {
		filter = argv[1]
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	resolvers := buildResolvers(loadConfig(ctx))
	resolver, ok := resolvers[scheme]
	if !ok {
		return fmt.Errorf("unknown or unavailable scheme %q", scheme)
	}
	lister, ok := resolver.(resolog.Lister)
	if !ok {
		return fmt.Errorf("scheme %q does not support listing", scheme)
	}

	refs, err := lister.List(ctx, filter)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		fmt.Fprintln(os.Stderr, "no matches")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "STATUS\tSTARTED\tREF")
	for _, r := range refs {
		started := ""
		if !r.StartedAt.IsZero() {
			started = r.StartedAt.Format(time.RFC3339)
		}
		label := r.Ref
		if r.Label != "" && r.Label != r.Ref {
			label = r.Label + "  (" + r.Ref + ")"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", dash(r.Status), dash(started), label)
	}
	return w.Flush()
}

func usage(fs *flag.FlagSet) func() {
	return func() {
		fmt.Fprintf(fs.Output(), `resolog — resource-aware CloudWatch log tailer

usage: resolog [flags] <ref>
       resolog ls <scheme> [filter]
       resolog version

  <ref> is a raw resource ARN, "<scheme>:<rest>", or a bare log group name.
  A raw ARN dispatches by its service, so you can paste one as-is. Schemes:
    log-group:<group[:stream]>   tail a log group (default if no scheme)
    sfn-execution:<arn>          tail a Step Functions execution (primary)
    batch-job:<jobId>            tail an AWS Batch job
    lambda:<name>                tail a Lambda function
    ecs-task:<arn|cluster/id>    tail an ECS task (one stream per container)

  ls filters: log-group=<prefix>  sfn-execution=<state-machine-arn>
              batch-job=<queue>   lambda=<name-prefix>  ecs-task=<cluster>

flags:
`)
		fs.PrintDefaults()
	}
}

// loadConfig loads AWS config once; nil means it failed (a real backend then
// reports that it needs config). Resolution of a bare log-group ref is lexical
// and still works, but there is nothing to tail it with.
func loadConfig(ctx context.Context) *aws.Config {
	c, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "resolog: AWS config unavailable:", err)
		return nil
	}
	return &c
}

// buildResolvers is the scheme dispatch table — the one and only dispatch layer,
// here in the CLI. Resolvers are composed explicitly. log-group resolves without
// a client; with one it also lists. The rest need a client.
func buildResolvers(cfg *aws.Config) map[string]resolog.Resolver {
	if cfg == nil {
		return map[string]resolog.Resolver{loggroup.Scheme: loggroup.New()}
	}
	batchResolver := batch.New(awsbatch.NewFromConfig(*cfg))
	ecsResolver := ecs.New(awsecs.NewFromConfig(*cfg))
	return map[string]resolog.Resolver{
		loggroup.Scheme: loggroup.New(loggroup.WithClient(cwl.NewFromConfig(*cfg))),
		batch.Scheme:    batchResolver,
		// The sfn resolver delegates running Batch / ECS tasks to their resolvers
		// so it can tail work that hasn't emitted a log stream into the history yet.
		sfn.Scheme: sfn.New(awssfn.NewFromConfig(*cfg),
			sfn.WithBatchResolver(batchResolver),
			sfn.WithECSResolver(ecsResolver)),
		lambda.Scheme: lambda.New(awslambda.NewFromConfig(*cfg)),
		ecs.Scheme:    ecsResolver,
	}
}

// splitRef turns a CLI argument into the (scheme, ref) pair the dispatch table
// expects. Three forms, tried in order:
//
//  1. A raw resource ARN ("arn:aws:ecs:...:task/...") — dispatched by the ARN's
//     own service, so you can paste an ARN without prefixing it with a scheme.
//  2. An explicit "<scheme>:<rest>" shorthand ("batch-job:abc", "log-group:/x").
//  3. Anything else — a bare log-group name.
func splitRef(arg string, resolvers map[string]resolog.Resolver) (scheme, ref string) {
	if s, r, ok := arnRef(arg); ok && isKnownScheme(s, resolvers) {
		return s, r
	}
	if i := strings.Index(arg, ":"); i >= 0 {
		if s := arg[:i]; isKnownScheme(s, resolvers) {
			return s, arg[i+1:]
		}
	}
	return loggroup.Scheme, arg
}

// arnRef maps a raw resource ARN to the (scheme, ref) its resolver expects. The
// ref is usually the ARN itself — the lambda and sfn resolvers already take ARNs
// — but a couple of services key off the bare resource id, so we strip the
// resource-type prefix for those. Returns ok=false for non-ARNs and for services
// with no resolver, letting splitRef fall through to its other forms.
//
// ARN shape: arn:partition:service:region:account:resource
func arnRef(arg string) (scheme, ref string, ok bool) {
	p := strings.SplitN(arg, ":", 6)
	if len(p) < 6 || p[0] != "arn" {
		return "", "", false
	}
	service, resource := p[2], p[5]
	switch service {
	case "lambda":
		// arn:aws:lambda:...:function:<name>[:<qualifier>]
		return lambda.Scheme, arg, true
	case "states":
		if strings.HasPrefix(resource, "execution:") {
			return sfn.Scheme, arg, true
		}
	case "ecs":
		if strings.HasPrefix(resource, "task/") {
			return ecs.Scheme, arg, true
		}
	case "batch":
		// The batch resolver keys off the bare job id, not the ARN.
		if id := strings.TrimPrefix(resource, "job/"); id != resource {
			return batch.Scheme, id, true
		}
	case "logs":
		// arn:aws:logs:...:log-group:<name>[:*] -> the bare group name.
		if name := strings.TrimPrefix(resource, "log-group:"); name != resource {
			return loggroup.Scheme, strings.TrimSuffix(name, ":*"), true
		}
	}
	return "", "", false
}

func isKnownScheme(s string, resolvers map[string]resolog.Resolver) bool {
	_, ok := resolvers[s]
	return ok
}

func buildBackend(name string, cfg *aws.Config, opts poll.Options) (resolog.Backend, error) {
	switch name {
	case "poll":
		if cfg == nil {
			return nil, fmt.Errorf("backend %q needs AWS config, which failed to load", name)
		}
		return poll.New(cwl.NewFromConfig(*cfg), opts), nil
	case "live":
		if cfg == nil {
			return nil, fmt.Errorf("backend %q needs AWS config, which failed to load", name)
		}
		return livetail.New(cwl.NewFromConfig(*cfg)), nil
	default:
		return nil, fmt.Errorf("unknown backend %q", name)
	}
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
