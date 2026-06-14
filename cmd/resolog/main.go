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
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsbatch "github.com/aws/aws-sdk-go-v2/service/batch"
	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/tawAsh1/resolog"
	"github.com/tawAsh1/resolog/backend/livetail"
	"github.com/tawAsh1/resolog/backend/mock"
	"github.com/tawAsh1/resolog/backend/poll"
	"github.com/tawAsh1/resolog/resolver/batch"
	"github.com/tawAsh1/resolog/resolver/lambda"
	"github.com/tawAsh1/resolog/resolver/loggroup"
	"github.com/tawAsh1/resolog/resolver/sfn"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "resolog:", err)
		os.Exit(1)
	}
}

func run(argv []string) error {
	if len(argv) > 0 && argv[0] == "ls" {
		return runLs(argv[1:])
	}
	return runTail(argv)
}

func runTail(argv []string) error {
	fs := flag.NewFlagSet("resolog", flag.ContinueOnError)
	backendName := fs.String("backend", "live", "backend: live|poll|mock")
	follow := fs.Bool("f", false, "keep polling for new events (poll backend)")
	since := fs.Duration("since", 0, "only fetch events newer than this ago, e.g. 10m (poll backend)")
	noColor := fs.Bool("no-color", false, "disable colored output")
	showTime := fs.Bool("t", false, "show timestamps")
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

	var sinceTime time.Time
	if *since > 0 {
		sinceTime = time.Now().Add(-*since)
	}
	backend, err := buildBackend(*backendName, cfg, poll.Options{Follow: *follow, Since: sinceTime})
	if err != nil {
		return err
	}

	res, err := resolver.Resolve(ctx, ref)
	if err != nil {
		return err
	}

	sink := resolog.NewRenderer(os.Stdout, !*noColor, *showTime)
	onError := func(s resolog.Source, err error) {
		fmt.Fprintf(os.Stderr, "resolog: %s: %v\n", s.LogGroup, err)
	}
	return resolog.Tail(ctx, res, backend, sink, resolog.WithErrorHandler(onError))
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

  <ref> is "<scheme>:<rest>" or a bare log group name. Schemes:
    log-group:<group[:stream]>   tail a log group (default if no scheme)
    sfn-execution:<arn>          tail a Step Functions execution (flagship)
    batch-job:<jobId>            tail an AWS Batch job
    lambda:<name>                tail a Lambda function

  ls filters: log-group=<prefix>  sfn-execution=<state-machine-arn>
              batch-job=<queue>   lambda=<name-prefix>

flags:
`)
		fs.PrintDefaults()
	}
}

// loadConfig loads AWS config once; nil means it failed (offline / no creds),
// in which case only the log-group resolver + mock backend are available.
func loadConfig(ctx context.Context) *aws.Config {
	c, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "resolog: AWS config unavailable, only log-group + mock work:", err)
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
	return map[string]resolog.Resolver{
		loggroup.Scheme: loggroup.New(loggroup.WithClient(cwl.NewFromConfig(*cfg))),
		batch.Scheme:    batchResolver,
		// The flagship delegates running Batch tasks to the batch resolver so it
		// can tail jobs that haven't emitted a log stream into the history yet.
		sfn.Scheme:    sfn.New(awssfn.NewFromConfig(*cfg), sfn.WithBatchResolver(batchResolver)),
		lambda.Scheme: lambda.New(awslambda.NewFromConfig(*cfg)),
	}
}

// splitRef separates a "<scheme>:<rest>" reference. The scheme must be a known
// key; otherwise the whole string is treated as a bare log-group reference.
// Index on the first ":" is safe even for ARNs ("arn:aws:..."), because the
// scheme prefix is checked against the known set first.
func splitRef(arg string, resolvers map[string]resolog.Resolver) (scheme, ref string) {
	if i := strings.Index(arg, ":"); i >= 0 {
		if s := arg[:i]; isKnownScheme(s, resolvers) {
			return s, arg[i+1:]
		}
	}
	return loggroup.Scheme, arg
}

func isKnownScheme(s string, resolvers map[string]resolog.Resolver) bool {
	_, ok := resolvers[s]
	return ok
}

func buildBackend(name string, cfg *aws.Config, opts poll.Options) (resolog.Backend, error) {
	switch name {
	case "mock":
		return mock.New(), nil
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
