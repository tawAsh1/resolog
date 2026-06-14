# resolog

**resolog** = *resolve* + *log*. A **resource-aware CloudWatch log tailer.**

Most tools answer *"how do I tail a log group?"* — `aws logs tail`,
[`lucagrulla/cw`](https://github.com/lucagrulla/cw), CloudWatch `StartLiveTail`.
resolog answers the question that comes *before* that:

> **Given a resource, what should I even tail?** — then interleaves every
> stream it finds, docker-compose style.

**Flagship:** hand it a Step Functions execution ARN and it tails the state
machine *plus* every Lambda / Batch / ECS task that execution ran, all together.
No existing tool does this.

## Architecture — three orthogonal seams

A consumer can plug in at any seam; nothing forces data through the whole stack.

| Seam | Role | Implementations |
| --- | --- | --- |
| **Resolver** | resource ref → log sources (+ terminal signal) | `log-group`, `batch-job`, `sfn-execution`, `lambda` |
| **Backend** | a source → a stream of events | `livetail` (real-time), `poll` (historical), `mock` |
| **Sink** | a stream of events → output | `Renderer` (default TUI), or your own |

```
consumer (batchkoi / cmd/resolog / your tool)
        │
        ▼
core (this package) + resolver/* + backend/*   ← one Go module
        │
        ▼
AWS SDK / stdlib
```

The core has **no registry and no plugin system**. Resolvers are just packages
that return a `New()` value; you wire them explicitly. Scheme dispatch
(`sfn-execution:arn:...`) is a single `map[string]Resolver` in
[`cmd/resolog`](cmd/resolog/main.go) — the consumer, not the library.

## Status

v0 — every resolver and backend is real and unit-tested:

- ✅ **core** (`types.go`, `run.go`, `render.go`) — interfaces, the `Tail`
  fan-in orchestrator with status-driven completion + grace period, and the
  default interleaved renderer. Covered by `run_test.go`.
- ✅ **`resolver/loggroup`** — dependency-free, fully working.
- ✅ **`resolver/sfn`** (flagship) — resolves an execution into its Lambda and
  Batch log sources from a single `GetExecutionHistory` walk, polls a running
  execution for new tasks, and fires `Done` on terminal status. Batch tasks can
  be delegated to `resolver/batch` (via `WithBatchResolver`) to cover
  still-running jobs; otherwise completed `.sync` jobs are mapped cheaply from
  history. Covered by `resolver/sfn/sfn_test.go`.
- ✅ **`resolver/batch`** — resolves a Batch job (expanding array children to one
  source each), reads custom `awslogs-group`, and fires `Done` on terminal job
  status. Covered by `resolver/batch/batch_test.go`.
- ✅ **`backend/poll`** — real CloudWatch tailing via `FilterLogEvents` (whole
  group or single stream), with pagination, follow-mode polling, and same-ms
  boundary de-duplication. Covered by `backend/poll/poll_test.go`.
- ✅ **`backend/livetail`** — real-time tailing via `StartLiveTail` (one session
  per source). Event conversion covered by `backend/livetail/livetail_test.go`.
- ✅ **`resolver/lambda`** — resolves a function to `/aws/lambda/<name>` (or its
  advanced-logging custom group via `GetFunctionConfiguration`). Covered by
  `resolver/lambda/lambda_test.go`.
- ✅ **`backend/mock`** — synthetic events so the CLI runs without AWS.
- ✅ **`Lister`** — implemented by every resolver: `log-group` (DescribeLogGroups,
  needs `WithClient`), `sfn-execution` (ListExecutions by state-machine ARN),
  `batch-job` (ListJobs RUNNING by queue), `lambda` (ListFunctions by prefix).
  Exposed via `resolog ls <scheme> [filter]`.

## Try it

```sh
go test ./...
go run ./cmd/resolog log-group:/my/group                        # real-time (StartLiveTail), the default
go run ./cmd/resolog --backend poll -f log-group:/my/group      # historical + follow
go run ./cmd/resolog --backend poll --since 30m \
    sfn-execution:arn:aws:states:...:execution:sm:run          # tail a whole execution
go run ./cmd/resolog --backend mock log-group:demo              # synthetic events, no AWS needed

go run ./cmd/resolog ls sfn-execution arn:aws:states:...:stateMachine:my-sm   # list, then pick a ref
go run ./cmd/resolog ls batch-job my-queue
go run ./cmd/resolog ls log-group /aws/lambda/
```

The real backends need AWS credentials (standard SDK chain) and the default is
`live`. Flags: `--backend live|poll|mock` · `-f` follow · `--since 10m` window ·
`-t` timestamps · `--no-color`.

With `--backend mock` the `sfn-execution` form still does **real** resolution
(it calls SFn) and then streams synthetic lines per discovered source — a way to
see the flagship's discovery + interleaving without touching CloudWatch.

## Design notes worth keeping

- **Completion is driven by resource status, never by "logs went quiet."**
  CloudWatch lags and delivers the last lines *after* a resource ends. The
  `Resolution.Done` channel carries the terminal signal; `Tail` applies a grace
  window before stopping. This keeps Batch/SFn status logic out of the tailer.
- **`StartLiveTail` has a per-session log-group cap** → executions with many
  tasks need paging across sessions (same idea as batchkoi's >32-child paging).
- **Resource→logs resolution has three tiers:** deterministic mapping
  (Lambda/ECS/Batch — clean), execution history (SFn — the flagship), and
  agent-dependent (EC2 — weak; deliberately *not* a headline feature).
- **Keep the public surface minimal** (v0: the interfaces + a couple of entry
  points). Surface area explodes with resource kinds and discovery heuristics.

## Relationship to batchkoi

resolog is extracted from [batchkoi](https://github.com/tawAsh1/batchkoi)'s log
tailer. batchkoi is both the origin and the first consumer; the `cmd/resolog` CLI
is a second, independent consumer — having two consumers keeps the API honest.
resolog never imports batchkoi.

## Naming

**resolog** = *resolve* + *log* — the name states the thesis: the value is in
*resolving* which logs a resource produces, not in the tailing itself. It also
sits in the same coined-word family as its sibling
[batchkoi](https://github.com/tawAsh1/batchkoi). Existing CloudWatch tailers
(`aws logs tail`, `lucagrulla/cw`, `TylerBrock/saw`, `kennu/cwtail`) occupy the
"tail a group" space; resolog deliberately names the resolver layer instead.
