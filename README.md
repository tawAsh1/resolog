# resolog

A resource-aware CloudWatch log tailer. **resolog** = *resolve* + *log*.

[![CI](https://github.com/tawAsh1/resolog/actions/workflows/ci.yml/badge.svg)](https://github.com/tawAsh1/resolog/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/tawAsh1/resolog.svg)](https://pkg.go.dev/github.com/tawAsh1/resolog)
[日本語 README](README.ja.md)

Most tools answer *"how do I tail a log group?"* (`aws logs tail`,
[lucagrulla/cw](https://github.com/lucagrulla/cw), `StartLiveTail`). resolog
answers the question that comes first: **given a resource, what should I even
tail?** — then interleaves every stream it finds, docker-compose style.

Flagship: hand it a Step Functions execution ARN and it tails the state machine
plus every Lambda and Batch task that execution ran, all together.

## Install

```sh
go install github.com/tawAsh1/resolog/cmd/resolog@latest
```

## Usage

The real backends use the standard AWS credential chain. The default backend is
`live` (StartLiveTail).

```sh
resolog log-group:/aws/lambda/my-fn                 # real-time tail
resolog --backend poll -f log-group:/my/group       # historical, then follow
resolog --backend poll --since 1h sfn-execution:<execution-arn>   # a whole execution
resolog --backend mock log-group:demo               # synthetic events, no AWS

resolog ls sfn-execution <state-machine-arn>        # list executions, pick one
resolog ls batch-job <queue>
resolog ls log-group /aws/lambda/
```

References are `<scheme>:<rest>`, or a bare log group name. Schemes:
`log-group`, `sfn-execution`, `batch-job`, `lambda`. Flags: `--backend
live|poll|mock`, `-f` follow, `--since 10m`, `-t` timestamps, `--no-color`.

## How it works

Three orthogonal seams; a consumer can plug in at any of them:

| Seam | Role |
| --- | --- |
| **Resolver** | resource ref → log sources (+ a terminal signal) |
| **Backend** | a source → a stream of events |
| **Sink** | a stream of events → output (default: an interleaved renderer) |

There is no registry and no plugin system: resolvers are plain packages you wire
explicitly. Scheme dispatch lives in the CLI, not the library. Two rules worth
knowing: completion is driven by **resource status, never by "logs went quiet"**
(CloudWatch lags, and the last lines arrive late), and each resolver/backend
declares the slice of the AWS client it needs as a local interface, so unused
service SDKs stay out of your `go.mod`.

Resolvers: `log-group`, `sfn-execution` (flagship), `batch-job` (array-aware),
`lambda`. Backends: `live` (StartLiveTail), `poll` (FilterLogEvents), `mock`.

## Library

```go
res, _ := sfn.New(sfnClient, sfn.WithBatchResolver(batch.New(batchClient))).
	Resolve(ctx, executionARN)
sink := resolog.NewRenderer(os.Stdout, true, false)
resolog.Tail(ctx, res, livetail.New(logsClient), sink)
```

## Status

v0, early. Every resolver and backend is implemented and unit-tested against
fake AWS APIs, but the real-AWS paths have not had a production shakedown yet.
The public surface (the three interfaces plus a couple of entry points) is kept
deliberately small.

resolog is extracted from [batchkoi](https://github.com/tawAsh1/batchkoi)'s log
tailer; batchkoi is its origin and intended first consumer. resolog never
imports batchkoi.

## License

MIT
