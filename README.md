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
resolog --backend poll --since 1h --sort -t sfn-execution:<execution-arn>   # a finished run, in time order

resolog ls sfn-execution <state-machine-arn>        # list executions, pick one
resolog ls batch-job <queue>
resolog ls log-group /aws/lambda/
```

References are `<scheme>:<rest>`, or a bare log group name. Schemes:
`log-group`, `sfn-execution`, `batch-job`, `lambda`. Flags: `--backend
live|poll`, `-f` follow, `--since 10m`, `--until 5m` (window upper bound, poll),
`--sort` (poll only; see Ordering), `-t` timestamps, `--no-color`.

## Ordering

By default lines print in **arrival order** — interleaved across sources, like
`docker compose logs`. Across different streams this is *not* time order.

`--sort` (poll, finished resources only) buffers everything and prints in time
order by **each resource's own reported clock**. Honest caveats:

- Clocks differ across resources; resolog never claims causal order between them.
- CloudWatch ingestion lags, so a finished task's last lines (e.g. a failure
  stack trace) can land late and fall outside the window.
- A whole-log-group source (e.g. a Lambda tailed by group) can include lines
  from *other* invocations within the window.
- `--sort` waits for the fetch to finish before printing; on Ctrl-C it flushes
  the ordered prefix it has so far.

The live frontier is intentionally never reordered.

## Library

resolog is also a Go library; the CLI is just one consumer. See the
[package docs](https://pkg.go.dev/github.com/tawAsh1/resolog).

```go
res, _ := sfn.New(sfnClient, sfn.WithBatchResolver(batch.New(batchClient))).
	Resolve(ctx, executionARN)
sink := resolog.NewRenderer(os.Stdout, true, false)
resolog.Tail(ctx, res, livetail.New(logsClient), sink)
```

## Status

v0, early. Every resolver and backend is implemented and unit-tested, but the
real-AWS paths have not had a production shakedown yet. Extracted from
[batchkoi](https://github.com/tawAsh1/batchkoi)'s log tailer.

## License

MIT
