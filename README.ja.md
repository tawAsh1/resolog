# resolog

リソースを起点に CloudWatch のログを追いかけるツールです。名前は resolve + log から。

[![CI](https://github.com/tawAsh1/resolog/actions/workflows/ci.yml/badge.svg)](https://github.com/tawAsh1/resolog/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/tawAsh1/resolog.svg)](https://pkg.go.dev/github.com/tawAsh1/resolog)
[English README](README.md)

たいていのツールは「ロググループをどう tail するか」を解きます
(`aws logs tail`、[lucagrulla/cw](https://github.com/lucagrulla/cw)、StartLiveTail)。
resolog はその手前の問い、「このリソースは結局どのログを見ればいいのか」を解きます。
解決したストリームは docker-compose のように色分けして1つに混ぜて流します。

目玉は Step Functions の実行 ARN を渡すパターンです。ステートマシン本体に加えて、その実行が
走らせた Lambda や Batch のタスクのログまでまとめて tail します。

## インストール

```sh
go install github.com/tawAsh1/resolog/cmd/resolog@latest
```

## 使い方

実際にログを取るバックエンドは標準の AWS 認証情報チェーンを使います。デフォルトのバックエンドは
`live`(StartLiveTail)です。

```sh
resolog log-group:/aws/lambda/my-fn                 # リアルタイムに tail
resolog --backend poll -f log-group:/my/group       # 履歴を出してから追従
resolog --backend poll --since 1h --sort -t sfn-execution:<実行ARN>   # 完了した実行を時刻順で

resolog ls sfn-execution <ステートマシンARN>        # 実行を一覧して選ぶ
resolog ls batch-job <キュー>
resolog ls log-group /aws/lambda/
```

参照は `<スキーム>:<残り>`、またはロググループ名そのままです。スキームは `log-group`、
`sfn-execution`、`batch-job`、`lambda` の4つ。フラグは `--backend live|poll`、
`-f`(追従)、`--since 10m`、`--until 5m`(窓の上限、poll)、`--sort`(poll 専用、下記)、
`-t`(タイムスタンプ)、`--no-color`。

## 並び順

既定では **到着順**(ソース横断でインタリーブ、`docker compose logs` と同じ)。別ストリーム間は
時刻順になりません。

`--sort`(poll・完了済みリソース専用)は全部バッファして、**各リソース自身が報告した時計**で
時刻順に出します。正直な注意点:

- リソース間で時計はズレる。resolog はリソース間の因果順を主張しません。
- CloudWatch の ingest は遅れるので、完了したタスクの最後の行(失敗時のスタックトレース等)が
  遅れて届き、窓から外れることがあります。
- グループ全体のソース(group 指定で tail した Lambda 等)は、窓内の**別の起動**のログを含むことがあります。
- `--sort` は取得完了まで出力しません。Ctrl-C 時はそこまでの整列済み分を flush します。

live フロンティアは意図的に並べ替えません。

## ライブラリとして

resolog はライブラリでもあり、CLI はその一利用者にすぎません。
詳しくは [パッケージドキュメント](https://pkg.go.dev/github.com/tawAsh1/resolog) を。

```go
res, _ := sfn.New(sfnClient, sfn.WithBatchResolver(batch.New(batchClient))).
	Resolve(ctx, executionARN)
sink := resolog.NewRenderer(os.Stdout, true, false)
resolog.Tail(ctx, res, livetail.New(logsClient), sink)
```

## 状態

v0、まだ初期段階です。resolver と backend はすべて実装・テスト済みですが、本物の AWS を
つないだ通し確認はまだです。[batchkoi](https://github.com/tawAsh1/batchkoi) のログ tailer を
切り出したものです。

## ライセンス

MIT
