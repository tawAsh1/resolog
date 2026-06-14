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
resolog --backend poll --since 1h sfn-execution:<実行ARN>   # 実行を丸ごと

resolog ls sfn-execution <ステートマシンARN>        # 実行を一覧して選ぶ
resolog ls batch-job <キュー>
resolog ls log-group /aws/lambda/
```

参照は `<スキーム>:<残り>`、またはロググループ名そのままです。スキームは `log-group`、
`sfn-execution`、`batch-job`、`lambda` の4つ。フラグは `--backend live|poll`、
`-f`(追従)、`--since 10m`、`-t`(タイムスタンプ)、`--no-color`。

## しくみ

直交する3つの継ぎ目でできていて、利用側はどの層からでも使えます。

| 継ぎ目 | 役割 |
| --- | --- |
| Resolver | リソース参照 → ログ源(+ 終了シグナル) |
| Backend | ログ源 → イベント列 |
| Sink | イベント列 → 出力(既定はインタリーブ表示) |

レジストリやプラグイン機構はありません。resolver はただのパッケージで、利用側が明示的に
組み合わせます。スキームの振り分けは CLI 側にあり、ライブラリ本体には持ち込みません。
覚えておくとよい方針が2つあります。終了判定は必ず**リソースのステータスで握り、「新着が
止まった」では判断しない**こと(CloudWatch は遅れるし、最後の行は後から届きます)。
そして各 resolver/backend は必要な AWS クライアントの一部だけをローカルな interface として
宣言するので、使わないサービスの SDK が `go.mod` に降りてきません。

Resolver は `log-group`、`sfn-execution`(目玉)、`batch-job`(配列ジョブ対応)、`lambda`。
Backend は `live`(StartLiveTail)、`poll`(FilterLogEvents)。

## ライブラリとして

```go
res, _ := sfn.New(sfnClient, sfn.WithBatchResolver(batch.New(batchClient))).
	Resolve(ctx, executionARN)
sink := resolog.NewRenderer(os.Stdout, true, false)
resolog.Tail(ctx, res, livetail.New(logsClient), sink)
```

## 状態

v0、まだ初期段階です。resolver と backend はすべて実装済みで、fake の AWS API に対する
ユニットテストは通っていますが、本物の AWS をつないだ通し確認はまだしていません。公開する
API(3つの interface といくつかの入口)は意図的に小さく保っています。

resolog は [batchkoi](https://github.com/tawAsh1/batchkoi) のログ tailer を切り出したものです。
batchkoi が出どころで、最初の利用者になる予定です。resolog 側から batchkoi を import する
ことはありません。

## ライセンス

MIT
