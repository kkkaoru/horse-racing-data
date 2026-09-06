# JRA-VAN Data Lab. Cloudflare Containers demo

Cloudflare Worker だけを公開入口とし、Cloudflare Containers の `linux/amd64` VM 上で Wine と公式
JV-Link COM SDK を動かすデモです。macOS、Windows VM、常設サーバー、ホストブリッジは不要です。

- Worker: API 認証、単一 Container/Durable Object へのルーティング、R2 状態保存
- Container: Wine Staging 11.16、64-bit JV-Link、ネイティブ COM クライアント、公式設定 UI (noVNC)
- R2: 認証済み Wine prefix を `terminal/prefix.tar.gz` として保持
- Durable Object: 常に同じ名前の論理端末へ直列ルーティング

> [!IMPORTANT]
> JV-Link の公式サポート対象は Windows です。Wine と Cloudflare Containers 上の動作は非公式です。
> 通信・認証プロトコルは再実装せず、公開 COM API のみを呼び出します。

## なぜ通常の Worker だけでは動かないか

Workers isolate は Windows COM や PE バイナリを実行できません。本デモの「Worker 単体」は、外部の Mac/PC を
使わず、同じ Cloudflare Worker デプロイに含まれる Containers を Linux 実行環境として使う構成です。
Containers のローカルディスクは停止時に消えるため、認証情報を含む専用 Wine prefix を R2 に checkpoint し、
同じ Durable Object 名の論理端末だけが restore します。

R2 オブジェクトを別 Worker・別端末へ複製して同時利用しないでください。R2 オブジェクトを削除すると端末認証を
失い、利用キー再発行が必要になる可能性があります。バックアップのダウンロードやログ出力を行う API は
意図的に提供していません。

## 構成

```text
client
  -> Worker (Bearer または Browser Basic 認証)
      -> singleton Container Durable Object
          -> nginx :8080
              -> Python API :8081 -> Wine -> JV-Link COM
              -> noVNC :6080 -> 公式 JV-Link 設定 UI
          -> Worker /internal/state -> private R2 bucket
```

利用キーは `JRA_VAN_DATALAB_KEY`、公開 API は `JRA_VAN_API_TOKEN`、Container から R2 へ checkpoint
する内部経路は別の `JRA_VAN_STATE_TOKEN` を Wrangler secret として保存します。いずれも image、設定ファイル、
URL、レスポンス、access log には含めません。

## 前提

- Cloudflare Containers を利用できる有料 Workers plan
- Docker 互換 engine（ローカル deploy の image build 用。Workers Builds を使う場合は不要）
- Bun
- JRA-VAN Data Lab. SDK 5.0.0 64-bit の `JVLinkSetup.exe`
- この Cloudflare 端末専用として発行・再発行した利用キー

既存の Mac/Windows で認証済みの利用キーを新規prefixへ再登録すると、既存端末を失効させる可能性があります。
通常はCloudflare専用キーを使用してください。既存のmacOS Wine論理端末そのものを移行する場合に限り、後述の
認証済みprefix移行手順を使用します。同じ認証状態をMacとCloudflareで同時実行しないでください。

## セットアップ

リポジトリルートから実行します。

```bash
bun run --filter jra-van-datalab-cloudflare-demo prepare-sdk
bunx wrangler r2 bucket create jra-van-datalab-cloudflare-state
```

3 個の secret を対話入力します。API token と state token は、それぞれ独立した 32 bytes 以上のランダム値を
使用してください。シェル履歴へ値を直接書かないでください。

```bash
cd apps/jra-van-datalab-cloudflare-demo
bunx wrangler secret put JRA_VAN_API_TOKEN
bunx wrangler secret put JRA_VAN_STATE_TOKEN
bunx wrangler secret put JRA_VAN_DATALAB_KEY
bun run deploy
```

## 認証済みmacOS Wine端末の移行

`apps/jra-van-datalab-wine-demo/.native-cache/prefix`で認証済みであり、repository rootの`.env`に
同じ17文字（ハイフン区切りも可）の`JRA_VAN_DATALAB_KEY`がある場合、macOS Wine runtimeのsymlinkを複製せず、
`server_info` / `uid_pass` registryと17-byteの`jvsdk64.dat`端末識別stateだけをLinux pristine prefixへ
importできます。`jvsdk64.dat`は内容だけでなく元の更新時刻も認証検証に必要なため、exportとR2 checkpointの
両方でtimestampを保持します。これにより利用キーの再登録や規約同意の自動化を行いません。

```bash
bun run --filter jra-van-datalab-cloudflare-demo import-native-auth
bun run --filter jra-van-datalab-cloudflare-demo deploy:authenticated
```

最初のcommandは秘密を表示せず、Linux用prefix archiveをignored・mode 600の`.migration/`へ生成します。2番目は
新しいAPI/state tokenを生成してmode 600の`.dev.vars`へ保存し、`.env`の利用キーをWrangler secretへpipeし、
archiveをprivate R2へ配置してdeployします。移行後はMac側のJV-Linkを停止したままにしてください。

## 初回の利用規約同意

利用規約への同意は自動化しません。Cloudflare 上の公式 UI を一度だけ起動します。

```bash
curl --fail --request POST \
  --header "Authorization: Bearer $JRA_VAN_API_TOKEN" \
  https://jra-van-datalab-cloudflare-demo.kaoru.workers.dev/bootstrap/start
```

ブラウザーで次を開きます。

```text
https://jra-van-datalab-cloudflare-demo.kaoru.workers.dev/bootstrap/vnc.html?path=bootstrap/websockify&resize=remote
```

Basic 認証ダイアログではユーザー名 `jvlink`、パスワードに `JRA_VAN_API_TOKEN` を入力します。日本語の規約を
確認して本人操作で同意し、公式 UI を閉じます。その後 checkpoint します。

```bash
curl --fail --request POST \
  --header "Authorization: Bearer $JRA_VAN_API_TOKEN" \
  https://jra-van-datalab-cloudflare-demo.kaoru.workers.dev/v1/checkpoint
```

## JV-Data の取得

```bash
curl --fail --show-error \
  --header "Authorization: Bearer $JRA_VAN_API_TOKEN" \
  --header 'Content-Type: application/json' \
  --data '{
    "dataSpec":"RACE",
    "fromTime":"20260829000000-20260830235959",
    "limit":10,
    "timeoutSeconds":600
  }' \
  --output records.txt \
  https://jra-van-datalab-cloudflare-demo.kaoru.workers.dev/v1/records
```

返却値は JV-Link の CP932 レコードを UTF-8 に変換したテキストです。`limit` は 1–100、timeout は 1–900 秒です。
同一端末の要求は直列実行され、完了時（エラー時を含む）に prefix を R2 へ checkpoint します。JVData download
cache は機密 prefix の肥大化を避けるため checkpoint 対象外ですが、利用キー、`ukey`、端末識別 registry、
`jvsdk64.dat`の内容とtimestampは保持します。
Container が停止・別ホストで再起動しても次回要求時に同じ archive を restore します。

## 動作確認

秘密情報や Wine を使わない単体検証:

```bash
bun run --filter jra-van-datalab-cloudflare-demo types
bun run --filter jra-van-datalab-cloudflare-demo format:check
bun run --filter jra-van-datalab-cloudflare-demo lint
bun run --filter jra-van-datalab-cloudflare-demo tsc
bun run --filter jra-van-datalab-cloudflare-demo test:coverage
cd apps/jra-van-datalab-cloudflare-demo
uv run ruff format --check container tests
uv run ruff check container tests
uv run pytest
shellcheck container/*.sh scripts/*.sh
```

Cloudflare 側の health check:

```bash
curl --fail https://jra-van-datalab-cloudflare-demo.kaoru.workers.dev/health
```

## 運用上の注意

- `wrangler deploy` の image rollout や platform host restart で Container disk は消えますが、R2 prefix は残ります。
- `/internal/state` は state token 専用です。API token ではアクセスできません。
- noVNC は初回設定時だけ利用し、通常は開いたままにしないでください。
- R2 bucket の削除、`terminal/prefix.tar.gz` の削除、別環境へのコピーは行わないでください。
- 利用キーを変更するときは JRA-VAN 側の端末手続きを先に完了してください。
- Worker URL を変更する場合は `wrangler.jsonc` の `JRA_VAN_STATE_URL` も変更してください。
