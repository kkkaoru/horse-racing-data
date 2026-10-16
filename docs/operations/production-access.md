# ローカルから本番を確認する

入口はリポジトリルートの **`bun run prod`** です。ページ確認は Cloudflare Access の service token、ログは Workers Observability API です。Playwright と対話ログインは使いません。

`bun run prod` はリポジトリの `.env` を読み、続けて `apps/pc-keiba-viewer/.env.local` があれば上書きします。テストは `PROD_SKIP_ENV_FILE=1` でこれを止めます。

## 初回だけ

1. Bun と Wrangler（リポジトリの依存関係）を用意します。service token が無いときだけ `cloudflared` が必要です。
2. ルート `.env` に次を置きます（値は `.env.example`）。既存ファイル全体は上書きしないでください。

   ```dotenv
   CLOUDFLARE_ACCOUNT_ID="78109ec18c7c85b194b19fb32e3bb149"
   PC_KEIBA_VIEWER_ORIGIN="https://pc-keiba-viewer.kkk4oru.com"
   ```

   Access Client ID/Secret は `apps/pc-keiba-viewer/.env.local` の `PC_KEIBA_ACCESS_CLIENT_ID` / `PC_KEIBA_ACCESS_CLIENT_SECRET` です。ホスト名・トークンは Git へ追加しません。

3. Cloudflare API の認証を確認します。ログ照会には `CLOUDFLARE_API_TOKEN`（なければ `CLOUDFLARE_DEBUG_TOKEN`）が必要です。

   ```sh
   bun run prod status
   # 未認証の場合のみ
   bunx wrangler login
   ```

**Wrangler／Cloudflare MCP の管理 API 認証と、Web サイトの Cloudflare Access 認証は別です。** API を読めても保護ページには入れません。Access ポリシーを Bypass に変える構成ではありません。非ブラウザ GET は Service Auth の service token だけを使います。

**Python `urllib` の直アクセスで HTTP 403 / Cloudflare 1010 が出る場合**: 1010はブラウザ署名による拒否で、D1権限不足の7403やAccess認証失敗とは別です。2026-09-26の実測では同じ機械から`urllib`は1010、既存の`curl`とservice tokenを使う`bun run prod get`は成功しました。PythonのUser-Agent偽装・WAF例外・Access Bypassを作るのではなく、Viewerは上記の`bun run prod get`、内部D1総数は既存管理トークン認証の`sync-realtime-data` Worker binding API、アカウント管理の読み取りは正式なExecutor Cloudflare MCP接続を使ってください。対象ホストごとに認証が異なります。

**D1直接APIが7403の場合**: `bun run prod`の限定SELECTはWrangler OAuth、監視用の`/api/internal/expected-race-count`は既存Worker secret + D1 bindingを使用し、API tokenの権限を付け足しません。Executor `cloudflare-api` の既存OAuth接続を使ったread-only D1 `SELECT COUNT`がHTTP200・57件・`rows_written=0`で成功した例があり、使う前に`executor tools search 'search' --namespace cloudflare-api --limit 3`で接続とquery schemaを発見してください。ただし、このOAuth成功は別の`CLOUDFLARE_DEBUG_TOKEN`のD1 Read権限やCloudflare側7403の根因を修復しません。直接API tokenによるD1 Readが必要なら、管理者が最小権限の専用tokenを付与・調査する必要があります。

## localhost の HTTPS Viewer

`bun run start:local` / `bun run --cwd apps/pc-keiba-viewer dev:https` は、既定で本番の
R2 Catalogを参照します。Wranglerのremote bindings認証が必要です。起動時に開発用
Cloudflare contextの初期化完了を待ち、`R2_RACE_DETAIL` は本番と同じ
`RaceDetailReadService` に接続します。本番へのデプロイは不要です。

ローカルPostgreSQLを表示対象にする場合は、既存Viewerを停止してから
`PC_KEIBA_DATABASE_TARGET=local bun run start:local` で起動してください。
ローカルPG自体の更新は別処理です。Wine出力の日付指定インポートは
[local-postgresql README](../../apps/local-postgresql/README.md#wine-jv-link-export--local-postgresql)を参照してください。

## 日常の確認

```sh
# 本番 HTML または API を GET（service token。未設定時のみ cloudflared）
bun run prod get /races/2026/09/12/83/01 > /tmp/production-race.html

# 通常のブラウザーで開く URL を表示
bun run prod url /races/2026/09/12/83/01

# 直近15分の Workers Observability イベント (最大20件)
bun run prod logs pc-keiba-viewer

# UTC期間を指定して過去のイベントをread-only検索 (最大7日、既定100件)
bun run prod logs finish-position-cron 2026-09-22T18:55:00Z 2026-09-22T19:30:00Z 100

# Queue ID とconsumer設定をread-onlyで確認（result_info.total_pagesで続きのpageを確認）
bun run prod queues

# Queue backlog の読み取り
bun run prod queue-metrics <QUEUE_ID>

# Container runtime metrics + billable usage の読み取り
bun run prod container-usage 2026-09-22T19:00:00Z 2026-09-22T19:22:00Z

# D1 query-level duration/rows をread-onlyで取得 (日付はinclusive、31日以内)
bun run prod d1-insights 9475ef83-d1da-4b47-9251-0cb431176aa8 2026-09-16 2026-09-22

# D1 の限定された read-only SELECT（Wrangler OAuth を使用）
bun run prod day-base-owner nar 20260925
bun run prod day-base-manifest nar 20260925
bun run prod race-source-count nar 20260925

# finish-position-cron の配信versionとbinding metadataをread-onlyで確認
bun run prod worker-deployments
bun run prod worker-version <VERSION_UUID>

# 予測結果の完了状況（GETのみ、Container起動なし）
bun run prod prediction-readiness 20260925

# active Workflow instances の読み取り
bun run prod workflows finish-position-race-day running

# 個別Workflow instanceのstep/error詳細をread-onlyで取得
bun run prod workflow-instance finish-position-race-day <INSTANCE_ID>

# 既存 viewer 設定の DETAIL_SECTION_CACHE_KV を本番から読む
bun run prod kv pred:fp:v1:20260912:83:01

# R2 の bucket/key を指定してローカルへダウンロード
bun run prod r2 your-feature-bucket/path/to/features.parquet /tmp/features.parquet
```

- `get` は対象 origin の GET だけです。任意の curl オプション、別ホストの URL、リダイレクト追従は受け付けません。
- `logs` はWorker名を指定してWorkers Observabilityのquery endpointを呼びますが、操作はread-onlyです。Worker名の既定は `pc-keiba-viewer`。UTC start/endを指定すると最大7日まで検索し、event limitは1–2,000（historical検索の既定100）。指定なしは直近15分・20件です。Cloudflare公式仕様上の保持期間はFree 3日/Paid最大7日、events view limit上限は2,000です。ログ保持期間外やsampled/limited eventsは取得できないため、結果がないことは処理未実行の証明ではありません。
- `logs` API reference: <https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/>; Workers Logs retention: <https://developers.cloudflare.com/workers/observability/logs/workers-logs/>.
- `queues [page]` はQueue一覧APIの1 pageをGETします。`result_info.total_pages`が1より大きければ、page番号を指定して続きも読めます。
- `queue-metrics` は指定Queue IDのCloudflare metrics endpointへGETのみを送り、`CLOUDFLARE_API_TOKEN` (または `CLOUDFLARE_DEBUG_TOKEN`) に Queues Read 権限が必要です。件数・oldest timestampはbest-effortの概数で、個別メッセージ内容は読みません。
- `container-usage <from-utc> <to-utc>` は最大7日・過去の範囲をCloudflare GraphQLのContainers metrics / usage datasetに照会する read-only queryです。`Analytics Read` 権限を要求し、hour+application単位でruntime (`containerUptime`, `cpuTimeSec`, workload memory/disk) とbillable-side `allocatedMemory`/`allocatedDisk`を返します。Usage includes platform micro-VM overhead; metrics exclude it. Both may be delayed/sampled and are not interchangeable。
- `d1-insights <database-uuid> <from-date> <to-date>` は最大31日 (両端日を含む) のCloudflare GraphQL `d1QueriesAdaptiveGroups` queryです。DBごと/normalized SQLごとの実行数、平均/合計 `queryDurationMs`、rows read/writtenを読み取ります。SQLを実行・変更せず、`Analytics Read` 権限のみ要求。Cloudflareはbound parameter値をquery textに含めない一方、SQL文字列は返すため、出力を外部共有しないこと。上位1,000 SQL groupまでのため、対象queryが出力にない場合は該当DB/期間で再照会し、結果を絶対ゼロと誤認しない。Cloudflare公式D1 analytics retentionは31日。`queryDurationMs` は過去集計であり、Workers Bindingの1回ごとの `meta.timings.sql_duration_ms` (SQL実行時間、network時間を除く) とは別。
- `workflows <name> [status]` はWorkflow instance list APIをGETします。statusは省略可能で、指定時はCloudflareのdocumented status値のみ受け付けます。
- `workflow-instance <name> <id>` は1 instanceのstatus/step/error詳細をGETします。両方のWorkflow commandに Workers Scripts Read 権限が必要です。読み取り専用で、pause/terminate/restartしません。
- `worker-deployments` と `worker-version <uuid>` は `finish-position-cron` 固定のWorkers Scripts Read GETです。現在100%のversion、rollback先、binding metadataを確認できます。secret値は表示しません。
- `prediction-readiness <YYYYMMDD>` は内部WorkerのGETを一回だけ実行し、`FINISH_POSITION_CRON_TRIGGER_TOKEN`で認証します。`preWeight.complete`は出力完了確認に使えますが、予測精度/parity検証の代わりにはなりません。Containerを起動せず、予測の再投入もしません。
- `day-base-owner` / `day-base-manifest` / `race-source-count` はcategory・日付を検証した固定のWrangler remote SELECTのみです。Wrangler OAuth のD1権限と `CLOUDFLARE_API_TOKEN` のD1 API権限は別物です。後者が7403でも前者の権限がある場合はSELECTでき、トークンを昇格したわけではありません。直接HTTP APIの7403を解消するにはアカウント管理者が当該トークンへD1 Read（書込が必要な場合はD1 Write）を付与する必要があります。<https://developers.cloudflare.com/fundamentals/api/reference/permissions/>
- KV は `apps/pc-keiba-viewer/wrangler.jsonc` の既存 binding を使います。KV/R2 には常に `--remote` を付けます。
- R2 は既存ファイル・シンボリックリンクを上書きしません。
- `bun run prod` は書き込み、デプロイ、予測再生成のコマンドを提供しません。承認済みのWorker-only releaseでは、前回releaseからの隔離candidateを検証後、`PREDICTION_WORKER_ONLY=1 REQUEUE_FINISH_POSITION_PREDICTIONS=0 bun run --filter finish-position-cron deploy` をそのcandidateで使います。既存のdeploy scriptがQueue pause/drain/resumeを担い、`--containers-rollout=none` はContainer image/instance更新を除外します。事前にactive Workflow・両Queue・rollback versionを記録し、Containerのbuild contextがdirtyな別変更を含まないことを確認してください。自動replayはオフです。

API references:

- <https://developers.cloudflare.com/api/resources/queues/methods/get_metrics/>
- <https://developers.cloudflare.com/analytics/graphql-api/tutorials/querying-container-metrics/>
- <https://developers.cloudflare.com/d1/observability/metrics-analytics/>
- <https://developers.cloudflare.com/d1/worker-api/return-object/>
- <https://developers.cloudflare.com/api/resources/workflows/subresources/instances/methods/list/>

## エージェントから Cloudflare MCP を使う場合

```sh
executor tools search 'search' --namespace cloudflare-api --limit 3
executor tools search 'query_worker_observability' --namespace cloudflare-observability --limit 3
```

実際に登録されている接続パスを確認し、Cloudflare スキルと API スキーマを読んでから実行してください。

## 検証

```sh
shellcheck scripts/prod.sh
cd scripts
uv run pytest
uv run ruff check tests/test_prod_script.py
uv run ruff format --check tests/test_prod_script.py
uv run basedpyright
uv run ty check
```

テストは偽の Bun / curl / cloudflared と一時ディレクトリを使います。本番や実際の認証キャッシュにはアクセスしません。
