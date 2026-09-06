# daily-keiba-sync

Cloudflare Workersだけで、JV-Link/NV-Link互換source Workerの公式データを既存R2 Data CatalogとNeonへ日次同期するorchestratorです。Cloudflare Containers、Docker、Python、PyIceberg、ローカルPostgreSQLは本番経路で使用しません。

## データフロー

1. Scheduled WorkerがD1に冪等なrunを作成し、JV/NV source Workerをservice bindingで直接呼び出す。
2. D1に保存したprovider別の前回取得日時を`fromTime`として差分取得する。取得開始時刻を今回のcutoffとして固定し、raw保存とNDJSONの`open`/`close`・件数検証が成功した後だけD1 cursorを単調増加で更新する。初回のみlookback期間を使用する。
3. Cron invocation内で、指定日時以降にsource Workerが返した差分Base64 NDJSON streamを日付/run別のR2 raw stagingへmultipart uploadする。
4. JVは`daily-keiba-sync-jv-raw-stage-jobs`、NVは`daily-keiba-sync-nv-raw-stage-jobs`へ別々に送る。各Queueは自providerのimmutable rawだけを固定長CP932 recordとして解析し、今回の差分responseに含まれる全recordのtable stageをR2へ保存して主キー単位で重複排除する。
5. D1の明示allowlistにある実在tableだけを`R2CatalogJob`へ送る。未構築tableのrecordはR2に残して`not_configured`とし、Catalog retryを行わない。
6. 初回だけ対象年partitionの既存Parquetをfile jobへ分割して主キー→file/position indexをD1に構築する。日次commitはこのindexからposition deleteを求め、新規row appendと同一Iceberg transactionでcommitする。
7. commit後は新規Parquetだけを読んでD1 indexを更新する。table leaseにより同じCatalog tableへの並列commitを禁止する。
8. 全対象R2 tableのcommit成功を確認した後だけ`NeonJob`をNeon Queueへ送り、同じR2 table stageを`INSERT ... ON CONFLICT DO UPDATE`する。
9. cursorを進める通常JV runでは、Neon同期済み`jvd_ra`から開催日を抽出し、`sync-realtime-data`の`discover-urls`を日ごとに自動登録する。これによりレースURL、netkeiba premiumデータ（調教評価等）、オッズ・馬体重・結果の既存plannerが起動する。通知に失敗したrunは成功確定せず、Queue retryで再通知する。
10. monitor cronが欠損runまたは20分以上更新のないrunを検出し、JV raw stage、NV raw stage、R2 Catalog、Neonの該当する未完了stageへ振り分けて再開する。

R2 Catalogが常に先です。R2成功後にNeonだけが失敗した場合、provider再取得やR2再commitをせず、同じR2 staging objectからNeon stageだけを再試行します。Queue messageにはrecord本体を入れず、run ID、table、R2 object key等のmetadataだけを入れます。

更新データ0件は正常です。source streamが正しい`open`/`close`を持ち`files: 0, records: 0`なら、runを`succeeded_empty`として完了し、日次cursorを更新します。HTTP error、空のHTTP body、不正Content-Type、欠落した`close`、件数不一致は取得エラーとしてcursorを更新しません。

## Schedule

Cloudflare CronはUTCです。

| Cron (UTC)   |   JST | 処理         |
| ------------ | ----: | ------------ |
| `0 16 * * *` | 01:00 | NV取得・同期 |
| `0 17 * * *` | 02:00 | NV監視・復旧 |
| `0 11 * * *` | 20:00 | JV取得・同期 |
| `0 12 * * *` | 21:00 | JV監視・復旧 |

通常runはD1 `provider_acquisition_cursors.last_acquired_at`以降の更新差分を取得します。同じ日時を次回にも含めるため境界は主キーupsertで冪等化されます。cursorがない初回だけ2日lookback、monitorによる初回補完だけ7日backfillを使います。開催日固定queryではありません。

## Cloudflare resources

- Worker: `daily-keiba-sync`
- D1: `daily-keiba-sync`（cursor/run/lease/operationのsource of truth）
- KV: `SYNC_CACHE`（D1 cursor readのL2 cache、TTL 300秒）
- JV raw parse/stage Queue/DLQ: `daily-keiba-sync-jv-raw-stage-jobs` / `daily-keiba-sync-jv-raw-stage-dlq`
- NV raw parse/stage Queue/DLQ: `daily-keiba-sync-nv-raw-stage-jobs` / `daily-keiba-sync-nv-raw-stage-dlq`
- R2 Catalog Queue/DLQ: `daily-keiba-sync-r2-catalog-jobs` / `daily-keiba-sync-r2-catalog-dlq`
- Neon Queue/DLQ: `daily-keiba-sync-neon-jobs` / `daily-keiba-sync-neon-dlq`
- source staging R2: `pc-keiba-source-staging`
- serving R2/Data Catalog: `pc-keiba-r2-catalog` / namespace `pc_keiba`
- service bindings: `jra-van-datalab-worker-only-probe`, `umacon-worker`, `sync-realtime-data`

D1はprovider別の前回取得日時、run/table status、recovery stageのsource of truthです。cursor readはCache API（L1、TTL 30秒）→KV（L2、TTL 300秒）→D1の順で行います。cursor更新後は該当providerのKV keyと実行coloのCache API entryをpurgeします。KVとCache APIはeventual/local cacheなので、lease、operation journal、Catalog positionなどの正しさを委ねません。stale cursorが並列実行で読まれても取得範囲が過去側へ広がるだけで、主キーupsertにより欠損は発生しません。Queueは明示retry、delay、DLQを持ち、staging keyはrunごとにimmutableです。

Cronとraw stage Queueは同じ処理の二重実行ではありません。Cronは対象providerの差分source streamを一度だけ取得し、耐久性のあるraw R2原本を確定する境界です。JV/NV raw stage Queueは物理的にも分離され、JV QueueはJV rawだけ、NV QueueはNV rawだけをproviderへ再アクセスせず解析・table stagingします。1つのjobやconsumer invocationでJV/NVを同時parseしません。取得の再試行とparseの再試行を分離する必要がなければCronへ統合できますが、現行構成ではprovider再取得を避けるため分離しています。

## Required secrets

```text
ADMIN_TOKEN
JRA_VAN_WORKER_API_TOKEN
UMMACON_WORKER_API_TOKEN
NEON_DATABASE_URL
R2_CATALOG_TOKEN
REALTIME_ADMIN_TOKEN
```

`R2_CATALOG_TOKEN`はCloudflare R2 Data Catalog REST APIへcommit可能なwrite tokenが必要です。値、provider URL/query、credential、response recordはログやAPI responseへ出しません。診断状態にはsafe stage名だけを保存します。

ローカル設定例はGit管理しない`.dev.vars`へ置きます。production secretは`wrangler secret bulk`またはproduction environment付きdeploy workflowで設定します。

## Commands

```bash
bun install
bun run --cwd apps/daily-keiba-sync verify
bun run --cwd apps/daily-keiba-sync d1:migrate:local
bun run --cwd apps/daily-keiba-sync dev
bun run --cwd apps/daily-keiba-sync deploy
```

全変更はformat、Oxlint 0 warnings、tsc 0 errors、Vitestのstatements/branches/functions/lines各95%以上を通す必要があります。

## Admin API

public endpointはBearer `ADMIN_TOKEN`で保護します。

```text
GET  /health
POST /admin/run
POST /admin/index
POST /admin/trigger
POST /admin/cache/purge
GET  /admin/status?provider=jv&runDate=20260903
```

手動run body:

```json
{ "provider": "nv", "fromTime": "20260903123456" }
```

`fromTime`はprovider更新日時のJST `YYYYMMDDhhmmss`です。NVは指定日時以降、JVは`fromTime`から`toTime`までを取得します。JVで`toTime`を省略した場合はrun日の終了時刻を使用します。手動runは日次cursorを変更しません。`/admin/run`はforced runとして独立IDを作り、取得とraw R2保存をrequest内で実行します。初回indexを単独検証する場合は`{"provider":"nv","tableName":"nvd_ra","partitionValue":"2026"}`を`/admin/index`へ送ります。

Cronと同じdaily/monitor pathをProduction検証から起動する場合は、`{"action":"run","provider":"nv"}`または`{"action":"monitor","provider":"nv"}`を`/admin/trigger`へ送ります。manual fallbackは同じdaily pathへ`force: true`を付け、その日の既存runに吸収されず新しいrunを作ります。1 requestでは1 providerだけを処理し、`run`は日次cursorを使って検証済みraw保存後にcursorを進めます。

`/admin/trigger`の`runDate`は取得実行日（JST）であり、開催日ではありません。未来日は400で拒否します。未来の開催日を指定してdaily keyを先に作ると、当日のCronが既存runに吸収されて取得をスキップするためです。明示した取得期間の検証には独立IDの`/admin/run`を使用してください。既に誤って作成された成功runからの復旧は、当日の`/admin/trigger`に`force: true`を指定して新しい差分取得を起動します。

cursor cacheを明示purgeする場合は`{"provider":"nv"}`を`/admin/cache/purge`へ送ります。空object `{}`ならJV/NVの両方をpurgeします。Cache APIのdeleteは実行coloだけに作用するため、KV deleteと短いTTLも併用し、cacheの値を同期authorityにはしません。

## Iceberg safety

- 初期allowlistは実在する`jvd_ra`、`jvd_se`、`nvd_ra`、`nvd_se`だけです。追加はD1 `catalog_targets`へ明示登録します。
- table schemaの列数、順序、型が固定長layoutと完全一致しなければcommitしません。
- Iceberg format v2以外には書きません。
- 既存Parquet探索は初回の年partition index構築時だけです。日次処理はD1 indexから既存positionを取得します。
- commit直前にCatalogのcurrent snapshotと各D1 partition indexのsnapshotを照合し、不一致なら`catalog-index-stale`でcommit前に停止します。
- position deleteとappendを同じIceberg transactionでcommitし、新規Parquetだけからindexを更新します。成功後は対象partitionのindex snapshotも同じsnapshotへ進めます。
- productionの4初期targetは現行Catalog snapshotからposition indexを再構築したうえで`enabled = 1`です。外部writerは停止し、Workerのsnapshot guardとtable leaseでsingle-writerを維持します。
- operation stateをcommit前後で保存し、不明なsnapshot進行時は再appendせず`catalog-commit-uncertain`で停止します。
- HTTP 404、schema不一致、queue誤配送等の恒久エラーは即ackし、retry stormを起こしません。
- source stageは主キーごとに一意化してから同期します。

## Local fallback

`apps/local-postgresql`の`replica:push`と`pc-keiba:update-and-sync`内の同期stepは、認証済み`/admin/trigger`を通してこのWorkerのdaily flowをNV/JV別に起動します。旧`replica:push:r2-catalog`と`replica:push:neon`は`replica:push:legacy`配下のbreak-glass復旧経路だけに残し、明示guardなしでは実行できません。macOS launchdの03:15自動writerは停止・退避済みです。データ取得以外の学習・特徴量処理を含む既存ローカルcommandは削除しません。
