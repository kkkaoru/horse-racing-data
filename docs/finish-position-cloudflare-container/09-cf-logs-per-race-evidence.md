# 09. CF Worker ログ / D1 audit — per-race process 証跡調査

調査日: 2026-06-19 / 調査範囲: `finish-position-cron` Worker / **read・SELECT 専用**(コード変更・deploy・新規 observability 有効化なし)。creds は値を出さず mask。

## 結論(要点)

- **着順予測の Worker 監査ログ(D1 `finish_position_cron_executions`)は per-race ではなく per-run(1 cron invocation = 1 行、`run_date` は丸ごと 1 日単位)**。
- **「着順予測 process が per-race で動いていた」証跡は D1 audit には存在しない**。per-race の granularity を持つ列(race_id / race_code / 1 race 1 行)も、per-race の多数 run/日も無い。
- 実在する per-race 処理は **脚質(running-style)推論**(`apps/sync-realtime-data`)であり、**finish-position(着順)予測とは別 process**。混同に注意。

## 1. D1 監査テーブル(最有力の永続ログ)

### スキーマ(`migrations/0001_create_audit.sql` + `src/audit.ts`)

テーブル `finish_position_cron_executions`:

| 列                | 型                             | 意味                                                                                 |
| ----------------- | ------------------------------ | ------------------------------------------------------------------------------------ |
| `id`              | integer PK autoincrement       | 行 ID                                                                                |
| `run_date`        | text                           | **run 対象日(1 日単位)**。per-race ではない                                          |
| `status`          | text                           | `started` / `success` / `error`                                                      |
| `races_predicted` | integer                        | **その run 全体で予測した race 数の集計値**(per-race 単位の行ではなく、1 run の合計) |
| `duration_ms`     | integer                        | run 所要 ms                                                                          |
| `error`           | text(nullable)                 | エラー                                                                               |
| `recorded_at`     | text default `datetime('now')` | 記録時刻                                                                             |

コメントに `one row per cron-triggered prediction run.` と明記。**Insert-only**(feedback_no_data_delete、DELETE/TRUNCATE/DROP なし)。

### 書込セマンティクス(`src/worker.ts` / `src/queue-consumer.ts`)

- `worker.ts` の `runPrediction()` は container を `start()` した直後に **`status="started"`, `racesPredicted=0`(`ZERO_RACES`)** で **1 run につき 1 行**を INSERT(`recordAudit`)。キーは `runDate`(1 日)。
- queue 経路(`queue-consumer.ts`)は **per-category**(jra/nar/ban-ei)で DO coordinator(`PredictRunCoordinator`)に `claimRun` / `completeRun` を呼ぶ。`completeRun` の `racesPredicted` は **その category の合計**であって per-race ではない。
- いずれの経路にも **race 単位で 1 行を残す処理は存在しない**。

### remote SELECT 実測(token は既存 OAuth、d1 read のみ)

```
bunx wrangler d1 execute finish-position-cron-db --remote \
  --config apps/finish-position-cron/wrangler.jsonc \
  --command "SELECT ... FROM finish_position_cron_executions ORDER BY recorded_at ASC" --json
```

サマリ:

- **総行数 = 6**(全件)。
- `recorded_at` 範囲 = `2026-06-03 10:31:22` 〜 `2026-06-03 13:00:12`(**最古 run = 2026-06-03**)。
- `run_date` は全行 `2026-06-03`(min=max)。**1 日分しか存在しない**(初期 deploy 検証ウィンドウ)。

全 6 行(時系列):

| id  | run_date   | status  | races_predicted | duration_ms | recorded_at         |
| --- | ---------- | ------- | --------------- | ----------- | ------------------- |
| 1   | 2026-06-03 | started | 0               | 571         | 2026-06-03 10:31:22 |
| 2   | 2026-06-03 | started | 0               | 781         | 2026-06-03 11:16:41 |
| 3   | 2026-06-03 | started | 0               | 764         | 2026-06-03 11:49:43 |
| 4   | 2026-06-03 | started | 0               | 1163        | 2026-06-03 12:42:16 |
| 5   | 2026-06-03 | started | 0               | 749         | 2026-06-03 12:50:52 |
| 6   | 2026-06-03 | started | 0               | 810         | 2026-06-03 13:00:12 |

### 読み取れること(granularity / per-race 証跡)

- **granularity = per-run(1 日 = 1 `run_date`)**。`races_predicted` は run 集計値であり、per-race 1 行ではない。**per-race の証跡なし**。
- 全 6 行が **`status="started"`、`races_predicted=0`、`duration_ms < 1.2s`**。これは container を `start()` した記録のみで、**`success`(races_predicted>0)に到達した行が 1 件も無い**。Cloudflare Containers の ~90-110s reap(DuckDB feature build ~10min 完走不能)と整合し、**この Worker 経由で着順予測が完走した記録は存在しない**。
- per-race 的に多数 run/日が走っていた時期も**無い**(6 行・1 日のみ、しかも全部 `started`)。当時の実運用着順予測は **legacy Mac launchd の local Docker pipeline**(別系統、D1 audit 対象外)で行われており、この Worker D1 には記録されない。

## 2. wrangler tail(live)

```
timeout 35 bunx wrangler tail finish-position-cron --config apps/finish-position-cron/wrangler.jsonc --format json
```

- ~35s のウィンドウで **出力ゼロ**(request / cron 発火なし)。
- 予測 cron(`*/20 1-11 * * *`)は wrangler.jsonc で **コメントアウト=disabled**。有効なのは Neon pre-wake / keep-warm の warm cron のみで、これらは特定分にしか発火しない。手動 invoke・エラーの痕跡もウィンドウ内には観測されず。
- → live tail からも **per-race 着順予測の痕跡は得られない**(現状は予測 cron 自体が止まっているため期待どおり)。

## 3. Cloudflare 観測ログ / Workers Logs(過去ログ)

- `wrangler.jsonc`: `observability.enabled=true`, `head_sampling_rate=0.1`(請求対策で sampling 済)。**新規有効化は一切していない**。
- wrangler に **observability/logs を query する subcommand は無い**(`wrangler observability` は Unknown argument、ログ系は `wrangler tail`=live のみ)。
- `wrangler deployments list` は取得可(直近は 2026-06-18 の複数 upload / secret change)。ただし**過去の実行ログ本体**は dashboard の Workers Logs(Workers Paid で retention 数日程度)か GraphQL Analytics API 経由でしか引けず、read-only の本調査では **retention 外 / CLI から取得不能**。
- → **過去の per-race 実行ログを観測ログから裏取りすることは不可**(retention/手段の制約)。永続証跡は D1 audit のみで、上記 1. のとおり per-race ではない。

## 4. realtime 系 worker との混同切り分け(重要)

- `apps/sync-realtime-data*` を grep した結果、per-race の processing は **脚質(running-style)推論**:
  - `running-style-cron.ts`: `race_running_styles already has all runners, and queues per-race Worker` / L203 `the per-race worker handleRunningStylePredictionJob builds features` → **per-race は running-style であって finish-position ではない**。
- これらのファイル中の `finish_position` / `getFinishPositionPool` は **実績着順の列・特徴量・共有 PG pool**(feature build / running-style 学習データ)であり、**着順予測スコアリング process ではない**。realtime worker で着順予測を per-race で回している箇所は**無い**。
- → **「per-race で動いていた process」= 脚質(running-style)推論**。**着順予測(finish-position)は per-race では動いておらず**、Worker 経由では D1 に per-run の `started` 行が 6 件残るのみ。当時の実運用予測は legacy launchd(別系統)。

## 付記(Hard rules 遵守)

- 実行は **SELECT / read のみ**(DELETE/TRUNCATE/DROP・コード変更・deploy・新規 observability 有効化なし)。
- creds(D1 database_id / NEON URL / TRIGGER_TOKEN 等)は値を出さず mask。
- 証跡が無いものは「無い」と明記(per-race 着順予測ログ・完走 run・過去観測ログ)。
