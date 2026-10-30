# 脚質・着順予測の Cloudflare Workflows / Workers 移行 — 引き継ぎ計画

更新: 2026-10-31 (JST) / 基準コミット `6aaf8e98`

この文書は次の担当エージェント向けの「現状と次にやること」です。2026-09-23〜09-27 の日次ログ (旧版本文) は
[`docs/history/container-full-migration-handoff-log-2026-09.md`](history/container-full-migration-handoff-log-2026-09.md)
にそのまま移しました。過去の数値・判断根拠はそちらを参照してください。

## 1. 目的と制約

**目的 (ユーザー指示 2026-09-23)**: jra / nar / ban-ei の全ての脚質予測・着順予測を Cloudflare Workflows と Cloudflare Workers で動かし、本番の Cloudflare Container 実行時間を最小化する。0 にする必要はない。残課題を 0 にし、本番での動作確認を必ず行う。関連目標として、勝率ヒートマップの cache warm を Workflows / Workers 上で安定させる (9/23 に NAR のヒートマップが表示されなかった)。

**「最小化」の解釈**: Worker で Python と同一の出力を再現できない処理 (DuckDB による重い特徴量生成、Worker に実行環境がないモデル) だけを Container に残す。それ以外の処理と、Container の待機時間・過大なインスタンスサイズを削る。

**制約 (変更不可)**

- 単勝人気・単勝オッズを予測・ルーティング・ブレンドに使わない。レポートでの参考表示だけは可。全 trainer と serving に `assert_no_banned_market_features` / `predict_lib.market_feature_policy` がある (`b0fbdcfd`, `5bb52c78`)。
- Neon を最小化する。新規の critical-path 依存は追加しない。調査・検証で Neon をクエリしない (ユーザーの明示許可がある場合を除く)。
- Worker 実装は、凍結入力に対して公開値が Python と完全一致 (ルーティング・Prophet 補正・guard を含む) するまで公開経路に入れない。Python は fallback として残す。
- R2 へのモデル upload は Claude からは実行不可。ユーザーがスクリプトを実行する (memory: model-release-r2-upload)。

## 2. 現状 (2026-10-31)

### 2.1 設定値の出所

下表のフラグは repo HEAD `6aaf8e98` の `apps/finish-position-cron/wrangler.jsonc` と `apps/sync-realtime-data/wrangler.jsonc` の値です。2026-10-31 に Cloudflare API の GET `/workers/scripts/{name}/settings` で本番と照合し、主要フラグは一致しました。

| Worker               | 本番 deployment                       | 備考                                     |
| -------------------- | ------------------------------------- | ---------------------------------------- |
| finish-position-cron | 2026-10-25T01:50Z, version `45c050f1` | `env.production` は無く top-level が本番 |
| sync-realtime-data   | 2026-10-25T01:49Z, version `2863c746` |                                          |

主要フラグ (finish-position-cron):

| フラグ                                                                | 値                      | 実際の効果                                                                                                     |
| --------------------------------------------------------------------- | ----------------------- | -------------------------------------------------------------------------------------------------------------- |
| `COORDINATOR_ENABLED`                                                 | `0`                     | 時刻駆動の coordinator は無効                                                                                  |
| `RESCORE_CATEGORIES`                                                  | `nar,ban-ei`            | **coordinator しか読まないので現在は無効果**                                                                   |
| `RESCORE_ENABLED`                                                     | `1`                     | 馬体重イベントで rescore。**JRA を含む全カテゴリ**                                                             |
| `JRA_WORKER_RESCORE_ENABLED`                                          | `0`                     | JRA Worker scorer は本番では使われない (`queue-consumer.ts` ~L2884)                                            |
| `BANEI_WORKER_RESCORE_SHADOW_ENABLED`                                 | `1`                     | ばんえい Worker CatBoost は shadow (書き込みなし、parity を log)                                               |
| `RESCORE_CONTAINER_ENABLED`                                           | `0`                     | 1vCPU/3GiB の rescore Container は使われず、**rescore は standard-4 の legacy `predict-{category}` DO で動く** |
| `RACE_CHAIN_CONTAINER_ENABLED` / `RACE_CHAIN_CONTAINER_CATEGORIES`    | `1` / `jra,nar,ban-ei`  | focused-full は race-chain Container で実行 (コメントの「Kept off」は誤り)                                     |
| `RACE_CHAIN_FUSED_ENABLED`, `WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED` | `1`, `1`                |                                                                                                                |
| `FOCUSED_FULL_WATCH_ENABLED`                                          | `1`                     | コメントの「Default-off」は誤り                                                                                |
| `DAY_BASE_SPLIT_ENABLED`                                              | `jra,nar,ban-ei`        |                                                                                                                |
| `DAY_BASE_MANIFEST_REQUIRED`                                          | `0`                     | sync-realtime-data も `0`                                                                                      |
| `PROPHET_SCORE_ADJUSTMENT_ENABLED`                                    | `jra,nar,ban-ei`        |                                                                                                                |
| `SOURCE_DATABASE_URL`                                                 | `r2-catalog://pc-keiba` | Container の特徴量入力は R2 Catalog。`conn_url.py` が不一致なら fail closed                                    |

sync-realtime-data: `RUNNING_STYLE_REQUIRE_DAY_BASE_CACHE_HIT=1`, `RUNNING_STYLE_D1_WRITE_ENABLED=1`。

### 2.2 処理ステップ別の実行場所

| ステップ                                   | 実行場所                                                                      | JRA                           | NAR                 | ばんえい                  | 主なファイル                                                                                           |
| ------------------------------------------ | ----------------------------------------------------------------------------- | ----------------------------- | ------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------ |
| day-base 生成 (DuckDB, 当日全レース特徴量) | **Container** (legacy `predict-{cat}` DO, standard-4) を Workflow から起動    | ○                             | ○                   | ○                         | `finish-position-cron/src/race-day-workflow.ts`, `day-base-prewarm.ts`, Container `/prewarm-day-base*` |
| day-base の R2 commit・per-race 分割       | Worker                                                                        | ○                             | ○                   | ○                         | `day-base-pickup.ts`, `day-base-race-materializer.ts`                                                  |
| fanout (レースごとの enqueue)              | Workflow → Worker                                                             | 脚質 readiness 必須           | 脚質 readiness 必須 | 脚質 gate なし            | `feature-hit-prediction.ts`, `running-style-readiness.ts`                                              |
| market-signal foundation                   | Worker                                                                        | ○                             | —                   | —                         | `race-chain-market-signal-hook.ts`                                                                     |
| focused-full (レース単位の着順予測)        | **Container** (race-chain, 2vCPU/6GiB)                                        | ○                             | ○                   | ○                         | `queue-consumer.ts` ~L3030-3180, `predict_upcoming.py`                                                 |
| 馬体重後 rescore                           | **Container** (legacy standard-4 DO)                                          | ○ (Worker scorer は flag off) | ○                   | ○ (Worker は shadow のみ) | `queue-consumer.ts` `processPerRaceRescore` ~L2862                                                     |
| 予測行の永続化                             | **Container → Neon UPSERT**                                                   | ○                             | ○                   | ○                         | `upsert_sql.py`, `_flush_scored`                                                                       |
| KV 公開 (`pred:fp:v1`)                     | Worker。**Neon を読んで** KV に書く                                           | ○                             | ○                   | ○                         | `prediction-kv-writer.ts` L234                                                                         |
| 脚質予測 (推論)                            | **Worker のみ** (sync-realtime-data, LightGBM flatbin)                        | ○                             | ○                   | ○ (source=nar, 場 65/83)  | `sync-realtime-data/src/running-style-{cron,queue,inference}.ts`                                       |
| 脚質の特徴量                               | Worker。R2 day-base parquet (cache hit 必須) → Catalog → Neon fallback        |                               |                     |                           | `running-style-feature-materialize.ts`                                                                 |
| 脚質の保存                                 | D1 → KV/Cache → **Neon (完了条件)** → R2 parquet → Catalog (05:30 JST append) |                               |                     |                           | `running-style-neon.ts`, `daily-keiba-sync/src/running-style-catalog.ts`                               |
| 勝率ヒートマップ warm                      | Workflow `pc-keiba-viewer-heatmap-warm` + DO owner + build queue              | ○                             | ○                   | ○                         | `pc-keiba-viewer/src/worker/heatmap-warm-workflow.ts`, `heatmap-execution-owner.ts`                    |
| Viewer cache warm / bust                   | Worker                                                                        | ○                             | ○                   | ○                         | `prediction-cache-warm.ts`, `prediction-cache-bust.ts`                                                 |

**結論**: 公開される着順予測はすべて Python Container が計算しています。Worker / Workflow はオーケストレーションと、R2 への書き出し・分割だけを担当します。Worker 単独で完結しているのは脚質推論だけで、それも Container が作る day-base に依存しています。

### 2.3 Worker 側 scorer (`apps/finish-position-cron/src/scoring/`)

| モジュール                    | 対象                                                                          | 状態                                                                                                                                                      |
| ----------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `jra-shadow-scorer.ts`        | CatBoost 4 種 (sim, jockey_pedigree_703, prior_corner_005, stage1_marketfree) | 公開経路は flag off。**L66/72/78 が `-nowin1` を固定しており、本番の `-noexo1` と不一致**。joken cell / named-race / lock1 rerank / MPX / hybrid は未対応 |
| `banei-shadow-scorer.ts`      | CatBoost sim + Prophet                                                        | shadow のみ。バージョンは `-nowin2` で一致                                                                                                                |
| `nar-stage1-shadow-scorer.ts` | XGBoost raw score                                                             | テスト専用。本番 import なし                                                                                                                              |
| `jra-scorer.ts`, `etop2.ts`   | CB iter20 + XGB E-top2                                                        | 実質 dead code (テストのみ)                                                                                                                               |

Worker 側に LightGBM / ONNX の着順 scorer はありません。ライブラリは `catboost-json-tree`, `xgboost-json-tree`, `hyparquet` です。

本番モデル (`model_meta.json`):

- JRA: `jra-cb-v9-sim-2013-clean-noexo1` + 31 cell + named-race cell 7 本 (`kyoto_kikka_sho` は `jra_lock1_rerank_rest`)
- NAR: `iter12-nar-xgb-hpo-v8-clean188-nowin2` + 12 XGB cell + Set-Transformer blend (`iter40-...-nowin2`)
- ばんえい: `banei-cb-v9-sim-2011-nowin2`
- `stage1_routing.json` は JRA / NAR とも `enabled:false`

### 2.4 Container 構成

| app                              | サイズ                               | max_instances | sleepAfter                 | 用途                               |
| -------------------------------- | ------------------------------------ | ------------- | -------------------------- | ---------------------------------- |
| FinishPositionPredictContainer   | standard-4 (4 vCPU / 12 GiB / 20 GB) | 10            | 45m (`container-class.ts`) | day-base 生成、rescore             |
| FinishPositionRaceChainContainer | 2 vCPU / 6 GiB / 12 GB               | 4             | 30m                        | focused-full                       |
| FinishPositionRescoreContainer   | 1 vCPU / 3 GiB / 6 GB                | 1             | 10s                        | 未使用 (flag off、32 日間 usage 0) |
| mlflow-ui-proxy                  | 1 vCPU / 3 GiB                       | 1             | —                          | 予測とは無関係だが課金対象         |

ソフトウェア上の slot 上限 (`container-slot-cap.ts`): 合計 10、うちばんえい予約 2。rescore 3、race-chain 3。

### 2.5 Container 実行時間の実測

出所は Cloudflare GraphQL `containersUsageAdaptiveGroups` (GiB-h, 課金対象メモリ時間) と `containersMetricsAdaptiveGroups` (vCPU-h)。JST 日で集計 (UTC hourly を +9h)。保持期間は 32 日なので、9 月の値は旧ログ由来です。生データは scratchpad (`cont_oct.json`) にあり、repo には入れていません。

| JST 日              | predict vCPU-h / GiB-h | race-chain GiB-h | mlflow GiB-h |
| ------------------- | ---------------------- | ---------------- | ------------ |
| 金 10/16            | 42.6 / 135.5           | 31.3             | 9.6          |
| 土 10/17            | 43.3 / 135.4           | 33.3             | 12.8         |
| 日 10/18            | 4.4 / 13.3             | 15.4             | 11.9         |
| 月〜金 10/19-23     | 2.7–4.3 / 7.6–15.6     | 4.0–10.2         | 3.4–11.5     |
| 土 10/24            | 2.1 / 7.4              | 38.1             | 7.0          |
| 日 10/25            | 4.3 / 14.4             | 0                | 6.3          |
| **月〜木 10/26-29** | **0 / 0**              | **0**            | 2.2–3.4      |
| 金 10/30            | 8.9 / 29.8             | 22.8             | 6.2          |

| 期間                        | predict                        | race-chain | mlflow |
| --------------------------- | ------------------------------ | ---------- | ------ |
| 9/30–10/15 (16 日)          | 278.1                          | 214.1      | 157.9  |
| 10/16–10/30 (15 日)         | 394.6 (うち 10/16-17 で 270.9) | 174.6      | 106.7  |
| 参考: 9 月ピーク日 (旧ログ) | 336–562 / 日                   |            |        |

読み方:

- 平常日の predict は 7–15 GiB-h/日 で、9 月から 1/20〜1/40 に下がっています。10/16–17 のスパイクは原因未特定です (同時期に finish-position-cron の例外が増加。10/16 は 13,976 req 中 1,277 exceptions)。
- **10/26–29 のゼロは削減ではなく障害**です。`finish-position-race-day` Workflow の NAR prewarm がこの 4 日すべて errored で、`day-base not ready reason=day-base-missing-or-invalid` になり、`prewarm-day-base` step が `busy` を返し続けました。Workers Observability (10/27 00–03Z) のログは `[day-base-prewarm] discovery deferred category=nar reason=catalog-races-empty` が 15 件、`Catalog bulk readiness entries failed with HTTP 502` が 12 件。R2 Catalog に NAR の当日レースが無い (または読めない) ため day-base が作られず、fanout Workflow も起動していません。10/30 に回復しています。根因は NV (UmaConn) 取込の停止です (P0-1)。この期間に NAR の着順・脚質予測が公開されたかは未確認です (§6)。
- Workers request 量: finish-position-cron は平常 21k–35k/日 (UTC)、10/22 は 464k、10/23 は 778k。`f5fcb204` (10/24, completion watch の自己再送停止) の修正内容と整合しますが、因果は未証明です。
- Workflow 成功率: `finish-position-race-day` は 10/24–30 で 117 起動中 47 失敗。`pc-keiba-viewer-heatmap-warm` は累計 825 complete / 0 errored ですが、10/26–28 はイベントがありません。

### 2.6 critical path に残る Neon 依存

| #   | 依存                                                                             | 場所                                                                                     | 影響                                                                   |
| --- | -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| N1  | 着順予測行の UPSERT                                                              | Container `upsert_sql.py`                                                                | 公開の正本                                                             |
| N2  | KV 公開が Neon を読む                                                            | `prediction-kv-writer.ts` L234。`prediction-result-artifact` は本番から呼ばれていない    | Neon 障害で KV 公開が止まる                                            |
| N3  | completion 判定の count 読み (KV を先に確認)                                     | `focused-full-completion.ts` L593                                                        | rescore gate、self-heal                                                |
| N4  | Neon pre-wake `SELECT 1`                                                         | `neon-warm.ts` (cron, fanout, rescore)                                                   |                                                                        |
| N5  | JRA のレース discovery が Neon `jvd_ra` のみ                                     | `sync-realtime-data/src/postgres.ts` `fetchJraRacesByDate`                               | Catalog commit 後に Neon が遅れると JRA 開催を取り逃す                 |
| N6  | 脚質の `completed` 条件が Neon 書き込み                                          | `running-style-neon.ts`                                                                  | Neon 障害で脚質が `sync-failed` になり、JRA/NAR の着順 fanout も止まる |
| N7  | 脚質特徴量の Catalog 不可時 fallback                                             | `running-style-feature-sql.ts`                                                           |                                                                        |
| N8  | odds 対象レース一覧・NAR DebaTable URL 合成                                      | `sync-realtime-data-hot/src/scheduled-race-list.ts`, `expected-race-count.ts`            |                                                                        |
| N9  | Viewer: ヒートマップ血統 fallback、ばんえい重量区分、paddock の脚質、trend、WIN5 | `pc-keiba-viewer/src/db/queries.ts` ほか (`getDb()` 43 箇所、Catalog 分岐の有無は未精査) |                                                                        |
| N10 | daily-keiba-sync の Neon backup が run 成功条件                                  | `daily-keiba-sync/src/neon-sync.ts`                                                      |                                                                        |

## 3. 完了済み (2026-09-23 以降の要点)

- 2026-09 の日次ログ・旧承認境界: [history](history/container-full-migration-handoff-log-2026-09.md)。
- **Workflow 化**: `RaceDayWorkflow` (`finish-position-race-day`)。steps は day-base-readiness → stop-day-base-container → release-day-base-owner → fanout → warm-heatmaps。day-base は Workflow 内で R2 commit される (`404ec910`)。manifest / repair generation を D1 0010/0011 に追加 (`358c57f5`)。prewarm cron は `0 21` / `30 0` UTC。
- **focused-full / commit barrier / lane** (10/17–10/24): `98e02c5f` 運用ルート (`GET /api/admin/focused-full-diagnostics`, `POST /api/admin/clear-focused-full-commits`)、`b842f85c`、`d9ae3f0e`、`bfd9df49`、`aaa55ccd`、`9b5406f9`、`df228220`、`f5fcb204`、`eba628bf`。provider 別に失敗を隔離する修正があり、10/24 の UmaConn 障害時に JRA が 0/36 になった問題に対応済み。
- **モデル**: 全カテゴリで単勝人気・オッズを除去 (`-nowin1` → `-nowin2`)。JRA は exotic residual を除去 (`-noexo1`, `938542e9`) し、fail closed guard を追加 (`d101cb52`, `d243faf6`)。脚質は第 1 コーナーラベルで再学習 (`2b580f07`)。JRA 1勝・未勝利の ungated は sim へルーティング (`8bda7464`)。
- **named-race / lock1**: 菊花賞 cell (`6bf19c53`, `c0468344`)。completion が named/lock の version を受理 (`0f326e58`, `f75be512`)。
- **Worker scorer の parity 修正**: 欠損特徴量を NaN 扱い (`ec1052b5`)、odds 欠損時は median (`71f55874`)、取消馬を除外 (`833a20f7`)、`allowPostTimeRescore` (`306ec82a`)。
- **scratch**: day-base は出走集合の完全一致を要求 (`ac0aa326`)。sync は JRA の「取消」表示馬を保持 (`e01edf05`)。
- **馬場**: 当日の馬場状態で baba layer を再計算し、rescore でも反映。入力は Catalog (`4709f7b7`, `1a1e72b2`)。v2 pedigree を不要時にスキップ (`57fa7d8e`)。
- **ヒートマップ**: Workflow 化 (`623e0399`)、owner DO と失敗時 backoff (10/04, 10/10 recovery docs)、build queue と R2 SQL gate (10/17–18)。10/31 に枠統計の 36h cache 汚染を修正 (`8481790c`)、`jvd_um` の UM ingestion (`d800a9c4`)、血統 cache key を v4/v3 に変更 (`6aaf8e98`)。
- **Container 実行時間**: 平常日 predict 7–15 GiB-h/日 (9 月の 1/20 以下)。

## 4. 残タスク (優先度順)

各タスクの共通前提は §5 のガードレールです。

### P0-1. 10/26–29 NAR 障害の確認と再発防止

- **根因 (2026-10-31 に D1 `daily-keiba-sync.sync_runs` で確認済み)**: NV (UmaConn) の取込が提供元の緊急メンテナンスで止まっていた。`provider='nv'` の daily run は run_date 20261025–20261029 がすべて `acquisition_failed` (files 0 / records 0、10/24 以降は `X-NV-Link-Failure-Stage=license-verify`)、20261030 に `succeeded` (files 123 / records 13,599) で回復。Catalog に NAR の当日レースが入らず `catalog-races-empty` になったのはその結果で、予測パイプライン側の不具合ではない。残る作業は下の検知と回復の改善。
- **目的**: NV 取込が止まった日に、それを早く検知し、復旧後に自動で追いつけるようにする。影響範囲 (予測が公開されなかったレース) も記録する。
- **対象ファイル**: `finish-position-cron/src/day-base-prewarm.ts` (`getDayBaseDiscoveryReadiness`)、`race-day-workflow.ts`、`pc-keiba-r2-catalog` の race list / bulk readiness エンドポイント、`daily-keiba-sync` (NV 取込: `0 16` / `0 17` UTC cron)、`pipeline-health-monitor/src/provider-sync-signal.ts`。
- **手順**:
  1. 読み取りのみで確認する。Workers Observability で 10/25–10/30 の `daily-keiba-sync` と `pc-keiba-r2-catalog` のエラーを見る。NV/UmaConn の取込 run 状態は D1 を read-only で見る。Viewer KV `pred:fp:v1` / `pred:rs:v1` の対象日キーの有無も確認する。
  2. 根因を分類する: NV 取込停止 / Catalog commit 失敗 / R2 SQL 502 / discovery 側の不具合。
  3. pipeline-health-monitor に「Catalog の当日 NAR レース数 0 かつ realtime discovery にレースあり」アラートが無ければ追加する。
  4. prewarm が `busy` (discovery deferred) を返し続けた場合に Workflow がエラー終了するだけで回復しない点を、再試行設計として見直す。
- **完了条件**: 根因を文書化する。検知アラートの単体テストを追加し、coverage 95% を維持する。
- **本番確認方法**: 次の NAR 開催日に `finish-position-race-day` instance の steps が `ready` → fanout > 0 になること。heatmap-warm Workflow の当日イベントがあること。
- **リスク**: D1 / Observability の読み取りで Neon を叩かないこと。Queue の purge / replay はしない。

### P0-2. 10/16–17 predict スパイクの帰属

- **目的**: 1 日 135 GiB-h の原因 (rebuild ループ、長時間 idle、DLQ 再試行など) を特定し、同種の再発を防ぐ。
- **対象ファイル**: `container-slot-cap.ts`、`day-base-pickup.ts`、`dlq-consumer.ts`。
- **手順**: GraphQL の hourly + instanceId 次元と、Workflow instance・Observability ログを突き合わせる。保持期間 32 日のため **11/16 頃までに実施する**。
- **完了条件**: 原因の時間帯・ジョブ・修正方針を本書に追記する。
- **本番確認方法**: 平常日 predict ≤ 15 GiB-h が 2 週間続くこと。
- **リスク**: 期限を過ぎるとデータが消える。

### P1-1. rescore を standard-4 DO から外す (ばんえい → NAR → JRA)

- **目的**: rescore 1 回 16–78 秒のために 12 GiB の Container が起動し、45m の sleepAfter まで残ることを避ける。
- **対象ファイル**: `queue-consumer.ts` `processPerRaceRescore`、`scoring/banei-shadow-scorer.ts`、`scoring/rescore-consumer.ts`、`wrangler.jsonc` (`BANEI_WORKER_RESCORE_*`, `RESCORE_CONTAINER_ENABLED`)。
- **手順**:
  1. ばんえい shadow parity log (Workers Observability) を 2 開催日分集計し、全頭の順位・確率が Python と完全一致することを確認する。
  2. 一致すれば、ばんえい Worker rescore の公開 flag を新設する (既存 JRA 経路と同じ構造で Container fallback を残す)。テストを追加する。
  3. 代替案、または並行案として `RESCORE_CONTAINER_ENABLED=1` (1vCPU/3GiB, 10s) を NAR に限定適用し、所要時間と結果一致を測る。メモリピーク (9/30 に約 4.18 GiB 観測、day-base 時) と rescore 単体のピークを比較してから判断する。
- **完了条件**: ばんえい rescore の Container 起動 0。NAR rescore が rescore Container か Worker に移る。両方で KV と Neon の世代・頭数が一致する。
- **本番確認方法**: 開催日に rescore 対象全レースで `prediction-readiness` が weight 後完了になること。Container usage の legacy predict 時間が rescore 時間帯に増えないこと。
- **リスク**: 取消馬が出たときの field-relative 特徴量 (`833a20f7` のコミットメッセージが未解決と明記)。15/16 頭ガード (JRA06R01 9/27 事例) は緩めない。

### P1-2. KV 公開を Neon 経由から外す (N2 / N3)

- **目的**: Container が予測結果 artifact を R2 に書き、Worker がそれを KV に公開する形にする。Neon は backup 書き込みに降格する。
- **対象ファイル**: `prediction-kv-writer.ts`、`prediction-result-artifact.ts` (本番 caller なし)、`focused-full-completion.ts`、Container `_flush_scored` / `upsert_sql.py`。
- **手順**:
  1. Container が per-race の結果 artifact を R2 に書くようにする (Container の R2 認証は read-only なので、Worker pickup 経由が必要。`focused-full-cache-pickup.ts` と同じ形)。
  2. KV writer は artifact を正とし、Neon との差分を shadow log する。
  3. completion 判定を KV / artifact 優先にする。
  4. 2 開催日 shadow 一致後に Neon 読みを外す。
- **完了条件**: Neon が停止していても KV 公開が完了する (テストで再現)。coverage 95%。
- **本番確認方法**: 開催日の全レースで KV 世代 = artifact 世代 = Neon 世代になること。
- **リスク**: R2 書き込み権限の変更はユーザー承認が必要 (旧ログの parity capture と同じ承認境界: shared bucket `pc-keiba-features-archive` の保存・保持)。

### P1-3. JRA discovery と脚質完了条件から Neon を外す (N5 / N6)

- **目的**: Neon の遅延・障害で JRA 開催の取りこぼしや脚質 `sync-failed` が起きないようにする。
- **対象ファイル**: `sync-realtime-data/src/postgres.ts`、`worker.ts` (`upsertDiscoveredUrls` 等)、`running-style-neon.ts`、`running-style-queue.ts`。
- **手順**: discovery の race list を `PC_KEIBA_R2_CATALOG` に切り替え、Neon は fallback にする。脚質の `completed` は D1 + KV + R2 parquet で判定し、Neon は非同期 retry (outbox) にする。
- **完了条件**: Neon を無効にした単体テストで JRA discovery と脚質完了が通る。coverage 95%。
- **本番確認方法**: 開催日朝の discovery-status で JRA レース数が Catalog と一致すること。脚質 state が `completed` になる時刻が従来以下であること。
- **リスク**: Catalog commit が遅れた日 (P0-1) は discovery も遅れる。両方失敗した場合の扱いを決めること。

### P2-1. JRA Worker scorer の整合と範囲決定

- **目的**: JRA rescore を Worker に移す前提を整える。
- **対象ファイル**: `scoring/jra-shadow-scorer.ts` L66/72/78、`scoring/rescore-consumer.ts`、`jra-parity-golden.json`、Container `named_race_cells.json` / `lock1_rerank.py` / `jra_hybrid_scorer.py` / `cell_routing.json`。
- **手順**:
  1. **必須の前提**: spec を `-noexo1` に揃え、golden を再生成する (R2 へのモデル配置はユーザー実行)。これを済ませずに `JRA_WORKER_RESCORE_ENABLED=1` にすると旧モデルが UPSERT される。
  2. 31 cell・named-race・lock1・hybrid・MPX のうち、Worker 化するものと Container に残すものをレース単位ルーティングで決める (残す場合は Container fallback)。
  3. 凍結入力で全頭完全一致を確認する。
- **完了条件**: Worker が担当するレースで Python と完全一致する。担当外は Container に回る。テスト追加。
- **本番確認方法**: shadow で 2 開催日一致してから flag on。開催日終了後に Neon / KV の model_version とスコアを照合する。
- **リスク**: named-race cell の version 受理漏れ (10/24 に発生) と同種の completion 不整合。

### P2-2. NAR Worker scorer

- **目的**: NAR の rescore と focused-full を Worker で動かす。
- **対象ファイル**: `scoring/nar-stage1-shadow-scorer.ts` (テスト専用) を本番 spec に拡張。
- **手順**: XGB 本体 + 12 cell を `xgboost-json-tree` で実装し、shadow で比較する。Set-Transformer blend は Worker に実行環境がないので、**Container に残すか、blend を外すか (精度影響あり) をユーザーに確認する**。
- **完了条件 / 本番確認 / リスク**: P2-1 と同じ。blend を外す場合は精度評価と承認が必要。

### P3-1. day-base 生成を Container 外へ (最大の削減、最大の工数)

- **目的**: 現在の Container 時間の主因である DuckDB day-base 生成を、R2 SQL / D1 / Workflow の chunk 実行に置き換える。脚質の day-base 依存 (`RUNNING_STYLE_REQUIRE_DAY_BASE_CACHE_HIT=1`) も解く。
- **対象ファイル**: Container `finish_position_features_duckdb` 系、`pc-keiba-r2-catalog` (`running-style-sql.ts`, `running-style-feature-ctes.ts`)、`race-day-workflow.ts`、`day-base-race-materializer.ts`。
- **手順**: 特徴量群を layer ごとに分割し、Catalog SQL で再現できる layer から Workflow step 化する。各 layer の parquet と既存 day-base の列単位一致を確認する。再現できない layer だけ Container に残す。
- **完了条件**: 全カテゴリで day-base 列の完全一致。Container の day-base 起動を「再現不能 layer のみ」に縮小する。
- **本番確認方法**: 開催日に新旧 day-base の hash / 行数 / 列一致を shadow で 2 開催日確認する。
- **リスク**: R2 SQL の 120 s timeout・60104/70200 エラー (旧ログ 9/29)、R2 SQL gate の 2 枠競合 (ヒートマップと共有)。

### P3-2. ヒートマップの本番監査と Neon 依存の削除

- **目的**: 10/31 の修正 (枠統計・血統) を本番で確認し、ばんえい重量区分と血統 fallback を Catalog に移す。
- **対象ファイル**: `pc-keiba-viewer/src/app/races/detail/detail-section-data.ts` (~L2700)、`pc-keiba-r2-catalog` heatmap stats、`queries.ts` `getWeightClassStats` / `getBloodlineStats`。
- **手順**: 次の JRA・NAR・ばんえい開催日に全レースで HTTP 200 / 枠行あり / 血統あり を一括確認する。その後、Catalog に重量区分を追加する。
- **完了条件**: 開催日 3 カテゴリ全レースのヒートマップが枠・血統つきで表示される。Neon `getDb()` 経路がヒートマップから消える。
- **本番確認方法**: `/api/cache-warm/win-rate-heatmaps` の Workflow instance が全 complete で、Viewer GET の payload を確認する (Playwright は禁止のため HTTP payload で確認)。
- **リスク**: Cache API は purge できないため、修正のたびに key version の bump が必要。

### P3-3. Container 構成の縮小と掃除

- **目的**: 削減を確定させる。
- **内容**:
  - P1-1 / P3-1 完了後に predict standard-4 の max_instances と slot cap を実測に合わせて下げる。
  - sleepAfter の短縮は旧承認境界でユーザー承認が必要。
  - 実験用 Container app 21 個 (zenz-_, nvidia-ambernet-_ 等。usage 0) の削除可否をユーザーに確認する。
  - 誤ったコメントを修正する (`wrangler.jsonc` の max_instances 12、sleepAfter 20m、「Kept off」「Default-off」、`focused-full-completion.ts` L141 の stage1 enabled)。
  - `docs/architecture/finish-position-prediction-system.md` (9/13 以降未更新) を更新する。
- **完了条件 / 本番確認**: deploy 後に GET `/containers/applications` で設定を確認し、翌開催日の usage を測る。
- **リスク**: max_instances を下げると開催日ピークで slot 枯渇する。

## 5. 進め方とガードレール

- **デプロイ順**: Worker-only の変更 → shadow (書き込みなし) → 2 開催日の一致確認 → 公開 flag on。Container image の変更は最後に単独で行う。1 回の deploy で 1 つの論点だけを扱う。
- **開催時間帯を避ける**: JRA は土日 09:30–17:00 JST、NAR はほぼ毎日 10:00–21:00 JST、ばんえいは 13:00–21:00 JST。deploy・flag 変更は JST 22:00–05:00 に行う。prewarm cron (06:00 / 09:30 JST) の直前も避ける。2026-10-31 は土曜の開催日。
- **単勝人気・オッズを予測に使わない** (§1)。新しい scorer / 特徴量でも `market_feature_policy` を通す。
- **Neon を新たに使わない・調査で叩かない**。D1 / KV / R2 / Observability の read-only で確認する。
- **リポジトリ規約** (`CLAUDE.md`):
  - coverage 4 指標 ≥ 95% を下げない。include を縮めない。
  - `v8 ignore` / `oxlint-disable` / `ts-ignore` の新規追加は禁止。
  - lefthook を `--no-verify` で迂回しない。
  - 変更と同じディレクトリのテストを更新する。
  - commit 前に `bun run --filter <pkg> test:coverage` と `uv run pytest` を実行する。
  - bun / bunx を使い、npm / npx は使わない。
- **旧承認境界 (継続)**: 以下はユーザー承認なしに行わない。
  - sleepAfter の短縮
  - 稼働中 Container の強制停止
  - Queue の purge / replay / owner の強制解放
  - 15 対 16 頭ガードの緩和
  - key 重複を示さない KV TTL 削減
  - 共有 bucket への新規書き込み領域
- **本番確認は必須**: 各タスクは開催日の実データ (Workflow instance の steps、KV 世代、Viewer GET payload、Container usage) で完了を確認する。HTTP 200 だけでは成功と見なさない (9/27 JRA06R01 は旧世代を 200 で返した)。
- **モデル upload**: R2 配置はユーザーにスクリプト実行を依頼する。

## 6. 未確認事項

- 10/26–29 に NAR (およびばんえい 10/26) の着順・脚質予測が公開されたか。(`catalog-races-empty` の根因は NV 取込停止と確認済み: P0-1)
- 10/16–17 predict スパイクの原因 (32 日保持のため 11/16 頃が期限)。
- 10/22–23 の request 急増と `f5fcb204` の因果。
- JRA06R01 (9/27) の 06 番の公式出走可否と snapshot 差の根因。NAR55R09 / 54R12 #12 の除外理由。
- preWeight KV 一時失効の恒久原因 (9 月から未解決)。
- D1 7403 の根因。Access service token のローテーション (旧ログ「資格情報の要対応事項」) が済んだか。
- 9/22 の Viewer score-warm 503 / byte-limit 問題の現状。
- ばんえい Worker shadow parity の直近一致率 (log 未集計)。
- `focused-full-cache` が独立ルートかどうか (Container 側は未追跡)。
- Viewer `queries.ts` の `getDb()` 43 箇所のうち、本番 `cloudflare` モードで Neon に到達するもの。
- 旧ログ・`docs/cloudflare-2026-09-observability-audit.md`・`docs/cloudflare-billing-review-20260927.md`・`docs/container-minimization-loop.md` から本書へのリンクは、旧版のセクション名を指している箇所がある。該当内容は history ファイル側にある。
