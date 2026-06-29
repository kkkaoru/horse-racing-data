# 08. Per-Race 再構築計画 — targeted-history 設計 + per-race timing（git 考古学ベース）

> 調査日: 2026-06-19 / read-only 調査 + 設計のみ（コード変更・重い pipeline 実行なし）
> 前提ドキュメント: `01`〜`07`。とくに `06-per-race-architecture.md`（per-race=非推奨と結論）との **差分理由**を §10 で明記。
> 全事実は `git show` の実コード由来。推測は「【推測】」で明示する。
>
> **結論先出し（4 点）**:
>
> 1. **USER 証言「元々 per-race だった」は着順予測の実装上は誤り**だが、**脚質（running-style）推論は実在の per-race CF process**であり、これが USER 記憶の本体。脚質は「**10 分毎 cron → Queue per-race fan-out → Worker 直推論**」で動いており、**着順 per-race 化の理想テンプレート**（§4 で精読・転用）。
> 2. **per-race の本質的理由は「タイミング」**（USER 指示）。bataiju は発走 T-30〜50 分前確定、odds は発走直前まで変動。各 race を**発走時刻の少し前に発火**して最新 bataiju+odds を取り込む必要がある。コード確認の結果、**odds/bataiju 特徴は history と分離可能**（§3 で証明）→ **2 段構成（朝 heavy build→R2 cache、各 race T-X に軽量 rescore）が最適**。
> 3. **着順 process には既に 2 段 scaffold が存在**（未完成）。`finish-position-cron` の `PredictMode = "full" | "rescore"`、`shouldRunRescoreCron`（`*/20 1-11`）、R2 feature cache binding、realtime fetch（`/api/odds`・`/api/horse-weight`）が**配線済み**。**container 側 rescore（R2 load + odds/bataiju 差替 + re-score）だけ未実装**。
> 4. **history build を per-race にしたい場合**は base build を **targeted-history（出走馬 ketto に semi-join）**に書き換えれば安くなる（§6）。だが **timing 観点では 2 段 rescore のほうが最短×CF 適合**（heavy build は 1 回/日のまま、per-race timing は軽量 rescore が担う）。**推奨は 2 段**（§7-§8）。

---

## 0. エグゼクティブサマリ

| 問い                                 | 事実 / 結論                                                                                                                                                                        | 根拠                                                                                 |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| original（着順）は per-race だったか | **No**（実装上）。scaffold から per-day worker × per-category build。per-race は score ループのみ                                                                                  | `674dd49` worker.ts / pipeline_runner.py                                             |
| 実在の per-race CF process は何か    | **脚質（running-style）推論**。`*/10` cron → Queue per-race fan-out → Worker 直推論（軽量 LightGBM、container 不要）                                                               | `sync-realtime-data` running-style-cron.ts / running-style-queue.ts                  |
| per-race の本質的理由                | **timing**（bataiju T-30〜50min・odds 直前確定の鮮度）。per-category 一括ではこの per-race 最適窓に合わせられない                                                                  | USER 指示 + 脚質の weight-watchdog T-180min 設計                                     |
| odds/bataiju は history と分離可能か | **Yes**。`odds_score`/`popularity_score`/`tansho_odds`/`tansho_ninkijun` は target-race-only。`weight_diff_from_avg` のみ mixed だが `weight_avg_5`(history) を cache すれば差替可 | builder L1471-1489（odds）/ L1395-1419（weight）                                     |
| 着順に 2 段 scaffold はあるか        | **Yes（worker 層は配線済、container rescore 未実装）**。`PredictMode full/rescore`・R2 cache・rescore cron・realtime fetcher が既存                                                | finish-position-cron types.ts/worker.ts/queue-consumer.ts + realtime_odds_fetcher.py |
| per-race timing の推奨 primitive     | **coordinator + 短間隔 cron**（脚質流用、最短）。precise が要れば Queue `delaySeconds`（repo に既存 precedent あり）。DO alarm は over-engineering                                 | §2 比較表                                                                            |
| 1 段 vs 2 段の判断                   | **2 段（朝 build→R2、T-X rescore）**。heavy 21y build は 1 回/日、per-race timing は軽量 rescore。CF 適合・bataiju/odds 鮮度最良・工数最短                                         | §7 / §8                                                                              |
| 工数（推奨 2 段ルート）              | container rescore 実装 **1.0-1.5 日** / rescore cron→per-race timing 化 **0.5-1 日** / parity 検証 **0.5 日** / 合計 **~2-3 日**                                                   | §9                                                                                   |

---

## 1. pivot 前後の事実（commit ハッシュ付き）

### 1.1 scaffold `674dd49`（2026-06-03 16:05）"scaffold Cloudflare Cron + Container"

**worker 側の粒度 = per-day, 1 container, 全カテゴリ**（per-race ではない）:

- `wrangler.jsonc`: cron `"0 18 * * *"`（JST 03:00 daily）、`"max_instances": 1`、container instance 名は **`daily-finish-position-predict`** 固定。
- `dispatch.ts` `buildPredictStartOptions`: envVars は **`RUN_DATE` / `RUN_DATE_ISO` / `PREDICT_DAYS_AHEAD` / `NEON_DATABASE_URL`** のみ。**`race_bango` / `keibajo` / `race_id` は渡していない**。
- `worker.ts` `dispatchPrediction`: `getContainer(..., "daily-finish-position-predict")` を 1 個だけ `start()`。Queue も per-race fan-out も無い。
- → **cron→container は per-day（1 日 1 container）**。granularity に race は無い。

**container 側 = per-category 一括 build、score だけ per-race**:

- `predict_upcoming.py` `main()`: `for category in CATEGORIES:` で **カテゴリ単位**にループ。各カテゴリで `_build_feature_rows(category, days_ahead, database_url)` を呼ぶ。
- `pipeline_runner.py` `build_upcoming_feature_rows(category, days_ahead, ...)`: feature pipeline を **window 全体で 1 回**実行し、最後に `frame.groupby(RACE_ID_FIELD)` で `race_id -> entries` に割る。→ **build は per-category（一括）、per-race は pandas groupby と score ループだけ**。
- `pipeline_runner.py` `_build_pipeline`: `finish_position_features_duckdb.py` を **`--upcoming-days-ahead {days}`** で起動（race を絞る arg は無い）。

> **この `--upcoming-days-ahead` flag が builder に実在しなかったのが pivot を生んだバグ**（次節）。

### 1.2 pivot `a28412d`（2026-06-03 16:55、scaffold の 50 分後）"add target-date mode so the predict container builds TODAY's all races"

commit message 自身が証言している:

> "The Option 2 container's pipeline_runner **assumed a `--upcoming-days-ahead` flag that finish_position_features_duckdb.py never had**, so it could not build features for today's races … This adds a `--target-date` / `--days-ahead` mode that emits feature rows for **every race on `[target_date, target_date + days_ahead]`** regardless of finish_position"

- 追加された arg は **`--target-date` / `--days-ahead`**。粒度は **当日 window の全 race**（"TODAY's **all** races"）。
- `pipeline_args.py`（この commit で新規）が `--target-date` / `--pg-url` / `--config` / `--category` / `--from-date` を組む。**race を 1 本に絞る arg は追加されていない**。
- → pivot は per-race→per-day の変更**ではない**。**「per-day 全 race を build できなかったバグの修正」**であり、scaffold から一貫して **粒度は per-day / per-category** だった。

### 1.3 結論（USER 証言の裏取り）

| USER 証言                                                              | 実コード                                                                        | 評価                                                                                                                |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| 「元々 Cloudflare Containers での着順予測 process は per-race だった」 | scaffold から per-day worker × per-category build。per-race は score ループのみ | **実装上は誤り**。ただし「per-race にしたい」という設計意図は妥当（§7 の 2 段 + §6 の targeted-history で実現可能） |

> 【推測】USER の記憶は、score が `race_id -> entries` で per-race に割れている点、当初設計議論（"Option 2"）の per-race 案、または**実在する脚質（running-style）per-race CF process（§4）**を指している可能性がある。**着順の build を per-race 化する実装は git 履歴に存在しない**が、**脚質は実際に per-race で動いており**、これが per-race パターンの本物の precedent。

### 1.4 per-race の本質的理由 = タイミング（USER 指示の核心）

per-race の狙いは「メモリ削減」ではなく **bataiju/odds の鮮度**:

- **bataiju（馬体重）**: 発走 **T-30〜50 分前**に発表。それ以前は前走値しかない。
- **odds（単勝オッズ・人気）**: 発走 **直前まで変動**。確定は発走時刻。
- per-category 一括（現行 03:00 + 20 分毎 rescore で全 race 再走）では、**各 race の「発走少し前」という per-race 最適窓に合わせられない** → 古い/欠損 bataiju・確定前 odds で score → 精度劣化。
- 6/18 NAR 急落も serve タイミング起因（`project_serve_skew_root_cause_2026_06_11` 系の知見と整合）。
- → **各 race を「発走時刻の T-X 分前」に発火**し、その瞬間の最新 bataiju+odds を fetch して score するのが per-race の本質。**timing が主目的、メモリ/scan 削減は副次**。

> この timing 要件は **history build（発走数時間前に確定）と late-binding 特徴（bataiju/odds、発走直前確定）を分離**できれば、**2 段構成**で綺麗に満たせる（§3 で分離可能性を証明、§7 で 2 段設計）。

---

## 2. per-race timing primitive の比較（発走 T-X 発火）

各 race の発走時刻 `race_start_at_jst`（D1 `realtime_race_sources`、storage.ts:86/286、`hasso_jikoku` HHMM から `formatRaceStartJst` で生成、storage.ts:344）を起点に **T-X 分前**に予測を発火する。CF primitive を比較:

| primitive                                                                                                                                         | precise さ                              | 安価さ                                    | 実装最短                                                                                                                                             | 評価                                                                       |
| ------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| **A. coordinator + 短間隔 cron** — N 分毎 cron が `realtime_race_sources` を見て「発走が T-X 以内 & 未予測」の race を選び Queue へ enqueue       | 中（cron 間隔の粒度。`*/5` なら ±5 分） | **最安**（cron tick + D1 SELECT のみ）    | **最短**（脚質が既に同型。着順 rescore cron も既存）                                                                                                 | **推奨**。脚質テンプレ流用で即実装                                         |
| **B. Queue `delaySeconds` 遅延配信** — coordinator が race ごとに `delaySeconds = (race_start − T-X) − now` で per-race message を発走 T-X に配信 | **高**（秒単位）                        | 安（Queue 標準機能、追加 binding 不要）   | 短（**repo に precedent あり**: `cache-warm/race-trends/route.ts:71-113` が `getWarmDelaySeconds(race, nowMs)` で発走時刻起点の delay 配信を実装済） | precise が要れば採用。coordinator と併用可（cron で拾って delay 配信）     |
| **C. Durable Object alarm** — race ごとの DO が自分の予測時刻に `alarm()` をセット                                                                | **最高**（秒単位、self-scheduling）     | 中（DO instance × race 数、storage 課金） | 長（DO lifecycle・alarm 管理の新規実装）                                                                                                             | **over-engineering**。脚質も DO alarm は使っていない。本ワークロードに不要 |

**推奨**: **A（coordinator + 短間隔 cron）を基本**、precise 要件が出たら **B（`delaySeconds`）に格上げ**。両者は「coordinator cron が拾って delay 配信」で連続的に移行できる。**C（DO alarm）は不採用**（脚質前例なし・複雑性過大）。

> precedent 補足: 着順 `finish-position-cron` には既に **rescore cron `RESCORE_CRON_RACE_HOURS = "*/20 1-11 * * *"`**（cron-decision.ts）と `shouldRunRescoreCron` が定義済（ただし wrangler.jsonc では未 active、L26-29 でコメントアウト）。これを `*/5` 等に細かくし、enqueue 時に発走時刻 gating（"発走が T-X 以内の race のみ"）を足せば **A がそのまま成立**。

---

## 3. late-binding 特徴（bataiju/odds）の history からの分離可能性

> **2 段 rescore が成立するか**＝「history を再計算せず bataiju/odds だけ差し替えて re-score できるか」をコードで確認した。**結論: ほぼ可能**（mixed 列 1 つだけ要再計算）。

### 3.1 late-binding 列（target-race-only、発走直前に確定）

| 列                     | 由来                                                                    | history 依存                                                      | 分離                                                                                              |
| ---------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `odds_score`           | `ln(tansho_odds)/ln(300)` clamp（builder L1475-1486 `legacy_five_cte`） | **無**（その race の odds のみ）                                  | **完全分離**                                                                                      |
| `popularity_score`     | `(ninkijun−1)/(runner_count−1)` clamp（L1475-1486）                     | **無**（その race の人気のみ）                                    | **完全分離**                                                                                      |
| `tansho_odds`          | `rec.tansho_odds`（realtime COALESCE 後、L490-493）                     | **無**                                                            | **完全分離**                                                                                      |
| `tansho_ninkijun`      | `rec.tansho_ninkijun`（realtime COALESCE 後、L486-489）                 | **無**                                                            | **完全分離**                                                                                      |
| `weight_diff_from_avg` | `current_bataiju − weight_avg_5`（weight_cte L1395-1419）               | **mixed**: `current_bataiju`=late-binding、`weight_avg_5`=history | **半分離**（history 側 `weight_avg_5` を cache すれば、late 側 `current_bataiju` 差替で再計算可） |

### 3.2 early-binding 列（history-derived、発走数時間前に確定）

- jockey/trainer career・keibajo・distance 勝率、pedigree（sire/damsire）、running-style history、recent form、days-since、`weight_avg_5`（5 走平均体重、history）等 **大多数の特徴**。これらは **発走の数時間前には確定**しており、朝の per-category build で固定できる。

### 3.3 分離の結論

- **完全分離 4 列**（odds_score / popularity_score / tansho_odds / tansho_ninkijun）+ **半分離 1 列**（weight_diff_from_avg）以外は **全て early-binding**。
- → **朝に history ベース特徴量（weight_avg_5 を含む）を build して R2 cache、各 race T-X で (a) cache load、(b) 最新 bataiju/odds fetch、(c) 上記 5 列だけ再計算、(d) score** が **構造上可能**。21y Neon scan は **1 回/日**で済む。
- **既存の `--realtime-odds` 注入機構がこの 5 列をちょうど上書きする**: `stage_realtime_odds_table`（builder L689-734）→ `rec` の COALESCE（L486-497）で realtime odds/bataiju が tansho_odds/ninkijun/bataiju を上書き → odds_score/popularity_score/weight_diff_from_avg が再計算される。**つまり「odds/bataiju 差替で再 score」の SQL 経路は既に存在**。現状は full build と一体だが、**rescore mode で「cache load + realtime 注入 + 該当列再計算 + score」に切り出すだけ**。

> 注意（feature 列を減らさない rule）: 2 段でも **全列を出力**する（cache load 時に early 列はそのまま、late 5 列のみ上書き）。列削減・lossy 化は禁止（`feedback_no_feature_reduction`）。

---

## 4. 脚質（running-style）per-race CF process — 理想テンプレート精読

> USER 証言「元々 per-race」の本体。**実在し稼働中**の per-race CF process。着順に転用する。

### 4.1 脚質 per-race アーキ（コード確認）

| 要素                  | 実装                                                                                                                                                                | file:line                                      |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| **cron schedule**     | `RUNNING_STYLE_INFERENCE_CRON = "*/10 * * * *"`（10 分毎、JST 全時間）+ prewarm `"0 12 * * *"`（前日 21:00）                                                        | running-style-cron.ts:34-35、wrangler.jsonc:51 |
| **fan-out primitive** | **Cloudflare Queues**（`sync-realtime-data-running-style-jobs`）。DO alarm でも coordinator-only でもない                                                           | wrangler.jsonc:74-76,100-113                   |
| **race 列挙**         | `planRunningStylePredictionsForDate()` が D1 `realtime_race_sources` から当日 race を列挙、`race_running_styles` で既予測を除外、未予測のみ enqueue                 | running-style-cron.ts:286-348                  |
| **per-race dispatch** | `sendPredictionJobs()` が race ごとに `queue.send` / `sendBatch`                                                                                                    | running-style-cron.ts:272-284                  |
| **per-race handler**  | `handleRunningStylePredictionJob()` が **Worker 内で直推論**（R2 から flat LightGBM load + per-race feature parquet load + JS tree eval）。**container 不要・軽量** | running-style-queue.ts:88-188,121-123          |
| **queue consumer**    | `max_batch_size:1`・`max_concurrency:1`（128 MiB isolate で model+parquet を 1 race ずつ。2026-06-09 に 3→1 へ削減、memory 理由）                                   | wrangler.jsonc:100-113                         |

### 4.2 脚質の timing（bataiju/odds 取得に適切な根拠）

- 脚質 cron 自体は **発走時刻 T-X gating を持たず**、10 分毎に「未予測 race を全部 enqueue」する（running-style-cron.ts に start-time check なし）。
- **bataiju 鮮度は別 pipeline（weight watchdog）が担保**:
  - **weight watchdog cron `"* * * * *"`（毎分）** → `findStaleWeightFetchRaces()` が `realtime_race_sources.race_start_at_jst` を見て **±窓（lookback 30min / lookahead 180min）**の race の weight を fetch（worker.ts:1340-1378）。
  - **`WEIGHT_FETCH_LEAD_MINUTES = 180`**（worker.ts:283）= 発走 **T-180 分から weight を取りに行く**。near-race は 10 分 cooldown（発走 30 分以内、worker.ts:298-302）。
  - → 脚質が enqueue する頃には **bataiju が D1/PG に最新化済**。脚質 feature parquet がそれを読む。
- odds は別 worker `sync-realtime-data-hot`（毎分 cron `"* * * * *"`）が担保。
- **要点**: 脚質は「**per-race 推論を 10 分毎に回す**」+「**bataiju/odds は専用 watchdog が発走時刻基準（T-180min〜直前）で先に最新化**」の**2 系統分業**で鮮度を達成している。per-race 推論側が発走時刻 T-X を直接見るのではなく、**データ最新化側が発走時刻を見る**設計。

### 4.3 着順への転用（差分 = heavy さ）

| 観点              | 脚質                                                              | 着順                                                                                                                                  | 転用方針                                                                                                                                                                                                        |
| ----------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 推論コスト        | 軽量（flat LightGBM JS eval、128 MiB Worker で可）                | **heavy**（DuckDB 21y build + CatBoost/XGBoost、Container 必須）                                                                      | **history build を Worker に載せるのは不可**。だが **2 段（朝 build→R2、T-X 軽量 rescore）の rescore 側は軽量**（cache load + odds/bataiju 注入 + score）→ **rescore は Worker or 軽 Container で可**になりうる |
| fan-out           | Queue per-race（流用可）                                          | 同じ Queue per-race を着順 rescore message に流用                                                                                     | **そのまま流用**（着順は `PredictQueueMessage` + Queue 既存）                                                                                                                                                   |
| bataiju/odds 取得 | 専用 watchdog（既存）が D1/PG を最新化、推論は parquet 経由で読む | **着順は `realtime_odds_fetcher.py` で `/api/odds`・`/api/horse-weight` を直 fetch**（既存）。watchdog が最新化したものを HTTP で取る | **既存 fetcher 流用**。脚質 watchdog の最新化を着順 rescore が HTTP で拾う                                                                                                                                      |
| timing            | 推論 10 分毎、データ最新化は発走時刻基準 watchdog                 | 同型: **rescore cron を短間隔化 + 発走 T-X gating**（§2-A）                                                                           | **脚質の「推論定期 + データ watchdog 発走時刻基準」分業を踏襲**                                                                                                                                                 |

**転用の核心**: 着順は heavy build を Worker 化できないが、**heavy build を「1 回/日の朝 per-category build → R2 cache」に隔離**し、**per-race timing は脚質と同じ「Queue per-race + 発走時刻基準の最新 bataiju/odds 取得」で軽量 rescore**にすれば、**脚質テンプレートをほぼそのまま流用**できる。脚質が container でなく Worker で済む理由（heavy history を事前 materialize 済 parquet から読む）を、着順は **「朝 build の R2 cache を読む」** で再現する。

---

## 5. history query の系譜（全 population staging はいつから・どこで）

| commit                                                                 | 出来事                                                                                                                                                                                                                                                             |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `b914419` "DuckDB+Parquet alternative feature builder"                 | `finish_position_features_duckdb.py` **誕生**。最初から **訓練用の batch builder**で、`rec` は `where race_date between '{history_start}' and '{to_date}'` の **全 population staging**。targeted-history 版は**存在しない**。                                     |
| `3db6123` "share horse history base and unify pedigree CTEs"           | `horse_history_base`（target × rec join）に集約。**join 条件 `h.ketto_toroku_bango = t.ketto_toroku_bango`** がここで確立（後述の targeted 化の鍵）。                                                                                                              |
| `1a44bd7` "bake bataiju into history base for 200x weight_agg speedup" | history side の最適化だが **全 population のまま**。                                                                                                                                                                                                               |
| `a28412d`（pivot）                                                     | `--target-date` mode 追加。**target side（出力する行）**を window に絞るが、**history side（`rec`）は全 population のまま**。upcoming 行を `jvd_se`/`nvd_se` から直接 union（`upcoming_target_union_sql`）。                                                       |
| HEAD                                                                   | `HISTORY_LOOKBACK_YEARS = 10`。`compute_history_start(from_date, 10)` で `history_start = target_date − 10y`。`rec` は **全 population × 10y**を staging（`_rec_select_from_corner_features` L373: `where race_date between '{history_start}' and '{to_date}'`）。 |

> file は **viewer に 1 本だけ存在**し（`apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`）、container は Dockerfile の `COPY apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py /app/pipeline/` で**そのまま再利用**する（scaffold 時は container に存在せず、pivot で `--upcoming-days-ahead` を `--target-date` に直して整合）。**1 か所直せば train と serve 両方に効く**。

**核心事実**: 全 population staging は **file 誕生時から一度も targeted になったことがない**。理由は単純で、この builder は **21y 訓練データを作るためのもの**（全 race が target）だから、history と target が実質一致していた。serve（per-day 数十 race）でこの builder を流用したため、**target が薄いのに history scan だけ全 population**という非対称が生じた。

---

## 6. per-race を安くする核心 — targeted-per-target-horse history（代替: 1 段 heavy per-race）

### 6.1 history join の構造（targeted 化が効く理由）

base build の history feature は **3 系統**に分かれ、必要な history population が異なる:

| 系統                                                                                        | join 条件                                                                                                                               | 必要な history                                     | targeted 可否                                                                      |
| ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- | ---------------------------------------------------------------------------------- |
| **per-horse**（recent form / weight / running-style / days-since / same-distance 等の大半） | `h.ketto_toroku_bango = t.ketto_toroku_bango`（builder L2018-2019 `HORSE_HISTORY_BASE_FROM`）                                           | **対象馬自身の過去走のみ**                         | **完全に targeted 可能**（対象 race の出走馬 ketto 集合に閉じる）                  |
| **jockey / trainer**（騎手・調教師の career/keibajo/distance 勝率）                         | `h.kishumei_ryakusho = t.kishumei_ryakusho` / `h.chokyoshimei_ryakusho = t.chokyoshimei_ryakusho`（builder L962 `partner_history_cte`） | **その騎手/調教師が乗った/管理した全馬の history** | **部分 targeted**（対象 race の騎手/調教師集合に絞れる。馬は問わない＝field 必要） |
| **pedigree / keibajo bias / 血統 cohort**                                                   | sire/damsire/keibajo で集計（`pedigree_*` / `track_bias_cte` builder L1265-1281）                                                       | **その血統/競馬場の cohort 全体**                  | **部分 targeted**（対象 race の sire/damsire/keibajo 集合に絞れる）                |

> 重要: **支配コストの per-horse 系統は完全に targeted 可能**。`horse_history_base` は既に `h.ketto_toroku_bango = t.ketto_toroku_bango` で join しており、**target を 1 race に絞れば、対象馬の history 行しか materialize されない**。問題は **その手前の `rec` staging が全 population を Neon から引いている**ことだけ。

### 6.2 何を変えるか（CTE 単位）

**変える 1 か所**: `rec` staging（`_rec_select_from_corner_features` L347-375 / `_rec_select_from_ban_ei` / `stage_rec_table` L586-606）の **history side を target 馬の ketto に semi-join**する。

現状（全 population）:

```sql
-- _rec_select_from_corner_features（抜粋）
select ... from pg.race_entry_corner_features
where race_date between '{history_start}' and '{to_date}'   -- ← 全 population × 10y
```

targeted（対象 race の出走馬 + 騎手/調教師 field に限定）:

```sql
-- 概念形（PG 側 prefilter で Neon scan を減らす）
select ... from pg.race_entry_corner_features h
where h.race_date between '{history_start}' and '{to_date}'
  and (
    h.ketto_toroku_bango in (select ketto from target_horses)        -- per-horse 系統
    or h.kishumei_ryakusho in (select kishu from target_jockeys)     -- jockey 系統 field
    or h.chokyoshimei_ryakusho in (select chokyoshi from target_trainers)  -- trainer 系統 field
  )
```

- `target_horses` / `target_jockeys` / `target_trainers` は **対象 race（数本）の出走馬・騎手・調教師の小集合**（per-race なら ~12-18 ketto / ~12 騎手 / ~12 調教師）。
- pedigree/keibajo cohort は **対象 race の sire/damsire/keibajo に絞った別 semi-join**を追加（sire ごとの cohort は大きいが、対象 race の sire 数本に限定すれば全 sire population よりずっと小さい）。

> **これは既存の per-year loop（builder L2158-2167 `t.kaisai_nen = '{year}'`）と同型のパターン**。違いは「target side の filter」だけでなく **`rec`（history source）自体を semi-join で prune** する点。per-year は target を絞っても rec を絞らない＝Neon scan は減らないが、**semi-join は Neon に WHERE を push down して scan 自体を減らす**。

### 6.3 Neon scan 量・メモリ・時間の見積り（targeted per-race が安い論証）

**論理 scan 量の比較**（1 日 JRA 36 races / NAR 36 races を仮定）:

| 方式                                            | history side の Neon scan                                                                                | per-race fan-out で N 回払うか                                                                                                                                                          |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **現行（全 population, per-day 1 build）**      | 全馬 × 10y の `race_entry_corner_features`（数千万行 scan）                                              | per-day なので 1 回                                                                                                                                                                     |
| **per-race × 現行設計（全 population のまま）** | 全 population × 10y を **race 数（N=36）回**繰り返す                                                     | **N 回**（doc 06 が非推奨にした理由＝ここ）                                                                                                                                             |
| **per-race × targeted-history（本 doc）**       | 1 race ≈ 18 頭の 10y history + その race の騎手/調教師/sire field。**他 race の馬の history は引かない** | N 回だが、**各回が ~18 頭分**。日合計 = Σ(各 race の出走馬の history) ≈ **全 population history の重複なし合計に漸近**。同じ馬が複数 race に出ない限り、**日合計 < 全 population scan** |

**なぜ日合計でも全 population 未満か（構造証明）**:

- 全 population staging は「**その日に走らない馬の history も全部**」引く（10y 分の全在籍馬）。
- targeted per-race は「**その日に走る馬の history だけ**」を race ごとに引く。当日出走馬の集合 ⊊ 10y 全在籍馬。
- 馬は通常 **1 日 1 race**（複数 race 重複は稀）なので、targeted 日合計の per-horse 系統は **当日出走馬の history を 1 回ずつ**＝重複ほぼ無し。
- → **targeted per-race の日合計 Neon scan ≈ 当日出走馬の history のみ ≪ 全 population history**。**fan-out は Neon を増やすのではなく減らす**（per-horse 系統において）。

**field 系統（jockey/trainer/pedigree）の補正**:

- jockey/trainer は「その騎手の全馬 history」が必要なので per-horse より広い。ただし **対象 race の騎手 ~12 人**に絞れば、全騎手 population よりは小さい。複数 race で同じ騎手が重複すると semi-join 結果が一部重なる（日合計で多少の重複 scan）。
- → field 系統は per-horse ほどドラスティックには減らないが、**全 population 比では確実に減る**。pedigree も同様（対象 race の sire 集合に限定）。
- 【推測】worst case でも「targeted per-race 日合計 ≈ 全 population の 0.3-0.7x」。per-horse が支配項なので **全体として安くなる**。実測 parity 検証で確定すべき（§8.4）。

**メモリ・OOM**:

- doc 05 の OOM 真因は **h2h の `O(全期間²)` pairwise**（doc 06 §1.3）。targeted-history で `rec` が ~18 頭分に縮めば、h2h の pair 中間テーブルも対象 race の field 内に閉じ、**6 GiB limit に大幅な余裕**。
- per-race なら DuckDB peak は数百 MB-1 GiB 程度（doc 07 §3 の "per-race 小集計" に一致）。**`standard-2`（6 GiB）でも収まる**見込み。OOM 構造的に回避。

**wall-clock**:

- per-race の固定 overhead（subprocess 起動 ×layer 数、PG ATTACH）が残る（doc 06 §3.2 の懸念）。
- 緩和: **小 batch（1 container で 4-8 race）**にすれば subprocess overhead を amortize しつつ history は batch 内 race の出走馬に絞れる。**held-fetch 不要なほど短時間**（1 race ~10-30s 見込み）→ 15 分枠に余裕、SIGTERM リスク無し。

---

## 7. 再構築アーキテクチャ（推奨 = 2 段 + Worker-native scoring）

### 7.0 なぜ per-race が必須か（実測）

per-category monolithic build（`--memory=12g`）の実測 wall-clock:

| カテゴリ | wall-clock  | 15 分 Queue 上限                     |
| -------- | ----------- | ------------------------------------ |
| NAR      | **13.1 分** | ぎりぎり超過リスク                   |
| JRA      | **19.2 分** | **超過（OOM は解消も時間オーバー）** |

- OOM は解消できても **JRA の 19.2 分が Queue consumer / Cron / DO alarm の 15 分上限を超える**（doc 07 §wall-time）→ **per-category monolithic は CF で完走不能**。
- → **build を分割（per-race or 朝の 1 段 build を切り出し）するのが必須**。
- 証跡: 着順 per-race CF は **一度も完走していない**（D1 audit 6 行 2026-06-03 が全て `status=started` / `races=0`）。実在の完走 per-race process は **脚質（§4）のみ**。

### 7.1 推奨アーキ: 2 段（朝 heavy build → R2、各 race T-X 軽量 rescore）

```
[Stage 1 — 朝 1 回/category] heavy history build（21y Neon scan 1 回）
  Cron (per-category, 朝)
    → Container（standard-4, full build）
        finish_position_features_duckdb.py --target-date（既存 full path）
        → history ベース全特徴を build → R2 feature cache に put（PredictMode=full の既存設計）

[Stage 2 — 各 race の発走 T-X 分前] 軽量 rescore（Neon scan 無し）
  Coordinator cron (短間隔, 例 */5)  ← realtime_race_sources.race_start_at_jst を見て
    → 発走 T-X 以内 & 未 rescore の race を Queue へ enqueue（per-race）
      → 軽量 rescore consumer（Worker-native or 軽 container）
          (a) R2 feature cache から該当 race の特徴 load（history 列は確定済）
          (b) /api/horse-weight・/api/odds で最新 bataiju/odds fetch（既存 realtime_odds_fetcher）
          (c) late-binding 5 列のみ再計算（odds_score/popularity_score/tansho_odds/tansho_ninkijun/weight_diff_from_avg、§3）
          (d) score（catboost-json-tree / xgboost-json-tree、§7.2）+ E-top2 override
          (e) Neon UPSERT race_finish_position_model_predictions（既存 upsert_sql, idempotent）
```

- **Stage 1 の heavy 21y Neon scan は 1 回/日**（per-category）。15 分超でも **Container held-fetch（wall-time 無制限, doc 07）**で完走させる。Mac launchd を production authority にしない。
- **Stage 2 は Neon scan 無し**（R2 cache + HTTP fetch のみ）→ **数秒**で完了 → 15 分枠に余裕、SIGTERM 無し、OOM 無し。
- **per-race timing の本質（bataiju/odds 鮮度）を Stage 2 が担う**。各 race の発走 T-X に最新値で score。
- 既存 scaffold をそのまま使える: `PredictMode = "full" | "rescore"`（types.ts:8-10）、R2 cache binding（types.ts:22）、`shouldRunRescoreCron`（cron-decision.ts）、realtime fetcher（realtime_odds_fetcher.py）。**未実装は container/worker 側の rescore 本体（cache load + 5 列差替 + score）のみ**。

### 7.2 scoring 経路 — Worker-native vs 軽量 container

repo に **TS tree 評価器が 2 つ既存**（どちらも未配線、package のみ）:

| package              | 対象                                   | bit-exact 検証                                                                                                                                  | file                                       |
| -------------------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `catboost-json-tree` | CatBoost oblivious-tree raw score      | **済**（`RawFormulaVal` と bit-exact、コメント L11-14）。「sync-realtime-data-features Worker で finish-position ranking を score」用に作られた | `packages/catboost-json-tree/src/index.ts` |
| `xgboost-json-tree`  | XGBoost `rank:pairwise/ndcg` raw score | 済（NAR `nar-xgb-v7-lineage` 用、コメント L2-17）                                                                                               | `packages/xgboost-json-tree/src/index.ts`  |

カテゴリ別 Worker-native 可否:

| カテゴリ   | 本番モデル                                                                                                               | TS 評価器                                                                                                                                               | Worker-native scoring                                                                                                                          |
| ---------- | ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| **JRA**    | iter20 CatBoost(244) + E-top2 XGBoost override（model_meta.py:48,74）                                                    | catboost + xgboost 両方あり。**E-top2 override は純粋な再 rank ロジック**（CB#1↔CB#2 swap + score 注入、etop2_override.py:1-22）→ **TS 移植は trivial** | **可能**（CB+XGB+override 全て TS で組める）                                                                                                   |
| **Ban-ei** | CatBoost                                                                                                                 | catboost あり                                                                                                                                           | **可能**                                                                                                                                       |
| **NAR**    | iter12 XGB baseline + iter30 CatBoost + **iter36 LightGBM** の per-class ensemble（booster_pool.py:7-23, model_meta.py） | XGB+CatBoost は TS あり。**LightGBM の TS 評価器は repo に無い**                                                                                        | **部分的**。LightGBM member のため **TS だけでは不足** → ① LightGBM-json-tree を新規実装、② NAR だけ軽量 container で Python score、のいずれか |

- **特徴量を Worker で組めるか**: Stage 2 は **R2 cache から特徴を読む**だけなので、Worker が DuckDB を持つ必要は無い（heavy 特徴は Stage 1 で確定）。late-binding 5 列の再計算は単純な算術（odds_score=ln/ln(300) clamp 等）で **TS で容易**。→ **特徴組み立ては Worker で可能**。
- **結論**: **JRA / Ban-ei は完全 Worker-native（catboost-json-tree + xgboost-json-tree + E-top2 TS 移植）で 2 段 rescore が組める**。**NAR は LightGBM member があるため、(a) lightgbm-json-tree を足すか (b) NAR rescore のみ軽量 container（cache load + Python score、Neon scan 無しなので数秒）**。

### 7.3 per-race timing 経路（脚質テンプレ流用）

- coordinator cron（脚質の `*/10` → 着順は `*/5` 等、§2-A）が `realtime_race_sources.race_start_at_jst`（storage.ts:86/286）を SELECT し、**発走 T-X 分以内 & 未 rescore** の race を Queue に enqueue（脚質 `planRunningStylePredictionsForDate` running-style-cron.ts:286-348 と同型）。
- precise が要れば Queue `delaySeconds`（repo precedent: `cache-warm/race-trends/route.ts:71-113`）で発走 T-X に秒単位配信。
- **bataiju/odds の最新化は既存の専用 watchdog（脚質側）が発走時刻基準で先に行う**（weight watchdog `* * * * *`, T-180min lead, worker.ts:283/1340-1378）。着順 rescore は最新化済の値を `/api/horse-weight`・`/api/odds` で HTTP 取得（realtime_odds_fetcher 既存）。

### 7.4 代替: 1 段 per-race targeted-history（§6）

timing より「heavy build も per-race 化」したい場合は §6 の targeted-history（base build を出走馬 ketto に semi-join）。per-race の Neon scan が race 数倍に膨れず、むしろ全 population 未満になる（§6.3 で構造証明）。だが **heavy build を per-race fan-out すると subprocess/ATTACH 固定 overhead × race 数が残り、2 段より重い**。**2 段（heavy は 1 回/日）を推奨**、targeted-history は **Stage 1 を更に絞りたい時の追加最適化**として併用可。

**CF 適合（2 段）**:

- Stage 2 per-race は **軽量**（R2 cache + HTTP + TS score、数百 MB / 数秒）→ Worker（128 MiB）or 軽 container。
- **OOM 回避**: Stage 2 に DuckDB heavy 集計が無い。
- **Neon cost**: heavy scan 1 回/日（現行と同等）+ Stage 2 は Neon UPSERT のみ → `e84b078`（Neon コスト削減）を regress しない。

---

## 8. 実装チェックリスト（最短ルート = 2 段 + Worker-native）

> 既存 scaffold を最大流用。**未実装は「Stage 2 rescore 本体」と「coordinator の発走時刻 gating」のみ**。

### 8.1 Stage 1 — 朝 heavy build → R2 cache（**ほぼ既存、put を足す**）

1. `pipeline_runner.py` の full build path（既存）で、最終 parquet を **R2 feature cache に put**（`PredictMode=full`、R2 binding は types.ts:22 で既存）。container 側 put 処理を実装。
   - test 観点: cache key（category+date）生成、put の冪等性。
2. queue-consumer.ts は full message を container `/predict?mode=full` に渡す（既存配線 queue-consumer.ts:44）。

### 8.2 Stage 2 — per-race 軽量 rescore（**新規実装、ここが本体**）

3. **rescore consumer 本体**（Worker-native 推奨。JRA/Ban-ei は Worker、NAR は §7.2 の選択）:
   - (a) R2 feature cache から該当 race の特徴 load。
   - (b) `/api/horse-weight/{raceKey}`・`/api/odds/{raceKey}` で最新 bataiju/odds fetch（realtime_odds_fetcher.py の TS 版 or 既存 Python 流用）。
   - (c) late-binding 5 列再計算（§3.1: odds_score/popularity_score/tansho_odds/tansho_ninkijun/weight_diff_from_avg）。
   - (d) score: `catboost-json-tree` + `xgboost-json-tree` で raw score → per-race rank。
   - (e) E-top2 override 適用（JRA、etop2_override.py のロジックを TS 移植）。
   - (f) Neon UPSERT（既存 upsert_sql）。
   - test 観点: cache load、5 列再計算が full build と一致、TS score が Python `RawFormulaVal` と bit-exact（既存 catboost-json-tree test の延長）、E-top2 override の 4 ケース分岐、空 odds fallback。
4. **TS scorer 配線**: `catboost-json-tree` / `xgboost-json-tree` を rescore worker に import（現状 package のみで未配線）。R2 から JSON tree model load。
   - test 観点: model load、feature 名 positional projection（booster_pool.py:52-56 の順序規律を TS でも厳守）。
5. **E-top2 override の TS 移植**: CB#1↔CB#2 swap + score 注入（etop2_override.py:1-22）。class 701 除外。
   - test 観点: 4 ケース（XGB#1==CB#1 / ==CB#2 / ∈CB#3+ / class701）。

### 8.3 per-race timing — coordinator の発走時刻 gating（**rescore cron を細粒度化**）

6. `cron-decision.ts` の `RESCORE_CRON_RACE_HOURS`（既存 `*/20 1-11`）を `*/5` 等に細かくし、`shouldRunRescoreCron` を active 化（wrangler.jsonc L26-29 のコメントアウト解除）。
   - test 観点: cron 判定。
7. `enqueuePredict`（queue-producer.ts 既存）の手前に **発走時刻 gating** を足す: `realtime_race_sources.race_start_at_jst` を SELECT し、**発走 T-X 分以内 & 未 rescore** の race だけ per-race message を enqueue（脚質 `planRunningStylePredictionsForDate` running-style-cron.ts:286-348 と同型）。
   - test 観点: T-X window 判定、既 rescore 除外、per-race message 生成。
8. （任意）precise 化: Queue `delaySeconds`（`cache-warm/race-trends/route.ts:71-113` の `getWarmDelaySeconds` を流用）で発走 T-X に秒単位配信。

### 8.4 NAR の LightGBM member 対応（§7.2）

9. NAR per-class ensemble に LightGBM member（iter36）があるため、**(a) lightgbm-json-tree を新規実装**（xgboost-json-tree と同様の純 TS tree eval）、**または (b) NAR rescore のみ軽量 container**（cache load + Python score、Neon scan 無しで数秒）。**段階導入なら NAR は当面 (b)**。
   - test 観点: LightGBM raw score 一致（(a) 採用時）。

### 8.5 運用

10. **Production authority は Cloudflare のみ**。CF 2 段は parity 確認まで shadow 運用 → 確認後 flip。
11. Stage 1 が 15 分超なら **Container held-fetch（wall-time 無制限）**を前提に Cloudflare path を調整する（doc 07）。Stage 2 は短時間で枠内。

---

## 9. 工数見積り + 段階

| 作業                                                                      | 工数            | 備考                                                                   |
| ------------------------------------------------------------------------- | --------------- | ---------------------------------------------------------------------- |
| Stage 1 full build → R2 cache put                                         | **0.5 日**      | full path は既存、put のみ                                             |
| Stage 2 rescore 本体（cache load + 5 列再計算 + score + E-top2 + UPSERT） | **1.0-1.5 日**  | TS scorer 配線 + E-top2 移植が中心。NAR (b) なら Python 流用で更に短縮 |
| coordinator 発走時刻 gating（rescore cron 細粒度 + enqueue gating）       | **0.5-1 日**    | 脚質テンプレ流用、Queue/producer 既存                                  |
| parity 検証（2 段 score == 現行 monolithic score）                        | **0.5 日**      | bit-exact 検証、accept gate 前提                                       |
| NAR LightGBM TS 評価器（(a) 採用時のみ）                                  | **+1.0 日**     | (b) 軽量 container なら 0                                              |
| **合計（JRA/Ban-ei 先行、NAR は (b)）**                                   | **~2.5-3.5 日** |                                                                        |

**段階推奨**: **① JRA を先行**（本番 deploy 済 iter22-etop2、CatBoost+XGB+E-top2 が全て TS 化可能、19.2 分で最も per-race の恩恵大）→ ② Ban-ei（CatBoost のみ、最単純）→ ③ NAR（LightGBM member のため (a)/(b) 判断）。

- 「まず NAR か全カテゴリか」への回答: **NAR ではなく JRA を先行**。理由は (i) JRA が 19.2 分で 15 分超＝per-race の必要性が最大、(ii) JRA は LightGBM member が無く Worker-native scoring が即可能、(iii) NAR は LightGBM 対応に追加工数。**全カテゴリ同時より JRA→Ban-ei→NAR の段階導入が低リスク**。

**最短ルート**: §8.1（cache put）→ §8.2/§8.3（JRA の rescore 本体 + timing）→ §8.4 parity → JRA flip → Ban-ei → NAR。

---

## 10. doc 06（per-race=非推奨）との差分理由

| 観点                      | doc 06 の前提・結論                                                                                                     | 本 doc（08）の前提・結論                                                                                               |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **history staging**       | **全 population staging を所与**（「base build は history を target 馬で絞らず全 population を staging」§0/§2.2）       | **その所与を外す**。base build を **targeted-history（出走馬 ketto に semi-join）**に書き換える                        |
| **per-race の Neon scan** | per-race にすると 21y 全 scan を **N 回払う** → Neon 数十倍 → 非推奨（doc 06 §0 表 A / §3.3）                           | per-race は **対象馬の history のみ**を引く → 日合計が **全 population 未満**になりうる → Neon **減る**（本 doc §6.3） |
| **per-race chunking**     | 「container 実行単位」にするのは非推奨。推奨は **layer 内部 scoping（B）+ 実行は per-category**                         | base build を targeted 化すれば **per-race/小 batch chunking が成立**（安く・OOM-free・held-fetch 不要）               |
| **「大改修」の評価**      | base build の history CTE を semi-join に書き換えるのは「大改修で精度不変保証が要る」と **コスト扱い**（§2.2 L145-147） | **同じ改修を「正しい投資」と評価**。per-year loop（既存）と同型パターンで工数 ~1.5 日、parity 検証で精度不変を確定     |

> **両 doc は矛盾しない**。doc 06 は「**現行の全 population 設計のまま** per-race chunk にするな」と正しく言っている。本 doc は (i)「**全 population 設計をやめれば** per-race chunk が安くなる」(§6)、(ii) **doc 06 が扱わなかった timing 要件（bataiju/odds 鮮度）こそ per-race の本質**で、これは **2 段（朝 build→R2、T-X 軽量 rescore）が最適**(§7)、と補う。doc 06 は「heavy build を per-race にするか否か」のみを論じ、**「heavy を 1 回/日に隔離して per-race は軽量 rescore に分ける」第 3 の道を検討していない**。本 doc はその 2 段案と Worker-native scoring 可能性を加えて結論を更新する。

---

## 11. 参照（commit / file:line）

**git 考古学（pivot 前後）**:

- scaffold worker per-day 粒度: `674dd49:apps/finish-position-cron/{worker.ts,dispatch.ts,wrangler.jsonc}`
- scaffold container per-category build: `674dd49:apps/finish-position-predict-container/src/{predict_upcoming.py,pipeline_runner.py}`
- pivot（per-day all races, race 絞り arg 無し）: `a28412d`（message + `pipeline_args.py` 新規）
- builder 誕生・全 population staging: `b914419`、HEAD `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`
  - `rec` 全 population staging: `_rec_select_from_corner_features` L347-375
  - per-horse history join（targeted の鍵）: `HORSE_HISTORY_BASE_FROM` L2015-2029（`h.ketto_toroku_bango = t.ketto_toroku_bango`）
  - jockey/trainer field join: `partner_history_cte` L943-975
  - 既存 per-year semi-join パターン: `stage_horse_history_derived` L2145-2172
  - upcoming target 直接 union: `upcoming_target_union_sql` L516-548
- container は viewer builder を COPY 再利用: `HEAD:apps/finish-position-predict-container/Dockerfile` L48-49

**着順 per-race 2 段 scaffold（既存・未完成）**:

- PredictMode full/rescore + R2 cache binding: `apps/finish-position-cron/src/types.ts` L8-10, 22, 47-53
- rescore cron 判定: `apps/finish-position-cron/src/cron-decision.ts`（`RESCORE_CRON_RACE_HOURS`, `shouldRunRescoreCron`）
- worker scheduled rescore enqueue: `apps/finish-position-cron/src/worker.ts` L34,36,140-147
- queue producer/consumer: `apps/finish-position-cron/src/{queue-producer.ts,queue-consumer.ts}` L44
- realtime fetch（odds/weight）: `apps/finish-position-predict-container/src/realtime_odds_fetcher.py` L51-52, 274-289
- realtime 注入 SQL: builder `stage_realtime_odds_table` L689-734 / COALESCE L486-497

**late-binding 分離**:

- odds_score/popularity_score: builder `legacy_five_cte` L1471-1489
- weight_avg_5/weight_diff_from_avg: builder `weight_cte` L1395-1419 / `materialize_target_current_bataiju` L2107-2124

**scoring（Worker-native）**:

- CatBoost TS 評価器（bit-exact, finish-position 用）: `packages/catboost-json-tree/src/index.ts` L1-14
- XGBoost TS 評価器（NAR rank 用）: `packages/xgboost-json-tree/src/index.ts` L2-17
- E-top2 override ロジック: `apps/finish-position-predict-container/src/predict_lib/etop2_override.py` L1-22
- feature count（JRA 244）/ E-top2 / per-class ensemble: `apps/finish-position-predict-container/src/predict_lib/model_meta.py` L48,74 / `booster_pool.py` L7-23

**脚質 per-race テンプレート**:

- cron `*/10` + prewarm: `apps/sync-realtime-data/src/running-style-cron.ts` L34-35
- Queue per-race fan-out: 同 L272-348、wrangler.jsonc L74-76,100-113
- per-race 推論（Worker, R2 flat LightGBM）: `apps/sync-realtime-data/src/running-style-queue.ts` L88-188,121-123
- 発走時刻 source: `apps/sync-realtime-data/src/storage.ts` L86,286,313-344（`hasso_jikoku`→`race_start_at_jst`）
- weight watchdog T-180min: `apps/sync-realtime-data/src/worker.ts` L283,298-302,1340-1378
- delaySeconds precedent: `apps/pc-keiba-viewer/src/app/api/cache-warm/race-trends/route.ts` L71-113

**前提 doc**:

- doc 05（OOM 実測, h2h pairwise, audit 0 races）/ 06（per-race=非推奨, 全 population 前提）/ 07（sizing, 15 分枠/held-fetch）: `docs/finish-position-cloudflare-container/0{5,6,7}-*.md`
