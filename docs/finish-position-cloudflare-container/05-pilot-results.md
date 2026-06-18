# CF Container 着順予測 フルパス Pilot 結果

## 実施概要

| 項目                   | 値                                     |
| ---------------------- | -------------------------------------- |
| 実施日時 (JST)         | 2026-06-19                             |
| Worker Version ID      | `6263cee2-0536-4429-a5bd-fa5487304638` |
| Container Image        | `005d35d9`                             |
| Pilot カテゴリ         | NAR                                    |
| Pilot runDate (KV key) | `20260619` / `20260620`                |
| /health レスポンス     | 200 OK                                 |

## Go/No-Go 判定 (Phase 1 = plumbing): **GO**

全確認項目をパス。SIGTERM/reap なし。Container → DO → Queue consumer のフルパスが疎通。

## Go/No-Go 判定 (Phase 2 = racesPredicted>0 確証): **NO-GO**

`racesPredicted>0` を CF HTTP serve mode で得られず。根因 = **HTTP `/predict` の
keepalive チャンクが pipeline 実行中に流れないバグ**。詳細は下記「Phase 2」節。

## Go/No-Go 判定 (Phase 3 = keepalive 修正後の再 pilot): **NO-GO (新ブロッカー)**

keepalive バグ (commit `44fa09f`) は **修正・実証済み** (progress が ~10s 毎に stream、
DO reap なし、フル pipeline が 15 分枠内で完走)。しかし `racesPredicted` は依然 **0**。
**新たな独立ブロッカーを CF container ログで特定**: head-to-head features layer の
**DuckDB OutOfMemoryException (7.4 GiB/7.4 GiB used)** で feature build が中断する。
`standard-4` (4 vCPU / 12 GiB) では DuckDB の実効メモリ上限が ~7.4 GiB で足りない。
詳細は下記「Phase 3」節。**本番 cutover は本 OOM 修正まで不可。Mac launchd authority を維持する。**

---

## 確認項目

### 1. Container 起動・完走 (SIGTERM/reap なし)

`wrangler tail` 観測:

```
GET http://do/predict?category=nar&daysAhead=2&mode=full&runDate=20260619 - Ok
Queue finish-position-predict-queue (1 message) - Ok

GET http://do/predict?category=nar&daysAhead=2&mode=full&runDate=20260620 - Ok
Queue finish-position-predict-queue (1 message) - Ok
```

- `Exception Thrown` / `Container error: Network connection lost` / SIGTERM: **なし**
- Container が sleepAfter=15m のホールドフェッチを正常完了

### 2. KV 状態 (status=success)

```json
predict:20260619:nar → {"racesPredicted":0,"startedAt":"2026-06-18T17:57:42.511Z","status":"success"}
predict:20260620:nar → {"racesPredicted":0,"startedAt":"2026-06-18T18:03:54.139Z","status":"success"}
```

- `status: success` 確認
- `racesPredicted: 0`: Phase 1 では「データ未登録」と推測したが、Phase 2 で **誤りと判明**。
  実データは存在し (Mac は同日 36 races)、0 の真因は HTTP keepalive バグ (下記 Phase 2)。

### 3. Queue Consumer 疎通

- Dedup (isAlreadyRunning KV チェック) 動作確認済み
- message.ack() 呼出し確認 (Queue - Ok ログ)
- 注: NDJSON parse は result 行を受領できず racesPredicted=0 となった (Phase 2 で根因確定。
  下記「Phase 2」節を参照 — keepalive 未送出により result 行が stream に流れない)

### 4. Mac launchd 互換性

- Dockerfile に `ENV PREDICT_SERVE_MODE=http` を追加
- Mac launchd `finish-position-predict-daily.sh` に `-e PREDICT_SERVE_MODE=""` を追加
- Mac 側 CLI バッチモードは変更なし (authority 維持)

### 5. Worker Secrets injection (container env)

`container-class.ts` で `containerFetch` 前に `this.envVars` へ以下を注入:

```typescript
this.envVars = {
  MODELS_DIR: "/models",
  NEON_DATABASE_URL: this.env.NEON_DATABASE_URL,
  PREDICT_DAYS_AHEAD: this.env.PREDICT_DAYS_AHEAD,
};
```

---

## 本番 Cutover 前の残タスク

| タスク                              | 状態                                  |
| ----------------------------------- | ------------------------------------- |
| predict/rescore cron の有効化       | **未実施 — Mac launchd が authority** |
| JRA / Ban-ei カテゴリ smoke         | 未実施                                |
| racesPredicted > 0 実レースでの確認 | 未実施 (JRA 開催日に実施推奨)         |
| Neon コスト測定 (21y full scan)     | 未実施                                |

### Neon コスト考察

Container は Neon へ直接接続 (Hyperdrive なし、DuckDB postgres extension で native libpq)。
21 年分フルスキャン 1 回のコンピュートコストは Mac launchd と同等と想定するが、
CF Container 経由で毎日発火する場合は月次コストを実測して評価すること。

---

## 判定根拠

- **90s reap リスク**: sleepAfter 10-15m の graceful + held-fetch (/predict がレスポンスを返すまで DO は生存)。今回 NAR pilot で 0 races の短時間完了でも reap なし確認。長時間 (JRA ~3-5 min) は JRA 開催日 smoke で別途確認。
- **DO 内部エラー**: Worker コード連続更新直後の transient reset (5 回目 pilot)。安定デプロイ後 (6 回目) は解消。本番は deploy 頻度が低いため問題なし。
- **runDate フォーマット**: ISO `2026-06-19` → YYYYMMDD `20260619` バグを修正済み (queue-consumer.ts)。

---

## 修正サマリ (本 pilot 実施中に Fix したもの)

| ファイル                           | 変更内容                                                                 |
| ---------------------------------- | ------------------------------------------------------------------------ |
| `wrangler.jsonc`                   | KV namespace id `PLACEHOLDER_KV_ID` → `d984fba531804927ac1b551200d4b3cb` |
| `container-class.ts`               | `this.envVars` に NEON_DATABASE_URL 等を注入                             |
| `queue-consumer.ts`                | `runDate` → `runYmd` (YYYYMMDD) バグ修正                                 |
| `worker.ts`                        | `resolveCategory()` で POST body の `category` フィールドをパース        |
| `Dockerfile`                       | `ENV PREDICT_SERVE_MODE=http` を追加 (CF Container HTTP サーバモード)    |
| `finish-position-predict-daily.sh` | `-e PREDICT_SERVE_MODE=""` を追加 (Mac launchd CLI モード維持)           |

---

## Phase 2: racesPredicted>0 確証 (2026-06-19)

Phase 1 で plumbing は GO。Phase 2 は実データのある日付で `racesPredicted>0` を取りに行った。

### 実施した CF 本番 run (NAR)

| runDate    | 所要時間 (DO held fetch)  | KV status | racesPredicted | reap/SIGTERM |
| ---------- | ------------------------- | --------- | -------------- | ------------ |
| `20260618` | ~471s (7.9 min)           | success   | **0**          | なし         |
| `20260619` | ~392s (6.5 min, 単独 run) | success   | **0**          | なし         |
| `20260620` | ~742s 内に完了            | success   | **0**          | なし         |

全 run が **15 分枠内で完走・SIGTERM/reap なし**。しかし `racesPredicted` は全て **0**。

### 矛盾の発見: Mac authority は同日 36-60 races を予測

Mac launchd の 6/19 run (同一 Neon branch / 同一コード):

```
[realtime-odds] zero rows collected category=nar target_date=20260619 races=36 — null-odds fallback
[predict-upcoming] ok run_date=20260619 races_predicted=375
```

→ Mac は 6/19 NAR で **36 races / 375 entry rows** を予測・UPSERT 成功。CF は同日 0。
データは存在する。CF 固有の問題と確定。

### ローカル再現による root-cause

CF が push した **同一 image `005d35d9`** をローカル docker で 2 モード比較 (Neon は同一 branch):

1. **CLI batch mode** (`PREDICT_SERVE_MODE=""`、Mac authority と同経路)
   `RUN_DATE=20260619 PREDICT_DAYS_AHEAD=2 PREDICT_CATEGORIES=nar`
   → `[predict-upcoming] ok run_date=20260619 races_predicted=732` ✅ **正常**
   (daysAhead=2 で 60 races / 732 rows、base build → layer chain → score → Neon UPSERT 完走)

2. **HTTP server mode** (`PREDICT_SERVE_MODE=http`、CF Container と同経路)
   `GET /predict?category=nar&daysAhead=2&mode=full&runDate=20260619`
   → NDJSON stream に **progress 2 行 (`starting` / `predict`、両方 elapsed_s=0.0) のみ**。
   pipeline は background で進行 (CPU 200% / RAM 14GB で layer chain 実行確認) するが、
   **result 行が一切流れず、curl は 900s で timeout (rc=28)**。

### 確定した根因: HTTP serve mode の keepalive チャンク非送出

`predict_lib/serve.py` の `iter_predict_chunks` は、
`starting` / `feature-build` / `predict` の progress 行を **pipeline 呼び出しの前に**
全て yield した後、`_run_predict_fn`(= `_predict_category`、base build + 14 layer +
score + UPSERT で 4-8 分) を **同期ブロックで実行し、その間 1 チャンクも yield しない**。

- pipeline 内部の heartbeat は subprocess stdout (= docker logs) に出るだけで、
  HTTP NDJSON stream には乗らない。
- 結果、DO の `renewActivityTimeout` は最初の 2 チャンクで止まり、
  数分間 stream が無音 → DO/client が応答完了または timeout と判断 →
  `parseNdjsonStream` が result 行を受け取れず racesPredicted=0 (または error)。
- serve.py docstring (lines 14-21) が約束する「~10s ごとの keepalive」は **未実装**。
  progress yield は blocking call の「前」に固まっており「最中」には出ない。

### NO-GO 理由と必要な修正

CF Container は **HTTP serve mode 必須** (port 8080 probe + held-fetch keepalive)。
現状そのモードで結果が返らないため **production cutover 不可**。

必要な修正 (別タスク):

- `iter_predict_chunks` を **pipeline をワーカースレッドで実行**し、メインスレッドで
  `progress_interval_s` ごとに progress 行を yield する構造へ変更
  (現在の前倒し yield では blocking 中に keepalive が出ない)。
- または pipeline の heartbeat を queue 経由で generator に渡し、stream に転送。
- 修正後、ローカル HTTP mode で result 行 (`racesPredicted=732`) が出ることを smoke 確認
  → CF 再 deploy → 本 pilot を再実行し racesPredicted>0 を確証。

### 確証できたこと / できなかったこと

- ✅ plumbing (KV dedup / Queue / DO / container 起動 / SIGTERM なし / 15 分枠内完走)
- ✅ predict pipeline 自体は image `005d35d9` の CLI mode で 732 予測・UPSERT 正常
  (= モデル / 特徴量 / Neon 接続 / UPSERT は全て健全)
- ❌ CF HTTP serve mode で racesPredicted>0 を結果として受領 (keepalive バグでブロック)

### 副次メモ

- 過去日 (6/18) は upcoming 判定 (`kakutei_chakujun` blank) で 0 races になる設計
  (backtest 特性)。ただし今回の 0 は過去日が原因ではなく上記 HTTP バグが主因
  (6/19 当日・6/20 翌日でも 0、Mac は 6/19 で 36 races)。
- Neon コスト: CLI mode の 21y scan は base build ~200-260s。HTTP mode でも同等の
  scan が background で走るため、本番化時は同コスト想定 (cutover 前に実測要)。

---

## Phase 3: keepalive 修正後の再 pilot (2026-06-19)

keepalive バグ修正 (commit `44fa09f`、`iter_predict_chunks` を threaded keepalive 化) を
main から取り込み、再 deploy + 再 pilot を実施。

### deploy

| 項目              | 値                                                                   |
| ----------------- | -------------------------------------------------------------------- |
| Worker Version ID | `48112547-eff5-47d4-b665-2cb17fcfaf95` (keepalive 修正後の最終)      |
| Container Image   | `5c701855` (44fa09f から再ビルド)                                    |
| crons             | warm 3 件のみ (`55 17` / `25 0` / `*/30 1-11`)、predict/rescore 無効 |
| /health           | 200 OK                                                               |

(途中で NEON_DATABASE_URL secret を `.env.replica` の direct endpoint に再設定し再 deploy、
Version `48112547`。データ branch は同一なので結果に影響なし。)

### keepalive 修正の実証 (ローカル HTTP mode、同一 image `5c701855`)

`GET /predict?category=nar&daysAhead=2&mode=full&runDate=20260619` の NDJSON stream:

```
20:32:32 {"type":"progress","stage":"starting","elapsed_s":0.0}
20:32:32 {"type":"progress","stage":"predict","elapsed_s":0.0}
20:32:42 {"type":"progress","stage":"predict","elapsed_s":10.1}
20:32:52 {"type":"progress","stage":"predict","elapsed_s":20.2}
... (以降 ~10s 毎に継続) ...
20:42:33 {"type":"progress","stage":"predict","elapsed_s":603.6}
```

→ **progress が ~10s 毎に確実に stream される (keepalive 効いている)**。
DO の `renewActivityTimeout` が継続的に呼ばれ、**reap は発生しない**。
Phase 2 の「2 行で無音」は完全に解消。**keepalive バグは修正確認。**

### CF 本番 run (NAR、複数回)

| runDate    | 所要時間 (pipeline) | KV status | racesPredicted | reap/SIGTERM |
| ---------- | ------------------- | --------- | -------------- | ------------ |
| `20260619` | ~330-480s           | success   | **0**          | なし         |

全 run が 15 分枠内で完走・SIGTERM/reap なし。`progress` 間隔 ~10s。
しかし `racesPredicted` は依然 **0**。Neon `race_finish_position_model_predictions` の
6/19 NAR は **375 rows / 36 races (gen 06:37 JST = Mac launchd 由来)** のまま、
**CF run 完了後 (06:57 JST) も件数・timestamp が一切更新されず** = CF は 0 件 UPSERT。

### root-cause: head-to-head features layer の DuckDB OOM

CF container observability ログ (`containers` dataset、applicationId `a0348266`) を取得して特定:

```
[realtime-odds] zero rows collected category=nar target_date=20260619 races=60 — null-odds fallback
... (base build 成功、layer chain 進行) ...
Traceback (most recent call last):
  File "/app/pipeline/finish-position-features/add-head-to-head-features.py", line 270, in <module>
  File ".../add-head-to-head-features.py", line 263, in main
  File ".../add-head-to-head-features.py", line 124, in stage_current_pair_aggregates
_duckdb.OutOfMemoryException: Out of Memory Error:
  could not allocate block of size 256.0 KiB (7.4 GiB/7.4 GiB used)
```

- base build は 60 races / 732 rows を正常生成。
- その後の **head-to-head features layer (`stage_current_pair_aggregates`、pairwise 集計)**
  が DuckDB の実効メモリ上限 **7.4 GiB** を使い切り OOM。
- container は `standard-4` (4 vCPU / **12 GiB**) だが DuckDB auto-detect の memory_limit +
  OS/Python overhead で実使用上限が ~7.4 GiB に留まり、pairwise 集計が収まらない。
- 2 回の CF run (21:47 / 21:57 UTC) で **同一 OOM が再現** = 確定的なブロッカー。

ローカル比較で確証: 同一 image `5c701855` を **CLI mode** (メモリ ~23 GiB の docker host) で
6/19 daysAhead=2 を流すと **`races_predicted=732`** で完走・Neon UPSERT 正常。
→ コード/モデル/Neon は健全。**差分はメモリ予算のみ** (CF 12 GiB → DuckDB 実効 7.4 GiB が不足)。

### NO-GO 理由と必要な修正 (Phase 4 候補)

keepalive は解決したが、**head-to-head layer の DuckDB OOM** で feature build が完走しないため
`racesPredicted>0` に到達不能 = **production cutover 不可**。

必要な修正 (別タスク):

- (a) `instance_type` をより大きいメモリ tier に引き上げ (例 `standard-4` → メモリ増の tier)。
- (b) `add-head-to-head-features.py` の `stage_current_pair_aggregates` に DuckDB
  `SET memory_limit=...` + `SET preserve_insertion_order=false` + temp spill (disk) を設定し、
  メモリ予算内で完走させる (OOM メッセージが提示する 3 解決策そのもの)。
- (c) pairwise 集計を per-venue / per-race で chunk 化し peak メモリを削減。
- いずれか適用後、ローカルを 7.4 GiB 制約付きで再現 smoke → CF 再 deploy → 再 pilot。

### 確証できたこと / できなかったこと (Phase 3 時点)

- ✅ keepalive 修正 (progress ~10s 毎 stream、reap なし、15 分枠内完走) — **解決確認**
- ✅ plumbing (KV dedup / Queue / DO / container 起動 / SIGTERM なし)
- ✅ pipeline 健全性 (CLI mode で `races_predicted=732`・Neon UPSERT 正常)
- ❌ CF (`standard-4` 12 GiB) で `racesPredicted>0` — head-to-head layer DuckDB OOM でブロック
- ❌ Neon への CF 由来 UPSERT (件数・timestamp 不変で確認)

### メモ

- 「Cloudflare で着順予測を生成可能」は **メモリ予算を満たせば成立** (CLI 同一 image で 732 実証済み)。
  残る障壁は実行環境のメモリのみで、コード/データ経路は確証済み。
- `memory_budget_kernel_panic` の教訓と整合: DuckDB は memory_limit/threads/temp spill の明示が必須。
  CF firecracker では host メモリが固定 (12 GiB) のため、Mac (24 GiB Colima) で通っても CF で OOM する。
