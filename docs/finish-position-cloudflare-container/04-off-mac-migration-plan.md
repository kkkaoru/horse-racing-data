# 04 — Off-Mac 発走前予測 migration plan: CF Container + Neon pre-wake

> **Date:** 2026-06-19
> **Status:** 設計ドキュメント。コード未変更。
> **Scope:** Mac launchd からの完全 off-Mac 移行の具体設計。
> Neon pre-wake cron の先行 deploy、reap-safe な held-request + renewActivityTimeout、
> Neon direct feature-build cost 対策、pilot gate、ファイル単位実装チェックリストを含む。

---

## 0. 背景と目的

現行の着順予測は **Mac launchd** が JST 03:00 / 09:30 に `finish-position-predict-daily.sh`
を実行するローカル docker パイプラインで稼働している。2026-06-03 の CF Container 試行は
`start()` + DO alarm keepalive で ~90 s に SIGTERM されて失敗した。

doc 03 (architecture-design) により、現 CF Containers ドキュメントは
「10 m idle timeout + `renewActivityTimeout()` explicit keepalive + 15 m graceful-stop」
という新しいコントラクトを示す。**held request + `renewActivityTimeout()` を使えば
per-category (~1–2.5 min) は明らかに収まる**。

本ドキュメントはその **off-Mac 移行を実装可能な粒度で設計する**。

### 移行で変わること vs 変わらないこと

| 側面                           | 変わらない                       | 変わる                                               |
| ------------------------------ | -------------------------------- | ---------------------------------------------------- |
| predict_upcoming.py のロジック | 全て                             | SOURCE_DATABASE_URL が Neon 直(CF に 127.0.0.1 なし) |
| 予測の書き込み先               | Neon UPSERT(idempotent)          | —                                                    |
| PREDICT_CATEGORIES 分割        | 既存 env var で zero code change | —                                                    |
| 予測のトリガー                 | —                                | launchd → CF Cron Trigger                            |
| container 起動方式             | —                                | start() → held fetch('/predict?category=…')          |
| keepalive 方式                 | —                                | DO alarm → renewActivityTimeout() per heartbeat      |
| カテゴリ並列                   | 直列(Mac)                        | 並列 container(各 category 独立 DO instance)         |
| Mac の役割                     | authority + fallback             | pilot 確認まで authority 維持。cutover 後 fallback   |

---

## 1. Neon pre-wake cron(先行 deploy — 最初に入れる)

### 目的

Neon compute はデフォルト 5 分 idle で suspend する。予測ウィンドウ数分前に
`SELECT 1` で wake させ、feature build 開始時の cold-start latency と
suspend 中の接続タイムアウトを排除する。**Container は起動しない**。

### warm の実装方針

`@neondatabase/serverless` HTTP driver を使い、Worker isolate 内で
`SELECT 1` 相当の SQL を発行する。container は不要。

```typescript
// warm-neon.ts (新規)  ← 別 SubAgent が実装
import { neon } from "@neondatabase/serverless";

export const warmNeon = async (neonUrl: string): Promise<void> => {
  const sql = neon(neonUrl);
  await sql`SELECT 1`;
};
```

### warm タイミングと autosuspend

Neon の autosuspend はデフォルト 5 分。warm は predict 開始の **5–8 分前** に打つ。
一方 `SELECT 1` 自体は ~100–400 ms で返るので、再 suspend (5 分) が起きる前に
predict cron が発火する cadence にすれば良い。

| 予測 cron (UTC) | predict 目的         | warm cron (UTC) | JST 換算  |
| --------------- | -------------------- | --------------- | --------- |
| `0 18 * * *`    | NAR/Ban-ei JST 03:00 | `52 17 * * *`   | JST 02:52 |
| `30 0 * * *`    | JRA JST 09:30        | `22 0 * * *`    | JST 09:22 |

warm と predict の間隔は ~8 分。Neon の autosuspend は 5 分なので
「warm → 5 分後 suspend → predict 開始時には再 suspend済み」を防げる。
ただし race-hours (JST 10:00–20:00) に re-predict する場合は別途検討が必要(§1.4)。

### scheduled() への同居

既存 `scheduled()` と同じ Worker に warm cron を追加する。`cron-decision.ts`
を拡張して warm/predict の振り分けを行う。

```
// cron-decision.ts の拡張ロジック(設計)
PREDICT_CRON_NAR  = "0 18 * * *"   // JST 03:00 NAR+Ban-ei
PREDICT_CRON_JRA  = "30 0 * * *"   // JST 09:30 JRA
WARM_CRON_NAR     = "52 17 * * *"  // JST 02:52 Neon warm
WARM_CRON_JRA     = "22 0 * * *"   // JST 09:22 Neon warm

cronAction(cron: string):
  "warm"    → WARM_CRON_NAR, WARM_CRON_JRA
  "nar"     → PREDICT_CRON_NAR
  "jra"     → PREDICT_CRON_JRA
  undefined → その他(no-op)
```

`worker.ts` の `handleScheduled()` は `cronAction` の結果で分岐する:

```typescript
// worker.ts scheduled() — 設計
const action = cronAction(event.cron);
if (action === "warm") {
  await warmNeon(env.NEON_DATABASE_URL);
  return;
}
if (action === "nar" || action === "jra") {
  // predict 側(§2 の held-request フロー)
}
```

### race-hours re-predict(JST 10:00–20:00)

現行 Mac では `race-prediction-guard` launchd が 20 分ごとに再実行している。
CF への移行後、同様の re-predict が必要なら **別 cron** を追加する。
ただしこれは本 migration 計画の **PHASE 3** 以降(pilot 完了後)のスコープ。
初期 pilot は 03:00 と 09:30 の 2 cron のみ。

---

## 2. Reap-safe な CF Container 実行 — held `/predict` + renewActivityTimeout

### 2.1 問題の根本

2026-06-03 の失敗は「`start()` 後に Worker request が即 return → container に
inbound HTTP なし → ~90 s で SIGTERM」というパターン。現 `container-class.ts` の
DO alarm keepalive (`schedule(30s, "keepalivePing")`) は DO alarm の delay/coalesce

- container 起動直後の race condition で不安定だった。

**正解: held request + `renewActivityTimeout()`**

Containers の公式 long-running idiom は以下の通り:

- container が `/predict?category=nar` エンドポイントを持つ persistent HTTP server を動かす
- Worker/DO が `await c.fetch('/predict?category=nar')` を **hold**する(response が返るまで待つ)
- response は chunked(進捗 line を 10–15 秒ごとに stream)にして request を open にし続ける
- DO 側で各 progress chunk 受信時に `this.renewActivityTimeout()` を呼ぶ
- container は「inbound HTTP あり」と「explicit keepalive」の両方で保護される

### 2.2 フローの全体像

```
CF Cron Trigger (scheduled())
  ↓
cronAction(cron) → "nar" or "jra"
  ↓
enqueueCategory(env, runDate, category)   // Queue に 1 msg 投入
  ↓
Queue consumer (queue() handler)
  ↓
c = getContainer(env.FINISH_POSITION_PREDICT_CONTAINER, `run:${runDate}:${category}`)
await c.fetch(`/predict?category=${category}&runDate=${runDate}`)  // held
  ↓
container HTTP server: GET /predict?category=nar&runDate=20260619
  → _predict_category("nar") を実行
  → 10–15 s ごとに "progress: layer=X races=Y\n" を response body に stream
  → 完了したら JSON {"racesPredicted": N, "status": "success"} を送り response 終了
  ↓
Worker: response.ok → ack; else → retry
```

### 2.3 container 側の HTTP server 変更

現在の `predict_upcoming.py` は liveness のためだけに port 8080 の raw TCP サーバを
持つ (`_serve_liveness_socket`)。これを **本格的な HTTP/1.1 サーバに置き換え**、
`/predict?category=X` を処理できるようにする。

**注意**: container 側 HTTP server の変更は `predict_upcoming.py` の改修が必要。
別 SubAgent が実装する。設計上のポイント:

```
GET /predict?category=nar&runDate=20260619
  → Content-Type: text/plain; Transfer-Encoding: chunked
  → 10–15 s ごとに "progress layer=X races=Y attempt=Z\n" を flush
  → 完了後 "result {\"racesPredicted\":N,\"status\":\"success\"}\n" を送り close

GET /health
  → 200 OK  (DO の start() wait + probe 用)
```

既存の `LIVENESS_RESPONSE` raw TCP server は `/health` エンドポイントに置き換える。

### 2.4 DO 側の renewActivityTimeout 呼び出し

`container-class.ts` の現行 keepalive loop を **held request + renewActivityTimeout**
に書き換える。

```typescript
// container-class.ts — 設計
export class FinishPositionPredictContainer extends Container<Env> {
  override defaultPort = 8080;
  override sleepAfter = "45m"; // held request が来るのでほぼ発動しないが安全マージン
  override enableInternet = true;

  // /predict ハンドラ: response を streaming で返しつつ DO keepalive
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/predict") {
      return this.handlePredictRequest(request, url);
    }
    // /health and all others
    return this.containerFetch(request);
  }

  private async handlePredictRequest(request: Request, url: URL): Promise<Response> {
    const category = url.searchParams.get("category") ?? "";
    const runDate = url.searchParams.get("runDate") ?? "";
    // Proxy to container; consume chunked stream and renew on each chunk
    const upstream = await this.containerFetch(
      new Request(`http://container/predict?category=${category}&runDate=${runDate}`),
    );
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    void this.streamWithKeepalive(upstream.body, writer);
    return new Response(readable, {
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  }

  private async streamWithKeepalive(
    body: ReadableStream<Uint8Array> | null,
    writer: WritableStreamDefaultWriter<Uint8Array>,
  ): Promise<void> {
    if (!body) {
      await writer.close();
      return;
    }
    const reader = body.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      this.renewActivityTimeout(); // ← per chunk
      await writer.write(value);
    }
    await writer.close();
  }
}
```

### 2.5 Worker 側の Queue consumer

`worker.ts` に `queue()` handler を追加する。現行の `handleScheduled` は
Queue へのメッセージ投入のみに変更し、実際の container 起動は consumer に移す。

```typescript
// worker.ts — queue() consumer 設計
export const handleQueue = async (
  batch: MessageBatch<PredictJobMessage>,
  env: Env,
): Promise<void> => {
  for (const msg of batch.messages) {
    const { category, runDate, runYmd } = msg.body;
    const containerName = `run:${runYmd}:${category}`;
    const container = getContainer(env.FINISH_POSITION_PREDICT_CONTAINER, containerName);
    try {
      const resp = await container.fetch(
        new Request(`http://container/predict?category=${category}&runDate=${runYmd}`),
      );
      const text = await resp.text();
      if (!resp.ok) {
        msg.retry();
        continue;
      }
      const lastLine = text.trimEnd().split("\n").at(-1) ?? "";
      if (lastLine.startsWith("result ")) {
        // parse racesPredicted from JSON suffix
        msg.ack();
      } else {
        msg.retry();
      }
    } catch {
      msg.retry();
    }
  }
};
```

### 2.6 instance_type / sleepAfter / max_instances 推奨値

| パラメータ      | 現在                         | 推奨(pilot 後) | 理由                                                    |
| --------------- | ---------------------------- | -------------- | ------------------------------------------------------- |
| `instance_type` | `standard-4`                 | `standard-4`   | 変更なし。DuckDB 12 GiB が必要                          |
| `sleepAfter`    | `"45m"` (container-class.ts) | `"45m"`        | held request が来るので idle にはならない。安全マージン |
| `max_instances` | `1`                          | `3`            | JRA/NAR/Ban-ei を並列実行するため                       |

### 2.7 per-category 並列 vs 直列

**並列を推奨。** `PREDICT_CATEGORIES` env var で per-category 分割は zero code change で
既に可能。Queue fan-out で 3 メッセージを同時投入し、`max_instances: 3` で
3 container が並列実行する。wall-clock = max(category runtime) ≈ JRA ~2–2.5 min。

直列(Mac 現行)は ~3.5 min かかっていたので並列で ~40% 短縮できる。

---

## 3. Neon 直 feature-build cost 対策(最重要 tradeoff)

### 3.1 問題の整理

Mac では `SOURCE_DATABASE_URL = 127.0.0.1:15432` (Colima PG mirror) で
feature build を行い、Neon コストを回避していた。CF Container には
127.0.0.1 がないため、feature build が **Neon を直読み** する。

feature build の PG アクセスパターン(01-current-mechanism.md §2.C より):

- DuckDB `ATTACH` で `--from-date 20100101` 以降の全履歴を scan
- NAR: ~140 s の layer chain、大量 CTE join
- JRA: ~200 s cumulative
- Neon は 5 分 idle で suspend → feature build 中は基本 active だが suspend を
  挟んだ layer 間 pause があると接続切断リスク

### 3.2 緩和案の評価

#### (a) 当日出走馬のみにスコープして全 21y scan を避けられるか

**結論: 不可。** 実際の SQL ロジックを確認すると:

各 layer は `--from-date 20100101` で **全履歴を history として使う**。
例えば `add-near-miss-features.py` は対象馬の直近 N 走の成績を過去全期間から
LEFT JOIN する。`add-head-to-head-features.py` は同馬が同じ相手と過去に対戦した
全レコードを参照する。

スコープを「当日出走馬」に限定しても、その馬の **history** は 21 年分必要なため
PG scan 量は変わらない。target rows 数(今日のレース)は減らせても、
**history side の scan は減らせない**。これが `predict_upcoming.py` の
`_predict_category()` が ~140–200 s かかる根本原因。

**→ (a) は効果なし。21y scan は削減不可。**

#### (b) Neon read replica / branch

Neon の read replica はアカウント設定次第で用意できる。ただし:

- replica の provisioning cost が発生する
- feature build が read replica を読み、写予定 (UPSERT) は primary に書く、
  という routing 変更が必要
- 現状 `SOURCE_DATABASE_URL` / `NEON_DATABASE_URL` の 2 URL 分離は既存設計に
  すでに存在する(Mac では LOCAL と Neon を分離していた)
- CF 環境では `SOURCE_DATABASE_URL` に read replica URL を入れ、
  `NEON_DATABASE_URL` に primary を入れるだけで対応可能

**→ (b) は可能。ただし replica provisioning cost と管理コストが増える。
pilot phase ではまず primary 直接で計測し、cost/latency が問題なら replica を検討。**

#### (c) R2 parquet feature store の活用

`memory: project_features_r2_parquet_2026_05_29` によると特徴量本体は
**per-race R2 Parquet** として保存済み。ただし feature store は
「過去レースの特徴量」であり、**今日 (UPCOMING) のレースの特徴量は
当日の feature build 完了まで存在しない**。

feature store が使えるのは「predict 前日夜に pre-compute して翌朝 predict 時は
feature store 読み取りのみ」という2段階設計の場合。

現行 predict は「当日 JRA 09:30 時点に当日レース (UPCOMING) の特徴量を
feature build → score → UPSERT」という**当日 realtime build** 設計であるため、
前日 feature store は使えない(odds など当日データを含む)。

ただし **R2 running-style parquet** (`add-pacestyle-features.py`) は
既に R2 から読んでいる (`RS_SOURCE=auto`)。これは問題なく CF でも動く
(R2_ACCOUNT_ID/R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY を secrets として渡す)。

**→ (c) は running-style の R2 読み取りは OK(既存)。feature build 全体の R2 cache は
UPCOMING の当日データを含むため前日 pre-compute は不可。実用的でない。**

#### (d) cost を許容して pre-wake で latency のみ最適化

**これが現実的な正解。** 以下の根拠:

1. **Neon cost の主体は compute time**。feature build 中 (~140–200 s) は compute が
   active 状態で課金される。pre-wake cron (§1) により cold-start 時間は短縮できるが、
   build 中の compute time そのものは変わらない。
2. **現行の Neon cost 削減 (commit e84b078)** は「viewer が idle 時に Neon を
   wake させない」という変更だった。CF Container からの feature build は
   **必要な時だけ** active になるため、同じ原則を守れる(常時接続ではない)。
3. **3 categories parallel** (§2.7) により wall-clock は ~2.5 min 程度に収まる。
   Neon compute は 3 categories × ~2.5 min = 実質 max 2.5 min/run (parallel なので加算でなく max)。
4. NAR/JRA の feature build は PG history scan が dominant。
   Neon の compute unit は実 CPU 使用量に依存するため、DuckDB が
   postgres_scanner で大量 row を引く間は high compute。これは
   Mac launchd が local PG で処理していた load の移転であり、**absolute cost が増える**。

**結論: cost 受容 + pre-wake で latency 最適化が現実解。**

具体策:

- `WARM_CRON` (§1) で predict 8 分前に Neon を wake → feature build 開始時の
  cold-start latency (~1–3 s → ~0.1 s) を削減
- feature build 中の Neon compute cost は受容する。`SELECT 1` warm のみで
  実際の feature build 前に Neon が active 状態を保持する
- **e84b078 の原則(idle 時に wake させない)を維持**:
  predict cron 以外の時間帯は warm cron も predict cron も発火しない

### 3.3 Neon idle timeout 対策(feature build 中の接続切断)

feature build が 140–200 s かかる中、layer 間で一時的に PG query がない期間が
あると Neon が idle timeout (5 min) で接続を切る可能性がある。

対策: feature build の DuckDB ATTACH は `connect_timeout` + `keepalives` オプションを
PG DSN に追加する。これは **Python 側の `predict_lib/conn_url.py` で DSN を構築する際**
に append できる。例:
`NEON_DATABASE_URL?connect_timeout=30&keepalives=1&keepalives_idle=60&keepalives_interval=10&keepalives_count=5`

これにより 60 s idle でも TCP keepalive が維持され、layer 間 pause で切断しない。
**ただし Neon 側の idle timeout (5 min) は TCP keepalive では延長できない**ことに注意:
Neon の compute idle timeout はアプリ側 TCP keepalive とは独立した server-side 設定。
feature build の layer chain は基本的に継続的に PG に query しているため、
実用上は問題にならない可能性が高い。pilot で実測確認が必要。

---

## 4. idempotent UPSERT + dual-run 安全性

### 4.1 既存設計の確認

`predict_upcoming.py` の UPSERT は:

```sql
INSERT INTO race_finish_position_model_predictions (
  model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango, ...
) VALUES (...) ON CONFLICT (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
DO UPDATE SET ...
```

primary key 全列で conflict = **完全冪等**。同じ (model_version, source, race_id, ketto) の
row を何度 UPSERT しても結果は同じ。

### 4.2 dual-run (Mac + CF) の安全性

- Mac が 03:00 JST に NAR predict → Neon に rows 書き込み
- CF が 03:00 JST に同じ NAR predict → 同じ rows を UPSERT

結果: **後から書いた方が勝つが内容は同じ**。model_version が同じなら
predicted_score / predicted_rank も同じ(決定論的)。

E-top2 の override も `is_etop2_override_active()` は純関数(同じ odds → 同じ override)。

**dual-run は完全に安全。**

### 4.3 audit row の重複

`finish_position_cron_executions` は insert-only audit log で DELETE 禁止。
dual-run では Mac と CF から 2 行書き込まれる。pilot phase ではこれで問題ない。
`source` カラムを "mac" / "cf" で区別するか、既存設計のまま日次 count で確認するか
は実装 SubAgent の判断に委ねる。

---

## 5. Hybrid cutover + pilot — go/no-go gate

### 概要

**reap incident 再発防止が最優先。いきなり全面 cutover しない。**

```
PHASE 1: Neon pre-wake cron 先行 deploy
PHASE 2: Pilot (1 category, Neon branch, ≥3 run)
PHASE 3: Dual-run (Mac + CF 並列, 本番 Neon)
PHASE 4: Cutover (Mac を authority から fallback に降格)
PHASE 5: Mac launchd 完全廃止(任意・時期未定)
```

### PHASE 1: Neon pre-wake cron 先行 deploy

**目的**: warm cron 単体で安全に deploy。Container は起動しない。  
**作業**:

- `cron-decision.ts` に `WARM_CRON_NAR` / `WARM_CRON_JRA` を追加
- `worker.ts` に warm handler 追加 (`SELECT 1` via `@neondatabase/serverless`)
- `wrangler.jsonc` の `triggers.crons` に 4 エントリ追加
  (warm × 2 + predict × 2、ただし predict cron は cron-decision で no-op にしておく)
- deploy + `wrangler tail` で warm cron の実行を確認
- **Neon metrics で warm が wake を引き起こしていることを確認**

**Go gate**: warm cron が 2 回以上正常実行 (wrangler tail でエラーなし)。

**危険度**: ゼロ。warm は `SELECT 1` のみで予測 UPSERT は行わない。

### PHASE 2: Pilot (NAR category, Neon branch)

**目的**: held `/predict` が JRA 相当の full leg を SIGTERM なしで完走できるか実測。  
**使用 Neon**: branch (prod ではなく dev branch) で output はゴミ行扱い。

**手順**:

1. Neon console で `pilot-predict-branch` を作成(production からブランチ)
2. pilot 用に `NEON_DATABASE_URL` を branch URL に差し替えた **手動 `POST /run`** で起動:
   ```sh
   curl -X POST https://finish-position-cron.<subdomain>.workers.dev/run \
     -H "Authorization: Bearer $TRIGGER_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"runDate":"20260619","category":"nar"}'
   ```
   ※ `/run` trigger が category パラメータを受け付けるよう拡張が必要 (dispatch.ts)
3. `wrangler tail finish-position-cron` でリアルタイム監視

**観測項目**:

| 指標              | 期待値                                    | 確認方法                                 |
| ----------------- | ----------------------------------------- | ---------------------------------------- |
| held request 完走 | 完走(SIGTERM なし)                        | wrangler tail に SIGTERM/onStop ログなし |
| 経過時間          | ≤ 3 min (NAR)                             | wrangler tail のタイムスタンプ差         |
| racesPredicted    | > 0 (当日 NAR レースあり)                 | response body の result JSON             |
| Neon UPSERT       | pilot branch に rows 書き込み             | Neon console で確認                      |
| Mac parity        | pilot branch rows ≈ Mac output            | 同日 Mac run との行数/score 比較         |
| audit row         | branch の finish_position_cron_executions | D1 query                                 |

**繰り返し**: ≥3 run(別日または同日再実行の UPSERT idempotency 確認も含む)。

**Go gate**:

- 全 3 run で SIGTERM なし + 完走
- `racesPredicted > 0` (レース存在日に実施)
- Mac との output parity (predictions 行数一致、top-1 rank 一致)
- mid-run で Neon が idle timeout/接続切断しない

**No-go**: いずれか 1 run で SIGTERM または接続エラーが発生したら PHASE 2 を継続。
**per-venue chunking** (PREDICT_KEIBAJO env var 追加) を試みる。
それでも失敗なら「CF Container held-request も不安定と判明 — Mac launchd 継続」。

### PHASE 3: Dual-run (本番 Neon, Mac と CF 並列)

**条件**: PHASE 2 の go gate を ≥3 run で通過後。

**作業**:

- `cron-decision.ts` の predict cron を actual NAR/JRA cron に向ける
- `wrangler.jsonc` の predict cron を active に
- Mac launchd は **そのまま動かし続ける**(authority)
- 本番 Neon への UPSERT が Mac と CF 両方から行われる(idempotent なので安全)

**期間**: ≥5 営業日(JRA 開催日を含む)

**観測項目**:

- CF が Mac より先に / 後に書いても viewer が正しい predictions を返すことを確認
- Neon compute cost が e84b078 以前に比べて大幅に増えていないことを確認
  (Neon dashboard の compute minutes で weekly trend を確認)
- CF の wrangler tail でエラーなし

**Go gate**:

- ≥5 日、JRA 開催日 ≥2 回を含む
- CF と Mac の output が一致(model_version, predicted_rank が同一)
- Neon cost の weekly average が pilot 前比 +20% 以内

### PHASE 4: Cutover (CF を authority に)

**作業**:

- Mac launchd を disabled に変更(bootout はしない — fallback として residual を維持)
- CF が sole authority に
- `DEPLOY.md` と `01-current-mechanism.md` の authority 記述を更新

**Mac の位置づけ**:

- launchd は bootout せず残す
- 障害時(CF Container deploy 問題 / Neon branch 問題等)に
  `launchctl kickstart` で即時 fallback できる状態を維持
- **月 1 回 manual kick で Mac の docker image が stale でないことを確認**する運用推奨

**Go gate**:

- CF が 7 日以上 sole authority として稼働
- 人手介入なし(DLQ も発火していない)

### PHASE 5: Mac launchd 完全廃止(任意)

**条件**: PHASE 4 が 30 日以上安定したら検討。  
**手順**: `launchctl bootout` → plist を `scripts/launchd/archive/` に移動。  
**この phase は急がない**。Mac が standby になっても低コストであり、
fallback 価値がある。

---

## 6. ファイル単位の実装チェックリスト

以下は「別 SubAgent が実装する」ための具体的な差分計画。

### 6.1 `apps/finish-position-cron/src/cron-decision.ts`

**変更**: warm cron 定数と振り分けロジックを追加。

```
追加:
  - WARM_CRON_NAR = "52 17 * * *"
  - WARM_CRON_JRA = "22 0 * * *"
  - PREDICT_CRON_JRA = "30 0 * * *"
  既存 PREDICT_CRON("0 18 * * *") = NAR+Ban-ei のまま維持

  export type CronAction = "warm-nar" | "warm-jra" | "predict-nar" | "predict-jra" | undefined
  export const cronAction = (cron: string): CronAction => ...

変更:
  - shouldRunPredictCron は cronAction ベースに書き換える or deprecate
```

**テスト**: `cron-decision.test.ts` に warm cron / predict cron / unknown の
各ケース追加。

### 6.2 `apps/finish-position-cron/src/warm-neon.ts` (新規)

**内容**:

```typescript
// Run with bun. Neon pre-wake helper: fire a lightweight SELECT 1 via
// @neondatabase/serverless to bring the Neon compute out of suspend before
// the predict cron fires. Does not start a Container.
import { neon } from "@neondatabase/serverless";

export const warmNeonCompute = async (neonUrl: string): Promise<void> => {
  const sql = neon(neonUrl);
  await sql`SELECT 1`;
};
```

**依存追加**: `@neondatabase/serverless` を `apps/finish-position-cron/package.json` に追加。

**テスト**: `warm-neon.test.ts` を新規作成。`neon()` を vi.mock でモックして
warmNeonCompute が `SELECT 1` を呼ぶことを確認。

### 6.3 `apps/finish-position-cron/src/worker.ts`

**変更**:

- `handleScheduled` を cronAction ベースに書き換え
  - `"warm-nar"` / `"warm-jra"` → `warmNeonCompute(env.NEON_DATABASE_URL)`
  - `"predict-nar"` / `"predict-jra"` → Queue へのメッセージ投入
- `handleQueue` 追加 (queue() consumer): held fetch + ack/retry
- `default export` に `queue` handler 追加

**テスト**: `worker.test.ts` に warm / predict の各 cron action ケース、
queue consumer の ack/retry ケースを追加。

### 6.4 `apps/finish-position-cron/src/dispatch.ts`

**変更**:

- `buildPredictStartOptions` は現行通りで OK(Container start 設定)
- Queue メッセージ投入用 `buildPredictJobMessage` を追加
  (category を含む `PredictJobMessage` を組み立てる純関数)

**テスト**: `dispatch.test.ts` に buildPredictJobMessage のテスト追加。

### 6.5 `apps/finish-position-cron/src/container-class.ts`

**変更**:

- 既存の DO alarm keepalive (`onStart / keepalivePing`) を削除
- `fetch(request)` override を追加:
  - `/predict?category=X&runDate=Y` → `containerFetch` proxy + `renewActivityTimeout()` per chunk
  - `/health` → `containerFetch` (そのまま透過)
  - その他 → `containerFetch`

**テスト**: `container-class.test.ts` に renewActivityTimeout が呼ばれることを確認する
ケースを追加(ContainerFetch を mock)。

**重要注意**: `Container` クラスは vitest pool で instantiate できないため、
`container-class.ts` はカバレッジ gate 対象外のまま維持する(既存 comment 通り)。

### 6.6 `apps/finish-position-cron/src/types.ts`

**変更**:

- `Env` に `FINISH_POSITION_PREDICT_JOBS: Queue<PredictJobMessage>` を追加
- `PredictJobMessage` interface を追加:
  ```typescript
  interface PredictJobMessage {
    runDate: string;
    runYmd: string;
    category: "jra" | "nar" | "ban-ei";
  }
  ```

### 6.7 `apps/finish-position-cron/wrangler.jsonc`

**変更**:

```jsonc
// triggers を有効化
"triggers": {
  "crons": [
    "52 17 * * *",  // JST 02:52 — Neon pre-wake (NAR/Ban-ei)
    "22 0 * * *",   // JST 09:22 — Neon pre-wake (JRA)
    "0 18 * * *",   // JST 03:00 — predict NAR+Ban-ei
    "30 0 * * *",   // JST 09:30 — predict JRA
  ],
},

// containers の max_instances を 3 に
"containers": [
  {
    "class_name": "FinishPositionPredictContainer",
    "image": "../finish-position-predict-container/Dockerfile",
    "image_build_context": "../..",
    "instance_type": "standard-4",
    "max_instances": 3,  // 変更: 1 → 3 (JRA/NAR/Ban-ei 並列用)
  },
],

// Queue binding 追加
"queues": {
  "producers": [
    {
      "binding": "FINISH_POSITION_PREDICT_JOBS",
      "queue": "finish-position-predict-jobs",
    },
  ],
  "consumers": [
    {
      "queue": "finish-position-predict-jobs",
      "max_batch_size": 1,
      "max_retries": 3,
      "retry_delay": 120,
    },
  ],
},
```

**新規 secrets**: Neon pre-wake は既存 `NEON_DATABASE_URL` を流用するので
新規 secret は不要。R2 running-style credentials
(`R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`) を
Container envVars に追加する場合は `wrangler secret put` で設定。

### 6.8 `apps/finish-position-predict-container/src/predict_upcoming.py`

**変更**:

- `_serve_liveness_socket` (raw TCP) を **本格 HTTP/1.1 server** に置き換え
- `/predict?category=X&runDate=Y` エンドポイントを追加
  - 受け取った category を `PREDICT_CATEGORIES` env に相当するフィルタとして
    `_predict_category()` を呼び出す
  - 10–15 s ごとに `progress: layer=X races=Y\n` を chunked response で flush
  - 完了後 `result {"racesPredicted": N, "status": "success"}\n` を送り close
- `/health` → 200 OK (既存 liveness の代替)
- LIVENESS_PORT / LIVENESS_RESPONSE 定数は HTTP server 用に変更

**Python HTTP server の選択肢**:

- `http.server.HTTPServer` (標準ライブラリ) + `BaseHTTPRequestHandler`
  → シンプル。chunked response は `send_response_only` + `wfile.flush()` で実装可能
- `flask` (依存追加が必要)
  → オーバーキル。標準ライブラリで十分

**テスト**: predict_upcoming の unit test (mock ベース) は I/O 境界のため
coverage gate 対象外(既存方針通り)。smoke test は pilot で実施。

### 6.9 `apps/finish-position-predict-container/DEPLOY.md`

**追記**:

- "CF Container cron が disable されている理由" セクションの更新
  (90s reap は現 docs では解消、held request + renewActivityTimeout に移行)
- pilot runbook セクション追加(§5 の PHASE 1-4 の手順)

---

## 7. wrangler.jsonc の crons 変更まとめ

```jsonc
// 現在
"triggers": { "crons": [] }

// Phase 1 後 (warm only)
"triggers": {
  "crons": [
    "52 17 * * *",  // Neon pre-wake NAR
    "22 0 * * *",   // Neon pre-wake JRA
  ],
}

// Phase 3+ (warm + predict)
"triggers": {
  "crons": [
    "52 17 * * *",  // Neon pre-wake NAR
    "22 0 * * *",   // Neon pre-wake JRA
    "0 18 * * *",   // predict NAR+Ban-ei
    "30 0 * * *",   // predict JRA
  ],
}
```

---

## 8. pilot の具体コマンド / 観測項目

### PHASE 1 観測コマンド

```sh
# wrangler tail でリアルタイム監視
bunx wrangler tail finish-position-cron --format=pretty

# warm cron 手動テスト (wrangler dev で local mock)
bunx wrangler dev --test-scheduled
curl "http://localhost:8787/cdn-cgi/handler/scheduled?cron=52+17+*+*+*"
# ログに "Neon warm: SELECT 1 OK" が出ることを確認
```

### PHASE 2 パイロット観測コマンド

```sh
# Neon branch の DATABASE_URL を取得後
# (Neon console → Branch → Connection string)

# pilot: NAR category, Neon branch, 手動 trigger
curl -X POST https://finish-position-cron.<subdomain>.workers.dev/run \
  -H "Authorization: Bearer $TRIGGER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"runDate":"20260619","category":"nar","neonUrl":"<branch_url>"}'
# NOTE: neonUrl override を /run handler が受け付ける拡張が必要

# wrangler tail でリアルタイム監視 (ログに progress line + result JSON が流れること)
bunx wrangler tail finish-position-cron --format=pretty

# 完了後 Neon branch の予測 rows 確認
# Neon console SQL editor または psql:
psql "<branch_url>" -c "
  SELECT model_version, COUNT(*) as rows, MAX(prediction_generated_at) as latest
  FROM race_finish_position_model_predictions
  WHERE prediction_generated_at > NOW() - INTERVAL '2 hours'
  GROUP BY model_version;"

# D1 audit 確認
bunx wrangler d1 execute finish-position-cron-db --remote \
  --command "SELECT run_date, status, races_predicted, duration_ms, error
             FROM finish_position_cron_executions
             ORDER BY recorded_at DESC LIMIT 5;"
```

### go/no-go チェックリスト (PHASE 2)

```
□ NAR category で ≥3 run を実施
□ 全 run: SIGTERM なし (wrangler tail に "onStop" / "SIGTERM" ログなし)
□ 全 run: racesPredicted > 0 (レース存在日に実施)
□ 経過時間: ≤ 3 min per run
□ Mac との parity:
    □ 同日 Mac run の predictions と行数一致
    □ 同日 Mac run の top-1 rank (predicted_rank=1 の ketto) が一致
□ Neon branch に E-top2 rows なし (NAR は E-top2 無関係)
□ 連続 run でも UPSERT が冪等(行数変化なし)
□ Neon idle timeout による mid-run 接続切断なし
```

---

## 9. 既存 doc (01 / 03) への更新追記指示

### 01-current-mechanism.md への追記

Section 6 (Known Constraints) の以下コメントを更新:

> **従来**: "90–110s reap timeout: Incompatible with 210+ s... BLOCKER."
>
> **更新**: "現在の CF docs では 10 m idle timeout + renewActivityTimeout() による
> explicit keepalive + 15 m graceful-stop が documented。held request + renewActivityTimeout
> 設計により per-category (~1–2.5 min) は問題なし。03-architecture-design.md §1.4 および
> 04-off-mac-migration-plan.md §2 参照。PHASE 2 pilot で実測確認後に確定。"

Section 7 (Recommendation) の末尾に追記:

> **2026-06-19 更新**: off-Mac migration 設計が 04-off-mac-migration-plan.md に完成。
> Neon pre-wake cron (PHASE 1) を先行 deploy、続けて held-request pilot (PHASE 2) を実施。
> Mac launchd は pilot 完了まで authority を維持。

### 03-architecture-design.md への追記

Section 5 (Feasibility verdict) の Recommendation に:

> **2026-06-19 更新**: migration plan が 04-off-mac-migration-plan.md に完成。
> §5 の pilot フローを具体化した。PHASE 1 (pre-wake) → PHASE 2 (pilot NAR) →
> PHASE 3 (dual-run) → PHASE 4 (cutover) の順で実施。
> Mac launchd は cutover 確認まで authority を維持。

---

## 10. hard rules の確認

| ルール                                                | 本設計での扱い                                               |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| bun/bunx のみ                                         | Worker 実装は bun。Python container は uv run のまま         |
| コード(.ts/.py/wrangler.jsonc)は編集しない            | 本 doc は設計のみ。コード変更なし                            |
| git push 禁止                                         | doc commit のみ。push なし                                   |
| `--no-verify` 禁止                                    | commit 時に lefthook 通過                                    |
| DELETE/TRUNCATE/DROP 禁止                             | 本設計に破壊的操作なし                                       |
| creds mask                                            | 本 doc に credentials 記載なし                               |
| GitHub workflow 禁止                                  | スケジュールは CF Cron Trigger のみ                          |
| e84b078 の Neon cost 削減を regress しない            | predict cron 以外の時間帯は warm も predict も発火しない設計 |
| Mac launchd を authority として維持(cutover 確認まで) | PHASE 1-3 は Mac 継続、PHASE 4 で降格                        |

---

## 11. 不都合な真実(楽観で塗らない)

1. **Neon direct feature-build cost は増加する。** Mac は local Colima PG を
   feature build に使っていたが CF には local PG がない。NAR+JRA 各 ~2–2.5 min の
   Neon active compute が日次で追加される。e84b078 より前の水準に戻る可能性がある。
   pilot の Neon dashboard 確認が必須。

2. **pilot が失敗する可能性がある。** doc 03 §1.4 の通り、6/3 の 90s reap は
   empirically 観測された事実であり、現 docs は「解消した」と明示してはいない
   (「新しい contract は 10 m idle timeout」とは書いている)。held request でも
   何らかの residual 制約があれば失敗する。その場合は per-venue chunking を試みるか、
   Mac launchd 継続を選択する。

3. **Queue + DO + Container は Mac launchd より複雑。** 障害モードが増える。
   Queue message が DLQ に落ちた場合の manual recovery 手順、Container DO が
   stuck した場合のリセット手順、wrangler tail の監視をどう自動化するか、
   などの運用設計が未整備。

4. **race-hours re-predict (intra-day) は本設計の scope 外。** Mac の
   `race-prediction-guard` launchd (JST 10:00–20:40, 20 min interval) に相当する
   CF Cron Trigger は本 plan では PHASE 3 以降の扱い。
   cutover 後も intra-day freshness が必要な場合は追加設計が必要。

5. **E-top2 の JRA leg が最も重い (~2.5 min)。** JRA は NAR より重く、
   E-top2 enabled で XGB + CB の両方をロードして score する。pilot では
   必ず JRA + E-top2 を含む run を実施して wall-clock を確認すること。
