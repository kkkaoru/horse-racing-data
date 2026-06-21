# 07. CF Containers / Queues 公式仕様 + per-race 着順予測 sizing

本書は **Cloudflare 公式 docs を唯一の正** として、「本番でレースごとに着順予測を回す」最適構成と
sizing を確定する。推測は使わず、全数値に情報源 URL を付す。公式に記載がない項目は
「**公式に記載なし**」と明記する。調査日 = 2026-06-19。

> 既存ピロット (`05-pilot-results.md`) は `standard-4` (12 GiB → DuckDB 実効 ~7.4 GiB) で
> head-to-head layer が DuckDB OOM。Phase 4 でメモリ規律 + クエリ最適化により 6 GiB limit /
> 4 threads / temp spill で **788s 完走 (h2h 通過)** を達成済み。本書はこの実測を公式仕様に
> 照らして「instance type / 並列構成 / 実装最短ルート」へ落とす。

---

## 0. 結論 (先に要点)

| 問い                         | 公式根拠に基づく結論                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **推奨 instance type**       | **`standard-4` (4 vCPU / 12 GiB / 20 GB disk)** = 唯一 DuckDB ~8 GiB 集計を賄える最大 tier。custom でも上限が standard-4 と同一 (max 12 GiB / 4 vCPU) のため、これ以上は account team 申請が必要。                                                                                                                                                                                                                                                          |
| **per-race vs per-category** | **per-category (現行) を維持**。per-race fan-out は「同一 21y base build を races 回繰り返す」ため Neon scan と起動コストが線形に膨らみ、**CF の設計思想 (重い stateful 処理 1 本 = held-fetch + DO) に per-category が適合**。per-race の利点 (小メモリ・並列) は本ワークロードの律速 (共有 base build) を解かない。                                                                                                                                       |
| **15 分枠は障害か**          | **No**。15 分上限は Queue consumer / Cron / DO alarm の **Worker invocation** 側のみ。**DO held-fetch (container fetch) は wall-time 無制限** (caller 接続中は DO がアクティブ)。よって long-running container 処理は held-fetch で実行し、Queue consumer はそれを await するだけで 15 分に縛られない設計が公式 idiom。ただし consumer 自体は 15 分で切れるため、**consumer は「キック + DO 状態 poll」に留め、重処理は DO 側 held-fetch に置く**のが安全。 |
| **過去の "90-110s reap"**    | **現行公式仕様には存在しない**。Cloudflare は「特定時間で instance を能動停止しない」と明記。停止トリガは (a) `sleepAfter` idle timeout、(b) `stop()`/`destroy()`、(c) host 再起動時の SIGTERM→15min→SIGKILL、(d) **OOM 時の restart**、(e) rollout 時の SIGTERM→15min→SIGKILL のみ。現行 `wrangler.jsonc` のコメント (L12-14) は **古い記述で要更新**。                                                                                                    |
| **OOM 公式挙動**             | OOM 時 instance は **OOM error を throw して restart**される (swap 非使用)。クラッシュではなく再起動。よって DuckDB memory_limit を host (12 GiB) より十分下に規律しないと restart ループになる。                                                                                                                                                                                                                                                           |

---

## 1. Container Instance Types (公式値)

出典: <https://developers.cloudflare.com/containers/platform-details/limits/>
(同値が 2025-10-01 / 2026-01-05 changelog にも掲載)

| Instance Type | vCPU | Memory  | Disk  |
| ------------- | ---- | ------- | ----- |
| `lite`        | 1/16 | 256 MiB | 2 GB  |
| `basic`       | 1/4  | 1 GiB   | 4 GB  |
| `standard-1`  | 1/2  | 4 GiB   | 8 GB  |
| `standard-2`  | 1    | 6 GiB   | 12 GB |
| `standard-3`  | 2    | 8 GiB   | 16 GB |
| `standard-4`  | 4    | 12 GiB  | 20 GB |

- `dev` / `standard` は後方互換 alias (= `lite` / `standard-1`)。
- `instance_type` の既定は `"lite"`
  (出典: <https://developers.cloudflare.com/workers/wrangler/configuration/#containers>)。
- **`standard-4` の実 memory は 12 GiB**。これは firecracker VM に与えられる総量であり、
  OS + Python + DuckDB metadata の overhead を差し引いた **DuckDB 実効使用可能量は ~7-8 GiB**
  (ピロットで `7.4 GiB/7.4 GiB used` OOM を実測)。公式は「container 内訳」を保証しないため、
  DuckDB `memory_limit` は **host 総量の ~50% (= 6 GiB) に明示設定し、超過分は temp spill** が安全
  (Phase 4 採用、`memory_budget_kernel_panic` の教訓と整合)。

### Custom Instance Types

出典: <https://developers.cloudflare.com/containers/platform-details/limits/#custom-instance-types>
（有効化: 2026-01-05 全ユーザー解禁
<https://developers.cloudflare.com/changelog/post/2026-01-05-custom-instance-types/>）

指定法 (Wrangler): `instance_type = { vcpu = 2, memory_mib = 6144, disk_mb = 12000 }`
(出典: <https://developers.cloudflare.com/workers/wrangler/configuration/#custom-instance-types>)

| Resource         | Limit                         |
| ---------------- | ----------------------------- |
| 最小 vCPU        | 1                             |
| **最大 vCPU**    | **4**                         |
| **最大 Memory**  | **12 GiB**                    |
| 最大 Disk        | 20 GB                         |
| Memory / vCPU 比 | 最小 3 GiB / vCPU             |
| Disk / Memory 比 | 最大 2 GB disk / 1 GiB memory |

> **重要**: custom でも上限は standard-4 と同一 (4 vCPU / 12 GiB / 20 GB)。
> 「standard-4 より大きい memory」は custom では作れない。
> より大きい instance / account limit が必要なら **account team / support ticket / 申請フォーム**経由
> (出典: limits ページ末尾 "If you need larger instance sizes ... contact your account team")。
> → 本ワークロード (DuckDB ~8 GiB pairwise + per-race 小集計) は **standard-4 が CF の天井**。
> これを満たせない場合は Mac launchd authority 維持か、メモリ規律 (Phase 4) で収めるしかない。

### Sizing 判断: どの instance type が適切か

| 処理                                                         | メモリ特性           | 適切な tier                                                                           |
| ------------------------------------------------------------ | -------------------- | ------------------------------------------------------------------------------------- |
| per-category full build (21y base + 14 layer + h2h pairwise) | DuckDB peak ~6-8 GiB | **`standard-4` 必須** (12 GiB host − overhead で 6 GiB limit + spill)                 |
| per-race の小集計のみ (base を共有前提)                      | 数百 MB              | `standard-1`/`standard-2` で足りるが、base build を共有できないと意味がない (§6 参照) |

---

## 2. 実行時間・ライフサイクル (公式)

### sleepAfter / renewActivityTimeout / lifecycle hooks

出典: <https://developers.cloudflare.com/containers/container-class/>

- **`sleepAfter`**: 既定 `"10m"`。`"30s"`/`"5m"`/`"1h"` 等の duration 文字列または秒数。
  「activity (incoming request) が無いまま経過したら container を停止」。**公式の最大値の記載なし**
  (= 上限の明文規定は **公式に記載なし**)。activity があるたびタイマーはリセット。
- **`renewActivityTimeout()`**: `sleepAfter` タイマーを手動リセット。
  「incoming request は自動でリセットするが、**background work / long-running operation** からは
  手動で呼んで activity 扱いにし sleep を防げ」と明記 → **held-fetch 中の keepalive idiom そのもの**。
- **`onActivityExpired()`**: `sleepAfter` 失効時に発火。override したら `await this.stop()`/`destroy()` を
  必ず呼ばないと sleep しない (タイマー renew され再発火)。
- **`stop(signal?)`**: 既定 `SIGTERM`。graceful shutdown を促し `onStop()` を発火。
- **`destroy()`**: 即時 kill。
- **`getState()`**: `'running' | 'healthy' | 'stopping' | 'stopped' | 'stopped_with_code'`。
  `running` = 起動中で health check 未通過、`healthy` = 受付可能。

### incoming request / DO の wall-time (核心)

出典: <https://developers.cloudflare.com/workflows/reference/limits/>
(「Wall time limits by invocation type」表)

| Invocation type                  | Wall time limit | 補足                                                                                                                                              |
| -------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Incoming HTTP request            | **Unlimited**   | client 接続中はハード上限なし。response body を stream 中も active。`waitUntil()` は応答/切断後 +30s。                                            |
| Cron Triggers                    | **15 minutes**  | scheduled Worker の上限。                                                                                                                         |
| Queue consumers                  | **15 minutes**  | 各 consumer invocation の上限。                                                                                                                   |
| Durable Object alarm handlers    | **15 minutes**  | alarm handler の上限。                                                                                                                            |
| **Durable Objects (RPC / HTTP)** | **Unlimited**   | **caller が接続している限りハード上限なし。request / RPC / response stream / WebSocket / pending I/O のいずれかが in-flight な間 DO は active。** |

> **per-request (held-fetch) で長時間処理する公式 idiom**:
> `Container extends DurableObject`。Worker → DO.fetch(held request) → container 内 pipeline。
> **DO への held-fetch は wall-time 無制限**であり、`renewActivityTimeout()` を background から
> 周期呼び出しして `sleepAfter` の idle reap を防ぐ (= keepalive)。
> ピロット Phase 3 で NDJSON ~10s 毎 progress により reap なし完走を実証済み。

### container 起動レイテンシ / cold start

出典: <https://developers.cloudflare.com/containers/faq/>

- cold start = 「完全停止状態から起動」。**典型 1-3 秒** (image size / コード実行に依存)。
- 「最初の新規 path への request は新 container boot のため遅い」
  (出典: get-started)。

### 最大 lifetime

出典: <https://developers.cloudflare.com/containers/faq/>

- **「Cloudflare は特定時間経過後に container instance を能動的にシャットダウンしない」**
  = max runtime のハード規定なし。
- ただし「`sleepAfter` を設定せず stop もしなければ、**host server が再起動するまで**走り続ける」
  = host 再起動という非決定的イベントで止まりうる。
- **「Cloudflare はいかなる instance も一定時間動く保証をしない」** (uptime 保証なし)。

---

## 3. 並列・スケール (公式)

### max_instances

出典: <https://developers.cloudflare.com/workers/wrangler/configuration/> (containers)

- `max_instances`: 「同時に running な container instance の最大数。**stopped はカウントしない**
  — overall では超過しうるが、**同時 running はこの数まで**。これを超える start request は error」。
- **既定 20**。**本番でのみ enforce** (local dev では超過可)。**公式の最大値の記載なし**
  (account 全体の concurrent resource limit が実質上限 → §3 account limits)。

### Account limits (per account, 2026 値)

出典: <https://developers.cloudflare.com/containers/platform-details/limits/> ("Account limits")

| Resource                          | Limit                |
| --------------------------------- | -------------------- |
| Concurrent memory                 | **6 TiB**            |
| Concurrent vCPU                   | **1,500**            |
| Concurrent disk                   | **30 TB**            |
| Image size                        | instance disk と同じ |
| **Total image storage / account** | **50 GB**            |

> 6 TiB / 12 GiB = ~512 standard-4 を同時 running 可能 (account 上限側)。
> 着順予測 (高々 3 カテゴリ × 数日 = 数 instance) は account limit に対して桁違いに小さい。
> → **並列スケールは account 制約ではなく Neon scan コストとロジック律速で決まる**。

### 「多数の小さな per-request job」を回す公式の推奨

出典: get-started (<https://developers.cloudflare.com/containers/get-started/>)、
container changelog (Queue integration)、Container class。

公式が提示する routing パターンは 2 系統:

1. **stateful / per-id routing**: `getContainer(env.MY_CONTAINER, id)` で **id 毎に専用 instance**。
   同一 id への request は同一 instance に集約。session / 重い stateful 処理向き。
2. **stateless load-balance**: `getRandom(env.MY_CONTAINER, N)` で N instance に分散。
   独立した stateless job 向き。

→ Queue で fan-out する場合、各 message から `getContainer(env.X, perMessageId)` を呼べば
**message 毎に container 並列**が作れる (公式 examples に Queue integration あり)。
ただし「**per-message = 独立で軽い stateless job**」が前提。着順予測の per-race は
**共有 21y base build に依存**するため、この前提を満たさない (§6)。

---

## 4. Queues 仕様 (公式)

設定: <https://developers.cloudflare.com/workers/wrangler/configuration/> (queues.consumers)
limits: <https://developers.cloudflare.com/queues/platform/limits/>
concurrency: <https://developers.cloudflare.com/queues/configuration/consumer-concurrency/>
JS API: <https://developers.cloudflare.com/queues/configuration/javascript-apis/>

| 項目                               | 既定                        | 最大 (公式)                                           |
| ---------------------------------- | --------------------------- | ----------------------------------------------------- |
| `max_batch_size`                   | (未設定可)                  | **100 messages**                                      |
| `max_batch_timeout`                | (未設定可)                  | **60 seconds**                                        |
| `max_retries`                      | (未設定可)                  | **100** (message retries 上限)                        |
| `max_concurrency`                  | 未設定=自動上限までスケール | **250** (concurrent consumer invocations, push-based) |
| message size                       | —                           | **128 KB**                                            |
| **consumer duration (wall clock)** | —                           | **15 minutes**                                        |
| per-queue throughput               | —                           | **5,000 msg/sec**                                     |
| per-queue backlog                  | —                           | **25 GB**                                             |
| message retention                  | —                           | configurable **14 日**まで (Free は 24h 固定)         |
| queues / account                   | —                           | **10,000**                                            |

- `dead_letter_queue`: `max_retries` 回失敗後の送り先。未定義なら反復失敗 message は **破棄**。
  指定 queue が無ければ自動作成。
- **delivery semantics**: at-least-once。batch は `queue()` 戻り (promise resolve / waitUntil resolve) 後に
  自動 ack。`message.ack()` / `message.retry(opts)` / `batch.retryAll(opts)` で明示制御。
  **`retry()`/`retryAll()` は failed invocation に数えない** → retry は scale-down を誘発しない。
- **consumer concurrency autoscaling**: backlog 量と増加率・失敗比率・`max_concurrency` で自動調整。
  最大 **250 並列**。スケール判定は「**1 batch 処理完了後**」に行われる (in-flight batch は判定に寄与しない)。
  `max_concurrency=1` で autoscale 無効。

### per-race fan-out に適すか

- **機構としては適合**: per-race を 1 message 化し `max_batch_size=1` にすれば、Queue が
  最大 250 並列まで consumer を自動スケール → message 毎に `getContainer(perRaceId)` で container 並列可。
- **しかし本ワークロードでは非効率** (§6): 各 race の予測は **21y base build を共有**する。
  per-race にすると base build が race 回数だけ重複 → Neon scan が線形膨張。
  Queue の at-least-once + 自動 ack + DLQ は per-category (カテゴリ単位 message) でも十分活用できる。

---

## 5. ハングアップ / reap / OOM の公式見解

### 過去 "90-110s reap" は何に置き換わったか

**現行公式仕様に "90-110s で batch instance を reap" という挙動は存在しない。**
公式 (faq) は明確に「**Cloudflare は特定時間経過後に container を能動シャットダウンしない**」と述べる。
停止が起きる条件は以下のみ:

| 停止トリガ                | タイミング / 挙動                                                                                         | 出典                       |
| ------------------------- | --------------------------------------------------------------------------------------------------------- | -------------------------- |
| `sleepAfter` idle timeout | activity 無で経過 → `onActivityExpired()` → 既定 `stop()`。**held-fetch + renewActivityTimeout で防止可** | container-class            |
| `stop()` / `destroy()`    | 明示停止 (SIGTERM / 即時 kill)                                                                            | container-class            |
| host server 再起動        | **SIGTERM → 15 分後 SIGKILL** (cleanup 猶予 15 分)                                                        | faq                        |
| **OOM**                   | **OOM error throw → instance restart** (swap 非使用)                                                      | faq                        |
| rollout (deploy)          | `rollout_active_grace_period` 経過後 **SIGTERM → 15 分 → SIGKILL**                                        | rollouts / wrangler config |

> ピロットの古い `wrangler.jsonc` コメント (L12-14「reaps batch instances at ~90-110s regardless
> of sleepAfter」) は **現行公式仕様と矛盾**。実測 (Phase 2-4) でも reap/SIGTERM は **一度も観測されず**、
> 788s / 471s / 742s の held-fetch が全て完走。→ **このコメントは更新すべき** (本書を参照)。
> 「90-110s」は inbound HTTP を持たない batch container が短時間で idle reap された旧挙動の
> 記憶と推測されるが、**held-fetch + renewActivityTimeout を使う現設計では該当しない**。

### 長時間ビルドが途中で殺される条件 (公式)

- **idle timeout (`sleepAfter`)**: held-fetch + `renewActivityTimeout()` 周期呼出しで回避 (実証済み)。
- **max lifetime**: 公式上ハード上限なし (host 再起動という非決定イベントのみ)。
- **OOM**: host メモリ (instance type の memory) 超過で **restart**。← **本ワークロードの実際のブロッカー**。
- **deploy/rollout**: 進行中 build が SIGTERM される。15 分以内に graceful 終了が必要。
  本番は deploy 頻度が低いので実害小。`rollout_active_grace_period` で猶予延長も可。

### OOM 時の container 挙動 (公式)

出典: <https://developers.cloudflare.com/containers/faq/>

> **「If you run out of memory, your instance will throw an Out of Memory (OOM) error and will be
> restarted.」** / **「Containers do not use swap memory.」**

- = **クラッシュして即死ではなく restart**。ただし処理途中の状態は失われ、held-fetch は中断される。
- swap が無いため、**DuckDB が host memory を超えた瞬間に OOM**。Mac (Colima 24 GiB) で通っても
  CF (12 GiB 固定) で OOM するのはこのため。
- 対策は instance type 引き上げ (= standard-4 が天井) か、**DuckDB memory_limit を host の ~50% に
  規律 + temp spill** (Phase 4 で実証: 6 GiB limit / 4 threads / spill で 788s 完走)。

---

## 6. per-race vs per-category の sizing 判断 (公式根拠付き)

### 2 案の定義

| 案               | 単位                       | 1 job の所要 / メモリ           | message 数/日 |
| ---------------- | -------------------------- | ------------------------------- | ------------- |
| (a) per-category | カテゴリ (jra/nar/ban-ei)  | ~6-13 分 / DuckDB peak ~6-8 GiB | 1-3           |
| (b) per-race     | レース (1 開催 30-60 race) | 数秒〜十数秒 / 小メモリ         | 数十〜100+    |

### 公式仕様に照らした判断

1. **CF の設計思想適合性**
   - CF Containers は「**CPU コア並列・大メモリ・大ディスクを要する resource-intensive な処理**を
     Worker から起動」する用途を第一に挙げる (containers top page)。
   - per-category の **DuckDB pairwise 集計 (~8 GiB)** はまさにこの「resource-intensive」像に一致。
   - per-race の小集計は `lite`/`basic` でも回るが、CF が container を勧める理由 (重い処理) から外れ、
     むしろ Worker 単体や軽量処理向き。

2. **共有 base build による律速 (決定的)**
   - 着順予測は **21y race_history を Neon から scan して base feature store を build** する工程が
     最も重い (CLI mode で base build ~200-260s, h2h pairwise が peak メモリ)。
   - per-race にすると **この共有 base build が race 数 (30-60) 回重複**。
     → Neon compute scan が線形膨張し、`cloudflare_observability_cost` / Neon コストが激増。
     → **per-race fan-out は律速 (共有 build) を解かず、むしろ悪化させる**。
   - per-category は base build を 1 回で共有 → Neon scan 1 回 / カテゴリ。

3. **15 分枠との整合**
   - per-category の held-fetch は **DO wall-time 無制限**で実行できる (§2)。
     Phase 4 実測 788s (~13.1 分) は **Queue consumer の 15 分枠にも収まる**が、
     consumer を「キック専用」にすれば 15 分制約自体が無関係になる (held-fetch は DO 側で無制限)。
   - per-race は各 job が短く 15 分に余裕だが、**job 数が多く Queue 並列で base build を多重起動**すると
     同時 DuckDB instance が account/Neon を圧迫 (`memory_budget_kernel_panic` 教訓: メモリ
     oversubscription は危険)。

4. **OOM 耐性**
   - どちらの案でも個々の container は standard-4 12 GiB 固定。per-race でも base build を含めば
     同じ OOM に当たる。**base build を含まない pure per-race 推論は小メモリで安全**だが、
     それは「base build を別工程で 1 回だけ実行し、結果 (parquet) を R2 共有」する前提
     (= 実質 per-category build + per-race score の 2 段) でしか成立しない。

### 結論

- **per-category (現行 Queue: category 単位 message, `max_batch_size=3`) を維持・推奨**。
  公式仕様 (resource-intensive 用途 / held-fetch 無制限 / OOM restart / Neon コスト) のいずれも
  per-category を支持。per-race fan-out は「軽い独立 stateless job」前提を満たさず、共有 base build を
  多重化して律速を悪化させる。
- **どうしても per-race 並列の利点 (障害分離・部分再試行) が欲しい場合の唯一の合理形** =
  「**build フェーズ (per-category, standard-4) → R2 に feature parquet 共有 → score フェーズ
  (per-race, 軽量 instance, Queue fan-out)**」の 2 段 pipeline。現行 `rescore` 構想
  (FEATURES_CACHE R2) がこれに近い。ただし日次 full には不要なオーバーエンジニアリング。

---

## 7. 推奨構成 (公式根拠 + 実装最短ルート)

### 推奨 instance type

**`standard-4` (4 vCPU / 12 GiB / 20 GB disk)** — DuckDB ~8 GiB 集計を賄える CF の最大 tier。
custom でも上限同一のため、これ以上はメモリ規律 (Phase 4) で収めるか account team 申請。
DuckDB は **`memory_limit` = host の ~50% (≈6 GiB) + `threads`=4 + `temp_directory` spill** を必須設定
(Phase 4 採用済 / `memory_budget_kernel_panic` 整合)。

### 推奨並列構成

```
Cron Trigger (warm/keepalive のみ Worker, <1s)
        │
        ▼
Queue producer ──► finish-position-predict-queue
        │            (message = category, max_batch_size=3, max_batch_timeout=5,
        │             max_retries→DLQ=finish-position-predict-dlq, at-least-once)
        ▼
Queue consumer (Worker, 15min 枠) ── 「キック + DO 状態 await」に専念
        │   message.ack() は held-fetch 完了後
        ▼
PredictRunCoordinator DO  ─ 強整合 dedup/state (KV→DO 移行済, commit 34b75b7)
        │
        ▼
FinishPositionPredictContainer (DO held-fetch, wall-time 無制限)
        │   standard-4 / sleepAfter 長め / renewActivityTimeout を ~10s 毎 (keepalive)
        ▼
   container 内 pipeline: base build → 14 layer → h2h(target_races filter) → score → Neon UPSERT
```

- **per-category** (現行 message 設計を維持)。`max_instances` は 3 (= 3 カテゴリ同時) で十分。
- **held-fetch keepalive**: `renewActivityTimeout()` を background から ~10s 毎
  (NDJSON progress と連動、Phase 3 実証済)。これで `sleepAfter` idle reap を完全回避。
- **OOM 規律**: DuckDB `memory_limit` 6 GiB / `threads` 4 / temp spill (Phase 4)。
- **DLQ**: `finish-position-predict-dlq` 維持。at-least-once + dedup DO で二重 UPSERT を防止。
- **observability**: `head_sampling_rate: 0.1` 維持 (`cloudflare_observability_cost` 教訓)。

### 実装最短ルート (read+doc のみ、コード変更は別タスク)

現行 `apps/finish-position-cron/` は **構成上ほぼ完成**しており、残りは Phase 4 commit の deploy のみ:

1. **`wrangler.jsonc` L12-14 の "90-110s reap" コメントを更新** (本書 §5 を参照: 現行仕様では reap なし)。
   → instance_type=`standard-4` / max_instances=3 / Queue 設定はそのまま正しい。
2. **Phase 4 (DuckDB memory 規律 + h2h target_races filter) を main に取り込み → CF deploy**。
3. CF 本番 pilot で `racesPredicted>0` を read-only でない production Neon secret で確証。
4. JRA / Ban-ei カテゴリ smoke。Neon コスト実測。
5. 問題なければ predict cron (`*/20 1-11` 等) を有効化し Mac launchd から cutover。
   ※ それまで **Mac launchd authority 維持** (`finish_position_local_cron_macos_launchd`)。

→ **per-race への作り替えは不要・非推奨**。最短ルートは「現行 per-category 構成 + Phase 4 OOM 修正の
deploy」であり、公式仕様 (held-fetch 無制限 / standard-4 が天井 / OOM restart / per-category 適合) と
完全に整合する。

---

## 付録: 主要出典 URL 一覧

- Instance types / account limits / custom limits:
  <https://developers.cloudflare.com/containers/platform-details/limits/>
- Custom instance types 解禁 (2026-01-05):
  <https://developers.cloudflare.com/changelog/post/2026-01-05-custom-instance-types/>
- Larger instance types (2025-10-01):
  <https://developers.cloudflare.com/changelog/post/2025-10-01-new-container-instance-types/>
- Container class (sleepAfter / renewActivityTimeout / stop / onActivityExpired / getState):
  <https://developers.cloudflare.com/containers/container-class/>
- Container FAQ (cold start / OOM restart / max lifetime / SIGTERM 15min):
  <https://developers.cloudflare.com/containers/faq/>
- Rollouts (SIGTERM→15min→SIGKILL / rollout_step_percentage / grace period):
  <https://developers.cloudflare.com/containers/platform-details/rollouts/>
- Wall time limits by invocation type (DO/HTTP=Unlimited, Queue/Cron/Alarm=15min):
  <https://developers.cloudflare.com/workflows/reference/limits/>
- Wrangler config (containers / max_instances / instance_type / queues / limits):
  <https://developers.cloudflare.com/workers/wrangler/configuration/>
- Queues platform limits (batch 100 / timeout 60s / retries 100 / concurrency 250 / consumer 15min):
  <https://developers.cloudflare.com/queues/platform/limits/>
- Queue consumer concurrency autoscaling (max 250, batch 完了後判定):
  <https://developers.cloudflare.com/queues/configuration/consumer-concurrency/>
- Queue JS API (ack / retry / retryAll / at-least-once):
  <https://developers.cloudflare.com/queues/configuration/javascript-apis/>
- Containers top (resource-intensive 用途 / Region:Earth):
  <https://developers.cloudflare.com/containers/>
- Get started (routing: getContainer per-id / getRandom load-balance):
  <https://developers.cloudflare.com/containers/get-started/>
  </content>
  </invoke>
