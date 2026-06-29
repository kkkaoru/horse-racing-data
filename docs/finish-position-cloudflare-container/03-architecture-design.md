# Finish-position prediction on Cloudflare Worker Container — architecture design

> **Superseded production note (2026-06-29):** Current production authority is
> Cloudflare-only. Mac launchd/local Docker references below describe the
> historical pre-cutover path and pilot comparison data; they are not production
> scheduler, authority, fallback, or ordering dependency.

> **Scope.** Design-only (read-only). No deploy, no image push, no secrets, no
> wrangler config changes. This document records (1) the _current_ Cloudflare
> Containers / Queues / Durable Objects / Workflows capabilities verified against
> the docs, (2) a chunked architecture that makes the ~3.5 min daily predict
> batch reliable on CF, and (3) an honest feasibility verdict + recommendation.
>
> **Date:** 2026-06-18. Sources: Cloudflare docs MCP (`/containers/*`,
> `/queues/*`, `/durable-objects/*`, `/workflows/*`, `/workers/wrangler/configuration`),
> cross-checked against the changelog. The repo memory
> `feedback_cloudflare_containers_90s_reap` (2026-06-03 incident) is **partially
> superseded** by current platform behavior — see §1.4.

---

## 0. The problem in one paragraph

The historical daily UPCOMING-race finish-position predictor (`predict_upcoming.py`) builds
a DuckDB+Postgres feature parquet and scores CatBoost/XGBoost models for three
categories (JRA / NAR / Ban-ei), writing ~525 predictions in **~3.5 min** as a
single batch. At the time of this design it ran on a **Mac launchd → local docker → Neon**
schedule (`scripts/launchd/com.kkk4oru.finish-position-predict.plist`,
JST 03:00), now superseded by Cloudflare-only production. A 2026-06-03 attempt to run it as a Cloudflare Container cron failed
because the container appeared to be reaped at ~90–110 s with no inbound HTTP
traffic — silent failure, partial traceback. E-top2 (JRA CatBoost **+** XGBoost,
task #39) makes the JRA leg even slower. The goal: can we make CF Containers a
reliable, fast-enough replacement, scheduled by a **Cloudflare Cron Trigger**
(GitHub Workflows forbidden for prediction)?

---

## 1. Current Cloudflare capability findings (verified 2026-06-18)

### 1.1 Container instance types (memory / CPU / disk)

Predefined types (changelog 2025-10-01, limits page confirmed current):

| Instance Type  | vCPU  | Memory     | Disk      |
| -------------- | ----- | ---------- | --------- |
| lite (`dev`)   | 1/16  | 256 MiB    | 2 GB      |
| basic          | 1/4   | 1 GiB      | 4 GB      |
| standard-1     | 1/2   | 4 GiB      | 8 GB      |
| standard-2     | 1     | 6 GiB      | 12 GB     |
| standard-3     | 2     | 8 GiB      | 16 GB     |
| **standard-4** | **4** | **12 GiB** | **20 GB** |

Custom instance constraints: min 1 vCPU, max 4 vCPU; max 12 GiB memory; max 20 GB
disk; **min 3 GiB memory per vCPU**. So `standard-4` (4 vCPU / 12 GiB / 20 GB) is
the ceiling — same as today's `wrangler.jsonc`. Image storage cap: 50 GB/account.

### 1.2 Account-level concurrency (changelog 2026-02-25, "15x")

| Limit (concurrent live instances) | Current |
| --------------------------------- | ------- |
| Memory                            | 6 TiB   |
| vCPU                              | 1,500   |
| Disk                              | 30 TB   |

Translation: **>1,000 `standard-2` or hundreds of `standard-4` instances can run
concurrently.** Per-category fan-out (3 instances) or even per-venue fan-out
(~10–20 instances) is trivially within budget. `max_instances` in config defaults
to 20 and only caps _simultaneously running_ instances (stopped ones don't count;
a start that would exceed the cap errors).

### 1.3 How a Container is invoked from a Worker

- A Container is a **Durable Object** under the hood: the `@cloudflare/containers`
  `Container` class extends `DurableObject`. Config = `containers[]` +
  `durable_objects.bindings` + `migrations.new_sqlite_classes` (exactly the
  current `finish-position-cron/wrangler.jsonc` shape).
- **Batch / cron pattern (official):** the `scheduled()` handler calls
  `getContainer(env.BINDING, name).start({ envVars, entrypoint, enableInternet })`.
  `start()` launches the image's entrypoint and resolves once the container has
  _launched_ (not finished). This is exactly the documented **Cron Container**
  example (`/containers/examples/cron/`) and exactly what `worker.ts` does today.
- **Request pattern:** `getContainer(...).fetch(request)` / `containerFetch()` proxy
  an HTTP request to `defaultPort` and **block until the container is listening**.
- Cold start: **"often 1–3 s," image-size dependent.** Negligible vs a 3.5 min batch.
- Secrets reach the container as env vars via `start({ envVars })` /
  `wrangler secret put` (already wired: `NEON_DATABASE_URL`, `TRIGGER_TOKEN`).

### 1.4 Lifecycle / reap behavior — **the 90s memory is partially outdated**

Current docs (`/containers/container-class/`, `/containers/get-started/`,
`/workers/wrangler/configuration`) state:

- **`sleepAfter` default is `"10m"`** — "how long to keep the container alive
  **without activity** before shutting it down." Accepts `"30s"`, `"5m"`, `"1h"`,
  or a number of seconds. **Activity resets the timer.**
- **`renewActivityTimeout()` exists for exactly this case:** _"Incoming requests
  reset the timer automatically. **Call this manually from background work, such as
  a scheduled task or a long-running operation, that should count as activity and
  prevent the container from sleeping.**"_ The docs even ship a `processJobs()`
  example that calls `this.renewActivityTimeout()` per job.
- **`onActivityExpired()`** fires when `sleepAfter` elapses with no activity; the
  default implementation calls `stop()`. You can override it.
- **Graceful shutdown is 15 minutes:** on `SIGTERM` (rollout or stop) "the
  container … still has **15 minutes** to shut down before it is forcibly killed."

**Reconciliation with the 2026-06-03 incident.** The memory recorded a hard
~90–110 s SIGTERM for a container with _no inbound HTTP traffic_, not extendable
by `sleepAfter` or a DO keepalive. The _current_ documented model has **no such
90 s cap** — instead a 10 m idle timeout, an explicit keep-alive API for
background work, and a 15 m graceful-stop window. Three possibilities, in order of
likelihood:

1. **Platform changed.** Containers were newly GA in mid-2025; the 6/3 incident
   predates the 2026-02-25 "15x" capacity expansion and the maturing of the
   lifecycle API. The keep-alive semantics and `sleepAfter` defaults documented
   today are the contract now.
2. **The 6/3 keepalive was ineffective as built.** The repo's `container-class.ts`
   keepalive uses `this.schedule(30s, "keepalivePing")` → `containerFetch(/keepalive)`.
   DO alarms (`schedule`) can be delayed/coalesced, and a `containerFetch` that
   races the container's own startup/health-check can fail and _stop renewing_.
   The current idiom is the **synchronous `renewActivityTimeout()` called from a
   held request** (see §3), which the 6/3 design did not use.
3. **Health-check / port readiness killed it.** If the liveness server wasn't
   ready on `defaultPort` fast enough, the platform may have treated the instance
   as unhealthy. The current code does run an HTTP/1.1 socket on 8080.

**Design consequence:** we must **not** assume the old 90 s cap, **and** we must
**not** blindly trust that `start()`-to-completion alone survives — we treat the
container's liveness as something we actively own via a **held request +
`renewActivityTimeout()`** (the documented pattern), and we **structure the work
so any single unit finishes well inside a few minutes** regardless. The chunked
design below is robust to _either_ world: it works if the 90 s cap is gone, and it
_still_ works if some residual short-no-traffic reap exists, because each chunk is
driven by a held inbound request.

> **This must be re-measured before any cutover.** §6 lists the exact probe. The
> memory `feedback_cloudflare_containers_90s_reap` should be updated to
> "superseded — current docs document 10m idle / 15m graceful-stop / explicit
> keep-alive; re-measure" rather than treated as a hard blocker.

### 1.5 Cloudflare Queues

`queues.consumers[]` options (verified): `max_batch_size`, `max_batch_timeout`
(seconds to fill a batch), `max_retries`, `dead_letter_queue` (auto-created if
absent; messages discarded after max*retries if no DLQ), `max_concurrency` (max
concurrent consumer invocations; autoscales if unset), `retry_delay` (seconds,
overridable per-message/per-batch). Pull consumers can do 5,000 msg/s/queue. Our
volume is **3–20 messages/day** — Queues throughput is a non-issue; we use it
purely for \_durable fan-out + retry + DLQ*, not throughput.

### 1.6 Cloudflare Workflows (alternative orchestrator)

`WorkflowEntrypoint.run(event, step)` with `step.do(name, cfg, fn)` (durable,
resumes from last successful step on retry), `step.sleep` / `step.sleepUntil`, and
per-step retry config (default `limit:5, delay:10s, backoff:exponential,
timeout:"10 minutes"`; up to 10,000 retries/step). Python Workflows exist (beta,
needs `compatibility_date <= 2025-08-01`). Workflows give durable multi-step
orchestration **but cannot themselves run CatBoost/XGBoost** (Workers/Pyodide
runtime, native wheels unsupported per `reference_cloudflare_python_workers_available`),
so a Workflow would still have to _drive a Container_ for the actual compute. It is
a viable orchestrator substitute for the DO (§4) but adds a product surface.

### 1.7 Hyperdrive

Pools Postgres connections near the DB and caches queries; "uncached queries and
new database connections have up to **90% less latency**." For our feature build
(many round-trips through `postgres_scanner` to Neon) Hyperdrive can materially cut
wall-clock and reduce Neon compute — directly relevant to keeping each chunk
short. Caveat: DuckDB's `postgres_scanner` opens a raw libpq connection; Hyperdrive
fronts Postgres over its connection string, so the container would point
`NEON_DATABASE_URL` at the Hyperdrive connection string. (Compatibility to be
verified in the probe — DuckDB must accept the Hyperdrive endpoint as a normal PG
DSN.)

---

## 2. The workload is cleanly chunkable (verified in code)

`predict_upcoming.py` already isolates the unit of work:

- `main()` → `_resolve_categories(PREDICT_CATEGORIES env)` → loops
  `for category in categories: _predict_category(...)`. **`PREDICT_CATEGORIES` is
  an existing comma-separated allowlist env var** — so we can already start a
  container that predicts only `jra`, only `nar`, etc., with **zero code change**.
- `_predict_category()` = `build feature rows (DuckDB) → per-race score → chunked
UPSERT`. Per-category feature build + scoring is independent across categories.
- Writes are **idempotent UPSERTs** into `race_finish_position_model_predictions`
  (no DELETE/TRUNCATE), and a per-run audit row into `finish_position_cron_executions`.
  Re-running a category is safe.

**Chunk granularity options:**

| Granularity         | #units/day           | Est. per-unit time                                          | Notes                                                                                                      |
| ------------------- | -------------------- | ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Per-category        | 3 (jra, nar, ban-ei) | JRA ~1.5–2.5 min (E-top2 heavier), NAR ~1 min, Ban-ei <30 s | **Zero code change** (`PREDICT_CATEGORIES`). Each unit well under the 15 m graceful window.                |
| Per-venue (keibajo) | ~10–20               | ~10–60 s each                                               | Needs a new `PREDICT_KEIBAJO` filter in the feature build + scoring. Maximum safety margin; more messages. |

Per-category is the **recommended default**: it is the smallest change, each unit
is already <~2.5 min (comfortably inside the 15 m graceful-stop window and even
inside a conservative few-minute held-request budget), and the failure blast radius
is one category. Per-venue is the fallback lever if JRA-with-E-top2 ever creeps
toward the held-request budget — implement only if measured necessary.

---

## 3. Recommended architecture — Cron → Queue fan-out → Container consumer, DO-orchestrated

```
                          ┌──────────────────────────────────────────────┐
   Cloudflare Cron        │  finish-position-cron Worker                   │
   Triggers               │                                               │
   ┌───────────────┐      │  scheduled(event):                            │
   │ 0 18 * * *    │─────▶│   guard(event.cron) → resolve runDate (JST)   │
   │ (JST 03:00,   │      │   for cat in due(event):                      │
   │  NAR+Ban-ei)  │      │     RUN_DO.enqueueRun(runDate, cat)  ──────────┐
   ├───────────────┤      │                                               ││
   │ 30 0 * * *    │─────▶│  (JST 09:30 JRA, odds-freshness guard:        ││
   │ (JST 09:30,   │      │   skip if mirror not yet caught up)           ││
   │  JRA)         │      │                                               ││
   └───────────────┘      │  POST /run (Bearer TRIGGER_TOKEN) → same path ││
                          └───────────────────────────────────────────────┘│
                                                                            │ producer.send()
                                                                            ▼
                                              ┌──────────────────────────────────────┐
                                              │  Queue: finish-position-predict-jobs   │
                                              │  msg = { runId, runDate, category,     │
                                              │          attempt, modelVersion }       │
                                              │  max_batch_size=1, max_retries=3,      │
                                              │  retry_delay=120, max_concurrency=3,   │
                                              │  dead_letter_queue=...-dlq             │
                                              └──────────────────────────────────────┘
                                                                            │ queue() consumer
                                                                            ▼
   ┌──────────────────────────────────────────────────────────────────────────────────┐
   │  finish-position-cron Worker — queue(batch) consumer                                │
   │  for msg in batch:                                                                  │
   │    RUN_DO.markChunkStarted(runId, category)                                         │
   │    c = getContainer(env.FP_CONTAINER, `run:${runId}:${category}`)  // 1 DO/chunk    │
   │    // HELD REQUEST drives liveness; DO calls renewActivityTimeout() per heartbeat   │
   │    resp = await c.fetch(`/predict?category=${category}&runDate=${runDate}`)         │
   │    if resp.ok && body.racesPredicted>0: RUN_DO.markChunkDone(...) ; msg.ack()       │
   │    else: msg.retry()   // → backoff → DLQ after max_retries                         │
   └──────────────────────────────────────────────────────────────────────────────────┘
                                                                            │ c.fetch (held)
                                                                            ▼
   ┌──────────────────────────────────────────────────────────────────────────────────┐
   │  FinishPositionPredictContainer  (Durable Object, standard-4: 4 vCPU/12GiB/20GB)    │
   │  - long-lived HTTP server on :8080 with GET /predict                                │
   │  - on /predict: set PREDICT_CATEGORIES=<category>, run _predict_category() inline,  │
   │    stream periodic chunk-progress so the request stays open the whole time          │
   │  - DO side: renewActivityTimeout() on each progress chunk → never idle-reaped       │
   │  - feature build via DuckDB → Neon (optionally through Hyperdrive)                   │
   │  - models BAKED into image (no runtime R2 dep) — unchanged                          │
   │  - idempotent UPSERT → Neon race_finish_position_model_predictions                  │
   │  - writes per-chunk audit row → finish_position_cron_executions                     │
   └──────────────────────────────────────────────────────────────────────────────────┘
                                                                            ▲
   ┌──────────────────────────────────────────────────────────────────────────────────┐
   │  PredictRunDO  (Durable Object — run coordinator, SQLite storage)                   │
   │  - one DO per runId; tracks chunk state {category: pending|started|done|failed}     │
   │  - dedup: enqueue is idempotent on (runId, category)                                │
   │  - aggregate: when all chunks terminal → write a run-summary audit row              │
   │  - watchdog alarm: if a chunk is "started" but stale > N min → re-enqueue (cap)     │
   │  - exposes /status for the /run + ops to read run progress                          │
   └──────────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 Why a **held request** (not `start()`-to-completion)

The current `worker.ts` uses `start()` and lets the entrypoint run to completion —
which is exactly the path that silently died at ~90 s on 6/3. The robust idiom,
straight from the docs, is:

- Container runs a **persistent HTTP server** on `defaultPort` with a `/predict`
  endpoint that executes **one category** and **keeps the response open** (chunked
  transfer / periodic progress lines) until done.
- The Worker/DO holds `c.fetch('/predict?...')`. Because the request is _in
  flight_, it counts as activity; and the DO additionally calls
  `renewActivityTimeout()` on each progress heartbeat (the documented
  background-work pattern). The container therefore **cannot be idle-reaped while a
  category is running**, and finishes in ~1–2.5 min — far inside the 15 m
  graceful-stop window.
- This converts "no inbound HTTP traffic" (the 6/3 failure precondition) into
  "continuous inbound HTTP traffic," which is the _supported_ mode for Containers.

### 3.2 Message schema (Queue)

```jsonc
{
  "runId": "20260618", // = runDate; stable, dedup key with category
  "runDate": "20260618", // YYYYMMDD JST
  "category": "jra", // "jra" | "nar" | "ban-ei"
  "modelVersion": "iter21-jra-cb-2013-v8", // pinned per category for audit/idempotency
  "attempt": 1, // incremented on DO-driven re-enqueue
}
```

One message per (runDate, category). `max_batch_size=1` so each consumer
invocation handles exactly one category (clean retry/ack boundary). DLQ captures a
category that fails `max_retries` times — surfaced via the run-summary audit row +
`wrangler tail`.

### 3.3 Component roles

- **Cron Trigger** — the _only_ scheduler (CF rule; GitHub Workflows forbidden).
  Two crons so odds freshness is respected: **`0 18 * * *` (JST 03:00)** enqueues
  **NAR + Ban-ei**; **`30 0 * * *` (JST 09:30)** enqueues **JRA** (after the Neon
  odds mirror has caught up — see the serve-skew memory; the 09:30 run is what
  fixed JRA serve skew on the Mac path, commit `fe871a6`). A small **odds-freshness
  guard** in `scheduled()` skips/defers JRA if the mirror is stale (re-enqueue via
  DO alarm).
- **Queue** — durable fan-out, retries with backoff, DLQ. Decouples "decide to run"
  from "run," so a transient Neon/DuckDB hiccup retries without losing the day.
- **Queue consumer (Worker)** — per message, opens the held `/predict` request to a
  per-chunk container DO, acks on success, retries on failure.
- **FinishPositionPredictContainer (DO+Container)** — the actual compute; one DO
  instance _per (runId, category)_ (`getContainer(binding, \`run:${runId}:${cat}\`)`)
so categories run on **separate, parallel** containers (we have the concurrency
budget). `renewActivityTimeout()` keeps it alive; idempotent UPSERT makes retries
  safe.
- **PredictRunDO (run coordinator)** — tracks chunk state in DO SQLite, dedups
  enqueues, aggregates a run summary, runs a **watchdog alarm** that re-enqueues a
  chunk stuck in `started` beyond a stale threshold (bounded attempts), and serves
  `/status`. This is the "hold state across the cron window" role.

### 3.4 Alternative: Cloudflare Workflows instead of the two DOs

A single **Workflow** can replace _both_ the Queue and `PredictRunDO`:
`step.do("predict-nar", {retries:{limit:3,...}, timeout:"10 minutes"}, () =>
container.fetch('/predict?category=nar'))`, one step per category, durable +
auto-retry + resumable. Pros: one orchestrator, built-in durable retry/state, no
hand-written watchdog. Cons: a Workflow still must drive a Container for the native
ML (it can't run CatBoost itself), so it's _orchestration only_; adds a new product
surface; Python Workflows are beta and pin an old compat date. **Recommendation:
start with Queue + DO** (smaller blast radius, matches the existing Worker shape and
the team's Queues familiarity in `sync-realtime-data-*`); **keep Workflows as a
clean refactor** if the DO watchdog logic grows.

---

## 4. Stability & speed measures

**Stability / idempotency**

- **Held request + `renewActivityTimeout()`** per progress heartbeat — eliminates
  the idle-reap failure mode (the 6/3 root cause).
- **Per-chunk isolation** — a JRA failure (e.g. E-top2 model load) cannot block NAR
  / Ban-ei; each is its own message, DO, and container.
- **Idempotent UPSERT** — Queue retries and DO re-enqueues are safe; no cleanup ever
  needed (consistent with the no-DELETE data rules).
- **DLQ + watchdog alarm** — a category that exhausts retries lands in the DLQ and
  the run-summary audit row marks it failed; a chunk stuck `started` is re-enqueued.
- **Audit at both levels** — per-chunk row (existing `finish_position_cron_executions`)
  - a run-summary row from `PredictRunDO`, so a partial day is visible.
- **Odds-freshness guard** — JRA only runs once the Neon mirror is current, carrying
  forward the serve-skew fix from the Mac path.

**Speed / parallelism**

- **Categories run in parallel** on separate containers (concurrency budget is 6 TiB
  / 1,500 vCPU — three `standard-4` is nothing). Wall-clock ≈ max(category) ≈ the
  JRA leg, not the sum → faster than today's sequential ~3.5 min.
- **standard-4** (4 vCPU / 12 GiB) for the DuckDB aggregation — unchanged.
- **Models baked into the image** — no runtime R2 read; warm on container start.
  Container cold start 1–3 s is negligible.
- **Hyperdrive** in front of Neon — up to 90% lower uncached query latency for the
  feature-build round trips (and lower Neon compute), _if_ DuckDB's `postgres_scanner`
  accepts the Hyperdrive DSN (verify in probe).
- **Per-venue fan-out** is the in-reserve lever if JRA+E-top2 ever approaches the
  per-request budget — already shown chunkable; needs a `PREDICT_KEIBAJO` filter.

**Observability / cost**

- `observability.head_sampling_rate: 0.1` stays (mandatory per
  `project_cloudflare_observability_cost`). New Queue consumer + DOs inherit it.
- `wrangler tail finish-position-cron` for the first live runs; DLQ depth + the
  run-summary audit row as the daily health signal.

---

## 5. Feasibility verdict + recommendation

### Verdict (historical): **VIABLE as a hybrid; do a measured pilot before any cutover.**

**Why viable now (changed since 6/3).** The single fact that made CF Containers
"unusable for this batch" — a hard ~90 s reap with no inbound traffic, not
extendable — is **not what the current docs describe**. Today's contract is: 10 m
idle timeout, an explicit `renewActivityTimeout()` for background work, a 15 m
graceful-stop window, an official Cron-Container example, and 15x more concurrency.
The workload is **already per-category chunkable with zero code change**
(`PREDICT_CATEGORIES`), each category finishes in ~1–2.5 min (well inside any of
these windows), writes are idempotent UPSERTs, and Queues+DO give durable
fan-out/retry/coordination. The architecture is robust to _either_ lifecycle world
because every unit is driven by a **held inbound request**, which is the supported
mode.

**Why not a blind migrate.** The 90 s reap was _empirically observed_ on this exact
image on 6/3; the docs strongly imply it's gone, but **we have not re-measured**.
At the time, the plan kept the legacy Mac launchd path enabled only until a
pilot confirmed a held `/predict` request survived a full JRA leg (incl. E-top2)
on the real image against a Neon branch. That pre-cutover authority statement is
superseded; current production must remain Cloudflare-only.

**Risks / costs (honest):**

- **Re-measure risk (primary).** If some residual short-no-traffic reap still bites
  the held-request design, fall back to **per-venue chunking** (each unit <60 s) or
  pause the Cloudflare cutover until the Cloudflare path is fixed. Probe first (§6).
- **Hyperdrive ↔ DuckDB `postgres_scanner` compatibility** is unverified; if it
  doesn't take the Hyperdrive DSN, point straight at Neon (lose the latency win, not
  correctness).
- **Cost:** Container minutes (3 categories × ~few min × `standard-4`, daily — small,
  CPU billed on utilization); Queue ops (a few/day — negligible); **Neon compute via
  Hyperdrive** — net should _drop_ vs today if Hyperdrive caches feature-build
  queries, but watch it (`e84b078` cut Neon idle cost; don't regress).
- **New surfaces to operate** — a Queue, a DLQ, and two DO classes vs one launchd
  plist. More moving parts than Mac.

**2026-06-19 更新**: off-Mac migration 設計が `04-off-mac-migration-plan.md` に完成。
以下の Recommendation を具体化した pilot フローは 04 を参照。
PHASE 1 (Neon pre-wake 先行 deploy) → PHASE 2 (held-request pilot, NAR category) →
PHASE 3 (dual-run, 本番 Neon) → PHASE 4 (cutover) の順で実施。
当時は Mac launchd を PHASE 4 cutover 確認まで authority とする計画だった。
現在は Cloudflare-only production に supersede 済み。

**Recommendation (concrete):**

1. **Historical pre-cutover step:** keep Mac launchd enabled only during the pilot window. Current production must not use it as scheduler or authority.
2. **Pilot (read-only-ish, Neon branch):** implement the held-request `/predict`
   endpoint + per-category Queue fan-out behind the existing Worker, deploy to a
   **Neon branch / throwaway DB**, and run §6's measurement. This is the decisive
   test the 6/3 work never ran (it used `start()`-to-completion + a DO-alarm
   keepalive, not a held request + `renewActivityTimeout()`).
3. **If the pilot passes** (full JRA-E-top2 leg completes via held request, ≥3 clean
   runs, parity vs historical Mac output): cut the JRA 09:30 + NAR/Ban-ei 03:00 crons over to
   CF, run dual only as a temporary comparison window, then retire the Mac plist.
   CF wins decisively because production must have no single-Mac dependency.
4. **If the pilot fails** (reap still bites even with held request): try **per-venue
   chunking**; if that also fails, stop the cutover and fix the Cloudflare path
   rather than making launchd production authority again.

The decision hinges entirely on **one measurement**, and the design is structured so
that measurement is cheap and the production path is never at risk during it.

---

## 6. The one decisive probe (run before any verdict flip)

On a **Neon branch / throwaway DB** (never prod), with the real predictor image:

1. Add a `/predict?category=<cat>&runDate=<ymd>` endpoint to the container that runs
   `_predict_category` inline and **streams a progress line every ~10 s** (chunked
   response) until done.
2. DO/Worker: `await c.fetch('/predict?...')`, calling `renewActivityTimeout()` on
   each progress chunk; log start→end wall-clock and the final `racesPredicted`.
3. Run the **JRA leg with E-top2 enabled** (the slowest unit). Record: did the held
   request complete? wall-clock? any SIGTERM in `wrangler tail` / `onStop`?
4. Repeat ≥3×; confirm idempotent UPSERT parity against a Mac run for the same date.
5. Probe **Hyperdrive**: point `NEON_DATABASE_URL` at the Hyperdrive DSN; confirm
   DuckDB `postgres_scanner` connects and measure the feature-build delta.

**Pass = JRA-E-top2 held request completes cleanly ≥3×, output parity with Mac, no
mid-run SIGTERM.** Only then update `feedback_cloudflare_containers_90s_reap` and
proceed to the dual-run cutover in §5.

---

## 7. Summary table

| Concern     | Legacy Mac launchd          | Proposed CF (Cron→Queue→Container, DO-coordinated)                                                           |
| ----------- | --------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Scheduler   | launchd JST 03:00           | **CF Cron Trigger** (03:00 NAR/Ban-ei, 09:30 JRA) ✔ rule-compliant                                           |
| 90 s reap   | n/a                         | **Held request + `renewActivityTimeout()`** → not idle-reaped; 15 m graceful window; **re-measure required** |
| Parallelism | sequential ~3.5 min         | per-category parallel containers → wall-clock ≈ max(category)                                                |
| Reliability | proven; **lost if Mac off** | durable Queue retries + DLQ + DO watchdog; **no Mac dependency**                                             |
| Idempotency | UPSERT                      | UPSERT (retries/re-enqueue safe)                                                                             |
| DB latency  | local Colima→Neon           | Neon via **Hyperdrive** (≤90% less uncached latency, verify DuckDB DSN)                                      |
| Cost        | free (Mac)                  | Container minutes + Queue ops + Neon (net Neon may drop w/ Hyperdrive)                                       |
| Verdict     | historical pre-cutover path | **viable → pilot on Neon branch → temporary comparison → cut over**                                          |
