# CF-Only Finish-Position Serving Architecture

Status: **design doc, partially implemented**. Written 2026-07-11 in response to a
user directive to move finish-position production serving to a **Mac-batch-free,
Cloudflare-only** architecture permanently. The Mac batch is being disabled
tonight; an overnight re-trigger loop covers the 2026-07-12 card only, as a
one-night bridge -- not a repeatable substitute. This doc is the durable plan for
what replaces it.

**What shipped tonight** (commit, not deployed -- see §6): the reliability fix
in §4.1-4.2 (retry-budget/stale-window reordering, claim-refresh-on-poll, DLQ
consumer). **What is spec-only** (not implemented tonight): the day-base
feature-build split (§2-3, already designed in a prior doc, not yet built),
the coverage self-healing cron (§4.3), and the corner-features independent
refresh job (§4.4).

**Framing correction, inherited from the prior design pass**: none of this is
novel. §2-3 completes `docs/finish-position-cloudflare-container/08-per-race-rebuild-plan.md`
(2026-06-19) via the implementation-ready version in
`apps/pc-keiba-viewer/tmp/serving-speedup-plan/design.md` (2026-07-11,
untracked -- tmp/ is not git-tracked per repo policy, so this doc is the
tracked record of its conclusions). §4 responds directly to defects found by
two parallel investigations the same night (`pb-queue-do`, `pb-silent-failures`
in the session's campaign log) and one live failure specimen observed during
tonight's deploy smoke test (a NAR focused-full run that returned "accepted"
and then produced zero prediction rows and zero errors after 10 minutes / 40
polls -- the exact silent-death shape this doc's §4.1-4.2 fixes).

**Update 2026-07-18 (RS scheduling, not finish-position)**: this doc's Mac-free
directive only ever covered finish-position generation; `race-prediction-guard.sh`
kept one Mac-dependent role after §0-6 shipped -- it was the thing that POSTed
`sync-realtime-data`'s `plan-running-style-predictions` job for the JST windows
`sync-realtime-data`'s own native running-style crons do not cover (`*/10 0-14 * * *`
today, `0 12 * * *` tomorrow-prewarm). Per the same permanent rule this doc
established for finish-position -- production prediction generation must not
depend on Mac batch processing -- that RS-kick scheduling gap is now also closed
by a Cloudflare Cron Trigger (`finish-position-cron`'s `src/running-style-kick.ts`,
two new crons in `wrangler.jsonc`). See
`docs/finish-position-prediction-system.md` §1.3 for the full design, the exact
crontab, and the guard-retirement sequence; the guard's RS-kick role is
superseded once that CF cron is verified live, matching how §1.2 above already
treats the guard's finish-position role as reduced to a monitor + CF-retrigger
loop.

---

## 0. Why Mac batch existed, and why removing it is not free

The finish-position container's per-race `mode=full` pipeline runs 7-17
sequential DuckDB feature-layer scripts plus CatBoost/XGBoost scoring against a
single per-category Container Durable Object slot (`predict-{category}`,
`buildPredictDoName` in `src/queue-consumer.ts`). A same-day audit
(`tmp/serving-latency-audit/`, referenced in this session's campaign log) found:

- JRA per-race full pipeline: **~27.5 min**, audited.
- NAR: **~13-17 min**, extrapolated from partial timing.
- Ban-ei: not directly audited; script-count-proportional estimate (7 of
  JRA's 17 layers) is **~11 min** -- flag as unmeasured, not a fact.
- Mac batch equivalent: **~5.3 min for all 36 JRA races** (~9 s/race
  amortized), because the Mac path builds the day-invariant feature layers
  once and reuses them across every race of the day -- exactly the
  optimization §2-3 below ports into the Container.

Because the Container path rebuilds every layer from scratch per race, and
because only one pipeline runs at a time per category (the single
`_FOCUSED_FULL_IN_FLIGHT` slot), a full JRA card serialized end-to-end would
take 36 x 27.5 min = 16.5 h -- longer than the entire race day. The Mac batch's
day-wide `race-prediction-guard.sh` escalation was the thing that actually
absorbed this gap in production, by detecting apparent CF stalls and
triggering a local Docker run that could churn through the whole card in
minutes. Tonight's directive removes that absorber. Section 5 below is the
proof that the Container path, once the day-base split lands, no longer needs
it -- and §4 is the reliability hardening required so that, in the meantime
and afterward, individual race failures degrade to a retried/re-driven message
instead of a silent, unrecoverable gap.

---

## 1. Layer freshness taxonomy (day-stable vs race-fresh)

This section is a condensed pointer to the full analysis already done in
`apps/pc-keiba-viewer/tmp/serving-speedup-plan/design.md` §1 (2026-07-11,
untracked). Full detail (per-script verdicts, evidence, the
`jra-jockey-pedigree-cell` same-day-cumulative exception) lives there; this doc
restates only what the implementation plan in §2-3 depends on, so the tracked
record does not silently lose it if the tmp/ file is cleaned up.

- **DAY-STABLE**: output is a deterministic function of data with an
  as-of-yesterday-or-earlier cutoff (career stats, pedigree, grade-race
  lineage, head-to-head, trainer-stable affinity, pace-style, course-numerical
  lookups, similar-race pools, sire-venue bias, `track_bias_cte` -- a 5-day
  trailing window ending strictly before target date, confirmed NOT a
  same-day leak despite its name). Safe to materialize once per
  `(category, target_date)`.
- **RACE-FRESH**: depends on same-day data (realtime odds, official same-day
  weight declaration, same-day track condition, or same-day results of
  _earlier races at the same venue_). Must run at/near request time.
- **MIXED**: some columns DAY-STABLE, some RACE-FRESH from the same script;
  Phase 1 (below) treats the whole script as RACE-FRESH until Phase 2 splits
  it at the SQL level.
- **Permanent exception**: `add-jra-jockey-pedigree-cell-features.py` windows
  `sum(...) over (partition by kishu_code order by race_seq rows between
unbounded preceding and 1 preceding)` at **race grain within the same
  calendar day** -- an earlier JRA race at the same venue today folds into a
  later race's "prior" window today, by design (feeds the
  `kyoso_joken_code=703` cell-routed 269-feature variant only). This is
  correct behavior, not a staleness bug, and **must never be moved into the
  day-base** -- doing so would silently break the one feature family whose
  entire value is seeing same-day-so-far results.

Per-category script partition (Phase 1 boundary, no SQL rewrites):

| Category | Total scripts | DAY_CHAIN | RACE_CHAIN                                                                                     |
| -------- | ------------- | --------- | ---------------------------------------------------------------------------------------------- |
| JRA      | 17            | 12        | 5: market-signal, near-miss, baba-pedigree-affinity, relationship-r1, jra-jockey-pedigree-cell |
| NAR      | 10            | 7         | 3: near-miss, baba-pedigree-affinity, relationship-r1                                          |
| Ban-ei   | 7             | 5         | 1: baba-pedigree-affinity                                                                      |

---

## 2. Day-base feature build (once per category per day)

### 2.1 What gets materialized, and where

- A new **day-base build**: `DAY_CHAIN` scripts run once per
  `(category, target_date)`, producing a parquet keyed
  `feat-daybase/{category}/{runDate}/features.parquet` in the existing
  `FEATURES_CACHE` R2 bucket (`pc-keiba-features-archive`) -- a **new, distinct
  key namespace**, not conflated with the existing per-race rescore cache key
  (`feat-cache/{category}/{runDate}/{keibajoCode}/{raceBango}/features.parquet`),
  which a prior design explicitly rejected merging (per-race cache granularity
  was rejected in `docs/finish-position-cloudflare-container/06-per-race-architecture.md`).
- **Container local disk** (`/tmp`, 20 GB on the `standard-4` instance) is used
  as a fast in-process existence check ahead of the R2 round trip: the
  category-scoped Container DO already survives across multiple same-day
  races in one long-lived process (`sleepAfter` -- see §4.1 for its new
  value), so a same-day, same-category day-base build is very likely still on
  local disk for the 2nd+ race of the day. R2 is the durable layer for
  restarts, deploys, and OOM/eviction, which do NOT preserve local disk.
- **Neon is explicitly not used** for this cache, per house convention
  (batch/bulk artifacts -> R2 Parquet, Neon/Postgres only for per-row writes)
  and cost-consciousness (no new compute-hours for something R2 already
  serves).

### 2.2 Trigger and invalidation

- **Trigger**: the existing `FEATURE_BUILD_CRON` (JST 09:30,
  `apps/finish-position-cron/wrangler.jsonc`) is currently a documented no-op
  in `handleScheduled` (`src/worker.ts`, `shouldRunFeatureBuildCron` branch).
  Repurpose it: for each category with races today, call a new container HTTP
  surface `GET /prewarm-day-base?category=...&runDate=...` that runs
  `build_day_base` and R2-PUTs the result via the existing
  Worker-DO-proxies-bytes pattern (`container-ndjson-proxy.ts`'s base64-in-NDJSON
  shape, same as the current per-race feature-cache PUT). This runs after the
  ~09:03 JST JRA mirror lands, so entrant lists are fresh at prewarm time.
- **Lazy fallback**: at request time, the read order is local disk -> R2 ->
  **build synchronously as part of this race's request** (the current
  full-chain behavior, scoped down to just `DAY_CHAIN`). This covers: the cron
  never fired, fired and errored, or fired for a category with zero races that
  day (self-correcting on the first real request).
- **Entry-list drift is the one real correctness risk that must ship with
  Phase 1, not be deferred**: the day-base reflects the entry list as of
  ~09:30. A same-day scratch or emergency-replacement entrant changes it.
  Mitigation: before scoring a race from a cached day-base, do a cheap PG
  existence check -- every `ketto_toroku_bango` in the race's _current_ entry
  list must appear in the day-base parquet for that race. Any miss -> do not
  silently score from a stale/incomplete day-base; fall back to the full,
  unmodified per-race `LAYER_CHAIN` for that one race only (the existing,
  already-proven-in-production path, kept permanently as the safety net).
- Same-day track condition (`babajotai_code_shiba/dirt`) is already always
  read at request time (RACE-FRESH, via `baba-pedigree-affinity`'s
  `current_baba_condition`), so it needs no day-base invalidation logic -- it
  was never cached.

### 2.3 Acceptance gate before enabling any category in production

The current per-race full-rebuild guarantees freshness by brute force. The
split must reproduce that guarantee analytically (§1) and then empirically,
because "reordering DAY_CHAIN before RACE_CHAIN is safe" is inference from
reading each script's SQL, not proof:

1. **Historical parity harness**, >=30 real historical race dates spanning
   JRA/NAR/Ban-ei, frozen `target_date`: run OLD (current unmodified
   `LAYER_CHAIN`) vs NEW (`DAY_CHAIN` once + `RACE_CHAIN` per race) against the
   same Neon snapshot, compare resulting feature parquets column-for-column,
   row-for-row (`race_id` + `umaban`-keyed join). Exact match on non-float
   columns, `rtol=1e-9` on numeric columns. Any mismatch fails the gate.
2. Must include: (a) >=1 `kyoso_joken_code=703` JRA race (exercises
   `jra-jockey-pedigree-cell`'s same-day-cumulative window against the split
   pipeline), (b) >=1 multi-race same-venue same-day scenario with the target
   race scheduled after 2+ earlier same-day races, (c) a synthetic scratch
   scenario (remove one entrant from the day-base snapshot, confirm the
   entry-list-drift fallback in §2.2 fires and matches OLD's output).
3. New test module,
   `apps/finish-position-predict-container/tests/test_day_base_parity.py`,
   documented as an integration test requiring live Neon access (same
   exemption class as `predict_upcoming.py`'s existing coverage carve-out --
   document the reason in the commit message per the repo's coverage rule
   #2). Run manually before flipping the per-category rollout flag; not part
   of the standard `pytest --cov-fail-under=95` gate.
4. Phase 2 (§3.2) reruns this same harness per refactored script, with a
   stricter bar (exact reproduction of the original single-script output).

---

## 3. Per-race incremental scoring

### 3.1 Phase 1 (mechanical split, no SQL rewrites)

`RACE_CHAIN` runs against the cached day-base as `--input-dir`, with the same
`--target-race` scoping already used today. File-level changes (not made
tonight, spec only):

- `apps/finish-position-predict-container/src/predict_lib/pipeline_args.py`:
  `DAY_CHAIN` / `RACE_CHAIN` dicts per §1's table, plus `day_chain_for()` /
  `race_chain_for()` helpers mirroring the existing `layer_chain_for()`. Keep
  `LAYER_CHAIN` unchanged as the full-rebuild fallback.
- `apps/finish-position-predict-container/src/pipeline_runner.py`: new
  `build_day_base(category, target_date, database_url) -> Path`;
  `_reset_category_work_dirs` must gain a mode that does not `rmtree` the
  day-base dir (name it so it is excluded by construction from the
  `feat-{category}-*` glob the reset currently sweeps); new local -> R2 ->
  build freshness check mirroring `weather_fetcher.py`'s
  fetch/materialize/fallback shape.
- `predict_lib/serve.py`: new `GET /prewarm-day-base` surface (§2.2).
- Shared R2 helper: extract `predict_upcoming.py`'s existing
  `_r2_get_parquet` SigV4 GET into `predict_lib/r2_client.py` so the existing
  per-race feat-cache and the new day-base cache share one implementation.
- `apps/finish-position-cron/src/worker.ts`: replace the `FEATURE_BUILD_CRON`
  no-op body with the prewarm dispatch (§2.2), in a new or extended TS module
  with matching `*.test.ts`.

Estimated effort: 3-5 engineering days + 1-2 days running the parity harness.

### 3.2 Phase 2 (SQL split of MIXED layers, needed to hit the <2 min target)

Rewrite `near-miss`, `baba-pedigree-affinity`, `relationship-r1` (smallest
race-fresh surface first) into a day-stable aggregation script (feeds the
day-base) + a thin race-fresh overlay script (joins the target row's own
current value onto the pre-aggregated table, no historical scan). Rerun the
§2.3 parity harness per script with the stricter bar.
`jra-jockey-pedigree-cell` is explicitly excluded (§1) -- it cannot be split,
it is correct exactly because it is race-fresh. `market-signal` should get its
own timing baseline (not measured in the source audit) before deciding
whether it is worth touching.

Estimated effort: 1-2 weeks, sequenced per script, each shippable
independently behind its own flag.

### 3.3 Projected per-race timing

|                        | JRA today          | JRA Phase 1                                                                                                                                             | JRA Phase 2 (projected, target)                                                                                                                                                                                                              |
| ---------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PG layers run per race | 16                 | 5                                                                                                                                                       | 5, but 3 are thin overlays                                                                                                                                                                                                                   |
| Estimate               | 27.5 min (audited) | ~9-10 min (~2.7x; the 5 remaining layers keep ~100 s/layer average, unattributed between history-scan cost and per-subprocess Neon connection overhead) | ~2 min (~13-16x; informed by the Mac batch's 64.8 s/24.4 s _total_ population-wide near-miss/baba-pedigree cost, so a single-race overlay join should be sub-5 s; remaining floor is per-subprocess Neon connection overhead, ~5-15 s/layer) |

**Both Phase 2 numbers, and all NAR/Ban-ei numbers in this section, are
projections, not measurements.** Phase 1's rollout must ship with per-layer
timing instrumentation (the 27.5-min JRA breakdown came from real logs, so
this is a small addition) so Phase 2's actual cost is measured empirically --
confirming whether Neon connection overhead or historical-scan cost dominates
-- before committing to the Phase 2 SQL-rewrite effort. This gap is treated as
a **blocking unknown for capacity planning**, not an assumption, in §5.

### 3.4 Rollout

Env-flag gated (`DAY_BASE_SPLIT_ENABLED`, mirroring the existing
`NAR_TRANSFORMER_BLEND_ENABLED` pattern), per-category, **JRA first** (worst
current number, most layers to save, no LightGBM member so Worker-native
scoring stays out of scope for this change). Validate via `DEPLOY.md`'s
existing focused-full smoke test (`POST /api/admin/run-focused-full-race`),
extended to diff split vs. non-split output for a specific real upcoming race
before/after enabling. Then NAR, then Ban-ei. Rollback = flip the flag to 0 --
no data migration, `race_finish_position_model_predictions` schema unchanged,
UPSERT-only semantics preserved throughout.

---

## 4. Reliability hardening

Once the Mac batch's `race-prediction-guard.sh` escalation is gone, the
Container/Queue/DO path is the **only** production path -- so its failure
modes, previously papered over by a same-day local re-run, must be
individually survivable. Four defects were diagnosed the same night this doc
was written; the first two are fixed and committed tonight, the last two are
design-only.

### 4.1 Retry-budget / stale-window reordering + claim-refresh-on-poll (SHIPPED)

**Defect** (found independently by two investigations, `pb-queue-do` and
`pb-silent-failures`, and reproduced live during tonight's deploy smoke test:
a NAR `35:01` focused-full run returned `accepted`, then after 10 minutes / 40
polls had zero prediction rows and zero errors -- a silent-death specimen):

A focused per-race full message gets a fast `accepted` response while the
container's real pipeline runs in a detached thread. Redeliveries poll a
Neon-backed completion check
(`isFocusedFullPredictionComplete`, `src/focused-full-completion.ts`) and a
DO-backed in-flight claim (`claimFocusedFullRace`,
`src/predict-run-coordinator.ts`) that only allows a second attempt to
actually relaunch the pipeline once the claim's timestamp is stale. Before
tonight's fix:

- `FOCUSED_FULL_RETRY_DELAY_SECONDS` (150 s) x `max_retries` (12, in
  `wrangler.jsonc`) = a **30-minute** total in-band retry budget per message.
- `FOCUSED_FULL_IN_FLIGHT_STALE_MS` = **35 minutes**.
- Because the retry budget (30 min) was _shorter_ than the stale window
  (35 min), a genuinely dead detached pipeline could never be reclaimed
  in-band -- every redelivery would see the claim as "started, not yet
  stale" and just retry-wait, until the 12th retry exhausted the queue's own
  retry budget and the message was dead-lettered. The dead-letter queue
  (`finish-position-predict-dlq`) had **zero consumer** (§4.2), so this was a
  silent, permanent black hole: same shape as the multi-week finish-position
  serving blackout this session's other work independently diagnosed.
- Separately, the claim's timestamp was never refreshed by intervening polls,
  so there was no way to distinguish "still being actively watched, just
  slow" from "abandoned" other than the fixed absolute window.

**Fix** (`src/predict-run-coordinator.ts`, `src/queue-consumer.ts`,
`src/container-class.ts`, `wrangler.jsonc`):

1. `claimFocusedFullRace` now **refreshes the claim's heartbeat timestamp**
   on every poll that observes the race as still genuinely in flight (not
   complete, not stale, not terminal). This converts staleness from "absolute
   time since the pipeline launched" into "time since the queue last actively
   confirmed it was still worth waiting for": as long as redeliveries keep
   landing every ~150 s, the claim never falsely goes stale for a
   legitimately slow pipeline (avoiding a false reclaim that would
   duplicate-launch a second pipeline for the same race). It only goes stale
   once redeliveries actually stop -- which happens either because the race
   finished/errored (terminal state) or because retries were exhausted and
   the message stopped being redelivered at all.
2. Because staleness is now a heartbeat gap rather than a pipeline-duration
   ceiling, `FOCUSED_FULL_IN_FLIGHT_STALE_MS` was shrunk from 35 min to **15
   min** -- 6x the nominal ~150 s poll cadence (margin for Cloudflare Queues
   redelivery jitter), reacting far faster to a genuine "nobody is polling
   this anymore" gap than the old 35-minute absolute ceiling did.
3. `max_retries` was raised from 12 to **16** (150 s x 16 = **40-minute**
   total retry budget), restoring the required ordering (stale window 15 min
   << retry budget 40 min, ~25 min / ~10 retries of margin) as defense in
   depth even for any path where the heartbeat refresh in (1) does not fire.
4. `SLEEP_AFTER` in `container-class.ts` was raised from `30m` to **`45m`** to
   stay above the new 40-minute retry budget, preserving its own documented
   invariant ("the container reliably outlives a single worst-case focused
   pipeline run").

Tests: `src/predict-run-coordinator.test.ts` (heartbeat refresh on the "still
fresh" branch, exact stored-record assertion), existing
`src/queue-consumer.test.ts` staleness-parameter assertion updated to the new
value. All four coverage metrics remain above the 95% package threshold (see
§6 for the exact numbers from this session's run).

### 4.2 Dead-letter queue consumer (SHIPPED)

**Defect**: `finish-position-predict-dlq` was configured as the primary
queue's `dead_letter_queue` but had no consumer entry in `wrangler.jsonc` at
all. Any message that exhausted retries -- for any reason, not just the §4.1
defect -- was retained by Cloudflare but never processed, acked, or surfaced
anywhere.

**Fix**: `src/dlq-consumer.ts` (new), wired into the existing `queue()` Worker
export via `MessageBatch.queue` routing (`src/worker.ts` -- one Worker script
consumes both queues, since Cloudflare Queues route by consumer _entry_, not
by handler; `batch.queue` names which queue the batch came from). For each
dead-lettered message:

1. Write a durable event row to a new D1 table,
   `finish_position_predict_dlq_events`
   (`apps/finish-position-cron/migrations/0002_create_dlq_events.sql`,
   built via `src/dlq-events.ts` mirroring the existing `audit.ts` pattern) --
   category, run date, mode, keibajo/race scope, redrive count, whether it was
   redriven. This is the durable trace that did not exist before tonight.
2. If the message is a focused-full per-race message
   (`isFocusedSkipDedupMessage`, now exported from `queue-consumer.ts` for
   reuse), force its DO claim to `status: "error"` via
   `completeFocusedFullRace`. A message reaching this consumer has, by
   definition, stopped being redelivered, so §4.1's heartbeat refresh cannot
   save it and the claim would only clear once its heartbeat naturally goes
   stale (15 min, per §4.1) -- forcing it here is faster and certain, making
   the race immediately reclaimable the instant the redrive below lands
   rather than depending on timing. Other message shapes (legacy per-category
   full, per-race/per-category rescore) do not hold a claim that can get
   stuck this way (`claimRun` has no staleness gate at all -- any non-success
   status is always reclaimable -- and rescore messages carry no
   completion-claim), so they need no unstick step.
3. Re-enqueue the original message once, bounded by a new
   `dlqRedriveCount` field on `PredictQueueMessage` (capped at 1 redrive) so a
   poison-pill message cannot bounce between the two queues forever. A second
   dead-letter landing is logged (`redriven=false` event row) and dropped.

New `wrangler.jsonc` consumer entry for `finish-position-predict-dlq`
(`max_batch_size: 10`, `max_batch_timeout: 30`, `max_retries: 3`, no
`dead_letter_queue` of its own -- the durable D1 row already written on first
landing is the permanent record if the redrive path itself fails
repeatedly). Tests: `src/dlq-consumer.test.ts` (11 cases: event recording,
claim unstick / no-unstick, redrive/no-redrive at budget, retry-on-D1-failure,
retry-on-DO-failure, multi-message batch), `src/dlq-events.test.ts` (record
builder, SQL, bind-param ordering, boolean encoding), `src/worker.test.ts`
(queue-name routing to `handleDlqQueue` vs `handleQueue`).

### 4.3 Coverage self-healing cron (SPEC ONLY -- not implemented tonight)

**What it replaces**: `race-prediction-guard.sh`'s day-wide `COUNT` check,
which this session's `pb-queue-do` investigation found was itself a source of
incidents -- a day-wide aggregate cannot distinguish "36 races done, 0
pending" from "34 races done, 2 genuinely still in flight," so the guard's
escalation to a local Docker re-run periodically fired against races that
were not actually stuck, producing duplicate-write incidents (same
`model_version` primary key, so an overwrite rather than a literal duplicate
row, but a wasted re-prediction that could regress a race already correctly
scored by CF, and the root reason a day-wide guard is the wrong granularity
per this repo's own `feedback_production_per_race_granularity` /
`feedback_eval_class_subgroup_mandatory` conventions).

**Design**: a new CF Worker cron, **per-race granularity**, not day-wide:

1. Schedule: every 15 min during JST 10:00-20:59 (mirrors the existing
   `*/10 1-11 * * *` per-race coordinator cadence, offset so the two crons do
   not collide on the same tick).
2. Query: races from `realtime_race_sources` (the same source the per-race
   coordinator already reads, `REALTIME_DB` binding) whose
   `race_start_at_jst` is more than a grace window (e.g. 15 min) in the past,
   joined against `race_finish_position_model_predictions` for the currently
   routed champion `model_version` (reuse
   `isFocusedFullPredictionComplete`'s existing expected-model-version
   resolution SQL in `src/focused-full-completion.ts` rather than
   duplicating it) with **zero matching rows**.
3. For each such race: check the focused-full DO claim
   (`claimFocusedFullRace`) before enqueueing anything. If a claim already
   exists and is fresh (not stale per §4.1's 15-minute heartbeat window), skip
   -- it is legitimately still in flight, and the coordinator's own
   redelivery loop or §4.2's DLQ path will handle it if it truly dies. Only
   enqueue a fresh `skipDedup: true` focused-full message for races with no
   claim, or a claim that is already stale/terminal-error -- i.e., races that
   were never enqueued at all (a coordinator/discovery gap) or that already
   exhausted the normal recovery paths.
4. Log every self-healing enqueue to a new column or a shared event table
   (extend `finish_position_predict_dlq_events` with a `source` discriminator,
   or add a parallel `finish_position_coverage_gap_events` table) --
   distinct from a DLQ redrive, since a coverage gap can arise from causes
   §4.2 never sees (the coordinator simply never enqueued the race, e.g. a
   discovery-timing miss), not only from a died pipeline.

This is the direct functional replacement for the guard's role and should be
implemented **before or alongside** the day-base work (§2-3), not after --
it is lower risk, higher immediate safety value, and does not depend on any
SQL-layer changes.

### 4.4 Corner-features independent refresh (SPEC ONLY -- not implemented tonight)

**Defect** (found by `pb2-ingestion-sync` this session): `race_entry_corner_features`
has **no independent refresh path**. It is populated only as a side effect of
the Win5-overlay pipeline (JRA Win5 gate + overlay short-circuit + 14-day
lookback + **local-PG-only** write, never pushed directly to Neon). Measured
damage: settled-race NULL rates of 33.4% (JRA), 12.6% (NAR), **49.6%
(Ban-ei)**.

**Why this matters for the reliability work in §4.1-4.2, not just for feature
quality**: `race_entry_corner_features` is the _expected-entrant_ source table
in `isFocusedFullPredictionComplete`'s own completion-check SQL (the query
this doc's §4.1 fix depends on to decide whether a race is done). If that
table is NULL or stale for a given race, `expected_rows` can be wrong,
degrading the completion check's accuracy independently of anything §4.1-4.2
fixed -- a race could look "incomplete forever" (if `expected_rows` is
undercounted) or "complete" prematurely (if it is overcounted). §4.3's
self-healing cron reuses the same completion-check SQL, so this defect
propagates into that design too.

**Design**: extract the corner-features build logic (already confirmed
upsert-only / date-scoped, no DELETE/TRUNCATE, per this session's live
rebuild exercise which regenerated it via direct layer-script invocation) into
a **scheduled CF Container job**, decoupled from Win5:

1. Trigger: fold into the existing `09:30` day-base prewarm dispatch (§2.2) --
   run immediately before the day-base build for each category with races
   today, so `RACE_CHAIN` (and the §4.3 cron's completion checks) never read a
   stale/absent corner-features row for a race scheduled today or tomorrow
   (`PREDICT_DAYS_AHEAD`).
2. Write path: **directly to Neon**, not the local-PG-mirror-then-sync path
   the Win5-overlay side effect currently uses -- the local-mirror hop is a
   proven staleness source elsewhere in this system (per this session's other
   findings on mirror lag) and there is no reason to route a from-scratch
   Container job through it.
3. Scope: remove the Win5-specific 14-day lookback limit; this job's job is
   coverage for _today's and tomorrow's_ scheduled races, independent of
   whether Win5 betting is active for them.
4. Safety: upsert-only, date-scoped, matching the already-verified-safe
   semantics of the existing build script (no new DELETE/TRUNCATE surface,
   consistent with this repo's no-data-delete convention).

---

## 5. Throughput math

Race-day inputs (per task brief): **JRA 36, NAR 44, Ban-ei 12** = 92 races.
Race-hours window: JST 10:00-20:59 (`*/30 1-11 * * *` in `wrangler.jsonc`,
UTC 1-11 = JST 10-20:59), **~11 hours**. Categories run on independent
Container DO instances (`predict-jra`, `predict-nar`, `predict-ban-ei`), so
they proceed **concurrently** with each other; races _within_ a category
currently serialize through that category's single in-flight slot.

### 5.1 Today (baseline, no day-base split) -- infeasible, hence Mac batch

| Category | Races | Per-race (measured/estimated)                                     | Category total (fully serial) |
| -------- | ----- | ----------------------------------------------------------------- | ----------------------------- |
| JRA      | 36    | 27.5 min (audited)                                                | 990 min = **16.5 h**          |
| NAR      | 44    | ~15 min (midpoint of 13-17 min extrapolated)                      | 660 min = **11.0 h**          |
| Ban-ei   | 12    | ~11 min (estimate, unaudited -- script-count-proportional to JRA) | 132 min = **2.2 h**           |

JRA and NAR both exceed or consume the entire 11-hour window with zero margin
under a fully-serial assumption. In practice races are naturally staggered by
real post times, so this worst case does not materialize every day, but any
multi-venue burst (2-3 JRA venues racing near-simultaneously) concentrates
enough same-category races into a narrow sub-window to exceed capacity --
which is exactly the failure mode `race-prediction-guard.sh`'s escalation was
built to catch. **This is why removing Mac tonight without the day-base split
already shipped is a real throughput risk for the very next multi-venue day**,
not just a reliability one; §4's fixes make failures recoverable, they do not
make the pipeline faster.

### 5.2 Target (Phase 2 SQL split, <2 min/race per task brief) -- clears with large margin

| Category | Races | Per-race target | Category total  |
| -------- | ----- | --------------- | --------------- |
| JRA      | 36    | 2 min           | 72 min = 1.2 h  |
| NAR      | 44    | 2 min           | 88 min = 1.47 h |
| Ban-ei   | 12    | 2 min           | 24 min = 0.4 h  |

Wall-clock for the whole day = max across categories (they run concurrently)
= **88 min ≈ 1.5 h**, against an 11-hour window -- **~86% of the window is
margin**, even under the pessimistic assumption that every race in a category
queues back-to-back with zero natural spacing (i.e., the target clears a
worst-case same-category burst of all 44 NAR races landing in one window,
which is far more extreme than any real multi-venue clustering). This is the
throughput proof the task brief asked for; it depends on Phase 2 (§3.2)
shipping, which is not yet built.

### 5.3 Interim checkpoint (Phase 1, JRA only measured/estimated)

| Category | Races | Per-race (Phase 1 design.md estimate)     | Category total      |
| -------- | ----- | ----------------------------------------- | ------------------- |
| JRA      | 36    | ~9.5 min (design.md's own ~2.7x estimate) | 342 min = **5.7 h** |
| NAR      | 44    | **not measured**                          | --                  |
| Ban-ei   | 12    | **not measured**                          | --                  |

JRA alone fits inside the 11-hour window with slack under a fully-serial
assumption, but a same-morning multi-venue burst (e.g., 2-3 venues x 2 early
races each = 6 races clustering near JST 10:00-11:00) would need
6 x 9.5 min = 57 min to clear before the next wave -- tight but survivable
within an hour, not comfortable. **NAR and Ban-ei Phase 1 numbers do not exist
yet** and must be measured (per §3.3's instrumentation requirement) before
Phase 1 alone can be treated as sufficient for any category. Per
`apps/pc-keiba-viewer/tmp/serving-speedup-plan/design.md` §6.1, per-venue DO
naming (splitting `predict-{category}` into `predict-{category}-{venue}`) is
the documented mitigation if Phase 1+2 still misses the freshest window on
the busiest multi-venue days -- deferred there and here, evaluate only if
real measurement shows Phase 1+2 insufficient.

### 5.4 Reliability overhead is not a throughput concern

§4.1's extra heartbeat-refresh writes (one DO `put` per poll on an
already-in-flight claim) and §4.2's DLQ path (bounded to at most one redrive
per message, cheap D1 insert + one extra queue round trip) add negligible
load relative to the multi-minute pipeline runs above -- not modeled
separately in §5.1-5.3.

---

## 6. Migration and rollback plan

**Tonight (2026-07-11, this doc's authoring session)**:

- §4.1-4.2 (reliability wave) is **committed, not deployed**. All four
  coverage metrics for `apps/finish-position-cron` after this change:
  Statements 99.54%, Branches 96.16%, Functions 100%, Lines 99.78% (494 tests,
  26 files, all passing; `tsc`/`oxlint`/`oxfmt --check` all clean) -- comfortably
  above the package's 95% gate on all four metrics, with no threshold or
  `include`/`exclude` changes.
- Rollback for tonight: trivial -- nothing is live, so there is nothing to
  roll back. The prior committed state (`max_retries: 12`,
  `FOCUSED_FULL_IN_FLIGHT_STALE_MS = 35 min`, `SLEEP_AFTER = "30m"`, no DLQ
  consumer) remains the deployed behavior until the next `wrangler deploy`.

**Next deploy (recommended as soon as possible, ideally before relying on the
2026-07-12 overnight re-trigger loop for a second night)**:

- Deploy the reliability wave (§4.1-4.2). This alone does **not** fix
  throughput (§5.1) -- it converts the existing (today, still
  Mac-assisted-in-practice-if-not-in-policy) failure mode from "silent,
  unrecoverable black hole" to "durably logged and retried/re-driven." Treat
  this as the minimum bar for letting the Mac guard's escalation role go
  unreplaced for more than one night.
- Apply the new D1 migration (`migrations/0002_create_dlq_events.sql`) via
  `bun run d1:migrate` (remote) before or as part of that deploy -- the
  migration was intentionally not applied tonight, matching "commit, don't
  deploy."
- Rollback: redeploy the prior Worker/container image; the D1 migration is
  additive (new table only, no column/schema change to existing tables) so it
  does not need to be reverted even if the code deploy is rolled back.

**Near-term (days, sequenced by risk/value, not all-at-once)**:

1. §4.3 (coverage self-healing cron) and §4.4 (corner-features independent
   refresh) -- prioritize **ahead of or alongside** the day-base work, not
   after. They are the most direct functional replacement for what the Mac
   guard provided, are lower-risk (no SQL-layer or scoring-path changes), and
   §4.4 is a dependency of §4.3's own completion-check accuracy.
2. Day-base Phase 1 (§2-3.1), JRA first, behind `DAY_BASE_SPLIT_ENABLED`,
   gated on the §2.3 parity harness. Ship with per-layer timing
   instrumentation so NAR/Ban-ei Phase 1 numbers (currently unmeasured, §5.3)
   become real before being relied on for capacity planning.
3. Day-base Phase 2 (§3.2) per script, smallest race-fresh surface first,
   each gated by the same parity harness at its stricter bar. This is the
   change that actually proves §5.2's margin; until it ships, §5.3's tighter
   (and partially unmeasured) numbers are the operative reality.
4. Re-evaluate per-venue DO naming (design.md §6.1) only if real measurement
   after Phase 1+2 shows a specific multi-venue day still missing its
   freshness window -- not proactively.

**Standing risk to name explicitly, not soften**: between tonight and the
completion of item 1 above, the system's only defense against a coverage gap
(a race that was never enqueued, or whose pipeline died and exhausted even the
widened §4.1 retry budget) is the DLQ consumer's one-time redrive (§4.2) plus
manual observation of the new `finish_position_predict_dlq_events` table.
There is no automated per-race gap-filling until §4.3 ships. This is an
accepted, time-boxed gap per the user's explicit directive to disable Mac
tonight, not an oversight -- but it should not be allowed to persist past a
few days without §4.3 landing.
