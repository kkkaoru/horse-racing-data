# Per-race latency Phase 2 — bottleneck analysis (2026-08-16)

Recorded 2026-08-16 04:40 JST by `pi-optimize-developer`.
Code + existing measurements only. No pipeline run, no image build, no deploy.

Phase 1 baseline: `per-race-latency-phase1-20260816.md`.

## Ranked bottlenecks

### B1. Production internals are not recorded (blocks every later claim)

This is not a speed bug. It is why we cannot measure production per-race
latency at all.

| Signal              | Where it dies                                                                                                                                            |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Layer elapsed       | `record_layer_timing_row` returns unless `PREDICT_DEBUG_LOGS` is on. Env is unset in Dockerfile and `wrangler.jsonc` vars. Table last write: 2026-07-18. |
| Child stdout/stderr | `run_with_stderr_capture` discards streams unless debug is on. Failure keeps a 4000-byte tail only.                                                      |
| NDJSON progress     | Worker `logPredictProgress` no-ops unless `message.debug === true`. Default admin/queue messages are not debug.                                          |
| HTTP duration       | Focused-full returns `"accepted"` and detaches (`_run_detached_focused_full`). Worker `durationMs` is accept-time.                                       |
| Per-run audit       | `_try_record_audit` is called only from CLI `main()`. The HTTP `/predict` path used in production does **not** write `finish_position_cron_executions`.  |
| D1 audit            | Still the six 2026-06-03 `started` rows. `0006_create_prediction_monitoring.sql` is not applied.                                                         |

What remains: `prediction_generated_at` (end time, no start) and D1 DLQ/coverage
tables (0816 empty).

Until B1 is fixed, "container got faster" cannot be proven in production.

### B2. Feature build is the compute cost (measured)

08-15 local 65 races: `feature_seconds` p50 511s, `score_seconds` p50 0.38s.
Scoring is not the optimization target.

JRA p50 9.9 min / max 14.6 min is the current **full `LAYER_CHAIN`** cost on
the host (17 scripts + DuckDB base). That is the path
`_build_feature_rows` takes when `DAY_BASE_SPLIT_ENABLED` does not include the
category, or when the split returns `None`.

### B3. Split reuse is optional and silent

`DAY_BASE_SPLIT_ENABLED` is a Worker secret (name confirmed; value unread).
`container-class.ts` forwards it. `is_day_base_split_enabled` is an allowlist;
empty/unset means every focused-full race runs full `LAYER_CHAIN`.

If the secret contains `jra`:

1. `ensure_day_base` may reuse a local or R2 day-base whose watermark matches.
2. On miss it **inline-builds** the whole day-base (`build_day_base`), then
   runs `RACE_CHAIN` (JRA: 5 scripts).
3. Any exception / entry-list drift returns `None` and falls back to full
   `LAYER_CHAIN` with no durable breadcrumb.

07-17 probe said catalog sources always miss. That is **stale**. Current
`ensure_day_base` does watermark reuse for `r2-catalog://`. The remaining
production question is only: is the secret non-empty, and do 2nd+ races on a
live shard actually hit?

Guess (unverified): secret is empty or unused tonight. Evidence: 08-15 local
p50 still looks like a full chain, and there are no 0816 layer-timing rows to
show `step=racechain-layer` vs `step=daybase-*`.

### B4. Accept-to-write wait dominates some races

`jra 04/01` canary HTTP 200 `accepted` at 02:57 JST; Neon FIRST write at
04:09:54 JST. **72 minutes**. 13 runners share one `prediction_generated_at`
(one UPSERT). 08-15 local worst compute is 14.6 min. Scoring is seconds.

Bounded intervals (replica READ ONLY + advisor's 03:15 snapshot):

| clock (JST) | fact                                    | implication                                                                       |
| ----------- | --------------------------------------- | --------------------------------------------------------------------------------- |
| 02:57       | HTTP 200 `accepted`                     | slot claimed / thread launched. Accept is seconds (08-15: 1.7s). Not completion.  |
| 02:57–03:15 | `04/01 = 0`. Image lacked writable belt | even a finished compute could not persist                                         |
| 03:22       | `jra 07/01` FIRST lands                 | some other job can write. 02:57 work did not land `04/01`                         |
| 03:44       | `jra 04/12` FIRST                       | still no `04/01`                                                                  |
| 03:44–04:09 | three 0815 UPDATEs, then `04/01` FIRST  | last +7.8 min matches JRA feature p50. That tail is the successful compute+UPSERT |

The 04:09 write does not prove the 02:57 thread succeeded. It is a later
successful attempt. Most of the 72 min is an unobserved failed/abandoned
accept, not DuckDB.

The 03:44→04:09 gap is the tail of that wait. The 0815 UPDATEs are later
writes to keys first served on 08-14, not proof of `mode=rescore`.

### B5. Per-process slot + category-scoped work dirs

`_FOCUSED_FULL_IN_FLIGHT` allows one focused-full pipeline per container
process. Work dirs are `feat-{category}-*`, not per-race. `RACE_SHARDED_DO=1`
gives up to 3 shards × 3 categories, `max_concurrency=9`. Throughput can
exceed one race at a time **across shards**. One shard still serializes.

This explains busy-requeue more than it explains a single race's compute time.

`build_pipeline` is also **not a resume API**. It always `_reset_category_work_dirs`
(rmtree `feat-{category}-base` and `feat-{category}-layer-*`) before the base
build. A fresh `predict_upcoming.py` start tonight would rebuild from layer 0,
not continue at layer 8. Manual layer continuation and the orchestrator are
not interchangeable.

## What is _not_ the bottleneck

- Scoring / model load (sub-second to ~1s).
- UPSERT SQL shape (`ON CONFLICT DO UPDATE` is cheap vs 8–10 min of DuckDB).
- D1 `finish_position_cron_executions` (unused for per-race).
- Lowering DuckDB below 1.5.5 (IcebergScan serialization missing).

## Optimization order after JRA host generation finishes

1. **Make internals visible** (Phase 3 design, implement only after gate):
   - Always write layer timing (lift debug gate, or write even when debug is off).
   - Always emit NDJSON `{type, stage, layer, elapsed_s}` on layer done.
   - Worker logs those lines without `message.debug`.
   - HTTP path writes a **per-race** audit (`started_at`, `ended_at`,
     `duration_ms`, `mode`, `path=full|split|rescore`, `keibajo`, `race`).
2. **Log-verify locally** against 08-15 logs + a single dry run that does
   not contend with host JRA (Phase 3/4). Compare full chain vs split
   2nd-race on a throwaway image tag.
3. **Only then** decide whether to enable `DAY_BASE_SPLIT_ENABLED=jra` or
   change code. Flag flip alone is not a speed claim until a 2nd-race
   watermark hit is observed.

## Phase gate

No production env change. No image. No `COORDINATOR_ENABLED` /
`RESCORE_ENABLED` edit. Phases 5–6 stay blocked.
