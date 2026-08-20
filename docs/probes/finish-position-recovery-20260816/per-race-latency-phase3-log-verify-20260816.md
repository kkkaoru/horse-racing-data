# Per-race latency Phase 3 — log-based verification (2026-08-16)

Recorded 2026-08-16 04:44 JST by `pi-optimize-developer`.
No DuckDB, no image, no deploy. Verification uses files and Neon/D1
already collected in Phases 1–2.

## What "log-based verification" can prove tonight

The user asked to verify from logs before a new image. Production container
stdout does not reach `wrangler tail` (Phase 1 §3). So tonight's verification
is:

1. Replay 08-15 local per-race JSON as the compute baseline.
2. Replay 08-16 Neon timestamps as the production _end_ baseline.
3. Pin the exact code gates that hide internals, against tests that already
   encode those gates.

A new image is required before we can prove a speedup. This note only proves
the measurement contract.

## V1. 08-15 local logs reproduce a stable feature-bound race

Source: `docs/probes/finish-position-recovery-20260815/local-generation-logs/*.log`

Each success line is one object with `feature_seconds`, `score_seconds`,
`races=1`, `rows`, `rows_written`.

Checks that already hold:

- 65/65 success objects have `races == 1` and `rows == rows_written`.
- `score_seconds / (feature_seconds + score_seconds)` p50 ≈ 0.07%.
- JRA p50 592s, NAR p50 512s, ban-ei p50 286s.

Interpretation: a healthy per-race **full chain** on this host is ~8–10 min
for JRA, almost all DuckDB layers. Any production accept→write >> 15 min is
not explained by this compute.

## V2. 08-16 Neon timestamps cannot reconstruct start

`prediction_generated_at` is forced to `now()` on every UPSERT
(`upsert_sql.build_upsert_sql`). There is no `prediction_started_at`.

HTTP `/predict` never calls `_try_record_audit` (only CLI `main()` does).
Therefore a successful focused-full race leaves **one end timestamp**.

Verified on `jra 0816 04/01`: 13 rows, one timestamp `19:09:54.051518Z`.
That is the write instant, not accept, not feature start, not score start.

## V3. The debug gate is enforced by existing tests

These tests document the current (unobservable) contract. They must flip when
we implement always-on timing.

| test                                                            | current contract                                                 |
| --------------------------------------------------------------- | ---------------------------------------------------------------- |
| `test_run_suppresses_child_stdout_without_debug`                | child stdout discarded                                           |
| `test_run_suppresses_child_stderr_without_debug`                | child stderr discarded                                           |
| `test_run_streams_child_output_when_debug_enabled`              | streams only if `PREDICT_DEBUG_LOGS=1`                           |
| `test_record_layer_timing_row_writes_row_via_mocked_connection` | write happens **only after** env is set                          |
| `test_iter_predict_chunks_sets_debug_env_during_predict`        | HTTP debug flag is per-request env mutation                      |
| Worker `logPredictProgress`                                     | `if (message.debug !== true) return` — no test asserts always-on |

Phase 3 conclusion: unobservability is intentional in the test suite, not an
accident of tonight's incident.

## V4. 04/01 72 min is not a 72 min feature build

Replay of bounded facts (Phase 2 B4):

```
02:57 accept
03:15 still 0 rows
03:22 other FIRST writes exist
04:09:54 04/01 FIRST (13 rows, one timestamp)
```

The only interval whose length matches V1 is the last ~7.8 min before 04:09:54.
The rest is an accepted job that left no row.

This is the log-verification of B1+B4: without start/layer logs we can bound
compute from below (last gap) but cannot see the failed attempt.

## V5. What a later local log-verify run must print

When JRA host generation is done and a throwaway image is allowed, one
focused-full (or local `--target-race`) must emit **all** of:

```text
[pipeline] step=base ... status=start ...
[pipeline] step=base ... status=done elapsed_seconds=...
[pipeline] step=layer index=N/... script=... status=done elapsed_seconds=...
{"type":"progress","stage":"feature-build","elapsed_s":...}
{"type":"result","status":"success","racesPredicted":1,...}
```

And Neon must gain a row in `_debug_finish_position_layer_timing` (or the
replacement table) with `keibajo_code` + `race_bango` set, **without**
`PREDICT_DEBUG_LOGS=1`.

Pass criteria for Phase 3→4:

- layer rows exist for that one race
- `sum(elapsed_seconds)` ≈ wall from start audit to `prediction_generated_at`
- score remains < 2s
- no write to today's JRA host work dirs (`/tmp/predict-upcoming/feat-jra-*`)

Fail if any of those require `debug=1` or `wrangler tail`.

## Planned patch (not applied)

Keep the patch small enough to land behind tests already listed in V3.

1. `record_layer_timing_row`: remove the `debug_logs_enabled()` early return.
   Timing writes stay best-effort, 5s connect, swallow errors.
2. `_log_pipeline_progress`: always print. These lines are already
   structured (`step=`, `elapsed_seconds=`).
3. `run_with_stderr_capture`: keep discarding noisy child output by default
   (volume). Do **not** stream every DuckDB line to Worker logs.
4. `build_progress_line` / layer loop: emit one NDJSON progress line per
   layer done (`stage=layer:<script>`, `elapsed_s=layer`, plus cumulative).
5. `logPredictProgress`: log `type=progress` always; keep debug-only for
   verbose fetch start/end.
6. HTTP success/error path: write a per-race audit (new columns or a new
   insert-only table). Do not overload `finish_position_cron_executions`
   (per-run schema, no keibajo).

Image tag for Phase 4 must be a **local alias**, never the production
`:953d086b` / registry tag.

## Still blocked

- Phase 4 image/container timing: wait for JRA host generation to finish.
- Phase 5–6 production verify/swap: advisor gate.
- `DAY_BASE_SPLIT_ENABLED` value: secret exists; reading it is not required
  for Phase 3. Do not `secret put`.
