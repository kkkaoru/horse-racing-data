# Per-race latency Phase 1 (2026-08-16)

Recorded 2026-08-16 04:25–04:37 JST by `pi-optimize-developer`.
Read-only. No container deploy, no DuckDB rebuild, no host-generation restart.
Host JRA batch (`/tmp/predict-upcoming`, layer-7) was treated as off-limits.

## What was measured

| Item                                | Method                                                                                                                     | When      |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | --------- |
| 08-15 local per-race wall           | Parse `docs/probes/finish-position-recovery-20260815/local-generation-logs/*.log` JSON `feature_seconds` + `score_seconds` | 04:27 JST |
| Tonight Neon first/rescore arrivals | Replica R1 `BEGIN READ ONLY`, `pg_is_in_recovery=true`                                                                     | 04:30 JST |
| Layer timing table                  | Same replica, `_debug_finish_position_layer_timing`                                                                        | 04:31 JST |
| Neon audit                          | Same replica, `finish_position_cron_executions`                                                                            | 04:30 JST |
| D1 audit / DLQ / coverage gaps      | `bunx wrangler d1 execute … --remote` SELECT only                                                                          | 04:36 JST |
| Worker env surface                  | `wrangler.jsonc` vars + `wrangler secret list` (names only)                                                                | 04:35 JST |

No new per-race pipeline was started. Tonight's host batch is a 17-layer day path, not the per-race path.

## 1. Existing per-race time (08-15 local logs)

65 successful logs. Scoring is not the cost.

| category |   n |         min |         p50 |         mean |          max |
| -------- | --: | ----------: | ----------: | -----------: | -----------: |
| jra      |  33 | 462s (7.7m) | 592s (9.9m) | 607s (10.1m) | 875s (14.6m) |
| nar      |  20 |        384s | 512s (8.5m) |         518s | 693s (11.6m) |
| ban-ei   |  12 |        218s | 286s (4.8m) |         303s |  469s (7.8m) |
| all      |  65 |        218s | 511s (8.5m) |         524s |         875s |

`score_seconds`: p50 0.38s, max 1.37s.

Vs 07-11 memory of "~7 min/race, worst 27.5 min":

- Same order of magnitude.
- JRA p50 is slower than 7 min (9.9 min).
- Worst 14.6 min is better than 27.5 min.
- These numbers are **host local full `LAYER_CHAIN`**, not tonight's production container latency.

17 failed/incomplete logs on 08-15 were connection drops or `ReadOnlySqlTransaction`, not timing samples.

## 2. 25-minute gap (03:44:49 → 04:09:54 JST)

Neither "one race took 25 minutes" nor "25 minutes idle".

Classifier (fact): `first_served_at = prediction_generated_at` → FIRST insert of that key; `first_served_at < prediction_generated_at` → later UPDATE of an existing key. UPDATE is **not** proof of `mode=rescore`. It only means the row existed earlier (often yesterday's 0815 first-serve).

0816 FIRST only: `jra 04/12` 18:44:49Z → `jra 04/01` 19:09:54Z = 25m05s.

All dates in that window:

| UTC      | race           | kind    |           delta |
| -------- | -------------- | ------- | --------------: |
| 18:44:49 | jra 0816 04/12 | FIRST   |               — |
| 18:46:56 | jra 0815 04/09 | RESCORE |         +126.9s |
| 19:00:30 | jra 0815 07/02 | RESCORE | +813.9s (13.6m) |
| 19:02:03 | nar 0815 44/03 | RESCORE |          +93.2s |
| 19:09:54 | jra 0816 04/01 | FIRST   |  +470.5s (7.8m) |

Largest single interval is 13.6 minutes (`jra 0815 07/02` rescore). The following FIRST arrived 7.8 minutes later, which matches the 08-15 local JRA band.

Same pattern earlier: 17:35:51Z `nar 0816 35/01` → 18:22:07Z `jra 0816 07/01` looks like 46 minutes if 0816-only; four 0815 RESCORE writes sit in the gap.

NAR FIRST often writes 0816 and 0817 0.5s apart (`PREDICT_DAYS_AHEAD=2`). Throughput math that counts each Neon race as a separate generation overcounts.

`RACE_SHARDED_DO=1` is set in `wrangler.jsonc`. Do **not** treat the gap as proof that one container serialized all of those writes. The durable fact is only that Neon kept receiving writes.

04:30 JST Neon 0816 coverage: **17 races / 186 runners**. Latest FIRST then: `nar 83/06` 19:30:26Z.
Re-read 04:45 JST: **18 races / 199 runners**. New FIRST: `nar 44/10` 19:41:20Z.

0816 FIRST-only inter-arrival (13 intervals after the first FIRST):
min 12s, p50 654s (10.9m), mean 708s, max 2776s (46.3m before `jra 07/01`).
Six gaps ≥10 min. These are **not** per-race latencies; they include other-date
UPDATEs and vanished accepts.

## 3. Why production internals are invisible

1. `_debug_finish_position_layer_timing` exists (393 rows) but **0 rows in the last 48h**. Last write 2026-07-18.
2. `record_layer_timing_row` returns immediately unless `PREDICT_DEBUG_LOGS` is truthy.
3. That env is not in `wrangler.jsonc` vars or the Dockerfile.
4. `run_with_stderr_capture` discards child stdout/stderr when debug is off (keeps a 4000-byte tail only on failure).
5. Worker `logPredictProgress` no-ops unless `message.debug === true`.
6. Focused-full returns `"accepted"` immediately. Worker duration is accept-time, not pipeline-time.
7. `prediction_generated_at` is an end timestamp only. There is no start timestamp.

D1 `finish_position_cron_executions` still has only the six 2026-06-03 `started` rows. Migration `0006_create_prediction_monitoring.sql` is **not** applied (`d1_migrations` ends at 0005). 0816 coverage-gap and DLQ tables are empty.

Neon `finish_position_cron_executions` ids 515–518 are **host local batch failures** (argv under `/Users/kkk4oru/...`), not production container per-race audits.

## 4. Observability design (not implemented)

Priority:

1. Always-on layer timing writes (lift the `PREDICT_DEBUG_LOGS` gate on the existing table, or a durable rename).
2. Always-on NDJSON layer-done lines; Worker `console.log` without `message.debug`.
3. Per-race audit: `keibajo`, `race_bango`, `started_at`, `duration_ms`, `mode`, `path=full|split|rescore`.

Do not implement until JRA host generation has finished and Phase 5 is gated.

## 5. Bottleneck facts vs guesses

Facts:

- 08-15 local: feature build is >99% of wall time.
- Production `SOURCE_DATABASE_URL=r2-catalog://pc-keiba`.
- `DAY_BASE_SPLIT_ENABLED` exists as a Worker **secret** (value unread). It is not a `wrangler.jsonc` var.
- `ensure_day_base` no longer unconditionally returns `None` for catalog sources (watermark reuse since 2026-07-18). 07-17 probe text is stale on that point.
- Split still does nothing unless the secret allowlist contains the category.
- Historical Neon layer timing (tiny n): one complete JRA per-race run 503s; day-base h2h success p50 44s.

Guess (unverified): the largest production win is enabling day-base + `RACE_CHAIN` (JRA 5 scripts) so the 2nd+ race on a live process skips 12 day-stable layers. First race per shard still pays the day-base. Confirm the secret value and a live 2nd-race watermark hit before treating this as a plan.

## 6. Phase gate

Phases 5–6 (production verify / image swap) are blocked until the advisor opens that gate.
No new image tag and no production deploy from this track tonight.
