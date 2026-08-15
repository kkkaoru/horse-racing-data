# Container observability design (2026-08-16)

Design only. No implementation tonight. No deploy.

`wrangler tail` on `finish-position-cron` showing 0 exceptions is not a
healthy container. It is the parent Worker. Python stdout never reaches
it. That gap stopped three investigations tonight.

## What was invisible (three concrete stops)

### 1. Which catalog the 04:09 04/12 writer attached

R2 HIT `feat-cache/catalog-v1/jra/20260816/04/12/features.parquet`
Last-Modified 04:09:35 JST. Current Neon 04/12 is the 05:04 host UPSERT.
Pedigree on that HIT is 0.0/NaN. Host `feat-jra-base` at 03:55 has
15/15 finite scores. `jvd_um` for those horses is dated 20260720–20260810.

Missing: the focused-full job's `SOURCE_DATABASE_URL` attach log, DuckDB
catalog snapshot id, and whether `stage_horse_pedigree` saw 15 sires.
HEAD headers do not carry that. Without container stdout we cannot tell
"same catalog, writer bug" from "different catalog generation".

If those three lines had been durable: the late-master hypothesis dies
in one query, not a second investigation (`ae3bd052`). **Saved ~40 min.**

### 2. `wrangler tail` 0/0 while 04/01 stayed MISS

04/01 focused-full accepted 05:51:12. At +20 min Neon was still the
05:04 host row. Other races completed in between. Worker logs: no
exception, often no line (observability `head_sampling_rate: 0.1`).

Focused-full returns `accepted` before the pipeline finishes
(`predict_lib.serve`). Child stdout/stderr is discarded unless
`PREDICT_DEBUG_LOGS=1` (tests lock that contract). HTTP `/predict`
never writes `finish_position_cron_executions` (CLI `main()` only).
`prediction_generated_at` is `now()` on UPSERT — one end timestamp,
no start.

Missing: accept clock, layer start, layer fail, detach-thread death.
We spent the 02:35–05:04 window guessing container busy vs Neon RO vs
queue skip. **Saved ~90 min** of that stall classification, and the
20 min 04/01 watch could have been a 2 min D1/Neon lookup.

### 3. PREWARM `success` with no R2 object

08-15 GraphQL: cron fired `success`. Objects 404. Cause candidates
were (a) `except BaseException: pass` on `parquet_payload_fn` then
`status=success` without `parquetBase64`, or (b) timeout with no last
line. Worker `handlePrewarmResponse` treated missing `parquetKey` as
success.

Code can now fail those paths (`cd90cb73`, `3cd71358`) — **not
deployed**. Even after deploy, we still cannot see _which_ layer of
DAY_CHAIN died, or the payload byte count, without a durable row.

If PREWARM had written `(runYmd, category, status, parquetKey, bytes,
error, elapsed)` to D1: 08-14 `internalError` vs 08-15 silent miss
would have been one SELECT at 05:22, not a failure-site doc.
**Saved ~30 min.**

## What already exists and why it did not help

| mechanism                                | tonight                                                                                                                                     |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Worker `observability` + `wrangler tail` | samples 10% of Worker logs; no container stdout                                                                                             |
| `PYTHONUNBUFFERED=1`                     | process is unbuffered; nobody reads the pipe                                                                                                |
| `PREDICT_DEBUG_LOGS`                     | off in production; tests require suppress                                                                                                   |
| NDJSON progress / keepalive              | focused-full detaches; stream ends at `accepted`                                                                                            |
| `finish_position_cron_executions`        | HTTP path does not insert                                                                                                                   |
| `_debug_finish_position_layer_timing`    | always-on write in code; **0 rows in last 48h** (prewarm-status note). Swallow-on-error. Invisible failure of the one durable clock we have |
| CF `containers` dataset                  | cited in 2026-06 pilot (`applicationId a0348266`). No `wrangler` query. GraphQL/Logpush only. Not used tonight                              |

Unobservability is intentional in the test suite (Phase 3), plus a
second failure: the Neon timing table that was supposed to bypass
tail wrote nothing and swallowed why.

## Options (do not implement tonight)

### A. Neon/D1 job + layer audit (recommended first)

Always insert, never behind debug:

1. One **job** row at accept: `run_id`, category, keibajo, race, mode
   (focused-full / prewarm / rescore), `accepted_at`.
2. One **layer** row per script: script, status, elapsed, error class
   (no secrets). Same shape as `_debug_finish_position_layer_timing`
   but fail **loud** if the write fails (log + job.status=degraded).
3. One **close** row: neon_rows, parquetKey or `-`, catalog attach
   fingerprint (warehouse + a hash of `data_sakusei` watermark, not
   the URL password).

Cost: one extra Neon/D1 write per layer (~17 JRA). Milliseconds vs
minutes of DuckDB. Needs a migration + image. Deploy only in a
confirmed window. Do not DELETE. Coverage tests for "write failed →
not success".

This is the only option that answers (1) catalog generation and (2)
04/01 MISS without tail.

### B. Forward a thin stderr summary on the NDJSON stream

Keep `PREDICT_DEBUG_LOGS` off for child DuckDB noise. Yield one
NDJSON `progress` line per layer from the existing keepalive thread
(`iter_predict_chunks` / `iter_prewarm_chunks` already have
`_iter_keepalive`). Focused-full must **not** close the stream at
`accepted`, or must write the same lines to A even after detach.

Cost: Worker CPU + `renewActivityTimeout` already paid. Still loses
history when nobody is tailed. Sampling 0.1 still drops lines.
Complement to A, not a replacement.

### C. Logpush / Workers Logs / containers dataset

Push Worker + container logs to R2. Retrospective, high cardinality.

Cost: billed events. Repo already capped `head_sampling_rate` at 0.1
after `cloudflare_observability_cost`. Turning sampling to 1.0 plus
container dataset without a filter repeats that bill. Use only after
A exists, with a allowlist of `[pipeline]` / `[day-base-prewarm]` /
`[predict-serve]` prefixes.

`wrangler` cannot query historical logs (`09-cf-logs-per-race-evidence.md`).
Logpush or GraphQL is required. That is an account-level setup, not a
code fix.

### D. R2 execution log object per race

`feat-logs/catalog-v1/{cat}/{ymd}/{keibajo}/{race}.ndjson` written
through the same FEATURES_CACHE proxy as parquet.

Cost: another base64 NDJSON on the result line; focused-full still
needs pickup. Useful as a forensic dump, worse than A for "is 04/01
alive right now".

## Cloudflare Containers constraints (do not pretend otherwise)

- **`wrangler tail` is the Worker isolate**, not the container PID.
  There is no `docker logs` equivalent in the CLI we have.
- **`containerFetch` / held HTTP** is the production contract. A
  detached focused-full thread is invisible the moment the response
  ends. Keepalive only works while the stream is open.
- **`sleepAfter` tracks request activity**, not CPU inside the image
  (containers gotchas). A silent 10 min DuckDB job can look idle.
- **Secrets are write-only.** We cannot log `SOURCE_DATABASE_URL`.
  Log a fingerprint (catalog warehouse public id + watermark), never
  the token.
- **Workers Logs sampling is a billing control.** Do not set
  `head_sampling_rate: 1` as the first fix.
- **Container log dataset exists** (pilot 2026-06) but is not wired
  into this team's daily path. Treat it as option C, not as "already
  on".

Not claimed: that CF cannot surface container stdout at all. Claimed:
**this repo and this CLI cannot see it tonight**, and tests encode
that.

## Suggested order after racing hours

1. Make `_debug_finish_position_layer_timing` fail loud and confirm
   why 48h is empty (writable belt? table missing? swallowed). That
   is a read-only diagnosis first.
2. Implement A (job/layer/close) with tests. Deploy in the same
   window as DuckDB 1.5.5, not with `DAY_BASE_SPLIT_ENABLED`.
3. Add B layer progress on prewarm + focused-full pickup, so tail
   during an incident shows movement.
4. Leave C/D until A has a week of rows.

Do not enable split or seed feat-cache to compensate for a collapse
we still cannot watch from the Worker.
