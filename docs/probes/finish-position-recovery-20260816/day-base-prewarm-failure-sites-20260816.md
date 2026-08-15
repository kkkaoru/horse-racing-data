# Day-base PREWARM failure sites (2026-08-16, read-only)

No deploy. Code + tonight's 404 evidence only.

## What the cron actually does

`wrangler.jsonc` `30 0 * * *` = 09:30 JST. `worker.ts` routes that
string to `runDayBasePrewarm` only.

1. Log `[day-base-prewarm] start`.
2. `enumerateTodaysRaces(D1, runYmd)`. On throw: log and **return**.
3. Distinct categories from today's card. Empty card: log skip, return.
4. Per category, `stub.fetch(http://do/prewarm-day-base?category&daysAhead&runDate)`
   on the **category-scoped** DO (`predict-jra` / `predict-nar` /
   `predict-ban-ei`). Never sharded. Never throws.
5. Container `GET /prewarm-day-base` runs `build_day_base` (DuckDB base +
   `DAY_CHAIN` only: JRA 12 / NAR 7 / Ban-ei 6 scripts).
6. Success NDJSON last line may embed `parquetBase64` +
   `parquetKey=feat-daybase/catalog-v1/{cat}/{ymd}/features.parquet`.
7. DO `fetch` wraps the stream in `proxyParquetFromNdjson`. R2 PUT happens
   only when the **last** NDJSON line is flushed and both base64 and key
   are present.

There is no D1 row. Evidence is Worker logs + the R2 object.

## Why 08-14 / 08-15 objects are 404

Past-day 404s are already measured
(`day-base-prewarm-status-20260816.md`). Ranked places the object can
fail to exist even when the cron **fired**:

| #   | site                                          | what happens                                                                                                                                                                                                      | matches                                            |
| --- | --------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| 1   | Worker / container timeout during `DAY_CHAIN` | JRA day-base is 12 layers including head-to-head (tonight RSS 6.95GB, ~3 min on host). Worker scheduled limit / DO fetch abort → GraphQL `internalError`. No last line → no R2 PUT.                               | 08-14 `internalError`; 08-15 later `internalError` |
| 2   | `iter_prewarm_chunks` swallows payload errors | `parquet_payload_fn` is wrapped in `except BaseException: pass`. Result is still `status=success` with **no** `parquetBase64`. Worker logs success. R2 PUT is skipped.                                            | 08-15 `success` + 404                              |
| 3   | `handlePrewarmResponse` never looks at R2     | It only logs the last NDJSON line. `status=success parquetKey=-` is treated as success. Silent cache miss.                                                                                                        | same                                               |
| 4   | enumerate / empty card                        | D1 throw or zero races → return before any DO fetch. Cron still "success".                                                                                                                                        | `sub=0` rows                                       |
| 5   | container 502 / `prewarm_fn is None`          | DO `fetch` catch → 502 JSON. HTTP path 404s only if `prewarm_fn` is None; production `main()` does wire it.                                                                                                       | 502 / non-ok warn                                  |
| 6   | `DAY_BASE_SPLIT_ENABLED` empty                | Even a successful R2 object is unused. `is_day_base_split_enabled` is an allowlist; unset/empty keeps full `LAYER_CHAIN` per race. Prewarm cannot reduce tonight's p50 until the secret lists `jra` (then `nar`). | structural, not the 404 itself                     |

#2 is the only path that produces **HTTP 200 + status=success + no object**
without a platform error. Confirm next from Worker logs:
`[day-base-prewarm] success ... parquetKey=-` vs a real key.

## Clock problem (independent of 404)

09:30 JST is 10 minutes before the first JRA post (09:40). Even a
healthy prewarm cannot feed 04/01 (post 09:40, cache deadline ~09:09).
Move the cron earlier (e.g. 06:30 JST) after the timeout path is fixed.
Do not do that tonight.

## Fix proposal (do not deploy tonight)

1. Stop swallowing `parquet_payload_fn` errors. Emit `status=error` so
   success cannot mean "built but not uploaded".
2. Make `logPrewarmResult` treat missing `parquetKey` on `success` as
   failed.
3. Persist a D1/audit row (`runYmd`, category, status, parquetKey,
   elapsed, error). Logs-only is how 07-12 looked like "cron never fired".
4. After (1)–(3) are in production, set `DAY_BASE_SPLIT_ENABLED=jra`
   only. Watch one day of `ensure_day_base` HIT vs inline rebuild.
5. Then move `30 0 * * *` earlier. Not before the upload is observable.

No secret change, no cron edit, no container deploy tonight.

## `feat-*-v7-final` (same night, separate question)

Measured on this host after the 0816 one-shots:

| path                                      | shape     | meaning                                                                                                                                                                                                 |
| ----------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `feat-jra-layer-16/.../data_0.parquet`    | 490 x 390 | JRA scored body (manual `_score_and_flush_races`)                                                                                                                                                       |
| `feat-nar-v7-final/.../data_0.parquet`    | 333 x 327 | NAR scored body (`current.rename(final_dir)`)                                                                                                                                                           |
| `feat-ban-ei-v7-final/.../data_0.parquet` | 117 x 271 | Ban-ei scored body                                                                                                                                                                                      |
| `feat-jra-v7-final/features.parquet`      | 1 x 8     | leftover. `race_id=jra:2026:0712:05:11`. Columns are the rescore late-binding set (`odds_score`, `popularity_score`, `tansho_odds`, `tansho_ninkijun`, `weight_diff_from_avg`, ids). Not 0816 features. |

`_final_parquet_dir` is only a rename target for the last layer. Production
scoring reads that directory after `build_pipeline` / `build_pipeline_from_day_base`.
A 1x8 `features.parquet` is the **rescore cache overlay**, not the full
pipeline output. Tonight's 80/80 rows did not come from the 1x8 file.
