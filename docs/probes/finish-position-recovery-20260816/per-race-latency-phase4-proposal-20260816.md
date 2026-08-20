# Phase 4 proposal — keep per-race predictions, share DAY features (2026-08-16)

Status: **proposal only**. Not implemented. Host NAR generation was live at
05:31 JST (`predict_upcoming.py` scoring 32 NAR races). No second pipeline
was started.

Production policy stays: enqueue + score + Neon UPSERT are **one race**
(`keibajo_code` + `raceBango` required). Day-scoped prediction cron stays
refused.

## 0. What “DAY share” is

Shared object = **feature parquet**, not predictions.

| thing               | unit           | stored                                                          |
| ------------------- | -------------- | --------------------------------------------------------------- |
| prediction rows     | one race       | Neon `race_finish_position_model_predictions`                   |
| RACE_CHAIN features | one race       | built per request                                               |
| DAY_CHAIN features  | category × day | `feat-daybase/catalog-v1/{category}/{runDate}/features.parquet` |

A race request should: read day-base → run that race’s RACE_CHAIN → score
that race → UPSERT that race. It must not write other races’ predictions.

## 1. Put / get key is from code, not invented

Python put key (prewarm payload):

- `predict_lib.serve.build_r2_day_base_key`
- `R2_DAY_BASE_PREFIX = "feat-daybase"`
- `R2_RAW_CATALOG_GENERATION = "catalog-v1"`
- key = `feat-daybase/catalog-v1/{category}/{runDate}/features.parquet`
- `predict_upcoming.py` `_prewarm_parquet_payload` sets
  `parquet_key = build_r2_day_base_key(category_str, run_date)`

Worker put (same string, no rewrite):

- `container-ndjson-proxy.ts` `FEATURES_CACHE.put(entry.key, …)`
- `entry.key` is the NDJSON `parquetKey` from the container

Python get / HEAD (same helper):

- `pipeline_runner.ensure_day_base` → `build_r2_day_base_key` →
  `r2_head_watermark` / `r2_get_parquet`

HEAD at 05:22 JST of that exact key for `{jra,nar,ban-ei}` ×
`{20260814,20260815,20260816}`: **9/9 HTTP 404**.

So the 404 is not a wrong guessed prefix.

## 2. Evidence tonight’s per-race path recomputes DAY layers

1. No day-base object for 08-14..16 (above).
2. No local `/tmp/predict-upcoming/daybase-*`.
3. 08-15 local per-race logs have no `step=daybase` / `step=racechain`.
   They are full-chain `feature_seconds` (JRA p50 9.9 min).
4. `is_day_base_split_enabled` is an allowlist. Secret name exists; value
   unread. Empty → every focused-full race runs full `LAYER_CHAIN`.
5. Even if the allowlist were on, `ensure_day_base` would miss (no local
   dir, R2 404) and either inline-build the whole day-base or fall back to
   full `LAYER_CHAIN`.

Tonight’s 9.9 min/race is **not** “RACE_CHAIN after a warm day-base”.
It is consistent with paying DAY+RACE every race.

## 3. PREWARM cron cannot feed the first JRA post

`wrangler.jsonc` `30 0 * * *` is Cloudflare UTC = **09:30 JST**.
First JRA post tonight is **09:40 JST**.

Even a successful prewarm starts ten minutes before post and must finish
a whole-card DAY_CHAIN (tonight JRA day-wide layers were 3–4 min each;
12 DAY scripts ⇒ tens of minutes). It cannot be the cache for 09:40.

Tonight at 05:31 JST the 08-16 prewarm had **not fired yet**.

## 4. Expected cut if DAY is built once (from tonight’s numbers)

| category | LAYER | DAY | RACE | 08-15 per-race p50 | if 2nd+ race pays RACE only                                                                                                                                           |
| -------- | ----: | --: | ---: | -----------------: | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| jra      |    17 |  12 |    5 |            9.9 min | remaining: baba ~4 min + near-miss (5 min before 8GB) + relationship ~2 min + market-signal + jockey-cell. **Guess: ~5–8 min** first-order, not 9.9 if DAY is skipped |
| nar      |    10 |   7 |    3 |            8.5 min | remaining: near-miss, baba, relationship. **Guess: well under 8.5**                                                                                                   |
| ban-ei   |     7 |   6 |    1 |            4.8 min | remaining: baba only (~4 min day-wide tonight). **Largest relative cut**                                                                                              |

These “after” numbers are **guesses** until a 2nd-race run is timed with
layer logs. The **ratio** is not a guess: DAY is 12/17 JRA, 7/10 NAR, 6/7
Ban-ei of the script count.

First race per process/shard still pays DAY (or a successful R2 GET).
`RACE_SHARDED_DO=1` means up to 3 shards × category can each miss once
unless R2 day-base exists.

Amortized host JRA tonight (day-wide 3–4 min/layer, 36 races) was
**1.4–1.9 min/race**. That is the shared-DAY ceiling, not a proposal to
enqueue a day-scoped prediction.

## 5. How to do this without changing per-race policy

Do **not**:

- re-enable day-scoped predict cron
- enqueue without `keibajoCode` + `raceBango`
- UPSERT a whole card from one message
- restart `predict_upcoming` to “resume” layers (`_reset_category_work_dirs`
  deletes `feat-{category}-*`)

Do:

1. **Always-on layer timing + HTTP per-race audit** so the next 72-minute
   accept is diagnosable (start/layer/end). Measurement first.
2. **Connect existing PREWARM → `ensure_day_base`**
   - confirm `DAY_BASE_SPLIT_ENABLED` allowlist (secret exists)
   - make a day-base object actually land at `build_r2_day_base_key`
   - move or add a prewarm **before** first post (09:30 JST is too late
     for 09:40). Cron time change is a production schedule change →
     Phase 5 gate.
3. **Per-race request stays focused-full**: read day-base, run
   `RACE_CHAIN` only, score one race, UPSERT one race.
4. Keep heavy layers at ≥8 GB (h2h RSS 6.95 GB tonight). That prevents
   vanished accepts, not the 9.9 min itself.

## 6. Phase gate

- Phase 4 image/timing: after NAR + Ban-ei host generation finish.
- Local alias tag only. Do not overwrite production image tags.
- Phases 5–6 (prod verify / swap): advisor + user gate. No deploy during
  09:40–20:50.
