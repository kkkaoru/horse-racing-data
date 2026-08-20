# Per-race latency Phase 4 candidates (2026-08-16 04:51 JST)

Not executed. Host JRA generation is in-flight (layer-9 done ~04:50).
No image, no DuckDB, no deploy.

## Tonight vs Phase 1 p50 — they do not match, and that is the finding

| series                 | unit                   | measured                                  | what it pays                                            |
| ---------------------- | ---------------------- | ----------------------------------------- | ------------------------------------------------------- |
| 08-15 local logs       | one race               | p50 9.9 min JRA (`feature_seconds`, n=33) | full `LAYER_CHAIN` (17 scripts) + base, `--target-race` |
| 08-16 host 04:43–04:50 | one **day-wide** layer | 3–4 min/layer at 8GB/4 threads            | one script over the whole JRA card parquet              |

Naive product 17 × 3.5 min = 51–68 min is the **whole-card** feature wall, not
one race. Amortized over ~36 JRA races: **1.4–1.9 min/race**.

08-15 per-race p50 9.9 min is **5–7× worse** than that amortization.

Guess (unverified): a per-race layer with `--target-race` is much cheaper than
a day-wide layer (9.9/17 ≈ 35 s implied average), but the race still **re-pays
the 21y history scan 17 times**. 08-15 also ran an n4 concurrent runner, which
may have inflated the p50.

Therefore Phase 4 should not chase “make each layer 3 min → 2 min” first.
It should stop making each race pay all 17 layers.

Production policy stays **per-race enqueue** (`keibajo_code` + `raceBango`
required). The win is reducing _fixed_ work inside that unit, not switching
back to a day-scoped queue message.

Tonight’s 8GB/4t day-wide times mapped onto the existing split
(`pipeline_args.DAY_CHAIN` / `RACE_CHAIN`):

| script (JRA)                   | chain    | tonight                                                               |
| ------------------------------ | -------- | --------------------------------------------------------------------- |
| head-to-head                   | DAY      | ~3 min, RSS 6.95 GB                                                   |
| trainer                        | DAY      | ~3 min (layer-9)                                                      |
| kohan3f-going                  | DAY      | ~3 min                                                                |
| pacestyle / course-numerical   | DAY      | ~1 min each                                                           |
| similar-race / sire-venue-bias | DAY      | ~1–2 min (advisor labeled layer-15 similar; 0-index 15 is sire-venue) |
| baba-pedigree                  | **RACE** | **~4 min — heaviest tonight**                                         |
| relationship                   | **RACE** | ~2 min                                                                |
| jockey-pedigree-cell           | **RACE** | running at 05:00                                                      |
| near-miss                      | **RACE** | ~5 min (04:11–04:16, _before_ 8GB)                                    |
| market-signal                  | **RACE** | ~2 min (03:55–03:57, before 8GB)                                      |

If a focused-full race only paid `RACE_CHAIN`, the day-stable 1–3 min layers
(including h2h at 6.95 GB) would be paid **once per card/shard**, not 36 times.

Category counts are not the same (`pipeline_args.py`):

| category | LAYER_CHAIN | DAY_CHAIN |               RACE_CHAIN | 08-15 per-race p50 |
| -------- | ----------: | --------: | -----------------------: | -----------------: |
| jra      |          17 |        12 |                        5 |            9.9 min |
| nar      |          10 |         7 |                        3 |            8.5 min |
| ban-ei   |           7 |         6 | 1 (`baba-pedigree` only) |            4.8 min |

Ban-ei's shorter p50 matches the shorter chain. After split, Ban-ei would
keep only baba-pedigree per race — the largest _relative_ cut. JRA still
keeps five race-fresh scripts, including tonight's heaviest baba (~4 min).

## Ranked candidates (execution order after JRA finishes)

1. **Always-on layer timing + HTTP per-race audit**
   Prerequisite. Without this, a faster image cannot be proven. Patch is
   specified in `per-race-latency-phase3-log-verify-20260816.md`. Local
   alias tag only.

2. **Day-base once + `RACE_CHAIN` per race** (JRA: 5 scripts)
   Largest expected compute win for 2nd+ focused-full races on a live
   process/shard. `DAY_BASE_SPLIT_ENABLED` secret exists; value unread.
   `ensure_day_base` already watermark-reuses catalog sources (07-17 probe
   is stale). First race per shard still pays the day-base.

3. **Do not restart `predict_upcoming` to “resume”**
   `_reset_category_work_dirs` deletes `feat-{category}-*`. Tonight’s
   layer-7+ artifacts would be thrown away.

4. **Keep h2h / heavy layers at ≥8 GB**
   Tonight RSS 6.95 GB on h2h. 2.7–3 GB OOM is a failed accept, not a
   slow race (04/01 72 min pattern).

5. **Small**
   Failed realtime-weight JSON retries in 08-15 logs are seconds, not
   minutes. Scoring is 0.38 s p50. Do not touch DuckDB pin (`==1.5.5`).

## Phase 4 measurement plan (still blocked)

When host JRA is done and advisor unblocks:

1. Implement (1) behind existing tests that currently require `PREDICT_DEBUG_LOGS`.
2. Build `finish-position-predict-local:opt-phase4-20260816` (never overwrite
   production tags).
3. Time **one** non-JRA race or a past settled race against local PG / catalog
   without touching `/tmp/predict-upcoming/feat-jra-*`.
4. Record per-layer elapsed and wall. Compare to 08-15 p50 only as a band,
   not as the same path.
5. Stop before production. Phases 5–6 stay gated.
