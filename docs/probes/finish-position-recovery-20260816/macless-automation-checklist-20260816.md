# 8/17+ Mac-less per-race feat-cache HIT — change list

Now (08:10 JST): waiting for JRA weight (~09:09). Baselines saved for
04/01 (gen 07:07:10Z, possible dead pedigree) and 07/01 (gen 05:04:08Z,
full-day healthy). No deploy.

Mac-less HIT needs **all three**. One missing → “we shipped a fix and
nothing changed”.

## The three pieces

| #   | Piece                            | What it does                                                                                                                                                                                                  | Commit                                                                                                                                                                                 | Deployed?                                                    | Extra approval                                           |
| --- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | -------------------------------------------------------- |
| 1   | PREWARM actually writes day-base | 09:30 JST cron must PUT `feat-daybase/.../YYYYMMDD/features.parquet` with a real `parquetKey`. Today: success+404 / swallowed payload                                                                         | `cd90cb73` (payload miss → error), `3cd71358` (success without key → failed). Timeout / `DAY_BASE_SPLIT` wiring still open                                                             | **No** (live image still 08-14 `0c76062e`, “keep split off”) | Image deploy (user window). Not a secret flip            |
| 2   | `DAY_BASE_SPLIT_ENABLED`         | First race of the day builds day-base **without** `--target-race` (or PREWARM did). Later races reuse it. JRA pedigree stays live                                                                             | Code exists since 07-12 (`58cb8b93` / `is_day_base_split_enabled`). Catalog `ensure_day_base` was always-None 07-15–07-18 (`e6111ca6`); watermark reuse later (`216a2fc5`, `3d75c0d1`) | Secret **unset / empty** on purpose                          | **Yes — team-lead + user.** Spec + 08-14 restore message |
| 3   | CacheMiss fallback must not PUT  | Scoped `mode=rescore` miss rebuilds `LAYER_CHAIN`+`--target-race` (dead JRA 5 components) and **used to** embed `perRaceParquets` → Worker PUT. First generate is always MISS, so cache was born contaminated | `6793ad7f` (do not embed on scoped fallback; pickup on focused-full HTTP success). Note: that commit also contains unrelated A8 files — do not amend                                   | **No**                                                       | Image deploy only (no secret)                            |

Also on the image pile, not in the three: DuckDB pin `67440b8b`,
`PIPELINE_DIR` `2139645b`, writable txn `85bfba82`. Do not ship
`DAY_BASE_SPLIT_ENABLED` on the same deploy as the first image
(`container-deploy-window-20260816.md`).

## Required order

```
A. Deploy Worker+container image that includes PREWARM fixes + 6793ad7f
   (split secret still empty)
B. Prove PREWARM: after 09:30 JST, HEAD feat-daybase for that calendar day
   is HIT (not same-morning 404). If this fails, STOP. Do not flip (2).
C. Flip DAY_BASE_SPLIT_ENABLED=jra (then nar) after team-lead/user OK
D. Watch first focused-full of the day: day-base reuse + live pedigree
   on a 2nd race; weight rescore is R2 HIT (no LAYER_CHAIN rebuild)
```

Why this order:

- **C before B** (split on, PREWARM still broken): `ensure_day_base`
  misses → first race `build_day_base` or CacheMiss fallback. Fallback
  still uses `--target-race`. After `6793ad7f` it will **not** PUT that
  dead vector, but pedigree on the **prediction row** is still dead, and
  there is still no HIT for the next rescore. Looks like “split did
  nothing”.
- **B without A/6793ad7f** (PREWARM works, old image): a weight MISS
  still PUTs the dead `--target-race` rebuild. Cache becomes the
  8/16 04/12 object again.
- **A only** (6793ad7f + PREWARM code, split off): morning focused-full
  still runs **all** layers with `--target-race` → dead pedigree on Neon.
  Pickup can now write a cache, but the object is the dead vector.
  Weight HIT then **freezes** the death. Worse than MISS.
- **2 without 3** is the 8/16 production HIT story.

So: **never enable (2) until (1) is proven live and (3) is on the same
image.**

## If we stop halfway

| Stop after                        | What 8/17 looks like                                                                                                                                                    |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Nothing shipped                   | Same as 8/16 before Mac seed: MISS → `--target-race` full → dead JRA pedigree; weight rescore repeats that                                                              |
| Image (1+3), split off            | Pickup writes cache; content still dead if first generate is focused-full with `--target-race`. Do not treat HIT as success — check `pedigree_score_for_race` positives |
| Image + PREWARM proven, split off | Day-base object may exist but unused. Per-race still full `--target-race`                                                                                               |
| All three                         | First race pays DAY (or PREWARM). Later full / weight rescore HIT live pedigree + late-binding overlay                                                                  |

## Not in this list

- Queue priority (04/01 20 min overtake). Independent. HIT only helps
  after dequeue.
- Host / wrangler seed of 8/16 objects. One-day patch, not 8/17+.

## 09:09 observation (today, no deploy)

When JRA bataiju arrives: HEAD cache for **04/01 and 07/01**; then Neon
ranks vs the `neon-*-ranks-before-weight-20260816.tsv` baselines.
04/01 movement confounds seed vs reverting the 07:07 overwrite. 07/01
is the clean contrast.
