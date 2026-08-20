# Healthy feat-cache seed for 20260816 (07:50 JST)

Purpose: weight-triggered `mode=rescore` must HIT a **live-pedigree**
parquet instead of CacheMissing into `--target-race` LAYER_CHAIN (which
kills the five JRA sire/damsire components).

## What was written

`wrangler r2 object put --remote` into `pc-keiba-features-archive`
(`FEATURES_CACHE`). Direct SigV4 PUT is 403 (read-only token; same as
`41adee27`).

Source: tonight’s full-day host parquet (`feat-jra-layer-16`,
`feat-nar-v7-final`, `feat-ban-ei-v7-final`), split with DuckDB
`COPY … WHERE race_id = ?` (same as `_split_parquet_by_race`).
`race_year` hive column is not in the object (hive_partitioning=false).

**80 / 80 PUT ok.** Includes overwrite of the 10 previous HITs.

Pre-overwrite copies of the degenerate JRA HITs (do not delete):

- `r2-hit-before-overwrite-0816/jra-04-12.parquet`
- `r2-hit-before-overwrite-0816/jra-07-01.parquet`

Verify GET `jra/04/01`: 13×390, `pedigree_score_for_race` **13/13 > 0**.
GET `jra/04/12` after overwrite: 15×390, score **15/15 > 0**.

## 04/12: old HIT vs new HIT (why the 27+ cols differ)

Join 15/15 horses. Mismatch cols **46** (rtol 1e-5).

**(a) Pedigree family — 28 cols.** Expected. Old HIT was `--target-race`
base (0.0/NaN scores, ranks only `{1,8}`). New object is full-day base
(dense scores, ranks 1..15). Rank moved on **14/15** horses; max abs **13**.
This is the quality fix.

**(b) `apply_fresh_snapshots` overwrite set — 0 of the 46.**
Those five are `tansho_odds`, `tansho_ninkijun`, `odds_score`,
`popularity_score`, `weight_diff_from_avg`. They did not differ. Rescore
will still refresh them from live odds/weight on HIT.

**(c) Other — 18 cols.** Not late-binding. Groups:

- `same_*_place2_rate` / `log1p_same_*_starts` / pair `log1p_*` (NaN vs
  number): same `--target-race` history shrink that emptied sire
  aggregates (only the race’s own horses in `rec`). Full-day has starts.
- `jk_venue_nichime_*` / `jk_fullcell_*` / their in-race ranks (3 horses,
  rank max abs 2): jockey-cell layer on a full-day vs focused rec. Not
  pedigree score; small vs the pedigree rank-13 move.

No leftover column without a family. Did not keep old production values
on (c): mixing a dead-pedigree HIT with a few live history cols would
leave the CacheMiss path’s worse vector.

## Weight path now

`triggerRescoreAfterWeights` → scoped `mode=rescore` → R2 HIT →
`apply_fresh_snapshots` overlays bataiju/odds on this healthy vector →
UPSERT. It should **not** rebuild LAYER_CHAIN with `--target-race`.

Not timed: dequeue wait can still be long (same queue, no priority).
HIT only helps **after** dequeue.

## Seeded keys

All 36 JRA + 32 NAR + 12 Ban-ei under
`feat-cache/catalog-v1/{jra|nar|ban-ei}/20260816/{keibajo}/{bango}/features.parquet`.
