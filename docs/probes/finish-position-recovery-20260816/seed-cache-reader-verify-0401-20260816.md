# Seed cache vs production reader (04/01, 08:21 JST)

No deploy. No DELETE. `WORK_DIR` rebound to `/tmp/fp-reader-verify`.

Path used: `_fetch_watermarked_per_race_cache` → `_load_cached_races`
(`pd.read_parquet`) → `apply_fresh_snapshots` → `score_races`. Same as
catalog `mode=rescore` after GET.

## Result: **readable**

| check                         | result                                                                                                                                                                             |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| R2 GET                        | 81153 bytes                                                                                                                                                                        |
| watermark / entry-list        | **accepted** (catalog env set; without it the check fails closed)                                                                                                                  |
| rows / races / cols           | **13 / 1 (`jra:2026:0816:04:01`) / 390**                                                                                                                                           |
| model `feature_names` missing | **0 / 250**                                                                                                                                                                        |
| late-binding cols present     | `tansho_odds`, `odds_score`, `popularity_score`, `weight_diff_from_avg` yes. `tansho_ninkijun` **absent** (cache has `tansho_ninkijun_1`, value NaN — morning build, no live odds) |
| synthetic bataiju 480         | `weight_diff_from_avg` `nan` → `20.0`                                                                                                                                              |
| `score_races`                 | **13 rows**, cell `jockey_pedigree_703`, then stage1 gate `score-spread-degraded` → `jra-cb-stage1-marketfree235-2013`                                                             |

So the seed object is not an unreadable 78-race trap. Production rescore
can HIT it and score. Odds overlay still needs a real snapshot (today’s
file has NaN tansho); that is data, not a broken parquet.

`feat-jra-*` still 19 dirs.
