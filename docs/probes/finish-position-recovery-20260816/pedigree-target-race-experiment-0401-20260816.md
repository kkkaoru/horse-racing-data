# `--target-race` DuckDB base experiment (2026-08-16 07:29 JST)

No PUT / deploy. `/tmp/predict-upcoming/feat-jra-*` untouched
(`feat-jra-base` cksum still `3737377236 274531`, 19 dirs).

Command: `finish_position_features_duckdb.py --category jra
--target-date 20260816 --days-ahead 0 --target-race 04:01`
→ `/tmp/fp-pedigree-target-0401/base` (13 rows, one race). Exit 0 in 243 s.

## 5 components + score on 04/01

| column                     | experiment `--target-race 04:01` | local full-day base 04/01 |
| -------------------------- | -------------------------------- | ------------------------- |
| sire_distance_win_rate     | pos **0**/13 (null 13)           | pos 11/13                 |
| dam_sire_distance_win_rate | pos **0**/13 (null 13)           | pos 11/13                 |
| sire_track_win_rate        | pos **0**/13 (null 13)           | pos 13/13                 |
| sire_keibajo_win_rate      | pos **0**/13 (null 13)           | pos 12/13                 |
| damsire_keibajo_win_rate   | pos **0**/13 (null 13)           | pos 13/13                 |
| pedigree_score_for_race    | live **0**/13 (null 13)          | live **13**/13            |

`--target-race` on the **base** (not RACE_CHAIN) **does** kill all five
components and the score.

## vs production HIT 04/12

There is no 04/01 production HIT. Pattern vs 04/12 HIT:

|                           | experiment 04/01 | prod HIT 04/12      |
| ------------------------- | ---------------- | ------------------- |
| positives on 5 components | **0**            | **0**               |
| score positives           | **0**            | **0**               |
| exact values              | all NULL         | mix of 0.0 and NULL |

Same **degeneracy** (no live pedigree). **Not** byte-equal to the HIT
(HIT has zeros, this run is all-null). So this explains “production
per-race base has no live pedigree”, not “this run reproduced 04/12”.

RACE_CHAIN `--target-race` yesterday did **not** kill scores because
those columns were already computed on a full-day day-base.
