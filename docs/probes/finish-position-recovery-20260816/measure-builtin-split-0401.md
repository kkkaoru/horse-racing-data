# Built-in DAY / RACE split timing (2026-08-16)

- status: `ok`
- day_base_seconds: `1333.8` (22.2 min; base 337.292 + DAY 1–8 780 + remaining 9–12 216)
- race_chain_seconds: `447.0` (7.45 min)
- race_chain_ok: `True`
- target: JRA `20260816` `04:01`
- day_base_dir: `/tmp/fp-builtin-split/daybase-jra-20260816/final`
- work_dir: `/tmp/fp-builtin-split`
- host_course_lookup: `apps/pc-keiba-viewer/finish-position/lookups/course-numerical-features.parquet`
- preserved `/tmp/predict-upcoming/feat-jra-*`: yes (19 dirs; layer-0 / layer-6 cksums unchanged)
- Neon write: no (`record_layer_timing_row` stubbed; catalog URL)
- R2: no (`r2_config=None`)
- production code: not edited (`COURSE_LOOKUP_PATH` rebound in the measure script only)

## DAY_CHAIN (once per category+day)

| step  | script                                    | seconds |
| ----- | ----------------------------------------- | ------: |
| base  | `finish_position_features_duckdb.py`      | 337.292 |
| 1/12  | `add-race-internal-features.py`           |   0.182 |
| 2/12  | `add-sectional-and-weight-features.py`    | 118.687 |
| 3/12  | `add-futan-juryo-features.py`             | 126.948 |
| 4/12  | `add-workout-features.py`                 |  65.296 |
| 5/12  | `add-grade-race-lineage-features.py`      | 142.512 |
| 6/12  | `add-head-to-head-features.py`            | 203.410 |
| 7/12  | `add-trainer-stable-affinity-features.py` | 122.751 |
| 8/12  | `add-pacestyle-features.py`               |   0.745 |
| 9/12  | `add-course-numerical-features.py`        |   0.086 |
| 10/12 | `add_kohan3f_going_features.py`           |  56.139 |
| 11/12 | `add-similar-race-features.py`            |  89.339 |
| 12/12 | `add-sire-venue-bias-features.py`         |  70.428 |

First attempt failed at 9/12 because `pipeline_args.COURSE_LOOKUP_PATH`
is `/app/lookups/course-numerical-features.parquet`. Host has the repo
copy only. Resume from `layer-7` after rebinding the path in the measure
script.

Tonight’s earlier full-day host base was **612 s** (03:44:53 → 03:55:05).
This run’s base **337 s** was faster, not stuck.

## RACE_CHAIN (`--target-race 04:01`)

Input was still the whole-day day-base parquet (**490 rows / 36 races**).
`--target-race` narrows PG history only.

| step    | script                                     |   seconds |
| ------- | ------------------------------------------ | --------: |
| 1/5     | `add-market-signal-features.py`            |    57.891 |
| 2/5     | `add-near-miss-features.py`                |   112.357 |
| 3/5     | `add-baba-pedigree-affinity-features.py`   |   118.184 |
| 4/5     | `add-relationship-r1-features.py`          |    92.877 |
| 5/5     | `add-jra-jockey-pedigree-cell-features.py` |    65.652 |
| **sum** |                                            | **447.0** |

08-15 local full `LAYER_CHAIN` p50 = **9.9 min**. This RACE_CHAIN =
**7.45 min**.
