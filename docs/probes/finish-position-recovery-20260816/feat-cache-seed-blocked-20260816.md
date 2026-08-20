# feat-cache seed blocked (2026-08-16 05:50 JST)

No R2 PUT of a local split. Existing HIT objects were not overwritten.

## Inventory (R2 HEAD of Neon 0816 races)

73 races: **HIT 10 / MISS 63**. JRA 2/36 (`04/12`, `07/01`).

Key: `feat-cache/catalog-v1/{jra|nar|ban-ei}/20260816/{keibajo}/{bango}/features.parquet`
Written only by HTTP focused-full → Worker `FEATURES_CACHE.put`.
`_score_and_flush_races` does not write this key. CLI `main()` does not.

## Materials counted, not named

| path                                                                    |  rows |  cols |
| ----------------------------------------------------------------------- | ----: | ----: | ---------------------------------------- |
| R2 `jra/20260816/04/12`                                                 |    15 |   390 | one race `jra:2026:0816:04:12`           |
| R2 `jra/20260816/07/01`                                                 |    10 |   390 | one race                                 |
| `/tmp/predict-upcoming/feat-jra-v7-final/features.parquet`              | **1** | **8** | dummy / leftover (`jra:2026:0712:05:11`) |
| `/tmp/predict-upcoming/feat-jra-layer-16/race_year=2026/data_0.parquet` |   490 |   390 | 36 JRA races                             |

## Schema compare: layer-16 vs R2 04/12

- Column **names and order**: 390/390 equal.
- **dtypes**: 16 columns differ (`int64`/`int32` on R2 vs `float64` locally).
  Several of those have NA on 04/01 locally (`course_full_gate_count` 13/13 NA)
  so they cannot be cast to the R2 integer types.
- **Values on the same race 04/12**: not equal (e.g. pedigree ranks / sire rates).
  Tonight’s manual day-wide layer chain ≠ the object the production
  focused-full path stored earlier tonight.

## Decision

Do **not** seed from layer-16. A HIT on that object would score the wrong
vector. MISS (full rebuild) is safer.

Safe way to populate cache: one focused-full HTTP request per race (same
writer as the 10 HITs). That is a full chain per race unless day-base is
warm. Not started (Ban-ei still running; dual-start rule).
