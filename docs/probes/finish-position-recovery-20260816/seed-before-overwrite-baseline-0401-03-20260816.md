# Seed-before overwrite baseline (venue 04, 08:50 JST)

No deploy. No R2 DELETE.

## Neon cannot show pedigree

`race_finish_position_model_predictions` has 24 columns. No `pedigree_*`.
Usable fields: `predicted_rank`, `predicted_score`, `odds_score`,
`model_version`, `prediction_generated_at`.

## Venue 04 `generated_at` (unchanged since advisor’s 08:43 list)

| race  |   n | gen (UTC)                 | vs seed 07:48–07:50 JST |
| ----- | --: | ------------------------- | ----------------------- |
| 04/01 |  13 | 22:07:10Z = **07:07 JST** | **before** seed         |
| 04/02 |  17 | 21:33:28Z = **06:33 JST** | **before**              |
| 04/03 |  15 | 21:14:03Z = **06:14 JST** | **before**              |
| 04/04 |  13 | 20:04:03Z = **05:04 JST** | full-day host (control) |

All four currently serve `model_version=jra-cb-stage1-marketfree235-2013`
and `odds_score=0.5664` on every horse (collapsed market). Odds column
does **not** distinguish seed-before overwrite from full-day.

## Pedigree on disk

04/01–03 were **MISS** before our 07:48 seed, so there is **no**
pre-overwrite R2 object for those three. Current R2 (after seed) is
healthy on all four:

| cache |   n | `pedigree_score_for_race` pos |
| ----- | --: | ----------------------------: |
| 04/01 |  13 |                        **13** |
| 04/02 |  17 |                        **17** |
| 04/03 |  15 |                        **15** |
| 04/04 |  13 |                        **13** |

Seed-before pedigree baseline is the preserved HIT from the **same
writer family** (focused-full / `--target-race` base):

`r2-hit-before-overwrite-0816/jra-04-12.parquet` — pos **0**, null 8, zero 7.

## What 09:09+ can prove

If a **new** Neon `generated_at` appears after seed on 04/xx:

- cache still healthy (pos = n) **and** ranks move vs
  `neon-*-ranks-before-weight-*.tsv` → rescore **HIT the seed**
  (or at least did not rebuild a dead `--target-race` vector into R2).
- cache becomes 0.0/NaN like old 04/12 → seed **not** used; fallback
  PUT or another `--target-race` writer ran.

04/01–03 Neon rows themselves stay a **rank** baseline, not a pedigree
baseline.
