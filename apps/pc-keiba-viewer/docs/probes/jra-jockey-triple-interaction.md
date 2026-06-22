# JRA Probe — Jockey × (Venue × Distance-band × Surface) triple interaction

**Date**: 2026-06-19
**Category**: JRA finish-position
**Verdict**: **REJECT** — signal is real but fully redundant with `odds_score` + existing `jockey_keibajo_win_rate`. partial ρ = **-0.006** (|ρ| ≪ 0.08 threshold).

## Hypothesis

The store already has single-axis jockey interactions
(`jockey_keibajo_win_rate`, `jockey_distance_win_rate`, `jockey_track_win_rate`,
`jockey_grade_win_rate`) but **no** 3-way interaction. A jockey may have a
specific edge at, e.g., Tokyo 1600m turf that differs from their venue-average.
Tested whether a leakage-safe as-of win rate in the exact cell
`(kishu_code × keibajo_code × kyori_band × surface)` adds signal beyond odds and
the existing venue-only jockey rate.

- `kyori_band` = {0:1000–1399, 1:1400–1799, 2:1800–2199, 3:2200+} (matches store banding, recovered from data)
- `surface` = `left(track_code,1)` (turf/dirt first-digit, matches existing `jockey_track_win_rate` convention)

## Method

- **Feature store**: `tmp/feat-jra-v8-iter18-class/race_year=*/data_0.parquet`
  (263 cols, 2006–2026, ~1.0M rows). NOTE: the requested
  `feat-jra-v8-iter19-kohan3f-going` store does not exist on disk;
  `feat-jra-v8-iter18-class` is the 263-column store carrying the named jockey
  features and was used instead.
- **History**: PG `jvd_se ⋈ jvd_ra` (jvd_se has `kishu_code`/`kakutei_chakujun`
  but **not** `kyori`/`track_code`; those come from `jvd_ra`). JRA only
  (keibajo 01–10), placed finishers 1–18.
- **As-of, leakage-safe**: replicated the existing jockey-feature pattern — for
  each target race, win rate over the jockey's races strictly before the race
  date (`hist.d < target.d`) in the same triple cell. DuckDB
  `memory_limit='4GB', threads=4`.
- **partial ρ**: rank-transform all variables, residualize `triple_rate` and
  `finish_position` on `[odds_score, jockey_keibajo_win_rate]`, Spearman of the
  residuals.

## Results

| metric                                        | value                   |
| --------------------------------------------- | ----------------------- |
| JRA history entry rows                        | 1,841,955               |
| eval rows (finish & odds not null)            | 990,719                 |
| **coverage: prior_starts ≥ 1**                | **98.57%**              |
| **coverage: prior_starts ≥ 5**                | **95.04%**              |
| raw Spearman (triple_rate vs finish)          | **-0.2084**             |
| **partial ρ \| (odds, jockey_keibajo) — ALL** | **-0.0060** (p=2.7e-09) |
| partial ρ \| (odds, jockey_keibajo) — cov ≥ 5 | -0.0055                 |
| partial ρ \| jockey_keibajo only              | -0.0391                 |
| partial ρ \| odds only                        | +0.0104                 |

Robustness (partial ρ stays ~-0.005, never near 0.08):
cov≥10 = -0.0050, cov≥20 = -0.0057, cov≥50 = -0.0053.

## Interpretation

Coverage is **not** the blocker — 95% of JRA runners have ≥5 prior starts in the
exact triple cell, and the feature is genuinely predictive in isolation
(raw ρ = -0.21, correct direction). The signal is **already fully priced**:
controlling for `odds_score` _alone_ collapses partial ρ from -0.21 to +0.01,
and adding `jockey_keibajo_win_rate` leaves -0.006. The market and the existing
venue-only jockey rate jointly absorb the 3-way edge.

Consistent with the standing market-efficiency frontier
(`project_science_track_saturation_2026_06_11`,
`project_relationship_perclass_investigation_2026_06_12`): partial ρ being
necessary-but-not-sufficient, here it fails the necessary condition outright.

**Do not adopt.** No incremental model validation warranted.

## Independent confirmation — leave-one-year-out (2026-06-19)

Re-ran with a second leakage-safe design (LOYO instead of as-of-date) against the
**`feat-jra-v8-iter19-kohan3f-going`** store (it does exist on disk; 2007–2026).
923,146 JRA rides 2007–2025; LOYO win/avg-finish/advantage per
`(kishu_code × keibajo_code × track_code × dist_band)`, ≥10 rides in other-years
pool. Joined to store on `race_id`+`umaban` (`race_id = jra:YYYY:MMDD:keibajo:race_bango`,
reconstructed from PG; store has no `kishu_code`).

partial Spearman ρ vs `finish_position` | controls **odds_score + jockey_keibajo +
jockey_distance + jockey_track**:

| feature                  | partial ρ   | n       | verdict |
| ------------------------ | ----------- | ------- | ------- |
| jc_win (cond win rate)   | **−0.0135** | 865,827 | fail    |
| jc_fin (cond avg finish) | **+0.0631** | 865,827 | fail    |
| jc_adv (cond advantage)  | **−0.0160** | 865,818 | fail    |

Raw Spearman is strong (jc_win −0.233, jc_fin +0.274 ≈ existing per-axis rates jkw
−0.241 / jdw −0.243 / jtw −0.251) but **fully shared** — residual signal collapses
to ≤0.063 once odds + the three existing jockey rates are controlled. Both methods
and both stores agree: **REJECT, redundant.**

## Reproduce

- As-of-date: `/tmp/jra_triple_probe.py` (→ `feat-jra-v8-iter18-class`).
- LOYO confirmation: `/tmp/jra_jockey_eval.py` (→ `feat-jra-v8-iter19-kohan3f-going`,
  DuckDB postgres scanner, partial Spearman with 4 controls).
