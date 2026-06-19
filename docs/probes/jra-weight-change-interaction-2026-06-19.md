# JRA Probe — 馬体重 × 馬体重増減 interaction (today-vs-last weight change)

**Date**: 2026-06-19
**Category**: JRA finish-position
**Verdict**: **REJECT** — best partial ρ = **+0.047** (`layoff_change`), all 5 candidates < 0.08 gate. Signal is real (U-shape) but fully priced by `odds_score`.

## Hypothesis

The store already has 5-race-window weight features (`weight_avg_5`,
`weight_diff_from_avg`, `is_returning_from_layoff`, `days_since_last_race`) but
**no today-vs-last single-race weight change** (`zogen_sa` / `bataiju`). Tested
whether the _single-race_ weight delta and its interactions add signal beyond
the market and the existing 5-race-avg deviation:

- a. `zogen` — signed today-vs-last change (kg)
- b. `abs_zogen` — magnitude of change
- c. `wclass_x_change` — extreme change (|band|=2) weighted up for light/vheavy body types
- d. `lastfin_x_change` — last-race finish × |change band|
- e. `layoff_change` — |change| gated on returning-from-layoff

`zogen` band = {big_loss <−8, small_loss <0, same 0, small_gain ≤8, big_gain >8}.

## Method

- **Feature store**: `tmp/feat-jra-v8/race_year=*/*.parquet` (179 cols, 2016–2025,
  725,812 rows). NOTE: the requested `tmp/v8/feat-jra-v8-iter19-kohan3f-going`
  store does **not** exist on disk (same gap hit by the two other 2026-06-19
  probes); `tmp/feat-jra-v8` is the JRA v8 store carrying the named weight
  features and was used instead.
- **Weight-change source**: PG `jvd_se.zogen_sa`/`zogen_fugo`/`bataiju` (signed
  delta + raw weight), joined on
  `(kaisai_nen, kaisai_tsukihi, keibajo_code, umaban, ketto_toroku_bango)`.
  `jvd_se` is already JRA-only (no `source` column; query in the task spec
  assumed one).
- **partial ρ**: rank-transform all vars, residualize candidate and `finish_norm`
  on the controls, Spearman of residuals. DuckDB `memory_limit='4GB', threads=4`.
- **Controls**: `odds_score + weight_diff_from_avg + last_race_finish_norm`
  (`weight_trend_5` from the task spec is not in this store; `last_race_finish_norm`
  substituted for the prior-form axis).

## Results

PG aggregate (full `jvd_se`, n≈3.7M) shows a clean **U-shape**: both big_loss and
big_gain finish worse than same/small change across every weight class
(mid: same 6.68 / small_gain 6.70 / **big_gain 7.38** / **big_loss 7.59**; effect
strongest for light & vheavy). Debut (`null`) is worst. So the marginal signal is
genuinely there.

| candidate           | raw ρ   | partial ρ \| 3 controls | gate     |
| ------------------- | ------- | ----------------------- | -------- |
| a_zogen_signed      | −0.0108 | +0.0017                 | fail     |
| b_abs_zogen         | +0.0617 | +0.0283                 | fail     |
| c_wclass_x_change   | +0.0537 | +0.0261                 | fail     |
| d_lastfin_x_change  | +0.2604 | +0.0024                 | fail     |
| **e_layoff_change** | +0.0625 | **+0.0470**             | **fail** |

eval rows (finish & odds non-null, joined): **473,728**.

Robustness — `abs_zogen` / `layoff_change` under weaker single controls (never near 0.08):

| candidate     | \| odds only | \| weight_diff_from_avg only |
| ------------- | ------------ | ---------------------------- |
| abs_zogen     | +0.0327      | +0.0606                      |
| layoff_change | +0.0527      | +0.0509 (with odds)          |

## Interpretation

The signal is real but **already priced by the market**. `abs_zogen` (the
U-shape) sits at +0.062 raw, holds at +0.061 against `weight_diff_from_avg`
alone (so the 5-race-avg feature is _not_ what eats it), but collapses to +0.033
once `odds_score` is added — the market reads paddock weight. `d_lastfin_x_change`
raw +0.26 is entirely its `last_race_finish_norm` component (partial +0.002).
`layoff_change` is the strongest survivor at +0.047, still well under gate.

Consistent with the standing market-efficiency frontier
(`project_science_track_saturation_2026_06_11`,
`project_relationship_perclass_investigation_2026_06_12`): partial ρ is
necessary-but-not-sufficient, and here all five candidates fail the necessary
condition. No incremental model validation warranted. **Do not adopt.**

## Reproduce

`apps/pc-keiba-viewer/tmp/weight_probe_eval.py` (joins
`tmp/weight_probe_raw.csv` exported from PG `jvd_se` into `tmp/feat-jra-v8`,
partial Spearman).
