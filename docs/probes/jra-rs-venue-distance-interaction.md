# Probe: JRA running-style × venue × distance × surface interaction

**Date:** 2026-06-19 **Verdict:** REJECT (partial ρ gate FAIL, all 3 formulations)

## Hypothesis

Running-style effectiveness varies by track geometry (e.g. Tokyo 1600m turf favors
closers; Nakayama 1200m dirt favors front-runners). A per-horse "style-venue advantage"
cross feature should add signal beyond existing style/odds features.

## Data facts (must-know)

- Feature store: `tmp/v8/feat-jra-v8-iter19-kohan3f-going/race_year=*/data_0.parquet`,
  263 cols, 941,970 usable JRA rows (keibajo 01-10), 2007-2026.
- **`rs_predicted_class` / `rs_p_*` are 90.2% NULL (100% NULL in several years)** — the
  team-lead's suggested `rs_predicted_class` is UNUSABLE. (Confirms memory
  `project_relationship_perclass_investigation_2026_06_12`: store rs*p*\* is a build artifact.)
- **`target_running_style_class`** (realized style, 0=逃/1=先/2=差/3=追) is 57.9% NULL and is
  ground-truth/leakage — used only to build the reference table, never as a predictor.
- **PG `jvd_se.kyakushitsu_hantei`** is JRA's official style judgment (0=unknown 470k,
  1=逃 220k, 2=先 636k, 3=差 745k, 4=追 755k). No corner derivation needed.
- Usable predicted-style proxy = `past_{nige,senkou,sashi,oikomi}_rate_self` (39% NULL,
  debut horses); expected style = argmax. Coverage 60.9% of rows.

## Method

1. Leave-one-year-out (LOYO) advantage table: mean `finish_norm` per
   `(keibajo, track_code, dist_band, realized_style)`, support ≥200, so a row's own year
   never feeds its reference (no leakage).
2. Assign each horse expected style from historical propensity argmax.
3. Cross → 3 feature formulations.
4. Partial Spearman ρ vs `finish_norm`, controls = `odds_score + past_nige_rate_self +
field_nige_pressure` (`rs_p_nige` substituted by `past_nige_rate_self` — rs_p_nige is 100% NULL).
   DuckDB memory_limit=4GB, threads=4. n = 333,579.

## Results

| feature                               | raw ρ   | partial ρ   | gate (≥0.08) |
| ------------------------------------- | ------- | ----------- | ------------ |
| relative adv (style mean − cell mean) | +0.0457 | **−0.0245** | FAIL         |
| absolute LOYO style mean              | +0.0473 | **−0.0221** | FAIL         |
| confidence-weighted advantage         | −0.0148 | **+0.0211** | FAIL         |

Style-advantage spread within (keibajo,track,band) is real: median 0.155, p90 0.459
finish_norm units — so the interaction physically exists, but it is **fully redundant**
with existing controls (|partial ρ| ≤ 0.025, sign even flips). Raw ρ is itself <0.05.

## Conclusion

REJECT — do not build this feature. The venue×distance×surface style interaction is
already absorbed non-linearly by the store's existing `same_track_win_rate`,
`horse_{track,distance,keibajo}_corner_1_norm_avg`, `field_*_pressure`, and odds. This is
the same outcome and mechanism as the 2026-06-12 relationship/per-class campaign: **partial
ρ is necessary-not-sufficient**, and here the feature fails even at the ρ stage. No model
verification warranted. JRA stays at the empirical frontier for this lever.
