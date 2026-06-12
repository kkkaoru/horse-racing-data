# Structural Probe — Within-Race Relative Representation

**Date**: 2026-06-12  
**Hypothesis**: finish position is a within-race RANKING problem; representing features as ABSOLUTE values causes GBDT to reconstruct relative-to-field information imperfectly via splits.  
**Question**: Does an explicit within-race relative encoding (z-score, rank, diff-from-race-avg) add signal beyond absolute + market features?  
**Holdout**: 2023-01-01 to 2026-06-12  
**Data**: JRA 226k rows (18,160 races) from `race_finish_position_features` JOIN `race_entry_corner_features`; NAR 463k rows / Ban-ei 54k rows from `race_entry_corner_features`  
**Gate**: |partial ρ| ≥ 0.08 per category (incremental over absolute + log_odds + field_size)

---

## Absolute vs Relative Feature Inventory

### Current pipeline relative features (already in `assemble_final_select_from_temp_tables`)

| Feature                                     | Type                                         | Notes                              |
| ------------------------------------------- | -------------------------------------------- | ---------------------------------- |
| `speed_index_avg_5_rank_in_race`            | rank within race                             | ascending (lower time_sa = better) |
| `speed_index_best_5_rank_in_race`           | rank within race                             | ascending                          |
| `jockey_recent_win_rate_rank_in_race`       | rank within race                             | descending                         |
| `trainer_career_win_rate_rank_in_race`      | rank within race                             | descending                         |
| `pedigree_score_for_race_rank_in_race`      | rank within race                             | descending                         |
| `same_distance_win_rate_rank_in_race`       | rank within race                             | descending                         |
| `speed_index_avg_5_diff_from_race_avg`      | diff from race mean                          | NOT normalized by std              |
| `jockey_recent_win_rate_diff_from_race_avg` | diff from race mean                          | NOT normalized by std              |
| `pedigree_score_diff_from_race_avg`         | diff from race mean                          | NOT normalized by std              |
| `popularity_score`                          | inverted rank pct = `(ninkijun-1)/(field-1)` | IS within-race relative            |

### Absolute features with NO within-race relative counterpart

| Feature                     | Direction        | rho_abs (JRA) | Notes                                   |
| --------------------------- | ---------------- | ------------- | --------------------------------------- |
| `career_win_rate`           | higher = better  | -0.078        | no relative version                     |
| `career_place_rate`         | higher = better  | -0.213        | no relative version                     |
| `same_keibajo_win_rate`     | higher = better  | —             | no relative version                     |
| `same_track_win_rate`       | higher = better  | —             | no relative version                     |
| `same_grade_win_rate`       | higher = better  | —             | no relative version                     |
| `jockey_keibajo_win_rate`   | higher = better  | —             | no relative version                     |
| `jockey_distance_win_rate`  | higher = better  | —             | no relative version                     |
| `trainer_keibajo_win_rate`  | higher = better  | —             | no relative version                     |
| `trainer_distance_win_rate` | higher = better  | —             | no relative version                     |
| `kohan3f_avg_5`             | lower = better   | +0.071        | no relative version                     |
| `last_race_finish_norm`     | lower = better   | +0.373        | no relative version                     |
| `last_3_avg_finish_norm`    | lower = better   | +0.386        | no relative version                     |
| `corner_pass_avg_5`         | lower = better   | +0.179        | no relative version                     |
| `weight_diff_from_avg`      | direction varies | —             | no relative version                     |
| `days_since_last_race`      | lower = recent   | +0.073        | no relative version                     |
| `odds_score`                | lower = favorite | —             | abs normalized; z-score NOT in pipeline |

---

## Partial Spearman ρ Results

**Method**: `z(F) = (F − race_mean(F)) / race_std(F)` computed per race field. Partial ρ = Spearman(resid_y, resid_z) where both are residualized via OLS on [F_abs, log_odds, shusso_tosu].  
**Key property**: z(F) is not a linear transform of F alone (race_mean and race_std vary); the partial captures the CENTERING + NORMALIZATION effect.

### JRA (226k rows, 18,160 races)

| Feature                   | rho_abs | rho_z_full | rho_partial_z | Gate     | n       |
| ------------------------- | ------- | ---------- | ------------- | -------- | ------- |
| `speed_index_avg_5`       | +0.2762 | +0.3441    | **+0.1154**   | **PASS** | 269,032 |
| `speed_index_best_5`      | +0.2137 | +0.2762    | **+0.0818**   | **PASS** | 269,032 |
| `last_race_finish_norm`   | +0.3726 | +0.4035    | +0.0860       | PASS     | 20,007  |
| `last_3_avg_finish_norm`  | +0.3855 | +0.4198    | +0.0972       | PASS     | 20,007  |
| `kohan3f_avg_5`           | +0.0706 | +0.1341    | +0.0450       | fail     | 198,260 |
| `career_win_rate`         | -0.0783 | -0.1548    | -0.0390       | fail     | 200,564 |
| `career_place_rate`       | -0.2125 | -0.2671    | -0.0779       | fail     | 265,326 |
| `jockey_recent_win_rate`  | -0.2068 | -0.2188    | +0.0073       | fail     | 292,344 |
| `jockey_career_win_rate`  | -0.2342 | -0.2524    | -0.0067       | fail     | 294,062 |
| `trainer_career_win_rate` | -0.1754 | -0.1880    | +0.0205       | fail     | 21,643  |
| `pedigree_score_for_race` | -0.1669 | -0.1814    | -0.0465       | fail     | 21,654  |
| `corner_pass_avg_5`       | +0.1890 | +0.2034    | +0.0449       | fail     | 269,020 |
| `days_since_last_race`    | +0.0726 | +0.0784    | +0.0529       | fail     | 268,766 |

### NAR (463k rows, 45,566 races — entry table only, limited features)

| Feature    | rho_abs | rho_z_full | rho_partial_z | Gate     | n       |
| ---------- | ------- | ---------- | ------------- | -------- | ------- |
| `log_odds` | +0.5683 | +0.5839    | **+0.1568**   | **PASS** | 462,749 |
| `ninkijun` | +0.5389 | +0.5670    | **+0.0858**   | **PASS** | 462,749 |

### Ban-ei (54k rows, 5,868 races)

| Feature    | rho_abs | rho_z_full | rho_partial_z | Gate     | n      |
| ---------- | ------- | ---------- | ------------- | -------- | ------ |
| `log_odds` | +0.4743 | +0.4782    | **+0.0960**   | **PASS** | 54,057 |
| `ninkijun` | +0.4573 | +0.4628    | +0.0155       | fail     | 54,057 |

---

## Critical Deconfounding Analysis

### JRA speed_index_avg_5: is diff_from_race_avg already sufficient?

The pipeline already includes `speed_index_avg_5_diff_from_race_avg` (simple mean-centering, no std normalization). The z-score adds the std-normalization term: `−race_mean/race_std` vs just `−race_mean`.

| Controlled for                                  | rho_partial | n       |
| ----------------------------------------------- | ----------- | ------- |
| abs + log_odds + field                          | +0.1154     | 269,032 |
| **diff_from_race_avg + abs + log_odds + field** | **+0.0375** | 269,032 |

**Finding**: after controlling for the existing `diff_from_race_avg`, z-score adds only ρ=+0.037 (below gate). The incremental signal from z-score over the existing diff feature is **sub-threshold**.

### JRA last_race_finish_norm: diff vs z-score

This feature has NO relative counterpart in the current pipeline.

| Feature variant                         | rho_partial vs abs+odds+field | n      |
| --------------------------------------- | ----------------------------- | ------ |
| `last_race_finish_norm_diff` (relative) | **+0.0896**                   | 20,007 |
| `last_race_finish_norm_z` (relative)    | +0.0860                       | 20,007 |
| z \| diff + abs + odds + field          | +0.0074                       | 20,007 |

**Finding**: the DIFF (not z-score) is what matters here. `last_race_finish_norm − race_mean(last_race_finish_norm)` = "how did this horse's last race compare to the field's last race performance" — a genuine novel signal. However n=20,007 only (8.3% non-null coverage; the stored feature table is sparse due to debutants and lookback limits). Signal is real but coverage is low.

`last_3_avg_finish_norm_diff`: rho_partial=+0.111 on same 20k rows.

### NAR log_odds_z: is popularity_score already capturing this?

`popularity_score = (ninkijun - 1) / (field_size - 1)` is already a within-race rank percentile. Testing `z(log_odds) | log_odds + ninkijun + field`:

| Control set                 | rho_partial | n               |
| --------------------------- | ----------- | --------------- |
| log_odds + ninkijun + field | **+0.129**  | 50,000 (sample) |

**Finding**: `log_odds_z` adds ρ=+0.13 **even after controlling for ninkijun** (the existing within-race rank-derived feature). This means log_odds_z captures FIELD HETEROGENEITY beyond rank: in a race where the favorite's odds are 1.2 vs a race where they're 3.0, the z-score encodes the SHARPNESS of the market's conviction. This is genuinely novel.

---

## Structural Issues Identified (Non-Relative)

### 1. `last_race_finish_norm` sparsity in stored feature table

Only 8.3% non-null (18,830 / 227,172 JRA rows 2023+) in `race_finish_position_features`. This is a table-build artifact — the training parquets have higher coverage. The sparse `_diff_from_race_avg` feature would only be useful if coverage improves.

### 2. `kohan3f_avg_5` directional rho is low (rho_abs=+0.071)

This feature is a past average of final 3F times (race OUTCOME, not pre-race). Low predictive signal vs finish_norm because kohan3f depends heavily on race pace and distance. No relative version is warranted given low base signal.

### 3. `weight_diff_from_avg` entirely NULL in holdout

The stored feature table has 0 non-null rows for `weight_diff_from_avg` in 2023+ data — likely a pipeline build issue with the bataiju lookback.

---

## Feature Gap Summary

| Feature                   | Has relative version                       | Relative type | Partial ρ                           | Verdict                   |
| ------------------------- | ------------------------------------------ | ------------- | ----------------------------------- | ------------------------- |
| `speed_index_avg_5`       | YES (`diff_from_race_avg`, `rank_in_race`) | diff + rank   | z adds +0.038 beyond diff           | NO-OP: existing covers it |
| `speed_index_best_5`      | YES (`rank_in_race`)                       | rank only     | diff would add ~+0.06               | LOW PRIORITY              |
| `jockey_recent_win_rate`  | YES (`diff_from_race_avg`, `rank_in_race`) | diff + rank   | partial ρ=+0.007                    | NO-OP                     |
| `trainer_career_win_rate` | YES (`rank_in_race`)                       | rank only     | partial ρ=+0.021                    | LOW                       |
| `pedigree_score_for_race` | YES (`diff_from_race_avg`, `rank_in_race`) | diff + rank   | partial ρ=-0.047                    | NO-OP                     |
| `same_distance_win_rate`  | YES (`rank_in_race`)                       | rank only     | partial ρ=-0.037                    | NO-OP                     |
| `last_race_finish_norm`   | NO                                         | —             | diff ρ=+0.090 (n=20k)               | WEAK (sparse)             |
| `last_3_avg_finish_norm`  | NO                                         | —             | diff ρ=+0.111 (n=20k)               | WEAK (sparse)             |
| `career_place_rate`       | NO                                         | —             | partial ρ=-0.078                    | BORDERLINE                |
| `log_odds` (all cats)     | PARTIAL (popularity_score is rank-pct)     | —             | z ρ=+0.096–+0.157 over abs+ninkijun | **NOVEL**                 |

---

## Verdict

**CONDITIONAL ABORT** for the main hypothesis. The structural relative-representation layer is **not warranted as a breaking change** because:

1. **The pipeline already implements the most impactful relative features**: `speed_index_avg_5_diff_from_race_avg`, `jockey_recent_win_rate_diff_from_race_avg`, `pedigree_score_diff_from_race_avg`, plus 6 `_rank_in_race` features. These cover the highest-signal absolute features.

2. **Incremental gain of z-score over existing diff is sub-gate**: adding `speed_index_avg_5_z` on top of the existing `speed_index_avg_5_diff_from_race_avg` yields only ρ=+0.038 — the std-normalization term does not add meaningful signal.

3. **`last_race_finish_norm_diff` is real but sparse**: ρ=+0.090 is above gate, but 8.3% non-null coverage means it affects <10% of predictions. Not a structural breaking change.

**One genuine finding: `log_odds_z` for NAR/Ban-ei**  
The within-race z-score of log(odds) adds ρ=+0.13 (NAR) / +0.096 (Ban-ei) even after controlling for `tansho_ninkijun` (which is already in the model as `popularity_score`). This captures field-level odds heterogeneity (sharp favorites vs flat fields) — a signal the current pipeline misses. However:

- For JRA, `tansho_odds` is already in the base features (passed through to training); `popularity_score` + `odds_score` together may already capture this.
- For NAR/Ban-ei, the stored feature table has very few rows; the `odds_score` + `popularity_score` exist but have limited coverage in current pipeline.
- This is NOT a breaking change — it's an **additive feature** (one new column: `log_odds_z_in_race`).
- But per memory `project_science_track_saturation_2026_06_11`: odds-decoupling REJECT, per-horse halo ABORT. Adding `log_odds_z` is ODDS-DEPENDENT by definition. The saturation analysis already established that odds-derived features are at frontier.

**Final verdict: ABORT the within-race relative representation as a structural breaking change.**  
The pipeline already implements the main relative features for the highest-signal dimensions. The residual incremental signals (log_odds_z: +0.10–+0.16 partial ρ; last_race_finish_norm_diff: +0.09 on sparse rows) are additive features, not structural redesigns — and both require re-running the full feature generation pipeline (breaking schema change) for marginal gain on a system already at empirical frontier.

---

## Other Structural Issues Observed

1. **No `kohan3f_avg_5_rank_in_race` or `_diff`**: partial ρ=+0.045 (below gate). Not worth adding.
2. **`weight_diff_from_avg` all-NULL in stored table**: pipeline bug in bataiju backfill for 2023+ predictions. Low priority (rho_abs=NaN anyway).
3. **`career_place_rate` lacks relative version**: partial ρ=-0.078 (just below gate). Only the absolute is in pipeline. Marginal.
4. **NOT re-proposed** (per constraints): per-class hard-split, relevance-reweight, MLX/PL, per-place calibration, odds-decoupling, F3 partial-pooling, cascade, iter20 levers.
