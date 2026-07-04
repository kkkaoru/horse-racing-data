# Race-level volatility (波乱度) model — 2026-07-04

Task #36. Builds a race-level, odds-free upset-likelihood score, for task #37 to use as a
dynamic modulator of how much the finish-position model should trust the market (odds).

## Rule compliance: inputs are odds-free

Every model **input** is computable strictly pre-race from past-form data only. The only
odds-derived quantity anywhere in this pipeline is `tansho_ninkijun` (winner's popularity
rank), and it is used **exclusively to construct the training label**, never as a feature.
Two columns that exist in the augmented store were explicitly excluded because they are
odds-derived despite the `field_*` / `*_vs_field` naming: `field_dominant_favorite_indicator`
(odds*rank1/odds_rank2) and `horse_popularity_vs_field` (ninkijun/shusso_tosu). All other
`field*\*` broadcast columns used here (`field_nige_pressure`, `field_avg_speed_index`,
`field_style_diversity`, `field_spread_past_corner_1_norm`, etc.) were traced to their source
SQL in `src/scripts/finish-position-features/add-race-internal-features.py` and confirmed to
be built only from past-form columns (`speed_index_avg_5`, `career_win_rate`,
`past_corner_1_norm_avg_5`, `past_nige_rate_self`, ...) — safe.

## Data & labels

Source: `tmp/candidate-eval-jra/augmented/race_year=2013..2025` (JRA, 250+ per-horse features).
Aggregated race-level store: `tmp/candidate-race-volatility/race_level_store.parquet`
(44,907 races with a valid label, 90 columns) via
`tmp/candidate-race-volatility/build_race_level_features.py`.

- **Primary label — `label_market_upset`**: 1 if the winner's `tansho_ninkijun` >= 4, else 0.
  Base rate 35.1% (15,774 / 44,907), matching the textbook JRA favorite-win-rate distribution
  (~33% for the 1st choice, ~66% cumulative for the top 3 favorites winning → ~34% "upset").
  244 races (0.5%) had no recorded winner ninkijun and were dropped.
- **Secondary label — `label_severity_mad`**: mean absolute displacement between actual finish
  position and a simple odds-free baseline ranking (horses ranked by `career_win_rate` desc,
  tie-broken by `speed_index_avg_5` desc). Reported only, not the training target.
- **Historical cell prior** (`cell_prior_upset_rate`): expanding, date-strict, empirical-Bayes
  shrunk (k=20 toward the running global rate) upset rate for the
  (venue × class-code × distance-band) cell, computed so that same-day races never leak into
  each other. This is the one feature built from past _labels_ rather than past _form_ — legal
  because it never touches the current race's odds, only prior race outcomes, and the task
  spec explicitly calls for this as a candidate feature.

## Race-level feature set (82 features fed to the model)

- Field ability dispersion: mean/std/range of `career_win_rate`, `career_place_rate`,
  `speed_index_avg_5`, `kohan3f_avg_5`, `past_corner_1_norm_avg_5`.
- Dominance gap: `max()` of the already-existing `*_diff_from_race_avg` columns
  (`top_horse_career_win_edge`, `top_horse_speed_edge`, `top_horse_pedigree_edge`,
  `top_jockey_recent_win_edge`) — how far ahead the best horse/jockey is from the field average.
- Experience heterogeneity: newcomer count, layoff count, age std/range, days-since-last-race
  mean/std, consecutive-race-count mean.
- Style congestion: `rs_p_nige/senkou/sashi/oikomi` means + std, plus the pre-existing
  `field_nige_pressure`, `field_pace_index`, `field_has_pure_nige_horse`, `field_style_diversity`.
- Head-to-head familiarity: mean `h2h_win_rate_vs_field`, `h2h_encounter_count`.
- Program context (categorical): `keibajo_code`, `grade_code`, `track_code`, `kyori_band`,
  `season_band`, `kyoso_joken_code`, `is_grade_race`, field size (`n_horses`).
- `cell_prior_upset_rate` + `cell_prior_n_obs` (confidence in the prior).

LightGBM binary classifier, native categorical handling, `min_child_samples>=40` (races are a
much smaller, noisier population than horse rows — needed to keep the model from fitting to
race-level noise).

## Method: walk-forward, blind-holdout HPO

1. **HPO (one look, never reused)**: 4-config grid, trained on 2013-2021, selected by AUC on a
   single 2022 holdout. Selected: `num_leaves=15, lr=0.03, min_child_samples=60,
feature/bagging_fraction=0.8`. Holdout AUC 0.580.
2. **Walk-forward folds**, same frozen hyperparameters:
   | fold | fit years | early-stop holdout | test year | in-sample? |
   |---|---|---|---|---|
   | `in_sample_2013_2022` | 2013-2022 (chronological 85%) | last 15% of dates in-window | 2013-2022 (scored) | yes |
   | `oos_2023` | 2013-2022 (chron. 85%) | last 15% of dates | 2023 | no |
   | `oos_2024` | 2013-2023 (chron. 85%) | last 15% of dates | 2024 | no |
   | `oos_2025` | 2013-2024 (chron. 85%) | last 15% of dates | 2025 | no |

   Each fold's early-stop validation set is the **last 15% of dates within the training
   window, excluded from tree-fitting** — not a whole separate year, and not a subset of the
   training set. First attempt used a whole prior year as the early-stop set while _also_
   including that same year in the training set; validation wasn't actually held out, early
   stopping never triggered (`best_iteration` pinned at the 3000-round ceiling every fold), and
   in-sample AUC came out at 0.977 against 0.56-0.58 OOS — a red flag caught and fixed before
   reporting any numbers here.

## Results

| test year             | n races | base rate (upset) | **AUC**   | best_iteration |
| --------------------- | ------- | ----------------- | --------- | -------------- |
| 2023 (OOS)            | 3,456   | 34.9%             | **0.577** | 124            |
| 2024 (OOS)            | 3,454   | 32.4%             | **0.611** | 75             |
| 2025 (OOS)            | 3,455   | 33.0%             | **0.607** | 66             |
| 2013-2022 (in-sample) | 34,542  | 35.6%             | 0.642     | 124            |

Stable across 3 independent OOS years (0.577 / 0.611 / 0.607, mean 0.598) — no decay, if
anything a slight _improvement_ as the expanding training window grows. In-sample AUC (0.642)
is only modestly above OOS, i.e. limited overfit once the early-stop leak was fixed.

### Calibration (predicted decile vs actual upset rate, OOS years)

| decile      | 2023 pred / actual | 2024 pred / actual | 2025 pred / actual |
| ----------- | ------------------ | ------------------ | ------------------ |
| 0 (lowest)  | 0.218 / 0.199      | 0.238 / 0.165      | 0.236 / 0.176      |
| 3           | 0.319 / 0.361      | 0.323 / 0.263      | 0.331 / 0.275      |
| 6           | 0.369 / 0.400      | 0.368 / 0.387      | 0.368 / 0.345      |
| 9 (highest) | 0.458 / 0.399      | 0.443 / 0.477      | 0.432 / 0.474      |

Monotonic-ish, well within noise for ~345 races/decile/year; top-decile vs bottom-decile
actual-rate lift is ~2.0x (2023), ~2.9x (2024), ~2.7x (2025) — a real, usable separation
between "calm" and "chaotic" races, though far from surgical.

### Per-venue AUC (all 10 JRA venues; 02/03/10 called out as the summer venues this project has

previously focused on — [[project_season_sex_weight_probe_2026_06_20]])

| venue         | 2023  | 2024  | 2025  | mean  |
| ------------- | ----- | ----- | ----- | ----- |
| 01 (札幌)     | 0.554 | 0.610 | 0.601 | 0.588 |
| **02 (函館)** | 0.533 | 0.613 | 0.601 | 0.582 |
| **03 (福島)** | 0.550 | 0.596 | 0.617 | 0.588 |
| 04 (新潟)     | 0.582 | 0.612 | 0.603 | 0.599 |
| 05 (東京)     | 0.603 | 0.697 | 0.634 | 0.645 |
| 06 (中山)     | 0.562 | 0.546 | 0.596 | 0.568 |
| 07 (中京)     | 0.593 | 0.614 | 0.587 | 0.598 |
| 08 (京都)     | 0.548 | 0.636 | 0.632 | 0.605 |
| 09 (阪神)     | 0.562 | 0.534 | 0.592 | 0.562 |
| **10 (小倉)** | 0.584 | 0.568 | 0.572 | 0.575 |

No venue collapses toward 0.5; 02/03/10 sit close to the all-venue average (no special
degradation at the summer venues this project has repeatedly probed and mostly exhausted for
direct odds-free finish-position gains).

### Top features (gain, consistent across all 3 OOS folds)

1. `n_horses` (field size) — by far the largest driver; bigger fields → more upset risk.
2. `top_horse_career_place_edge` — the dominance gap; a clearly-superior favorite by past
   place-rate suppresses volatility.
3. `career_place_rate_std` — field ability dispersion.
4. `jockey_career_win_rate_std` — jockey-quality dispersion.
5. `h2h_encounter_count_mean` — how much head-to-head history exists in the field.
6. `field_pace_index`, `consecutive_race_count_mean`, `cell_prior_n_obs`,
   `trainer_career_win_rate_mean`, `kohan3f_avg_5_std`.

`cell_prior_upset_rate` itself ranks outside the top 20 in 2/3 folds — the historical base
rate helps but the race's own composition (field size, dominance gap, dispersion) carries
most of the signal. This is reassuring for task #37: the score is not just re-deriving a
venue/class lookup table, it's reading the actual entered field.

### External validation against the independent severity label

The model was trained only on the binary market-upset label. Its OOS predicted probability
was then checked against `label_severity_mad` (mean abs. rank displacement vs. the simple
past-form baseline ranking) — a label it never saw during training:

| year | Spearman ρ (score vs. severity) | p-value |
| ---- | ------------------------------- | ------- |
| 2023 | 0.429                           | ~5e-155 |
| 2024 | 0.487                           | ~7e-205 |
| 2025 | 0.486                           | ~2e-204 |

A strong, stable, highly significant correlation with a fully independent notion of
"how much the finish order departed from a simple form-based expectation" — good evidence the
score is capturing genuine race-composition volatility, not an artifact of the market label.

## Honest assessment for task #37

- **The signal is real and stable** (3/3 OOS years > 0.5, no decay, corroborated by an
  independent label), but it is **modest, not sharp** — AUC ~0.58-0.61 and a ~2-3x top-vs-bottom
  decile lift. This is a coarse dial, not a fine one.
- **Recommended use in #37**: bucket into 3-5 volatility tiers (e.g. quintiles of
  `volatility_score`) rather than using the raw probability as a continuous multiplier —
  the calibration is good in relative ranking terms but not precise enough in absolute terms
  to justify smooth continuous weighting.
- **Biggest single driver is field size** (`n_horses`). Task #37 should check whether this
  alone (already available without any model) already captures most of the usable variance
  before crediting the full model — worth an ablation if #37's fusion doesn't beat a
  field-size-only baseline by a clear margin.
- Per-venue and per-year stability both hold; no red flags for the specific summer venues
  (02/03/10) this project has repeatedly investigated in other odds-free work.

## Outputs

- `tmp/candidate-race-volatility/race_level_store.parquet` — race-level feature + label store
  (44,907 races, 90 cols).
- `tmp/candidate-race-volatility/volatility_scores.parquet` — **the task #37 deliverable**:
  `race_id, race_year, volatility_score, fold, in_sample` for all 44,907 races 2013-2025.
  `fold` ∈ {`in_sample_2013_2022`, `oos_2023`, `oos_2024`, `oos_2025`}; `in_sample=True` only
  for the 2013-2022 block (honest label — that block's early-stop tail was held out from
  fitting but the block as a whole is the model's own training window).
- `tmp/candidate-race-volatility/wf_report.json` — full per-fold AUC/calibration/per-venue/
  feature-importance detail backing the tables above.
- `tmp/candidate-race-volatility/build_race_level_features.py`,
  `tmp/candidate-race-volatility/train_volatility_model.py` — reproducible pipeline.
