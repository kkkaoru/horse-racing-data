---
science_track_entry: true
hypothesis_id: H-BANEI-SECTIONAL-RACEINTERNAL
date: 2026-06-11
based_on_iteration: banei-cb-v7-lineage-wf-21y (production baseline)
scope: Ban-ei (keibajo_code=83), train 2007-2022, holdout 2023-2026
status: REJECT (probe PASS at 0.0825 ≥ 0.08 bar; retrain gate REJECT — LB95 top1 = −1.071pp)
verdict: REJECT — probe marginal pass (signal declining in recent years); retrain shows no reliable improvement
production_change: none
artifacts:
  feature_builder: tmp/banei/build_sectional_raceinternal_features.py
  sectional_features: tmp/banei/sectional_features/
  probe_output: tmp/banei/sectional_probe.json
  judge_output: tmp/banei/sectional_retrain_judge.json
  model_artifact: tmp/banei/models/banei-cb-sectional-ri.cbm
---

## Hypothesis

Ban-ei is **uniquely data-sparse** at the sectional layer: `corner_1/3/4` are all zero
in the DB (all 324,491 rows), unlike JRA/NAR which have corner position data. The only
timing signal available in `nvd_se` is:

- `soha_time`: total finish time in 1/10s units (decimal string, e.g. `"2036"` = 203.6s)
- `time_sa`: signed difference from winner in 1/10s (string e.g. `"-014"` = 1.4s ahead,
  `"+513"` = 51.3s behind winner)

The existing Ban-ei feature set (5 layers / 128 columns) uses `speed_index_avg_5` and
`speed_index_best_5` (computed from `time_sa` globally) but **never computes within-race
z-scores** (horse time vs field mean/std for that specific race). The hypothesis:
past-race `soha_time` z-scores and `time_sa` averages capture pacing/power relative
to the contemporaneous field, orthogonal to the absolute speed features.

For RACE_INTERNAL: actual numeric `futan_kg` (hex-decoded) and `bataiju_kg` rank/dev
features were absent; only bucket-based `current_futan_class` existed.

## Feature Availability Audit

**Corner times**: all zero for all years (2005–2026). Ban-ei sectional data is not
present in this DB. The `corner_pass_avg_5` column inherited from the JRA/NAR layer
has 0% coverage for Ban-ei.

**soha_time coverage**:

| Year range | Rows    | soha_time > 0 | Coverage |
| ---------- | ------- | ------------- | -------- |
| 2007–2013  | 116,534 | 115,134       | 98.8%    |
| 2014–2022  | 134,736 | 131,708       | 97.8%    |
| 2023–2026  | 56,210  | 55,001        | 97.8%    |

**futan_juryo encoding**: hex string (e.g. `"28A"` → 650 kg). The prior
`add-ban-ei-raw-features.py` attempted decimal parse and silently produced NaN for all
rows. Correct parse: `try_cast('0x'||trim(futan_juryo) as integer)`.

## Feature Engineering

### SECTIONAL layer (5 features)

All features computed strictly from prior races (race_date < current race_date):

1. **`past_soha_zscore_avg5`**: mean of per-race z-score of `soha_time` over last 5
   race-days. z = (horse_soha − race_mean) / race_std. Positive = slower than field.
2. **`past_soha_zscore_best5`**: minimum (most-negative = fastest relative) z-score in
   last 5 race-days.
3. **`past_soha_zscore_avg10`**: same as avg5 but over last 10 race-days.
4. **`past_time_sa_dev_avg5`**: mean of parsed `time_sa` value over last 5 race-days.
   Positive = historically slower than winner; negative = historically led race.
5. **`past_time_sa_dev_avg10`**: same over last 10 race-days.

Coverage: 96.7% (horses with at least one prior race with valid soha_time).

### RACE_INTERNAL layer (11 features)

All use pre-race-known columns only (no leakage):

6.  **`futan_kg_rank_in_race`**: rank by hex-decoded futan_kg within race
7.  **`futan_kg_dev_from_race_avg`**: futan_kg − race_avg(futan_kg)
8.  **`bataiju_kg_rank_in_race`**: rank by hex-decoded bataiju within race
9.  **`bataiju_kg_dev_from_race_avg`**: bataiju_kg − race_avg
10. **`grade_rank_rank_in_race`**: rank by current_grade_rank within race (constant
    within race by design — all horses share the same grade → no within-race variation)
11. **`career_win_rate_rank_in_race`**: rank by career_win_rate within race
12. **`career_win_rate_zscore_in_race`**: z-score of career_win_rate within race
13. **`pedigree_score_zscore_in_race`**: z-score of pedigree_score_for_race within race
14. **`weight_avg5_rank_in_race`**: rank by weight_avg_5 within race
15. **`weight_avg5_zscore_in_race`**: z-score of weight_avg_5 within race
16. **`field_soha_avg_last5`**: race-level average of `past_soha_zscore_avg5` across
    all field horses (captures field-quality level — constant within race)

## Probe Results

**Method**: per-race demeaned partial Spearman ρ. All race effects removed by
demeaning each variable within race before OLS-residualizing on controls. Controls: 13
existing features (speed_index_avg_5, career_win_rate, pedigree_score, popularity_score,
current_futan_class, horse_futan_class_career_win_rate, current_grade_rank, etc.).
Note: `corner_pass_avg_5` excluded from controls (0% Ban-ei coverage).

**Dataset**: 311,142 rows / 33,851 races (all years 2007–2026)

| Feature                        | Coverage | WRC ρ  | Partial ρ | Clears 0.08? |
| ------------------------------ | -------- | ------ | --------- | ------------ |
| past_soha_zscore_avg5          | 96.7%    | 0.280  | 0.055     | No           |
| past_soha_zscore_best5         | 96.7%    | 0.154  | 0.005     | No           |
| past_soha_zscore_avg10         | 96.7%    | 0.259  | 0.063     | No           |
| **past_time_sa_dev_avg5**      | 96.7%    | 0.305  | **0.083** | **Yes**      |
| past_time_sa_dev_avg10         | 96.7%    | 0.285  | 0.080     | No           |
| futan_kg_rank_in_race          | 100%     | −0.007 | −0.013    | No           |
| futan_kg_dev_from_race_avg     | 100%     | 0.007  | 0.012     | No           |
| bataiju_kg_rank_in_race        | 100%     | 0.065  | 0.029     | No           |
| bataiju_kg_dev_from_race_avg   | 100%     | −0.065 | −0.024    | No           |
| grade_rank_rank_in_race        | 100%     | NaN    | NaN       | No           |
| career_win_rate_rank_in_race   | 100%     | 0.081  | 0.008     | No           |
| career_win_rate_zscore_in_race | 91.1%    | −0.083 | −0.010    | No           |
| pedigree_score_zscore_in_race  | 96.0%    | −0.057 | 0.011     | No           |
| weight_avg5_rank_in_race       | 100%     | 0.013  | 0.006     | No           |
| weight_avg5_zscore_in_race     | 74.9%    | −0.027 | −0.005    | No           |
| field_soha_avg_last5           | 98.2%    | NaN    | −0.002    | No           |
| Composite sectional            | —        | —      | 0.074     | No           |
| Composite all                  | —        | —      | 0.073     | No           |

**Bar**: partial ρ ≥ 0.08 on full dataset. `past_time_sa_dev_avg5` = **0.0825** → PROCEED.

### Signal Decay Warning

The 0.0825 full-period partial ρ hides a **severe temporal decline**:

| Period    | past_time_sa_dev_avg5 partial ρ |
| --------- | ------------------------------- |
| 2007–2009 | 0.106 – 0.118                   |
| 2010–2013 | 0.093 – 0.096                   |
| 2014–2019 | 0.070 – 0.095                   |
| 2020–2022 | 0.045 – 0.068                   |
| 2023–2026 | **0.039 – 0.057**               |

Holdout period partial ρ ≈ 0.047 — far below the 0.08 bar. The partial ρ
bar is formally met on the full dataset, so retrain was performed per protocol,
but the expected improvement in the holdout was already low.

The `speed_index_avg_5` ↔ `past_soha_zscore_avg5` Spearman correlation is 0.72,
confirming that soha_time z-scores are largely redundant with the existing speed index
in recent years (as the speed index has become better calibrated for this venue).

## Retrain Results (Step 3)

**Model**: CatBoost YetiRank, default hyperparams (iter=300, depth=8, lr=0.05,
l2_leaf_reg=3.0). Early stopped at iteration **40** (od_wait=30). 133 features total
(128 baseline + 16 new features minus 1 constant grade_rank feature).

**4-axis delta table vs canonical baseline** (n=5,976 holdout races):

| Axis     | Baseline | New model | Delta  | LB95 (pp) |
| -------- | -------- | --------- | ------ | --------- |
| top1     | 0.34404  | 0.34337   | −0.067 | −1.071    |
| place2   | 0.55890  | 0.55723   | −0.167 | −1.205    |
| place3   | 0.43173  | 0.43574   | +0.401 | −0.653    |
| top3_box | 0.09237  | 0.09321   | +0.084 | −0.519    |

**Per-year breakdown**:

| Year | Races | top1    | place2  | place3  | top3_box |
| ---- | ----- | ------- | ------- | ------- | -------- |
| 2023 | 1,788 | 0.34676 | 0.56376 | 0.44463 | 0.09452  |
| 2024 | 1,788 | 0.33669 | 0.55089 | 0.43736 | 0.08613  |
| 2025 | 1,692 | 0.35165 | 0.56442 | 0.42908 | 0.09811  |
| 2026 | 708   | 0.33192 | 0.53955 | 0.42514 | 0.09605  |

**Bootstrap gate** (10,000 race resamples, seed=42):

| Gate                       | Threshold | Value                | Pass? |
| -------------------------- | --------- | -------------------- | ----- |
| G1: LB95 top1 > 0pp        | > 0       | −1.071pp             | FAIL  |
| G2: All axes ≥ −0.05pp     | ≥ −0.05   | top1=−0.067pp        | FAIL  |
| G3: ≥2 axes point-positive | ≥ 2       | 2 (place3, top3_box) | PASS  |

**Decision: REJECT** (G1 and G2 both fail).

Note: The model was early-stopped at iteration 40 (vs baseline's iteration 32),
indicating the new features slowed convergence without providing a stable signal.

## Root Cause Analysis

1. **Correlation trap**: `past_time_sa_dev_avg5` (ρ=0.72 with `speed_index_avg_5`)
   is nearly collinear with the existing strongest signal. CatBoost allocates few
   splits to the new features since speed_index already captures the same variance.

2. **Signal decay**: the historical partial ρ of 0.08 is entirely carried by
   2007–2013 races (~30% of training data). In the 2023–2026 holdout window,
   the signal is ≈0.046 — not strong enough to move metrics.

3. **Grade rank constant**: `grade_rank_rank_in_race` has 0 within-race variation
   because all horses in a Ban-ei race share the same grade — this feature adds
   noise with no discriminating power.

4. **Futan/bataiju orthogonality failure**: `current_futan_class` already encodes
   futan_kg at bucket resolution; adding exact kg devations at 100%+ correlation adds
   only noise.

## Production Recommendation

**DO NOT DEPLOY** this feature layer. The new features either duplicate existing
signals (soha_time vs speed_index) or carry only early-period signal that has decayed.

### Next Ban-ei steps

Given REJECT here and the prior HPO REJECT (−0.201pp) and futan-ratio ABORT:

1. **Feature freshness** — investigate WHY signal from `time_sa` was strong pre-2020
   but weak post-2020 (possible: field quality mixing changed, sectional timing method
   changed, futan distribution shifted). Understanding the mechanism may reveal an
   interaction feature.

2. **Ban-ei-specific time normalization** — futan_juryo varies 450–1000kg
   (vs JRA 54–58kg); soha_time for the same distance varies 2x depending on futan class.
   A within-futan-class par time normalization (analogous to H-BABA-PAR-TIME for NAR)
   may extract incremental signal from soha_time that speed_index misses.

3. **New data acquisition** — checkpoint times (障害通過/hill passage) for Ban-ei
   reportedly exist in some data vendors (e.g. JV-Data `nvd_se` fields beyond what
   this DB stores). Verifying if additional sectional data exists in the DB is the
   highest-priority audit before any further Ban-ei retrain.

4. **Distance × futan interaction features** — the only Ban-ei distances are 200/400/500m
   (ばんえい); a horse's futan-normalized speed figure by kyori may be more predictive
   than overall soha_time given the distinct pacing requirements of each distance.
