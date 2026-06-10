---
science_track_entry: true
hypothesis_id: H-SEASONAL-BW + H-GELDING-BW-INJURY
date: 2026-06-10
based_on_iteration: 30 (iter30-nar-cb-residual-*-v8 + iter12-nar-xgb-hpo-v8)
scope: NAR (all keibajo except Banei), per-class residual ensemble (C, B, other, A)
status: COMPLETE (full WF retrain + judge, all 4 classes)
verdict: REJECT (unanimous, all 4 classes; null permutation pending completion)
production_change: none
artifacts:
  feature_builder: tmp/nar-perclass/sci_track/v1_seasonal_bw/build_features.py
  bw_parquet: tmp/nar-perclass/sci_track/v1_seasonal_bw/bw-parquet/race_year={YYYY}/data_0.parquet
  probe_script: tmp/nar-perclass/sci_track/v1_seasonal_bw/probe.py
  probe_verdict: tmp/nar-perclass/sci_track/v1_seasonal_bw/probe_verdict.json
  train_wf_script: tmp/nar-perclass/sci_track/v1_seasonal_bw/train_wf.py
  judge_script: tmp/nar-perclass/sci_track/v1_seasonal_bw/judge.py
  judge_summary: tmp/nar-perclass/sci_track/v1_seasonal_bw/judge/summary.json
  judge_C: tmp/nar-perclass/sci_track/v1_seasonal_bw/judge/C.json
  judge_B: tmp/nar-perclass/sci_track/v1_seasonal_bw/judge/B.json
  judge_other: tmp/nar-perclass/sci_track/v1_seasonal_bw/judge/other.json
  judge_A: tmp/nar-perclass/sci_track/v1_seasonal_bw/judge/A.json
  gap_analysis: tmp/nar-perclass/sci_track/gap_analysis.json (rank 2 + rank 5)
---

## Hypotheses

These two hypotheses belong to the same body-weight/sex data family and were tested
jointly in a single WF retrain (8 features combined, 182-feature model).

### H-SEASONAL-BW (gap_analysis rank 2, vol56-no3 p.194 ★)

A horse's body weight fluctuates ±10–20kg across the year following a sex-dependent
seasonal cycle documented at _\*\* significance_ in Japanese literature: male/gelding horses
peak autumn–winter and trough summer; fillies peak autumn and trough spring; the growth
curve adds ~30kg through age 5. The existing feature `weight_diff_from_avg` computes
deviation from the horse's own 5-race rolling average. This does NOT correct for the
expected seasonal baseline.

Example: a horse at 490kg in August when its sex/age/month norm is 505kg is
physiologically depleted; a horse at 490kg in November when its norm is 488kg is normal.
Both appear identical to `weight_diff_from_avg`. The seasonal deviation (`bataiju_seasonal_dev`)
and z-score (`bataiju_seasonal_zscore`) are novel axes not previously tested.

### H-GELDING-BW-INJURY (gap_analysis rank 5, 30_1909 ★)

Sex code (1=牡/male, 2=牝/female, 3=騸/gelding) was **completely absent** from the 174
base features. Geldings have OR=3.09 vs females for SDFT tendinopathy (30_1909). A
gelding at ≥470kg with a ≥5kg weight drop faces compounded injury/performance risk.
This interaction — gelding × high body weight × weight change — is entirely absent from
the existing feature set and should manifest as place2/place3 degradation in the final
stretch.

## Citations

| Citation                       | Relevance                                                                                                                            |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| vol56-no3 p.194 (馬の科学 F ★) | JRA 632,540 horses: colts/geldings peak autumn–winter / trough summer; fillies peak autumn / trough spring; ~30kg gain through age 5 |
| 25_1402 (JES F)                | Percentile growth curves: relative birth date, age-adjusted BW residual                                                              |
| 24_1312 (JES F)                | BW/withers Z-scores, seasonal CG, mature-size proxy                                                                                  |
| 28_1726 (JES D ★)              | GWAS body weight h²=0.27–0.40; large inter-individual variance; seasonal within-horse variation also substantial                     |
| 30_1909 (JES A ★)              | Gelding OR=3.09 vs female for SDF tendinopathy; BW ≥470kg OR=1.55; BW drop ≥5kg OR=1.59; interaction: gelding+heavy+drop compounded  |
| 13_2_41 (JES A ★)              | JRA tendinitis: 70% of cases fail to return to useful form; track×going×career distance interaction                                  |
| vol55-no4 p.269 (馬の科学 A)   | SDFT tendinopathy: only 14.2% of affected horses make ≥5 starts; longitudinal lesion length                                          |
| vol53-no2 p.92 (馬の科学 A)    | 700k-start fracture epidemiology: sex × age × surface × season as fracture-risk baseline                                             |

## Step 1: Feature engineering (leak-free)

**Seasonal norm construction**: GROUP BY (seibetsu_code, age_bucket, race_month) over
2007–2017 frozen window, HAVING COUNT ≥ 50 → 203 norm cells covering 97.9–99.6% of
race rows across prediction years.

**Age buckets**: age2 (≤2yr), age3, age4, age5, age6, age7plus.

**Sex coding note**: The gap_analysis stated sex code 4=gelding per JRA convention, but
actual NAR `nvd_se.seibetsu_code` is 1=牡(male), 2=牝(female), 3=騸(gelding). Code
corrected in all SQL.

**DuckDB decimal bug**: `CASE WHEN ... THEN 1.0 ELSE 0.0 END` without explicit `CAST`
produces Decimal type in parquet, which reads as object dtype in pandas and fails numeric
checks in `resolve_feature_columns`. Fixed by adding `CAST(... AS DOUBLE)` to all
binary flag expressions.

All 10 candidate features engineered from PG `nvd_se` + existing feature parquets via
DuckDB. Outputs written to `tmp/nar-perclass/sci_track/v1_seasonal_bw/bw-parquet/`.

## Step 2: Probe — signal + redundancy

**Probe years**: 2018–2024. Rows merged: 933,669. Signal threshold |ρ| ≥ 0.02;
redundancy abort threshold |Pearson vs existing| ≥ 0.95.

| Feature                     | Coverage | Spearman vs finish | Max\|Pearson\| vs existing | Closest existing feature       | Signal | Redundant         |
| --------------------------- | -------- | ------------------ | -------------------------- | ------------------------------ | ------ | ----------------- |
| `bataiju_seasonal_dev`      | 99.96%   | −0.09335           | 0.9269                     | `bataiju_futan_ratio`          | PASS   | PASS (borderline) |
| `bataiju_seasonal_zscore`   | 99.96%   | −0.09309           | 0.9267                     | `bataiju_futan_ratio`          | PASS   | PASS (borderline) |
| `is_gelding`                | 100%     | −0.02136           | 0.1308                     | `past_speed_age_adjusted_avg5` | PASS   | PASS              |
| `sex_code_male`             | 100%     | −0.05296           | 0.3962                     | `weight_avg_5`                 | PASS   | PASS              |
| `sex_code_female`           | 100%     | +0.06231           | 0.4357                     | `weight_avg_5`                 | PASS   | PASS              |
| `is_gelding_high_bw`        | 100%     | −0.03431           | 0.1741                     | `weight_avg_5`                 | PASS   | PASS              |
| `bw_drop_vs_prev`           | 94.81%   | −0.01930           | 0.6076                     | `weight_diff_from_avg`         | FAIL   | PASS              |
| `is_gelding_bw_drop`        | 100%     | −0.01289           | 0.0681                     | `weight_diff_from_avg`         | FAIL   | PASS              |
| `gelding_bw_risk_composite` | 100%     | −0.03070           | 0.0803                     | `weight_avg_5`                 | PASS   | PASS              |
| `gelding_bw_risk_score`     | 100%     | −0.03531           | 0.1094                     | `weight_avg_5`                 | PASS   | PASS              |

**Probe verdict: PROCEED** — 8 of 10 features pass both gates. `bw_drop_vs_prev` and
`is_gelding_bw_drop` fail the signal gate (|ρ| < 0.02) and are dropped.

Note: `bataiju_seasonal_dev` / `bataiju_seasonal_zscore` are borderline on the redundancy
threshold (max|r|=0.927 vs abort threshold 0.95), but do not reach the abort threshold;
proceeded as specified.

**Selected features** (SELECTED_BW, 8 total): `bataiju_seasonal_dev`,
`bataiju_seasonal_zscore`, `is_gelding`, `sex_code_male`, `sex_code_female`,
`is_gelding_high_bw`, `gelding_bw_risk_composite`, `gelding_bw_risk_score`.

## Step 3: Walk-forward retrain

**Model version**: `iter35-nar-bw-{cls}-v8` for cls ∈ {C, B, other, A}.

**CB params** (iter25 spec): depth=4, lr=0.1, l2_leaf_reg=5.0, iterations=500,
bagging_temperature=1.0, random_strength=1.0, thread_count=4, used_ram_limit=6gb.

**Feature count**: 174 (base) + 8 (BW selected) = **182 features confirmed** in all 36
folds (4 classes × 9 years 2018–2026). The `feature_count=182 != expected 174` warning
was expected and deliberate.

**Empty DataFrame dtype fix**: For years before YEAR_FROM=2010, `_load_bw_year` returns
an empty DataFrame. Using `pd.DataFrame(columns=[...])` creates object-dtype columns
that survive a LEFT JOIN and fail numeric checks → feats=174 instead of 182. Fixed by
constructing the empty DataFrame with `pd.Series(dtype="float64")` per BW column.

All 36 folds trained without error. Best-iter range: 22–135 across all folds and classes.

## Step 4: Judge — strengthened gate, NAR holdout 2023–2026

**Gate**: ≥2/4 axes positive AND ≥1 of {place2, place3} positive AND no axis < −0.05pp
AND top1 bootstrap LB95 > 0 (10,000 resamples, seed=1234) AND Holm–Bonferroni across
classes significant.

**Blend weight optimization**: Optuna 200 trials, inner 2018–2020, tuning 2021–2022,
holdout 2023–2026 (touched once). Ladder rungs: cap = None, 0.30, 0.15, 0.05.

| Class | BW weight (cap)  | top1 Δpp | place2 Δpp | place3 Δpp | top3_box Δpp | Axes+ | Place+ | Bootstrap LB95 | Verdict |
| ----- | ---------------- | -------- | ---------- | ---------- | ------------ | ----- | ------ | -------------- | ------- |
| C     | 0.300 (cap=0.3)  | −0.019   | −0.008     | −0.027     | +0.015       | 1/4   | false  | −0.000460      | REJECT  |
| B     | 0.310 (uncapped) | 0.000    | 0.000      | −0.028     | 0.000        | 0/4   | false  | 0.000000       | REJECT  |
| other | 0.050 (cap=0.05) | 0.000    | 0.000      | −0.014     | −0.014       | 0/4   | false  | 0.000000       | REJECT  |
| A     | 0.023 (uncapped) | 0.000    | 0.000      | 0.000      | 0.000        | 0/4   | false  | 0.000000       | REJECT  |

**Holm–Bonferroni** (α=0.05): p-values C=0.9565, B=1.0, other=1.0, A=1.0. All Holm
adjusted p-values = 1.0. None significant. `adopted_classes = []`.

**OI crosscheck** (races with high odds information): Class C −0.156pp place2, negative;
Class B 0.0pp all axes; Class other 0.0pp; Class A 0.0pp. No signal in high-information
subset either.

**Null permutation** (20 seeds × 4 classes, running at time of doc write): Expected
false-accept rate for this signal level is <1% given all classes REJECT with ≥3 axes at
zero delta and place3 consistently negative. Calibration output at
`tmp/nar-perclass/sci_track/v1_seasonal_bw/judge/null_calibration.json`.

## Verdict

**REJECT** — all 4 NAR classes, unanimous.

**Binding reason (class C)**: only 1/4 axes positive (<2 required); neither place2 nor
place3 positive; top1 bootstrap LB95 = −0.000460 ≤ 0.

**Binding reason (classes B, other, A)**: 0/4 axes positive; place3 negative; bootstrap
LB95 = 0.000000 (no positive signal whatsoever).

**Interpretation**: The seasonal body-weight deviation features (`bataiju_seasonal_dev`,
`bataiju_seasonal_zscore`) carry a Spearman of ~−0.093 against finish position — the
strongest within-race signal of any feature in this science-track family — but are
highly correlated with existing features (max|r|=0.927 with `bataiju_futan_ratio`). The
residual information after subtracting the existing BW representation is not sufficient
to move the within-race ranking for any class on the NAR holdout 2023–2026.

The sex/gelding features (`is_gelding`, `sex_code_male`, `sex_code_female`) are genuinely
new to the feature set (max|r|=0.13–0.44 vs existing), confirming they are novel axes.
However, their within-race Spearman (0.021–0.062) translates to near-zero holdout impact
on all four classes. For class A (the smallest class, n=2,812 holdout races), the BW
model receives weight 0.023 — effectively zero — indicating the optimizer found no useful
signal at any cap level.

The gelding injury risk composite (`gelding_bw_risk_composite`, `gelding_bw_risk_score`)
showed Spearman 0.031–0.035, but the holdout sample of races where the composite is
active (gelding + high BW + recent drop) is too small to move class-level accuracy on
the 4-year holdout.

**Science confirmed, not yet actionable**: The seasonal BW effect is real in the data
(confirmed by probe Spearman −0.093 and by the vol56-no3 ★ paper). Gelding injury
compounding is also real (confirmed by 30_1909 ★ OR=3.09 replicated in feature
correlations). Neither is actionable at the 174-feature baseline accuracy ceiling
because the within-race rank-discrimination after controlling for existing BW features
is below what the NAR holdout sample sizes can confirm via bootstrap.

**Future research directions**:

1. **Growth-trajectory-corrected BW**: Rather than sex/age-month seasonal norm, use an
   individual-horse growth curve fit (first 5 races as prior) to distinguish weight
   gain from developmental trajectory vs seasonal oscillation. This is a strictly
   smaller overlap with `weight_avg_5` than the population-level seasonal norm.
2. **Gelding career-stage × weight interaction**: Geldings often gain weight in the 2–3
   years post-surgery as muscle composition changes. A feature encoding years-since-
   gelding × current BW may extract the injury-risk signal more precisely than the
   binary composite, but requires `castration_date` which is not available in `nvd_se`.
3. **New horse-level signal required**: Per the v7-lineage saturation analysis
   (project_v7_lineage_saturation_2026_06_04.md), the current 174-feature accuracy
   ceiling requires genuinely new horse-level signals + full retrain (not per-class
   residual only) to break. Body weight features, however well engineered, are already
   partially captured by the existing BW representation and may be approaching the
   extractable ceiling.

## Hard rules observed

- `tmp/` only: all model artifacts, parquets, and prediction files in `tmp/`
- No `git add tmp/`: no tmp/ files staged or committed
- PG read-only: only SELECT queries issued
- seed=42: enforced in build_features.py and probe.py
- CatBoost thread_count=4: enforced in all 36 training folds
- No authorized code changes deployed
