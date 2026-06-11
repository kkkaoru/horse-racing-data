# D2a — Horse-Locality Feature Feasibility Probe

**Date**: 2026-06-11
**Probe**: H-LOCALITY-FEATURE (recommended in d2a-rs-impute-feasibility.md as the correct mechanism after ABORT of mean-imputation)
**Model**: NAR iter12 recipe (XGBoost rank:pairwise, same HPO params as production)
**Holdout**: 2023–2025 (3 WF folds)
**Target venues**: 43 (Funabashi) + 44 (Kawasaki) = 5,612 holdout races
**Feature parquet WITHOUT**: `apps/pc-keiba-viewer/tmp/feat-nar-v7-final` (182 features)
**Feature parquet WITH**: `apps/pc-keiba-viewer/tmp/feat-nar-d2a-locality` (187 features = 182 + 5 new)
**Module**: `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-horse-locality-features.py`
**Tests**: `apps/pc-keiba-viewer/tests/test_add_horse_locality_features.py` (29 tests, 100% coverage)
**Output JSON**: `tmp/rootcause/d2a_locality.json`
**Verdict**: **ABORT**

---

## Feature Design

### New Features Added (5 columns)

All features are computed from the horse's PAST races only. The JOIN uses `race_date STRICTLY LESS THAN` the current race's race_date — the target race row is excluded from its own aggregate.

| Feature                     | Type                | Semantics                                                                                                                                                                                    |
| --------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pct_career_at_keibajo`     | float [0,1] or NULL | Fraction of prior races at the CURRENT keibajo_code. NULL for debut horses (0 prior races).                                                                                                  |
| `n_career_races_at_keibajo` | integer ≥ 0         | Prior count of races at the current venue (0 = debut or never raced here before).                                                                                                            |
| `n_career_races_total`      | integer ≥ 0         | Prior total races across all venues (0 = debut).                                                                                                                                             |
| `n_distinct_keibajo`        | integer ≥ 0         | Number of distinct venues visited in prior history (0 = debut; 1 = exclusively local).                                                                                                       |
| `rs_features_null_flag`     | 0 or 1              | 1 when all four RS rate columns (`past_nige/senkou/sashi/oikomi_rate_self`) are simultaneously NULL, else 0. Lets the model explicitly key off the NULL condition plus the locality context. |

### Leak-Free Confirmation

- `build_prior_table` / `stage_locality_aggregates` uses `rh.race_date < cr.race_date` (strict less-than).
- Tested by `test_leak_free_excludes_target_race_itself` (same-date row excluded) and `test_leak_free_excludes_future_races` (future rows excluded).
- No future-information leakage possible: the aggregate is purely over the horse's completed-race history.

### Why This Mechanism (vs Imputation)

The previous probe (d2a-rs-impute-feasibility.md, ABORT) showed that XGBoost's learned NULL-routing is competitive with mean imputation because the model already implicitly groups "locally-anchored horses with unknown style" into a common branch. Mean imputation collapses individual variation within that branch without adding new information.

This probe adds explicit locality features so the model can CONDITION on local-anchoredness as a continuous signal — combining it with the existing NULL-routing branch, rather than replacing it. The rs_features_null_flag further allows direct interaction: a locally-anchored horse (high pct) with rs_features_null_flag=1 gets a distinct path in the tree ensemble.

---

## Materialization

Locality-augmented parquet: `tmp/feat-nar-d2a-locality` (2,058,436 rows, 10 race years 2016–2025).

### Locality signal by venue (holdout 2023, sampled)

| keibajo_code | avg pct_career_at_keibajo | avg n_career_races_total | rs_null_flag count | n rows |
| ------------ | ------------------------- | ------------------------ | ------------------ | ------ |
| 43           | 0.467                     | 25.3                     | 1,410              | 7,273  |
| 44           | 0.681                     | 24.3                     | 4,013              | 13,751 |
| 46           | 0.527                     | 36.7                     | 377                | 8,603  |

Venue 44 (Kawasaki) has the highest average locality (0.68) confirming the diagnosed locally-anchored population.

---

## WITH vs WITHOUT Comparison (WF holdout 2023–2025)

### Per-fold results — Venues 43+44

| Fold | WITHOUT top1 | WITH top1 | Δ top1  | WITHOUT fukusho_2p | WITH fukusho_2p |
| ---- | ------------ | --------- | ------- | ------------------ | --------------- |
| 2023 | 0.4766       | 0.4691    | −0.75pp | 0.7744\*           | 0.7734\*        |
| 2024 | 0.4691       | 0.4729    | +0.38pp | —                  | —               |
| 2025 | 0.5227       | 0.5232    | +0.05pp | —                  | —               |

\*fukusho_2p computed over combined 3-fold holdout

### Aggregated (weighted by n_races) — Venues 43+44

| Metric     | WITHOUT   | WITH      | Δ (pp) | Gate (pp) | Pass? |
| ---------- | --------- | --------- | ------ | --------- | ----- |
| top1       | 0.48949   | 0.48842   | −0.107 | ≥ +3.0    | FAIL  |
| top1 LB95  | 0.48069\* | 0.47962\* | —      | —         | —     |
| place2     | 0.88079   | 0.88097   | +0.018 | ≥ −0.05   | PASS  |
| place3     | 0.97969   | 0.97933   | −0.036 | ≥ −0.05   | PASS  |
| fukusho_2p | 0.77441   | 0.77334   | −0.107 | ≥ 0.0     | FAIL  |

\*LB95 computed using Wilson interval on the combined 3-fold holdout

### Aggregated — Global NAR

| Metric | WITHOUT | WITH    | Δ (pp) | Gate (pp) | Pass? |
| ------ | ------- | ------- | ------ | --------- | ----- |
| top1   | 0.58583 | 0.58556 | −0.027 | ≥ +0.3    | FAIL  |

---

## Probe Gate

| Condition                               | Required  | Actual   | Result |
| --------------------------------------- | --------- | -------- | ------ |
| 43/44 top1 ≥ +3.0pp vs baseline         | ≥ +3.0pp  | −0.107pp | FAIL   |
| Global NAR top1 ≥ +0.3pp vs baseline    | ≥ +0.3pp  | −0.027pp | FAIL   |
| place2 no regression (≥ −0.05pp, 43/44) | ≥ −0.05pp | +0.018pp | PASS   |
| place3 no regression (≥ −0.05pp, 43/44) | ≥ −0.05pp | −0.036pp | PASS   |
| fukusho_2p no regression (≥ 0.0pp)      | ≥ 0.0pp   | −0.107pp | FAIL   |

**VERDICT: ABORT** — 3 of 5 gate conditions failed.

---

## Feature Importance of Locality Features (fold 2025)

Locality features are at the bottom of the XGBoost gain distribution:

| Feature                   | Gain | % of total gain |
| ------------------------- | ---- | --------------- |
| n_career_races_at_keibajo | 23.0 | 0.366%          |
| n_career_races_total      | 19.8 | 0.315%          |
| pct_career_at_keibajo     | 19.4 | 0.308%          |
| n_distinct_keibajo        | 15.2 | 0.242%          |
| rs_features_null_flag     | 0.0  | 0.000%          |

Total locality gain: ~77.4 out of ~6,287 total = **1.23% of total gain**.

Compare to the top features:

| Rank | Feature              | Gain  |
| ---- | -------------------- | ----- |
| 1    | target_corner_4_norm | 934.4 |
| 2    | odds_score           | 445.4 |
| 3    | popularity_score     | 218.5 |
| 4    | target_corner_3_norm | 180.7 |
| 5    | target_corner_1_norm | 166.6 |

The locality features collectively account for less than the 15th-ranked individual feature.

`rs_features_null_flag` gained zero splits — XGBoost's NULL-routing already captures the NULL/non-NULL distinction implicitly via default split directions, so an explicit binary flag adds no new information.

---

## Root Cause of ABORT

### XGBoost already treats locality implicitly via NULL-routing AND existing venue features

The model already has `keibajo_code` as a feature (categorical), `same_keibajo_win_rate`, `jockey_keibajo_win_rate`, `trainer_keibajo_win_rate`, and `horse_keibajo_corner_1_norm_avg`. These implicitly capture venue affinity from the jockey/trainer/horse perspective.

The new locality features add the horse's historical ratio at the specific venue — but XGBoost with NULL-routing can already learn "if past_nige_rate_self is NULL AND keibajo_code is 43 or 44, route to this branch". The explicit locality fraction (pct_career_at_keibajo) provides a softer version of the same information the model is already deriving from the NULL pattern plus venue code.

### 43/44 top1 variability is high — gains at +0.38pp (2024) are within noise

The per-fold 43/44 top1 ranges from 0.469 to 0.523 (5.4pp range). A true signal of +3pp would need to appear consistently across all three folds. The observed pattern (−0.75, +0.38, +0.05) is consistent with noise around zero.

### rs_features_null_flag = 0 gain — confirms implicit handling

The XGBoost model gained ZERO information from an explicit NULL-flag binary variable, confirming that the model's learned default-split routing for missing values is functionally equivalent to the explicit flag.

---

## Summary of D2a Probes (Completed)

| Probe                                         | Status | Key Finding                                                                                                        |
| --------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------ |
| H-RS-KEIBAJO-IMPUTE (mean imputation)         | ABORT  | Imputation erases inter-horse variation the model already handles via NULL-routing                                 |
| H-LOCALITY-FEATURE (explicit locality signal) | ABORT  | Locality features gain <1.25% of total; model already captures venue affinity via existing features + NULL-routing |

### D2 Conclusion

The NAR Funabashi/Kawasaki (~7pp model-specific gap identified in d2a-nar-venue-gap-diagnosis.md) is NOT capturable via data-completeness feature engineering applied to the existing feature parquet. Both probes confirm:

1. XGBoost's learned NULL-routing is already extracting the available signal from locally-anchored horses.
2. Venue-affinity information is already captured by existing keibajo-level features.
3. The ~7pp gap reflects missing discriminative signal (no corner history) that cannot be recovered by re-expressing existing information in new feature columns.

**To proceed further on 43/44 headroom**, the required next step is a qualitatively new data source — either:

- Running-style model extension: extend the v3 RS model to produce predictions for locally-anchored horses by learning from their corner-adjacent patterns (e.g., fractional times, gate draws, pace patterns at 43/44 specifically).
- Real-time corner data at 43/44: if real-time corner position data exists at these venues, the RS model could be applied at inference time.

D2 branch closes. The 43/44 headroom is not currently capturable via feature engineering on the existing parquet.
