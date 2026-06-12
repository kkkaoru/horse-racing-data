---
science_track_entry: true
hypothesis_id: JRA-RELATIONSHIP-VERIFY
date: 2026-06-12
based_on: JRA-RELATIONSHIP-FEATURES-PERCLASS (jra-relationship-features-perclass.md)
scope: JRA only (source='jra'), holdout kaisai_nen 2023-2026
status: ABORT — features produce no positive signal even with NULL-routing
verdict: >
  nige_vs_field and oikomi_in_fast_field added to iter14 JRA CatBoost with NULLs kept
  (CatBoost routes NULL natively). New model retrained on 2007-2025 (923k rows, 243 feats).
  Equal-footing per-class judge: ALL target classes (005/010/016) show negative or flat
  deltas with LB95 < 0. Pooled full-holdout also fails no-regression gate. ABORT.
production_change: none
probe_script: tmp/v8/train_verify_jra_relationship_features.py
log: tmp/v8/relationship-verify.log
---

## Background

The prior probe (jra-relationship-features-perclass.md) found strong partial Spearman ρ
(+0.10..+0.12 for nige_vs_field, −0.10..−0.13 for oikomi_in_fast_field) in classes
005/010/016/other. However, the LightGBM incremental check showed −0.48pp due to row-dropping
(87.4% running-style coverage → training shrank 283k→166k rows).

The established lesson: CatBoost/XGBoost route NULL natively, so the proper test **keeps NULLs**
and does not drop or impute rows. This probe re-tests with NULLs kept throughout.

## Experimental Setup

### Feature construction

- Base: `apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course` (241 features, 2006-2026)
- New: `tmp/v8/feat-jra-v8-iter14-relfeats` (243 features = base + 2 new cols)
- `nige_vs_field` = rs_p_nige − field_mean(rs_p_nige per race, over non-null horses only)
- `oikomi_in_fast_field` = rs_p_oikomi × field_mean(rs_p_nige per race)
- NULL when horse's own rs_p_nige / rs_p_oikomi is NULL (~1.6% in 2024-2025, 100% in pre-2024)

### NULL coverage in training data

The production feature store has rs_p_nige populated **only for 2024-2025**. Pre-2024 (2006-2023)
and 2026 are all NULL (100%). This means:

- Training 2007-2025: nige_vs_field null rate = **89.9%** (only 2024-2025 have values)
- Holdout 2023-2026: null rate 42.3% (2023=100% null, 2024-2025≈1.6%, 2026=100%)
- CatBoost routes NULL rows to dedicated leaf splits; the signal is learnable from 2024-2025 only

### Model training

- Architecture: CatBoost YetiRank (same as iter14 production)
- Hyperparams: depth=8, lr=0.05, l2_leaf_reg=3.0, max_iter=1000, od_wait=30
- Train: 2007-2025 (923,146 rows), Val: 2026 (18,824 rows, early stopping)
- Model version: `iter14-jra-cb-pacestyle-course-v8-relfeats`
- Best iteration: 210 (vs iter14 production: ~300 typical), training time: 54.3s

### Scoring

- Holdout: kaisai_nen 2023-2026, rows with valid finish_position
- 160,347 holdout rows, 11,703 races
- kyoso_joken_code joined from PG jvd_ra (keibajo_code 01-10 = JRA)
- class_null rate = 0.000 (all races matched)

## Per-Class Equal-Footing Judge

Holdout 2023-2026. Bootstrap: 10,000 iterations, seed=42, vectorized numpy.
LB95 = 5th percentile of bootstrap deltas (paired, races resampled with replacement).

### Top-1 accuracy

| Class  | N races | base top1 | new top1 | delta       | LB95        |
| ------ | ------- | --------- | -------- | ----------- | ----------- |
| 005    | 3,147   | 41.47%    | 41.28%   | −0.19pp     | −0.64pp     |
| 010    | 1,583   | 43.40%    | 42.89%   | −0.51pp     | −1.07pp     |
| 016    | 727     | 37.96%    | 37.83%   | −0.14pp     | −0.83pp     |
| 703    | 4,229   | 49.56%    | 49.52%   | −0.05pp     | −0.45pp     |
| 701    | 953     | 45.65%    | 46.17%   | +0.52pp     | −0.21pp     |
| other  | 1,064   | 41.64%    | 40.70%   | −0.94pp     | −1.79pp     |
| pooled | 11,703  | 44.79%    | 44.60%   | **−0.19pp** | **−0.41pp** |

### Fukusho-2p (≥2 of predicted top-3 are actual top-3)

| Class  | base fk2p | new fk2p | delta       | LB95        |
| ------ | --------- | -------- | ----------- | ----------- |
| 005    | 65.40%    | 64.82%   | −0.57pp     | −1.02pp     |
| 010    | 64.69%    | 64.62%   | −0.06pp     | −0.69pp     |
| 016    | 56.95%    | 57.08%   | +0.14pp     | −1.10pp     |
| 703    | 75.31%    | 75.34%   | +0.02pp     | −0.31pp     |
| 701    | 72.19%    | 72.72%   | +0.52pp     | −0.21pp     |
| other  | 61.18%    | 61.09%   | −0.09pp     | −1.03pp     |
| pooled | 68.53%    | 68.42%   | **−0.11pp** | **−0.34pp** |

### Accept gate evaluation

Gate: ≥1 of target classes (005/010/016) with LB95 > 0 on fk2p or top1 **AND**
pooled no-regression (top1 LB95 ≥ −0.05pp, fk2p LB95 ≥ −0.05pp, top3_box LB95 ≥ −0.05pp).

| Criterion                                | Result                 |
| ---------------------------------------- | ---------------------- |
| 005/010/016 target classes with LB95 > 0 | **0/3** (all negative) |
| Pooled top1 LB95 ≥ −0.05pp               | FAIL (−0.41pp)         |
| Pooled fk2p LB95 ≥ −0.05pp               | FAIL (−0.34pp)         |
| **GATE**                                 | **FAIL**               |

**STEP-3 VERDICT: ABORT**

## Root-Cause Analysis

### Why the features fail despite strong partial ρ

The partial Spearman ρ (0.10-0.13) measured in the prior probe was a **population-level signal**
computed on 2023+ holdout where rs_p_nige is only non-null for 2024-2025. The GBDT in production
already has `rs_p_nige` and `rs_p_oikomi` as direct features. The new within-race relative forms
(`nige_vs_field`, `oikomi_in_fast_field`) add the field_mean(rs_p_nige) interaction, but:

1. **Training sparsity**: 89.9% null in training (only 2024-2025 have values). CatBoost learns
   a NULL split, but with only 10% of training rows active, the features cannot build deep
   specialised splits. The model gets effectively the same information from the existing
   `rs_p_nige` + `self_nige_rate_minus_field_avg` (which uses historical style rates as proxy).

2. **Redundancy with existing features**: The iter14 production model already includes:
   - `self_nige_rate_minus_field_avg` — historical within-race relative nige rate
   - `rs_p_nige` — absolute running-style probability (same base signal)
   - `rs_p_nige_x_field_pace` — cross term with field_pace_index
   - `field_nige_pressure` — field-level aggregated nige pressure
     These collectively capture the within-race pacing context. The new features are
     a marginal reparametrisation of the same underlying signal space.

3. **Holdout composition**: 2023 (100% null) + 2026 (100% null) = majority of holdout has null
   new features, so the model is evaluated mostly on the same path as the base model.
   Only 2024-2025 (≈55% of holdout races by year) can exhibit differential behaviour.

4. **Best iteration regression**: New model stopped at iter 210 vs typical ~300 for base.
   The 2 additional near-null features may have added noise that triggered early stopping,
   producing a shallower, potentially less-well-calibrated model.

### What the partial ρ result actually measured

The ρ=0.10-0.13 measured in the prior probe reflects _association_ that persists after
partialing out log(odds). CatBoost natively captures this via the existing `rs_p_nige` feature;
the within-race relative form (`nige_vs_field`) adds no incremental information once the direct
probability and existing relative proxies are already in the model.

### Comparison with original probe's LightGBM check

| Check                                   | Train rows     | Delta top1 | Root cause              |
| --------------------------------------- | -------------- | ---------- | ----------------------- |
| Original probe (LightGBM, rows dropped) | 166k (of 283k) | −0.48pp    | Row-drop selection bias |
| This probe (CatBoost, NULLs kept)       | 923k           | −0.19pp    | Genuine null result     |

The row-drop in the original probe was masking a **genuine negative result**, not understating
a positive one. Keeping NULLs and using the full training set confirms ABORT.

## Conclusion

The JRA relationship features `nige_vs_field` and `oikomi_in_fast_field` are confirmed
as having **no actionable incremental value** on top of the iter14 production feature set:

- The prior ABORT conclusion was correct in outcome, though the stated root cause
  (coverage = blocker) was wrong. The real root cause is redundancy with existing features.
- Both features are redundant given `self_nige_rate_minus_field_avg`, `rs_p_nige`,
  `rs_p_nige_x_field_pace`, and `field_nige_pressure` already in the 241-feature set.
- No production change. Iter14 feature store and iter14 base model remain unchanged.
- Artifacts: `tmp/v8/feat-jra-v8-iter14-relfeats/` (temp, not git-tracked),
  `apps/finish-position-predict-container/models/finish-position/jra/iter14-jra-cb-pacestyle-course-v8-relfeats/` (temp model, not deployed).

## Hard Rules Observed

- tmp/ artifacts not git-tracked, no `git add -f`
- Read-only PG: no writes, no INSERTs
- git push: FORBIDDEN (commit only)
- No model deploy or production change (ABORT = no proceed to step 4)
- Container not rebuilt; model_meta.py, per_class.py unchanged
