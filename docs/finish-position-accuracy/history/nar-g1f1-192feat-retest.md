# NAR G1+F1 192-feat Equal-Footing Retest — ABORT

**Date**: 2026-06-12  
**Status**: ABORT — 192-feat store with both G1+F1 bugs fixed is statistically worse than iter12 on equal feature footing

---

## Motivation

The prior `g1-f1-combined-nar-retrain-judge.md` used a 174-feature store that was missing 18 features vs iter12's 192-feature production base (RS v3 pacestyle features + trainer-grade features). That experiment was REJECT vs true production ensemble.

Root cause hypothesis: the 18 dropped features were causing the degradation, not the underlying G1/F1 bugs. This experiment tests that hypothesis by rebuilding the full 192-feature pipeline with both bugs fixed and comparing against iter12 base WF on equal footing (single models, same holdout years 2023-2026).

### Bug descriptions

- **G1 (near-miss NULL explosion)**: `career_place2_rate`, `recent_place2_count_5`, `jockey_career_place2_rate` were ~75% NULL in the year-sliced production build vs 4-10% NULL in full-history build. Fixed by using `feat-nar-v7-baba-21y-f1` as base (full-history near-miss).
- **F1 (pedigree sire/damsire NULL)**: JVD-only JOIN gave 44.8% NULL for sire/damsire; UNION with NVD dropped to 3.9%. Fixed in `pedigree_staging.py`.

---

## Feature store build

Store path: `apps/pc-keiba-viewer/tmp/feat-nar-v7-baba-21y-f1-192/`  
Build pipeline:

1. Base: `feat-nar-v7-baba-21y-f1` (f1-pedigree fixed, full-history near-miss = G1+F1 fixed)
2. - trainer-grade layer (`add-trainer-stable-affinity-features.py --category nar`)
3. - pacestyle layer (`add-pacestyle-features.py --category nar`)
4. - `shusso_tosu` NULL stub (to match iter12 production feature set: constant-NULL BIGINT at index 2, gain=25.3 in iter12 metadata)

Feature parity check vs iter12: 192 features in all years 2010-2024, zero missing vs iter12, zero extra.

---

## Walk-forward training

Script: `tmp/nar_192feat_wf_train.py`  
HPO: iter12 params (`apps/pc-keiba-viewer/finish-position/nar/v8-iter12-hpo/best-params.json`)  
Folds: 2007-2026 (20 folds), 6 parallel workers, NTHREAD_PER_FOLD=2, EARLY_STOPPING_ROUNDS=30  
Total duration: 558.2s  
All 20 folds completed (0 errors, 0 skipped)

Best iteration range: 105 (2014) – 371 (2016); typical 240-370 for large train sets.

---

## Equal-footing judge

Script: `tmp/nar_192feat_g1f1_judge.py`  
Holdout: 2023-2026 (45573 races each)  
Method: 10k paired bootstrap, seed=42

| Metric     | iter12  | new192  | Diff (pp)  | LB95 (pp)  | UB95 (pp) | p       |
| ---------- | ------- | ------- | ---------- | ---------- | --------- | ------- |
| top1       | 58.679% | 58.361% | **−0.318** | −0.476     | −0.162    | <0.0001 |
| place2     | 35.260% | 35.150% | −0.110     | −0.320     | +0.092    | 0.308   |
| place3     | 27.319% | 27.284% | −0.035     | −0.241     | +0.182    | 0.758   |
| fukusho_2p | 87.960% | 87.780% | **−0.180** | **−0.294** | −0.064    | 0.002   |
| top3_box   | 34.736% | 34.771% | +0.035     | −0.118     | +0.189    | 0.668   |

### Gate result: ABORT

- top1 net-negative: True (−0.318pp, p<0.0001, CI entirely below zero)
- fukusho_2p LB95 ≤ 0: True (LB95 = −0.294pp)
- Both ABORT conditions met → no ensemble rebuild

---

## Diagnosis

The 192-feat store with G1+F1 fixes applied is **statistically significantly worse** than iter12 on equal feature footing. This is unexpected: the data quality improvements (lower NULL rates for near-miss and pedigree features) did not translate to better WF predictions.

Possible explanations:

1. **iter12 model adapted to NULL patterns**: The iter12 model was trained on data with the same NULL structure (75% NULL for near-miss in production-style slicing, 44.8% NULL for pedigree). The model's feature routing (`nan_value_treatment=AsFalse`) and learned split thresholds are calibrated to these NULL patterns. Changing NULL rates mid-flight disrupts the learned representation.

2. **NULL as implicit signal**: The near-miss NULL explosion in year-sliced builds may correlate with horse career stage (first appearance at venue, limited history). These NULLs carry real information that the model learned to use. Filling them in removes the signal.

3. **shusso_tosu stub behavior**: The NULL stub is unchanged (iter12 also has it as constant NULL), so this is not a confound.

4. **Feature set is the same** (192 features, identical names and order) — so this is not a feature-count issue. The degradation is purely from changed NULL structure in the training data.

### Key lesson

This experiment confirms the finding from `rootcause-i5-data-quality.md` and the D-phase: **imputing or repairing NULLs in training data for this model is counterproductive**. The GBDTs already optimally route around NULLs; changing NULL rates degrade rather than improve performance. The fixes that appeared promising in isolation do not translate to higher accuracy in practice.

The data quality improvements (G1+F1) should **not** be applied to retrain NAR. iter12 remains the production model with its existing NULL structure.

---

## Decision

**ABORT**. iter12 production artifacts unchanged. No ensemble rebuild. No model_meta/manifest changes.

Ref: `tmp/nar-192feat-g1f1-judge-result.json`, `tmp/nar-192feat-g1f1-wf-summary.json`
