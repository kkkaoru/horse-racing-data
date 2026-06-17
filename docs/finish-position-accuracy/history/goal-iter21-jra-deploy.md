# iter21-jra-cb-2013-v8 — Deploy Log (2026-06-17)

## Status: ABORTED — Holdout Gate FAILED

**iter21-jra-cb-2013-v8 was NOT deployed.** The HPO selection bias check (Step 1)
failed. Full retrain was not performed. active_models untouched.

---

## Context

HPO (goal-jra-iter20-hpo.md, commit 7630606) found a config beating deployed iter20
in 3-fold leave-one-year-out CV:

| Metric | Baseline (iter20) | Best (iter21) | Delta CV |
| ------ | ----------------- | ------------- | -------- |
| top1   | 45.09%            | 45.66%        | +0.57pp  |
| place2 | 23.62%            | 23.58%        | -0.04pp  |
| place3 | 16.77%            | 16.97%        | +0.20pp  |

HPO LB95 (CV-pooled paired bootstrap): top1 +0.21pp (gate ≥ 0.0 → PASS in HPO report).

Best params (Trial 26): depth=7, learning_rate=0.0795, l2_leaf_reg=2.645, od_wait=65,
random_strength=1.098, bagging_temperature=1.194, iterations=1000, seed=2068.

---

## Step 1: Holdout Confirm (FAILED → STOP)

**Goal**: Address HPO selection bias by training BOTH configs on 2013-2022 and
evaluating on 2023-2025 (same holdout as CV folds), computing an exact single-config
delta with paired bootstrap (10k, seed=42).

**Train script**: `tmp/iter21/holdout_confirm.py`
**Train set**: 485,275 rows (2013-2022)
**Holdout set**: 141,523 rows (2023-2025)
**Feature store**: `tmp/v8/feat-jra-v8-iter19-kohan3f-going` (244 features)

### Results

| Metric     | iter20  | iter21  | Delta        |
| ---------- | ------- | ------- | ------------ |
| top1       | 44.486% | 44.814% | **+0.328pp** |
| place2     | 59.585% | 59.566% | -0.019pp     |
| place3     | 43.164% | 42.904% | **-0.260pp** |
| top3_box   | 15.601% | 15.543% | -0.058pp     |
| fukusho_2p | 90.854% | 91.095% | +0.241pp     |
| n_races    | 10,365  | 10,365  |              |

**Paired bootstrap (top1, 10k, seed=42)**:

- top1 delta: +0.328pp
- top1 LB95: **-0.029pp**
- n_races: 10,365

### Gate Evaluation

| Gate condition          | Value    | Threshold  | Result   |
| ----------------------- | -------- | ---------- | -------- |
| top1 LB95 >= 0.0pp      | -0.029pp | >= 0.0pp   | **FAIL** |
| place2 delta >= -0.05pp | -0.019pp | >= -0.05pp | PASS     |
| place3 delta >= -0.05pp | -0.260pp | >= -0.05pp | **FAIL** |

**HOLDOUT GATE: FAIL**

---

## Analysis

The CV gain (+0.57pp top1) was driven by HPO selection bias across 28 trials.
In the independent holdout:

1. **top1 LB95 falls below zero**: The +0.328pp point estimate is within noise
   (LB95 = -0.029pp). The true improvement could be zero or negative.
2. **place3 regresses significantly (-0.260pp)**: Well beyond the -0.05pp floor.
   This regression was masked in CV pooling (2024 fold showed -0.38pp but was
   averaged with positive 2023/2025 folds).

The iter20 deployed config (depth=8, lr=0.05) appears to generalize at least as
well as the HPO config on this holdout distribution, once selection bias is removed.

---

## Outcome

- **No full retrain performed** (Step 2 skipped per task spec: "If this FAILS, STOP")
- **No model artifact created** for iter21-jra-cb-2013-v8
- **No model_meta.py changes** made
- **No Docker image built**
- **active_models NOT touched** (confirmed)
- **JRA production remains**: iter20-jra-cb-2013-v8 (Docker split2 / active_models unchanged)

---

## Diagnosis: Why did the CV gain not survive?

The 3-fold CV evaluated the SAME holdout years (2023-2025) as folds. HPO ran 28
trials against this evaluation set, selecting the best of many configurations.
This creates optimizer's curse / selection bias: the selected config is expected
to outperform on the CV folds simply by chance, inflating the apparent gain.

The holdout confirm trains BOTH configs from scratch (to eliminate fitting-order
effects) and computes an unbiased point estimate. The +0.57pp CV gain shrinks to
+0.328pp with LB95 below zero.

### Best_iteration note

iter20-baseline holdout: best_iteration=353 (vs 174 in full 2013-2025 retrain)
iter21-hpo holdout: best_iteration=486 (od_wait=65 kept training longer)

The higher best_iteration for iter21 (od_wait=65 vs 30) is expected and consistent
with HPO findings, but the regularization improvement does not translate to
statistically significant holdout gains.

---

## Results artifact

`tmp/iter21/holdout_confirm_results.json` — full metric + bootstrap results.
`tmp/iter21/holdout_confirm.log` — full training log.

---

## Next Steps

JRA iter20 remains production. Potential paths:

1. **Signal-level changes**: New horse-level features not in current 244-feature
   store (next experimental cycle).
2. **Longer HPO with explicit selection-bias correction**: Run more trials with
   a separate final holdout that HPO never touches (requires partitioning into
   HPO-eval vs holdout-confirm year sets).
3. **Deeper depth ablation**: The HPO showed depth=6 competitive with higher lr;
   a more controlled experiment on a blind holdout could validate.
