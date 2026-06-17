# JRA Implementation-Diverse Base Ensemble (CB + XGB + LGB)

**Date:** 2026-06-17
**Feature store:** `tmp/v8/feat-jra-v8-iter19-kohan3f-going` (244 features)
**Baseline:** iter20-jra-cb-2013-v8 (CatBoost YetiRank, same 244-feat store, train 2013-2025)
**Verdict: ABORT**

---

## Motivation

Prior ensemble experiments tested per-class RESIDUAL stacks on top of the CatBoost
production model. This experiment tests a clean **implementation-diverse base ensemble**
(CatBoost + XGBoost + LightGBM) where all three learners train on the same raw feature
store with independent algorithmic implementations. This avoids residual-stacking
co-adaptation and tests whether framework diversity itself adds signal.

---

## Experimental Design

| Dimension            | Setting                                        |
| -------------------- | ---------------------------------------------- |
| Feature store        | `feat-jra-v8-iter19-kohan3f-going` (244 feats) |
| Train years          | 2013–2022                                      |
| Tune / val for blend | 2023–2024 (blend weights and stack fit here)   |
| **Blind holdout**    | **2025** (gate never saw this)                 |
| Relevance            | finish_position 1→3, 2→2, 3→1, else 0          |
| group_id             | race_id                                        |
| Bootstrap            | 10k iterations, paired race-level, seed=42     |
| Thread limit         | 6 per model                                    |
| Holdout races        | 3,455                                          |

---

## Base Learner Configurations

### CatBoost (iter20 production config)

```
loss_function = YetiRank
depth = 8, lr = 0.05, l2_leaf_reg = 3.0
bootstrap_type = Bayesian, od_type = Iter, od_wait = 30
iterations = 1000, seed = 2068, threads = 6
early-stop val = 2023–2024
```

### XGBoost

```
objective = rank:ndcg, eval_metric = ndcg@3
max_depth = 8, lr = 0.05, lambda = 3.0
subsample = 0.8, colsample_bytree = 0.8, min_child_weight = 5
num_boost_round = 1000, early_stopping_rounds = 30
seed = 2068, threads = 6
```

### LightGBM

```
objective = lambdarank, metric = ndcg@3
num_leaves = 127 (~depth 8), lr = 0.05, reg_lambda = 3.0
min_child_samples = 20, subsample = 0.8, colsample_bytree = 0.8
num_boost_round = 1000, early_stopping_rounds = 30
label_gain = [0, 1, 2, 3]
seed = 2068, threads = 6
```

---

## Base Model Solo Results (Blind 2025 Holdout, n=3,455 races)

| Model               | top1    | place2  | place3  | top3_box | fukusho_2p |
| ------------------- | ------- | ------- | ------- | -------- | ---------- |
| CatBoost (baseline) | 43.994% | 62.923% | 73.575% | 15.514%  | 66.787%    |
| XGBoost             | 45.384% | 63.213% | 73.459% | 14.819%  | 66.252%    |
| LightGBM            | 44.776% | 63.184% | 73.430% | 15.311%  | 66.512%    |

XGBoost and LightGBM individually outperform CatBoost on top1 (+1.39pp and +0.78pp
respectively) but both show marginal place3 regression vs. the CatBoost baseline.

---

## Combination Methods

### (a) Rank-average [w\_CB=0.33, w\_XGB=0.33, w\_LGB=0.33]

Per-race min-max normalization applied to each model's raw scores before averaging.

### (b) Weighted blend [w\_CB=0.00, w\_XGB=0.50, w\_LGB=0.50]

Grid search over (0.0…1.0, step 0.1) × 3 weights summing to 1,
maximizing top1 on tune set (2023–2024).
Optimal weights: CB weight collapses to 0, XGB=LGB=0.50.
Tune top1: rank_avg=45.56%, weighted=45.77%.

### (c) Ridge stacking

Ridge regression (alpha=1.0) fitted on tune-set per-race normalized scores to predict
`-finish_position`. Coefficients: CB=7.279, XGB=-1.299, LGB=2.606.

---

## Blind 2025 Holdout Results

Deltas relative to CatBoost baseline. Bootstrap 10k, seed=42.
Format: `mean_Δ [LB95]` (unit: percentage points).

| Method             | top1    | place2  | place3  | top3_box | fukusho_2p |
| ------------------ | ------- | ------- | ------- | -------- | ---------- |
| CatBoost baseline  | 43.994% | 62.923% | 73.575% | 15.514%  | 66.787%    |
| (a) Rank-average   | 45.065% | 63.415% | 73.922% | 15.485%  | 66.744%    |
| (b) Weighted blend | 45.297% | 63.734% | 73.922% | 15.138%  | 66.527%    |
| (c) Ridge stack    | 44.197% | 63.242% | 73.690% | 15.572%  | 66.975%    |

**Deltas vs CatBoost baseline (ensemble − baseline):**

| Method             | Δtop1 [LB95]            | Δplace2 [LB95]         | Δplace3 [LB95]          |
| ------------------ | ----------------------- | ---------------------- | ----------------------- |
| (a) Rank-average   | +1.073pp [**+0.405pp**] | +0.495pp [−0.174pp]    | +0.351pp [−0.261pp]     |
| (b) Weighted blend | +1.308pp [**+0.492pp**] | +0.815pp [**0.000pp**] | +0.351pp [**−0.434pp**] |
| (c) Ridge stack    | +0.203pp [−0.232pp]     | +0.321pp [−0.087pp]    | +0.117pp [−0.261pp]     |

---

## Accept Gate (Best Method: Weighted Blend)

| Metric | LB95         | Threshold  | Pass?    |
| ------ | ------------ | ---------- | -------- |
| top1   | +0.492pp     | >= 0.00pp  | YES      |
| place2 | 0.000pp      | >= −0.05pp | YES      |
| place3 | **−0.434pp** | >= −0.05pp | **FAIL** |

**Gate result: ABORT** — place3 LB95 = −0.434pp, far below the −0.05pp threshold.

---

## Analysis

### What worked

- XGBoost outperforms CatBoost solo on top1 by +1.39pp (absolute) on the 2025 holdout.
  This is genuine algorithmic diversity: XGBoost's pairwise ranking loss learns a
  complementary decision surface.
- The weighted blend (XGB+LGB, no CB) achieves the highest top1 (+1.308pp mean Δ) and
  passes the top1 and place2 gates with LB95 > 0.
- Rank-average passes top1 LB95 at +0.405pp — signal is real, not noise.

### Why the gate fails

The place3 regression is the gating failure. The weighted blend (which excludes CB
entirely in favor of XGB+LGB) regresses place3 by −0.434pp LB95. CatBoost YetiRank
appears better-calibrated for 3rd-place prediction than XGB rank:ndcg or LGB lambdarank.
The blend weights selected by maximizing tune top1 over-optimise for the winner while
slightly hurting place3.

### Ridge stacking reveals CB dominance

Ridge coefs: CB=7.279, XGB=−1.299, LGB=2.606. The meta-learner heavily weights CB
and assigns a **negative** coefficient to XGB — indicating that CB and XGB are
positively correlated but CB better captures the signal XGB tries to add. The
stack result (+0.203pp top1, LB95=−0.232pp) does not pass gate A.

### Comparison to prior rejected ensembles

Previous per-class residual stacks (iter21 chain, iter22 residual) were also rejected.
This experiment confirms that adding implementation diversity (XGB, LGB) does not
overcome the gate because the place3 LB95 interval crosses −0.05pp. The root issue
is not the stacking mechanism but that no combination of these three base learners
on the same 244-feat store simultaneously improves top1 LB95 AND preserves place3
within −0.05pp.

### XGB solo as alternative

If the task were **top1-only**, XGB solo (+1.39pp) would be compelling. But the
multi-metric gate (top1 + place2 + place3) with the −0.05pp floor for place metrics
correctly catches the place3 regression.

---

## Verdict: ABORT

The implementation-diverse ensemble does not satisfy all gate conditions on the blind
2025 holdout. Production remains iter20-jra-cb-2013-v8 (CatBoost YetiRank, 244 feats).

**No new signal identified.** The 244-feat store is empirically at frontier for this
combination of ranking algorithms. Future avenues:

- New features that improve place3 specifically (currently at 73.6%)
- Per-position calibration after prediction
- Full retraining with expanded feature set (new horse-level signals)
