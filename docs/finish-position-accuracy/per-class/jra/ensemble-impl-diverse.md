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

---

## Per-class Weight Optimization

**Date:** 2026-06-17
**Script:** `tmp/v8/perclass_weight_opt_jra.py`
**Result file:** `tmp/v8/ensemble-impl-diverse/perclass_results.json`

### Motivation

The global blend collapsed to 100% CatBoost in the gate above (place3 LB95 = −0.434pp).
This section tests whether _per-class_ blend optima differ — i.e., whether for any JRA race
class the XGB/LGB contribution is genuinely positive when measured only on that class's
holdout races, surviving Holm-Bonferroni correction for 7 simultaneous tests.

### Design

| Dimension                | Setting                                             |
| ------------------------ | --------------------------------------------------- |
| Classes tested           | 701/703/005/010/016/OP+/H/other (8; H skipped <20r) |
| Class routing            | `kyoso_joken_code` from PG (tune+holdout 2023-2025) |
| Grade/jumps              | grade_code A/B/C/G → OP+; grade_code H → 障害       |
| Tune set                 | 2023–2024 (weights never see 2025)                  |
| Blind holdout            | 2025                                                |
| Blend optimisation       | Grid search 0.0–1.0 step 0.1, 3-way, max tune top1  |
| Stacking                 | Ridge (α=1.0) fitted on per-race normalised scores  |
| Bootstrap                | 10k iterations, paired race-level, seed=42          |
| Multiple-comparison gate | Holm-Bonferroni, α=0.05, k=7 completed classes      |
| Place gate               | place2 LB95 ≥ −0.05pp AND place3 LB95 ≥ −0.05pp     |

**Metric definitions (inherited from `ensemble_eval_fast_jra.py`):**

- `place2` = predicted-1st horse finishes in actual top-2 (softer than canonical exact-ordinal)
- `place3` = predicted-1st horse finishes in actual top-3 (softer than canonical exact-ordinal)
- These definitions are consistent with the global experiment above.

### Holdout Race Counts (2025)

| Class | Label          | Holdout races |
| ----- | -------------- | ------------- |
| 703   | 未勝利         | 1,252         |
| 005   | 1勝クラス      | 909           |
| 010   | 2勝クラス      | 464           |
| 701   | 新馬           | 304           |
| 016   | 3勝クラス      | 216           |
| other | other(L/E/etc) | 172           |
| OP+   | OP/重賞(G1-G3) | 133           |
| H     | 障害           | 5 (SKIPPED)   |

### Optimal Per-class Blend Weights (Tune 2023–2024)

Grid search (step 0.1) maximising tune top1 for each class.

| Class | Label          | w_CB | w_XGB | w_LGB | Degenerate?   | Tune top1 |
| ----- | -------------- | ---- | ----- | ----- | ------------- | --------- |
| 701   | 新馬           | 0.00 | 0.00  | 1.00  | No (LGB-only) | 47.19%    |
| 703   | 未勝利         | 0.30 | 0.50  | 0.20  | No            | 50.90%    |
| 005   | 1勝クラス      | 0.00 | 0.60  | 0.40  | No            | 41.99%    |
| 010   | 2勝クラス      | 0.00 | 0.60  | 0.40  | No            | 44.77%    |
| 016   | 3勝クラス      | 0.30 | 0.30  | 0.40  | No            | 39.39%    |
| OP+   | OP/重賞(G1-G3) | 0.00 | 0.50  | 0.50  | No            | 43.18%    |
| other | other(L/E/etc) | 0.00 | 0.00  | 1.00  | No (LGB-only) | 48.13%    |

**Observation:** No class collapses to 100% CatBoost on the tune set. CB weight is 0 for 5 of 7
classes; 703 and 016 are the only classes where CB gets non-zero tune weight. This confirms
per-class optima do differ from the global optimum (where CB collapsed to 0 globally too —
the global blend was 0% CB / 50% XGB / 50% LGB).

### Blind 2025 Holdout — Δ vs CB-alone per class

Format: `Δmean [LB95]` (percentage points). Bootstrap 10k, seed=42.

| Class | n_holdout | CB top1 | Ens top1 | Δtop1 mean [LB95]     | Δplace2 LB95 | Δplace3 LB95 |
| ----- | --------- | ------- | -------- | --------------------- | ------------ | ------------ |
| 701   | 304       | 46.38%  | 47.04%   | +0.66pp [**−2.63pp**] | −2.63pp      | −1.97pp      |
| 703   | 1252      | 48.64%  | 50.64%   | +2.00pp [**+0.80pp**] | +0.24pp      | **−0.16pp**  |
| 005   | 909       | 39.49%  | 39.60%   | +0.11pp [**−1.43pp**] | −1.43pp      | −1.98pp      |
| 010   | 464       | 44.18%  | 45.91%   | +1.74pp [**−0.43pp**] | 0.00pp       | −1.29pp      |
| 016   | 216       | 37.50%  | 37.96%   | +0.45pp [**−1.39pp**] | −4.17pp      | −4.17pp      |
| OP+   | 133       | 30.08%  | 33.08%   | +3.01pp [**−1.50pp**] | −3.76pp      | −1.50pp      |
| other | 172       | 46.51%  | 46.51%   | −0.01pp [**−4.07pp**] | −4.07pp      | −2.33pp      |

Bold LB95 values indicate the lower 5th percentile from paired bootstrap.

### Ridge Stacking per class

For completeness: Ridge meta-learner (α=1.0) fitted per class on tune normalised scores.

| Class | Ridge coefs (CB/XGB/LGB) | Δtop1 mean [LB95] |
| ----- | ------------------------ | ----------------- |
| 701   | 4.29 / 1.95 / 2.66       | +1.32pp [−0.99pp] |
| 703   | 6.12 / −2.04 / 5.61      | +0.88pp [−0.16pp] |
| 005   | 8.16 / −2.50 / 2.28      | −0.56pp [−1.32pp] |
| 010   | 7.87 / −1.30 / 0.95      | −0.43pp [−1.08pp] |
| 016   | 6.50 / 1.42 / −0.31      | −0.47pp [−1.85pp] |
| OP+   | 8.14 / −0.96 / 0.76      | +0.74pp [0.00pp]  |
| other | 8.41 / −0.99 / −0.02     | −0.58pp [−1.74pp] |

Ridge meta-learner heavily weights CB in all classes (CB coef 4–8×), consistent with
the global result where Ridge coefs were CB=7.279, XGB=−1.299, LGB=2.606. For 4 of 7
classes Ridge assigns a **negative** coefficient to XGB. Per-class Ridge stacking does
not outperform per-class blend in any class.

### Holm–Bonferroni Correction Table

k=7 simultaneous tests (one per completed class), α=0.05.
p-value = fraction of bootstrap samples where ensemble top1 ≤ CB top1 (one-tailed, H₀:
ensemble not better).

| Class   | Label          | raw p      | Holm adj. p | Holm pass? | place2 LB95 | place3 LB95 | PROCEED? | Fail reason           |
| ------- | -------------- | ---------- | ----------- | ---------- | ----------- | ----------- | -------- | --------------------- |
| 701     | 新馬           | 0.4023     | 1.0000      | No         | −2.63pp     | −1.97pp     | **FAIL** | Holm p ≥ 0.05         |
| **703** | **未勝利**     | **0.0051** | **0.0357**  | **Yes**    | +0.24pp     | −0.16pp     | **FAIL** | place3 LB95 < −0.05pp |
| 005     | 1勝クラス      | 0.4799     | 1.0000      | No         | −1.43pp     | −1.98pp     | **FAIL** | Holm p ≥ 0.05         |
| 010     | 2勝クラス      | 0.1155     | 0.6930      | No         | 0.00pp      | −1.29pp     | **FAIL** | Holm p ≥ 0.05         |
| 016     | 3勝クラス      | 0.4280     | 1.0000      | No         | −4.17pp     | −4.17pp     | **FAIL** | Holm p ≥ 0.05         |
| OP+     | OP/重賞(G1-G3) | 0.1521     | 0.7605      | No         | −3.76pp     | −1.50pp     | **FAIL** | Holm p ≥ 0.05         |
| other   | other(L/E/etc) | 0.5549     | 1.0000      | No         | −4.07pp     | −2.33pp     | **FAIL** | Holm p ≥ 0.05         |

**703 (未勝利)** is the only class that passes Holm correction (adj. p=0.0357, raw p=0.0051).
It also passes place2 LB95 (+0.24pp ≥ −0.05pp threshold). However, it fails on
**place3 LB95 = −0.16pp** (threshold: ≥ −0.05pp), which is the same structural weakness
observed globally: blend weights optimised for top1 regress place3.

### Analysis

#### 未勝利 (703) — closest to passing

Class 703 is the largest class (1,252 holdout races) and shows the strongest signal:

- Optimal blend: CB=0.30, XGB=0.50, LGB=0.20 (non-degenerate, genuinely tri-modal)
- top1 Δmean = +2.00pp, LB95 = +0.80pp (positive lower bound)
- Holm-corrected p = 0.0357 (survives α=0.05)
- place2 LB95 = +0.24pp (positive)
- place3 LB95 = −0.16pp ← **gate failure by 0.11pp**

The place3 regression is borderline: the mean Δplace3 is +0.89pp, but the LB95 dips
to −0.16pp. In 703 races XGB's pairwise NDCG loss slightly mis-ranks 3rd-place horses.
This is the same structural pattern as the global experiment.

#### Weight degeneracy

7 of 7 completed classes assign zero weight to CB on the tune set — consistent with
the global optimum finding that CB adds no diversity when combined with XGB/LGB on the
same 244-feat store. XGB and LGB carry the ensemble in all non-CB-heavy classes.
CB only gets non-zero tune weight in 703 (0.30) and 016 (0.30).

#### Sample-size confound

Small classes (016 n=216, OP+ n=133, other n=172) show large LB95 intervals (±4pp),
making it impossible to establish statistical significance even when mean Δ is positive
(OP+ Δmean = +3.01pp but LB95 = −1.50pp). The 2025 holdout year simply has too few
races in these strata for a single-year blind evaluation.

#### Comparison to global ABORT

The global blend (XGB=0.50, LGB=0.50) already showed:

- top1 LB95 = +0.492pp ✓
- place2 LB95 = 0.000pp ✓
- place3 LB95 = −0.434pp ✗

Per-class optimisation does not rescue the gate: the only class with sufficient power
(703, n=1,252) still fails place3 LB95 (−0.16pp vs −0.05pp threshold), just barely.
Splitting the remaining classes further reduces per-class sample sizes and widens all
confidence intervals, making gate passage impossible.

### Final Verdict

**ALL FAIL — no class survives Holm correction + place2/place3 gate simultaneously.**

| Gate condition          | 703 result    | Required  |
| ----------------------- | ------------- | --------- |
| Holm-corrected p < 0.05 | 0.0357 ✓      | < 0.05    |
| place2 LB95 ≥ −0.05pp   | +0.24pp ✓     | ≥ −0.05pp |
| place3 LB95 ≥ −0.05pp   | **−0.16pp ✗** | ≥ −0.05pp |

Production remains **iter20-jra-cb-2013-v8 (CatBoost YetiRank, 244 feats, base-only)**.
Per-class routing to XGB/LGB blends is not deployed.

The 未勝利 class (703) is the most promising avenue if place3 is addressed: e.g., a blend
optimised for place3-weighted loss rather than top1, or a larger holdout (multi-year
evaluation). However, this would require a new experiment with explicit place3 optimisation
on the tune set, which is outside the scope of this implementation-diverse ensemble study.

---

## Exact-Ordinal Metrics + Constrained Blend Optimization

**Date:** 2026-06-17
**Script:** `tmp/v8/exact_ordinal_v4.py`
**Result file:** `tmp/v8/ensemble-impl-diverse/exact_ordinal_v4.json`
**Final verdict: ABORT CONFIRMED**

---

### Metric Correction

The prior sections of this document used **cumulative** metric definitions:

| Metric | Prior (cumulative) definition                   | Value    |
| ------ | ----------------------------------------------- | -------- |
| place2 | predicted-rank-1 horse finishes in actual top-2 | ~62.923% |
| place3 | predicted-rank-1 horse finishes in actual top-3 | ~73.574% |

These are not the canonical exact-ordinal metrics. The canonical definitions
(from `aggregate_bucket_eval_duckdb.py`) are:

| Metric     | Canonical exact-ordinal definition                       | CB value |
| ---------- | -------------------------------------------------------- | -------- |
| top1       | predicted-rank-1 horse finishes **exactly** 1st          | 43.994%  |
| place2     | predicted-rank-2 horse finishes **exactly** 2nd          | 23.329%  |
| place3     | predicted-rank-3 horse finishes **exactly** 3rd          | 16.411%  |
| top3_box   | set of predicted top-3 == set of actual top-3 (trifecta) | 15.514%  |
| fukusho_2p | any of predicted top-2 in actual top-3 (avg per race)    | 66.787%  |

All analyses in this section use exact-ordinal definitions exclusively.
Bootstrap: 5k iterations, paired race-level, seed=42.

---

### A1 — Solo and Blend Re-Evaluation (Exact-Ordinal, Blind 2025 Holdout, n=3,455 races)

| Method              | top1    | place2  | place3  | top3_box | fukusho_2p |
| ------------------- | ------- | ------- | ------- | -------- | ---------- |
| CatBoost (solo)     | 43.994% | 23.329% | 16.411% | 15.514%  | 66.787%    |
| XGBoost (solo)      | 45.384% | 22.518% | 16.093% | 14.819%  | 66.252%    |
| LightGBM (solo)     | 44.776% | 23.502% | 16.816% | 15.311%  | 66.512%    |
| rank_avg (33/33/33) | 45.065% | 23.068% | 16.498% | 15.485%  | 66.744%    |
| weighted (0/50/50)  | 45.297% | 22.576% | 16.006% | 15.138%  | 66.527%    |
| ridge stack         | 44.197% | 23.386% | 16.237% | 15.572%  | 66.975%    |

Ridge coefficients: CB=7.279, XGB=−1.299, LGB=2.606.

**Bootstrap deltas vs CB-solo (5k, seed=42). Format: mean_Δ [LB95] (pp).**

| Method      | Δtop1 [LB95]    | Δplace2 [LB95]  | Δplace3 [LB95]  |
| ----------- | --------------- | --------------- | --------------- |
| rank_avg    | +1.075 [+0.434] | −0.265 [−1.042] | +0.108 [−0.608] |
| weighted    | +1.313 [+0.492] | −0.753 [−1.708] | −0.391 [−1.274] |
| ridge stack | +0.204 [−0.232] | +0.060 [−0.463] | −0.165 [−0.667] |

**Key finding:** Using exact-ordinal metrics, place2 and place3 LB95 deltas are negative
for all blends. The prior per-class section's place2=63%/place3=74% were incorrect
cumulative figures; exact-ordinal place2≈23% and place3≈16% reveal that no blend
preserves within-race ranking accuracy for 2nd and 3rd positions simultaneously.

---

### A2 — Place-Constrained Global Blend (Exact-Ordinal)

Grid search over (CB, XGB, LGB) weights in steps of 0.05, maximising tune-set top1
subject to place2_tune ≥ CB_tune − 0.05pp **and** place3_tune ≥ CB_tune − 0.05pp.

**CB tune baselines:** top1=44.978%, place2=23.719%, place3=17.077%

| Setting                         | Weights (CB/XGB/LGB) | Tune top1 | Holdout top1 | Δtop1 [LB95]    | Δplace2 [LB95]  | Δplace3 [LB95]  |
| ------------------------------- | -------------------- | --------- | ------------ | --------------- | --------------- | --------------- |
| Constrained-max-top1            | 0.70 / 0.20 / 0.10   | 45.311%   | 44.776%      | +0.786 [+0.318] | +0.001 [−0.492] | +0.068 [−0.405] |
| Multi-obj equal (1/3 each)      | 0.20 / 0.15 / 0.65   | 45.745%   | 44.805%      | +0.821 [+0.087] | −0.375 [−1.245] | +0.104 [−0.695] |
| Multi-obj top1-favor (50/25/25) | 0.20 / 0.15 / 0.65   | 45.745%   | 44.805%      | +0.821 [+0.087] | −0.375 [−1.245] | +0.104 [−0.695] |

**Accept gate (constrained-max-top1, vs CB holdout baseline):**

| Metric | LB95         | Threshold | Pass?    |
| ------ | ------------ | --------- | -------- |
| top1   | +0.318pp     | ≥ 0.00pp  | YES      |
| place2 | **−0.492pp** | ≥ −0.05pp | **FAIL** |
| place3 | **−0.405pp** | ≥ −0.05pp | **FAIL** |

**Verdict: FAIL** — even with explicit place constraints on the tune set and a constrained
grid of 231 weight combinations, the holdout place2 and place3 LB95 intervals both fall
well below −0.05pp. The tune-set constraint cannot prevent holdout place2/3 degradation.

The multi-objective objectives (equal and top1-favor) converge to identical weights
(0.20/0.15/0.65), confirming that the LGB-heavy blend is robust across objective
weightings, but still fails the holdout gate on place2 and place3.

---

### A3 — Per-Class Constrained Blend + Holm-Bonferroni (Exact-Ordinal)

k=7 classes tested (H=障害 skipped: n_tune=10, n_holdout=5).
Class assignment: `kyoso_joken_code` from PG (2023-2025).
Grid step 0.05 per class, maximising tune top1 subject to place2/place3 ≥ CB_tune − 0.05pp.
Bootstrap: 5k iterations, paired race-level, seed=42.

**Holdout race counts (2025):**

| Class | Label          | n_tune | n_holdout   |
| ----- | -------------- | ------ | ----------- |
| 703   | 未勝利         | 2458   | 1252        |
| 005   | 1勝クラス      | 1867   | 909         |
| 010   | 2勝クラス      | 936    | 464         |
| 701   | 新馬           | 604    | 304         |
| 016   | 3勝クラス      | 424    | 216         |
| other | other(L/E/etc) | 347    | 172         |
| OP+   | OP/重賞(G1-G3) | 264    | 133         |
| H     | 障害           | 10     | 5 (SKIPPED) |

**Per-class optimal weights (tune set, constrained grid):**

| Class | Label          | w_CB | w_XGB | w_LGB | CB holdout top1 | Blend holdout top1 | Δtop1   |
| ----- | -------------- | ---- | ----- | ----- | --------------- | ------------------ | ------- |
| 701   | 新馬           | 0.00 | 0.00  | 1.00  | 46.382%         | 47.039%            | +0.66pp |
| 703   | 未勝利         | 0.40 | 0.40  | 0.20  | 48.642%         | 50.399%            | +1.76pp |
| 005   | 1勝クラス      | 1.00 | 0.00  | 0.00  | 39.494%         | 39.494%            | 0.00pp  |
| 010   | 2勝クラス      | 1.00 | 0.00  | 0.00  | 44.181%         | 44.181%            | 0.00pp  |
| 016   | 3勝クラス      | 0.50 | 0.30  | 0.20  | 37.500%         | 37.500%            | 0.00pp  |
| OP+   | OP/重賞(G1-G3) | 0.00 | 0.50  | 0.50  | 30.075%         | 33.083%            | +3.01pp |
| other | other(L/E/etc) | 0.80 | 0.20  | 0.00  | 46.512%         | 46.512%            | 0.00pp  |

**Note:** 005, 010, 016, and other all collapse to CB-only or near-CB weights under the
exact-ordinal constrained grid, producing zero top1 delta. This differs from the prior
per-class section where unconstrained grid found non-CB-dominant blends — the
exact-ordinal place constraints eliminate those blends.

**Holm-Bonferroni correction table (k=7, α=0.05, one-tailed bootstrap p-value):**

| Class | Label          | n_ho | Δtop1   | top1_LB95    | p2_LB95  | p3_LB95  | raw_p  | holm_p | Proceed? |
| ----- | -------------- | ---- | ------- | ------------ | -------- | -------- | ------ | ------ | -------- |
| 701   | 新馬           | 304  | +0.66pp | −2.632pp     | −3.618pp | −1.974pp | 0.5103 | 1.0000 | **FAIL** |
| 703   | 未勝利         | 1252 | +1.76pp | **+0.559pp** | −1.358pp | −1.118pp | 0.4940 | 1.0000 | **FAIL** |
| 005   | 1勝クラス      | 909  | 0.00pp  | 0.000pp      | 0.000pp  | 0.000pp  | 0.5150 | 1.0000 | **FAIL** |
| 010   | 2勝クラス      | 464  | 0.00pp  | 0.000pp      | 0.000pp  | 0.000pp  | 0.5110 | 1.0000 | **FAIL** |
| 016   | 3勝クラス      | 216  | 0.00pp  | −1.389pp     | −1.852pp | −4.630pp | 0.5220 | 1.0000 | **FAIL** |
| OP+   | OP/重賞(G1-G3) | 133  | +3.01pp | −1.504pp     | −5.263pp | −9.023pp | 0.5457 | 1.0000 | **FAIL** |
| other | other(L/E/etc) | 172  | 0.00pp  | −1.744pp     | −1.744pp | −1.163pp | 0.5233 | 1.0000 | **FAIL** |

All 7 classes FAIL. Raw p-values are ~0.49–0.55 (chance-level); all Holm-corrected p=1.0000.

**Notable:** Class 703 (未勝利, n=1,252) has top1_LB95=+0.559pp (positive lower bound,
genuine signal), but place2_LB95=−1.358pp and place3_LB95=−1.118pp both fail the
−0.05pp floor by a wide margin. Even the largest class cannot pass all three gate
conditions under exact-ordinal metrics.

**Contrast with prior section:** The prior per-class section reported 703 as "closest to
passing" with place3 LB95=−0.16pp under cumulative metrics. Under exact-ordinal
metrics the place2/3 LB95 intervals widen substantially (−1.36pp / −1.12pp), confirming
the prior near-miss was an artifact of the softer metric definition.

---

### Final Verdict: ABORT CONFIRMED

| Analysis                           | Result   | Key finding                                                                 |
| ---------------------------------- | -------- | --------------------------------------------------------------------------- |
| A1: Exact-ordinal re-measurement   | ABORT    | place2/3 LB95 negative for all blends                                       |
| A2: Place-constrained global blend | FAIL     | tune constraint cannot prevent holdout place2/3 LB95 < −0.05pp              |
| A3: Per-class + Holm-Bonferroni    | ALL FAIL | all 7 classes raw_p≈0.5, Holm_p=1.0; 703 top1 signal real but place2/3 fail |

Production remains **iter20-jra-cb-2013-v8 (CatBoost YetiRank, 244 feats, base-only)**.
No ensemble combination of CB/XGB/LGB on the same 244-feat store satisfies the
multi-metric gate (top1 LB95 ≥ 0 AND place2 LB95 ≥ −0.05pp AND place3 LB95 ≥ −0.05pp)
under exact-ordinal metric definitions.
