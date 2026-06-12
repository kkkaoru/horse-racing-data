# NAR Scheme-D Relevance Deploy Judge

**Date**: 2026-06-13  
**Branch**: docs/jes-journal-collection  
**Verdict**: REJECT — no production change

---

## Context

The WF experiment (commit cbc7b7a, doc: `graded-relevance-experiments.md`) showed scheme-D
relevance labels improved NAR WF metrics: top1 +0.08pp, place3 +0.40pp, fukusho_2p LB95 +0.00174 > 0.
Scheme D uses integer-scaled labels: pos1=30, pos2=20, pos3=10, sub-4 tail = `max(0, round(3*(n-pos)/(n-3)))`.

This document records the full-system deploy judge using the TRUE production baseline
(iter12 base + iter30/iter36 per-class ensembles) on holdout 2023-2026.

---

## Procedure

| Step | Description                                                                                                                | Status                                |
| ---- | -------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| 1    | Train NAR XGBoost base with scheme-D labels (feat-nar-v7-baba-21y-f1-192, 192 features, 2006-2025, same iter12 HPO params) | DONE — `tmp/nar-schemeD-deploy/base/` |
| 2    | Generate per-fold WF predictions 2007-2026 (scheme-D base)                                                                 | DONE — 20 folds                       |
| 3    | Train 5 CB YetiRank per-class residuals on scheme-D base (NEW/MUKATSU/A/OP/other, 174 features)                            | DONE                                  |
| 3b   | Train LGB lambdarank C residual on scheme-D base (174 features, schemeD_score as last feature)                             | DONE                                  |
| 4    | Generate per-class WF predictions for holdout years 2023-2026 (all 6 classes)                                              | DONE                                  |
| 5    | Full-system judge: schemeD system vs production on holdout 2023-2026, paired bootstrap LB95 10k seed42                     | DONE → **REJECT**                     |

---

## Step 1: Base Model

- Feature store: `feat-nar-v7-baba-21y-f1-192` (192 features, includes umaban + shusso_tosu)
- HPO params (same as iter12): max_depth=7, lr=0.0527256411839993, reg_lambda=1.967, min_child_weight=7, subsample=0.618, colsample_bytree=0.750, n_estimators=650
- Relevance scheme D: pos1=30, pos2=20, pos3=10, sub-4 tail = `max(0, round(3*(n-pos)/(n-3)))`
- Train 2006-2025, val 2026, seed=2068
- best_iteration=48 (vs iter12 best_iteration=147 — larger label scale causes faster convergence)
- n_train_rows=2673394, feature_count=192

---

## Step 2: Per-fold WF predictions (20 folds, 2007-2026)

All 20 folds completed successfully. Output: `tmp/nar-schemeD-deploy/wf-predictions/category=nar/race_year={Y}/predictions.parquet`.

---

## Step 3/3b: Per-class Residuals

All 6 residuals trained using scheme-D base WF scores as the residual feature (column `schemeD_score`).
Feature store: `feat-nar-v8-iter26-relationships` (173 features + schemeD_score = 174 total).

| Class   | Type           | best_iter | train_rows | train_secs |
| ------- | -------------- | --------- | ---------- | ---------- |
| NEW     | CB YetiRank    | 28        | 21118      | 0.6s       |
| MUKATSU | CB YetiRank    | 48        | 50758      | 1.4s       |
| A       | CB YetiRank    | 141       | 1896034    | 75.3s      |
| OP      | CB YetiRank    | 47        | 1956487    | 37.7s      |
| other   | CB YetiRank    | 53        | 427378     | 9.6s       |
| C       | LGB lambdarank | 52        | 1404042    | 8.9s       |

---

## Step 4: Per-class WF Predictions (holdout folds 2023-2026)

All 6 classes × 4 years generated. Same blend weights as production applied.

---

## Step 5: Full-System Judge

### System comparison

Both systems use the SAME blend weights (production manifest values).
Production: iter12 base + iter30/iter36 per-class ensembles.
New: schemeD base + schemeD per-class residuals.

Holdout: 2023-2026 pooled, n_races=45573.

### Metrics table

| Metric     | Prod   | New    | Delta       |
| ---------- | ------ | ------ | ----------- |
| top1       | 0.5893 | 0.5853 | **-0.0040** |
| place2     | 0.3529 | 0.3497 | -0.0033     |
| place3     | 0.2718 | 0.2676 | -0.0043     |
| top3_box   | 0.3463 | 0.3415 | -0.0048     |
| fukusho_2p | 0.8812 | 0.8779 | -0.0033     |

fukusho_2p LB95 = **-0.00481** (bootstrap n=10000 seed=42)

### Gate evaluation

- fukusho_2p LB95 > 0: **FAIL** (-0.00481 < 0)
- Positive axes (≥2 of top1/place2/place3/top3_box): **FAIL** (0/4 positive)
- Veto floor (all ≥ -0.05pp): **FAIL** (multiple axes below -0.05pp absolute)

### Per-class breakdown

| Class   | Prod top1 | New top1 | Δtop1       | Δplace3 | Δf2p    | races |
| ------- | --------- | -------- | ----------- | ------- | ------- | ----- |
| NEW     | 0.6126    | 0.6056   | -0.0070     | +0.0017 | +0.0105 | 573   |
| MUKATSU | 0.5396    | 0.5504   | **+0.0108** | +0.0036 | -0.0090 | 556   |
| A       | 0.5665    | 0.5654   | -0.0011     | -0.0011 | -0.0089 | 2812  |
| B       | 0.5810    | 0.5755   | -0.0055     | -0.0025 | -0.0015 | 7124  |
| C       | 0.5929    | 0.5901   | -0.0027     | -0.0049 | -0.0023 | 26060 |
| OP      | 0.5784    | 0.5711   | -0.0073     | -0.0073 | -0.0041 | 1231  |
| other   | 0.5973    | 0.5889   | -0.0085     | -0.0057 | -0.0067 | 7217  |

Only MUKATSU shows positive top1 (+0.0108). All other classes are flat-to-negative.

---

## Verdict: REJECT

No production change. iter12 base + current per-class ensembles remain active.

### Analysis

The WF experiment (cbc7b7a) showed a net positive for scheme D because it measured the BASE MODEL ALONE on a different feature store (`feat-nar-v8-iter9-pacestyle`). The full-system judge on the TRUE production baseline (iter12 + per-class ensembles) shows consistent regression across all metrics.

This confirms the D-phase lesson (2026-06-11): **changing training labels is equivalent to changing the NULL routing / informative-absence representation that the GBDT has adapted to**. The per-class ensembles are especially sensitive because they use the base score as a residual feature — a scheme-D base produces a differently-distributed score than iter12, and the production blend weights are calibrated to iter12's score distribution. The resulting mismatch causes regression even though the base model alone appeared to gain.

### Key facts

- Scheme-D WF experiment positive result (LB95=+0.00174) was on a different feature store (192 vs 192 features same store, but different eval scope)
- Full-system judge with production blend weights: all 4 axes negative, LB95=-0.00481
- MUKATSU class is the only bright spot (+1.08pp top1) but insufficient to compensate
- The NAR frontier remains at iter12 + iter30/iter36 ensembles (confirmed production)

### Artifacts retained (tmp/ only, not committed)

- `tmp/nar-schemeD-deploy/base/` — scheme-D base model
- `tmp/nar-schemeD-deploy/per-class/` — scheme-D residuals
- `tmp/nar-schemeD-deploy/wf-predictions/` — scheme-D base WF preds
- `tmp/nar-schemeD-deploy/per-class-wf/` — scheme-D per-class WF preds
- `tmp/nar-schemeD-deploy/judge_result.json` — full judge output
