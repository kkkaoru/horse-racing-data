# NAR True-Production-Baseline Judge

**Date**: 2026-06-12
**Decision**: REJECT (both candidates)
**Triggered by**: Commit 1b3815b applied a single-vs-single gate that did NOT account for the
6 per-class ensembles active in the production container. This document re-runs the judge
against the true production baseline and produces the final REJECT verdict.

---

## Background

Commit 1b3815b adopted `nar-xgb-v7-g1f1-combined-wf-21y` (G1+F1 bugs fixed, 174 feat) by
comparing it against `nar-xgb-v7-lineage-wf-21y` (single base vs single base). That was the
correct judge for the model alone, but the production container actually serves:

- **B class**: `iter12-nar-xgb-hpo-v8` (single model, no ensemble)
- **NEW / MUKATSU / A / OP / other**: `iter30-nar-cb-ensemble-{cls}-v8`
  (blend of iter12 XGBoost + iter30 CatBoost residual, weights 0.20–0.69 / 0.31–0.80)
- **C class**: `iter36-nar-lgb-ensemble-C-v8`
  (blend of iter12 XGBoost + iter36 LightGBM LambdaRank residual, weights ~0.498 / 0.502)

The `finish_position_active_models` flip in 1b3815b updated only the **viewer/display registry**.
`apps/finish-position-predict-container/src/predict_lib/model_meta.py` still returns
`iter12-nar-xgb-hpo-v8` for NAR, and all 6 per-class manifests embed `iter12` as `is_baseline`.

This created a **viewer↔container mismatch**: viewer showed g1f1 but container served iter12+ensembles.

---

## Evaluation Setup

- **Holdout window**: 2023–2026 (45,573 races, race-level paired bootstrap, 10,000 iter, seed=42)
- **Production baseline** (TRUE): iter12 + 6 per-class ensembles reconstructed via rank-blend
- **Metrics**: top1, place2, place3, fukusho_2p, top3_box

### Accept gate (strengthened)

| Gate               | Condition                                                      | Must satisfy                |
| ------------------ | -------------------------------------------------------------- | --------------------------- |
| G1 SOLE PLACE VETO | fukusho_2p paired-bootstrap LB95 > 0                           | LB95 strictly positive      |
| G2                 | top1 OR fukusho_2p point-positive                              | at least one of the two > 0 |
| G3                 | ≥ 2 of 5 axes point-positive                                   | count(diff > 0) ≥ 2         |
| G4 VETO FLOOR      | top1 ≥ −0.05pp AND fukusho_2p ≥ −0.05pp AND top3_box ≥ −0.05pp | all three ≥ −0.0005         |

**ALL gates must pass for ADOPT.**

---

## Candidate A: New base alone vs production ensemble

Candidate A = `nar-xgb-v7-g1f1-combined-wf-21y` (174 feat, no ensembles) for all classes.

### Results (holdout 2023–2026, 45,573 races)

| Metric     | Production% | Candidate A% | Diff (pp)    | LB95 (pp)    | UB95 (pp) |
| ---------- | ----------- | ------------ | ------------ | ------------ | --------- |
| top1       | 58.930%     | 58.565%      | **−0.364pp** | −0.533pp     | −0.189pp  |
| place2     | 35.293%     | 35.236%      | −0.057pp     | −0.292pp     | +0.182pp  |
| place3     | 27.183%     | 27.262%      | +0.079pp     | −0.169pp     | +0.323pp  |
| fukusho_2p | 88.116%     | 87.969%      | **−0.147pp** | **−0.281pp** | −0.015pp  |
| top3_box   | 34.629%     | 34.844%      | +0.215pp     | +0.031pp     | +0.402pp  |

### Gate results

| Gate               | Condition            | Value                                  | Pass?    |
| ------------------ | -------------------- | -------------------------------------- | -------- |
| G1 SOLE PLACE VETO | fukusho_2p LB95 > 0  | −0.281pp                               | **FAIL** |
| G2                 | top1 OR fuk positive | both negative                          | **FAIL** |
| G3                 | ≥ 2 positive         | place3 +0.079pp, top3_box +0.215pp = 2 | PASS     |
| G4 top1 floor      | top1 ≥ −0.05pp       | −0.364pp                               | **FAIL** |
| G4 fuk floor       | fukusho_2p ≥ −0.05pp | −0.147pp                               | **FAIL** |
| G4 top3 floor      | top3_box ≥ −0.05pp   | +0.215pp                               | PASS     |

**VERDICT CANDIDATE A: REJECT**

The new base alone is significantly weaker than the ensemble production on top1 (−0.364pp,
p=0.000) and fukusho_2p (−0.147pp, LB95=−0.281pp). top3_box shows a genuine improvement
(+0.215pp, LB95=+0.031pp) but this is not sufficient to clear the G1 sole place veto or G4
veto floors.

---

## Candidate B: New base + 6 rebuilt ensembles vs production ensemble

Candidate B = `nar-xgb-v7-g1f1-combined-wf-21y` + 6 CatBoost residuals rebuilt on the
174-feature `feat-nar-v7-baba-21y-f1` store with g1f1 WF scores as the baseline input feature.

### Ensemble rebuild methodology

- **Feature store**: `apps/pc-keiba-viewer/tmp/feat-nar-v7-baba-21y-f1` (173 numeric features)
- **Base score feature**: `g1f1_score` (g1f1 WF `predicted_score`, leak-free per-fold)
- **Architecture**: CatBoost YetiRank, `depth=8, lr=0.05, l2=3.0, iterations=1000, od_wait=30`
- **Expected feature count**: 174 (173 + 1 g1f1_score) — **confirmed for all 6 classes all folds**
- **Class inclusion chain**: same as iter30 (nar_chain_for)
- **Output**: `tmp/nar-cand-b-residuals/{cls}/predictions/category=nar/race_year={Y}/`

### Blend weight optimization (validation 2018–2022, maximize fukusho_2p)

| Class   | Chosen alpha (w_base) | w_residual | Val fukusho_2p |
| ------- | --------------------- | ---------- | -------------- |
| NEW     | 0.55                  | 0.45       | 92.12%         |
| MUKATSU | 0.80                  | 0.20       | 85.05%         |
| C       | 0.00                  | 1.00       | 88.71%         |
| A       | 0.00                  | 1.00       | 89.30%         |
| OP      | 0.00                  | 1.00       | 86.82%         |
| other   | 0.00                  | 1.00       | 89.01%         |

Note: alpha=0.00 for C/A/OP/other means the residual-only path was optimal on validation,
suggesting the rebuilt CatBoost is a stronger single scorer than the g1f1 base alone on
these classes' validation window.

### Results (holdout 2023–2026, 45,573 races)

| Metric     | Production% | Candidate B% | Diff (pp)    | LB95 (pp)    | UB95 (pp) |
| ---------- | ----------- | ------------ | ------------ | ------------ | --------- |
| top1       | 58.930%     | 58.712%      | **−0.217pp** | **−0.402pp** | −0.035pp  |
| place2     | 35.293%     | 35.275%      | −0.018pp     | −0.250pp     | +0.217pp  |
| place3     | 27.183%     | 27.155%      | −0.029pp     | −0.270pp     | +0.211pp  |
| fukusho_2p | 88.116%     | 88.168%      | +0.053pp     | **−0.075pp** | +0.182pp  |
| top3_box   | 34.629%     | 34.780%      | +0.151pp     | −0.024pp     | +0.329pp  |

### Gate results

| Gate               | Condition            | Value                               | Pass?    |
| ------------------ | -------------------- | ----------------------------------- | -------- |
| G1 SOLE PLACE VETO | fukusho_2p LB95 > 0  | −0.075pp                            | **FAIL** |
| G2                 | top1 OR fuk positive | fuk +0.053pp                        | PASS     |
| G3                 | ≥ 2 positive         | fuk +0.053pp, top3_box +0.151pp = 2 | PASS     |
| G4 top1 floor      | top1 ≥ −0.05pp       | −0.217pp                            | **FAIL** |
| G4 fuk floor       | fukusho_2p ≥ −0.05pp | +0.053pp                            | PASS     |
| G4 top3 floor      | top3_box ≥ −0.05pp   | +0.151pp                            | PASS     |

**VERDICT CANDIDATE B: REJECT**

Candidate B substantially reduces the top1 gap (−0.364pp → −0.217pp) and achieves a small
fukusho_2p gain (+0.053pp) compared to Candidate A. However:

- G1 sole place veto still fails: fukusho_2p LB95 = −0.075pp (not strictly positive)
- G4 top1 veto floor fails: −0.217pp exceeds the −0.05pp floor threshold

The rebuilding reduces the regression, but the new base (174-feat, 18 RS v3 features dropped)
cannot fully compensate for the ensemble-calibration it inherited from iter12.

---

## Final verdict and root cause

**REJECT (both candidates)** — deciding numbers:

- **Candidate A**: G1 fails (fukusho_2p LB95 = −0.281pp), G4 top1 fails (−0.364pp)
- **Candidate B**: G1 fails (fukusho_2p LB95 = −0.075pp), G4 top1 fails (−0.217pp)

Root cause: The 18 features dropped from iter12 (192) to g1f1 (174) include RS v3 probability
features (`rs_p_nige`, `rs_p_senkou`, `rs_p_sashi`, `rs_p_oikomi`, `rs_predicted_class`,
`rs_confidence_entropy`, etc.) and trainer-grade signals. These features matter for top1
precision, and their removal creates a structural deficit that is not compensated by the
G-1/F1 NULL fixes which improve fukusho_2p (set membership) rather than top1 precision.

The single-vs-single gate in commit 1b3815b correctly identified that fukusho*2p improved
vs the \_corresponding* single-model baseline (which shared the same 18 missing features in
the v7-lineage store). But against the ensemble production system, the production iter12
base (with all 192 features including RS v3) retains a top1 advantage that the new base
cannot match.

---

## Deploy state after this judge

| Component                                    | Before                                        | After                                                    |
| -------------------------------------------- | --------------------------------------------- | -------------------------------------------------------- |
| `finish_position_active_models.nar` (viewer) | nar-xgb-v7-g1f1-combined-wf-21y (wrong)       | **iter12-nar-xgb-hpo-v8** (reverted)                     |
| `model_meta.py` NAR base (container)         | iter12-nar-xgb-hpo-v8                         | iter12-nar-xgb-hpo-v8 (unchanged)                        |
| Docker image                                 | finish-position-predict-local:split2 (iter12) | finish-position-predict-local:split2 (iter12, unchanged) |
| Per-class ensembles                          | iter30/iter36 on iter12 base                  | iter30/iter36 on iter12 base (unchanged)                 |

The viewer registry mismatch (introduced by 1b3815b) is now corrected via UPSERT on
`finish_position_active_models`. Container serving is unaffected.

No docker image rebuild required. Container smoke test not applicable (no container change).

---

## Path forward

The new base `nar-xgb-v7-g1f1-combined-wf-21y` cannot beat the production ensemble without RS v3 features. Options to make the G1+F1 fixes production-worthy:

1. **Re-add RS v3 features to the v7-baba-21y-f1 store** and retrain the base to match iter12's feature set (192 feat), then rebuild ensembles on top. This is the full-stack retrain path.
2. **Accept the current iter12+ensemble system as the production baseline** and only consider future base upgrades that include RS v3 signals.

---

## Files

- Judge scripts: `tmp/nar_true_deploy_judge.py` (Candidate A), `tmp/nar_cand_b_residual_train.py` + `tmp/nar_cand_b_blend_eval.py` (Candidate B)
- Judge results: `tmp/nar-true-deploy-judge-result.json`, `tmp/nar-cand-b-blend-eval-result.json`
- Candidate B residuals: `tmp/nar-cand-b-residuals/{NEW,MUKATSU,C,A,OP,other}/` (throwaway, not tracked)
- Residual feature store: `apps/pc-keiba-viewer/tmp/feat-nar-v7-baba-21y-f1`
- New base artifact (retained): `apps/finish-position-predict-container/models/finish-position/nar/nar-xgb-v7-g1f1-combined-wf-21y/`
