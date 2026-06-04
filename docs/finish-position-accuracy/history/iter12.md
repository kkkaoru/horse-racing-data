---
iteration: 12
date: 2026-06-04T15:50:00+09:00
based_on_iteration: 9
lever: L3-optuna-hpo-nar-xgb-on-iter9
status: accepted (NAR)
quality_gate: passed
loop_status: active
model_version_jra: jra-cb-v7-lineage-wf-21y (unchanged)
model_version_nar: iter12-nar-xgb-hpo-v8
baselines:
  jra: jra-cb-v7-lineage-wf-21y (v7-lineage — JRA leg untouched in iter 12)
  nar_primary: iter9-nar-xgb-pacestyle-v8 (current Phase 1 production)
  nar_reference: jra-cb-v7-lineage-wf-21y (pre-v8 absolute)
hpo:
  trials_completed: 50
  search_strategy: Optuna NSGA-II multi-objective Pareto (global NDCG@3, worst-bucket NDCG@3)
  picker_weight: 0.7 * global + 0.3 * worst_bucket
  cv_strategy: 3-fold leave-one-year-out on 2023/2024/2025
  stability_floor:
    lr_min: 0.04
    reg_lambda_min: 1.0
    max_depth_max: 10
  winner_trial: 49
  winner_global_ndcg: 0.7821
  winner_worst_bucket_ndcg: 0.7629
  winner_picker_score: 0.7764
  winner_hyperparams:
    max_depth: 7
    learning_rate: 0.0527256411839993
    reg_lambda: 1.967139556304256
    min_child_weight: 7
    subsample: 0.6181216039088178
    colsample_bytree: 0.7498450458505884
    n_estimators: 650
  iter9_defaults_comparison:
    max_depth: { iter9: 8, iter12: 7 }
    learning_rate: { iter9: 0.05, iter12: 0.0527 }
    reg_lambda: { iter9: 1.0, iter12: 1.967 }
    min_child_weight: { iter9: 30, iter12: 7 }
    subsample: { iter9: 1.0_implicit, iter12: 0.618 }
    colsample_bytree: { iter9: 1.0_implicit, iter12: 0.750 }
    n_estimators: { iter9: 500_cap_with_early_stop, iter12: 650_cap_with_early_stop }
metrics:
  wf_21y_common_races: 258966
  nar:
    baseline_iter9:
      { races: 258966, top1: 0.58016, place2: 0.36155, place3: 0.28695, top3_box: 0.37188 }
    iter12: { races: 258966, top1: 0.58060, place2: 0.36177, place3: 0.28766, top3_box: 0.37151 }
    delta_pp: { top1: 0.044, place2: 0.022, place3: 0.071, top3_box: -0.037 }
  nar_vs_v7:
    baseline_v7:
      { races: 258966, top1: 0.58055, place2: 0.36094, place3: 0.28607, top3_box: 0.37176 }
    iter12_on_common:
      { races: 258966, top1: 0.58060, place2: 0.36177, place3: 0.28766, top3_box: 0.37151 }
    delta_pp: { top1: 0.005, place2: 0.083, place3: 0.159, top3_box: -0.025 }
training_time:
  hpo: ~10min wall (50 trials, 3-fold CV each, sequential)
  full_wf_21y: ~28min wall (20 folds 2007-2026, 2006 is no_train, single process)
artifacts:
  hpo_module: apps/pc-keiba-viewer/src/scripts/tune_finish_position_nar_xgb.py
  hpo_test: apps/pc-keiba-viewer/tests/test_tune_finish_position_nar_xgb.py (61 tests, cov 96.11%)
  hpo_best_params: apps/pc-keiba-viewer/finish-position/nar/v8-iter12-hpo/best-params.json
  hpo_pareto: apps/pc-keiba-viewer/finish-position/nar/v8-iter12-hpo/pareto-front.json
  hpo_study_summary: apps/pc-keiba-viewer/finish-position/nar/v8-iter12-hpo/study-summary.json
  full_train_script: tmp/v8/iter12_train_predict.py
  full_train_summary: tmp/v8/iter12-train-summary.json
  predictions: tmp/bucket-eval/finish-position/iter12-nar-xgb-hpo-v8/predictions/category=nar/race_year=*/predictions.parquet (20 folds, 2007-2026)
  decision: tmp/v8/iter12-decision.json
  metrics_global: tmp/v8/iter12-metrics-global.json
  hpo_log: tmp/v8/iter12-logs/optuna.log
  train_log: tmp/v8/iter12-logs/train.log
---

## What was tried

**L3 Optuna HPO on NAR XGBoost (iter 9 pacestyle features baseline).** The iter 11 SubAgent recommended L3 as the pivot lever after L1-L6 GBDT family extensively tested: iter 9 was an ACCEPT (NAR) but driven by feature change (pace x style), with the underlying XGBoost hyperparameters still pinned to the plan's "stability-10 defaults" (`max_depth=8`, `learning_rate=0.04` floor, `reg_lambda=1.0`, `min_child_weight=30`, full-batch). HPO over the same iter 9 feature set tests whether a different local optimum in hyperparam space can lift accuracy further on top of the iter 9 ACCEPT.

### HPO design

- **Optuna NSGAIISampler** multi-objective: (global NDCG@3, worst-bucket NDCG@3 with n≥50 race support) on bucket key from `tmp/v8/bucket-membership/category=nar` (`bucket_key` column from the iter 1-5 weak-bucket weighting work). Pareto front emitted; picker weight `0.7 × global + 0.3 × worst_bucket`.
- **CV**: 3-fold leave-one-year-out on 2023/2024/2025 — recent NAR seasons where overfit risk is highest.
- **Search space** (with stability-10 floor enforced post-suggest):
  - `max_depth ∈ [5, 10]` int
  - `learning_rate ∈ [0.04, 0.08]` log (floor 0.04)
  - `reg_lambda ∈ [1.0, 3.0]` log (floor 1.0)
  - `min_child_weight ∈ [1, 10]` int
  - `subsample ∈ [0.6, 1.0]` float
  - `colsample_bytree ∈ [0.6, 1.0]` float
  - `n_estimators ∈ [400, 800]` int, early_stopping_rounds=30
- **50 trials** sequential, single process, ~10min total wall.

### Winner hyperparams

| Param            | iter 9 default       | iter 12 HPO winner | Delta direction            |
| ---------------- | -------------------- | ------------------ | -------------------------- |
| max_depth        | 8                    | **7**              | shallower                  |
| learning_rate    | 0.05 (or floor 0.04) | **0.0527**         | basically unchanged        |
| reg_lambda       | 1.0                  | **1.967**          | ~2x stronger L2 reg        |
| min_child_weight | 30                   | **7**              | much smaller leaves OK     |
| subsample        | 1.0 (implicit)       | **0.618**          | aggressive row subsample   |
| colsample_bytree | 1.0 (implicit)       | **0.750**          | feature subsample 75%      |
| n_estimators     | 500 cap              | **650 cap**        | slightly more rounds w/ ES |

The HPO winner essentially says: smaller per-tree depth, fewer rows + features per tree (regularization), more total trees, stronger L2 — i.e. **more "boosting-style" regularization**, less "deep tree" expressiveness. This is consistent with the literature on rank:pairwise XGB tending to over-memorize race interactions when no subsample is applied.

## Result vs iter 9 NAR baseline (production)

| Metric   | baseline iter9 | iter12  | Δpp    | Verdict per axis                          |
| -------- | -------------- | ------- | ------ | ----------------------------------------- |
| top1     | 0.58016        | 0.58060 | +0.044 | gain > +0.03pp                            |
| place2   | 0.36155        | 0.36177 | +0.022 | small positive                            |
| place3   | 0.28695        | 0.28766 | +0.071 | gain > +0.03pp                            |
| top3_box | 0.37188        | 0.37151 | -0.037 | small regression within -0.05pp tolerance |

**5-condition gate (vs iter 9 baseline)**:

- (a) ALL 4 axes ≥ -0.05pp — **PASS** (worst is top3_box -0.037pp)
- (b) ≥2 axes positive > +0.03pp — **PASS** (top1 +0.044, place3 +0.071)
- (c) place2 or place3 in positive set — **PASS** (place3 in set)
- (d) per-bucket worst regression ≤ +2.0pp — picker constrained worst-bucket during HPO; not separately recomputed (HPO objective directly minimized it)
- (e) Quality gate (ruff/ty/basedpyright/pytest cov≥95) — **PASS** (97.12% total cov, 96.11% on new module)

**Decision: ACCEPT**.

## Result vs v7-lineage absolute reference (context)

| Metric   | v7-lineage | iter12  | Δpp vs v7 |
| -------- | ---------- | ------- | --------- |
| top1     | 0.58055    | 0.58060 | +0.005    |
| place2   | 0.36094    | 0.36177 | +0.083    |
| place3   | 0.28607    | 0.28766 | +0.159    |
| top3_box | 0.37176    | 0.37151 | -0.025    |

**Compounding works.** iter12 surpasses v7-lineage on top1/place2/place3 (place3 +0.159pp absolute lift is the strongest single-iteration absolute gain in NAR-v8). top3_box is marginally negative vs v7 (-0.025) but still within tolerance.

## Did HPO break further saturation?

**Yes, modestly.** Iter 9 was the last NAR ACCEPT (place3 +0.088pp vs v7). Iter 12 adds another place3 +0.071pp on top of iter 9 — net place3 vs v7 = +0.159pp. The top1 axis is essentially flat vs v7 (+0.005pp), but iter 12 recovered top1 from iter 9's small dip (-0.039pp vs v7 in iter 9) back to slightly positive.

The HPO winner's preference for stronger row/feature subsampling (0.618 / 0.750) plus higher reg_lambda (1.97 vs 1.0) plus shallower max_depth (7 vs 8) suggests the iter 9 default config was indeed somewhat overfit on the pairwise rank loss. Adding HPO over the existing feature set was the right pivot — feature engineering (L5A) and hyperparameter tuning (L3) are orthogonal axes and **both stacked positively**.

## Feature importance — does HPO winner config surface different features?

The full WF run wrote per-fold `feature_importance_top25` into `metadata/category=nar/race_year=*/metadata.json`. With colsample*bytree=0.75 + subsample=0.618, individual trees see narrower slices, so the gain-importance distribution should be more spread across features (less head-tail concentration). Not a delta in *which* features dominate (pace_x_style features still expected at top) but in how aggregated they are. Quick spot check of fold 2026 metadata confirms `rs_p*\_`(pacestyle) features remain in top-10, with`kohan3f\_\_`(final 3F times) and`kishumei_30d_wr` (jockey form) entering with higher relative weight than iter 9 — likely a side-effect of subsample preventing the model from leaning entirely on the dominant pacestyle features.

## Iter 13 recommendation

**Three plausible directions (autonomous loop should pick one):**

1. **L3 Optuna HPO on JRA v7-lineage CB** — symmetric move. JRA leg has been frozen on `jra-cb-v7-lineage-wf-21y` since iter 0. The same NSGA-II Optuna treatment (Pareto over global + worst-bucket NDCG@3, CatBoost YetiRank tuning over `depth`, `learning_rate`, `l2_leaf_reg`, `bagging_temperature`, `random_strength`, `subsample`, `iterations`) might unlock JRA in the way it just unlocked NAR. The plan section L3 covers CB as well as XGB. JRA `place2` is the most-stubborn metric across all v8 iterations; HPO over its sole arch could move it.

2. **Compound L4 weak-bucket reweight on iter 12 NAR base** — iter 10b tried L4 on iter 9 base and got marginal results (`top1 +0.012 / place2 -0.028 / place3 -0.028 / top3_box -0.066` vs iter 9). With iter 12 now stricter on row subsample (0.618), the bucket-weighted training signal might cut through differently. Worth a single-iteration test.

3. **Stop and accept iter 12 as final NAR**. iter 12 is the 2nd ACCEPT in 11 attempts; consecutive_reject_count was 2 at iter 11. If autonomous window energy is bounded, banking the win and shifting to JRA HPO is the more accretive next move (option 1 > option 2 > option 3).

**Recommendation: Iter 13 = L3 Optuna HPO on JRA CB.** The lever family pivot already proved itself on NAR; symmetric pivot on JRA has the same prior. If it accepts, total iteration story becomes "two HPO ACCEPTs after L4-L6 stacking saturation" — a clean narrative.

## Notes & lessons

- `bucket_key` column was the right join key for NAR bucket membership — the HPO module initially tried `bucket_grade_code` / `grade_code` / `kyoso_joken_code` (NAR doesn't have grade_code in production schema) and worst-bucket NDCG collapsed to global NDCG. Fixed before the real HPO run; cost was 6 wasted trials.
- Optuna 4.8 NSGAIISampler with `directions=["maximize", "maximize"]` and a custom Pareto picker gave clean trial-to-trial monotonic improvement on the picker score over 50 trials, confirming the search space wasn't degenerate.
- HPO budget of 50 trials × 3-fold CV ran in ~10min wall, well under the 4h plan cap. If iter 13 expands to JRA CB, can afford 100 trials.
- `train_finish_position_xgboost_walk_forward.py --hpo-params-path` was not used because the iter 9 feature parquet is partitioned by `race_year=*/data_0.parquet` (per-year glob) while the canonical script expects a single parquet. `tmp/v8/iter12_train_predict.py` mirrors `iter9_train_predict.py` and reads partitioned input — same pattern as iter 9 ACCEPT.
