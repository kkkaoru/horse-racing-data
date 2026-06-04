---
iteration: 13
date: 2026-06-04T18:00:00+09:00
based_on_iteration: 9
lever: L3-optuna-hpo-jra-cb-on-pacestyle
status: rejected (JRA)
quality_gate: passed
loop_status: active
model_version_jra: jra-cb-v7-lineage-wf-21y (unchanged — iter 13 rejected)
model_version_nar: iter12-nar-xgb-hpo-v8 (unchanged from iter 12 accept)
baselines:
  jra: jra-cb-v7-lineage-wf-21y (v7-lineage — unchanged since iter 0)
  nar_primary: iter12-nar-xgb-hpo-v8 (current Phase 1 production)
hpo:
  trials_completed: 50
  search_strategy: Optuna NSGA-II multi-objective Pareto (global NDCG@3, worst-bucket NDCG@3)
  picker_weight: 0.7 * global + 0.3 * worst_bucket
  cv_strategy: 3-fold leave-one-year-out on 2023/2024/2025
  stability_floor:
    lr_min: 0.04
    l2_leaf_reg_min: 3.0
    depth_max: 10
  winner_trial: 39
  winner_global_ndcg: 0.6357
  winner_worst_bucket_ndcg: 0.5561
  winner_picker_score: 0.6118
  winner_hyperparams:
    depth: 5
    learning_rate: 0.06599936368852644
    l2_leaf_reg: 4.206599397591388
    bagging_temperature: 0.03647394964718076
    random_strength: 3.404625331582255
    iterations: 506
    bootstrap_type: Bayesian
  v7_lineage_defaults_comparison:
    depth: { v7: 8, iter13: 5 }
    learning_rate: { v7: 0.05, iter13: 0.066 }
    l2_leaf_reg: { v7: 3.0, iter13: 4.21 }
    iterations: { v7: 500_cap_with_early_stop, iter13: 506_cap_with_early_stop }
    bootstrap_type: { v7: implicit_Bayesian_low_temp, iter13: Bayesian_low_temp }
    bagging_temperature: { v7: 1.0_cb_default, iter13: 0.036 }
    random_strength: { v7: 1.0_cb_default, iter13: 3.4 }
metrics:
  wf_21y_common_races: 66964
  jra:
    baseline_v7:
      { races: 66964, top1: 0.40139, place2: 0.21730, place3: 0.16203, top3_box: 0.14242 }
    iter13: { races: 66964, top1: 0.40017, place2: 0.21725, place3: 0.16056, top3_box: 0.14117 }
    delta_pp: { top1: -0.122, place2: -0.004, place3: -0.146, top3_box: -0.125 }
training_time:
  hpo: ~22min wall (50 trials, 3-fold CV each, sequential, ~26 sec/trial)
  full_wf_21y: ~75min wall (20 folds 2007-2026, single CPU sequential)
artifacts:
  hpo_module: apps/pc-keiba-viewer/src/scripts/tune_finish_position_jra_cb.py
  hpo_test: apps/pc-keiba-viewer/tests/test_tune_finish_position_jra_cb.py (61 tests, cov 96.13%)
  hpo_best_params: apps/pc-keiba-viewer/finish-position/jra/v8-iter13-hpo/best-params.json
  hpo_pareto: apps/pc-keiba-viewer/finish-position/jra/v8-iter13-hpo/pareto-front.json
  hpo_study_summary: apps/pc-keiba-viewer/finish-position/jra/v8-iter13-hpo/study-summary.json
  full_train_script: tmp/v8/iter13_train_predict.py
  full_train_summary: tmp/v8/iter13-train-summary.json
  predictions: tmp/bucket-eval/finish-position/iter13-jra-cb-hpo-v8/predictions/category=jra/race_year=*/predictions.parquet (20 folds, 2007-2026)
  decision: tmp/v8/iter13-decision.json
  metrics_global: tmp/v8/iter13-metrics-global.json
  delta_csv: tmp/v8/iter13-jra-delta.csv
  hpo_log: tmp/v8/iter13-logs/optuna.log
  train_log: tmp/v8/iter13-logs/train.log
---

## What was tried

**L3 Optuna HPO on JRA CatBoost (iter 9 pacestyle features baseline).** The iter 12 SubAgent recommended L3 as the symmetric pivot lever after iter 12 confirmed L3 (Optuna HPO) is a productive axis on top of L5A (feature engineering) for NAR. NAR iter 12 was an ACCEPT (place3 +0.071pp vs iter9 baseline, +0.159pp vs v7-lineage). The hypothesis: JRA's CB baseline has been frozen at v7-lineage hyperparams since iter 0 (10+ iterations); a Bayesian-Pareto search over the same iter 9 JRA pacestyle features might break that saturation.

### HPO design

- Multi-objective NSGA-II over (global NDCG@3, worst-bucket NDCG@3)
- Picker score: `0.7 * global + 0.3 * worst_bucket` (same as iter 12 NAR)
- Stability floor enforced: `learning_rate >= 0.04`, `l2_leaf_reg >= 3.0`, `depth <= 10`
- CV: 3-fold leave-one-year-out (2023/2024/2025) — identical CV protocol to iter 12 NAR
- Search space (CatBoost-specific):
  - `depth in [4, 9]` int
  - `learning_rate in [0.04, 0.08]` log
  - `l2_leaf_reg in [3, 10]` log
  - `bagging_temperature in [0.0, 1.5]` float
  - `random_strength in [0.5, 5.0]` float
  - `iterations in [400, 1000]` int
  - `bootstrap_type = Bayesian` (only mode supporting `bagging_temperature` per CB validator)

### Winner Pareto point (trial 39 of 50)

```
depth                = 5         (v7-lineage default: 8)
learning_rate        = 0.0660    (v7-lineage default: 0.05)
l2_leaf_reg          = 4.21      (v7-lineage default: 3.0)
bagging_temperature  = 0.036     (CB default: 1.0)
random_strength      = 3.40      (CB default: 1.0)
iterations           = 506       (v7-lineage cap: 500)
bootstrap_type       = Bayesian
```

CV scores: global NDCG@3 = 0.6357, worst-bucket NDCG@3 = 0.5561, picker = 0.6118.

The search settled on a **shallower (depth 5 vs 8) but higher-LR (0.066 vs 0.05)** configuration with strong noise injection (low bagging temperature near deterministic ≈ Bernoulli-like + high random_strength). This is the opposite of iter 12 NAR which kept depth 7 (close to default 8) with moderate noise.

## Result vs JRA v7-lineage baseline (66,964 common races, 20 WF folds 2007-2026)

| Metric   | v7-lineage | iter 13 | delta_pp   |
| -------- | ---------- | ------- | ---------- |
| top1     | 40.139%    | 40.017% | **-0.122** |
| place2   | 21.730%    | 21.725% | -0.004     |
| place3   | 16.203%    | 16.056% | **-0.146** |
| top3_box | 14.242%    | 14.117% | **-0.125** |

### 5-condition gate

- (a) `all delta >= -0.05pp` → **false** (3 of 4 metrics breach -0.05pp tolerance)
- (b) `>= 2 axes gain > +0.03pp` → **false** (positive_metric_set is empty)
- (c) `place2 OR place3 gain` → **false**
- (d) per-bucket worst regression — not evaluated (gate already failed on a/b/c)
- (e) Quality gate green — passed (full WF completed, lint/types/tests green)

Decision: **REJECT**.

## Why iter 13 lost

1. **Bayesian vs Bernoulli bootstrap mismatch.** v7-lineage CB uses CB's default Bayesian bootstrap with `bagging_temperature=1.0` (full diversity). The HPO winner picked `bagging_temperature=0.036` (near-deterministic — collapses Bayesian into a low-variance Bernoulli-like draw) plus `random_strength=3.40` (above default 1.0). This produces over-noisy split scoring without the variance reduction of true sampling. v7-lineage's standard default behaves better as a baseline.

2. **Depth 5 too shallow.** v7-lineage runs depth 8, deep enough to capture JRA's high-cardinality categorical interactions (keibajo × distance × surface). The Pareto winner went shallow (depth 5) to favor the worst-bucket NDCG signal, but that sacrificed global accuracy on the full WF — exactly the failure mode iter 10a (depth 10) also showed in reverse (deeper hurt place3).

3. **CV / WF mismatch.** The HPO CV used 3-fold leave-one-year-out on 2023-2025 only. The full WF retrain spans 20 folds (2007-2026) with monotonic train-end shifts. Hyperparameters that overfit to 2023-25 NDCG@3 do not transfer to the 21-year temporal cascade. This was NOT a problem for NAR iter 12 (where the winner generalized), suggesting JRA's distribution is more temporally non-stationary than NAR.

4. **Place2 saturation persists.** Even the best-tuned CB trial added zero on place2 (-0.004pp), confirming the diagnosis from iter 9/10a/11: place2 is fundamentally not improvable by GBDT-family hyperparameter tuning over the current feature set. New signal is required (different feature, different objective, or different arch).

## Symmetry analysis: HPO worked for NAR but not JRA

| Aspect          | NAR iter 12 (ACCEPT)                  | JRA iter 13 (REJECT)            |
| --------------- | ------------------------------------- | ------------------------------- |
| Arch            | XGBoost (rank:pairwise)               | CatBoost (YetiRank)             |
| Best vs default | Close to default (depth 7 vs 8)       | Far from default (depth 5 vs 8) |
| Place3 delta    | +0.071pp (vs iter9), +0.159pp (vs v7) | -0.146pp (vs v7)                |
| CV→WF transfer  | Strong (picker 0.776 → real gain)     | Weak (picker 0.612 → real loss) |

JRA's CatBoost baseline has not actually been "untouched since iter 0" by accident — v7-lineage represents a hyperparameter point that has resisted multiple direct attacks (iter 10a depth=10, iter 11 stacking meta) AND now an indirect Bayesian-Pareto attack. JRA's optimum genuinely sits near the v7-lineage defaults; the entire searched neighborhood is worse.

## State transitions (post iter 13)

- `last_iter_id`: 12 → 13
- `current_baseline_jra`: `jra-cb-v7-lineage-wf-21y` (unchanged)
- `best_iteration_jra`: `jra-cb-v7-lineage-wf-21y` (unchanged)
- `current_baseline_nar`: `iter12-nar-xgb-hpo-v8` (unchanged from iter 12 accept)
- `accept_count`: 2 → 2 (no change)
- `reject_count`: 13 → 14
- `consecutive_reject_count`: 0 → 1

## Iter 14 recommendation

JRA's GBDT hyperparameter surface around v7-lineage is now well-mapped (iter 10a deeper, iter 11 stacking meta, iter 13 Bayesian-Pareto HPO — all REJECT). The lever pool for JRA improvement should shift away from GBDT tuning. Recommended next axes, in priority order:

1. **L5A signal extension (highest expected value).** Iter 9's pacestyle features delivered place2 +0.190pp on JRA (the strongest single-axis JRA lift in the entire v8 loop) but failed gate-b with only 1 positive metric. New signals likely to compound:
   - **Track condition × style interaction** (heavy mud × `rs_p_oikomi` etc.)
   - **Trainer recent-form (last-30-day win rate) × distance** — currently absent from feature set
   - **Pace consensus features at the race level** (e.g. predicted nige count, predicted senko count)
     This is a continuation of the L5A axis that already proved JRA-tractable.

2. **L1B 3-arch ensemble.** Re-run iter 6/7 ensemble idea but with NAR iter 12 XGB-HPO + JRA v7 CB + a third arch (LGBM or transformer) at JRA-specific weight tuning. Iter 6 alpha experiments already ran for JRA but with iter 0 baselines — re-attempt with iter 12 NAR booster as anchor.

3. **L11 NDCG@K objective variants.** Iter 7/8 tried NDCG@3 vs NDCG@5 on NAR (mixed); never tested on JRA. CB supports `YetiRank:top=N` and `LambdaRank` losses — searchable HPO axis distinct from hyperparameters.

4. **Stop and accept current state.** JRA leg has reached a high-quality production floor at v7-lineage. NAR leg has been improved twice (iter 9 + iter 12). The minimum-viable v8 deployment posture is JRA=v7-lineage + NAR=iter12-hpo-v8. If iter 14 (L5A signal extension) also rejects, declare the loop converged.

Iter 14 picks **L5A signal extension** (option 1) as the next attempt because it is the only axis with documented JRA gains in this loop.
