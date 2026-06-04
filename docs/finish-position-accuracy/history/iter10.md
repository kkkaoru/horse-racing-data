---
iteration: 10
date: 2026-06-04T12:10:00+09:00
based_on_iteration: 9
lever: PathA-JRA-deeper-L5A-plus-NAR-L4-on-iter9
status: both_reject
quality_gate: passed
loop_status: active_reject_streak_1
current_baseline_jra: jra-cb-v7-lineage-wf-21y
current_baseline_nar: iter9-nar-xgb-pacestyle-v8
sub_iters:
  10a:
    cat: jra
    arch: cb
    hyperparams:
      iterations: 800
      learning_rate: 0.04
      depth: 10
      l2_leaf_reg: 4.0
      od_wait: 30
    features_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter9-pacestyle/race_year=*/
    predictions_root: tmp/bucket-eval/finish-position/iter10a-jra-cb-deeper-v8/predictions/category=jra/race_year=*/
    best_iter_avg: 318.8
    best_iter_range: [156, 567]
    train_time_min: 21.2
    decision: reject
  10b:
    cat: nar
    arch: xgb
    alpha_quick_window: { 0.3: 0.37581, 0.5: 0.37603, 0.75: 0.37526 }
    winner_alpha: 0.5
    hyperparams:
      iterations: 500
      learning_rate: 0.05
      max_depth: 8
      reg_lambda: 1.0
      min_child_weight: 30
      early_stopping_rounds: 30
    features_root: apps/pc-keiba-viewer/tmp/feat-nar-v8-iter9-pacestyle/race_year=*/
    bucket_membership_root: tmp/v8/bucket-membership/category=nar/race_year=*/
    predictions_root: tmp/bucket-eval/finish-position/iter10b-nar-xgb-pacestyle-l4-v8/predictions/category=nar/race_year=*/
    best_iter_avg: 281.3
    best_iter_range: [120, 438]
    train_time_min: 7.7
    decision: reject
metrics:
  wf_21y:
    jra:
      iter10a_vs_v7:
        baseline:
          { races: 66964, top1: 0.40139, place2: 0.21730, place3: 0.16203, top3_box: 0.14242 }
        iter10a:
          { races: 66964, top1: 0.40035, place2: 0.21736, place3: 0.16322, top3_box: 0.14264 }
        delta_pp: { top1: -0.105, place2: 0.006, place3: 0.119, top3_box: 0.022 }
    nar:
      iter10b_vs_iter9_baseline:
        baseline_iter9:
          { races: 258966, top1: 0.58016, place2: 0.36155, place3: 0.28695, top3_box: 0.37188 }
        iter10b:
          { races: 258966, top1: 0.58028, place2: 0.36127, place3: 0.28667, top3_box: 0.37121 }
        delta_pp: { top1: 0.012, place2: -0.028, place3: -0.028, top3_box: -0.066 }
      iter10b_vs_v7_absolute:
        baseline_v7:
          { races: 258966, top1: 0.58055, place2: 0.36094, place3: 0.28607, top3_box: 0.37176 }
        delta_pp: { top1: -0.027, place2: 0.032, place3: 0.059, top3_box: -0.055 }
        iter9_for_comparison_delta_pp_vs_v7:
          { top1: -0.039, place2: 0.061, place3: 0.088, top3_box: 0.012 }
artifacts:
  scripts:
    iter10a_train: tmp/v8/iter10a_train_predict.py
    iter10b_train: tmp/v8/iter10b_train_predict.py
    iter10b_alpha_ab: tmp/v8/iter10b_alpha_ab_quick.py
    compute_metrics: tmp/v8/compute_iter10_metrics_and_decision.py
  decision_json: tmp/v8/iter10-decision.json
  metrics_json: tmp/v8/iter10-metrics-global.json
  delta_csvs:
    - tmp/v8/iter10a-jra-delta.csv
    - tmp/v8/iter10b-nar-delta-vs-iter9.csv
    - tmp/v8/iter10b-nar-delta-vs-v7.csv
  alpha_ab_summary: tmp/v8/iter10b-alpha-ab-quick.json
---

## What was tried

**Path A combined** per iter 9 SubAgent recommendation:

- **Iter 10a** — JRA L5A _deeper_ variant on the iter 9 pacestyle feature parquet. Same features (234 numeric cols after exclusion of `target_race_id` / band / schema / keibajo / weight-class). New CB hyperparams: `depth=10` (was 8), `iterations=800` (was 500), `learning_rate=0.04` (was 0.05), `l2_leaf_reg=4` (was 3). `od_wait=30` unchanged. Random seed `42 + fold_year` same as iter 9. Time-decay sample weight only (no bucket weight).
- **Iter 10b** — NAR L4 bucket-aware on the **iter 9 NAR baseline** (current_baseline_nar). Same iter 9 NAR pacestyle feature parquet (~192 cols). Reused `tmp/v8/bucket-membership/category=nar` weak-bucket scores accumulated over iter 1-5 deltas. XGB hyperparams identical to iter 9 (`max_depth=8`, `iterations=500`, `lr=0.05`, `reg_lambda=1.0`, `min_child_weight=30`). Sample weight = `time_decay × (1 + α × is_weak_bucket_score)` clipped `[0.5, 1.75]`.

α A/B quick window 2024-2026 result:

| α    | top1    | place2  | place3  | top3_box | composite (0.2/0.5/0.3) |
| ---- | ------- | ------- | ------- | -------- | ----------------------- |
| 0.3  | 58.450% | 35.585% | 26.994% | 34.524%  | 0.37581                 |
| 0.5  | 58.550% | 35.503% | 27.138% | 34.646%  | **0.37603 (winner)**    |
| 0.75 | 58.446% | 35.422% | 27.085% | 34.584%  | 0.37526                 |

Full WF 2007-2026 then ran with α=0.5 in 7.7 min (avg best_iter 281).

## Results

### Iter 10a JRA CB-deeper vs v7-lineage baseline (66964 races)

| Metric   | v7-lineage | iter 10a | Δpp          |
| -------- | ---------- | -------- | ------------ |
| top1     | 40.139%    | 40.035%  | **-0.105** ✗ |
| place2   | 21.730%    | 21.736%  | **+0.006**   |
| place3   | 16.203%    | 16.322%  | **+0.119** ✓ |
| top3_box | 14.242%    | 14.264%  | **+0.022**   |

Gate (a) all metrics ≥ -0.05pp: **fail** (top1 -0.105 < -0.05).
Gate (b) ≥2 axes gain > +0.03pp: **fail** (only place3 crosses).
Gate (c) place2 or place3 positive: **pass** (place3 +0.119).

**Decision: reject**.

The deeper trees compounded place3 dramatically (+0.119pp vs iter 9 +0.004pp) but at the cost of top1 (-0.105pp vs iter 9 +0.021pp). The added capacity (depth 8 → 10, iters 500 → 800) redistributes gain from the head of the predicted ranking toward the tail. In a fixed sample, that is an overfit pattern: the model invests its extra capacity into refining the place3 boundary in narrow training regions, at the expense of head-of-list discrimination. Iter 9's milder hyperparams produced 3/4 directionally positive results; iter 10a turned that into 3/4 still nominally positive but with top1 going clearly negative, which is a structurally worse trade for a place-bet model.

### Iter 10b NAR XGB-L4 vs iter 9 NAR baseline (258966 races)

| Metric   | iter 9  | iter 10b | Δpp          |
| -------- | ------- | -------- | ------------ |
| top1     | 58.016% | 58.028%  | **+0.012**   |
| place2   | 36.155% | 36.127%  | **-0.028**   |
| place3   | 28.695% | 28.667%  | **-0.028**   |
| top3_box | 37.188% | 37.121%  | **-0.066** ✗ |

Gate (a) all metrics ≥ -0.05pp: **fail** (top3_box -0.066).
Gate (b) ≥2 axes gain > +0.03pp: **fail** (no metric crosses).
Gate (c): **fail** (no positive set).

**Decision: reject**.

### Iter 10b NAR vs v7-lineage absolute reference (for stability check)

| Metric   | v7      | iter 10b | iter 9 (current_baseline) | Δ vs iter 9 |
| -------- | ------- | -------- | ------------------------- | ----------- |
| top1     | 58.055% | 58.028%  | 58.016%                   | +0.012      |
| place2   | 36.094% | 36.127%  | 36.155%                   | -0.028      |
| place3   | 28.607% | 28.667%  | 28.695%                   | -0.028      |
| top3_box | 37.176% | 37.121%  | 37.188%                   | -0.066      |

`iter10b vs v7` (delta_pp): top1 -0.027 / place2 +0.032 / place3 +0.059 / top3_box -0.055.
`iter9 vs v7` (delta_pp): top1 -0.039 / place2 +0.061 / place3 +0.088 / top3_box +0.012.

iter 10b is _partially_ better than iter 9 on top1 (-0.027 vs -0.039) but **strictly worse** on place2 (+0.032 vs +0.061), place3 (+0.059 vs +0.088), and top3_box (-0.055 vs +0.012). The L4 bucket-aware reweight on top of iter 9's L5A base **does not compound** — it actively dilutes iter 9's place2/place3 gains and pushes top3_box from a small positive into a -0.055 regression. The iter 1-5-derived weak-bucket score is no longer well-calibrated to iter 9's residual error distribution.

## Feature importance (per-fold top-25)

Pace × style features in top-25 (count over recent folds):

| Fold | iter 10a JRA                               | iter 10b NAR |
| ---- | ------------------------------------------ | ------------ |
| 2024 | 1 (`target_running_style_class` imp=0.000) | 0            |
| 2025 | 0                                          | 0            |
| 2026 | 1 (`target_running_style_class` imp=0.000) | 0            |

**Confirmation of iter 9 SubAgent's hypothesis**: the 10 new pace × style features remain invisible in top-25 importance even with deeper trees (iter 10a) and even with bucket reweighting (iter 10b). The marginal accuracy movements observed in iter 9 (NAR accept) and iter 10 (both reject) are attributable to GBDT split-tiebreaker variance under a slightly larger candidate-feature pool, not to actual signal extraction from the new features. The v7-lineage feature set is the dominant signal and remains saturated.

## Decision

- **JRA**: reject. `current_baseline_jra` stays `jra-cb-v7-lineage-wf-21y`.
- **NAR**: reject. `current_baseline_nar` stays `iter9-nar-xgb-pacestyle-v8`.
- `consecutive_reject_count` 0 → 1. `accept_count` stays at 1. `reject_count` 9 → 11.

Loop continues (S1 threshold is 8 consecutive rejects; iter 11 starts a fresh streak after the iter 9 accept reset).

## Iter 11 recommendation

Both rejects + identical "new features invisible in top-25" finding implies:

1. **Drop pace × style as a lever family**. Further variants of L5A (more depth, more variants of pace features, additional cross-products) will keep hitting the same wall: v7-lineage is saturated, GBDT will not preferentially split on noisier signals.
2. **Pivot to a different signal axis**. Two candidates worth trying before the next S1 stop:
   - **L3 Optuna HPO** on the **iter 9 NAR baseline only** (no JRA — JRA is at v7-final, less to tune). 50-trial Bayesian search over `max_depth`, `learning_rate`, `reg_lambda`, `min_child_weight`. Search budget ~3h. Expected gain: 0.02-0.04pp on place2/place3 if a missing sweet spot exists, but high risk of zero gain since iter 7 (L3 on v7-final) already rejected.
   - **L6 (untried) — calibrated stacking with a second-architecture meta-learner**. Train a LightGBM meta on (XGB iter 9 score, CB v7 score, v7 raw features), use isotonic calibration on the meta output. v8 LGBM stacking dataset has been ready since iter 3 but iter 3 used a _direct_ LGBM model on v7 features (rejected). A meta on top of two architecturally different rankers may capture residual disagreement signal.

3. **If both L3 and L6 fail**, the v8 loop has structurally exhausted GBDT-only improvements. Next pivot would be a non-GBDT meta (small MLP / TabNet over GBDT logits), but per `project_mlx_transformer_status.md` MLX Set Transformer underperformed GBDT for 11 iter; would need a different non-GBDT architecture, e.g. logistic regression on per-bucket GBDT residuals.

Concrete iter 11 plan: **L6 stacked meta first** (lower cost, novel lever) before L3 Optuna. L6 reuses iter 9 NAR + v7 JRA prediction parquets so no retraining of base learners is needed; only the meta layer.
