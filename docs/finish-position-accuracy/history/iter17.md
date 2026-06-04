---
iteration: 17
date: 2026-06-04T19:35:00+09:00
based_on_iteration: 12
lever: L5D-bataiju-barei-kyori-top3-NAR-XGB-on-iter12-HPO
status: rejected (NAR)
quality_gate: passed
loop_status: active
model_version_jra: iter14-jra-cb-pacestyle-course-v8 (unchanged)
model_version_nar: iter12-nar-xgb-hpo-v8 (UNCHANGED — iter 17 rejected)
baselines:
  nar_primary: iter12-nar-xgb-hpo-v8 (current Phase 1 production)
  nar_v7_reference: jra-cb-v7-lineage-wf-21y (absolute pre-v8 anchor)
features:
  base_root: apps/pc-keiba-viewer/tmp/feat-nar-v8-iter9-pacestyle (iter 9 NAR pacestyle base, REUSED)
  bataiju_source: tmp/v8/iter17-bataiju-barei-kyori-features.parquet (5.7M rows, 13 cols)
  joined_root: tmp/feat-nar-v8-iter17-bataiju (per-year, 21 race_year dirs)
  bataiju_top3_added:
    - bataiju_z_in_barei_kyori_stratum
    - horse_bataiju_trajectory_deviation
    - bataiju_x_kyori_log
  effective_feature_count: 194-195 (iter 9 NAR base 192 + 3 bataiju cols)
  bataiju_join_match_rate: 92.06%-99.93% per year (LEFT JOIN, no row loss)
signal_motivation:
  finding_summary: NAR 2600m+ barei corr +0.193; bataiju z=+2sigma -> 16.10% win vs z=-2sigma 4.03% (4.0x spread, monotone)
  source: tmp/v8/iter17-bataiju-barei-kyori-features.parquet (prep step 6b8e7d0)
training:
  arch: XGBoost LambdaRank (LambdaMART, ndcg eval)
  hyperparams_iter12_HPO_winner:
    max_depth: 7
    learning_rate: 0.0527256411839993
    reg_lambda: 1.967139556304256
    min_child_weight: 7
    subsample: 0.6181216039088178
    colsample_bytree: 0.7498450458505884
    n_estimators: 650
  hpo_params_source: apps/pc-keiba-viewer/finish-position/nar/v8-iter12-hpo/best-params.json
  random_seed_base: 42 (+ fold_year stabilization)
  fold_count: 20 (2007..2026)
  train_start_year: 2006
  best_iter_avg: 298.9 (min 146 max 539)
  total_train_sec: 431.9 (~7.2 min wall, 20 folds, sequential)
  avg_fold_sec: 20.6
metrics:
  wf_21y_common_races_vs_iter12: 258966
  nar:
    baseline_iter12:
      { races: 258966, top1: 0.58060, place2: 0.36177, place3: 0.28766, top3_box: 0.37151 }
    iter17: { races: 258966, top1: 0.57989, place2: 0.36188, place3: 0.28763, top3_box: 0.37186 }
    delta_pp_vs_iter12: { top1: -0.071, place2: 0.011, place3: -0.003, top3_box: 0.035 }
    delta_pp_vs_v7: { top1: -0.065, place2: 0.094, place3: 0.156, top3_box: 0.010 }
decision_gate_5_condition_nar:
  cond_a_no_regression_le_5bps: false (top1 -0.071 < -0.05)
  cond_b_two_axes_gain_gt_3bps: false (only top3_box +0.035)
  cond_c_place2_or_place3_gain: false (neither over +0.03)
  positive_metric_set: [top3_box]
  decision: reject
feature_importance:
  bataiju_in_top25_any_fold: 0/20
  bataiju_z_in_barei_kyori_stratum_top25_count: 0/20
  horse_bataiju_trajectory_deviation_top25_count: 0/20
  bataiju_x_kyori_log_top25_count: 0/20
  top5_features_by_avg_gain:
    [target_corner_4_norm, odds_score, target_corner_3_norm, popularity_score, target_corner_1_norm]
  interpretation: |
    Despite the strong univariate signal (z=+2sigma -> 16.10% win rate, 4.0x spread vs z=-2sigma),
    XGBoost with iter 12 HPO did not promote any of the 3 bataiju features into the top-25 in ANY fold.
    The existing pacestyle/corner/odds/popularity features fully subsume the signal in tree-split-gain
    terms. This matches the global metric outcome: top3_box +0.035pp (only positive axis), all 3 other
    axes flat or slightly negative.
artifacts:
  feature_build_script: tmp/v8/iter17_build_nar_features.py
  feature_build_summary: tmp/v8/iter17-build-summary.json
  train_script: tmp/v8/iter12_train_predict.py (REUSED, --features-root flag)
  train_log: tmp/v8/iter17-train.log
  train_summary: tmp/v8/iter17-train-summary.json
  predictions: tmp/bucket-eval/finish-position/iter17-nar-xgb-bataiju-v8/predictions/category=nar/race_year=*/predictions.parquet (20 folds, 2007-2026)
  metrics_script: tmp/v8/compute_iter17_metrics_and_decision.py
  decision: tmp/v8/iter17-decision.json
  metrics_global: tmp/v8/iter17-metrics-global.json
next_iteration_recommendation:
  iter18: |
    Skip JRA L5D variant — bataiju top-3 features did not crack top-25 on NAR with stronger univariate
    signal, so JRA (weaker bataiju spread) is unlikely to benefit. Options:
    - L7_subsample_lr_finer_HPO on NAR XGB iter 12 base (50-trial Optuna with narrowed search around winner)
    - L5E_explicit_2600m+_indicator + bataiju (force tree to see the strong-signal sub-population)
    - L8_NAR_target_encoded_chokyoshi (trainer ID target-encoding 21-year)
  defer_to_user_review: |
    Given iter 17 is consecutive_reject 3 with no JRA exploration today, recommend pausing for user
    review at JST 20:00 before committing to iter 18 lever. Loop state set to active but iter18 not
    auto-launched.
---

# Iter 17: L5D Bataiju x Barei x Kyori top-3 features on NAR XGB iter 12 HPO base

## Summary

iter 17 attempted to layer the 3 strongest bataiju-derived features (from the iter 17 prep
correlation analysis) on top of the iter 9 NAR pacestyle feature parquet, then retrain NAR XGBoost
with iter 12 HPO winner hyperparameters. Despite a strong univariate signal (z-scored bataiju
relative to age-distance stratum showed 4.0x win-rate spread), the global 4-metric outcome was a
small mixed delta: top1 -0.071pp, place2 +0.011pp, place3 -0.003pp, top3_box +0.035pp vs iter 12.

The 5-condition decision gate failed on cond_a (top1 regression beyond -0.05pp tolerance) and
cond_b (only 1 axis above +0.03pp). **Decision: reject; NAR baseline remains iter12-nar-xgb-hpo-v8.**

## Feature engineering posture

- iter 9 NAR pacestyle base (192 features) preserved verbatim — no column reduction (`feedback_no_feature_reduction`).
- 3 bataiju features LEFT JOIN'd by (race_id, ketto_toroku_bango), match 92-99.9% per year.
- Effective feature count went 192 -> 194-195 across folds.

## Hyperparameter posture

iter 12 HPO winner params replayed verbatim. No new HPO run launched — keeping this iteration's
delta isolated to the feature-set delta only.

## Why the strong univariate signal didn't translate

The univariate analysis was conditional only on `barei x kyori` strata (raw 2D groupby). Once
XGBoost sees the full 192-feature context (pacestyle, corner positions, odds, popularity, recent
finishes), the bataiju z-score becomes redundant: corners 1/3/4 + odds_score + popularity capture
the same conditioning information. Result: 0/20 folds had any bataiju column in top-25 importance.

The 35bps top3_box gain is consistent with a tiny, non-redundant residual signal — useful but
insufficient to clear the gate.

## Recommendation

Defer iter 18 selection to user review at JST 20:00. Candidate levers documented above. Loop
status remains active.
