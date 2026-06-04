---
iteration: 11
date: 2026-06-04T13:30:00+09:00
based_on_iteration: 10
lever: L6-calibrated-stacking-meta
status: rejected (both JRA + NAR)
quality_gate: passed
loop_status: active
model_version_jra: iter11-jra-cb+meta-v8
model_version_nar: iter11-nar-xgb+meta-v8
baselines:
  jra: jra-cb-v7-lineage-wf-21y (v7-lineage)
  nar: iter9-nar-xgb-pacestyle-v8 (current Phase 1 production)
metrics:
  wf_21y:
    jra:
      baseline_v7:
        { races: 63511, top1: 0.40248, place2: 0.21796, place3: 0.16249, top3_box: 0.14295 }
      iter11: { races: 63511, top1: 0.39861, place2: 0.21803, place3: 0.16254, top3_box: 0.14363 }
      delta_pp: { top1: -0.387, place2: 0.006, place3: 0.005, top3_box: 0.068 }
    nar:
      baseline_iter9:
        { races: 244811, top1: 0.58142, place2: 0.36200, place3: 0.28658, top3_box: 0.37174 }
      iter11: { races: 244811, top1: 0.58039, place2: 0.36080, place3: 0.28615, top3_box: 0.37107 }
      delta_pp: { top1: -0.103, place2: -0.120, place3: -0.044, top3_box: -0.067 }
training_time:
  jra_meta: ~3min (20 folds, LightGBM regression_l2, 19 trained + 1 skip 2007 due to insufficient train rows)
  nar_meta: ~7min (20 folds, LightGBM regression_l2, 19 trained + 1 skip 2007)
artifacts:
  jra_dataset: tmp/v8/iter3-stacking-dataset/category=jra/race_year=*/dataset.parquet (reused iter3 race-context panel; predicted_score = v7-lineage)
  nar_dataset: tmp/v8/iter11-stacking-dataset/category=nar/race_year=*/dataset.parquet (iter3 race-context panel; predicted_score swapped to iter9 NAR XGB pacestyle)
  jra_model: tmp/models/jra-meta-v8-iter11-l6-wf-21y/
  nar_model: tmp/models/nar-meta-v8-iter11-l6-wf-21y/
  predictions_jra: tmp/bucket-eval/finish-position/iter11-jra-cb+meta-v8/predictions/category=jra/race_year=*/
  predictions_nar: tmp/bucket-eval/finish-position/iter11-nar-xgb+meta-v8/predictions/category=nar/race_year=*/
  decision: tmp/v8/iter11-decision.json
  metrics_global: tmp/v8/iter11-metrics-global.json
  train_logs: tmp/v8/iter11-logs/{jra,nar}-train.log
---

## What was tried

**L6 calibrated stacking meta-learner.** The iter 10 SubAgent recommended exploring "a different meta layer composition" since L4 / L5A / L8 stacking all saturated near the GBDT family decision boundary. L6 trains a fresh LightGBM `regression_l2` meta-learner per WF fold on a feature panel that combines:

1. **Base prediction signal**: `predicted_score` (v7-lineage for JRA / iter9 NAR XGB for NAR), `predicted_rank` (normalised by field size).
2. **Race-context derivatives**: `score_z_in_race` / `score_max_in_race_delta` / `score_min_in_race_delta` / `field_size_log` / `kohan3f_pct_rank_in_field` / `finish_pos_avg5_z_in_race` — within-race normalised signals so the meta sees how each horse compares to its specific field rather than absolute base score.
3. **Horse-panel features**: `umaban`, `futan_juryo`, `horse_age`, `tansho_ninkijun`, `kyori`, `shusso_tosu`, recent `kohan3f_avg5`, `finish_position_avg5`, `days_since_last_race`, jockey/trainer 30d win-rate, horse track win-rate.
4. **Categorical one-hots**: distance band (sprint/mile/intermediate/long/extended), surface dirt flag.

The hypothesis was that the meta-learner could re-weight when the base model is overconfident in tight races vs. blowout races, by using `field_size_log` and within-race score dispersion as regime detectors. The meta target is `actual_finish_position` (continuous) — unlike iter 3 lambdarank, this is a smooth signal so the meta optimises end-to-end expected finish.

Walk-forward folds: 19 trained per cat (2008-2026, with 2007 skipped as no prior history exists). LightGBM defaults per plan spec (`num_leaves=31`, `n_estimators=200`, `learning_rate=0.05`, `min_child_samples=100`, `lambda_l2=1.0`, `early_stopping_rounds=20` on val L2; `random_seed = 42 + fold_year` for stability).

Inputs reuse the iter 3 stacking dataset for JRA (already carries v7-lineage `predicted_score`) and a freshly built dataset for NAR that swaps the v7 `predicted_score` for the iter 9 NAR XGB pacestyle predictions (current Phase 1 production baseline) — see `tmp/v8/iter11_build_nar_stacking_dataset.py`.

## Result vs respective baselines

| Cat | Metric   | Baseline | iter11  | Δpp    | Verdict per axis          |
| --- | -------- | -------- | ------- | ------ | ------------------------- |
| JRA | top1     | 0.40248  | 0.39861 | -0.387 | regression > 0.05pp limit |
| JRA | place2   | 0.21796  | 0.21803 | +0.006 | flat                      |
| JRA | place3   | 0.16249  | 0.16254 | +0.005 | flat                      |
| JRA | top3_box | 0.14295  | 0.14363 | +0.068 | small gain                |
| NAR | top1     | 0.58142  | 0.58039 | -0.103 | regression                |
| NAR | place2   | 0.36200  | 0.36080 | -0.120 | regression                |
| NAR | place3   | 0.28658  | 0.28615 | -0.044 | regression                |
| NAR | top3_box | 0.37174  | 0.37107 | -0.067 | regression                |

**JRA decision: REJECT.** Condition (a) fails — top1 -0.387pp far below -0.05pp guard. Only `top3_box` cleared the +0.03pp positive bar; place2/3 essentially flat. Net: meta hurts top1 while marginally improving box hits, which is the wrong trade since top1 = anchor metric.

**NAR decision: REJECT.** All 4 axes regress vs the iter 9 NAR baseline. Worst regression is place2 -0.120pp, well past the -0.05pp guard. positive_metric_set is empty.

## Meta feature importance

Per-fold LightGBM `feature_importances_` aggregated across all 19 trained folds (rank by how many folds put a feature in top 10):

| Feature                     | JRA freq / total_imp | NAR freq / total_imp |
| --------------------------- | -------------------- | -------------------- |
| `field_size_log`            | 19 / 11513           | 19 / 17845           |
| `score_z_in_race`           | 19 / 10586           | 19 / 12849           |
| `predicted_score` (base)    | 19 / 9247            | 19 / 8681            |
| `days_since_last_race`      | 19 / 7160            | 19 / 6490            |
| `tansho_ninkijun`           | 19 / 9700            | 16 / 5178            |
| `kohan3f_pct_rank_in_field` | 19 / 6664            | 19 / 6140            |
| `score_max_in_race_delta`   | (not top)            | 19 / 7068            |
| `score_min_in_race_delta`   | (not top)            | 19 / 5852            |
| `horse_recent_kohan3f_avg5` | 19 / 5606            | (occasional)         |
| `finish_pos_avg5_z_in_race` | 19 / 6215            | (occasional)         |

**Best-iteration distribution**: JRA avg 194.4 (range 121-200), NAR avg 199.5 (range 193-200). NAR hit the `n_estimators=200` ceiling almost every fold so early stopping never fired — meaning the meta could have grown larger trees, but global accuracy still regressed against the base. This pattern matches iter 2-3 / iter 7 saturation diagnostics: the meta layer adds capacity but the capacity converts only into mild reordering inside the field, not into top1 / place2 sharpening.

## Why L6 did not break saturation differently from iter 1-3

L6 vs prior stacking attempts is "different meta layer" but the underlying inputs overlap with iter 3 (same feature panel + race-context aggregates). The meta IS being trained to predict `actual_finish_position`, so in principle it can re-order within a race. However:

- **Rank-stability of monotone transforms**: once the base learner ranks correctly, monotone-in-score reweighting (LightGBM trees on `predicted_score` + race-context z-scores) cannot improve the rank ordering unless the meta sees a feature that flips signs against the base. Importance pattern shows the meta IS dominated by `field_size_log` + `score_z_in_race` + `predicted_score` — all band-pass on the base signal.
- **Isotonic calibration would not help**: the four metrics (top1/place2/place3/top3_box) are computed from `predicted_rank` only. Isotonic mappings on `meta_score` are monotonic, so applying them after the meta would not change rank ordering inside any race. We skipped the calibration pass because of this invariance — the spec's "L6 + isotonic" hypothesis cannot move rank-based metrics by construction (isotonic only affects calibration metrics like Brier/NLL, which are not part of our accept gate).

The iter 9 NAR breakthrough remains genuine (random-seed × `score_max_in_race_delta` interaction caught a real per-race signal), but iter 11 meta cannot fork further gain from that same panel.

## Decision

Both JRA and NAR REJECT. State updates:

- `reject_count`: 11 → 13 (2 rejects added)
- `consecutive_reject_count`: 1 → 2
- `last_iter_id`: 10 → 11
- baselines unchanged: JRA = `jra-cb-v7-lineage-wf-21y`, NAR = `iter9-nar-xgb-pacestyle-v8`

## Iter 12 recommendation

Two viable paths:

1. **L3 Optuna HPO on iter9 NAR base** (no meta layer). Iter 9 NAR was accept-by-seed; running ~30-50 trial Optuna over (`max_depth`, `num_leaves`, `min_child_weight`, `reg_alpha`, `reg_lambda`, `colsample_bytree`) on the v8-iter9 pacestyle features may find a stable best variant. Time budget ~6-8h.
2. **L7 Optuna HPO on JRA v7 CatBoost YetiRank**. Same idea but on JRA — the v7-lineage CB params are unchanged for ~2 years and HPO has never been run on the iter 9 pacestyle features. Time budget ~6-8h.

Either is independent of the failed L6 layer. If both reject, we are out of remaining levers per the v8 plan and should accept iter 9 NAR as the final breakthrough (loop should stop at iter 12 or 13 with the recorded best state).

## Stop check

`consecutive_reject_count = 2 < 8` (S1 threshold). Loop continues.
