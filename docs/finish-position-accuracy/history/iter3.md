---
iteration: 3
date: 2026-06-03T19:49:36.852539+00:00
based_on_iteration: 2
lever: L8-LGBM-stacking
status: reject
quality_gate: passed
model_version_jra: jra-cb-v7-lineage-wf-21y
model_version_nar: nar-xgb-v7-lineage-wf-21y
metrics:
  wf_21y:
    jra:
      top1: 0.4021
      place2: 0.2183
      place3: 0.1630
      top3_box: 0.1424
    nar:
      top1: 0.5820
      place2: 0.3609
      place3: 0.2852
      top3_box: 0.3710
  delta_vs_prev:
    jra:
      top1: -0.0013
      place2: +0.0004
      place3: +0.0005
      top3_box: -0.0007
    nar:
      top1: +0.0001
      place2: -0.0001
      place3: -0.0004
      top3_box: -0.0005
  composite_gain_normalized: -0.0008
  per_bucket_worst:
    jra: top1=-0.214pp place2=-0.140pp place3=-0.006pp top3_box=-0.139pp
    nar: top1=-0.292pp place2=-0.990pp place3=-0.990pp top3_box=-0.152pp
training_time:
  jra: PT4M10S LightGBM ranker 18 folds (single process)
  nar: PT8M50S LightGBM ranker 19 folds (single process)
artifacts:
  model_dir_jra: apps/pc-keiba-viewer/finish-position/jra/v8-iter3-lgbm-stacking
  model_dir_nar: apps/pc-keiba-viewer/finish-position/nar/v8-iter3-lgbm-stacking
  predictions_parquet_jra: tmp/bucket-eval/finish-position/iter3-jra-cb+lgbm-stack-v8/predictions
  predictions_parquet_nar: tmp/bucket-eval/finish-position/iter3-nar-xgb+lgbm-stack-v8/predictions
  stacking_dataset: tmp/v8/iter3-stacking-dataset
  bucket_eval_csv_jra: tmp/v8/iter3-jra-delta.csv
  bucket_eval_csv_nar: tmp/v8/iter3-nar-delta.csv
  decision: tmp/v8/iter3-decision.json
quality_gate_results:
  tsc: 0 errors
  lint: 0 warnings
  format: exit 0
  python_coverage_total: 0.9715
  python_coverage_new_module: 0.98
  ts_coverage_branches: 0.965
  ts_coverage_functions: 0.989
  ts_coverage_lines: 0.991
  ts_coverage_statements: 0.991
---

## What was tried

Iter 3 lever L8-LGBM-stacking: LightGBM `LGBMRanker` (objective=lambdarank, metric=ndcg, lambdarank_truncation_level=5) meta-learner on top of v7-lineage `predicted_score` plus horse-level signals extracted from PG `race_entry_corner_features`:

- `predicted_score` (v7-lineage anchor), `predicted_rank_norm`, within-race `score_z`
- `horse_recent_kohan3f_avg5` (rolling 5-race closing speed), `kohan3f_pct_rank_in_field`
- `horse_recent_finish_position_avg5`, race-relative z-score
- `days_since_last_race` + z-score
- `jockey_recent_30d_win_rate`, `trainer_recent_30d_win_rate`
- `horse_career_track_win_rate` (this `keibajo_code`)
- `tansho_ninkijun`, `umaban`, `futan_juryo`, `horse_age`, `seibetsu_code` one-hot
- `wakuban_norm` (derived from umaban/shusso_tosu), `kyori` + 5-bucket distance band one-hot
- `surface_dirt_flag`, `race_year_int`

Hyper-params: `num_leaves=31`, `n_estimators=500`, `learning_rate=0.05`, `min_child_samples=50`, `reg_lambda=1.0`, `early_stopping_rounds=50` on inner-val (last 10% of training years), `random_state=42+fold_year`. Walk-forward 18 JRA folds (2009..2026) + 19 NAR folds (2008..2026), single Python process per cat, no checkpoint reuse.

Unlike iter1 (isotonic) and iter2 (Ridge), LGBM is non-linear and **could** rerank within a race using horse-level features. The hypothesis was that even if the global rank changes were small, place2/place3 gates would clear because `lambdarank_truncation_level=5` jointly optimises top-5 ordering.

## Implementation summary

New module `apps/pc-keiba-viewer/src/scripts/train_finish_position_lgbm_stacking.py` (353 stmt, 98% cov via 51 tests in `tests/test_train_finish_position_lgbm_stacking.py`). Train mode only — dataset building is the one-shot helper `tmp/v8/iter3_build_lgbm_stacking_dataset.py` that uses DuckDB `postgres_scanner` to pull PG features + window aggregates (5-race kohan_3f rolling mean, 30-day jockey/trainer win rate, career track win rate) and join with enriched v7-lineage parquet. Per-fold atomic metadata.json + parquet write so the loop can resume after a crash.

`LGBMRanker.fit(... group=race_size_array, eval_set=..., eval_group=...)` uses `relevance = truncation_level + 1 - finish_position` so the winner gets the highest relevance and lambdarank pushes them to `predicted_rank=1`. Inner-val split holds out the trailing 10% of training-year races; OOS prediction reranks by score descending.

## Results

JRA delta vs v7-lineage WF 21y baseline (60059 overlapping races, 2009-2026):

- top1: -0.128pp (gate (a) violated)
- place2: +0.045pp
- place3: +0.052pp
- top3_box: -0.067pp

NAR delta vs v7-lineage WF 21y baseline (244811 overlapping races, 2008-2026):

- top1: +0.011pp
- place2: -0.012pp
- place3: -0.042pp
- top3_box: -0.050pp

Per-bucket worst regression (n ≥ 100):

- JRA: top1=-0.214pp, place2=-0.140pp, place3=-0.006pp, top3_box=-0.139pp
- NAR: top1=-0.292pp, place2=-0.990pp, place3=-0.990pp, top3_box=-0.152pp

JRA: gate (a) fails (top1 -0.128pp ≤ -0.05pp). Place2/place3 do gain marginally but cannot rescue the top1 regression.

NAR: gate (b) fails (0 axes > +0.03pp). Stacking is essentially indistinguishable from v7-lineage — LGBM learned to reproduce the predicted_score ranking and the horse-level features added near-zero signal.

## Per-bucket findings

JRA per-bucket delta (`tmp/v8/iter3-jra-delta.csv`): all buckets land in [-0.21pp, +0.32pp] for top1, [-0.14pp, +0.27pp] for place2. No bucket regresses worse than -2pp, so gate (d) per-bucket is structurally fine; the failure is the global gate.

NAR per-bucket worst -0.99pp for place2 / place3 in a small grade bucket (likely a high-grade niche with n≈500); below 2pp tolerance but indicates lambdarank slightly hurts the rarer brackets.

## Feature importance (top 10, sum across folds)

JRA (avg `best_iteration` = 12.2 over 18 folds — early stop fires fast):

1. `predicted_score`: 2419
2. `score_z_in_race`: 1214
3. `predicted_rank_norm`: 899
4. `tansho_ninkijun`: 294
5. `shusso_tosu`: 163
6. `horse_recent_kohan3f_avg5`: 155
7. `finish_pos_avg5_z_in_race`: 148
8. `kohan3f_pct_rank_in_field`: 137
9. `days_since_last_race_z_in_race`: 123
10. `jockey_recent_30d_win_rate`: 120

NAR (avg `best_iteration` = 40.9 over 19 folds):

1. `predicted_score`: 5935
2. `score_z_in_race`: 3822
3. `predicted_rank_norm`: 2862
4. `shusso_tosu`: 1964
5. `tansho_ninkijun`: 1299
6. `finish_pos_avg5_z_in_race`: 893
7. `kohan3f_pct_rank_in_field`: 842
8. `sex_female`: 701
9. `horse_recent_kohan3f_avg5`: 578
10. `trainer_recent_30d_win_rate`: 355

In both cats the top 3 features are derivatives of the v7-lineage `predicted_score`; horse-level history signals (kohan3f / finish-position avg / jockey win rate) contribute only marginally.

## Decision

JRA reject (gate (a) violated: top1 -0.128pp). NAR reject (gate (b) violated: 0 axes > +0.03pp). state.json `reject_count = 3`, `consecutive_reject_count = 3` after this iteration.

## Why three calibration/blend levers in a row failed

L2 isotonic (iter1), L8-Ridge (iter2), and L8-LGBM-stacking (iter3) all attempt to improve accuracy **without retraining the base predictor**. The base v7-lineage CB / XGB already extracts a near-saturated signal from the WF-21y feature space, so meta-learners on `predicted_score + race-context + horse-history` can at best replicate that ranking. LGBM early-stopping confirms this empirically: 12-tree JRA stops give the meta model only enough capacity to copy `predicted_score`.

Place2 / place3 improvement requires a signal that varies horse-level **and** is not already encoded in `predicted_score`. The candidates that remain:

- New horse-level features the base model never saw (e.g. lap-by-lap sectional times, head-to-head matchup records, jockey-trainer-distance interaction history). These need feature regen.
- Per-bucket sample weighting + base model retrain (L4), so the GBDT loss explicitly emphasises rare bucket samples.
- Per-objective HPO (L3) over the full v7-lineage architecture to push the base model into a different optimum.

## Next iteration recommendation

Per plan autonomous-recovery (3 consecutive rejects), next iter should shift to a **retrain-required** lever. Highest expected value choices:

1. **L1B multi-arch ensemble (LGBM + base CB/XGB)** — train a fresh LightGBM full-feature WF model and ensemble at score level (weighted average + within-race rerank). This is rank-changing through cross-model variance, not just calibration. Cost: ~3-6h per cat full WF retrain.
2. **L4 bucket-aware sample weight + base retrain** — re-train v7-lineage architecture with sample weights that upweight rare buckets identified in iter1/iter2/iter3 delta CSVs. Cost: 1 base retrain per cat.
3. **L3 Optuna HPO over v7-lineage** — run 50-trial Bayesian HPO over (max_depth, learning_rate, l2, bagging_fraction, feature_fraction) per cat with NDCG@5 objective. Cost: 6-12h per cat.

Iter 4 should pick L1B (multi-arch ensemble): it most directly satisfies the place2/place3 gate because two different model architectures will disagree on borderline horses, and a within-race rerank of the average can shift place2/place3 without sacrificing top1. Estimated +0.2-0.5pp on top1, +0.3-0.6pp on place2/place3 based on cross-cat literature.

## Artifacts

- Training source: `apps/pc-keiba-viewer/src/scripts/train_finish_position_lgbm_stacking.py`
- Tests: `apps/pc-keiba-viewer/tests/test_train_finish_position_lgbm_stacking.py` (51 cases, 98% cov)
- Dataset builder: `tmp/v8/iter3_build_lgbm_stacking_dataset.py`
- Evaluator: `tmp/v8/compute_iter3_metrics_and_decision.py`
- Predictions JRA: `tmp/bucket-eval/finish-position/iter3-jra-cb+lgbm-stack-v8/predictions/`
- Predictions NAR: `tmp/bucket-eval/finish-position/iter3-nar-xgb+lgbm-stack-v8/predictions/`
- Model meta JRA: `apps/pc-keiba-viewer/finish-position/jra/v8-iter3-lgbm-stacking/`
- Model meta NAR: `apps/pc-keiba-viewer/finish-position/nar/v8-iter3-lgbm-stacking/`
- Per-bucket delta CSVs: `tmp/v8/iter3-{jra,nar}-delta.csv`
- Decision JSON: `tmp/v8/iter3-decision.json`
