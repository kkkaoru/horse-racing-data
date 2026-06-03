---
iteration: 4
date: 2026-06-03T20:20:26.068298+00:00
based_on_iteration: 3
lever: L1B-CB+LGBM-rank-ensemble
status: reject
quality_gate: passed
model_version_jra: jra-cb-v7-lineage-wf-21y
model_version_nar: nar-xgb-v7-lineage-wf-21y
metrics:
  wf_21y:
    jra:
      top1: 0.4034
      place2: 0.2180
      place3: 0.1625
      top3_box: 0.1431
    nar:
      top1: 0.5820
      place2: 0.3612
      place3: 0.2856
      top3_box: 0.3715
  delta_vs_prev:
    jra:
      top1: +0.0001
      place2: +0.0001
      place3: +0.0000
      top3_box: -0.0000
    nar:
      top1: +0.0001
      place2: +0.0001
      place3: -0.0000
      top3_box: +0.0000
  composite_gain_normalized: +0.0002
  per_bucket_worst:
    jra: top1=-0.009pp place2=+0.000pp place3=-0.020pp top3_box=-0.012pp
    nar: top1=-0.001pp place2=-0.098pp place3=-0.556pp top3_box=-0.495pp
training_time:
  jra: PT3M0S LightGBM ranker 20 folds (single process, lambdarank truncation_level=5)
  nar: PT5M30S LightGBM ranker 20 folds (single process, lambdarank truncation_level=5)
ensemble_weights:
  jra:
    w_base: 0.90
    w_lgbm: 0.10
  nar:
    w_base: 0.70
    w_lgbm: 0.30
artifacts:
  feature_parquet_jra: tmp/feat-v20-merged/jra
  feature_parquet_nar: tmp/feat-v20-merged/nar
  feature_parquet_source: fallback (v6 stacked 164-numeric, v7-final regen not run; plan-permitted)
  lgbm_predictions_jra: tmp/bucket-eval/finish-position/iter4-jra-lgbm-v8/predictions
  lgbm_predictions_nar: tmp/bucket-eval/finish-position/iter4-nar-lgbm-v8/predictions
  ensemble_predictions_jra: tmp/bucket-eval/finish-position/iter4-jra-cb+lgbm-rank-v8/predictions
  ensemble_predictions_nar: tmp/bucket-eval/finish-position/iter4-nar-xgb+lgbm-rank-v8/predictions
  ensemble_weights_jra: apps/pc-keiba-viewer/finish-position/jra/v8-iter4-ensemble/weights.json
  ensemble_weights_nar: apps/pc-keiba-viewer/finish-position/nar/v8-iter4-ensemble/weights.json
  bucket_eval_csv_jra: tmp/v8/iter4-jra-delta.csv
  bucket_eval_csv_nar: tmp/v8/iter4-nar-delta.csv
  decision: tmp/v8/iter4-decision.json
quality_gate_results:
  tsc: 0 errors
  lint: 0 warnings
  format: exit 0
  python_coverage_total: 0.9715
  ts_coverage_branches: 0.965
  ts_coverage_functions: 0.989
  ts_coverage_lines: 0.991
  ts_coverage_statements: 0.991
---

## What was tried

Iter 4 lever L1B-CB+LGBM-rank-ensemble: train a fresh LightGBM `lambdarank` walk-forward model on the full feature parquet (164 numeric features, 20 folds each cat) and ensemble at the **rank level** with the active v7-lineage baseline (CB-JRA / XGB-NAR):

1. Train LGBM WF 21y both cats over `tmp/feat-v20-merged/{jra,nar}/race_year=*/data.parquet` (v6 stacked 164 numeric features after dropping label / partition columns). Objective `lambdarank`, `lambdarank_truncation_level=5` (Lever L17 fused into iter 4 trainer), `num_leaves=63`, `learning_rate=0.05`, `early_stopping_rounds=50`. Single process, sequential, ~3-5 min per cat (v6 cached parquet is much smaller than v7-lineage full-feature parquet — feature regen explicitly fell back per plan Step 2 since v7-final parquet was absent).
2. Per (cat, race_year) ensemble: join LGBM predictions with baseline `predicted_rank` by `(race_id, ketto_toroku_bango)`, rank-normalize each to `[0,1]`, score = `w_base*base_rank_norm + w_lgbm*lgbm_rank_norm`, re-rank within race by ascending score.
3. Weight grid search `w_lgbm in [0.10, 0.20, 0.30, 0.40, 0.50]`, picking the weight that maximises the 20-fold OOS composite = mean(top1, place2, place3, top3_box).

Hypothesis: two independently-trained architectures should disagree on borderline horses; rank-average should rerank place2/place3 candidates without sacrificing top1.

## Implementation summary

Three one-shot Python helpers:

- `tmp/v8/iter4_train_lgbm_predict.py` (LGBM walk-forward, mirrors official `train_finish_position_lightgbm_walk_forward.py` per-fold logic but additionally persists per-fold predictions parquet to `tmp/bucket-eval/finish-position/iter4-{cat}-lgbm-v8/predictions/`). The official trainer writes only `metadata.json`, so iter 4 needed a wrapper to expose predictions for ensembling.
- `tmp/v8/iter4_apply_ensemble.py` (DuckDB COPY ... PARTITION_BY for ensemble parquet, grid search via `evaluate_weight` + per-race rank metric SQL).
- `tmp/v8/compute_iter4_metrics_and_decision.py` (mirror of `compute_iter3_metrics_and_decision.py`).

All three are throwaway iter4 helpers under `tmp/` (orchestrator scope, per `feedback_code_changes_via_subagent`). No edits to enforced `apps/pc-keiba-viewer/src/scripts/*` so the 95% coverage gate remains untouched.

## Results — grid search

JRA (60059 races, min_year=2009):

| w_lgbm | races | top1   | place2 | place3 | top3_box | composite           |
| ------ | ----- | ------ | ------ | ------ | -------- | ------------------- |
| 0.10   | 59987 | 0.4034 | 0.2180 | 0.1625 | 0.1431   | 0.2317 **(picked)** |
| 0.20   | 59987 | 0.4033 | 0.2180 | 0.1624 | 0.1430   | 0.2317              |
| 0.30   | 59987 | 0.4032 | 0.2179 | 0.1621 | 0.1428   | 0.2315              |
| 0.40   | 59987 | 0.4029 | 0.2178 | 0.1615 | 0.1421   | 0.2311              |
| 0.50   | 59987 | 0.4010 | 0.2168 | 0.1612 | 0.1401   | 0.2298              |

NAR (244811 races, min_year=2008):

| w_lgbm | races  | top1   | place2 | place3 | top3_box | composite           |
| ------ | ------ | ------ | ------ | ------ | -------- | ------------------- |
| 0.10   | 244582 | 0.5820 | 0.3611 | 0.2857 | 0.3715   | 0.4000              |
| 0.20   | 244582 | 0.5820 | 0.3611 | 0.2857 | 0.3715   | 0.4000              |
| 0.30   | 244582 | 0.5820 | 0.3612 | 0.2856 | 0.3715   | 0.4001 **(picked)** |
| 0.40   | 244582 | 0.5820 | 0.3611 | 0.2854 | 0.3713   | 0.4000              |
| 0.50   | 244582 | 0.5824 | 0.3608 | 0.2843 | 0.3697   | 0.3993              |

## Delta vs v7-lineage baseline at best weight

JRA `w_lgbm=0.10`:

- top1: +0.007pp
- place2: +0.011pp
- place3: +0.003pp
- top3_box: -0.004pp

NAR `w_lgbm=0.30`:

- top1: +0.006pp
- place2: +0.009pp
- place3: -0.001pp
- top3_box: +0.004pp

Per-bucket worst regression (n >= 100):

- JRA: top1=-0.009pp, place2=+0.000pp, place3=-0.020pp, top3_box=-0.012pp (gate (d) clear)
- NAR: top1=-0.001pp, place2=-0.098pp, place3=-0.556pp, top3_box=-0.495pp (gate (d) clear)

## Decision

Both cats `reject` on gate (b) — all 4 deltas land in [-0.005pp, +0.011pp]. No axis crosses the +0.03pp positive threshold. The ensemble is rank-preserving at low weights and lossy at high weights; the grid optimum sits at the minimum tested LGBM weight (JRA) or near it (NAR), indicating the LGBM signal is essentially noise relative to the baseline.

state.json: `reject_count = 4`, `consecutive_reject_count = 4`. Distance to stop S1 (8 consecutive reject) = 4 more iters.

## Why iter 4 ensemble failed

Same root cause as iter 1/2/3 plus a feature-regen caveat:

1. **Feature space collapse**: LGBM was trained on the v6 `feat-v20-merged` parquet (164 numeric features) rather than the v7-lineage 226-JRA / 175-NAR feature set the active CB/XGB baseline uses. The v6 set is a strict subset of v7-lineage signal, so the LGBM saw less information than the baseline.
2. **Architectural overlap**: even if features were equal, GBDT-on-tabular-features is a homogeneous family. CB YetiRank, XGB rank:pairwise, and LGBM lambdarank converge on similar decision boundaries on saturated features. Rank-average ensembles of three GBDTs is closer to model averaging than to architectural diversity.
3. **v6 < v7 evidence**: at `w_lgbm=0.50` (equal-weight blend), JRA composite drops to 0.2298 vs baseline 0.2317 (−0.19pp). This confirms the LGBM model is the weaker ranker — any non-trivial weight on it hurts.

A meaningful L1B ensemble would require: (a) v7-lineage feature parquet regen so LGBM trains on parity feature space, **and** (b) either a non-GBDT secondary head (FT-Transformer / Set Transformer) or a different label scheme (regression on `finish_norm` instead of pairwise ranking).

## Next iteration recommendation

Given 4 consecutive rejects on calibration / stacking / homogeneous-arch ensemble, the next iter should hit a fundamentally different layer:

1. **L4 bucket-aware sample weighting + base CB/XGB retrain** (highest priority) — Retrain v7-lineage with `sample_weight = (1 + alpha * is_weak_bucket)` for buckets identified in iter1-3 delta CSVs. The base GBDT loss directly optimises rare brackets that the active model under-fits. Cost: ~5h CB-JRA + ~6h XGB-NAR = ~11h. Lever expected gain: +0.2pp worst-bucket, +0.1pp top1/place2.
2. **L3 Optuna HPO over v7-lineage** — 50-trial Bayesian HPO over depth, lr, l2, feature_fraction with NDCG@5 objective. Cost: ~6-12h per cat. Lever expected gain: +0.2pp top1, +0.1pp place2/place3.
3. **L5A pace × style horse-level features** — Add running-style v3-based pace dispersion (front-runner field share, pace_index variance) as horse-level signals **into** the base feature parquet, then retrain. Cost: feature regen ~6h + retrain ~5h per cat. Lever expected gain: +0.3pp place2/place3.
4. **(Skip L1A)** — pure CB+XGB rank-average without LGBM is also unlikely to help given the iter4 finding that GBDT-on-GBDT ensembles are degenerate.

Iter 5 should pick **L4 bucket-aware** as the first non-meta retrain, since it directly addresses gate (d) (per-bucket worst) which iter 1-4 only marginally moved. L3 HPO is iter 6 if L4 misses gate (b).

## Artifacts

- Trainer: `tmp/v8/iter4_train_lgbm_predict.py`
- Ensemble apply: `tmp/v8/iter4_apply_ensemble.py`
- Evaluator: `tmp/v8/compute_iter4_metrics_and_decision.py`
- Preconditions JSON: `tmp/v8/iter4-preconditions.json`
- LGBM predictions JRA: `tmp/bucket-eval/finish-position/iter4-jra-lgbm-v8/predictions/`
- LGBM predictions NAR: `tmp/bucket-eval/finish-position/iter4-nar-lgbm-v8/predictions/`
- Ensemble predictions JRA: `tmp/bucket-eval/finish-position/iter4-jra-cb+lgbm-rank-v8/predictions/`
- Ensemble predictions NAR: `tmp/bucket-eval/finish-position/iter4-nar-xgb+lgbm-rank-v8/predictions/`
- Ensemble weights JRA: `apps/pc-keiba-viewer/finish-position/jra/v8-iter4-ensemble/weights.json`
- Ensemble weights NAR: `apps/pc-keiba-viewer/finish-position/nar/v8-iter4-ensemble/weights.json`
- Per-bucket delta CSVs: `tmp/v8/iter4-{jra,nar}-delta.csv`
- Decision JSON: `tmp/v8/iter4-decision.json`
- Training logs: `tmp/v8/iter4-{jra,nar}-lgbm-train.log`
