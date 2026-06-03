---
iteration: 5
date: 2026-06-04T05:55:00+09:00
based_on_iteration: 4
lever: L1B-CB+LGBM-rank-ensemble-v7-final
status: reject
quality_gate: passed
model_version_jra: jra-cb-v7-lineage-wf-21y
model_version_nar: nar-xgb-v7-lineage-wf-21y
metrics:
  wf_21y:
    jra:
      top1: 0.4032
      place2: 0.2180
      place3: 0.1627
      top3_box: 0.1430
    nar:
      top1: 0.5820
      place2: 0.3611
      place3: 0.2856
      top3_box: 0.3715
  delta_vs_baseline:
    jra:
      top1: -0.0216pp
      place2: +0.0167pp
      place3: +0.0300pp
      top3_box: -0.0117pp
    nar:
      top1: +0.0045pp
      place2: +0.0057pp
      place3: -0.0065pp
      top3_box: +0.0012pp
  composite_gain_normalized: +0.0000
  per_bucket_worst:
    jra: top1=-0.079pp place2=-0.020pp place3=-0.033pp top3_box=-0.061pp
    nar: top1=-0.152pp place2=-0.457pp place3=-0.556pp top3_box=-0.129pp
training_time:
  jra: PT6M3S LightGBM ranker 20 folds (single process, lambdarank truncation_level=5)
  nar: PT5M21S LightGBM ranker 20 folds (single process, lambdarank truncation_level=5)
ensemble_weights:
  jra:
    w_base: 0.70
    w_lgbm: 0.30
  nar:
    w_base: 0.70
    w_lgbm: 0.30
artifacts:
  feature_parquet_jra: apps/pc-keiba-viewer/tmp/feat-jra-v7-lineage (21y, 221 cols)
  feature_parquet_nar: apps/pc-keiba-viewer/tmp/feat-nar-v7-lineage (21y, mixed 134/177 cols)
  feature_parquet_source: v7-lineage regenerated for NAR 2006-2015 + 2026 (16-min PG aggregate run)
  lgbm_predictions_jra: tmp/bucket-eval/finish-position/iter5-jra-lgbm-v7-final/predictions
  lgbm_predictions_nar: tmp/bucket-eval/finish-position/iter5-nar-lgbm-v7-final/predictions
  ensemble_predictions_jra: tmp/bucket-eval/finish-position/iter5-jra-cb+lgbm-rank-v8/predictions
  ensemble_predictions_nar: tmp/bucket-eval/finish-position/iter5-nar-xgb+lgbm-rank-v8/predictions
  ensemble_weights_jra: apps/pc-keiba-viewer/finish-position/jra/v8-iter5-ensemble/weights.json
  ensemble_weights_nar: apps/pc-keiba-viewer/finish-position/nar/v8-iter5-ensemble/weights.json
  bucket_eval_csv_jra: tmp/v8/iter5-jra-delta.csv
  bucket_eval_csv_nar: tmp/v8/iter5-nar-delta.csv
  decision: tmp/v8/iter5-decision.json
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

Iter 5 lever L1B-CB+LGBM-rank-ensemble-v7-final: rigorous saturation test of iter 4. Train a fresh LightGBM lambdarank walk-forward model on the **full v7-lineage feature parquet** (221 cols JRA / 162 col intersection NAR, vs iter 4's 178-col v6-merged set) and rank-average ensemble with the active v7-lineage baseline (CB-JRA / XGB-NAR):

1. Feature regen — JRA already had 21y v7-lineage features (2006..2026, 221 cols); reused as-is. NAR was missing 2006..2015 + 2026 — regenerated via `finish_position_features_duckdb.py --category nar --from-date 20060101 --to-date 20151231 --keep-existing-output` (5.3 min, 3.48M rows, 134 cols) plus `--from-date 20260101 --to-date 20261231` (2 min, 52k rows, 134 cols). NAR 2016/2017 partitions retained their existing 177-col schema but had a known cartesian-join bug (5-7x duplicated rows per horse-race); the training loader applies an in-memory dedup on `(race_id, ketto_toroku_bango)` to restore the canonical one-row-per-horse invariant before LightGBM ingests the data.
2. Train LGBM WF 20 folds both cats over the v7-lineage parquet. Objective `lambdarank`, `lambdarank_truncation_level=5`, `num_leaves=63`, `learning_rate=0.05`, `early_stopping_rounds=50`, `seed=42+fold_year`. Per-fold features = intersection of train/valid numeric columns (range 119..206 per fold).
3. Per (cat, race_year) ensemble: join LGBM predictions with baseline `predicted_rank` on `(race_id, ketto_toroku_bango)`, rank-normalize each to `[0,1]`, score = `w_base*base_rank_norm + w_lgbm*lgbm_rank_norm`, re-rank within race.
4. Weight grid search `w_lgbm in [0.10, 0.20, 0.30, 0.40, 0.50]`, picking weight maximising 20-fold OOS composite = mean(top1, place2, place3, top3_box).

Hypothesis: iter 4's v6 feature space was strict subset of v7-lineage; using parity feature space should make the LGBM signal additive to the active CB/XGB baseline (current_best=0.40337 JRA / 0.58193 NAR top1).

## Implementation summary

Three iter5-prefixed Python helpers under `tmp/v8/` (orchestrator scope only; no source-code edits, so 95% coverage gate untouched):

- `tmp/v8/iter5_train_lgbm_predict.py` — clone of iter4 trainer with two key changes:
  - `load_year()` now globs `data*.parquet` (handles both single-file `data.parquet` and partitioned `data_N.parquet` v7-lineage layout).
  - Adds in-memory dedup on `(race_id, ketto_toroku_bango)` to safely consume NAR 2016/2017 partitions that have an upstream cartesian-join bug. The on-disk parquet is not modified; only the in-memory training DF is deduped before LightGBM grouping.
- `tmp/v8/iter5_apply_ensemble.py` — clone of iter4 ensemble (DuckDB COPY ... PARTITION_BY) with iter5 output paths.
- `tmp/v8/compute_iter5_metrics_and_decision.py` — clone of iter4 evaluator with iter5 prediction roots.

## Results — grid search

JRA (60059 races, min_year=2009):

| w_lgbm | races | top1   | place2 | place3 | top3_box | composite           |
| ------ | ----- | ------ | ------ | ------ | -------- | ------------------- |
| 0.10   | 60059 | 0.4034 | 0.2179 | 0.1624 | 0.1431   | 0.2317              |
| 0.20   | 60059 | 0.4033 | 0.2178 | 0.1625 | 0.1431   | 0.2317              |
| 0.30   | 60059 | 0.4032 | 0.2180 | 0.1627 | 0.1430   | 0.2317 **(picked)** |
| 0.40   | 60059 | 0.4038 | 0.2179 | 0.1619 | 0.1426   | 0.2316              |
| 0.50   | 60059 | 0.4026 | 0.2182 | 0.1631 | 0.1412   | 0.2313              |

NAR (244811 races, min_year=2008):

| w_lgbm | races  | top1   | place2 | place3 | top3_box | composite           |
| ------ | ------ | ------ | ------ | ------ | -------- | ------------------- |
| 0.10   | 244811 | 0.5819 | 0.3611 | 0.2856 | 0.3715   | 0.4000              |
| 0.20   | 244811 | 0.5819 | 0.3611 | 0.2856 | 0.3715   | 0.4000              |
| 0.30   | 244811 | 0.5820 | 0.3611 | 0.2856 | 0.3715   | 0.4000 **(picked)** |
| 0.40   | 244811 | 0.5821 | 0.3609 | 0.2852 | 0.3713   | 0.3999              |
| 0.50   | 244811 | 0.5830 | 0.3609 | 0.2840 | 0.3694   | 0.3993              |

## Delta vs v7-lineage baseline at best weight

JRA `w_lgbm=0.30`:

- top1: -0.022pp
- place2: +0.017pp
- place3: **+0.030pp** (matches but does not exceed the +0.030pp threshold)
- top3_box: -0.012pp

NAR `w_lgbm=0.30`:

- top1: +0.005pp
- place2: +0.006pp
- place3: -0.007pp
- top3_box: +0.001pp

Per-bucket worst regression (n >= 100):

- JRA: top1=-0.079pp, place2=-0.020pp, place3=-0.033pp, top3_box=-0.061pp (gate (d) clear)
- NAR: top1=-0.152pp, place2=-0.457pp, place3=-0.556pp, top3_box=-0.129pp (gate (d) clear)

## Decision

Both cats `reject` on gate (b) — JRA's place3 lands exactly at +0.030pp (not strictly greater), only place2 (+0.017) and place3 (+0.030) are non-negative on JRA so even relaxing the strict-inequality test only 1 axis passes, leaving the gate (b) "2 axes > +0.03pp" condition violated. NAR's largest gain is place2=+0.006pp, far below the gate.

state.json: `reject_count = 5`, `consecutive_reject_count = 5`. Distance to stop S1 (8 consecutive reject) = 3 more iters.

## LGBM training stats

JRA — 20 folds, total 6 min, best_iter min/avg/max = 40/198/446. Top features (frequency in per-fold top-10):

- `popularity_score_diff_from_race_avg`: 20/20
- `target_corner_4_norm`: 20/20
- `popularity_score`: 20/20
- `inverse_odds_market_share`: 20/20
- `target_corner_3_norm`: 20/20
- `odds_score`: 19/20
- `tansho_ninkijun_raw`: 19/20

NAR — 20 folds, total 5 min, best_iter min/avg/max = 101/213/295. Top features:

- `target_corner_4_norm`: 20/20
- `recent_finish`: 20/20
- `target_corner_1_norm`: 20/20
- `last_3_avg_finish_norm`: 20/20
- `target_corner_3_norm`: 20/20
- `avg_finish`: 20/20
- `corner_pass_avg_5`: 20/20
- `odds_score`: 16/20

## Why iter 5 ensemble failed (despite v7-feature parity)

Iter 5 was the targeted test of iter 4's hypothesis that feature-space mismatch (v6 vs v7-lineage) caused the small ensemble lift. The result rejects that hypothesis:

1. **Feature parity did not unlock gains**: JRA composite is **identical to iter 4 to 4 decimal places** (0.2317), despite training LGBM on 206 v7-lineage features instead of 164 v6 features. NAR composite is also identical (0.4000). The grid optimum shifted slightly (iter 4 JRA w_lgbm=0.10 → iter 5 JRA w_lgbm=0.30) but the maximum composite is unchanged.
2. **LGBM top features mirror the baseline**: in both cats, `popularity_score` / `odds_score` / `target_corner_4_norm` dominate gain. These are the same signals the active CB-YetiRank / XGB-rank:pairwise baselines also weight heavily. Three GBDT rankers reading the same dominant features produce highly-correlated scores; rank-averaging them only re-orders ties.
3. **Architectural saturation, not feature saturation**: the iter 4 -> iter 5 controlled experiment isolated feature-space as the variable while holding everything else (lambdarank objective, weight grid, ensemble formula) constant. The null result means the bottleneck is **model family**, not input features.
4. **JRA place3 modest signal**: place3 = +0.030pp is the only positive non-trivial delta (vs iter 4's +0.003pp). Suggests v7 features contain marginal extra place3 information the LGBM extracts, but the rank-average mixer is too coarse to lift it past the gate.

A meaningful place2/place3 lift now requires (a) a **non-GBDT secondary head** (FT-Transformer / Set Transformer / GBDT-on-residuals), or (b) **L4 bucket-aware retrain of the base CB/XGB** so the loss directly optimises under-fit slices rather than ensembling them post-hoc.

## Next iteration recommendation

Given 5 consecutive rejects on calibration / stacking / homogeneous-arch ensemble (iter 1 isotonic, iter 2 Ridge, iter 3 LGBM stacking, iter 4 LGBM rank-ensemble v6, iter 5 LGBM rank-ensemble v7-final), the saturation diagnosis is now confirmed at the **architectural family** level. Iter 6 should pivot decisively:

1. **L4 bucket-aware sample-weight retrain (highest priority)** — Retrain v7-lineage CB-JRA and XGB-NAR with `sample_weight = (1 + alpha * is_weak_bucket_score)` for buckets identified in iter 1-4 delta CSVs (e.g. JRA `kyoso_joken_code in {005, 010, 016}` and NAR `grade_code A/C` slices). The base GBDT loss directly optimises under-fit slices instead of post-hoc ensembling. Cost: ~5h CB-JRA + ~6h XGB-NAR = ~11h. Lever expected gain: +0.2pp worst-bucket, +0.1pp top1/place2.
2. **L19 FT-Transformer (MLX) as third head** — true architectural diversity. FT-Transformer treats categorical+numeric features via attention rather than tree splits, so its decision surface is orthogonal to GBDTs. Train on the same v7-lineage features. Cost: ~3h training. Lever expected gain: +0.1-0.2pp place2/place3.
3. **L3 Optuna HPO over v7-lineage CB+XGB** — 50-trial Bayesian HPO over depth, lr, l2, feature_fraction with NDCG@5 objective. Cost: ~6-12h per cat. Lever expected gain: +0.2pp top1, +0.1pp place2/place3.
4. **(Skip more LGBM variants)** — iter 3-5 all confirmed LGBM-as-meta or LGBM-as-co-head adds essentially zero to v7-lineage. No more LGBM-only ensembles.

Iter 6 recommendation: **L4 bucket-aware retrain CB-JRA + XGB-NAR**, since it directly targets gate (d) per-bucket worst (which iter 1-5 left ~0.1-0.5pp regressions on rare slices) and re-validates the base ranker on the active feature set. L19 transformer is iter 7.

## Artifacts

- Trainer: `tmp/v8/iter5_train_lgbm_predict.py`
- Ensemble apply: `tmp/v8/iter5_apply_ensemble.py`
- Evaluator: `tmp/v8/compute_iter5_metrics_and_decision.py`
- Preconditions JSON: `tmp/v8/iter5-preconditions.json`
- LGBM predictions JRA: `tmp/bucket-eval/finish-position/iter5-jra-lgbm-v7-final/predictions/`
- LGBM predictions NAR: `tmp/bucket-eval/finish-position/iter5-nar-lgbm-v7-final/predictions/`
- LGBM training summaries: `tmp/v8/iter5-{jra,nar}-lgbm-summary.json`
- Ensemble predictions JRA: `tmp/bucket-eval/finish-position/iter5-jra-cb+lgbm-rank-v8/predictions/`
- Ensemble predictions NAR: `tmp/bucket-eval/finish-position/iter5-nar-xgb+lgbm-rank-v8/predictions/`
- Ensemble weights JRA: `apps/pc-keiba-viewer/finish-position/jra/v8-iter5-ensemble/weights.json`
- Ensemble weights NAR: `apps/pc-keiba-viewer/finish-position/nar/v8-iter5-ensemble/weights.json`
- Per-bucket delta CSVs: `tmp/v8/iter5-{jra,nar}-delta.csv`
- Decision JSON: `tmp/v8/iter5-decision.json`
- Training logs: `tmp/v8/iter5-logs/{jra,nar}-lgbm.log`
- NAR feature regen logs: `tmp/v8/iter5-logs/nar-{2006-2015,2026}-regen.log` + `-status.json`
