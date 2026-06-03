---
iteration: 2
date: 2026-06-03T19:20:41.433449+00:00
based_on_iteration: 1
lever: L8-stacking-rs
status: reject
quality_gate: passed
model_version_jra: jra-cb-v7-lineage-wf-21y
model_version_nar: nar-xgb-v7-lineage-wf-21y
metrics:
  wf_21y:
    jra:
      top1: 0.3929
      place2: 0.2119
      place3: 0.1598
      top3_box: 0.1362
    nar:
      top1: 0.5819
      place2: 0.3611
      place3: 0.2856
      top3_box: 0.3715
  delta_vs_prev:
    jra:
      top1: -0.0096
      place2: -0.0060
      place3: -0.0026
      top3_box: -0.0067
    nar:
      top1: 0.0000
      place2: 0.0000
      place3: 0.0000
      top3_box: 0.0000
  composite_gain_normalized: -0.0062
  per_bucket_worst:
    jra: top1=-1.662pp place2=-1.899pp place3=-1.477pp top3_box=-0.844pp
    nar: top1=+0.000pp place2=+0.000pp place3=+0.000pp top3_box=+0.000pp
training_time:
  jra: PT0M40S Ridge sklearn 19 folds
  nar: PT0M55S Ridge sklearn 19 folds
artifacts:
  model_dir_jra: tmp/bucket-eval/finish-position/iter2-jra-cb+rs-stack-v8
  model_dir_nar: tmp/bucket-eval/finish-position/iter2-nar-cb+rs-stack-v8
  predictions_parquet: tmp/bucket-eval/finish-position/iter2-{cat}-cb+rs-stack-v8/predictions
  bucket_eval_csv: tmp/v8/iter2-{jra,nar}-delta.csv
  running_style_survey: tmp/v8/iter2-running-style-survey.json
  stacking_dataset: tmp/v8/iter2-stacking-dataset
---

## What was tried

Iter 2 lever L8-modified stacking: Ridge meta-learner consuming v7-lineage `predicted_score` + race-context (distance band one-hot, surface dirt flag, num_horses, mean_field_score, score_std_in_race, race_year) + (JRA only) actual running-style one-hot from `jvd_se.kyakushitsu_hantei` (0=未判定, 1=逃げ, 2=先行, 3=差し, 4=追込). Walk-forward per fold year 2007-2026, inner 5-fold year-block CV picks alpha from {0.1, 1, 10, 100}, OOS rerank within race.

NAR could not use running-style features because `nvd_se.kyakushitsu_hantei` is **all zeros** (NAR has no historical running-style truth label); NAR stacks on race-context only.

## Implementation summary

New module `apps/pc-keiba-viewer/src/scripts/train_finish_position_stacking_metalearner.py` (368 LOC, 98% cov via 72 tests in `tests/test_train_finish_position_stacking_metalearner.py`). Two subcommands:

- `--mode build-dataset`: joins enriched v7-lineage parquet + race-context (kyori/track_code) + (JRA) kyakushitsu_hantei into per-horse stacking parquet.
- `--mode train`: walk-forward Ridge per fold year, year-block inner CV for alpha, rerank predictions, write per-fold metadata + reranked parquet.

Race-context and JRA running-style labels were pre-extracted from local PG via DuckDB `postgres_scanner` into `tmp/v8/iter2-pg-inputs/` (parquet) to keep the trainer offline. fold_year=2007 skipped (insufficient training rows, train_size=0). JRA dataset ~942k rows, NAR ~2.57M rows.

Evaluation via `tmp/v8/compute_iter2_metrics_and_decision.py` (DuckDB). Comparison restricted to race_ids present in iter2 OOS (excluding fold_year=2007) to keep apples-to-apples.

## Results

JRA delta pp: top1 −0.957, place2 −0.605, place3 −0.265, top3_box −0.672 (n=63511 races). All 4 axes regress beyond −0.05pp gate.

NAR delta pp: top1 +0.000, place2 +0.000, place3 +0.000, top3_box +0.000 (n=244811 races). Ridge with race-context-only inputs produces ranking identical to baseline because race-context features are race-constant (do not vary per horse within a race), so Ridge's linear combination preserves the per-race ordering of `predicted_score`.

## Per-bucket findings

JRA worst per-bucket regression (n≥100, grade_code dim): top1 −1.66pp, place2 −1.90pp, place3 −1.48pp, top3_box −0.84pp — adding kyakushitsu_hantei via Ridge actively hurts every grade bucket. NAR per-bucket all 0.00pp (no rank changes).

## Decision

Both JRA and NAR REJECT.

- JRA: gate-(a) violated — top1 delta −0.957pp ≪ −0.05pp. Ridge cannot beat CatBoost YetiRank because the linear meta-learner is too weak to combine the rank-based base score with class-imbalanced kyakushitsu signal; the running-style feature drags the score toward the population mean.
- NAR: gate-(b) violated — 0 axes > +0.03pp. Ridge on race-context-only inputs is degenerate (preserves baseline ranking exactly).

State: `current_baseline_*` unchanged (v7-lineage), `reject_count` 1→2, `consecutive_reject_count` 1→2.

## Next iteration recommendation

Iter 2 confirms two earlier lessons in addition to iter1's:

1. **Linear meta-learners over rank-based base models lose info**: Ridge averaging over CatBoost YetiRank scores discards the latent rank-pair information CatBoost extracts. If we want stacking, the meta-learner must itself be rank-aware (e.g., LGBM lambdarank or XGB pairwise).
2. **Race-constant features cannot rerank**: Stacking inputs must vary per-horse within a race to influence ranking. NAR iter2 is a clean negative: identical metrics prove this.

Recommended next levers (priority order):

- **L1B / L1C ensemble (multi-arch retrain)**: blend CatBoost + LGBM + XGB at score level using rank-fusion (Borda / reciprocal-rank) rather than linear meta. Requires WF feature parquet — blocked unless we regenerate `tmp/feat-{cat}-v7-final/`.
- **L15 sectional fitness signal**: closing-leg speed from `jvd_se.kohan_*` columns into a new per-horse feature. Direct per-horse signal that does vary within a race; addresses iter2's "race-constant" failure mode.
- **L4 bucket-aware sample weighting on weak buckets**: amplify training weight on the grade buckets where iter1/iter2 showed regression; cheaper than full retrain.

If running-style is to be retried, must use a non-linear meta (gradient boosted ranker, NOT Ridge) and supply running-style as one of many varied per-horse signals so it doesn't dominate the regularization.

## Quality Gate Results

- python:check: ruff + ty + basedpyright + pytest (cov-fail-under=95) all green, 1458 tests pass, total cov=97.08%, new module cov=98%.
- lint: 0 warnings (oxlint TS side unchanged from iter1, Python ruff clean).
- format:check: exit 0 (oxfmt TS, ruff format Python).
- tsc: 0 errors.
- No new `# type: ignore`, `# noqa`, `// oxlint-disable`, `// eslint-disable`, `// @ts-ignore`, or `/* v8 ignore */` comments introduced.
