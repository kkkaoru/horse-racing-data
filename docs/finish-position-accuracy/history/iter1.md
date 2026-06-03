---
iteration: 1
date: 2026-06-03T18:54:24.711937+00:00
based_on_iteration: 0
lever: L2
status: reject
quality_gate: passed
model_version_jra: jra-cb-v7-lineage-wf-21y
model_version_nar: nar-xgb-v7-lineage-wf-21y
metrics:
  wf_21y:
    jra:
      top1: 0.4014
      place2: 0.2173
      place3: 0.1619
      top3_box: 0.1424
    nar:
      top1: 0.5805
      place2: 0.3609
      place3: 0.2861
      top3_box: 0.3718
  delta_vs_prev:
    jra:
      top1: 0
      place2: 0
      place3: -0.0001
      top3_box: 0
    nar:
      top1: 0
      place2: 0
      place3: 0
      top3_box: 0
  composite_gain_normalized: -0.0001
  per_bucket_worst:
    jra: top1=+0.000pp place2=+0.000pp place3=-0.092pp top3_box=+0.000pp
    nar: top1=+0.000pp place2=-0.075pp place3=-0.000pp top3_box=-0.000pp
training_time:
  jra: PT0S calibration-only
  nar: PT0S calibration-only
artifacts:
  model_dir_jra: apps/pc-keiba-viewer/finish-position/jra/v8-iter1-calibration
  model_dir_nar: apps/pc-keiba-viewer/finish-position/nar/v8-iter1-calibration
  predictions_parquet: tmp/v8/calibrated-predictions/iter1-CAT-ARCH+iso-v8/predictions
  bucket_eval_csv: tmp/v8/iter1-{jra,nar}-delta.csv
---

## What was tried

Iter 1 lever L2: per-(cat × bucket) isotonic calibration on WF v7-lineage predictions. JRA bucket_dim=kyoso_joken (6 buckets), NAR bucket_dim=grade (7 buckets), min_bucket_samples=500. Calibration-only — no model retrain.

## Implementation summary

Enriched WF predictions parquet with PG actuals + bucket dim columns (one-shot helper). Ran calibrate_finish_position.py --mode fit then --mode apply. Computed 4-metric global + per-bucket via direct DuckDB scan of enriched + calibrated parquets (bypassed PG bucket-eval pipeline due to runaway 14min/year query plan on contention).

## Results

JRA delta pp: top1 +0.000, place2 +0.001, place3 -0.012, top3_box +0.000. NAR delta pp: top1 +0.000, place2 +0.000, place3 +0.000, top3_box -0.000.

## Per-bucket findings

Worst per-bucket regression (n>=100): JRA top1 +0.000pp place2 +0.000pp place3 -0.092pp top3_box +0.000pp; NAR top1 +0.000pp place2 -0.075pp place3 -0.000pp top3_box -0.000pp.

## Decision

JRA reject (gate-(b) violated: only 0 axes > +0.03pp); NAR reject (gate-(b) violated: only 0 axes > +0.03pp).

## Next iteration recommendation

Iter 2 候補: L1B multi-arch ensemble (+LGBM, retrain ~6h), L15 sectional fitness (closing speed → place2/3), or L4 bucket-aware sample weighting to target worst-bucket regressions surfaced here. Re-fit L2 calibration after weak-bucket signal collection is also viable.

## Quality Gate Results

- tsc: 0 errors
- lint: 0 warnings
- format:check: exit 0
- test:coverage: branches=0/functions=0/lines=0/statements=0
- python:check: pytest cov=0
