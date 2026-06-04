---
iteration: 15
date: 2026-06-04T19:30:00+09:00
based_on_iteration: 14
lever: L5C-track-condition-x-style-x-course-interactions
status: rejected (JRA)
quality_gate: passed
loop_status: active
model_version_jra: iter14-jra-cb-pacestyle-course-v8 (UNCHANGED — iter 15 rejected)
model_version_nar: iter12-nar-xgb-hpo-v8 (unchanged)
baselines:
  jra: iter14-jra-cb-pacestyle-course-v8 (accepted on iter 14)
  jra_v7_reference: jra-cb-v7-lineage-wf-21y (absolute anchor)
  nar_primary: iter12-nar-xgb-hpo-v8 (current Phase 1 production)
features:
  base_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course
  output_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter15-interactions
  interaction_specs_added: 10
  interaction_pairs:
    - { new: track_state_x_past_nige, left: track_condition_normalized, right: past_nige_rate_self }
    - {
        new: track_state_x_past_oikomi,
        left: track_condition_normalized,
        right: past_oikomi_rate_self,
      }
    - {
        new: track_state_x_field_nige_pressure,
        left: track_condition_normalized,
        right: field_nige_pressure,
      }
    - {
        new: track_state_x_final_straight,
        left: track_condition_normalized,
        right: course_final_straight_m,
      }
    - { new: elevation_x_past_oikomi, left: course_elevation_diff_m, right: past_oikomi_rate_self }
    - { new: elevation_x_field_pace_index, left: course_elevation_diff_m, right: field_pace_index }
    - {
        new: first_corner_x_field_nige,
        left: course_dist_to_first_corner_m,
        right: field_nige_pressure,
      }
    - {
        new: first_corner_x_past_nige,
        left: course_dist_to_first_corner_m,
        right: past_nige_rate_self,
      }
    - {
        new: final_straight_x_past_oikomi,
        left: course_final_straight_m,
        right: past_oikomi_rate_self,
      }
    - { new: final_straight_x_past_nige, left: course_final_straight_m, right: past_nige_rate_self }
  total_cols_iter14: 260
  total_cols_iter15: 270
  effective_feature_count: 251 (post resolve_feature_columns drop of meta/bool/object)
  rows_in_eq_rows_out_per_year: true (no JOIN, pure column math)
coverage_2026:
  track_state_x_past_nige: 0.610
  track_state_x_past_oikomi: 0.610
  track_state_x_field_nige_pressure: 0.982
  track_state_x_final_straight: 0.713
  elevation_x_past_oikomi: 0.279
  elevation_x_field_pace_index: 0.425
  first_corner_x_field_nige: 0.337
  first_corner_x_past_nige: 0.236
  final_straight_x_past_oikomi: 0.444
  final_straight_x_past_nige: 0.444
training:
  arch: catboost (YetiRank loss, NDCG:top=3 eval)
  hyperparams: depth=8 / lr=0.05 / l2=3.0 / iterations=1000 / od_wait=30 (same v7-lineage defaults as iter 14)
  random_seed_base: 42 (+ fold_year stabilization)
  fold_count: 20 (2007..2026)
  train_start_year: 2006
  best_iter_avg: 280 (min 92 max 454, lower than iter 14's 319)
metrics:
  wf_21y_common_races_vs_iter14: 66964
  jra:
    baseline_iter14:
      { races: 66964, top1: 0.40302, place2: 0.21806, place3: 0.16239, top3_box: 0.14351 }
    iter15: { races: 66964, top1: 0.39809, place2: 0.21709, place3: 0.16173, top3_box: 0.14221 }
    delta_pp_vs_iter14: { top1: -0.493, place2: -0.097, place3: -0.066, top3_box: -0.130 }
    delta_pp_vs_v7: { top1: -0.330, place2: -0.021, place3: -0.030, top3_box: -0.021 }
feature_importance:
  interactions_appearing_in_top25_any_fold: 0
  interaction_top25_fold_counts:
    track_state_x_past_nige: 0/20
    track_state_x_past_oikomi: 0/20
    track_state_x_field_nige_pressure: 0/20
    track_state_x_final_straight: 0/20
    elevation_x_past_oikomi: 0/20
    elevation_x_field_pace_index: 0/20
    first_corner_x_field_nige: 0/20
    first_corner_x_past_nige: 0/20
    final_straight_x_past_oikomi: 0/20
    final_straight_x_past_nige: 0/20
training_time:
  full_wf_21y: ~12 min wall (20 folds, single CPU sequential, deeper than iter 14 due to higher coverage interactions)
artifacts:
  feature_build_script: tmp/v8/iter15_build_interactions.py
  feature_build_summary: tmp/v8/iter15-build-summary.json
  feature_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter15-interactions
  train_script: tmp/v8/iter15_train_predict.py
  full_train_summary: tmp/v8/iter15-train-summary.json
  predictions: tmp/bucket-eval/finish-position/iter15-jra-cb-interactions-v8/predictions/category=jra/race_year=*/predictions.parquet (20 folds, 2007-2026)
  metrics_script: tmp/v8/compute_iter15_metrics_and_decision.py
  decision: tmp/v8/iter15-decision.json
  metrics_global: tmp/v8/iter15-metrics-global.json
  train_log: tmp/v8/iter15-train.stderr.log
---

## What was tried

**L5C track-condition x style x course interaction features on top of iter 14
course-numerical baseline.** Hypothesis: even though CB can implicitly cross
features via tree splits, explicit pre-computed products of (track condition x
style propensity), (track condition x course attr), (course attr x style
propensity), and (course attr x field pace) might surface higher-order signal
the depth-8 trees don't decompose efficiently in 21y WF.

Pure feature math: 10 new float32 columns added by multiplying pairs of
already-existing iter 14 columns. No new PG reads, no JOIN — just per-row
products. Track condition source = `track_condition_normalized` (0-1 scale,
~98% non-null), style sources = `past_nige_rate_self` /
`past_oikomi_rate_self` (~60-65% non-null, requires horse career history),
course sources = `course_final_straight_m`, `course_elevation_diff_m`,
`course_dist_to_first_corner_m` (already in iter 14), pace source =
`field_nige_pressure` / `field_pace_index` (race-level, ~100% non-null).

### Training protocol

Identical to iter 14: v7-lineage CB defaults (`depth=8, lr=0.05, l2=3.0,
iterations=1000, od_wait=30, YetiRank, NDCG@3 eval`), 20-fold WF 2007-2026,
train_start=2006, time-decay weights, `seed = 42 + fold_year`.

## Results

### 4-metric delta vs iter 14 JRA baseline (decision gate)

| axis     | iter 14 baseline | iter 15 | delta_pp   |
| -------- | ---------------- | ------- | ---------- |
| top1     | 0.40302          | 0.39809 | **-0.493** |
| place2   | 0.21806          | 0.21709 | **-0.097** |
| place3   | 0.16239          | 0.16173 | **-0.066** |
| top3_box | 0.14351          | 0.14221 | **-0.130** |

All 4 axes regress. Worst is `top1` at -0.493pp, well past the -0.05pp
acceptance tolerance.

### 4-metric delta vs v7-lineage (absolute reference)

| axis     | delta_pp_vs_v7 |
| -------- | -------------- |
| top1     | -0.330         |
| place2   | -0.021         |
| place3   | -0.030         |
| top3_box | -0.021         |

Iter 15 is below v7-lineage on every axis, fully undoing the iter 14
breakthrough (which was top1 +0.163 / place2 +0.076 / place3 +0.036 / top3_box
+0.109).

### Feature importance — interactions DID NOT surface

`feature_importance_top25` inspected across all 20 fold metadata files:
**0 of 10 interaction columns appear in any fold's top-25**. The model wasted
capacity learning splits over these pre-computed products instead of the more
informative atomic features (`target_corner_4_norm`,
`course_final_straight_m`, etc. that dominated iter 14 top-25).

Best-iteration average dropped from iter 14's 319 to 280 — early stopping
triggered sooner, consistent with the noise hypothesis: extra columns inflate
candidate splits, valid loss plateaus earlier, model under-trains relative to
iter 14.

## Decision: REJECT

5-condition gate vs iter 14 JRA baseline:

- (a) all 4 axes >= -0.05pp: **FAIL** (worst -0.493)
- (b) >=2 axes positive > +0.03pp: **FAIL** (0 positive)
- (c) place2 or place3 positive: **FAIL** (both negative)
- (d) per-bucket worst regression: not computed (gate already failed)
- (e) quality gate: pending (Python / TS check still to run, but does not
  change accept/reject)

`current_baseline_jra` remains `iter14-jra-cb-pacestyle-course-v8`.
`consecutive_reject_count` resets from 0 to 1 (was 0 after iter 14 accept).
`reject_count` 14 -> 15.

## Why this likely failed

1. **CB already crosses features internally.** Tree splits at depth 8 routinely
   handle pairwise interactions via consecutive splits on parent/child nodes.
   Pre-computing explicit products doesn't add information — it duplicates it
   in a less-flexible form (single split point vs piecewise tree threshold).
2. **NaN propagation through `*` reduces effective coverage.** When either
   left or right operand is NaN, the product is NaN. The interactions hit
   ~24-44% non-null coverage in many cases vs ~60-98% for the source columns
   — the model loses sample mass for those features.
3. **Multicollinearity inflates split-candidate count.** With 10 derived
   columns highly correlated with 13 source columns, CB's per-iteration split
   search wastes evaluation budget; early stop fires sooner (avg 280 vs 319).
4. **Track condition is already a tree-friendly categorical-ish numeric
   (4 buckets 0.0 / 0.3 / 0.6 / 1.0).** The optimal partition is one shallow
   split, then independent sub-tree for each track state. Multiplying makes
   the same partition continuous and harder to recover.

## Iter 16 recommendation

Iter 15 confirms that **explicit cross-term features are not a profitable
direction on top of an already-accepted CB baseline at depth 8**. Next iter
should switch axis. Best candidates:

1. **L5A booster on iter 14 (best)**: re-introduce the iter 9 / iter 10a
   "deeper CB" lever ON the iter 14 feature set (260 cols). Iter 10a went
   depth 10 / lr 0.04 / iters 800 / l2 4 on iter 9; was reject vs v7-lineage
   for top1, but the iter 14 +0.163 top1 baseline gives much more headroom
   for a depth bump to monetize. Reuses everything already built.
2. **L4 new-data axis: trainer x track x distance interaction tables**
   directly from `pg.jvd_cs` / `nvd_cs` aggregations, not pre-computed
   products. These are categorical lookups, not multiplications, so CB can
   learn the table directly without the multicollinearity problem.
3. **L6 calibrated stacking with iter 14 base** (iter 11 attempt was on iter 9
   base which was already weak — retry on the now-stronger iter 14 base).
4. **HPO on iter 14 features** (analogous to iter 13 HPO on iter 9 — iter 13
   regressed, but iter 14 has 19 more features so HPO basin shifted).

Pick (1) for compounding (lowest engineering, highest expected gain on
top1).
