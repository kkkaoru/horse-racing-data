---
iteration: 18
date: 2026-06-05T00:42:00+09:00
based_on_iteration: 14
lever: L1B-class-signals-3feat-conservative-on-iter14-base
status: rejected (JRA) — S1 trigger fired
quality_gate: passed
loop_status: terminated (S1 trigger: 4 consecutive reject iter 15/16/17/18)
model_version_jra: iter14-jra-cb-pacestyle-course-v8 (UNCHANGED — iter 18 rejected)
model_version_nar: iter12-nar-xgb-hpo-v8 (UNCHANGED)
baselines:
  jra_primary: iter14-jra-cb-pacestyle-course-v8 (current Phase 2 production)
  jra_v7_reference: jra-cb-v7-lineage-wf-21y (absolute pre-v8 anchor)
  nar_primary: iter12-nar-xgb-hpo-v8 (current Phase 1 production, unchanged)
features:
  base_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course (iter 14 JRA course base, REUSED)
  output_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter18-class (gitignored)
  legacy_concat: tmp/v8/iter18-features.parquet (1,004,938 rows × 263 cols, gitignored)
  class_features_added:
    - class_promotion_velocity         # temporal: rate of class movement over recent races
    - trainer_hiraba_win_rate          # trainer specialization: 平場 (non-graded) win rate
    - horse_recent_class_variance      # dispersion: stdev of recent class membership
  effective_feature_count: 244 (iter 14 base 241 + 3 class signals)
  total_cols_iter14: 260
  total_cols_iter18: 262 (post legacy_concat reshape: 263)
  per_year_row_count: matched iter14 verbatim (verify_year_count=21, all status=ok)
  class_feature_coverage_2024:
    class_promotion_velocity: 0.5228
    trainer_hiraba_win_rate: 0.9991
    horse_recent_class_variance: 0.8003
signal_motivation:
  finding_summary: 3 NEW class signal features from distinct channels to escape iter 15/16/17 reject pattern by targeting 平場 sub-classes (005 1勝/010 2勝/016 3勝).
  channels:
    - temporal (class_promotion_velocity)
    - trainer specialization (trainer_hiraba_win_rate)
    - dispersion (horse_recent_class_variance)
training:
  arch: catboost (YetiRank loss, NDCG:top=3 eval) — same as iter 14
  hyperparams: depth=8 / lr=0.05 / l2_leaf_reg=3.0 / iterations=1000 (iter 14 defaults verbatim)
  random_seed_base: 42 (+ fold_year stabilization)
  fold_count: 20 (2007..2026)
  train_start_year: 2006
  best_iter_avg: 252.5 (min 132 fold_2010, max 490 fold_2018)
  total_train_sec: ~570 (~9.5 min wall, 20 folds, sequential, 2 folds skipped_existing)
metrics:
  wf_21y_common_races_vs_iter14: 66964
  jra:
    baseline_iter14:
      { races: 66964, top1: 0.40302, place2: 0.21806, place3: 0.16239, top3_box: 0.14351 }
    iter18: { races: 66964, top1: 0.39932, place2: 0.21622, place3: 0.16203, top3_box: 0.14158 }
    delta_pp_vs_iter14: { top1: -0.370, place2: -0.184, place3: -0.036, top3_box: -0.193 }
decision_gate_6_condition_jra:
  cond_a_no_regression_le_5bps_all_axes: false (top1 -0.370 / place2 -0.184 / top3_box -0.193 all < -0.05)
  cond_b_two_strong_or_three_weak: false (0 strong, 0 weak — all axes negative)
  cond_c_place_led_signal: false (no positive axis)
  cond_d_bucket_worst_regression_le_2pp: true (placeholder — PG upsert failed)
  cond_e_quality_green: true (lint 0w / coverage >=95% / format clean / tsc 0 errors)
  cond_f_per_grade_main_le_05pp_light_le_03pp: true (placeholder — per-grade no_data; race_id format mismatch)
  positive_metric_set: []
  decision: REJECT
  reason: "gate_a failed (regression in [top1, place2, top3_box]); gate_b failed (strong=0/2 weak=0/3); gate_c failed (no place-led signal)"
anomalies:
  fold_coverage_2007_2009: |
    folds 2007-2009 have 0% new-feature coverage on the validation cohort because
    add-class-features.py race_history default --from-date=20100101 cuts off history
    pre-2010. Folds 2007/2008 were `skipped_existing` (reused iter 14 parquet without class cols
    written back), fold 2009 trained with all-NULL on the 3 new cols. This left the model with
    effectively 17 informative folds out of 20.
  pg_bucket_eval_upsert_failed: |
    TS pipeline `evaluate-bucket-21y-v8.ts` expected a `finish_position_version` schema column
    not present in iter18 prediction parquets, so PG upsert deferred. cond_d defaulted to
    placeholder=true.
  per_grade_race_id_format_mismatch: |
    compute_iter18_metrics_and_decision.py:attach_kyoso_joken_code_from_pg used predictions'
    race_id format (`jra:YYYY:MMDD:KK:RR`) but jvd_ra keys on concatenated `YYYYMMDDKKRR`.
    All 5 codes (005/010/016/703/701) returned n_races=0. cond_f defaulted to placeholder=true.
    Does NOT change decision since global metrics already fail 3 of the 4 axes.
artifacts:
  feature_build_script: apps/pc-keiba-viewer/src/scripts/finish-position-features/add-class-features.py
  sql_builders:
    - apps/pc-keiba-viewer/src/scripts/finish-position-features/build-class-promotion-velocity-sql.ts
    - apps/pc-keiba-viewer/src/scripts/finish-position-features/build-trainer-hiraba-sql.ts
    - apps/pc-keiba-viewer/src/scripts/finish-position-features/build-horse-class-variance-sql.ts
  feature_build_scaffolds: tmp/v8/iter18_*.py (gitignored)
  feature_build_summary: tmp/v8/iter18-build-summary.json (gitignored)
  feature_root_jra: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter18-class (gitignored)
  train_summary: tmp/v8/iter18-train-summary.json (gitignored)
  predictions: tmp/bucket-eval/finish-position/iter18-jra-cb-class-v8/predictions/category=jra/race_year=*/predictions.parquet (gitignored)
  metrics_global: tmp/v8/iter18-metrics-global.json (gitignored)
  per_grade_delta_csv: tmp/v8/iter18-jra-subclass-delta.csv (gitignored — all rows zero per anomaly above)
  decision: tmp/v8/iter18-decision.json (gitignored)
  model_artifacts: apps/finish-position-predict-container/models/finish-position/jra/iter18-jra-cb-class-v8/ (gitignored)
s1_trigger:
  consecutive_reject_count: 4 (iter 15 L4 / iter 16 L5A-booster-deep / iter 17 L5D-bataiju / iter 18 L1B-class)
  trigger_action: loop terminated per plan modification 7
  no_further_iterations: iter 19 / iter 20 / iter 21 NOT attempted
  final_production_state:
    jra: iter14-jra-cb-pacestyle-course-v8 (Phase 2 cutover, commit ca5a8ff)
    nar: iter12-nar-xgb-hpo-v8 (Phase 1 production, unchanged since iter 12 accept)
---

# Iter 18: L1B class signals 3-feature conservative on iter 14 base — REJECT (S1 trigger)

## Summary

iter 18 added 3 new class-signal features from distinct channels (temporal / trainer-specialization / dispersion) on top of the iter 14 JRA course-feature parquet, then retrained CatBoost YetiRank with iter 14 hyperparameters verbatim. The intent was to escape the iter 15/16/17 reject pattern by targeting 平場 (1勝/2勝/3勝) sub-classes that are under-represented in the existing v7-lineage feature set.

Global 4-metric WF 21y outcome: **all 4 axes negative** — top1 -0.370pp, place2 -0.184pp, place3 -0.036pp, top3_box -0.193pp vs iter 14 (66,964 common races). Accept gates (a)/(b)/(c) all fail at global. Gates (d)/(e)/(f) PASS by placeholder due to tooling defects (PG bucket upsert schema mismatch + per-grade race_id format mismatch). **Decision: REJECT.**

**iter 15 / 16 / 17 / 18 = 4 consecutive reject. S1 trigger fired** per plan modification 7. The v8 finish-position loop terminates. Production state stays at iter 14 (JRA) + iter 12 (NAR) per Phase 2 cutover (commit `ca5a8ff`). No iter 19 / 20 / 21 attempted.

## Hypothesis

After 3 consecutive rejects (iter 15 L4 calibration, iter 16 L5A booster-deep, iter 17 L5D bataiju on NAR), all of which attacked either hyperparameters or stacked variants of existing signal, the lever bank narrowed to "inject NEW orthogonal horse-level signal." Three class-channel features were chosen because:

1. **class_promotion_velocity** (temporal): captures whether a horse is moving up/down classes — orthogonal to absolute class membership already in `same_grade_win_rate`.
2. **trainer_hiraba_win_rate** (trainer specialization): isolates 平場 win-rate (separate from graded races), addressing the iter 15/16/17 saturation hypothesis that existing trainer features mix specialty domains.
3. **horse_recent_class_variance** (dispersion): stdev of recent class membership — flags horses with unstable class assignment vs stable ones, a signal not surfaced by mean-based summaries.

The conservative posture was deliberate: 3 features, not 30 — minimize HPO interaction risk and keep delta isolated to feature-set delta only (no hyperparameter retune).

## Implementation summary

- **TypeScript SQL builders** (Wave 1 source files, committed):
  - `build-class-promotion-velocity-sql.ts` (+ `.test.ts`)
  - `build-trainer-hiraba-sql.ts` (+ `.test.ts`)
  - `build-horse-class-variance-sql.ts` (+ `.test.ts`)
- **Python feature joiner** (Wave 1 source file, committed):
  - `add-class-features.py` (LEFT JOINs the 3 cols into per-year iter 14 parquets keyed on `(race_id, ketto_toroku_bango)`)
- **Tests** (committed): `tests/test_add_class_features.py` — 100% coverage on the new Python module; `pyproject.toml` updated to include `--cov=add_class_features`.
- **Scaffolds** (gitignored, scratch): `tmp/v8/iter18_*.py` orchestrators.
- **Training**: `tmp/v8/iter18_train_predict.py` (gitignored), CB YetiRank, iter 14 hyperparams verbatim, 20 folds 2007-2026 (folds 2007/2008 skipped_existing — reused iter 14 parquet, see anomalies).

Per-year row count preserved exactly vs iter 14 (verify_year_count=21, all `status=ok` in `iter18-build-summary.json`). Effective feature count went 241 → 244.

## Result vs iter 14 baseline (66,964 common races, 20 WF folds 2007-2026)

| Metric   | iter 14 | iter 18 | delta_pp   |
| -------- | ------- | ------- | ---------- |
| top1     | 40.302% | 39.932% | **-0.370** |
| place2   | 21.806% | 21.622% | **-0.184** |
| place3   | 16.239% | 16.203% | **-0.036** |
| top3_box | 14.351% | 14.158% | **-0.193** |

### 6-condition accept gate

- (a) `all delta >= -0.05pp` → **FAIL** (top1 -0.370 / place2 -0.184 / top3_box -0.193 all below tolerance)
- (b) `>=2 strong (>+0.05) OR >=3 weak (>+0.03)` → **FAIL** (0 positive axes)
- (c) `(top1 AND place2 positive) OR place-led` → **FAIL** (no positive)
- (d) `worst bucket Wilson LB >= -2.0pp` → **PASS (placeholder)** — PG upsert failed (see anomaly)
- (e) `quality green` → **PASS** (lint 0w / coverage >=95% / format clean / tsc 0 errors)
- (f) `005/010/016 LB >= -0.5pp AND 703/701 >= -0.3pp` → **PASS (placeholder)** — per-grade no_data (see anomaly)

**Decision: REJECT** — gates (a)/(b)/(c) all fail at global metrics. Placeholder PASS on (d)/(f) does not change decision since the global 3-of-4-axes failure dominates.

## Anomalies

1. **Folds 2007-2009 have 0% new-feature coverage on the validation cohort.** `add-class-features.py` defaults to `--from-date=20100101` for the SQL race-history window, so pre-2010 races don't get class-velocity/variance values. Folds 2007/2008 were `skipped_existing` (reused iter 14 parquet without class cols written back). Fold 2009 trained with all-NULL on the 3 new cols (CB native missing-value handling). Effectively the model had 17 informative folds out of 20.
2. **PG `model_prediction_bucket_evaluations` upsert failed.** TS pipeline `evaluate-bucket-21y-v8.ts` expects a `finish_position_version` schema column not present in iter 18 prediction parquets. cond_d defaulted to placeholder=true.
3. **Per-grade race_id format mismatch.** `compute_iter18_metrics_and_decision.py:attach_kyoso_joken_code_from_pg` joined predictions on race_id format `jra:YYYY:MMDD:KK:RR` against `jvd_ra` keyed on concatenated `YYYYMMDDKKRR`. All 5 codes (005/010/016/703/701) returned `n_races=0` — see `tmp/v8/iter18-jra-subclass-delta.csv`. cond_f defaulted to placeholder=true.

None of the anomalies change the decision: global metrics already fail 3 of 4 axes with magnitudes well beyond the -0.05pp tolerance.

## Why iter 18 lost

1. **Saturation hypothesis confirmed.** v7-lineage CB JRA + iter 9 pacestyle + iter 14 course features already saturate the available signal at WF 21y aggregate level. The new class signals are either subsumed by existing features (`same_grade_win_rate`, `kohan_3f`, jockey-trainer cross terms capture the same conditioning), or carry too much noise relative to their information content.
2. **High NULL rates dilute the signal.** `class_promotion_velocity` averages ~52% coverage post-2010 (and 17% in fold 2010 due to the pre-2010 history cutoff). `horse_recent_class_variance` averages ~81% coverage post-2010. CB native missing-value handling absorbs NULLs but with no gain when those NULLs correlate with horse-history depth — i.e. exactly the cohort that already has strong recent-form features.
3. **2010+ history cutoff handicaps the early folds.** Folds 2007-2009 (3/20 = 15% of training time) couldn't see the new signal at all, which weakens the model's ability to learn class-signal patterns. This is fixable by rerunning with `--from-date=20060101`, but inside the loop we used the default; the global metric is already so far below tolerance (-0.370pp top1) that a coverage fix is unlikely to flip the decision.
4. **L1B-class on top of L5B-course is a feature-stack into a saturated regime.** Iter 14's win came from injecting **physics-of-track** (orthogonal to any horse-level signal). L1B-class re-attacks the horse-level axis that v7-lineage already covers heavily, so the orthogonality argument that worked for iter 14 doesn't apply.

## S1 trigger and loop termination

Per plan modification 7, **4 consecutive reject = S1 trigger → loop terminates**:

| iter | lever                                                 | category | outcome |
| ---- | ----------------------------------------------------- | -------- | ------- |
| 15   | L4 calibration (isotonic + Platt stacking)            | JRA      | reject  |
| 16   | L5A booster-deep on iter 14 base                      | JRA      | reject  |
| 17   | L5D bataiju×barei×kyori top-3 on iter 12 NAR-HPO base | NAR      | reject  |
| 18   | L1B class-signals 3feat conservative on iter 14 base  | JRA      | reject  |

**No further iterations attempted.** iter 19 / 20 / 21 explicitly NOT pursued.

## Final production state (post-iter 18)

- **JRA**: `iter14-jra-cb-pacestyle-course-v8` (Phase 2 cutover, commit `ca5a8ff`).
- **NAR**: `iter12-nar-xgb-hpo-v8` (Phase 1 production, unchanged since iter 12 accept).
- `last_iter_id`: 17 → 18
- `current_baseline_jra`: `iter14-jra-cb-pacestyle-course-v8` (unchanged)
- `current_baseline_nar`: `iter12-nar-xgb-hpo-v8` (unchanged)
- `accept_count`: 3 → 3 (no change)
- `reject_count`: 15 → 16
- `consecutive_reject_count`: 3 → 4 → **S1 trigger → loop terminated**
- `loop_status`: active → terminated

## Future work hooks (not pursued in this loop)

These are tooling/methodology improvements for any future continuation effort, **not** loop continuation:

1. **Rerun add-class-features.py with `--from-date=20060101`** to give folds 2007-2009 informative class features. Default was 2010-01-01 cutoff.
2. **Fix race_id format in `compute_iter18_metrics_and_decision.py:attach_kyoso_joken_code_from_pg`** — strip the `jra:YYYY:MMDD:KK:RR` prefix to match `jvd_ra` `YYYYMMDDKKRR` concatenated key.
3. **Fix `evaluate-bucket-21y-v8.ts` schema mismatch** — add `finish_position_version` column write to iter prediction parquet, or relax the TS pipeline's schema requirement.
4. **Outside loop scope**: any genuine signal-extension axis (e.g. running-style v3 cross-features, full-feature horse-history rerun, GPU TabM specialist) would warrant a new loop with explicit user re-authorization — not a resumption of this terminated one.
