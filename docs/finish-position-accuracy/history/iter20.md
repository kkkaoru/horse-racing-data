---
iteration: 20
date: 2026-06-05T12:00:00+09:00
based_on_iteration: 14
lever: L-per-class-architecture-on-iter14-base
status: rejected (JRA) — hypothesis falsified across all 6 classes
quality_gate: passed
loop_status: 6 consecutive reject (iter 15/16/17/18/19/20) — per-class architecture pivot also rejected
model_version_jra: iter14-jra-cb-pacestyle-course-v8 (UNCHANGED — iter 20 rejected)
model_version_nar: iter12-nar-xgb-hpo-v8 (UNCHANGED)
baselines:
  jra_primary: iter14-jra-cb-pacestyle-course-v8 (current Phase 2 production)
  jra_v7_reference: jra-cb-v7-lineage-wf-21y (absolute pre-v8 anchor)
  nar_primary: iter12-nar-xgb-hpo-v8 (current Phase 1 production, unchanged)
features:
  base_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course (iter 14 JRA course base, REUSED verbatim)
  effective_feature_count: 241 (identical to iter 14 — NO new features added)
  feature_set_delta_vs_iter14: zero columns added/removed
architecture:
  pattern: per-class JRA model split by kyoso_joken_code
  classes: ["005", "010", "016", "703", "701", "other"]
  per_class_routing: each race predicted by the class-specific model based on kyoso_joken_code
  fallback: "other" model handles races without a primary class match
training:
  arch: catboost (YetiRank loss, NDCG:top=3 eval) — same as iter 14
  default_hyperparams: depth=8 / lr=0.05 / l2_leaf_reg=3.0 / iterations=1000 (iter 14 defaults verbatim)
  hpo_hyperparams: depth=5 / iterations=317 (from Optuna NSGA-II on 005 + 703)
  fold_count: 20 (2007..2026)
  train_start_year: 2006
  hpo:
    framework: Optuna NSGA-II
    trials_per_class: 20
    search_space: { depth: [3,7], iterations: [200,800], lr: continuous, l2_leaf_reg: continuous }
    cv_folds: [2022, 2024, 2025]
    classes_tested: ["005", "703"]
metrics:
  wf_21y_common_races_vs_iter14: 66964
  per_class_default_hyperparams_top1_delta_pp:
    "005": { label: "1勝クラス", n_races: 19744, delta_pp: -1.266 }
    "010": { label: "2勝クラス", n_races: 8653,  delta_pp: -2.057 }
    "016": { label: "3勝クラス", n_races: 3584,  delta_pp: -3.181 }
    "703": { label: "未勝利",   n_races: 23953, delta_pp: -0.948 }
    "701": { label: "新馬",     n_races: 5474,  delta_pp: -1.242 }
    "other": { label: "other",  n_races: 5556,  delta_pp: -2.412 }
  per_class_hpo_top1_delta_pp:
    "005": { delta_pp: -0.962, hpo_recovery_pp: 0.304 }
    "703": { delta_pp: -0.618, hpo_recovery_pp: 0.330, place2_delta_pp: 0.025, place3_delta_pp: 0.029 }
  routed_aggregate_jra_vs_iter14:
    races: 66964
    top1_delta_pp: -1.450
    place2_delta_pp: -0.409
    place3_delta_pp: -0.237
    top3_box_delta_pp: -0.561
decision_gate_6_condition_jra:
  cond_a_no_regression_le_5bps_all_axes: false (all 4 axes regressed routed-aggregate)
  cond_b_two_strong_or_three_weak: false (0 strong, 0 weak — all axes negative routed-aggregate)
  cond_c_place_led_signal: false (no positive axis routed-aggregate)
  cond_d_bucket_worst_regression_le_2pp: true (placeholder — deferred)
  cond_e_quality_green: true (lint 0w / coverage >=95% / format clean / tsc 0 errors)
  cond_f_per_grade_main_le_05pp_light_le_03pp: false (every class restricted-subset delta exceeds threshold under default hyperparams; HPO closes ~25-35% gap but insufficient)
  positive_metric_set: []
  decision: REJECT
  reason: "routed-aggregate top1 -1.450pp; all 6 classes regressed on own subset; HPO recovers only +0.304pp (005) / +0.330pp (703) — cross-class signal loss dominates"
hypothesis_falsified:
  predicted: "per-class models with class-specific optimization extract more within-class signal than a global model averaged across classes"
  observed: "every per-class model regressed on its own subset (016 worst at -3.18pp; 703 best at -0.95pp); HPO closes only 25-35% of the gap; routed-aggregate top1 -1.45pp"
  conclusion: "per-class architecture LOSES cross-class signal sharing implicit in the global iter 14's 241 features (e.g. a 005 race horse's history at 703 informs its 005 prediction); regression is inversely proportional to per-class sample size — classic overfitting on smaller subsets with the iter 14 depth=8/iter=1000 capacity"
bug_fixed_during_evaluation:
  description: "compute_iter20_metrics_and_decision.py::perclass_candidate_root had hardcoded model_version path; --candidate-model-version override was cosmetic only"
  symptom: "pre-fix HPO numbers were bit-identical to default-hyperparam runs (smoking gun)"
  fix: "added optional model_version parameter to perclass_candidate_root; HPO predictions now read from the correct directory"
  post_fix_results: { "005": -0.962, "703": -0.618 }
infrastructure_built_gitignored_tmp:
  parameterized_trainer: tmp/v8/iter20_train_predict_per_class.py
  hpo_driver: tmp/v8/tune_jra_cb_per_class.py
  evaluation_script: tmp/v8/compute_iter20_metrics_and_decision.py
  loop_runner: tmp/v8/run_per_class_loop.sh
  reusable_for: future per-class signals (sectional times, breeding × class, hierarchical transfer learning)
artifacts:
  decision: tmp/v8/iter20-decision.json (gitignored)
  per_class_train_summary: tmp/v8/iter20-per-class-train-summary.json (gitignored)
  per_class_metrics: tmp/v8/iter20-per-class-metrics.json (gitignored)
  routed_aggregate_metrics: tmp/v8/iter20-routed-aggregate-metrics.json (gitignored)
  predictions: tmp/bucket-eval/finish-position/iter20-jra-cb-per-class-v8/predictions/... (gitignored)
  model_artifacts: apps/finish-position-predict-container/models/finish-position/jra/per-class/* (gitignored)
six_consecutive_reject:
  count: 6 (iter 15 / iter 16 / iter 17 / iter 18 / iter 19 / iter 20)
  pattern: "iter 14 saturation extends to per-class architecture pivot — single-model and per-class regimes both bounded by the existing 241-feature signal envelope"
  next_directions:
    - "new horse-level signals that ARE class-specific (breeding × class, pace style × class interactions)"
    - "hierarchical / transfer learning starting from iter 14 weights, fine-tune per class"
    - "per-class feature SELECTION (not addition) — prune iter 14 features that don't dominate per class"
final_production_state:
  jra: iter14-jra-cb-pacestyle-course-v8 (Phase 2 cutover, commit ca5a8ff — unchanged)
  nar: iter12-nar-xgb-hpo-v8 (Phase 1 production — unchanged since iter 12 accept)
  banei: banei-cb-v7-lineage-wf-21y (unchanged)
---

# Iter 20: Per-class model architecture — REJECT (all 6 classes)

## Summary

iter 20 pivoted away from global-model tweaks (iter 15-19 all rejected) and tested a fundamentally different architecture: **split the JRA model by `kyoso_joken_code`** into 6 per-class specialists (005 1勝クラス / 010 2勝クラス / 016 3勝クラス / 703 未勝利 / 701 新馬 / other). The hypothesis was that per-class HPO and per-class fits would enable easier optimization than the global iter 14 model averaged across all classes.

**All 6 per-class models regressed on their own subsets** vs the iter 14 baseline restricted to the same race subset. The magnitude of regression is **inversely proportional to per-class sample size**: 016 (smallest, n=3,584) is worst at -3.18pp top1; 703 (largest, n=23,953) is best at -0.95pp. Optuna HPO on the 2 largest classes (005 + 703) found smaller models (depth=5, iter=317 vs default depth=8, iter=1000) and recovered ~25-35% of the gap — but the absolute level is still negative (005 HPO -0.962pp, 703 HPO -0.618pp). The routed-aggregate over all 6 per-class default models on 66,964 common races is **top1 -1.450pp / place2 -0.409pp / place3 -0.237pp / top3_box -0.561pp** — every axis negative. **Decision: REJECT.**

This is the **6th consecutive reject** (iter 15 / 16 / 17 / 18 / 19 / 20). The per-class architecture pivot, expected to break the iter 14 saturation ceiling, was itself rejected. Production state stays at iter 14 (JRA) + iter 12 (NAR) per Phase 2 cutover (commit `ca5a8ff`).

## Hypothesis

After 5 consecutive rejects (iter 15 calibration / iter 16 booster-deep / iter 17 NAR cross-features / iter 18 class signals / iter 19 sample weighting) confirmed single-model saturation on the iter 14 feature set, the natural pivot was **architectural rather than parametric**:

1. **Split the global JRA model into per-class specialists** by `kyoso_joken_code` (005 / 010 / 016 / 703 / 701 / other).
2. **Optimize each independently** — per-class HPO, per-class depth/iteration count, per-class loss configuration if needed.
3. **Expected outcome**: each per-class model, freed from cross-class loss averaging, extracts more within-class signal than the global model's per-class slice. Per-class top1 should rise on each class's own subset; routed-aggregate should beat global iter 14.

This represented a **scope expansion** beyond the iterative loop — new training pipeline (per-class fits × per-class HPO), new evaluation surface (per-class restricted metrics + routed-aggregate), new deployment routing.

## Implementation summary

### Phase A1: Parameterized infrastructure (gitignored, `tmp/v8/`)

- `iter20_train_predict_per_class.py` — parameterized per-class CB YetiRank trainer (20 WF folds 2007-2026, 241 features identical to iter 14)
- `tune_jra_cb_per_class.py` — Optuna NSGA-II HPO driver, 20 trials per class, search space depth [3,7] × iterations [200,800] × continuous lr / l2_leaf_reg, CV on years 2022/2024/2025
- `compute_iter20_metrics_and_decision.py` — Mode-A (per-class restricted metric) / Mode-B (routed-aggregate) / Mode-C (HPO override) evaluator
- `run_per_class_loop.sh` — orchestrates per-class train → predict → eval sweep

All Phase A1 artifacts live in `tmp/v8/` (gitignored). No tracked source files were modified.

### Phase A2: Default-hyperparam per-class training

All 6 classes trained with iter 14 hyperparams verbatim (depth=8, lr=0.05, l2=3.0, iterations=1000) using the 241-feature iter 14 set on 20 WF folds 2007-2026. Per-class sample sizes (restricted to that class's race subset within the 66,964 common races):

| Class | Label     | n_races |
| ----- | --------- | ------- |
| 005   | 1勝クラス | 19,744  |
| 010   | 2勝クラス | 8,653   |
| 016   | 3勝クラス | 3,584   |
| 703   | 未勝利    | 23,953  |
| 701   | 新馬      | 5,474   |
| other | other     | 5,556   |

### Phase A2.5: HPO sweep on 005 + 703 (2 largest classes)

To test whether default hyperparameters were the bottleneck, ran Optuna NSGA-II on the two largest sample-size classes (005 and 703). Best hyperparams found: depth=5, iterations=317 (vs default depth=8, iterations=1000) — substantially smaller models, consistent with smaller per-class subsets needing less capacity. Then retrained 005 and 703 on these HPO params and re-evaluated.

## Result vs iter 14 baseline (restricted to each class's race subset)

### Per-class top1 delta

| Class           | n_races | Default (depth=8, iter=1000) | HPO (depth=5, iter=317) | HPO improvement | Decision                                      |
| --------------- | ------- | ---------------------------- | ----------------------- | --------------- | --------------------------------------------- |
| 005 (1勝クラス) | 19,744  | **-1.266pp**                 | **-0.962pp**            | +0.304pp        | REJECT                                        |
| 010 (2勝クラス) | 8,653   | **-2.057pp**                 | (not tested)            | n/a             | REJECT                                        |
| 016 (3勝クラス) | 3,584   | **-3.181pp**                 | (not tested)            | n/a             | REJECT                                        |
| 703 (未勝利)    | 23,953  | **-0.948pp**                 | **-0.618pp**            | +0.330pp        | REJECT (place2/3 small positives — see below) |
| 701 (新馬)      | 5,474   | **-1.242pp**                 | (not tested)            | n/a             | REJECT                                        |
| other           | 5,556   | **-2.412pp**                 | (not tested)            | n/a             | REJECT                                        |

### Routed-aggregate (all 6 per-class default models routed by kyoso_joken_code, 66,964 races)

| Metric   | delta vs iter 14 global |
| -------- | ----------------------- |
| top1     | **-1.450pp**            |
| place2   | **-0.409pp**            |
| place3   | **-0.237pp**            |
| top3_box | **-0.561pp**            |

Every axis negative; magnitude well beyond the -0.05pp accept tolerance. **Decision: REJECT.**

### 6-condition accept gate (routed-aggregate)

- (a) `all delta >= -0.05pp` → **FAIL** (all 4 axes regressed)
- (b) `>=2 strong (>+0.05) OR >=3 weak (>+0.03)` → **FAIL** (0 strong, 0 weak)
- (c) `(top1 AND place2 positive) OR place-led` → **FAIL** (no positive axis)
- (d) `worst bucket Wilson LB >= -2.0pp` → **PASS (placeholder)** — deferred
- (e) `quality green` → **PASS** (lint 0w / coverage >=95% / format clean / tsc 0 errors)
- (f) `per-class main >= -0.5pp / light >= -0.3pp` → **FAIL** (every per-class delta exceeds threshold under default hyperparams; HPO closes ~25-35% gap but absolute level still below threshold for 005 / 703)

## Why per-class with iter 14 hyperparams failed

The default-hyperparam per-class regression is **inversely proportional to sample size** — a textbook overfitting signature:

| Class | n_races | Default top1 delta_pp |
| ----- | ------- | --------------------- |
| 016   | 3,584   | **-3.181** (worst)    |
| 010   | 8,653   | **-2.057**            |
| other | 5,556   | **-2.412**            |
| 701   | 5,474   | **-1.242**            |
| 005   | 19,744  | **-1.266**            |
| 703   | 23,953  | **-0.948** (best)     |

The CB YetiRank model at depth=8 / iterations=1000 is calibrated for the **full iter 14 cohort** (~1 million rows). Restricting to a per-class subset (3,584-23,953 races each) reduces effective training data by 4-20× while keeping model capacity constant — classic overfitting on smaller subsets. The smallest class (016) regresses worst by ~3.2pp; the largest (703) regresses least by ~0.95pp.

## Why HPO did not save per-class

HPO on 005 + 703 found substantially smaller models (depth=5, iterations=317 vs default depth=8, iterations=1000), confirming capacity overshoot:

- **005**: default -1.266pp → HPO -0.962pp (**+0.304pp recovery**, still ~95% of the gap remains)
- **703**: default -0.948pp → HPO -0.618pp (**+0.330pp recovery**, still ~65% of the gap remains)

HPO recovered roughly **25-35% of the regression gap**, not enough to flip the decision. Notably, **703 HPO showed small positive deltas on place2 (+0.025pp) and place3 (+0.029pp)** — the user's emphasized place metrics. But top1 still dominates the accept gate at -0.618pp, so 703 alone is also REJECT.

### Root cause hypothesis: cross-class signal loss

The global iter 14 model implicitly uses **cross-class signal sharing** via the 241 features. For example:

- A 005-class horse's history at 703-class races informs its 005 prediction (the 241 features carry that history regardless of current race class).
- Trainer / jockey / breeding features generalize across class boundaries (a trainer who excels at 010 also tells you about their 005 horses).
- Pacestyle and course-conditioning features are class-agnostic and contribute jointly to all class predictions.

When we split into per-class models, **each model only sees its own class's training rows** during gradient updates. Cross-class generalization signal is lost. Per-class HPO recovers a fraction by right-sizing model capacity, but cannot restore the lost cross-class information content.

This is consistent with the iter 15-19 saturation pattern: **the binding constraint is signal availability, not capacity allocation**. Per-class architecture didn't add signal — it subtracted cross-class signal sharing while only partially compensating via per-class capacity tuning.

## Bug fixed during evaluation

`compute_iter20_metrics_and_decision.py::perclass_candidate_root` had a **hardcoded model_version path** — the `--candidate-model-version` CLI override was cosmetic only (the output JSON's `model_version` field changed, but the predictions were read from the wrong directory).

**Symptom (smoking gun)**: pre-fix HPO numbers (-1.266pp / -0.948pp) were **bit-identical to default-hyperparam runs**, which is statistically impossible for genuinely different model fits.

**Fix**: added an optional `model_version` parameter to `perclass_candidate_root`; HPO predictions now read from the correct directory.

**Post-fix HPO results**: 005 -0.962pp / 703 -0.618pp (~25-35% recovery vs default). These reflect actual HPO model predictions and are reported as the final HPO numbers above.

## Infrastructure value (positive outcome despite REJECT)

Even though iter 20 was rejected, the per-class infrastructure built in Phase A1 is **reusable and not dead code**:

- **Parameterized per-class trainer** (`iter20_train_predict_per_class.py`) — generic over class code; can train any kyoso_joken_code subset with arbitrary hyperparameters.
- **HPO driver** (`tune_jra_cb_per_class.py`) — Optuna NSGA-II loop with per-class CV, ready to apply to new feature sets or new class definitions.
- **Mode-A / B / C evaluator** (`compute_iter20_metrics_and_decision.py`) — supports per-class restricted metric, routed-aggregate, and HPO-override modes.

These can be applied with future per-class horse-level signals:

- **Sectional times** (rapping_time / 3F / 1F) — per-class pace × style finer granularity.
- **Class-specific feature engineering** — breeding effectiveness × class, jockey × class win-rate.
- **Hierarchical / transfer learning** scaffolds — fine-tune per class starting from iter 14 weights.

The infrastructure just doesn't help with iter 14's **existing** 241 features.

## What would actually help (future directions)

The 6 consecutive reject pattern indicates the iter 14 feature envelope is fully saturated for both single-model and per-class regimes. Genuinely new signal axes are required:

1. **New horse-level signals that ARE class-specific** — breeding effectiveness × class, pace style × class interactions, trainer specialization × class (similar to the iter 19 hypothesis but applied as fresh features, not loss reweighting).
2. **Hierarchical / transfer learning** — start from iter 14 weights, fine-tune per class. This preserves cross-class signal sharing in the base weights while allowing per-class refinement on top, avoiding the cross-class signal loss observed in iter 20.
3. **Per-class feature SELECTION (not addition)** — identify which subset of iter 14's 241 features dominate per class (e.g., via SHAP per-class importance) and prune the others. Might reduce overfitting on smaller subsets without losing dominant signal. Lower-risk than full retraining.

None of these are pursued in this loop; they would require a new scoped task with explicit user authorization.

## v8 loop status: 6 consecutive REJECT

| iter | lever                                                       | category | outcome | failure mode                                                       |
| ---- | ----------------------------------------------------------- | -------- | ------- | ------------------------------------------------------------------ |
| 15   | L4 calibration (isotonic + Platt stacking)                  | JRA      | reject  | post-hoc calibration doesn't recover regression                    |
| 16   | L5A booster-deep on iter 14 base                            | JRA      | reject  | deeper trees don't extract more iter 14 signal                     |
| 17   | L5D bataiju × barei × kyori top-3 on iter 12 NAR-HPO base   | NAR      | reject  | physiological cross-features don't add NAR signal                  |
| 18   | L1B class-signals 3-feat conservative on iter 14 base       | JRA      | reject  | 3 orthogonal class features don't escape saturation                |
| 19   | L4-class sample weight α=1.4 on 005/010/016 on iter 14 base | JRA      | reject  | capacity reallocation hurts the classes it boosts                  |
| 20   | L-per-class-architecture on iter 14 base (6 classes + HPO)  | JRA      | reject  | per-class loses cross-class signal sharing; HPO recovers ~30% only |

The pattern is consistent across both **parametric levers (iter 15/16/19)** and **structural levers (iter 18/20)**: every lever that stays within the iter 14 feature envelope (241 columns) hits the same saturation ceiling. **New horse-level signal** is the only remaining axis.

## Final production state (unchanged)

- **JRA**: `iter14-jra-cb-pacestyle-course-v8` (Phase 2 cutover, commit `ca5a8ff` — unchanged).
- **NAR**: `iter12-nar-xgb-hpo-v8` (Phase 1 production, unchanged since iter 12 accept).
- **Ban-ei**: `banei-cb-v7-lineage-wf-21y` (unchanged).
- `last_iter_id`: 19 → 20
- `current_baseline_jra`: `iter14-jra-cb-pacestyle-course-v8` (unchanged)
- `current_baseline_nar`: `iter12-nar-xgb-hpo-v8` (unchanged)
- `accept_count`: 3 → 3 (no change)
- `reject_count`: 17 → 18
- `consecutive_reject_count`: 5 → 6 — **per-class architecture pivot also rejected**
- `loop_status`: paused / pivoted at iter 19 → 6 consecutive reject at iter 20; no further iterations attempted under current feature envelope
