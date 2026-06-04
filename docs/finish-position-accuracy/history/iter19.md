---
iteration: 19
date: 2026-06-05T01:30:00+09:00
based_on_iteration: 14
lever: L4-class-sample-weight-on-iter14-base
status: rejected (JRA) — hypothesis falsified
quality_gate: passed
loop_status: pivoted (5 consecutive reject iter 15/16/17/18/19 → per-class model architecture as separate task)
model_version_jra: iter14-jra-cb-pacestyle-course-v8 (UNCHANGED — iter 19 rejected)
model_version_nar: iter12-nar-xgb-hpo-v8 (UNCHANGED)
baselines:
  jra_primary: iter14-jra-cb-pacestyle-course-v8 (current Phase 2 production)
  jra_v7_reference: jra-cb-v7-lineage-wf-21y (absolute pre-v8 anchor)
  nar_primary: iter12-nar-xgb-hpo-v8 (current Phase 1 production, unchanged)
features:
  base_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course (iter 14 JRA course base, REUSED verbatim)
  output_root: tmp/bucket-eval/finish-position/iter19-jra-cb-l4class-v8 (gitignored)
  effective_feature_count: 241 (identical to iter 14 — NO new features added)
  feature_set_delta_vs_iter14: zero columns added/removed
sample_weight_lever:
  alpha: 1.4
  boosted_codes: ["005", "010", "016"]   # 1勝クラス / 2勝クラス / 3勝クラス
  weight_function: alpha × time_decay (multiplicative on existing time-decay weights)
  pg_join_path: Option B — JOIN kyoso_joken_code from PG at training time (no parquet rewrite)
  total_boosted_rows_by_fold_end: 458,182 (sum across all 20 training cohorts)
training:
  arch: catboost (YetiRank loss, NDCG:top=3 eval) — same as iter 14
  hyperparams: depth=8 / lr=0.05 / l2_leaf_reg=3.0 / iterations=1000 (iter 14 defaults verbatim)
  random_seed_base: 42 (+ fold_year stabilization)
  fold_count: 20 (2007..2026)
  train_start_year: 2006
  best_iter_avg: ~263 (min 131 fold_2010, max 461 fold_2020)
  total_train_sec: ~573 (~9.5 min wall, 20 folds sequential)
metrics:
  wf_21y_common_races_vs_iter14: 66964
  jra:
    baseline_iter14:
      { races: 66964, top1: 0.40302, place2: 0.21806, place3: 0.16239, top3_box: 0.14351 }
    iter19: { races: 66964, top1: 0.39889, place2: 0.21636, place3: 0.16086, top3_box: 0.14106 }
    delta_pp_vs_iter14: { top1: -0.414, place2: -0.170, place3: -0.152, top3_box: -0.245 }
  per_grade_jra:
    "005": { label: "1勝クラス", n_races: 19744, baseline_top1: 0.37829, iter19_top1: 0.37470, delta_pp: -0.360, wilson_lb_pp: -1.315 }   # BOOSTED
    "010": { label: "2勝クラス", n_races: 8653,  baseline_top1: 0.37894, iter19_top1: 0.37351, delta_pp: -0.543, wilson_lb_pp: -1.987 }   # BOOSTED
    "016": { label: "3勝クラス", n_races: 3584,  baseline_top1: 0.34235, iter19_top1: 0.33510, delta_pp: -0.725, wilson_lb_pp: -2.917 }   # BOOSTED
    "703": { label: "未勝利",   n_races: 23953, baseline_top1: 0.44091, iter19_top1: 0.43706, delta_pp: -0.384, wilson_lb_pp: -1.273 }   # REFERENCE
    "701": { label: "新馬",     n_races: 5474,  baseline_top1: 0.42565, iter19_top1: 0.42181, delta_pp: -0.384, wilson_lb_pp: -2.235 }   # REFERENCE
decision_gate_6_condition_jra:
  cond_a_no_regression_le_5bps_all_axes: false (all 4 axes regressed: top1 -0.414 / place2 -0.170 / place3 -0.152 / top3_box -0.245)
  cond_b_two_strong_or_three_weak: false (0 strong, 0 weak — all axes negative)
  cond_c_place_led_signal: false (no positive axis)
  cond_d_bucket_worst_regression_le_2pp: true (placeholder — deferred to TS evaluate-bucket-21y-v8.ts)
  cond_e_quality_green: true (lint 0w / coverage >=95% / format clean / tsc 0 errors)
  cond_f_per_grade_main_le_05pp_light_le_03pp: false (005 -0.36 borderline, 010 -0.54 EXCEEDS -0.5pp main, 016 -0.73 EXCEEDS -0.5pp main, 701/703 both -0.38 EXCEEDS -0.3pp light)
  positive_metric_set: []
  decision: REJECT
  reason: "gate_a failed (regression in [top1, place2, place3, top3_box]); gate_b failed (strong=0/2 weak=0/3); gate_c failed (place-led signal missing); gate_f failed (per-grade wilson lower bound regression on boosted main codes and reference light codes)"
hypothesis_falsified:
  predicted: "boost 005/010/016 sample weight 1.4× → those sub-classes improve at small global cost"
  observed: "boosted sub-classes regressed MORE than unboosted reference classes (005 -0.36 / 010 -0.54 / 016 -0.73 vs 701 -0.38 / 703 -0.38)"
  conclusion: "α=1.4 sample weight on saturated-signal classes ACTIVELY hurts the classes it boosts — the model cannot extract more signal from the boosted cohort, but loses generalization from de-weighted other races"
artifacts:
  decision: tmp/v8/iter19-decision.json (gitignored)
  train_summary: tmp/v8/iter19-train-summary.json (gitignored)
  metrics_global: tmp/v8/iter19-metrics-global.json (gitignored)
  per_grade_delta_csv: tmp/v8/iter19-jra-subclass-delta.csv (gitignored)
  predictions: tmp/bucket-eval/finish-position/iter19-jra-cb-l4class-v8/predictions/category=jra/race_year=*/predictions.parquet (gitignored)
  model_artifacts: apps/finish-position-predict-container/models/finish-position/jra/iter19-jra-cb-l4class-v8/ (gitignored)
five_consecutive_reject:
  count: 5 (iter 15 / iter 16 / iter 17 / iter 18 / iter 19)
  pattern: "lever bank exhausted within current feature × hyperparameter regime — single-model JRA saturation confirmed"
  pivot: "per-class model architecture (separate task) — split JRA by kyoso_joken_code, per-class HPO, per-class deployment routing"
final_production_state:
  jra: iter14-jra-cb-pacestyle-course-v8 (Phase 2 cutover, commit ca5a8ff — unchanged)
  nar: iter12-nar-xgb-hpo-v8 (Phase 1 production — unchanged since iter 12 accept)
  banei: banei-cb-v7-lineage-wf-21y (unchanged)
---

# Iter 19: L4-class sample weight α=1.4 on iter 14 base — REJECT

## Summary

iter 19 reused the iter 14 JRA course-feature parquet verbatim (241 features, **zero feature delta**, hyperparameters unchanged) and applied a **sample weight multiplier α=1.4** to the three 平場 classes (005 1勝クラス / 010 2勝クラス / 016 3勝クラス) at training time. `kyoso_joken_code` was JOINed from PG (Option B — no parquet rewrite). Twenty WF folds 2007-2026 retrained with the same YetiRank loss / NDCG:top=3 eval.

Global 4-metric WF 21y outcome: **all 4 axes regressed** — top1 -0.414pp, place2 -0.170pp, place3 -0.152pp, top3_box -0.245pp vs iter 14 (66,964 common races). Per-grade outcome contradicts the hypothesis: **the boosted classes (005/010/016) regressed MORE than the unboosted reference classes (703/701)** — 005 -0.36pp / 010 -0.54pp / 016 -0.73pp on boosted vs 703 -0.38pp / 701 -0.38pp on reference. **Decision: REJECT.**

This is the **5th consecutive reject** (iter 15 / 16 / 17 / 18 / 19) on the iter 14 base. Sample weighting falsifies cleanly: the saturation constraint is **signal availability**, not capacity allocation. User has pivoted to **per-class model architecture design as a separate task**, so iter 20 Optuna HPO (planned next sweep) is shelved. Production state unchanged.

## Hypothesis

After iter 15-18 demonstrated that single-feature/single-hyperparameter levers can't escape iter 14 saturation, the natural next move was **capacity reallocation rather than capacity expansion**: keep the feature set and hyperparameters identical, but tilt the loss to focus on the under-performing 平場 sub-classes (005 / 010 / 016). The hypothesis:

1. **005/010/016 sub-classes have lower iter 14 top1** (~37-34%) than 未勝利/新馬 (~44-43%), suggesting the model "leaves accuracy on the table" for these grades.
2. **A modest α=1.4 boost** (well short of class-balancing) should let the model spend more training capacity on the under-performing cohort.
3. **Expected outcome**: boosted classes improve by some amount, reference classes degrade by a smaller amount, net global ≈ 0 or slightly positive on the place-led axis.

The conservative α=1.4 (not 2.0+) was deliberate to limit the de-weighting penalty on the rest of the data.

## Implementation summary

- **Scaffolds** (gitignored, scratch): `tmp/v8/iter19_*.py` orchestrators.
- **Feature set**: identical to iter 14 — `apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course` reused without modification. 241 features per row.
- **Class boost mechanism**: at train time, multiply existing time-decay sample weights by `α=1.4` for any row whose `kyoso_joken_code ∈ {005, 010, 016}`.
- **`kyoso_joken_code` lookup**: Option B — JOIN `jvd_ra.kyoso_joken_code` from PG at training time (keyed on `race_id`), no parquet rewrite, no feature column added. Boosted-row counts per fold grow monotonically from 23,954 (fold 2007) to 458,182 (fold 2026) as the training window extends.
- **Hyperparameters**: depth=8 / lr=0.05 / l2_leaf_reg=3.0 / iterations=1000 — iter 14 defaults verbatim, no retune.
- **Training**: CB YetiRank, 20 folds 2007-2026, sequential. Best iteration averages ~263 (min 131 fold 2010, max 461 fold 2020) — comparable range to iter 14's ~250.
- **Total training time**: ~573 seconds (~9.5 min wall).

No new test files were required because the lever lives entirely in `tmp/v8/iter19_*` orchestrators (gitignored). The TS/Python source tree is unchanged.

## Result vs iter 14 baseline (66,964 common races, 20 WF folds 2007-2026)

### Global 4-metric

| Metric   | iter 14 | iter 19 | delta_pp   |
| -------- | ------- | ------- | ---------- |
| top1     | 40.302% | 39.889% | **-0.414** |
| place2   | 21.806% | 21.636% | **-0.170** |
| place3   | 16.239% | 16.086% | **-0.152** |
| top3_box | 14.351% | 14.106% | **-0.245** |

### Per-grade slice (boosted vs reference)

| kyoso_joken_code | label     | role      | n_races | iter 14 top1 | iter 19 top1 | delta_pp   | Wilson LB pp |
| ---------------- | --------- | --------- | ------- | ------------ | ------------ | ---------- | ------------ |
| 005              | 1勝クラス | BOOSTED   | 19,744  | 37.829%      | 37.470%      | **-0.360** | -1.315       |
| 010              | 2勝クラス | BOOSTED   | 8,653   | 37.894%      | 37.351%      | **-0.543** | -1.987       |
| 016              | 3勝クラス | BOOSTED   | 3,584   | 34.235%      | 33.510%      | **-0.725** | -2.917       |
| 703              | 未勝利    | REFERENCE | 23,953  | 44.091%      | 43.706%      | **-0.384** | -1.273       |
| 701              | 新馬      | REFERENCE | 5,474   | 42.565%      | 42.181%      | **-0.384** | -2.235       |

**Critical observation**: 010 (-0.54) and 016 (-0.73) — the two classes most aggressively boosted — regressed substantially MORE than the unboosted 703/701 (-0.38 each). 005 (-0.36) is essentially tied with the reference classes despite being boosted. The α=1.4 multiplier directly hurt the cohort it targeted.

### 6-condition accept gate

- (a) `all delta >= -0.05pp` → **FAIL** (all 4 axes regressed below -0.05pp tolerance)
- (b) `>=2 strong (>+0.05) OR >=3 weak (>+0.03)` → **FAIL** (0 positive axes)
- (c) `(top1 AND place2 positive) OR place-led` → **FAIL** (no positive axis)
- (d) `worst bucket Wilson LB >= -2.0pp` → **PASS (placeholder)** — deferred to TS `evaluate-bucket-21y-v8.ts` post-train
- (e) `quality green` → **PASS** (lint 0w / coverage >=95% / format clean / tsc 0 errors)
- (f) `005/010/016 LB >= -0.5pp AND 703/701 >= -0.3pp` → **FAIL** (010 -0.543 / 016 -0.725 exceed main -0.5pp threshold; 701/703 both -0.384 exceed light -0.3pp threshold)

**Decision: REJECT** — gates (a)/(b)/(c)/(f) all fail; the per-grade gate (f) failure is especially diagnostic because it shows the BOOSTED main codes regressed worse than the threshold.

## Why iter 19 lost — hypothesis falsified

The naïve mental model behind α=1.4 was: "the model has untapped capacity that it spends on easy 未勝利/新馬 races; if I redirect a fraction of that capacity to the under-performing 平場 races, those races get more accurate." Both halves of this model are wrong:

1. **Target classes regressed MORE than reference classes.** If the boost had worked as theorized, 005/010/016 should have improved (or at worst stayed flat) while 703/701 paid the cost. The actual ordering is **016 (-0.73) > 010 (-0.54) > 701/703 (-0.38) ≈ 005 (-0.36)**. The two most aggressively up-weighted classes (010, 016) ate the largest top1 hits.
2. **α=1.4 boost ACTIVELY hurt the classes it was trying to help.** This is not a side effect — it's the headline finding. Up-weighting saturated-signal rows makes the gradient signal noisier on those rows (because there is no more learnable structure to extract), while simultaneously removing relative weight from the rest of the data where the model still had cross-class generalization to learn from. Both sides lose.
3. **Signal — not capacity — is the binding constraint.** The v7-lineage CB + iter 9 pacestyle + iter 14 course feature set has already extracted what it can extract from a single global model on the 平場 cohort. Telling the model to "try harder" on those rows is equivalent to telling it to fit residual noise.

## 5 consecutive reject lineage

| iter | lever                                                       | category | outcome | falsified hypothesis                          |
| ---- | ----------------------------------------------------------- | -------- | ------- | --------------------------------------------- |
| 15   | L4 calibration (isotonic + Platt stacking)                  | JRA      | reject  | post-hoc calibration recovers regression      |
| 16   | L5A booster-deep on iter 14 base                            | JRA      | reject  | deeper trees extract more iter 14 signal      |
| 17   | L5D bataiju × barei × kyori top-3 on iter 12 NAR-HPO base   | NAR      | reject  | physiological cross-features add NAR signal   |
| 18   | L1B class-signals 3-feat conservative on iter 14 base       | JRA      | reject  | 3 orthogonal class features escape saturation |
| 19   | L4-class sample weight α=1.4 on 005/010/016 on iter 14 base | JRA      | reject  | capacity reallocation helps target classes    |

The pattern is consistent: **every lever that stays within {iter 14 feature set ∪ near-neighbor hyperparameters} hits saturation**. iter 15 attacked calibration, iter 16 attacked tree depth, iter 17 attacked NAR feature stacking, iter 18 attacked feature injection, iter 19 attacked loss weighting — none escaped.

## Why sample weight didn't work (analytical)

Sample weighting only helps when the model has **unused capacity on the up-weighted rows that the original loss did not budget for**. For iter 14's CB YetiRank loss with 1000 iterations at depth 8, the relevant question is: was the model under-fitting the 平場 cohort? The answer from iter 19's per-grade regression is clearly **no**:

- If iter 14 were under-fitting 005/010/016, then α=1.4 would let the model spend more iterations carving partitions that distinguish 平場 horses → top1 on those classes would rise.
- What we observe instead is that **boosted classes regressed worse than reference classes**, which means α=1.4 pushed the model from "fitting available structure" into "fitting noise" on those rows.

In other words, the v7-lineage CB + iter 9 pacestyle + iter 14 course features have **already extracted the available signal** from 005/010/016 races. Rebalancing sample weight TOWARDS those rows means:

1. The model has **less effective data** to refine its 005/010/016 predictions (because cross-cohort generalization is now weighted lower).
2. The model has **no additional signal** to learn from the boosted rows themselves (saturation).

Net effect: both sides lose. This is a **backward effect** — the lever made the targeted cohort worse, not just net-zero.

The corollary is that **any single-model lever within the existing feature × hyperparameter regime is unlikely to escape this saturation**. We need either (a) genuinely new horse-level signal that the existing 241 features don't carry (iter 18 tried and failed because the 3 chosen signals overlapped with v7-lineage), or (b) **per-class specialization** — separate model fits, per-class HPO, per-class feature selection — so each class can extract whatever within-class signal exists without being averaged against other classes.

## Pivot to per-class model architecture

iter 20 Optuna HPO was the next planned sweep, but user has **shelved it** in favor of a **per-class model architecture redesign as a separate task** outside this iterative loop. The motivation:

1. **Single-model saturation is confirmed** by 5 consecutive reject covering 5 distinct lever classes (calibration / capacity / cross-feature / new-signal-injection / loss-weighting).
2. **Per-class HPO** would tune depth / lr / l2 / iterations / loss type per kyoso_joken_code, giving each class the structure best matched to its sample size and signal-to-noise ratio.
3. **Per-class deployment routing** would predict each race with the class-specific model, with a fallback to the global model only when class metadata is missing.

This is a **scope expansion** (new code surface for routing + per-class training pipelines + per-class evaluation) so it is being handled as a separate task. The v8 finish-position iterative loop is **paused at iter 19 reject**; no iter 20 attempt under the current single-model regime.

## Final production state (post-iter 19)

- **JRA**: `iter14-jra-cb-pacestyle-course-v8` (Phase 2 cutover, commit `ca5a8ff` — unchanged).
- **NAR**: `iter12-nar-xgb-hpo-v8` (Phase 1 production, unchanged since iter 12 accept).
- **Ban-ei**: `banei-cb-v7-lineage-wf-21y` (unchanged).
- `last_iter_id`: 18 → 19
- `current_baseline_jra`: `iter14-jra-cb-pacestyle-course-v8` (unchanged)
- `current_baseline_nar`: `iter12-nar-xgb-hpo-v8` (unchanged)
- `accept_count`: 3 → 3 (no change)
- `reject_count`: 16 → 17
- `consecutive_reject_count`: 4 → 5 → **loop pivot to per-class architecture (separate task)**
- `loop_status`: terminated (iter 18 S1) → paused / pivoted at iter 19

## Future work hooks (deferred to per-class task)

These belong to the **per-class architecture task**, not loop continuation:

1. **Per-class CB train+HPO**: separate Optuna study per kyoso_joken_code (005 / 010 / 016 / 701 / 703 at minimum), with class-specific search spaces.
2. **Class-routing inference layer**: container-side dispatch by `kyoso_joken_code` to the right specialist model, with a fallback path to iter 14 when class metadata is unavailable.
3. **Per-class evaluation surface**: `evaluate-bucket-21y-v8.ts` extension to slice by kyoso_joken_code natively and report Wilson LB per (class × bucket).
4. **Cross-class regularization**: investigate whether shared embedding layer + per-class head architecture (e.g. multi-task GBDT-equivalent) outperforms fully separate fits — a likely follow-up after the per-class baseline is established.
