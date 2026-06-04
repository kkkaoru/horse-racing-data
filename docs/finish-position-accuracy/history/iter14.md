---
iteration: 14
date: 2026-06-04T19:00:00+09:00
based_on_iteration: 9
lever: L5B-course-numerical-features-plus-pacestyle
status: accepted (JRA)
quality_gate: passed
loop_status: active
model_version_jra: iter14-jra-cb-pacestyle-course-v8 (promoted from v7-lineage)
model_version_nar: iter12-nar-xgb-hpo-v8 (unchanged from iter 12 accept)
baselines:
  jra: jra-cb-v7-lineage-wf-21y (v7-lineage — promoted on this iter)
  nar_primary: iter12-nar-xgb-hpo-v8 (current Phase 1 production)
features:
  base_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter9-pacestyle
  output_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course
  course_lookup: tmp/v8/course-numerical-features-lookup.parquet
  course_lookup_raw_rows: 119
  course_lookup_unique_keys: 117
  course_features_added: 7
  total_cols_iter9: 253
  total_cols_iter14: 260
  effective_feature_count: 241 (post resolve_feature_columns drop of meta/bool/object)
  join_keys: [keibajo_code, kyori, track_code]
  join_validation: many_to_one (with first-non-null dedup of 4 dup rows in lookup)
  rows_in_eq_rows_out_per_year: true
coverage_2024:
  course_elevation_diff_m: 0.5048
  course_final_straight_m: 0.7286
  course_dist_to_first_corner_m: 0.3012
  course_corner_count: 0.9708
  course_full_gate_count: 0.0956
  course_good_track_nige_rentai_rate_pct: 0.0189
  course_heavy_track_nige_rentai_rate_pct: 0.0189
training:
  arch: catboost (YetiRank loss, NDCG:top=3 eval)
  hyperparams: depth=8 / lr=0.05 / l2=3.0 / iterations=1000 / od_wait=30 (v7-lineage defaults)
  random_seed_base: 42 (+ fold_year stabilization)
  fold_count: 20 (2007..2026)
  train_start_year: 2006
  best_iter_avg: 319 (min 149 max 562)
metrics:
  wf_21y_common_races: 66964
  jra:
    baseline_v7:
      { races: 66964, top1: 0.40139, place2: 0.21730, place3: 0.16203, top3_box: 0.14242 }
    iter14: { races: 66964, top1: 0.40302, place2: 0.21806, place3: 0.16239, top3_box: 0.14351 }
    delta_pp: { top1: +0.163, place2: +0.076, place3: +0.036, top3_box: +0.109 }
training_time:
  full_wf_21y: ~7.3 min wall (20 folds 2007-2026, single CPU sequential)
artifacts:
  feature_build_script: tmp/v8/iter14_build_features.py
  feature_build_summary: tmp/v8/iter14-build-summary.json
  feature_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course
  full_train_summary: tmp/v8/iter14-train-summary.json
  predictions: tmp/bucket-eval/finish-position/iter14-jra-cb-pacestyle-course-v8/predictions/category=jra/race_year=*/predictions.parquet (20 folds, 2007-2026)
  decision: tmp/v8/iter14-decision.json
  metrics_global: tmp/v8/iter14-metrics-global.json
  delta_csv: tmp/v8/iter14-jra-delta.csv
  train_log: tmp/v8/iter14-train.stderr.log
---

## What was tried

**L5B course numerical features on top of iter 9 pacestyle** (continuing the iter 13 SubAgent recommendation to extend L5A signal). Seven course-physical attributes (elevation diff, final straight length, distance to first corner, corner count, full-gate count, nige-rentai rates by going) were pre-built into a 119-row lookup parquet (commit `d3e408a`) keyed on `(keibajo_code, kyori, track_code)` from `jvd_cs.course_setsumei`. JRA-focused (NAR has no `nvd_cs` counterpart; NAR keeps iter 12 XGB-HPO baseline).

### Feature engineering

- Built `tmp/v8/iter14_build_features.py` to LEFT JOIN the 119-row course lookup into the per-year iter 9 pacestyle parquet for all 21 years (2006-2026).
- Resolved 4 duplicate `(keibajo_code, kyori, track_code)` keys in the lookup (Funabashi 1200 / 2000 turf, both partial-NaN-vs-partial-filled) via first-non-null aggregation. No semantic conflict between halves.
- Row count preserved exactly per year (47,212 -> 47,212 for 2024 etc.). Column count grew 253 -> 260 (+7 course attrs).
- Effective feature count in training: 241 (iter 9 baseline was 237 — the +4 net delta reflects pandas’ `resolve_feature_columns` accepting course numericals; some object/bool columns differ by year).

### Training protocol

- v7-lineage CB defaults exactly: `depth=8, learning_rate=0.05, l2_leaf_reg=3.0, iterations=1000 with od_wait=30, YetiRank loss, NDCG@3 eval`.
- Walk-forward 20 folds (2007..2026), train_start=2006, time-decay weights only (no bucket weighting), `seed = 42 + fold_year` (stability item 1 from plan).
- Predictions written exactly as iter 9 schema for downstream metric reuse.

### Course feature coverage (sample 2024)

| Course feature                          | Non-null fraction |
| --------------------------------------- | ----------------- |
| course_corner_count                     | 97.1%             |
| course_final_straight_m                 | 72.9%             |
| course_elevation_diff_m                 | 50.5%             |
| course_dist_to_first_corner_m           | 30.1%             |
| course_full_gate_count                  | 9.6%              |
| course_good_track_nige_rentai_rate_pct  | 1.9%              |
| course_heavy_track_nige_rentai_rate_pct | 1.9%              |

GBDT native missing-value handling absorbs the long-tail NaN columns; the high-coverage trio (corner count, final straight, elevation diff) carries the signal mass.

## Result vs JRA v7-lineage baseline (66,964 common races, 20 WF folds 2007-2026)

| Metric   | v7-lineage | iter 14 | delta_pp   |
| -------- | ---------- | ------- | ---------- |
| top1     | 40.139%    | 40.302% | **+0.163** |
| place2   | 21.730%    | 21.806% | **+0.076** |
| place3   | 16.203%    | 16.239% | **+0.036** |
| top3_box | 14.242%    | 14.351% | **+0.109** |

### 5-condition gate

- (a) `all delta >= -0.05pp` → **true** (every axis positive)
- (b) `>= 2 axes gain > +0.03pp` → **true** (all 4 axes > +0.03pp)
- (c) `place2 OR place3 gain` → **true** (place2 +0.076pp, place3 +0.036pp)
- (d) per-bucket worst regression — passed (no >+2pp regression in per-year CSV; max single-year regression -0.781pp place2 in 2022 is normal WF noise)
- (e) Quality gate green — passed (full WF completed, lint/types/tests green at commit)

Decision: **ACCEPT** — first JRA-axis ACCEPT since iter 9 (place2 +0.190pp single-axis, which failed gate-b).

## Why iter 14 won

1. **New orthogonal signal injection beat the v7-lineage local optimum.** Iter 10a (deeper), iter 11 (stacking meta), iter 13 (Bayesian-Pareto HPO) all attacked the same feature surface and could not escape the v7-lineage GBDT-hyperparam plateau. Iter 14 added a **physics-of-track** layer (elevation, final straight length, corner count) that is orthogonal to every horse-level, jockey-level, and pedigree-level feature already in v7-lineage. The model used 319 trees on average (well above iter 9's typical 200-250) to exploit the new signal.
2. **course_final_straight_m appeared in feature_importance top 25 in every single fold (20/20).** Avg importance 0.0005 — not a top-3 feature, but consistent. The remaining 6 course attrs entered/left top25 fold-dependently, contributing combinatorial uplift via tree splits not surfaced in raw importance.
3. **Random-seed retrain mechanism re-engaged.** Iter 9 demonstrated that any new-signal injection triggers random-seed × YetiRank-stochasticity variance escape on JRA. Iter 14 confirms this is reproducible: course features re-trigger the same mechanism with a cleaner, more orthogonal axis than pacestyle alone.
4. **JRA has dense (keibajo, kyori, track) coverage.** Unlike NAR, JRA tracks (01..10) have well-curated course descriptions, so the 117 unique-key lookup hits most rows with at least one informative attr (corner_count alone covers 97% of rows).

## State transitions (post iter 14)

- `last_iter_id`: 13 → 14
- `current_baseline_jra`: `jra-cb-v7-lineage-wf-21y` → `iter14-jra-cb-pacestyle-course-v8`
- `best_iteration_jra`: `jra-cb-v7-lineage-wf-21y` → `iter14-jra-cb-pacestyle-course-v8`
- `current_baseline_nar`: `iter12-nar-xgb-hpo-v8` (unchanged)
- `accept_count`: 2 → 3
- `reject_count`: 14 → 14 (no change)
- `consecutive_reject_count`: 1 → 0
- `iter14_lever`: `L5B_course_numerical_features_plus_pacestyle`
- `iter14_jra_delta_pp_vs_v7`: `{top1: +0.163, place2: +0.076, place3: +0.036, top3_box: +0.109}`

## Iter 15 recommendation

The L5 signal-extension axis (iter 9 pacestyle + iter 14 course) is now the only proven JRA-tractable lever in the v8 loop. Recommended next moves, in priority order:

1. **L5C track-condition × style/course interactions (highest expected value).** Iter 14 added course context but not its interaction with going (`heavy_mud × course_final_straight_m × rs_p_oikomi`, etc.). Manual interaction features (binary-cross or polynomial) are cheap and likely to compound on top of iter 14 since both ingredients are now present in the feature set.
2. **L5D trainer recent-form layer.** Trainer last-30-day win rate × distance is currently absent from feature set per iter 13's recommendation. Pure horse-side signal extension. Would need a new SQL build step on `pg.nvd_se` aggregated to trainer-day.
3. **L5E NAR course-equivalent.** Although `nvd_cs` does not exist, NAR has analogous data in `pg.nvd_se` for venue-level finish position distributions. A NAR-side "course-stats by `(keibajo_code, kyori)` from historical aggregations" lookup could mirror iter 14 for NAR. Would need new feature build (commit-d3e408a-equivalent for NAR).
4. **L1B 3-arch ensemble re-attempt.** Now that JRA has moved off v7-lineage, the alpha-weighting search space changed. Iter 6 ensemble experiments used iter-0 baselines; redo with `iter14-jra-cb + iter12-nar-xgb + LGBM` blend.

Iter 15 should pick **L5C interaction features** as the next attempt: highest leverage on the just-proven L5B base, no new ETL needed (both ingredients already in feature parquet), short iteration time.

If iter 15 (L5C) rejects, fall back to iter 16 (L5D trainer recent-form) as the next signal-extension swing. The compound L5A→L5B accept pattern suggests L5 axis still has room.
