---
experiment: jra-lgb-lambdarank-full-judge
date: 2026-06-11
status: REJECT — NAR-C lgb-lambdarank residual does NOT transfer to JRA (multi-year holdout, all 4 axes negative on both powered classes)
follows: history/jra-lgb-lambdarank-transfer-guard.md
question: >
  After the single-test-year transfer guard PROCEEDed (703 test-2023 Δtop1 +0.733pp,
  005 test-2023 Δtop1 +0.428pp, no iter13 collapse), does the NAR-C lgb-lambdarank
  residual recipe survive a full walk-forward retrain with nested HPO and a powered
  judge (bootstrap LB95 + 4-axis no-regression gate + Holm) over JRA classes 703/005?
scope:
  category: jra
  classes_judged: [703, 005]
  classes_skipped: [010] # skipped after best-case 703 failed by wide margin
  classes_excluded: [016] # underpowered n≈727 per prior power analysis
  base_model: iter14-jra-cb-pacestyle-course-v8
  base_score_feature: iter14_score (per-fold leak-free predictions)
wf_design:
  type: expanding-window walk-forward
  holdout_years: [2023, 2024, 2025, 2026]
  per_year: "train 2007..y-1 chain-filtered; val y-1 target-only; holdout y target-only"
  nested_hpo:
    inner_train_end: 2020
    tuning_years: [2021, 2022]
    n_trials: 60 # Optuna TPE, n_jobs=1
    holdout_scored: ONCE (after HPO frozen)
  final_artifact_train: "2007-2025 chain-filtered"
recipe:
  lgb_objective: lambdarank
  lgb_metric: ndcg@3
  lgb_truncation_level: 3
  relevance_labels: "{1st→3, 2nd→2, 3rd→1, else→0}"
  group_weights: "time-decay 0.5→1.0 linear by race year"
  base_score_feature: "iter14_score as last feature (index 253 of 254)"
  feature_count: 254
  blend: rank_blend(w_base, w_lgb) — w_lgb optimized jointly with LGB params via Optuna
judge_gate:
  strengthened: true
  rule: "ADOPT iff LB95>0 AND all 4 axes >= -0.05pp AND >=2 axes point-positive AND Holm-corrected"
  bootstrap: "10k race-resample, seed 42, percentile 2.5"
  holm: "across classes × axes — no-op here (no class reaches significance)"
artifacts:
  judge_json: tmp/jra-lgb/full_retrain_judge.json
  retrain_script: tmp/jra-lgb/jra_lgb_bootstrap_only.py
  model_703: tmp/jra-lgb/models/jra_703_lgb_lambdarank.txt (best_iter=734)
  model_005: tmp/jra-lgb/models/jra_005_lgb_lambdarank.txt (best_iter=736)
  features_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter26-relationships (254 cols)
  iter14_preds_root: tmp/bucket-eval/finish-position/iter14-jra-cb-pacestyle-course-v8/predictions
---

## Verdict — REJECT at category level

The NAR-C lgb-lambdarank residual recipe **does not transfer to JRA**. On the multi-year
expanding-window holdout (2023–2026) with nested HPO, **all 4 axes are negative on both
powered classes**, and the bootstrap LB95 on the best (least-negative) axis is well below 0
for both. No ADOPT, no narrow-miss, no user-override candidate.

| class | n_races | top1 Δ  | place2 Δ | place3 Δ | top3_box Δ | LB95 (top3_box) | verdict |
| ----- | ------- | ------- | -------- | -------- | ---------- | --------------- | ------- |
| 703   | 4229    | −0.2365 | −1.1350  | −0.9695  | −0.1656    | **−0.4436**     | REJECT  |
| 005   | 3147    | −0.4766 | −2.5104  | −1.9384  | −0.3495    | **−0.8113**     | REJECT  |
| 010   | —       | —       | —        | —        | —          | —               | SKIPPED |

LB95 is computed on each class's least-negative axis (`top3_box`), i.e. the _most generous_
axis for the candidate. Even there, the 95% lower bound of the paired race-bootstrap delta
is firmly negative. Every other axis is worse. Holm correction is a no-op: no class reaches
even point-positive significance to correct.

### Class 703 (未勝利 — n=4229 races, holdout 2023–2026)

| axis     | base (iter14) | blend   | Δpp     |
| -------- | ------------- | ------- | ------- |
| top1     | 49.4207       | 49.1842 | −0.2365 |
| place2   | 69.7564       | 68.6214 | −1.1350 |
| place3   | 80.3027       | 79.3332 | −0.9695 |
| top3_box | 98.0374       | 97.8718 | −0.1656 |

HPO winner: `lr=0.0882, num_leaves=15, min_data_in_leaf=122, lambda_l2=0.144,
feature_fraction=0.723, bagging_fraction=0.944, max_depth=5, n_estimators=738`, blend
`w_lgb=0.8341 / w_base=0.1659`. Final model best_iter=734. Bootstrap LB95(top3_box)=−0.4436pp.

### Class 005 (1勝クラス — n=3147 races, holdout 2023–2026)

| axis     | base (iter14) | blend   | Δpp     |
| -------- | ------------- | ------- | ------- |
| top1     | 41.2774       | 40.8008 | −0.4766 |
| place2   | 61.5825       | 59.0721 | −2.5104 |
| place3   | 72.7995       | 70.8611 | −1.9384 |
| top3_box | 95.9644       | 95.6149 | −0.3495 |

HPO winner: `lr=0.1185, num_leaves=22, min_data_in_leaf=46, lambda_l2=1.534,
feature_fraction=0.467, bagging_fraction=0.767, max_depth=6, n_estimators=738`, blend
`w_lgb=0.8936 / w_base=0.1064`. Final model best_iter=736. Bootstrap LB95(top3_box)=−0.8113pp.

### Class 010 (2勝クラス) — SKIPPED

Deliberately skipped. The single best-powered JRA class (703, n=4229) failed by a wide
margin (LB95 −0.4436pp, all 4 axes negative), and 005 (n=3147) confirmed the identical
all-negative pattern even more strongly (LB95 −0.8113pp). There is no scenario in which a
lower-powered class flips the category-level conclusion of a hypothesis whose two best
cases both failed decisively. Retraining 010 would only burn compute and risk colliding
with the 03:00 JST finish-position prediction cron.

## Methodological lesson — single-test-year transfer guards false-positive

This is the central finding and the reason the doc is worth keeping.

The transfer guard (`jra-lgb-lambdarank-transfer-guard.md`) scored a **single** held-out
test year (2023) and reported:

- 703: test-2023 Δtop1 **+0.733pp** (SURVIVE)
- 005: test-2023 Δtop1 **+0.428pp** (SURVIVE)

Both looked like genuine, non-collapsing transfers. The full multi-year nested-HPO holdout
(2023, 2024, 2025, 2026 pooled, 4229 / 3147 races) inverts the picture entirely:

- 703: top1 Δ flips to **−0.2365pp**; every other axis negative
- 005: top1 Δ flips to **−0.4766pp**; place2/place3 down 2–2.5pp

The +0.733/+0.428 single-year readings were noise around a true negative effect. A single
test year (n≈930–1230 races) is far too thin to distinguish a real residual gain from
sampling variance — particularly for a rank-blend whose effect size is sub-percentage-point.

**Takeaway for future residual/blend experiments:** a single-test-year transfer guard is a
cheap _abort_ filter (it correctly catches the iter13 catastrophic-collapse pattern), but it
is **not** a valid _proceed_ signal. A PROCEED on a residual blend must be backed by a
multi-year expanding-window holdout with a powered judge (bootstrap LB95 + multi-axis
no-regression gate). The cheap guard's PROCEED should be read as "not obviously broken,
worth the full retrain to find out" — never as positive evidence of transfer.

## Production recommendation

- **703**: do NOT deploy. Keep the active iter14-jra-cb-pacestyle-course-v8 base.
- **005**: do NOT deploy. Keep the active base.
- **010**: not tested; no action.
- **Category JRA**: the lgb-lambdarank residual lineage is closed for JRA. The model
  artifacts in `tmp/jra-lgb/models/` are retained for reproducibility only and are NOT to be
  registered. The production registry / `per_class.py` were not touched.

## Resource / safety notes

- PG read-only throughout (class-map fetch only). No DELETE/TRUNCATE/DROP.
- num_threads=6, Optuna n_jobs=1 (≤60 trials), single process, peak RSS ~9.5GB (< 10GB cap).
- Run terminated cleanly after 005 finished (SIGTERM to python child + uv parent) to vacate
  the box before the 02:53 JST cutoff, leaving the 03:00 JST finish-position cron unobstructed.
- All scratch output under `tmp/` (never git-tracked); only this docs file is committed.

## Quality Gate Results

- tsc: n/a — no TypeScript changed
- lint: n/a — experiment script lives in tmp/ (not an enforced package)
- format:check: n/a
- test:coverage: n/a — no enforced-package file modified
- python:check: n/a — retrain script is tmp/ scratch, not under an enforced coverage target
