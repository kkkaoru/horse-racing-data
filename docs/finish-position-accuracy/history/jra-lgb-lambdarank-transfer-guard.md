---
experiment: jra-lgb-lambdarank-transfer-guard
date: 2026-06-11
status: PROCEED — transfer holds on both powered JRA classes (703, 005); no WF collapse
follows: history/oi-2026-06-10-iter36-lgb-lambdarank-residual-C-adopt.md
question: Does the NAR-C lgb-lambdarank residual recipe (iter36, +0.342pp top1) transfer to JRA per-class buckets WITHOUT walk-forward collapse (iter13 pattern)?
scope:
  category: jra
  classes_tested: [703, 005]
  classes_excluded: [016] # underpowered n≈727 per prior power analysis
  base_model: iter14-jra-cb-pacestyle-course-v8
  base_score_feature: iter14_score (per-fold leak-free predictions, 2007–2023)
split:
  train: "2007–2021, chain-filtered per JRA class inclusion chain"
  val: 2022 # residual training early-stop signal
  test: 2023 # collapse check: val-vs-test divergence = iter13 pattern
  note: "val and test are target-class-only (703-only or 005-only races)"
recipe:
  lgb_objective: lambdarank
  lgb_metric: ndcg@3
  lgb_truncation_level: 3
  lgb_params: NAR-C hpo_C.json winner_params (reused as-is, no new HPO)
  feature_count: 254 (iter26-relationships 253 cols + iter14_score at index 253)
  relevance_labels: "{1st→3, 2nd→2, 3rd→1, else→0}"
  blend: rank_blend w_base=0.50 / w_lgb=0.50 (neutral probe; NAR used 0.4976/0.5024)
  blend_search: NOT RUN — transfer guard only; Optuna blend search deferred to full retrain
collapse_check_rationale: >
  iter13 produced a CV gain that collapsed in walk-forward (CV→WF divergence).
  This guard checks val_delta vs test_delta: if val_top1>0 AND test_top1≤0, that
  is the iter13 collapse pattern and we ABORT the expensive full retrain.
artifacts:
  transfer_guard_json: tmp/jra-lgb/transfer_guard.json
  guard_script: tmp/jra-lgb/jra_lgb_transfer_guard.py
  features_root: apps/pc-keiba-viewer/tmp/feat-jra-v8-iter26-relationships (254 cols)
  iter14_preds_root: tmp/bucket-eval/finish-position/iter14-jra-cb-pacestyle-course-v8/predictions
  lgb_params_source: tmp/nar-perclass/h4_altloss_hpo/hpo_C.json pick.winner_params
---

## Recipe — NAR-C lgb-lambdarank residual

The iter36 NAR class-C adopt (`oi-2026-06-10-iter36-lgb-lambdarank-residual-C-adopt.md`)
established the following recipe, which this guard replicates for JRA:

1. **Base model score** — `iter14_score` is the per-fold leak-free predicted score from
   `iter14-jra-cb-pacestyle-course-v8`, attached as the last feature (index 253 for JRA's
   254-feature matrix, vs index 173 for NAR's 174-feature matrix). This mirrors the NAR
   recipe where `iter12_score` is feature index 173.

2. **Chain-filtered training** — JRA uses the static class inclusion chain:
   `703 ⊃ {703, 701}`, `005 ⊃ {005, 703, 701}`. Train rows include target class + all
   lower-tier classes; validation and test rows are target-class-only.

3. **LightGBM LambdaRank** — `objective=lambdarank`, `metric=ndcg`, `ndcg_eval_at=[3]`,
   `lambdarank_truncation_level=3`. Relevance labels: `{1st→3, 2nd→2, 3rd→1, else→0}`.
   Time-decay group weights on train (0.5→1.0 linear by race year). `group_id=race_id`.
   Early stopping rounds=30. `num_threads=4` (capped per resource constraint).

4. **Params** — NAR-C HPO winner reused as-is:
   `lr=0.137, num_leaves=15, min_data_in_leaf=75, lambda_l2=1.04,
feature_fraction=0.606, bagging_fraction=0.963, max_depth=8, n_estimators=738`.
   LGB early-stopped at iterations 54 (703) and 57 (005).

5. **Rank blend** — `0.5 × rank(iter14_score) + 0.5 × rank(lgb_lambdarank_score)`
   (neutral 50/50 probe; the NAR recipe found ~0.4976/0.5024 via Optuna TPE).

## Mini walk-forward design

```
TRAIN  2007–2021   chain-filtered  JRA 703: 22 707 races / 005: 38 371 races
VAL    2022        target-only     JRA 703:  1 237 races / 005:    933 races
TEST   2023        target-only     JRA 703:  1 228 races / 005:    934 races
```

The iter13 collapse pattern is: `val_Δtop1 > 0` AND `test_Δtop1 ≤ 0`. We check this
per class and issue ABORT if any powered class shows it.

## Per-class mini-WF deltas

### Class 703 (未勝利 — n_test=1228 races)

| axis     | base (iter14) | candidate (blend) | Δpp (VAL)  | Δpp (TEST) |
| -------- | ------------- | ----------------- | ---------- | ---------- |
| top1     | 48.424        | 49.879            | **+1.455** | **+0.733** |
| place2   | 68.310        | 68.715            | +0.404     | +0.081     |
| place3   | 78.739        | 79.224            | +0.485     | −0.163     |
| top3_box | 98.060        | 98.222            | +0.162     | +0.163     |

Verdict: **SURVIVE** — val_top1 positive AND test_top1 positive. No collapse.

### Class 005 (1勝クラス — n_test=934 races)

| axis     | base (iter14) | candidate (blend) | Δpp (VAL)  | Δpp (TEST) |
| -------- | ------------- | ----------------- | ---------- | ---------- |
| top1     | 39.508        | 39.936            | **+0.750** | **+0.428** |
| place2   | 60.171        | 59.850            | −0.107     | −0.321     |
| place3   | 71.842        | 71.092            | −0.322     | −0.749     |
| top3_box | 95.396        | 96.146            | −0.107     | +0.750     |

Verdict: **SURVIVE** — val_top1 positive AND test_top1 positive. No collapse.

### Class 016 (3勝クラス)

Excluded — underpowered (n≈727 per prior power analysis). Would not be a reliable
primary decision signal even if tested.

## Collapse verdict

| class | val_Δtop1 | test_Δtop1 | collapse? | verdict |
| ----- | --------- | ---------- | --------- | ------- |
| 703   | +1.455    | +0.733     | NO        | SURVIVE |
| 005   | +0.750    | +0.428     | NO        | SURVIVE |

**No iter13 collapse pattern detected.** Both powered classes show positive top1 delta
on the held-out TEST year (not just val). The val→test attenuation is normal
(val: +1.455/+0.750 → test: +0.733/+0.428) — the signal shrinks but does not flip sign.

## Place-axis tradeoff

The place-axis pattern mirrors the NAR-C result:

- **703**: top3_box stays positive (+0.163pp test). place3 has a small regression
  (−0.163pp test). This is consistent with the LambdaRank ndcg@3 objective concentrating
  on winner/top-2 at the cost of 3rd-place accuracy.
- **005**: place3 regresses more strongly (−0.749pp test). top3_box is positive (+0.750pp).
  This is the same top1-vs-place3 tradeoff as NAR-C — the LambdaRank objective wins top1
  by bumping a horse that sometimes displaces the 3rd-actual from the top-3 predicted set.

The automatic 4-axis place-protecting gate (no-reg threshold −0.05pp on place2/place3/
top3_box) would REJECT 005 (place3 −0.749pp) and possibly pass 703 (all place axes within
−0.2pp). A user win-priority override decision is required for 005, same as it was for NAR-C.

## PROCEED decision — JRA residual matrix prepped

**DECISION: PROCEED** to full retrain on ≥1 powered class.

Both 703 and 005 show non-collapsing top1 gains on the TEST year. The transfer check
passes. The expensive full retrain is justified.

**Residual training matrix is prepped and ready** for the full retrain:

- Feature parquet: `apps/pc-keiba-viewer/tmp/feat-jra-v8-iter26-relationships/` (254 cols,
  years 2006–2025 available)
- Base scores: `tmp/bucket-eval/finish-position/iter14-jra-cb-pacestyle-course-v8/predictions/`
  (years 2007–2026 available)
- HPO params: `tmp/nar-perclass/h4_altloss_hpo/hpo_C.json` pick.winner_params (ready for
  JRA class-specific HPO or direct reuse)
- Class map: PG `jvd_ra` table (read-only, confirmed accessible, 238 315 rows)

The full retrain should:

1. Run a fresh per-class Optuna HPO (analogous to `h4_hpo.py` but for JRA 703 and 005),
   since the NAR-C params were tuned on NAR data.
2. Re-optimise the blend weight via Optuna TPE (200 trials) on the per-class judge slice
   (holdout 2023–2026) — do not assume 0.5/0.5 is optimal for JRA.
3. Apply the same 4-axis accept gate (top1 + place2 + place3 + top3_box, ≥2 positive axes
   with ≥1 of {place2/place3}), or request a win-priority override if 703/005 repeat the
   NAR-C place-axis tradeoff pattern.

## Quality Gate Results

- tsc: n/a — no TypeScript changed
- lint: n/a — experiment script lives in tmp/ (not an enforced package)
- format:check: n/a
- test:coverage: n/a — no enforced-package file modified
- python:check: n/a — guard script is tmp/ scratch, not under an enforced coverage target
