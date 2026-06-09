---
iteration: 36
date: 2026-06-10T07:45:00+09:00
based_on_iteration: 32-33
follows: history/oi-2026-06-10-rounds-r2-r4-pgvector.md
lever: L-altloss-lgb-lambdarank-residual-C (H4 alt-loss + fresh HPO LightGBM LambdaRank residual for NAR class C)
status: ADOPT (user-approved win-priority override of the automatic 4-axis place-protecting gate)
quality_gate: passed — 382 tests / 100% coverage on apps/finish-position-predict-container; oxfmt + python:check green via lefthook (commit 3669b6d)
model_version_jra: iter14-jra-cb-pacestyle-course-v8 + per-class ensembles (UNCHANGED — no JRA card on 2026-06-10)
model_version_nar: per-class production config — NAR class C flipped to iter36-nar-lgb-ensemble-C-v8; all other classes UNCHANGED (other→iter30-nar-cb-ensemble-other-v8, A→iter30, NEW/MUKATSU→iter30, B→iter12 baseline fallback)
scope:
  venue: 大井 Ōi (keibajo_code=44) is the headline 2026-06-10 card (5 C races), but the route flip applies to ALL NAR class-C races
  target_card: 2026-06-10
  routing: Ōi C×5 (R1,2,6,8,9) / other×4 (R3,4,5,11) / B×3 (R7,10,12)
  goal: maximise NAR class-C top1 (win-hit) accepting a small place3/top3_box tradeoff
model:
  ensemble_version: iter36-nar-lgb-ensemble-C-v8
  ensemble_type: rank_blend
  members:
    - { model_version: iter12-nar-xgb-hpo-v8, weight: 0.4976, is_baseline: true }
    - {
        model_version: iter36-nar-lgb-lambdarank-residual-C-v8,
        weight: 0.5024,
        is_baseline: false,
        architecture: lightgbm-lambdarank,
      }
  residual_objective: lambdarank (ndcg@3, truncation 3, best_iteration 102, seed 42)
  residual_features: 174 (iter12_score is feature index 173, matching the iter30 C residual ordering)
  blend_search: optuna_tpe, 200 trials, seed 42
metrics:
  holdout_nar_C: # 2023-2026 holdout, 26060 races (judge/C.json)
    baseline: { top1: 58.945, place2: 35.288, place3: 27.268, top3_box: 34.793 }
    candidate: { top1: 59.286, place2: 35.322, place3: 27.057, top3_box: 34.601 }
    delta_pp: { top1: +0.342, place2: +0.035, place3: -0.211, top3_box: -0.192 }
    bootstrap_top1: { delta_lb_95: +0.0012, delta_mean: +0.0034, p_delta_gt_0: 0.9994 }
  holdout_oi_C: # Ōi keibajo=44 slice, 1928 races
    delta_pp: { top1: +0.207, place2: -0.571, place3: -0.363, top3_box: +0.104 }
artifacts:
  model_dir_nar: apps/finish-position-predict-container/models/finish-position/nar/per-class/C/iter36-nar-lgb-lambdarank-residual-C-v8/ (gitignored — model.txt + metadata.json, baked into the local image)
  ensemble_manifest: apps/finish-position-predict-container/models/finish-position/nar/per-class/C/iter36-nar-lgb-ensemble-C-v8/manifest.json (gitignored)
  source_experiment: tmp/nar-perclass/h4_altloss_hpo (judge/C.json — re-optimised blend, chosen_rung_cap=null)
  smoke_log: tmp/v8/smoke/run-20260610-nar-iter36.log
  prediction_log: ~/Library/Logs/finish-position-predict/20260610.log
---

## What was tried

Following the four-lever Ōi exhaustion documented at iter 32–33 (reweight / venue-features / specialist / pgvector-JRA all rejected on a same-night basis), the genuinely-new signal was sourced offline: an **H4 alt-loss LightGBM LambdaRank residual** for NAR class C, trained with a fresh HPO over the existing 174-feature C chain (`iter12_score` carried as the 174th feature, index 173, exactly matching the iter30 CatBoost C-residual ordering). The residual replaces the iter30 CatBoost residual in the per-class blend; the blend weights were re-optimised by Optuna TPE (200 trials) against the C judge slice, yielding `iter12-nar-xgb-hpo-v8 @ 0.4976 + iter36-nar-lgb-lambdarank-residual-C-v8 @ 0.5024`.

This is the first new model since the iter 32–33 rounds to beat the production per-class config on NAR class-C top1 with a positive bootstrap lower bound.

## Implementation summary

Container support for the LightGBM architecture + the route flip landed in **commit `3669b6d`** (`feat(per-class): adopt LightGBM LambdaRank residual for NAR class C`):

- `src/lightgbm_adapter.py` (new) — loads `model.txt` (native `Booster.save_model` text dump) and adapts it to the scorer `predict()` contract; scores rows positionally on the member's own `metadata.json` feature order. Mirrors `catboost_adapter` / `xgboost_adapter` and, like them, is intentionally outside `--cov=predict_lib` (native-I/O wrapper, exercised at deploy time).
- `predict_lib/model_meta.py` — adds `"lightgbm"` to the `Architecture` literal, plus `is_lightgbm_model_version` / `member_model_file_name` (the `-lgb-` / `-lambdarank-` tokens resolve to the LightGBM arch and the `model.txt` artifact file).
- `predict_lib/booster_pool.py` + `predict_lib/ensemble_routing.py` — architecture-aware member loading (`load_booster_from_path` dispatch) and `resolve_member_architecture` so the lgb residual loads via `lightgbm_adapter` while the iter12 XGBoost baseline keeps its path.
- `predict_lib/per_class.py` — `("nar", "C") → iter36-nar-lgb-ensemble-C-v8`.
- `predict_lib/scorer.py` — keeps the float64 path for the LightGBM ranking member.
- `Dockerfile` — installs `libgomp1` (the GNU OpenMP runtime LightGBM's wheel dynamically links via `libgomp.so.1`; CatBoost / XGBoost bundle their own OpenMP, so this gap only surfaced once a LightGBM member was added — without it the member fails to import at score time).
- `pyproject.toml` / `uv.lock` — add `lightgbm>=4.5.0` (resolves to 4.6.0).
- Tests updated in the same commit: 382 pass at 100% coverage.

The model artifacts (`model.txt`, `metadata.json`, ensemble `manifest.json`) are gitignored scratch baked into the local docker image at build time — NOT committed.

## Results

NAR class-C holdout (2023–2026, 26 060 races), production blend vs candidate:

| axis     | baseline (iter30 blend) | candidate (iter36 blend) | Δpp    |
| -------- | ----------------------- | ------------------------ | ------ |
| top1     | 58.945                  | 59.286                   | +0.342 |
| place2   | 35.288                  | 35.322                   | +0.035 |
| place3   | 27.268                  | 27.057                   | -0.211 |
| top3_box | 34.793                  | 34.601                   | -0.192 |

Top1 paired bootstrap: delta_lb_95 = **+0.0012**, delta_mean = +0.0034, P(Δ>0) = **0.9994** — the top1 gain is robustly positive, not sampling noise.

Ōi (keibajo=44) C slice (1928 races): top1 +0.207pp, top3_box +0.104pp, place2 -0.571pp, place3 -0.363pp.

## Per-bucket findings

The change is class-C-only. The automatic 4-axis place-protecting accept gate (`no-reg threshold -0.05pp` on each of place2/place3/top3_box) **rejected** the candidate because place3 (-0.211pp) and top3_box (-0.192pp) regress beyond -0.05pp, even though top1 improves with a positive bootstrap LB. All other NAR classes are untouched and keep their iter30 ensembles / iter12 baseline fallback.

## Decision

**ADOPT — user-approved win-priority override.** The user explicitly authorised deploying this model accepting the place3 / top3_box tradeoff in exchange for the robust +0.342pp top1 (win-hit) gain on the global NAR-C holdout. This overrides the automatic gate's REJECT verdict, which is tuned to protect the place axes by default.

Deploy verification (2026-06-10 card):

1. **Image** — rebuilt `finish-position-predict-local:split2` from the working tree (with the `libgomp1` Dockerfile fix); verified inside the image: `import lightgbm` → 4.6.0, the C `model.txt` loads as a 174-feature Booster, and the baked registry routes `("nar","C") → iter36-nar-lgb-ensemble-C-v8`.
2. **Smoke** (isolated `horse_racing_smoke` DB, NOT production) — `RUN_DATE=20260610 PREDICT_CATEGORIES=nar`, `races_predicted=518`, exit clean. The 5 Ōi C races (R1,2,6,8,9) routed to `iter36-nar-lgb-ensemble-C-v8` with **zero** `score-error` / `member-column-gap` / `member-metadata-missing` / `ensemble fallback` lines. Other classes unchanged (other→iter30-ens-other, B→iter12).
3. **Production** — ran the authorised daily wrapper `RUN_DATE=20260610 finish-position-predict-daily.sh`; docker exit 0, `races_predicted=518`, idempotent UPSERT into Neon `race_finish_position_model_predictions`. No DELETE (the prior iter30 C rows remain; the new iter36 rows carry a strictly-later `prediction_generated_at`).
4. **Neon routing verified** (read-only) — all 12 Ōi races present; effective (latest-recency) per-race model:
   - C (R1, R2, R6, R8, R9) → `iter36-nar-lgb-ensemble-C-v8`
   - other (R3, R4, R5, R11) → `iter30-nar-cb-ensemble-other-v8`
   - B (R7, R10, R12) → `iter12-nar-xgb-hpo-v8` (baseline fallback)

   The viewer's prediction-selection (`finish_position_active_models` has only the `nar→iter12` category row, no per-C row) resolves C via the recency fallback to the freshest `prediction_generated_at` = the iter36 rows, so the new model is what surfaces tomorrow.

5. **wrangler** — NOT deployed. The `finish-position-cron` Worker has the Cloudflare Container cron disabled by design (Containers reap batch instances at ~90–110s; this ~10 min DuckDB feature build cannot complete in that window). The load-bearing artifact is the local docker image + `finish-position-predict-daily.sh` (steps 1–4 above), which already produced and verified tomorrow's prediction. No Worker source changed this round, the model artifacts are gitignored (a remote container rebuild would not bake the iter36 model), and the already-deployed Worker is only used for the ad-hoc `/run` + `/health` + D1 audit endpoints — so forcing a deploy would change nothing that serves the prediction and risk swapping the cloud container reference to a model-less image. Reported clearly rather than forced, per the deploy runbook.

## Next iteration recommendation

The H4 alt-loss LightGBM residual is now proven on C only. The natural follow-on is to evaluate the same alt-loss + fresh-HPO LambdaRank residual on the `other` and `B` NAR classes (and the JRA per-class buckets) under the identical per-year WF + paired-bootstrap gate, then let the user decide per class whether the top1 gain justifies the place-axis tradeoff. The place3 regression on C is the cost of the LambdaRank ndcg@3 objective concentrating on the winner; a place-aware co-objective (or a place-axis-protecting blend constraint) is the next lever if the place tradeoff proves undesirable in live results.

## Quality Gate Results

- tsc: n/a — no TypeScript changed this round
- lint: pass — oxfmt clean (lefthook pre-commit, commit 3669b6d)
- format:check: pass
- test:coverage: n/a — no enforced TS package modified
- python:check: pass — ruff + ty + basedpyright clean; pytest 382 passed, coverage 100.00% over `predict_lib`
