# E-top2 place-preserving XGBoost override — deploy doc (iter22-jra-etop2)

## Status: STAGED (2026-06-18)

JRA_ETOP2_ENABLED = False until orchestrator flips.
Rollback anchor: split2 image = 4d1746f535e5 (iter20-jra-cb-2013-v8).

## What it is

E-top2 applies a place-preserving XGBoost override on top of CB iter20 for JRA.
After CB iter20 scores each race, XGB xgb-jra-2013-v8 scores the same race.
When XGB's rank-1 horse == CB's rank-2 horse (and race class != 701), CB rank-1
and rank-2 are swapped. CB rank-3 and below are unchanged — place3 is preserved
by construction.

Implementation: score injection. CB rank-2 gets max(cb_scores) + 1.0 (new rank-1),
CB rank-1 gets max(cb_scores) + 0.5 (new rank-2). All other horses keep CB scores.

Override fraction (2025 blind): 13.1% of races (453/3455).

## Gate results

### Blind 2025 (original ADOPT decision)

Source: docs/finish-position-accuracy/per-class/jra/place-preserving-override.md

| metric | delta   | LB95    | verdict |
| ------ | ------- | ------- | ------- |
| top1   | +1.42pp | +0.58pp | PASS    |
| place2 | +0.75pp | +0.06pp | PASS    |
| place3 | 0.00pp  | 0.00pp  | PASS    |

Decision: ADOPT (2026-06-18).

### WF per-fold re-confirm (wf_perfold_etop2_v2.py, 2026-06-18)

Metric: exact-ordinal (top1 = pred-rank-1 actual=1,
place2 = pred-rank-2 actual=2, place3 = pred-rank-3 actual=3).
Source: CB iter20 precomputed scores + XGB xgb-jra-2013-v8 live inference.
Class 701 exclusion: kyoso_joken_code fetched from PG jvd_ra per fold.

| fold | n_races | overrides | top1_delta | place2_delta | place3_delta |
| ---- | ------- | --------- | ---------- | ------------ | ------------ |
| 2023 | 3456    | 410       | +0.67pp    | +0.09pp      | 0.00pp       |
| 2024 | 3454    | 382       | -0.32pp    | 0.00pp       | 0.00pp       |
| 2025 | 3455    | 416       | +1.42pp    | +0.75pp      | 0.00pp       |

2024 top1 is -0.32pp but place2 is exactly 0.00pp (not negative). Place2 is
non-negative across all folds. The task stop condition was "place2 point-negative
across folds" -- that condition is NOT met. Proceed confirmed.

## Artifacts

- XGB model (baked, gitignored scratch):
  apps/finish-position-predict-container/models/finish-position/jra/xgb-jra-2013-v8/
  model.json (9.1MB, rank:ndcg, 244 features, train 2013-2022, seed=2068)
  metadata.json (244 feature names from iter20 metadata)

- R2 key (for production download): finish-position/jra/xgb-jra-2013-v8/model.json

- XGB config: objective=rank:ndcg, eval=ndcg@3, max_depth=8, lr=0.05,
  lambda=3.0, subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
  num_boost_round=1000, early_stopping=30, seed=2068.

## Code changes (STAGED commit)

- src/predict_lib/etop2_override.py (NEW): pure module, apply_etop2_scores() +
  is_etop2_override_active(). ETOP2_EXCLUDED_CLASS = "701".
- src/predict_lib/model_meta.py (MODIFIED): added JRA_ETOP2_ENABLED=False,
  JRA_ETOP2_MODEL_VERSION, JRA_ETOP2_XGB_MODEL_VERSION, build_r2_xgb_etop2_key().
- src/predict_upcoming.py (MODIFIED): added \_score_one_race_etop2(),
  \_load_xgb_etop2_booster(), JRA_ETOP2_ENABLED gate in \_predict_category().
  extract_race_class_code("jra", entries) pulls kyoso_joken_code from entries
  (loaded from PG alongside features).
- tests/test_etop2_override.py (NEW): 26 tests, 100% branch coverage.
- tests/test_model_meta.py (MODIFIED): 5 new tests for E-top2 constants.

Coverage: 100% (501 tests). Ruff 0 warnings. Basedpyright 0 errors.

## Smoke test (iter22-etop2-smoke, 2026-06-18)

Image: finish-position-predict-local:iter22-etop2-smoke (JRA_ETOP2_ENABLED=True).
XGB model baked via Dockerfile COPY apps/finish-position-predict-container/models /models.
RUN_DATE=20260607, PREDICT_CATEGORIES=jra.
Note: This date has only 1 JRA race (source.config showed from_date=20260607).
SOURCE_DATABASE_URL=local PG (postgresql://horse_racing:...@127.0.0.1:15432/horse_racing)

Result: PASS on scoring / FAIL on Neon UPSERT (expected in local smoke context).

Feature pipeline: all 14 JRA layers ran successfully. DuckDB base: 357 rows, 51s.
Layer sequence: source.stage (49s) → base build (357 rows) → 14 layer scripts
(race-internal, market-signal, sectional-weight, futan, workout, near-miss,
lineage, head-to-head, baba-pedigree-affinity, trainer-stable-affinity,
pacestyle, course-numerical, relationship-r1, kohan3f-going).

Override confirmed:
[etop2] override fired race_id=jra:2026:0607:05:01 class=703
Class 703 (not 701) → eligible. Fired for 1/1 JRA races on the smoke date.
Unique rank-1 confirmed by construction (apply_etop2_scores sets cb_rank2_idx
= cb_max + 1.0 and cb_rank1_idx = cb_max + 0.5, all other entries keep CB scores).

Exit code: 1, cause: psycopg.errors.AdminShutdown during \_flush_predictions to Neon.
Root cause: Neon idle-timeout (21 minutes) killed the Neon connection established
at startup while the local-PG feature build was running. The E-top2 scoring code
(\_score_one_race_etop2) completed without errors before the connection loss.
This failure is pre-existing and category-isolated -- it does not affect scoring
correctness. In production the feature build uses SOURCE_DATABASE_URL (local PG)
and the Neon connection is kept alive by the audit path; smoke failure here is
expected when the Neon link is left idle for the full 21-minute feature build.

## Flip runbook (orchestrator)

Prerequisites:

1. Override fired confirmed (etop2 log line present): DONE (class=703, 1/1 races).
2. Scoring code PASS (no exception in \_score_one_race_etop2): DONE.
3. XGB model uploaded to R2: finish-position/jra/xgb-jra-2013-v8/{model,metadata}.json.

Step 1: flip JRA_ETOP2_ENABLED in model_meta.py:
JRA_ETOP2_ENABLED: Final[bool] = True

Step 2: rebuild production image:
docker build -f apps/finish-position-predict-container/Dockerfile \
 -t finish-position-predict-local:split2 .

Step 3: update active_models in Neon:
UPDATE finish_position_active_models
SET model_version = 'iter22-jra-etop2'
WHERE category = 'jra';

Step 4: commit + tag.

## Rollback

docker tag finish-position-predict-local:iter20-2013-candidate \
 finish-position-predict-local:split2

# rollback anchor image ID: 4d1746f535e5 (iter20-jra-cb-2013-v8)
