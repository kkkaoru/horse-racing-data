# iter20-jra-cb-2013-v8 — Deploy Log (2026-06-17)

## Summary

Deployed iter20-jra-cb-2013-v8 as JRA production candidate (smoke passed; registry flip
deferred to orchestrator). Replaces iter19-jra-cb-kohan3f-going-v8.

**Model**: iter20-jra-cb-2013-v8 (CatBoost, 244 features)
**Change**: training start year 2006 → 2013 (removes pre-2013 non-stationary data)
**Artifact**: `apps/finish-position-predict-container/models/finish-position/jra/iter20-jra-cb-2013-v8/`
**Smoke image**: `finish-position-predict-local:iter20-2013-candidate` (throwaway tag)

## Training Details

| Parameter      | Value                                                                 |
| -------------- | --------------------------------------------------------------------- |
| Script         | `tmp/deploy_2013/train_iter20_jra_2013.py`                            |
| Feature store  | `tmp/v8/feat-jra-v8-iter19-kohan3f-going` (244 feats, same as iter19) |
| Train start    | **20130101** (iter19 used 20060101)                                   |
| Train end      | 20251231                                                              |
| Val year       | 2026                                                                  |
| Train rows     | 626,798 (vs 923,146 for iter19 / 2006+ baseline)                      |
| Architecture   | CatBoost YetiRank                                                     |
| depth          | 8                                                                     |
| learning_rate  | 0.05                                                                  |
| l2_leaf_reg    | 3.0                                                                   |
| iterations     | 1000, od_wait=30                                                      |
| random_seed    | 2068                                                                  |
| thread_count   | 6                                                                     |
| bootstrap_type | Bayesian                                                              |
| best_iteration | 174                                                                   |
| train_duration | 64.0s                                                                 |

### Time-decay sample weights: NOT used (confirmed)

CatBoost YetiRank (pairwise loss) does not support object-level sample weights.
Production catboost training also omits weights — confirmed by:

- `tmp/v8/iter19_kohan3f_going_verify.py` (iter19 production train): uses `Pool(data=..., label=..., group_id=...)` — no `weight=` arg
- `tmp/window_ablation_wf/run_wf.py` (WF validation): explicit comment: "Note: YetiRank (pairwise loss) does not support object-level weights in CatBoost. Production catboost training also omits sample weights for this reason."
- iter20 training (`tmp/deploy_2013/train_iter20_jra_2013.py`): identical Pool construction, no weights
- The WF results in `goal-jra-window-ablation.md` compare both arms under identical no-weight conditions.

### Walk-forward confirmation

From `docs/finish-position-accuracy/history/goal-jra-window-ablation.md` (3-fold WF):

| Metric     | 2006+   | 2013+   | Delta (pp)   |
| ---------- | ------- | ------- | ------------ |
| top1       | 44.515% | 45.094% | **+0.579pp** |
| place2     | 23.232% | 23.618% | **+0.386pp** |
| place3     | 16.450% | 16.768% | **+0.318pp** |
| top3_box   | 15.755% | 15.716% | -0.039pp     |
| fukusho_2p | 83.464% | 83.695% | **+0.232pp** |

Gate result: ADOPT (all 5 metrics positive or within -0.05pp gate).

## Artifact Location

```
apps/finish-position-predict-container/models/finish-position/jra/iter20-jra-cb-2013-v8/
  model.json     (2,457,971 bytes, CatBoost binary in JSON format)
  metadata.json  (244 features, best_iteration=174, n_train_rows=626798)
```

The `tmp/v8/models/iter20-jra-cb-2013-v8/` scratch copy matches the baked artifact.

## Production Code Changes

- `apps/finish-position-predict-container/src/predict_lib/model_meta.py`:
  - `MODEL_VERSION_BY_CATEGORY["jra"]`: `iter19-jra-cb-kohan3f-going-v8` → `iter20-jra-cb-2013-v8`
  - docstring updated to reflect 2026-06-17 deploy
- Tests (`tests/test_model_meta.py`, `tests/test_per_class.py`, `tests/test_ensemble_routing.py`, `tests/test_upcoming.py`):
  - `JRA_FALLBACK_MODEL_VERSION` constant updated to `iter20-jra-cb-2013-v8`
  - All iter19-specific lambda checks updated
- Coverage: 100% (470 tests, 0 failures), ruff 0 warnings, basedpyright 0 errors

## Smoke Test (RUN_DATE=20260607)

- Image: `finish-position-predict-local:iter20-2013-candidate`
- Exit code: **0**
- Rows predicted: **357 JRA rows**
- Races: **24** (verified via Neon query)
- Rank-1 count: **24** (exactly 1 rank-1 per race — guard passed)
- Races with bad rank-1 count: **0**
- Score range: [-0.64, +3.47], mean 1.14 (sane CatBoost output)
- model_version in Neon output: **iter20-jra-cb-2013-v8** (confirmed via Neon query)
- Errors: none (`score-error:`, `BinderException`, `member-column-gap:`, `member-metadata-missing:` — absent)
- Realtime-odds timeout: expected for past date (20260607 is historical; null-odds fallback applied)
- Top1 sanity (20260607 actuals): 7/24 races = 29.2% (single day, n=24, not statistically significant)

Note: PK for race_finish_position_model_predictions includes model_version, so iter20 smoke rows
(model_version='iter20-jra-cb-2013-v8') do NOT overwrite production iter19 rows.

## FLIP COMMANDS (deferred — orchestrator executes)

### Step 1: Docker split2 retag

```sh
docker tag finish-position-predict-local:iter20-2013-candidate finish-position-predict-local:split2
```

Current split2 (rollback anchor): SHA ff690853e006 (`iter19-jra-cb-kohan3f-going-v8`)

### Step 2: finish_position_active_models flip

**Local PG (3-col PK: category):**

```sql
UPDATE finish_position_active_models
SET model_version = 'iter20-jra-cb-2013-v8',
    activated_at = now()
WHERE category = 'jra';
```

**Neon (4-col PK: category + subclass IS NULL):**

```sql
UPDATE finish_position_active_models
SET model_version = 'iter20-jra-cb-2013-v8',
    activated_at = now()
WHERE category = 'jra'
  AND subclass IS NULL;
```

### Rollback

If iter20 shows regression after live traffic, revert to iter19:

```sh
# Docker rollback
docker tag finish-position-predict-local:iter20-2013-candidate finish-position-predict-local:iter20-2013-backup
docker tag <old-split2-sha-ff690853e006> finish-position-predict-local:split2

# DB rollback (local PG)
UPDATE finish_position_active_models
SET model_version = 'iter19-jra-cb-kohan3f-going-v8',
    activated_at = now()
WHERE category = 'jra';

# DB rollback (Neon)
UPDATE finish_position_active_models
SET model_version = 'iter19-jra-cb-kohan3f-going-v8',
    activated_at = now()
WHERE category = 'jra'
  AND subclass IS NULL;
```

Rollback anchor: iter19 artifact is still in
`apps/finish-position-predict-container/models/finish-position/iter19-jra-cb-kohan3f-going-v8/`
and in `tmp/v8/models/iter19-jra-cb-kohan3f-going-v8/`.

## Confirmed: active_models NOT touched

The `finish_position_active_models` registry was NOT modified by this SubAgent.
Only the model_meta.py code + baked image artifact were updated.
Registry flip is deferred to the orchestrator per task specification.

## Monitor G2 post-deploy

Per-class WF showed G2 (n=114) has place2/place3 regression (-2.6/-4.4pp).
G2 is 1.1% of races and high-variance (small n in WF). Monitor G2 place2/3
in the first week of live traffic to confirm this is within WF variance.
