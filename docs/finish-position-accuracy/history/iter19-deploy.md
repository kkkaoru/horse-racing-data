# iter19-jra-cb-kohan3f-going-v8 — Deploy Log (2026-06-13)

## Summary

Deployed iter19-jra-cb-kohan3f-going-v8 as JRA production base model (base-only,
no per-class ensembles). Replaces iter14-jra-cb-pacestyle-course-v8 + iter25/26
per-class ensemble system.

**Model**: iter19-jra-cb-kohan3f-going-v8 (CatBoost, 244 features)
**Improvement**: fukusho_2p LB95 +0.10pp vs full production system (holdout 2023-2026,
n=11703 races); pooled top1 +0.43pp LB95, fukusho_2p +0.28pp LB95, top3_box +0.26pp LB95
**Deploy config**: base-only for ALL JRA classes (drops iter25/26 ensembles)

## New Features (3 columns, 241→244)

| Feature              | Description                                                            | NULL rate (train)     |
| -------------------- | ---------------------------------------------------------------------- | --------------------- |
| `kohan3f_firm_avg5`  | avg kohan_3f over FIRM starts (going 1-2) in last-5 going-coded priors | ~27.6% NULL           |
| `kohan3f_soft_avg5`  | avg kohan_3f over SOFT starts (going 3-4) in last-5 going-coded priors | ~84.2% NULL           |
| `kohan3f_going_diff` | firm_avg5 - soft_avg5 (NULL when either is NULL)                       | NULL when either NULL |

Going derived from `jvd_ra.babajotai_code_shiba` (turf, track_code 10-29) or
`babajotai_code_dirt` (dirt, track_code 51-69). JRA only; NAR/Ban-ei chains
unchanged.

## Verification Reference

See `docs/finish-position-accuracy/history/jra-kohan3f-verify.md` for full
verification results. Candidate B (iter19 base-only) was the ADOPTED configuration.

## Code Changes (commit 5562961)

- `apps/finish-position-predict-container/src/predict_lib/pipeline_args.py`:
  - Added `KOHAN3F_GOING_SCRIPT = "add_kohan3f_going_features.py"` (14th JRA layer)
  - Split `SCRIPTS_WITH_FROM_DATE` from `SCRIPTS_WITH_PG_URL` (kohan3f uses
    `--history-from-year`, not `--from-date`)
- `apps/finish-position-predict-container/src/predict_lib/model_meta.py`:
  - `MODEL_VERSION_BY_CATEGORY["jra"]`: iter14→iter19
  - `FEATURE_COUNT_BY_CATEGORY["jra"]`: 241→244
- `apps/finish-position-predict-container/src/predict_lib/per_class.py`:
  - Removed all 5 JRA per-class entries from `PER_CLASS_MODEL_VERSIONS`
  - NAR per-class unchanged
- Tests: 470 pass, 100% coverage, ruff 0 warnings, basedpyright 0 errors

## Parity Check

Serve-path computation vs training-store values for 20260517 (most recent date
in training store):

- **484/484 rows: exact match** (0 mismatches, float tolerance 1e-9)
- Confirmed: kohan3f_going_diff = firm_avg5 - soft_avg5 via SQL NULL propagation

## Serve-Skew Check

NULL rates at serve == training (identical):

- `kohan3f_firm_avg5`: 67.47% non-null (2023+ data)
- `kohan3f_soft_avg5`: 14.74% non-null (2023+ data)

No serve skew — features are PG-historical with no realtime dependency.

## Smoke Test (RUN_DATE=20260607)

- Image: `finish-position-predict-local:iter19-candidate` (SHA ff690853e006)
- Exit code: 0
- Rows predicted: 357 JRA rows, 24 races
- model_version for 2026-06-07: ALL `iter19-jra-cb-kohan3f-going-v8` (base-only confirmed)
- 1 rank-1 per race: 24 races, 24 rank-1 rows (guard passed)
- Score range: [-0.72, +3.84], mean 1.33 (sane CatBoost output)
- No `score-error:`, `member-column-gap:`, `member-metadata-missing:` lines
- Feature build: 14 layers (relationship layer output = 241 cols → kohan3f adds 3 = 244)

## FLIP Executed (17:00-23:00 JST window, 2026-06-13)

### Docker: split2 retag

```sh
docker tag finish-position-predict-local:iter19-candidate finish-position-predict-local:split2
```

- Old split2: d81c4deb3430 (iter14-jra-cb-pacestyle-course-v8)
- New split2: ff690853e006 (iter19-jra-cb-kohan3f-going-v8)

### finish_position_active_models flip

```sql
UPDATE finish_position_active_models
SET model_version = 'iter19-jra-cb-kohan3f-going-v8',
    activated_at = now()
WHERE category = 'jra';
```

- Previous: `iter14-jra-cb-pacestyle-course-v8` (activated 2026-06-04)
- New: `iter19-jra-cb-kohan3f-going-v8`
- NAR/Ban-ei: unchanged

## First Live Run

**2026-06-14 (Sunday) 09:30 JST JRA run** — iter19 serves for the first time in
production. NAR 03:00 cron is unaffected (no change to NAR model/chain).

## Notes

- Timing: 6/13 09:30 pass was intentionally left on iter14 (one-change-at-a-time;
  first live test of 09:30 serve-skew fix)
- Per-class ensembles are DROPPED for JRA: manifest.json files remain on disk but
  `PER_CLASS_MODEL_VERSIONS` has no JRA entries so they are never loaded
- NAMED_PER_CLASS_CODES_BY_CATEGORY["jra"] is preserved (still {"005","010","016","701","703"})
  for normalize_class_code correctness — removing it would change routing semantics
