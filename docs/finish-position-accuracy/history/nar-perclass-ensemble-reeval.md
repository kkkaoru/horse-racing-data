# NAR Per-Class Ensemble Re-eval: Stale-Ensemble Check After G1+F1 Retrain

**Date**: 2026-06-12
**Trigger**: Commit 1b3815b deployed `nar-xgb-v7-g1f1-combined-wf-21y` as new NAR base (G-1 + F1 bugs fixed).
**Question**: Are the NAR per-class residual ensembles stale (trained on old base residuals)?

---

## Step 1: Routing Determination

### What was actually flipped in 1b3815b

Commit 1b3815b updated `finish_position_active_models` (the local-PG/viewer-side registry)
and added the judge doc. It did **not** update
`apps/finish-position-predict-container/src/predict_lib/model_meta.py`.

The container prediction path resolves the NAR base entirely from `model_meta.py`:

```python
MODEL_VERSION_BY_CATEGORY: Final[dict[Category, str]] = {
    "jra": "iter14-jra-cb-pacestyle-course-v8",
    "nar": "iter12-nar-xgb-hpo-v8",   # <-- NOT updated
    "ban-ei": "banei-cb-v7-lineage-wf-21y",
}
```

`model_version_for("nar")` still returns `"iter12-nar-xgb-hpo-v8"`. The
`finish_position_active_models` table is consumed only by the viewer import
scripts (`import-predictions-sql.ts`, `score_finish_position_local.py`) and the
Neon display layer — not by the Cloudflare Container prediction pipeline.

### Per-class ensemble status

Six NAR sub-classes are registered in `PER_CLASS_MODEL_VERSIONS` with manifest
files present on disk:

| sub-class | active ensemble label             | baseline member in manifest |
| --------- | --------------------------------- | --------------------------- |
| NEW       | iter30-nar-cb-ensemble-NEW-v8     | iter12-nar-xgb-hpo-v8       |
| MUKATSU   | iter30-nar-cb-ensemble-MUKATSU-v8 | iter12-nar-xgb-hpo-v8       |
| C         | iter36-nar-lgb-ensemble-C-v8      | iter12-nar-xgb-hpo-v8       |
| A         | iter30-nar-cb-ensemble-A-v8       | iter12-nar-xgb-hpo-v8       |
| OP        | iter30-nar-cb-ensemble-OP-v8      | iter12-nar-xgb-hpo-v8       |
| other     | iter30-nar-cb-ensemble-other-v8   | iter12-nar-xgb-hpo-v8       |

All manifests embed `iter12-nar-xgb-hpo-v8` as `is_baseline: true`. The residual
members (CatBoost for iter30, LightGBM lambdarank for iter36) were trained on
iter12's raw scores from the old 192-feature store.

`resolve_per_class_resolution` in the container finds these manifests on disk
and returns `PerClassEnsemble` objects, so **the per-class ensemble path IS
active** in serving. However, the baseline member loaded at inference is still
`iter12-nar-xgb-hpo-v8` (model file:
`models/finish-position/nar/iter12-nar-xgb-hpo-v8/model.json`), not the new
base. The new model artifact lives at
`models/finish-position/nar/nar-xgb-v7-g1f1-combined-wf-21y/model.json` but is
not referenced by any manifest or by `model_meta.py`.

### Verdict

> **NO-OP: Container serves `iter12-nar-xgb-hpo-v8` as the per-class ensemble
> baseline member. `model_meta.py` has NOT been updated to
> `nar-xgb-v7-g1f1-combined-wf-21y`. No stale-ensemble condition exists today.**

The `finish_position_active_models` flip in 1b3815b was viewer/display-side only.
The container will continue to serve `iter12` (via both the ensemble member path
and the category-global fallback for NAR class `B`) until `model_meta.py` is
explicitly updated.

---

## Forward risk note

When `model_meta.py` is updated to point to `nar-xgb-v7-g1f1-combined-wf-21y`:

- Feature count changes from 192 to 174 (18 features dropped, mostly RS v3 and
  trainer-grade features).
- All six per-class residual members were trained on iter12's scores from the
  192-feature store. Their residual feature (`iter12_score` injected as a
  synthetic column) will become stale.
- The ensemble manifests will also break at load time because the manifest's
  `baseline_version` field (`"iter12-nar-xgb-hpo-v8"`) will not match the new
  `model_version_for("nar")` when the manifest loader attempts to identify the
  baseline member.
- **Required action at that time**: rebuild all 6 per-class residual members on
  the corrected `feat-nar-v7-baba-21y-f1` store + new base scores, re-run the
  judge against the deployed new base, and flip manifests only on ADOPT.

No action needed today. This document records the routing state at the time of
the G1+F1 base deploy for future reference.

---

## Files inspected

- `apps/finish-position-predict-container/src/predict_lib/model_meta.py` — container NAR base version (still `iter12`)
- `apps/finish-position-predict-container/src/predict_lib/per_class.py` — `PER_CLASS_MODEL_VERSIONS` registry
- `apps/finish-position-predict-container/models/finish-position/nar/per-class/*/manifest.json` — all 6 NAR ensemble manifests
- `apps/finish-position-predict-container/models/finish-position/nar/nar-xgb-v7-g1f1-combined-wf-21y/metadata.json` — new base metadata (174 features)
- `apps/finish-position-predict-container/models/finish-position/nar/iter12-nar-xgb-hpo-v8/metadata.json` — old base metadata (192 features)
- `docs/finish-position-accuracy/history/g1-f1-combined-nar-retrain-judge.md` — 1b3815b judge doc
