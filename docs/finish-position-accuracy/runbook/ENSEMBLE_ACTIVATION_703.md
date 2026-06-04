# 703 Ensemble Activation Runbook (Phase B-2 + Phase C)

- **Date introduced**: 2026-06-05 (JST)
- **Scope**: JRA `kyoso_joken_code='703'` (未勝利戦) only. All other JRA classes
  and NAR / Ban-ei categories are out of scope.
- **Status**: Manifest stored on disk; **NOT active in production**. Every 703
  race still routes to the category-global fallback
  (`iter14-jra-cb-pacestyle-course-v8`). Container behaviour at runtime is
  unchanged.
- **Predecessor**: `PER_CLASS_ROUTING.md` (Phase B-1 routing skeleton, registry
  empty).

## 1. Overview

iter 23 ensemble optimisation (TPE over 200 trials) produced a single TRUE
per-class win for JRA `703` (未勝利戦):

- **Holdout top1**: 0.49563 (n=4229, 2023-2026)
- **iter14 holdout top1**: 0.49421
- **Delta**: **+0.142pp** (sole `ACCEPT` with a non-zero positive delta in iter 23)

Ensemble structure (rank-blend within race, weighted sum of per-race
rank-normalised member scores):

| Member                              | Weight   | Role                             |
| ----------------------------------- | -------- | -------------------------------- |
| `iter14-jra-cb-pacestyle-course-v8` | 0.200000 | Baseline floor (≥ 0.20 enforced) |
| `iter22-jra-cb-residual-703-v8`     | 0.691385 | Dominant — residual on iter14    |
| `iter21-jra-cb-chain-703-v8`        | 0.053688 | Chain on iter20-HPO              |
| `iter20-jra-cb-perclass-703-hpo-v8` | 0.050329 | Per-class HPO variant            |
| `iter20-jra-cb-perclass-703-v8`     | 0.004598 | Per-class default (near-zero)    |

Manifest: `apps/finish-position-predict-container/models/finish-position/jra/per-class/703/iter23-jra-cb-ensemble-703-v8/manifest.json`.

Other iter 23 ACCEPT classes (005 / 701 / other) have `delta=+0.000pp` — tied
with iter14, not truly winning. iter 23 010 / 016 REJECT.

User dual-constraint reminder:

1. **Model separation by detailed race classification**: the 703 ensemble is a
   `kyoso_joken_code`-scoped routing target — fits the per-class architecture
   from Phase B-1 exactly.
2. **Do not decrease accuracy**: the 20% iter14 floor guarantees the ensemble
   degrades gracefully toward iter14 if member boosters perform badly out of
   sample.

## 2. Prerequisites for activation

All of the following must hold before the production cron run picks up the
ensemble. Each item ships independently and can be staged in any order, but
the PG activation row (Phase D) is the final cutover step.

### A. Single-shot deploy models for iter 20 / 21 / 22 members

Currently only **per-fold walk-forward (WF)** artifacts exist for the four
non-iter14 ensemble members. Daily production predicts FUTURE races where no
fold_year applies, so single-shot models trained on the FULL 21-year window
(2007-2026) are required.

Required artifacts (each as `model.json` + `metadata.json`):

| Model version                       | Source iter | Notes                               |
| ----------------------------------- | ----------- | ----------------------------------- |
| `iter20-jra-cb-perclass-703-v8`     | iter 20     | Per-class default (703 subset only) |
| `iter20-jra-cb-perclass-703-hpo-v8` | iter 20 HPO | Optuna-tuned hyperparams            |
| `iter21-jra-cb-chain-703-v8`        | iter 21     | Chain on iter20-HPO                 |
| `iter22-jra-cb-residual-703-v8`     | iter 22     | Residual fit on iter14 scores       |

Output path convention (already used by iter14):

```
apps/finish-position-predict-container/models/finish-position/jra/per-class/703/{model_version}/
├── model.json
└── metadata.json
```

iter14 reference: `apps/finish-position-predict-container/models/finish-position/jra/per-class/703/iter14-jra-cb-pacestyle-course-v8/model.json` (mirror this layout for the new members).

Estimated training time on the M5 Pro: ~5-15 min per model (single-shot
CatBoost YetiRank on ~80k 703-subset rows × ~280 features).

### B. Container code changes (Phase B-2)

Phase B-1 added single-model routing. Phase B-2 must extend it to ensemble
routing:

**`apps/finish-position-predict-container/src/predict_lib/per_class.py`**

- Add frozen dataclasses:

  ```python
  @dataclass(frozen=True)
  class EnsembleMember:
      model_version: str
      weight: float
      is_baseline: bool

  @dataclass(frozen=True)
  class PerClassEnsemble:
      model_version: str  # the ensemble's own label, e.g. iter23-jra-cb-ensemble-703-v8
      members: tuple[EnsembleMember, ...]
  ```

- Add `load_ensemble_manifest(category, kyoso_joken_code) -> PerClassEnsemble | None`
  that reads `models/finish-position/{category}/per-class/{code}/{model_version}/manifest.json`
  and returns a `PerClassEnsemble`. Returns `None` on missing file, invalid
  JSON, or any member-validation failure — callers fall back to the single
  model path.
- Extend `resolve_per_class_model_version` (or add a sibling
  `resolve_per_class_ensemble`) to return the ensemble manifest when one is
  registered for `(category, kyoso_joken_code)`.

**`apps/finish-position-predict-container/src/predict_lib/scorer.py`**

- Add `score_with_ensemble(ensemble: PerClassEnsemble, features_df) -> np.ndarray`:
  1. For each member, load its booster via the booster pool.
  2. Score each member to produce per-row raw scores.
  3. Rank-normalise within race (reuse
     `apps/pc-keiba-viewer/src/scripts/finish-position-features/per_class_ensemble_lib.py::normalize_within_race`
     — copy the implementation into the container; do NOT import across
     packages).
  4. Weighted blend (reuse `blend_normalized` from the same lib).
  5. Return blended ranks as the final `score` column.
- Existing single-model path stays untouched.

**`apps/finish-position-predict-container/src/predict_upcoming.py`**

- For each race in the daily loop:
  1. Extract `kyoso_joken_code` (already done in Phase B-1).
  2. Call `resolve_per_class_ensemble(category, code)`.
  3. If result is non-`None` → use ensemble scoring path; UPSERT
     `model_version` = ensemble's own label (`iter23-jra-cb-ensemble-703-v8`).
  4. Else → existing `resolve_per_class_model_version` single-model path.

**Booster pool (`predict_lib/booster_pool.py` or equivalent loader)**

- At container startup, pre-load every booster referenced by every registered
  ensemble manifest (from `per_class_codes_for("jra")` + manifest lookups).
- Keyed by `model_version`; one booster per key.
- On any member load failure → log + fall back to iter14 alone for that
  ensemble (NEVER hard-fail the daily cron).

### C. Tests (≥ 95% coverage gate per `apps/finish-position-predict-container`)

| Test file                                             | New coverage                                                                       |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `tests/test_per_class.py`                             | `load_ensemble_manifest` happy path, missing file, malformed JSON, bad weights.    |
| `tests/test_scorer.py`                                | `score_with_ensemble` rank-normalisation, weighted blend, single-member edge case. |
| `tests/test_predict_upcoming.py`                      | Integration: race with 703 code routes to ensemble path; race without falls back.  |
| `tests/test_booster_pool.py` (new or extend existing) | Pre-load all registered ensemble members; missing member triggers fallback path.   |

Coverage thresholds enforced by `apps/finish-position-predict-container/pyproject.toml`
(`--cov-fail-under=95`). No `# type: ignore`, `# noqa`, or `/* v8 ignore */` may
be added — fix lint warnings in code.

### D. Dockerfile updates

- COPY the four new single-shot models into the container under
  `/app/models/finish-position/jra/per-class/703/`.
- COPY the manifest at
  `/app/models/finish-position/jra/per-class/703/iter23-jra-cb-ensemble-703-v8/manifest.json`
  (already on disk, just needs COPY layer).
- Image-size impact: 4 boosters × ~10-50 MB each → **+~50-200 MB**. Cloudflare
  Container 2GB limit comfortably accommodates this.

### E. PG schema (already in place from Phase B-1)

Migration `apps/local-postgresql/sql/20260605000000_add_subclass_to_finish_position_active_models.sql`
already added the `subclass text` column + composite unique index on
`(category, coalesce(subclass, ''))`. No new migration required.

## 3. Step-by-step activation procedure

### Phase A — Single-shot deploy training (offline, no production impact)

Note: the `tmp/v8/deploy_train_*_single.py` scripts for iter 20 / 21 / 22 do
NOT exist yet. They must be created mirroring
`tmp/v8/deploy_train_iter14_jra_single.py` but with:

- iter-specific feature set (per-class feature filter for iter 20, residual
  target for iter 22, chain previous-model score for iter 21).
- `--class-filter 703` to restrict training rows to JRA 未勝利戦.
- Full 21-year training window (no fold split).

Training loop:

```sh
for MV in iter20-jra-cb-perclass-703-v8 iter20-jra-cb-perclass-703-hpo-v8 \
          iter21-jra-cb-chain-703-v8 iter22-jra-cb-residual-703-v8; do
  uv run --project apps/pc-keiba-viewer python tmp/v8/deploy_train_${MV%-v8}_single.py \
    --class-filter 703 \
    --model-version "$MV" \
    --output-dir "apps/finish-position-predict-container/models/finish-position/jra/per-class/703/$MV"
done
```

Verify each output directory contains both `model.json` and `metadata.json`
before proceeding.

### Phase B — Container ensemble routing code

Implement section 2.B in `predict_lib/per_class.py`, `predict_lib/scorer.py`,
`predict_upcoming.py`, and the booster-loading code. Write tests in the same
turn (see section 2.C — `feedback_update_tests_with_features`).

Local coverage gate:

```sh
bun run --filter finish-position-predict-container python:check
```

Must report 0 lint warnings, 0 type errors, and ≥ 95% on all 4 coverage
metrics over `predict_lib/`.

### Phase C — Local container smoke test

```sh
cd apps/finish-position-predict-container
docker build -t finish-position-predict-local:per-class-ensemble ../..

# Verify routing table
docker run --rm --entrypoint python finish-position-predict-local:per-class-ensemble -c "
from predict_lib.per_class import (
    resolve_per_class_model_version,
    load_ensemble_manifest,
    PER_CLASS_MODEL_VERSIONS,
)
print('per-class registry:', PER_CLASS_MODEL_VERSIONS)
print('703 ensemble:', load_ensemble_manifest('jra', '703'))
print('005 fallback:', resolve_per_class_model_version('jra', '005'))
"

# Verify member booster files load
docker run --rm --entrypoint python finish-position-predict-local:per-class-ensemble -c "
from predict_lib.booster_pool import preload_all
preload_all()
print('booster pool ready')
"
```

Expected: 703 ensemble manifest loads with 5 members; member booster files
exist; fallback for unregistered classes returns iter14.

### Phase D — PG row insertion (production)

Local PG dry-run first:

```sh
docker exec horse-racing-local-postgresql psql -U horse_racing -d horse_racing -c "
insert into finish_position_active_models (category, subclass, model_version)
  values ('jra', '703', 'iter23-jra-cb-ensemble-703-v8')
  on conflict (category, coalesce(subclass, ''))
  do update set model_version = excluded.model_version, activated_at = now();
"

docker exec horse-racing-local-postgresql psql -U horse_racing -d horse_racing -c "
select category, subclass, model_version, activated_at
from finish_position_active_models
where category = 'jra' order by subclass nulls first;
"
```

Expected output: two rows — `(jra, NULL, iter14-...)` fallback +
`(jra, 703, iter23-jra-cb-ensemble-703-v8)`.

After local dry-run passes, run the same upsert against the production NEON
database via the same upsert builder
(`buildActivatePerClassModelSql` in `apps/pc-keiba-viewer/src/scripts/finish-position-features/import-predictions-sql.ts`).

### Phase E — Production deploy + monitor

```sh
# Tag + push container image (user runs the actual push in their normal pipeline)
docker tag finish-position-predict-local:per-class-ensemble \
  <registry>/finish-position-predict-container:per-class-ensemble-v1
```

Wait for the next daily launchd cron run
(`scripts/launchd/com.kkk4oru.finish-position-predict.plist`, JST 03:00).
After it completes, verify:

```sh
docker exec horse-racing-local-postgresql psql -U horse_racing -d horse_racing -c "
select model_version, count(*) as preds
from race_finish_position_model_predictions
where source = 'jra' and inserted_at > now() - interval '24 hours'
group by 1 order by 1;
"
```

Expected: distinct `model_version` values include
`iter23-jra-cb-ensemble-703-v8` for the 703 races and
`iter14-jra-cb-pacestyle-course-v8` for the rest.

## 4. Rollback procedure

`feedback_no_data_delete` forbids `DELETE` against the active-models table.
Use UPDATE to revert the 703 row's `model_version` back to iter14 — routing
semantics then equal "no per-class registration":

```sql
update finish_position_active_models
  set model_version = 'iter14-jra-cb-pacestyle-course-v8', activated_at = now()
  where category = 'jra' and subclass = '703';
```

Container-side rollback (independent of PG):

- Remove the 703 ensemble registration from `PER_CLASS_MODEL_VERSIONS` (or the
  equivalent ensemble-registry dict) in `predict_lib/per_class.py`.
- Rebuild + redeploy the container image.
- Next cron run reverts 703 to iter14 alone.

Both layers are safe to leave dual-active during rollback: the PG row points
to iter14 _and_ the container code emits iter14, so the viewer and the daily
cron stay consistent.

## 5. Monitoring checklist (post-activation)

For 7 days after activation:

- **Daily**: check `race_finish_position_model_predictions` distinct
  `model_version` distribution. Expect roughly the 703 race fraction (~10-15%
  of a typical JRA day) labelled with the ensemble model_version, remainder
  iter14.
- **Daily**: scan launchd cron logs (`scripts/launchd/`) for any
  "ensemble member load failed → fell back to iter14" warnings. A persistent
  fallback warning means one of the single-shot models is missing or
  corrupted.
- **Weekly (Mon JST)**: once race results land, compute 703-subset top1
  accuracy from `race_finish_position_model_predictions` joined against
  `pg.nvd_se` finishers. Compare to the iter14 historical baseline on the same
  window.
- **Alert threshold**: if delta_top1 falls below **-0.5pp** (below the
  half-width of the holdout 95% CI), suspect regression — investigate before
  the next iter and consider rollback.

## 6. Effort estimate

| Phase                               | Estimate       |
| ----------------------------------- | -------------- |
| A. Single-shot training (4 models)  | ~1 hour        |
| B. Container code + tests           | ~3-4 hours     |
| C. Local smoke test                 | ~30 min        |
| D. PG row insert (local + prod)     | ~5 min         |
| E. Production deploy + first verify | ~30 min        |
| **Total focused implementation**    | **~5-6 hours** |

## 7. Risk assessment

- **Container image size**: +~200 MB acceptable within Cloudflare Container 2
  GB limit; well within Mac launchd container budget.
- **Memory at startup**: 4 additional CatBoost boosters in JSON form ≈ 50-100
  MB each in RAM → +~200-400 MB resident. Within container memory budget.
- **Inference latency**: ensemble path requires 4 additional booster
  scoring calls + per-race rank normalisation + weighted blend → +~50-100 ms
  per 703 race. Negligible vs. feature build (~minutes per day).
- **Failure mode — missing member booster**: Phase B-2 code MUST catch the
  load failure, log it, and fall back to scoring the race with iter14 alone.
  Daily cron MUST NOT crash because one ensemble member is unavailable.
- **Statistical noise**: +0.142pp holdout gain has half-width ≈ 0.7pp at 95%
  CI on n=4229. Real-world gain may sit anywhere in roughly ±0.5pp around the
  point estimate. The 20% iter14 floor keeps the realised loss bounded even if
  the gain evaporates.
- **Per-class drift**: 未勝利戦 distribution shifts year-over-year (3-year-old
  composition vs older). iter22 residual was fit on 2007-2022; out-of-sample
  decay risk is real. Monitoring (section 5) catches this early.

## 8. Other ACCEPT classes (005 / 701 / other)

iter 23 also marked classes 005 / 701 / other as ACCEPT, but each delta is
exactly **+0.000pp** — tied with iter14, not truly winning. Activating them
would:

- Add container complexity (more ensemble manifests, more boosters to load).
- Inflate image size with no measurable benefit.
- Multiply the surface area for member-booster load failures.

**Recommendation**: ship 703 only in the initial Phase B-2 cutover. The 005 /
701 / other manifests remain on disk as documentation of the search outcome
and as starting points for future iters that find a TRUE win on those
subsets. They are NOT scheduled for activation.

iter 23 010 / 016 REJECTED outright — those classes stay on iter14 with no
manifest at all.

## 9. References

- iter 23 manifest: `apps/finish-position-predict-container/models/finish-position/jra/per-class/703/iter23-jra-cb-ensemble-703-v8/manifest.json`
- Phase B-1 routing skeleton: commit `6951015` (per_class.py + PG migration)
- Phase B-1 runbook: `docs/finish-position-accuracy/runbook/PER_CLASS_ROUTING.md`
- iter 23 loop state: `tmp/v8/state.json`
- v8 production deploy: commit `b0e6aad` (iter12-NAR + iter14-JRA full deploy)
- iter14 single-shot trainer (template for new deploy*train*\* scripts):
  `tmp/v8/deploy_train_iter14_jra_single.py`
- Per-class ensemble normalisation/blend reference implementation:
  `apps/pc-keiba-viewer/src/scripts/finish-position-features/per_class_ensemble_lib.py`
- v7-lineage saturation context (why per-class became the next lever):
  memory entry `project_v7_lineage_saturation_2026_06_04.md`
