# Per-Class JRA Model Routing (Phase B Architecture)

- **Date introduced**: 2026-06-05 (JST)
- **Scope**: JRA finish-position prediction routing. NAR / Ban-ei are explicitly
  excluded from per-class routing.
- **Status**: Architecture deployed; **registry is empty** — every JRA class
  currently routes to the category-global fallback model
  (`iter14-jra-cb-pacestyle-course-v8`). Container behaviour at runtime is
  identical to the pre-architecture state.

## Summary

The v8 production deploy uses a single global model per category. Phase B adds a
second routing axis — `kyoso_joken_code` (JRA race-class code: 000, 005, 010,
016, 701, 703, ...) — so that future per-class winners can be activated
piecemeal without disturbing classes that have no per-class winner yet.

v8 iter 20 (2026-06-04) confirmed that all six candidate per-class JRA models
(005 / 010 / 016 / 703 / 701 / other) lose to iter 14 globally on their own
subsets. The Phase B architecture is therefore deployed with an **empty
registry**: every class falls back to iter 14, which is what production was
already serving.

User dual-constraint reminder:

1. **Model separation by detailed race classification**: architecture YES — a
   future per-class winner can be activated via a single PG row insert.
2. **Do not decrease accuracy**: no per-class model beats iter 14 on its own
   subset yet, so the registry stays empty until a winner emerges.

## Implementation pieces

| Component                                                                                    | Role                                                                                                                                                                     |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `apps/finish-position-predict-container/src/predict_lib/per_class.py`                        | Pure helpers: `resolve_per_class_model_version`, `per_class_codes_for`, `is_per_class_enabled_for`, plus the `PER_CLASS_MODEL_VERSIONS` registry (currently empty).      |
| `apps/finish-position-predict-container/src/predict_upcoming.py`                             | Per-race scoring loop calls `resolve_per_class_model_version(category, race_kyoso_joken_code)` and passes the result to `build_prediction_rows`.                         |
| `apps/finish-position-predict-container/src/predict_lib/upcoming.py`                         | `build_prediction_rows` accepts an optional `model_version` override; defaults to `model_version_for(category)` for backwards compatibility.                             |
| `apps/local-postgresql/sql/20260605000000_add_subclass_to_finish_position_active_models.sql` | Idempotent PG migration: adds `subclass text` column + replaces the `category` PK with a unique index on `(category, coalesce(subclass, ''))`.                           |
| `apps/pc-keiba-viewer/src/scripts/finish-position-features/import-predictions-sql.ts`        | Updated DDL + activation SQL builders (`buildAddSubclassColumnSql`, `buildDropLegacyPkSql`, `buildActiveModelsSubclassUniqueIndexSql`, `buildActivatePerClassModelSql`). |
| `apps/pc-keiba-viewer/src/db/queries.ts`                                                     | Viewer lookups now filter `subclass is null` so they always read the category-global fallback row (Phase C will add per-class lookup paths).                             |

## Routing semantics

For each race in the daily prediction loop:

1. Extract `kyoso_joken_code` from the first feature-parquet entry (all entries
   in a race share the same race-class).
2. Call `resolve_per_class_model_version(category, kyoso_joken_code)`:
   - If `category` is NOT in `PER_CLASS_ENABLED_CATEGORIES` (NAR, Ban-ei) →
     return `MODEL_VERSION_BY_CATEGORY[category]`.
   - If `kyoso_joken_code` is `None` / empty → return
     `MODEL_VERSION_BY_CATEGORY[category]`.
   - If `(category, kyoso_joken_code)` is not in `PER_CLASS_MODEL_VERSIONS` →
     return `MODEL_VERSION_BY_CATEGORY[category]`.
   - Otherwise return the registered per-class `model_version`.
3. Use the resolved `model_version` for both scoring (Phase C will pre-load a
   per-class booster pool here) and UPSERT metadata.

For Phase B the booster pool is **not** extended — the single fallback booster
covers every race because the registry is empty. When Phase C registers the
first per-class winner, the booster-loading code in `_load_booster` will need
to load per-class artifacts under
`apps/finish-position-predict-container/models/finish-position/jra/per-class/{code}/{model_version}/`
and route at scoring time. The `predict_lib.per_class.per_class_codes_for`
helper exists specifically for that pre-load step.

## PG schema after migration

```sql
-- finish_position_active_models (post-migration)
--   category text not null
--   subclass text                  -- NULL = category-global fallback
--   model_version text not null
--   activated_at timestamptz not null default now()
--
-- Unique constraint: (category, coalesce(subclass, ''))
--   -> one NULL-subclass row per category (fallback)
--   -> plus one row per (category, registered subclass)
```

Existing rows (one per category, subclass NULL) are preserved by the migration:
they are the category-global fallback rows that drive current production. The
migration is idempotent — running it twice is safe.

## How to register a per-class winner (future Phase C)

When a future iter produces a per-class JRA model that beats iter 14 on its own
subset under the 4-axis accept gate (top1 / place2 / place3 / top3_box):

1. **Train + save the booster artifact** under
   `apps/finish-position-predict-container/models/finish-position/jra/per-class/{code}/{model_version}/model.json`
   and `metadata.json`.
2. **Add the model_version registration to `predict_lib/per_class.py`**:
   ```python
   PER_CLASS_MODEL_VERSIONS: Final[dict[tuple[Category, str], str]] = {
       ("jra", "005"): "iter21-jra-cb-class005-v8",
   }
   ```
   Tests in `tests/test_per_class.py` already cover this code path via
   monkeypatch — add a real-production test only when the registry settles.
3. **Extend container `_load_booster` to pre-load per-class boosters** for the
   codes returned by `per_class_codes_for("jra")` and route at scoring time.
4. **Insert the PG activation row** so the viewer (and any downstream consumer
   reading `finish_position_active_models`) reports the per-class winner for
   that class:
   ```sql
   insert into finish_position_active_models (category, subclass, model_version)
     values ('jra', '005', 'iter21-jra-cb-class005-v8')
     on conflict (category, coalesce(subclass, ''))
     do update set model_version = excluded.model_version, activated_at = now();
   ```
   The same upsert is produced by the TS builder `buildActivatePerClassModelSql`.
5. **Update the Dockerfile** to COPY the per-class model directory into
   `/models/finish-position/jra/per-class/{code}/`.
6. **Rebuild + redeploy the container image** so the daily launchd run picks up
   the new artifact.

## Rollback procedure

To revert a per-class registration:

1. **PG**: `DELETE` is forbidden by `feedback_no_data_delete`. Instead, UPDATE
   the per-class row to point back at the category-global model. Since the
   row's `model_version` then equals the fallback, the routing semantics are
   indistinguishable from no registration:
   ```sql
   update finish_position_active_models
     set model_version = 'iter14-jra-cb-pacestyle-course-v8', activated_at = now()
     where category = 'jra' and subclass = '005';
   ```
2. **Container code**: remove the `("jra", "005"): ...` entry from
   `PER_CLASS_MODEL_VERSIONS` in the next deploy; in the meantime the dual-write
   (PG row points to fallback model_version) keeps the viewer correct.

## Coverage and quality rules

- Per `apps/pc-keiba-viewer/CLAUDE.md` and the container's `pyproject.toml`,
  every metric on `predict_lib/` and `predict_lib/per_class.py` must stay
  ≥ 95%. As of Phase B both are at 100%.
- No `// oxlint-disable*`, `# type: ignore`, `/* v8 ignore */`, or
  `# noqa` may be added. Lint warnings must be resolved in code.
- The TS builders in `import-predictions-sql.ts` are exercised by
  `import-predictions-sql.test.ts`; new builders MUST have a matching test
  case.
- Migration files are idempotent and never DELETE / TRUNCATE / DROP data.

## References

- v8 iter 20 reject (per-class JRA candidates lose to iter 14 globally):
  `tmp/v8/state.json` (loop history)
- v8 production deploy: commit `b0e6aad` (iter12-NAR + iter14-JRA full deploy)
- Phase 1 production flip (NAR): `phase1-nar-production-flip-2026-06-04.md`
- Container `predict_upcoming.py` flow: see top-of-file docstring (Phase B
  additions inline)
