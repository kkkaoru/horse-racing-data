# Phase 1 — NAR Production Flip to iter9-nar-xgb-pacestyle-v8

- **Date**: 2026-06-04 (JST)
- **Operator**: SubAgent (autonomous, Phase 1 dispatch from v8 iterative loop orchestrator)
- **Scope**: PG-level production flip for **NAR** only. JRA and Ban-ei unchanged.
- **Status**: Partial flip — historical predictions UPSERTed + `finish_position_active_models.nar` updated. Daily container scoring **NOT yet** deployed (see "Phase 1b deferred" below).

## Summary

The v8 11-iteration sweep produced one ACCEPT — **iter 9** — gating on NAR. Per user
instruction, we flipped the NAR active model in PG so the viewer (and any downstream
consumer reading `finish_position_active_models`) immediately starts reporting on the
new model. JRA active model is unchanged at `jra-cb-v7-lineage` because no JRA-only
ACCEPT occurred in the v8 sweep.

## Pre/Post-flip state (`finish_position_active_models`)

| category | pre-flip (snapshot)  | post-flip                        |
| -------- | -------------------- | -------------------------------- |
| `jra`    | `jra-cb-v7-lineage`  | `jra-cb-v7-lineage` (unchanged)  |
| `nar`    | `nar-xgb-v7-lineage` | `iter9-nar-xgb-pacestyle-v8`     |
| `ban-ei` | `ban-ei-cb-v7-grade` | `ban-ei-cb-v7-grade` (unchanged) |

Pre-flip snapshot: `tmp/v8/phase1-pre-flip-snapshot.json` (use for autonomous revert
if smoke regression is detected within the autonomous window).

## Gains vs v7-lineage baseline (from iter 9 metrics, commit 57e5b97)

NAR macro delta (vs `nar-xgb-v7-lineage`):

| metric   | delta (pp) |
| -------- | ---------: |
| top1     |     -0.039 |
| place2   |     +0.061 |
| place3   |     +0.088 |
| top3_box |     +0.012 |

ACCEPT decision driven by place2 + place3 gains. top1 regression of -0.039pp
accepted by the loop policy because place2/place3 are the primary objective for
NAR pari-mutuel.

## What was done

1. Snapshotted pre-flip active model into `tmp/v8/phase1-pre-flip-snapshot.json`.
2. Converted iter 9 NAR predictions parquets (per-year, 21 years) to JSONL via
   `tmp/v8/flip_iter9_nar_to_jsonl.py` (DuckDB-driven streaming, 2,571,888 rows).
3. UPSERTed JSONL into `race_finish_position_model_predictions` with
   `model_version = 'iter9-nar-xgb-pacestyle-v8'` via the existing TS importer
   `apps/pc-keiba-viewer/src/scripts/finish-position-features/import-finish-position-predictions.ts`
   (idempotent ON CONFLICT DO UPDATE).
4. UPDATED `finish_position_active_models.nar -> iter9-nar-xgb-pacestyle-v8`
   via the same importer's `--activate-category nar` flag.
5. Verified row count + active model state via psycopg.
6. Updated `tmp/v8/state.json` with `production_flipped_nar = true` and
   `production_flip_partial = true`.

## Phase 1b deferred — daily container scoring still uses v7-lineage

The iter 9 NAR training driver `tmp/v8/iter9_train_predict.py` saved
only `metadata.json` + per-fold `predictions.parquet`. It did **not** call
`booster.save_model()`, so no XGBoost artifact exists on disk that could be
copied into `apps/finish-position-predict-container/models/finish-position/nar/`.

Until Phase 1b ships, the practical effect is:

- **Viewer historical pages**: show iter 9 NAR predictions (model_version flipped, predictions written).
- **Daily 03:00 JST container scoring** (Mac launchd `com.kkk4oru.finish-position-predict.plist`):
  still uses the v7-lineage NAR baked-in model artifact inside the container image.

To complete Phase 1b, the following must happen:

1. Re-run iter 9 NAR training with `booster.save_model(...)` added — preferably
   producing one final-fold model trained on all 21 years (no held-out fold).
2. Place artifact under
   `apps/finish-position-predict-container/models/finish-position/nar/iter9-nar-xgb-pacestyle-v8/`
   following existing v7-lineage layout.
3. Update `apps/finish-position-predict-container/Dockerfile` to reference the
   new model directory.
4. Rebuild + redeploy the container image to whatever serving target the launchd
   plist invokes.

Phase 1b is **not auto-dispatched** in this run; it is queued for the next
orchestrator pass once iter 11 outcomes are known.

## Revert procedure (if smoke regression detected)

```python
import psycopg
con = psycopg.connect(
    host="127.0.0.1", port=15432,
    user="horse_racing", password="horse_racing", dbname="horse_racing")
con.execute(
    "UPDATE finish_position_active_models "
    "SET model_version = 'nar-xgb-v7-lineage', activated_at = NOW() "
    "WHERE category = 'nar'")
con.commit()
```

Predictions rows for `iter9-nar-xgb-pacestyle-v8` may stay in PG — they are
inert once the active model points back to v7-lineage. Per repo policy
(`feedback_no_data_delete`), no DELETE is performed during revert.

## References

- iter 9 ACCEPT commit: `57e5b97`
- iter 9 NAR predictions: `tmp/bucket-eval/finish-position/iter9-nar-xgb-pacestyle-v8/predictions/category=nar/race_year=*/predictions.parquet`
- iter 9 NAR metadata: `tmp/bucket-eval/finish-position/iter9-nar-xgb-pacestyle-v8/metadata/category=nar/race_year=*/metadata.json`
- v8 loop state: `tmp/v8/state.json`
- Plan reference: `~/.claude/plans/imperative-riding-wave.md`
