# Deploy runbook — daily finish-position prediction (Cloudflare Cron + Container)

Automated daily serving of UPCOMING-race finish-position predictions with the
retrained **v7-lineage** models. Scheduling is a **Cloudflare Cron Trigger**
(GitHub Workflow for prediction is FORBIDDEN by project rule). The feature build
needs Python / DuckDB / CatBoost / XGBoost, which a plain Worker cannot run, so a
**Cloudflare Container** (`finish-position-predict-container`) does the work and a
tiny **Cron Trigger Worker** (`finish-position-cron`) starts it on schedule.

This file is a runbook. None of it has been run for you — **no `wrangler deploy`,
no image push, no secrets set.** Run the steps below from your machine, logged in
to your own Cloudflare account, with Docker running.

> Credentials are never written here. Use `wrangler secret put` and env refs.
> The Neon connection string lives ONLY as the `NEON_DATABASE_URL` Worker secret.

---

## Architecture

```
Cron Trigger ("0 18 * * *" = JST 03:00)
        │
        ▼
finish-position-cron Worker   ── scheduled(event) ──►  getContainer(...).start({
  (apps/finish-position-cron)                              entrypoint: predict_upcoming.py,
        │                                                  envVars: { NEON_DATABASE_URL, RUN_DATE, ... },
        │                                                  enableInternet: true })
        │                                              ── writes 1 "started" audit row to D1
        ▼
FinishPositionPredictContainer (Durable Object, standard-4: 4 vCPU / 12 GiB / 20 GB)
  image = apps/finish-position-predict-container/Dockerfile
        │
        ▼ predict_upcoming.py, per category (jra / nar / ban-ei):
  1. build v7-lineage feature parquet for TODAY's races (DuckDB base in
     --target-date mode + v7 layer scripts, reused unchanged) — see "Today's
     races feature build" below
  2. load model from R2 finish-position/{category}/{modelVersion}/model.json
  3. score → rank within race → dedupe → chunked UPSERT into
     race_finish_position_model_predictions  (model_version = {category}-v7-lineage-wf-21y)
  4. write 1 detailed audit row to finish_position_cron_executions
```

### Today's races feature build (UPCOMING + already-run)

The feature build is driven by `RUN_DATE` (JST `YYYYMMDD`, set by the Worker)
and `PREDICT_DAYS_AHEAD`. `pipeline_runner.py` invokes the reused base
builder in `--target-date` mode:

```sh
python finish_position_features_duckdb.py \
  --category {jra|nar|ban-ei} \
  --target-date $RUN_DATE --days-ahead $PREDICT_DAYS_AHEAD \
  --pg-url "$NEON_DATABASE_URL" --output-dir <base>
```

`--target-date` makes the build emit feature rows for **every** race on
[RUN_DATE, RUN_DATE + days-ahead], including UPCOMING races whose
`finish_position` is still NULL. It is leakage-safe by construction: all
historical aggregates join prior races only (`h.race_date < t.race_date` with
`finish_position is not null` on the history side), so a target race's own
outcome is never used and the vector is computable before the race is run. To
cover today's races that the derived `race_entry_corner_features` table has not
yet been refreshed with, `--target-date` mode also pulls the day's target rows
straight from `jvd_se`/`jvd_ra` (JRA) and `nvd_se`/`nvd_ra` (NAR / Ban-ei),
deduped against the corner-feature rows (corner-feature row wins when present).
Each layer then LEFT-JOINs its history, preserving UPCOMING rows with NULL/0
history features — exactly how the model saw NULLs in training (CatBoost/XGBoost
treat absent numeric inputs as 0; the scorer fills any absent feature with 0.0 in
the `metadata.json` `feature_names` order, so vector order parity is
guaranteed).

> **Full feature parity — wired.** `pipeline_args.LAYER_CHAIN` runs the COMPLETE
> per-category chain that reproduces the exact feature set each model was trained
> on (validated against `metadata.json` `feature_names`: 226 JRA / 175 NAR / 111
> Ban-ei, **0 names missing → no missing-layer zero-fill**). The chains are:
>
> - **JRA (226)** — base → `add-race-internal` → `add-market-signal` →
>   `add-sectional-and-weight` → `add-futan-juryo` → `add-workout` →
>   `add-near-miss` → `add-grade-race-lineage` → `add-head-to-head` →
>   `add-baba-pedigree-affinity` → `add-trainer-stable-affinity`.
> - **NAR (175)** — base → `add-race-internal` → `add-near-miss` →
>   `add-grade-race-lineage` → `add-head-to-head` → `add-baba-pedigree-affinity`
>   (NAR was never built with the market / sectional / futan / workout layers, and
>   the trainer layer is dropped — it is counter-productive on NAR per
>   FINISH_POSITION_MODEL_V7_LINEAGE.md §8).
> - **Ban-ei (111)** — base → `add-grade-race-lineage` → `add-head-to-head` →
>   `add-baba-pedigree-affinity` → `add-banei-futan-class` →
>   `add-banei-grade-career` (distinct base; no JRA v6 layers).
>
> Per-layer flags are shaped in `pipeline_args.build_layer_argv`: the pure-DuckDB
> `add-race-internal` layer takes only `--input-dir`/`--output-dir`;
> Postgres-reading layers also take `--pg-url`/`--from-date`; the lineage layer
> adds `--config lineage-races/{category}.json`; the trainer layer adds
> `--category {jra,nar}`. The Dockerfile already COPYs the entire
> `finish-position-features/` directory, so every chain script (and the
> `lineage-races/` configs + `_resource_defaults.py`) is present in the image.
>
> The one-shot "v3 merger" in FINISH_POSITION_MODEL_V6_STACKED.md §2 only
> re-prioritised the VALUE of market-signal columns that `add-market-signal`
> already computes straight from Postgres; it adds no new feature NAMES and is not
> part of the automated 21y v7 build, so it is intentionally not reproduced.
> Columns sourced from `race_entry_corner_features` (speed indices, corner
> positions, weather, odds, baba condition) are still NULL for races whose entries
> / odds the realtime pipeline has not yet ingested — that is intrinsic
> pre-race data availability, not a missing-layer gap.

### Why these choices (Cloudflare docs, 2026)

- Containers are GA. `containers[]` in `wrangler.jsonc` pairs with a Durable
  Object binding + `migrations.new_sqlite_classes`
  (`/containers/get-started/`, `/sandbox/get-started/`).
- `scheduled()` → `getContainer(env.BINDING, name).start({ entrypoint, envVars,
enableInternet })` is the documented batch / cron container pattern
  (`/containers/container-class/` — `start()` example is literally a
  `scheduled` nightly job).
- `standard-4` (4 vCPU / 12 GiB / 20 GB) is the heaviest predefined instance type
  (changelog 2025-10-01); custom types (changelog 2026-01-05) can go to the same
  ceiling if you need to tune.
- Secrets reach the container as env vars via `start({ envVars })`
  (`/containers/examples/env-vars-and-secrets/`).
- `wrangler deploy` builds the image and pushes it to the Cloudflare Registry
  automatically when Docker is running locally (`/containers/get-started/`).

---

## Prerequisites

- bun / bunx + uv (no npm/npx).
- Docker Desktop (or Colima) running locally — required for `wrangler deploy` to
  build + push the container image. Verify with `docker info`.
- `wrangler login` to your Cloudflare account (account_id `78109ec18c7c85b194b19fb32e3bb149`).
- The v7-lineage models already uploaded to R2 bucket
  `pc-keiba-finish-position-models` under
  `finish-position/{jra,nar,ban-ei}/{modelVersion}/model.json` + `metadata.json`
  (done in Stage 6a of `FINISH_POSITION_MODEL_V7_LINEAGE.md` §10).
- A Neon (Postgres) connection string for the production DB.

---

## Step 1 — create the audit D1 database

```sh
cd apps/finish-position-cron
bunx wrangler d1 create finish-position-cron-db
```

Copy the printed `database_id` into `apps/finish-position-cron/wrangler.jsonc`,
replacing `REPLACE_WITH_D1_DATABASE_ID`. Then apply the migration:

```sh
bun run --filter finish-position-cron d1:migrate
```

This creates `finish_position_cron_executions` (insert-only audit — never
DELETE / TRUNCATE / DROP, per project rule).

## Step 2 — grant the Worker the model R2 bucket (read)

The container reads models from R2. If you want the Worker (rather than the
container's own egress) to stage models, add the bucket binding. In this scaffold
the container fetches models over HTTPS directly using its own egress; if you
prefer a Worker R2 binding, add to `wrangler.jsonc`:

```jsonc
"r2_buckets": [
  { "binding": "FINISH_POSITION_MODELS", "bucket_name": "pc-keiba-finish-position-models" }
]
```

and have the deploy/warm step copy the model objects into the container's
`MODELS_DIR` (`/models`) at start. Either path is acceptable; pick one before
deploy.

## Step 3 — set the Neon secret (never commit it)

```sh
cd apps/finish-position-cron
bunx wrangler secret put NEON_DATABASE_URL
# paste: postgresql://<user>:<password>@<host>/<db>?sslmode=require
```

`PREDICT_DAYS_AHEAD` is a plain var in `wrangler.jsonc` (default `"2"`); change it
there if you want a wider window.

## Step 4 — build + push the image and deploy the Worker

`wrangler deploy` (run from the cron Worker dir) builds the Dockerfile referenced
by `containers[].image` using `image_build_context` (the repo root, so the
Dockerfile can COPY both the container `src/` and the reused
`apps/pc-keiba-viewer` feature-pipeline scripts), pushes it to the Cloudflare
Registry, and deploys the Worker + cron trigger + Durable Object migration.

```sh
# Docker must be running.
cd apps/finish-position-cron
bun run --filter finish-position-cron deploy
```

## Step 5 — verify

1. **Cron registered**: `bunx wrangler deployments list` / dashboard → Worker →
   Triggers shows `0 18 * * *`.
2. **Manual dry-run** (optional, before the first scheduled fire):
   ```sh
   bunx wrangler dev --test-scheduled
   curl "http://localhost:8787/cdn-cgi/handler/scheduled?cron=0+18+*+*+*"
   ```
   (Local containers run without the `max_instances` cap; use a Neon branch /
   throwaway DB for a true dry-run so you do not write into prod.)
3. **Audit rows** after a real fire:
   ```sh
   bunx wrangler d1 execute finish-position-cron-db --remote \
     --command "select run_date, status, races_predicted, duration_ms, error
                from finish_position_cron_executions
                order by recorded_at desc limit 5;"
   ```
   Expect a `started` row from the Worker and (from the container) a `success`
   row with `races_predicted > 0`. An `error` row carries the failure message.
4. **Predictions written** (Neon):
   ```sql
   select model_version, source, count(*)
   from race_finish_position_model_predictions
   where model_version like '%-v7-lineage-wf-21y'
     and prediction_generated_at > now() - interval '1 day'
   group by 1, 2 order by 1, 2;
   ```

## Step 6 — observability

`observability.head_sampling_rate` is `0.1` in `wrangler.jsonc` (mandatory for
new Workers to bound log cost). Watch `wrangler tail finish-position-cron` during
the first scheduled run.

---

## Rollback

1. **Disable the cron** (stop new runs) — remove `triggers.crons` from
   `wrangler.jsonc` and `wrangler deploy`, OR pause the trigger in the dashboard.
2. **Revert the active model** so the viewer serves the previous version
   (predictions already written stay; nothing is deleted):
   ```sql
   update finish_position_active_models
   set model_version = 'jra-cb-v6-stacked', activated_at = now()
   where category = 'jra';
   -- repeat for nar / ban-ei with their previous model_version
   ```
   (Per `FINISH_POSITION_MODEL_V7_LINEAGE.md` §7 — the old eval rows +
   predictions remain, so rollback is immediate.)
3. The container writes are **UPSERTs only**; there is no DELETE / TRUNCATE /
   DROP at any point, so a bad run can be re-run idempotently after a fix rather
   than cleaned up.

---

## What is verified by unit tests vs. at deploy time

- **Unit-tested (CI / pre-commit, ≥ 95% coverage):** the pure logic — `race_id`
  parse, batch dedupe (NAR zero-ketto collision), chunked UPSERT SQL, within-race
  ranking, v7 model-version / R2-key mapping, audit-record builder, feature-row
  projection + float32 quantisation, the upcoming-prediction transform; and on the
  Worker side — the cron-gate decision, JST run-date helpers, container
  start-options builder, audit builder, and the `scheduled()` dispatch (mocked
  Container binding + D1).
- **Verified at deploy time (this runbook):** the real container run — Neon TCP
  read, DuckDB + v7-layer feature build, native CatBoost / XGBoost model load and
  score, and the live UPSERT into Neon. These are I/O boundaries
  (`predict_upcoming.py`, `db_driver.py`, `catboost_adapter.py`,
  `xgboost_adapter.py`, `pipeline_runner.py`) intentionally outside the coverage
  gate.
