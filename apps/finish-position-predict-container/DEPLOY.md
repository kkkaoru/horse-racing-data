# Deploy runbook - finish-position prediction container

Current production authority: Cloudflare only.

Production finish-position generation is owned by Cloudflare Cron / Queue /
Worker / Container, coordinated with `sync-realtime-data`. Mac launchd and local
Docker commands are deprecated local/manual smoke tools only. They are not a
production scheduler, fallback, or ordering dependency.

## Current Production Models

The source of truth is
`apps/finish-position-predict-container/src/predict_lib/model_meta.json` plus the
explicit NAR transformer metadata in `predict_lib/model_meta.py`.

| Category    | Production model_version                      | Notes                                                                            |
| ----------- | --------------------------------------------- | -------------------------------------------------------------------------------- |
| JRA         | `jra-cb-v9-sim-2013-clean`                    | Clean 250-feature default.                                                       |
| JRA cell    | `jra-cb-v9-sim-2013-clean-jockey-pedigree269` | Routed only for `kyoso_joken_code=703`, where the local cell gate improved top1. |
| NAR         | `iter40-nar-settransformer-blend-v1`          | Clean188 XGBoost base plus clean113 Set Transformer score-z fusion.              |
| Ban-ei      | `banei-cb-v9-sim-2011`                        | Default.                                                                         |
| Ban-ei cell | `banei-cb-v8-window2011-wf-15y`               | Routed for `grade_code=E`.                                                       |

Historical leaky JRA/NAR artifacts must not be selected in production. NAR
rollback is `NAR_TRANSFORMER_BLEND_ENABLED=0`, which keeps the leak-free
`iter12-nar-xgb-hpo-v8-clean188` base and disables only the transformer blend.

## Architecture

```text
sync-realtime-data Worker
  feature generation + running-style generation per race
        |
        v
focused per-race full message
        |
        v
finish-position-cron Worker
        |
        v
Cloudflare Queue -> FinishPositionPredictContainer
        |
        v
per-race DuckDB feature build + scoring -> Neon UPSERT
```

The container always scores race by race in production. Day/category local
training or offline prediction generation may batch work, but production
messages must stay focused to one race and include feature generation in the
container path.

## Prerequisites

- `bun` / `bunx` and `uv`; do not use `npm` / `npx`.
- Docker or Colima running locally for `wrangler deploy`, because Wrangler builds
  and pushes the container image.
- Logged-in Wrangler account for this project.
- Worker secrets:
  - `NEON_DATABASE_URL`
  - `TRIGGER_TOKEN`
  - `NAR_TRANSFORMER_BLEND_ENABLED` (`1` for current default-on operation; `0`
    for NAR clean188 base-only rollback)

## Build And Deploy

Run from the cron Worker package so Wrangler picks up the Worker and container
configuration:

```sh
cd apps/finish-position-cron
bun run deploy -- --containers-rollout immediate
```

This builds the Docker image from the repo root build context, bakes
`apps/finish-position-predict-container/models/` into the container, pushes the
image to Cloudflare, and deploys the Worker.

## Verify Deployment

```sh
cd apps/finish-position-cron
bunx wrangler deployments list
bunx wrangler containers list
bunx wrangler secret list
curl -fsS https://finish-position-cron.kaoru.workers.dev/
```

Expected:

- latest deployment is at 100 percent
- container app is `active`
- required secrets exist
- health endpoint returns `{"ok":true,...}`

## Focused Per-Race Smoke

Use a real upcoming race that has source rows. Keep the trigger token in the
environment and never print it.

```sh
curl -fsS -X POST \
  https://finish-position-cron.kaoru.workers.dev/api/admin/run-focused-full-race \
  -H "Authorization: Bearer $FINISH_POSITION_CRON_TRIGGER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"category":"nar","runYmd":"20260710","keibajoCode":"45","raceBango":"03","debug":true}'
```

`accepted` means the container launched detached focused work. Poll Neon for the
target race until prediction rows appear. For NAR transformer production smoke,
the target race must write `model_version='iter40-nar-settransformer-blend-v1'`
with one row per runner and ranks `1..n`. `already-complete` is also a valid
NAR smoke result when those iter40 rows already exist; it proves the Worker and
Container focused completion guards are checking the transformer model, not the
clean188 rollback base.

For the JRA 703 cell, choose a race whose `race_entry_corner_features` rows all
have `source='jra'` and `kyoso_joken_code='703'`. The target race must write
`model_version='jra-cb-v9-sim-2013-clean-jockey-pedigree269'` with ranks `1..n`.

## Neon Verification Queries

```sql
select source, model_version, count(*), max(prediction_generated_at)
from race_finish_position_model_predictions
where prediction_generated_at > now() - interval '2 hours'
group by source, model_version
order by max(prediction_generated_at) desc;
```

Focused target check:

```sql
select count(*), min(predicted_rank), max(predicted_rank), max(model_version)
from race_finish_position_model_predictions
where source = $1
  and (kaisai_nen || kaisai_tsukihi) = $2
  and keibajo_code = $3
  and race_bango = $4;
```

## Rollback

- NAR transformer only: set the Worker secret to `0`.

  ```sh
  cd apps/finish-position-cron
  printf 0 | bunx wrangler secret put NAR_TRANSFORMER_BLEND_ENABLED
  ```

  This keeps production leak-free by serving the clean188 base. Do not roll back
  to historical leaky NAR artifacts.

- Bad code/image deploy: use Wrangler rollback or redeploy a known-good commit.
  Prediction writes are UPSERT-only; do not delete prediction rows as part of
  rollback.

## Deprecated Local Smoke

Scripts under `scripts/launchd/` and local Docker tags are retained for manual
operator smoke only. They must not be installed or treated as production
authority. Production correctness is established by Cloudflare deployment plus
focused per-race Neon writes.
