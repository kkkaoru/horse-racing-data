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

| Category     | Production model_version                      | Notes                                                                            |
| ------------ | --------------------------------------------- | -------------------------------------------------------------------------------- |
| JRA          | `jra-cb-v9-sim-2013-clean`                    | Clean 250-feature default (Stage-2).                                             |
| JRA cell     | `jra-cb-v9-sim-2013-clean-jockey-pedigree269` | Routed only for `kyoso_joken_code=703`, where the local cell gate improved top1. |
| JRA cell     | `jra-cb-v10-prior-corner274-2013`             | Routed only for dirt, field size <=10, and `kyoso_joken_code=005`.               |
| JRA fallback | `jra-cb-stage1-marketfree235-2013`            | Stage-1 gated fallback; see "Stage-1 Market-Free Gated Fallback" below.          |
| NAR          | `iter40-nar-settransformer-blend-v1`          | Clean188 XGBoost base plus clean113 Set Transformer score-z fusion.              |
| Ban-ei       | `banei-cb-v9-sim-2011`                        | Default.                                                                         |
| Ban-ei cell  | `banei-cb-v8-window2011-wf-15y`               | Routed for `grade_code=E`.                                                       |

## Stage-1 Market-Free Gated Fallback (JRA)

`jra-cb-stage1-marketfree235-2013` (235 feat: the champion's 250 minus the 15
market/odds-derived features) serves in place of the champion only when
`predict_lib.stage1_routing.resolve_stage1_gate` trips, per race, inside the
shared `_score_and_flush_races` core (`mode=full` and `mode=rescore` both get
it). Two independent conditions route to Stage-1, either sufficient:

1. **Freshness gate**: every entry in the race lacks a real `tansho_ninkijun`
   (the whole odds board never populated for this race — an odds-serving
   incident, not a single horse's missing odds).
2. **Stddev safety net** (only when `enable_stddev_safety_net` is true for the
   category): the champion's own within-race `predicted_score` population
   stddev falls below `stage1_routing.json`'s `stddev_threshold` (JRA: 0.4,
   independently re-validated against real 2026-07-12/07-18/07-11/07-19 Neon
   prediction data — see `predict_lib/stage1_routing.py`'s module docstring
   for the full evidence). Some Stage-2 scoring paths within-race normalize
   their score (e.g. a z-fusion blend) so this signature structurally cannot
   apply -- such a category sets `enable_stddev_safety_net: false` and gets
   freshness-gate-only coverage instead of a sentinel threshold value.

Config lives in `src/predict_lib/stage1_routing.json`
(`{"jra": {"enabled": true, "model_version": "...", "feature_count": 235,
"architecture": "catboost", "stddev_threshold": 0.4,
"enable_stddev_safety_net": true}}`) — tunable without a code change,
mirroring `running_style_cell_routing.json`'s convention. An
absent category means the gate never trips for it (unchanged behaviour).

**Rollback** (no redeploy needed if the tracked file is already baked with
`enabled: true` and you only need the gate OFF for the NEXT build): set
`"enabled": false` for `"jra"` in `stage1_routing.json` and rebuild/redeploy —
every race then serves Stage-2 (the champion) exactly as before this fallback
existed. The Stage-1 artifact loader is also fail-closed at the code level
(mirrors the NAR transformer / E-top2 companion-load pattern): a missing,
corrupt, unapproved, or feature-mismatched Stage-1 artifact disables the
fallback for that run automatically (falls back to champion-only), so a
broken Stage-1 artifact can never block or degrade ordinary serving even
without an operator action.

Historical leaky JRA/NAR artifacts must not be selected in production. NAR
rollback is `NAR_TRANSFORMER_BLEND_ENABLED=0`, which keeps the leak-free
`iter12-nar-xgb-hpo-v8-clean188` base and disables only the transformer blend.

## Artifact Integrity Preflight

`production-artifacts.json` is the deterministic digest contract (SHA-256 +
size) for every artifact key production selectors can reach: category-default
and cell-routed finish-position models (`model_meta.json` +
`predict_lib/cell_routing.json`), the conditional NAR transformer bundle, and
the JRA/NAR running-style latest-model + calibrator pair. `predict_lib.
artifact_integrity` derives that reachable key set and fails closed with one
of three outcomes:

- `MATCH` / exit `0`: every selected key is manifested and each observed byte
  stream matches its declared SHA-256 and size.
- `INTEGRITY_FAILURE` / exit `1`: invalid manifest, a selected-but-unmanifested
  or missing artifact, or a size/hash mismatch. Do not build, upload, deploy,
  delete, overwrite, or automatically roll back anything.
- `INDETERMINATE` / exit `2`: bytes could not be established because a local
  read failed (e.g. permission denied). Deployment still fails closed, but
  this is not evidence of drift.

The manifest never activates a model; `model_meta.json` / `cell_routing.json`
/ the tracked deploy declarations below remain the sole activation authority.

**This preflight is wired into both build paths so a normal deploy command
cannot skip it:**

- `apps/finish-position-cron/package.json`'s `deploy` script runs the verifier
  against the staged `models/` build context before `wrangler deploy`:

  ```sh
  cd apps/finish-position-cron
  bun run deploy -- --containers-rollout immediate
  ```

  A nonzero verifier exit aborts before `wrangler deploy` ever runs.

- The container `Dockerfile` also runs
  `python -m predict_lib.artifact_integrity --artifact-root /models --system
finish-position` as a `RUN` step against the baked `/models` tree, so even a
  direct `docker build` / `wrangler deploy` (bypassing the bun script above)
  fails the image build on a corrupt, mismatched, or unmanifested artifact.
  Running-style bytes are out of scope for this container-local check (they
  are never baked into this image), but the verifier's selector-closure check
  -- is every reachable key even manifested at all -- always runs unscoped, so
  a newly cell-routed-but-unmanifested running-style variant (see below) still
  fails this same `RUN` step closed.

Run the verifier ad hoc any time:

```sh
bun run --filter finish-position-predict-container artifact:verify
bun run --filter finish-position-predict-container artifact:verify -- \
  --artifact-root models --system finish-position
```

### Tracked deploy declarations (secrets this app cannot read back)

Two Cloudflare Worker secrets select artifacts that neither this container nor
its verifier can read back from Cloudflare -- secrets are write-only from the
CLI, there is no `wrangler secret get`:

| Secret                            | Worker                 | Consumer                                                           |
| --------------------------------- | ---------------------- | ------------------------------------------------------------------ |
| `NAR_TRANSFORMER_BLEND_ENABLED`   | `finish-position-cron` | `predict_lib.model_meta` (container runtime)                       |
| `RUNNING_STYLE_CELL_ROUTING_JSON` | `sync-realtime-data`   | `running-style-cell-router.ts` (out of scope for this app to edit) |

Each has a tracked local declaration under `src/predict_lib/` that the
verifier consults instead of ambient process state:

- `deploy_flags.json` (`{"nar_transformer_blend_enabled": true|false}`).
- `running_style_cell_routing.json` (mirrors `RunningStyleCellRoutingConfig`:
  `{category: {defaultVariantId, rules: [{conditions, variantId}], variants:
{variantId: {modelKey}}}}`; `{}` means no live cell routing, matching
  production today).

**Whoever runs `wrangler secret put NAR_TRANSFORMER_BLEND_ENABLED` or
`wrangler secret put RUNNING_STYLE_CELL_ROUTING_JSON` MUST update the matching
tracked file in the SAME commit.** Otherwise local verification can again
diverge from what is actually live -- the exact failure mode this preflight
exists to close. The `--nar-transformer enabled|disabled` CLI flag remains
available to explicitly override the tracked declaration (e.g. to check a
rollback before flipping the tracked file); omit it to use the tracked value.

The two divergence directions are NOT equally safe if this discipline is ever
skipped:

- Tracked file says the bundle is ON but the live secret is actually OFF
  (e.g. `deploy_flags.json` still says `true` after someone flips the
  Cloudflare secret to `0` without updating the file): the preflight keeps
  requiring and checking artifacts production no longer serves. This only
  over-blocks -- annoying (a deploy may fail on stale artifacts nobody staged
  on purpose), never unsafe.
- Tracked file says OFF but the live secret is actually ON (e.g.
  `deploy_flags.json` says `false`, or `running_style_cell_routing.json` is
  missing a variant, while the real Cloudflare secret still serves it): this
  is the DANGEROUS direction. The preflight silently stops checking an
  artifact that is genuinely live, so a corrupt or mismatched byte for that
  artifact would go unnoticed and still ship. There is no automated detection
  for this today (the Cloudflare API cannot read a secret's value back) --
  the "update in the same commit" discipline plus code review is the only
  safeguard, so treat any PR touching either secret as needing extra scrutiny
  on whether the tracked file actually matches.

The running-style routing declaration's reachable `variants[*].modelKey`
values are folded into the selector closure for **all three** categories (jra,
nar, AND ban-ei -- the config schema allows all three even though only jra/nar
have a live artifact today), so a newly cell-routed-but-unmanifested variant
fails closed instead of silently reporting `MATCH`. The manifest schema
represents the real running-style topology -- N routed models per source
sharing exactly ONE calibrator (`tryLoadCalibrators(bucket, job.source)` only
ever loads a jra or nar calibrator, including for a Ban-ei race, whose
`job.source` is `"nar"`) -- by validating calibrator coverage at the
category/source level rather than requiring a model+calibrator pair inside
every bundle, so adding a second routed model for the same source no longer
produces a spurious duplicate-serving-key rejection.

### Scope

This verifier only ever reads LOCAL build-context bytes: a staged `models/`
directory, or the container image's own `/models` tree at `docker build` time.
It performs no network re-verification of the live R2 running-style objects
(`running-style/models/{jra,nar}/{latest.flatbin,calibrators.json}`) -- those
are attestation-recorded (`source_ref: "attestation://..."` in the manifest),
not re-hashed over the network. If a future caller adds a network-based
observer, an auth or network failure MUST report `state="unavailable"` (->
`INDETERMINATE`), never `"missing"` (-> `INTEGRITY_FAILURE`) or a fabricated
`"present"` (-> `MATCH`): a transient network issue must never silently block,
nor silently pass, an ordinary offline build. The JRA running-style pair is
pinned as one release: model
`278690ed18e7aa1f6847ee4349b1ec98281a5633cc10bd000de55c608ffbfc76` and
calibrator `0f23a5a40dea956b1de06699432e130beb86dc3448bd3188485e60d1a7067ee7`.
Those live bytes are attested, but their exact durable non-`tmp` MLflow/tracked
source remains provenance debt; this manifest records that HOLD and must not
be used to substitute the older tracked calibrator.

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

The Stage-1 fallback has no dedicated forced-trigger smoke endpoint (it only
activates during a genuine odds-serving incident or a collapsed score
spread); the direct verification is `tests/test_stage1_routing.py` (52 unit
cases against real gate-decision logic) plus watching for
`model_version='jra-cb-stage1-marketfree235-2013'` rows should a real incident
occur. Confirm the artifact loaded correctly at container startup by checking
for a `[stage1-gate] loaded category=jra ...` line in the container logs (a
load failure logs `... load failed -> Stage-1 fallback disabled this run: ...`
instead and never blocks ordinary champion serving).

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

- JRA Stage-1 fallback only: set `"enabled": false` for `"jra"` in
  `src/predict_lib/stage1_routing.json` and rebuild/redeploy. Every JRA race
  then serves the champion (Stage-2) unconditionally, identical to
  pre-fallback behaviour. No secret is involved (the config is a tracked
  file baked into the image, not a Cloudflare secret).
- NAR transformer only: set the Worker secret to `0`.

  ```sh
  cd apps/finish-position-cron
  printf 0 | bunx wrangler secret put NAR_TRANSFORMER_BLEND_ENABLED
  ```

  Also update `src/predict_lib/deploy_flags.json` to
  `{"nar_transformer_blend_enabled": false}` in the SAME commit -- see
  "Artifact Integrity Preflight" above. Skipping this desyncs the local
  preflight from the secret you just set.

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
