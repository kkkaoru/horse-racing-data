# mlflow

MLflow tracking/registry helper library and CLI for the horse-racing prediction
models (finish-position 着順予測 and running-style 脚質予測), with CELL-level
(category × class × subgroup × racetrack × season × surface) evaluation.

This is a standalone `uv` project — it does not import from any other app in
this monorepo. The canonical CELL schema in `src/mlflow_tracking/cells.py`
replicates (by hand, not by import) the boundary rules defined in
`apps/finish-position-predict-container/src/predict_lib/subgroup.py`.

## Layout

- `src/mlflow_tracking/config.py` — tracking URI / data dir / R2 artifact-store
  environment resolution.
- `src/mlflow_tracking/cells.py` — canonical CELL key/metric schema and column
  normalization.
- `src/mlflow_tracking/logging_api.py` — run/metric logging helpers built on
  `mlflow.MlflowClient` (chunked `log_batch`, cell-table logging, headline
  metric selection).
- `src/mlflow_tracking/registry.py` — registered-model naming, version
  registration by URI reference, and champion/challenger alias management.
- `src/mlflow_tracking/backfill_finish_position.py` — backfills the registry
  from the finish-position container's on-disk `metadata.json` /
  `manifest.json` / `model_meta.json` / `cell_routing.json`.
- `src/mlflow_tracking/backfill_running_style.py` — backfills running-style
  (jra/nar only — Ban-ei is out of scope) model versions from
  `apps/pc-keiba-viewer/tmp/models/` (a shared staging dir, may not exist),
  syncs the R2 mutable-pointer production version, and attaches the canonical
  isotonic calibrator JSON.
- `src/mlflow_tracking/ingest_eval.py` — file-based ingestion of
  `trial_registry_*.duckdb`, `serve_accuracy_report.py --json` output
  (finish-position or running-style shape, auto-routed), and generic
  cell-metrics tables (parquet/JSON). Every eval run is tagged with a required
  `eval_regime` (see below).
- `src/mlflow_tracking/training_run.py` — ingests a single
  `hr-mlflow-training-run/v1` manifest (a fixed cross-repo contract) emitted
  by env-gated trainer hooks in pc-keiba-viewer, with optional
  register/champion side effects. Also upserts a `timelines` point for a
  live-serve manifest (`eval_regime="serve"` + a `date` tag) -- this is the
  actual automatic path, since `ingest_eval.py`'s own timeline wiring is
  manual/CLI-only (see below).
- `src/mlflow_tracking/timeline.py` — a generic metric TIME-SERIES
  ("timeline") layer, so accuracy trends render as real line charts instead
  of one flat dot per run. Exactly one PERSISTENT run per (task, category)
  pair lives in the `timelines` experiment; each call appends one point
  (step = date, as a YYYYMMDD int) to that run rather than creating a new
  run. Idempotent: the run is found by a `timeline_key` tag search (not
  recreated), and each metric key is deduped by step (re-ingesting the same
  date never double-draws a point). The run is always left `FINISHED` after
  every append -- MLflow's chart rendering doesn't depend on run status, and
  a permanently-`RUNNING` timeline run would show a stuck/live indicator in
  run-list views that never resolves. Wired into BOTH
  `ingest_eval.py::ingest_serve_accuracy` (manual/CLI-only) and
  `training_run.py::log_training_run` (the actual automatic live-serve path
  -- see that module's docstring for the empirical reasoning).
- `src/mlflow_tracking/backfill_serve_timeline.py` — backfills `timelines`
  points over a historical date range by re-invoking
  `serve_accuracy_report.py --json` once per date (jra/nar only -- see
  below) and feeding its payload through `ingest_eval.ingest_serve_accuracy`.
- `src/mlflow_tracking/export_production.py` — exports MLflow registry state
  (champion/challenger aliases + version tags) into schema-compatible
  candidates for the finish-position production serving path
  (`cell_routing.json` fragments, `model_meta.json`-adjacent active-model
  pointers), with optional R2 upload. These are drop-in candidates for manual
  review — the actual bake/deploy step stays outside this package.
- `src/mlflow_tracking/sync_production.py` — syncs genuinely-served
  production predictions (from the racing Neon database's
  `race_finish_position_model_predictions` / `race_running_style_model_predictions`
  tables) into MLflow, and evaluates them against finalized results (from the
  local PostgreSQL replica) once those results exist. Along with
  `champion_cell_eval.py` below, this is the one deliberate, narrow exception
  to this package's otherwise file-based-only ingestion design (see
  `ingest_eval.py`'s docstring) — both read live Neon + local-replica
  Postgres directly via `db.py`'s injected-connection helpers, read-only.
  Idempotency: at most one run per (date, category, model_version), gated by
  the `sync_base_logged`/`sync_eval_logged` tags, so re-running the same
  range every day (the realistic cron shape) never re-logs already-logged
  base tracking or evaluation, but does retry an unfilled evaluation on a
  later call once results become final.
- `src/mlflow_tracking/champion_cell_eval.py` — evaluates each category's
  CURRENT champion model version at CELL granularity (venue × class ×
  distance/season/surface/field-size bands) over a trailing window of
  genuinely-served predictions, reusing the same Neon + local-replica
  Postgres read path as `sync_production.py` above (see that entry for the
  file-based-design exception this represents). Idempotency: at most one run
  per (category, task, window_days, as_of date) — re-running for the same day
  is a cheap no-op that reuses the existing run's summary rather than
  re-querying or re-logging the cell table.
- `src/mlflow_tracking/cli.py` — the `mlflow_tracking` CLI (see below).

## Usage

```sh
# One-time setup: create the data dir, switch sqlite to WAL mode, and ensure
# every canonical experiment exists.
uv run python -m mlflow_tracking.cli init

# Backfill the finish-position registry from the container's models/ directory.
# Fails loudly (non-zero exit + stderr warning per category) if any
# category's champion alias could not be synced from model_meta.json --
# pass --allow-missing-champion to proceed anyway (e.g. on a from-scratch
# registry before any category has ever shipped).
uv run python -m mlflow_tracking.cli backfill-finish-position [--allow-missing-champion]

# Backfill running-style model versions. Path defaults to
# apps/pc-keiba-viewer/tmp/models/ when omitted.
uv run python -m mlflow_tracking.cli backfill-running-style [/path/to/artifacts]

# Ingest a research-harness trial registry / serve-accuracy report / generic
# cell-metrics table. --eval-regime is REQUIRED on the latter two -- pass
# "unspecified" rather than guessing, since RS eval numbers exist in both a
# true out-of-sample regime and a misleadingly high leaky self-consistency
# regime (docs/finish-position-accuracy/history/rs-model-audit.md).
uv run python -m mlflow_tracking.cli ingest-trial-registry trial_registry_jra.duckdb
uv run python -m mlflow_tracking.cli ingest-serve-accuracy serve_accuracy.json --eval-regime serve
uv run python -m mlflow_tracking.cli log-eval cell_report.parquet --eval-regime oos --experiment finish-position/wf-eval

# Ingest a training-run manifest (hr-mlflow-training-run/v1 — a fixed schema
# consumed by pc-keiba-viewer trainer hooks; see training_run.py's docstring).
uv run python -m mlflow_tracking.cli log-training-run manifest.json

# Backfill the `timelines` experiment's finish-position/running-style points
# over a historical date range by re-invoking serve_accuracy_report.py once
# per date (jra/nar only — see "Timelines" below for the Ban-ei gap).
# --skip-existing skips a date when both the finish-position and
# running-style timelines already have a point there (cheap resume of a
# partially-completed backfill).
uv run python -m mlflow_tracking.cli backfill-serve-timeline \
  --category jra --date-from 20260601 --date-to 20260630 --skip-existing

# Export production-serving-compatible candidates from the registry
# (champion/challenger aliases + version tags). These are drop-in-schema
# CANDIDATES for manual review, not automatic deploys -- the actual
# model_meta.json bake / cell_routing.json swap / Neon
# finish_position_active_models flip stays an explicit human/orchestrated
# step outside this package. Default output dir: apps/mlflow/data/exports/.
# --upload-r2 s3://bucket/key is optional on both. export-active-models fails
# loudly (non-zero exit + stderr listing categories) if any category has no
# champion alias -- pass --allow-missing to write a partial file anyway
# (missing categories are also recorded in the provenance sidecar).
uv run python -m mlflow_tracking.cli export-cell-routing --category jra --output cell_routing_jra.json
uv run python -m mlflow_tracking.cli export-active-models --output active_models.json [--allow-missing]

# Sync genuinely-served production predictions (finish-position always;
# running-style too, for jra/nar) into MLflow over a date range, evaluating
# against finalized results where they already exist. Requires NEON_PRIMARY_URL
# and (usually already-default) HORSE_RACING_LOCAL_PG_URL -- see Configuration
# below. Meant to be re-run daily over a small overlapping window (e.g.
# "yesterday+today"); already-logged base tracking/eval is never re-logged.
# --no-traces is accepted for interface completeness only -- MLflow traces are
# never emitted regardless (see the ⚠️ callout below for why).
uv run python -m mlflow_tracking.cli sync-production \
  --date-from 20260701 --date-to 20260708 --categories jra,nar,banei

# Evaluate each category's CURRENT champion model at CELL granularity over a
# trailing window of genuinely-served predictions (default: 90 days ending
# today). Requires the same two env vars as sync-production above. Champion
# coverage is often sparse or empty for a given category -- an empty cell
# table with has_champion_coverage=false is a normal, valid outcome, not a
# bug. --as-of overrides "today" for a reproducible re-run of a past window.
uv run python -m mlflow_tracking.cli eval-champion-cells \
  --category jra,nar --window-days 90 --as-of 20260708

# Registry management.
uv run python -m mlflow_tracking.cli set-champion jra-finish-position 7
uv run python -m mlflow_tracking.cli list-models
```

Both `python -m mlflow_tracking ...` (via `__main__.py`) and
`python -m mlflow_tracking.cli ...` (via `cli.py`'s own guard) work.

## Configuration

| Env var                                     | Default                                                               | Purpose                                                                                                   |
| ------------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `HORSE_RACING_MLFLOW_DATA_DIR`              | `apps/mlflow/data`                                                    | Local tracking-store data directory                                                                       |
| `HORSE_RACING_MLFLOW_BACKEND_URI`           | —                                                                     | Tracking/registry store URI override (wins over both)                                                     |
| `MLFLOW_TRACKING_URI`                       | `sqlite:////<data dir>/mlflow.db`                                     | Generic mlflow tracking URI (fallback)                                                                    |
| `HORSE_RACING_MLFLOW_ARTIFACTS_MODE`        | `local`                                                               | `local` or `r2`                                                                                           |
| `HORSE_RACING_MLFLOW_R2_BUCKET`             | —                                                                     | Required when `ARTIFACTS_MODE=r2`                                                                         |
| `HORSE_RACING_MLFLOW_R2_PREFIX`             | `mlflow`                                                              | R2 key prefix for artifacts                                                                               |
| `R2_ACCOUNT_ID` / `CLOUDFLARE_ACCOUNT_ID`   | —                                                                     | Cloudflare account id (first one set wins)                                                                |
| `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | —                                                                     | R2 S3-compatible credentials                                                                              |
| `NEON_PRIMARY_URL`                          | —                                                                     | Racing Neon Postgres DSN (read-only); required by `sync-production` / `eval-champion-cells`               |
| `HORSE_RACING_LOCAL_PG_URL`                 | `postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing` | Local PostgreSQL replica DSN (read-only, finalized results) for `sync-production` / `eval-champion-cells` |

Resolution order for the tracking URI: `HORSE_RACING_MLFLOW_BACKEND_URI` (this
repo's own env, always wins) > `MLFLOW_TRACKING_URI` (generic mlflow env, for
interop with external tooling/docs) > the computed sqlite default.

### ⚠️ Timelines and serve-accuracy are 0–100% scale; wf-eval is 0–1 fraction — never compare them in the same chart

The `timelines` experiment, `finish-position/serve-accuracy`, and
`running-style/eval` all report LIVE-SERVE metrics (`top1_pct`,
`place2_pct`, `overall_accuracy_pct`, `fp_top1_pct`, `rs_overall_accuracy_pct`,
...) on a **0–100 PERCENTAGE scale**. `finish-position/wf-eval` instead
reports OFFLINE walk-forward metrics (`top1_accuracy`, `place2_accuracy`,
`overall_top1_accuracy`, ...) on a **0–1 FRACTION scale**, and is a
different evaluation regime entirely (held-out walk-forward backtesting,
not what actually got served). These two families are never
interconvertible by a decimal shift alone — the underlying populations and
regimes differ too — so **never plot or compare them side by side in the
MLflow UI's chart/compare view**; a `0.445` wf-eval fraction next to a
`44.5` serve-accuracy percentage is not "the same metric at different
scales", it is two different measurements of two different things.

`timelines` currently only covers **jra/nar** — `serve_accuracy_report.py`
(the only source of live-serve numbers) has no Ban-ei support at all (its
own `VALID_CATEGORIES = ("jra", "nar")`), so there is nothing for
`upsert_timeline_point`/`backfill-serve-timeline` to backfill for Ban-ei
without a separate, out-of-scope change to that script first.

### ⚠️ `export-cell-routing` is a synthesis, not a reproduction — always diff before baking

`export-cell-routing` rebuilds a `cell_routing.json` fragment purely from
registry state (the champion alias + `routing_scope="class:<code>"` tags), by
convention (see `build_cell_routing_export`'s docstring). It is **not** a
faithful reproduction of the live, hand-tuned `cell_routing.json`:

- A synthesized `class-<code>` rule can **shadow** a real hand-tuned rule
  that targets the same class code by a different variant name (e.g. a
  registry-synthesized `class-703` rule can silently take priority over a
  live `jockey_pedigree_703` rule, changing which model actually serves that
  class).
- Rules whose variants were never captured as `routing_scope="class:<code>"`
  registry tags (e.g. a live `prior_corner_dirt_smallfield_005` rule) are
  **absent** from the export entirely — this tool cannot recover conditions
  it was never told about.

Treat every `export-cell-routing` output as a **MANUAL-REVIEW candidate**:
always diff it against the live `cell_routing.json` before any bake, and
never wire it into an automated deploy step.

### Production export provenance

`apps/finish-position-predict-container/src/predict_lib/cell_router.py`'s
`load_cell_router()` treats **every** top-level key in `cell_routing.json` as
a category-routing entry to parse (verified read-only) — it has no tolerance
for an extra key like `_mlflow`. So `export-cell-routing` /
`export-active-models` never embed provenance in the export file itself;
they write a sidecar `<output>.provenance.json` (`exported_at`,
`registry_versions_used`, `run_ids`) next to it instead.

In `r2` mode, `config.apply_r2_env()` maps the repo-standard R2 env vars above
onto the `MLFLOW_S3_ENDPOINT_URL` / `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION` variables mlflow's boto3-backed
S3 artifact store reads, without overwriting anything already set.

### ⚠️ MLflow traces are not used — here's why and what replaces them

`sync-production` deliberately never emits an MLflow trace, even though a
per-race/per-horse "here's exactly what was predicted" audit trail sounds
like precisely what `MlflowClient.start_trace`/`end_trace` are for. This was
evaluated and rejected, not overlooked: in the installed `mlflow-skinny`
3.14, those calls persist through a process-wide OpenTelemetry singleton that
resolves its destination via the GLOBAL `mlflow.get_tracking_uri()` state at
first use — **not** the calling `MlflowClient`'s own `tracking_uri`. This was
reproduced empirically: writing a trace via an explicit, isolated-store
`MlflowClient` silently failed until `mlflow.set_tracking_uri()` was ALSO
called globally. Adopting traces here would mean introducing global mutable
tracking-URI state into a package built entirely around explicit-client,
hermetically-testable design — exactly the failure class already responsible
for a real recorded incident (see `tests/conftest.py`'s
`clear_ambient_backend_uri` docstring): a leaked global tracking URI once
caused a test run to silently overwrite two production champion aliases with
fake test data.

Decision: this package does not use MLflow traces. The `predictions.json` /
`eval.json` (finish-position) table artifacts — and their running-style
analogs — logged by `sync-production` on every run serve as the per-race/
per-horse audit trail instead, viewable in the MLflow UI's Artifacts /
Evaluation views.

## Development

```sh
uv run pytest                                     # tests + coverage (>=95%)
uv run ruff check                                 # lint
uv run basedpyright --project pyrightconfig.json  # type check (strict)
uv run ty check                                   # type check (ty)
bunx oxlint . --no-error-on-unmatched-pattern      # oxlint
bunx oxfmt --check package.json pyproject.toml README.md
```

## Daily automation (LaunchAgent)

A Mac launchd LaunchAgent (`com.horse-racing.mlflow-production-sync`) runs
`sync-production` then `eval-champion-cells` once daily at **22:30 JST**
(same-day racing has finished and results have typically already mirrored
into the local PostgreSQL replica by then). Both commands are idempotent, so
a delayed launchd catch-up fire (e.g. after the Mac was asleep at 22:30) is
harmless to re-run.

Source files live in this repo at `apps/mlflow/scripts/launchd/`:

- `mlflow-production-sync-daily.sh` — the wrapper script that runs both CLI
  subcommands.
- `com.horse-racing.mlflow-production-sync.plist` — the LaunchAgent
  definition (a version-controlled copy; the installed copy lives under
  `~/Library/LaunchAgents/`, outside git).

No secrets live in either file — the CLI's own `main()` loads
`apps/mlflow/.env.local` then a repo-root `.env` allow-listed fallback before
parsing arguments (see the "SECRETS" note in the plist header comment).

```sh
# Install (copies nothing -- launchctl reads directly from wherever you point it,
# but the convention in this repo is to also keep a copy under ~/Library/LaunchAgents/
# so `launchctl list` and Finder both show a consistent, discoverable location):
cp apps/mlflow/scripts/launchd/com.horse-racing.mlflow-production-sync.plist \
   ~/Library/LaunchAgents/com.horse-racing.mlflow-production-sync.plist
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/com.horse-racing.mlflow-production-sync.plist

# Verify loaded:
launchctl print gui/$(id -u)/com.horse-racing.mlflow-production-sync

# Trigger a manual run right now (does not wait for 22:30 JST):
launchctl kickstart -k gui/$(id -u)/com.horse-racing.mlflow-production-sync

# Logs:
tail -f ~/Library/Logs/mlflow-production-sync.log

# Stop / uninstall:
launchctl bootout gui/$(id -u)/com.horse-racing.mlflow-production-sync
rm ~/Library/LaunchAgents/com.horse-racing.mlflow-production-sync.plist
```
