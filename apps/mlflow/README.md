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
  register/champion side effects.
- `src/mlflow_tracking/export_production.py` — exports MLflow registry state
  (champion/challenger aliases + version tags) into schema-compatible
  candidates for the finish-position production serving path
  (`cell_routing.json` fragments, `model_meta.json`-adjacent active-model
  pointers), with optional R2 upload. These are drop-in candidates for manual
  review — the actual bake/deploy step stays outside this package.
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

# Registry management.
uv run python -m mlflow_tracking.cli set-champion jra-finish-position 7
uv run python -m mlflow_tracking.cli list-models
```

Both `python -m mlflow_tracking ...` (via `__main__.py`) and
`python -m mlflow_tracking.cli ...` (via `cli.py`'s own guard) work.

## Configuration

| Env var                                     | Default                           | Purpose                                               |
| ------------------------------------------- | --------------------------------- | ----------------------------------------------------- |
| `HORSE_RACING_MLFLOW_DATA_DIR`              | `apps/mlflow/data`                | Local tracking-store data directory                   |
| `HORSE_RACING_MLFLOW_BACKEND_URI`           | —                                 | Tracking/registry store URI override (wins over both) |
| `MLFLOW_TRACKING_URI`                       | `sqlite:////<data dir>/mlflow.db` | Generic mlflow tracking URI (fallback)                |
| `HORSE_RACING_MLFLOW_ARTIFACTS_MODE`        | `local`                           | `local` or `r2`                                       |
| `HORSE_RACING_MLFLOW_R2_BUCKET`             | —                                 | Required when `ARTIFACTS_MODE=r2`                     |
| `HORSE_RACING_MLFLOW_R2_PREFIX`             | `mlflow`                          | R2 key prefix for artifacts                           |
| `R2_ACCOUNT_ID` / `CLOUDFLARE_ACCOUNT_ID`   | —                                 | Cloudflare account id (first one set wins)            |
| `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | —                                 | R2 S3-compatible credentials                          |

Resolution order for the tracking URI: `HORSE_RACING_MLFLOW_BACKEND_URI` (this
repo's own env, always wins) > `MLFLOW_TRACKING_URI` (generic mlflow env, for
interop with external tooling/docs) > the computed sqlite default.

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

## Development

```sh
uv run pytest                                     # tests + coverage (>=95%)
uv run ruff check                                 # lint
uv run basedpyright --project pyrightconfig.json  # type check (strict)
uv run ty check                                   # type check (ty)
bunx oxlint . --no-error-on-unmatched-pattern      # oxlint
bunx oxfmt --check package.json pyproject.toml README.md
```
