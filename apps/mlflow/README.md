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
  (step = `step_for_date(date_yyyymmdd)`, days-since-2020-01-01) to that run
  rather than creating a new run. Idempotent: the run is found by a
  `timeline_key_v2` tag search (not recreated), and each metric key is
  deduped by step (re-ingesting the same date never double-draws a point).
  The run is always left `FINISHED` after every append -- MLflow's chart
  rendering doesn't depend on run status, and a permanently-`RUNNING`
  timeline run would show a stuck/live indicator in run-list views that
  never resolves. Wired into BOTH `ingest_eval.py::ingest_serve_accuracy`
  (manual/CLI-only) and `training_run.py::log_training_run` (the actual
  automatic live-serve path -- see that module's docstring for the
  empirical reasoning).
  - **v2 step scheme (2026-07-10):** the original scheme (`step =
int(date_yyyymmdd)`, e.g. 20260516) made MLflow 3.14's GenAI/Eval
    run-detail chart tiles (where every timeline run gets auto-routed, since
    it carries no params/source tags) spin forever at that ~20-million step
    magnitude -- confirmed with 3 throwaway diagnostic runs left in the real
    `timelines` experiment, tagged `junk=true`/`diagnostic=true`. The fix:
    `step_for_date` computes step as days-since-2020-01-01 instead (small,
    monotonic; step N == 2020-01-01 + N days). v2 runs are tagged
    `timeline_key_v2` (a different tag KEY, not just a different value) and
    named `timeline-{task}-{category}-v2`, so they can never be confused
    with (found by, or appended to by) the 5 pre-existing v1 runs. The
    one-time migration (`migrate_timeline_v2.py`) copied every v1 run's full
    metric history into its new v2 counterpart, then tagged the OLD run
    `deprecated=true` / `superseded_by=<new_run_id>` -- v1 runs are never
    deleted, only superseded.
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
  later call once results become final. Also emits one MLflow trace (+
  Feedback assessments) per race/horse via `trace_emit.py`, the SAME pass
  that computes eval metrics — see "MLflow traces" below.
- `src/mlflow_tracking/trace_emit.py` — hardened, isolated trace/span/
  assessment emitters built on `mlflow.tracing.client.TracingClient`
  (constructed explicitly, never the global fluent API) — see "MLflow
  traces" below for the full shape/rationale. Every semi-internal mlflow
  API surface this feature depends on is contained to this one file.
- `src/mlflow_tracking/backfill_traces.py` — one-time (but safely
  re-runnable) historical backfill: emits traces for every already-
  evaluated (date, category, model_version) run in both production-usage
  experiments, re-using `trace_emit.py`'s exact emit functions — see
  "MLflow traces" below.
- `src/mlflow_tracking/champion_cell_eval.py` — evaluates each category's
  CURRENT champion model version at CELL granularity (venue × class ×
  distance/season/surface/field-size bands) over a trailing window of
  genuinely-served predictions, reusing the same Neon + local-replica
  Postgres read path as `sync_production.py` above (see that entry for the
  file-based-design exception this represents). Idempotency: at most one run
  per (category, task, window_days, as_of date) — re-running for the same day
  is a cheap no-op that reuses the existing run's summary rather than
  re-querying or re-logging the cell table.
- `src/mlflow_tracking/cell_eval_runs.py` — per-CELL-PER-SERVED-VERSION
  evaluation, as individual PERSISTENT MLflow runs so a single cell's
  accuracy trend over time is a real, drillable line chart in the UI —
  structurally different from `champion_cell_eval.py` above, which scores
  only the champion and logs one run per (category, task) with the per-cell
  breakdown as a table inside it. This module instead evaluates EVERY
  model_version that served enough volume this window (champion, every
  cell-routed variant, and any other version), and logs one run PER
  (category, cell, model_version), tagged with each cell dimension
  individually (`venue`, `class_code`, `distance_band`, `season_band`,
  `surface`, `field_size_band` for finish-position; RS omits the last two),
  plus `model_version`, `champion_relation` (`champion`/`variant`/`other`),
  `window_days`, and `low_n`. Idempotency: found-or-created by a
  `cell_run_key = "{category}:{cell_key}:{model_version}:{window_days}"` tag,
  with each day's metric point additionally deduped by step (mirroring
  `timeline.py`'s own idiom) — a same-day re-run is a cheap no-op. A
  `--min-races` volume floor (default 20) gates run creation per
  (model_version, cell) group; groups below it are skipped but counted
  (never silently dropped), and the floor is automatically raised — with a
  warning — if it would otherwise create more than 1,500 runs for one
  category/task in a single invocation.
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
# Also emits one MLflow trace (+ Feedback assessments) per race/horse the
# same pass eval metrics are computed -- see "MLflow traces" below.
# --no-traces skips trace/assessment emission for this call only (base
# tracking and eval metrics/tables are unaffected). --partial-coverage-threshold
# configures the coverage-ratio gap check below (default 80.0).
uv run python -m mlflow_tracking.cli sync-production \
  --date-from 20260701 --date-to 20260708 --categories jra,nar,banei \
  --partial-coverage-threshold 80.0

# One-time (but safely re-runnable) historical backfill: emit MLflow traces
# for every already-evaluated (date, category, model_version) run in BOTH
# production-usage experiments -- see "MLflow traces" below. Requires the
# same two env vars as sync-production above. --date-from/--date-to
# optionally restrict which runs are considered (useful for chunking a very
# large backfill); omitted, every run ever found is processed. Idempotent:
# a race/horse whose trace already exists is a cheap no-op, so an
# interrupted run is trivially safe to resume by just re-running.
uv run python -m mlflow_tracking.cli backfill-traces \
  [--date-from 20260601] [--date-to 20260708]

# Evaluate each category's CURRENT champion model at CELL granularity over a
# trailing window of genuinely-served predictions (default: 90 days ending
# today). Requires the same two env vars as sync-production above. Champion
# coverage is often sparse or empty for a given category -- an empty cell
# table with has_champion_coverage=false is a normal, valid outcome, not a
# bug. --as-of overrides "today" for a reproducible re-run of a past window.
uv run python -m mlflow_tracking.cli eval-champion-cells \
  --category jra,nar --window-days 90 --as-of 20260708

# Score EVERY model_version that served enough volume this window (not just
# the champion) at CELL granularity, logging one PERSISTENT, individually
# drillable MLflow run per (category, cell, model_version) -- appending one
# metric point per day this command runs, so a single cell's accuracy shows
# as a real trend line in the UI. Requires the same two env vars as
# sync-production above. --min-races (default 20) gates run creation per
# (model_version, cell) group; below-floor groups are skipped but counted,
# never silently dropped, and the floor is auto-raised (with a warning) if
# it would otherwise exceed 1,500 runs for one category/task.
uv run python -m mlflow_tracking.cli eval-cells \
  --category jra,nar,banei --window-days 90 --min-races 20

# One-time metrics-only backfill: append fp_place4_pct/fp_place5_pct/
# fp_place6_pct (feedback_eval_rank_1_to_6) to every already-evaluated
# finish-position production-usage run (tagged sync_eval_logged=true) and
# its corresponding v2 finish-position timeline run, WITHOUT re-logging any
# existing metric/table/artifact -- see refresh_eval_metrics.py's own module
# docstring. Idempotent: a run already carrying all 3 keys is skipped with
# no re-query. Requires the same two env vars as sync-production above.
uv run python -m mlflow_tracking.cli refresh-eval-metrics

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

### Serving-gap detection, and the coverage-ratio check (`GAP_TYPE_PARTIAL_COVERAGE`)

Every `sync-production` call also checks, per (date, category), whether
finish-position was genuinely serving that day, logging an idempotent marker
run (`serving_gap_key = "{date}:{category}"`, tagged `serving_gap=true`) in
`finish-position/production-usage` whenever it wasn't. There are now THREE
mutually-exclusive gap types (a `gap_type` tag on the marker run):

- **`no_rows`** — literally zero finish-position rows landed that day, while
  something else DID (running-style served for jra/nar, or the local race
  calendar shows races happened for banei). Real incident: 2026-07-04, JRA
  had 12 running-style predictions logged and zero finish-position rows.
- **`backfill_only`** — finish-position rows exist, but every one of them is
  a delayed backfill re-prediction (`prediction_generated_at` days after the
  race), never a genuinely LIVE serve.
- **`partial_coverage`** (2026-07-11) — a NEW, genuinely different check: the
  two gap types above are both gated on `races_live == 0`, so a day where
  SOME races serve live — no matter how few — was invisible to both. Real
  incident: JRA's champion `jra-cb-v9-sim-2013-clean` has never written a
  row since its 07-04 deploy; only a cell-routed variant serves, for one
  narrow class-code slice, about **3% of scheduled races** (11/485, 16/501,
  16/476 on recent race days). Because that variant counts as
  champion-derived and `races_live > 0`, the day read as fully healthy to
  every prior check.

  This closes the gap with an INDEPENDENT "races expected" oracle,
  `serve_eval.fetch_races_scheduled(conn, category, date_str)` — a direct
  count from the local replica's own `jvd_ra` (jra) / `nvd_ra` excluding
  Ban-ei's keibajo_code (nar) / `nvd_ra` filtered to Ban-ei's keibajo_code
  (banei) race calendar, for ALL THREE categories (a generalization of the
  pre-existing Ban-ei-only `fetch_banei_race_count`, which stays a separate,
  behavior-unchanged function used only by the older RS-vs-FP check). Two
  new metrics are computed once per (date, category) and logged onto every
  per-`model_version` run created that day, AND onto the `finish-position`
  timeline point:

  - **`fp_races_scheduled`** — the race-calendar oracle count.
  - **`fp_coverage_pct`** — `100 * races_live / races_scheduled`. `None`
    (never logged, never a fabricated `0.0`) when zero races were scheduled
    that day.

  `partial_coverage` fires whenever `races_live > 0` AND `fp_coverage_pct`
  falls below a configurable threshold — default **80.0**, the
  `sync-production` CLI's `--partial-coverage-threshold` flag (threaded
  through `sync_production_range`'s `partial_coverage_threshold` parameter).
  Because this check requires `races_live > 0` while `no_rows`/
  `backfill_only` require `races_live == 0`, the three types can never fire
  for the SAME (date, category) in the same call — they safely share the
  one marker-run-per-(date, category) idempotency key without ever
  overwriting each other's `gap_type` tag.

There is also a SEPARATE, per-(date, category, **task**) champion-mismatch
check (`champion_gap_key = "{date}:{category}:{task}"`, tag
`champion_gap=true`): live rows were genuinely served, but none of them came
from the currently-registered champion model_version OR a cell-routed
variant of it (`f"{champion_label}-<routing-scope>"`) — e.g. a rollback left
production silently serving a superseded, unrelated challenger build for
weeks.

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

### MLflow traces: Option B (`TracingClient`, never the fluent API) — how, why, and what's deferred

**This section supersedes an earlier "MLflow traces are not used" decision.**
That decision was correct about the hazard it identified — evaluated and
rejected, not overlooked — but had not yet found the fix. In the installed
`mlflow-skinny` 3.14.0, the FLUENT API (`mlflow.start_trace()`/
`mlflow.end_trace()`/`mlflow.log_feedback()`) persists through a process-wide
OpenTelemetry singleton that resolves its destination via the GLOBAL
`mlflow.get_tracking_uri()` state at first use — **not** the calling
`MlflowClient`'s own `tracking_uri`. This was reproduced empirically: writing
a trace via an explicit, isolated-store `MlflowClient` silently failed until
`mlflow.set_tracking_uri()` was ALSO called globally — exactly the failure
class already responsible for a real recorded incident (see
`tests/conftest.py`'s `clear_ambient_backend_uri` docstring): a leaked global
tracking URI once caused a test run to silently overwrite two production
champion aliases with fake test data. The fluent API also has a SECOND,
independent hazard: trace export is async by default, so an assessment
logged immediately after ending a trace can race the trace actually landing
in the store (observed empirically as a `ForeignKeyViolation` on
`assessments.trace_id`).

**Option B**, implemented in `trace_emit.py`, avoids BOTH hazards by using a
different, lower-level write path: `mlflow.tracing.client.TracingClient`
constructed DIRECTLY with an explicit `tracking_uri` (never the global
`mlflow.set_tracking_uri()`), with manually-built `TraceInfo`/`Span` objects
written via `TracingClient.start_trace(trace_info)` / `.log_spans(...)` /
`.log_assessment(...)` — synchronous, direct store writes with no OTel
exporter, no background thread, and no global state anywhere in the path.
See `trace_emit.py`'s own module docstring for the full technical rationale,
including a landmine found empirically during THIS implementation (not
anticipated by the earlier investigation): `client_request_id` is a real
`VARCHAR(50)` column on the live Neon-backed store, so a human-readable
business key is hashed down to a fixed, safe length rather than used
directly — this is genuinely semi-internal mlflow API surface, and that
risk is deliberately contained to `trace_emit.py` alone.

**Trace/span/assessment shape:**

- **Finish-position**: one trace per (race, model_version) — root span
  `predict-race` (`SpanType.TASK`), attributes `race_key`/`category`/
  `model_version`/`race_date`. Child spans (`SpanType.TOOL`): `score-model`
  (carries `model_version` — ONE span name shared across every
  model_version, so the Tool-calls page's per-name aggregation groups them
  together; per-version splitting stays possible via this attribute or the
  `model_version` trace tag) and `upsert-neon`. Assessments (`Feedback`,
  `source_type=CODE`, `source_id="sync_production"`): `place1_hit` through
  `place6_hit` (booleans — `place1_hit` is `FpRaceEvalRow.top1_hit`), each
  logged ONLY when that rank is applicable to the race (the place4/5/6
  small-field exclusion from `serve_eval.py` is respected exactly — a
  too-small field gets NO assessment for that rank, never a fabricated
  `False`), plus `top3_box_score` (float 0.0/1.0, `FpRaceEvalRow.
top3_box_hit`'s existing meaning verbatim — no more graduated per-race
  definition exists anywhere in this package).
- **Running-style (judgment call — see `trace_emit.py`'s own docstring for
  the full rationale)**: RS predicts PER-HORSE, not per-race, so this
  package emits ONE TRACE PER HORSE, root span `predict-horse`, attributes
  `race_key`/`ketto_toroku_bango`/`category`/`model_version`/`race_date`.
  Same two child span names as FP. One assessment, `predicted_class_hit`
  (boolean, `RsHorseEvalRow.hit` verbatim).
- **Timing is NOMINAL, always, and disclosed**: there is no real per-race/
  per-horse serving-latency measurement anywhere in this pipeline (a
  different team's domain, out of scope here). Every span carries an
  explicit `timing="approximate"` attribute; only the root span's
  `start_time` is real (the row's actual `prediction_generated_at`,
  historical timestamps honored). The Usage page's latency panel will show
  these nominal numbers until the serving pipeline itself ever emits real
  timing data.
- **`status` is always `OK`**: every row this reaches already matched a
  finalized result (see `serve_eval.py`'s join). A serving GAP day has zero
  rows and therefore zero traces — never an `ERROR` trace fabricated for a
  race/horse that simply never got served. `TraceState.ERROR` is
  unreachable code in `trace_emit.py` today, by design, not an oversight.
- **Destinations**: ONLY `finish-position/production-usage` and
  `running-style/production-usage` ever receive a trace — every other
  experiment in this package intentionally has none.

**Idempotency**: every trace's `client_request_id` is a deterministic hash of
its business key, and every emit call does a pre-emit `search_traces`
existence check before writing anything — a re-run (daily sync re-covering
an overlapping range, or a re-run of `backfill-traces`) creates zero
duplicate traces/assessments. Proven both by hermetic tests
(`tests/test_trace_emit.py`, `tests/test_backfill_traces.py`,
`tests/test_sync_production.py`) and by a real double-run against the live
store (see this feature's own verification notes).

**Volume/retention — a conscious deferral, not an oversight**: an
archival/retention scheduler for traces is deliberately NOT built here.
Trace/span/assessment rows will accumulate indefinitely in the tracking
store, exactly like every other run/metric this package already logs (see
this package's no-deletes policy elsewhere in this doc). Building retention
logic is explicitly OUT OF SCOPE for this feature.

**Backfill**: `backfill_traces.py` (CLI: `backfill-traces`) walks every
already-evaluated (date, category, model_version) run in both production-
usage experiments and emits traces for the underlying rows, using the
EXACT SAME `trace_emit.py` functions the daily sync calls — there is
exactly one trace-emission code path. See the Usage section above for the
command.

### Job-execution traces (`trace_emit.job_trace`): the other 10 experiments

The per-race/per-horse traces above are this package's Usage/Quality/
Tool-calls signal for the two `production-usage` experiments specifically —
they answer "what got served in production". Every OTHER real experiment in
`config.ALL_EXPERIMENT_NAMES` (10 of them; `internal/smoke-tests` is the one
deliberate exception, see below) instead gets a **job-execution trace**: one
trace per real CLI/job invocation, built on the SAME hardened `TracingClient`
plumbing in `trace_emit.py` (`job_trace`/`JobTrace`), but answering a
different, much simpler question — "did this job run, and how long did it
take" — with REAL, directly-measured wall-clock timing (no "nominal/
approximate" disclosure needed, unlike the per-race work: a job invocation is
happening right now, in-process, so its duration is directly observable).

```python
tracing_client = trace_emit.build_tracing_client(client)
with trace_emit.job_trace(tracing_client, experiment_id, "eval-cells") as t:
    with t.step("resolve-champion"):
        ...
    t.feedback("runs_created", float(runs_created))
```

- `t.step(name, **attributes)` — a nested context manager; each one becomes
  a `SpanType.TOOL` child span with REAL start/end timing.
- `t.feedback(name, value, *, rationale="")` — queues a bool/numeric
  `Feedback` assessment, flushed once the trace is written.
- On an exception, the trace AND its root span are logged `TraceState.ERROR`
  / `SpanStatusCode.ERROR` (a failed step's own span is marked ERROR too),
  and the exception ALWAYS re-raises — this is the ONE place in this package
  where `TraceState.ERROR` is reachable at all (see `trace_emit.py`'s own
  docstring for why it is unreachable everywhere else). A SEPARATE,
  best-effort safety net catches a failure while merely trying to PERSIST the
  trace itself (`warnings.warn`, never raised) — observability must never
  become a new failure mode for the job it observes.
- `t.discard()` silently drops the trace (nothing written) if the `with`
  block exits normally — used where a call spans a whole date range but only
  sometimes has anything to report (e.g. the `timelines` wiring below), so a
  day with nothing new to sync doesn't leave an empty, uninformative trace.
  Has no effect when the block raises.
- **No idempotency/dedup check, deliberately**: unlike the per-race work's
  `client_request_id` existence check, a job-execution trace is correctly
  ONE-PER-INVOCATION by design — every real run of a wired command SHOULD
  create a brand-new trace, since there is no "business key" a second call
  could ever collide with (a second real invocation is a second, genuinely
  new piece of work). Do not "fix" this into an idempotency check.
- **Historical honesty**: there is no record of past job invocations to
  reconstruct (unlike the per-race work's real historical
  `prediction_generated_at` timestamps) — no `backfill_job_traces.py` exists
  or is planned. These traces simply start accumulating from the first real
  invocation after this shipped; a store queried before that will correctly
  show empty tabs for these experiments until then.

**Per-experiment wiring** (job_name → steps → Feedback):

| Experiment                                                                                   | Call site                                                                                                                                                                                                                                                        | job_name                                                       | Steps                                                                                           | Feedback                                                               |
| -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `finish-position/champion-eval`, `running-style/champion-eval`                               | `champion_cell_eval.eval_champion_cells_for_category`                                                                                                                                                                                                            | `eval-champion-cells`                                          | `resolve-champion`, `eval-join`, `aggregate-cells`, `log-run`                                   | `has_champion_coverage` (bool), `cells_evaluated` (numeric)            |
| `finish-position/cell-eval`, `running-style/cell-eval`                                       | `cell_eval_runs.eval_cells_for_category`                                                                                                                                                                                                                         | `eval-cells`                                                   | `resolve-champion`, `collect-rows`, `build-eval-rows`, `log-runs`                               | `runs_created` (numeric), `runs_skipped_low_volume` (numeric)          |
| `finish-position/registry-backfill`                                                          | `backfill_finish_position.backfill_finish_position`                                                                                                                                                                                                              | `backfill-finish-position`                                     | `register-base-versions`, `register-per-class-versions`, `champion-sync`, `cell-routing-ingest` | `champion_sync_ok` (bool, aggregate across categories)                 |
| `running-style/registry-backfill`                                                            | `backfill_running_style.backfill_running_style`                                                                                                                                                                                                                  | `backfill-running-style`                                       | `register-versions`, `register-production-pointers`, `champion-sync`                            | `champion_sync_ok` (bool, aggregate)                                   |
| `finish-position/serve-accuracy` or `running-style/eval`                                     | `ingest_eval.ingest_serve_accuracy`                                                                                                                                                                                                                              | `ingest-serve-accuracy`                                        | (none named)                                                                                    | `races` (numeric, best-effort); `eval_regime`/`era` as span ATTRIBUTES |
| `finish-position/serve-accuracy` (SERVE mode) or `running-style/eval`                        | `training_run.log_training_run`                                                                                                                                                                                                                                  | `log-training-run-serve`                                       | (none named)                                                                                    | `races` (numeric); `eval_regime`/`era` as span ATTRIBUTES              |
| `finish-position/wf-eval` or `running-style/eval` (WF/oos/self-consistency/unspecified mode) | `training_run.log_training_run`                                                                                                                                                                                                                                  | `log-training-run-wf`                                          | `parse-manifest`, `log-metrics`, `ingest-metadata`                                              | `ingestion_ok` (bool)                                                  |
| `timelines`                                                                                  | `sync_production.sync_production_range` (own trace); `ingest_eval.ingest_serve_accuracy` (nested); `training_run.log_training_run` (nested, via `_maybe_log_serve_timeline_point`) — **NOT** `backfill_serve_timeline.backfill_serve_timeline` itself, see below | `sync-production`, `ingest-serve-accuracy`, `log-training-run` | (none named; each nested trace wraps exactly the real `timeline.upsert_timeline_point` call)    | `points_appended` (numeric)                                            |

**`races` Feedback — a documented best-effort proxy**: neither the
finish-position nor running-style `serve_accuracy_report.py` payload shape
guarantees a field literally named `races`; `ingest_eval._races_feedback_value`
/ `training_run._races_feedback_value` each check a small, independently-
duplicated set of known key names (`races`, `race_count`, `total_horses`,
`horse_count`) across whichever section(s) are present, returning `0.0` (a
deliberately honest "unknown", never a fabricated nonzero guess) when none
match.

**`timelines` wiring — "one trace per call, not per point" (mostly)**:
`sync_production_range` walks a whole date range and may call `timeline.
upsert_timeline_point` many times across it — bundling all of them into ONE
job trace per top-level call (not one per date) is the "meaningful unit of
work" grain this shape calls for. Its `points_appended` Feedback is a
CALL-COUNT proxy (`fp_eval_logged + rs_eval_logged`), not a finer "how many
individual metric keys were genuinely new after per-step dedup" count —
threading that back through `timeline.py`'s widely-used return contract
would be a much larger change. When nothing was appended (the common case on
most days), the trace is DISCARDED (`JobTrace.discard()`), not logged empty.
This `timelines` job trace is DELIBERATELY UNCONDITIONAL, independent of
`emit_traces`/`--no-traces` (that flag scopes only the per-race/per-horse
emission into the production-usage experiments — a much larger trace volume
— not this one lightweight summary trace into a different experiment).
`ingest_eval.ingest_serve_accuracy` and `training_run.log_training_run`
instead know SYNCHRONOUSLY whether they will upsert 0, 1, or 2 points (no
date-range loop involved), so their nested `timelines` trace is simply never
opened at all when there is nothing to report — no `discard()` needed there.

**`backfill_serve_timeline` is the ONE deliberate exception — no trace of its
own, by design, not an oversight**: it never calls `timeline.upsert_
timeline_point` directly; every date it processes routes through `ingest_
eval.ingest_serve_accuracy` (already fully job_trace-wired). A first
implementation ALSO wrapped `backfill_serve_timeline`'s own whole-range loop
in a second, redundant `timelines` job trace — and a real test run caught the
resulting double-count immediately (a 2-date range produced 3 `timelines`
traces instead of 1: 2 from the per-date `ingest_serve_accuracy` delegation

- 1 redundant aggregate). The correct signal for this module is therefore
  "N traces, one per genuinely ingested date", entirely via delegation — see
  `backfill_serve_timeline.py`'s own docstring for the full incident/rationale.

**`internal/smoke-tests` gets NEITHER trace shape, deliberately**: it is a
manual, one-shot smoke-test destination (see "Smoke-test runs" below), not a
real recurring job or production-serving path — empty Usage/Quality/
Tool-calls tabs there are by design, not a gap to fill.

### ⚠️ `win5-xgb-*-rs-overlay-*` is intentionally out of scope

The `finish-position/serve-accuracy` experiment contains a
`win5-xgb-*-rs-overlay-*` run family (e.g.
`win5-xgb-v7-lineage-v1-rs-overlay-20260627`) representing ad-hoc,
manually-dated batch overlay evaluations run outside this package's normal
ingestion paths — not a registry-managed model family. There is no
corresponding registered model/version for it, so it deliberately carries no
`champion`/`challenger` alias, no `routing_scope` tag, and no
`backfill-finish-position` coverage. Do not treat its presence in the MLflow
UI as drift or an oversight, and do not "fix" it by registering a model or
backfilling tags for it.

### Smoke-test runs: `internal/smoke-tests` + `run_type=smoke`

A one-shot manual smoke run (e.g. a throwaway dry-run of `log-training-run`
to verify the ingestion pipeline end-to-end against the real store) should
never land in a real per-task experiment (`finish-position/wf-eval`,
`running-style/eval`, ...), where it would sit alongside genuine
backfill/eval/production data and confuse any dashboard or headline-metric
rollup built against that experiment. `config.EXPERIMENT_SMOKE_TESTS`
(`"internal/smoke-tests"`, included in `config.ALL_EXPERIMENT_NAMES` so
`init` always creates it) is the dedicated home for this.

Route a smoke run there via `log-training-run`'s existing, already-generic
manifest overrides — no new CLI flag or schema field is needed:

```json
{
  "schema": "hr-mlflow-training-run/v1",
  "task": "finish-position",
  "category": "jra",
  "model_version": "smoke-test-dry-run",
  "eval_regime": "unspecified",
  "experiment": "internal/smoke-tests",
  "tags": { "run_type": "smoke" }
}
```

The manifest's `experiment` field (already free-form, not restricted to a
canonical list — see `training_run.py::_resolve_experiment`'s override) picks
the target experiment, and its generic `tags` object (merged verbatim into
the run's tags) is what MUST carry `run_type=smoke` on every run logged this
way — this is a required convention, not a suggestion, since it is the only
thing that lets a stray real manifest accidentally routed to
`internal/smoke-tests` be told apart from a genuine smoke run later. Nothing
in `log_training_run` enforces `run_type=smoke` automatically when routing
to this experiment (by design — the manifest contract stays uniform across
every destination experiment), so a caller wiring up a new smoke check must
set both fields itself.

### Filtering out junk runs in the MLflow UI

A handful of runs across this store are marked `junk=true` (verified
one-offs, diagnostic throwaways, or superseded test artifacts — see e.g.
`timeline.py`'s v2-migration entry above for the 3 diagnostic `timelines`
runs tagged this way) rather than deleted, per this package's no-deletes
policy. To hide them while browsing an experiment's run list in the MLflow
UI, use the search bar's filter syntax:

```
tags.junk != 'true'
```

`junk` is a plain tag, not a built-in column, so it is not shown by default
— use the run-list column picker (the columns/gear icon above the run
table) to add `junk` (and, where relevant, `run_type`) as a visible column
before filtering, so a run's junk status is visible at a glance rather than
only affecting which rows appear.

### Two views over per-cell accuracy: champion rollup vs. per-cell drilldown

Two different modules both expose CELL-granularity accuracy, for two
different jobs — reach for whichever matches the question you're actually
asking:

- **`champion_cell_eval.py`** (`finish-position/champion-eval`,
  `running-style/champion-eval`) answers "how is the model we're currently
  serving doing, broken down by cell?" — it logs exactly ONE run per
  (category, task), with the full per-cell breakdown as a table _inside_
  that single run. Good for a quick, holistic health check of the current
  champion, or for diffing one day's rollup against another's summary
  fields.
- **`cell_eval_runs.py`** (`finish-position/cell-eval`,
  `running-style/cell-eval`) answers "how has THIS specific cell's accuracy
  trended over time, and how does the champion compare to a specific
  cell-routed variant there?" — it logs one PERSISTENT run PER (category,
  cell, model_version), so a single cell (e.g. 中山 × class A × sprint ×
  turf) for a single served version renders as its own drillable run with a
  real line chart across days. Use this when you need to inspect or compare
  individual cells/variants rather than skim an overall rollup.

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
`sync-production`, then `eval-champion-cells`, then `eval-cells` once daily
at **22:30 JST** (same-day racing has finished and results have typically
already mirrored into the local PostgreSQL replica by then). `eval-cells` is
the third step: it scores every model_version that served enough volume
that window (not just the champion) at cell granularity, appending one
metric point to each (category, cell, model_version)'s own persistent run —
this is what makes a single cell's accuracy trend visible as a line chart in
the UI, day over day. All three commands are idempotent, so a delayed
launchd catch-up fire (e.g. after the Mac was asleep at 22:30) is harmless to
re-run.

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
