"""File-based ingestion of research-harness evaluation artifacts into MLflow.

All three entry points are file-based only (no database network connections —
Neon cost rule): trial_registry duckdb files, serve_accuracy_report.py --json
output, and generic cell-metrics tables (parquet/JSON).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

import duckdb
import pandas as pd
from mlflow import MlflowClient
from mlflow.entities import Metric, RunTag

from mlflow_tracking import cells, config, timeline, trace_emit
from mlflow_tracking.logging_api import (
    get_or_create_experiment,
    log_batch_chunked,
    log_cell_table,
    log_eval_run,
    log_json_artifact,
    select_headline_metrics,
)

TRIAL_REGISTRY_EXPERIMENT: Final[str] = config.EXPERIMENT_FP_WF_EVAL
SERVE_ACCURACY_EXPERIMENT: Final[str] = config.EXPERIMENT_FP_SERVE_ACCURACY
CELL_REPORT_EXPERIMENT: Final[str] = config.EXPERIMENT_FP_WF_EVAL

TRIAL_METRIC_COLUMNS: Final[tuple[str, ...]] = (
    "rank1_accuracy",
    "rank2_accuracy",
    "rank3_accuracy",
    "rank4_accuracy",
    "rank5_accuracy",
    "rank6_accuracy",
    "top1_accuracy",
    "place2_accuracy",
    "place3_accuracy",
    "ndcg_at_3",
    "race_count",
    "rank1_lb95",
    "rank2_lb95",
    "rank3_lb95",
    "rank4_lb95",
    "rank5_lb95",
    "rank6_lb95",
)
TRIAL_TAG_COLUMNS: Final[tuple[str, ...]] = (
    "category",
    "class_code",
    "subgroup_dimension",
    "subgroup_value",
    "season_band",
    "verdict",
    "verdict_reason",
    "model_version",
    "train_window_start",
    "train_window_end",
)

FP_SERVE_METRIC_KEYS: Final[tuple[str, ...]] = (
    "top1_pct",
    "place2_pct",
    "place3_pct",
    "fukusho_2p_pct",
    "top3_box_pct",
)
FP_SUBGROUP_METRIC_KEYS: Final[tuple[str, ...]] = FP_SERVE_METRIC_KEYS
RS_SERVE_METRIC_KEYS: Final[tuple[str, ...]] = ("overall_accuracy_pct", "macro_f1_pct")

# Idempotency tag for `ingest_serve_accuracy`, mirroring sync_production.py's
# `SYNC_KEY_TAG` / timeline.py's `TIMELINE_KEY_TAG` tag-search idiom: a
# repeated manual/CLI ingest of the same underlying payload (verified real
# duplicate -- two identical win5-overlay runs created 28 seconds apart by a
# repeated call) must reuse the same run instead of creating a new one every
# time.
SERVE_KEY_TAG: Final[str] = "serve_key"

# Idempotency tag for `ingest_cell_report`'s OPTIONAL `run_key` parameter --
# same tag-search idiom as `SERVE_KEY_TAG` above, but opt-in: unlike
# `ingest_serve_accuracy` (which always derives its own key from the
# payload), a generic cell-metrics table has no reliable identity of its own
# (two unrelated `retest_wf.py` reports can share the same --run-name), so
# reuse is only attempted when a caller explicitly opts in via `--run-key`.
RUN_KEY_TAG: Final[str] = "run_key"

# Best-effort key list for the job_trace `races` Feedback on `ingest_serve_
# accuracy` (see trace_emit.job_trace) -- neither `FP_SERVE_METRIC_KEYS` nor
# `RS_SERVE_METRIC_KEYS` above includes a race/horse COUNT field, so this
# checks a small set of known key names that MAY appear in either section's
# raw payload dict, returning 0.0 (a deliberately honest "unknown", never a
# fabricated nonzero guess) when none of them are present.
_RACES_LIKE_KEYS: Final[tuple[str, ...]] = ("races", "race_count", "total_horses", "horse_count")


def _races_feedback_value(fp: object, rs: object) -> float:
    """Best-effort extraction of a "how many races/horses did this serve-
    accuracy check cover" numeric value for the job_trace `races` Feedback
    (see `_RACES_LIKE_KEYS`'s own comment) -- checks the finish_position
    section first, then running_style, returning the first match."""
    for section in (fp, rs):
        if not isinstance(section, dict):
            continue
        for key in _RACES_LIKE_KEYS:
            value = section.get(key)
            if isinstance(value, int | float):
                return float(value)
    return 0.0


def _group_trial_rows(
    columns: list[str], rows: list[tuple[object, ...]]
) -> dict[str, list[dict[str, object]]]:
    """Group trial rows by model_version, falling back to one group per trial_id
    for rows with no model_version (exploratory trials not yet tied to a run)."""
    groups: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        record = dict(zip(columns, row, strict=True))
        model_version = record.get("model_version")
        key = str(model_version) if model_version else f"trial:{record.get('trial_id')}"
        groups.setdefault(key, []).append(record)
    return groups


def _log_trial_group(
    client: MlflowClient,
    experiment_id: str,
    run_name: str,
    records: list[dict[str, object]],
) -> str | None:
    """Log one trial-group run, or return None without creating a run when
    `records` yields zero metrics.

    `metric_items`/`tag_items` are computed FIRST, before any
    `client.create_run` call, specifically so that check is possible: this
    function never logs an artifact (no `log_json_artifact`/`log_table` call
    exists here), so a group where none of `TRIAL_METRIC_COLUMNS` are
    populated across every record would otherwise create a genuinely empty,
    useless run with no metrics and no artifacts (verified real example: a
    wf-eval run named "v1" with nothing logged on it). `tag_items` alone
    being non-empty does not save a group -- tags with no metrics are still
    not worth a run -- so only `metric_items` gates creation.
    """
    metric_items: list[Metric] = []
    tag_values: dict[str, set[str]] = {}
    for step, record in enumerate(records):
        for column in TRIAL_METRIC_COLUMNS:
            value = record.get(column)
            if isinstance(value, int | float):
                metric_items.append(Metric(column, float(value), 0, step))
        for column in TRIAL_TAG_COLUMNS:
            value = record.get(column)
            if value is not None:
                tag_values.setdefault(column, set()).add(str(value))
    if not metric_items:
        return None

    run = client.create_run(experiment_id, run_name=run_name)
    run_id = run.info.run_id
    tag_items = [
        RunTag(column, values.pop() if len(values) == 1 else ";".join(sorted(values)))
        for column, values in tag_values.items()
    ]
    log_batch_chunked(client, run_id, metrics=metric_items, tags=tag_items)
    client.set_terminated(run_id, status="FINISHED")
    return run_id


def ingest_trial_registry(
    client: MlflowClient,
    duckdb_path: Path,
    *,
    experiment_name: str = TRIAL_REGISTRY_EXPERIMENT,
) -> list[str]:
    """Ingest a trial_registry_{category}.duckdb file, one run per model_version
    (rows without a model_version get one run per trial_id).

    Read-only DuckDB connection: this is a local file read, not a Neon/network
    query. A group whose records populate none of `TRIAL_METRIC_COLUMNS`
    produces no run at all (see `_log_trial_group`'s own docstring) -- such a
    group's `None` result is filtered out here so this function's own
    `list[str]` return type stays honest (no `None` entries for callers to
    trip over).
    """
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        rows = con.execute("SELECT * FROM trials").fetchall()
        columns = [desc[0] for desc in con.description]
    finally:
        con.close()

    groups = _group_trial_rows(columns, rows)
    experiment_id = get_or_create_experiment(client, experiment_name)
    run_ids = [
        _log_trial_group(client, experiment_id, key, records) for key, records in groups.items()
    ]
    return [run_id for run_id in run_ids if run_id is not None]


def validate_eval_regime(eval_regime: str) -> None:
    """Fail closed on a blank eval_regime.

    RS eval numbers exist in two very different regimes — true out-of-sample
    (JRA ~48.3%, NAR ~52.3%) vs. a leaky self-consistency regime that reports
    a misleading ~94-95%
    (docs/finish-position-accuracy/history/rs-model-audit.md) — so this must
    never silently default; callers who don't know the regime must pass
    "unspecified" explicitly so it stays visible in the UI.
    """
    if not eval_regime.strip():
        raise ValueError(
            "eval_regime must not be blank; pass 'unspecified' explicitly if the regime is unknown"
        )


def _resolve_serve_accuracy_experiment(fp: object) -> str:
    """Route to the FP serve-accuracy experiment when a finish_position
    section is present (even alongside running_style), else the RS eval
    experiment."""
    if isinstance(fp, dict):
        return config.EXPERIMENT_FP_SERVE_ACCURACY
    return config.EXPERIMENT_RS_EVAL


def _serve_accuracy_identity(fp: object, rs: object) -> tuple[str, str, str]:
    """Extract (date_str, category, era) from whichever section is present."""
    for section in (fp, rs):
        if isinstance(section, dict):
            return (
                str(section.get("date_str", "")),
                str(section.get("category", "")),
                str(section.get("era", "")),
            )
    raise ValueError("serve-accuracy payload has neither finish_position nor running_style section")


def _serve_accuracy_model_version_identity(fp: object, rs: object) -> str:
    """Return a stable, deterministic model-version identity string for the
    `serve_key` idempotency tag (see `SERVE_KEY_TAG`), so re-ingesting the
    exact same payload always derives the exact same key.

    Prefers the running-style section's singular `model_version` field when
    `rs` is a dict and that field is a non-empty string -- an `rs` payload
    always identifies exactly one model. Finish-position payloads have no
    singular equivalent: they only carry `model_version_counts`, a
    `{version: count}` dict, since a single day's FP serve-accuracy report
    can legitimately span multiple model versions mid-rollout. For that case,
    the sorted set of version keys is joined with "+", which is
    order-independent (the same set of versions always produces the same
    string regardless of dict insertion order) and ignores the counts
    themselves (irrelevant to identity). Falls back to the literal
    "unspecified" when neither section yields anything identifying, so this
    helper never raises -- an unidentifiable payload still gets a stable (if
    coarse) key rather than blocking the ingest.
    """
    if isinstance(rs, dict):
        rs_model_version = rs.get("model_version")
        if isinstance(rs_model_version, str) and rs_model_version:
            return rs_model_version
    if isinstance(fp, dict):
        model_version_counts = fp.get("model_version_counts")
        if isinstance(model_version_counts, dict) and model_version_counts:
            return "+".join(sorted(str(version) for version in model_version_counts))
    return "unspecified"


def _find_serve_run(client: MlflowClient, experiment_id: str, serve_key: str) -> str | None:
    """Find the run tagged with `serve_key`, mirroring
    `sync_production._find_sync_run` / `timeline._find_timeline_run`'s exact
    tag-search idiom."""
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{SERVE_KEY_TAG} = '{serve_key}'",
        max_results=1,
    )
    return matches[0].info.run_id if matches else None


def _fp_serve_metrics(fp: Mapping[str, object]) -> list[Metric]:
    metric_items: list[Metric] = []
    for key in FP_SERVE_METRIC_KEYS:
        value = fp.get(key)
        if isinstance(value, int | float):
            metric_items.append(Metric(f"fp_{key}", float(value), 0, 0))
    subgroups = fp.get("subgroups")
    if not isinstance(subgroups, list):
        return metric_items
    for subgroup in subgroups:
        if not isinstance(subgroup, dict):
            continue
        dimension = subgroup.get("dimension")
        band = subgroup.get("band")
        for key in FP_SUBGROUP_METRIC_KEYS:
            value = subgroup.get(key)
            if isinstance(value, int | float):
                metric_items.append(Metric(f"fp_{dimension}_{band}_{key}", float(value), 0, 0))
    return metric_items


def _rs_serve_metrics(rs: Mapping[str, object]) -> list[Metric]:
    result: list[Metric] = []
    for key in RS_SERVE_METRIC_KEYS:
        value = rs.get(key)
        if isinstance(value, int | float):
            result.append(Metric(f"rs_{key}", float(value), 0, 0))
    return result


def ingest_serve_accuracy(
    client: MlflowClient,
    json_path: Path,
    *,
    eval_regime: str,
    experiment_name: str | None = None,
) -> str:
    """Ingest one serve_accuracy_report.py --json payload as one MLflow run,
    tagged with date/category/era/eval_regime/model_version_counts.

    Detects and accepts both finish-position and running-style payload
    shapes (that script has a mode for each). Unless `experiment_name` is
    given explicitly, routes to finish-position/serve-accuracy when a
    finish_position section is present (even alongside running_style, since
    that combined shape is still fundamentally a serve-health check), else to
    running-style/eval.

    Idempotent by `serve_key = "{model_version_identity}:{date_str}:{category}"`
    (see `_serve_accuracy_model_version_identity`/`SERVE_KEY_TAG`), mirroring
    `sync_production.py`'s `sync_key` pattern: re-ingesting the identical
    payload (a real, verified duplicate -- two identical win5-overlay runs
    were created 28 seconds apart by a repeated call before this) reuses the
    same run instead of creating a new one every time. Unlike
    `sync_production.py`'s base-tracking tags (which are logged at most ONCE
    per sync_key, since predictions are immutable once generated), every
    field below is UNCONDITIONALLY re-logged on a reused run -- this is a
    deliberate update-in-place, not a skip: `log_batch_chunked`'s metrics are
    keyed by (name, step) so re-logging the same values is a no-op in effect,
    and `log_json_artifact` simply overwrites the same artifact path, so a
    second call with a genuinely different payload for the same identity
    (e.g. a corrected number for the same day) also has its LATEST data win,
    rather than being silently dropped.

    Also upserts a point onto the `timelines` experiment's per-(task,
    category) persistent run for whichever of finish-position/running-style
    sections are present (see timeline.py's module docstring for why this
    exists as a separate layer). This is a MANUAL/CLI-only ingestion path
    (see this CLI subcommand's own `ingest-serve-accuracy` docs) -- the
    empirically-real, automatic live-serve path is
    `training_run.py::log_training_run`'s own serve-eval-regime timeline
    hook, which this function's own JSON-payload shape does not go through.
    Both are wired so a manual backfill (this function) and the live cron
    path (training_run.py) converge on the same timeline runs.

    ★ Job-execution trace (2026-07-11): one `trace_emit.job_trace` per call
    into this SAME `resolved_experiment` (`finish-position/serve-accuracy`
    or `running-style/eval`) -- Feedback `races` (numeric, see
    `_races_feedback_value`); `eval_regime`/`era` are span ATTRIBUTES, not
    Feedback (per this package's own convention: a Feedback is a scored
    judgment about outcome quality, an attribute is descriptive metadata).
    When this call ALSO upserts a `timelines` point (whenever the finish-
    position/running-style timeline-metric extraction is non-empty for the
    section(s) present), the REAL `timeline.upsert_timeline_point` call
    itself is wrapped in its OWN NESTED job_trace landing in
    `config.EXPERIMENT_TIMELINES` (Feedback `points_appended`, 1 or 2) --
    mirroring `sync_production.py`'s "one trace per call, not per point"
    convention, except here the decision is known SYNCHRONOUSLY (0/1/2,
    computed before opening the trace) rather than needing `JobTrace.
    discard()` after a whole date-range loop, so the inner trace is simply
    never opened at all when nothing would be appended.
    """
    validate_eval_regime(eval_regime)
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    fp = payload.get("finish_position")
    rs = payload.get("running_style")
    date_str, category, era = _serve_accuracy_identity(fp, rs)
    resolved_experiment = (
        experiment_name if experiment_name is not None else _resolve_serve_accuracy_experiment(fp)
    )

    experiment_id = get_or_create_experiment(client, resolved_experiment)
    tracing_client = trace_emit.build_tracing_client(client)
    with trace_emit.job_trace(
        tracing_client,
        experiment_id,
        "ingest-serve-accuracy",
        attributes={"category": category, "date": date_str, "eval_regime": eval_regime, "era": era},
    ) as t:
        model_version_identity = _serve_accuracy_model_version_identity(fp, rs)
        serve_key = f"{model_version_identity}:{date_str}:{category}"
        run_id = _find_serve_run(client, experiment_id, serve_key)
        if run_id is None:
            run = client.create_run(
                experiment_id,
                run_name=f"{category}-{date_str}",
                tags={SERVE_KEY_TAG: serve_key},
            )
            run_id = run.info.run_id

        tags: dict[str, str] = {
            "date": date_str,
            "category": category,
            "era": era,
            "eval_regime": eval_regime,
        }
        metric_items: list[Metric] = []
        if isinstance(fp, dict):
            metric_items.extend(_fp_serve_metrics(fp))
            model_version_counts = fp.get("model_version_counts")
            if isinstance(model_version_counts, dict) and model_version_counts:
                tags["model_version_counts"] = json.dumps(model_version_counts, ensure_ascii=False)
        if isinstance(rs, dict):
            metric_items.extend(_rs_serve_metrics(rs))
            rs_model_version = rs.get("model_version")
            if isinstance(rs_model_version, str) and rs_model_version:
                tags["rs_model_version"] = rs_model_version

        # Timeline upsert rides along in the same log_batch_chunked call
        # below by writing the returned timeline run_id into `tags` before
        # `tag_items` is built from it -- see timeline.py's module docstring
        # for why a persistent per-(task, category) run is needed for the
        # UI's line charts to show a real trend instead of one dot per day.
        fp_timeline_metrics = timeline.fp_metrics_for_timeline(fp) if isinstance(fp, dict) else {}
        rs_timeline_metrics = timeline.rs_metrics_for_timeline(rs) if isinstance(rs, dict) else {}
        if fp_timeline_metrics or rs_timeline_metrics:
            timelines_experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
            points_appended = 0
            with trace_emit.job_trace(
                tracing_client,
                timelines_experiment_id,
                "ingest-serve-accuracy",
                attributes={"category": category, "date": date_str},
            ) as tt:
                if fp_timeline_metrics:
                    timeline_run_id = timeline.upsert_timeline_point(
                        client, "finish-position", category, date_str, fp_timeline_metrics
                    )
                    tags["timeline_run_id:finish-position"] = timeline_run_id
                    points_appended += 1
                if rs_timeline_metrics:
                    timeline_run_id = timeline.upsert_timeline_point(
                        client, "running-style", category, date_str, rs_timeline_metrics
                    )
                    tags["timeline_run_id:running-style"] = timeline_run_id
                    points_appended += 1
                tt.feedback("points_appended", float(points_appended))

        tag_items = [RunTag(k, v) for k, v in tags.items() if v]
        log_batch_chunked(client, run_id, metrics=metric_items, tags=tag_items)
        log_json_artifact(client, run_id, "serve_accuracy.json", payload)
        client.set_terminated(run_id, status="FINISHED")
        t.feedback("races", _races_feedback_value(fp, rs))
    return run_id


def read_cell_table(path: Path) -> pd.DataFrame:
    """Read a cell-metrics table from a parquet or JSON file."""
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return pd.DataFrame(payload)
    raise ValueError(f"unsupported cell-report file extension: {path.suffix}")


def normalize_cell_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply cells.COLUMN_ALIASES renames to a cell-metrics DataFrame's columns.

    Canonical names always win: an alias column colliding with an existing
    canonical column is dropped rather than overwriting it.
    """
    result = df.copy()
    rename_map: dict[str, str] = {}
    drop_columns: list[str] = []
    for alias, canonical in cells.COLUMN_ALIASES.items():
        if alias not in result.columns:
            continue
        if canonical in result.columns:
            drop_columns.append(alias)
        else:
            rename_map[alias] = canonical
    if drop_columns:
        result = result.drop(columns=drop_columns)
    if rename_map:
        result = result.rename(columns=rename_map)
    return result


def _find_run_by_key(client: MlflowClient, experiment_id: str, run_key: str) -> str | None:
    """Find the run tagged with `run_key`, mirroring `_find_serve_run`'s /
    `sync_production._find_sync_run`'s / `timeline._find_timeline_run`'s exact
    tag-search idiom."""
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{RUN_KEY_TAG} = '{run_key}'",
        max_results=1,
    )
    return matches[0].info.run_id if matches else None


def _log_cell_report_onto_run(
    client: MlflowClient,
    run_id: str,
    *,
    cell_df: pd.DataFrame,
    aggregate_metrics: Mapping[str, float] | None,
    tags: Mapping[str, str],
) -> None:
    """Log a cell report's metrics/tags/table onto an ALREADY-OPEN run.

    Mirrors `training_run.py`'s own `_ingest_cell_report` (which logs a cell
    report into an already-open run for the identical reason: a caller-
    supplied run identity -- there a `hr-mlflow-training-run/v1` manifest's
    own run, here a `run_key`-reused run -- must be updated in place, not
    re-created via `logging_api.log_eval_run`, which only ever creates a
    fresh run). Every field is UNCONDITIONALLY re-logged, exactly like
    `ingest_serve_accuracy`'s own reused-run semantics: `log_batch_chunked`'s
    metrics are keyed by (name, step) so re-logging identical values is a
    no-op in effect, and `log_cell_table`/tag writes simply overwrite, so a
    later call with a genuinely corrected report also has its latest data win.
    """
    metric_items = [
        Metric(str(k), float(v), 0, 0) for k, v in (aggregate_metrics or {}).items()
    ]
    headline = select_headline_metrics(cell_df)
    metric_items.extend(Metric(k, v, 0, 0) for k, v in headline.items())
    tag_items = [RunTag(k, v) for k, v in tags.items()]
    log_batch_chunked(client, run_id, metrics=metric_items, tags=tag_items)
    log_cell_table(client, run_id, cell_df)
    client.set_terminated(run_id, status="FINISHED")


def ingest_cell_report(
    client: MlflowClient,
    path: Path,
    *,
    eval_regime: str,
    aggregate_metrics: Mapping[str, float] | None = None,
    experiment_name: str = CELL_REPORT_EXPERIMENT,
    run_name: str | None = None,
    run_key: str | None = None,
) -> str:
    """Ingest a generic cell-metrics table (parquet or JSON records) as one run.

    Universal entry point for research-harness reports (retest_wf.py style,
    finish-position OR running-style): cell dimensions are normalized via
    cells.normalize_cell_columns semantics before logging, and the full table
    is preserved as a table + parquet artifact via logging_api.log_eval_run.
    `eval_regime` is required (see `validate_eval_regime`) because a cell
    report can just as easily be a leaky self-consistency report as a true
    held-out one, and that distinction must stay visible in the UI.

    `run_key` (optional, default None) makes repeated ingestion of the SAME
    underlying report idempotent: when given, an existing run tagged
    `run_key` (see `RUN_KEY_TAG`) in `experiment_name` is found and reused
    (its metrics/tags/table are re-logged in place via
    `_log_cell_report_onto_run` -- see that function's own docstring for why
    this bypasses `logging_api.log_eval_run`) instead of creating a new run.
    Omitted (the default), behavior is UNCHANGED from before this parameter
    existed: every call creates a brand-new run via `logging_api.log_eval_run`,
    exactly as today -- a generic cell-metrics table has no reliable identity
    of its own (unlike `ingest_serve_accuracy`'s payload-derived `serve_key`),
    so opt-in reuse via an explicit, caller-chosen key is the only safe
    default here.
    """
    validate_eval_regime(eval_regime)
    cell_df = normalize_cell_dataframe(read_cell_table(path))
    resolved_run_name = run_name if run_name is not None else path.stem
    tags = {"source_file": path.name, "eval_regime": eval_regime}

    if run_key is None:
        return log_eval_run(
            client,
            experiment_name,
            metrics=aggregate_metrics,
            cell_df=cell_df,
            run_name=resolved_run_name,
            tags=tags,
        )

    experiment_id = get_or_create_experiment(client, experiment_name)
    existing_run_id = _find_run_by_key(client, experiment_id, run_key)
    if existing_run_id is not None:
        run_id = existing_run_id
    else:
        run = client.create_run(
            experiment_id, run_name=resolved_run_name, tags={RUN_KEY_TAG: run_key}
        )
        run_id = run.info.run_id
    _log_cell_report_onto_run(
        client,
        run_id,
        cell_df=cell_df,
        aggregate_metrics=aggregate_metrics,
        tags={**tags, RUN_KEY_TAG: run_key},
    )
    return run_id
