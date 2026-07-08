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

from mlflow_tracking import cells, config, timeline
from mlflow_tracking.logging_api import (
    get_or_create_experiment,
    log_batch_chunked,
    log_eval_run,
    log_json_artifact,
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
) -> str:
    run = client.create_run(experiment_id, run_name=run_name)
    run_id = run.info.run_id
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
    query.
    """
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        rows = con.execute("SELECT * FROM trials").fetchall()
        columns = [desc[0] for desc in con.description]
    finally:
        con.close()

    groups = _group_trial_rows(columns, rows)
    experiment_id = get_or_create_experiment(client, experiment_name)
    return [
        _log_trial_group(client, experiment_id, key, records) for key, records in groups.items()
    ]


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
    run = client.create_run(experiment_id, run_name=f"{category}-{date_str}")
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

    # Timeline upsert rides along in the same log_batch_chunked call below by
    # writing the returned timeline run_id into `tags` before `tag_items` is
    # built from it -- see timeline.py's module docstring for why a
    # persistent per-(task, category) run is needed for the UI's line charts
    # to show a real trend instead of one dot per day.
    if isinstance(fp, dict):
        fp_timeline_metrics = timeline.fp_metrics_for_timeline(fp)
        if fp_timeline_metrics:
            timeline_run_id = timeline.upsert_timeline_point(
                client, "finish-position", category, date_str, fp_timeline_metrics
            )
            tags["timeline_run_id:finish-position"] = timeline_run_id
    if isinstance(rs, dict):
        rs_timeline_metrics = timeline.rs_metrics_for_timeline(rs)
        if rs_timeline_metrics:
            timeline_run_id = timeline.upsert_timeline_point(
                client, "running-style", category, date_str, rs_timeline_metrics
            )
            tags["timeline_run_id:running-style"] = timeline_run_id

    tag_items = [RunTag(k, v) for k, v in tags.items() if v]
    log_batch_chunked(client, run_id, metrics=metric_items, tags=tag_items)
    log_json_artifact(client, run_id, "serve_accuracy.json", payload)
    client.set_terminated(run_id, status="FINISHED")
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


def ingest_cell_report(
    client: MlflowClient,
    path: Path,
    *,
    eval_regime: str,
    aggregate_metrics: Mapping[str, float] | None = None,
    experiment_name: str = CELL_REPORT_EXPERIMENT,
    run_name: str | None = None,
) -> str:
    """Ingest a generic cell-metrics table (parquet or JSON records) as one run.

    Universal entry point for research-harness reports (retest_wf.py style,
    finish-position OR running-style): cell dimensions are normalized via
    cells.normalize_cell_columns semantics before logging, and the full table
    is preserved as a table + parquet artifact via logging_api.log_eval_run.
    `eval_regime` is required (see `validate_eval_regime`) because a cell
    report can just as easily be a leaky self-consistency report as a true
    held-out one, and that distinction must stay visible in the UI.
    """
    validate_eval_regime(eval_regime)
    cell_df = normalize_cell_dataframe(read_cell_table(path))
    return log_eval_run(
        client,
        experiment_name,
        metrics=aggregate_metrics,
        cell_df=cell_df,
        run_name=run_name if run_name is not None else path.stem,
        tags={"source_file": path.name, "eval_regime": eval_regime},
    )
