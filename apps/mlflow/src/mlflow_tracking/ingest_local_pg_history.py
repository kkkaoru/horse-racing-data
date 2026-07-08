"""File-based ingestion of local-PostgreSQL-replica historical evaluation exports.

Source tables: ``model_prediction_evaluations`` and
``running_style_model_bucket_evaluations`` on the LOCAL PostgreSQL replica
(``apps/local-postgresql``, read-only, typically ``127.0.0.1:15432`` --
**never Neon**). Both are written by the OFFLINE walk-forward bucket-eval
harness (``aggregate_bucket_eval_duckdb.py`` /
``evaluate_running_style_pipeline.py`` in ``apps/pc-keiba-viewer``), never by
a live-serve path, so ``eval_regime="wf"`` is the correct default tag for
both.

Consistent with the rest of this package's file-based-ingestion-only design
(see ``ingest_eval.py``'s module docstring -- no database network
connections), every function here takes a PARQUET/JSON FILE PATH exported
from the tables, not a live database connection: export the two tables with
a one-off, read-only script before calling these.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Final

import pandas as pd
from mlflow import MlflowClient

from mlflow_tracking import config, ingest_eval
from mlflow_tracking.logging_api import log_eval_run

SOURCE_TAG: Final[str] = "local-pg-bucket-eval"

MODEL_PREDICTION_EVAL_METRIC_COLUMNS: Final[tuple[str, ...]] = (
    "race_count",
    "prediction_count",
    "top1_accuracy",
    "top3_box_accuracy",
    "top3_exact_accuracy",
    "place1_accuracy",
    "place2_accuracy",
    "place3_accuracy",
    "top3_winner_capture",
    "top5_winner_capture",
    "pair_score",
    "ndcg_at_3",
    "top3_place_relation",
)

RS_BUCKET_STYLE_CLASSES: Final[tuple[str, ...]] = ("nige", "senkou", "sashi", "oikomi")
RS_BUCKET_CORNER_KEYS: Final[tuple[str, ...]] = ("corner1", "corner3", "corner4", "finish")
RS_BUCKET_CELL_DIMENSION_COLUMNS: Final[tuple[str, ...]] = (
    "keibajo_code",
    "kyori",
    "kyoso_shubetsu_code",
    "kyoso_joken_code",
    "condition_key",
    "track_code",
    "grade_code",
    "race_name",
)


def _to_float(value: object) -> float | None:
    """Coerce a Postgres-export scalar (int/float/Decimal/NaN) to a plain
    float metric value, or None when it is not a usable number."""
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, int | float):
        numeric = float(value)
        return None if math.isnan(numeric) else numeric
    return None


def _epoch_ms(value: object) -> int | None:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return None


def _numeric_metrics(row: Mapping[str, object], columns: Sequence[str]) -> dict[str, float]:
    result: dict[str, float] = {}
    for column in columns:
        value = _to_float(row.get(column))
        if value is not None:
            result[column] = value
    return result


def read_table(path: Path) -> pd.DataFrame:
    """Read a local-PG-history export table from parquet or JSON."""
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".json":
        return pd.read_json(path, orient="records")
    raise ValueError(f"unsupported local-pg-history export file extension: {path.suffix}")


def ingest_model_prediction_evaluations(
    client: MlflowClient,
    path: Path,
    *,
    eval_regime: str = "wf",
    experiment_name: str = config.EXPERIMENT_FP_WF_EVAL,
) -> list[str]:
    """Ingest a ``model_prediction_evaluations`` export (see `read_table`) as
    one MLflow run per row -- each row already IS one model_version's full
    offline bucket-eval result over one evaluation window (unlike the trial
    registry's per-cell rows, this table has no per-model grouping to do).
    """
    ingest_eval.validate_eval_regime(eval_regime)
    rows = read_table(path).to_dict("records")
    return [
        _ingest_one_model_prediction_evaluation(client, row, eval_regime, experiment_name)
        for row in rows
    ]


def _ingest_one_model_prediction_evaluation(
    client: MlflowClient,
    row: Mapping[str, object],
    eval_regime: str,
    experiment_name: str,
) -> str:
    model_version = str(row.get("model_version", ""))
    window_from = str(row.get("evaluation_window_from", ""))
    window_to = str(row.get("evaluation_window_to", ""))
    tags: dict[str, object] = {
        "model_version": model_version,
        "category": str(row.get("category", "")),
        "evaluation_window_from": window_from,
        "evaluation_window_to": window_to,
        "eval_regime": eval_regime,
        "source": SOURCE_TAG,
        "timestamp_source": "table-column",
    }
    for optional_key in ("subgroup_key", "subgroup_value"):
        value = row.get(optional_key)
        if isinstance(value, str) and value:
            tags[optional_key] = value
    run_time = _epoch_ms(row.get("evaluated_at"))
    return log_eval_run(
        client,
        experiment_name,
        tags=tags,
        metrics=_numeric_metrics(row, MODEL_PREDICTION_EVAL_METRIC_COLUMNS),
        run_name=f"{model_version}-{window_from}-{window_to}",
        start_time=run_time,
        end_time=run_time,
    )


def _confusion_accuracy_pct(row: Mapping[str, object]) -> float | None:
    """Overall bucket accuracy (%) from the 4x4 running-style confusion matrix."""
    diagonal = 0.0
    total = 0.0
    for actual in RS_BUCKET_STYLE_CLASSES:
        for predicted in RS_BUCKET_STYLE_CLASSES:
            value = _to_float(row.get(f"cm_actual_{actual}_pred_{predicted}_count"))
            if value is None:
                continue
            total += value
            if actual == predicted:
                diagonal += value
    if total <= 0:
        return None
    return diagonal / total * 100.0


def _log_loss_averages(row: Mapping[str, object]) -> dict[str, float]:
    result: dict[str, float] = {}
    for style in RS_BUCKET_STYLE_CLASSES:
        total = _to_float(row.get(f"log_loss_{style}_sum"))
        count = _to_float(row.get(f"log_loss_{style}_count"))
        if total is not None and count:
            result[f"log_loss_{style}_avg"] = total / count
    return result


def _corner_pair_score_averages(row: Mapping[str, object]) -> dict[str, float]:
    result: dict[str, float] = {}
    for corner in RS_BUCKET_CORNER_KEYS:
        total = _to_float(row.get(f"{corner}_pair_score_sum"))
        count = _to_float(row.get(f"{corner}_pair_score_count"))
        if total is not None and count:
            result[f"{corner}_pair_score_avg"] = total / count
    return result


def bucket_row_to_cell_record(row: Mapping[str, object]) -> dict[str, object]:
    """Reduce one raw confusion-matrix bucket row to one CELL-report record
    (dimension columns + computed rate metrics), so
    ingest_eval.normalize_cell_dataframe / logging_api.select_headline_metrics
    can consume it the same way as any other cell report."""
    record: dict[str, object] = {
        column: row.get(column) for column in RS_BUCKET_CELL_DIMENSION_COLUMNS
    }
    race_count = _to_float(row.get("race_count"))
    if race_count is not None:
        record["race_count"] = race_count
    accuracy_pct = _confusion_accuracy_pct(row)
    if accuracy_pct is not None:
        record["accuracy_pct"] = accuracy_pct
    record.update(_log_loss_averages(row))
    record.update(_corner_pair_score_averages(row))
    top2_hit_count = _to_float(row.get("top2_hit_count"))
    prediction_count = _to_float(row.get("prediction_count"))
    if top2_hit_count is not None and prediction_count:
        record["top2_hit_pct"] = top2_hit_count / prediction_count * 100.0
    return record


def _group_key(row: Mapping[str, object]) -> tuple[str, str, str]:
    return (
        str(row.get("model_version", "")),
        str(row.get("category", "")),
        str(row.get("running_style_feature_version", "")),
    )


def ingest_running_style_bucket_evaluations(
    client: MlflowClient,
    path: Path,
    *,
    eval_regime: str = "wf",
    experiment_name: str = config.EXPERIMENT_RS_EVAL,
) -> list[str]:
    """Ingest a ``running_style_model_bucket_evaluations`` export (see
    `read_table`) as one MLflow run per (model_version, category,
    running_style_feature_version) group, preserving per-bucket CELL
    granularity -- each source row is already one venue x distance x
    condition x track x grade cell -- via the same normalize + cell-table
    logging path as ``ingest_eval.ingest_cell_report``.
    """
    ingest_eval.validate_eval_regime(eval_regime)
    rows = read_table(path).to_dict("records")
    groups: dict[tuple[str, str, str], list[Mapping[str, object]]] = {}
    for row in rows:
        groups.setdefault(_group_key(row), []).append(row)
    return [
        _ingest_one_running_style_bucket_group(
            client, key, group_rows, eval_regime, experiment_name
        )
        for key, group_rows in groups.items()
    ]


def _ingest_one_running_style_bucket_group(
    client: MlflowClient,
    key: tuple[str, str, str],
    rows: list[Mapping[str, object]],
    eval_regime: str,
    experiment_name: str,
) -> str:
    model_version, category, feature_version = key
    cell_records = [bucket_row_to_cell_record(row) for row in rows]
    cell_df = ingest_eval.normalize_cell_dataframe(pd.DataFrame(cell_records))
    timestamps: list[int] = []
    for row in rows:
        timestamp = _epoch_ms(row.get("evaluated_at"))
        if timestamp is not None:
            timestamps.append(timestamp)
    start_time = min(timestamps) if timestamps else None
    end_time = max(timestamps) if timestamps else None
    tags = {
        "model_version": model_version,
        "category": category,
        "running_style_feature_version": feature_version,
        "eval_regime": eval_regime,
        "source": SOURCE_TAG,
        "timestamp_source": "table-column",
    }
    return log_eval_run(
        client,
        experiment_name,
        tags=tags,
        cell_df=cell_df,
        run_name=f"{model_version}-{feature_version}",
        start_time=start_time,
        end_time=end_time,
    )
