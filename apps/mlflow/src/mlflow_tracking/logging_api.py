"""Run/metric logging helpers built on mlflow.MlflowClient.

Every function takes an explicit ``MlflowClient`` (constructed by the caller
with a tracking URI) instead of relying on the fluent API's global tracking
state, so tests can point at an isolated sqlite tmp_path store without any
cross-test global-state leakage.
"""

from __future__ import annotations

import json
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final, cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from mlflow import MlflowClient
from mlflow.entities import Metric, Param, RunTag
from mlflow.utils.validation import MAX_ENTITIES_PER_BATCH

from mlflow_tracking import cells, config

CELL_METRICS_JSON_ARTIFACT: Final[str] = "cell_metrics.json"
CELL_METRICS_PARQUET_ARTIFACT: Final[str] = "cell_metrics.parquet"

# Mirrors mlflow.utils.validation.MAX_PARAM_VAL_LENGTH; kept as a local
# constant (rather than imported) because it drives an app-level routing
# decision (param vs. artifact), not a value passed to mlflow itself.
MAX_PARAM_VALUE_LENGTH: Final[int] = 6000


def get_or_create_experiment(
    client: MlflowClient,
    name: str,
    *,
    artifact_location: str | None = None,
) -> str:
    """Return the experiment_id for `name`, creating it (with tags) if absent."""
    existing = client.get_experiment_by_name(name)
    if existing is not None:
        return existing.experiment_id
    resolved_location = (
        artifact_location if artifact_location is not None else config.resolve_artifact_location()
    )
    return client.create_experiment(name, artifact_location=resolved_location)


def _chunk_bounds(total: int, size: int) -> list[tuple[int, int]]:
    """Return (start, end) index pairs covering range(total) in chunks of `size`."""
    return [(i, min(i + size, total)) for i in range(0, total, size)]


def _log_batch_metrics(
    client: MlflowClient, run_id: str, metrics: Sequence[Metric], chunk_size: int
) -> None:
    for start, end in _chunk_bounds(len(metrics), chunk_size):
        client.log_batch(run_id, metrics=list(metrics[start:end]))


def _log_batch_params(
    client: MlflowClient, run_id: str, params: Sequence[Param], chunk_size: int
) -> None:
    for start, end in _chunk_bounds(len(params), chunk_size):
        client.log_batch(run_id, params=list(params[start:end]))


def _log_batch_tags(
    client: MlflowClient, run_id: str, tags: Sequence[RunTag], chunk_size: int
) -> None:
    for start, end in _chunk_bounds(len(tags), chunk_size):
        client.log_batch(run_id, tags=list(tags[start:end]))


def log_batch_chunked(
    client: MlflowClient,
    run_id: str,
    *,
    metrics: Sequence[Metric] = (),
    params: Sequence[Param] = (),
    tags: Sequence[RunTag] = (),
    chunk_size: int = MAX_ENTITIES_PER_BATCH,
) -> None:
    """Log metrics/params/tags via MlflowClient.log_batch, respecting the
    combined per-call entity limit (mlflow.utils.validation.MAX_ENTITIES_PER_BATCH).

    Each kind is chunked and sent in its own log_batch call(s) so a single
    call's combined entity count never needs cross-kind bin-packing to stay
    under the limit.
    """
    _log_batch_metrics(client, run_id, metrics, chunk_size)
    _log_batch_params(client, run_id, params, chunk_size)
    _log_batch_tags(client, run_id, tags, chunk_size)


def log_json_artifact(client: MlflowClient, run_id: str, filename: str, payload: object) -> None:
    """Write `payload` as pretty-printed JSON and log it as a run artifact."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        artifact_path = Path(tmp_dir) / filename
        serialized = json.dumps(payload, ensure_ascii=False, indent=2)
        artifact_path.write_text(serialized, encoding="utf-8")
        client.log_artifact(run_id, str(artifact_path))


def log_cell_table(client: MlflowClient, run_id: str, cell_df: pd.DataFrame) -> None:
    """Log the full per-cell metrics table twice: as a JSON table (renders in
    the MLflow UI Evaluation tab) and as a parquet artifact (efficient bulk
    re-read for downstream analysis)."""
    client.log_table(run_id, data=cell_df, artifact_file=CELL_METRICS_JSON_ARTIFACT)
    with tempfile.TemporaryDirectory() as tmp_dir:
        parquet_path = Path(tmp_dir) / CELL_METRICS_PARQUET_ARTIFACT
        pq.write_table(pa.Table.from_pandas(cell_df, preserve_index=False), parquet_path)
        client.log_artifact(run_id, str(parquet_path))


def _column(df: pd.DataFrame, name: str) -> pd.Series:
    """Select a single column as a Series.

    pandas' ``__getitem__`` overloads resolve to a broader union for a
    non-literal ``str`` key than the single-column-selection case actually
    returns, so this narrows it back to ``Series`` at the one place callers
    need it.
    """
    return cast(pd.Series, df[name])


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    """Weighted mean of `values` by `weights`, EXCLUDING any row where
    `values` is NA (None/NaN) from both the numerator AND the denominator.

    This matters now that a per-cell metric column can legitimately carry a
    missing value for SOME rows but not others (e.g. `place4_pct`/
    `place5_pct`/`place6_pct` -- None for a cell whose races all had too
    small a field for that rank, see `champion_cell_eval._aggregate_fp_cells`
    /`serve_eval.compute_rank_pct`). Before this fix, `(values * weights).sum()`
    already skipped a NaN product (pandas' default `skipna=True`), but
    `weights.sum()` alone still summed EVERY row's weight, including the
    ones whose value contributed nothing to the numerator -- silently
    diluting the mean with phantom weight from rows that were never really
    part of this metric's population. Every existing caller (a column with
    no NA values at all) sees IDENTICAL behavior to before this fix: the
    `notna()` mask keeps every row, so nothing changes for e.g. `top1_pct`/
    `place2_pct`.
    """
    mask = values.notna()
    filtered_values = values[mask]
    filtered_weights = weights[mask]
    total_weight = float(filtered_weights.sum())
    if total_weight <= 0:
        return None
    return float((filtered_values * filtered_weights).sum() / total_weight)


def select_headline_metrics(
    cell_df: pd.DataFrame,
    *,
    category_column: str = "category",
    weight_column: str = "race_count",
) -> dict[str, float]:
    """Reduce a per-cell metrics table to headline run metrics.

    Metric-set agnostic: every numeric column that is not part of the
    canonical CELL dimensional key (``cells.CELL_KEY_COLUMNS``) and is not
    the weight column itself is treated as a metric candidate, so
    finish-position (top1/place2/...), running-style
    (accuracy/macro_f1/...), and any future metric family are all summarized
    without hardcoding a metric name list. Emits an overall race-count-
    weighted mean per candidate metric present in ``cell_df``, plus one more
    per (category, metric) pair when a category column with more than one
    distinct value exists. The full per-cell table is not summarized further
    here — it stays intact in the logged table/parquet artifacts.
    """
    result: dict[str, float] = {}
    excluded_columns = set(cells.CELL_KEY_COLUMNS) | {weight_column}
    available_metrics = [
        column
        for column in cell_df.columns
        if column not in excluded_columns
        and pd.api.types.is_numeric_dtype(_column(cell_df, column))
    ]
    has_weights = weight_column in cell_df.columns
    default_weights = pd.Series([1.0] * len(cell_df), index=cell_df.index)
    weights = _column(cell_df, weight_column) if has_weights else default_weights
    for metric in available_metrics:
        value = _weighted_mean(_column(cell_df, metric), weights)
        if value is not None:
            result[f"overall_{metric}"] = value

    if category_column not in cell_df.columns:
        return result
    categories = _column(cell_df, category_column).dropna().unique()
    if len(categories) <= 1:
        return result
    for category in categories:
        subset = cast(pd.DataFrame, cell_df[_column(cell_df, category_column) == category])
        subset_weights = (
            _column(subset, weight_column)
            if has_weights
            else pd.Series([1.0] * len(subset), index=subset.index)
        )
        for metric in available_metrics:
            value = _weighted_mean(_column(subset, metric), subset_weights)
            if value is not None:
                result[f"{category}_{metric}"] = value
    return result


def _now_ms() -> int:
    return int(time.time() * 1000)


def _metric_timestamp(start_time: int | None, end_time: int | None) -> int:
    """Pick the timestamp metrics should carry so backfilled runs show their
    true historical time in the UI rather than the moment of ingestion."""
    if end_time is not None:
        return end_time
    if start_time is not None:
        return start_time
    return _now_ms()


def flatten_dict(data: Mapping[str, object], *, prefix: str = "", sep: str = ".") -> dict[str, str]:
    """Recursively flatten a nested mapping into dotted-key string params."""
    result: dict[str, str] = {}
    for key, value in data.items():
        full_key = f"{prefix}{sep}{key}" if prefix else str(key)
        if isinstance(value, dict):
            nested = cast(Mapping[str, object], value)
            result.update(flatten_dict(nested, prefix=full_key, sep=sep))
        else:
            result[full_key] = str(value)
    return result


def route_json_fields(
    payload: Mapping[str, object],
    *,
    identity_tag_keys: frozenset[str],
    flatten_param_keys: frozenset[str] = frozenset(),
) -> tuple[dict[str, str], dict[str, str], dict[str, object]]:
    """Split a metadata/manifest JSON payload into (params, tags, artifact_payloads).

    - Keys in `identity_tag_keys` (e.g. model_version/architecture/category)
      become tags verbatim.
    - Keys in `flatten_param_keys` whose value is itself a dict (e.g.
      hyperparams) are dict-flattened into params via `flatten_dict`.
    - Every other value becomes a param, UNLESS its rendered form would
      exceed MLflow's per-param value length limit, in which case it is
      routed to `artifact_payloads` under its own key instead. This handles
      long fields (e.g. feature_names) generically, without hardcoding their
      names, so any future long provenance field routes correctly too.
    """
    params: dict[str, str] = {}
    tags: dict[str, str] = {}
    artifacts: dict[str, object] = {}
    for key, value in payload.items():
        if key in identity_tag_keys:
            tags[key] = str(value)
            continue
        if key in flatten_param_keys and isinstance(value, dict):
            nested = cast(Mapping[str, object], value)
            params.update(flatten_dict(nested, prefix=key))
            continue
        is_container = isinstance(value, dict | list)
        candidate = json.dumps(value, ensure_ascii=False) if is_container else str(value)
        if len(candidate) > MAX_PARAM_VALUE_LENGTH:
            artifacts[key] = value
        else:
            params[key] = candidate
    return params, tags, artifacts


def log_eval_run(
    client: MlflowClient,
    experiment_name: str,
    *,
    params: Mapping[str, object] | None = None,
    tags: Mapping[str, object] | None = None,
    metrics: Mapping[str, float] | None = None,
    cell_df: pd.DataFrame | None = None,
    run_name: str | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    artifact_location: str | None = None,
) -> str:
    """Create (or reuse) an experiment, log one run, and return its run_id.

    Supports explicit epoch-ms start_time/end_time so historical trials/
    reports can be backfilled as FINISHED runs at their true historical time
    rather than left RUNNING or timestamped "now".
    """
    experiment_id = get_or_create_experiment(
        client, experiment_name, artifact_location=artifact_location
    )
    run = client.create_run(experiment_id, start_time=start_time, run_name=run_name)
    run_id = run.info.run_id

    metric_ts = _metric_timestamp(start_time, end_time)
    param_items = [Param(str(k), str(v)) for k, v in (params or {}).items()]
    tag_items = [RunTag(str(k), str(v)) for k, v in (tags or {}).items()]
    metric_items = [Metric(str(k), float(v), metric_ts, 0) for k, v in (metrics or {}).items()]
    log_batch_chunked(client, run_id, metrics=metric_items, params=param_items, tags=tag_items)

    if cell_df is not None:
        headline = select_headline_metrics(cell_df)
        if headline:
            headline_items = [Metric(k, v, metric_ts, 0) for k, v in headline.items()]
            log_batch_chunked(client, run_id, metrics=headline_items)
        log_cell_table(client, run_id, cell_df)

    client.set_terminated(run_id, status="FINISHED", end_time=end_time)
    return run_id
