"""Cloudflare-safe preview sync for production prediction usage.

This path reads only the racing Neon prediction tables and writes lightweight
run tags and metrics to the MLflow backend. It intentionally does not read the
local PostgreSQL replica, emit traces, write artifacts, or set
``sync_base_logged``. The nightly full sync can therefore reuse the same run
and still write the final prediction artifacts and result-based evaluation.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Final, Literal

import psycopg2
from mlflow import MlflowClient
from mlflow.entities import Metric, RunTag
from mlflow.exceptions import MlflowException

from mlflow_tracking import config, db, serve_eval, sync_production, timeline
from mlflow_tracking.logging_api import get_or_create_experiment, log_batch_chunked

PREVIEW_LOGGED_AT_TAG: Final[str] = "sync_preview_logged_at"
Task = Literal["finish-position", "running-style"]

_ISOLATED_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    ValueError,
    KeyError,
    TypeError,
    psycopg2.Error,
    MlflowException,
)


@dataclass
class SyncProductionPreviewSummary:
    dates_processed: int
    fp_runs_created: int = 0
    fp_runs_updated: int = 0
    rs_runs_created: int = 0
    rs_runs_updated: int = 0
    errors: list[str] = field(default_factory=list)


def _preview_rows(
    neon_conn: db.ConnectionLike,
    category: str,
    date_str: str,
    task: Task,
) -> list[dict[str, object]]:
    source = serve_eval.resolve_source(category)
    if task == "finish-position":
        raw_rows = serve_eval.fetch_fp_prediction_rows(neon_conn, source, date_str, date_str)
        genuine_rows = serve_eval.filter_genuine_rows(raw_rows)
        if category == "jra":
            return genuine_rows
        non_banei_rows, banei_rows = serve_eval.partition_by_banei(genuine_rows)
        return banei_rows if category == "banei" else non_banei_rows
    raw_rows = serve_eval.fetch_rs_prediction_rows(neon_conn, source, date_str, date_str)
    return serve_eval.filter_genuine_rows(raw_rows)


def _log_preview_metrics(
    client: MlflowClient,
    run_id: str,
    metric_prefix: str,
    rows: list[dict[str, object]],
) -> None:
    live_rows, backfill_rows = serve_eval.partition_live_backfill(rows)
    metrics = [
        Metric(
            f"{metric_prefix}_races",
            float(_distinct_race_count(rows)),
            0,
            0,
        ),
        Metric(f"{metric_prefix}_horses", float(len(rows)), 0, 0),
        Metric(
            f"{metric_prefix}_races_live",
            float(_distinct_race_count(live_rows)),
            0,
            0,
        ),
        Metric(
            f"{metric_prefix}_races_backfilled",
            float(_distinct_race_count(backfill_rows)),
            0,
            0,
        ),
    ]
    logged_at = datetime.now(UTC).isoformat()
    log_batch_chunked(
        client,
        run_id,
        metrics=metrics,
        tags=[RunTag(PREVIEW_LOGGED_AT_TAG, logged_at)],
    )
    client.set_terminated(run_id, status="FINISHED")


def _distinct_race_count(rows: list[dict[str, object]]) -> int:
    return len({(str(row["keibajo_code"]), str(row["race_bango"])) for row in rows})


def _get_or_create_run(
    client: MlflowClient,
    experiment_id: str,
    *,
    sync_key: str,
    task: Task,
    category: str,
    model_version: str,
    date_str: str,
) -> tuple[str, bool]:
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{sync_production.SYNC_KEY_TAG} = '{sync_key}'",
        max_results=1,
    )
    if matches:
        return matches[0].info.run_id, False
    run = client.create_run(
        experiment_id,
        run_name=f"{date_str} {category} {model_version}",
        tags={
            sync_production.SYNC_KEY_TAG: sync_key,
            "task": task,
            "category": category,
            "model_version": model_version,
            "date": date_str,
            sync_production.EVAL_REGIME_TAG: sync_production.SERVE_REGIME,
        },
    )
    return run.info.run_id, True


def _sync_task_preview(
    client: MlflowClient,
    experiment_id: str,
    neon_conn: db.ConnectionLike,
    category: str,
    date_str: str,
    task: Task,
) -> tuple[int, int]:
    rows = _preview_rows(neon_conn, category, date_str, task)
    created = 0
    updated = 0
    metric_prefix = "fp" if task == "finish-position" else "rs"
    for model_version, group_rows in serve_eval.group_by_model_version(rows).items():
        sync_key = f"{date_str}:{category}:{model_version}"
        run_id, run_created = _get_or_create_run(
            client,
            experiment_id,
            sync_key=sync_key,
            task=task,
            category=category,
            model_version=model_version,
            date_str=date_str,
        )
        _log_preview_metrics(client, run_id, metric_prefix, group_rows)
        if run_created:
            created += 1
        else:
            updated += 1
    return created, updated


def sync_production_preview_range(
    client: MlflowClient,
    date_from: str,
    date_to: str,
    categories: Sequence[str] = sync_production.FP_CATEGORIES,
    *,
    neon_connect: Callable[[], db.ConnectionLike] = db.connect_racing_neon,
) -> SyncProductionPreviewSummary:
    """Write current production prediction snapshots into MLflow run metrics."""
    timeline.validate_yyyymmdd(date_from)
    timeline.validate_yyyymmdd(date_to)
    if date_to < date_from:
        raise ValueError(f"date_to ({date_to!r}) must not be before date_from ({date_from!r})")

    start = date(int(date_from[:4]), int(date_from[4:6]), int(date_from[6:8]))
    end = date(int(date_to[:4]), int(date_to[4:6]), int(date_to[6:8]))
    dates: list[str] = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    summary = SyncProductionPreviewSummary(dates_processed=len(dates))
    fp_experiment_id: str | None = None
    rs_experiment_id: str | None = None
    neon_conn = neon_connect()
    try:
        for date_str in dates:
            for category in categories:
                if fp_experiment_id is None:
                    fp_experiment_id = get_or_create_experiment(
                        client, config.EXPERIMENT_FP_PRODUCTION_USAGE
                    )
                try:
                    created, updated = _sync_task_preview(
                        client,
                        fp_experiment_id,
                        neon_conn,
                        category,
                        date_str,
                        "finish-position",
                    )
                except _ISOLATED_EXCEPTIONS as exc:
                    summary.errors.append(f"{date_str}:{category}:finish-position: {exc}")
                else:
                    summary.fp_runs_created += created
                    summary.fp_runs_updated += updated

                if category not in sync_production.RS_CATEGORIES:
                    continue
                if rs_experiment_id is None:
                    rs_experiment_id = get_or_create_experiment(
                        client, config.EXPERIMENT_RS_PRODUCTION_USAGE
                    )
                try:
                    created, updated = _sync_task_preview(
                        client,
                        rs_experiment_id,
                        neon_conn,
                        category,
                        date_str,
                        "running-style",
                    )
                except _ISOLATED_EXCEPTIONS as exc:
                    summary.errors.append(f"{date_str}:{category}:running-style: {exc}")
                else:
                    summary.rs_runs_created += created
                    summary.rs_runs_updated += updated
    finally:
        neon_conn.close()
    return summary
