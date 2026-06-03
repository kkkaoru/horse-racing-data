#!/usr/bin/env python3
"""Container entrypoint: daily finish-position prediction for UPCOMING races.

This is the heavy orchestration that the Cloudflare Cron Trigger Worker starts
as a batch container job. It is intentionally thin — every decision lives in the
unit-tested ``predict_lib`` package — and it is excluded from the coverage gate
because it only wires together real I/O (Neon Postgres over TCP, R2 over HTTPS,
DuckDB subprocess feature build, native CatBoost/XGBoost). That integration is
verified at deploy time per ``DEPLOY.md``, not by unit tests.

Flow per category (jra / nar / ban-ei):
  1. List UPCOMING races (today .. today + PREDICT_DAYS_AHEAD, finish_position
     NULL) from Neon via ``NEON_DATABASE_URL``.
  2. Build the v7-lineage feature parquet by running the repo feature pipeline
     (DuckDB base build + the v7 layer scripts) against the same Postgres.
  3. Load the production model from R2 ``finish-position/{category}/{modelVersion}/``.
  4. Score, rank within race, and UPSERT into
     ``race_finish_position_model_predictions`` under ``{category}-v7-lineage-wf-21y``
     in idempotent, deduped, chunked batches.
  5. Record one audit row in ``finish_position_cron_executions``.

Run with: ``uv run python src/predict_upcoming.py`` (envvars set by the Worker).
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from db_driver import ConnectionLike, connect_postgres
from predict_lib.audit import (
    AuditStatus,
    audit_params,
    build_audit_insert_sql,
    build_audit_record,
    build_audit_table_ddl,
)
from predict_lib.dedupe import dedupe_batch
from predict_lib.model_meta import (
    CATEGORIES,
    METADATA_FILE_NAME,
    MODEL_FILE_NAME,
    Category,
    architecture_for,
    build_r2_object_key,
    feature_count_for,
)
from predict_lib.scorer import (
    BoosterLike,
    assert_feature_count,
    build_feature_matrix,
    score_matrix,
)
from predict_lib.upcoming import build_prediction_rows, rank_race_entries
from predict_lib.upsert_sql import (
    DEFAULT_CHUNK_SIZE,
    build_upsert_sql,
    chunk_rows,
    flatten_params,
)

NEON_DATABASE_URL_ENV: str = "NEON_DATABASE_URL"
RUN_DATE_ENV: str = "RUN_DATE"
DAYS_AHEAD_ENV: str = "PREDICT_DAYS_AHEAD"
MODELS_DIR_ENV: str = "MODELS_DIR"
DEFAULT_DAYS_AHEAD: int = 2
RACE_ID_KETTO_INDEX: int = 6
RACE_ID_PART_RANGE: range = range(1, 6)


@dataclass(frozen=True)
class PredictWindow:
    """The TODAY-races feature-build window passed to the pipeline.

    ``target_date`` is the JST ``YYYYMMDD`` run date (the cron Worker's
    ``RUN_DATE``); ``days_ahead`` widens the window past that day; the build emits
    feature rows for every race in [target_date, target_date + days_ahead],
    including UPCOMING ones whose ``finish_position`` is still NULL.
    """

    target_date: str
    days_ahead: int
    database_url: str


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        message = f"{name} environment variable is required"
        raise RuntimeError(message)
    return value


def _load_model_metadata(models_dir: Path, category: Category) -> Sequence[str]:
    key = build_r2_object_key(category, METADATA_FILE_NAME)
    metadata = json.loads((models_dir / key).read_text(encoding="utf-8"))
    feature_names = list(metadata["feature_names"])
    assert_feature_count(feature_names, feature_count_for(category))
    return feature_names


def _score_one_race(
    booster: BoosterLike,
    race_id: str,
    category: Category,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> list[list[object]]:
    matrix = build_feature_matrix(entries, feature_names, architecture_for(category))
    scores = score_matrix(booster, matrix)
    ranked = rank_race_entries(entries, scores)
    return build_prediction_rows(race_id, category, ranked)


def _row_to_pk_map(row: Sequence[object]) -> Mapping[str, object]:
    race_id = ":".join(str(row[index]) for index in RACE_ID_PART_RANGE)
    return {"race_id": race_id, "ketto_toroku_bango": row[RACE_ID_KETTO_INDEX]}


def _execute(connection: ConnectionLike, sql: str, params: Sequence[object]) -> None:
    cursor = connection.cursor()
    cursor.execute(sql, params)
    connection.commit()


def _flush_predictions(connection: ConnectionLike, rows: Sequence[Sequence[object]]) -> int:
    deduped = dedupe_batch([_row_to_pk_map(row) for row in rows])
    if not deduped:
        return 0
    written = 0
    for chunk in chunk_rows(rows, DEFAULT_CHUNK_SIZE):
        sql = build_upsert_sql(len(chunk))
        _execute(connection, sql, flatten_params(chunk))
        written += len(chunk)
    return written


def _record_audit(
    connection: ConnectionLike,
    run_date: str,
    status: AuditStatus,
    races_predicted: int,
    duration_ms: int,
    error: str | None,
) -> None:
    _execute(connection, build_audit_table_ddl(), [])
    record = build_audit_record(run_date, status, races_predicted, duration_ms, error)
    _execute(connection, build_audit_insert_sql(), audit_params(record))


def _predict_category(
    connection: ConnectionLike,
    category: Category,
    models_dir: Path,
    window: PredictWindow,
) -> int:
    feature_names = _load_model_metadata(models_dir, category)
    booster = _load_booster(models_dir, category)
    races = _build_feature_rows(category, window)
    written = 0
    for race_id, entries in races.items():
        rows = _score_one_race(booster, race_id, category, entries, feature_names)
        written += _flush_predictions(connection, rows)
    return written


def _load_booster(models_dir: Path, category: Category) -> BoosterLike:
    model_path = models_dir / build_r2_object_key(category, MODEL_FILE_NAME)
    if architecture_for(category) == "xgboost":
        from xgboost_adapter import load_xgboost_booster  # bundled in image

        return load_xgboost_booster(str(model_path))
    from catboost_adapter import load_catboost_booster  # bundled in image

    return load_catboost_booster(str(model_path))


def _build_feature_rows(
    category: Category,
    window: PredictWindow,
) -> Mapping[str, list[Mapping[str, object]]]:
    """Run the repo feature pipeline and load the resulting parquet per race.

    Delegated to the bundled pipeline scripts (DuckDB base build in
    ``--target-date`` mode + v7 layers); see ``DEPLOY.md`` for the exact
    subprocess invocation chain. Returns a map of ``race_id`` -> ordered entry
    feature dicts for today's races (incl. UPCOMING).
    """
    from pipeline_runner import build_upcoming_feature_rows  # bundled in image

    return build_upcoming_feature_rows(
        category, window.target_date, window.days_ahead, window.database_url
    )


def _connect(database_url: str) -> ConnectionLike:
    return connect_postgres(database_url)


def _try_record_audit(
    database_url: str,
    run_date: str,
    races_predicted: int,
    duration_ms: int,
    error_text: str,
) -> None:
    """Try to record an audit row; never raise so the real traceback survives.

    Audit recording opens a fresh connection because the original connection may
    be in a poisoned state (e.g. the original error came from psycopg itself).
    Any failure here is swallowed and logged to stderr so the caller's traceback
    still reaches the container logs.
    """
    try:
        audit_connection = _connect(database_url)
    except BaseException as audit_connect_error:
        print(
            f"[predict-upcoming] audit connect failed: {audit_connect_error}",
            file=sys.stderr,
        )
        return
    try:
        _record_audit(audit_connection, run_date, "error", races_predicted, duration_ms, error_text)
    except BaseException as audit_write_error:
        print(
            f"[predict-upcoming] audit write failed: {audit_write_error}",
            file=sys.stderr,
        )
    finally:
        try:
            audit_connection.close()
        except BaseException as audit_close_error:
            print(
                f"[predict-upcoming] audit close failed: {audit_close_error}",
                file=sys.stderr,
            )


def main() -> int:
    started = time.monotonic()
    try:
        database_url = _require_env(NEON_DATABASE_URL_ENV)
        run_date = _require_env(RUN_DATE_ENV)
        days_ahead = int(os.environ.get(DAYS_AHEAD_ENV, str(DEFAULT_DAYS_AHEAD)))
        models_dir = Path(os.environ.get(MODELS_DIR_ENV, "/models"))
        window = PredictWindow(
            target_date=run_date, days_ahead=days_ahead, database_url=database_url
        )
        connection = _connect(database_url)
    except BaseException as bootstrap_error:
        # Pre-connect failure (missing env var, bad URL, Neon down, etc). Nothing
        # to audit-write into yet — emit the full traceback so a future silent
        # startup crash is visible in container logs.
        traceback.print_exc()
        print(f"[predict-upcoming] bootstrap failed: {bootstrap_error}", file=sys.stderr)
        return 1
    races_predicted = 0
    try:
        for category in CATEGORIES:
            races_predicted += _predict_category(connection, category, models_dir, window)
        duration_ms = int((time.monotonic() - started) * 1000)
        _record_audit(connection, run_date, "success", races_predicted, duration_ms, None)
    except BaseException as error:
        duration_ms = int((time.monotonic() - started) * 1000)
        # Print the FULL traceback to stderr first so it always reaches container
        # logs even if the audit write also fails (e.g. poisoned connection).
        traceback.print_exc()
        error_text = f"{type(error).__name__}: {error}"
        try:
            connection.close()
        except BaseException as close_error:
            print(
                f"[predict-upcoming] post-error close failed: {close_error}",
                file=sys.stderr,
            )
        _try_record_audit(database_url, run_date, races_predicted, duration_ms, error_text)
        print(f"[predict-upcoming] failed: {error_text}", file=sys.stderr)
        return 1
    connection.close()
    print(f"[predict-upcoming] ok run_date={run_date} races_predicted={races_predicted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
