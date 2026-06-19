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
  2. Build the v8 feature parquet (JRA=241 / NAR=192 / Ban-ei=111) by running
     the repo feature pipeline (DuckDB base build + the v7 layer scripts + the
     v8 pacestyle / course-numerical layers per
     ``predict_lib.pipeline_args.LAYER_CHAIN``) against the same Postgres.
  3. Load the production model from R2 ``finish-position/{category}/{modelVersion}/``.
  4. Score, rank within race, and UPSERT into
     ``race_finish_position_model_predictions`` under the v8 ``model_version``
     resolved by ``predict_lib.model_meta.model_version_for`` in idempotent,
     deduped, chunked batches.
  5. Record one audit row in ``finish_position_cron_executions``.

Startup gate
------------
Set ``PREDICT_SERVE_MODE=http`` (env var) OR pass ``--serve`` (CLI argument) to
start the HTTP ``/predict`` server mode instead of the one-shot CLI batch run.
In server mode:
  - ``GET /ping``    → 200 ``ok``  (Container health-check probe)
  - ``GET /predict?category=...&runDate=...&daysAhead=0``
                     → 200 Transfer-Encoding: chunked, application/x-ndjson
                       NDJSON progress lines + final result line per request

Run with: ``uv run python src/predict_upcoming.py`` (envvars set by the Worker).
Server:   ``PREDICT_SERVE_MODE=http uv run python src/predict_upcoming.py``
       or ``uv run python src/predict_upcoming.py --serve``
"""

from __future__ import annotations

import contextlib
import http.server
import json
import os
import socket
import sys
import threading
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import final, override

from db_driver import ConnectionLike, connect_postgres_with_retry, is_transient_error
from predict_lib.audit import (
    AuditStatus,
    audit_params,
    build_audit_insert_sql,
    build_audit_record,
    build_audit_table_ddl,
)
from predict_lib.booster_pool import BoosterPool
from predict_lib.conn_url import normalise_database_url, resolve_source_url
from predict_lib.dedupe import dedupe_batch
from predict_lib.ensemble_routing import (
    EnsembleRouteOutcome,
    init_member_pool,
    score_race_with_resolution,
)
from predict_lib.etop2_override import apply_etop2_scores, is_etop2_override_active
from predict_lib.late_binding import OddsSnapshot
from predict_lib.model_meta import (
    CATEGORIES,
    JRA_ETOP2_ENABLED,
    JRA_ETOP2_MODEL_VERSION,
    METADATA_FILE_NAME,
    MODEL_FILE_NAME,
    Category,
    architecture_for,
    build_r2_object_key,
    build_r2_xgb_etop2_key,
    feature_count_for,
    model_version_for,
)
from predict_lib.per_class import resolve_per_class_resolution
from predict_lib.rescore import (
    RaceFreshSnapshot,
    RaceScope,
    apply_fresh_snapshots,
    filter_races_by_scope,
)
from predict_lib.scorer import BoosterLike, assert_feature_count, build_feature_matrix, score_matrix
from predict_lib.serve import (
    CacheMissError,
    PredictCategoryFn,
    PredictParams,
    R2Config,
    build_r2_feat_cache_key,
    iter_predict_chunks,
    parse_predict_params,
    parse_request_path,
)
from predict_lib.upcoming import build_prediction_rows, rank_race_entries
from predict_lib.upsert_sql import (
    DEFAULT_CHUNK_SIZE,
    build_upsert_sql,
    chunk_rows,
    flatten_params,
)

PREDICT_SERVE_MODE_ENV: str = "PREDICT_SERVE_MODE"
"""When set to ``http``, the container starts an HTTP server instead of CLI batch."""
# R2 feature-cache environment variables (optional — all must be present to
# enable R2 caching; any missing var silently disables R2 put/get).
R2_ACCOUNT_ID_ENV: str = "R2_ACCOUNT_ID"
R2_ACCESS_KEY_ID_ENV: str = "R2_ACCESS_KEY_ID"
R2_SECRET_ACCESS_KEY_ENV: str = "R2_SECRET_ACCESS_KEY"
R2_BUCKET_ENV: str = "R2_BUCKET"
NEON_DATABASE_URL_ENV: str = "NEON_DATABASE_URL"
# Optional override: source URL for the DuckDB feature-build subprocess. When
# set, feature building (which sustains a long-running ATTACH against Postgres
# and is sensitive to Neon's compute idle timeout) uses this URL instead of
# ``NEON_DATABASE_URL``. The predictions UPSERT + audit always use
# ``NEON_DATABASE_URL`` so today's predictions land in the canonical store.
# Typical local-Docker setup: feature build against the local logical replica
# (no SSL idle eviction); predictions UPSERT to Neon.
SOURCE_DATABASE_URL_ENV: str = "SOURCE_DATABASE_URL"
RUN_DATE_ENV: str = "RUN_DATE"
DAYS_AHEAD_ENV: str = "PREDICT_DAYS_AHEAD"
MODELS_DIR_ENV: str = "MODELS_DIR"
# Optional comma-separated allowlist of categories to predict (e.g.
# "nar,ban-ei"). When unset or empty, ALL categories in CATEGORIES are
# attempted. Used to skip a category that is known-broken for the day (e.g.
# JRA on a non-race-day, or while a Neon-side scan timeout is being debugged)
# without blocking the others.
CATEGORIES_ENV: str = "PREDICT_CATEGORIES"
DEFAULT_DAYS_AHEAD: int = 2
RACE_ID_KETTO_INDEX: int = 6
RACE_ID_PART_RANGE: range = range(1, 6)
# Per-category feature-parquet column name carrying the per-class routing
# code. JRA uses ``kyoso_joken_code`` (000/005/010/016/701/703/...) which the
# DuckDB base build projects directly from the source rows. NAR uses
# ``nar_subclass`` (NEW / MUKATSU / C / B / A / OP / other) which the DuckDB
# base build derives from ``kyoso_joken_meisho`` via a regex CASE expression
# (apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py
# ``nar_subclass_case_sql``). Ban-ei has no per-class registry today so its
# entry is omitted; the upstream extractor short-circuits for disabled
# categories. Used by predict_lib.per_class.resolve_per_class_model_version
# to route a race to its per-class model when one is registered; absent /
# None / disabled defers to the category-global fallback.
CLASS_CODE_FIELD_BY_CATEGORY: Mapping[Category, str] = {
    "jra": "kyoso_joken_code",
    "nar": "nar_subclass",
}
HTTP_PORT: int = 8080
"""Port the HTTP server listens on in both CLI batch mode (liveness) and server mode."""
# Cloudflare Containers reaps batch instances that receive no HTTP traffic
# (independent of @cloudflare/containers' JS-side sleepAfter). The predictor
# is a long-running batch job, so we both (a) listen on a port so the start
# probe + DO containerFetch resolve, AND (b) honour repeated HTTP keepalive
# pings from the Worker DO's scheduled loop. The server is tiny on purpose —
# the only HTTP requirement is "200 OK on every request".
LIVENESS_PORT: int = HTTP_PORT
LIVENESS_BACKLOG: int = 8
LIVENESS_RESPONSE: bytes = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
LIVENESS_RECV_BYTES: int = 4096


def _handle_liveness_connection(conn: socket.socket) -> None:
    try:
        conn.recv(LIVENESS_RECV_BYTES)
    except OSError:
        return
    try:
        conn.sendall(LIVENESS_RESPONSE)
    except OSError:
        return
    finally:
        try:
            conn.close()
        except OSError:
            return


def _serve_liveness_socket(port: int) -> None:
    """Trivial HTTP server: 200 OK to every request. Daemonised + idempotent on
    socket errors so a transient probe error never crashes the predictor."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", port))
        server.listen(LIVENESS_BACKLOG)
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            _handle_liveness_connection(conn)


def _start_liveness_thread(port: int) -> None:
    """Spawn the liveness server as a daemon thread so the predictor exits
    naturally when main() returns."""
    thread = threading.Thread(target=_serve_liveness_socket, args=(port,), daemon=True)
    thread.start()


@dataclass(frozen=True)
class PredictWindow:
    """The TODAY-races feature-build window passed to the pipeline.

    ``target_date`` is the JST ``YYYYMMDD`` run date (the cron Worker's
    ``RUN_DATE``); ``days_ahead`` widens the window past that day; the build emits
    feature rows for every race in [target_date, target_date + days_ahead],
    including UPCOMING ones whose ``finish_position`` is still NULL.

    ``scope`` is the optional Stage-2 race-scope filter (both sides ``None`` =
    every race, the full-path default).  Only the rescore path narrows it; the
    full build path always uses the all-races scope so its behaviour is
    unchanged.
    """

    target_date: str
    days_ahead: int
    database_url: str
    scope: RaceScope = field(default_factory=RaceScope)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        message = f"{name} environment variable is required"
        raise RuntimeError(message)
    return value


def _resolve_categories(raw: str | None) -> tuple[Category, ...]:
    """Filter ``CATEGORIES`` by the optional ``PREDICT_CATEGORIES`` allowlist.

    Empty / unset ``raw`` returns the full canonical tuple. Otherwise only the
    categories in the comma-separated allowlist are returned, preserving the
    canonical order. Unknown tokens are dropped (so a typo can never silently
    select an unsupported category).
    """
    if not raw:
        return CATEGORIES
    requested = {token.strip() for token in raw.split(",") if token.strip()}
    return tuple(category for category in CATEGORIES if category in requested)


def _load_model_metadata(models_dir: Path, category: Category) -> Sequence[str]:
    key = build_r2_object_key(category, METADATA_FILE_NAME)
    metadata = json.loads((models_dir / key).read_text(encoding="utf-8"))
    feature_names = list(metadata["feature_names"])
    assert_feature_count(feature_names, feature_count_for(category))
    return feature_names


def extract_race_class_code(
    category: Category, entries: Sequence[Mapping[str, object]]
) -> str | None:
    """Return the race's per-class routing code from the first entry, or None.

    The column name is per-category: JRA reads ``kyoso_joken_code`` (numeric
    race-class code), NAR reads ``nar_subclass`` (derived sub-class string).
    Categories not in :data:`CLASS_CODE_FIELD_BY_CATEGORY` (Ban-ei today)
    return ``None`` so the per-class router short-circuits to the
    category-global fallback. All entries of one race share the same
    race-class, so the first entry is representative. ``None`` and empty
    strings collapse to ``None`` so the per-class router falls back to the
    category-global model.
    """
    if not entries:
        return None
    field = CLASS_CODE_FIELD_BY_CATEGORY.get(category)
    if field is None:
        return None
    raw = entries[0].get(field)
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "":
        return None
    return text


def _score_one_race(
    fallback_booster: BoosterLike,
    pool: BoosterPool,
    models_dir: Path,
    race_id: str,
    category: Category,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> list[list[object]]:
    class_code = extract_race_class_code(category, entries)
    resolution = resolve_per_class_resolution(models_dir, category, class_code)
    outcome: EnsembleRouteOutcome = score_race_with_resolution(
        resolution=resolution,
        race_id=race_id,
        entries=entries,
        feature_names=feature_names,
        architecture=architecture_for(category),
        pool=pool,
        fallback_booster=fallback_booster,
        fallback_model_version=model_version_for(category),
    )
    if outcome.fallback_reason is not None:
        print(
            f"[predict-upcoming] ensemble fallback category={category} "
            f"race_id={race_id} class_code={class_code} "
            f"reason={outcome.fallback_reason}",
            file=sys.stderr,
        )
    ranked = rank_race_entries(entries, outcome.scores)
    return build_prediction_rows(race_id, category, ranked, outcome.model_version)


def _score_one_race_etop2(
    cb_booster: BoosterLike,
    xgb_booster: BoosterLike,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> list[list[object]]:
    """Score one JRA race with E-top2 place-preserving override.

    Both CB iter20 and XGB xgb-jra-2013-v8 score the same feature matrix
    (identical 244-feature order). The override is applied per-race:
    when XGB#1 == CB#2 and race class != 701, CB#2 is promoted to rank-1.

    The class code is read from the ``kyoso_joken_code`` column of the first
    entry (same field used by :func:`extract_race_class_code` for JRA). If the
    column is absent, the class is treated as None (override eligible).
    """
    # Both models use the same 244-feature order
    cb_matrix = build_feature_matrix(entries, feature_names, "catboost")
    xgb_matrix = build_feature_matrix(entries, feature_names, "xgboost")
    cb_scores = score_matrix(cb_booster, cb_matrix)
    xgb_scores = score_matrix(xgb_booster, xgb_matrix)

    class_code = extract_race_class_code("jra", entries)
    override_scores = apply_etop2_scores(cb_scores, xgb_scores, class_code)

    fired = is_etop2_override_active(cb_scores, xgb_scores, class_code)
    if fired:
        print(
            f"[etop2] override fired race_id={race_id} class={class_code}",
            file=sys.stderr,
        )

    ranked = rank_race_entries(entries, override_scores)
    return build_prediction_rows(race_id, "jra", ranked, JRA_ETOP2_MODEL_VERSION)


def _row_to_pk_map(row: Sequence[object]) -> Mapping[str, object]:
    race_id = ":".join(str(row[index]) for index in RACE_ID_PART_RANGE)
    return {"race_id": race_id, "ketto_toroku_bango": row[RACE_ID_KETTO_INDEX]}


def execute(
    connection: ConnectionLike,
    sql: str,
    params: Sequence[object],
    database_url: str,
) -> ConnectionLike:
    """Execute ``sql`` against ``connection``, reconnecting once on transient loss.

    Returns the (possibly new) connection so callers can rebind after a
    reconnect. On AdminShutdown or "connection is lost/closed" mid-write,
    opens a fresh Neon connection via :func:`_connect` and retries the
    statement once. Any second failure propagates to the caller.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        connection.commit()
        return connection
    except BaseException as exc:
        if not is_transient_error(exc):
            raise
        # Transient mid-write failure: attempt a single reconnect then retry.
        print(
            f"[predict-upcoming] mid-write transient error ({type(exc).__name__}): {exc} "
            "— reconnecting and retrying once",
            file=sys.stderr,
        )
        try:
            connection.rollback()
        except BaseException as rb_exc:
            print(
                f"[predict-upcoming] rollback failed: {rb_exc}",
                file=sys.stderr,
            )
        with contextlib.suppress(BaseException):
            connection.close()
        fresh = _connect(database_url)
        cursor = fresh.cursor()
        cursor.execute(sql, params)
        fresh.commit()
        return fresh


def flush_predictions(
    connection: ConnectionLike,
    rows: Sequence[Sequence[object]],
    database_url: str,
) -> tuple[int, ConnectionLike]:
    """Flush ``rows`` to Neon in chunks; reconnects on transient mid-write errors.

    Returns ``(written, connection)`` where ``connection`` may be a fresh object
    after a reconnect so the caller can update its reference.
    """
    deduped = dedupe_batch([_row_to_pk_map(row) for row in rows])
    if not deduped:
        return 0, connection
    written = 0
    for chunk in chunk_rows(rows, DEFAULT_CHUNK_SIZE):
        sql = build_upsert_sql(len(chunk))
        connection = execute(connection, sql, flatten_params(chunk), database_url)
        written += len(chunk)
    return written, connection


def _record_audit(
    connection: ConnectionLike,
    run_date: str,
    status: AuditStatus,
    races_predicted: int,
    duration_ms: int,
    error: str | None,
    database_url: str,
) -> None:
    execute(connection, build_audit_table_ddl(), [], database_url)
    record = build_audit_record(run_date, status, races_predicted, duration_ms, error)
    execute(connection, build_audit_insert_sql(), audit_params(record), database_url)


def _score_races(
    races: Mapping[str, Sequence[Mapping[str, object]]],
    category: Category,
    models_dir: Path,
    feature_names: Sequence[str],
) -> list[list[list[object]]]:
    """Score every race in ``races`` into per-race prediction rows.

    Builds the (per-category) booster pool + E-top2 companion once, then routes
    each race through the JRA E-top2 override or the ensemble/per-class path.
    Connection-free and CPU-bound so the caller can defer the Neon connect until
    the first write (avoiding Neon autosuspend during the long score phase).
    """
    fallback_booster = _load_booster(models_dir, category)
    pool = init_member_pool(models_dir, category)
    xgb_etop2_booster: BoosterLike | None = None
    if JRA_ETOP2_ENABLED and category == "jra":
        xgb_etop2_booster = _load_xgb_etop2_booster(models_dir)
    scored: list[list[list[object]]] = []
    for race_id, entries in races.items():
        if xgb_etop2_booster is not None:
            rows = _score_one_race_etop2(
                fallback_booster,
                xgb_etop2_booster,
                race_id,
                entries,
                feature_names,
            )
        else:
            rows = _score_one_race(
                fallback_booster, pool, models_dir, race_id, category, entries, feature_names
            )
        scored.append(rows)
    return scored


def _flush_scored(
    database_url: str,
    category: Category,
    scored: Sequence[Sequence[Sequence[object]]],
) -> int:
    """Open a fresh Neon connection, UPSERT every scored race, return rows written.

    ``flush_predictions`` may internally reconnect on AdminShutdown / "connection
    is lost" mid-write and returns the (possibly new) connection so the right
    object is closed at the end.
    """
    connection = _connect(database_url)
    try:
        written = 0
        for rows in scored:
            rows_written, connection = flush_predictions(connection, rows, database_url)
            written += rows_written
    finally:
        try:
            connection.close()
        except BaseException as close_error:
            print(
                f"[predict-upcoming] connection close failed category={category}: {close_error}",
                file=sys.stderr,
            )
    return written


def _score_and_flush_races(
    database_url: str,
    category: Category,
    models_dir: Path,
    races: Mapping[str, Sequence[Mapping[str, object]]],
) -> int:
    """Score ``races`` then UPSERT to Neon; the shared core of full + rescore.

    The races map is supplied by the caller — built from the 21y Neon scan on
    the full path, or read from the R2 / local feature cache (with the 5
    late-binding columns refreshed) on the rescore path.
    """
    feature_names = _load_model_metadata(models_dir, category)
    scored = _score_races(races, category, models_dir, feature_names)
    return _flush_scored(database_url, category, scored)


def _predict_category(
    database_url: str,
    category: Category,
    models_dir: Path,
    window: PredictWindow,
) -> int:
    # Score all races before opening the Neon write connection. The feature
    # build is the longest step (DuckDB base build + 14 layer scripts, typically
    # 2-5 min) and Neon autosuspends after ~60s of idle — connecting before the
    # build would cause AdminShutdown on the first UPSERT. Scoring is CPU-bound
    # and connection-free, so we defer the Neon connect until the first write.
    races = _build_feature_rows(category, window)
    return _score_and_flush_races(database_url, category, models_dir, races)


def _load_booster(models_dir: Path, category: Category) -> BoosterLike:
    model_path = models_dir / build_r2_object_key(category, MODEL_FILE_NAME)
    if architecture_for(category) == "xgboost":
        from xgboost_adapter import load_xgboost_booster  # bundled in image

        return load_xgboost_booster(str(model_path))
    from catboost_adapter import load_catboost_booster  # bundled in image

    return load_catboost_booster(str(model_path))


def _load_xgb_etop2_booster(models_dir: Path) -> BoosterLike:
    """Load the XGBoost companion model for E-top2 JRA override.

    Resolves the artifact at ``models/finish-position/jra/xgb-jra-2013-v8/
    model.json`` (same path as baked into the image alongside CB iter20).
    Called once at category startup when JRA_ETOP2_ENABLED is True.
    """
    model_path = models_dir / build_r2_xgb_etop2_key(MODEL_FILE_NAME)
    from xgboost_adapter import load_xgboost_booster  # bundled in image

    return load_xgboost_booster(str(model_path))


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
    """Open a Neon connection with retry on transient errors (DNS blips, AdminShutdown)."""
    return connect_postgres_with_retry(database_url)


def _try_record_audit(
    database_url: str,
    run_date: str,
    races_predicted: int,
    duration_ms: int,
    error_text: str | None,
) -> None:
    """Try to record an audit row; never raise so the real traceback survives.

    Opens a fresh Neon connection for each audit write. Used for both success
    and failure paths so the audit connection is always opened lazily — after
    the feature build and UPSERT are complete — avoiding Neon autosuspend on
    long-running feature pipelines. ``error_text=None`` records a "success"
    row; a non-empty string records "error" or "partial" as appropriate.
    Any failure here is swallowed and logged to stderr so the caller's
    traceback still reaches the container logs.
    """
    status: AuditStatus = "success" if error_text is None else "error"
    try:
        audit_connection = _connect(database_url)
    except BaseException as audit_connect_error:
        print(
            f"[predict-upcoming] audit connect failed: {audit_connect_error}",
            file=sys.stderr,
        )
        return
    try:
        _record_audit(
            audit_connection,
            run_date,
            status,
            races_predicted,
            duration_ms,
            error_text,
            database_url,
        )
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


# ---------------------------------------------------------------------------
# HTTP server mode (PREDICT_SERVE_MODE=http or --serve)
# ---------------------------------------------------------------------------


def _load_r2_config() -> R2Config | None:
    """Build an :class:`R2Config` from environment variables, or ``None``.

    Returns ``None`` when any of the four required env vars is absent or empty
    so the caller can silently skip R2 operations on a non-containerised run
    (e.g. local ``docker run`` without R2 secrets, or Mac launchd cron).
    """
    account_id = os.environ.get(R2_ACCOUNT_ID_ENV, "").strip()
    access_key_id = os.environ.get(R2_ACCESS_KEY_ID_ENV, "").strip()
    secret_access_key = os.environ.get(R2_SECRET_ACCESS_KEY_ENV, "").strip()
    bucket = os.environ.get(R2_BUCKET_ENV, "").strip()
    if not account_id or not access_key_id or not secret_access_key or not bucket:
        return None
    return R2Config(
        account_id=account_id,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        bucket=bucket,
    )


def _r2_put_parquet(r2: R2Config, object_key: str, local_path: Path) -> None:
    """Upload a local parquet file to R2 via the S3-compatible API.

    Uses stdlib ``urllib.request`` + HMAC-SHA256 AWS Signature Version 4 so no
    external dep is added.  On any failure (network, auth, missing file) the
    exception propagates to the caller, which logs and continues so the
    prediction run is never blocked by a cache upload failure.

    Args:
        r2:          R2 credentials and bucket name.
        object_key:  R2 object key (e.g. ``feat-cache/jra/20260619/features.parquet``).
        local_path:  Local parquet path to upload.
    """
    import hashlib
    import hmac
    import urllib.request
    from datetime import UTC, datetime

    data = local_path.read_bytes()
    content_hash = hashlib.sha256(data).hexdigest()
    now = datetime.now(UTC)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    host = f"{r2.account_id}.r2.cloudflarestorage.com"
    url = f"https://{host}/{r2.bucket}/{object_key}"

    canonical_headers = f"host:{host}\nx-amz-content-sha256:{content_hash}\nx-amz-date:{amzdate}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = (
        f"PUT\n/{r2.bucket}/{object_key}\n\n{canonical_headers}\n{signed_headers}\n{content_hash}"
    )

    credential_scope = f"{datestamp}/auto/s3/aws4_request"
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n{amzdate}\n{credential_scope}\n"
        + hashlib.sha256(canonical_request.encode()).hexdigest()
    )

    def _sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    signing_key = _sign(
        _sign(
            _sign(
                _sign(
                    f"AWS4{r2.secret_access_key}".encode(),
                    datestamp,
                ),
                "auto",
            ),
            "s3",
        ),
        "aws4_request",
    )
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={r2.access_key_id}/{credential_scope},"
        f" SignedHeaders={signed_headers}, Signature={signature}"
    )
    req = urllib.request.Request(
        url,
        data=data,
        method="PUT",
        headers={
            "Authorization": auth_header,
            "x-amz-date": amzdate,
            "x-amz-content-sha256": content_hash,
            "Content-Type": "application/octet-stream",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        _ = resp.read()
    print(
        f"[predict-serve] R2 put ok key={object_key} bytes={len(data)}",
        file=sys.stderr,
    )


def _r2_get_parquet(r2: R2Config, object_key: str, dest_path: Path) -> bool:
    """Download an R2 object to ``dest_path``.

    Returns ``True`` on success, ``False`` when the object does not exist (HTTP
    404).  Any other error (network, auth) propagates to the caller.

    Args:
        r2:          R2 credentials and bucket name.
        object_key:  R2 object key to download.
        dest_path:   Local destination path (will be created / overwritten).
    """
    import hashlib
    import hmac
    import urllib.error
    import urllib.request
    from datetime import UTC, datetime

    payload_hash = hashlib.sha256(b"").hexdigest()
    now = datetime.now(UTC)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    host = f"{r2.account_id}.r2.cloudflarestorage.com"
    url = f"https://{host}/{r2.bucket}/{object_key}"

    canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = (
        f"GET\n/{r2.bucket}/{object_key}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )

    credential_scope = f"{datestamp}/auto/s3/aws4_request"
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n{amzdate}\n{credential_scope}\n"
        + hashlib.sha256(canonical_request.encode()).hexdigest()
    )

    def _sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    signing_key = _sign(
        _sign(
            _sign(
                _sign(
                    f"AWS4{r2.secret_access_key}".encode(),
                    datestamp,
                ),
                "auto",
            ),
            "s3",
        ),
        "aws4_request",
    )
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={r2.access_key_id}/{credential_scope},"
        f" SignedHeaders={signed_headers}, Signature={signature}"
    )
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Authorization": auth_header,
            "x-amz-date": amzdate,
            "x-amz-content-sha256": payload_hash,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(data)
    print(
        f"[predict-serve] R2 get ok key={object_key} bytes={len(data)}",
        file=sys.stderr,
    )
    return True


def _make_predict_fn(
    database_url: str,
    models_dir: Path,
    source_url: str,
    r2: R2Config | None,
) -> PredictCategoryFn:
    """Build the full-pipeline ``predict_fn`` adapter for :func:`iter_predict_chunks`.

    Wraps ``_predict_category`` with the environment-resolved Neon URL, models
    directory, and source URL so the serve handler only passes (category,
    run_date, days_ahead) per request.

    When *r2* is not ``None``, the final feature parquet is uploaded to R2 after
    a successful prediction run so ``mode=rescore`` can reuse it on subsequent
    calls.  Any R2 upload failure is logged and swallowed so the prediction run
    is never blocked by a cache-write failure.
    """

    def _predict(category_str: str, run_date: str, days_ahead: int) -> int:
        from predict_lib.model_meta import resolve_category

        category = resolve_category(category_str)
        window = PredictWindow(target_date=run_date, days_ahead=days_ahead, database_url=source_url)
        written = _predict_category(database_url, category, models_dir, window)
        # Upload the built feature parquet to R2 so rescore can reuse it.
        if r2 is not None:
            _try_r2_put(r2, category_str, run_date)
        return written

    return _predict


def _try_r2_put(r2: R2Config, category: str, run_date: str) -> None:
    """Upload the local feature parquet to R2; log + swallow any failure."""
    from pipeline_runner import WORK_DIR  # bundled in image

    final_dir = WORK_DIR / f"feat-{category}-v7-final"
    # Collect all parquet files from the final dir (partitioned layout).
    parquet_files = list(final_dir.rglob("*.parquet"))
    if not parquet_files:
        print(
            f"[predict-serve] R2 put skip: no parquet in {final_dir}",
            file=sys.stderr,
        )
        return
    # Use the first (or only) parquet file as the cache payload.
    # Single-file assumption is valid for the final merged layer output.
    local_path = parquet_files[0]
    object_key = build_r2_feat_cache_key(category, run_date)
    try:
        _r2_put_parquet(r2, object_key, local_path)
    except BaseException as put_err:
        print(
            f"[predict-serve] R2 put failed key={object_key} error={put_err}",
            file=sys.stderr,
        )


def _ensure_cached_parquet(
    final_dir: Path,
    category_str: str,
    run_date: str,
    r2: R2Config | None,
) -> None:
    """Ensure ``final_dir`` holds a cached feature parquet, fetching from R2 if needed.

    Raises :class:`CacheMissError` when no local parquet exists and either R2 is
    not configured or the R2 object is absent, so ``iter_predict_chunks`` falls
    back to the full pipeline automatically.
    """
    if any(final_dir.rglob("*.parquet")):
        return
    if r2 is None:
        raise CacheMissError(
            f"no local feature cache for category={category_str} run_date={run_date}"
        )
    object_key = build_r2_feat_cache_key(category_str, run_date)
    dest_path = final_dir / "features.parquet"
    if not _r2_get_parquet(r2, object_key, dest_path):
        raise CacheMissError(f"R2 cache miss: {object_key} not found in bucket {r2.bucket}")


def _load_cached_races(final_dir: Path) -> dict[str, list[Mapping[str, object]]]:
    """Read the cached feature parquet into a ``race_id`` -> entries map directly.

    This bypasses ``pipeline_runner.build_upcoming_feature_rows`` (which always
    runs the DuckDB base build + layer chain) so the rescore path never triggers
    the 21y Neon scan — it only reads the already-built parquet from the cache.
    """
    import pandas as pd

    from pipeline_runner import RACE_ID_FIELD  # bundled in image

    frame = pd.read_parquet(final_dir)
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for race_id, race_frame in frame.groupby(RACE_ID_FIELD):
        grouped[str(race_id)] = list(race_frame.to_dict(orient="records"))
    return grouped


def _scope_race_keys(
    races: dict[str, list[dict[str, object]]],
    scope: RaceScope,
) -> list[tuple[str, str]]:
    """Return the distinct (keibajo_code, race_bango) pairs to fetch fresh odds for.

    Filters the cache to the requested scope first so the realtime fetch only
    hits the races that will actually be rescored.
    """
    from predict_lib.race_id import parse_race_id

    scoped = filter_races_by_scope(races, scope)
    keys: dict[tuple[str, str], None] = {}
    for race_id in scoped:
        parts = parse_race_id(race_id)
        keys[(parts.keibajo_code, parts.race_bango)] = None
    return list(keys)


def _as_entry_map(
    races: Mapping[str, list[Mapping[str, object]]],
) -> dict[str, list[dict[str, object]]]:
    """Narrow the read-only cache map to the mutable dict-entry shape the pure
    rescore helpers expect (pandas ``to_dict`` already yields plain dicts)."""
    return {race_id: [dict(entry) for entry in entries] for race_id, entries in races.items()}


def _fetch_fresh_snapshots(
    category_str: str,
    run_date: str,
    race_keys: list[tuple[str, str]],
) -> dict[tuple[str, str], RaceFreshSnapshot]:
    """Fetch the latest odds + bataiju per race and build per-race snapshots.

    All HTTP I/O happens here (the only side effect on the rescore path); the
    returned snapshots feed the pure :func:`apply_fresh_snapshots`.  Failures for
    an individual race are swallowed by the fetcher (returns empty), leaving that
    race on the builder's median / NULL fallback.
    """
    from realtime_odds_fetcher import (  # bundled in image
        HttpRealtimeOddsFetcher,
        fetch_odds_for_race,
        fetch_weight_for_race,
        source_for_category,
    )

    fetcher = HttpRealtimeOddsFetcher()
    source = source_for_category(category_str)
    snapshots: dict[tuple[str, str], RaceFreshSnapshot] = {}
    for keibajo_code, race_bango in race_keys:
        odds_rows = fetch_odds_for_race(fetcher, source, run_date, keibajo_code, race_bango)
        weight_map = fetch_weight_for_race(fetcher, source, run_date, keibajo_code, race_bango)
        odds_by_umaban = {
            row[2]: OddsSnapshot(tansho_odds=row[3], tansho_ninkijun=row[4]) for row in odds_rows
        }
        bataiju_by_umaban = {umaban: float(kg) for umaban, kg in weight_map.items()}
        snapshots[(keibajo_code, race_bango)] = RaceFreshSnapshot(
            odds_by_umaban=odds_by_umaban,
            bataiju_by_umaban=bataiju_by_umaban,
        )
    return snapshots


RescoreFactory = Callable[[RaceScope], PredictCategoryFn]
"""Builds a scope-bound rescore ``PredictCategoryFn`` for a single request."""


def _make_rescore_fn(
    database_url: str,
    models_dir: Path,
    source_url: str,
    r2: R2Config | None,
    scope: RaceScope,
) -> PredictCategoryFn:
    """Build the rescore-path ``rescore_fn`` adapter for :func:`iter_predict_chunks`.

    The rescore path (Stage 2 of the per-race rebuild):
    1. Ensures the pre-built feature parquet from a prior ``mode=full`` run is
       available locally (downloads from R2 when configured, else raises
       :class:`CacheMissError` so the full pipeline runs).
    2. Reads the cached parquet directly into a ``race_id`` -> entries map — NO
       DuckDB build, NO 21y Neon scan.
    3. Fetches the latest tansho odds + bataiju for the in-scope races and
       recomputes the 5 late-binding columns (odds_score / popularity_score /
       tansho_odds / tansho_ninkijun / weight_diff_from_avg) per horse.
    4. Filters to the requested race scope (a single race or whole keibajo when
       ``keibajoCode`` / ``raceBango`` are set; all races otherwise).
    5. Scores (NAR ensemble routing / JRA E-top2) and UPSERTs the predictions.

    ``source_url`` is unused on this path (no Neon feature scan) but kept in the
    signature for parity with :func:`_make_predict_fn`.
    """
    del source_url  # no Neon feature scan on the rescore path

    def _rescore(category_str: str, run_date: str, days_ahead: int) -> int:
        del days_ahead  # the cache already spans the morning build window
        from pipeline_runner import WORK_DIR  # bundled in image
        from predict_lib.model_meta import resolve_category

        category = resolve_category(category_str)
        final_dir = WORK_DIR / f"feat-{category}-v7-final"
        _ensure_cached_parquet(final_dir, category_str, run_date, r2)

        races = _as_entry_map(_load_cached_races(final_dir))
        race_keys = _scope_race_keys(races, scope)
        snapshots = _fetch_fresh_snapshots(category_str, run_date, race_keys)
        refreshed = apply_fresh_snapshots(races, snapshots, category)
        scoped = filter_races_by_scope(refreshed, scope)
        return _score_and_flush_races(database_url, category, models_dir, scoped)

    return _rescore


def _make_rescore_factory(
    database_url: str,
    models_dir: Path,
    source_url: str,
    r2: R2Config | None,
) -> RescoreFactory:
    """Return a factory that binds the request's race scope to a rescore fn.

    The HTTP handler calls this per request with the ``keibajoCode`` /
    ``raceBango`` scope parsed from the query string, so a single startup-time
    binding serves every per-race rescore request.
    """

    def _factory(scope: RaceScope) -> PredictCategoryFn:
        return _make_rescore_fn(database_url, models_dir, source_url, r2, scope)

    return _factory


def _scope_from_params(params: PredictParams) -> RaceScope:
    """Build the race scope from the parsed ``keibajoCode`` / ``raceBango`` params."""
    return RaceScope(keibajo_code=params.keibajo_code, race_bango=params.race_bango)


class _PredictHandler(http.server.BaseHTTPRequestHandler):
    """Minimal HTTP/1.1 request handler for ``/ping`` and ``/predict``."""

    predict_fn: PredictCategoryFn  # injected by make_handler_class
    rescore_factory: RescoreFactory | None  # injected by make_handler_class

    @override
    def log_message(self, format: str, *args: object) -> None:
        # Redirect access log to stderr to avoid polluting stdout.
        print(f"[predict-serve] {format % args}", file=sys.stderr)

    def do_GET(self) -> None:  # N802: stdlib BaseHTTPRequestHandler requires this name
        path, query = parse_request_path(self.path)

        if path == "/ping":
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/predict":
            result = parse_predict_params(query)
            if isinstance(result, str):
                # Validation error — return 400 before writing any body.
                error_body = result.encode()
                self.send_response(400)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(error_body)))
                self.end_headers()
                self.wfile.write(error_body)
                return

            # Start 200 chunked response immediately so the DO renews its timeout.
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()

            # Bind the request's race scope (keibajoCode / raceBango) to a fresh
            # rescore fn so each per-race rescore request only touches its races.
            rescore_fn: PredictCategoryFn | None = (
                self.rescore_factory(_scope_from_params(result))
                if self.rescore_factory is not None
                else None
            )
            for chunk in iter_predict_chunks(result, self.predict_fn, rescore_fn=rescore_fn):
                # HTTP/1.1 chunked encoding: hex length + CRLF + data + CRLF
                size_line = f"{len(chunk):X}\r\n".encode()
                try:
                    self.wfile.write(size_line + chunk + b"\r\n")
                    self.wfile.flush()
                except OSError as write_err:
                    print(
                        f"[predict-serve] write error: {write_err}",
                        file=sys.stderr,
                    )
                    return

            # Terminating chunk
            try:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except OSError:
                pass
            return

        # Unknown path
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()


def make_handler_class(
    predict_fn: PredictCategoryFn,
    rescore_factory: RescoreFactory | None,
) -> type[_PredictHandler]:
    """Return a ``_PredictHandler`` subclass with ``predict_fn`` + ``rescore_factory`` bound.

    ``predict_fn`` is stored as a ``staticmethod`` object so Python's descriptor
    protocol does NOT inject ``self`` when accessed on an instance.  Plain
    function assignment to a class attribute creates a bound method and prepends
    ``self``, which causes a 4-argument ``TypeError`` on the 3-argument
    ``_predict`` signature.  ``rescore_factory`` is invoked per request inside
    ``do_GET`` with the request's race scope, so it is also stored as a
    ``staticmethod`` to avoid the same ``self`` injection.
    """
    _predict: PredictCategoryFn = predict_fn
    _rescore_factory: RescoreFactory | None = rescore_factory

    @final
    class _BoundHandler(_PredictHandler):
        predict_fn = staticmethod(_predict)
        rescore_factory = staticmethod(_rescore_factory) if _rescore_factory is not None else None

    return _BoundHandler


def serve_http(
    port: int,
    predict_fn: PredictCategoryFn,
    rescore_factory: RescoreFactory | None = None,
) -> None:
    """Start the blocking HTTP server on *port*.

    This function never returns (the server runs until the process is killed).
    It is intentionally NOT covered by unit tests — it is the I/O-boundary glue
    that creates the real socket and blocks forever.  The pure logic it delegates
    to (``iter_predict_chunks``, ``parse_predict_params``, etc.) is fully tested
    in ``tests/test_serve.py``.
    """
    handler_cls = make_handler_class(predict_fn, rescore_factory)
    with http.server.HTTPServer(("0.0.0.0", port), handler_cls) as httpd:
        print(f"[predict-serve] listening on :{port}", file=sys.stderr)
        httpd.serve_forever()


def _is_serve_mode(argv: list[str]) -> bool:
    """Return True when the process should start HTTP server mode.

    Activated by ``PREDICT_SERVE_MODE=http`` environment variable OR by passing
    ``--serve`` as a CLI argument.  Case-insensitive env-var check to tolerate
    ``HTTP`` / ``Http`` typos.
    """
    if os.environ.get(PREDICT_SERVE_MODE_ENV, "").strip().lower() == "http":
        return True
    return "--serve" in argv


def main() -> int:
    """Entry point for both CLI batch mode and HTTP server mode.

    Server mode is activated when ``PREDICT_SERVE_MODE=http`` is set or ``--serve``
    is passed.  Otherwise, the one-shot CLI batch run is executed (Mac launchd
    cron path — unchanged).
    """
    if _is_serve_mode(sys.argv):
        try:
            database_url = normalise_database_url(_require_env(NEON_DATABASE_URL_ENV))
            source_url = resolve_source_url(os.environ.get(SOURCE_DATABASE_URL_ENV), database_url)
            models_dir = Path(os.environ.get(MODELS_DIR_ENV, "/models"))
        except BaseException as bootstrap_error:
            traceback.print_exc()
            print(f"[predict-serve] bootstrap failed: {bootstrap_error}", file=sys.stderr)
            return 1
        r2 = _load_r2_config()
        predict_fn = _make_predict_fn(database_url, models_dir, source_url, r2)
        rescore_factory = _make_rescore_factory(database_url, models_dir, source_url, r2)
        serve_http(HTTP_PORT, predict_fn, rescore_factory)
        return 0  # unreachable but satisfies the return type

    started = time.monotonic()
    _start_liveness_thread(LIVENESS_PORT)
    try:
        database_url = normalise_database_url(_require_env(NEON_DATABASE_URL_ENV))
        source_url = resolve_source_url(os.environ.get(SOURCE_DATABASE_URL_ENV), database_url)
        run_date = _require_env(RUN_DATE_ENV)
        days_ahead = int(os.environ.get(DAYS_AHEAD_ENV, str(DEFAULT_DAYS_AHEAD)))
        models_dir = Path(os.environ.get(MODELS_DIR_ENV, "/models"))
        window = PredictWindow(target_date=run_date, days_ahead=days_ahead, database_url=source_url)
        # Validate the Neon URL at startup (fail fast on bad credentials /
        # unreachable host) but immediately close the probe connection. The
        # write connection is opened lazily inside _predict_category, after the
        # feature build, so Neon autosuspend during the long feature-build phase
        # cannot kill the write connection before the first UPSERT.
        probe = _connect(database_url)
        probe.close()
    except BaseException as bootstrap_error:
        # Pre-connect failure (missing env var, bad URL, Neon down, etc). Nothing
        # to audit-write into yet — emit the full traceback so a future silent
        # startup crash is visible in container logs.
        traceback.print_exc()
        print(f"[predict-upcoming] bootstrap failed: {bootstrap_error}", file=sys.stderr)
        return 1
    races_predicted = 0
    categories = _resolve_categories(os.environ.get(CATEGORIES_ENV))
    failures: list[str] = []
    for category in categories:
        try:
            races_predicted += _predict_category(database_url, category, models_dir, window)
        except BaseException as category_error:
            # Per-category isolation: one category's failure (e.g. Neon SSL
            # idle-timeout during the long-running DuckDB postgres_scanner) must
            # not block the others. Log the full traceback then move on. We
            # collect the error texts so the final audit row records the partial
            # failure rather than masking it.
            traceback.print_exc()
            text = f"{category}: {type(category_error).__name__}: {category_error}"
            print(f"[predict-upcoming] category failed: {text}", file=sys.stderr)
            failures.append(text)
    duration_ms = int((time.monotonic() - started) * 1000)
    if failures:
        error_text = "; ".join(failures)
        _try_record_audit(database_url, run_date, races_predicted, duration_ms, error_text)
        if races_predicted == 0:
            print(f"[predict-upcoming] failed: {error_text}", file=sys.stderr)
            return 1
        print(
            f"[predict-upcoming] partial run_date={run_date} races_predicted={races_predicted}"
            f" failures={error_text}"
        )
        return 0
    _try_record_audit(database_url, run_date, races_predicted, duration_ms, None)
    print(f"[predict-upcoming] ok run_date={run_date} races_predicted={races_predicted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
