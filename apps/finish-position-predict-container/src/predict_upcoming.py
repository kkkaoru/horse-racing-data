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

Run with: ``uv run python src/predict_upcoming.py`` (envvars set by the Worker).
"""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
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
from predict_lib.booster_pool import BoosterPool
from predict_lib.conn_url import normalise_database_url, resolve_source_url
from predict_lib.dedupe import dedupe_batch
from predict_lib.ensemble_routing import (
    EnsembleRouteOutcome,
    init_member_pool,
    score_race_with_resolution,
)
from predict_lib.etop2_override import apply_etop2_scores, is_etop2_override_active
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
from predict_lib.scorer import BoosterLike, assert_feature_count, build_feature_matrix, score_matrix
from predict_lib.upcoming import build_prediction_rows, rank_race_entries
from predict_lib.upsert_sql import (
    DEFAULT_CHUNK_SIZE,
    build_upsert_sql,
    chunk_rows,
    flatten_params,
)

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
# Cloudflare Containers reaps batch instances that receive no HTTP traffic
# (independent of @cloudflare/containers' JS-side sleepAfter). The predictor
# is a long-running batch job, so we both (a) listen on a port so the start
# probe + DO containerFetch resolve, AND (b) honour repeated HTTP keepalive
# pings from the Worker DO's scheduled loop. The server is tiny on purpose —
# the only HTTP requirement is "200 OK on every request".
LIVENESS_PORT: int = 8080
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
    database_url: str,
    category: Category,
    models_dir: Path,
    window: PredictWindow,
) -> int:
    feature_names = _load_model_metadata(models_dir, category)
    fallback_booster = _load_booster(models_dir, category)
    # Pool is built per-category at the top of the prediction loop so cold-start
    # cost is paid once and every race in the loop is a dict lookup. Categories
    # with no registered ensembles (NAR / Ban-ei today) get an empty pool and
    # always take the single-model fast path inside score_race_with_resolution.
    pool = init_member_pool(models_dir, category)

    # E-top2 path: load XGB companion model for JRA when the flag is active.
    # The XGB model uses the same 244-feature order as CB iter20 (same store).
    xgb_etop2_booster: BoosterLike | None = None
    if JRA_ETOP2_ENABLED and category == "jra":
        xgb_etop2_booster = _load_xgb_etop2_booster(models_dir)

    # Score all races before opening the Neon write connection. The feature
    # build is the longest step (DuckDB base build + 14 layer scripts, typically
    # 2-5 min) and Neon autosuspends after ~60s of idle — connecting before the
    # build would cause AdminShutdown on the first UPSERT. Scoring is CPU-bound
    # and connection-free, so we defer the Neon connect until the first write.
    races = _build_feature_rows(category, window)
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

    # All races scored — now open the Neon connection and flush.
    connection = _connect(database_url)
    try:
        written = 0
        for rows in scored:
            written += _flush_predictions(connection, rows)
    finally:
        try:
            connection.close()
        except BaseException as close_error:
            print(
                f"[predict-upcoming] connection close failed category={category}: {close_error}",
                file=sys.stderr,
            )
    return written


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
    return connect_postgres(database_url)


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
        _record_audit(audit_connection, run_date, status, races_predicted, duration_ms, error_text)
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
    _start_liveness_thread(LIVENESS_PORT)
    try:
        database_url = normalise_database_url(_require_env(NEON_DATABASE_URL_ENV))
        source_url = resolve_source_url(os.environ.get(SOURCE_DATABASE_URL_ENV), database_url)
        run_date = _require_env(RUN_DATE_ENV)
        days_ahead = int(os.environ.get(DAYS_AHEAD_ENV, str(DEFAULT_DAYS_AHEAD)))
        models_dir = Path(os.environ.get(MODELS_DIR_ENV, "/models"))
        window = PredictWindow(
            target_date=run_date, days_ahead=days_ahead, database_url=source_url
        )
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
