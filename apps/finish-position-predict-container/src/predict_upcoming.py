#!/usr/bin/env python3
"""Container entrypoint: daily finish-position prediction for UPCOMING races (N-variant).

This is the heavy orchestration that the Cloudflare Cron Trigger Worker starts
as a batch container job. It is intentionally thin — every decision lives in the
unit-tested ``predict_lib`` package — and it is excluded from the coverage gate
because it only wires together real I/O (R2 Catalog and models over HTTPS,
Neon result writes, DuckDB subprocess feature build, native CatBoost/XGBoost). That integration is
verified at deploy time per ``DEPLOY.md``, not by unit tests.

Flow per category (jra / nar / ban-ei):
  1. List UPCOMING races (today .. today + PREDICT_DAYS_AHEAD, finish_position
     NULL) from the raw Iceberg Catalog via ``SOURCE_DATABASE_URL``.
  2. Build the v8 feature parquet (JRA=241 / NAR=192 / Ban-ei=111) by running
     the repo feature pipeline (DuckDB base build + the v7 layer scripts + the
     v8 pacestyle / course-numerical layers per
     ``predict_lib.pipeline_args.LAYER_CHAIN``) against the same Catalog.
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

import base64
import contextlib
import hashlib
import http.server
import json
import math
import os
import shutil
import socket
import sys
import threading
import time
import traceback
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Final, Literal, cast, final, get_args, override

from db_driver import ConnectionLike, connect_postgres_with_retry, is_transient_error
from predict_lib.audit import (
    AuditStatus,
    audit_params,
    build_audit_insert_sql,
    build_audit_record,
    build_audit_table_ddl,
)
from predict_lib.booster_pool import PoolBooster
from predict_lib.cell_router import (
    CONFIG_FILE_NAME as CELL_ROUTING_CONFIG_FILE_NAME,
)
from predict_lib.cell_router import (
    CellRouter,
    build_base_model_r2_key,
    card_max_race_bango_for_race_id,
    derive_card_max_race_bango_by_card,
    entry_has_race_name,
    load_cell_router,
    overlay_race_name_onto_entry,
    overlay_race_names_on_races,
)
from predict_lib.conn_url import is_catalog_source_url, normalise_database_url, resolve_source_url
from predict_lib.debug_log import debug_log, query_debug_enabled
from predict_lib.dedupe import dedupe_batch
from predict_lib.dynamic_market_shadow import (
    SHADOW_BATCH_SIZE,
    UPSET_FEATURE_NAMES,
    DynamicMarketShadowRecord,
    ProbabilityModel,
    build_joint_additional_candidate_record,
    build_shadow_batch_upsert_sql,
    build_shadow_migration_sql,
    build_shadow_records,
    build_shadow_table_ddl,
    classifier_version,
    served_baseline_from_rows,
    shadow_batch_params,
    surface_expert_version,
)
from predict_lib.ensemble_routing import catboost_model_feature_names, member_feature_order_matches
from predict_lib.etop2_override import apply_etop2_scores, is_etop2_override_active
from predict_lib.feature_guard import is_degenerate_feature_matrix
from predict_lib.focused_full_cache import FocusedFullCachePayload, FocusedFullCacheStore
from predict_lib.jra_hybrid_scorer import (
    JraHybridScorer,
    fuse_jra_hybrid_scores,
)
from predict_lib.jra_joint_alternate_scorer import (
    CHAMPION_MODEL_VERSION as JOINT_CHAMPION_MODEL_VERSION,
)
from predict_lib.jra_joint_alternate_scorer import (
    MODEL_VERSION as JOINT_ALTERNATE_MODEL_VERSION,
)
from predict_lib.jra_joint_alternate_scorer import (
    SPECIALIST_MODEL_VERSION as JOINT_SPECIALIST_MODEL_VERSION,
)
from predict_lib.jra_joint_alternate_scorer import (
    JraJointAlternateScorer,
)
from predict_lib.late_binding import OddsSnapshot
from predict_lib.lock1_rerank import apply_lock1_rerank_rest
from predict_lib.model_meta import (
    CATEGORIES,
    JRA_DIRT_HYBRID_ENABLED,
    JRA_DIRT_HYBRID_MODEL_VERSION,
    JRA_ETOP2_ENABLED,
    JRA_ETOP2_MODEL_VERSION,
    JRA_ETOP2_XGB_MODEL_VERSION,
    METADATA_FILE_NAME,
    MODEL_FILE_NAME,
    MODEL_META_JSON_PATH,
    NAR_ETOP2_MODEL_VERSION,
    NAR_TRANSFORMER_BLEND_ENABLED,
    NAR_TRANSFORMER_BLEND_WEIGHT,
    NAR_TRANSFORMER_MODEL_VERSION,
    Architecture,
    Category,
    architecture_for,
    assert_no_within_race_leak_columns,
    assert_production_model_version_allowed,
    build_r2_jra_dirt_hybrid_key,
    build_r2_nar_transformer_key,
    build_r2_object_key,
    build_r2_xgb_etop2_key,
    feature_count_for,
    model_version_for,
)
from predict_lib.nar_etop2_override import (
    apply_nar_etop2_scores,
    is_nar_etop2_override_active,
)
from predict_lib.r2_client import (
    R2ObjectIdentity,
    r2_get_parquet,
    r2_head_identity,
    r2_head_watermark,
)
from predict_lib.rescore import (
    RaceFreshSnapshot,
    RaceScope,
    apply_fresh_snapshots,
    filter_post_weight_active_runners,
    filter_races_by_scope,
    race_scope_from_target_race,
    validate_post_weight_snapshots,
)
from predict_lib.scorer import BoosterLike, assert_feature_count, build_feature_matrix, score_matrix
from predict_lib.serve import (
    RESCORE_ATTESTATION_FUTURE_SKEW_MS,
    RESCORE_ATTESTATION_TTL_MS,
    CacheMissError,
    CacheValidationError,
    FocusedFullCachePopulateFn,
    FocusedFullCompletionFn,
    FocusedFullTerminalFn,
    ParquetPayloadFn,
    PerRaceParquetPayloadFn,
    PredictCategoryFn,
    PredictParams,
    PrewarmBackgroundFn,
    PrewarmBuildFn,
    PrewarmCommitFn,
    PrewarmExistingObjectFn,
    PrewarmParquetPayloadFn,
    R2Config,
    RescoreCacheAttestation,
    WeightSnapshotGeneration,
    build_focused_full_cache_response_body,
    build_focused_full_race_key,
    build_focused_full_status_response_body,
    build_prewarm_cache_key,
    build_prewarm_status_response_body,
    build_r2_day_base_key,
    build_r2_feat_cache_key,
    build_r2_per_race_feat_cache_key,
    build_r2_running_style_foundation_key,
    is_scoped_rescore_cache_miss_fallback,
    iter_predict_chunks,
    iter_prewarm_chunks,
    mark_focused_full_progress,
    parse_day_base_cache_identity,
    parse_focused_full_cache_query,
    parse_predict_params,
    parse_prewarm_params,
    parse_request_path,
    run_prewarm_in_background,
)
from predict_lib.stage1_routing import (
    STAGE1_ROUTING_PATH,
    Stage1CategoryConfig,
    extract_predicted_scores,
    is_named_race_cell_score,
    load_stage1_routing,
    race_passes_top1_swap_weather_gate,
    resolve_stage1_gate,
)
from predict_lib.stage1_top1_swap import apply_top1_score_swap
from predict_lib.subgroup import classify_surface
from predict_lib.transformer_scorer import (
    TransformerScorer,
    fuse_ensemble_transformer,
    load_transformer,
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
"""Write-only destination for prediction UPSERTs and audit records."""
FRESH_SNAPSHOT_MAX_WORKERS: Final[int] = 4
"""Maximum simultaneous odds/weight HTTP requests during rescore."""
NEON_WRITE_STATEMENT_PREFIXES: Final[frozenset[str]] = frozenset(
    {
        "ALTER",
        "COMMENT",
        "CREATE",
        "DELETE",
        "DROP",
        "GRANT",
        "INSERT",
        "MERGE",
        "REVOKE",
        "TRUNCATE",
        "UPDATE",
    }
)
"""Statements permitted on the Container's write-only Neon boundary."""
# Required source URL for the DuckDB feature-build subprocess. Production uses
# ``r2-catalog://pc-keiba`` and never falls back to Neon or local PostgreSQL.
# Prediction UPSERT and audit writes continue to use ``NEON_DATABASE_URL``.
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
# Per-category feature-parquet column name carrying the historical race-class
# code. JRA uses ``kyoso_joken_code`` (000/005/010/016/701/703/...) which the
# DuckDB base build projects directly from the source rows. NAR uses
# ``nar_subclass`` (NEW / MUKATSU / C / B / A / OP / other) which the DuckDB
# base build derives from ``kyoso_joken_meisho`` via a regex CASE expression
# (apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py
# ``nar_subclass_case_sql``). Production serving no longer uses this for
# per-class routing; it remains for E-top2 helper tests and diagnostics.
CLASS_CODE_FIELD_BY_CATEGORY: Mapping[Category, str] = {
    "jra": "kyoso_joken_code",
    "nar": "nar_subclass",
}
HTTP_PORT: int = 8080
"""Port the HTTP server listens on in both CLI batch mode (liveness) and server mode."""
FOCUSED_FULL_CALLBACK_HEADER: Final[str] = "x-focused-full-completion-callback"
FOCUSED_FULL_CALLBACK_TIMEOUT_SECONDS: Final[float] = 5.0
FOCUSED_FULL_CALLBACK_USER_AGENT: Final[str] = "horse-racing-prediction-container/1.0"
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
    assert_production_model_version_allowed(
        model_version_for(category),
        context=f"category default metadata category={category}",
    )
    assert_no_within_race_leak_columns(
        feature_names,
        context=f"category default metadata category={category}",
    )
    assert_feature_count(feature_names, feature_count_for(category))
    return feature_names


def extract_race_class_code(
    category: Category, entries: Sequence[Mapping[str, object]]
) -> str | None:
    """Return the legacy race-class diagnostic code from the first entry.

    The column name is per-category: JRA reads ``kyoso_joken_code`` (numeric
    race-class code), NAR reads ``nar_subclass`` (derived sub-class string).
    Categories not in :data:`CLASS_CODE_FIELD_BY_CATEGORY` return ``None``.
    All entries of one race share the same race-class, so the first entry is
    representative. ``None`` and empty strings collapse to ``None``.
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


def _representative_entry(
    entries: Sequence[Mapping[str, object]],
) -> Mapping[str, object] | None:
    """Return one entry standing in for the race's row-level subgroup metadata.

    Every horse in a race shares the same race-level metadata (distance, field
    size, surface, class, venue), so the first entry is representative. An empty
    race yields ``None`` and the subgroup classifier falls back to ``None`` for
    every dimension.
    """
    if not entries:
        return None
    return entries[0]


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
        debug_log(f"[etop2] override fired race_id={race_id} class={class_code}")

    ranked = rank_race_entries(entries, override_scores)
    return build_prediction_rows(
        race_id,
        "jra",
        ranked,
        JRA_ETOP2_MODEL_VERSION,
        _representative_entry(entries),
        entries=entries,
    )


def score_one_race_nar_etop2(
    xgb_booster: BoosterLike,
    cb_booster: BoosterLike,
    race_id: str,
    category: Category,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> list[list[object]]:
    """Score one NAR race with the E-top2 per-class place-preserving override.

    Mirror image of :func:`_score_one_race_etop2`: NAR production scores with
    XGBoost (``iter12-nar-xgb-hpo-v8``) as the BASE, and the CatBoost CB-2013
    model (``cb-nar-2013-v8``) supplies the override signal. Both score the same
    192-feature matrix. The override fires per race only for ADOPT classes when
    CB#1 == XGB#2 (CB#3 stays at rank-3, preserving place3).

    The class code is read from the ``nar_subclass`` column of the first entry
    (same field used by :func:`extract_race_class_code` for NAR). When the column
    is absent / class is not in the ADOPT set, the pure XGB ranking is emitted.
    """
    xgb_matrix = build_feature_matrix(entries, feature_names, "xgboost")
    cb_matrix = build_feature_matrix(entries, feature_names, "catboost")
    xgb_scores = score_matrix(xgb_booster, xgb_matrix)
    cb_scores = score_matrix(cb_booster, cb_matrix)

    nar_class = extract_race_class_code("nar", entries)
    override_scores = apply_nar_etop2_scores(xgb_scores, cb_scores, nar_class)

    fired = is_nar_etop2_override_active(xgb_scores, cb_scores, nar_class)
    if fired:
        debug_log(f"[nar-etop2] override fired race_id={race_id} class={nar_class}")

    ranked = rank_race_entries(entries, override_scores)
    return build_prediction_rows(
        race_id,
        "nar",
        ranked,
        NAR_ETOP2_MODEL_VERSION,
        _representative_entry(entries),
        entries=entries,
    )


def _row_to_pk_map(row: Sequence[object]) -> Mapping[str, object]:
    race_id = ":".join(str(row[index]) for index in RACE_ID_PART_RANGE)
    return {"race_id": race_id, "ketto_toroku_bango": row[RACE_ID_KETTO_INDEX]}


def _require_neon_write_statement(sql: str) -> None:
    first_token = sql.lstrip().partition(" ")[0].partition("\n")[0].upper()
    if first_token not in NEON_WRITE_STATEMENT_PREFIXES:
        raise ValueError(
            f"Neon read/non-write statement prohibited in prediction Container: {first_token}"
        )


def execute(
    connection: ConnectionLike,
    sql: str,
    params: Sequence[object],
    database_url: str,
) -> ConnectionLike:
    """Execute write-only ``sql`` against ``connection``, reconnecting once on transient loss.

    Returns the (possibly new) connection so callers can rebind after a
    reconnect. On AdminShutdown or "connection is lost/closed" mid-write,
    opens a fresh Neon connection via :func:`_connect` and retries the
    statement once. Any second failure propagates to the caller.

    Forces each write onto a writable transaction (BEGIN + SET TRANSACTION
    READ WRITE) to defend against Neon's txn-mode pooler inheriting
    default_transaction_read_only=on from a prior session (same root cause
    as the 2026-08-10 sync-realtime-data incident fixed in commit 171ed4d2).
    Without this, INSERT/UPSERT silently fails with SQLSTATE 25006 when the
    pooler hands out a read-only session. Read statements are rejected before
    a cursor is opened; all prediction inputs must come from Cloudflare.
    """
    _require_neon_write_statement(sql)
    try:
        cursor = connection.cursor()
        cursor.execute("BEGIN")
        cursor.execute("SET TRANSACTION READ WRITE")
        cursor.execute(sql, params)
        connection.commit()
        return connection
    except BaseException as exc:
        if not is_transient_error(exc):
            raise
        # Transient mid-write failure: attempt a single reconnect then retry.
        debug_log(
            f"[predict-upcoming] mid-write transient error ({type(exc).__name__}): {exc} "
            "— reconnecting and retrying once"
        )
        try:
            connection.rollback()
        except BaseException as rb_exc:
            debug_log(f"[predict-upcoming] rollback failed: {rb_exc}")
        with contextlib.suppress(BaseException):
            connection.close()
        fresh = _connect(database_url)
        cursor = fresh.cursor()
        cursor.execute("BEGIN")
        cursor.execute("SET TRANSACTION READ WRITE")
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


def _as_architecture(value: str) -> Architecture:
    for candidate in get_args(Architecture):
        if value == candidate:
            return candidate
    raise ValueError(f"unknown base_architecture: {value!r}")


def _feature_set_hash(feature_names: Sequence[str]) -> str:
    """Order-independent hash matching local feature-selection policy."""
    normalized = sorted({name.strip() for name in feature_names if name.strip()})
    canonical = json.dumps(normalized, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _validate_variant_feature_contract(
    variant_name: str,
    model_version: str,
    metadata_feature_names: Sequence[str],
    config_feature_names: Sequence[str] | None,
    config_feature_set_hash: str | None,
) -> None:
    """Ensure a cell-routing variant's config matches its baked artifact."""
    if config_feature_names is not None and tuple(metadata_feature_names) != tuple(
        config_feature_names
    ):
        raise ValueError(
            f"cell-routing variant={variant_name} model_version={model_version} "
            "feature_names do not match metadata.json"
        )
    if config_feature_set_hash is not None:
        actual_hash = _feature_set_hash(metadata_feature_names)
        if actual_hash != config_feature_set_hash:
            raise ValueError(
                f"cell-routing variant={variant_name} model_version={model_version} "
                f"feature_set_hash mismatch: config={config_feature_set_hash} "
                f"metadata={actual_hash}"
            )


def _variant_booster_feature_order_matches(
    booster: BoosterLike,
    architecture: Architecture,
    metadata_feature_names: Sequence[str],
) -> bool:
    """Cross-check a cell-routing variant's booster against its metadata.json order.

    ``_feature_set_hash`` (used by :func:`_validate_variant_feature_contract`) is
    deliberately ORDER-INDEPENDENT -- it sorts before hashing, matching the local
    feature-SELECTION policy it was built for -- so it can never catch a variant
    whose ``metadata.json`` carries the right SET of feature names in the WRONG
    order relative to what the booster was actually trained on. CatBoost / XGBoost
    score positionally, so a silent order drift there would score every entry
    against permuted columns while still "succeeding" (no exception, a smooth,
    plausible-looking but wrong score). This reuses the SAME order-check already
    applied to per-class ensemble members (:func:`predict_lib.ensemble_routing.
    member_feature_order_matches`) so cell-routing variants get the identical
    guarantee. Returns True (no-op pass) for XGBoost, mirroring
    ``catboost_model_feature_names``'s own empty-tuple-is-a-match contract for
    boosters that do not expose a trained column order.
    """
    record = PoolBooster(
        booster=booster, architecture=architecture, feature_names=tuple(metadata_feature_names)
    )
    booster_names = catboost_model_feature_names(record)
    return member_feature_order_matches(booster_names, metadata_feature_names)


@dataclass(frozen=True)
class VariantModel:
    """A loaded cell-routing variant: its booster plus its feature contract.

    ``score_races`` builds a ``dict[str, VariantModel]`` keyed by variant name for
    every non-default variant in the category's routing config, so a race routed
    to a variant scores against that variant's model, feature order, and
    architecture. The default variant is served by the already-loaded
    category-global ``fallback_booster`` and never enters the pool.
    """

    booster: BoosterLike
    feature_names: Sequence[str]
    architecture: Architecture
    model_version: str
    top1_swap_base: VariantModel | None = None
    routing_mode: str = "direct"
    minimum_candidate_margin: float | None = None
    minimum_candidate_top_z: float | None = None
    maximum_candidate_v2_rank: int | None = None
    consensus_members: tuple[VariantModel, ...] = ()
    consensus_required_votes: int | None = None
    lock1_rerank_model: VariantModel | None = None


@dataclass(frozen=True)
class DynamicMarketShadowModels:
    """Complete shadow-only classifier, experts, and joint candidate bundle."""

    classifier: ProbabilityModel
    classifier_version: str
    experts: Mapping[str, VariantModel]
    joint_alternate: JraJointAlternateScorer
    joint_champion: VariantModel
    joint_specialist: VariantModel


@dataclass(frozen=True)
class ModelBundle:
    """Immutable category scoring runtime shared within one Container process."""

    cell_router: CellRouter
    dynamic_market_shadow: DynamicMarketShadowModels | None
    fallback_booster: BoosterLike
    feature_names: tuple[str, ...]
    jra_dirt_hybrid: JraHybridScorer | None
    nar_transformer: TransformerScorer | None
    stage1_config: Stage1CategoryConfig | None
    stage1_model: VariantModel | None
    variant_pool: Mapping[str, VariantModel]
    xgb_etop2_booster: BoosterLike | None


@dataclass(frozen=True)
class ModelBundleCacheKey:
    """Identity of every file boundary that can change a loaded model bundle."""

    artifact_signature: tuple[tuple[str, int, int, int], ...]
    category: Category
    feature_names_override: tuple[str, ...] | None
    models_dir: str


_MODEL_BUNDLE_CACHE: dict[ModelBundleCacheKey, ModelBundle] = {}
_MODEL_BUNDLE_CACHE_LOCK: threading.Lock = threading.Lock()
_MODEL_ROUTING_PATHS: tuple[Path, ...] = (
    Path(__file__).parent / "predict_lib" / CELL_ROUTING_CONFIG_FILE_NAME,
    MODEL_META_JSON_PATH,
    STAGE1_ROUTING_PATH,
)


def _artifact_file_stamp(path: Path, label: str) -> tuple[str, int, int, int]:
    """Return a cheap immutable identity stamp, including a missing-file state."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        return (label, -1, -1, -1)
    return (label, stat.st_ino, stat.st_size, stat.st_mtime_ns)


def _model_artifact_signature(models_dir: Path) -> tuple[tuple[str, int, int, int], ...]:
    """Stamp selected model bytes and routing declarations without reading them."""
    resolved = models_dir.resolve()
    model_stamps = tuple(
        _artifact_file_stamp(path, path.relative_to(resolved).as_posix())
        for path in sorted(resolved.rglob("*"))
        if path.is_file()
    )
    routing_stamps = tuple(
        _artifact_file_stamp(path, f"routing:{path.resolve()}") for path in _MODEL_ROUTING_PATHS
    )
    return model_stamps + routing_stamps


def _model_bundle_cache_key(
    models_dir: Path,
    category: Category,
    feature_names_override: Sequence[str] | None,
) -> ModelBundleCacheKey:
    resolved = models_dir.resolve()
    return ModelBundleCacheKey(
        artifact_signature=_model_artifact_signature(resolved),
        category=category,
        feature_names_override=(
            tuple(feature_names_override) if feature_names_override is not None else None
        ),
        models_dir=str(resolved),
    )


def _load_named_model_metadata(
    path: Path, expected_version: str, *, require_shadow_only: bool
) -> tuple[str, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("model_version") != expected_version:
        raise ValueError(f"named model metadata identity mismatch: {path}")
    raw_names = payload.get("feature_names")
    if (
        not isinstance(raw_names, list)
        or not raw_names
        or not all(isinstance(name, str) for name in raw_names)
    ):
        raise ValueError(f"named model feature_names invalid: {path}")
    feature_names = tuple(raw_names)
    if payload.get("feature_count") != len(feature_names) or (
        require_shadow_only and payload.get("shadow_only") is not True
    ):
        raise ValueError(f"named model metadata contract invalid: {path}")
    return feature_names


def _load_shadow_metadata(path: Path, expected_version: str) -> tuple[str, ...]:
    return _load_named_model_metadata(path, expected_version, require_shadow_only=True)


def _load_dynamic_market_shadow_models(
    models_dir: Path, category: Category
) -> DynamicMarketShadowModels | None:
    """Load the complete JRA loop-43 shadow bundle, or fail closed to no shadow."""
    if category != "jra":
        return None
    try:
        classifier_model_version = classifier_version()
        classifier_dir = models_dir / "finish-position" / "jra" / classifier_model_version
        classifier_features = _load_shadow_metadata(
            classifier_dir / METADATA_FILE_NAME, classifier_model_version
        )
        if classifier_features != UPSET_FEATURE_NAMES:
            raise ValueError("dynamic-market classifier feature order mismatch")
        from catboost_adapter import load_catboost_probability_model

        classifier = load_catboost_probability_model(str(classifier_dir / MODEL_FILE_NAME))
        experts: dict[str, VariantModel] = {}
        for surface in ("turf", "dirt", "obstacle"):
            for market_free in (False, True):
                model_version = surface_expert_version(surface, market_free=market_free)
                artifact_dir = models_dir / "finish-position" / "jra" / model_version
                expert_features = _load_shadow_metadata(
                    artifact_dir / METADATA_FILE_NAME, model_version
                )
                assert_no_within_race_leak_columns(
                    expert_features,
                    context=f"dynamic-market shadow expert={model_version}",
                )
                booster = _load_booster_by_arch(artifact_dir / MODEL_FILE_NAME, "catboost")
                if not _variant_booster_feature_order_matches(booster, "catboost", expert_features):
                    raise ValueError(
                        f"dynamic-market shadow booster feature order mismatch: {model_version}"
                    )
                experts[model_version] = VariantModel(
                    booster=booster,
                    feature_names=expert_features,
                    architecture="catboost",
                    model_version=model_version,
                )
        joint_dir = models_dir / "finish-position" / "jra" / JOINT_ALTERNATE_MODEL_VERSION
        joint_alternate = JraJointAlternateScorer.load(joint_dir)
        champion_dir = models_dir / "finish-position" / "jra" / JOINT_CHAMPION_MODEL_VERSION
        champion_features = _load_named_model_metadata(
            champion_dir / METADATA_FILE_NAME,
            JOINT_CHAMPION_MODEL_VERSION,
            require_shadow_only=False,
        )
        champion_booster = _load_booster_by_arch(champion_dir / MODEL_FILE_NAME, "catboost")
        if not _variant_booster_feature_order_matches(
            champion_booster, "catboost", champion_features
        ):
            raise ValueError("joint alternate champion feature order mismatch")
        specialist_dir = models_dir / "finish-position" / "jra" / JOINT_SPECIALIST_MODEL_VERSION
        specialist_features = _load_shadow_metadata(
            specialist_dir / METADATA_FILE_NAME, JOINT_SPECIALIST_MODEL_VERSION
        )
        specialist_booster = _load_booster_by_arch(specialist_dir / MODEL_FILE_NAME, "catboost")
        if not _variant_booster_feature_order_matches(
            specialist_booster, "catboost", specialist_features
        ):
            raise ValueError("joint alternate specialist feature order mismatch")
        return DynamicMarketShadowModels(
            classifier=classifier,
            classifier_version=classifier_model_version,
            experts=MappingProxyType(experts),
            joint_alternate=joint_alternate,
            joint_champion=VariantModel(
                booster=champion_booster,
                feature_names=champion_features,
                architecture="catboost",
                model_version=JOINT_CHAMPION_MODEL_VERSION,
            ),
            joint_specialist=VariantModel(
                booster=specialist_booster,
                feature_names=specialist_features,
                architecture="catboost",
                model_version=JOINT_SPECIALIST_MODEL_VERSION,
            ),
        )
    except BaseException as load_error:
        debug_log(f"[dynamic-market-shadow] bundle unavailable -> disabled: {load_error}")
        return None


def _load_model_bundle(
    models_dir: Path,
    category: Category,
    feature_names_override: Sequence[str] | None,
) -> ModelBundle:
    """Load and validate every scoring artifact exactly once for a cache key."""
    feature_names = tuple(
        feature_names_override
        if feature_names_override is not None
        else _load_model_metadata(models_dir, category)
    )
    fallback_booster = _load_booster(models_dir, category)
    cell_router = load_cell_router()
    variant_pool: dict[str, VariantModel] = {}
    if cell_router.has_routing(category):
        routing_config = cell_router.routing_for(category)
        for variant_name, variant_spec in routing_config.variants.items():
            if variant_name == routing_config.default_variant:
                continue
            architecture = _as_architecture(variant_spec.architecture)
            model_path = models_dir / build_base_model_r2_key(
                category, variant_spec.model_version, MODEL_FILE_NAME
            )
            booster = _load_booster_by_arch(model_path, architecture)
            metadata_path = models_dir / build_base_model_r2_key(
                category, variant_spec.model_version, METADATA_FILE_NAME
            )
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            variant_feature_names = tuple(str(name) for name in metadata["feature_names"])
            assert_production_model_version_allowed(
                variant_spec.model_version,
                context=f"cell-routing variant={variant_name} category={category}",
            )
            assert_no_within_race_leak_columns(
                variant_feature_names,
                context=f"cell-routing variant={variant_name} category={category}",
            )
            assert_feature_count(variant_feature_names, variant_spec.feature_count)
            _validate_variant_feature_contract(
                variant_name,
                variant_spec.model_version,
                variant_feature_names,
                variant_spec.feature_names,
                variant_spec.feature_set_hash,
            )
            if not _variant_booster_feature_order_matches(
                booster, architecture, variant_feature_names
            ):
                debug_log(
                    f"[cell-routing] variant={variant_name} category={category} "
                    f"version={variant_spec.model_version} feature-order-mismatch: "
                    "booster's own trained column order disagrees with "
                    "metadata.json -> not loaded, races fall back to category default"
                )
                continue
            routing_mode = getattr(variant_spec, "routing_mode", "direct")
            if routing_mode not in {
                "direct",
                "jra_variant_top1_swap",
                "jra_lock1_rerank_rest",
                "nar_transformer_top1_swap",
                "nar_transformer_top2_swap",
                "nar_transformer_top2_consensus_swap",
            }:
                debug_log(
                    f"[cell-routing] variant={variant_name} category={category} "
                    f"unsupported routing_mode={routing_mode} -> not loaded"
                )
                continue
            top1_swap_base: VariantModel | None = None
            if routing_mode == "jra_variant_top1_swap":
                base_variant = getattr(variant_spec, "base_variant", None)
                if category != "jra" or base_variant is None or base_variant not in variant_pool:
                    debug_log(
                        f"[cell-routing] variant={variant_name} category={category} "
                        f"missing top1-swap base_variant={base_variant} -> not loaded"
                    )
                    continue
                top1_swap_base = variant_pool[base_variant]
            lock1_rerank_model: VariantModel | None = None
            if routing_mode == "jra_lock1_rerank_rest":
                rerank_variant = getattr(variant_spec, "rerank_variant", None)
                if (
                    category != "jra"
                    or rerank_variant is None
                    or rerank_variant not in variant_pool
                ):
                    debug_log(
                        f"[cell-routing] variant={variant_name} category={category} "
                        f"missing lock1 rerank_variant={rerank_variant} -> not loaded"
                    )
                    continue
                lock1_rerank_model = variant_pool[rerank_variant]
            consensus_members: tuple[VariantModel, ...] = ()
            if routing_mode == "nar_transformer_top2_consensus_swap":
                consensus_names = getattr(variant_spec, "consensus_variants", ())
                if category != "nar" or any(name not in variant_pool for name in consensus_names):
                    debug_log(
                        f"[cell-routing] variant={variant_name} category={category} "
                        f"missing consensus_variants={consensus_names} -> not loaded"
                    )
                    continue
                consensus_members = tuple(variant_pool[name] for name in consensus_names)
            variant_pool[variant_name] = VariantModel(
                booster=booster,
                feature_names=variant_feature_names,
                architecture=architecture,
                model_version=variant_spec.model_version,
                routing_mode=routing_mode,
                top1_swap_base=top1_swap_base,
                minimum_candidate_margin=getattr(variant_spec, "minimum_candidate_margin", None),
                minimum_candidate_top_z=getattr(variant_spec, "minimum_candidate_top_z", None),
                maximum_candidate_v2_rank=getattr(variant_spec, "maximum_candidate_v2_rank", None),
                consensus_members=consensus_members,
                consensus_required_votes=getattr(variant_spec, "consensus_required_votes", None),
                lock1_rerank_model=lock1_rerank_model,
            )
            debug_log(
                f"[cell-routing] loaded variant={variant_name} category={category} "
                f"version={variant_spec.model_version} features={variant_spec.feature_count}"
            )
    xgb_etop2_booster = (
        _load_xgb_etop2_booster(models_dir) if JRA_ETOP2_ENABLED and category == "jra" else None
    )
    jra_dirt_hybrid = (
        _load_jra_dirt_hybrid(models_dir, feature_names)
        if JRA_DIRT_HYBRID_ENABLED and category == "jra"
        else None
    )
    nar_transformer = (
        _load_nar_transformer(models_dir, feature_names)
        if NAR_TRANSFORMER_BLEND_ENABLED and category == "nar"
        else None
    )
    stage1_config = load_stage1_routing().get(category)
    stage1_model = (
        _load_stage1_model(models_dir, category, stage1_config)
        if stage1_config is not None and stage1_config.enabled
        else None
    )
    dynamic_market_shadow = _load_dynamic_market_shadow_models(models_dir, category)
    return ModelBundle(
        cell_router=cell_router,
        dynamic_market_shadow=dynamic_market_shadow,
        fallback_booster=fallback_booster,
        feature_names=feature_names,
        jra_dirt_hybrid=jra_dirt_hybrid,
        nar_transformer=nar_transformer,
        stage1_config=stage1_config,
        stage1_model=stage1_model,
        variant_pool=MappingProxyType(variant_pool),
        xgb_etop2_booster=xgb_etop2_booster,
    )


def _get_model_bundle(
    models_dir: Path,
    category: Category,
    feature_names_override: Sequence[str] | None = None,
) -> ModelBundle:
    """Return a thread-safe process-local model bundle for this artifact identity."""
    key = _model_bundle_cache_key(models_dir, category, feature_names_override)
    with _MODEL_BUNDLE_CACHE_LOCK:
        cached = _MODEL_BUNDLE_CACHE.get(key)
        if cached is not None:
            return cached
        loaded = _load_model_bundle(models_dir, category, feature_names_override)
        _MODEL_BUNDLE_CACHE[key] = loaded
        return loaded


def clear_model_bundle_cache() -> None:
    """Clear process-local bundles for deterministic tests and controlled reloads."""
    with _MODEL_BUNDLE_CACHE_LOCK:
        _MODEL_BUNDLE_CACHE.clear()


def score_races(
    races: Mapping[str, Sequence[Mapping[str, object]]],
    category: Category,
    models_dir: Path,
    feature_names: Sequence[str] | None = None,
    card_max_race_bango: int | None = None,
    race_names_by_race_id: Mapping[str, Mapping[str, object]] | None = None,
) -> list[list[list[object]]]:
    """Score every race in ``races`` into per-race prediction rows.

    Loads the category default booster and any non-default cell variants once,
    then routes each race through data-driven cell routing first. A matching
    cell variant is scored directly. Otherwise the request falls back to the
    category default model, except for the explicitly enabled JRA E-top2
    override path.
    Connection-free and CPU-bound so the caller can defer the Neon connect until
    the first write (avoiding Neon autosuspend during the long score phase).

    After a race's normal (Stage-2) rows are computed, ``predict_lib.
    stage1_routing.resolve_stage1_gate`` gets one more say per race when
    ``stage1_routing.json`` configures a fallback for this category: an
    odds-serving incident (freshness gate fails) or a collapsed within-race
    score spread (stddev safety net trips) re-scores that one race with the
    Stage-1 market-free fallback booster instead, overriding the Stage-2 rows.
    A successful dedicated named-race cell (catalog variant such as
    ``niigata_bsn`` / ``sapporo_suzuran``, or ``jra-named-*`` model_version)
    is exempt: Stage-1 must not clobber it. Uncatalogued open-class races
    that stay on pooled ``joken_999`` remain eligible for the gate. A
    category absent from ``stage1_routing.json`` (or a fallback artifact
    that fails to load) is a pure no-op -- unchanged Stage-2-only behaviour.

    ``card_max_race_bango`` feeds the ``is_final_race`` cell-routing dimension
    (see ``cell_router.resolve_dimension``). Two sourcing modes, chosen per
    caller shape (see ``tmp/kochi-final/cell_design.md`` section 3.2):
    - Whole-category requests (``mode=full`` / whole-category ``mode=rescore``,
      no single-race scope) pass ``None`` here; ``races`` already contains
      every race on every card it touches, so this function derives each
      card's registered-max race_bango for free from that same batch
      (``cell_router.derive_card_max_race_bango_by_card``) -- zero extra I/O,
      guaranteed consistent with what is actually being scored.
    - Single-race-scoped requests (per-race rescore, focused-full-race) pass
      an explicit value sourced by the HTTP caller from discovery, because a
      lone race in ``races`` cannot self-derive its card's size (it would
      trivially compute itself as the only, hence "final", race). This
      explicit value always wins over batch derivation when supplied.

    ``race_names_by_race_id`` overlays ``kyosomei_*`` onto entries that lack
    them. New feature parquets emit sidecar race-name columns; older caches
    do not. Overlay fills blanks immediately before
    ``cell_router.resolve_variant``. Named cells such as ``niigata_bsn`` /
    ``sapporo_suzuran`` cannot be resolved from venue+class alone.
    """
    bundle = _get_model_bundle(models_dir, category, feature_names)
    fallback_booster = bundle.fallback_booster
    cell_router = bundle.cell_router
    resolved_feature_names = bundle.feature_names
    variant_pool = bundle.variant_pool
    xgb_etop2_booster = bundle.xgb_etop2_booster
    jra_dirt_hybrid = bundle.jra_dirt_hybrid
    nar_transformer = bundle.nar_transformer
    stage1_config = bundle.stage1_config
    stage1_model = bundle.stage1_model
    # See this function's docstring: an explicit caller-supplied value always
    # wins; batch self-derivation only ever runs (and is only ever correct)
    # when none was supplied, i.e. this is a whole-category request whose
    # ``races`` already spans every card it touches.
    card_max_race_bango_by_card = (
        derive_card_max_race_bango_by_card(races.keys())
        if variant_pool and card_max_race_bango is None
        else {}
    )
    named_race_variants: frozenset[str] = frozenset()
    if variant_pool and cell_router.has_routing(category):
        named_race_index = getattr(cell_router.routing_for(category), "named_race_index", {})
        named_race_variants = frozenset(cell.variant for cell in named_race_index.values())
    scored: list[list[list[object]]] = []
    routable_races = overlay_race_names_on_races(races, race_names_by_race_id)
    for race_id, entries in routable_races.items():
        effective_booster = fallback_booster
        effective_feature_names = resolved_feature_names
        effective_architecture = architecture_for(category)
        cell_variant_model: VariantModel | None = None
        resolved_variant: str | None = None
        if variant_pool:
            effective_card_max_race_bango = (
                card_max_race_bango
                if card_max_race_bango is not None
                else card_max_race_bango_for_race_id(race_id, card_max_race_bango_by_card)
            )
            variant = cell_router.resolve_variant(
                category, entries, card_max_race_bango=effective_card_max_race_bango
            )
            resolved_variant = variant
            if variant in variant_pool:
                vm = variant_pool[variant]
                effective_booster = vm.booster
                effective_feature_names = vm.feature_names
                effective_architecture = vm.architecture
                cell_variant_model = vm
                debug_log(f"[cell-routing] race={race_id} category={category} -> {variant}")
            elif variant != cell_router.routing_for(category).default_variant:
                debug_log(
                    f"[cell-routing] race={race_id} category={category} "
                    f"resolved missing variant={variant}; using default"
                )
        if cell_variant_model is not None:
            if category == "jra" and cell_variant_model.routing_mode == "jra_variant_top1_swap":
                rows = _score_one_race_variant_top1_swap(
                    cell_variant_model,
                    race_id,
                    category,
                    entries,
                )
            elif category == "jra" and cell_variant_model.routing_mode == "jra_lock1_rerank_rest":
                rows = _score_one_race_lock1_rerank_rest(
                    cell_variant_model,
                    race_id,
                    category,
                    entries,
                )
            elif (
                category == "nar" and cell_variant_model.routing_mode == "nar_transformer_top1_swap"
            ):
                rows = _score_one_race_nar_top1_swap(
                    fallback_booster,
                    nar_transformer,
                    cell_variant_model,
                    race_id,
                    entries,
                    resolved_feature_names,
                )
            elif (
                category == "nar"
                and cell_variant_model.routing_mode == "nar_transformer_top2_consensus_swap"
            ):
                rows = _score_one_race_nar_top2_consensus_swap(
                    fallback_booster,
                    nar_transformer,
                    cell_variant_model,
                    race_id,
                    entries,
                    resolved_feature_names,
                )
            elif (
                category == "nar" and cell_variant_model.routing_mode == "nar_transformer_top2_swap"
            ):
                rows = _score_one_race_nar_top2_swap(
                    fallback_booster,
                    nar_transformer,
                    cell_variant_model,
                    race_id,
                    entries,
                    resolved_feature_names,
                )
            elif (
                category == "jra"
                and resolved_variant == "prior_corner_dirt_smallfield_005"
                and jra_dirt_hybrid is not None
            ):
                rows = _score_one_race_jra_dirt_hybrid(
                    effective_booster,
                    jra_dirt_hybrid,
                    race_id,
                    entries,
                    effective_feature_names,
                    effective_architecture,
                    cell_variant_model.model_version,
                )
            else:
                rows = _score_one_race_direct(
                    effective_booster,
                    race_id,
                    category,
                    entries,
                    effective_feature_names,
                    effective_architecture,
                    cell_variant_model.model_version,
                )
        elif xgb_etop2_booster is not None:
            rows = _score_one_race_etop2(
                effective_booster,
                xgb_etop2_booster,
                race_id,
                entries,
                effective_feature_names,
            )
        elif nar_transformer is not None:
            rows = _score_one_race_nar_blend(
                effective_booster,
                nar_transformer,
                race_id,
                entries,
                effective_feature_names,
            )
        else:
            rows = _score_one_race_direct(
                effective_booster,
                race_id,
                category,
                entries,
                effective_feature_names,
                effective_architecture,
                model_version_for(category),
            )
        if stage1_model is not None and stage1_config is not None and rows:
            scored_named_cell = cell_variant_model is not None and is_named_race_cell_score(
                model_version=cell_variant_model.model_version,
                resolved_variant=resolved_variant,
                named_race_variants=named_race_variants,
            )
            gate = resolve_stage1_gate(
                config=stage1_config,
                entries=entries,
                stage2_scores=extract_predicted_scores(rows),
                skip_named_race_cell=scored_named_cell,
            )
            if gate.reason == "named-race-cell" and cell_variant_model is not None:
                debug_log(
                    f"[stage1-gate] race={race_id} category={category} "
                    f"reason={gate.reason} keep={cell_variant_model.model_version}"
                )
            if gate.use_stage1:
                debug_log(
                    f"[stage1-gate] race={race_id} category={category} "
                    f"reason={gate.reason} stddev={gate.stddev} -> {stage1_model.model_version}"
                )
                rows = (
                    _score_one_race_direct(
                        stage1_model.booster,
                        race_id,
                        category,
                        entries,
                        stage1_model.feature_names,
                        stage1_model.architecture,
                        stage1_model.model_version,
                    )
                    if stage1_model.top1_swap_base is None
                    else _score_one_race_stage1_top1_swap(
                        stage1_model,
                        stage1_config,
                        race_id,
                        category,
                        entries,
                    )
                )
        scored.append(rows)
    return scored


def score_dynamic_market_shadow(
    races: Mapping[str, Sequence[Mapping[str, object]]],
    scored: Sequence[Sequence[Sequence[object]]],
    category: Category,
    models_dir: Path,
) -> list[DynamicMarketShadowRecord]:
    """Score JRA loop-43 counterfactuals without changing any served row."""
    bundle = _get_model_bundle(models_dir, category)
    shadow = bundle.dynamic_market_shadow
    stage1_model = bundle.stage1_model
    if category != "jra" or shadow is None or stage1_model is None:
        return []
    served_by_race = dict(zip(races, scored, strict=True))
    records: list[DynamicMarketShadowRecord] = []
    for race_id, entries in races.items():
        try:
            served_baseline = served_baseline_from_rows(served_by_race[race_id])
            if served_baseline is None:
                continue
            representative = _representative_entry(entries)
            if representative is None:
                continue
            raw_track_code = representative.get("track_code")
            surface = classify_surface(None if raw_track_code is None else str(raw_track_code))
            if surface is None:
                continue
            market_version = surface_expert_version(surface, market_free=False)
            market_free_version = surface_expert_version(surface, market_free=True)
            market_expert = shadow.experts[market_version]
            market_free_expert = shadow.experts[market_free_version]
            required_contracts = (
                bundle.feature_names,
                stage1_model.feature_names,
                market_expert.feature_names,
                market_free_expert.feature_names,
                shadow.joint_alternate.feature_names,
                shadow.joint_champion.feature_names,
                shadow.joint_specialist.feature_names,
            )
            if any(is_degenerate_feature_matrix(entries, names) for names in required_contracts):
                debug_log(f"[dynamic-market-shadow] race={race_id} skipped: degenerate features")
                continue
            champion_scores = score_matrix(
                bundle.fallback_booster,
                build_feature_matrix(entries, bundle.feature_names, "catboost"),
            )
            market_free_scores = score_matrix(
                stage1_model.booster,
                build_feature_matrix(
                    entries, stage1_model.feature_names, stage1_model.architecture
                ),
            )
            surface_market_scores = score_matrix(
                market_expert.booster,
                build_feature_matrix(entries, market_expert.feature_names, "catboost"),
            )
            surface_market_free_scores = score_matrix(
                market_free_expert.booster,
                build_feature_matrix(entries, market_free_expert.feature_names, "catboost"),
            )
            race_records = build_shadow_records(
                race_id=race_id,
                entries=entries,
                champion_scores=champion_scores,
                market_free_scores=market_free_scores,
                surface_market_scores=surface_market_scores,
                surface_market_free_scores=surface_market_free_scores,
                classifier=shadow.classifier,
                classifier_model_version=shadow.classifier_version,
                market_expert_version=market_version,
                market_free_expert_version=market_free_version,
                served_baseline=served_baseline,
            )
            records.extend(race_records)
            if race_records:
                joint_champion_scores = score_matrix(
                    shadow.joint_champion.booster,
                    build_feature_matrix(
                        entries,
                        shadow.joint_champion.feature_names,
                        shadow.joint_champion.architecture,
                    ),
                )
                joint_specialist_scores = score_matrix(
                    shadow.joint_specialist.booster,
                    build_feature_matrix(
                        entries,
                        shadow.joint_specialist.feature_names,
                        shadow.joint_specialist.architecture,
                    ),
                )
                candidate = shadow.joint_alternate.select_candidate(
                    entries,
                    joint_champion_scores,
                    joint_specialist_scores,
                    served_baseline.top5,
                )
                records.append(
                    build_joint_additional_candidate_record(
                        race_id=race_id,
                        entries=entries,
                        candidate=candidate,
                        specialist_model_version=shadow.joint_specialist.model_version,
                        champion_model_version=shadow.joint_champion.model_version,
                        served_baseline=served_baseline,
                    )
                )
        except BaseException as shadow_error:
            debug_log(f"[dynamic-market-shadow] race={race_id} failed closed: {shadow_error}")
    return records


def _score_one_race_direct(
    booster: BoosterLike,
    race_id: str,
    category: Category,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    architecture: Architecture,
    model_version: str,
) -> list[list[object]]:
    if is_degenerate_feature_matrix(entries, feature_names):
        debug_log(
            f"[feature-guard] race_id={race_id} category={category} "
            f"model_version={model_version} rejected: feature matrix mostly missing "
            "-> skipping write (self-heal will retry)"
        )
        return []
    matrix = build_feature_matrix(entries, feature_names, architecture)
    scores = score_matrix(booster, matrix)
    ranked = rank_race_entries(entries, scores)
    return build_prediction_rows(
        race_id,
        category,
        ranked,
        model_version,
        _representative_entry(entries),
        entries=entries,
    )


def _score_one_race_lock1_rerank_rest(
    primary: VariantModel,
    race_id: str,
    category: Category,
    entries: Sequence[Mapping[str, object]],
) -> list[list[object]]:
    """Lock the primary winner, then re-rank remaining horses by the rest model."""
    rest = primary.lock1_rerank_model
    if rest is None:
        raise ValueError("Cell variant lock1 rerank requires a rest model")
    if is_degenerate_feature_matrix(entries, primary.feature_names) or is_degenerate_feature_matrix(
        entries, rest.feature_names
    ):
        debug_log(
            f"[feature-guard] race_id={race_id} category={category} "
            f"model_version={primary.model_version} rejected: feature matrix mostly missing "
            "-> skipping write (self-heal will retry)"
        )
        return []
    lock_scores = score_matrix(
        primary.booster,
        build_feature_matrix(entries, primary.feature_names, primary.architecture),
    )
    rest_scores = score_matrix(
        rest.booster,
        build_feature_matrix(entries, rest.feature_names, rest.architecture),
    )
    horse_ids = [str(entry["ketto_toroku_bango"]) for entry in entries]
    adjusted_scores = apply_lock1_rerank_rest(horse_ids, lock_scores, rest_scores)
    ranked = rank_race_entries(entries, adjusted_scores)
    return build_prediction_rows(
        race_id,
        category,
        ranked,
        primary.model_version,
        _representative_entry(entries),
        entries=entries,
    )


def _score_one_race_variant_top1_swap(
    companion: VariantModel,
    race_id: str,
    category: Category,
    entries: Sequence[Mapping[str, object]],
) -> list[list[object]]:
    """Score a routed base + companion and exchange only their top horses."""
    base = companion.top1_swap_base
    if base is None:
        raise ValueError("Cell variant top1 swap requires a base model")
    if is_degenerate_feature_matrix(entries, base.feature_names) or is_degenerate_feature_matrix(
        entries, companion.feature_names
    ):
        debug_log(
            f"[feature-guard] race_id={race_id} category={category} "
            f"model_version={companion.model_version} rejected: feature matrix mostly missing "
            "-> skipping write (self-heal will retry)"
        )
        return []
    base_scores = score_matrix(
        base.booster,
        build_feature_matrix(entries, base.feature_names, base.architecture),
    )
    companion_scores = score_matrix(
        companion.booster,
        build_feature_matrix(entries, companion.feature_names, companion.architecture),
    )
    horse_ids = [str(entry["ketto_toroku_bango"]) for entry in entries]
    should_swap = _passes_variant_top1_confidence_gate(companion, base_scores, companion_scores)
    adjusted_scores = (
        apply_top1_score_swap(horse_ids, base_scores, companion_scores)
        if should_swap
        else list(base_scores)
    )
    ranked = rank_race_entries(entries, adjusted_scores)
    return build_prediction_rows(
        race_id,
        category,
        ranked,
        companion.model_version,
        _representative_entry(entries),
        entries=entries,
    )


def _passes_variant_top1_confidence_gate(
    companion: VariantModel,
    base_scores: Sequence[float],
    companion_scores: Sequence[float],
) -> bool:
    """Fail closed unless optional companion confidence thresholds are met."""
    minimum_margin = companion.minimum_candidate_margin
    minimum_top_z = companion.minimum_candidate_top_z
    maximum_v2_rank = companion.maximum_candidate_v2_rank
    if minimum_margin is None and minimum_top_z is None and maximum_v2_rank is None:
        return True
    if (
        len(companion_scores) < 2
        or len(base_scores) != len(companion_scores)
        or any(not math.isfinite(score) for score in companion_scores)
        or any(not math.isfinite(score) for score in base_scores)
    ):
        return False
    descending = sorted(companion_scores, reverse=True)
    margin = descending[0] - descending[1]
    mean = sum(companion_scores) / len(companion_scores)
    variance = sum((score - mean) ** 2 for score in companion_scores) / (len(companion_scores) - 1)
    standard_deviation = math.sqrt(variance)
    if not math.isfinite(standard_deviation) or standard_deviation <= 0.0:
        return False
    top_z = (descending[0] - mean) / standard_deviation
    companion_top_index = max(range(len(companion_scores)), key=companion_scores.__getitem__)
    base_order = sorted(range(len(base_scores)), key=base_scores.__getitem__, reverse=True)
    candidate_v2_rank = base_order.index(companion_top_index) + 1
    return (
        (minimum_margin is None or margin >= minimum_margin)
        and (minimum_top_z is None or top_z >= minimum_top_z)
        and (maximum_v2_rank is None or candidate_v2_rank <= maximum_v2_rank)
    )


def _score_one_race_stage1_top1_swap(
    companion: VariantModel,
    config: Stage1CategoryConfig,
    race_id: str,
    category: Category,
    entries: Sequence[Mapping[str, object]],
) -> list[list[object]]:
    """Score Stage-1 base + companion and exchange only their top horses."""
    base = companion.top1_swap_base
    if base is None:
        raise ValueError("Stage-1 top1 swap requires a base model")
    if not race_passes_top1_swap_weather_gate(config, entries):
        debug_log(
            f"[stage1-top1-swap] race={race_id} category={category} "
            f"weather gate closed -> {base.model_version}"
        )
        return _score_one_race_direct(
            base.booster,
            race_id,
            category,
            entries,
            base.feature_names,
            base.architecture,
            base.model_version,
        )
    if is_degenerate_feature_matrix(entries, base.feature_names) or is_degenerate_feature_matrix(
        entries, companion.feature_names
    ):
        debug_log(
            f"[feature-guard] race_id={race_id} category={category} "
            f"model_version={companion.model_version} rejected: feature matrix mostly missing "
            "-> skipping write (self-heal will retry)"
        )
        return []
    base_scores = score_matrix(
        base.booster,
        build_feature_matrix(entries, base.feature_names, base.architecture),
    )
    companion_scores = score_matrix(
        companion.booster,
        build_feature_matrix(entries, companion.feature_names, companion.architecture),
    )
    horse_ids = [str(entry["ketto_toroku_bango"]) for entry in entries]
    adjusted_scores = apply_top1_score_swap(horse_ids, base_scores, companion_scores)
    ranked = rank_race_entries(entries, adjusted_scores)
    return build_prediction_rows(
        race_id,
        category,
        ranked,
        companion.model_version,
        _representative_entry(entries),
        entries=entries,
    )


def _load_jra_dirt_hybrid(models_dir: Path, feature_names: Sequence[str]) -> JraHybridScorer | None:
    """Load the JRA dirt-small-005 artifact, failing closed to prior-corner only."""
    artifact_dir = (models_dir / build_r2_jra_dirt_hybrid_key("manifest.json")).parent
    try:
        scorer = JraHybridScorer.load(artifact_dir)
        assert_no_within_race_leak_columns(
            scorer.feature_order,
            context=f"JRA hybrid model_version={JRA_DIRT_HYBRID_MODEL_VERSION}",
        )
    except BaseException as load_error:
        debug_log(f"[jra-dirt-hybrid] load failed -> prior-corner-only: {load_error}")
        return None
    missing = [name for name in scorer.feature_order if name not in set(feature_names)]
    if missing:
        debug_log(
            f"[jra-dirt-hybrid] feature-contract gap ({len(missing)}) "
            f"-> prior-corner-only: {missing[:5]}"
        )
        return None
    debug_log(
        f"[jra-dirt-hybrid] loaded seeds={len(scorer.seeds)} "
        f"features={len(scorer.feature_order)} version={JRA_DIRT_HYBRID_MODEL_VERSION}"
    )
    return scorer


def _load_nar_transformer(
    models_dir: Path, feature_names: Sequence[str]
) -> TransformerScorer | None:
    """Load the NAR transformer-blend artifact, or None for ensemble-only fallback.

    Fail-closed at startup (mirrors the E-top2 companion-load pattern): a missing
    / unreadable artifact OR a feature-contract gap (any of the 113 transformer
    features absent from the category's clean188-feature production build) returns
    None so ``score_races`` scores NAR with the pure iter12 ensemble unchanged.
    Loaded once per category run from the baked MODELS_DIR (no runtime R2 read).
    """
    artifact_dir = (models_dir / build_r2_nar_transformer_key("norm.json")).parent
    try:
        transformer = load_transformer(artifact_dir)
    except BaseException as load_error:
        debug_log(f"[nar-transformer] load failed -> ensemble-only: {load_error}")
        return None
    known = set(feature_names)
    try:
        assert_no_within_race_leak_columns(
            transformer.feature_order,
            context=f"NAR transformer model_version={NAR_TRANSFORMER_MODEL_VERSION}",
        )
    except ValueError as leak_error:
        debug_log(f"[nar-transformer] leak guard failed -> ensemble-only: {leak_error}")
        return None
    missing = [name for name in transformer.feature_order if name not in known]
    if missing:
        debug_log(
            f"[nar-transformer] feature-contract gap ({len(missing)}) "
            f"-> ensemble-only: {missing[:5]}"
        )
        return None
    debug_log(
        f"[nar-transformer] loaded seeds={len(transformer.seeds)} "
        f"features={len(transformer.feature_order)} "
        f"version={NAR_TRANSFORMER_MODEL_VERSION}"
    )
    return transformer


def _score_one_race_jra_dirt_hybrid(
    booster: BoosterLike,
    hybrid: JraHybridScorer,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
    architecture: Architecture,
    fallback_model_version: str,
) -> list[list[object]]:
    """Fuse the prior-corner cell model with the weighted three-seed companion."""
    if is_degenerate_feature_matrix(entries, feature_names):
        debug_log(
            f"[feature-guard] race_id={race_id} category=jra "
            "rejected: feature matrix mostly missing -> skipping write "
            "(self-heal will retry)"
        )
        return []
    base_scores = score_matrix(booster, build_feature_matrix(entries, feature_names, architecture))
    scores: Sequence[float] = base_scores
    model_version = fallback_model_version
    if len(entries) >= 2 and not hybrid.missing_feature_keys(entries):
        try:
            companion_scores = hybrid.companion_scores(entries)
            scores = fuse_jra_hybrid_scores(
                base_scores,
                companion_scores,
                companion_weight=hybrid.companion_weight,
            )
            model_version = JRA_DIRT_HYBRID_MODEL_VERSION
        except BaseException as blend_error:
            debug_log(
                f"[jra-dirt-hybrid] race fail -> prior-corner-only race_id={race_id}: {blend_error}"
            )
    ranked = rank_race_entries(entries, scores)
    return build_prediction_rows(
        race_id,
        "jra",
        ranked,
        model_version,
        _representative_entry(entries),
        entries=entries,
    )


def _score_one_race_nar_top1_swap(
    fallback_booster: BoosterLike,
    transformer: TransformerScorer | None,
    companion: VariantModel,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> list[list[object]]:
    """Swap only production NAR top-1 with a routed specialist's top horse."""
    if is_degenerate_feature_matrix(entries, feature_names) or is_degenerate_feature_matrix(
        entries, companion.feature_names
    ):
        debug_log(
            f"[feature-guard] race_id={race_id} category=nar "
            f"model_version={companion.model_version} rejected: feature matrix mostly missing "
            "-> skipping write (self-heal will retry)"
        )
        return []
    base_scores = score_matrix(
        fallback_booster,
        build_feature_matrix(entries, feature_names, "xgboost"),
    )
    production_scores: Sequence[float] = base_scores
    if (
        transformer is not None
        and len(entries) >= 2
        and not transformer.missing_feature_keys(entries)
    ):
        try:
            production_scores = fuse_ensemble_transformer(
                base_scores,
                transformer.seed_score_mean(entries),
                NAR_TRANSFORMER_BLEND_WEIGHT,
            )
        except BaseException as blend_error:
            debug_log(
                f"[nar-top1-routing] transformer fail -> ensemble-only "
                f"race_id={race_id}: {blend_error}"
            )
    companion_scores = score_matrix(
        companion.booster,
        build_feature_matrix(entries, companion.feature_names, companion.architecture),
    )
    horse_ids = [str(entry["ketto_toroku_bango"]) for entry in entries]
    adjusted_scores = apply_top1_score_swap(horse_ids, production_scores, companion_scores)
    ranked = rank_race_entries(entries, adjusted_scores)
    return build_prediction_rows(
        race_id,
        "nar",
        ranked,
        companion.model_version,
        _representative_entry(entries),
        entries=entries,
    )


def _score_one_race_nar_top2_swap(
    fallback_booster: BoosterLike,
    transformer: TransformerScorer | None,
    companion: VariantModel,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> list[list[object]]:
    """Conditionally swap production NAR ranks two and three using a Top2 head."""
    if is_degenerate_feature_matrix(entries, feature_names) or is_degenerate_feature_matrix(
        entries, companion.feature_names
    ):
        debug_log(
            f"[feature-guard] race_id={race_id} category=nar "
            f"model_version={companion.model_version} rejected: feature matrix mostly missing "
            "-> skipping write (self-heal will retry)"
        )
        return []
    base_scores = score_matrix(
        fallback_booster,
        build_feature_matrix(entries, feature_names, "xgboost"),
    )
    production_scores: Sequence[float] = base_scores
    if (
        transformer is not None
        and len(entries) >= 2
        and not transformer.missing_feature_keys(entries)
    ):
        try:
            production_scores = fuse_ensemble_transformer(
                base_scores,
                transformer.seed_score_mean(entries),
                NAR_TRANSFORMER_BLEND_WEIGHT,
            )
        except BaseException as blend_error:
            debug_log(
                f"[nar-top2-routing] transformer fail -> ensemble-only "
                f"race_id={race_id}: {blend_error}"
            )
    companion_scores = score_matrix(
        companion.booster,
        build_feature_matrix(entries, companion.feature_names, companion.architecture),
    )
    horse_ids = [str(entry["ketto_toroku_bango"]) for entry in entries]
    production_order = sorted(
        range(len(entries)), key=lambda index: (-float(production_scores[index]), horse_ids[index])
    )
    adjusted_scores = list(production_scores)
    if len(production_order) >= 3:
        rank2_index, rank3_index = production_order[1:3]
        minimum_margin = companion.minimum_candidate_margin
        threshold = 0.0 if minimum_margin is None else minimum_margin
        if float(companion_scores[rank3_index]) > float(companion_scores[rank2_index]) + threshold:
            adjusted_scores[rank2_index], adjusted_scores[rank3_index] = (
                adjusted_scores[rank3_index],
                adjusted_scores[rank2_index],
            )
    ranked = rank_race_entries(entries, adjusted_scores)
    return build_prediction_rows(
        race_id,
        "nar",
        ranked,
        companion.model_version,
        _representative_entry(entries),
        entries=entries,
    )


def _score_one_race_nar_top2_consensus_swap(
    fallback_booster: BoosterLike,
    transformer: TransformerScorer | None,
    companion: VariantModel,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> list[list[object]]:
    """Swap production ranks two and three only when enough panel heads agree."""
    members = (companion, *companion.consensus_members)
    if is_degenerate_feature_matrix(entries, feature_names) or any(
        is_degenerate_feature_matrix(entries, member.feature_names) for member in members
    ):
        debug_log(
            f"[feature-guard] race_id={race_id} category=nar "
            f"model_version={companion.model_version} rejected: feature matrix mostly missing "
            "-> skipping write (self-heal will retry)"
        )
        return []
    base_scores = score_matrix(
        fallback_booster,
        build_feature_matrix(entries, feature_names, "xgboost"),
    )
    production_scores: Sequence[float] = base_scores
    if (
        transformer is not None
        and len(entries) >= 2
        and not transformer.missing_feature_keys(entries)
    ):
        try:
            production_scores = fuse_ensemble_transformer(
                base_scores,
                transformer.seed_score_mean(entries),
                NAR_TRANSFORMER_BLEND_WEIGHT,
            )
        except BaseException as blend_error:
            debug_log(
                f"[nar-top2-consensus-routing] transformer fail -> ensemble-only "
                f"race_id={race_id}: {blend_error}"
            )
    horse_ids = [str(entry["ketto_toroku_bango"]) for entry in entries]
    production_order = sorted(
        range(len(entries)), key=lambda index: (-float(production_scores[index]), horse_ids[index])
    )
    adjusted_scores = list(production_scores)
    if len(production_order) >= 3:
        rank2_index, rank3_index = production_order[1:3]
        votes = 0
        for member in members:
            scores = score_matrix(
                member.booster,
                build_feature_matrix(entries, member.feature_names, member.architecture),
            )
            if float(scores[rank3_index]) > float(scores[rank2_index]):
                votes += 1
        required_votes = companion.consensus_required_votes
        threshold = len(members) if required_votes is None else required_votes
        if votes >= threshold:
            adjusted_scores[rank2_index], adjusted_scores[rank3_index] = (
                adjusted_scores[rank3_index],
                adjusted_scores[rank2_index],
            )
    ranked = rank_race_entries(entries, adjusted_scores)
    return build_prediction_rows(
        race_id,
        "nar",
        ranked,
        companion.model_version,
        _representative_entry(entries),
        entries=entries,
    )


def _score_one_race_nar_blend(
    fallback_booster: BoosterLike,
    transformer: TransformerScorer,
    race_id: str,
    entries: Sequence[Mapping[str, object]],
    feature_names: Sequence[str],
) -> list[list[object]]:
    """Score one NAR race with the Set-Transformer x base score-level z-fusion blend.

    The pure clean188 XGBoost base scores the category matrix; the transformer
    contributes its mean seed score over the clean 113-feature subset. The two are
    within-race z-normalised (scale-invariant) then fused 0.5/0.5 at the score
    level, reproducing the deploy gate (deploy variant ``score_z_55``); the
    caller's ``rank_race_entries`` applies the ketto-ascending tie-break on the
    fused scores. Fail-closed per race: field < 2, a feature gap on this race's
    entries, or any transformer exception falls back to the pure base ranking
    under the category-global model_version (auditable); blended rows write
    NAR_TRANSFORMER_MODEL_VERSION.
    """
    if is_degenerate_feature_matrix(entries, feature_names):
        debug_log(
            f"[feature-guard] race_id={race_id} category=nar "
            "rejected: feature matrix mostly missing -> skipping write "
            "(self-heal will retry)"
        )
        return []
    matrix = build_feature_matrix(entries, feature_names, "xgboost")
    base_scores = score_matrix(fallback_booster, matrix)
    scores: Sequence[float] = base_scores
    model_version = model_version_for("nar")
    if len(entries) >= 2 and not transformer.missing_feature_keys(entries):
        try:
            transformer_score_mean = transformer.seed_score_mean(entries)
            scores = fuse_ensemble_transformer(
                base_scores, transformer_score_mean, NAR_TRANSFORMER_BLEND_WEIGHT
            )
            model_version = NAR_TRANSFORMER_MODEL_VERSION
        except BaseException as blend_error:
            debug_log(
                f"[nar-transformer] race fail -> ensemble-only race_id={race_id}: {blend_error}"
            )
            scores = base_scores
            model_version = model_version_for("nar")
    ranked = rank_race_entries(entries, scores)
    return build_prediction_rows(
        race_id,
        "nar",
        ranked,
        model_version,
        _representative_entry(entries),
        entries=entries,
    )


def _flush_scored(
    database_url: str,
    category: Category,
    scored: Sequence[Sequence[Sequence[object]]],
    race_start_at_jst: str | None = None,
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
            if race_start_at_jst is not None:
                _assert_before_race_start(race_start_at_jst)
            rows_written, connection = flush_predictions(connection, rows, database_url)
            written += rows_written
    finally:
        try:
            connection.close()
        except BaseException as close_error:
            debug_log(
                f"[predict-upcoming] connection close failed category={category}: {close_error}"
            )
    return written


def _flush_dynamic_market_shadow(
    database_url: str,
    records: Sequence[DynamicMarketShadowRecord],
) -> int:
    """Best-effort shadow persistence that can never fail served predictions."""
    if not records:
        return 0
    connection: ConnectionLike | None = None
    try:
        connection = _connect(database_url)
        connection = execute(connection, build_shadow_table_ddl(), [], database_url)
        for migration_sql in build_shadow_migration_sql():
            connection = execute(connection, migration_sql, [], database_url)
        written = 0
        for start in range(0, len(records), SHADOW_BATCH_SIZE):
            batch = records[start : start + SHADOW_BATCH_SIZE]
            connection = execute(
                connection,
                build_shadow_batch_upsert_sql(len(batch)),
                shadow_batch_params(batch),
                database_url,
            )
            written += len(batch)
        return written
    except BaseException as shadow_error:
        debug_log(f"[dynamic-market-shadow] persistence failed open: {shadow_error}")
        return 0
    finally:
        if connection is not None:
            try:
                connection.close()
            except BaseException as close_error:
                debug_log(f"[dynamic-market-shadow] connection close failed: {close_error}")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _assert_before_race_start(race_start_at_jst: str) -> None:
    """Reject scoring or persistence once the attested race start is reached."""
    try:
        race_start = datetime.fromisoformat(race_start_at_jst)
    except ValueError as exc:
        raise CacheValidationError("invalid raceStartAtJst deadline") from exc
    if race_start.tzinfo is None:
        raise CacheValidationError("invalid raceStartAtJst deadline")
    if _utc_now() >= race_start:
        raise CacheValidationError(
            f"post-weight prediction deadline reached: raceStartAtJst={race_start_at_jst}"
        )


def _race_ids_missing_race_name(
    races: Mapping[str, Sequence[Mapping[str, object]]],
) -> list[str]:
    """Return race ids whose first entry has no usable kyosomei field."""
    missing: list[str] = []
    for race_id, entries in races.items():
        if not entries:
            continue
        if not entry_has_race_name(entries[0]):
            missing.append(race_id)
    return missing


def _load_race_names_by_race_id(
    races: Mapping[str, Sequence[Mapping[str, object]]],
    source_url: str | None,
) -> dict[str, dict[str, object]]:
    """Load official race names from the feature catalog for parquet rows that lack them."""
    missing = _race_ids_missing_race_name(races)
    if not missing:
        return {}
    if source_url is None or source_url.strip() == "" or not is_catalog_source_url(source_url):
        debug_log(
            f"[cell-routing] race-name overlay skipped; source_url missing races={len(missing)}"
        )
        return {}
    from pipeline_runner import query_race_names

    names = query_race_names(source_url, missing)
    unresolved = [race_id for race_id in missing if race_id not in names]
    if unresolved:
        raise RuntimeError(
            "race-name overlay failed; named-cell routing cannot fail closed to sim "
            f"count={len(unresolved)} sample={unresolved[0]}"
        )
    return names


def _score_and_flush_races(
    database_url: str,
    category: Category,
    models_dir: Path,
    races: Mapping[str, Sequence[Mapping[str, object]]],
    card_max_race_bango: int | None = None,
    race_start_at_jst: str | None = None,
    source_url: str | None = None,
) -> int:
    """Score ``races`` then UPSERT to Neon; the shared core of full + rescore.

    The races map is supplied by the caller — built from the 21y Neon scan on
    the full path, or read from the R2 / local feature cache (with the 5
    late-binding columns refreshed) on the rescore path. ``card_max_race_bango``
    is forwarded to :func:`score_races` untouched -- see that function's
    docstring for the whole-category-vs-single-race sourcing split.
    """
    if race_start_at_jst is not None:
        _assert_before_race_start(race_start_at_jst)
    race_names_by_race_id = _load_race_names_by_race_id(races, source_url)
    scored = score_races(
        races,
        category,
        models_dir,
        None,
        card_max_race_bango=card_max_race_bango,
        race_names_by_race_id=race_names_by_race_id,
    )
    if race_start_at_jst is not None:
        _assert_before_race_start(race_start_at_jst)
        written = _flush_scored(
            database_url,
            category,
            scored,
            race_start_at_jst=race_start_at_jst,
        )
    else:
        written = _flush_scored(database_url, category, scored)
    try:
        shadow_records = score_dynamic_market_shadow(races, scored, category, models_dir)
    except BaseException as shadow_error:
        debug_log(f"[dynamic-market-shadow] scoring failed open: {shadow_error}")
        shadow_records = []
    shadow_written = _flush_dynamic_market_shadow(database_url, shadow_records)
    if shadow_written:
        debug_log(f"[dynamic-market-shadow] category={category} records_written={shadow_written}")
    return written


def predict_category(
    database_url: str,
    category: Category,
    models_dir: Path,
    window: PredictWindow,
    target_race: str | None = None,
    r2_config: R2Config | None = None,
    card_max_race_bango: int | None = None,
) -> int:
    # Score all races before opening the Neon write connection. The feature
    # build is the longest step (DuckDB base build + 14 layer scripts, typically
    # 2-5 min) and Neon autosuspends after ~60s of idle — connecting before the
    # build would cause AdminShutdown on the first UPSERT. Scoring is CPU-bound
    # and connection-free, so we defer the Neon connect until the first write.
    # ``target_race`` ("keibajo:bango") restricts the DuckDB build to one race so
    # the Container can generate features per race instead of scanning the day.
    # ``r2_config`` is forwarded so the day-base/RACE_CHAIN split path (when
    # enabled for this category) can fetch a prewarmed day-base from R2 --
    # unused by the unmodified full-pipeline fallback. ``card_max_race_bango``
    # is forwarded to :func:`_score_and_flush_races` untouched.
    # Even when the builder is asked for one race, the on-disk parquet / groupby
    # can still contain extra races (day-base leftover, RACE_CHAIN passthrough).
    # Filter before score + UPSERT so a scoped run never writes other races.
    races = _build_feature_rows(category, window, target_race=target_race, r2_config=r2_config)
    if target_race is not None:
        races = filter_races_by_scope(
            _as_entry_map(races), race_scope_from_target_race(target_race)
        )
        active_entry_count = sum(len(entries) for entries in races.values())
        if active_entry_count == 0:
            raise RuntimeError(
                f"focused prediction produced zero active feature rows category={category} "
                f"target_date={window.target_date} target_race={target_race}"
            )
    written = _score_and_flush_races(
        database_url,
        category,
        models_dir,
        races,
        card_max_race_bango=card_max_race_bango,
        source_url=window.database_url,
    )
    if target_race is not None and written == 0:
        raise RuntimeError(
            f"focused prediction wrote zero rows category={category} "
            f"target_date={window.target_date} target_race={target_race}"
        )
    return written


def _load_booster_by_arch(model_path: Path, architecture: Architecture) -> BoosterLike:
    """Load a booster from ``model_path`` using the right native adapter.

    The XGBoost / CatBoost adapters are imported lazily because they pull in the
    bundled native libraries; isolating the dispatch here lets the category
    fallback and every cell-routing variant share one load path (and lets tests
    patch this single seam without importing the heavy adapters)."""
    if architecture == "xgboost":
        from xgboost_adapter import load_xgboost_booster  # bundled in image

        return load_xgboost_booster(str(model_path))
    from catboost_adapter import load_catboost_booster  # bundled in image

    return load_catboost_booster(str(model_path))


def _load_booster(models_dir: Path, category: Category) -> BoosterLike:
    model_path = models_dir / build_r2_object_key(category, MODEL_FILE_NAME)
    return _load_booster_by_arch(model_path, architecture_for(category))


def _load_xgb_etop2_booster(models_dir: Path) -> BoosterLike:
    """Load the XGBoost companion model for E-top2 JRA override.

    Resolves the artifact at ``models/finish-position/jra/xgb-jra-2013-v8/
    model.json`` (same path as baked into the image alongside CB iter20).
    Called once at category startup when JRA_ETOP2_ENABLED is True.
    """
    assert_production_model_version_allowed(
        JRA_ETOP2_XGB_MODEL_VERSION,
        context="JRA E-top2 companion",
    )
    model_path = models_dir / build_r2_xgb_etop2_key(MODEL_FILE_NAME)
    from xgboost_adapter import load_xgboost_booster  # bundled in image

    return load_xgboost_booster(str(model_path))


def _load_stage1_variant_artifact(
    models_dir: Path,
    category: Category,
    model_version: str,
    feature_count: int,
    architecture: Architecture,
    context: str,
) -> VariantModel:
    assert_production_model_version_allowed(model_version, context=context)
    model_path = models_dir / build_base_model_r2_key(category, model_version, MODEL_FILE_NAME)
    booster = _load_booster_by_arch(model_path, architecture)
    meta_path = models_dir / build_base_model_r2_key(category, model_version, METADATA_FILE_NAME)
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    feature_names = [str(name) for name in metadata["feature_names"]]
    assert_no_within_race_leak_columns(feature_names, context=context)
    assert_feature_count(feature_names, feature_count)
    if not _variant_booster_feature_order_matches(booster, architecture, feature_names):
        raise ValueError(
            f"{context} version={model_version} booster's own trained column order "
            "disagrees with metadata.json"
        )
    return VariantModel(
        booster=booster,
        feature_names=feature_names,
        architecture=architecture,
        model_version=model_version,
    )


def _load_stage1_model(
    models_dir: Path, category: Category, config: Stage1CategoryConfig
) -> VariantModel | None:
    """Load the Stage-1 market-free gated-fallback booster, or None to disable.

    Fail-closed at startup (mirrors ``_load_nar_transformer``'s pattern): any
    load failure, an unapproved model_version, a leak-column artifact, or a
    feature-count/order mismatch disables the fallback for this run -- every
    race then scores Stage-2 (the champion) unchanged, exactly as if
    ``stage1_routing.json`` had no entry for this category. A missing or
    broken fallback artifact must never block or degrade ordinary serving.
    """
    try:
        architecture = _as_architecture(config.architecture)
        companion = _load_stage1_variant_artifact(
            models_dir,
            category,
            config.model_version,
            config.feature_count,
            architecture,
            f"stage1 fallback category={category}",
        )
        if config.top1_swap_base_model_version is not None:
            base = _load_stage1_variant_artifact(
                models_dir,
                category,
                config.top1_swap_base_model_version,
                config.feature_count,
                architecture,
                f"stage1 top1-swap base category={category}",
            )
            companion = VariantModel(
                booster=companion.booster,
                feature_names=companion.feature_names,
                architecture=companion.architecture,
                model_version=companion.model_version,
                top1_swap_base=base,
            )
    except BaseException as load_error:
        debug_log(
            f"[stage1-gate] category={category} version={config.model_version} "
            f"load failed -> Stage-1 fallback disabled this run: {load_error}"
        )
        return None
    debug_log(
        f"[stage1-gate] loaded category={category} version={config.model_version} "
        f"features={config.feature_count}"
    )
    return companion


def _build_feature_rows(
    category: Category,
    window: PredictWindow,
    target_race: str | None = None,
    r2_config: R2Config | None = None,
) -> Mapping[str, list[Mapping[str, object]]]:
    """Run the repo feature pipeline and load the resulting parquet per race.

    Delegated to the bundled pipeline scripts (DuckDB base build in
    ``--target-date`` mode + v7 layers); see ``DEPLOY.md`` for the exact
    subprocess invocation chain. Returns a map of ``race_id`` -> ordered entry
    feature dicts for today's races (incl. UPCOMING). When ``target_race``
    ("keibajo:bango") is set it is forwarded to the DuckDB builder's
    ``--target-race`` so only that one race is built.

    When ``target_race`` is set AND
    ``predict_lib.pipeline_args.is_day_base_split_enabled(category)`` is True,
    the focused per-race day-base/RACE_CHAIN split path
    (``pipeline_runner.build_upcoming_feature_rows_split``) is tried first --
    it re-runs only the RACE_CHAIN layers against a cached per-category+day
    day-base instead of the full ``LAYER_CHAIN``. When that path returns
    ``None`` (day-base unavailable, entry-list drift, or any error) this falls
    through unchanged to the existing full ``build_upcoming_feature_rows``
    call below -- the split path is purely an opt-in fast path, never a
    behavioural change to the fallback.

    A scoped ``mode=rescore`` CacheMiss fallback
    (:func:`predict_lib.serve.is_scoped_rescore_cache_miss_fallback`) skips the
    split entirely: ``build_upcoming_feature_rows_split`` would otherwise
    inline-``build_day_base`` for the whole card. The fallback must rebuild
    only the named race via ``LAYER_CHAIN`` + ``--target-race``.
    """
    from pipeline_runner import (  # bundled in image
        build_upcoming_feature_rows,
        build_upcoming_feature_rows_split,
    )
    from predict_lib.container_role import (
        DayBaseRequiredError,
        PredictContainerRole,
        predict_container_role,
    )
    from predict_lib.pipeline_args import is_day_base_split_enabled

    role = predict_container_role()
    if role is PredictContainerRole.RACE_CHAIN and target_race is None:
        raise DayBaseRequiredError("focused race scope is required")

    use_split = role is PredictContainerRole.RACE_CHAIN or (
        is_day_base_split_enabled(category) and not is_scoped_rescore_cache_miss_fallback()
    )
    if target_race is not None and use_split:
        split_rows = build_upcoming_feature_rows_split(
            category,
            window.target_date,
            window.days_ahead,
            window.database_url,
            target_race,
            r2_config=r2_config,
        )
        if split_rows is not None:
            return split_rows
        if role is PredictContainerRole.RACE_CHAIN:
            raise DayBaseRequiredError(
                f"race-chain returned no rows category={category} "
                f"target_date={window.target_date} target_race={target_race}"
            )

    return build_upcoming_feature_rows(
        category,
        window.target_date,
        window.days_ahead,
        window.database_url,
        target_race=target_race,
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
        debug_log(f"[predict-upcoming] audit connect failed: {audit_connect_error}")
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
        debug_log(f"[predict-upcoming] audit write failed: {audit_write_error}")
    finally:
        try:
            audit_connection.close()
        except BaseException as audit_close_error:
            debug_log(f"[predict-upcoming] audit close failed: {audit_close_error}")


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


RACE_ID_KEIBAJO_INDEX: int = 3
RACE_ID_BANGO_INDEX: int = 4
RACE_ID_PART_COUNT: int = 5


def _split_parquet_by_race(
    final_dir: Path,
    category_str: str,
    run_date: str,
) -> list[dict[str, str]]:
    """Split the whole-day feature parquet under ``final_dir`` into per-race payloads.

    Uses DuckDB (bundled in the image) to read every parquet under ``final_dir``,
    enumerate the distinct ``race_id`` values, and COPY each race's rows to a
    temp parquet. ``race_id`` has the form
    ``source:nen:tsukihi:keibajo_code:race_bango``; the keibajo / bango parts are
    extracted by a plain ``:`` split. Each per-race parquet is base64-encoded and
    paired with its per-race R2 key from :func:`build_r2_per_race_feat_cache_key`.
    Races whose ``race_id`` does not split into exactly 5 parts are skipped.
    """
    import tempfile

    import duckdb  # bundled in image

    payloads: list[dict[str, str]] = []
    glob_path = str(final_dir / "**" / "*.parquet")
    con = duckdb.connect(":memory:")
    try:
        race_ids = [
            str(row[0])
            for row in con.execute(
                "SELECT DISTINCT race_id FROM read_parquet(?, hive_partitioning = false)",
                [glob_path],
            ).fetchall()
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            expected_source = "jra" if category_str == "jra" else "nar"
            for race_id in race_ids:
                parts = race_id.split(":")
                if len(parts) != RACE_ID_PART_COUNT:
                    debug_log(f"[predict-serve] per_race_parquet skip malformed race_id={race_id}")
                    continue
                race_run_date = f"{parts[1]}{parts[2]}"
                if parts[0] != expected_source or race_run_date != run_date:
                    debug_log(
                        "[predict-serve] per_race_parquet skip out-of-scope "
                        f"race_id={race_id} category={category_str} run_date={run_date}"
                    )
                    continue
                keibajo_code = parts[RACE_ID_KEIBAJO_INDEX]
                race_bango = parts[RACE_ID_BANGO_INDEX]
                out_path = Path(tmp_dir) / f"{keibajo_code}_{race_bango}.parquet"
                # DuckDB binds COPY's destination placeholder before the
                # placeholders inside its SELECT, irrespective of text order.
                con.execute(
                    "COPY (SELECT * FROM read_parquet(?, hive_partitioning = false) "
                    "WHERE race_id = ?) TO ? (FORMAT PARQUET)",
                    [str(out_path), glob_path, race_id],
                )
                data = out_path.read_bytes()
                encoded = base64.b64encode(data).decode("ascii")
                parquet_key = build_r2_per_race_feat_cache_key(
                    category_str, run_date, keibajo_code, race_bango
                )
                payloads.append({"parquetBase64": encoded, "parquetKey": parquet_key})
    finally:
        con.close()
    debug_log(
        f"[predict-serve] per_race_parquet ready races={len(payloads)} "
        f"category={category_str} run_date={run_date}"
    )
    return payloads


def _seed_focused_full_per_race_payloads(
    category_str: str,
    run_date: str,
    keibajo_code: str,
    race_bango: str,
) -> list[dict[str, str]] | None:
    """Build the per-race R2 payload for one focused-full race.

    A focused-full run must seed
    ``feat-cache/catalog-v1/{category}/{runDate}/{keibajo}/{race}/features.parquet``
    so the next scoped ``mode=rescore`` can watermark-read it instead of
    CacheMissing into a full rebuild. A DuckDB ``race_id`` split both isolates
    and verifies the requested race. If the parquet is unreadable or the
    requested race is absent, fail closed and return no payload; attributing
    unverified bytes to the requested key would poison the scoped cache.
    Never writes the day-level ``feat-cache/.../{runDate}/features.parquet``
    key -- a single-race parquet must not overwrite the whole-day cache.
    """
    from pipeline_runner import WORK_DIR  # bundled in image

    final_dir = WORK_DIR / f"feat-{category_str}-v7-final"
    if next(final_dir.rglob("*.parquet"), None) is None:
        return None
    expected_key = build_r2_per_race_feat_cache_key(
        category_str, run_date, keibajo_code, race_bango
    )
    try:
        split = _split_parquet_by_race(final_dir, category_str, run_date)
    except Exception as split_error:
        debug_log(f"[predict-serve] focused-full per-race split failed: {split_error}")
        return None
    matched = [item for item in split if item["parquetKey"] == expected_key]
    if not matched:
        debug_log(
            "[predict-serve] focused-full per-race seed skipped: "
            f"expected race absent key={expected_key}"
        )
        return None
    return matched


def _write_refreshed_to_parquet(
    final_dir: Path,
    refreshed: dict[str, list[dict[str, object]]],
) -> None:
    """Overwrite the cached parquet in ``final_dir`` with the refreshed entries.

    Flattens the race_id->entries map into a single DataFrame and writes it as
    ``features.parquet`` so the per-race split reads updated features.
    """
    import pandas as pd

    all_entries = [entry for entries in refreshed.values() for entry in entries]
    if not all_entries:
        return
    target = final_dir / "features.parquet"
    pd.DataFrame(all_entries).to_parquet(target, index=False)


def _make_predict_fn(
    database_url: str,
    models_dir: Path,
    source_url: str,
    r2: R2Config | None,
    focused_full_cache_store: FocusedFullCacheStore | None = None,
) -> tuple[
    PredictCategoryFn, ParquetPayloadFn, PerRaceParquetPayloadFn, FocusedFullCachePopulateFn
]:
    """Build the full-pipeline ``predict_fn`` + the parquet payload adapters.

    Returns a tuple of:
    - ``predict_fn``: the prediction callable passed to ``iter_predict_chunks``.
    - ``parquet_payload_fn``: reads the built feature parquet from the local
      tmp directory and returns ``(parquet_base64, parquet_key)`` so the Worker
      DO can proxy the bytes to R2 via its FEATURES_CACHE binding — bypassing the
      read-only S3 token limitation in the Container env.  Falls back silently to
      the legacy ``_try_r2_put`` (SigV4 PUT) when both R2 credentials AND the
      parquet file exist.
    - ``per_race_parquet_payload_fn``: splits that same whole-day parquet by
      ``race_id`` (via DuckDB) into one parquet per race and returns a list of
      ``{"parquetBase64", "parquetKey"}`` dicts so the Worker DO can also seed a
      per-race R2 object, letting a Stage-2 rescore hit a single race even when
      the whole-day parquet upload was skipped.
    - ``focused_full_cache_populate_fn``: the
      :data:`predict_lib.serve.FocusedFullCachePopulateFn` injected into
      ``iter_predict_chunks`` for the detached focused-full path (see that
      type's docstring for why this exists at all). Seeds the per-race
      ``feat-cache/catalog-v1/{category}/{runDate}/{keibajo}/{race}/features.parquet``
      payload from *params* directly (not ``_last_run``) so a later scoped
      rescore can watermark-read it. Does not write the whole-day cache key.

    ``parquet_payload_fn`` / ``per_race_parquet_payload_fn`` share a
    thread-safe ``_last_run`` state box (guarded by ``_last_run_lock``, a lock
    private to THIS factory call) so they can retrieve category/run_date after
    the predict thread completes — this is safe for the synchronous
    request/response path they serve (the read happens moments after the
    matching write, and ``predict_lib.serve._PIPELINE_EXEC_LOCK`` serializes
    every pipeline call process-wide). ``focused_full_cache_populate_fn``
    deliberately does NOT use ``_last_run``: it runs inside a *detached*
    background thread with no such tight coupling to a live response, so it
    takes ``category``/``run_date`` explicitly from the ``PredictParams`` the
    caller already has — see that function's own docstring for the race this
    avoids.
    """
    _last_run: list[tuple[str, str]] = []
    _last_run_lock = threading.Lock()

    def _predict(
        category_str: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        from pipeline_runner import pipeline_execution_scope
        from predict_lib.model_meta import resolve_category

        category = resolve_category(category_str)
        target_race = f"{keibajo_code}:{race_bango}" if keibajo_code and race_bango else None
        window = PredictWindow(target_date=run_date, days_ahead=days_ahead, database_url=source_url)

        progress_fn: Callable[[str], None] | None = None
        if target_race is not None:
            race_key = f"{category_str}:{run_date}:{target_race}"

            def _mark_progress(detail: str) -> None:
                mark_focused_full_progress(race_key, detail)

            progress_fn = _mark_progress

        with pipeline_execution_scope(progress_fn=progress_fn):
            written = predict_category(
                database_url,
                category,
                models_dir,
                window,
                target_race=target_race,
                r2_config=r2,
                card_max_race_bango=card_max_race_bango,
            )
        # Record the last successful run so parquet_payload_fn can retrieve it.
        with _last_run_lock:
            _last_run.clear()
            _last_run.append((category_str, run_date))
        return written

    def _last_run_snapshot() -> tuple[str, str] | None:
        """Return a snapshot of the most recent ``(category, run_date)``, if any.

        Reading under ``_last_run_lock`` avoids observing the momentary empty
        window between ``clear()`` and ``append()`` in ``_predict`` above if a
        DIFFERENT concurrent request's predict_fn call is mutating ``_last_run``
        at the same instant this one reads it (``_PIPELINE_EXEC_LOCK`` prevents
        two predict_fn bodies from running at once, but does not by itself
        order a request's OWN post-pipeline payload read against the NEXT
        request's write once that lock is released).
        """
        with _last_run_lock:
            if not _last_run:
                return None
            return _last_run[-1]

    def _build_parquet_payload(category_str: str, run_date: str) -> tuple[str, str] | None:
        """Read the built feature parquet for *category_str*/*run_date* from the
        local tmp directory and return ``(parquet_base64, parquet_key)``.

        Pure function of its explicit arguments (no ``_last_run`` read) so it
        can be shared safely by both the ``_last_run``-based zero-arg
        ``_parquet_payload`` (below) and the explicit-args focused-full cache
        populate path.
        """
        from pipeline_runner import WORK_DIR  # bundled in image

        final_dir = WORK_DIR / f"feat-{category_str}-v7-final"
        parquet_files = list(final_dir.rglob("*.parquet"))
        if not parquet_files:
            debug_log(f"[predict-serve] parquet_payload skip: no parquet in {final_dir}")
            return None
        local_path = parquet_files[0]
        parquet_key = build_r2_feat_cache_key(category_str, run_date)
        data = local_path.read_bytes()
        encoded = base64.b64encode(data).decode("ascii")
        debug_log(f"[predict-serve] parquet_payload ready key={parquet_key} bytes={len(data)}")
        return encoded, parquet_key

    def _parquet_payload() -> tuple[str, str] | None:
        """Return ``(parquet_base64, parquet_key)`` for the last successful run."""
        snapshot = _last_run_snapshot()
        if snapshot is None:
            return None
        category_str, run_date = snapshot
        return _build_parquet_payload(category_str, run_date)

    def _build_per_race_payloads(category_str: str, run_date: str) -> list[dict[str, str]] | None:
        """Split the whole-day parquet for *category_str*/*run_date* by
        ``race_id`` into per-race payloads.

        Same explicit-args / no-shared-state shape as
        :func:`_build_parquet_payload`, for the same reason. Returns ``None``
        when there is no parquet on disk or the split fails; the focused-full
        completion hook converts that absence into a retryable terminal error.
        """
        from pipeline_runner import WORK_DIR  # bundled in image

        final_dir = WORK_DIR / f"feat-{category_str}-v7-final"
        parquet_files = list(final_dir.rglob("*.parquet"))
        if not parquet_files:
            debug_log(f"[predict-serve] per_race_parquet skip: no parquet in {final_dir}")
            return None
        try:
            return _split_parquet_by_race(final_dir, category_str, run_date)
        except BaseException as split_error:
            debug_log(f"[predict-serve] per_race_parquet split failed: {split_error}")
            return None

    def _per_race_parquet_payloads() -> list[dict[str, str]] | None:
        """Split the last successful run's whole-day parquet by ``race_id``."""
        snapshot = _last_run_snapshot()
        if snapshot is None:
            return None
        category_str, run_date = snapshot
        return _build_per_race_payloads(category_str, run_date)

    def _populate_focused_full_cache(params: PredictParams) -> None:
        """Explicit-args cache populate for the detached focused-full path.

        Deliberately does not read ``_last_run`` -- *params* already carries
        the exact ``(category, run_date)`` this run was for, so there is no
        shared-state read to race against a subsequent request's write. See
        :data:`predict_lib.serve.FocusedFullCachePopulateFn` for the calling
        contract (required for semantic success, called before slot release).
        """
        if focused_full_cache_store is None:
            raise RuntimeError("focused-full cache store is unavailable")
        category_str = params.category
        run_date = params.run_date
        per_race: list[dict[str, str]] | None
        if params.keibajo_code is not None or params.race_bango is not None:
            if params.keibajo_code is None or params.race_bango is None:
                raise RuntimeError("focused-full cache race scope is incomplete")
            per_race = _seed_focused_full_per_race_payloads(
                category_str, run_date, params.keibajo_code, params.race_bango
            )
        else:
            per_race = _build_per_race_payloads(category_str, run_date)
        if per_race is None:
            raise RuntimeError("focused-full cache payload is unavailable")
        race_key = build_focused_full_race_key(params)
        focused_full_cache_store.put(
            race_key,
            FocusedFullCachePayload(
                parquet_base64=None,
                parquet_key=None,
                per_race_parquets=per_race,
            ),
        )

    return _predict, _parquet_payload, _per_race_parquet_payloads, _populate_focused_full_cache


def _make_prewarm_fn(
    database_url: str,
    r2_config: R2Config | None = None,
    prewarm_commit_fn: PrewarmCommitFn | None = None,
) -> PrewarmBuildFn:
    """Build the ``GET /prewarm-day-base`` build callable bound to ``database_url``.

    Mirrors ``_make_predict_fn``'s closure-binding pattern: ``serve.py`` stays
    I/O-free, so the real Neon URL is bound here (not passed through the query
    string) and ``pipeline_runner.build_day_base`` does the actual DuckDB base
    build + DAY_CHAIN layers.

    ``r2_config`` (task #32, 2026-07-19) is forwarded to ``build_day_base`` so
    the RS-freshness component of its watermark
    (``pipeline_runner._compute_rs_watermark``) can be computed -- the SAME
    combined watermark this prewarm job's own local-disk / cross-process
    trust checks already use, so the R2 metadata this job's parquet upload
    carries (via ``_prewarm_parquet_payload`` reading back
    ``watermark.json``) agrees with what a reader later compares against.
    """

    def _prewarm(category_str: str, run_date: str, days_ahead: int) -> Path | None:
        from pipeline_runner import build_day_base, pipeline_execution_scope  # bundled in image
        from predict_lib.model_meta import resolve_category

        category = resolve_category(category_str)

        def _commit_running_style_foundation(
            foundation_category: Category,
            foundation_run_date: str,
            base_dir: Path,
            entrant_watermark: tuple[str, int],
        ) -> None:
            if prewarm_commit_fn is None:
                return
            parquet_file = next(iter(sorted(base_dir.rglob("*.parquet"))), None)
            if parquet_file is None:
                raise RuntimeError("running-style foundation parquet missing after base build")
            encoded = base64.b64encode(parquet_file.read_bytes()).decode("ascii")
            prewarm_commit_fn(
                build_r2_running_style_foundation_key(foundation_category, foundation_run_date),
                encoded,
                {
                    "maxDataSakuseiNengappi": entrant_watermark[0],
                    "rowCount": entrant_watermark[1],
                    "rsPredictedAtMax": "none",
                    "rsRowCount": 0,
                },
                None,
            )

        # Detached prewarm runs outside the HTTP prediction scope.  Apply the
        # same immutable end-to-end deadline here; otherwise a DuckDB child
        # can outlive the pickup lease indefinitely, keep the liveness socket
        # active, and leave a billable Container running without ever landing
        # the day-base object.
        with pipeline_execution_scope():
            final_dir = build_day_base(
                category,
                run_date,
                days_ahead,
                database_url,
                r2_config=r2_config,
                running_style_foundation_commit_fn=_commit_running_style_foundation,
            )
        # Commit the terminal payload from the pipeline thread itself. The
        # Worker-to-Container response can be severed while the long-running
        # DuckDB chain is still healthy; relying on the response generator to
        # perform this commit loses a completed day-base in that case.
        if final_dir is not None and prewarm_commit_fn is not None:
            payload = _prewarm_parquet_payload(category, run_date, final_dir)
            if payload is not None:
                encoded, parquet_key, watermark, watermark_error = payload
                prewarm_commit_fn(parquet_key, encoded, watermark, watermark_error)
        return final_dir

    return _prewarm


def _prewarm_parquet_payload(
    category_str: str, run_date: str, day_base_dir: Path
) -> tuple[str, str, Mapping[str, str | int] | None, str | None] | None:
    """Read the day-base parquet and return
    ``(base64, R2 key, watermark, watermark_error)``
    for the Worker DO proxy -- the ``PrewarmParquetPayloadFn`` injected into
    :func:`predict_lib.serve.iter_prewarm_chunks`. Returns ``None``
    (non-blocking) when no parquet file is found under ``day_base_dir``.

    ``day_base_dir`` is ``build_day_base``'s own return value -- the day
    dir's ``final`` subdirectory (``pipeline_runner._day_base_dir(...) /
    "final"``). The watermark ``build_day_base`` writes (task #24's
    RS-aware watermark, task #32's R2 sidecar) lives one level up, at
    ``day_base_dir.parent / "watermark.json"``
    (``pipeline_runner._watermark_path``) -- read via the same
    ``_read_watermark`` helper ``ensure_day_base`` uses for the local-disk
    fast path, so the R2 metadata and the local-disk trust check are always
    built from an identical value. Missing/unreadable (offline/Postgres
    source day-bases never write one) degrades to ``None`` -- the Worker
    then PUTs the parquet without ``customMetadata``, not an error (see
    :func:`predict_lib.serve.build_prewarm_result_line`).
    """
    import pipeline_runner  # bundled in image

    # ``_read_watermark`` is module-private (leading underscore); accessed via
    # getattr + cast (same pattern ``tests/test_pipeline_runner.py`` already
    # uses for this module's other private helpers) so strict basedpyright
    # does not flag ``reportPrivateUsage`` on a static
    # ``pipeline_runner._read_watermark`` expression, and the attribute name
    # is read from a variable (not a string literal) so ruff's B009 does not
    # fire either.
    read_watermark_attr = "_read_watermark"
    read_watermark = cast(
        "Callable[[Path], tuple[str, int, str, int] | None]",
        getattr(pipeline_runner, read_watermark_attr),
    )
    read_reason_attr = "_read_watermark_reason"
    read_reason = cast(
        "Callable[[Path], str | None]",
        getattr(pipeline_runner, read_reason_attr),
    )

    parquet_files = list(day_base_dir.rglob("*.parquet"))
    if not parquet_files:
        debug_log(f"[predict-serve] prewarm_parquet_payload skip: no parquet in {day_base_dir}")
        return None
    data = parquet_files[0].read_bytes()
    encoded = base64.b64encode(data).decode("ascii")
    parquet_key = build_r2_day_base_key(category_str, run_date)
    watermark_tuple = read_watermark(day_base_dir.parent)
    watermark: Mapping[str, str | int] | None = None
    if watermark_tuple is not None:
        max_updated, row_count, rs_predicted_at_max, rs_row_count = watermark_tuple
        watermark = {
            "maxDataSakuseiNengappi": max_updated,
            "rowCount": row_count,
            "rsPredictedAtMax": rs_predicted_at_max,
            "rsRowCount": rs_row_count,
        }
    watermark_error = read_reason(day_base_dir.parent)
    debug_log(
        f"[predict-serve] prewarm_parquet_payload ready key={parquet_key} bytes={len(data)} "
        f"watermark={'present' if watermark is not None else 'absent'} "
        f"reason={watermark_error if watermark_error is not None else '-'}"
    )
    return encoded, parquet_key, watermark, watermark_error


def _make_prewarm_existing_object_fn(
    r2: R2Config | None, database_url: str
) -> PrewarmExistingObjectFn:
    """Return a freshness-checked skip for an already-cached day-base object.

    Presence alone is never sufficient: the R2 metadata must equal the live
    Catalog entrant plus running-style watermark used by ``ensure_day_base``.
    """

    def _existing(category_str: str, run_date: str) -> str | None:
        if r2 is None:
            return None
        from pipeline_runner import compute_day_base_watermark
        from predict_lib.model_meta import resolve_category

        category = resolve_category(category_str)
        live_watermark = compute_day_base_watermark(category, run_date, database_url, r2_config=r2)
        if live_watermark is None:
            return None
        object_key = build_r2_day_base_key(category_str, run_date)
        if r2_head_watermark(r2, object_key) != live_watermark:
            return None
        return object_key

    return _existing


def _make_prewarm_commit_fn(store: FocusedFullCacheStore) -> PrewarmCommitFn:
    """Stash day-base bytes for Worker FEATURES_CACHE pickup.

    Container R2 tokens are read-only (see focused_full_cache.py). A SigV4 PUT
    here would 403; the Worker binding is the only working write path.
    """

    def _commit(
        parquet_key: str,
        parquet_b64: str,
        watermark: Mapping[str, str | int] | None,
        watermark_error: str | None = None,
    ) -> None:
        category, run_date = parse_day_base_cache_identity(parquet_key)
        watermark_state = "present" if watermark is not None else "absent"
        reason_state = watermark_error if watermark_error is not None else "-"
        debug_log(
            f"[predict-serve] prewarm_commit stored key={parquet_key} "
            f"watermark={watermark_state} reason={reason_state}"
        )
        store.put(
            build_prewarm_cache_key(category, run_date),
            FocusedFullCachePayload(
                parquet_base64=parquet_b64,
                parquet_key=parquet_key,
                per_race_parquets=None,
                daybase_watermark=dict(watermark) if watermark is not None else None,
                watermark_error=watermark_error,
            ),
        )

    return _commit


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
    if not r2_get_parquet(r2, object_key, dest_path):
        raise CacheMissError(f"R2 cache miss: {object_key} not found in bucket {r2.bucket}")


def _log_rescore_attestation_lifecycle(
    category: str,
    run_date: str,
    scope: RaceScope,
    status: str,
    reason: str,
) -> None:
    """Emit one bounded, credential-free attestation decision with debug disabled."""
    payload = {
        "event": "rescore-cache-attestation",
        "category": category,
        "runDate": run_date,
        "venue": scope.keibajo_code,
        "race": scope.race_bango,
        "status": status,
        "reason": reason,
    }
    print(json.dumps(payload, separators=(",", ":")), file=sys.stderr, flush=True)


def _rescore_attestation_error(
    category: str,
    run_date: str,
    scope: RaceScope,
    reason: str,
) -> CacheValidationError:
    _log_rescore_attestation_lifecycle(category, run_date, scope, "rejected", reason)
    return CacheValidationError(f"rescore cache attestation rejected: {reason}")


def _attested_identity_matches(
    identity: R2ObjectIdentity | None,
    attestation: RescoreCacheAttestation,
) -> bool:
    """Compare the ETag identity shared by Workers R2 and S3 HEAD.

    Workers ``R2Object.version`` is upload-unique, while R2's S3 HeadObject
    does not expose it as ``x-amz-version-id``. The required version query
    parameter remains parsed for rolling protocol compatibility, but ETag is
    the cross-surface strong identity. HEAD before and after GET closes the
    object-replacement race, while the candidate entrant hash independently
    binds content to the Worker evidence.
    """
    return identity is not None and identity.etag == attestation.feature_cache_etag


def _candidate_entry_attestation(candidate_dir: Path, target_race: str) -> tuple[str, int]:
    """Return the exact canonical entrant hash/count for one candidate parquet.

    An empty hash/count is the fail-closed malformed/mixed-race sentinel. I/O
    failures still raise so the caller can distinguish unavailable validation
    from a candidate that positively disagrees with the Worker evidence.
    """
    import duckdb

    from predict_lib.race_id import parse_race_id

    expected_venue, expected_race = (part.zfill(2) for part in target_race.split(":", 1))
    glob_path = str(candidate_dir / "**" / "*.parquet")
    connection = duckdb.connect(":memory:")
    try:
        rows = connection.execute(
            "SELECT race_id, ketto_toroku_bango, umaban "
            "FROM read_parquet(?, hive_partitioning = false)",
            [glob_path],
        ).fetchall()
    finally:
        connection.close()
    tokens: list[str] = []
    for race_id, ketto, umaban in rows:
        try:
            parts = parse_race_id(str(race_id))
        except ValueError:
            return "", 0
        if (
            parts.keibajo_code.zfill(2) != expected_venue
            or parts.race_bango.zfill(2) != expected_race
        ):
            return "", 0
        ketto_text = str(ketto).strip() if ketto is not None else ""
        umaban_text = str(umaban).strip() if umaban is not None else ""
        if (
            not ketto_text
            or not umaban_text.isascii()
            or not umaban_text.isdigit()
            or int(umaban_text) <= 0
        ):
            return "", 0
        tokens.append(f"{ketto_text}:{int(umaban_text)}")
    if not tokens or len(tokens) != len(set(tokens)):
        return "", 0
    canonical = "\n".join(sorted(tokens))
    return hashlib.sha256(canonical.encode()).hexdigest(), len(tokens)


def _fetch_watermarked_per_race_cache(
    final_dir: Path,
    category_str: str,
    run_date: str,
    scope: RaceScope,
    r2: R2Config,
    source_url: str,
    attestation: RescoreCacheAttestation | None = None,
) -> bool:
    """Fetch + watermark-validate a per-race R2 feature cache into ``final_dir``.

    The catalog-source counterpart to :func:`_ensure_cached_parquet` -- only
    called for a single-race ``scope`` (the coordinator's per-race rescore
    shape; a whole-category rescore never reaches this function, see the
    caller). e6111ca6 made ``mode=rescore`` unconditionally distrust ANY
    cached parquet for a catalog source, because the pre-``262f06cb`` R2
    cache was provably untrustworthy (``tmp/rc2-defect-sweep/
    sweepC-class3-r2-cache-inventory.txt``: never populated by the dominant
    focused-full path at all, and half of the few objects that DID exist held
    a DIFFERENT date's races than their key implied). ``262f06cb`` fixed the
    write gap (focused-full's detached pipeline now hands its payload to
    :class:`predict_lib.focused_full_cache.FocusedFullCacheStore`, picked up
    once Neon confirms completion and proxied to R2 the normal way) and every
    object it writes lives under the ``build_r2_per_race_feat_cache_key``
    per-race, per-date key namespace (``feat-cache/catalog-v1/...`` --
    tagged with the current source-architecture generation, so a pre-migration
    stale object can never collide with it). What was still missing is this
    function: a caller willing to actually READ that now-trustworthy cache,
    gated behind a real freshness check rather than blanket rejection.

    The watermark check reuses :func:`pipeline_runner.day_base_covers_entry_list`
    UNMODIFIED, pointed at the downloaded candidate instead of a day-base
    directory: it queries the CURRENT entrant list for this exact race from
    the live source and confirms every current horse appears in the
    candidate's OWN rows for this exact ``keibajo_code``/``race_bango``
    (matched via the parquet's own ``race_id`` column, never the R2 key's
    embedded date). This closes both risks the sweep documented in one check:
    entry-list drift (a late scratch/add since focused-full built the cache)
    fails the coverage test directly, and a candidate holding the WRONG
    race's rows entirely matches zero current entrants for THIS race and is
    rejected the same way -- there is no code path where a mismatched-content
    object can pass.

    The candidate is downloaded into an isolated sibling directory first,
    never ``final_dir`` directly, so a failed or watermark-rejected candidate
    can never contaminate ``final_dir`` with partial or untrusted content;
    ``final_dir`` itself is only reset and populated after the watermark
    passes. Returns ``True`` (features now live in ``final_dir``) or
    ``False`` (caller must fall back to :class:`CacheMissError`, the
    unchanged pre-existing behavior) on ANY failure -- R2 miss, network
    error, watermark rejection, or any exception from the freshness check.
    Never raises.
    """
    if scope.keibajo_code is None or scope.race_bango is None:
        if attestation is not None:
            raise _rescore_attestation_error(
                category_str, run_date, scope, "exact-race-scope-required"
            )
        return False
    object_key = build_r2_per_race_feat_cache_key(
        category_str, run_date, scope.keibajo_code, scope.race_bango
    )
    candidate_dir = final_dir.parent / f"{final_dir.name}-per-race-candidate"
    candidate_path = candidate_dir / "features.parquet"
    try:
        shutil.rmtree(candidate_dir, ignore_errors=True)
        if attestation is not None:
            now_ms = time.time_ns() // 1_000_000
            if (
                attestation.issued_at_ms > now_ms + RESCORE_ATTESTATION_FUTURE_SKEW_MS
                or now_ms - attestation.issued_at_ms > RESCORE_ATTESTATION_TTL_MS
            ):
                raise _rescore_attestation_error(
                    category_str, run_date, scope, "expired-during-container-wait"
                )
            before_identity = r2_head_identity(r2, object_key)
            if not _attested_identity_matches(before_identity, attestation):
                raise _rescore_attestation_error(
                    category_str, run_date, scope, "feature-identity-mismatch"
                )
        if not r2_get_parquet(r2, object_key, candidate_path):
            if attestation is not None:
                raise _rescore_attestation_error(
                    category_str, run_date, scope, "feature-object-missing"
                )
            return False
        from predict_lib.model_meta import resolve_category

        target_race = f"{scope.keibajo_code}:{scope.race_bango}"
        category = resolve_category(category_str)
        if attestation is not None:
            candidate_hash, candidate_count = _candidate_entry_attestation(
                candidate_dir, target_race
            )
            if (
                candidate_hash != attestation.entry_set_hash
                or candidate_count != attestation.entry_count
            ):
                raise _rescore_attestation_error(
                    category_str, run_date, scope, "entry-set-mismatch"
                )
            after_identity = r2_head_identity(r2, object_key)
            if not _attested_identity_matches(after_identity, attestation):
                raise _rescore_attestation_error(
                    category_str, run_date, scope, "feature-identity-changed"
                )
            _log_rescore_attestation_lifecycle(
                category_str, run_date, scope, "accepted", "exact-entry-and-identity-match"
            )
        else:
            from pipeline_runner import day_base_covers_entry_list  # bundled in image

            if not day_base_covers_entry_list(
                candidate_dir, category, run_date, target_race, source_url
            ):
                debug_log(
                    f"[predict-serve] rescore per-race cache watermark rejected "
                    f"key={object_key} target_race={target_race}"
                )
                return False
        shutil.rmtree(final_dir, ignore_errors=True)
        final_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(candidate_path, final_dir / "features.parquet")
        debug_log(f"[predict-serve] rescore per-race cache watermark accepted key={object_key}")
        return True
    except CacheValidationError:
        raise
    except BaseException as exc:
        if attestation is not None:
            raise _rescore_attestation_error(
                category_str, run_date, scope, "validation-unavailable"
            ) from exc
        debug_log(f"[predict-serve] rescore per-race cache fetch failed key={object_key}: {exc}")
        return False
    finally:
        shutil.rmtree(candidate_dir, ignore_errors=True)


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
    generation: WeightSnapshotGeneration,
) -> dict[tuple[str, str], RaceFreshSnapshot]:
    """Fetch the latest odds + bataiju per race and build per-race snapshots.

    All HTTP I/O happens here (the only side effect on the rescore path); the
    returned snapshots feed the pure :func:`apply_fresh_snapshots`. Odds remain
    optional, but a horse-weight fetch or parse failure propagates so post-weight
    scoring cannot silently succeed with the pre-weight NULL fallback.
    """
    from realtime_odds_fetcher import (  # bundled in image
        HttpRealtimeOddsFetcher,
        fetch_odds_for_race,
        fetch_required_weight_for_race,
        source_for_category,
    )

    if len(race_keys) != 1:
        raise ValueError("post-weight snapshot verification requires exactly one race")
    fetcher = HttpRealtimeOddsFetcher()
    source = source_for_category(category_str)
    odds_futures: dict[tuple[str, str], Future[list[tuple[str, str, int, float, int]]]] = {}
    weight_futures: dict[tuple[str, str], Future[dict[int, int]]] = {}
    with ThreadPoolExecutor(max_workers=FRESH_SNAPSHOT_MAX_WORKERS) as executor:
        for keibajo_code, race_bango in race_keys:
            race_key = (keibajo_code, race_bango)
            odds_futures[race_key] = executor.submit(
                fetch_odds_for_race,
                fetcher,
                source,
                run_date,
                keibajo_code,
                race_bango,
            )
            weight_futures[race_key] = executor.submit(
                fetch_required_weight_for_race,
                fetcher,
                source,
                run_date,
                keibajo_code,
                race_bango,
                expected_count=generation.count,
                expected_fetched_at=generation.fetched_at,
                expected_hash=generation.snapshot_hash,
            )

        snapshots: dict[tuple[str, str], RaceFreshSnapshot] = {}
        for race_key in race_keys:
            odds_rows = odds_futures[race_key].result()
            weight_map = weight_futures[race_key].result()
            odds_by_umaban = {
                row[2]: OddsSnapshot(tansho_odds=row[3], tansho_ninkijun=row[4])
                for row in odds_rows
            }
            bataiju_by_umaban = {umaban: float(kg) for umaban, kg in weight_map.items()}
            snapshots[race_key] = RaceFreshSnapshot(
                odds_by_umaban=odds_by_umaban,
                bataiju_by_umaban=bataiju_by_umaban,
            )
    return snapshots


RescoreFactory = Callable[
    [RaceScope, RescoreCacheAttestation | None, WeightSnapshotGeneration | None, str | None],
    tuple[PredictCategoryFn, PerRaceParquetPayloadFn],
]
"""Builds a scope-bound rescore ``PredictCategoryFn`` + per-race payload fn for a request."""


def _make_rescore_fn(
    database_url: str,
    models_dir: Path,
    source_url: str,
    r2: R2Config | None,
    scope: RaceScope,
    attestation: RescoreCacheAttestation | None,
    weight_snapshot_generation: WeightSnapshotGeneration | None,
    race_start_at_jst: str | None,
) -> tuple[PredictCategoryFn, PerRaceParquetPayloadFn]:
    """Build the rescore-path ``rescore_fn`` + per-race parquet payload fn.

    For an offline non-Catalog source, the rescore path (Stage 2 of the
    per-race rebuild):
    1. Ensures the pre-built feature parquet from a prior ``mode=full`` run is
       available locally (downloads from R2 when configured, else raises
       :class:`CacheMissError` so the full pipeline runs).
    2. Reads the cached parquet directly into a ``race_id`` -> entries map — NO
       DuckDB build, NO 21y Neon scan.
    3. Fetches the latest tansho odds + bataiju for the in-scope races and
       recomputes the 5 late-binding columns (odds_score / popularity_score /
       tansho_odds / tansho_ninkijun / weight_diff_from_avg) per horse.
    4. Writes the weight/odds-refreshed entries back to ``features.parquet`` so the
       per-race split seeds R2 with the rebuilt features.
    5. Filters to the requested race scope (a single race or whole keibajo when
       ``keibajoCode`` / ``raceBango`` are set; all races otherwise).
    6. Scores (cell routing / JRA E-top2) and UPSERTs the predictions.

    Returns ``(rescore_fn, per_race_payloads_fn)``; the second fn re-splits the
    refreshed parquet by ``race_id`` so :func:`iter_predict_chunks` can embed the
    per-race payloads in the result line for both full and rescore modes.

    Production ``r2-catalog://`` sources use a DIFFERENT cache: not the
    whole-day object step 1 describes, but the per-race, watermark-validated
    cache :func:`_fetch_watermarked_per_race_cache` populates from
    ``262f06cb``'s focused-full write path -- only reachable when the
    request carries a single-race ``scope`` (the coordinator's per-race
    rescore shape; a whole-category rescore still has no safe cache to trust
    and goes straight to :class:`CacheMissError`, same as before). Either
    way, any failure -- object absent, watermark rejected, any exception --
    falls back to :class:`CacheMissError`, which makes
    :func:`iter_predict_chunks` run the raw Catalog full build. A caller
    without that fallback fails closed.
    """
    catalog_source = is_catalog_source_url(source_url)
    _last: list[tuple[str, str]] = []

    def _rescore(
        category_str: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        # ``days_ahead`` is unused (the cache already spans the morning build
        # window); the per-race scope is bound by the rescore factory, so the
        # ``keibajo_code`` / ``race_bango`` args carried by the shared
        # PredictCategoryFn contract are ignored here. ``card_max_race_bango``
        # is NOT scope-bound (unlike keibajo_code/race_bango) -- it is forwarded
        # untouched to ``_score_and_flush_races``, whose ``score_races`` call
        # self-derives it from ``scoped`` when this stays None (the
        # whole-category rescore shape, i.e. ``scope`` has no keibajo_code /
        # race_bango) and trusts this explicit value otherwise (the per-race
        # rescore shape, where self-derivation from a single-race ``scoped`` map
        # would be wrong -- see score_races' docstring).
        del days_ahead, keibajo_code, race_bango
        from pipeline_runner import WORK_DIR  # bundled in image
        from predict_lib.model_meta import resolve_category

        category = resolve_category(category_str)
        final_dir = WORK_DIR / f"feat-{category}-v7-final"
        if attestation is not None and not catalog_source:
            raise _rescore_attestation_error(
                category_str, run_date, scope, "catalog-source-required"
            )
        if catalog_source:
            # Whole-category rescore (no single-race scope) has no per-race
            # object to watermark-check against -- unchanged, still fails
            # closed immediately. A single-race scope gets one real attempt
            # at the watermark-validated cache before falling back.
            fetched = False
            if attestation is not None and r2 is None:
                raise _rescore_attestation_error(
                    category_str, run_date, scope, "r2-configuration-unavailable"
                )
            if scope.keibajo_code is not None and scope.race_bango is not None and r2 is not None:
                fetched = _fetch_watermarked_per_race_cache(
                    final_dir,
                    category_str,
                    run_date,
                    scope,
                    r2,
                    source_url,
                    attestation,
                )
            if not fetched:
                raise CacheMissError(
                    "processed feature cache unavailable or failed watermark "
                    "check for r2-catalog source; raw Catalog rebuild required"
                )
        else:
            _ensure_cached_parquet(final_dir, category_str, run_date, r2)

        races = _as_entry_map(_load_cached_races(final_dir))
        race_keys = _scope_race_keys(races, scope)
        if weight_snapshot_generation is None:
            raise CacheValidationError("post-weight snapshot generation unavailable")
        if race_start_at_jst is None:
            raise CacheValidationError("post-weight race deadline unavailable")
        snapshots = _fetch_fresh_snapshots(
            category_str, run_date, race_keys, weight_snapshot_generation
        )
        scoped_races = filter_races_by_scope(races, scope)
        if weight_snapshot_generation.active_horse_numbers is not None:
            excluded = weight_snapshot_generation.excluded_horse_numbers
            if excluded is None:
                raise CacheValidationError("post-weight entry snapshot generation incomplete")
            scoped_races = filter_post_weight_active_runners(
                scoped_races,
                weight_snapshot_generation.active_horse_numbers,
                excluded,
            )
        validate_post_weight_snapshots(scoped_races, snapshots)
        refreshed = apply_fresh_snapshots(scoped_races, snapshots, category)

        _write_refreshed_to_parquet(final_dir, refreshed)
        _last.clear()
        _last.append((category_str, run_date))

        return _score_and_flush_races(
            database_url,
            category,
            models_dir,
            refreshed,
            card_max_race_bango=card_max_race_bango,
            race_start_at_jst=race_start_at_jst,
            source_url=source_url,
        )

    def _per_race_payloads() -> list[dict[str, str]] | None:
        if not _last:
            return None
        from pipeline_runner import WORK_DIR  # bundled in image

        cat_str, rd = _last[-1]
        final_dir = WORK_DIR / f"feat-{cat_str}-v7-final"
        try:
            return _split_parquet_by_race(final_dir, cat_str, rd)
        except BaseException as err:
            debug_log(f"[predict-serve] rescore per_race_parquet split failed: {err}")
            return None

    return _rescore, _per_race_payloads


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

    def _factory(
        scope: RaceScope,
        attestation: RescoreCacheAttestation | None,
        weight_snapshot_generation: WeightSnapshotGeneration | None,
        race_start_at_jst: str | None,
    ) -> tuple[PredictCategoryFn, PerRaceParquetPayloadFn]:
        return _make_rescore_fn(
            database_url,
            models_dir,
            source_url,
            r2,
            scope,
            attestation,
            weight_snapshot_generation,
            race_start_at_jst,
        )

    return _factory


def _scope_from_params(params: PredictParams) -> RaceScope:
    """Build the race scope from the parsed ``keibajoCode`` / ``raceBango`` params."""
    return RaceScope(keibajo_code=params.keibajo_code, race_bango=params.race_bango)


def expected_model_version_for_entries(
    category: Category,
    entries: Sequence[Mapping[str, object]],
    card_max_race_bango: int | None = None,
    race_name: Mapping[str, object] | None = None,
) -> str:
    """Resolve routing metadata without consulting Neon.

    The helper remains useful for routing-contract tests and Cloudflare-side
    completion metadata. Production Container completion is not wired and does
    not query Neon.
    """
    cell_router = load_cell_router()
    routable_entries = [overlay_race_name_onto_entry(entry, race_name) for entry in entries]
    if cell_router.has_routing(category):
        routing = cell_router.routing_for(category)
        variant = cell_router.resolve_variant(
            category, routable_entries, card_max_race_bango=card_max_race_bango
        )
        spec = routing.variants.get(variant)
        if variant != routing.default_variant and spec is not None:
            return spec.model_version
    if category == "nar" and NAR_TRANSFORMER_BLEND_ENABLED:
        return NAR_TRANSFORMER_MODEL_VERSION
    return model_version_for(category)


def post_focused_full_completion_callback(
    callback_url: str,
    params: PredictParams,
    status: Literal["success", "error"],
    error: str | None,
) -> None:
    """Nudge the signed Worker callback without exposing its bearer URL in logs."""
    body = json.dumps(
        {
            "raceKey": build_focused_full_race_key(params),
            "status": status,
            "error": error,
        },
        separators=(",", ":"),
    ).encode()
    request = urllib.request.Request(
        callback_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": FOCUSED_FULL_CALLBACK_USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=FOCUSED_FULL_CALLBACK_TIMEOUT_SECONDS,
        ) as response:
            response.read()
    except (OSError, ValueError):
        print(
            "[focused-full] completion callback unavailable; durable poll remains active",
            file=sys.stderr,
            flush=True,
        )


class _PredictHandler(http.server.BaseHTTPRequestHandler):
    """Minimal HTTP/1.1 request handler for ``/ping``, ``/predict``,
    ``/prewarm-day-base``, and ``/prewarm-day-base-cache``."""

    predict_fn: PredictCategoryFn  # injected by make_handler_class
    parquet_payload_fn: ParquetPayloadFn  # injected by make_handler_class
    per_race_parquet_payload_fn: PerRaceParquetPayloadFn  # injected by make_handler_class
    rescore_factory: RescoreFactory | None  # injected by make_handler_class
    focused_full_completion_fn: FocusedFullCompletionFn | None  # injected by make_handler_class
    # injected by make_handler_class
    focused_full_cache_populate_fn: FocusedFullCachePopulateFn | None
    focused_full_cache_store: FocusedFullCacheStore | None  # injected by make_handler_class
    prewarm_fn: PrewarmBuildFn | None  # injected by make_handler_class
    prewarm_parquet_payload_fn: PrewarmParquetPayloadFn | None  # injected by make_handler_class
    prewarm_existing_object_fn: PrewarmExistingObjectFn | None  # injected by make_handler_class
    prewarm_commit_fn: PrewarmCommitFn | None  # injected by make_handler_class
    prewarm_background_fn: PrewarmBackgroundFn | None  # injected by make_handler_class

    @override
    def log_message(self, format: str, *args: object) -> None:
        # Redirect access log to stderr to avoid polluting stdout.
        _, query = parse_request_path(self.path)
        if query_debug_enabled(query):
            print(f"[predict-serve] {format % args}", file=sys.stderr, flush=True)

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
            rescore_fn: PredictCategoryFn | None = None
            rescore_per_race_fn: PerRaceParquetPayloadFn | None = None
            if result.mode == "rescore" and self.rescore_factory is not None:
                rescore_fn, rescore_per_race_fn = self.rescore_factory(
                    _scope_from_params(result),
                    result.rescore_cache_attestation,
                    result.weight_snapshot_generation,
                    result.race_start_at_jst,
                )
            effective_per_race_fn: PerRaceParquetPayloadFn | None = (
                rescore_per_race_fn
                if result.mode == "rescore" and rescore_per_race_fn is not None
                else self.per_race_parquet_payload_fn
            )
            callback_url = self.headers.get(FOCUSED_FULL_CALLBACK_HEADER)
            terminal_fn: FocusedFullTerminalFn | None = None
            if callback_url is not None:

                def _notify_terminal(
                    params: PredictParams,
                    status: Literal["success", "error"],
                    error: str | None,
                ) -> None:
                    post_focused_full_completion_callback(callback_url, params, status, error)

                terminal_fn = _notify_terminal
            for chunk in iter_predict_chunks(
                result,
                self.predict_fn,
                rescore_fn=rescore_fn,
                parquet_payload_fn=self.parquet_payload_fn,
                per_race_parquet_payload_fn=effective_per_race_fn,
                focused_full_completion_fn=self.focused_full_completion_fn,
                focused_full_cache_populate_fn=self.focused_full_cache_populate_fn,
                focused_full_terminal_fn=terminal_fn,
            ):
                # HTTP/1.1 chunked encoding: hex length + CRLF + data + CRLF
                size_line = f"{len(chunk):X}\r\n".encode()
                try:
                    self.wfile.write(size_line + chunk + b"\r\n")
                    self.wfile.flush()
                except OSError as write_err:
                    debug_log(f"[predict-serve] write error: {write_err}")
                    return

            # Terminating chunk
            try:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except OSError:
                pass
            return

        if path == "/focused-full-cache":
            cache_result = parse_focused_full_cache_query(query)
            if isinstance(cache_result, str):
                # Validation error — return 400 before writing any body.
                error_body = cache_result.encode()
                self.send_response(400)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(error_body)))
                self.end_headers()
                self.wfile.write(error_body)
                return

            payload = (
                self.focused_full_cache_store.peek(build_focused_full_race_key(cache_result))
                if self.focused_full_cache_store is not None
                else None
            )
            body = build_focused_full_cache_response_body(payload)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/focused-full-status":
            status_result = parse_focused_full_cache_query(query)
            if isinstance(status_result, str):
                error_body = status_result.encode()
                self.send_response(400)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(error_body)))
                self.end_headers()
                self.wfile.write(error_body)
                return
            body = build_focused_full_status_response_body(status_result)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/prewarm-day-base-cache":
            cache_params = parse_prewarm_params(query)
            if isinstance(cache_params, str):
                error_body = cache_params.encode()
                self.send_response(400)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(error_body)))
                self.end_headers()
                self.wfile.write(error_body)
                return
            payload = (
                self.focused_full_cache_store.peek(
                    build_prewarm_cache_key(cache_params.category, cache_params.run_date)
                )
                if self.focused_full_cache_store is not None
                else None
            )
            body = build_focused_full_cache_response_body(payload)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/prewarm-day-base-status":
            status_params = parse_prewarm_params(query)
            if isinstance(status_params, str):
                error_body = status_params.encode()
                self.send_response(400)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(error_body)))
                self.end_headers()
                self.wfile.write(error_body)
                return
            body = build_prewarm_status_response_body(status_params)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/prewarm-day-base":
            prewarm_result = parse_prewarm_params(query)
            if isinstance(prewarm_result, str):
                # Validation error — return 400 before writing any body.
                error_body = prewarm_result.encode()
                self.send_response(400)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(error_body)))
                self.end_headers()
                self.wfile.write(error_body)
                return

            if self.prewarm_fn is None:
                # Prewarm not wired (e.g. local/test run without the day-base
                # split configured) — 404 rather than silently no-op.
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            # Start 200 chunked response immediately so the DO renews its timeout
            # (same keepalive contract as /predict — see iter_prewarm_chunks).
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()

            cache_store = self.focused_full_cache_store

            def _invalidate_previous_payload(category: str, run_date: str) -> None:
                if cache_store is not None:
                    cache_store.pop(build_prewarm_cache_key(category, run_date))

            for chunk in iter_prewarm_chunks(
                prewarm_result,
                self.prewarm_fn,
                parquet_payload_fn=self.prewarm_parquet_payload_fn,
                existing_object_fn=self.prewarm_existing_object_fn,
                commit_fn=self.prewarm_commit_fn,
                background_fn=self.prewarm_background_fn,
                invalidate_fn=_invalidate_previous_payload,
            ):
                size_line = f"{len(chunk):X}\r\n".encode()
                try:
                    self.wfile.write(size_line + chunk + b"\r\n")
                    self.wfile.flush()
                except OSError as write_err:
                    debug_log(f"[predict-serve] write error: {write_err}")
                    return

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
    parquet_payload_fn: ParquetPayloadFn,
    per_race_parquet_payload_fn: PerRaceParquetPayloadFn,
    rescore_factory: RescoreFactory | None,
    focused_full_completion_fn: FocusedFullCompletionFn | None,
    prewarm_fn: PrewarmBuildFn | None = None,
    prewarm_parquet_payload_fn: PrewarmParquetPayloadFn | None = None,
    focused_full_cache_populate_fn: FocusedFullCachePopulateFn | None = None,
    focused_full_cache_store: FocusedFullCacheStore | None = None,
    prewarm_existing_object_fn: PrewarmExistingObjectFn | None = None,
    prewarm_commit_fn: PrewarmCommitFn | None = None,
    prewarm_background_fn: PrewarmBackgroundFn | None = None,
) -> type[_PredictHandler]:
    """Return a ``_PredictHandler`` subclass with bound callables.

    ``predict_fn`` / ``parquet_payload_fn`` / ``per_race_parquet_payload_fn`` are
    stored as ``staticmethod`` objects so Python's descriptor protocol does NOT
    inject ``self`` when accessed on an instance.  ``rescore_factory`` is invoked
    per request inside ``do_GET`` with the request's race scope, so it is also
    stored as a ``staticmethod`` to avoid the same ``self`` injection.
    ``focused_full_completion_fn`` follows the same optional-``staticmethod``
    pattern as ``rescore_factory`` since it may be ``None`` in tests / local runs.
    ``prewarm_fn`` / ``prewarm_parquet_payload_fn`` follow the same
    optional-``staticmethod`` pattern -- ``None`` when the day-base prewarm
    endpoint is not wired (e.g. a local run without R2 configured), in which
    case ``do_GET`` 404s ``/prewarm-day-base`` instead of erroring.
    ``focused_full_cache_populate_fn`` is the same optional-``staticmethod``
    pattern; ``focused_full_cache_store`` is a plain object reference (not a
    callable), so it needs no ``staticmethod`` wrapping -- both are ``None``
    when the ``GET /focused-full-cache`` pickup endpoint is not wired.
    """
    _predict: PredictCategoryFn = predict_fn
    _parquet_payload: ParquetPayloadFn = parquet_payload_fn
    _per_race_parquet_payload: PerRaceParquetPayloadFn = per_race_parquet_payload_fn
    _rescore_factory: RescoreFactory | None = rescore_factory
    _completion: FocusedFullCompletionFn | None = focused_full_completion_fn
    _prewarm: PrewarmBuildFn | None = prewarm_fn
    _prewarm_parquet_payload: PrewarmParquetPayloadFn | None = prewarm_parquet_payload_fn
    _cache_populate: FocusedFullCachePopulateFn | None = focused_full_cache_populate_fn
    _cache_store: FocusedFullCacheStore | None = focused_full_cache_store
    _prewarm_existing: PrewarmExistingObjectFn | None = prewarm_existing_object_fn
    _prewarm_commit: PrewarmCommitFn | None = prewarm_commit_fn
    _prewarm_background: PrewarmBackgroundFn | None = prewarm_background_fn

    @final
    class _BoundHandler(_PredictHandler):
        predict_fn = staticmethod(_predict)
        parquet_payload_fn = staticmethod(_parquet_payload)
        per_race_parquet_payload_fn = staticmethod(_per_race_parquet_payload)
        rescore_factory = staticmethod(_rescore_factory) if _rescore_factory is not None else None
        focused_full_completion_fn = staticmethod(_completion) if _completion is not None else None
        prewarm_fn = staticmethod(_prewarm) if _prewarm is not None else None
        prewarm_parquet_payload_fn = (
            staticmethod(_prewarm_parquet_payload) if _prewarm_parquet_payload is not None else None
        )
        focused_full_cache_populate_fn = (
            staticmethod(_cache_populate) if _cache_populate is not None else None
        )
        focused_full_cache_store = _cache_store
        prewarm_existing_object_fn = (
            staticmethod(_prewarm_existing) if _prewarm_existing is not None else None
        )
        prewarm_commit_fn = staticmethod(_prewarm_commit) if _prewarm_commit is not None else None
        prewarm_background_fn = (
            staticmethod(_prewarm_background) if _prewarm_background is not None else None
        )

    return _BoundHandler


def serve_http(
    port: int,
    predict_fn: PredictCategoryFn,
    parquet_payload_fn: ParquetPayloadFn,
    per_race_parquet_payload_fn: PerRaceParquetPayloadFn,
    rescore_factory: RescoreFactory | None = None,
    focused_full_completion_fn: FocusedFullCompletionFn | None = None,
    prewarm_fn: PrewarmBuildFn | None = None,
    prewarm_parquet_payload_fn: PrewarmParquetPayloadFn | None = None,
    focused_full_cache_populate_fn: FocusedFullCachePopulateFn | None = None,
    focused_full_cache_store: FocusedFullCacheStore | None = None,
    prewarm_existing_object_fn: PrewarmExistingObjectFn | None = None,
    prewarm_commit_fn: PrewarmCommitFn | None = None,
    prewarm_background_fn: PrewarmBackgroundFn | None = None,
) -> None:
    """Start the blocking HTTP server on *port*.

    This function never returns (the server runs until the process is killed).
    It is intentionally NOT covered by unit tests — it is the I/O-boundary glue
    that creates the real socket and blocks forever.  The pure logic it delegates
    to (``iter_predict_chunks``, ``parse_predict_params``, etc.) is fully tested
    in ``tests/test_serve.py``; the real-socket concurrency behaviour this
    function wires up (``ThreadingHTTPServer`` + ``make_handler_class``) is
    covered end-to-end in ``tests/test_predict_upcoming.py``.

    Uses ``ThreadingHTTPServer`` (not the single-threaded ``HTTPServer``) so a
    slow or hung request handler cannot block ``accept()`` for every other
    connection — a single-threaded server let one wedged ``/predict`` handler
    starve ``/ping`` health checks and OTHER races' focused-full busy-checks
    behind it, which Cloudflare's platform then killed for exceeding its
    connect-timeout ("Container is taking too long to accept the connection").
    ``predict_lib.serve._PIPELINE_EXEC_LOCK`` (not this server config) is what
    keeps concurrent handlers from corrupting the shared category-scoped
    ``pipeline_runner.WORK_DIR`` directories — see that lock's docstring.
    ``daemon_threads = True`` so a still-running handler thread never blocks
    process shutdown.
    """
    handler_cls = make_handler_class(
        predict_fn,
        parquet_payload_fn,
        per_race_parquet_payload_fn,
        rescore_factory,
        focused_full_completion_fn,
        prewarm_fn,
        prewarm_parquet_payload_fn,
        focused_full_cache_populate_fn,
        focused_full_cache_store,
        prewarm_existing_object_fn,
        prewarm_commit_fn,
        prewarm_background_fn,
    )
    httpd = http.server.ThreadingHTTPServer(("0.0.0.0", port), handler_cls)
    httpd.daemon_threads = True
    with httpd:
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
    serve_mode = _is_serve_mode(sys.argv)
    startup_mode = "serve" if serve_mode else "one-shot"
    print(f"[predict-startup] mode={startup_mode}", file=sys.stderr, flush=True)
    if serve_mode:
        try:
            database_url = normalise_database_url(_require_env(NEON_DATABASE_URL_ENV))
            source_url = resolve_source_url(os.environ.get(SOURCE_DATABASE_URL_ENV))
            models_dir = Path(os.environ.get(MODELS_DIR_ENV, "/models"))
        except BaseException as bootstrap_error:
            traceback.print_exc()
            print(f"[predict-serve] bootstrap failed: {bootstrap_error}", file=sys.stderr)
            return 1
        r2 = _load_r2_config()
        focused_full_cache_store = FocusedFullCacheStore()
        predict_fn, parquet_payload_fn, per_race_payload_fn, cache_populate_fn = _make_predict_fn(
            database_url, models_dir, source_url, r2, focused_full_cache_store
        )
        rescore_factory = _make_rescore_factory(database_url, models_dir, source_url, r2)
        # Production Container access to Neon is write-only. Completion/dedup
        # belongs to the Cloudflare Worker, which has Catalog, R2, KV, and DO
        # state available before dispatch. Never install the legacy completion
        # callback here: it SELECTed source and prediction rows from Neon for
        # every focused-full start, adding compute wakeups and several seconds
        # to the critical path.
        focused_full_completion_fn: FocusedFullCompletionFn | None = None
        prewarm_commit_fn = _make_prewarm_commit_fn(focused_full_cache_store)
        prewarm_fn = _make_prewarm_fn(source_url, r2, prewarm_commit_fn)
        print(
            f"[predict-startup] binding HTTP server on :{HTTP_PORT}",
            file=sys.stderr,
            flush=True,
        )
        serve_http(
            HTTP_PORT,
            predict_fn,
            parquet_payload_fn,
            per_race_payload_fn,
            rescore_factory,
            focused_full_completion_fn,
            prewarm_fn,
            _prewarm_parquet_payload,
            cache_populate_fn,
            focused_full_cache_store,
            _make_prewarm_existing_object_fn(r2, source_url),
            prewarm_commit_fn,
            run_prewarm_in_background,
        )
        return 0  # unreachable but satisfies the return type

    started = time.monotonic()
    _start_liveness_thread(LIVENESS_PORT)
    try:
        database_url = normalise_database_url(_require_env(NEON_DATABASE_URL_ENV))
        source_url = resolve_source_url(os.environ.get(SOURCE_DATABASE_URL_ENV))
        run_date = _require_env(RUN_DATE_ENV)
        days_ahead = int(os.environ.get(DAYS_AHEAD_ENV, str(DEFAULT_DAYS_AHEAD)))
        models_dir = Path(os.environ.get(MODELS_DIR_ENV, "/models"))
        window = PredictWindow(target_date=run_date, days_ahead=days_ahead, database_url=source_url)
        # Do not wake Neon merely to probe credentials. The first connection is
        # opened lazily at the output boundary and is used only for DDL/UPSERT
        # writes after Cloudflare-hosted feature generation has completed.
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
            races_predicted += predict_category(database_url, category, models_dir, window)
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
