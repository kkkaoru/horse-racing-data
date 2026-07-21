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
import os
import shutil
import socket
import sys
import threading
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast, final, get_args, override

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
    build_base_model_r2_key,
    card_max_race_bango_for_race_id,
    derive_card_max_race_bango_by_card,
    load_cell_router,
)
from predict_lib.conn_url import is_catalog_source_url, normalise_database_url, resolve_source_url
from predict_lib.dedupe import dedupe_batch
from predict_lib.ensemble_routing import catboost_model_feature_names, member_feature_order_matches
from predict_lib.etop2_override import apply_etop2_scores, is_etop2_override_active
from predict_lib.feature_guard import is_degenerate_feature_matrix
from predict_lib.focused_full_cache import FocusedFullCachePayload, FocusedFullCacheStore
from predict_lib.late_binding import OddsSnapshot
from predict_lib.model_meta import (
    CATEGORIES,
    JRA_ETOP2_ENABLED,
    JRA_ETOP2_MODEL_VERSION,
    JRA_ETOP2_XGB_MODEL_VERSION,
    METADATA_FILE_NAME,
    MODEL_FILE_NAME,
    NAR_ETOP2_MODEL_VERSION,
    NAR_TRANSFORMER_BLEND_ENABLED,
    NAR_TRANSFORMER_BLEND_WEIGHT,
    NAR_TRANSFORMER_MODEL_VERSION,
    Architecture,
    Category,
    architecture_for,
    assert_no_within_race_leak_columns,
    assert_production_model_version_allowed,
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
from predict_lib.r2_client import r2_get_parquet
from predict_lib.rescore import (
    RaceFreshSnapshot,
    RaceScope,
    apply_fresh_snapshots,
    filter_races_by_scope,
)
from predict_lib.scorer import BoosterLike, assert_feature_count, build_feature_matrix, score_matrix
from predict_lib.serve import (
    CacheMissError,
    FocusedFullCachePopulateFn,
    FocusedFullCompletionFn,
    ParquetPayloadFn,
    PerRaceParquetPayloadFn,
    PredictCategoryFn,
    PredictParams,
    PrewarmBuildFn,
    PrewarmParquetPayloadFn,
    R2Config,
    build_focused_full_cache_response_body,
    build_focused_full_race_key,
    build_r2_day_base_key,
    build_r2_feat_cache_key,
    build_r2_per_race_feat_cache_key,
    iter_predict_chunks,
    iter_prewarm_chunks,
    parse_focused_full_cache_query,
    parse_predict_params,
    parse_prewarm_params,
    parse_request_path,
)
from predict_lib.stage1_routing import (
    Stage1CategoryConfig,
    extract_predicted_scores,
    load_stage1_routing,
    resolve_stage1_gate,
)
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
FOCUSED_FULL_COMPLETION_CONNECT_TIMEOUT_SECONDS: int = 10
"""Connect timeout for the focused-full completion check against Neon.

Kept short (vs. the retrying ``connect_postgres_with_retry`` used for the
prediction UPSERT) because a completion-check failure is swallowed and
treated as "not complete" -- a slow/unreachable Neon must never delay
launching a genuine prediction pipeline."""
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
        print(
            f"[etop2] override fired race_id={race_id} class={class_code}",
            file=sys.stderr,
        )

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
        print(
            f"[nar-etop2] override fired race_id={race_id} class={nar_class}",
            file=sys.stderr,
        )

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


def score_races(
    races: Mapping[str, Sequence[Mapping[str, object]]],
    category: Category,
    models_dir: Path,
    feature_names: Sequence[str],
    card_max_race_bango: int | None = None,
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
    A category absent from ``stage1_routing.json`` (or a fallback artifact
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
    """
    fallback_booster = _load_booster(models_dir, category)
    cell_router = load_cell_router()
    variant_pool: dict[str, VariantModel] = {}
    if cell_router.has_routing(category):
        routing_config = cell_router.routing_for(category)
        for vname, vspec in routing_config.variants.items():
            if vname == routing_config.default_variant:
                continue  # the default variant is served by fallback_booster
            arch = _as_architecture(vspec.architecture)
            model_path = models_dir / build_base_model_r2_key(
                category, vspec.model_version, MODEL_FILE_NAME
            )
            booster = _load_booster_by_arch(model_path, arch)
            meta_path = models_dir / build_base_model_r2_key(
                category, vspec.model_version, METADATA_FILE_NAME
            )
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
            fnames = list(metadata["feature_names"])
            assert_production_model_version_allowed(
                vspec.model_version,
                context=f"cell-routing variant={vname} category={category}",
            )
            assert_no_within_race_leak_columns(
                fnames,
                context=f"cell-routing variant={vname} category={category}",
            )
            assert_feature_count(fnames, vspec.feature_count)
            _validate_variant_feature_contract(
                vname,
                vspec.model_version,
                fnames,
                vspec.feature_names,
                vspec.feature_set_hash,
            )
            if not _variant_booster_feature_order_matches(booster, arch, fnames):
                print(
                    f"[cell-routing] variant={vname} category={category} "
                    f"version={vspec.model_version} feature-order-mismatch: "
                    "booster's own trained column order disagrees with "
                    "metadata.json -> not loaded, races fall back to category default",
                    file=sys.stderr,
                )
                continue
            variant_pool[vname] = VariantModel(
                booster=booster,
                feature_names=fnames,
                architecture=arch,
                model_version=vspec.model_version,
            )
            print(
                f"[cell-routing] loaded variant={vname} category={category} "
                f"version={vspec.model_version} features={vspec.feature_count}",
                file=sys.stderr,
            )
    xgb_etop2_booster: BoosterLike | None = None
    if JRA_ETOP2_ENABLED and category == "jra":
        xgb_etop2_booster = _load_xgb_etop2_booster(models_dir)
    nar_transformer: TransformerScorer | None = None
    if NAR_TRANSFORMER_BLEND_ENABLED and category == "nar":
        nar_transformer = _load_nar_transformer(models_dir, feature_names)
    stage1_config = load_stage1_routing().get(category)
    stage1_model: VariantModel | None = None
    if stage1_config is not None and stage1_config.enabled:
        stage1_model = _load_stage1_model(models_dir, category, stage1_config)
    # See this function's docstring: an explicit caller-supplied value always
    # wins; batch self-derivation only ever runs (and is only ever correct)
    # when none was supplied, i.e. this is a whole-category request whose
    # ``races`` already spans every card it touches.
    card_max_race_bango_by_card = (
        derive_card_max_race_bango_by_card(races.keys())
        if variant_pool and card_max_race_bango is None
        else {}
    )
    scored: list[list[list[object]]] = []
    for race_id, entries in races.items():
        effective_booster = fallback_booster
        effective_feature_names = feature_names
        effective_architecture = architecture_for(category)
        cell_variant_model: VariantModel | None = None
        if variant_pool:
            effective_card_max_race_bango = (
                card_max_race_bango
                if card_max_race_bango is not None
                else card_max_race_bango_for_race_id(race_id, card_max_race_bango_by_card)
            )
            variant = cell_router.resolve_variant(
                category, entries, card_max_race_bango=effective_card_max_race_bango
            )
            if variant in variant_pool:
                vm = variant_pool[variant]
                effective_booster = vm.booster
                effective_feature_names = vm.feature_names
                effective_architecture = vm.architecture
                cell_variant_model = vm
                print(
                    f"[cell-routing] race={race_id} category={category} -> {variant}",
                    file=sys.stderr,
                )
            elif variant != cell_router.routing_for(category).default_variant:
                print(
                    f"[cell-routing] race={race_id} category={category} "
                    f"resolved missing variant={variant}; using default",
                    file=sys.stderr,
                )
        if cell_variant_model is not None:
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
            gate = resolve_stage1_gate(
                config=stage1_config,
                entries=entries,
                stage2_scores=extract_predicted_scores(rows),
            )
            if gate.use_stage1:
                print(
                    f"[stage1-gate] race={race_id} category={category} "
                    f"reason={gate.reason} stddev={gate.stddev} -> {stage1_model.model_version}",
                    file=sys.stderr,
                )
                rows = _score_one_race_direct(
                    stage1_model.booster,
                    race_id,
                    category,
                    entries,
                    stage1_model.feature_names,
                    stage1_model.architecture,
                    stage1_model.model_version,
                )
        scored.append(rows)
    return scored


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
        print(
            f"[feature-guard] race_id={race_id} category={category} "
            f"model_version={model_version} rejected: feature matrix mostly missing "
            "-> skipping write (self-heal will retry)",
            file=sys.stderr,
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
        print(
            f"[nar-transformer] load failed -> ensemble-only: {load_error}",
            file=sys.stderr,
        )
        return None
    known = set(feature_names)
    try:
        assert_no_within_race_leak_columns(
            transformer.feature_order,
            context=f"NAR transformer model_version={NAR_TRANSFORMER_MODEL_VERSION}",
        )
    except ValueError as leak_error:
        print(
            f"[nar-transformer] leak guard failed -> ensemble-only: {leak_error}",
            file=sys.stderr,
        )
        return None
    missing = [name for name in transformer.feature_order if name not in known]
    if missing:
        print(
            f"[nar-transformer] feature-contract gap ({len(missing)}) "
            f"-> ensemble-only: {missing[:5]}",
            file=sys.stderr,
        )
        return None
    print(
        f"[nar-transformer] loaded seeds={len(transformer.seeds)} "
        f"features={len(transformer.feature_order)} "
        f"version={NAR_TRANSFORMER_MODEL_VERSION}",
        file=sys.stderr,
    )
    return transformer


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
        print(
            f"[feature-guard] race_id={race_id} category=nar "
            "rejected: feature matrix mostly missing -> skipping write "
            "(self-heal will retry)",
            file=sys.stderr,
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
            print(
                f"[nar-transformer] race fail -> ensemble-only race_id={race_id}: {blend_error}",
                file=sys.stderr,
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
    card_max_race_bango: int | None = None,
) -> int:
    """Score ``races`` then UPSERT to Neon; the shared core of full + rescore.

    The races map is supplied by the caller — built from the 21y Neon scan on
    the full path, or read from the R2 / local feature cache (with the 5
    late-binding columns refreshed) on the rescore path. ``card_max_race_bango``
    is forwarded to :func:`score_races` untouched -- see that function's
    docstring for the whole-category-vs-single-race sourcing split.
    """
    feature_names = _load_model_metadata(models_dir, category)
    scored = score_races(
        races, category, models_dir, feature_names, card_max_race_bango=card_max_race_bango
    )
    return _flush_scored(database_url, category, scored)


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
    races = _build_feature_rows(category, window, target_race=target_race, r2_config=r2_config)
    return _score_and_flush_races(
        database_url, category, models_dir, races, card_max_race_bango=card_max_race_bango
    )


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
        assert_production_model_version_allowed(
            config.model_version, context=f"stage1 fallback category={category}"
        )
        architecture = _as_architecture(config.architecture)
        model_path = models_dir / build_base_model_r2_key(
            category, config.model_version, MODEL_FILE_NAME
        )
        booster = _load_booster_by_arch(model_path, architecture)
        meta_path = models_dir / build_base_model_r2_key(
            category, config.model_version, METADATA_FILE_NAME
        )
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        fnames = [str(name) for name in metadata["feature_names"]]
        assert_no_within_race_leak_columns(fnames, context=f"stage1 fallback category={category}")
        assert_feature_count(fnames, config.feature_count)
        if not _variant_booster_feature_order_matches(booster, architecture, fnames):
            raise ValueError(
                f"stage1 fallback category={category} version={config.model_version} "
                "booster's own trained column order disagrees with metadata.json"
            )
    except BaseException as load_error:
        print(
            f"[stage1-gate] category={category} version={config.model_version} "
            f"load failed -> Stage-1 fallback disabled this run: {load_error}",
            file=sys.stderr,
        )
        return None
    print(
        f"[stage1-gate] loaded category={category} version={config.model_version} "
        f"features={config.feature_count}",
        file=sys.stderr,
    )
    return VariantModel(
        booster=booster,
        feature_names=fnames,
        architecture=architecture,
        model_version=config.model_version,
    )


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
    """
    from pipeline_runner import (  # bundled in image
        build_upcoming_feature_rows,
        build_upcoming_feature_rows_split,
    )
    from predict_lib.pipeline_args import is_day_base_split_enabled

    if target_race is not None and is_day_base_split_enabled(category):
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


RACE_ID_KEIBAJO_INDEX: int = 3
RACE_ID_BANGO_INDEX: int = 4
RACE_ID_MIN_PARTS: int = 5


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
    Races whose ``race_id`` does not split into at least 5 parts are skipped.
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
            for race_id in race_ids:
                parts = race_id.split(":")
                if len(parts) < RACE_ID_MIN_PARTS:
                    print(
                        f"[predict-serve] per_race_parquet skip malformed race_id={race_id}",
                        file=sys.stderr,
                    )
                    continue
                keibajo_code = parts[RACE_ID_KEIBAJO_INDEX]
                race_bango = parts[RACE_ID_BANGO_INDEX]
                out_path = Path(tmp_dir) / f"{keibajo_code}_{race_bango}.parquet"
                con.execute(
                    "COPY (SELECT * FROM read_parquet(?, hive_partitioning = false) "
                    "WHERE race_id = ?) TO ? (FORMAT PARQUET)",
                    [glob_path, race_id, str(out_path)],
                )
                data = out_path.read_bytes()
                encoded = base64.b64encode(data).decode("ascii")
                parquet_key = build_r2_per_race_feat_cache_key(
                    category_str, run_date, keibajo_code, race_bango
                )
                payloads.append({"parquetBase64": encoded, "parquetKey": parquet_key})
    finally:
        con.close()
    print(
        f"[predict-serve] per_race_parquet ready races={len(payloads)} "
        f"category={category_str} run_date={run_date}",
        file=sys.stderr,
    )
    return payloads


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
      type's docstring for why this exists at all). Computes the SAME payload
      shape as the two functions above but from *params* directly, not from
      ``_last_run`` — see ``_build_parquet_payload``/``_build_per_race_payloads``
      below.

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
        from predict_lib.model_meta import resolve_category

        category = resolve_category(category_str)
        target_race = f"{keibajo_code}:{race_bango}" if keibajo_code and race_bango else None
        window = PredictWindow(target_date=run_date, days_ahead=days_ahead, database_url=source_url)
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
            print(
                f"[predict-serve] parquet_payload skip: no parquet in {final_dir}",
                file=sys.stderr,
            )
            return None
        local_path = parquet_files[0]
        parquet_key = build_r2_feat_cache_key(category_str, run_date)
        data = local_path.read_bytes()
        encoded = base64.b64encode(data).decode("ascii")
        print(
            f"[predict-serve] parquet_payload ready key={parquet_key} bytes={len(data)}",
            file=sys.stderr,
        )
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
        (non-blocking) when there is no parquet on disk or the split fails for
        any reason — a missing per-race cache must never fail predictions.
        """
        from pipeline_runner import WORK_DIR  # bundled in image

        final_dir = WORK_DIR / f"feat-{category_str}-v7-final"
        parquet_files = list(final_dir.rglob("*.parquet"))
        if not parquet_files:
            print(
                f"[predict-serve] per_race_parquet skip: no parquet in {final_dir}",
                file=sys.stderr,
            )
            return None
        try:
            return _split_parquet_by_race(final_dir, category_str, run_date)
        except BaseException as split_error:
            print(
                f"[predict-serve] per_race_parquet split failed: {split_error}",
                file=sys.stderr,
            )
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
        contract (best-effort, called before the slot is released).
        """
        if focused_full_cache_store is None:
            return
        category_str = params.category
        run_date = params.run_date
        payload = _build_parquet_payload(category_str, run_date)
        per_race = _build_per_race_payloads(category_str, run_date)
        if payload is None and per_race is None:
            return
        race_key = build_focused_full_race_key(params)
        focused_full_cache_store.put(
            race_key,
            FocusedFullCachePayload(
                parquet_base64=payload[0] if payload is not None else None,
                parquet_key=payload[1] if payload is not None else None,
                per_race_parquets=per_race,
            ),
        )

    return _predict, _parquet_payload, _per_race_parquet_payloads, _populate_focused_full_cache


def _make_prewarm_fn(database_url: str) -> PrewarmBuildFn:
    """Build the ``GET /prewarm-day-base`` build callable bound to ``database_url``.

    Mirrors ``_make_predict_fn``'s closure-binding pattern: ``serve.py`` stays
    I/O-free, so the real Neon URL is bound here (not passed through the query
    string) and ``pipeline_runner.build_day_base`` does the actual DuckDB base
    build + DAY_CHAIN layers.
    """

    def _prewarm(category_str: str, run_date: str, days_ahead: int) -> Path | None:
        from pipeline_runner import build_day_base  # bundled in image
        from predict_lib.model_meta import resolve_category

        category = resolve_category(category_str)
        return build_day_base(category, run_date, days_ahead, database_url)

    return _prewarm


def _prewarm_parquet_payload(
    category_str: str, run_date: str, day_base_dir: Path
) -> tuple[str, str] | None:
    """Read the day-base parquet and return ``(base64, R2 key)`` for the Worker
    DO proxy -- the ``PrewarmParquetPayloadFn`` injected into
    :func:`predict_lib.serve.iter_prewarm_chunks`. Returns ``None`` (non-blocking)
    when no parquet file is found under ``day_base_dir``.
    """
    parquet_files = list(day_base_dir.rglob("*.parquet"))
    if not parquet_files:
        print(
            f"[predict-serve] prewarm_parquet_payload skip: no parquet in {day_base_dir}",
            file=sys.stderr,
        )
        return None
    data = parquet_files[0].read_bytes()
    encoded = base64.b64encode(data).decode("ascii")
    parquet_key = build_r2_day_base_key(category_str, run_date)
    print(
        f"[predict-serve] prewarm_parquet_payload ready key={parquet_key} bytes={len(data)}",
        file=sys.stderr,
    )
    return encoded, parquet_key


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


def _fetch_watermarked_per_race_cache(
    final_dir: Path,
    category_str: str,
    run_date: str,
    scope: RaceScope,
    r2: R2Config,
    source_url: str,
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
        return False
    object_key = build_r2_per_race_feat_cache_key(
        category_str, run_date, scope.keibajo_code, scope.race_bango
    )
    candidate_dir = final_dir.parent / f"{final_dir.name}-per-race-candidate"
    candidate_path = candidate_dir / "features.parquet"
    try:
        shutil.rmtree(candidate_dir, ignore_errors=True)
        if not r2_get_parquet(r2, object_key, candidate_path):
            return False
        from pipeline_runner import day_base_covers_entry_list  # bundled in image
        from predict_lib.model_meta import resolve_category

        target_race = f"{scope.keibajo_code}:{scope.race_bango}"
        category = resolve_category(category_str)
        if not day_base_covers_entry_list(
            candidate_dir, category, run_date, target_race, source_url
        ):
            print(
                f"[predict-serve] rescore per-race cache watermark rejected "
                f"key={object_key} target_race={target_race}",
                file=sys.stderr,
            )
            return False
        shutil.rmtree(final_dir, ignore_errors=True)
        final_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(candidate_path, final_dir / "features.parquet")
        print(
            f"[predict-serve] rescore per-race cache watermark accepted key={object_key}",
            file=sys.stderr,
        )
        return True
    except BaseException as exc:
        print(
            f"[predict-serve] rescore per-race cache fetch failed key={object_key}: {exc}",
            file=sys.stderr,
        )
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


RescoreFactory = Callable[[RaceScope], tuple[PredictCategoryFn, PerRaceParquetPayloadFn]]
"""Builds a scope-bound rescore ``PredictCategoryFn`` + per-race payload fn for a request."""


def _make_rescore_fn(
    database_url: str,
    models_dir: Path,
    source_url: str,
    r2: R2Config | None,
    scope: RaceScope,
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
        if catalog_source:
            # Whole-category rescore (no single-race scope) has no per-race
            # object to watermark-check against -- unchanged, still fails
            # closed immediately. A single-race scope gets one real attempt
            # at the watermark-validated cache before falling back.
            fetched = False
            if scope.keibajo_code is not None and scope.race_bango is not None and r2 is not None:
                fetched = _fetch_watermarked_per_race_cache(
                    final_dir, category_str, run_date, scope, r2, source_url
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
        snapshots = _fetch_fresh_snapshots(category_str, run_date, race_keys)
        refreshed = apply_fresh_snapshots(races, snapshots, category)

        _write_refreshed_to_parquet(final_dir, refreshed)
        _last.clear()
        _last.append((category_str, run_date))

        scoped = filter_races_by_scope(refreshed, scope)
        return _score_and_flush_races(
            database_url, category, models_dir, scoped, card_max_race_bango=card_max_race_bango
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
            print(
                f"[predict-serve] rescore per_race_parquet split failed: {err}",
                file=sys.stderr,
            )
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

    def _factory(scope: RaceScope) -> tuple[PredictCategoryFn, PerRaceParquetPayloadFn]:
        return _make_rescore_fn(database_url, models_dir, source_url, r2, scope)

    return _factory


def _scope_from_params(params: PredictParams) -> RaceScope:
    """Build the race scope from the parsed ``keibajoCode`` / ``raceBango`` params."""
    return RaceScope(keibajo_code=params.keibajo_code, race_bango=params.race_bango)


def _expected_model_version_for_entries(
    category: Category,
    entries: Sequence[Mapping[str, object]],
    card_max_race_bango: int | None = None,
) -> str:
    """Return the model_version current production routing should write.

    A focused full run must not be skipped just because an older per-class/base
    model already scored the same race. Completion is tied to the same
    per-cell-first routing contract used by ``score_races``. ``card_max_race_bango``
    mirrors ``score_races``' single-race-scoped caller shape: this function is
    always called for exactly one race (see ``_focused_full_prediction_complete``,
    the only caller), so an explicit value -- never self-derivation, which
    would be wrong for a lone race -- is the only correct source here.
    """
    cell_router = load_cell_router()
    if cell_router.has_routing(category):
        routing = cell_router.routing_for(category)
        variant = cell_router.resolve_variant(
            category, entries, card_max_race_bango=card_max_race_bango
        )
        spec = routing.variants.get(variant)
        if spec is not None:
            return spec.model_version
    if category == "nar" and NAR_TRANSFORMER_BLEND_ENABLED:
        return NAR_TRANSFORMER_MODEL_VERSION
    return model_version_for(category)


def _focused_full_prediction_complete(database_url: str, params: PredictParams) -> bool:
    """Return True when race *params* already has complete predictions in Neon.

    Complete only when the model_version selected by current per-cell-first
    routing has scored as many distinct horses (ketto_toroku_bango) as
    race_entry_corner_features lists for the race. Best-effort: any error or
    zero expected rows returns False so a genuine prediction still launches.
    """
    keibajo_code = params.keibajo_code
    race_bango = params.race_bango
    if keibajo_code is None or race_bango is None:
        return False
    source = "jra" if params.category == "jra" else "nar"
    kaisai_nen = params.run_date[:4]
    kaisai_tsukihi = params.run_date[4:8]
    try:
        import psycopg

        conn = psycopg.connect(
            database_url, connect_timeout=FOCUSED_FULL_COMPLETION_CONNECT_TIMEOUT_SECONDS
        )
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                select distinct
                  ketto_toroku_bango,
                  grade_code,
                  track_code,
                  kyori,
                  kyoso_joken_code,
                  kaisai_tsukihi,
                  keibajo_code,
                  shusso_tosu
                from race_entry_corner_features
                where source = %s and kaisai_nen = %s and kaisai_tsukihi = %s
                  and keibajo_code = %s and race_bango = %s
                """,
                (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango),
            )
            expected_rows = cursor.fetchall()
            expected_horses = {str(row[0]).strip() for row in expected_rows if row[0] is not None}
            category = cast(Category, params.category)
            if not expected_horses:
                expected_model_version = _expected_model_version_for_entries(
                    category, [], card_max_race_bango=params.card_max_race_bango
                )
                cursor.execute(
                    """
                    select count(distinct ketto_toroku_bango)::int as actual_rows,
                           min(predicted_rank)::int as min_rank,
                           max(predicted_rank)::int as max_rank
                    from race_finish_position_model_predictions
                    where source = %s and kaisai_nen = %s and kaisai_tsukihi = %s
                      and keibajo_code = %s and race_bango = %s
                      and model_version = %s
                    """,
                    (
                        source,
                        kaisai_nen,
                        kaisai_tsukihi,
                        keibajo_code,
                        race_bango,
                        expected_model_version,
                    ),
                )
                existing_row = cursor.fetchone()
                if existing_row is None:
                    return False
                actual_rows = int(existing_row[0]) if existing_row[0] is not None else 0
                min_rank = int(existing_row[1]) if existing_row[1] is not None else 0
                max_rank = int(existing_row[2]) if existing_row[2] is not None else 0
                return actual_rows > 0 and min_rank == 1 and max_rank == actual_rows
            # race_id is synthesized (this table carries no such column) so the
            # is_final_race cell-routing dimension can resolve here exactly as
            # it would in score_races -- see resolve_dimension's race_id-based
            # race_bango decode.
            race_id = f"{source}:{kaisai_nen}:{kaisai_tsukihi}:{keibajo_code}:{race_bango}"
            entries = [
                {
                    "ketto_toroku_bango": row[0],
                    "grade_code": row[1],
                    "track_code": row[2],
                    "kyori": row[3],
                    "kyoso_joken_code": row[4],
                    "kaisai_tsukihi": row[5],
                    "keibajo_code": row[6],
                    "shusso_tosu": row[7],
                    "race_id": race_id,
                }
                for row in expected_rows
            ]
            expected_model_version = _expected_model_version_for_entries(
                category, entries, card_max_race_bango=params.card_max_race_bango
            )
            cursor.execute(
                """
                with expected as (
                  select distinct ketto_toroku_bango
                  from race_entry_corner_features
                  where source = %s and kaisai_nen = %s and kaisai_tsukihi = %s
                    and keibajo_code = %s and race_bango = %s
                )
                select count(distinct p.ketto_toroku_bango)::int as actual_rows
                from race_finish_position_model_predictions p
                join expected e on e.ketto_toroku_bango = p.ketto_toroku_bango
                where p.source = %s and p.kaisai_nen = %s and p.kaisai_tsukihi = %s
                  and p.keibajo_code = %s and p.race_bango = %s
                  and p.model_version = %s
                """,
                (
                    source,
                    kaisai_nen,
                    kaisai_tsukihi,
                    keibajo_code,
                    race_bango,
                    source,
                    kaisai_nen,
                    kaisai_tsukihi,
                    keibajo_code,
                    race_bango,
                    expected_model_version,
                ),
            )
            row = cursor.fetchone()
            if row is None:
                return False
            actual_rows = int(row[0]) if row[0] is not None else 0
            return actual_rows >= len(expected_horses)
        finally:
            conn.close()
    except Exception as exc:
        print(f"[predict-serve] completion check failed: {exc}", file=sys.stderr, flush=True)
        return False


def _make_focused_full_completion_fn(database_url: str) -> FocusedFullCompletionFn:
    def _fn(params: PredictParams) -> bool:
        return _focused_full_prediction_complete(database_url, params)

    return _fn


class _PredictHandler(http.server.BaseHTTPRequestHandler):
    """Minimal HTTP/1.1 request handler for ``/ping``, ``/predict``, and
    ``/prewarm-day-base``."""

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

    @override
    def log_message(self, format: str, *args: object) -> None:
        # Redirect access log to stderr to avoid polluting stdout.
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
            if self.rescore_factory is not None:
                rescore_fn, rescore_per_race_fn = self.rescore_factory(_scope_from_params(result))
            effective_per_race_fn: PerRaceParquetPayloadFn | None = (
                rescore_per_race_fn
                if result.mode == "rescore" and rescore_per_race_fn is not None
                else self.per_race_parquet_payload_fn
            )
            for chunk in iter_predict_chunks(
                result,
                self.predict_fn,
                rescore_fn=rescore_fn,
                parquet_payload_fn=self.parquet_payload_fn,
                per_race_parquet_payload_fn=effective_per_race_fn,
                focused_full_completion_fn=self.focused_full_completion_fn,
                focused_full_cache_populate_fn=self.focused_full_cache_populate_fn,
            ):
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
                self.focused_full_cache_store.pop(build_focused_full_race_key(cache_result))
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

            for chunk in iter_prewarm_chunks(
                prewarm_result,
                self.prewarm_fn,
                parquet_payload_fn=self.prewarm_parquet_payload_fn,
            ):
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
    if _is_serve_mode(sys.argv):
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
        focused_full_completion_fn = _make_focused_full_completion_fn(database_url)
        prewarm_fn = _make_prewarm_fn(source_url)
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
