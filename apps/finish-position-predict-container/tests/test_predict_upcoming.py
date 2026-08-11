"""Tests for the small pure helpers in ``predict_upcoming.py``.

``predict_upcoming.py`` itself is the I/O orchestration entrypoint (Neon TCP,
R2, DuckDB subprocess, native CatBoost / XGBoost load) and is excluded from
the ``--cov=predict_lib`` coverage gate per ``pyproject.toml``. The
per-category class-code extractor and the reconnect-on-write helpers are
structurally pure though — they are covered here without any real Neon I/O.

Tests here run alongside the predict_lib suite but do NOT count towards the
predict_lib coverage threshold; they are exclusively a correctness check for
the small helpers.
"""

from __future__ import annotations

import base64
import http.client
import http.server
import json
import sys
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import cast, final, override
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Import the cross-module helpers directly so the tests stay I/O-free.
import predict_lib.nar_etop2_override as nar_etop2_override
import predict_upcoming
from predict_lib.cell_router import CellRouter, build_base_model_r2_key
from predict_lib.focused_full_cache import FocusedFullCachePayload, FocusedFullCacheStore
from predict_lib.model_meta import (
    METADATA_FILE_NAME,
    NAR_ETOP2_MODEL_VERSION,
    Architecture,
    Category,
)
from predict_lib.rescore import RaceScope
from predict_lib.scorer import BoosterLike
from predict_lib.serve import (
    ParquetPayloadFn,
    PerRaceParquetPayloadFn,
    PredictCategoryFn,
    PredictParams,
    R2Config,
    activate_scoped_rescore_cache_miss_fallback,
    build_focused_full_race_key,
    iter_predict_chunks,
    parse_predict_params,
)
from predict_lib.stage1_routing import Stage1CategoryConfig
from predict_upcoming import (
    VariantModel,
    execute,
    extract_race_class_code,
    flush_predictions,
    make_handler_class,
    score_one_race_nar_etop2,
    score_races,
)

_LOAD_MODEL_METADATA_ATTR = "_load_model_metadata"
_LOAD_NAR_TRANSFORMER_ATTR = "_load_nar_transformer"
_LOAD_STAGE1_MODEL_ATTR = "_load_stage1_model"
_BUILD_FEATURE_ROWS_ATTR = "_build_feature_rows"
_MAKE_PREWARM_FN_ATTR = "_make_prewarm_fn"
_MAKE_RESCORE_FN_ATTR = "_make_rescore_fn"
_PREWARM_PARQUET_PAYLOAD_ATTR = "_prewarm_parquet_payload"
_ENSURE_CACHED_PARQUET_ATTR = "_ensure_cached_parquet"
_load_model_metadata = cast(
    Callable[[Path, Category], Sequence[str]],
    getattr(predict_upcoming, _LOAD_MODEL_METADATA_ATTR),
)
_load_nar_transformer = cast(
    Callable[[Path, Sequence[str]], object | None],
    getattr(predict_upcoming, _LOAD_NAR_TRANSFORMER_ATTR),
)
_load_stage1_model = cast(
    Callable[[Path, Category, Stage1CategoryConfig], VariantModel | None],
    getattr(predict_upcoming, _LOAD_STAGE1_MODEL_ATTR),
)
_build_feature_rows = cast(
    Callable[..., Mapping[str, list[Mapping[str, object]]]],
    getattr(predict_upcoming, _BUILD_FEATURE_ROWS_ATTR),
)
_make_prewarm_fn = cast(
    Callable[[str, R2Config | None], Callable[[str, str, int], Path | None]],
    getattr(predict_upcoming, _MAKE_PREWARM_FN_ATTR),
)
_make_rescore_fn = cast(
    Callable[
        [str, Path, str, R2Config | None, RaceScope],
        tuple[PredictCategoryFn, PerRaceParquetPayloadFn],
    ],
    getattr(predict_upcoming, _MAKE_RESCORE_FN_ATTR),
)
_prewarm_parquet_payload = cast(
    Callable[[str, str, Path], tuple[str, str, Mapping[str, str | int] | None] | None],
    getattr(predict_upcoming, _PREWARM_PARQUET_PAYLOAD_ATTR),
)
_ensure_cached_parquet = cast(
    Callable[[Path, str, str, R2Config | None], None],
    getattr(predict_upcoming, _ENSURE_CACHED_PARQUET_ATTR),
)
_FETCH_WATERMARKED_PER_RACE_CACHE_ATTR = "_fetch_watermarked_per_race_cache"
_fetch_watermarked_per_race_cache = cast(
    Callable[[Path, str, str, RaceScope, R2Config, str], bool],
    getattr(predict_upcoming, _FETCH_WATERMARKED_PER_RACE_CACHE_ATTR),
)
_VARIANT_BOOSTER_FEATURE_ORDER_MATCHES_ATTR = "_variant_booster_feature_order_matches"
_variant_booster_feature_order_matches = cast(
    Callable[[BoosterLike, Architecture, Sequence[str]], bool],
    getattr(predict_upcoming, _VARIANT_BOOSTER_FEATURE_ORDER_MATCHES_ATTR),
)
_SCORE_ONE_RACE_DIRECT_ATTR = "_score_one_race_direct"
_score_one_race_direct = cast(
    Callable[
        [
            BoosterLike,
            str,
            Category,
            Sequence[Mapping[str, object]],
            Sequence[str],
            Architecture,
            str,
        ],
        list[list[object]],
    ],
    getattr(predict_upcoming, _SCORE_ONE_RACE_DIRECT_ATTR),
)

# ---------------------------------------------------------------------------
# Minimal stub connection
# ---------------------------------------------------------------------------


@final
class _StubCursor:
    """Minimal cursor stub that records the last execute call."""

    def __init__(self) -> None:
        self.last_sql: str = ""
        self.last_params: object = None

    def execute(self, query: str, params: object = None) -> object:
        self.last_sql = query
        self.last_params = params
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class _StubConnection:
    """Minimal connection stub that records commits, rollbacks, and closes."""

    committed: int
    rolledback: int
    closed: bool
    _raise_on_execute: Exception | None
    _cursor: _StubCursor

    def __init__(self, raise_on_execute: Exception | None = None) -> None:
        self.committed = 0
        self.rolledback = 0
        self.closed = False
        self._raise_on_execute = raise_on_execute
        self._cursor = _StubCursor()

    def cursor(self) -> _StubCursor:
        if self._raise_on_execute is not None:
            raise self._raise_on_execute
        return self._cursor

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolledback += 1

    def close(self) -> None:
        self.closed = True


def testextract_race_class_code_jra_returns_kyoso_joken_code() -> None:
    entries = [
        {"kyoso_joken_code": "005", "umaban": 1},
        {"kyoso_joken_code": "005", "umaban": 2},
    ]
    assert extract_race_class_code("jra", entries) == "005"


def testextract_race_class_code_jra_returns_none_when_missing() -> None:
    # JRA entry without the kyoso_joken_code field -> None.
    entries = [{"umaban": 1}]
    assert extract_race_class_code("jra", entries) is None


def testextract_race_class_code_jra_returns_none_for_empty_string() -> None:
    # PG returns the empty string for some legacy races; we collapse it to
    # None for legacy diagnostics.
    entries = [{"kyoso_joken_code": "  ", "umaban": 1}]
    assert extract_race_class_code("jra", entries) is None


def testextract_race_class_code_jra_strips_whitespace() -> None:
    entries = [{"kyoso_joken_code": " 703 ", "umaban": 1}]
    assert extract_race_class_code("jra", entries) == "703"


def testextract_race_class_code_nar_returns_nar_subclass() -> None:
    # Phase F: NAR reads ``nar_subclass`` rather than ``kyoso_joken_code``.
    entries = [
        {"nar_subclass": "NEW", "umaban": 1},
        {"nar_subclass": "NEW", "umaban": 2},
    ]
    assert extract_race_class_code("nar", entries) == "NEW"


def testextract_race_class_code_nar_returns_each_named_subclass() -> None:
    # All six NAR sub-classes pass through verbatim — the extractor reads from
    # the first entry only since all entries of a race share the same class.
    for subclass in ("NEW", "MUKATSU", "C", "B", "A", "OP"):
        entries = [{"nar_subclass": subclass, "umaban": 1}]
        assert extract_race_class_code("nar", entries) == subclass


def testextract_race_class_code_nar_returns_other_when_no_meisho_match() -> None:
    # The DuckDB build emits ``"other"`` literal when nothing matches the
    # nar_subclass_case_sql regex — pass it through verbatim so the resolver
    # routes to the NAR ``other`` ensemble.
    entries = [{"nar_subclass": "other", "umaban": 1}]
    assert extract_race_class_code("nar", entries) == "other"


def testextract_race_class_code_nar_returns_none_when_field_absent() -> None:
    # A NAR entry without the nar_subclass field collapses to None — the
    # router then falls back to the NAR iter 12 baseline.
    entries = [{"umaban": 1}]
    assert extract_race_class_code("nar", entries) is None


def testextract_race_class_code_nar_returns_none_when_field_null() -> None:
    entries = [{"nar_subclass": None, "umaban": 1}]
    assert extract_race_class_code("nar", entries) is None


def testextract_race_class_code_banei_returns_none_by_dispatch() -> None:
    # Ban-ei is NOT in the per-category dispatch map — the extractor returns
    # None regardless of whether the columns exist so the resolver
    # short-circuits to the Ban-ei category-global model.
    entries = [
        {"kyoso_joken_code": "BAN", "nar_subclass": "other", "umaban": 1},
    ]
    assert extract_race_class_code("ban-ei", entries) is None


def testextract_race_class_code_returns_none_for_empty_entries() -> None:
    # No entries -> nothing to read; safe None.
    assert extract_race_class_code("jra", []) is None
    assert extract_race_class_code("nar", []) is None


def testextract_race_class_code_coerces_non_string_value() -> None:
    # Defensive: numeric class code (DuckDB sometimes emits int for the JRA
    # numeric codes) is str-coerced before strip + return so the resolver
    # sees a clean string key.
    entries = [{"kyoso_joken_code": 703, "umaban": 1}]
    assert extract_race_class_code("jra", entries) == "703"


# ---------------------------------------------------------------------------
# execute — reconnect-on-write
# ---------------------------------------------------------------------------

_DB_URL = "postgresql://host/db"


def test_execute_succeeds_on_happy_path() -> None:
    # Normal path: execute+commit returns the same connection unchanged.
    conn = _StubConnection()
    result = execute(conn, "SELECT 1", [], _DB_URL)
    assert result is conn
    assert conn.committed == 1
    assert conn.rolledback == 0


def test_execute_non_transient_error_propagates_without_reconnect() -> None:
    # Non-transient errors (e.g. programming error) must NOT trigger a reconnect.
    auth_exc = Exception("password authentication failed")
    conn = _StubConnection(raise_on_execute=auth_exc)
    try:
        execute(conn, "SELECT 1", [], _DB_URL)
    except Exception as exc:
        assert exc is auth_exc
    else:
        raise AssertionError("should have raised")


def test_execute_reconnects_and_retries_on_admin_shutdown() -> None:
    # AdminShutdown mid-write: old connection should be rolled back + closed;
    # a fresh connection should be used for the retry and returned.
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    bad_conn = _StubConnection(raise_on_execute=admin_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn) as mock_connect:
        result = execute(bad_conn, "INSERT ...", ["p"], _DB_URL)

    assert result is fresh_conn
    assert fresh_conn.committed == 1
    mock_connect.assert_called_once_with(_DB_URL)
    # Old connection was asked to rollback + close (both may fail gracefully).


def test_execute_reconnects_on_connection_is_lost() -> None:
    lost_exc = Exception("the connection is lost")
    bad_conn = _StubConnection(raise_on_execute=lost_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn):
        result = execute(bad_conn, "INSERT ...", [], _DB_URL)

    assert result is fresh_conn
    assert fresh_conn.committed == 1


def test_execute_retry_failure_propagates() -> None:
    # If the reconnect attempt also raises, that error propagates to the caller.
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    bad_conn = _StubConnection(raise_on_execute=admin_exc)
    also_bad_conn = _StubConnection(raise_on_execute=RuntimeError("retry also failed"))

    with patch("predict_upcoming._connect", return_value=also_bad_conn):
        try:
            execute(bad_conn, "INSERT ...", [], _DB_URL)
        except RuntimeError as exc:
            assert "retry also failed" in str(exc)
        else:
            raise AssertionError("should have raised")


def test_execute_rollback_failure_is_swallowed() -> None:
    # If rollback also raises (connection already dead), the reconnect still
    # proceeds and the overall execute still succeeds on the fresh connection.
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")

    class _FailRollbackConn(_StubConnection):
        @override
        def rollback(self) -> None:
            raise RuntimeError("rollback also failed")

    bad_conn = _FailRollbackConn(raise_on_execute=admin_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn):
        result = execute(bad_conn, "INSERT ...", [], _DB_URL)

    assert result is fresh_conn
    assert fresh_conn.committed == 1


# ---------------------------------------------------------------------------
# flush_predictions — per-race dedup + reconnect propagation
# ---------------------------------------------------------------------------


def _make_pred_row(race_id: str, ketto: str) -> list[object]:
    # A prediction row whose structure satisfies _row_to_pk_map:
    #   index 0     — placeholder (not used by _row_to_pk_map)
    #   indices 1-5 — race_id parts joined by ":" to form the race_id key
    #   index 6     — ketto_toroku_bango
    # race_id is expected to be "p1:p2:p3:p4:p5" (5 colon-separated parts).
    parts = race_id.split(":")
    assert len(parts) == 5, f"race_id must have exactly 5 parts, got {parts}"
    return ["placeholder", parts[0], parts[1], parts[2], parts[3], parts[4], ketto]


def test_flush_predictions_empty_rows_returns_zero() -> None:
    conn = _StubConnection()
    written, returned_conn = flush_predictions(conn, [], _DB_URL)
    assert written == 0
    assert returned_conn is conn
    assert conn.committed == 0


def test_flush_predictions_writes_rows_and_returns_connection() -> None:
    conn = _StubConnection()
    rows = [_make_pred_row("20260619:05:11:01:01", "HORSE1")]
    written, returned_conn = flush_predictions(conn, rows, _DB_URL)
    assert written == 1
    assert returned_conn is conn
    assert conn.committed >= 1


def test_flush_predictions_returns_fresh_conn_after_reconnect() -> None:
    # Simulate AdminShutdown on first execute call; verify the returned
    # connection is the fresh one (not the original dead conn).
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    dead_conn = _StubConnection(raise_on_execute=admin_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn):
        rows = [_make_pred_row("20260619:05:11:01:01", "HORSE1")]
        written, returned_conn = flush_predictions(dead_conn, rows, _DB_URL)

    assert returned_conn is fresh_conn
    assert written == 1
    assert fresh_conn.committed >= 1


# ---------------------------------------------------------------------------
# make_handler_class — staticmethod binding (regression for 4-arg TypeError)
# ---------------------------------------------------------------------------
#
# Python's descriptor protocol makes plain function class attributes behave as
# bound methods when accessed on an instance, injecting ``self`` as the first
# argument.  This caused a ``TypeError`` in production:
#   _make_predict_fn.<locals>._predict() takes 3 positional arguments but 4
#   were given
# because ``self.predict_fn(category, run_date, days_ahead)`` was dispatched as
# ``predict_fn(self, category, run_date, days_ahead)``.
#
# The fix wraps the callables with ``staticmethod`` at class-definition time.
# These tests pin that contract: the class attributes must remain plain
# callables callable without any instance, i.e. NOT bound methods.


def _fake_predict(
    category: str,
    run_date: str,
    days_ahead: int,
    keibajo_code: str | None = None,
    race_bango: str | None = None,
    card_max_race_bango: int | None = None,
) -> int:
    """Dummy predict_fn that returns the length of category as a sentinel."""
    return len(category)


def _fake_parquet_payload() -> tuple[str, str] | None:
    """Dummy parquet_payload_fn that returns None (no parquet available)."""
    return None


def _fake_per_race_parquet_payload() -> list[dict[str, str]] | None:
    """Dummy per_race_parquet_payload_fn that returns None (no per-race split)."""
    return None


def _fake_rescore(
    category: str,
    run_date: str,
    days_ahead: int,
    keibajo_code: str | None = None,
    race_bango: str | None = None,
    card_max_race_bango: int | None = None,
) -> int:
    """Dummy rescore_fn that returns a fixed sentinel value."""
    return 99


def _fake_rescore_factory(
    scope: RaceScope,
) -> tuple[PredictCategoryFn, PerRaceParquetPayloadFn]:
    """Dummy rescore_factory that ignores the scope and returns a fixed fn + payload fn."""
    del scope

    def _per_race() -> list[dict[str, str]] | None:
        return None

    return _fake_rescore, _per_race


def test_make_handler_class_predict_fn_callable_without_instance() -> None:
    """predict_fn on the handler class must be callable as a plain 3-arg function."""
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
    )
    # Call directly on the class (no instance) — must NOT inject self.
    result = handler_cls.predict_fn("nar", "20260618", 0, None, None, None)
    assert result == len("nar")


def test_make_handler_class_parquet_payload_fn_callable_without_instance() -> None:
    """parquet_payload_fn stored as staticmethod must be callable without an instance."""
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
    )
    fn: ParquetPayloadFn = handler_cls.__dict__["parquet_payload_fn"].__func__
    result = fn()
    assert result is None


def test_make_handler_class_per_race_parquet_payload_fn_callable_without_instance() -> None:
    """per_race_parquet_payload_fn stored as staticmethod must be callable without an instance."""
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
    )
    fn: PerRaceParquetPayloadFn = handler_cls.__dict__["per_race_parquet_payload_fn"].__func__
    result = fn()
    assert result is None


def test_make_handler_class_rescore_factory_callable_without_instance() -> None:
    """rescore_factory on the handler class must be callable without an instance."""
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
    )
    factory = handler_cls.rescore_factory
    assert factory is not None
    rescore, per_race = factory(RaceScope())
    result = rescore("jra", "20260618", 1, None, None, None)
    assert result == 99
    assert per_race() is None


def test_make_handler_class_rescore_factory_none_when_not_provided() -> None:
    """When rescore_factory=None, the class attribute must also be None."""
    handler_cls = make_handler_class(
        _fake_predict, _fake_parquet_payload, _fake_per_race_parquet_payload, None, None
    )
    assert handler_cls.rescore_factory is None


def test_make_handler_class_predict_fn_not_bound_method() -> None:
    """Accessing predict_fn on the class must NOT produce a bound method."""
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
    )
    import inspect

    # A bound method has a __self__; a staticmethod result does not.
    assert not inspect.ismethod(handler_cls.predict_fn), (
        "predict_fn must not be a bound method — staticmethod wrapping is required"
    )


def test_make_handler_class_predict_fn_accepts_exactly_6_args() -> None:
    """Verify predict_fn exposes the 6-arg contract and no injected ``self``.

    The staticmethod wrapping must keep the signature at exactly six parameters
    (category, run_date, days_ahead, keibajo_code, race_bango,
    card_max_race_bango) — if ``self`` were injected by the descriptor protocol
    the count would be seven.
    """
    import inspect

    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
    )
    sig = inspect.signature(handler_cls.predict_fn)
    params = list(sig.parameters.values())
    assert len(params) == 6, (
        f"predict_fn must have exactly 6 parameters "
        f"(category, run_date, days_ahead, keibajo_code, race_bango, "
        f"card_max_race_bango), got {len(params)}: {[p.name for p in params]}"
    )


# ---------------------------------------------------------------------------
# serve_http concurrency — real ThreadingHTTPServer + real sockets
# ---------------------------------------------------------------------------
#
# serve_http() itself is I/O-boundary glue (real socket, blocking
# serve_forever()) and is excluded from the coverage gate, but the
# concurrency behaviour it wires up -- ThreadingHTTPServer + make_handler_class
# -- is exactly what the 2026-07-11/12 incident needed: a slow /predict
# handler must not block /ping or another race's request from being accepted
# and answered. A single-threaded http.server.HTTPServer let one wedged
# handler starve every other queued connection until Cloudflare's platform
# connect-timeout (~6s) killed them. These tests spin up a REAL
# http.server.ThreadingHTTPServer on 127.0.0.1 (ephemeral port) with the
# production handler_cls and drive it over real sockets to prove the fix.


def _start_threading_server(
    predict_fn: PredictCategoryFn,
) -> tuple[http.server.ThreadingHTTPServer, threading.Thread, int]:
    """Start a real ThreadingHTTPServer with the production handler_cls."""
    handler_cls = make_handler_class(
        predict_fn,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        None,
        None,
    )
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    httpd.daemon_threads = True
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread, port


def _stop_threading_server(
    httpd: http.server.ThreadingHTTPServer, thread: threading.Thread
) -> None:
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=2.0)


def _get(port: int, path: str, timeout: float = 5.0) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        return response.status, response.read()
    finally:
        conn.close()


def test_threading_server_ping_stays_responsive_during_slow_batch_predict() -> None:
    """A slow batch (day-level, non-focused) ``mode=full`` predict_fn call
    blocks its OWN handler thread for its whole duration (that path is not
    detached, unlike focused-full). Under the old single-threaded
    ``http.server.HTTPServer`` this would also block every OTHER queued
    connection's ``accept()`` -- reproducing the incident shape where
    Cloudflare killed unrelated connections for exceeding its connect-timeout.
    ``ThreadingHTTPServer`` must keep ``/ping`` answering fast regardless.
    """
    started = threading.Event()
    release = threading.Event()

    def _slow_predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        started.set()
        release.wait(timeout=5.0)
        return 1

    httpd, thread, port = _start_threading_server(_slow_predict)
    try:
        errors: list[BaseException] = []

        def _fire_batch_predict() -> None:
            try:
                _get(
                    port,
                    "/predict?category=jra&runDate=20260619&daysAhead=0&mode=full",
                    timeout=5.0,
                )
            except BaseException as exc:  # collected below, not raised on this thread
                errors.append(exc)

        batch_thread = threading.Thread(target=_fire_batch_predict)
        batch_thread.start()
        try:
            assert started.wait(timeout=2.0), "slow batch predict_fn never started"

            ping_started = perf_counter()
            status, body = _get(port, "/ping", timeout=2.0)
            ping_elapsed = perf_counter() - ping_started
            assert status == 200
            assert body == b"ok"
            assert ping_elapsed < 1.0, (
                f"/ping took {ping_elapsed:.2f}s while a slow /predict handler "
                "was in flight -- accept()/handler concurrency regressed"
            )
        finally:
            release.set()
            batch_thread.join(timeout=5.0)
        assert errors == []
    finally:
        _stop_threading_server(httpd, thread)


def test_threading_server_focused_full_accept_stays_fast_while_unrelated_predict_in_flight() -> (
    None
):
    """A focused-full request's own accept()/NDJSON response must stay fast
    even while an UNRELATED slow batch predict_fn call occupies another
    handler thread (and, via ``_PIPELINE_EXEC_LOCK``, is the current holder of
    the process-wide pipeline-execution lock). The focused-full response only
    waits for the single-process SLOT claim (independent of
    ``_PIPELINE_EXEC_LOCK``) -- its own detached pipeline execution correctly
    queues behind the lock, but the HTTP response reporting status='accepted'
    must not.
    """
    batch_started = threading.Event()
    batch_release = threading.Event()

    def _slow_predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        batch_started.set()
        batch_release.wait(timeout=5.0)
        return 1

    httpd, thread, port = _start_threading_server(_slow_predict)
    try:
        errors: list[BaseException] = []

        def _fire_batch_predict() -> None:
            try:
                _get(
                    port,
                    "/predict?category=jra&runDate=20260619&daysAhead=0&mode=full",
                    timeout=5.0,
                )
            except BaseException as exc:  # collected below, not raised on this thread
                errors.append(exc)

        batch_thread = threading.Thread(target=_fire_batch_predict)
        batch_thread.start()
        try:
            assert batch_started.wait(timeout=2.0), "slow batch predict_fn never started"

            focused_started = perf_counter()
            status, body = _get(
                port,
                "/predict?category=jra&runDate=20260619&daysAhead=0"
                "&mode=full&keibajoCode=05&raceBango=01",
                timeout=2.0,
            )
            focused_elapsed = perf_counter() - focused_started
            assert status == 200
            last_line = body.strip().splitlines()[-1]
            assert json.loads(last_line)["status"] == "accepted"
            assert focused_elapsed < 1.0, (
                f"focused-full request took {focused_elapsed:.2f}s while an "
                "unrelated slow batch predict_fn held another handler thread"
            )
        finally:
            batch_release.set()
            batch_thread.join(timeout=5.0)
        assert errors == []
    finally:
        _stop_threading_server(httpd, thread)


def test_threading_server_focused_full_busy_check_stays_fast_during_slow_pipeline() -> None:
    """Two REAL concurrent focused-full requests for DIFFERENT races in the
    same category, driven over real sockets: the second must get a fast
    ``status='busy'`` response while the first race's detached pipeline is
    still in flight -- the single-process slot's fast-reject semantics
    (already unit-tested at the ``iter_predict_chunks`` layer) must survive
    the switch to real handler concurrency end-to-end.
    """
    a_started = threading.Event()
    a_release = threading.Event()

    def _slow_predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        a_started.set()
        a_release.wait(timeout=5.0)
        return 1

    httpd, thread, port = _start_threading_server(_slow_predict)
    try:
        first_status, first_body = _get(
            port,
            "/predict?category=jra&runDate=20260619&daysAhead=0&mode=full&keibajoCode=05&raceBango=01",
            timeout=5.0,
        )
        assert first_status == 200
        assert json.loads(first_body.strip().splitlines()[-1])["status"] == "accepted"
        assert a_started.wait(timeout=2.0), "race A's detached pipeline never started"

        try:
            busy_started = perf_counter()
            second_status, second_body = _get(
                port,
                "/predict?category=jra&runDate=20260619&daysAhead=0"
                "&mode=full&keibajoCode=05&raceBango=02",
                timeout=2.0,
            )
            busy_elapsed = perf_counter() - busy_started
            assert second_status == 200
            second_last_line = second_body.strip().splitlines()[-1]
            assert json.loads(second_last_line)["status"] == "busy"
            assert busy_elapsed < 1.0, (
                f"busy-check took {busy_elapsed:.2f}s while race A's pipeline "
                "was in flight -- slot fast-reject semantics regressed"
            )
        finally:
            a_release.set()
    finally:
        _stop_threading_server(httpd, thread)


# ---------------------------------------------------------------------------
# GET /focused-full-cache route -- real socket, drives do_GET's actual
# dispatch (not just make_handler_class's staticmethod binding) since this
# session's own investigation found that "wiring compiles" and "wiring
# actually runs end to end" can silently diverge (see FocusedFullCachePayload
# / FocusedFullCachePopulateFn's docstrings for the incident this endpoint
# exists to fix).
# ---------------------------------------------------------------------------


def _start_server_with_cache_store(
    store: FocusedFullCacheStore | None,
) -> tuple[http.server.ThreadingHTTPServer, threading.Thread, int]:
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        None,
        None,
        focused_full_cache_store=store,
    )
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    httpd.daemon_threads = True
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread, port


def test_focused_full_cache_route_returns_found_true_and_consumes_entry() -> None:
    store = FocusedFullCacheStore()
    store.put(
        "jra:20260712:05:09",
        FocusedFullCachePayload(
            parquet_base64="YQ==",
            parquet_key="feat-cache/jra/20260712/05/09/features.parquet",
            per_race_parquets=None,
        ),
    )
    httpd, thread, port = _start_server_with_cache_store(store)
    try:
        status, body = _get(
            port,
            "/focused-full-cache?category=jra&runDate=20260712&keibajoCode=05&raceBango=09",
        )
        assert status == 200
        assert json.loads(body.decode()) == {
            "found": True,
            "parquetBase64": "YQ==",
            "parquetKey": "feat-cache/jra/20260712/05/09/features.parquet",
            "perRaceParquets": None,
        }

        # A second pickup for the same race must find nothing -- pop() consumes.
        status_again, body_again = _get(
            port,
            "/focused-full-cache?category=jra&runDate=20260712&keibajoCode=05&raceBango=09",
        )
        assert status_again == 200
        assert json.loads(body_again.decode()) == {"found": False}
    finally:
        _stop_threading_server(httpd, thread)


def test_focused_full_cache_route_400_on_invalid_query() -> None:
    httpd, thread, port = _start_server_with_cache_store(FocusedFullCacheStore())
    try:
        status, body = _get(port, "/focused-full-cache?runDate=20260712")
        assert status == 400
        assert b"category" in body
    finally:
        _stop_threading_server(httpd, thread)


def test_focused_full_cache_route_found_false_when_store_not_wired() -> None:
    """A local/test run without a cache store injected must degrade to
    found=false, never a 500 -- matches the endpoint's non-blocking contract."""
    httpd, thread, port = _start_server_with_cache_store(None)
    try:
        status, body = _get(
            port,
            "/focused-full-cache?category=jra&runDate=20260712&keibajoCode=05&raceBango=09",
        )
        assert status == 200
        assert json.loads(body.decode()) == {"found": False}
    finally:
        _stop_threading_server(httpd, thread)


# ---------------------------------------------------------------------------
# make_handler_class — /prewarm-day-base wiring
# ---------------------------------------------------------------------------


def _fake_prewarm(category: str, run_date: str, days_ahead: int) -> Path | None:
    """Dummy prewarm build fn that returns a deterministic sentinel path."""
    return Path(f"/tmp/daybase-{category}-{run_date}-{days_ahead}")


def _fake_prewarm_parquet_payload(
    category: str, run_date: str, day_base_dir: Path
) -> tuple[str, str, Mapping[str, str | int] | None] | None:
    return "cGF5bG9hZA==", f"feat-daybase/{category}/{run_date}/features.parquet", None


def test_make_handler_class_prewarm_fn_callable_without_instance() -> None:
    """prewarm_fn on the handler class must be callable as a plain 3-arg function."""
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
        _fake_prewarm,
        _fake_prewarm_parquet_payload,
    )
    prewarm_fn = handler_cls.prewarm_fn
    assert prewarm_fn is not None
    result = prewarm_fn("nar", "20260712", 1)
    assert result == Path("/tmp/daybase-nar-20260712-1")


def test_make_handler_class_prewarm_fn_none_when_not_provided() -> None:
    """When prewarm_fn=None, the class attribute must also be None."""
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
    )
    assert handler_cls.prewarm_fn is None
    assert handler_cls.prewarm_parquet_payload_fn is None


def test_make_handler_class_prewarm_parquet_payload_fn_callable_without_instance() -> None:
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
        _fake_prewarm,
        _fake_prewarm_parquet_payload,
    )
    payload_fn = handler_cls.prewarm_parquet_payload_fn
    assert payload_fn is not None
    result = payload_fn("jra", "20260712", Path("/tmp/x"))
    assert result == ("cGF5bG9hZA==", "feat-daybase/jra/20260712/features.parquet", None)


def test_make_handler_class_prewarm_fn_not_bound_method() -> None:
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        _fake_rescore_factory,
        None,
        _fake_prewarm,
        _fake_prewarm_parquet_payload,
    )
    import inspect

    assert not inspect.ismethod(handler_cls.prewarm_fn), (
        "prewarm_fn must not be a bound method — staticmethod wrapping is required"
    )


# ---------------------------------------------------------------------------
# make_handler_class — focused-full cache wiring
# ---------------------------------------------------------------------------


def test_make_handler_class_focused_full_cache_populate_fn_callable_without_instance() -> None:
    """focused_full_cache_populate_fn on the handler class must be callable as
    a plain 1-arg function, mirroring the other optional-staticmethod fields."""
    calls: list[PredictParams] = []

    def _populate(params: PredictParams) -> None:
        calls.append(params)

    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        None,
        None,
        focused_full_cache_populate_fn=_populate,
    )
    populate_fn = handler_cls.focused_full_cache_populate_fn
    assert populate_fn is not None
    params = PredictParams(
        category="jra",
        run_date="20260619",
        days_ahead=0,
        mode="full",
        keibajo_code="05",
        race_bango="09",
    )
    populate_fn(params)
    assert calls == [params]


def test_make_handler_class_focused_full_cache_populate_fn_none_when_not_provided() -> None:
    handler_cls = make_handler_class(
        _fake_predict, _fake_parquet_payload, _fake_per_race_parquet_payload, None, None
    )
    assert handler_cls.focused_full_cache_populate_fn is None
    assert handler_cls.focused_full_cache_store is None


def test_make_handler_class_focused_full_cache_populate_fn_not_bound_method() -> None:
    def _populate(_params: PredictParams) -> None:
        return None

    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        None,
        None,
        focused_full_cache_populate_fn=_populate,
    )
    import inspect

    assert not inspect.ismethod(handler_cls.focused_full_cache_populate_fn), (
        "focused_full_cache_populate_fn must not be a bound method -- "
        "staticmethod wrapping is required"
    )


def test_make_handler_class_focused_full_cache_store_is_the_same_object() -> None:
    """Unlike the callables, the store is a plain object reference and must
    not be staticmethod-wrapped -- it needs no descriptor protection since
    only functions get bound-method injection."""
    store = FocusedFullCacheStore()
    handler_cls = make_handler_class(
        _fake_predict,
        _fake_parquet_payload,
        _fake_per_race_parquet_payload,
        None,
        None,
        focused_full_cache_store=store,
    )
    assert handler_cls.focused_full_cache_store is store


# ---------------------------------------------------------------------------
# _make_prewarm_fn / _prewarm_parquet_payload
# ---------------------------------------------------------------------------


def test_make_prewarm_fn_resolves_category_and_calls_build_day_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pipeline_runner

    captured: list[tuple[object, ...]] = []

    def fake_build_day_base(
        category: str,
        target_date: str,
        days_ahead: int,
        database_url: str,
        realtime_odds_path: Path | None = None,
        venue_weather_dir: Path | None = None,
        r2_config: R2Config | None = None,
    ) -> Path | None:
        captured.append((category, target_date, days_ahead, database_url, r2_config))
        return Path("/tmp/daybase-final")

    monkeypatch.setattr(pipeline_runner, "build_day_base", fake_build_day_base)
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    prewarm_fn = _make_prewarm_fn("postgresql://u:p@h/db", r2)
    result = prewarm_fn("nar", "20260712", 2)

    assert result == Path("/tmp/daybase-final")
    assert captured == [("nar", "20260712", 2, "postgresql://u:p@h/db", r2)]


def test_prewarm_parquet_payload_reads_parquet_and_builds_key(tmp_path: Path) -> None:
    day_base_dir = tmp_path / "daybase-final"
    day_base_dir.mkdir()
    (day_base_dir / "data.parquet").write_bytes(b"PARQUET-DATA")

    result = _prewarm_parquet_payload("jra", "20260712", day_base_dir)

    assert result is not None
    encoded, key, watermark = result
    import base64

    assert base64.b64decode(encoded) == b"PARQUET-DATA"
    assert key == "feat-daybase/catalog-v1/jra/20260712/features.parquet"
    assert watermark is None


def test_prewarm_parquet_payload_reads_watermark_sidecar_when_present(tmp_path: Path) -> None:
    day_dir = tmp_path / "daybase-jra-20260712"
    day_base_dir = day_dir / "final"
    day_base_dir.mkdir(parents=True)
    (day_base_dir / "data.parquet").write_bytes(b"PARQUET-DATA")
    import pipeline_runner

    # ``_write_watermark`` is module-private; accessed via getattr + cast
    # (same pattern ``tests/test_pipeline_runner.py`` uses) so strict
    # basedpyright does not flag ``reportPrivateUsage``.
    write_watermark_attr = "_write_watermark"
    write_watermark = cast(
        "Callable[[Path, tuple[str, int, str, int]], None]",
        getattr(pipeline_runner, write_watermark_attr),
    )

    write_watermark(day_dir, ("20260712", 946, "2026-07-18T09:00:00", 12))

    result = _prewarm_parquet_payload("jra", "20260712", day_base_dir)

    assert result is not None
    _encoded, _key, watermark = result
    assert watermark == {
        "maxDataSakuseiNengappi": "20260712",
        "rowCount": 946,
        "rsPredictedAtMax": "2026-07-18T09:00:00",
        "rsRowCount": 12,
    }


def test_ensure_cached_parquet_fetches_only_catalog_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    final_dir = tmp_path / "feat-jra-v7-final"
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")
    captured_keys: list[str] = []

    def fake_r2_get_parquet(_r2: R2Config, object_key: str, _dest: Path) -> bool:
        captured_keys.append(object_key)
        return True

    monkeypatch.setattr(predict_upcoming, "r2_get_parquet", fake_r2_get_parquet)

    _ensure_cached_parquet(final_dir, "jra", "20260712", r2)

    assert captured_keys == ["feat-cache/catalog-v1/jra/20260712/features.parquet"]


def test_catalog_rescore_whole_category_scope_forces_full_fallback_without_reading_processed_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A whole-category rescore (no single-race scope) has no per-race object
    to watermark-check against, so it still fails closed immediately -- the
    one part of the pre-(13) behavior that stays unconditional. See
    ``test_catalog_rescore_per_race_scope_*`` for the single-race scope,
    where a watermark-gated cache attempt now happens first."""
    cache_reads: list[bool] = []
    fetch_watermarked_calls: list[bool] = []
    full_calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        predict_upcoming,
        "_ensure_cached_parquet",
        lambda *args, **kwargs: cache_reads.append(True),
    )
    monkeypatch.setattr(
        predict_upcoming,
        "_fetch_watermarked_per_race_cache",
        lambda *args, **kwargs: fetch_watermarked_calls.append(True) or False,
    )
    rescore_fn, _per_race_payload_fn = _make_rescore_fn(
        "postgresql://neon-output/db",
        tmp_path,
        "r2-catalog://pc-keiba",
        R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b"),
        RaceScope(),
    )

    def full_predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        full_calls.append(
            (category, run_date, days_ahead, keibajo_code, race_bango, card_max_race_bango)
        )
        return 1

    params = PredictParams(
        category="jra",
        run_date="20260712",
        days_ahead=0,
        mode="rescore",
        card_max_race_bango=12,
    )
    chunks = list(iter_predict_chunks(params, full_predict, rescore_fn=rescore_fn))
    parsed = [json.loads(chunk.decode()) for chunk in chunks]

    assert cache_reads == []
    assert fetch_watermarked_calls == []
    assert full_calls == [("jra", "20260712", 0, None, None, 12)]
    assert any(item.get("stage") == "rescore-fallback-to-full" for item in parsed)
    assert parsed[-1]["status"] == "success"


def test_catalog_rescore_per_race_scope_falls_back_when_watermarked_cache_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single-race scope now gets one real attempt at the watermarked
    per-race cache before falling back -- when that attempt fails (R2 miss,
    watermark rejected, or any exception, all collapsed to a ``False``
    return by ``_fetch_watermarked_per_race_cache`` itself), the outcome is
    identical to the pre-(13) behavior: full pipeline fallback."""
    fetch_calls: list[tuple[object, ...]] = []
    full_calls: list[tuple[object, ...]] = []

    def fake_fetch(*args: object) -> bool:
        fetch_calls.append(args)
        return False

    monkeypatch.setattr(predict_upcoming, "_fetch_watermarked_per_race_cache", fake_fetch)
    rescore_fn, _per_race_payload_fn = _make_rescore_fn(
        "postgresql://neon-output/db",
        tmp_path,
        "r2-catalog://pc-keiba",
        R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b"),
        RaceScope(keibajo_code="05", race_bango="11"),
    )

    def full_predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        full_calls.append(
            (category, run_date, days_ahead, keibajo_code, race_bango, card_max_race_bango)
        )
        return 1

    params = PredictParams(
        category="jra",
        run_date="20260712",
        days_ahead=0,
        mode="rescore",
        keibajo_code="05",
        race_bango="11",
        card_max_race_bango=12,
    )
    chunks = list(iter_predict_chunks(params, full_predict, rescore_fn=rescore_fn))
    parsed = [json.loads(chunk.decode()) for chunk in chunks]

    assert len(fetch_calls) == 1
    assert full_calls == [("jra", "20260712", 0, "05", "11", 12)]
    assert any(item.get("stage") == "rescore-fallback-to-full" for item in parsed)
    assert parsed[-1]["status"] == "success"


def test_catalog_rescore_per_race_scope_uses_watermarked_cache_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the watermarked per-race cache attempt succeeds, the rescore
    proceeds on the cached-and-refreshed path -- the full pipeline is never
    invoked, and no ``rescore-fallback-to-full`` stage is emitted."""
    import pandas as pd

    from pipeline_runner import RACE_ID_FIELD

    def fake_fetch(final_dir: Path, *_rest: object) -> bool:
        final_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame(
            [{RACE_ID_FIELD: "jra:2026:0712:05:11", "umaban": 1, "ketto_toroku_bango": "H1"}]
        )
        frame.to_parquet(final_dir / "features.parquet")
        return True

    monkeypatch.setattr(predict_upcoming, "_fetch_watermarked_per_race_cache", fake_fetch)
    monkeypatch.setattr(predict_upcoming, "_fetch_fresh_snapshots", lambda *a, **k: {})
    monkeypatch.setattr(predict_upcoming, "_score_and_flush_races", lambda *a, **k: 1)
    full_calls: list[object] = []
    rescore_fn, _per_race_payload_fn = _make_rescore_fn(
        "postgresql://neon-output/db",
        tmp_path,
        "r2-catalog://pc-keiba",
        R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b"),
        RaceScope(keibajo_code="05", race_bango="11"),
    )

    def full_predict(*args: object) -> int:
        full_calls.append(args)
        return 1

    params = PredictParams(
        category="jra",
        run_date="20260712",
        days_ahead=0,
        mode="rescore",
        keibajo_code="05",
        race_bango="11",
    )
    chunks = list(iter_predict_chunks(params, full_predict, rescore_fn=rescore_fn))
    parsed = [json.loads(chunk.decode()) for chunk in chunks]

    assert full_calls == []
    assert not any(item.get("stage") == "rescore-fallback-to-full" for item in parsed)
    assert parsed[-1]["status"] == "success"


# ---------------------------------------------------------------------------
# _fetch_watermarked_per_race_cache -- direct unit tests
# ---------------------------------------------------------------------------


def test_fetch_watermarked_per_race_cache_false_without_race_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    r2_calls: list[str] = []
    monkeypatch.setattr(
        predict_upcoming,
        "r2_get_parquet",
        lambda *_a: r2_calls.append("called") or True,
    )
    final_dir = tmp_path / "feat-jra-v7-final"
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = _fetch_watermarked_per_race_cache(
        final_dir, "jra", "20260712", RaceScope(keibajo_code="05"), r2, "r2-catalog://pc-keiba"
    )

    assert result is False
    assert r2_calls == []


def test_fetch_watermarked_per_race_cache_false_on_r2_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(predict_upcoming, "r2_get_parquet", lambda *_a: False)
    final_dir = tmp_path / "feat-jra-v7-final"
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = _fetch_watermarked_per_race_cache(
        final_dir,
        "jra",
        "20260712",
        RaceScope(keibajo_code="05", race_bango="11"),
        r2,
        "r2-catalog://pc-keiba",
    )

    assert result is False
    assert not final_dir.exists()


def test_fetch_watermarked_per_race_cache_requests_the_per_race_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[str] = []

    def fake_r2_get_parquet(_r2: R2Config, object_key: str, _dest: Path) -> bool:
        captured.append(object_key)
        return False

    monkeypatch.setattr(predict_upcoming, "r2_get_parquet", fake_r2_get_parquet)
    final_dir = tmp_path / "feat-jra-v7-final"
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    _fetch_watermarked_per_race_cache(
        final_dir,
        "jra",
        "20260712",
        RaceScope(keibajo_code="05", race_bango="11"),
        r2,
        "r2-catalog://pc-keiba",
    )

    assert captured == ["feat-cache/catalog-v1/jra/20260712/05/11/features.parquet"]


def test_fetch_watermarked_per_race_cache_false_when_watermark_rejects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pipeline_runner

    def fake_r2_get_parquet(_r2: R2Config, _key: str, dest: Path) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"CANDIDATE-BYTES")
        return True

    monkeypatch.setattr(predict_upcoming, "r2_get_parquet", fake_r2_get_parquet)
    monkeypatch.setattr(pipeline_runner, "day_base_covers_entry_list", lambda *a, **k: False)
    final_dir = tmp_path / "feat-jra-v7-final"
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = _fetch_watermarked_per_race_cache(
        final_dir,
        "jra",
        "20260712",
        RaceScope(keibajo_code="05", race_bango="11"),
        r2,
        "r2-catalog://pc-keiba",
    )

    assert result is False
    assert not final_dir.exists()


def test_fetch_watermarked_per_race_cache_false_when_watermark_check_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pipeline_runner

    def fake_r2_get_parquet(_r2: R2Config, _key: str, dest: Path) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"CANDIDATE-BYTES")
        return True

    def raising_check(*_args: object, **_kwargs: object) -> bool:
        raise RuntimeError("source unreachable")

    monkeypatch.setattr(predict_upcoming, "r2_get_parquet", fake_r2_get_parquet)
    monkeypatch.setattr(pipeline_runner, "day_base_covers_entry_list", raising_check)
    final_dir = tmp_path / "feat-jra-v7-final"
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = _fetch_watermarked_per_race_cache(
        final_dir,
        "jra",
        "20260712",
        RaceScope(keibajo_code="05", race_bango="11"),
        r2,
        "r2-catalog://pc-keiba",
    )

    assert result is False
    assert not final_dir.exists()


def test_fetch_watermarked_per_race_cache_true_populates_final_dir_when_watermark_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pipeline_runner

    def fake_r2_get_parquet(_r2: R2Config, _key: str, dest: Path) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"CANDIDATE-BYTES")
        return True

    entry_list_calls: list[tuple[object, ...]] = []

    def fake_covers(
        day_base_dir: Path,
        category: object,
        run_date: object,
        target_race: object,
        source_url: object,
    ) -> bool:
        entry_list_calls.append((day_base_dir, category, run_date, target_race, source_url))
        return True

    monkeypatch.setattr(predict_upcoming, "r2_get_parquet", fake_r2_get_parquet)
    monkeypatch.setattr(pipeline_runner, "day_base_covers_entry_list", fake_covers)
    final_dir = tmp_path / "feat-jra-v7-final"
    final_dir.mkdir(parents=True)
    (final_dir / "stale.parquet").write_bytes(b"OLD-STALE-CONTENT")
    r2 = R2Config(account_id="a", access_key_id="k", secret_access_key="s", bucket="b")

    result = _fetch_watermarked_per_race_cache(
        final_dir,
        "jra",
        "20260712",
        RaceScope(keibajo_code="05", race_bango="11"),
        r2,
        "r2-catalog://pc-keiba",
    )

    assert result is True
    assert (final_dir / "features.parquet").read_bytes() == b"CANDIDATE-BYTES"
    assert not (final_dir / "stale.parquet").exists()
    assert entry_list_calls == [
        (
            final_dir.parent / "feat-jra-v7-final-per-race-candidate",
            "jra",
            "20260712",
            "05:11",
            "r2-catalog://pc-keiba",
        )
    ]
    candidate_dir = final_dir.parent / "feat-jra-v7-final-per-race-candidate"
    assert not candidate_dir.exists()


def test_prewarm_parquet_payload_returns_none_when_no_parquet(tmp_path: Path) -> None:
    day_base_dir = tmp_path / "daybase-final"
    day_base_dir.mkdir()

    result = _prewarm_parquet_payload("jra", "20260712", day_base_dir)

    assert result is None


# ---------------------------------------------------------------------------
# NAR E-top2 historical helper (iter23-nar-etop2)
# ---------------------------------------------------------------------------
#
# NAR production dispatch no longer uses this helper. These tests keep the
# historical place-preserving helper behavior pinned while separate tests assert
# that ``score_races`` ignores a synthetic NAR E-top2 flag and uses per-cell or
# category-default direct scoring.


@final
class _ScoreByUmaban:
    """Fake booster that returns a per-row score keyed by the row's umaban.

    ``build_feature_matrix`` projects each entry onto the numeric feature order,
    dropping the umaban / ketto columns, so the fake instead consults a closure
    map indexed by the row's position in the race. The scores are supplied as a
    parallel list in entry order at construction time.
    """

    def __init__(self, scores: list[float]) -> None:
        self._scores = scores

    def predict(self, matrix: object) -> list[float]:
        del matrix  # scores are positional, not feature-derived
        return list(self._scores)


@final
class _ScoreByUmabanWithFeatureNames:
    """``_ScoreByUmaban`` plus a CatBoost-style ``feature_names_`` attribute.

    Mirrors what a real CatBoost booster exposes after training: the exact
    positional column order it was fit on. Used to exercise
    ``_variant_booster_feature_order_matches`` / ``score_races``'s cell-routing
    variant order-check against a booster whose own trained order may or may
    not agree with the variant's ``metadata.json``.
    """

    def __init__(self, scores: list[float], feature_names_: list[str]) -> None:
        self._scores = scores
        self.feature_names_ = feature_names_

    def predict(self, matrix: object) -> list[float]:
        del matrix
        return list(self._scores)


_NAR_RACE_ID: str = "nar:20260620:30:02:11"


def _nar_entries() -> list[dict[str, object]]:
    """Three NAR entries (umaban 1/2/3) in class ``A`` (an ADOPT class)."""
    return [
        {"ketto_toroku_bango": "H1", "umaban": 1, "nar_subclass": "A", "feat": 0.1},
        {"ketto_toroku_bango": "H2", "umaban": 2, "nar_subclass": "A", "feat": 0.2},
        {"ketto_toroku_bango": "H3", "umaban": 3, "nar_subclass": "A", "feat": 0.3},
    ]


def _run_nar_etop2(
    xgb: BoosterLike,
    cb: BoosterLike,
    entries: list[dict[str, object]],
) -> list[list[object]]:
    """Invoke ``score_one_race_nar_etop2`` for the shared NAR test race."""
    return score_one_race_nar_etop2(xgb, cb, _NAR_RACE_ID, "nar", entries, ["feat"])


def test_score_one_race_nar_etop2_override_promotes_xgb_rank2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When CB#1 == XGB#2 in an ADOPT class, XGB#2 is promoted to rank-1."""
    monkeypatch.setattr(
        nar_etop2_override,
        "NAR_ETOP2_ADOPT_CLASSES",
        frozenset({"A", "B", "NEW", "other"}),
    )
    # XGB base ranking: H1 (0.9) > H2 (0.5) > H3 (0.1) -> XGB#2 = H2 (umaban 2).
    xgb = _ScoreByUmaban([0.9, 0.5, 0.1])
    # CB rank-1 = H2 (umaban 2) -> equals XGB#2, so the override fires.
    cb = _ScoreByUmaban([0.2, 0.8, 0.1])

    rows = _run_nar_etop2(xgb, cb, _nar_entries())

    by_rank = {row[9]: row[7] for row in rows}  # predicted_rank -> umaban
    assert by_rank[1] == 2, "XGB#2 (umaban 2) must be promoted to rank-1"
    assert by_rank[2] == 1, "XGB#1 (umaban 1) must be demoted to rank-2"
    assert by_rank[3] == 3, "rank-3 (umaban 3) must be preserved"


def test_score_one_race_nar_etop2_writes_iter23_model_version() -> None:
    """Every emitted row is labelled with NAR_ETOP2_MODEL_VERSION."""
    xgb = _ScoreByUmaban([0.9, 0.5, 0.1])
    cb = _ScoreByUmaban([0.2, 0.8, 0.1])

    rows = _run_nar_etop2(xgb, cb, _nar_entries())

    assert rows, "at least one prediction row must be emitted"
    assert all(row[0] == NAR_ETOP2_MODEL_VERSION for row in rows)
    assert NAR_ETOP2_MODEL_VERSION == "iter23-nar-etop2"


def test_score_one_race_nar_etop2_no_override_for_reject_class() -> None:
    """A non-ADOPT class (``C``) keeps the pure XGB base ranking."""
    entries = [dict(entry, nar_subclass="C") for entry in _nar_entries()]
    xgb = _ScoreByUmaban([0.9, 0.5, 0.1])
    cb = _ScoreByUmaban([0.2, 0.8, 0.1])

    rows = _run_nar_etop2(xgb, cb, entries)

    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 1, "REJECT class must keep XGB#1 (umaban 1) at rank-1"
    assert by_rank[2] == 2
    assert by_rank[3] == 3


@final
class _NoRoutingRouter:
    """Cell-router stub for tests focused on non-routing score paths."""

    def has_routing(self, category: str) -> bool:
        del category
        return False


def test_score_races_ignores_nar_etop2_flag_and_uses_category_default() -> None:
    """NAR production dispatch is per-cell first, then category default direct."""
    entries = _nar_entries()
    races = {"nar:20260620:30:02:11": entries}
    xgb = _ScoreByUmaban([0.9, 0.5, 0.1])

    with (
        patch("predict_upcoming._load_booster", return_value=xgb),
        patch("predict_upcoming.load_cell_router", return_value=_NoRoutingRouter()),
        patch("predict_upcoming.NAR_ETOP2_ENABLED", True, create=True),
    ):
        scored = score_races(races, "nar", Path("/models"), ["feat"])

    rows = scored[0]
    assert all(row[0] == "iter12-nar-xgb-hpo-v8-clean188" for row in rows)
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 1


# ---------------------------------------------------------------------------
# Cell-routing variant pool (score_races)
# ---------------------------------------------------------------------------
#
# score_races builds a dict[str, VariantModel] from the routing config's
# ``variants`` map: every non-default variant is loaded into the pool, the
# default variant is served by the already-loaded category-global fallback.
# A race whose resolved variant is in the pool scores against that variant's
# booster + feature order + architecture; otherwise the fallback scores it.
# These tests duck-type the routing config so they exercise the pool logic
# without depending on the concrete cell_router dataclasses.


@final
class _FakeVariantSpec:
    """Stand-in for ``cell_router.VariantSpec`` (model_version/feature_count/architecture)."""

    def __init__(
        self,
        model_version: str,
        feature_count: int,
        architecture: str,
        *,
        feature_names: tuple[str, ...] | None = None,
        feature_set_hash: str | None = None,
    ) -> None:
        self.model_version = model_version
        self.feature_count = feature_count
        self.architecture = architecture
        self.feature_names = feature_names
        self.feature_set_hash = feature_set_hash


@final
class _FakeRouting:
    """Stand-in for ``CategoryRouting`` carrying a variants map + default variant."""

    def __init__(self, variants: dict[str, _FakeVariantSpec], default_variant: str) -> None:
        self.variants = variants
        self.default_variant = default_variant


@final
class _FakeRouter:
    """Stand-in for ``CellRouter`` that always routes a ban-ei race to ``resolved``."""

    def __init__(self, routing: _FakeRouting, resolved: str) -> None:
        self._routing = routing
        self._resolved = resolved

    def has_routing(self, category: str) -> bool:
        del category
        return True

    def routing_for(self, category: str) -> _FakeRouting:
        del category
        return self._routing

    def resolve_variant(
        self,
        category: str,
        entries: Sequence[Mapping[str, object]],
        card_max_race_bango: int | None = None,
    ) -> str:
        del category, entries, card_max_race_bango
        return self._resolved


def _banei_entries() -> list[dict[str, object]]:
    """Three Ban-ei entries (umaban 1/2/3) carrying the single ``feat`` column."""
    return [
        {"ketto_toroku_bango": "B1", "umaban": 1, "feat": 0.1},
        {"ketto_toroku_bango": "B2", "umaban": 2, "feat": 0.2},
        {"ketto_toroku_bango": "B3", "umaban": 3, "feat": 0.3},
    ]


def _write_variant_metadata(
    models_dir: Path, category: str, model_version: str, feature_names: list[str]
) -> None:
    meta_path = models_dir / build_base_model_r2_key(category, model_version, METADATA_FILE_NAME)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps({"feature_names": feature_names}), encoding="utf-8")


def _write_category_metadata(
    models_dir: Path, category: Category, feature_names: list[str]
) -> None:
    key = predict_upcoming.build_r2_object_key(category, METADATA_FILE_NAME)
    meta_path = models_dir / key
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps({"feature_names": feature_names}), encoding="utf-8")


def test_load_model_metadata_rejects_within_race_leak_columns(tmp_path: Path) -> None:
    _write_category_metadata(
        tmp_path,
        "nar",
        ["feat"] * 187 + ["target_corner_2_norm"],
    )
    with pytest.raises(ValueError, match="target_corner_2_norm"):
        _load_model_metadata(tmp_path, "nar")


def test_load_model_metadata_accepts_clean_category_default(tmp_path: Path) -> None:
    feature_names = [f"feat_{index}" for index in range(188)]
    _write_category_metadata(tmp_path, "nar", feature_names)
    assert list(_load_model_metadata(tmp_path, "nar")) == feature_names


def test_variant_model_holds_booster_and_feature_contract() -> None:
    """VariantModel is a frozen carrier for the booster + feature order + arch."""
    booster = _ScoreByUmaban([0.1])
    vm = VariantModel(
        booster=booster,
        feature_names=["a", "b"],
        architecture="catboost",
        model_version="cell-v1",
    )
    assert vm.booster is booster
    assert list(vm.feature_names) == ["a", "b"]
    assert vm.architecture == "catboost"
    assert vm.model_version == "cell-v1"


def test_score_races_routes_to_pooled_variant_and_skips_default(tmp_path: Path) -> None:
    """A non-default variant is loaded into the pool and scores its routed race.

    The Ban-ei ``base`` variant (catboost) is loaded; the ``sim`` default is
    skipped (served by the fallback). The race routes to ``base`` so the variant
    booster — not the fallback — drives the ranking.
    """
    _write_variant_metadata(tmp_path, "ban-ei", "banei-cb-v8-window2011-wf-15y", ["feat"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-cb-v9-sim-2011", 1, "catboost"),
            "base": _FakeVariantSpec("banei-cb-v8-window2011-wf-15y", 1, "catboost"),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="base")
    fallback = _ScoreByUmaban([0.9, 0.1, 0.1])  # would rank umaban 1 first
    variant_booster = _ScoreByUmaban([0.1, 0.9, 0.3])  # ranks umaban 2 first
    loaded: list[str] = []

    def _fake_load_by_arch(model_path: Path, architecture: Architecture) -> BoosterLike:
        del architecture
        loaded.append(str(model_path))
        return variant_booster

    races = {"ban-ei:20260620:65:01:01": _banei_entries()}
    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=fallback),
        patch("predict_upcoming._load_booster_by_arch", side_effect=_fake_load_by_arch),
    ):
        scored = score_races(races, "ban-ei", tmp_path, ["feat"])

    assert len(loaded) == 1, "only the non-default variant should be loaded"
    assert "banei-cb-v8-window2011-wf-15y" in loaded[0], "the loaded variant must be the base model"
    assert "banei-cb-v9-sim-2011" not in loaded[0], (
        "the default variant must be served by the fallback"
    )
    rows = scored[0]
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 2, "the pooled variant booster must drive the ranking"
    assert all(row[0] == "banei-cb-v8-window2011-wf-15y" for row in rows)


def test_score_races_rejects_cell_variant_feature_contract_mismatch(
    tmp_path: Path,
) -> None:
    """Config feature_names must match the baked metadata contract exactly."""
    _write_variant_metadata(tmp_path, "ban-ei", "banei-cb-v8-window2011-wf-15y", ["feat"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-cb-v9-sim-2011", 1, "catboost"),
            "cell": _FakeVariantSpec(
                "banei-cb-v8-window2011-wf-15y",
                1,
                "catboost",
                feature_names=("other_feat",),
            ),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="cell")

    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=_ScoreByUmaban([0.9])),
        patch("predict_upcoming._load_booster_by_arch", return_value=_ScoreByUmaban([0.1])),
        pytest.raises(ValueError, match=r"feature_names do not match metadata\.json"),
    ):
        score_races({"ban-ei:20260620:65:01:01": _banei_entries()}, "ban-ei", tmp_path, ["feat"])


def test_score_races_rejects_cell_variant_feature_hash_mismatch(tmp_path: Path) -> None:
    """Config feature_set_hash must match the baked metadata feature names."""
    _write_variant_metadata(tmp_path, "ban-ei", "banei-cb-v8-window2011-wf-15y", ["feat"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-cb-v9-sim-2011", 1, "catboost"),
            "cell": _FakeVariantSpec(
                "banei-cb-v8-window2011-wf-15y",
                1,
                "catboost",
                feature_names=("feat",),
                feature_set_hash="not-the-metadata-hash",
            ),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="cell")

    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=_ScoreByUmaban([0.9])),
        patch("predict_upcoming._load_booster_by_arch", return_value=_ScoreByUmaban([0.1])),
        pytest.raises(ValueError, match="feature_set_hash mismatch"),
    ):
        score_races({"ban-ei:20260620:65:01:01": _banei_entries()}, "ban-ei", tmp_path, ["feat"])


def test_score_races_rejects_cell_variant_within_race_leak_metadata(
    tmp_path: Path,
) -> None:
    _write_variant_metadata(
        tmp_path,
        "ban-ei",
        "banei-cb-v8-window2011-wf-15y",
        ["target_corner_4_norm"],
    )
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-cb-v9-sim-2011", 1, "catboost"),
            "cell": _FakeVariantSpec("banei-cb-v8-window2011-wf-15y", 1, "catboost"),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="cell")

    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=_ScoreByUmaban([0.9])),
        patch("predict_upcoming._load_booster_by_arch", return_value=_ScoreByUmaban([0.1])),
        pytest.raises(ValueError, match="within-race leak columns"),
    ):
        score_races({"ban-ei:20260620:65:01:01": _banei_entries()}, "ban-ei", tmp_path, ["feat"])


def test_variant_booster_feature_order_matches_no_feature_names_attr_is_a_match() -> None:
    """A booster with no ``feature_names_`` (e.g. XGBoost) is never order-checked."""
    booster = _ScoreByUmaban([0.1])
    assert _variant_booster_feature_order_matches(booster, "catboost", ["a", "b"]) is True


def test_variant_booster_feature_order_matches_exact_order_is_a_match() -> None:
    booster = _ScoreByUmabanWithFeatureNames([0.1], ["a", "b", "c"])
    assert _variant_booster_feature_order_matches(booster, "catboost", ["a", "b", "c"]) is True


def test_variant_booster_feature_order_matches_permuted_order_is_a_mismatch() -> None:
    booster = _ScoreByUmabanWithFeatureNames([0.1], ["b", "a", "c"])
    assert _variant_booster_feature_order_matches(booster, "catboost", ["a", "b", "c"]) is False


# --- feature_guard wiring at _score_one_race_direct's actual call site -----
#
# 2026-07-17 gap found during the bug-regression-test-audit campaign
# (docs/probes/bug-regression-test-audit-2026-07-17.md): commit 57a4cd7f
# added predict_lib/feature_guard.py with thorough unit tests
# (tests/test_feature_guard.py) for the pure is_degenerate_feature_matrix
# logic, but nothing exercised the WIRING -- the `if
# is_degenerate_feature_matrix(...): return []` guard actually placed inside
# _score_one_race_direct / _score_one_race_nar_blend. Confirmed by mutation
# (temporarily deleting both wiring blocks, restored afterward): the full
# predict-container suite (1288 tests) still passed at 99.81% coverage with
# the guard completely disconnected -- i.e. every existing test would have
# missed a regression that silently re-enabled writing predictions from a
# degenerate feature matrix. These two tests close that gap by calling
# _score_one_race_direct directly, the same function score_races() itself
# calls at both its cell-routing-variant and category-default call sites.
def test_score_one_race_direct_skips_write_for_degenerate_feature_matrix() -> None:
    booster = _ScoreByUmaban([0.9, 0.1])
    # Every feature absent for both entries -- the guard's target failure
    # signature (a whole-race input failure, not one legitimately-sparse
    # debut horse).
    degenerate_entries: list[dict[str, object]] = [
        {"ketto_toroku_bango": "H1", "umaban": 1},
        {"ketto_toroku_bango": "H2", "umaban": 2},
    ]
    rows = _score_one_race_direct(
        booster,
        "jra:20260101:05:01:01",
        "jra",
        degenerate_entries,
        ["a", "b", "c", "d"],
        "catboost",
        "jra-cb-v9-sim-2013-clean",
    )
    assert rows == []


def test_score_one_race_direct_scores_healthy_feature_matrix() -> None:
    """Companion to the test above: a fully-populated feature matrix must
    still be scored normally -- the guard must not false-positive on a
    healthy race."""
    booster = _ScoreByUmaban([0.9, 0.1])
    healthy_entries: list[dict[str, object]] = [
        {"ketto_toroku_bango": "H1", "umaban": 1, "a": 1.0, "b": 2.0},
        {"ketto_toroku_bango": "H2", "umaban": 2, "a": 3.0, "b": 4.0},
    ]
    rows = _score_one_race_direct(
        booster,
        "jra:20260101:05:01:01",
        "jra",
        healthy_entries,
        ["a", "b"],
        "catboost",
        "jra-cb-v9-sim-2013-clean",
    )
    assert len(rows) == 2


def _banei_entries_two_features() -> list[dict[str, object]]:
    """Three Ban-ei entries (umaban 1/2/3) fully populated on two columns.

    Distinct from :func:`_banei_entries` (single ``feat`` column) so the
    feature-order-mismatch tests below can exercise a real two-column
    permutation without tripping ``feature_guard.is_degenerate_feature_matrix``
    on an unrelated missing-column count.
    """
    return [
        {"ketto_toroku_bango": "B1", "umaban": 1, "a": 0.1, "b": 1.1},
        {"ketto_toroku_bango": "B2", "umaban": 2, "a": 0.2, "b": 1.2},
        {"ketto_toroku_bango": "B3", "umaban": 3, "a": 0.3, "b": 1.3},
    ]


def test_score_races_rejects_variant_with_feature_order_mismatch(tmp_path: Path) -> None:
    """A CatBoost variant whose own trained order disagrees with metadata.json

    (``_feature_set_hash`` alone cannot catch this -- it is order-independent
    by design) must not be scored: the race falls back to the category default
    instead of silently scoring against a permuted matrix.
    """
    _write_variant_metadata(tmp_path, "ban-ei", "banei-cb-v8-window2011-wf-15y", ["a", "b"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-cb-v9-sim-2011", 1, "catboost"),
            "cell": _FakeVariantSpec("banei-cb-v8-window2011-wf-15y", 2, "catboost"),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="cell")
    fallback = _ScoreByUmaban([0.9, 0.1, 0.1])  # ranks umaban 1 first
    # The baked booster's OWN trained order ("b", "a") disagrees with the
    # metadata.json order ("a", "b") written above.
    mismatched_variant_booster = _ScoreByUmabanWithFeatureNames([0.1, 0.9, 0.3], ["b", "a"])

    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=fallback),
        patch("predict_upcoming._load_booster_by_arch", return_value=mismatched_variant_booster),
    ):
        scored = score_races(
            {"ban-ei:20260620:65:01:01": _banei_entries_two_features()},
            "ban-ei",
            tmp_path,
            ["a", "b"],
        )

    rows = scored[0]
    assert all(row[0] == "banei-cb-v9-sim-2011" for row in rows), (
        "an order-mismatched variant must fall back to the category default"
    )
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 1, "the fallback booster (not the mismatched variant) must have scored"


def test_score_races_accepts_variant_with_matching_feature_order(tmp_path: Path) -> None:
    """A CatBoost variant whose trained order matches metadata.json loads normally."""
    _write_variant_metadata(tmp_path, "ban-ei", "banei-cb-v8-window2011-wf-15y", ["a", "b"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-cb-v9-sim-2011", 1, "catboost"),
            "cell": _FakeVariantSpec("banei-cb-v8-window2011-wf-15y", 2, "catboost"),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="cell")
    fallback = _ScoreByUmaban([0.9, 0.1, 0.1])
    matching_variant_booster = _ScoreByUmabanWithFeatureNames([0.1, 0.9, 0.3], ["a", "b"])

    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=fallback),
        patch("predict_upcoming._load_booster_by_arch", return_value=matching_variant_booster),
    ):
        scored = score_races(
            {"ban-ei:20260620:65:01:01": _banei_entries_two_features()},
            "ban-ei",
            tmp_path,
            ["a", "b"],
        )

    rows = scored[0]
    assert all(row[0] == "banei-cb-v8-window2011-wf-15y" for row in rows), (
        "an order-matched variant must be used, not the fallback"
    )
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 2, "the matched variant booster must have driven the ranking"


def test_score_races_rejects_unallowed_nar_cell_variant(tmp_path: Path) -> None:
    """A routed cell variant must be explicitly approved before production serving."""
    _write_variant_metadata(tmp_path, "nar", "nar-cell-v1", ["feat"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("iter12-nar-xgb-hpo-v8-clean188", 1, "xgboost"),
            "cell": _FakeVariantSpec("nar-cell-v1", 1, "xgboost"),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="cell")
    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=_ScoreByUmaban([0.9])),
        patch("predict_upcoming._load_booster_by_arch", return_value=_ScoreByUmaban([0.1])),
        pytest.raises(ValueError, match="not allowed"),
    ):
        score_races({"nar:20260620:54:01:01": _nar_entries()}, "nar", tmp_path, ["feat"])


def test_score_races_falls_back_when_resolved_variant_not_in_pool(tmp_path: Path) -> None:
    """When the resolved variant is the default (not pooled), the fallback scores."""
    _write_variant_metadata(tmp_path, "ban-ei", "banei-cb-v8-window2011-wf-15y", ["feat"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-cb-v9-sim-2011", 1, "catboost"),
            "base": _FakeVariantSpec("banei-cb-v8-window2011-wf-15y", 1, "catboost"),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="sim")  # default -> not in pool
    fallback = _ScoreByUmaban([0.9, 0.1, 0.1])  # ranks umaban 1 first
    variant_booster = _ScoreByUmaban([0.1, 0.9, 0.3])

    def _fake_load_by_arch(model_path: Path, architecture: Architecture) -> BoosterLike:
        del model_path, architecture
        return variant_booster

    races = {"ban-ei:20260620:65:01:01": _banei_entries()}
    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=fallback),
        patch("predict_upcoming._load_booster_by_arch", side_effect=_fake_load_by_arch),
    ):
        scored = score_races(races, "ban-ei", tmp_path, ["feat"])

    rows = scored[0]
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 1, "the fallback booster must drive the ranking for the default variant"
    assert all(row[0] == "banei-cb-v9-sim-2011" for row in rows)


def test_score_races_nar_default_uses_category_model_without_per_class() -> None:
    """NAR defaults score directly with the category model, not per-class routing."""
    routing = _FakeRouting(
        variants={"sim": _FakeVariantSpec("iter12-nar-xgb-hpo-v8-clean188", 1, "xgboost")},
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="sim")
    fallback = _ScoreByUmaban([0.9, 0.1, 0.1])

    races = {"nar:20260620:54:01:01": _nar_entries()}
    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=fallback),
    ):
        scored = score_races(races, "nar", Path("/models"), ["feat"])

    rows = scored[0]
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 1
    assert all(row[0] == "iter12-nar-xgb-hpo-v8-clean188" for row in rows)


def test_load_nar_transformer_returns_none_when_feature_order_has_leak(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    transformer = SimpleNamespace(
        feature_order=("feat", "target_running_style_class"),
        seeds=(object(),),
    )
    with patch("predict_upcoming.load_transformer", return_value=transformer):
        loaded = _load_nar_transformer(tmp_path, ["feat"])
    captured = capsys.readouterr()
    assert loaded is None
    assert "leak guard failed" in captured.err


def test_load_nar_transformer_accepts_clean_feature_order(tmp_path: Path) -> None:
    transformer = SimpleNamespace(feature_order=("feat",), seeds=(object(),))
    with patch("predict_upcoming.load_transformer", return_value=transformer):
        loaded = _load_nar_transformer(tmp_path, ["feat"])
    assert loaded is transformer


# ---------------------------------------------------------------------------
# Stage-1 market-free gated fallback (JRA): _load_stage1_model's fail-closed
# loading, and score_races's end-to-end gate wiring.
# ---------------------------------------------------------------------------

_STAGE1_TEST_CONFIG = Stage1CategoryConfig(
    enabled=True,
    model_version="jra-cb-stage1-marketfree235-2013",
    feature_count=1,
    architecture="catboost",
    stddev_threshold=0.4,
    enable_stddev_safety_net=True,
)


def _jra_entries_with_ninkijun(
    ninkijun_by_umaban: Mapping[int, int | None],
) -> list[dict[str, object]]:
    """JRA entries (umaban keys of ``ninkijun_by_umaban``, in order) carrying a
    single ``feat`` column plus ``tansho_ninkijun`` per the supplied map."""
    return [
        {
            "ketto_toroku_bango": f"H{umaban}",
            "umaban": umaban,
            "feat": float(umaban) / 10,
            "tansho_ninkijun": ninkijun,
        }
        for umaban, ninkijun in ninkijun_by_umaban.items()
    ]


def test_load_stage1_model_returns_none_on_unapproved_model_version(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = Stage1CategoryConfig(
        enabled=True,
        model_version="jra-cb-not-on-the-allowlist",
        feature_count=1,
        architecture="catboost",
        stddev_threshold=0.4,
        enable_stddev_safety_net=True,
    )

    loaded = _load_stage1_model(tmp_path, "jra", config)

    assert loaded is None
    assert "load failed" in capsys.readouterr().err


def test_load_stage1_model_returns_none_on_missing_metadata_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """No metadata.json staged at all -- the booster load itself never even
    gets a chance since ``_load_booster_by_arch`` is real here (unpatched)."""
    loaded = _load_stage1_model(tmp_path, "jra", _STAGE1_TEST_CONFIG)

    assert loaded is None
    assert "load failed" in capsys.readouterr().err


def test_load_stage1_model_returns_none_on_leak_column(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_variant_metadata(
        tmp_path, "jra", _STAGE1_TEST_CONFIG.model_version, ["target_corner_2_norm"]
    )

    with patch("predict_upcoming._load_booster_by_arch", return_value=_ScoreByUmaban([0.1])):
        loaded = _load_stage1_model(tmp_path, "jra", replace(_STAGE1_TEST_CONFIG, feature_count=1))

    assert loaded is None
    assert "load failed" in capsys.readouterr().err


def test_load_stage1_model_returns_none_on_feature_count_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_variant_metadata(
        tmp_path, "jra", _STAGE1_TEST_CONFIG.model_version, ["feat_a", "feat_b"]
    )

    with patch("predict_upcoming._load_booster_by_arch", return_value=_ScoreByUmaban([0.1])):
        loaded = _load_stage1_model(tmp_path, "jra", _STAGE1_TEST_CONFIG)  # feature_count=1

    assert loaded is None
    assert "load failed" in capsys.readouterr().err


def test_load_stage1_model_accepts_clean_artifact(tmp_path: Path) -> None:
    _write_variant_metadata(tmp_path, "jra", _STAGE1_TEST_CONFIG.model_version, ["feat"])
    booster = _ScoreByUmaban([0.1])

    with patch("predict_upcoming._load_booster_by_arch", return_value=booster):
        loaded = _load_stage1_model(tmp_path, "jra", _STAGE1_TEST_CONFIG)

    assert loaded is not None
    assert loaded.booster is booster
    assert list(loaded.feature_names) == ["feat"]
    assert loaded.architecture == "catboost"
    assert loaded.model_version == _STAGE1_TEST_CONFIG.model_version


def test_score_races_stage1_gate_routes_to_stage1_when_odds_missing(tmp_path: Path) -> None:
    """Every entry lacking a real tansho_ninkijun -- the freshness gate fails
    closed to Stage-1 regardless of how healthy the champion's own scores
    would have looked."""
    _write_variant_metadata(tmp_path, "jra", _STAGE1_TEST_CONFIG.model_version, ["feat"])
    entries = _jra_entries_with_ninkijun({1: None, 2: None})
    champion = _ScoreByUmaban([3.0, -3.0])  # a healthy spread, if it were used
    stage1_booster = _ScoreByUmaban([1.0, 2.0])

    with (
        patch("predict_upcoming.load_cell_router", return_value=CellRouter({})),
        patch("predict_upcoming.load_stage1_routing", return_value={"jra": _STAGE1_TEST_CONFIG}),
        patch("predict_upcoming._load_booster", return_value=champion),
        patch("predict_upcoming._load_booster_by_arch", return_value=stage1_booster),
    ):
        scored = score_races({"jra:20260620:05:01:01": entries}, "jra", tmp_path, ["feat"])

    rows = scored[0]
    assert all(row[0] == _STAGE1_TEST_CONFIG.model_version for row in rows)


def test_score_races_stage1_gate_keeps_champion_when_odds_fresh_and_spread_healthy(
    tmp_path: Path,
) -> None:
    _write_variant_metadata(tmp_path, "jra", _STAGE1_TEST_CONFIG.model_version, ["feat"])
    entries = _jra_entries_with_ninkijun({1: 1, 2: 2})
    champion = _ScoreByUmaban([3.0, -3.0])  # stddev well above the 0.4 threshold
    stage1_booster = _ScoreByUmaban([1.0, 2.0])

    with (
        patch("predict_upcoming.load_cell_router", return_value=CellRouter({})),
        patch("predict_upcoming.load_stage1_routing", return_value={"jra": _STAGE1_TEST_CONFIG}),
        patch("predict_upcoming._load_booster", return_value=champion),
        patch("predict_upcoming._load_booster_by_arch", return_value=stage1_booster),
    ):
        scored = score_races({"jra:20260620:05:01:01": entries}, "jra", tmp_path, ["feat"])

    rows = scored[0]
    assert all(row[0] == "jra-cb-v9-sim-2013-clean" for row in rows)


def test_score_races_stage1_stddev_safety_net_trips_despite_fresh_odds(tmp_path: Path) -> None:
    """Odds look fresh (every entry has a real tansho_ninkijun) but the
    champion's own ranking still collapsed -- the stddev safety net must
    still catch it and route to Stage-1."""
    _write_variant_metadata(tmp_path, "jra", _STAGE1_TEST_CONFIG.model_version, ["feat"])
    entries = _jra_entries_with_ninkijun({1: 1, 2: 2})
    champion = _ScoreByUmaban([0.5, 0.500001])  # collapsed spread, well under 0.4
    stage1_booster = _ScoreByUmaban([1.0, 2.0])

    with (
        patch("predict_upcoming.load_cell_router", return_value=CellRouter({})),
        patch("predict_upcoming.load_stage1_routing", return_value={"jra": _STAGE1_TEST_CONFIG}),
        patch("predict_upcoming._load_booster", return_value=champion),
        patch("predict_upcoming._load_booster_by_arch", return_value=stage1_booster),
    ):
        scored = score_races({"jra:20260620:05:01:01": entries}, "jra", tmp_path, ["feat"])

    rows = scored[0]
    assert all(row[0] == _STAGE1_TEST_CONFIG.model_version for row in rows)


def test_score_races_stage1_disabled_config_never_overrides_champion(tmp_path: Path) -> None:
    """A disabled Stage-1 config must never fire, even on odds-missing races."""
    disabled_config = replace(_STAGE1_TEST_CONFIG, enabled=False)
    entries = _jra_entries_with_ninkijun({1: None, 2: None})
    champion = _ScoreByUmaban([3.0, -3.0])

    with (
        patch("predict_upcoming.load_cell_router", return_value=CellRouter({})),
        patch("predict_upcoming.load_stage1_routing", return_value={"jra": disabled_config}),
        patch("predict_upcoming._load_booster", return_value=champion),
    ):
        scored = score_races({"jra:20260620:05:01:01": entries}, "jra", tmp_path, ["feat"])

    rows = scored[0]
    assert all(row[0] == "jra-cb-v9-sim-2013-clean" for row in rows)


def test_score_races_stage1_absent_category_config_never_overrides(tmp_path: Path) -> None:
    """No "jra" entry in stage1_routing.json at all -- pure no-op, matches
    behaviour before this fallback existed."""
    entries = _jra_entries_with_ninkijun({1: None, 2: None})
    champion = _ScoreByUmaban([3.0, -3.0])

    with (
        patch("predict_upcoming.load_cell_router", return_value=CellRouter({})),
        patch("predict_upcoming.load_stage1_routing", return_value={}),
        patch("predict_upcoming._load_booster", return_value=champion),
    ):
        scored = score_races({"jra:20260620:05:01:01": entries}, "jra", tmp_path, ["feat"])

    rows = scored[0]
    assert all(row[0] == "jra-cb-v9-sim-2013-clean" for row in rows)


def test_score_races_stage1_broken_artifact_falls_back_to_champion(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Stage-1 gate WOULD trip (odds missing) but the artifact itself fails
    to load (no metadata.json staged) -- must fail closed to the champion,
    never leave the race unscored."""
    entries = _jra_entries_with_ninkijun({1: None, 2: None})
    champion = _ScoreByUmaban([3.0, -3.0])

    with (
        patch("predict_upcoming.load_cell_router", return_value=CellRouter({})),
        patch("predict_upcoming.load_stage1_routing", return_value={"jra": _STAGE1_TEST_CONFIG}),
        patch("predict_upcoming._load_booster", return_value=champion),
    ):
        scored = score_races({"jra:20260620:05:01:01": entries}, "jra", tmp_path, ["feat"])

    rows = scored[0]
    assert all(row[0] == "jra-cb-v9-sim-2013-clean" for row in rows)
    assert "load failed" in capsys.readouterr().err


def test_score_races_warns_when_resolved_non_default_variant_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A rule pointing at an undeclared/unloaded variant falls back but is visible."""
    _write_variant_metadata(tmp_path, "ban-ei", "banei-cb-v8-window2011-wf-15y", ["feat"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-cb-v9-sim-2011", 1, "catboost"),
            "base": _FakeVariantSpec("banei-cb-v8-window2011-wf-15y", 1, "catboost"),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="missing-cell")
    fallback = _ScoreByUmaban([0.9, 0.1, 0.1])
    variant_booster = _ScoreByUmaban([0.1, 0.9, 0.3])

    def _fake_load_by_arch(model_path: Path, architecture: Architecture) -> BoosterLike:
        del model_path, architecture
        return variant_booster

    races = {"ban-ei:20260620:65:01:01": _banei_entries()}
    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=fallback),
        patch("predict_upcoming._load_booster_by_arch", side_effect=_fake_load_by_arch),
    ):
        scored = score_races(races, "ban-ei", tmp_path, ["feat"])

    captured = capsys.readouterr()
    assert "resolved missing variant=missing-cell; using default" in captured.err
    rows = scored[0]
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 1


@pytest.mark.parametrize(("actual_rows", "expected"), [(0, False), (2, True)])
def test_focused_full_prediction_complete_checks_expected_cell_model_version(
    monkeypatch: pytest.MonkeyPatch,
    actual_rows: int,
    expected: bool,
) -> None:
    """Existing rows for another model_version must not skip a run.

    Completion is tied to the model_version current routing would write. NAR
    carries no cell routing after the a957 revert (2026-07-03), so a venue-54
    grade-E race resolves to the category default
    (``iter12-nar-xgb-hpo-v8-clean188``).
    """

    @final
    class _FocusedCursor:
        def __init__(self) -> None:
            self.executed_params: list[object] = []

        def execute(self, query: str, params: object = None) -> None:
            del query
            self.executed_params.append(params)

        def fetchall(self) -> list[tuple[object, ...]]:
            return [
                ("H1", "E", "20", 1400, None, "0702", "54", 12),
                ("H2", "E", "20", 1400, None, "0702", "54", 12),
            ]

        def fetchone(self) -> tuple[int]:
            return (actual_rows,)

    @final
    class _FocusedConnection:
        def __init__(self, cursor: _FocusedCursor) -> None:
            self._cursor = cursor
            self.closed = False

        def cursor(self) -> _FocusedCursor:
            return self._cursor

        def close(self) -> None:
            self.closed = True

    cursor = _FocusedCursor()
    connection = _FocusedConnection(cursor)

    def _connect(database_url: str, connect_timeout: int) -> _FocusedConnection:
        assert database_url == "postgresql://example"
        assert connect_timeout > 0
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=_connect))
    params = PredictParams(
        category="nar",
        run_date="20260702",
        days_ahead=0,
        mode="full",
        keibajo_code="54",
        race_bango="03",
    )

    completion_fn = predict_upcoming.__dict__["_focused_full_prediction_complete"]
    assert callable(completion_fn)
    assert completion_fn("postgresql://example", params) is expected
    final_params = cursor.executed_params[-1]
    assert isinstance(final_params, tuple)
    assert final_params[-1] == predict_upcoming.NAR_TRANSFORMER_MODEL_VERSION
    assert connection.closed is True


@pytest.mark.parametrize(
    ("existing_row", "expected"),
    [((5, 1, 5), True), ((5, 1, 4), False), ((0, None, None), False)],
)
def test_focused_full_prediction_complete_uses_existing_prediction_fallback_when_sources_empty(
    monkeypatch: pytest.MonkeyPatch,
    existing_row: tuple[int, int | None, int | None],
    expected: bool,
) -> None:
    """A completed NAR iter40 race still counts when source rows aged out."""

    @final
    class _FocusedCursor:
        def __init__(self) -> None:
            self.executed_params: list[object] = []

        def execute(self, query: str, params: object = None) -> None:
            del query
            self.executed_params.append(params)

        def fetchall(self) -> list[tuple[object, ...]]:
            return []

        def fetchone(self) -> tuple[int, int | None, int | None]:
            return existing_row

    @final
    class _FocusedConnection:
        def __init__(self, cursor: _FocusedCursor) -> None:
            self._cursor = cursor
            self.closed = False

        def cursor(self) -> _FocusedCursor:
            return self._cursor

        def close(self) -> None:
            self.closed = True

    cursor = _FocusedCursor()
    connection = _FocusedConnection(cursor)

    def _connect(database_url: str, connect_timeout: int) -> _FocusedConnection:
        assert database_url == "postgresql://example"
        assert connect_timeout > 0
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=_connect))
    params = PredictParams(
        category="nar",
        run_date="20260710",
        days_ahead=0,
        mode="full",
        keibajo_code="45",
        race_bango="03",
    )

    completion_fn = predict_upcoming.__dict__["_focused_full_prediction_complete"]
    assert callable(completion_fn)
    assert completion_fn("postgresql://example", params) is expected
    final_params = cursor.executed_params[-1]
    assert isinstance(final_params, tuple)
    assert final_params[-1] == predict_upcoming.NAR_TRANSFORMER_MODEL_VERSION
    assert connection.closed is True


def test_focused_full_prediction_complete_uses_jra_kyoso_joken_code_for_model_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """JRA 703 focused completion must wait for the cell variant model_version."""

    candidate_version = "jra-cb-v9-sim-2013-clean-jockey-pedigree269"
    default_version = "jra-cb-v9-sim-2013-clean"

    @final
    class _Jra703Router:
        def has_routing(self, category: str) -> bool:
            return category == "jra"

        def routing_for(self, category: str) -> _FakeRouting:
            assert category == "jra"
            return _FakeRouting(
                variants={
                    "default": _FakeVariantSpec(default_version, 250, "catboost"),
                    "jra_kyoso_joken_703_jockey_pedigree269": _FakeVariantSpec(
                        candidate_version, 269, "catboost"
                    ),
                },
                default_variant="default",
            )

        def resolve_variant(
            self,
            category: str,
            entries: Sequence[Mapping[str, object]],
            card_max_race_bango: int | None = None,
        ) -> str:
            del card_max_race_bango
            assert category == "jra"
            class_codes = {str(entry.get("kyoso_joken_code", "")).strip() for entry in entries}
            if "703" in class_codes:
                return "jra_kyoso_joken_703_jockey_pedigree269"
            return "default"

    @final
    class _FocusedCursor:
        def __init__(self) -> None:
            self.executed_params: list[object] = []

        def execute(self, query: str, params: object = None) -> None:
            if not self.executed_params:
                assert "kyoso_joken_code" in query
            self.executed_params.append(params)

        def fetchall(self) -> list[tuple[object, ...]]:
            return [
                ("H1", "A", "24", 2000, "703", "0705", "10", 14),
                ("H2", "A", "24", 2000, "703", "0705", "10", 14),
            ]

        def fetchone(self) -> tuple[int]:
            return (2,)

    @final
    class _FocusedConnection:
        def __init__(self, cursor: _FocusedCursor) -> None:
            self._cursor = cursor
            self.closed = False

        def cursor(self) -> _FocusedCursor:
            return self._cursor

        def close(self) -> None:
            self.closed = True

    cursor = _FocusedCursor()
    connection = _FocusedConnection(cursor)

    def _connect(database_url: str, connect_timeout: int) -> _FocusedConnection:
        assert database_url == "postgresql://example"
        assert connect_timeout > 0
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=_connect))
    monkeypatch.setattr(predict_upcoming, "load_cell_router", lambda: _Jra703Router())
    params = PredictParams(
        category="jra",
        run_date="20260705",
        days_ahead=0,
        mode="full",
        keibajo_code="10",
        race_bango="02",
    )

    completion_fn = predict_upcoming.__dict__["_focused_full_prediction_complete"]
    assert callable(completion_fn)
    assert completion_fn("postgresql://example", params) is True
    final_params = cursor.executed_params[-1]
    assert isinstance(final_params, tuple)
    assert final_params[-1] == candidate_version
    assert final_params[-1] != default_version
    assert connection.closed is True


def test_focused_full_prediction_complete_uses_jra_prior_corner_cell_model_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """JRA 005 dirt small-field focused completion must wait for the prior-corner variant."""

    candidate_version = "jra-cb-v10-prior-corner274-2013"
    default_version = "jra-cb-v9-sim-2013-clean"

    @final
    class _JraPriorCornerRouter:
        def has_routing(self, category: str) -> bool:
            return category == "jra"

        def routing_for(self, category: str) -> _FakeRouting:
            assert category == "jra"
            return _FakeRouting(
                variants={
                    "default": _FakeVariantSpec(default_version, 250, "catboost"),
                    "prior_corner_dirt_smallfield_005": _FakeVariantSpec(
                        candidate_version, 274, "catboost"
                    ),
                },
                default_variant="default",
            )

        def resolve_variant(
            self,
            category: str,
            entries: Sequence[Mapping[str, object]],
            card_max_race_bango: int | None = None,
        ) -> str:
            del card_max_race_bango
            assert category == "jra"
            for entry in entries:
                track_code = str(entry.get("track_code", "")).strip()
                class_code = str(entry.get("kyoso_joken_code", "")).strip()
                runners = int(str(entry.get("shusso_tosu", "0")).strip() or "0")
                if track_code.startswith("2") and class_code == "005" and runners <= 10:
                    return "prior_corner_dirt_smallfield_005"
            return "default"

    @final
    class _FocusedCursor:
        def __init__(self) -> None:
            self.executed_params: list[object] = []

        def execute(self, query: str, params: object = None) -> None:
            if not self.executed_params:
                assert "kyoso_joken_code" in query
                assert "track_code" in query
                assert "shusso_tosu" in query
            self.executed_params.append(params)

        def fetchall(self) -> list[tuple[object, ...]]:
            return [
                ("H1", "A", "23", 1800, "005", "0705", "10", 10),
                ("H2", "A", "23", 1800, "005", "0705", "10", 10),
            ]

        def fetchone(self) -> tuple[int]:
            return (2,)

    @final
    class _FocusedConnection:
        def __init__(self, cursor: _FocusedCursor) -> None:
            self._cursor = cursor
            self.closed = False

        def cursor(self) -> _FocusedCursor:
            return self._cursor

        def close(self) -> None:
            self.closed = True

    cursor = _FocusedCursor()
    connection = _FocusedConnection(cursor)

    def _connect(database_url: str, connect_timeout: int) -> _FocusedConnection:
        assert database_url == "postgresql://example"
        assert connect_timeout > 0
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=_connect))
    monkeypatch.setattr(predict_upcoming, "load_cell_router", lambda: _JraPriorCornerRouter())
    params = PredictParams(
        category="jra",
        run_date="20260705",
        days_ahead=0,
        mode="full",
        keibajo_code="10",
        race_bango="02",
    )

    completion_fn = predict_upcoming.__dict__["_focused_full_prediction_complete"]
    assert callable(completion_fn)
    assert completion_fn("postgresql://example", params) is True
    final_params = cursor.executed_params[-1]
    assert isinstance(final_params, tuple)
    assert final_params[-1] == candidate_version
    assert final_params[-1] != default_version
    assert connection.closed is True


# ---------------------------------------------------------------------------
# Per-race mode=full feature generation (target_race wiring)
# ---------------------------------------------------------------------------
#
# When mode=full carries keibajoCode + raceBango, the Container builds features
# for a single race (DuckDB --target-race) instead of scanning the whole day.
# The production full-pipeline predict fn builds a "keibajo:bango" target_race
# string from the scope and forwards it through to the DuckDB feature builder;
# the HTTP handler parses the scope and the shared PredictCategoryFn contract
# carries it from iter_predict_chunks into the predict fn.
#
# The orchestration fns that wire this are module-private, so the tests reach
# the production predict fn through the module object (``getattr``) rather than
# importing the private name, which the strict type checker forbids.


def _noop_sleep(_seconds: float) -> None:
    """No-op sleep injected so the keepalive loop never blocks the test."""


def _build_real_full_predict_fn() -> Callable[..., int]:
    """Return the production full-pipeline predict fn (R2 disabled).

    Drives the real per-category orchestration so the test exercises the actual
    ``keibajo:bango`` target_race construction and its forward to the pipeline.
    """
    make_fn: Callable[..., tuple[Callable[..., int], object, object, object]] = vars(
        predict_upcoming
    )["_make_predict_fn"]
    predict_fn, _payload_fn, _per_race_fn, _cache_populate_fn = make_fn(
        _DB_URL, Path("/models"), _DB_URL, None
    )
    return predict_fn


def _capture_target_race(captured: dict[str, object]) -> Callable[..., Mapping[str, object]]:
    """Build a fake ``build_upcoming_feature_rows`` that records its target_race."""

    def _fake_build(
        category: Category,
        target_date: str,
        days_ahead: int,
        database_url: str,
        target_race: str | None = None,
    ) -> Mapping[str, list[Mapping[str, object]]]:
        captured["target_race"] = target_race
        return {}

    return _fake_build


def test_full_predict_fn_builds_target_race_from_scope() -> None:
    """A per-race full request forwards "keibajo:bango" to the DuckDB builder."""
    import pipeline_runner

    captured: dict[str, object] = {}
    predict_fn = _build_real_full_predict_fn()
    with (
        patch.object(
            pipeline_runner,
            "build_upcoming_feature_rows",
            side_effect=_capture_target_race(captured),
        ),
        patch("predict_upcoming._score_and_flush_races", return_value=3),
    ):
        written = predict_fn("jra", "20260628", 0, "01", "05")

    assert captured["target_race"] == "01:05"
    assert written == 3


def test_full_predict_fn_target_race_none_without_scope() -> None:
    """The whole-window full request forwards target_race=None (no per-race filter)."""
    import pipeline_runner

    captured: dict[str, object] = {"target_race": "sentinel"}
    predict_fn = _build_real_full_predict_fn()
    with (
        patch.object(
            pipeline_runner,
            "build_upcoming_feature_rows",
            side_effect=_capture_target_race(captured),
        ),
        patch("predict_upcoming._score_and_flush_races", return_value=0),
    ):
        written = predict_fn("nar", "20260628", 2)

    assert captured["target_race"] is None
    assert written == 0


def test_predict_category_filters_to_target_race_before_score_and_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scoped predict_category must score/UPSERT only the named race."""
    captured: dict[str, object] = {}

    def fake_build(
        category: Category,
        window: predict_upcoming.PredictWindow,
        target_race: str | None = None,
        r2_config: object = None,
    ) -> Mapping[str, list[Mapping[str, object]]]:
        return {
            "jra:2026:0712:05:11": [{"umaban": 1}],
            "jra:2026:0712:05:12": [{"umaban": 2}],
        }

    def fake_score_and_flush(
        database_url: str,
        category: Category,
        models_dir: Path,
        races: Mapping[str, Sequence[Mapping[str, object]]],
        card_max_race_bango: int | None = None,
    ) -> int:
        captured["races"] = dict(races)
        return len(races)

    monkeypatch.setattr(predict_upcoming, "_build_feature_rows", fake_build)
    monkeypatch.setattr(predict_upcoming, "_score_and_flush_races", fake_score_and_flush)

    written = predict_upcoming.predict_category(
        _DB_URL,
        "jra",
        Path("/models"),
        predict_upcoming.PredictWindow(target_date="20260712", days_ahead=0, database_url=_DB_URL),
        target_race="05:11",
    )

    assert written == 1
    scored = captured["races"]
    assert isinstance(scored, dict)
    assert list(scored.keys()) == ["jra:2026:0712:05:11"]


def test_predict_category_without_target_race_keeps_all_races(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_build(
        category: Category,
        window: predict_upcoming.PredictWindow,
        target_race: str | None = None,
        r2_config: object = None,
    ) -> Mapping[str, list[Mapping[str, object]]]:
        return {
            "jra:2026:0712:05:11": [{"umaban": 1}],
            "jra:2026:0712:05:12": [{"umaban": 2}],
        }

    def fake_score_and_flush(
        database_url: str,
        category: Category,
        models_dir: Path,
        races: Mapping[str, Sequence[Mapping[str, object]]],
        card_max_race_bango: int | None = None,
    ) -> int:
        captured["races"] = dict(races)
        return len(races)

    monkeypatch.setattr(predict_upcoming, "_build_feature_rows", fake_build)
    monkeypatch.setattr(predict_upcoming, "_score_and_flush_races", fake_score_and_flush)

    written = predict_upcoming.predict_category(
        _DB_URL,
        "jra",
        Path("/models"),
        predict_upcoming.PredictWindow(target_date="20260712", days_ahead=0, database_url=_DB_URL),
    )

    assert written == 2
    scored = captured["races"]
    assert isinstance(scored, dict)
    assert sorted(scored.keys()) == [
        "jra:2026:0712:05:11",
        "jra:2026:0712:05:12",
    ]


# ---------------------------------------------------------------------------
# _make_predict_fn's focused-full cache populate closure (explicit-args,
# NOT the _last_run-based path -- see FocusedFullCachePopulateFn's docstring
# for why the two must stay independent).
# ---------------------------------------------------------------------------


def _build_real_predict_fn_and_cache_populate(
    store: FocusedFullCacheStore | None,
) -> tuple[Callable[..., int], Callable[[PredictParams], None]]:
    make_fn: Callable[
        ..., tuple[Callable[..., int], object, object, Callable[[PredictParams], None]]
    ] = vars(predict_upcoming)["_make_predict_fn"]
    predict_fn, _payload_fn, _per_race_fn, populate_fn = make_fn(
        _DB_URL, Path("/models"), _DB_URL, None, store
    )
    return predict_fn, populate_fn


def test_populate_focused_full_cache_noop_when_store_not_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _predict_fn, populate_fn = _build_real_predict_fn_and_cache_populate(None)
    params = PredictParams(
        category="jra", run_date="20260712", days_ahead=0, keibajo_code="05", race_bango="09"
    )
    populate_fn(params)  # must not raise even though no run ever happened


def test_populate_focused_full_cache_stores_payload_for_this_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pipeline_runner

    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path)
    final_dir = tmp_path / "feat-jra-v7-final"
    final_dir.mkdir()
    (final_dir / "features.parquet").write_bytes(b"not-real-parquet-bytes")

    store = FocusedFullCacheStore()
    _predict_fn, populate_fn = _build_real_predict_fn_and_cache_populate(store)
    params = PredictParams(
        category="jra", run_date="20260712", days_ahead=0, keibajo_code="05", race_bango="09"
    )

    populate_fn(params)

    payload = store.pop(build_focused_full_race_key(params))
    assert payload is not None
    # Focused-full must not overwrite the whole-day feat-cache key with a
    # single-race parquet. Seed the per-race object so scoped rescore can hit it.
    assert payload.parquet_base64 is None
    assert payload.parquet_key is None
    assert payload.per_race_parquets == [
        {
            "parquetBase64": base64.b64encode(b"not-real-parquet-bytes").decode("ascii"),
            "parquetKey": "feat-cache/catalog-v1/jra/20260712/05/09/features.parquet",
        }
    ]


def test_populate_focused_full_cache_stores_nothing_when_no_parquet_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pipeline_runner

    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path)

    store = FocusedFullCacheStore()
    _predict_fn, populate_fn = _build_real_predict_fn_and_cache_populate(store)
    params = PredictParams(
        category="jra", run_date="20260712", days_ahead=0, keibajo_code="05", race_bango="09"
    )

    populate_fn(params)

    assert store.pop(build_focused_full_race_key(params)) is None


def test_populate_focused_full_cache_two_races_do_not_cross_contaminate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression test for the _last_run shared-state race this design avoids
    by construction: two different (category, run_date) runs must each land
    under their OWN race key with their OWN bytes, never swapped."""
    import pipeline_runner

    monkeypatch.setattr(pipeline_runner, "WORK_DIR", tmp_path)
    jra_dir = tmp_path / "feat-jra-v7-final"
    jra_dir.mkdir()
    (jra_dir / "features.parquet").write_bytes(b"jra-bytes")
    nar_dir = tmp_path / "feat-nar-v7-final"
    nar_dir.mkdir()
    (nar_dir / "features.parquet").write_bytes(b"nar-bytes")

    store = FocusedFullCacheStore()
    _predict_fn, populate_fn = _build_real_predict_fn_and_cache_populate(store)
    jra_params = PredictParams(
        category="jra", run_date="20260712", days_ahead=0, keibajo_code="05", race_bango="09"
    )
    nar_params = PredictParams(
        category="nar", run_date="20260712", days_ahead=0, keibajo_code="44", race_bango="02"
    )

    populate_fn(jra_params)
    populate_fn(nar_params)

    jra_payload = store.pop(build_focused_full_race_key(jra_params))
    nar_payload = store.pop(build_focused_full_race_key(nar_params))
    assert jra_payload is not None
    assert nar_payload is not None
    assert jra_payload.per_race_parquets == [
        {
            "parquetBase64": base64.b64encode(b"jra-bytes").decode("ascii"),
            "parquetKey": "feat-cache/catalog-v1/jra/20260712/05/09/features.parquet",
        }
    ]
    assert nar_payload.per_race_parquets == [
        {
            "parquetBase64": base64.b64encode(b"nar-bytes").decode("ascii"),
            "parquetKey": "feat-cache/catalog-v1/nar/20260712/44/02/features.parquet",
        }
    ]


def test_parse_predict_params_full_mode_keeps_race_scope() -> None:
    """A full-mode request carries keibajoCode / raceBango as the per-race scope."""
    result = parse_predict_params(
        "category=jra&runDate=20260628&daysAhead=0&mode=full&keibajoCode=01&raceBango=05"
    )
    assert not isinstance(result, str)
    assert result.mode == "full"
    assert result.keibajo_code == "01"
    assert result.race_bango == "05"


def test_full_mode_handler_flow_passes_race_scope_to_predict_fn() -> None:
    """Mirror _PredictHandler.do_GET: parse a full-mode query, then drive the stream.

    Proves keibajoCode / raceBango parsed from a full-mode request reach the
    predict fn (the per-race feature-generation scope), exactly as the handler
    wires ``parse_predict_params`` -> ``iter_predict_chunks(result, predict_fn)``.
    """
    parsed = parse_predict_params(
        "category=jra&runDate=20260628&daysAhead=0&mode=full&keibajoCode=01&raceBango=05"
    )
    assert not isinstance(parsed, str)

    recorded: list[tuple[str, str, int, str | None, str | None]] = []

    def _recording_predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        card_max_race_bango: int | None = None,
    ) -> int:
        recorded.append((category, run_date, days_ahead, keibajo_code, race_bango))
        return 1

    list(iter_predict_chunks(parsed, _recording_predict, sleep_fn=_noop_sleep))

    assert recorded == [("jra", "20260628", 0, "01", "05")]


def test_predict_params_default_full_mode_has_no_race_scope() -> None:
    """The whole-window full request leaves keibajoCode / raceBango unset (None)."""
    params = PredictParams(category="jra", run_date="20260628", days_ahead=0)
    assert params.mode == "full"
    assert params.keibajo_code is None
    assert params.race_bango is None


# ---------------------------------------------------------------------------
# _build_feature_rows — day-base/RACE_CHAIN split wiring
# ---------------------------------------------------------------------------


def test_build_feature_rows_uses_split_path_when_enabled_and_target_race_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pipeline_runner

    monkeypatch.setenv("DAY_BASE_SPLIT_ENABLED", "jra")

    split_called: list[object] = []

    def fake_split(
        category: str,
        target_date: str,
        days_ahead: int,
        database_url: str,
        target_race: str,
        r2_config: object = None,
    ) -> Mapping[str, list[Mapping[str, object]]]:
        split_called.append(
            (category, target_date, days_ahead, database_url, target_race, r2_config)
        )
        return {"race-1": [{"umaban": 1}]}

    def fake_full(*args: object, **kwargs: object) -> Mapping[str, list[Mapping[str, object]]]:
        raise AssertionError("full path must not be called when split succeeds")

    monkeypatch.setattr(pipeline_runner, "build_upcoming_feature_rows_split", fake_split)
    monkeypatch.setattr(pipeline_runner, "build_upcoming_feature_rows", fake_full)

    window = predict_upcoming.PredictWindow(
        target_date="20260712", days_ahead=0, database_url="postgresql://u:p@h/db"
    )
    result = _build_feature_rows("jra", window, target_race="05:11")

    assert result == {"race-1": [{"umaban": 1}]}
    assert len(split_called) == 1


def test_build_feature_rows_falls_back_to_full_when_split_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pipeline_runner

    monkeypatch.setenv("DAY_BASE_SPLIT_ENABLED", "jra")
    monkeypatch.setattr(
        pipeline_runner, "build_upcoming_feature_rows_split", lambda *args, **kwargs: None
    )

    full_called: list[object] = []

    def fake_full(
        category: str,
        target_date: str,
        days_ahead: int,
        database_url: str,
        target_race: str | None = None,
    ) -> Mapping[str, list[Mapping[str, object]]]:
        full_called.append((category, target_date, days_ahead, database_url, target_race))
        return {"race-2": [{"umaban": 2}]}

    monkeypatch.setattr(pipeline_runner, "build_upcoming_feature_rows", fake_full)

    window = predict_upcoming.PredictWindow(
        target_date="20260712", days_ahead=0, database_url="postgresql://u:p@h/db"
    )
    result = _build_feature_rows("jra", window, target_race="05:11")

    assert result == {"race-2": [{"umaban": 2}]}
    assert len(full_called) == 1


def test_build_feature_rows_skips_split_when_flag_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pipeline_runner

    monkeypatch.delenv("DAY_BASE_SPLIT_ENABLED", raising=False)

    def fake_split(*args: object, **kwargs: object) -> Mapping[str, list[Mapping[str, object]]]:
        raise AssertionError("split path must not be called when flag disabled")

    monkeypatch.setattr(pipeline_runner, "build_upcoming_feature_rows_split", fake_split)
    monkeypatch.setattr(
        pipeline_runner, "build_upcoming_feature_rows", lambda *args, **kwargs: {"race-3": []}
    )

    window = predict_upcoming.PredictWindow(
        target_date="20260712", days_ahead=0, database_url="postgresql://u:p@h/db"
    )
    result = _build_feature_rows("jra", window, target_race="05:11")

    assert result == {"race-3": []}


def test_build_feature_rows_skips_split_when_target_race_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Whole-category/whole-day dispatch (target_race=None) must never hit the
    single-race split path, even when the category is in the allowlist."""
    import pipeline_runner

    monkeypatch.setenv("DAY_BASE_SPLIT_ENABLED", "jra")

    def fake_split(*args: object, **kwargs: object) -> Mapping[str, list[Mapping[str, object]]]:
        raise AssertionError("split path must not be called for whole-category dispatch")

    monkeypatch.setattr(pipeline_runner, "build_upcoming_feature_rows_split", fake_split)
    monkeypatch.setattr(
        pipeline_runner, "build_upcoming_feature_rows", lambda *args, **kwargs: {"race-4": []}
    )

    window = predict_upcoming.PredictWindow(
        target_date="20260712", days_ahead=0, database_url="postgresql://u:p@h/db"
    )
    result = _build_feature_rows("jra", window)

    assert result == {"race-4": []}


def test_build_feature_rows_skips_split_during_scoped_rescore_cache_miss_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scoped CacheMiss recovery must not inline-build a whole-day day-base."""
    import pipeline_runner

    monkeypatch.setenv("DAY_BASE_SPLIT_ENABLED", "jra")

    def fake_split(*args: object, **kwargs: object) -> Mapping[str, list[Mapping[str, object]]]:
        raise AssertionError("split path must not run during scoped CacheMiss fallback")

    full_called: list[str | None] = []

    def fake_full(
        category: str,
        target_date: str,
        days_ahead: int,
        database_url: str,
        target_race: str | None = None,
    ) -> Mapping[str, list[Mapping[str, object]]]:
        full_called.append(target_race)
        return {"jra:2026:0712:05:11": [{"umaban": 1}]}

    monkeypatch.setattr(pipeline_runner, "build_upcoming_feature_rows_split", fake_split)
    monkeypatch.setattr(pipeline_runner, "build_upcoming_feature_rows", fake_full)

    window = predict_upcoming.PredictWindow(
        target_date="20260712", days_ahead=0, database_url="postgresql://u:p@h/db"
    )
    with activate_scoped_rescore_cache_miss_fallback():
        result = _build_feature_rows("jra", window, target_race="05:11")

    assert result == {"jra:2026:0712:05:11": [{"umaban": 1}]}
    assert full_called == ["05:11"]
