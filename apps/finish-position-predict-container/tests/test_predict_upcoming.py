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

import sys
from pathlib import Path
from typing import final, override
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Import the private helpers directly so the tests stay I/O-free.
from predict_upcoming import (
    _execute,
    _flush_predictions,
    extract_race_class_code,
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
    # None so the per-class router falls back to iter14.
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
# _execute — reconnect-on-write
# ---------------------------------------------------------------------------

_DB_URL = "postgresql://host/db"


def test_execute_succeeds_on_happy_path() -> None:
    # Normal path: execute+commit returns the same connection unchanged.
    conn = _StubConnection()
    result = _execute(conn, "SELECT 1", [], _DB_URL)
    assert result is conn
    assert conn.committed == 1
    assert conn.rolledback == 0


def test_execute_non_transient_error_propagates_without_reconnect() -> None:
    # Non-transient errors (e.g. programming error) must NOT trigger a reconnect.
    auth_exc = Exception("password authentication failed")
    conn = _StubConnection(raise_on_execute=auth_exc)
    try:
        _execute(conn, "SELECT 1", [], _DB_URL)
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
        result = _execute(bad_conn, "INSERT ...", ["p"], _DB_URL)

    assert result is fresh_conn
    assert fresh_conn.committed == 1
    mock_connect.assert_called_once_with(_DB_URL)
    # Old connection was asked to rollback + close (both may fail gracefully).


def test_execute_reconnects_on_connection_is_lost() -> None:
    lost_exc = Exception("the connection is lost")
    bad_conn = _StubConnection(raise_on_execute=lost_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn):
        result = _execute(bad_conn, "INSERT ...", [], _DB_URL)

    assert result is fresh_conn
    assert fresh_conn.committed == 1


def test_execute_retry_failure_propagates() -> None:
    # If the reconnect attempt also raises, that error propagates to the caller.
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    bad_conn = _StubConnection(raise_on_execute=admin_exc)
    also_bad_conn = _StubConnection(raise_on_execute=RuntimeError("retry also failed"))

    with patch("predict_upcoming._connect", return_value=also_bad_conn):
        try:
            _execute(bad_conn, "INSERT ...", [], _DB_URL)
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
        result = _execute(bad_conn, "INSERT ...", [], _DB_URL)

    assert result is fresh_conn
    assert fresh_conn.committed == 1


# ---------------------------------------------------------------------------
# _flush_predictions — per-race dedup + reconnect propagation
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
    written, returned_conn = _flush_predictions(conn, [], _DB_URL)
    assert written == 0
    assert returned_conn is conn
    assert conn.committed == 0


def test_flush_predictions_writes_rows_and_returns_connection() -> None:
    conn = _StubConnection()
    rows = [_make_pred_row("20260619:05:11:01:01", "HORSE1")]
    written, returned_conn = _flush_predictions(conn, rows, _DB_URL)
    assert written == 1
    assert returned_conn is conn
    assert conn.committed >= 1


def test_flush_predictions_returns_fresh_conn_after_reconnect() -> None:
    # Simulate AdminShutdown on first _execute call; verify the returned
    # connection is the fresh one (not the original dead conn).
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    dead_conn = _StubConnection(raise_on_execute=admin_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn):
        rows = [_make_pred_row("20260619:05:11:01:01", "HORSE1")]
        written, returned_conn = _flush_predictions(dead_conn, rows, _DB_URL)

    assert returned_conn is fresh_conn
    assert written == 1
    assert fresh_conn.committed >= 1
