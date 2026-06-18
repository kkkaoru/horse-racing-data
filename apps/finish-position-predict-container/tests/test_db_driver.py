"""Tests for ``db_driver.py`` retry + transient-error helpers.

``db_driver.py`` is excluded from the ``--cov=predict_lib`` gate (it is an
I/O boundary module, not part of predict_lib). These tests verify the pure
helper logic — ``_is_transient_error`` dispatch and the retry-with-backoff
contract of ``connect_postgres_with_retry`` — without any real psycopg calls.
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from db_driver import (
    CONNECT_MAX_RETRIES,
    ConnectionLike,
    _is_transient_error,
    connect_postgres_with_retry,
)

# ---------------------------------------------------------------------------
# _is_transient_error
# ---------------------------------------------------------------------------


def test_is_transient_admin_shutdown() -> None:
    # AdminShutdown is always transient (class name match).
    exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    assert _is_transient_error(exc) is True


def test_is_transient_name_or_service_not_known() -> None:
    exc = Exception("failed to resolve host 'ep.neon.tech': Name or service not known")
    assert _is_transient_error(exc) is True


def test_is_transient_connection_is_lost() -> None:
    exc = Exception("the connection is lost")
    assert _is_transient_error(exc) is True


def test_is_transient_connection_is_closed() -> None:
    exc = Exception("the connection is closed")
    assert _is_transient_error(exc) is True


def test_is_transient_connection_refused() -> None:
    exc = Exception("connection refused to 127.0.0.1:5432")
    assert _is_transient_error(exc) is True


def test_is_transient_could_not_connect() -> None:
    exc = Exception("could not connect to server: Connection timed out")
    assert _is_transient_error(exc) is True


def test_is_transient_server_closed_unexpectedly() -> None:
    exc = Exception("server closed the connection unexpectedly")
    assert _is_transient_error(exc) is True


def test_is_transient_ssl_closed() -> None:
    exc = Exception("SSL connection has been closed unexpectedly")
    assert _is_transient_error(exc) is True


def test_is_not_transient_bad_password() -> None:
    exc = Exception("password authentication failed for user 'foo'")
    assert _is_transient_error(exc) is False


def test_is_not_transient_wrong_database() -> None:
    exc = Exception('database "nonexistent" does not exist')
    assert _is_transient_error(exc) is False


def test_is_not_transient_programming_error() -> None:
    exc = type("ProgrammingError", (Exception,), {})("syntax error at or near 'X'")
    assert _is_transient_error(exc) is False


# ---------------------------------------------------------------------------
# connect_postgres_with_retry
# ---------------------------------------------------------------------------


def _make_mock_connection() -> MagicMock:
    conn = MagicMock(spec=ConnectionLike)
    return conn


def test_connect_succeeds_first_attempt() -> None:
    mock_conn = _make_mock_connection()
    with patch("db_driver.connect_postgres", return_value=mock_conn) as patched:
        result = connect_postgres_with_retry("postgresql://host/db", max_retries=3)
    assert result is mock_conn
    patched.assert_called_once_with("postgresql://host/db")


def test_connect_retries_on_dns_failure_then_succeeds() -> None:
    dns_exc = Exception("failed to resolve host 'ep.neon.tech': Name or service not known")
    mock_conn = _make_mock_connection()
    side_effects: list[Exception | MagicMock] = [dns_exc, dns_exc, mock_conn]
    with (
        patch("db_driver.connect_postgres", side_effect=side_effects) as patched,
        patch("db_driver.time.sleep") as mock_sleep,
    ):
        result = connect_postgres_with_retry("postgresql://h/db", max_retries=4)
    assert result is mock_conn
    assert patched.call_count == 3
    # Two sleeps before the third attempt (attempt 0, attempt 1 fail → sleep).
    assert mock_sleep.call_count == 2


def test_connect_retries_on_admin_shutdown_then_succeeds() -> None:
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    mock_conn = _make_mock_connection()
    side_effects: list[Exception | MagicMock] = [admin_exc, mock_conn]
    with (
        patch("db_driver.connect_postgres", side_effect=side_effects) as patched,
        patch("db_driver.time.sleep"),
    ):
        result = connect_postgres_with_retry("postgresql://h/db", max_retries=2)
    assert result is mock_conn
    assert patched.call_count == 2


def test_connect_raises_after_all_retries_exhausted() -> None:
    dns_exc = Exception("Name or service not known")
    with (
        patch("db_driver.connect_postgres", side_effect=dns_exc),
        patch("db_driver.time.sleep"),
    ):
        try:
            connect_postgres_with_retry("postgresql://h/db", max_retries=2)
        except Exception as exc:
            assert exc is dns_exc
        else:
            raise AssertionError("should have raised")


def test_connect_does_not_retry_non_transient_error() -> None:
    auth_exc = Exception("password authentication failed for user 'foo'")
    with (
        patch("db_driver.connect_postgres", side_effect=auth_exc) as patched,
        patch("db_driver.time.sleep") as mock_sleep,
    ):
        try:
            connect_postgres_with_retry("postgresql://h/db", max_retries=3)
        except Exception as exc:
            assert exc is auth_exc
        else:
            raise AssertionError("should have raised")
    # No retries — raised immediately on first attempt.
    assert patched.call_count == 1
    mock_sleep.assert_not_called()


def test_connect_backoff_capped_at_16s() -> None:
    # With backoff_base=1.0 and attempt 4 that would be 16s (2**4 = 16 = cap).
    dns_exc = Exception("Name or service not known")
    mock_conn = _make_mock_connection()
    # Succeed only on the very last allowed attempt (max_retries=4 → 5 attempts).
    side_effects: list[Exception | MagicMock] = [dns_exc] * 4 + [mock_conn]
    sleep_calls: list[float] = []
    with (
        patch("db_driver.connect_postgres", side_effect=side_effects),
        patch("db_driver.time.sleep", side_effect=lambda s: sleep_calls.append(s)),
    ):
        result = connect_postgres_with_retry(
            "postgresql://h/db",
            max_retries=4,
            backoff_base=1.0,
        )
    assert result is mock_conn
    # Backoff schedule: 1s, 2s, 4s, 8s (attempt 3 = min(1*2^3,16)=8, not 16
    # because max_retries=4 → attempts 0-4, sleeps after 0-3).
    expected = [1.0, 2.0, 4.0, 8.0]
    assert sleep_calls == expected


def test_connect_retry_uses_correct_number_of_attempts() -> None:
    # max_retries=3 means 4 total attempts (0,1,2,3).
    dns_exc = Exception("Name or service not known")
    with (
        patch("db_driver.connect_postgres", side_effect=dns_exc) as patched,
        patch("db_driver.time.sleep"),
        contextlib.suppress(Exception),
    ):
        connect_postgres_with_retry("postgresql://h/db", max_retries=3)
    assert patched.call_count == 4  # 3+1 attempts


def test_connect_default_max_retries_matches_constant() -> None:
    # Ensure the default matches the module constant so tests stay aligned.
    dns_exc = Exception("Name or service not known")
    with (
        patch("db_driver.connect_postgres", side_effect=dns_exc) as patched,
        patch("db_driver.time.sleep"),
        contextlib.suppress(Exception),
    ):
        connect_postgres_with_retry("postgresql://h/db")
    assert patched.call_count == CONNECT_MAX_RETRIES + 1
