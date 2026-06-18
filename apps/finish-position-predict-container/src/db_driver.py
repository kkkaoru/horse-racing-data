"""Neon / Postgres connection boundary.

Isolates the dynamically-imported psycopg driver behind a minimal typed
``ConnectionLike`` protocol so the rest of the predictor stays strict, using the
same importlib idiom as the viewer's ``default_psycopg_connect``: resolving
``psycopg.connect`` dynamically keeps the connection typed as ``Any`` (assignable
to our protocol) without pulling psycopg's concrete Connection stub, whose
overloaded ``cursor()`` over-constrains our minimal surface. I/O boundary — not
unit-tested; exercised at deploy time per DEPLOY.md.

Retry logic: ``connect_postgres_with_retry`` wraps ``connect_postgres`` with
exponential backoff for transient failures (DNS resolution failures, Neon
autosuspend during idle, lost/closed connection errors). Three failure modes
observed in production (June 2026):
  1. ``psycopg.errors.AdminShutdown``: Neon compute autosuspended during the
     long feature build; the write connection finds a dead socket.
  2. ``psycopg.OperationalError`` / "Name or service not known": transient DNS
     failure at container startup (Docker bridge resolver blip).
  3. "connection is lost" / "connection is closed": Neon closed the TCP
     connection between connect and first use (race condition).
All three are transient and resolve on a fresh connect attempt.
"""

from __future__ import annotations

import importlib
import sys
import time
from typing import Protocol, cast

# Transient error substrings that warrant a retry. AdminShutdown is a psycopg
# error class name (not a message substring); we also match it by class name
# check in connect_postgres_with_retry. The string tokens here cover the
# OperationalError + "connection is lost/closed" family.
_TRANSIENT_ERROR_TOKENS: tuple[str, ...] = (
    "name or service not known",
    "connection is lost",
    "connection is closed",
    "connection refused",
    "could not connect to server",
    "server closed the connection unexpectedly",
    "ssl connection has been closed",
)

CONNECT_MAX_RETRIES: int = 4
CONNECT_BACKOFF_BASE_SECONDS: float = 1.0


class CursorLike(Protocol):
    def execute(self, query: str, params: object = ...) -> object: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


class ConnectionLike(Protocol):
    def cursor(self) -> CursorLike: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


def connect_postgres(database_url: str) -> ConnectionLike:
    """Open a psycopg connection typed as the minimal ``ConnectionLike``."""
    module = importlib.import_module("psycopg")
    connect_fn = module.connect
    # cast is necessary because psycopg.Connection.cursor() accepts an optional
    # cursor_factory keyword argument that widens its signature beyond what our
    # minimal CursorLike protocol declares.  The runtime behaviour is identical;
    # we narrow the view here so the rest of the codebase can stay strictly typed.
    return cast("ConnectionLike", connect_fn(database_url))


def _is_transient_error(exc: BaseException) -> bool:
    """Return True when ``exc`` looks like a transient Neon/network error.

    Checks both the exception class name (for ``AdminShutdown``, which is a
    psycopg error subclass with a distinctive name) and the lowercased message
    string against ``_TRANSIENT_ERROR_TOKENS`` (for OperationalError variants).
    """
    # AdminShutdown is always transient: Neon compute woke up or restarted.
    if type(exc).__name__ == "AdminShutdown":
        return True
    msg = str(exc).lower()
    return any(token in msg for token in _TRANSIENT_ERROR_TOKENS)


def connect_postgres_with_retry(
    database_url: str,
    max_retries: int = CONNECT_MAX_RETRIES,
    backoff_base: float = CONNECT_BACKOFF_BASE_SECONDS,
) -> ConnectionLike:
    """Open a psycopg connection with exponential-backoff retry on transient errors.

    Retries up to ``max_retries`` times for DNS failures, AdminShutdown,
    "connection is lost/closed", and similar transient Neon/network errors.
    Non-transient errors (bad credentials, wrong database name) are re-raised
    immediately without retrying.

    Backoff schedule: ``backoff_base * 2**attempt`` seconds between attempts
    (1s, 2s, 4s, 8s for the defaults), capped at 16s to keep total added
    latency under ~30s for the worst case.
    """
    last_exc: BaseException | None = None
    for attempt in range(max_retries + 1):
        try:
            return connect_postgres(database_url)
        except BaseException as exc:
            if not _is_transient_error(exc):
                raise
            last_exc = exc
            if attempt == max_retries:
                break
            sleep_seconds = min(backoff_base * (2**attempt), 16.0)
            print(
                f"[db_driver] connect attempt {attempt + 1} failed: {exc!r} "
                f"— retrying in {sleep_seconds:.1f}s",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)
    # All retries exhausted — raise the last transient error.
    assert last_exc is not None
    raise last_exc
