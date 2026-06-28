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
autosuspend during idle, lost/closed connection errors). Failure modes
observed in production (June 2026):
  1. ``psycopg.errors.AdminShutdown``: Neon compute autosuspended during the
     long feature build; the write connection finds a dead socket.
  2. ``psycopg.OperationalError`` / "Name or service not known" (gai errno -2,
     EAI_NONAME): transient DNS failure at container startup (Docker bridge
     resolver blip).
  3. ``psycopg.OperationalError`` / "Temporary failure in name resolution"
     (gai errno -3, EAI_AGAIN): upstream DNS server timed out / momentarily
     unavailable — distinct from EAI_NONAME but equally transient. Observed
     on 2026-06-28 when NAR was the only category to fall into this gap
     between Ban-ei (succeeded) and the next retry window.
  4. "failed to resolve host" (psycopg wrapper prefix surrounding both
     EAI_NONAME and EAI_AGAIN messages): future-proofs against new glibc /
     musl error string variants.
  5. "connection is lost" / "connection is closed" / SSL eof: Neon closed
     the TCP connection between connect and first use (race condition).
All are transient and resolve on a fresh connect attempt.
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
    # DNS gai errno -2 (EAI_NONAME): "Name or service not known".
    "name or service not known",
    # DNS gai errno -3 (EAI_AGAIN): "Temporary failure in name resolution"
    # — distinct from EAI_NONAME but equally transient (upstream resolver
    # timeout / momentary unavailability). Added 2026-06-29 after the
    # 2026-06-28 NAR failure where the prior token list missed this variant.
    "temporary failure in name resolution",
    # psycopg's gaierror wrapper prefixes both EAI_NONAME and EAI_AGAIN
    # messages with this string. Matching the prefix future-proofs against
    # other glibc / musl gai error variants we have not yet observed.
    "failed to resolve host",
    "connection is lost",
    "connection is closed",
    "connection refused",
    "could not connect to server",
    "server closed the connection unexpectedly",
    "ssl connection has been closed",
    # psycopg surfaces a mid-query SSL hangup as "consuming input failed:
    # SSL error: unexpected eof while reading" — also transient (Neon TCP
    # closed mid-stream); observed 2026-06-28 alongside DNS gap.
    "consuming input failed",
    "unexpected eof",
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


def is_transient_error(exc: BaseException) -> bool:
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
            if not is_transient_error(exc):
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
