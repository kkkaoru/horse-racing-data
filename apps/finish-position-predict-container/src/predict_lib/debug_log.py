"""Process-wide debug-log helper for container processing stderr.

Default is silent. The HTTP layer sets ``PREDICT_DEBUG_LOGS`` for the duration
of a ``debug=1`` / ``debug=true`` request, then restores the previous value so
tests and later requests stay isolated. Result NDJSON on stdout is protocol,
not a debug log, and must stay unsuppressed.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from queue import Empty, SimpleQueue
from typing import Final
from urllib.parse import parse_qs

PREDICT_DEBUG_LOGS_ENV: Final[str] = "PREDICT_DEBUG_LOGS"
TRUE_DEBUG_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on", "debug"})
_DEBUG_PROGRESS_QUEUE: Final[SimpleQueue[str]] = SimpleQueue()
_OPERATIONAL_PROGRESS_QUEUE: Final[SimpleQueue[str]] = SimpleQueue()
"""Process-wide queue of debug-only pipeline step tokens.

``record_debug_progress`` is called from the predict thread (where
``debug_logs_scope`` is active). ``drain_debug_progress`` is called from the
HTTP generator thread so those tokens can ride the existing NDJSON progress
protocol that the Worker already logs as ``Predict progress``.
"""


def parse_debug_flag(raw: str | None) -> bool:
    """Return True only for explicit debug tokens; missing/invalid is off."""
    if raw is None:
        return False
    return raw.strip().lower() in TRUE_DEBUG_VALUES


def query_debug_enabled(query_string: str) -> bool:
    """Read the first ``debug`` query value; missing or invalid is off."""
    values = parse_qs(query_string, keep_blank_values=True).get("debug")
    if not values:
        return False
    return parse_debug_flag(values[0])


def debug_logs_enabled() -> bool:
    return parse_debug_flag(os.environ.get(PREDICT_DEBUG_LOGS_ENV))


def debug_log(*args: object) -> None:
    """Write one stderr line only when debug mode is on."""
    if not debug_logs_enabled():
        return
    print(*args, file=sys.stderr, flush=True)


def record_debug_progress(message: str) -> None:
    """Queue one Worker-visible step token when debug mode is on.

    No-op when debug is off so the default ``/predict`` stream stays quiet.
    """
    if not debug_logs_enabled():
        return
    _DEBUG_PROGRESS_QUEUE.put(message)


def drain_debug_progress() -> list[str]:
    """Pop every queued debug step token. Empty when debug is off or idle."""
    messages: list[str] = []
    while True:
        try:
            messages.append(_DEBUG_PROGRESS_QUEUE.get_nowait())
        except Empty:
            return messages


def record_operational_progress(message: str) -> None:
    """Queue low-volume, credential-free production telemetry.

    Unlike debug progress this is always enabled. Callers must only pass a
    bounded allowlist of operational tokens; raw argv, URLs, and exceptions do
    not belong on this channel.
    """
    _OPERATIONAL_PROGRESS_QUEUE.put(message)


def drain_operational_progress() -> list[str]:
    """Pop every queued operational progress token."""
    messages: list[str] = []
    while True:
        try:
            messages.append(_OPERATIONAL_PROGRESS_QUEUE.get_nowait())
        except Empty:
            return messages


@contextmanager
def debug_logs_scope(enabled: bool) -> Generator[None, None, None]:
    """Set the process debug flag for a request, then restore the prior value."""
    previous = os.environ.get(PREDICT_DEBUG_LOGS_ENV)
    os.environ[PREDICT_DEBUG_LOGS_ENV] = "1" if enabled else "0"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(PREDICT_DEBUG_LOGS_ENV, None)
        else:
            os.environ[PREDICT_DEBUG_LOGS_ENV] = previous
