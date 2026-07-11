"""Chunked NDJSON streaming helpers for the HTTP ``/predict`` server mode.

This module is the pure-logic layer between the raw socket I/O (handled by the
thin ``HTTPServer`` glue in ``predict_upcoming.py``) and the per-category
prediction pipeline (``_predict_category``).  Every public function / generator
here is side-effect-free enough to be fully covered by unit tests with mocked
I/O -- no real sockets, no real ML inference required.

HTTP contract summary (full spec lives in CLAUDE.md):
  GET /ping               -> 200 body ``ok``
  GET /predict?...        -> 200 Transfer-Encoding: chunked application/x-ndjson
                            progress lines  every ~10-15 s  +  final result line

Chunked encoding rationale
--------------------------
The Cloudflare Container DO calls ``renewActivityTimeout`` whenever it receives
a chunk.  The prediction pipeline takes 3-8 minutes end-to-end, so the server
must emit periodic keepalive chunks -- a single silent long response would be
reaped by the Container runtime.  ``iter_predict_chunks`` yields one progress
JSON line roughly every 10 s (controlled by the caller's time.monotonic) during
``_predict_category`` execution and then yields a final result line.

Credential masking
------------------
Exception messages may contain database URLs (``postgresql://user:pass@host/db``).
``mask_error_message`` replaces the user-info portion with ``[REDACTED]`` before
the message is included in any outbound NDJSON body.

Focused per-race guarded dispatch
---------------------------------
A "focused per-race full" request (``mode=full`` with both ``keibajoCode`` and
``raceBango`` present -- one specific race) is serialized by a single
per-process slot because the feature pipeline writes category-scoped work dirs.
Once a request claims the slot, it starts the pipeline in a detached in-process
thread and immediately returns ``accepted``. The Worker queue consumer polls
Neon completion on redelivery instead of holding a Cloudflare request open for
the whole 15+ minute feature chain.

Three non-success statuses are possible for focused full:

- :data:`FOCUSED_FULL_ACCEPTED_STATUS` (``"accepted"``) -- the slot is already
  held by this same race's own still-running pipeline, or this request just
  launched the detached pipeline. The Worker-side queue retry keeps polling the
  Neon-based completion check.
- :data:`FOCUSED_FULL_BUSY_STATUS` (``"busy"``) -- a DIFFERENT race in the
  same category already holds the single per-process pipeline slot, so this
  request's pipeline was NOT launched at all. This status exists to fix a
  follow-on incident: earlier code returned ``"accepted"`` in this case too,
  so the other race's queue message kept getting redelivered, burning its
  whole retry budget doing zero work before dead-lettering with no
  prediction written.  A distinct ``"busy"`` status lets the Worker queue
  consumer re-enqueue a fresh copy (resetting the retry budget) instead.
- :data:`FOCUSED_FULL_ALREADY_COMPLETE_STATUS` (``"already-complete"``) --
  the slot was free, claimed, but a completion check found the race already
  has complete predictions in Neon (a redundant redelivery of an
  already-finished race); the slot is released immediately and no ~15-27 min
  pipeline is re-run.

Because the DuckDB base build and v7 layer chain write to category-scoped
(not race-scoped) work directories (``pipeline_runner.WORK_DIR/feat-{category}-*``),
at most one focused-full pipeline may run at a time per container process --
see :func:`_claim_focused_full_slot`.  The category/day-level batch path and
``mode=rescore`` requests (even with race scope) are entirely unaffected and
keep the held-response keepalive behaviour.
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, final
from urllib.parse import parse_qs, urlparse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROGRESS_INTERVAL_S: Final[float] = 10.0
"""Emit a progress keepalive chunk at least every this many seconds."""

_CRED_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(postgres(?:ql)?://)[^:@/]+(?::[^@/]*)?@",
    re.IGNORECASE,
)
"""Matches ``postgresql://user:pass@`` or ``postgres://user@`` credential prefixes."""

SUPPORTED_CATEGORIES: Final[frozenset[str]] = frozenset({"jra", "nar", "ban-ei"})
"""Valid values for the ``category`` query parameter."""

DATE_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\d{8}$")
"""Matches exactly 8 ASCII digits (YYYYMMDD)."""

PredictMode = Literal["full", "rescore"]
"""Prediction mode for ``GET /predict``.

- ``"full"``    (default) — run the full 21y Neon scan + feature build, then
                score and UPSERT.  After a successful build the feature parquet
                is uploaded to R2 as a cache so ``rescore`` can skip the scan.
- ``"rescore"`` — skip the Neon feature-build scan; load the cached parquet from
                R2 (or the local ``/tmp`` working directory), fetch the latest
                realtime odds, re-score, and UPSERT.  Falls back to ``full``
                automatically when no cached parquet is found.
"""

SUPPORTED_MODES: Final[frozenset[str]] = frozenset({"full", "rescore"})
"""Valid values for the ``mode`` query parameter."""

PREDICT_DEBUG_LOGS_ENV: Final[str] = "PREDICT_DEBUG_LOGS"
"""Process env flag read by pipeline_runner to decide whether to stream debug logs."""

R2_FEAT_CACHE_PREFIX: Final[str] = "feat-cache"
"""R2 object key prefix for feature-parquet cache objects."""

FOCUSED_FULL_ACCEPTED_STATUS: Final[str] = "accepted"
"""``build_result_line`` ``status`` value for a focused per-race ``mode=full``
request whose same-race pipeline is already in flight.

See :func:`is_focused_full_request` for the exact request shape and the
"Focused per-race guarded dispatch" note in this module's docstring. The
Worker-side queue consumer treats this status as "in progress, poll Neon for
completion" rather than a final outcome.
"""

FOCUSED_FULL_BUSY_STATUS: Final[str] = "busy"
"""``build_result_line`` status when a focused per-race full request could not
start because another race in the same category already holds the single
per-process pipeline slot. The Worker queue consumer re-enqueues a fresh copy
(resetting its retry budget) instead of burning the DLQ budget while it waits."""

FOCUSED_FULL_ALREADY_COMPLETE_STATUS: Final[str] = "already-complete"
"""``build_result_line`` status when the race already has complete predictions
in Neon (checked before launching a fresh ~15-27 min pipeline). No pipeline is
launched and the slot is not consumed; the Worker queue consumer acks it."""


# ---------------------------------------------------------------------------
# URL parsing helpers
# ---------------------------------------------------------------------------


def _first_qs(params: dict[str, list[str]], key: str) -> str | None:
    """Return the first value for *key* from a ``parse_qs`` result, or ``None``."""
    values = params.get(key)
    if not values:
        return None
    return values[0]


@final
class PredictParams:
    """Parsed + validated query parameters for ``GET /predict``.

    ``keibajo_code`` / ``race_bango`` are the optional race-scope filter for the
    Stage-2 per-race rescore: when set, only the matching race(s) are rescored.
    Both ``None`` (the default, full path) means "all races for the category".
    """

    __slots__ = (
        "category",
        "days_ahead",
        "debug_logs",
        "keibajo_code",
        "mode",
        "race_bango",
        "run_date",
    )

    def __init__(
        self,
        category: str,
        run_date: str,
        days_ahead: int,
        mode: PredictMode = "full",
        keibajo_code: str | None = None,
        race_bango: str | None = None,
        debug_logs: bool = False,
    ) -> None:
        self.category: str = category
        self.run_date: str = run_date
        self.days_ahead: int = days_ahead
        self.mode: PredictMode = mode
        self.keibajo_code: str | None = keibajo_code
        self.race_bango: str | None = race_bango
        self.debug_logs: bool = debug_logs


def is_focused_full_request(params: PredictParams) -> bool:
    """Return True when *params* targets exactly one race with ``mode=full``.

    A "focused per-race full" request supplies both ``keibajo_code`` and
    ``race_bango`` (the per-race scope filter) together with ``mode="full"``.
    This is the request shape whose pipeline :func:`iter_predict_chunks`
    serializes through the focused-full slot before using the normal keepalive
    path. Day/category batch requests
    (no race scope) and ``mode="rescore"`` requests (even with race scope)
    both return False and keep the original blocking behaviour.
    """
    return (
        params.mode == "full" and params.keibajo_code is not None and params.race_bango is not None
    )


def parse_predict_params(query_string: str) -> PredictParams | str:
    """Parse ``?category=...&runDate=...&daysAhead=...&mode=...`` into :class:`PredictParams`.

    Returns a :class:`PredictParams` on success or a human-readable error string
    on validation failure.  The caller converts an error string to a 400 response
    (or an NDJSON error body).

    Validation rules:
    - ``category`` must be one of ``jra``, ``nar``, ``ban-ei``.
    - ``runDate`` must be exactly 8 ASCII digits (YYYYMMDD).
    - ``daysAhead`` must be a non-negative integer (default ``0``).
    - ``mode`` must be one of ``full``, ``rescore`` (default ``full``).
    """
    qs = parse_qs(query_string, keep_blank_values=True)

    category = _first_qs(qs, "category")
    if category is None:
        return "missing required parameter: category"
    if category not in SUPPORTED_CATEGORIES:
        return f"invalid category: {category!r}; must be one of jra, nar, ban-ei"

    run_date = _first_qs(qs, "runDate")
    if run_date is None:
        return "missing required parameter: runDate"
    if not DATE_PATTERN.match(run_date):
        return f"invalid runDate: {run_date!r}; must be YYYYMMDD"

    raw_days = _first_qs(qs, "daysAhead")
    if raw_days is None:
        days_ahead = 0
    else:
        try:
            days_ahead = int(raw_days)
        except ValueError:
            return f"invalid daysAhead: {raw_days!r}; must be a non-negative integer"
        if days_ahead < 0:
            return f"invalid daysAhead: {days_ahead}; must be non-negative"

    raw_mode = _first_qs(qs, "mode")
    if raw_mode is None or raw_mode == "full":
        mode: PredictMode = "full"
    elif raw_mode == "rescore":
        mode = "rescore"
    else:
        return f"invalid mode: {raw_mode!r}; must be one of full, rescore"

    keibajo_code = _optional_scope_value(_first_qs(qs, "keibajoCode"))
    race_bango = _optional_scope_value(_first_qs(qs, "raceBango"))
    debug_logs = _parse_debug_flag(_first_qs(qs, "debug"))

    return PredictParams(
        category=category,
        run_date=run_date,
        days_ahead=days_ahead,
        mode=mode,
        keibajo_code=keibajo_code,
        race_bango=race_bango,
        debug_logs=debug_logs,
    )


@final
class PrewarmParams:
    """Parsed + validated query parameters for ``GET /prewarm-day-base``.

    Deliberately a narrower parameter set than :class:`PredictParams`
    (category / runDate / daysAhead only, no mode / keibajoCode / raceBango):
    the day-base build is always whole-day scope (see
    ``pipeline_runner.build_day_base``), never a single race.
    """

    __slots__ = ("category", "days_ahead", "run_date")

    def __init__(self, category: str, run_date: str, days_ahead: int) -> None:
        self.category: str = category
        self.run_date: str = run_date
        self.days_ahead: int = days_ahead


def parse_prewarm_params(query_string: str) -> PrewarmParams | str:
    """Parse ``?category=...&runDate=...&daysAhead=...`` into :class:`PrewarmParams`.

    Returns a :class:`PrewarmParams` on success or a human-readable error
    string on validation failure -- mirrors :func:`parse_predict_params`'s
    validation style (same ``category`` / ``runDate`` / ``daysAhead`` rules),
    minus the ``mode`` / ``keibajoCode`` / ``raceBango`` parameters that only
    apply to the per-race ``/predict`` request shape.
    """
    qs = parse_qs(query_string, keep_blank_values=True)

    category = _first_qs(qs, "category")
    if category is None:
        return "missing required parameter: category"
    if category not in SUPPORTED_CATEGORIES:
        return f"invalid category: {category!r}; must be one of jra, nar, ban-ei"

    run_date = _first_qs(qs, "runDate")
    if run_date is None:
        return "missing required parameter: runDate"
    if not DATE_PATTERN.match(run_date):
        return f"invalid runDate: {run_date!r}; must be YYYYMMDD"

    raw_days = _first_qs(qs, "daysAhead")
    if raw_days is None:
        days_ahead = 0
    else:
        try:
            days_ahead = int(raw_days)
        except ValueError:
            return f"invalid daysAhead: {raw_days!r}; must be a non-negative integer"
        if days_ahead < 0:
            return f"invalid daysAhead: {days_ahead}; must be non-negative"

    return PrewarmParams(category=category, run_date=run_date, days_ahead=days_ahead)


def _optional_scope_value(raw: str | None) -> str | None:
    """Normalize an optional race-scope query value (absent / blank -> None).

    A present non-blank value is kept verbatim (stripped of surrounding
    whitespace) so the worker can pass a zero-padded or un-padded keibajo /
    race number — ``rescore.race_matches_scope`` normalizes padding at compare
    time, so no strict 2-digit validation is enforced here.
    """
    if raw is None:
        return None
    text = raw.strip()
    if text == "":
        return None
    return text


def _parse_debug_flag(raw: str | None) -> bool:
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on", "debug"}


def parse_request_path(raw_path: str) -> tuple[str, str]:
    """Split a raw HTTP request path into ``(path, query_string)``.

    ``raw_path`` is the request target from an HTTP/1.1 GET line, e.g.
    ``/predict?category=jra&runDate=20260619&daysAhead=0``.  Returns the path
    component and the query string (without the leading ``?``), both as strings.
    The query string is empty when no ``?`` is present.
    """
    parsed = urlparse(raw_path)
    return parsed.path, parsed.query or ""


# ---------------------------------------------------------------------------
# Credential masking
# ---------------------------------------------------------------------------


def mask_error_message(message: str) -> str:
    """Replace ``user:pass@host`` credential prefix in *message* with ``[REDACTED]@``.

    Handles both ``postgresql://`` and ``postgres://`` schemes.  Strings that
    contain no matching pattern are returned unchanged.

    Examples::

        >>> mask_error_message("postgresql://user:secret@neon.tech/db")
        'postgresql://[REDACTED]@neon.tech/db'
        >>> mask_error_message("no credentials here")
        'no credentials here'
    """
    return _CRED_PATTERN.sub(r"\1[REDACTED]@", message)


# ---------------------------------------------------------------------------
# NDJSON line builders
# ---------------------------------------------------------------------------


def build_progress_line(stage: str, elapsed_s: float) -> bytes:
    """Return a single UTF-8 NDJSON progress line (newline-terminated).

    The Worker DO calls ``renewActivityTimeout`` on every chunk it receives, so
    these lines are the keepalive heartbeat that prevents Container reaping.

    Args:
        stage:     Short free-text description of the current pipeline stage.
        elapsed_s: Elapsed seconds since the request started (rounded to 1 dp).
    """
    payload = {"type": "progress", "stage": stage, "elapsed_s": round(elapsed_s, 1)}
    return (json.dumps(payload, ensure_ascii=False) + "\n").encode()


def build_result_line(
    category: str,
    run_date: str,
    races_predicted: int,
    *,
    status: str,
    error: str | None = None,
    parquet_base64: str | None = None,
    parquet_key: str | None = None,
    per_race_parquets: list[dict[str, str]] | None = None,
) -> bytes:
    """Return a single UTF-8 NDJSON result line (newline-terminated).

    This is the *final* line of a chunked response.  ``status`` is usually
    ``"success"`` or ``"error"``; it is also passed as
    ``FOCUSED_FULL_ACCEPTED_STATUS`` (``"accepted"``) by
    :func:`iter_predict_chunks` for a focused per-race full request whose
    same-race pipeline is already in flight -- see that constant's docstring.
    ``error`` is included only when non-None (masked via
    :func:`mask_error_message` before encoding).

    When ``parquet_base64`` and ``parquet_key`` are both provided (only on
    ``mode=full`` success), the Worker DO reads these fields and proxies the
    parquet bytes to R2 via its FEATURES_CACHE binding — bypassing the
    read-only S3 token limitation in the Container.

    When ``per_race_parquets`` is provided (only on ``mode=full`` success), each
    element is a ``{"parquetBase64": ..., "parquetKey": ...}`` dict for one race;
    the Worker DO proxies every per-race parquet to R2 under the per-race key
    (``build_r2_per_race_feat_cache_key``) so a Stage-2 rescore can hit a single
    race object even when the whole-day parquet upload was skipped.

    Args:
        category:        The category that was predicted (e.g. ``"jra"``).
        run_date:        The YYYYMMDD run date.
        races_predicted: Number of races written (may be partial on error).
        status:          ``"success"``, ``"error"``, or ``FOCUSED_FULL_ACCEPTED_STATUS``
                         (``"accepted"``) for a same-race focused-full redelivery.
        error:           Optional exception message; credentials will be masked.
        parquet_base64:  Optional base64-encoded feature parquet bytes for Worker R2 proxy.
        parquet_key:     Optional R2 object key matching ``build_r2_feat_cache_key``.
        per_race_parquets: Optional list of per-race ``{"parquetBase64", "parquetKey"}`` dicts.
    """
    payload: dict[str, object] = {
        "type": "result",
        "category": category,
        "runDate": run_date,
        "racesPredicted": races_predicted,
        "status": status,
    }
    if error is not None:
        payload["error"] = mask_error_message(error)
    if parquet_base64 is not None:
        payload["parquetBase64"] = parquet_base64
    if parquet_key is not None:
        payload["parquetKey"] = parquet_key
    if per_race_parquets is not None:
        payload["perRaceParquets"] = per_race_parquets
    return (json.dumps(payload, ensure_ascii=False) + "\n").encode()


PREWARM_EMPTY_STATUS: Final[str] = "empty"
"""``build_prewarm_result_line`` status when the day-base build emitted zero
target rows for the category+day (e.g. JRA on a NAR-only weekday) -- not an
error, just nothing to cache. Mirrors ``build_day_base``'s ``None`` return."""


def build_prewarm_result_line(
    category: str,
    run_date: str,
    *,
    status: str,
    error: str | None = None,
    parquet_base64: str | None = None,
    parquet_key: str | None = None,
) -> bytes:
    """Return a single UTF-8 NDJSON result line for ``GET /prewarm-day-base``.

    Mirrors :func:`build_result_line`'s shape (``type``, ``category``,
    ``runDate``, ``status``, optional ``error`` / ``parquetBase64`` /
    ``parquetKey``) minus ``racesPredicted`` -- prewarm builds the day-base
    cache, it does not score or write any predictions. ``status`` is one of
    ``"success"``, ``"error"``, or :data:`PREWARM_EMPTY_STATUS` (``"empty"``).
    """
    payload: dict[str, object] = {
        "type": "result",
        "category": category,
        "runDate": run_date,
        "status": status,
    }
    if error is not None:
        payload["error"] = mask_error_message(error)
    if parquet_base64 is not None:
        payload["parquetBase64"] = parquet_base64
    if parquet_key is not None:
        payload["parquetKey"] = parquet_key
    return (json.dumps(payload, ensure_ascii=False) + "\n").encode()


# ---------------------------------------------------------------------------
# R2 feature-cache helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class R2Config:
    """R2 credentials and bucket name for feature-parquet caching.

    All fields are required.  The I/O glue in ``predict_upcoming.py`` populates
    this from environment variables (``R2_ACCOUNT_ID``, ``R2_ACCESS_KEY_ID``,
    ``R2_SECRET_ACCESS_KEY``, ``R2_BUCKET``).  When any env var is absent the
    glue skips R2 operations silently so ``mode=full`` still completes without
    caching (degraded-but-functional path).
    """

    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str


def build_r2_feat_cache_key(category: str, run_date: str) -> str:
    """Return the R2 object key for a category+date feature-parquet cache.

    Key format: ``feat-cache/{category}/{runDate}/features.parquet``

    This matches the per-race R2 Parquet layout used elsewhere in the repo
    (``feedback_r2_parquet_over_d1``).  The key is deterministic so repeated
    ``mode=full`` runs overwrite the same object (idempotent cache update).

    Args:
        category: One of ``"jra"``, ``"nar"``, ``"ban-ei"``.
        run_date: YYYYMMDD string (e.g. ``"20260619"``).
    """
    return f"{R2_FEAT_CACHE_PREFIX}/{category}/{run_date}/features.parquet"


def build_r2_per_race_feat_cache_key(
    category: str, run_date: str, keibajo_code: str, race_bango: str
) -> str:
    """Return the R2 object key for a per-race feature-parquet cache.

    Key format: ``feat-cache/{category}/{runDate}/{keibajoCode}/{raceBango}/features.parquet``
    """
    return (
        f"{R2_FEAT_CACHE_PREFIX}/{category}/{run_date}/{keibajo_code}/{race_bango}/features.parquet"
    )


R2_DAY_BASE_PREFIX: Final[str] = "feat-daybase"
"""R2 object key prefix for the per-category+day DAY_CHAIN-only cache.

Deliberately a DISTINCT namespace from :data:`R2_FEAT_CACHE_PREFIX`
(``feat-cache/...``, the existing whole-day / per-race FULL-pipeline cache
used by ``mode=rescore``). A prior design explicitly rejected merging cache
granularities: the day-base object holds ONLY the DAY_CHAIN output (no
MARKET_SIGNAL / NEAR_MISS / BABA_PEDIGREE / RELATIONSHIP / JRA
jockey-pedigree-cell columns yet -- see ``pipeline_args.DAY_CHAIN`` /
``RACE_CHAIN``), while ``feat-cache`` objects are the FULL post-RACE_CHAIN,
directly-scoreable parquet. Conflating the two keys would let a rescore
request silently load a day-base-only parquet missing the RACE_CHAIN columns,
corrupting the feature vector without raising."""


def build_r2_day_base_key(category: str, run_date: str) -> str:
    """Return the R2 object key for a category+day day-base feature-parquet cache.

    Key format: ``feat-daybase/{category}/{runDate}/features.parquet``

    This is the whole-day DAY_CHAIN-only cache object written once per
    category+day (see ``pipeline_runner.build_day_base``) and read by every
    per-race ``build_pipeline_from_day_base`` call for that day -- see
    :data:`R2_DAY_BASE_PREFIX` for why this is a distinct namespace from
    :func:`build_r2_feat_cache_key`.

    Args:
        category: One of ``"jra"``, ``"nar"``, ``"ban-ei"``.
        run_date: YYYYMMDD string (e.g. ``"20260712"``).
    """
    return f"{R2_DAY_BASE_PREFIX}/{category}/{run_date}/features.parquet"


# ---------------------------------------------------------------------------
# Cache-miss sentinel
# ---------------------------------------------------------------------------


class CacheMissError(Exception):
    """Raised by a rescore function when no cached feature parquet is available.

    ``iter_predict_chunks`` catches this specific exception type to trigger the
    automatic ``rescore -> full`` fallback path (emitting a
    ``"rescore-fallback-to-full"`` progress line before re-running the full
    pipeline).  All other exceptions from a rescore function propagate normally
    and are encoded as error result lines.

    Usage in ``predict_upcoming.py``::

        if not cached_parquet_exists(category, run_date):
            raise CacheMissError(f"no cache for {category}/{run_date}")
    """


# ---------------------------------------------------------------------------
# Core streaming generator
# ---------------------------------------------------------------------------

PredictCategoryFn = Callable[[str, str, int, str | None, str | None], int]
"""Signature of the ``_predict_category`` adapter passed to :func:`iter_predict_chunks`.

Args:
    category:     One of ``"jra"``, ``"nar"``, ``"ban-ei"``.
    run_date:     YYYYMMDD date string.
    days_ahead:   Non-negative integer window extension.
    keibajo_code: Optional per-race scope keibajo code (``None`` = all races).
    race_bango:   Optional per-race scope race number (``None`` = all races).

When ``keibajo_code`` and ``race_bango`` are both set, the full path builds and
scores only that one race (``mode=full`` per-race feature generation); both
``None`` is the whole-window default.  The rescore path ignores these args
because its race scope is bound by the rescore factory.

Returns:
    Number of races predicted (written to Neon).

Raises:
    Any exception from the prediction pipeline -- will be caught by
    :func:`iter_predict_chunks` and encoded as an error result line.
"""

TimeFn = Callable[[], float]
"""Returns the current monotonic time in seconds (injectable for testing)."""

SleepFn = Callable[[float], None]
"""Suspends execution for the given number of seconds (injectable for testing)."""

_POLL_INTERVAL_S: Final[float] = 1.0
"""How often (in seconds) the keepalive loop polls the worker thread for completion.

This is kept at 1 s so the generator wakes up frequently enough to detect when
the pipeline finishes, while still yielding progress only every
``progress_interval_s`` (~10 s).  The separation between poll granularity and
keepalive frequency prevents busy-waiting while still producing timely keepalives.
"""


def _run_predict_fn(
    predict_fn: PredictCategoryFn,
    params: PredictParams,
) -> int:
    """Call *predict_fn* with the category / run_date / days_ahead + race scope.

    Thin wrapper so the call-site in ``iter_predict_chunks`` stays concise and
    the signature is the same whether we are on the full or rescore path.  The
    optional ``keibajo_code`` / ``race_bango`` scope is forwarded so the full
    path can build + score a single race; the rescore path ignores them.
    """
    previous = os.environ.get(PREDICT_DEBUG_LOGS_ENV)
    os.environ[PREDICT_DEBUG_LOGS_ENV] = "1" if params.debug_logs else "0"
    try:
        return predict_fn(
            params.category,
            params.run_date,
            params.days_ahead,
            params.keibajo_code,
            params.race_bango,
        )
    finally:
        if previous is None:
            os.environ.pop(PREDICT_DEBUG_LOGS_ENV, None)
        else:
            os.environ[PREDICT_DEBUG_LOGS_ENV] = previous


def _run_in_thread[T](
    fn: Callable[[], T],
) -> tuple[threading.Thread, list[T], list[BaseException]]:
    """Run *fn* in a daemon thread; return (thread, result_box, error_box).

    ``result_box`` is a 1-element list that will hold the return value of *fn*
    when it completes successfully.  ``error_box`` is a 1-element list that will
    hold the exception when *fn* raises.  Exactly one of the two will be
    populated after the thread finishes.  The caller polls ``thread.is_alive()``
    (or calls ``thread.join(timeout=...)``) to detect completion.

    Generic over the return type so this single helper backs both the
    ``PredictCategoryFn`` (``int``) callables driven by
    :func:`iter_predict_chunks` and the ``PrewarmBuildFn`` (``Path | None``)
    callable driven by :func:`iter_prewarm_chunks`.
    """
    result_box: list[T] = []
    error_box: list[BaseException] = []

    def _target() -> None:
        try:
            result_box.append(fn())
        except BaseException as exc:
            error_box.append(exc)

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    return thread, result_box, error_box


def _iter_keepalive[T](
    fn: Callable[[], T],
    stage: str,
    *,
    time_fn: TimeFn,
    sleep_fn: SleepFn,
    progress_interval_s: float,
    elapsed_fn: Callable[[], float],
    last_progress: float,
) -> Generator[bytes, None, tuple[T | None, BaseException | None, float]]:
    """Run *fn* in a background daemon thread, yielding keepalive progress lines.

    Shared thread/poll/keepalive loop used by :func:`iter_predict_chunks` (both
    the primary predict/rescore call AND the rescore-cache-miss fallback call)
    and :func:`iter_prewarm_chunks`, so the shape that keeps the Cloudflare
    Container from reaping a long-running request lives in exactly one place.

    Yields ``build_progress_line(stage, elapsed_fn())`` whenever *fn* is still
    running in its background thread and at least *progress_interval_s*
    seconds have elapsed since the most recent keepalive. Returns
    ``(result, error, last_progress)`` as the generator's return value
    (retrieved via ``result, error, last_progress = yield from
    _iter_keepalive(...)``) -- exactly one of ``result`` / ``error`` is
    non-``None``.

    ``last_progress`` is the caller's most recent keepalive timestamp (usually
    just after a pre-flight progress line) so the FIRST interval check inside
    this helper measures from the same baseline an un-factored inline loop
    would have used -- no extra or missing keepalive line versus the original.
    The returned ``last_progress`` lets the caller continue the SAME running
    interval clock after this call returns (e.g. for a "post-pipeline
    progress" check, or a second call to this helper for the rescore-fallback
    retry).
    """
    thread, result_box, error_box = _run_in_thread(fn)
    while thread.is_alive():
        sleep_fn(_POLL_INTERVAL_S)
        now = time_fn()
        if now - last_progress >= progress_interval_s:
            yield build_progress_line(stage, elapsed_fn())
            last_progress = now
    thread.join()
    if error_box:
        return None, error_box[0], last_progress
    return result_box[0], None, last_progress


ParquetPayloadFn = Callable[[], tuple[str, str] | None]
"""Returns ``(parquet_base64, parquet_key)`` after a successful full run, or ``None``.

Called by ``iter_predict_chunks`` once after the full-pipeline thread completes
successfully (not on error, not on rescore-only path).  The result is injected
into the final result line so the Worker DO can proxy the parquet bytes to R2
without requiring a write-capable S3 token in the Container environment.
"""

PerRaceParquetPayloadFn = Callable[[], list[dict[str, str]] | None]
"""Returns a list of per-race ``{"parquetBase64", "parquetKey"}`` dicts, or ``None``.

Called by ``iter_predict_chunks`` once after the pipeline thread completes
successfully, for **both** ``mode=full`` and ``mode=rescore``.  Each element
carries one race's feature parquet (split from the whole-day parquet by
``race_id``) and the per-race R2 key from :func:`build_r2_per_race_feat_cache_key`,
so the Worker DO can proxy every per-race object to R2 — letting the rescore path
also seed weight/odds-refreshed per-race objects.  ``None`` means no per-race
split was produced (non-blocking — a missing per-race cache must not fail
predictions).
"""


def build_focused_full_race_key(params: PredictParams) -> str:
    """Return a stable identity string for one focused-full pipeline run.

    Format: ``"{category}:{run_date}:{keibajo_code}:{race_bango}"``.  Used for
    logging and as the identity token passed to
    :func:`_claim_focused_full_slot` / :func:`_release_focused_full_slot` --
    NOT as the guard's granularity.  The guard itself is a single
    process-wide slot regardless of which race this key names (see those
    functions' docstrings for why).
    """
    return f"{params.category}:{params.run_date}:{params.keibajo_code}:{params.race_bango}"


FOCUSED_FULL_SLOT_CLAIMED: Final[Literal["claimed"]] = "claimed"
FOCUSED_FULL_SLOT_IN_FLIGHT_SELF: Final[Literal["in-flight-self"]] = "in-flight-self"
FOCUSED_FULL_SLOT_BUSY: Final[Literal["busy"]] = "busy"

FocusedFullSlotState = Literal["claimed", "in-flight-self", "busy"]
"""Result of attempting to claim the single per-process focused-full slot.
- ``"claimed"``: slot was free, now held by this race -> run through keepalive.
- ``"in-flight-self"``: slot already held by THIS SAME race key (redelivery of a
  race whose own pipeline is still running) -> do not launch, return accepted.
- ``"busy"``: slot held by a DIFFERENT race in this category -> return busy.
"""

FocusedFullClaimFn = Callable[[str], FocusedFullSlotState]
"""Attempts to claim the single per-process focused-full pipeline slot.

Takes the race key (see :func:`build_focused_full_race_key`) and returns one
of the three :data:`FocusedFullSlotState` values: ``"claimed"`` when the slot
was free and is now held by this race key, ``"in-flight-self"`` when the slot
is already held by this SAME race key (a redelivery of a race whose own
pipeline is still running), or ``"busy"`` when the slot is held by a
DIFFERENT race and no new pipeline may be launched. Injectable (like
:data:`TimeFn` / :data:`SleepFn`) so tests can control the guard
deterministically without real thread races.
"""

FocusedFullReleaseFn = Callable[[str], None]
"""Releases the single per-process focused-full pipeline slot.

Takes the race key that was passed to the matching claim call.  Injectable
for the same reason as :data:`FocusedFullClaimFn`.
"""

FocusedFullCompletionFn = Callable[[PredictParams], bool]
"""Returns True when the focused per-race full prediction for these params
already exists (complete) in Neon. Injected (like predict_fn / claim_fn) so
serve.py stays I/O-free and unit-testable; the real Neon query lives in
predict_upcoming.py. Must not raise (a raising fn is treated as 'not complete').
"""

_FOCUSED_FULL_LOCK: Final[threading.Lock] = threading.Lock()
"""Guards ``_FOCUSED_FULL_IN_FLIGHT`` across concurrent HTTP requests.

The HTTP server used by ``predict_upcoming.py`` is a single-threaded
``http.server.HTTPServer`` (not ``ThreadingHTTPServer``), but queue redeliveries
and future server changes can still present repeated focused requests. This lock
protects the claim/release check-then-set around the category-scoped work
directories in ``pipeline_runner.WORK_DIR``.
"""

_FOCUSED_FULL_IN_FLIGHT: Final[list[str | None]] = [None]
"""Single-slot in-process guard: the race key of the one focused-full pipeline
currently running in this container process, or ``None`` when the slot is free.

A single slot (not a per-race set) is deliberate: ``pipeline_runner.py``
builds the DuckDB base feature parquet and each v7 layer's output under work
directories keyed by ``category`` only (``WORK_DIR/feat-{category}-base``,
``feat-{category}-layer-{index}``, ``feat-{category}-v7-final``) -- NOT by
race.  A container instance always serves exactly one category (see
``apps/finish-position-cron/src/queue-consumer.ts::buildPredictDoName``, which
names Durable Objects ``predict-{category}``, never per-race), so two
concurrent focused-full pipelines for two different races in that category
would read/write the SAME shared directories and could corrupt each other's
intermediate files.  At most one focused-full pipeline may therefore run at a
time per container process, regardless of which race it is for.
"""

_DETACHED_FOCUSED_FULL_THREADS: Final[list[threading.Thread]] = []
"""Strong references to detached focused-full worker threads.

Focused full requests can outlive Cloudflare's HTTP fetch window. The request
therefore starts the pipeline in-process and returns ``accepted`` immediately;
this list prevents the worker thread object from being garbage-collected before
it releases the focused slot.
"""


def _run_detached_focused_full(fn: Callable[[], int]) -> None:
    """Run *fn* in a background thread independent of the HTTP response."""

    def _target() -> None:
        try:
            fn()
        except BaseException as exc:
            print(
                f"[focused-full] detached pipeline failed: {type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
        finally:
            current_thread = threading.current_thread()
            with _FOCUSED_FULL_LOCK:
                _DETACHED_FOCUSED_FULL_THREADS[:] = [
                    thread
                    for thread in _DETACHED_FOCUSED_FULL_THREADS
                    if thread is not current_thread and thread.is_alive()
                ]

    thread = threading.Thread(target=_target, daemon=True)
    with _FOCUSED_FULL_LOCK:
        _DETACHED_FOCUSED_FULL_THREADS.append(thread)
    thread.start()


def _claim_focused_full_slot(race_key: str) -> FocusedFullSlotState:
    """Claim (or observe) the single per-process focused-full pipeline slot.

    Returns :data:`FOCUSED_FULL_SLOT_CLAIMED` (and records *race_key* as
    in-flight) if no other focused-full pipeline is currently
    running in this container process.  Returns
    :data:`FOCUSED_FULL_SLOT_IN_FLIGHT_SELF` when the slot is already held by
    this SAME race key (a redelivery of a race whose own pipeline is still
    running -- do not launch a second thread for it).  Returns
    :data:`FOCUSED_FULL_SLOT_BUSY` when the slot is held by a DIFFERENT race.
    The DuckDB base build + v7 layer chain (``pipeline_runner.py``) writes to
    CATEGORY-scoped work directories (``WORK_DIR/feat-{category}-base``,
    ``feat-{category}-layer-N``, ``feat-{category}-v7-final``) -- not
    race-scoped -- and a container instance serves exactly one category, so
    at most one focused-full pipeline may run at a time to avoid two
    concurrent builds for different races corrupting each other's shared
    intermediate files.
    """
    with _FOCUSED_FULL_LOCK:
        current = _FOCUSED_FULL_IN_FLIGHT[0]
        if current is None:
            _FOCUSED_FULL_IN_FLIGHT[0] = race_key
            return FOCUSED_FULL_SLOT_CLAIMED
        if current == race_key:
            return FOCUSED_FULL_SLOT_IN_FLIGHT_SELF
        return FOCUSED_FULL_SLOT_BUSY


def _release_focused_full_slot(race_key: str) -> None:
    """Release the focused-full slot, if it is still held by *race_key*.

    A no-op when the slot is already free or held by a different key -- this
    should not happen given the single-slot claim/release protocol, but the
    guard avoids releasing a slot claimed by a later, unrelated request.
    """
    with _FOCUSED_FULL_LOCK:
        if _FOCUSED_FULL_IN_FLIGHT[0] == race_key:
            _FOCUSED_FULL_IN_FLIGHT[0] = None


def _focused_full_result_line(params: PredictParams, status: str) -> bytes:
    """Build an early-return result line for a focused per-race full request."""
    return build_result_line(params.category, params.run_date, 0, status=status)


def _focused_full_is_complete(
    params: PredictParams, completion_fn: FocusedFullCompletionFn | None
) -> bool:
    """Return True when *completion_fn* reports the race already complete.

    ``None`` (no check wired) and any exception both yield False so the pipeline
    still runs -- a completion-check failure must never block a prediction.
    """
    if completion_fn is None:
        return False
    try:
        return completion_fn(params)
    except BaseException:
        return False


def _focused_full_preflight(
    params: PredictParams,
    claim_fn: FocusedFullClaimFn,
    release_fn: FocusedFullReleaseFn,
    completion_fn: FocusedFullCompletionFn | None,
) -> tuple[str, str]:
    """Claim the focused-full slot and return ``(race_key, status)``.

    Status is ``"run"`` when the caller should execute the pipeline through the
    normal keepalive path.
    Other statuses are final result statuses to return immediately.
    """
    race_key = build_focused_full_race_key(params)
    state = claim_fn(race_key)
    if state == FOCUSED_FULL_SLOT_BUSY:
        return race_key, FOCUSED_FULL_BUSY_STATUS
    if state == FOCUSED_FULL_SLOT_IN_FLIGHT_SELF:
        return race_key, FOCUSED_FULL_ACCEPTED_STATUS
    # state == FOCUSED_FULL_SLOT_CLAIMED: slot was free. Skip re-running a race
    # already complete in Neon (a redundant redelivery) so it does not hold the
    # slot for another ~15-27 min and starve other races.
    if _focused_full_is_complete(params, completion_fn):
        release_fn(race_key)
        return race_key, FOCUSED_FULL_ALREADY_COMPLETE_STATUS
    return race_key, "run"


def iter_predict_chunks(
    params: PredictParams,
    predict_fn: PredictCategoryFn,
    *,
    rescore_fn: PredictCategoryFn | None = None,
    parquet_payload_fn: ParquetPayloadFn | None = None,
    per_race_parquet_payload_fn: PerRaceParquetPayloadFn | None = None,
    focused_full_claim_fn: FocusedFullClaimFn | None = None,
    focused_full_release_fn: FocusedFullReleaseFn | None = None,
    focused_full_completion_fn: FocusedFullCompletionFn | None = None,
    time_fn: TimeFn = time.monotonic,
    sleep_fn: SleepFn = time.sleep,
    progress_interval_s: float = PROGRESS_INTERVAL_S,
) -> Generator[bytes, None, None]:
    """Yield NDJSON bytes chunks for a single ``/predict`` request.

    This generator drives the prediction pipeline via *predict_fn* (or
    *rescore_fn* when ``params.mode == "rescore"``) and emits:

    - **Progress lines** at the start and just before the long-running pipeline
      call (yielded eagerly so the Worker DO renews its activity timeout before
      the multi-minute feature-build step begins).
    - **Periodic keepalive progress lines** every *progress_interval_s* seconds
      **while the pipeline is running in a background thread**.  Each yielded
      line causes the Worker DO to call ``renewActivityTimeout``, preventing the
      Container runtime from reaping the long-running job.
    - A ``"rescore-fallback-to-full"`` progress line when ``mode=rescore`` but
      *rescore_fn* raised a cache-miss exception and the pipeline fell back to
      *predict_fn*.
    - A single **result line** as the last yield -- either success, error, or
      (for a focused per-race full request only) one of
      :data:`FOCUSED_FULL_ACCEPTED_STATUS`, :data:`FOCUSED_FULL_BUSY_STATUS`,
      or :data:`FOCUSED_FULL_ALREADY_COMPLETE_STATUS`.

    Threading keepalive design
    --------------------------
    The prediction pipeline (*predict_fn* / *rescore_fn*) is launched in a
    background daemon thread via :func:`_run_in_thread`.  The generator's main
    thread loops with ``thread.join(timeout=_POLL_INTERVAL_S)``; when the join
    times out (thread still alive) AND *progress_interval_s* has elapsed since
    the last yield, a new ``"predict"`` progress line is emitted.  This ensures
    the DO receives a keepalive chunk roughly every *progress_interval_s* seconds
    throughout the entire 3-8-minute pipeline run.

    Mode dispatch:
    - ``mode=full`` (default) -- always calls *predict_fn*.
    - ``mode=rescore`` with *rescore_fn* provided -- calls *rescore_fn* first;
      on ``CacheMissError`` falls back to *predict_fn* after emitting a
      ``rescore-fallback-to-full`` progress line.
    - ``mode=rescore`` without *rescore_fn* -- falls back to *predict_fn*
      immediately (same as ``mode=full``; emits fallback progress line).

    Guarded dispatch (focused per-race full only)
    --------------------------------------------
    When ``is_focused_full_request(params)`` is True, the single per-process
    guard (:func:`_claim_focused_full_slot` by default, or
    *focused_full_claim_fn* / *focused_full_release_fn* when injected) is
    consulted before entering the normal poll/join loop, with three possible
    outcomes:

    - **Slot free** (``"claimed"``): the slot is claimed for this race. Unless
      *focused_full_completion_fn* reports the race already has complete
      predictions in Neon (see below), *predict_fn* is run through the normal
      keepalive thread/poll/join path and returns final ``success`` or
      ``error``.
    - **Slot already held by THIS SAME race key** (``"in-flight-self"``): a
      redelivery of a race whose own pipeline is still running. No second
      thread is launched; the generator yields
      ``status=FOCUSED_FULL_ACCEPTED_STATUS`` again (keep polling).
    - **Slot held by a DIFFERENT race** (``"busy"``): no thread is launched;
      the generator yields ``status=FOCUSED_FULL_BUSY_STATUS`` (``"busy"``) so
      the Worker queue consumer re-enqueues a fresh copy (resetting its retry
      budget) instead of burning the DLQ budget while it waits.

    On the just-claimed path, before launching, *focused_full_completion_fn*
    (when provided) is consulted: if it reports the race already has complete
    predictions in Neon, the slot is released immediately, no pipeline is
    launched, and the generator yields ``status=FOCUSED_FULL_ALREADY_COMPLETE_STATUS``
    (``"already-complete"``) instead -- this avoids re-running a redundant
    redelivery of an already-complete race, which would otherwise hold the
    slot for another ~15-27 min and starve other races. A ``None`` completion
    fn, or one that raises, is treated as "not complete" so a genuine
    prediction always still runs.

    The focused branch is purely additive: ``mode=rescore`` and non-focused
    ``mode=full`` requests are completely unaffected and keep using the
    poll/join loop described above.

    The generator NEVER raises -- all exceptions from both *predict_fn* and
    *rescore_fn* are caught and encoded as an error result line so the HTTP
    response always terminates cleanly.

    Args:
        params:              Parsed request parameters (incl. ``mode``).
        predict_fn:          Full-pipeline callable matching :data:`PredictCategoryFn`.
        rescore_fn:          Optional rescore-path callable; ``None`` triggers
                             automatic fallback to *predict_fn* for rescore mode.
        parquet_payload_fn:  Optional callable invoked after a successful full-pipeline
                             run.  Returns ``(parquet_base64, parquet_key)`` or ``None``
                             when no parquet is available.  The result is embedded in the
                             success result line so the Worker DO can proxy the bytes to
                             R2 without a write-capable S3 token.
        per_race_parquet_payload_fn: Optional callable invoked after a successful
                             pipeline run for **both** ``mode=full`` and
                             ``mode=rescore``.  Returns a list of per-race
                             ``{"parquetBase64", "parquetKey"}`` dicts (or ``None``)
                             embedded in the result line as ``perRaceParquets`` so the
                             Worker DO can proxy each per-race object to R2.
        focused_full_claim_fn: Optional override for :func:`_claim_focused_full_slot`
                             (injectable so tests can control the single-process
                             guard deterministically). Only consulted for a
                             focused per-race full request.
        focused_full_release_fn: Optional override for :func:`_release_focused_full_slot`,
                             called once the claimed pipeline finishes
                             (success or error), and also
                             (synchronously) when *focused_full_completion_fn*
                             reports the race already complete. Only used for a
                             focused per-race full request whose slot was claimed.
        focused_full_completion_fn: Optional callable consulted only on the
                             just-claimed path, before launching. Returns True
                             when the race already has complete predictions in
                             Neon, in which case the slot is released and
                             :data:`FOCUSED_FULL_ALREADY_COMPLETE_STATUS` is
                             returned instead of launching a fresh pipeline.
                             ``None`` (the default) or a raising callable both
                             behave as "not complete" -- a genuine prediction
                             is never blocked by a completion-check failure.
        time_fn:             Monotonic clock (injectable for deterministic tests).
        sleep_fn:            Sleep callable (injectable for deterministic tests).
        progress_interval_s: Minimum seconds between progress keepalive lines.
    """
    started = time_fn()

    def _elapsed() -> float:
        return time_fn() - started

    # Emit a pre-flight progress line immediately so the DO renews its timeout
    # before the (potentially multi-minute) feature-build step starts.
    yield build_progress_line("starting", _elapsed())

    last_progress = time_fn()

    # Emit a progress heartbeat just before the long-running pipeline call.
    # Always emit here (not interval-gated) because the pipeline is about to
    # block the thread for several minutes.
    now = time_fn()
    if now - last_progress >= progress_interval_s:
        yield build_progress_line("feature-build", _elapsed())
        last_progress = now

    yield build_progress_line("predict", _elapsed())
    last_progress = time_fn()  # reset after forced emit

    races_predicted = 0

    # Determine which callable to run first and whether a rescore->full fallback
    # may be needed.
    use_rescore_first = params.mode == "rescore" and rescore_fn is not None
    needs_rescore_fallback = params.mode == "rescore" and rescore_fn is None

    if needs_rescore_fallback:
        # No rescore fn available — fall back to full pipeline immediately.
        yield build_progress_line("rescore-fallback-to-full", _elapsed())
        last_progress = time_fn()

    # Choose the first callable to launch in a thread.
    # Capture the chosen callable in a single variable so basedpyright sees one
    # unambiguous definition of ``first_call``.
    if use_rescore_first:
        assert rescore_fn is not None
        _rescore_fn_ref: PredictCategoryFn = rescore_fn

        def _call_rescore() -> int:
            return _run_predict_fn(_rescore_fn_ref, params)

        first_call: Callable[[], int] = _call_rescore
    else:

        def _call_predict() -> int:
            return _run_predict_fn(predict_fn, params)

        first_call = _call_predict

    if is_focused_full_request(params):
        # Focused full uses the single per-process slot because the feature
        # pipeline writes category-scoped work dirs. Once claimed, detach the
        # real pipeline from the HTTP response and return "accepted"; Worker /
        # Queue callers poll Neon completion on redelivery. This avoids tying a
        # 15+ minute feature chain to Cloudflare's request lifetime.
        claim_fn: FocusedFullClaimFn = (
            focused_full_claim_fn if focused_full_claim_fn is not None else _claim_focused_full_slot
        )
        release_fn: FocusedFullReleaseFn = (
            focused_full_release_fn
            if focused_full_release_fn is not None
            else _release_focused_full_slot
        )
        focused_race_key, focused_status = _focused_full_preflight(
            params, claim_fn, release_fn, focused_full_completion_fn
        )
        if focused_status != "run":
            yield _focused_full_result_line(params, focused_status)
            return

        unguarded_first_call = first_call

        def _call_focused_and_release() -> int:
            try:
                return unguarded_first_call()
            finally:
                release_fn(focused_race_key)

        _run_detached_focused_full(_call_focused_and_release)
        yield _focused_full_result_line(params, FOCUSED_FULL_ACCEPTED_STATUS)
        return

    # Keepalive loop: run the pipeline in a background thread, yielding progress
    # lines while it runs so the Worker DO can call renewActivityTimeout and the
    # Container is not reaped (see _iter_keepalive).
    thread_result, thread_error, last_progress = yield from _iter_keepalive(
        first_call,
        "predict",
        time_fn=time_fn,
        sleep_fn=sleep_fn,
        progress_interval_s=progress_interval_s,
        elapsed_fn=_elapsed,
        last_progress=last_progress,
    )

    # Check thread outcome.
    if thread_error is not None:
        first_exc = thread_error

        # If rescore raised CacheMissError, fall back to the full pipeline.
        if use_rescore_first and isinstance(first_exc, CacheMissError):
            yield build_progress_line("rescore-fallback-to-full", _elapsed())
            last_progress = time_fn()

            def _fallback_call() -> int:
                return _run_predict_fn(predict_fn, params)

            fb_result, fb_error, last_progress = yield from _iter_keepalive(
                _fallback_call,
                "predict",
                time_fn=time_fn,
                sleep_fn=sleep_fn,
                progress_interval_s=progress_interval_s,
                elapsed_fn=_elapsed,
                last_progress=last_progress,
            )

            if fb_error is not None:
                fallback_exc = fb_error
                error_msg = f"{type(fallback_exc).__name__}: {fallback_exc}"
                yield build_result_line(
                    params.category,
                    params.run_date,
                    races_predicted,
                    status="error",
                    error=error_msg,
                )
                return

            assert fb_result is not None
            races_predicted = fb_result
        else:
            # Non-CacheMissError (or non-rescore path): encode as error result.
            error_msg = f"{type(first_exc).__name__}: {first_exc}"
            yield build_result_line(
                params.category,
                params.run_date,
                races_predicted,
                status="error",
                error=error_msg,
            )
            return
    else:
        assert thread_result is not None
        races_predicted = thread_result

    # Post-pipeline progress (only if interval elapsed -- pipeline was fast in tests).
    now = time_fn()
    if now - last_progress >= progress_interval_s:
        yield build_progress_line("complete", _elapsed())

    # On a successful full-pipeline run, embed the parquet payload so the Worker
    # DO can proxy the bytes to R2 via its FEATURES_CACHE binding (bypassing the
    # read-only S3 token limitation in the Container env).  Errors from the
    # payload fn are swallowed — a missing cache upload must not block predictions.
    parquet_b64: str | None = None
    parquet_key_val: str | None = None
    if parquet_payload_fn is not None and params.mode == "full":
        try:
            payload_result = parquet_payload_fn()
            if payload_result is not None:
                parquet_b64, parquet_key_val = payload_result
        except BaseException:
            pass

    per_race_parquets: list[dict[str, str]] | None = None
    if per_race_parquet_payload_fn is not None:
        try:
            per_race_parquets = per_race_parquet_payload_fn()
        except BaseException:
            per_race_parquets = None

    yield build_result_line(
        params.category,
        params.run_date,
        races_predicted,
        status="success",
        parquet_base64=parquet_b64,
        parquet_key=parquet_key_val,
        per_race_parquets=per_race_parquets,
    )


# ---------------------------------------------------------------------------
# /prewarm-day-base — day-base build streaming generator
# ---------------------------------------------------------------------------

PrewarmBuildFn = Callable[[str, str, int], Path | None]
"""Signature of the day-base build callable passed to :func:`iter_prewarm_chunks`.

Args:
    category:   One of ``"jra"``, ``"nar"``, ``"ban-ei"``.
    run_date:   YYYYMMDD date string.
    days_ahead: Non-negative integer window extension.

Returns the day-base's final parquet directory on success, or ``None`` when
the base build emitted zero target rows for this category+day (mirrors
``pipeline_runner.build_day_base``'s own ``Path | None`` contract). ``database_url``
(and any realtime-odds / venue-weather wiring) is already bound by the
caller's closure -- matching how :data:`PredictCategoryFn` is bound in
``predict_upcoming.py``'s ``_make_predict_fn``.

Raises:
    Any exception from the day-base build -- caught by
    :func:`iter_prewarm_chunks` and encoded as an error result line.
"""

PrewarmParquetPayloadFn = Callable[[str, str, Path], tuple[str, str] | None]
"""Reads the day-base parquet under the given directory for (category,
run_date) and returns ``(parquet_base64, parquet_key)``, or ``None`` when no
parquet file is found. Injected so :func:`iter_prewarm_chunks` stays I/O-free
(mirrors :data:`ParquetPayloadFn` for ``/predict``); the real file read lives
in ``predict_upcoming.py``. Unlike :data:`ParquetPayloadFn` (which reads
mutable ``_last_run`` state because it takes zero args), this callable
receives the day-base directory directly from the just-completed build
result, so no shared state box is needed.
"""


def iter_prewarm_chunks(
    params: PrewarmParams,
    build_fn: PrewarmBuildFn,
    *,
    parquet_payload_fn: PrewarmParquetPayloadFn | None = None,
    time_fn: TimeFn = time.monotonic,
    sleep_fn: SleepFn = time.sleep,
    progress_interval_s: float = PROGRESS_INTERVAL_S,
) -> Generator[bytes, None, None]:
    """Yield NDJSON bytes chunks for a single ``GET /prewarm-day-base`` request.

    Same chunked-keepalive shape as :func:`iter_predict_chunks` (see the
    module docstring's "Chunked encoding rationale") -- JRA's DAY_CHAIN alone
    is still 12 layers and can take several minutes, so this must never be a
    blocking single-response endpoint. Drives *build_fn* via the shared
    :func:`_iter_keepalive` thread/poll/keepalive loop and emits:

    - A pre-flight ``"starting"`` progress line, then a ``"day-base-build"``
      progress line just before the long-running build call.
    - Periodic ``"day-base-build"`` keepalive progress lines every
      *progress_interval_s* seconds while *build_fn* runs in a background
      thread.
    - A single result line as the last yield: ``status="success"`` (with
      ``parquetBase64`` / ``parquetKey`` when *parquet_payload_fn* is provided
      and finds a parquet), :data:`PREWARM_EMPTY_STATUS` (``"empty"``) when
      *build_fn* returns ``None`` (zero target rows for this category+day), or
      ``status="error"`` when *build_fn* raises.

    The generator NEVER raises -- any exception from *build_fn* is caught and
    encoded as an error result line so the HTTP response always terminates
    cleanly, same contract as :func:`iter_predict_chunks`.

    Args:
        params:              Parsed request parameters.
        build_fn:            Day-base build callable matching :data:`PrewarmBuildFn`.
        parquet_payload_fn:  Optional callable invoked after a successful build.
                             Returns ``(parquet_base64, parquet_key)`` or ``None``
                             when no parquet is available. Embedded in the success
                             result line so the Worker DO can proxy the bytes to R2
                             without a write-capable S3 token in the Container.
        time_fn:             Monotonic clock (injectable for deterministic tests).
        sleep_fn:            Sleep callable (injectable for deterministic tests).
        progress_interval_s: Minimum seconds between progress keepalive lines.
    """
    started = time_fn()

    def _elapsed() -> float:
        return time_fn() - started

    yield build_progress_line("starting", _elapsed())
    last_progress = time_fn()

    yield build_progress_line("day-base-build", _elapsed())
    last_progress = time_fn()  # reset after forced emit

    def _call_build() -> Path | None:
        return build_fn(params.category, params.run_date, params.days_ahead)

    day_base_dir, error, last_progress = yield from _iter_keepalive(
        _call_build,
        "day-base-build",
        time_fn=time_fn,
        sleep_fn=sleep_fn,
        progress_interval_s=progress_interval_s,
        elapsed_fn=_elapsed,
        last_progress=last_progress,
    )

    if error is not None:
        error_msg = f"{type(error).__name__}: {error}"
        yield build_prewarm_result_line(
            params.category, params.run_date, status="error", error=error_msg
        )
        return

    if day_base_dir is None:
        yield build_prewarm_result_line(
            params.category, params.run_date, status=PREWARM_EMPTY_STATUS
        )
        return

    # Post-build progress (only if interval elapsed -- build was fast in tests).
    now = time_fn()
    if now - last_progress >= progress_interval_s:
        yield build_progress_line("complete", _elapsed())

    parquet_b64: str | None = None
    parquet_key_val: str | None = None
    if parquet_payload_fn is not None:
        try:
            payload_result = parquet_payload_fn(params.category, params.run_date, day_base_dir)
            if payload_result is not None:
                parquet_b64, parquet_key_val = payload_result
        except BaseException:
            pass

    yield build_prewarm_result_line(
        params.category,
        params.run_date,
        status="success",
        parquet_base64=parquet_b64,
        parquet_key=parquet_key_val,
    )
