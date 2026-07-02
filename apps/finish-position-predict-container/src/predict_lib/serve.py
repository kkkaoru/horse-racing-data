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

Focused per-race fire-and-forget dispatch
------------------------------------------
A "focused per-race full" request (``mode=full`` with both ``keibajoCode`` and
``raceBango`` present -- one specific race) is a special case that does NOT
follow the keepalive-poll-then-join design above.  Its DuckDB feature build +
sequential v7 layer chain + CatBoost/XGBoost scoring can take 10-20+ minutes,
which exceeds the duration the Cloudflare Queues consumer is allowed to hold a
``stub.fetch()`` call open; blocking on it caused the platform to silently
kill the response, leading to queue redelivery and eventual dead-lettering
with zero Neon rows written (the incident this dispatch fixes).

For this one request shape (see :func:`is_focused_full_request`),
``iter_predict_chunks`` launches the pipeline in a background daemon thread
and immediately yields a ``status="accepted"`` result line (see
:data:`FOCUSED_FULL_ACCEPTED_STATUS`) WITHOUT waiting for the thread -- it
never enters the keepalive/join loop.  The background thread keeps running in
the same long-lived container OS process after the HTTP response completes;
the Worker-side queue's own retry/redelivery mechanism is expected to poll a
Neon-based completion check until that thread finishes and UPSERTs.

Because the DuckDB base build and v7 layer chain write to category-scoped
(not race-scoped) work directories (``pipeline_runner.WORK_DIR/feat-{category}-*``),
at most one focused-full pipeline may run at a time per container process --
see :func:`_claim_focused_full_slot`.  The category/day-level batch path and
``mode=rescore`` requests (even with race scope) are entirely unaffected by
this branch and keep the original blocking behaviour.
"""

from __future__ import annotations

import json
import re
import threading
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass
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

R2_FEAT_CACHE_PREFIX: Final[str] = "feat-cache"
"""R2 object key prefix for feature-parquet cache objects."""

FOCUSED_FULL_ACCEPTED_STATUS: Final[str] = "accepted"
"""``build_result_line`` ``status`` value for a focused per-race ``mode=full``
request whose background pipeline was merely *launched*, not waited on.

See :func:`is_focused_full_request` for the exact request shape and the
"Focused per-race fire-and-forget dispatch" note in this module's docstring
for the full incident background.  The Worker-side queue consumer is expected
to treat this status as "in progress, poll Neon for completion" rather than a
final outcome.
"""


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

    __slots__ = ("category", "days_ahead", "keibajo_code", "mode", "race_bango", "run_date")

    def __init__(
        self,
        category: str,
        run_date: str,
        days_ahead: int,
        mode: PredictMode = "full",
        keibajo_code: str | None = None,
        race_bango: str | None = None,
    ) -> None:
        self.category: str = category
        self.run_date: str = run_date
        self.days_ahead: int = days_ahead
        self.mode: PredictMode = mode
        self.keibajo_code: str | None = keibajo_code
        self.race_bango: str | None = race_bango


def is_focused_full_request(params: PredictParams) -> bool:
    """Return True when *params* targets exactly one race with ``mode=full``.

    A "focused per-race full" request supplies both ``keibajo_code`` and
    ``race_bango`` (the per-race scope filter) together with ``mode="full"``.
    This is the request shape whose background pipeline
    :func:`iter_predict_chunks` launches fire-and-forget instead of awaiting
    -- see :data:`FOCUSED_FULL_ACCEPTED_STATUS`.  Day/category batch requests
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

    return PredictParams(
        category=category,
        run_date=run_date,
        days_ahead=days_ahead,
        mode=mode,
        keibajo_code=keibajo_code,
        race_bango=race_bango,
    )


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
    background pipeline was merely launched, not awaited -- see that
    constant's docstring.  ``error`` is included only when non-None (masked
    via :func:`mask_error_message` before encoding).

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
                         (``"accepted"``) for a fire-and-forget focused-full launch.
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
    return predict_fn(
        params.category,
        params.run_date,
        params.days_ahead,
        params.keibajo_code,
        params.race_bango,
    )


def _run_in_thread(
    fn: Callable[[], int],
) -> tuple[threading.Thread, list[int], list[BaseException]]:
    """Run *fn* in a daemon thread; return (thread, result_box, error_box).

    ``result_box`` is a 1-element list that will hold the return value of *fn*
    when it completes successfully.  ``error_box`` is a 1-element list that will
    hold the exception when *fn* raises.  Exactly one of the two will be
    populated after the thread finishes.  The caller polls ``thread.is_alive()``
    (or calls ``thread.join(timeout=...)``) to detect completion.
    """
    result_box: list[int] = []
    error_box: list[BaseException] = []

    def _target() -> None:
        try:
            result_box.append(fn())
        except BaseException as exc:
            error_box.append(exc)

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    return thread, result_box, error_box


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


FocusedFullClaimFn = Callable[[str], bool]
"""Attempts to claim the single per-process focused-full pipeline slot.

Takes the race key (see :func:`build_focused_full_race_key`) and returns
True when the slot was free and is now claimed, False when another
focused-full pipeline is already in flight and no new one may be launched.
Injectable (like :data:`TimeFn` / :data:`SleepFn`) so tests can control the
guard deterministically without real thread races.
"""

FocusedFullReleaseFn = Callable[[str], None]
"""Releases the single per-process focused-full pipeline slot.

Takes the race key that was passed to the matching claim call.  Injectable
for the same reason as :data:`FocusedFullClaimFn`.
"""

_FOCUSED_FULL_LOCK: Final[threading.Lock] = threading.Lock()
"""Guards ``_FOCUSED_FULL_IN_FLIGHT`` across concurrent HTTP requests.

The HTTP server used by ``predict_upcoming.py`` is a single-threaded
``http.server.HTTPServer`` (not ``ThreadingHTTPServer``), so ``do_GET`` calls
never overlapped under the old blocking design.  A focused-full request now
returns almost immediately *before* its background pipeline finishes, which
frees the server to accept a new connection (e.g. for a different race in the
same category) while the first race's pipeline is still writing to the
category-scoped work directories in ``pipeline_runner.WORK_DIR``.  This lock
protects the claim/release check-then-set from a race between the accept-loop
thread (claiming for a new request) and a background pipeline thread (calling
release for a finished one).
"""

_FOCUSED_FULL_IN_FLIGHT: Final[list[str | None]] = [None]
"""Single-slot in-process guard: the race key of the one focused-full
background pipeline currently running in this container process, or ``None``
when the slot is free.

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


def _claim_focused_full_slot(race_key: str) -> bool:
    """Claim the single per-process focused-full pipeline slot.

    Returns True (and records *race_key* as in-flight) if no other
    focused-full background pipeline is currently running in this container
    process; False if one already is.  The DuckDB base build + v7 layer chain
    (``pipeline_runner.py``) writes to CATEGORY-scoped work directories
    (``WORK_DIR/feat-{category}-base``, ``feat-{category}-layer-N``,
    ``feat-{category}-v7-final``) -- not race-scoped -- and a container
    instance serves exactly one category, so at most one focused-full
    pipeline may run at a time to avoid two concurrent builds for different
    races corrupting each other's shared intermediate files.
    """
    with _FOCUSED_FULL_LOCK:
        if _FOCUSED_FULL_IN_FLIGHT[0] is not None:
            return False
        _FOCUSED_FULL_IN_FLIGHT[0] = race_key
        return True


def _release_focused_full_slot(race_key: str) -> None:
    """Release the focused-full slot, if it is still held by *race_key*.

    A no-op when the slot is already free or held by a different key -- this
    should not happen given the single-slot claim/release protocol, but the
    guard avoids releasing a slot claimed by a later, unrelated request.
    """
    with _FOCUSED_FULL_LOCK:
        if _FOCUSED_FULL_IN_FLIGHT[0] == race_key:
            _FOCUSED_FULL_IN_FLIGHT[0] = None


def iter_predict_chunks(
    params: PredictParams,
    predict_fn: PredictCategoryFn,
    *,
    rescore_fn: PredictCategoryFn | None = None,
    parquet_payload_fn: ParquetPayloadFn | None = None,
    per_race_parquet_payload_fn: PerRaceParquetPayloadFn | None = None,
    focused_full_claim_fn: FocusedFullClaimFn | None = None,
    focused_full_release_fn: FocusedFullReleaseFn | None = None,
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
      (for a focused per-race full request only) ``"accepted"``.

    Threading keepalive design
    --------------------------
    The prediction pipeline (*predict_fn* / *rescore_fn*) is launched in a
    background daemon thread via :func:`_run_in_thread`.  The generator's main
    thread loops with ``thread.join(timeout=_POLL_INTERVAL_S)``; when the join
    times out (thread still alive) AND *progress_interval_s* has elapsed since
    the last yield, a new ``"predict"`` progress line is emitted.  This ensures
    the DO receives a keepalive chunk roughly every *progress_interval_s* seconds
    throughout the entire 3-8-minute pipeline run.

    **Exception**: a focused per-race full request (see
    :func:`is_focused_full_request`) never enters this poll/join loop at all --
    see "Fire-and-forget dispatch" below.

    Mode dispatch:
    - ``mode=full`` (default) -- always calls *predict_fn*.
    - ``mode=rescore`` with *rescore_fn* provided -- calls *rescore_fn* first;
      on ``CacheMissError`` falls back to *predict_fn* after emitting a
      ``rescore-fallback-to-full`` progress line.
    - ``mode=rescore`` without *rescore_fn* -- falls back to *predict_fn*
      immediately (same as ``mode=full``; emits fallback progress line).

    Fire-and-forget dispatch (focused per-race full only)
    -------------------------------------------------------
    When ``is_focused_full_request(params)`` is True, *predict_fn* is launched
    in a background daemon thread via :func:`_run_in_thread` but is NOT polled
    or joined.  Instead the generator immediately yields a single result line
    with ``status=FOCUSED_FULL_ACCEPTED_STATUS`` (``"accepted"``) and
    ``racesPredicted=0``, then returns.  Before launching, the single
    per-process guard (:func:`_claim_focused_full_slot` by default, or
    *focused_full_claim_fn* / *focused_full_release_fn* when injected) is
    consulted: if another focused-full pipeline is already in flight for this
    container process, NO new thread is launched at all -- the ``"accepted"``
    line is still yielded (the caller is expected to rely on the queue's retry
    to observe the in-flight pipeline's eventual Neon write, whichever race
    triggered it). See the module docstring's "Focused per-race fire-and-forget
    dispatch" section for the full incident background. This branch is purely
    additive: ``mode=rescore`` and non-focused ``mode=full`` requests are
    completely unaffected and keep using the poll/join loop described above.

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
                             called from the background thread once the launched
                             pipeline finishes (success or error). Only used for a
                             focused per-race full request whose slot was claimed.
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
        # Fire-and-forget dispatch: launch the pipeline in a background daemon
        # thread and return almost instantly instead of polling/joining it --
        # see the module docstring's "Focused per-race fire-and-forget
        # dispatch" section and this function's docstring for the incident
        # rationale.  The single per-process guard prevents two concurrent
        # focused-full pipelines (for different races) from corrupting the
        # SAME category-scoped work directories (pipeline_runner.WORK_DIR).
        race_key = build_focused_full_race_key(params)
        claim_fn: FocusedFullClaimFn = (
            focused_full_claim_fn if focused_full_claim_fn is not None else _claim_focused_full_slot
        )
        release_fn: FocusedFullReleaseFn = (
            focused_full_release_fn
            if focused_full_release_fn is not None
            else _release_focused_full_slot
        )
        if claim_fn(race_key):

            def _run_focused_full_and_release() -> int:
                try:
                    return first_call()
                finally:
                    release_fn(race_key)

            _run_in_thread(_run_focused_full_and_release)
        # else: another focused-full pipeline is already in flight for this
        # process -- do NOT launch a second one; the queue retry will observe
        # the in-flight pipeline's own eventual Neon write.
        yield build_result_line(
            params.category,
            params.run_date,
            0,
            status=FOCUSED_FULL_ACCEPTED_STATUS,
        )
        return

    thread, result_box, error_box = _run_in_thread(first_call)

    # Keepalive loop: poll the thread while it runs, yielding progress lines so
    # the Worker DO can call renewActivityTimeout and the Container is not reaped.
    while thread.is_alive():
        sleep_fn(_POLL_INTERVAL_S)
        now = time_fn()
        if now - last_progress >= progress_interval_s:
            yield build_progress_line("predict", _elapsed())
            last_progress = now

    thread.join()  # final join to synchronise memory visibility

    # Check thread outcome.
    if error_box:
        first_exc = error_box[0]

        # If rescore raised CacheMissError, fall back to the full pipeline.
        if use_rescore_first and isinstance(first_exc, CacheMissError):
            yield build_progress_line("rescore-fallback-to-full", _elapsed())
            last_progress = time_fn()

            def _fallback_call() -> int:
                return _run_predict_fn(predict_fn, params)

            fb_thread, fb_result_box, fb_error_box = _run_in_thread(_fallback_call)

            while fb_thread.is_alive():
                sleep_fn(_POLL_INTERVAL_S)
                now = time_fn()
                if now - last_progress >= progress_interval_s:
                    yield build_progress_line("predict", _elapsed())
                    last_progress = now

            fb_thread.join()

            if fb_error_box:
                fallback_exc = fb_error_box[0]
                error_msg = f"{type(fallback_exc).__name__}: {fallback_exc}"
                yield build_result_line(
                    params.category,
                    params.run_date,
                    races_predicted,
                    status="error",
                    error=error_msg,
                )
                return

            races_predicted = fb_result_box[0]
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
        races_predicted = result_box[0]

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
