"""Hardened MLflow trace/span/assessment emitters for `sync_production.py`.

★ THIS MODULE SUPERSEDES the earlier "MLflow traces are not used" decision
(see the OLD version of this package's README section of the same name, and
`sync_production.py`'s own module docstring before this change) -- that
decision was correct about the hazard it identified, but identified the
wrong FIX (there wasn't one, at the time). It was based on the FLUENT API
(`mlflow.start_trace()`/`mlflow.end_trace()`/`mlflow.log_feedback()`), which
persists through a process-wide OpenTelemetry singleton that resolves its
destination via the GLOBAL `mlflow.get_tracking_uri()` state -- reproduced
empirically in this package's own diagnostic history (see
`tests/conftest.py`'s `clear_ambient_backend_uri` docstring for the real
2026-07-08 incident this exact failure class already caused: a leaked
global tracking URI silently overwrote two production champion aliases with
fake test data). A predecessor's "Phase 1" prototype investigation (see the
two scratchpad scripts referenced in that investigation's own history) found
a DIFFERENT, lower-level write path that this module builds on:
`mlflow.tracing.client.TracingClient(tracking_uri=...)` constructed
DIRECTLY (a per-instance client, never the bare `mlflow` module, never
`mlflow.set_tracking_uri()`), with manually-constructed `TraceInfo`/`Span`
objects written via `TracingClient.start_trace(trace_info)` /
`.log_spans(...)` / `.log_assessment(...)` / `.search_traces(...)`. This
path resolves its destination ENTIRELY from the explicit `tracking_uri`
string passed to its own constructor -- there is no OTel exporter, no
background thread, no async queue, and no global state anywhere in it. This
sidesteps BOTH hazards the original "traces are not used" decision was
about:

  1. The global-tracking-URI hazard above (this module never calls
     `mlflow.set_tracking_uri()` or any bare `mlflow.*` fluent function --
     grep this file: there is no top-level `import mlflow` at all, only
     narrow imports from `mlflow.entities`/`mlflow.tracing.*`/
     `mlflow.exceptions`/`mlflow.tracking.client`).
  2. The async trace-export RACE the fluent API also has (a
     `log_feedback()` call immediately after `end_trace()` can run before
     the trace has actually landed in the store, observed empirically by
     the Phase 1 prototype as a `ForeignKeyViolation` on
     `assessments.trace_id`): `TracingClient.start_trace`/`log_spans`/
     `log_assessment` are synchronous, direct store writes -- by the time
     `emit_fp_race_trace`/`emit_rs_horse_trace` below call
     `log_assessment`, `start_trace` has ALREADY returned, so the trace row
     already exists.

Verified against **mlflow-skinny==3.14.0** (see this package's own
`pyproject.toml` pin -- confirmed the exact installed version by reading
`.venv/lib/python3.12/site-packages/mlflow_skinny-3.14.0.dist-info` in this
project directly) by reading `mlflow/tracing/client.py`,
`mlflow/store/tracking/dbmodels/models.py`, and `mlflow/utils/search_utils.py`
directly in this project's own `.venv`. `TracingClient`, manual
`TraceInfo`/`Span` construction via `opentelemetry.sdk.trace.ReadableSpan`,
and the `mlflow.tracing.utils` helpers used below are genuinely
SEMI-INTERNAL API surface (not the documented top-level `mlflow.*` fluent
API) that a future mlflow upgrade could change or remove without notice.
That risk is deliberately CONTAINED to this one file: every other module in
this package that wants to emit a trace imports ONLY the public functions
below (`emit_fp_race_traces`/`emit_rs_horse_traces`/`build_tracing_client`),
never `mlflow.tracing`/`opentelemetry` internals directly -- so an upgrade
breakage surfaces here, in one place, not scattered across the codebase.

★ A landmine found empirically HERE, NOT anticipated by the Phase 1
prototype (whose own diag script only ever used short model_version
strings): `mlflow/store/tracking/dbmodels/models.py`'s `SqlTraceInfo.
client_request_id` is a real `Column(String(50), nullable=True)` -- a
genuine `VARCHAR(50)` column on Postgres (this package's live, Neon-backed
production store). A human-readable business key like
`"{race_date}:{category}:{venue}:{race_bango}:{model_version}"` can easily
exceed 50 characters once a cell-routed variant model_version (e.g.
`"...-jockey-pedigree269"`) is involved, and Postgres would raise a
`DataError` (value too long for type character varying(50)) on insert.
sqlite -- this package's hermetic test backend (see `tests/conftest.py`) --
does NOT enforce VARCHAR length at all, so a test suite running only
against sqlite would never catch this until it broke in production. This
module therefore NEVER puts the raw business key into `client_request_id`
directly: `_client_request_id` hashes it down to a small, FIXED-LENGTH,
well-under-50-character value (a 2-char task prefix + ":" + a 40-hex-char
SHA-1 digest == 43 characters, always, regardless of how long the input
key is), and carries the full human-readable key on a plain trace TAG
(`TRACE_BUSINESS_KEY_TAG`) instead -- trace tag VALUES are `String(8000)`,
with no such risk for any realistic key length here.

Idempotency (the single most important correctness property here, since
this runs against real production data and is re-run many times -- daily
incremental sync AND a historical backfill that may be interrupted and
resumed): every emitted trace's `client_request_id` is a DETERMINISTIC hash
of its business key (see `_client_request_id`), and every emit function
performs a pre-emit `search_traces` existence check
(`attribute.client_request_id = '<hash>'`, scoped to the target experiment)
BEFORE writing anything. A second call for the same business key is a
cheap no-op: one `search_traces` round-trip, zero writes (no trace, no
spans, no assessments) -- this is what makes both the daily sync and a
re-run of `backfill_traces.py` safe to call repeatedly / resume after an
interruption. See `emit_fp_race_traces`/`emit_rs_horse_traces`'s own
docstrings for the per-row error-isolation this is paired with (one bad
row's trace-emission failure must never block every other row's).

Destinations: ONLY `config.EXPERIMENT_FP_PRODUCTION_USAGE` /
`config.EXPERIMENT_RS_PRODUCTION_USAGE` ever receive a trace from this
module -- by construction, not by an enforced allow-list inside this file:
every caller in this package (`sync_production.py`, `backfill_traces.py`)
only ever resolves and passes in one of those two experiment ids. Every
OTHER experiment in this package (`wf-eval`, `champion-eval`, `cell-eval`,
`timelines`, `registry-backfill`, `internal/smoke-tests`, ...)
intentionally has NONE -- a trace's Usage/Quality/Tool-calls value is
specifically about "what was actually served in production", which is
exactly what those two experiments (and only those two) already represent
(see `sync_production.py`'s own module docstring).

`status` is always `OK`: every row this module is ever called with already
represents a genuinely-served, successfully-joined prediction -- the eval
join in `serve_eval.py` (`build_fp_race_eval_rows`/`build_rs_horse_eval_rows`)
already filters out anything that did not match a finalized result, and
this module is only ever called with rows that survived that join (see
`sync_production.py`'s wiring). There is currently NO source of "this
prediction attempt itself failed" signal anywhere in this pipeline -- a
serving GAP day (see `sync_production._log_serving_gap`) has ZERO rows and
therefore ZERO traces are emitted for it; this module never fabricates an
ERROR trace for a race/horse that simply never got served, since that would
misrepresent reality. `TraceState.ERROR` / `SpanStatusCode.ERROR` are
therefore UNREACHABLE CODE in this module today -- a documented, deliberate
absence, not an oversight -- until some future, separate, out-of-scope
change gives the serving pipeline itself a real failure signal to plumb
through here.

Timing is NOMINAL, always -- disclosed, never hidden: there is no real
per-race/per-horse serving-latency measurement anywhere in this pipeline (a
different team's domain, out of scope here). Every span this module emits
carries an explicit `TIMING_ATTR_KEY: TIMING_APPROXIMATE` attribute, and
`_NOMINAL_SCORE_MODEL_MS`/`_NOMINAL_UPSERT_NEON_MS` are small FIXED
constants, not a measurement. The root span's `start_time`, however, is
NEVER fabricated: it is always the row's REAL `prediction_generated_at`
(historical timestamps honored exactly like `timeline.py`'s own
historical-timestamp philosophy -- see that module's `upsert_timeline_point`
docstring) -- only the DURATION is nominal. The MLflow UI's Usage-page
latency panel will show these nominal/approximate numbers until the
serving pipeline itself (out of scope here) ever emits real timing data.

RS design decision (the team-lead spec was written FP-centric; this is a
judgment call this module resolves and documents, since RS has no natural
per-race aggregate the way FP's ranking does -- see `serve_eval.py`'s own
module docstring on the two evaluation granularities): running-style
predictions are PER-HORSE (`serve_eval.RsHorseEvalRow`), so this module
emits ONE TRACE PER HORSE, not per race. Root span name `"predict-horse"`
(the RS-appropriate parallel to FP's `"predict-race"`), root attributes
`race_key`/`ketto_toroku_bango`/`category`/`model_version`/`race_date`. The
alternative considered and rejected -- one trace per RACE with a per-horse
child span each -- would have no natural root-level "did this race's
prediction succeed" outcome to attach to the root span or to gate
assessments on (RS has no race-level hit/miss the way FP's ranking does);
per-horse is the actual unit RS predicts AND evaluates at
(`RsHorseEvalRow.hit`), so it is the natural trace unit too.
"""

from __future__ import annotations

import hashlib
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Final, cast

from mlflow.entities import AssessmentSource, AssessmentSourceType, Feedback
from mlflow.entities.span import Span, SpanType
from mlflow.entities.span_status import SpanStatus, SpanStatusCode
from mlflow.entities.trace_info import TraceInfo
from mlflow.entities.trace_location import TraceLocation
from mlflow.entities.trace_state import TraceState
from mlflow.exceptions import MlflowException
from mlflow.tracing.client import TracingClient
from mlflow.tracing.constant import SpanAttributeKey, TraceTagKey
from mlflow.tracing.utils import (
    build_otel_context,
    dump_span_attribute_value,
    encode_trace_id,
)
from mlflow.tracking.client import MlflowClient
from opentelemetry.sdk.resources import Resource as OTelResource
from opentelemetry.sdk.trace import ReadableSpan as OTelReadableSpan

from mlflow_tracking import serve_eval

# ── Public constants (shared by tests + sync_production.py + backfill_traces.py) ──

TRACE_BUSINESS_KEY_TAG: Final[str] = "trace_business_key"
ASSESSMENT_SOURCE_ID: Final[str] = "sync_production"

TIMING_ATTR_KEY: Final[str] = "timing"
TIMING_APPROXIMATE: Final[str] = "approximate"

FP_ROOT_SPAN_NAME: Final[str] = "predict-race"
RS_ROOT_SPAN_NAME: Final[str] = "predict-horse"
SCORE_MODEL_SPAN_NAME: Final[str] = "score-model"
UPSERT_NEON_SPAN_NAME: Final[str] = "upsert-neon"

TOP3_BOX_ASSESSMENT_NAME: Final[str] = "top3_box_score"
RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME: Final[str] = "predicted_class_hit"

# Nominal (NOT measured -- see module docstring) per-span durations, ms.
_NOMINAL_SCORE_MODEL_MS: Final[int] = 400
_NOMINAL_UPSERT_NEON_MS: Final[int] = 100

_FP_PREFIX: Final[str] = "fp"
_RS_PREFIX: Final[str] = "rs"

_NS_PER_MS: Final[int] = 1_000_000


@dataclass
class TraceEmitSummary:
    """Outcome counters for one `emit_fp_race_traces`/`emit_rs_horse_traces` call.

    `traces_created` counts rows that got a brand-new trace (+ spans +
    assessments) THIS call. `traces_already_existed` counts rows whose
    business key already had a trace from a PREVIOUS call (the idempotency
    fast-path -- one cheap `search_traces` round-trip, zero writes) -- this
    is the normal, expected outcome for every row on a re-run (daily sync
    re-covering an overlapping range, or a resumed backfill), not an error.
    `errors` isolates one row's trace-emission failure (an `MlflowException`
    from the tracking store, or a defensive lookup miss -- see this
    dataclass's callers' own docstrings) from every OTHER row in the same
    call: a bad row is recorded here and skipped, never raised, so it can
    never abort the rest of the batch OR the metric/table logging this
    trace emission is layered on top of in `sync_production.py`.
    """

    traces_created: int = 0
    traces_already_existed: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _ChildSpanSpec:
    """One child (TOOL) span to emit under a trace's root span."""

    name: str
    duration_ms: int
    attributes: dict[str, object]


def build_tracing_client(client: MlflowClient) -> TracingClient:
    """Construct a `TracingClient` resolved from `client`'s OWN explicit
    `tracking_uri` property -- never via `mlflow.set_tracking_uri()` or any
    other ambient/global state (see this module's own docstring for why
    that distinction is the entire reason this module is safe to use where
    the fluent API was not). `MlflowClient.tracking_uri` is itself always
    the EXPLICIT uri that instance was constructed with (or resolved from
    `config.get_tracking_uri()` by `cli.build_client()` -- either way, never
    read back from global state), so this never introduces a new global
    dependency of its own.
    """
    return TracingClient(tracking_uri=client.tracking_uri)


def _client_request_id(prefix: str, business_key: str) -> str:
    """Deterministic, LENGTH-SAFE `client_request_id` for `business_key`.

    See this module's own docstring for the `VARCHAR(50)` landmine this
    exists to avoid: `f"{prefix}:{sha1_hex_digest}"` is ALWAYS exactly
    `len(prefix) + 1 + 40` characters (2 + 1 + 40 == 43 for both `"fp"`/
    `"rs"` prefixes used in this module), regardless of how long
    `business_key` itself is -- well under the 50-character column limit,
    with no truncation logic needed (truncation would risk collisions;
    fixed-length hashing does not).
    """
    digest = hashlib.sha1(business_key.encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


def _new_id(bits: int) -> int:
    """Return a random nonzero unsigned integer with `bits` bits, suitable
    as an OTel trace/span id. Only ever called when a NEW trace is about to
    be written (after the idempotency existence check already found
    nothing) -- collisions are not a correctness concern for the same
    reason `tool_calls_proto.py`'s own diagnostic use of this exact pattern
    was fine: OTel trace/span ids are 128-bit/64-bit random identifiers by
    design, not business keys (the business key -- and this module's own
    idempotency check -- is `TRACE_BUSINESS_KEY_TAG`/`client_request_id`,
    never the trace id itself).
    """
    value = 0
    while value == 0:
        value = random.getrandbits(bits)
    return value


def _find_existing_trace_id(
    tracing_client: TracingClient, experiment_id: str, client_request_id: str
) -> str | None:
    """Return the trace_id already carrying `client_request_id` in
    `experiment_id`, or None if no such trace exists yet.

    `include_spans=False`: this is purely an existence check, never a read
    of trace content, so downloading span data would be pure waste.
    `attribute.client_request_id` is a genuine, indexed-by-column MLflow
    search key (`SearchTraceUtils.VALID_SEARCH_ATTRIBUTE_KEYS`, confirmed by
    reading `mlflow/utils/search_utils.py` in this project's own `.venv` --
    see this module's own docstring) -- NOT a tag search, so this does not
    depend on `TRACE_BUSINESS_KEY_TAG` being queryable at all (that tag
    exists for human debugging/UI search, not for this idempotency check).
    """
    matches = tracing_client.search_traces(
        experiment_ids=[experiment_id],
        filter_string=f"attribute.client_request_id = '{client_request_id}'",
        max_results=1,
        include_spans=False,
    )
    return matches[0].info.trace_id if matches else None


def _make_span(
    *,
    trace_id_int: int,
    trace_id_str: str,
    span_id_int: int,
    parent_id_int: int | None,
    name: str,
    span_type: str,
    start_ns: int,
    end_ns: int,
    attributes: Mapping[str, object],
) -> Span:
    """Build one `Span` via a raw `opentelemetry.sdk.trace.ReadableSpan` --
    mirrors `tool_calls_proto.py`'s own `_make_span` helper exactly (the
    proven reference implementation this module builds on). `status` is
    always OK -- see this module's own docstring for why `ERROR` is
    unreachable here.
    """
    encoded_attributes = {
        SpanAttributeKey.REQUEST_ID: dump_span_attribute_value(trace_id_str),
        SpanAttributeKey.SPAN_TYPE: dump_span_attribute_value(span_type),
        **{key: dump_span_attribute_value(value) for key, value in attributes.items()},
    }
    status = SpanStatus(status_code=SpanStatusCode.OK, description="")
    otel_span = OTelReadableSpan(
        name=name,
        context=build_otel_context(trace_id_int, span_id_int),
        parent=build_otel_context(trace_id_int, parent_id_int) if parent_id_int else None,
        start_time=start_ns,
        end_time=end_ns,
        attributes=encoded_attributes,
        status=status.to_otel_status(),
        resource=OTelResource.get_empty(),
    )
    return Span(otel_span)


def _log_feedback(
    tracing_client: TracingClient, trace_id: str, *, name: str, value: bool | float, rationale: str
) -> None:
    """Log one `Feedback` assessment onto `trace_id` via
    `TracingClient.log_assessment` -- a direct, synchronous store write (see
    this module's own docstring for why this sidesteps the async-export
    race the fluent `mlflow.log_feedback` path has)."""
    tracing_client.log_assessment(
        trace_id,
        Feedback(
            name=name,
            value=value,
            source=AssessmentSource(
                source_type=AssessmentSourceType.CODE, source_id=ASSESSMENT_SOURCE_ID
            ),
            rationale=rationale,
        ),
    )


def _emit_trace(
    tracing_client: TracingClient,
    experiment_id: str,
    *,
    client_request_id: str,
    business_key: str,
    root_span_name: str,
    root_attributes: Mapping[str, object],
    child_specs: Sequence[_ChildSpanSpec],
    start_time: datetime,
    tags: Mapping[str, str],
) -> str | None:
    """Emit one trace (root span + child spans), or skip (return None) if a
    trace for `client_request_id` already exists -- the shared idempotent
    core both `emit_fp_race_trace`/`emit_rs_horse_trace` build on.

    `start_time` becomes the root span's (and the whole trace's) start --
    always the row's REAL `prediction_generated_at`, never fabricated (see
    module docstring). Every child span's own start/end is nominal,
    sequential (score-model, then upsert-neon), immediately following
    `start_time` -- mirroring `tool_calls_proto.py`'s own sequential-cursor
    construction.
    """
    existing_trace_id = _find_existing_trace_id(tracing_client, experiment_id, client_request_id)
    if existing_trace_id is not None:
        return None

    trace_id_int = _new_id(128)
    trace_id_str = f"tr-{encode_trace_id(trace_id_int)}"
    root_span_id_int = _new_id(64)

    start_ns = int(start_time.timestamp() * 1_000_000_000)
    cursor_ns = start_ns
    child_spans: list[Span] = []
    for spec in child_specs:
        span_start_ns = cursor_ns
        span_end_ns = span_start_ns + spec.duration_ms * _NS_PER_MS
        cursor_ns = span_end_ns
        child_spans.append(
            _make_span(
                trace_id_int=trace_id_int,
                trace_id_str=trace_id_str,
                span_id_int=_new_id(64),
                parent_id_int=root_span_id_int,
                name=spec.name,
                span_type=SpanType.TOOL,
                start_ns=span_start_ns,
                end_ns=span_end_ns,
                attributes=spec.attributes,
            )
        )
    end_ns = cursor_ns

    root_span = _make_span(
        trace_id_int=trace_id_int,
        trace_id_str=trace_id_str,
        span_id_int=root_span_id_int,
        parent_id_int=None,
        name=root_span_name,
        span_type=SpanType.TASK,
        start_ns=start_ns,
        end_ns=end_ns,
        attributes=root_attributes,
    )

    trace_info = TraceInfo(
        trace_id=trace_id_str,
        trace_location=TraceLocation.from_experiment_id(experiment_id),
        request_time=start_ns // _NS_PER_MS,
        state=TraceState.OK,
        execution_duration=(end_ns - start_ns) // _NS_PER_MS,
        client_request_id=client_request_id,
        tags={
            **tags,
            TRACE_BUSINESS_KEY_TAG: business_key,
            TraceTagKey.TRACE_NAME: root_span_name,
        },
        trace_metadata={},
    )
    tracing_client.start_trace(trace_info)
    tracing_client.log_spans(experiment_id, [root_span, *child_spans])
    return trace_id_str


# ── FP: one trace per (race, model_version) ─────────────────────────────────


def fp_race_key(eval_row: serve_eval.FpRaceEvalRow) -> str:
    """`"{race_date}:{category}:{venue}:{race_bango}"` -- the race-grain
    identity a trace's business key is built from, mirroring `sync_
    production.py`'s own `sync_key = "{date}:{category}:{model_version}"`
    ordering convention, extended with venue/race_bango."""
    return f"{eval_row.race_date}:{eval_row.category}:{eval_row.venue}:{eval_row.race_bango}"


def _log_fp_assessments(
    tracing_client: TracingClient, trace_id: str, eval_row: serve_eval.FpRaceEvalRow
) -> None:
    """Log the 6 rank-hit Feedback assessments (place1_hit == top1_hit
    through place6_hit) plus `top3_box_score`, respecting the EXACT
    small-field exclusion `serve_eval._compute_race_hits` already applies:
    a rank whose `place{n}_hit` is None (not applicable to this race at all
    -- see that function's own docstring) gets NO Feedback logged for it,
    never a fabricated `False`.

    `top3_box_score` uses `top3_box_hit`'s existing 0/1 meaning verbatim,
    logged as a float (`0.0`/`1.0`) rather than a bool -- decided (not
    merely defaulted) after checking this package for a more graduated
    per-race definition: none exists anywhere in `mlflow_tracking` (only
    the DAY/CELL-level AGGREGATED `top3_box_pct` PERCENTAGE, computed across
    many races -- see `serve_eval.aggregate_fp_day_metrics`/`champion_cell_
    eval._aggregate_fp_cells`); there is nothing more graduated at the
    single-race grain this per-trace assessment lives at.
    """
    rank_hits: tuple[tuple[int, int | None], ...] = (
        (1, eval_row.top1_hit),
        (2, eval_row.place2_hit),
        (3, eval_row.place3_hit),
        (4, eval_row.place4_hit),
        (5, eval_row.place5_hit),
        (6, eval_row.place6_hit),
    )
    for rank, hit in rank_hits:
        if hit is None:
            continue
        _log_feedback(
            tracing_client,
            trace_id,
            name=f"place{rank}_hit",
            value=bool(hit),
            rationale=(
                f"place{rank}_hit: did the model's top pick finish within the top {rank} "
                "(serve_eval.FpRaceEvalRow)?"
            ),
        )
    _log_feedback(
        tracing_client,
        trace_id,
        name=TOP3_BOX_ASSESSMENT_NAME,
        value=float(eval_row.top3_box_hit),
        rationale=(
            "top3_box_score: did the model's top-3 picks box the actual top 3 in any order "
            "(0.0/1.0, serve_eval.FpRaceEvalRow.top3_box_hit)?"
        ),
    )


def emit_fp_race_trace(
    tracing_client: TracingClient,
    experiment_id: str,
    eval_row: serve_eval.FpRaceEvalRow,
    *,
    generated_at: datetime,
) -> str | None:
    """Emit one trace (+ assessments) for one FP race-eval row, or skip
    (return None) if a trace for this exact (race, model_version) already
    exists (idempotent no-op -- see module docstring). Root span
    `FP_ROOT_SPAN_NAME`; child spans `SCORE_MODEL_SPAN_NAME` (carries
    `model_version` -- one span NAME shared across every model_version, per
    team-lead design, so the Tool-calls page's per-name aggregation groups
    them together; per-model_version splitting stays possible via this
    attribute or the `model_version` trace tag) and `UPSERT_NEON_SPAN_NAME`.
    """
    race_key = fp_race_key(eval_row)
    business_key = f"{race_key}:{eval_row.model_version}"
    client_request_id = _client_request_id(_FP_PREFIX, business_key)

    root_attributes: dict[str, object] = {
        "race_key": race_key,
        "category": eval_row.category,
        "model_version": eval_row.model_version,
        "race_date": eval_row.race_date,
        TIMING_ATTR_KEY: TIMING_APPROXIMATE,
    }
    child_specs = (
        _ChildSpanSpec(
            name=SCORE_MODEL_SPAN_NAME,
            duration_ms=_NOMINAL_SCORE_MODEL_MS,
            attributes={
                "race_key": race_key,
                "category": eval_row.category,
                "model_version": eval_row.model_version,
                TIMING_ATTR_KEY: TIMING_APPROXIMATE,
            },
        ),
        _ChildSpanSpec(
            name=UPSERT_NEON_SPAN_NAME,
            duration_ms=_NOMINAL_UPSERT_NEON_MS,
            attributes={
                "race_key": race_key,
                "category": eval_row.category,
                TIMING_ATTR_KEY: TIMING_APPROXIMATE,
            },
        ),
    )
    trace_id = _emit_trace(
        tracing_client,
        experiment_id,
        client_request_id=client_request_id,
        business_key=business_key,
        root_span_name=FP_ROOT_SPAN_NAME,
        root_attributes=root_attributes,
        child_specs=child_specs,
        start_time=generated_at,
        tags={
            "category": eval_row.category,
            "model_version": eval_row.model_version,
            "race_date": eval_row.race_date,
        },
    )
    if trace_id is None:
        return None
    _log_fp_assessments(tracing_client, trace_id, eval_row)
    return trace_id


def _fp_generated_at_lookup(
    rows: Sequence[Mapping[str, object]],
) -> dict[tuple[str, str], datetime]:
    """Map (keibajo_code, race_bango) -> that race's real
    `prediction_generated_at`, from the RAW prediction rows a `FpRaceEvalRow`
    group was built from (`FpRaceEvalRow` itself carries no timestamp --
    see `serve_eval.py`). Every row sharing one race shares one
    `prediction_generated_at` batch (see `sync_production.
    _CategoryDateOutcome`'s own docstring), so the FIRST row encountered per
    race key wins -- not a re-derivation, just picking any one of an
    already-identical set.
    """
    result: dict[tuple[str, str], datetime] = {}
    for row in rows:
        key = (str(row["keibajo_code"]), str(row["race_bango"]))
        if key not in result:
            result[key] = cast(datetime, row["prediction_generated_at"])
    return result


def emit_fp_race_traces(
    tracing_client: TracingClient,
    experiment_id: str,
    eval_rows: Sequence[serve_eval.FpRaceEvalRow],
    rows: Sequence[Mapping[str, object]],
) -> TraceEmitSummary:
    """Emit one trace (+ assessments) per row in `eval_rows`, isolating one
    bad row's failure from every other row (mirrors `sync_production.py`'s
    own per-(date, category, task) isolation philosophy, applied one level
    deeper: one bad RACE never blocks the rest of that day's traces, or the
    metric/table logging this call is layered on top of).

    `rows` must be the SAME raw prediction-row group `eval_rows` was built
    from (see `sync_production._sync_fp_eval`'s call site) -- used ONLY to
    look up each race's real `prediction_generated_at` (see
    `_fp_generated_at_lookup`), since `FpRaceEvalRow` itself does not carry
    that field. A race present in `eval_rows` with no match in this lookup
    is recorded in `errors` and skipped -- constructed consistently by
    every caller in this package today, so this should not happen in
    practice, but a missing lookup must never crash the whole sync loop.
    """
    summary = TraceEmitSummary()
    generated_at_by_race = _fp_generated_at_lookup(rows)
    for eval_row in eval_rows:
        key = (eval_row.venue, eval_row.race_bango)
        generated_at = generated_at_by_race.get(key)
        if generated_at is None:
            summary.errors.append(
                f"{key}:{eval_row.model_version}: no prediction_generated_at found for "
                "trace emission"
            )
            continue
        try:
            trace_id = emit_fp_race_trace(
                tracing_client, experiment_id, eval_row, generated_at=generated_at
            )
        except (MlflowException, ValueError, KeyError, TypeError) as exc:
            summary.errors.append(f"{key}:{eval_row.model_version}: {exc}")
            continue
        if trace_id is None:
            summary.traces_already_existed += 1
        else:
            summary.traces_created += 1
    return summary


# ── RS: one trace per horse ──────────────────────────────────────────────────


def rs_race_key(eval_row: serve_eval.RsHorseEvalRow) -> str:
    """`"{race_date}:{category}:{venue}:{race_bango}"` -- same shape as
    `fp_race_key`, RS's race-grain identity (the horse-grain identity is
    this plus `ketto_toroku_bango`, see `emit_rs_horse_trace`)."""
    return f"{eval_row.race_date}:{eval_row.category}:{eval_row.venue}:{eval_row.race_bango}"


def _log_rs_assessments(
    tracing_client: TracingClient, trace_id: str, eval_row: serve_eval.RsHorseEvalRow
) -> None:
    """Log the single `predicted_class_hit` Feedback assessment -- RS's
    parallel to FP's rank-hit assessments (see module docstring's RS design
    rationale): `RsHorseEvalRow.hit` is ALREADY the exact 0/1 "did the
    predicted class match the actual class" boolean, computed once in
    `serve_eval.build_rs_horse_eval_rows` -- this is a direct, verbatim use
    of that field, not a re-derivation.
    """
    _log_feedback(
        tracing_client,
        trace_id,
        name=RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME,
        value=bool(eval_row.hit),
        rationale=(
            f"predicted_class_hit: predicted={eval_row.predicted_label!r} "
            f"actual={eval_row.actual_label!r} (serve_eval.RsHorseEvalRow.hit)"
        ),
    )


def emit_rs_horse_trace(
    tracing_client: TracingClient,
    experiment_id: str,
    eval_row: serve_eval.RsHorseEvalRow,
    *,
    generated_at: datetime,
) -> str | None:
    """Emit one trace (+ assessment) for one RS horse-eval row, or skip
    (return None) if a trace for this exact (horse, model_version) already
    exists (idempotent no-op -- see module docstring). Root span
    `RS_ROOT_SPAN_NAME`; child spans share the SAME `SCORE_MODEL_SPAN_NAME`/
    `UPSERT_NEON_SPAN_NAME` names FP uses (see those constants' own
    docstrings for why one universal name per tool step, across both
    tasks, is the deliberate design).
    """
    race_key = rs_race_key(eval_row)
    business_key = f"{race_key}:{eval_row.ketto_toroku_bango}:{eval_row.model_version}"
    client_request_id = _client_request_id(_RS_PREFIX, business_key)

    root_attributes: dict[str, object] = {
        "race_key": race_key,
        "ketto_toroku_bango": eval_row.ketto_toroku_bango,
        "category": eval_row.category,
        "model_version": eval_row.model_version,
        "race_date": eval_row.race_date,
        TIMING_ATTR_KEY: TIMING_APPROXIMATE,
    }
    child_specs = (
        _ChildSpanSpec(
            name=SCORE_MODEL_SPAN_NAME,
            duration_ms=_NOMINAL_SCORE_MODEL_MS,
            attributes={
                "race_key": race_key,
                "ketto_toroku_bango": eval_row.ketto_toroku_bango,
                "category": eval_row.category,
                "model_version": eval_row.model_version,
                TIMING_ATTR_KEY: TIMING_APPROXIMATE,
            },
        ),
        _ChildSpanSpec(
            name=UPSERT_NEON_SPAN_NAME,
            duration_ms=_NOMINAL_UPSERT_NEON_MS,
            attributes={
                "race_key": race_key,
                "category": eval_row.category,
                TIMING_ATTR_KEY: TIMING_APPROXIMATE,
            },
        ),
    )
    trace_id = _emit_trace(
        tracing_client,
        experiment_id,
        client_request_id=client_request_id,
        business_key=business_key,
        root_span_name=RS_ROOT_SPAN_NAME,
        root_attributes=root_attributes,
        child_specs=child_specs,
        start_time=generated_at,
        tags={
            "category": eval_row.category,
            "model_version": eval_row.model_version,
            "race_date": eval_row.race_date,
            "ketto_toroku_bango": eval_row.ketto_toroku_bango,
        },
    )
    if trace_id is None:
        return None
    _log_rs_assessments(tracing_client, trace_id, eval_row)
    return trace_id


def _rs_generated_at_lookup(
    rows: Sequence[Mapping[str, object]],
) -> dict[tuple[str, str, str], datetime]:
    """Map (keibajo_code, race_bango, ketto_toroku_bango) -> that horse's
    real `prediction_generated_at`, from the RAW prediction rows an RS eval
    group was built from -- one row per horse already (unlike FP, no
    within-race aggregation is needed here)."""
    result: dict[tuple[str, str, str], datetime] = {}
    for row in rows:
        key = (
            str(row["keibajo_code"]),
            str(row["race_bango"]),
            str(row["ketto_toroku_bango"]),
        )
        result[key] = cast(datetime, row["prediction_generated_at"])
    return result


def emit_rs_horse_traces(
    tracing_client: TracingClient,
    experiment_id: str,
    eval_rows: Sequence[serve_eval.RsHorseEvalRow],
    rows: Sequence[Mapping[str, object]],
) -> TraceEmitSummary:
    """Emit one trace (+ assessment) per row in `eval_rows`, with the SAME
    per-row error isolation as `emit_fp_race_traces` (see its own
    docstring) -- one bad horse never blocks the rest of that day's traces.

    `rows` must be the SAME raw prediction-row group `eval_rows` was built
    from, used only to look up each horse's real `prediction_generated_at`
    (see `_rs_generated_at_lookup`) -- same rationale as `emit_fp_race_
    traces`'s own docstring.
    """
    summary = TraceEmitSummary()
    generated_at_by_horse = _rs_generated_at_lookup(rows)
    for eval_row in eval_rows:
        key = (eval_row.venue, eval_row.race_bango, eval_row.ketto_toroku_bango)
        generated_at = generated_at_by_horse.get(key)
        if generated_at is None:
            summary.errors.append(
                f"{key}:{eval_row.model_version}: no prediction_generated_at found for "
                "trace emission"
            )
            continue
        try:
            trace_id = emit_rs_horse_trace(
                tracing_client, experiment_id, eval_row, generated_at=generated_at
            )
        except (MlflowException, ValueError, KeyError, TypeError) as exc:
            summary.errors.append(f"{key}:{eval_row.model_version}: {exc}")
            continue
        if trace_id is None:
            summary.traces_already_existed += 1
        else:
            summary.traces_created += 1
    return summary
