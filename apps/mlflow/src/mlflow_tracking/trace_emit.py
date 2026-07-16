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
`config.EXPERIMENT_RS_PRODUCTION_USAGE` ever receive a trace from
`emit_fp_race_trace(s)`/`emit_rs_horse_trace(s)` -- by construction, not by
an enforced allow-list inside this file: every caller of THOSE SPECIFIC
functions (`sync_production.py`, `backfill_traces.py`) only ever resolves
and passes in one of those two experiment ids. ★ 2026-07-11 update: this is
no longer true of the module as a whole -- `job_trace` (see this module's
own "JOB-EXECUTION TRACES" section further down) is a SEPARATE, generic
primitive that OTHER modules (`champion_cell_eval.py`, `cell_eval_runs.py`,
`backfill_finish_position.py`, `backfill_running_style.py`, `ingest_eval.py`,
`training_run.py`, `sync_production.py`, `backfill_serve_timeline.py`) use
to emit ONE trace per real CLI/job invocation into every OTHER experiment in
this package except `internal/smoke-tests` (see that section for the full
per-experiment wiring and why smoke-tests stays untraced). The per-race/
per-horse functions above and the job-execution `job_trace` primitive below
are two genuinely different trace SHAPES answering two different questions
("what got served in production" vs. "did this job run, and how long did it
take") -- they happen to share this one file's hardened `TracingClient`-
direct plumbing (see `_make_span`/`_log_feedback`/`_new_id`/`encode_trace_id`
below), not their destination or semantics.

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
therefore UNREACHABLE CODE for `emit_fp_race_trace`/`emit_rs_horse_trace`
(and the shared `_emit_trace`/`_make_span` machinery, WHEN CALLED FROM
THOSE TWO FUNCTIONS specifically) -- a documented, deliberate absence, not
an oversight -- until some future, separate, out-of-scope change gives the
serving pipeline itself a real failure signal to plumb through here. ★
2026-07-11: this is now a DELIBERATE, NARROW exception, not a whole-module
invariant -- `JobTrace` (see "JOB-EXECUTION TRACES" below) legitimately DOES
reach `TraceState.ERROR`/`SpanStatusCode.ERROR` when the job it wraps raises,
since a job-execution trace is about a REAL, currently-happening piece of
work that genuinely can fail, unlike a per-race/per-horse row that (by the
time this module is ever called with it) already represents a successfully-
served, successfully-joined prediction. `_make_span` therefore now accepts
an optional `status` override (defaulting to OK, preserving the old
always-OK behavior for every existing caller that omits it) precisely so
`JobTrace` can pass ERROR explicitly, in the one place that legitimately
needs to.

── JOB-EXECUTION TRACES (`job_trace`, `JobTrace`) ──────────────────────────

Added 2026-07-11 to extend Usage/Quality/Tool-calls-page coverage beyond the
two `production-usage` experiments above to the OTHER 10 real experiments in
this package (every one of `config.ALL_EXPERIMENT_NAMES` except the two
`production-usage` experiments and `internal/smoke-tests`, which stays
deliberately untraced -- see below). This is a fundamentally SIMPLER shape
than the per-race/per-horse work above: a job-execution trace represents one
real CLI/job invocation happening RIGHT NOW, in-process, so its wall-clock
timing is measured for REAL (`datetime.now(UTC)` at trace/step start and
end) -- there is no "nominal/approximate" honesty caveat to disclose here,
unlike `TIMING_APPROXIMATE` above (that caveat was specific to backfilling
HISTORICAL per-race predictions whose actual serving latency was never
recorded; a job invocation's own duration, by contrast, is directly
observable as it happens).

`job_trace(tracing_client, experiment_id, job_name, *, attributes=None)`
returns a `JobTrace` context manager:

    with trace_emit.job_trace(tracing_client, experiment_id, "eval-cells") as t:
        with t.step("resolve-champion"):
            champion = resolve_champion(...)
        with t.step("collect-rows", category=category):
            rows = collect_rows(...)
        t.feedback("runs_created", float(runs_created))

- Each `t.step(name, **attributes)` is a NESTED context manager: entering it
  records a real wall-clock start, exiting it records a real wall-clock end,
  and the elapsed span is logged as one `SpanType.TOOL` child span under the
  job's root (`SpanType.TASK`) span -- mirroring how `_emit_trace`'s
  `score-model`/`upsert-neon` child spans are structured, except these
  durations are measured, never nominal constants.
- `t.feedback(name, value, *, rationale="")` queues one `Feedback`
  assessment (bool or numeric), flushed once the `with` block exits --
  AFTER the trace/spans are written via `start_trace`/`log_spans` (mirroring
  `_emit_trace`'s own start_trace-then-log_assessment ordering, so
  `log_assessment`'s foreign-key requirement is always satisfied).
- On a normal exit, the trace is `TraceState.OK` / root span `SpanStatusCode.
  OK`. On an exception propagating out of the `with` block, the trace AND
  its root span are logged as `TraceState.ERROR`/`SpanStatusCode.ERROR`
  (a `_JobStep` whose own body raised also marks THAT step's span ERROR,
  for finer-grained "which step failed" visibility) -- and the original
  exception is ALWAYS re-raised afterward; `__exit__` never returns True,
  so this context manager can never swallow a caller's exception. A
  SEPARATE, best-effort safety net wraps only the actual `TracingClient`
  write calls (`start_trace`/`log_spans`/`log_assessment`) in their own
  `except MlflowException` + `warnings.warn`: a transient tracking-store
  write failure while trying to RECORD that a job ran must never itself
  make a genuinely-successful job look like it failed (or mask the real
  exception from a genuinely-failed one) -- observability must never
  become a new failure mode for the pipeline it observes.
- `t.discard()` marks the trace to be silently dropped (nothing written at
  all) IF AND ONLY IF the `with` block exits normally -- used by callers
  that only want to emit a trace CONDITIONALLY on some real outcome (e.g.
  the shared `timelines` experiment: several call sites open a job_trace
  that spans their WHOLE invocation but only actually appended a timeline
  point some of the time, see `sync_production.py`/`backfill_serve_
  timeline.py`'s own wiring) rather than emitting an empty, uninformative
  trace on every single call. `discard()` has NO effect when the block
  raises: a real failure is never silently dropped, regardless of any
  earlier `discard()` call -- that would defeat the entire point of this
  class.

★ No idempotency/dedup check here, DELIBERATELY, unlike `_emit_trace`'s
`client_request_id` existence check above -- and this is not an oversight
to "fix" later. Job-execution traces are correctly ONE-PER-INVOCATION by
design: every real run of a wired CLI command SHOULD create a brand-new
trace, because there is no stable "business key" a second call could ever
collide with -- there IS no second call representing the same event; a
second real invocation of `eval-cells` (say) is a second, independent,
genuinely-new piece of work, and deserves its own trace exactly like a
second real HTTP request to a traced web service would. A future
maintainer must NOT add a `search_traces` pre-check "to match `_emit_trace`'s
idempotency style" here -- that would incorrectly suppress traces for real,
distinct job runs.

★ Historical honesty: there is no record of past job invocations to
reconstruct (unlike the per-race work above, which had real historical
`prediction_generated_at` timestamps to backfill against via
`backfill_traces.py`). Job-execution traces simply start accumulating from
the first real invocation after this code ships -- there is no
`backfill_job_traces.py` and none is planned; a store queried before any
wired command has run since this shipped will correctly show empty Usage/
Quality/Tool-calls tabs for these experiments until the first real run.

★ `internal/smoke-tests` is DELIBERATELY left with no `job_trace` wiring at
all: it is a manual, one-shot smoke-test destination (see this package's
README "Smoke-test runs" section), not a real recurring job -- empty
Usage/Quality/Tool-calls tabs there are by design, not a gap to fill.

★ INVESTIGATED 2026-07-11, VERDICT: NOT A BUG, DO-NOT-RELITIGATE WITHOUT NEW
EVIDENCE -- the MLflow UI's "Tool calls" tab was reported showing "0 Total
Tool Calls" for `job_trace`-produced experiments (e.g. `finish-position/
champion-eval`) while rendering correctly (nonzero) for the per-race/
per-horse `production-usage` experiments, despite both paths writing
genuine `SpanType.TOOL` child spans through this same file's shared
`_make_span` helper. Two independent methods found NO code-level emission
difference between the two paths, and both currently show correct,
matching data:

  1. Raw-SQL structural diff of the stored `spans` table (bypassing any
     entity-reconstruction normalization) for representative traces from
     both waves: `name`/`type`/`status` columns are byte-for-byte identical
     in shape (`type='TOOL'`, `status='OK'`, plain-text `name`) for both a
     job_trace step span (e.g. `resolve-champion`) and a per-race child
     span (e.g. `score-model`). No `None`/NULL dimension value anywhere
     that could trip the frontend's "drop the whole data point if any
     dimension is None" logic (`sql_trace_metrics_utils.
     convert_results_to_metric_data_points`, read directly from the
     installed `mlflow` package).
  2. The exact frontend query was reverse-engineered from the INSTALLED,
     BUILT mlflow==3.14.0 JS bundle (`apps/mlflow-ui`'s full `mlflow`
     install ships `server/js/build/static/js/*`; this package's own
     `mlflow-skinny` does not) -- the "Tool calls" tab's KPI tiles POST
     `{view_type: "SPANS", metric_name: "span_count", filters: ["span.type
     = \"TOOL\""], dimensions: ["span_name", "span_status"], start_time_ms,
     end_time_ms}` to `/ajax-api/3.0/mlflow/traces/metrics`, bounded by the
     Overview page's selected time range (default "Last 7 days"). This
     EXACT request was then captured live -- chrome-devtools MCP was dead
     for this investigation, so a headless Chrome instance was driven
     directly over its own remote-debugging (CDP) protocol instead --
     navigating to the real running UI's Tool-calls tab for
     `finish-position/champion-eval` (experiment_id 9): the live server
     returned correct nonzero `span_count` data points, and the rendered
     page showed "8 Total Tool Calls / 100.00% Success Rate", not 0. The
     same capture against `running-style/production-usage` (experiment_id
     8) showed "2.83K Total Tool Calls", and against a minimal, isolated
     single-trace repro logged directly into `internal/smoke-tests`
     (`repro-job-trace-debug`, 2 `TOOL` spans -- left over from an earlier
     stage of this same investigation) showed "2 Total Tool Calls / 100.00%
     Success Rate". All three are internally consistent and correct.

The most likely explanation for the ORIGINAL "0 Total Tool Calls"
observation is simply the already-documented, deliberate absence of
backfill above (see "★ Historical honesty"): `job_trace` shipped in this
same commit with NO historical backfill, so any experiment's Tool-calls tab
legitimately shows empty/zero until the first real wired-CLI invocation
actually lands a trace with `TOOL` spans -- a store queried in that narrow
window before-first-run shows exactly what was reported, and self-resolves
the moment that first real job runs. By the time of this follow-up
investigation, every one of the 10 `job_trace`-wired experiments already
had at least one real invocation logged, and every Tool-calls tab checked
rendered correctly. No code change was made to this file as a result of
this investigation -- there was nothing to fix.

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

── ASSESSMENT-COMPLETENESS HEALING (`heal_fp_race_traces`/`heal_rs_horse_traces`) ──

Added 2026-07-11 after a real production audit found two genuinely-broken
traces: one RS trace with ZERO assessments logged at all, and one FP trace
missing `top3_box_score` despite that assessment being UNCONDITIONAL --
always attached, per `_log_fp_assessments`'s own contract -- for every trace
this module ever creates. Root cause: `emit_fp_race_trace`/
`emit_rs_horse_trace` call `_emit_trace` (writes the trace row + spans,
synchronously, via `start_trace`/`log_spans`) and then, in a SEPARATE step,
call `_log_fp_assessments`/`_log_rs_assessments` (one or more further
`log_assessment` store writes). If a transient store error strikes ANYWHERE
between the first `log_assessment` call succeeding and a LATER one failing
(or the very first one, for RS's single-assessment case), the trace and its
spans already exist -- `_emit_trace` already returned a real `trace_id` --
but the assessment set is left partially (or, for RS, entirely) unwritten,
and nothing here retries. Worse: the NEXT time this exact business key is
processed (the next day's cron, or a backfill re-run), `_find_existing_trace_
id` finds the already-created trace and `emit_fp_race_trace`/
`emit_rs_horse_trace` return `None` immediately -- the documented, INTENDED
idempotent-no-op fast path for "this race already has a trace" -- so the
missing assessment(s) are never retried by the normal emit path, ever again.

`fp_expected_assessment_names(eval_row)`/`rs_expected_assessment_names()`
are the single source of truth for what a FULLY-WRITTEN trace's assessment
set looks like for a given eval row -- derived with the EXACT SAME rules
`_log_fp_assessments`/`_log_rs_assessments` themselves use to decide what to
log (the small-field exclusion: a rank whose `place{n}_hit` is None --
`total_starters < n`, see `serve_eval._compute_race_hits`'s own docstring --
is correctly ABSENT from the expected set, never counted as "missing";
`top3_box_score` is unconditionally expected, always). `fp_missing_
assessment_names`/`rs_missing_assessment_names` subtract a trace's ACTUAL
logged names (read straight off `TraceInfo.assessments`, which `search_
traces` eagerly loads via `selectinload(SqlTraceInfo.assessments)` --
confirmed by reading `sqlalchemy_store.py` in this project's own `.venv` --
so this costs no extra query beyond the one `search_traces` call already
needed to find the trace) from that expected set.

`heal_fp_race_traces(tracing_client, experiment_id, eval_rows)`/`heal_rs_
horse_traces(...)` are the public, batch, per-row-isolated entry points
(mirroring `emit_fp_race_traces`/`emit_rs_horse_traces`'s own isolation
philosophy exactly): for EVERY row given -- a freshly-created trace from
THIS SAME call, a trace from a previous call, or (the whole point) a trace
left incomplete by a previous PARTIAL write -- look up its existing trace by
the same deterministic `client_request_id` `emit_fp_race_trace`/
`emit_rs_horse_trace` would compute, and top up ONLY the names still
missing. A trace already fully complete (the overwhelming majority, on any
call after the first) costs one `search_traces` round-trip and ZERO writes,
exactly like `_find_existing_trace_id`'s own existence check. A trace not
found at all (this row's OWN trace creation itself failed elsewhere, already
recorded as its own error) is silently skipped -- healing an assessment onto
a trace that was never created would be nonsensical, and is not this
function's job to diagnose. `_log_fp_assessments` grew an `only_names`
parameter (`None`, the default, preserves every EXISTING caller's "log
everything expected" behavior unchanged) precisely so the FP healing path
can reuse the exact same value/rationale-construction logic as fresh
emission, rather than a second, driftable copy of it -- FP's expected set can
have up to 7 members (6 ranks + `top3_box_score`), so a "missing" subset can
genuinely be partial. `_log_rs_assessments` grew NO such parameter: RS's
expected set (`rs_expected_assessment_names()`) is always a fixed singleton,
so a "missing" set is either empty (nothing to heal) or exactly that one
name -- logging "everything" and logging "only what's missing" are the same
call for RS, so an `only_names` gate there would be dead code (see that
function's own docstring).

★ Never re-logs (duplicates) an assessment name already present: this is a
DEFENSIVE choice, not merely an optimization -- `TracingClient.log_assessment`
unconditionally calls `store.create_assessment(...)` (confirmed by reading
`mlflow/tracing/client.py` in this project's own `.venv`), which always
inserts a brand-new assessment row with a fresh id; the store enforces no
uniqueness constraint on (trace_id, name) at all, so calling `log_assessment`
for an already-present name would silently create a SECOND assessment with
the same name on the same trace, not update or reject the first. The
`only_names`-gated re-log inside `_log_fp_assessments` (FP) and the
`if not missing: continue` guard inside `heal_rs_horse_traces` (RS) -- both
built on the `expected - existing` subtraction `fp_missing_assessment_names`/
`rs_missing_assessment_names` provide -- together guarantee this can never
happen from this module's own call sites.

Invoked exclusively from `backfill_traces.py` (see that module's own
docstring), NOT from `sync_production.py`'s daily incremental path -- the
daily path's own `emit_fp_race_trace(s)`/`emit_rs_horse_trace(s)` calls, and
their "zero writes when a trace already exists" contract, are UNCHANGED
(still exactly what `test_trace_emit.py`'s existing idempotency tests
assert). `backfill_traces.py` already re-derives `eval_rows` for EVERY
already-evaluated run on EVERY call (see its own docstring's "Idempotent by
construction, NOT by a run-level skip-cache" section) -- the natural,
already-scheduled place for a periodic completeness sweep, without adding a
second `search_traces` round-trip to the much hotter, much higher-volume
daily sync path for a defect that (by construction: a transient store error
landing in the narrow window between two specific writes) is rare.
"""

from __future__ import annotations

import hashlib
import random
import warnings
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final, Literal, cast

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

# ── job_trace (JOB-EXECUTION TRACES) constants -- see module docstring ─────

# Distinct from TIMING_APPROXIMATE above: a job_trace's timing is a REAL,
# directly-measured wall-clock duration, never a nominal constant, so it
# gets its own, differently-named attribute value rather than reusing
# TIMING_APPROXIMATE under the same TIMING_ATTR_KEY (which would falsely
# imply the same "not really measured" caveat applies here too).
TIMING_REAL: Final[str] = "real"

# Distinct AssessmentSource.source_id from ASSESSMENT_SOURCE_ID
# ("sync_production", used only by the per-race/per-horse Feedback above) --
# job_trace is called from many different modules (champion_cell_eval.py,
# cell_eval_runs.py, backfill_finish_position.py, backfill_running_style.py,
# ingest_eval.py, training_run.py, sync_production.py,
# backfill_serve_timeline.py), so "sync_production" would misattribute every
# OTHER caller's Feedback to a module that did not actually log it.
JOB_TRACE_SOURCE_ID: Final[str] = "job_trace"

# Tags every job_trace-produced TraceInfo with `trace_kind="job"` -- lets a
# future UI/search distinguish a job-execution trace from a per-race/
# per-horse one without depending on which experiment it happens to live in.
JOB_TRACE_KIND_TAG: Final[str] = "trace_kind"
JOB_TRACE_KIND_VALUE: Final[str] = "job"


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


@dataclass
class TraceHealSummary:
    """Outcome counters for one `heal_fp_race_traces`/`heal_rs_horse_traces`
    call -- see this module's own "ASSESSMENT-COMPLETENESS HEALING" docstring
    section.

    `traces_checked` counts every row this call examined, regardless of
    outcome. `traces_healed` counts traces that were missing at least one
    EXPECTED assessment and got the missing one(s) topped up this call;
    `assessments_added` is the total count of individual Feedback assessments
    newly logged across every healed trace THIS call (`>= traces_healed`,
    since one trace can be missing more than one assessment at once -- e.g.
    the real RS trace this feature was built to fix, which had ZERO
    assessments logged at all). A trace already fully complete is silently
    left alone -- NOT counted as healed, and never re-logged -- the normal,
    expected outcome for the overwhelming majority of traces on any call
    after the underlying defect (see this module's own docstring) is fixed.
    `errors` isolates one race/horse's lookup-or-log failure from every other
    one in the same call, mirroring `TraceEmitSummary`'s own philosophy.
    """

    traces_checked: int = 0
    traces_healed: int = 0
    assessments_added: int = 0
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


def _find_existing_trace_assessment_names(
    tracing_client: TracingClient, experiment_id: str, client_request_id: str
) -> tuple[str, frozenset[str]] | None:
    """Return `(trace_id, existing_assessment_names)` for the trace already
    carrying `client_request_id` in `experiment_id`, or None if no such trace
    exists yet -- the lookup `heal_fp_race_traces`/`heal_rs_horse_traces` use
    (see this module's own "ASSESSMENT-COMPLETENESS HEALING" docstring
    section).

    Same `search_traces` call `_find_existing_trace_id` already makes
    (`include_spans=False`, one round-trip), just also reading
    `TraceInfo.assessments` off the same response instead of discarding it --
    `search_traces` eagerly loads assessments regardless of `include_spans`
    (`selectinload(SqlTraceInfo.assessments)`, confirmed by reading
    `sqlalchemy_store.py` in this project's own `.venv`), so this is never an
    extra query beyond the one an existence check already costs.
    """
    matches = tracing_client.search_traces(
        experiment_ids=[experiment_id],
        filter_string=f"attribute.client_request_id = '{client_request_id}'",
        max_results=1,
        include_spans=False,
    )
    if not matches:
        return None
    match = matches[0]
    return match.info.trace_id, frozenset(a.name for a in match.info.assessments)


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
    status: SpanStatus | None = None,
) -> Span:
    """Build one `Span` via a raw `opentelemetry.sdk.trace.ReadableSpan` --
    mirrors `tool_calls_proto.py`'s own `_make_span` helper exactly (the
    proven reference implementation this module builds on).

    `status` defaults to OK when omitted -- every EXISTING caller
    (`_emit_trace`, used by `emit_fp_race_trace`/`emit_rs_horse_trace`) omits
    it and keeps its previous always-OK behavior unchanged; see this
    module's own docstring for why `ERROR` is unreachable for those two
    functions specifically. `JobTrace` (see the "JOB-EXECUTION TRACES"
    section of this module's docstring) is the one deliberate exception that
    passes an explicit ERROR `SpanStatus` here, since a job's root/step spans
    genuinely can fail.
    """
    encoded_attributes = {
        SpanAttributeKey.REQUEST_ID: dump_span_attribute_value(trace_id_str),
        SpanAttributeKey.SPAN_TYPE: dump_span_attribute_value(span_type),
        **{key: dump_span_attribute_value(value) for key, value in attributes.items()},
    }
    resolved_status = status if status is not None else SpanStatus(status_code=SpanStatusCode.OK)
    otel_span = OTelReadableSpan(
        name=name,
        context=build_otel_context(trace_id_int, span_id_int),
        parent=build_otel_context(trace_id_int, parent_id_int) if parent_id_int else None,
        start_time=start_ns,
        end_time=end_ns,
        attributes=encoded_attributes,
        status=resolved_status.to_otel_status(),
        resource=OTelResource.get_empty(),
    )
    return Span(otel_span)


def _log_feedback(
    tracing_client: TracingClient,
    trace_id: str,
    *,
    name: str,
    value: bool | float,
    rationale: str,
    source_id: str = ASSESSMENT_SOURCE_ID,
) -> None:
    """Log one `Feedback` assessment onto `trace_id` via
    `TracingClient.log_assessment` -- a direct, synchronous store write (see
    this module's own docstring for why this sidesteps the async-export
    race the fluent `mlflow.log_feedback` path has).

    `source_id` defaults to `ASSESSMENT_SOURCE_ID` ("sync_production"),
    preserving every existing caller's behavior unchanged (`_log_fp_
    assessments`/`_log_rs_assessments`, both genuinely called only from
    `sync_production.py`'s own call graph). `JobTrace.feedback` (see the
    "JOB-EXECUTION TRACES" section of this module's docstring) passes
    `JOB_TRACE_SOURCE_ID` instead, since it is called from many OTHER
    modules that `sync_production` would misattribute.
    """
    tracing_client.log_assessment(
        trace_id,
        Feedback(
            name=name,
            value=value,
            source=AssessmentSource(source_type=AssessmentSourceType.CODE, source_id=source_id),
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


def _fp_rank_hits(eval_row: serve_eval.FpRaceEvalRow) -> tuple[tuple[int, int | None], ...]:
    """`(rank, hit)` pairs for ranks 1..6 -- the single shared source both
    `_log_fp_assessments` (what to LOG) and `fp_expected_assessment_names`
    (what SHOULD be logged) derive from, so the two can never drift apart."""
    return (
        (1, eval_row.top1_hit),
        (2, eval_row.place2_hit),
        (3, eval_row.place3_hit),
        (4, eval_row.place4_hit),
        (5, eval_row.place5_hit),
        (6, eval_row.place6_hit),
    )


def fp_expected_assessment_names(eval_row: serve_eval.FpRaceEvalRow) -> frozenset[str]:
    """The exact set of assessment names a FULLY-WRITTEN FP trace for
    `eval_row` carries: `place{n}_hit` for every rank whose `_fp_rank_hits`
    value is not None, plus the unconditional `TOP3_BOX_ASSESSMENT_NAME`.

    Mirrors `_log_fp_assessments`'s own gating exactly (a rank whose
    `place{n}_hit` is None -- this race's `total_starters < n`, see
    `serve_eval._compute_race_hits`'s small-field-exclusion docstring -- is
    correctly ABSENT here too, never counted as "missing" by a completeness
    check downstream). See this module's own "ASSESSMENT-COMPLETENESS
    HEALING" docstring section for why this single source of truth exists.
    """
    names = {f"place{rank}_hit" for rank, hit in _fp_rank_hits(eval_row) if hit is not None}
    names.add(TOP3_BOX_ASSESSMENT_NAME)
    return frozenset(names)


def fp_missing_assessment_names(
    eval_row: serve_eval.FpRaceEvalRow, existing_names: Collection[str]
) -> frozenset[str]:
    """`fp_expected_assessment_names(eval_row) - existing_names` -- the
    assessment names a healing pass must top up for this trace. Empty when
    `existing_names` already covers everything expected (the trace is
    already complete -- the common case on any call after the first)."""
    return fp_expected_assessment_names(eval_row) - set(existing_names)


def _log_fp_assessments(
    tracing_client: TracingClient,
    trace_id: str,
    eval_row: serve_eval.FpRaceEvalRow,
    *,
    only_names: frozenset[str] | None = None,
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

    `only_names`, when given, restricts this call to logging ONLY the
    assessment names it contains -- used by `heal_fp_race_traces`'s
    completeness top-up (see this module's own "ASSESSMENT-COMPLETENESS
    HEALING" docstring section) so it never re-logs (duplicates) a name
    already present on the trace. `None` (every EXISTING caller --
    `emit_fp_race_trace`'s fresh-emission path) means "log every expected
    name", the original, unchanged behavior.
    """
    for rank, hit in _fp_rank_hits(eval_row):
        if hit is None:
            continue
        name = f"place{rank}_hit"
        if only_names is not None and name not in only_names:
            continue
        _log_feedback(
            tracing_client,
            trace_id,
            name=name,
            value=bool(hit),
            rationale=(
                f"place{rank}_hit: did the model's top pick finish within the top {rank} "
                "(serve_eval.FpRaceEvalRow)?"
            ),
        )
    if only_names is not None and TOP3_BOX_ASSESSMENT_NAME not in only_names:
        return
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


def heal_fp_race_traces(
    tracing_client: TracingClient,
    experiment_id: str,
    eval_rows: Sequence[serve_eval.FpRaceEvalRow],
) -> TraceHealSummary:
    """For every row in `eval_rows`, look up its existing FP trace (by the
    SAME deterministic `client_request_id` `emit_fp_race_trace` computes) and
    top up any EXPECTED-but-missing assessment -- see this module's own
    "ASSESSMENT-COMPLETENESS HEALING" docstring section for the full defect
    this fixes and the design rationale.

    Per-row isolated exactly like `emit_fp_race_traces` (one bad race's
    lookup/log failure is recorded in `errors` and skipped, never aborting
    the rest of the batch). A race with no trace found at all is silently
    skipped -- that race's OWN trace-creation failure is a DIFFERENT problem,
    already surfaced elsewhere (`emit_fp_race_traces`'s own `errors`), not
    this function's job to diagnose or re-attempt.
    """
    summary = TraceHealSummary()
    for eval_row in eval_rows:
        summary.traces_checked += 1
        race_key = fp_race_key(eval_row)
        client_request_id = _client_request_id(_FP_PREFIX, f"{race_key}:{eval_row.model_version}")
        try:
            found = _find_existing_trace_assessment_names(
                tracing_client, experiment_id, client_request_id
            )
            if found is None:
                continue
            trace_id, existing_names = found
            missing = fp_missing_assessment_names(eval_row, existing_names)
            if not missing:
                continue
            _log_fp_assessments(tracing_client, trace_id, eval_row, only_names=missing)
        except (MlflowException, ValueError, KeyError, TypeError) as exc:
            summary.errors.append(f"{race_key}:{eval_row.model_version}: {exc}")
            continue
        summary.traces_healed += 1
        summary.assessments_added += len(missing)
    return summary


# ── RS: one trace per horse ──────────────────────────────────────────────────


def rs_race_key(eval_row: serve_eval.RsHorseEvalRow) -> str:
    """`"{race_date}:{category}:{venue}:{race_bango}"` -- same shape as
    `fp_race_key`, RS's race-grain identity (the horse-grain identity is
    this plus `ketto_toroku_bango`, see `emit_rs_horse_trace`)."""
    return f"{eval_row.race_date}:{eval_row.category}:{eval_row.venue}:{eval_row.race_bango}"


def rs_expected_assessment_names() -> frozenset[str]:
    """The exact set of assessment names a FULLY-WRITTEN RS trace carries --
    always exactly `{RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME}`, unconditionally
    (RS has no small-field-style exclusion at all, unlike FP's rank 4/5/6 --
    see this module's own RS design-decision docstring). Takes no `eval_row`
    argument (unlike its FP twin, `fp_expected_assessment_names`) precisely
    because the expected set never depends on anything about the row."""
    return frozenset({RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME})


def rs_missing_assessment_names(existing_names: Collection[str]) -> frozenset[str]:
    """`rs_expected_assessment_names() - existing_names` -- empty unless the
    trace is missing its one expected assessment entirely (the real defect
    this module's "ASSESSMENT-COMPLETENESS HEALING" section fixes: a genuine
    production RS trace was found with ZERO assessments logged at all)."""
    return rs_expected_assessment_names() - set(existing_names)


def _log_rs_assessments(
    tracing_client: TracingClient, trace_id: str, eval_row: serve_eval.RsHorseEvalRow
) -> None:
    """Log the single `predicted_class_hit` Feedback assessment -- RS's
    parallel to FP's rank-hit assessments (see module docstring's RS design
    rationale): `RsHorseEvalRow.hit` is ALREADY the exact 0/1 "did the
    predicted class match the actual class" boolean, computed once in
    `serve_eval.build_rs_horse_eval_rows` -- this is a direct, verbatim use
    of that field, not a re-derivation.

    No `only_names` parameter (unlike its FP twin, `_log_fp_assessments`):
    `rs_expected_assessment_names()` is always a fixed singleton, so a
    "missing" set for RS (see `rs_missing_assessment_names`) can only ever be
    empty (nothing to heal, this function is never called) or exactly that
    one name (heal by logging it, identical to a fresh emission) -- an
    `only_names`-gated partial-skip branch would be dead code, unreachable by
    any real caller, for RS specifically.
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


def heal_rs_horse_traces(
    tracing_client: TracingClient,
    experiment_id: str,
    eval_rows: Sequence[serve_eval.RsHorseEvalRow],
) -> TraceHealSummary:
    """RS twin of `heal_fp_race_traces` -- see its own docstring and this
    module's "ASSESSMENT-COMPLETENESS HEALING" section for the full defect
    and design rationale (RS's own real-world instance: a trace found with
    ZERO assessments logged at all)."""
    summary = TraceHealSummary()
    for eval_row in eval_rows:
        summary.traces_checked += 1
        race_key = rs_race_key(eval_row)
        business_key = f"{race_key}:{eval_row.ketto_toroku_bango}:{eval_row.model_version}"
        client_request_id = _client_request_id(_RS_PREFIX, business_key)
        try:
            found = _find_existing_trace_assessment_names(
                tracing_client, experiment_id, client_request_id
            )
            if found is None:
                continue
            trace_id, existing_names = found
            missing = rs_missing_assessment_names(existing_names)
            if not missing:
                continue
            # `missing` is necessarily rs_expected_assessment_names() in
            # full here (a fixed singleton -- see `_log_rs_assessments`'s
            # own docstring for why it takes no `only_names` parameter),
            # so logging "everything" is identical to logging "only what's
            # missing" for RS specifically.
            _log_rs_assessments(tracing_client, trace_id, eval_row)
        except (MlflowException, ValueError, KeyError, TypeError) as exc:
            summary.errors.append(
                f"{race_key}:{eval_row.ketto_toroku_bango}:{eval_row.model_version}: {exc}"
            )
            continue
        summary.traces_healed += 1
        summary.assessments_added += len(missing)
    return summary


# ── JOB-EXECUTION TRACES: one trace per CLI/job invocation ──────────────────
#
# See this module's own docstring ("JOB-EXECUTION TRACES" section) for the
# full rationale/contrast with the FP/RS per-race/per-horse functions above.


class _JobStep:
    """Context manager returned by `JobTrace.step(name, **attributes)` --
    construct only via that method, never directly.

    Records REAL wall-clock start/end (`datetime.now(UTC)` at `__enter__`/
    `__exit__`) as one `SpanType.TOOL` child span under the enclosing job's
    root span. Never swallows an exception raised inside the `with` block --
    `__exit__` always returns False -- it only ADDITIONALLY marks this one
    step's own span `SpanStatusCode.ERROR` when that happens (for
    finer-grained "which step failed" visibility in the trace UI); the
    exception itself still propagates to, and is handled by, the enclosing
    `JobTrace`.
    """

    def __init__(self, job_trace_: JobTrace, name: str, attributes: Mapping[str, object]) -> None:
        self._job_trace: JobTrace = job_trace_
        self._name: str = name
        self._attributes: dict[str, object] = dict(attributes)
        self._start: datetime | None = None

    def __enter__(self) -> _JobStep:
        self._start = datetime.now(UTC)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> Literal[False]:
        end = datetime.now(UTC)
        start = self._start if self._start is not None else end
        status = (
            SpanStatus(status_code=SpanStatusCode.ERROR, description=str(exc)[:500])
            if exc_type is not None
            else None
        )
        self._job_trace.record_step(self._name, start, end, self._attributes, status)
        return False


class JobTrace:
    """Context manager: one MLflow trace per real CLI/job invocation.

    Construct via `job_trace(...)` below, never directly. See this module's
    own docstring ("JOB-EXECUTION TRACES") for the full shape/rationale,
    including why there is deliberately NO idempotency/dedup check here (a
    job-execution trace is correctly ONE-PER-INVOCATION by design) and why
    `TraceState.ERROR`/`SpanStatusCode.ERROR` -- unreachable for the FP/RS
    per-race/per-horse functions above -- are legitimately reachable here.
    """

    def __init__(
        self,
        tracing_client: TracingClient,
        experiment_id: str,
        job_name: str,
        *,
        attributes: Mapping[str, object] | None = None,
    ) -> None:
        self._tracing_client: TracingClient = tracing_client
        self._experiment_id: str = experiment_id
        self._job_name: str = job_name
        self._root_attributes: dict[str, object] = dict(attributes) if attributes else {}
        self._trace_id_int: int = _new_id(128)
        self._trace_id_str: str = f"tr-{encode_trace_id(self._trace_id_int)}"
        self._root_span_id_int: int = _new_id(64)
        self._child_spans: list[Span] = []
        self._pending_feedback: list[tuple[str, bool | float, str]] = []
        self._start: datetime | None = None
        self._discarded: bool = False

    @property
    def trace_id(self) -> str:
        """The trace_id this job's trace will be (or was) logged under --
        exposed mainly for tests/callers that want to cross-reference the
        trace after the `with` block closes."""
        return self._trace_id_str

    def __enter__(self) -> JobTrace:
        self._start = datetime.now(UTC)
        return self

    def step(self, name: str, **attributes: object) -> _JobStep:
        """Return a nested context manager for one named step inside this
        job's `with` block, e.g. `with t.step("resolve-champion"): ...`.
        Each step becomes its own `SpanType.TOOL` child span with REAL
        (measured, not nominal) start/end timing -- see `_JobStep`.
        `**attributes` (if given) become that child span's own attributes,
        e.g. `t.step("eval-join", category=category)`.
        """
        return _JobStep(self, name, attributes)

    def record_step(
        self,
        name: str,
        start: datetime,
        end: datetime,
        attributes: Mapping[str, object],
        status: SpanStatus | None,
    ) -> None:
        """Record one already-completed step as a child span (called by
        `_JobStep.__exit__` -- NOT part of the `step`/`feedback`/`discard`
        public contract callers are meant to use directly, but deliberately
        NOT underscore-prefixed either: `_JobStep` lives in this same module
        but is a DIFFERENT class, and a leading underscore here would make
        that legitimate same-module cross-class call a `reportPrivateUsage`
        violation under this package's strict basedpyright config).
        """
        start_ns = int(start.timestamp() * 1_000_000_000)
        end_ns = int(end.timestamp() * 1_000_000_000)
        self._child_spans.append(
            _make_span(
                trace_id_int=self._trace_id_int,
                trace_id_str=self._trace_id_str,
                span_id_int=_new_id(64),
                parent_id_int=self._root_span_id_int,
                name=name,
                span_type=SpanType.TOOL,
                start_ns=start_ns,
                end_ns=end_ns,
                attributes=attributes,
                status=status,
            )
        )

    def feedback(self, name: str, value: bool | float, *, rationale: str = "") -> None:
        """Queue one `Feedback` assessment (bool or numeric), flushed once
        this job's `with` block exits -- see this class's own docstring for
        why queuing-then-flushing-at-`__exit__` (rather than logging
        immediately) is safe: the trace row is always written via
        `start_trace`/`log_spans` FIRST, so `log_assessment`'s foreign-key
        requirement is always already satisfied by the time queued feedback
        is flushed."""
        self._pending_feedback.append((name, value, rationale))

    def discard(self) -> None:
        """Mark this job trace to be silently dropped (nothing written at
        all) if the `with` block exits NORMALLY -- see this module's own
        docstring for why some callers need this (a `timelines` sub-trace is
        only meaningful when it actually appended a point this invocation).
        Has NO effect when the block raises: a real failure is never
        silently dropped by `discard()`, regardless of when/whether it was
        called."""
        self._discarded = True

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> Literal[False]:
        end = datetime.now(UTC)
        failed = exc_type is not None
        if self._discarded and not failed:
            return False

        start = self._start if self._start is not None else end
        start_ns = int(start.timestamp() * 1_000_000_000)
        end_ns = int(end.timestamp() * 1_000_000_000)

        trace_state = TraceState.ERROR if failed else TraceState.OK
        root_status = (
            SpanStatus(status_code=SpanStatusCode.ERROR, description=str(exc)[:500])
            if failed
            else None
        )
        root_attributes = {**self._root_attributes, TIMING_ATTR_KEY: TIMING_REAL}
        root_span = _make_span(
            trace_id_int=self._trace_id_int,
            trace_id_str=self._trace_id_str,
            span_id_int=self._root_span_id_int,
            parent_id_int=None,
            name=self._job_name,
            span_type=SpanType.TASK,
            start_ns=start_ns,
            end_ns=end_ns,
            attributes=root_attributes,
            status=root_status,
        )
        trace_info = TraceInfo(
            trace_id=self._trace_id_str,
            trace_location=TraceLocation.from_experiment_id(self._experiment_id),
            request_time=start_ns // _NS_PER_MS,
            state=trace_state,
            execution_duration=(end_ns - start_ns) // _NS_PER_MS,
            tags={
                TraceTagKey.TRACE_NAME: self._job_name,
                JOB_TRACE_KIND_TAG: JOB_TRACE_KIND_VALUE,
            },
            trace_metadata={},
        )
        try:
            self._tracing_client.start_trace(trace_info)
            self._tracing_client.log_spans(self._experiment_id, [root_span, *self._child_spans])
            for name, value, rationale in self._pending_feedback:
                _log_feedback(
                    self._tracing_client,
                    self._trace_id_str,
                    name=name,
                    value=value,
                    rationale=rationale or f"{name} (job_trace: {self._job_name})",
                    source_id=JOB_TRACE_SOURCE_ID,
                )
        except MlflowException as trace_exc:
            # Observability must never become a NEW failure mode for the
            # pipeline it observes -- a transient tracking-store write
            # failure while trying to RECORD that a job ran must never mask
            # (nor be mistaken for) the job's own real outcome. See this
            # module's own docstring for the full rationale.
            warnings.warn(
                f"job_trace: failed to persist job-execution trace for "
                f"{self._job_name!r} (experiment_id={self._experiment_id!r}): {trace_exc}",
                stacklevel=2,
            )
        return False  # NEVER suppress the wrapped block's own exception (if any).


def job_trace(
    tracing_client: TracingClient,
    experiment_id: str,
    job_name: str,
    *,
    attributes: Mapping[str, object] | None = None,
) -> JobTrace:
    """Return a NEW `JobTrace` context manager for one job/CLI-invocation
    execution into `experiment_id`, named `job_name` (becomes the root
    span's name, `SpanType.TASK`, and the `TraceTagKey.TRACE_NAME` tag).

    Takes an already-resolved `experiment_id` (not an experiment NAME)
    -- consistent with every other function in this module
    (`emit_fp_race_trace(s)`/`emit_rs_horse_trace(s)` all take `experiment_id`
    too) -- since resolving a name to an id requires an `MlflowClient` (via
    `logging_api.get_or_create_experiment`), not just the `TracingClient`
    this whole module is otherwise built around, and every real call site
    already has the resolved id in hand from its own `get_or_create_
    experiment` call before it ever needs a `JobTrace`.

    `attributes` (if given) become the root span's own attributes (e.g.
    `{"category": category, "task": task}`), visible on the Tool-calls page.

    Usage (see this module's own docstring for the full contract):

        tracing_client = trace_emit.build_tracing_client(client)
        with trace_emit.job_trace(tracing_client, experiment_id, "eval-cells") as t:
            with t.step("resolve-champion"):
                ...
            t.feedback("runs_created", float(runs_created))
    """
    return JobTrace(tracing_client, experiment_id, job_name, attributes=attributes)
