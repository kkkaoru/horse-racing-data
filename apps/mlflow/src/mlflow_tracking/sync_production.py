"""Sync genuinely-served production predictions into MLflow, and evaluate them
against finalized race results once those results exist.

Two production tables in the racing Neon database
(`race_finish_position_model_predictions` / `race_running_style_model_predictions`)
accumulate rows written every day by the live finish-position and
running-style serving paths, for jra/nar/banei. Those rows are the ONLY
record of what was actually served in production -- everything else in this
package (backfill_finish_position.py, backfill_running_style.py,
ingest_eval.py) reconstructs history from offline walk-forward artifacts or
serve_accuracy_report.py snapshots, neither of which is "this exact row was
shown to a bettor on this exact day". `serve_eval.py` already implements the
query/join/aggregate logic this module needs (see its own module docstring
for the FP-is-per-race / RS-is-per-horse distinction, and
`serve_eval.GEN_LAG_TOLERANCE_DAYS` for how "genuinely served" is
distinguished from decades-old offline backfill rows that happen to share
the same table). This module's job is orchestration on top of that library:
walk a date range, group by model_version, log one MLflow run per
(date, category, model_version) recording that usage, and -- once finalized
results exist locally -- join and log evaluation metrics that also feed the
`timelines` experiment (see timeline.py) so its charts gain dense, real data
points instead of the sparse manual-backfill-only points they had before.

Idempotency (the trickiest part of this module) is built around two boolean-
string tags on each run, `sync_base_logged` / `sync_eval_logged` -- see
`sync_production_range`'s own docstring for the full state-machine
explanation of why a "re-run the last few days every day" cron caller is
both cheap (nothing already-logged is ever re-logged) and eventually
correct (a prediction published a few days ahead of race day gets its
evaluation filled in on a LATER call, once results become final, rather
than being stuck with no eval forever).

★ MLflow traces (2026-07-10, supersedes the earlier "traces are not used"
decision -- see `trace_emit.py`'s own module docstring for the full
before/after story): once the eval join for a (date, category,
model_version) group succeeds (the SAME pass that computes place-hits, see
`_sync_fp_eval`/`_sync_rs_eval` below), this module ALSO emits one MLflow
trace per race (finish-position) / per horse (running-style) via
`trace_emit.emit_fp_race_traces`/`trace_emit.emit_rs_horse_traces`, with
Feedback assessments attached in the same call. This is gated by the
`emit_traces` parameter below (default True; the `sync-production` CLI's
`--no-traces` flag threads `emit_traces=False` here) and is fully
idempotent on its own terms (see `trace_emit.py`'s own docstring) -- INDEPENDENT
of `sync_eval_logged`: a trace is only ever skipped because
`trace_emit`'s own `client_request_id` existence check found one already
there, never because the OWNING run's tag says so. `backfill_traces.py`
calls these exact same `trace_emit` functions over the FULL historical
`sync_eval_logged=true` run population, so there is exactly ONE
trace-emission code path shared by the daily sync and the historical
backfill.

★ JOB-EXECUTION trace (2026-07-11, see `trace_emit.py`'s own "JOB-EXECUTION
TRACES" docstring section): `sync_production_range` itself ALSO gets ONE
`trace_emit.job_trace` per call, but landing in `timelines`, NEVER in either
production-usage experiment -- a job-level trace there would double-count
the per-race/per-horse traces above, which are already this module's Usage-
page signal for those two experiments. `timelines` gets one instead because
this whole call's `_sync_fp_eval`/`_sync_rs_eval` passes may collectively
call `timeline.upsert_timeline_point` many times across the date/category
range, and bundling all of them into ONE trace per `sync_production_range`
call (never one per date) is the "meaningful unit of work" grain
`trace_emit.py`'s own docstring calls for. Feedback `points_appended`
(numeric) counts how many `_sync_fp_eval`/`_sync_rs_eval` calls this call
returned `logged=True` from (`fp_eval_logged + rs_eval_logged`) -- each such
call makes EXACTLY one `timeline.upsert_timeline_point` call, so this is an
exact count of upsert *attempts*, not a finer "how many individual metric
keys were genuinely new after that function's own per-step dedup" count
(threading that back through would be a much larger change to `timeline.py`'s
widely-used return contract). When zero points were appended this call
(the common case: most days sync nothing new), the trace is discarded
(`JobTrace.discard()`) rather than logged empty -- see that method's own
docstring. Deliberately UNCONDITIONAL, independent of `emit_traces`/
`--no-traces` (see the code's own comment at `job_tracing_client` for why).

★ COVERAGE-RATIO / partial-serving detection (2026-07-11, see
`GAP_TYPE_PARTIAL_COVERAGE`'s own module-level comment for the full real
production blind spot this closes): every prior serving-gap check here
(`GAP_TYPE_NO_ROWS`/`GAP_TYPE_BACKFILL_ONLY`) is keyed off `races_live == 0`,
so a day where a SMALL FRACTION of races serve live -- e.g. a champion whose
own exact `model_version` label never writes a row, with only a cell-routed
variant serving for one narrow class-code slice, ~3% of scheduled races --
reads as fully healthy to all of them. This module now also computes
`fp_races_scheduled` (`serve_eval.fetch_races_scheduled`'s direct jvd_ra/
nvd_ra race-calendar count, for ALL THREE categories) and `fp_coverage_pct`
(`races_live / races_scheduled * 100`) once per (date, category) in
`_sync_fp_category_date`, attaches both to every per-model-version-group run
that day (`_log_base_tracking`) AND to the finish-position `timelines` point
(`_sync_fp_eval` -> `timeline.fp_metrics_for_timeline`), and -- in
`sync_production_range` -- fires a NEW, genuinely different gap type,
`GAP_TYPE_PARTIAL_COVERAGE`, whenever `coverage_pct` falls below a
configurable threshold (`partial_coverage_threshold`, default
`DEFAULT_PARTIAL_COVERAGE_THRESHOLD` = 80.0, the `sync-production` CLI's
`--partial-coverage-threshold` flag) EVEN THOUGH `races_live > 0`.

★ BOTH-PIPELINES-DARK fallback (2026-07-11, see `_resolve_expected_races`'s
own docstring for the exact three-way branch): the pre-existing jra/nar
no_rows/backfill_only check above compares FP against running-style's OWN
`races_observed` as an "expected races" proxy -- which has nothing to compare
against on a day RS itself also observed zero races (real examples: JRA
2026-06-13, 06-14, 06-20, 06-21, 06-28, `races_live == 0` for BOTH pipelines
simultaneously), so the check silently never fired even though races may
genuinely have been scheduled. `_resolve_expected_races` now falls back, in
exactly that case, to the SAME race-calendar oracle already built for banei
(`fp_outcome.races_scheduled`, see `GAP_TYPE_PARTIAL_COVERAGE`'s own comment
above -- reused directly, no second query), tagged the same
`GAP_SOURCE_RACE_CALENDAR`. Not a new gap TYPE -- still `GAP_TYPE_NO_ROWS`/
`GAP_TYPE_BACKFILL_ONLY`, selected the same way as before -- just a new
ORACLE reached for an existing check on a day the old proxy had nothing to
offer.

★ SELF-HEAL sweep for interrupted invocations (2026-07-11, see
`_heal_stale_running_runs`'s own docstring for the mechanics and
`sync_production_range`'s own docstring for the exact wiring): a real
incident left 18 runs in the two production-usage experiments tagged
`sync_base_logged=true` but stuck `status=RUNNING` forever -- an interrupted
process killed between `_log_base_tracking` setting that tag and this
module's own `client.set_terminated(run_id, status="FINISHED")` call a few
lines later, at the end of each model_version-group iteration (see
`_sync_fp_category_date`'s own docstring). This hid 54% of that experiment's
volume from any `status=FINISHED` filter/dashboard. `sync_production_range`
now runs this sweep ONCE, at the very start of every call, before any
date/category is visited (gated by the new `repair_stale_running` parameter,
default True): any run matching `status=RUNNING AND
tags.sync_base_logged='true'` whose `start_time` is older than
`stale_running_hours` (default `DEFAULT_STALE_RUNNING_HOURS` = 6.0,
configurable via the `sync-production` CLI's `--stale-running-hours` flag,
skippable entirely via `--no-repair-stale-running`) is force-terminated
FINISHED -- old enough that it can only be a genuinely abandoned run, never
one still legitimately in progress. This is a pure hygiene fix for
downstream `status=FINISHED` consumers: this module's own idempotency
machinery (`_find_sync_run`/`sync_key`) already finds a run by tag alone
regardless of its status, so a stuck-RUNNING run was never a correctness bug
for `sync_production_range` itself, only for anything reading run status
elsewhere. The 18 already-stuck runs from the real incident are being
repaired manually/separately -- this sweep is the CODE fix so the failure
mode never silently recurs.
"""

from __future__ import annotations

import tempfile
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final, Literal, cast

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from mlflow import MlflowClient
from mlflow.entities import Metric, RunTag
from mlflow.exceptions import MlflowException
from mlflow.tracing.client import TracingClient

from mlflow_tracking import config, db, registry, serve_eval, timeline, trace_emit
from mlflow_tracking.logging_api import get_or_create_experiment, log_batch_chunked

# finish-position serving covers all 3 categories; running-style has no
# Ban-ei support at all (an existing repo-wide rule -- see registry.py's
# Task/Category definitions and backfill_serve_timeline.py's own
# SUPPORTED_CATEGORIES for the same restriction applied to a sibling module).
FP_CATEGORIES: Final[tuple[str, ...]] = ("jra", "nar", "banei")
RS_CATEGORIES: Final[tuple[str, ...]] = ("jra", "nar")

SYNC_KEY_TAG: Final[str] = "sync_key"
SYNC_BASE_LOGGED_TAG: Final[str] = "sync_base_logged"
SYNC_EVAL_LOGGED_TAG: Final[str] = "sync_eval_logged"
CHAMPION_AT_SYNC_TAG: Final[str] = "champion_at_sync"
EVAL_REGIME_TAG: Final[str] = "eval_regime"
SERVE_REGIME: Final[str] = "serve"

# `champion_served` is a tag KEY shared by two entirely different run
# families with two entirely different value spaces -- never confused with
# each other since they live in different experiments/tag-search families:
#   - On the champion-GAP marker run (`_log_champion_gap`, keyed by
#     `CHAMPION_GAP_KEY_TAG`): a static "false", set once at creation, simply
#     recording why that marker exists at all (a gap is only ever logged
#     when NOTHING champion-derived served, see `_check_champion_gap`).
#   - On the regular per-(date, category, model_version) sync run
#     (`_get_or_create_run_and_tags`/`_log_base_tracking`, keyed by
#     `SYNC_KEY_TAG`): set to CHAMPION_SERVED_VARIANT_VALUE ("variant") ONLY
#     when that run's own `model_version` is a cell-routed VARIANT of the
#     resolved champion (see `_classify_served_model_version`) -- absent
#     entirely for an exact champion match or an unrelated model_version, so
#     a tag search for `tags.champion_served = 'variant'` finds exactly the
#     runs this widened-serving signal is about.
CHAMPION_SERVED_TAG: Final[str] = "champion_served"
CHAMPION_SERVED_VARIANT_VALUE: Final[str] = "variant"

# Tags the serving-gap marker run (see sync_production_range's own docstring
# and _log_serving_gap below) is identified/searched by -- a distinct tag
# family from SYNC_KEY_TAG's (date, category, model_version) grain, since a
# gap is a property of (date, category) alone, with no model_version.
SERVING_GAP_KEY_TAG: Final[str] = "serving_gap_key"
SERVING_GAP_TAG: Final[str] = "serving_gap"
GAP_SOURCE_RUNNING_STYLE: Final[str] = "running_style"
GAP_SOURCE_RACE_CALENDAR: Final[str] = "race_calendar"
GAP_TYPE_NO_ROWS: Final[str] = "no_rows"
GAP_TYPE_BACKFILL_ONLY: Final[str] = "backfill_only"
# ★ GAP_TYPE_PARTIAL_COVERAGE (2026-07-11) -- a THIRD, genuinely different gap
# shape from the two above: `GAP_TYPE_NO_ROWS`/`GAP_TYPE_BACKFILL_ONLY` are
# both keyed off `fp_outcome.races_live == 0` (see `sync_production_range`'s
# own docstring), so a day where SOME races serve live is invisible to both,
# no matter how small a fraction of the day that "some" actually is. The real
# incident this closes: JRA's champion `jra-cb-v9-sim-2013-clean` has never
# written a row since its 07-04 deploy -- only a cell-routed variant serves,
# for one narrow class-code slice, ~3% of scheduled races (e.g. 11/485 real
# starters on one day). Because that variant is champion-derived (see
# `_is_champion_or_variant`) and `races_live > 0`, the day reads as fully
# healthy to every check above. This gap type instead compares
# `fp_outcome.races_live` against `fp_outcome.races_scheduled` -- the
# INDEPENDENT `serve_eval.fetch_races_scheduled` race-calendar oracle for ALL
# THREE categories (not just banei), computed unconditionally once per
# (date, category) in `_sync_fp_category_date` regardless of whether any
# rows were ever synced -- and fires whenever the resulting COVERAGE RATIO
# (`fp_outcome.coverage_pct`) falls below a configurable threshold (default
# `DEFAULT_PARTIAL_COVERAGE_THRESHOLD`, see the `--partial-coverage-threshold`
# CLI flag), REGARDLESS of `races_live` being nonzero. Deliberately gated on
# `races_live > 0` in `sync_production_range`'s own check (see there): a
# totally-empty day (`races_live == 0`) is already, and continues to be,
# fully covered by `GAP_TYPE_NO_ROWS`/`GAP_TYPE_BACKFILL_ONLY` above, so the
# two families are mutually exclusive per (date, category) per call -- this
# is what lets both safely share `_log_serving_gap`'s one marker-run-per-
# (date, category) idempotency key without ever overwriting each other's
# `gap_type` tag.
GAP_TYPE_PARTIAL_COVERAGE: Final[str] = "partial_coverage"

# ★ Both-pipelines-dark fallback (2026-07-11) -- `GAP_SOURCE_RACE_CALENDAR` was
# originally banei-only (running-style has no Ban-ei model, so there was
# never an RS `races_observed` proxy to use there in the first place). It is
# now ALSO the fallback oracle `_resolve_expected_races` reaches for on
# jra/nar when RS's own `races_observed` proxy is itself 0 -- see that
# function's own docstring for the residual blind-spot this closes (a day
# where FP and RS are BOTH dark leaves the RS-vs-FP comparison with nothing
# to compare against). No new gap_source constant needed: this is the exact
# same tag/oracle, just reached via a second code path.

# Default coverage-ratio threshold (percent, 0-100 scale) for
# GAP_TYPE_PARTIAL_COVERAGE: a (date, category) with
# `coverage_pct < DEFAULT_PARTIAL_COVERAGE_THRESHOLD` is flagged, even though
# `races_live > 0`. 80.0 was chosen as a conservative floor -- real healthy
# serving days observed in this store cover effectively 100% of scheduled
# races, so anything below 80% is unambiguously a partial-serving incident,
# not routine day-to-day variance. Configurable via `sync-production`'s
# `--partial-coverage-threshold` flag, threaded through
# `sync_production_range`'s `partial_coverage_threshold` parameter.
DEFAULT_PARTIAL_COVERAGE_THRESHOLD: Final[float] = 80.0

# ★ Self-heal sweep for interrupted invocations (2026-07-11) -- see
# `_heal_stale_running_runs`'s own docstring for the full mechanics and
# `sync_production_range`'s own docstring for the real incident this closes
# (an interrupted process left 18 runs stuck `status=RUNNING` forever, tagged
# `sync_base_logged=true`, hiding 54% of one experiment's volume from any
# `status=FINISHED` filter). `DEFAULT_STALE_RUNNING_HOURS` = 6.0 was chosen
# as comfortably longer than any real (date, category, model_version) sync
# pass ever legitimately takes -- a run still RUNNING this long after its own
# `start_time` can only be abandoned, never one still genuinely in progress.
# Configurable via `sync-production`'s `--stale-running-hours` flag, threaded
# through `sync_production_range`'s `stale_running_hours` parameter; the
# sweep itself is skippable entirely via `--no-repair-stale-running`
# (`repair_stale_running=False`).
DEFAULT_STALE_RUNNING_HOURS: Final[float] = 6.0

# Tags the champion-mismatch marker run (see _log_champion_gap below) is
# identified/searched by -- keyed by (date, category, task), a THIRD distinct
# grain from both SYNC_KEY_TAG's and SERVING_GAP_KEY_TAG's: a champion
# mismatch is a property of one task's serving on one day, not of a specific
# model_version run (which already carries its own `champion_at_sync` tag,
# see below) nor of the FP-vs-RS comparison the serving-gap family tracks.
CHAMPION_GAP_KEY_TAG: Final[str] = "champion_gap_key"
CHAMPION_GAP_TAG: Final[str] = "champion_gap"

TRUE_STR: Final[str] = "true"
FALSE_STR: Final[str] = "false"

_PREDICTIONS_JSON_ARTIFACT: Final[str] = "predictions.json"
_PREDICTIONS_PARQUET_ARTIFACT: Final[str] = "predictions.parquet"
_EVAL_JSON_ARTIFACT: Final[str] = "eval.json"
_EVAL_PARQUET_ARTIFACT: Final[str] = "eval.parquet"

# Exceptions isolated per (date, category, task) so one bad combination never
# aborts the rest of the range (see sync_production_range's docstring):
# ValueError/KeyError/TypeError cover malformed row shapes or bad category
# strings raised by this module's own or serve_eval's defensive code;
# psycopg2.Error covers a live query failure against either Postgres source;
# MlflowException covers a tracking-store failure (e.g. transient sqlite
# lock, a malformed search filter).
_ISOLATED_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    ValueError,
    KeyError,
    TypeError,
    psycopg2.Error,
    MlflowException,
)


@dataclass
class SyncProductionSummary:
    """Outcome counters for one `sync_production_range` call.

    Every count is scoped to THIS call only -- e.g. `fp_runs_reused` counts
    runs found already existing (from a previous call, possibly days ago),
    not runs reused within this same call (a given (date, category,
    model_version) sync_key is only ever visited once per call, since dates
    and categories are each walked without repetition).

    `serving_gaps_detected` counts every (date, category) pair, THIS call,
    for which ANY serving-gap check (see `sync_production_range`'s own
    docstring -- RS-vs-FP for jra/nar or the local race calendar for banei,
    for `GAP_TYPE_NO_ROWS`/`GAP_TYPE_BACKFILL_ONLY`; OR the independent
    coverage-ratio check, for `GAP_TYPE_PARTIAL_COVERAGE`, 2026-07-11) found
    a genuine gap and logged/found its marker run -- whether that marker run
    was freshly created or already existed from a previous day's call counts
    the same here, since this field answers "how many gaps did this call
    observe", not "how many NEW marker runs did this call create". This is
    one shared counter across all three gap TYPES (not a per-type count) --
    a caller that needs to distinguish which type fired for a given (date,
    category) reads the marker run's own `gap_type` tag instead (see
    `_log_serving_gap`); the two families are mutually exclusive per (date,
    category) per call (see `GAP_TYPE_PARTIAL_COVERAGE`'s own comment), so
    this field is never double-counting the SAME pair twice in one call.

    `champion_gaps_detected` is the same kind of THIS-call observation count,
    for the champion-mismatch check (see `_check_champion_gap`'s own
    docstring): live rows were genuinely served for a (date, category, task)
    this call, but none of them came from the currently-registered champion
    model_version.

    `traces_created`/`traces_already_existed` sum `trace_emit.
    TraceEmitSummary`'s own fields (see that dataclass's docstring) across
    every fp/rs eval pass this call -- `traces_already_existed` is the
    NORMAL, expected count on any re-run (daily sync re-covering an
    overlapping range), not an error. Both stay 0 when `emit_traces=False`
    (no `trace_emit` call is ever made in that case). Per-trace emission
    failures are folded into `errors` (prefixed `...:trace-emit:...`), same
    as every other isolated failure family in this function.

    `stale_running_healed` (2026-07-11) counts every run this call force-
    terminated FINISHED via the startup self-heal sweep
    (`_heal_stale_running_runs` -- see that function's own docstring and
    `sync_production_range`'s own docstring's "SELF-HEAL" section for the
    real incident this closes). This is a CALL-level total across BOTH
    production-usage experiments (FP always; RS too, whenever this call's
    `categories` includes an `RS_CATEGORIES` member), not scoped to any one
    (date, category) -- the sweep runs exactly ONCE, before the date/category
    loop even starts, since a stale run left over from a PRIOR call could
    belong to any date that call covered, not necessarily one THIS call
    revisits. Stays 0 whenever `repair_stale_running=False` (the sweep is
    skipped entirely) or when the sweep simply found nothing old enough to
    heal. A sweep failure (e.g. a transient tracking-store error) is folded
    into `errors` (prefixed `stale-running-heal:...`), same as every other
    isolated failure family in this function -- it never prevents the rest
    of the call (date/category sync, gap checks) from proceeding.
    """

    dates_processed: int
    fp_runs_created: int
    fp_runs_reused: int
    fp_eval_logged: int
    fp_eval_skipped_no_results: int
    rs_runs_created: int
    rs_runs_reused: int
    rs_eval_logged: int
    rs_eval_skipped_no_results: int
    serving_gaps_detected: int
    champion_gaps_detected: int
    traces_created: int
    traces_already_existed: int
    stale_running_healed: int
    errors: list[str]


@dataclass
class _CategoryDateOutcome:
    """Per-(date, category) counters, summed by the caller into whichever of
    `SyncProductionSummary`'s fp_*/rs_* fields apply.

    `races_observed` is the distinct race count of genuinely-served rows for
    this exact (date, category) -- ALWAYS set correctly by both
    `_sync_fp_category_date` and `_sync_rs_category_date`, including on their
    early-empty-`rows` return path (explicitly set to 0 there rather than
    left at this field's default, so a reader of that return statement sees
    the invariant held, not an implicit default doing the work). This is what
    lets `sync_production_range` detect a serving gap (see its own
    docstring) without any extra Neon query -- both functions already have
    `rows` in hand by the time they would otherwise return.

    `races_live`/`races_backfilled` split `races_observed` by
    `serve_eval.classify_serving_kind` (via `serve_eval.partition_live_backfill`):
    `races_live` counts races with at least one row generated within
    `serve_eval.LIVE_LAG_TOLERANCE_DAYS` of the race date (same-day/near
    same-day live serving, or pre-race publishing); `races_backfilled` counts
    races whose rows only ever arrived later than that -- still within the
    broader genuine window (`serve_eval.GEN_LAG_TOLERANCE_DAYS`), so not
    decades-old WF-era noise, but clearly a delayed re-run rather than the
    live serving path. `races_observed == races_live + races_backfilled` in
    practice, since a given race's rows share one `prediction_generated_at`
    batch. This split is what lets the serving-gap check treat a
    backfill-only day (rows exist, but none are genuinely live) as a real gap
    instead of a served day -- see `sync_production_range`'s own docstring.

    `champion_model_version`/`champion_live_races`/`served_model_versions_live`
    feed the champion-vs-served mismatch check (`_check_champion_gap`):
    `champion_model_version` is the CURRENT champion label resolved fresh
    this call (None when no champion alias is set at all);
    `champion_live_races` counts races among the LIVE rows that came from
    that model_version OR a cell-routed VARIANT of it (see
    `_is_champion_or_variant` -- widened 2026-07-10; the old exact-match-only
    accumulation made `_check_champion_gap` fire a false "champion did not
    serve" alarm on a day where only a variant, never the bare champion
    label, actually served); `served_model_versions_live` is the set of
    every distinct model_version that had at least one live race this call
    (used only for the champion-gap marker run's diagnostic tag, naming what
    WAS served instead). All three stay at their defaults (None/0/empty) on
    the early-empty-`rows` return path -- never inspected there, since
    `_check_champion_gap` short-circuits on `races_live == 0` first.

    `traces_created`/`traces_already_existed`/`trace_errors` accumulate the
    `trace_emit.TraceEmitSummary` returned by `_sync_fp_eval`/`_sync_rs_eval`
    each time they successfully compute `eval_rows` (see those functions'
    own docstrings) -- all stay at their defaults (0/0/empty) whenever
    `emit_traces=False`, or on any date/category where the eval join itself
    never produced `eval_rows` (nothing to emit a trace for yet).

    `races_scheduled`/`coverage_pct` (2026-07-11, FP-only -- `_sync_rs_
    category_date` never sets either, both stay at their defaults for every
    RS outcome) are the new coverage-ratio pair (see
    `GAP_TYPE_PARTIAL_COVERAGE`'s own module-level comment for the full
    incident this closes). `races_scheduled` is `serve_eval.
    fetch_races_scheduled`'s race-calendar count for this (date, category),
    computed UNCONDITIONALLY in `_sync_fp_category_date` -- including on its
    early-empty-`rows` return path, mirroring `races_observed`'s own
    "explicitly set, never left at an implicit default" discipline -- since
    it is a pure local-replica query, independent of whether any Neon
    prediction rows exist at all. `coverage_pct` is `_coverage_pct(races_live,
    races_scheduled)`: `100.0 * races_live / races_scheduled`, or None when
    `races_scheduled` is 0 (an undefined ratio -- no races were scheduled
    that day at all, never a fabricated 0.0/100.0).
    """

    runs_created: int = 0
    runs_reused: int = 0
    eval_logged: int = 0
    eval_skipped_no_results: int = 0
    races_observed: int = 0
    races_scheduled: int = 0
    coverage_pct: float | None = None
    races_live: int = 0
    races_backfilled: int = 0
    champion_model_version: str | None = None
    champion_live_races: int = 0
    served_model_versions_live: frozenset[str] = frozenset()
    traces_created: int = 0
    traces_already_existed: int = 0
    trace_errors: list[str] = field(default_factory=list)


def _date_range_yyyymmdd(date_from: str, date_to: str) -> list[str]:
    """Return every YYYYMMDD string from `date_from` to `date_to`, inclusive.

    A small local copy of backfill_serve_timeline.py's private helper of the
    same name -- not imported, per this module's own design note that a
    private cross-module import between sibling top-level modules isn't
    worth the coupling for a few lines of date arithmetic.
    """
    start = date(int(date_from[:4]), int(date_from[4:6]), int(date_from[6:8]))
    end = date(int(date_to[:4]), int(date_to[4:6]), int(date_to[6:8]))
    dates: list[str] = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


def _coverage_pct(races_live: int, races_scheduled: int) -> float | None:
    """Return `100.0 * races_live / races_scheduled`, or None when
    `races_scheduled <= 0` -- an undefined ratio (nothing was scheduled that
    day for this category at all), never a fabricated 0.0/100.0 and never a
    ZeroDivisionError. Mirrors `serve_eval.compute_rank_pct`'s own
    None-for-"not applicable" precedent.
    """
    if races_scheduled <= 0:
        return None
    return 100.0 * races_live / races_scheduled


def _log_table_and_parquet(
    client: MlflowClient, run_id: str, df: pd.DataFrame, json_name: str, parquet_name: str
) -> None:
    """Log `df` twice: as an MLflow table artifact (renders in the UI's
    Evaluation tab) and as a parquet artifact (efficient bulk re-read).

    Mirrors `logging_api.log_cell_table`'s exact mechanics with caller-chosen
    artifact filenames -- kept local rather than added to logging_api.py
    since this module's tables (`predictions.*` / `eval.*`) are a distinct
    artifact family from that function's hardcoded `cell_metrics.*` names.
    """
    client.log_table(run_id, data=df, artifact_file=json_name)
    with tempfile.TemporaryDirectory() as tmp_dir:
        parquet_path = Path(tmp_dir) / parquet_name
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), parquet_path)
        client.log_artifact(run_id, str(parquet_path))


def _distinct_race_count(rows: list[dict[str, object]]) -> int:
    """Count distinct (keibajo_code, race_bango) races represented in `rows`."""
    return len({(str(row["keibajo_code"]), str(row["race_bango"])) for row in rows})


_FP_PREDICTION_TABLE_COLUMNS: Final[tuple[str, ...]] = (
    "venue",
    "race_bango",
    "predicted_top1_ketto",
    "distance_band",
    "field_size_band",
    "season_band",
    "class_code",
    "surface",
)


def _fp_prediction_table(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build the FP usage-tracking prediction table: one row per race.

    Distinct from `serve_eval.build_fp_race_eval_rows`: this is a plain
    usage-tracking view built purely from `rows` (never `results`, which may
    not exist yet for a prediction published ahead of race day) -- it shows
    only which horse each race's model picked for the win, not whether that
    pick was correct. `rows` is always nonempty here: every caller passes a
    `group_by_model_version` value, which by construction never contains an
    empty list.
    """
    groups: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in rows:
        key = (str(row["keibajo_code"]), str(row["race_bango"]))
        groups.setdefault(key, []).append(row)

    records: list[dict[str, object]] = []
    for (keibajo_code, race_bango), group_rows in groups.items():
        predicted_top1_ketto: str | None = None
        for row in group_rows:
            if row["predicted_rank"] == 1:
                predicted_top1_ketto = str(row["ketto_toroku_bango"])
                break
        first_row = group_rows[0]
        records.append(
            {
                "venue": keibajo_code,
                "race_bango": race_bango,
                "predicted_top1_ketto": predicted_top1_ketto,
                "distance_band": first_row["distance_band"],
                "field_size_band": first_row["field_size_band"],
                "season_band": first_row["season_band"],
                "class_code": first_row["class_code"],
                "surface": first_row["surface"],
            }
        )
    return pd.DataFrame(records, columns=list(_FP_PREDICTION_TABLE_COLUMNS))


_RS_PREDICTION_TABLE_COLUMNS: Final[tuple[str, ...]] = (
    "venue",
    "race_bango",
    "ketto_toroku_bango",
    "predicted_class",
    "predicted_label",
)


def _rs_prediction_table(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build the RS usage-tracking prediction table: one row per horse.

    `rows` is always nonempty here, for the same reason as
    `_fp_prediction_table`'s docstring above.
    """
    records = [
        {
            "venue": str(row["keibajo_code"]),
            "race_bango": str(row["race_bango"]),
            "ketto_toroku_bango": str(row["ketto_toroku_bango"]),
            "predicted_class": row["predicted_class"],
            "predicted_label": row["predicted_label"],
        }
        for row in rows
    ]
    return pd.DataFrame(records, columns=list(_RS_PREDICTION_TABLE_COLUMNS))


def _fp_category_filtered_rows(
    category: str, genuine_rows: list[dict[str, object]]
) -> list[dict[str, object]]:
    """Apply the FP-specific Ban-ei partition to already-genuine-filtered rows.

    `category == "jra"` passes rows through unchanged -- `partition_by_banei`
    is never called for jra, since source="jra" can never contain a Ban-ei
    keibajo_code row in the first place (Ban-ei predictions are always
    stored under source="nar", see `serve_eval.resolve_source`). For "nar"
    only the non-Ban-ei subset survives; for "banei" only the Ban-ei subset
    does.
    """
    if category == "jra":
        return genuine_rows
    non_banei_rows, banei_rows = serve_eval.partition_by_banei(genuine_rows)
    return banei_rows if category == "banei" else non_banei_rows


def _resolve_champion_label(client: MlflowClient, category: str, task: registry.Task) -> str | None:
    """Return the CURRENT champion model version's `model_version` tag value
    for (category, task), or None when there is no resolvable champion.

    Every step of the registry lookup chain (normalize category -> resolve
    registered-model name -> fetch the registered model -> read its
    champion alias -> fetch that version) is wrapped in one
    `except MlflowException` -- a registered model that does not exist yet,
    a champion alias that was never set, and a model-version row that
    vanished all surface as `MlflowException` from the underlying mlflow
    store, and all three mean the same thing here: "no champion resolvable
    right now", not a fatal error worth propagating out of a sync loop.
    """
    try:
        normalized = registry.normalize_category(category)
        name = registry.registered_model_name(normalized, task)
        registered_model = client.get_registered_model(name)
        version = registered_model.aliases.get(registry.CHAMPION_ALIAS)
        if version is None:
            return None
        model_version = client.get_model_version(name, str(version))
        return model_version.tags.get("model_version")
    except MlflowException:
        return None


def _champion_at_sync_tag_value(
    client: MlflowClient, category: str, task: registry.Task, model_version: str
) -> str:
    """Return "true"/"false" for whether `model_version` IS the current
    champion for (category, task), resolved AT THE MOMENT base tracking is
    logged (never re-resolved later -- see `CHAMPION_AT_SYNC_TAG`'s name).

    Deliberately EXACT-MATCH-ONLY, unchanged semantics: a variant serving is
    surfaced separately via `CHAMPION_SERVED_TAG` (see
    `_champion_served_variant_tag` below), additively, not by redefining
    what this tag has always meant to any existing caller/dashboard.
    """
    champion_label = _resolve_champion_label(client, category, task)
    return TRUE_STR if champion_label == model_version else FALSE_STR


# ── Champion-or-variant serving predicate ────────────────────────────────────
#
# Mirrors champion_cell_eval.py's OWN copy of this exact rule
# (`_classify_champion_match`/`_is_champion_derived` there) -- DUPLICATED
# here rather than imported, for the identical reason
# `_resolve_champion_label` above duplicates that module's
# `_resolve_champion_model_version` instead of sharing it (see this module's
# own docstring): the two modules were built by separate agents in parallel
# with zero file-ownership overlap, and that stays true for this predicate
# too. See champion_cell_eval.py's module docstring ("★ CELL-ROUTED VARIANT
# widening") for the full incident/rationale this fixes on that module's
# side; the fix here is the analogous one for `_check_champion_gap`'s
# `outcome.champion_live_races == 0` "champion did not serve" signal, which
# the OLD exact-match-only accumulation made structurally over-eager to fire
# on any day a variant (not the bare champion label) served -- a false
# "champion gap" alarm, not a real one.

_ServedMatchKind = Literal["champion", "variant", "other"]

# Tracked production selectors whose served label is intentionally not a
# ``<champion>-<scope>`` string.  The left-hand label is the registry/Neon
# category base; the right-hand label is a routed or companion scorer that
# DEPLOY.md and the container's tracked routing config declare as part of the
# same production policy.  Keeping this relation explicit prevents valid NAR
# companion/fallback rows and the Ban-ei grade-E route from being reported as
# unrelated challengers merely because their immutable artifact names predate
# the later suffix convention.
_DECLARED_CHAMPION_DERIVED_PAIRS: Final[frozenset[tuple[str, str]]] = frozenset(
    {
        (
            "iter12-nar-xgb-hpo-v8-clean188",
            "iter40-nar-settransformer-blend-v1",
        ),
        (
            "iter12-nar-xgb-hpo-v8-clean188",
            "iter12-nar-xgb-hpo-v8-stage1-marketfree-184",
        ),
        (
            "banei-cb-v9-sim-2011",
            "banei-cb-v8-window2011-wf-15y",
        ),
    }
)


def _classify_served_model_version(
    served_model_version: str, champion_label: str | None
) -> _ServedMatchKind:
    """Classify one served `model_version` against the resolved champion
    label for (category, task).

    Returns "champion" for an exact match, "variant" when `served_model_
    version` starts with `f"{champion_label}-"` (a cell-routed variant of
    the champion), and "other" for everything else -- including
    `champion_label is None` (no resolvable champion at all: comparing a
    served row against nothing is never a match) and a served label that
    merely shares `champion_label` as a raw string prefix WITHOUT the "-"
    separator (e.g. champion "iter14" must not classify served "iter140" as
    a variant just because `str.startswith("iter14")` is True -- the
    routing-scope suffix is always "-"-delimited in practice, so a served
    label missing that delimiter is an unrelated model, not a variant of
    this champion).
    """
    if champion_label is None:
        return "other"
    if served_model_version == champion_label:
        return "champion"
    declared_pair = (champion_label, served_model_version)
    if (
        served_model_version.startswith(f"{champion_label}-")
        or declared_pair in _DECLARED_CHAMPION_DERIVED_PAIRS
    ):
        return "variant"
    return "other"


def _is_champion_or_variant(served_model_version: str, champion_label: str | None) -> bool:
    """True iff `served_model_version` is the exact champion OR a cell-routed
    variant of it (see `_classify_served_model_version`). Used to widen the
    `champion_live_races` accumulation in `_sync_fp_category_date`/
    `_sync_rs_category_date` so `_check_champion_gap` no longer fires a
    false "champion did not serve" alarm on a day where only a variant
    (never the bare champion label) actually served."""
    return _classify_served_model_version(served_model_version, champion_label) != "other"


def _champion_served_variant_tag(
    client: MlflowClient, category: str, task: registry.Task, model_version: str
) -> RunTag | None:
    """Return a `champion_served="variant"` RunTag when `model_version` is a
    cell-routed VARIANT of the current champion for (category, task) -- e.g.
    a champion labeled "jra-cb-v9-sim-2013-clean" actually served in
    production as "jra-cb-v9-sim-2013-clean-jockey-pedigree269" for one
    class-code cell -- or None otherwise (an exact champion match, an
    unrelated model_version, or no resolvable champion at all).

    ADDITIVE to `CHAMPION_AT_SYNC_TAG`, never a replacement for it: an exact
    match keeps `champion_at_sync="true"` and gets no `champion_served` tag
    at all (there is nothing extra to say); a variant keeps
    `champion_at_sync="false"` (still not an EXACT match, that tag's
    unchanged contract) AND additionally gets `champion_served="variant"`,
    so a caller can distinguish "served a variant of the champion" from
    "served something entirely unrelated" without either tag lying about
    what it has always meant.
    """
    champion_label = _resolve_champion_label(client, category, task)
    if _classify_served_model_version(model_version, champion_label) == "variant":
        return RunTag(CHAMPION_SERVED_TAG, CHAMPION_SERVED_VARIANT_VALUE)
    return None


def _find_sync_run(client: MlflowClient, experiment_id: str, sync_key: str) -> str | None:
    """Find the run tagged with `sync_key`, mirroring
    `timeline._find_timeline_run`'s exact tag-search idiom."""
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{SYNC_KEY_TAG} = '{sync_key}'",
        max_results=1,
    )
    return matches[0].info.run_id if matches else None


def _get_or_create_run_and_tags(
    client: MlflowClient,
    experiment_id: str,
    *,
    sync_key: str,
    task: str,
    category: str,
    model_version: str,
    date_str: str,
) -> tuple[str, bool, dict[str, str]]:
    """Find or create the sync run for `sync_key`; return (run_id, created,
    existing_tags).

    `existing_tags` is `{}` for a newly created run (nothing has been logged
    on it yet, so both `SYNC_BASE_LOGGED_TAG`/`SYNC_EVAL_LOGGED_TAG` are
    correctly treated as absent) and the run's real current tag dict
    otherwise -- callers use this single snapshot to decide independently
    whether to (re)log the base tracking and/or the evaluation parts, per
    this module's own docstring on the idempotency state machine.

    `EVAL_REGIME_TAG` is set to `SERVE_REGIME` ("serve") unconditionally at
    creation time, never conditionally: every run this function creates
    represents a genuinely-served production prediction, by this whole
    module's own design (see its module docstring's opening paragraph) --
    there is no code path in this module that logs anything else, so there
    is nothing to branch on here.

    `run_name` is set at creation to `"{date_str} {category} {model_version}"`
    -- matching the display name already carried by every pre-existing run in
    the real store (renamed there by a prior pass before this function set
    names at creation time), so a freshly-created run is never blank/default
    before some later rename step.
    """
    run_id = _find_sync_run(client, experiment_id, sync_key)
    if run_id is not None:
        return run_id, False, client.get_run(run_id).data.tags
    run = client.create_run(
        experiment_id,
        run_name=f"{date_str} {category} {model_version}",
        tags={
            SYNC_KEY_TAG: sync_key,
            "task": task,
            "category": category,
            "model_version": model_version,
            "date": date_str,
            EVAL_REGIME_TAG: SERVE_REGIME,
        },
    )
    return run.info.run_id, True, {}


def _log_base_tracking(
    client: MlflowClient,
    run_id: str,
    *,
    task: registry.Task,
    metric_prefix: str,
    category: str,
    date_str: str,
    model_version: str,
    rows: list[dict[str, object]],
    prediction_table: pd.DataFrame,
    races_scheduled: int | None = None,
    coverage_pct: float | None = None,
) -> None:
    """Log the base production-usage tracking for one (date, category,
    model_version) group and mark `sync_base_logged=true`.

    Called at most ONCE per sync_key (enforced by the caller checking
    `SYNC_BASE_LOGGED_TAG` first): predictions are immutable once generated,
    and `client.log_table`'s APPEND semantics would silently duplicate rows
    in the prediction-table artifact on a second call for the same run.
    `model_version`/`date`/`category` are already set as tags at run
    creation time (see `_get_or_create_run_and_tags`) and never change, so
    only `champion_at_sync` (and, additively, `champion_served`) need to be
    (re-)written here.

    `{metric_prefix}_races_live`/`{metric_prefix}_races_backfilled` split
    `{metric_prefix}_races` by `serve_eval.partition_live_backfill` -- see
    `_CategoryDateOutcome`'s own docstring for the full rationale (this is
    the same split, just scoped to one model_version's own `rows` rather
    than the whole category-date).

    `champion_served="variant"` (see `_champion_served_variant_tag`) is
    attached ONLY when `model_version` is a cell-routed variant of the
    resolved champion -- resolved at this SAME moment as `champion_at_sync`
    (never re-resolved later, matching that tag's own "AT THE MOMENT base
    tracking is logged" contract), and simply omitted otherwise.

    `races_scheduled`/`coverage_pct` (2026-07-11, FP-only -- `_sync_rs_
    category_date`'s own call site never passes them, so they stay None
    there) are the DAY-level coverage-ratio pair (see `_CategoryDateOutcome`'s
    own docstring / `GAP_TYPE_PARTIAL_COVERAGE`'s module-level comment) --
    NOT scoped to this one model_version's own `rows`, unlike every other
    metric logged above. Deliberately DUPLICATED onto every model-version-
    group run created for this (date, category) rather than logged once
    somewhere else: this file already duplicates date/category CONTEXT
    (tags) onto every such run, and this is the same judgment call applied
    to two more values. `races_scheduled` (an int, never None when the FP
    caller passes it) is logged whenever given; `coverage_pct` is skipped
    (never logged as a fabricated 0.0) when it is None -- which happens when
    `races_scheduled` was 0 (see `_coverage_pct`'s own docstring), or when
    this is an RS call that never computes it at all.
    """
    champion_value = _champion_at_sync_tag_value(client, category, task, model_version)
    variant_tag = _champion_served_variant_tag(client, category, task, model_version)
    champion_tags = [RunTag(CHAMPION_AT_SYNC_TAG, champion_value)]
    if variant_tag is not None:
        champion_tags.append(variant_tag)
    live_rows, backfill_rows = serve_eval.partition_live_backfill(rows)
    metrics = [
        Metric(f"{metric_prefix}_races", float(_distinct_race_count(rows)), 0, 0),
        Metric(f"{metric_prefix}_horses", float(len(rows)), 0, 0),
        Metric(f"{metric_prefix}_races_live", float(_distinct_race_count(live_rows)), 0, 0),
        Metric(
            f"{metric_prefix}_races_backfilled", float(_distinct_race_count(backfill_rows)), 0, 0
        ),
    ]
    if races_scheduled is not None:
        metrics.append(Metric(f"{metric_prefix}_races_scheduled", float(races_scheduled), 0, 0))
    if coverage_pct is not None:
        metrics.append(Metric(f"{metric_prefix}_coverage_pct", float(coverage_pct), 0, 0))
    log_batch_chunked(client, run_id, metrics=metrics, tags=champion_tags)
    _log_table_and_parquet(
        client, run_id, prediction_table, _PREDICTIONS_JSON_ARTIFACT, _PREDICTIONS_PARQUET_ARTIFACT
    )
    log_batch_chunked(client, run_id, tags=[RunTag(SYNC_BASE_LOGGED_TAG, TRUE_STR)])


def _sync_fp_eval(
    client: MlflowClient,
    run_id: str,
    *,
    tracing_client: TracingClient | None,
    experiment_id: str,
    category: str,
    date_str: str,
    model_version: str,
    rows: list[dict[str, object]],
    local_conn: db.ConnectionLike,
    races_scheduled: int,
    coverage_pct: float | None,
) -> tuple[bool, trace_emit.TraceEmitSummary]:
    """Attempt the FP evaluation join for one (date, category, model_version)
    group. Returns (True, trace_summary) (and logs metrics/table/timeline
    point, plus marks `sync_eval_logged=true`, plus emits one MLflow trace
    per race via `trace_emit.emit_fp_race_traces` -- see this module's own
    docstring) only when at least one race matched a finalized result;
    returns (False, an empty TraceEmitSummary) (logging/emitting nothing)
    otherwise, so the caller leaves the tag absent and a future call retries
    once results are final.

    `tracing_client` is None exactly when the caller's `emit_traces=False`
    (see `sync_production_range`) -- trace emission is skipped entirely in
    that case, not attempted-and-discarded.

    `races_scheduled`/`coverage_pct` (2026-07-11) are the same DAY-level
    coverage-ratio pair `_log_base_tracking` also (independently) receives --
    see that function's own docstring and `_CategoryDateOutcome`'s. Both are
    REQUIRED here (unlike `_log_base_tracking`'s own optional pair, which
    must also serve RS callers that never compute them): this function is
    FP-only, with exactly one caller (`_sync_fp_category_date`), which always
    has a real `outcome.races_scheduled` (an int, computed unconditionally)
    in hand by the time it calls this. They are merged directly into
    `day_metrics` (not part of `serve_eval.aggregate_fp_day_metrics`'s own
    return shape) BEFORE building `metric_items`/calling `timeline.
    fp_metrics_for_timeline`, so both the per-run `fp_races_scheduled`/
    `fp_coverage_pct` metrics below and the `finish-position` timeline's own
    `fp_races_scheduled`/`fp_coverage_pct` points come from this exact one
    merge -- see `timeline._FP_TIMELINE_METRIC_MAP`'s own comment for why
    that map is the single place the timeline metric NAME is decided.
    """
    results = serve_eval.fetch_race_results(local_conn, category, date_str)
    eval_rows = serve_eval.build_fp_race_eval_rows(category, model_version, rows, results)
    if not eval_rows:
        return False, trace_emit.TraceEmitSummary()

    day_metrics = serve_eval.aggregate_fp_day_metrics(eval_rows)
    day_metrics["races_scheduled"] = float(races_scheduled)
    day_metrics["coverage_pct"] = coverage_pct
    # `place4_pct`/`place5_pct`/`place6_pct` (and now `coverage_pct`) may be
    # None (every race this day had too small a field for that rank -- see
    # `serve_eval.aggregate_fp_day_metrics`'s own docstring; `coverage_pct`
    # is None when zero races were scheduled that day, see `_coverage_pct`'s
    # own docstring), so this must skip a None value rather than blindly
    # `float()`-casting it -- mirroring `_sync_rs_eval`'s own
    # `isinstance(macro_f1_pct, int | float)` idiom for its own possibly-None
    # `macro_f1_pct` value.
    metric_items = [
        Metric(f"fp_{key}", float(value), 0, 0)
        for key, value in day_metrics.items()
        if key != "races" and isinstance(value, int | float)
    ]
    # Named distinctly from the base-tracking `fp_races` metric (ALL served
    # races that day), which this is not: only races that matched at least
    # one finalized result count here. `day_metrics["races"]` is documented
    # (see `serve_eval.aggregate_fp_day_metrics`'s own docstring) to always
    # be a plain float, never None -- `cast` here for the same reason
    # `_sync_rs_eval`/`_fp_cell_metrics` cast their own always-present,
    # loosely-Optional-typed fields.
    metric_items.append(
        Metric("fp_races_evaluated", float(cast(float, day_metrics["races"])), 0, 0)
    )
    log_batch_chunked(client, run_id, metrics=metric_items)
    _log_table_and_parquet(
        client,
        run_id,
        serve_eval.fp_eval_rows_to_dataframe(eval_rows),
        _EVAL_JSON_ARTIFACT,
        _EVAL_PARQUET_ARTIFACT,
    )
    timeline_run_id = timeline.upsert_timeline_point(
        client, "finish-position", category, date_str, timeline.fp_metrics_for_timeline(day_metrics)
    )
    log_batch_chunked(
        client,
        run_id,
        tags=[
            RunTag("timeline_run_id:finish-position", timeline_run_id),
            RunTag(SYNC_EVAL_LOGGED_TAG, TRUE_STR),
        ],
    )
    trace_summary = (
        trace_emit.emit_fp_race_traces(tracing_client, experiment_id, eval_rows, rows)
        if tracing_client is not None
        else trace_emit.TraceEmitSummary()
    )
    return True, trace_summary


def _sync_rs_eval(
    client: MlflowClient,
    run_id: str,
    *,
    tracing_client: TracingClient | None,
    experiment_id: str,
    category: str,
    date_str: str,
    model_version: str,
    rows: list[dict[str, object]],
    local_conn: db.ConnectionLike,
) -> tuple[bool, trace_emit.TraceEmitSummary]:
    """Attempt the RS evaluation join for one (date, category, model_version)
    group. Same "log only on a nonempty join, else leave retry-able" contract
    as `_sync_fp_eval` -- including the trace-emission side effect (one
    trace per HORSE here, via `trace_emit.emit_rs_horse_traces`, see this
    module's own docstring and `trace_emit.py`'s RS design-decision section)
    and the same `tracing_client is None` skip-entirely behavior."""
    results = serve_eval.fetch_race_results(local_conn, category, date_str)
    race_meta = serve_eval.fetch_race_metadata(local_conn, category, date_str)
    eval_rows = serve_eval.build_rs_horse_eval_rows(
        category, model_version, rows, results, race_meta
    )
    if not eval_rows:
        return False, trace_emit.TraceEmitSummary()

    day_metrics = serve_eval.aggregate_rs_day_metrics(eval_rows)
    # `aggregate_rs_day_metrics` returns `dict[str, object]` (its `per_class`
    # value is a list, not a float), but `overall_accuracy_pct` itself is
    # documented to always be a plain float (0.0 when there are zero rows,
    # never None) -- see that function's own docstring.
    overall_accuracy_pct = cast(float, day_metrics["overall_accuracy_pct"])
    metric_items = [Metric("rs_overall_accuracy_pct", float(overall_accuracy_pct), 0, 0)]
    macro_f1_pct = day_metrics["macro_f1_pct"]
    if isinstance(macro_f1_pct, int | float):
        metric_items.append(Metric("rs_macro_f1_pct", float(macro_f1_pct), 0, 0))
    log_batch_chunked(client, run_id, metrics=metric_items)
    _log_table_and_parquet(
        client,
        run_id,
        serve_eval.rs_eval_rows_to_dataframe(eval_rows),
        _EVAL_JSON_ARTIFACT,
        _EVAL_PARQUET_ARTIFACT,
    )
    timeline_run_id = timeline.upsert_timeline_point(
        client, "running-style", category, date_str, timeline.rs_metrics_for_timeline(day_metrics)
    )
    log_batch_chunked(
        client,
        run_id,
        tags=[
            RunTag("timeline_run_id:running-style", timeline_run_id),
            RunTag(SYNC_EVAL_LOGGED_TAG, TRUE_STR),
        ],
    )
    trace_summary = (
        trace_emit.emit_rs_horse_traces(tracing_client, experiment_id, eval_rows, rows)
        if tracing_client is not None
        else trace_emit.TraceEmitSummary()
    )
    return True, trace_summary


def _sync_fp_category_date(
    client: MlflowClient,
    tracing_client: TracingClient | None,
    experiment_id: str,
    neon_conn: db.ConnectionLike,
    local_conn: db.ConnectionLike,
    category: str,
    date_str: str,
) -> _CategoryDateOutcome:
    """Sync every genuinely-served FP model_version group for (category,
    date_str). A day with zero surviving rows for this category (the normal
    case -- production serving is sparse, not daily) is a silent no-op,
    except that `races_observed` is still explicitly set to 0 on the
    returned outcome (see `_CategoryDateOutcome`'s own docstring on why this
    matters for the caller's serving-gap check).

    Every run visited here (freshly created or reused) is left `FINISHED`
    (via `client.set_terminated`) before this function returns, regardless
    of whether the base-tracking pass and/or the eval pass actually logged
    anything this call -- mirroring `timeline.upsert_timeline_point`'s own
    "FINISHED after every call" philosophy (see that function's docstring):
    MLflow run status is orthogonal to this module's `sync_base_logged`/
    `sync_eval_logged` idempotency tags, and a run left permanently RUNNING
    (the previous behavior -- this module never called `set_terminated` at
    all) is simply wrong, not a meaningful "still in progress" signal.

    Also computes the `races_live`/`races_backfilled`/`champion_model_version`/
    `champion_live_races`/`served_model_versions_live` fields documented on
    `_CategoryDateOutcome` -- the champion label is resolved fresh, once per
    call (never cached across calls), so a champion-alias change is reflected
    the very next `sync_production_range` call, not just for newly-created
    runs.

    `races_scheduled`/`coverage_pct` (2026-07-11) are computed here EXACTLY
    ONCE per (date, category) call -- via `serve_eval.fetch_races_scheduled`/
    `_coverage_pct` -- BEFORE the early-empty-`rows` return, since the
    race-calendar oracle query is independent of whether any Neon prediction
    rows exist at all (see `_CategoryDateOutcome`'s own docstring). Both
    values are then threaded down into EVERY model-version-group's own
    `_log_base_tracking`/`_sync_fp_eval` calls below (duplicated per group,
    not per-day-once -- see `_log_base_tracking`'s own docstring for why
    that duplication is a deliberate, documented judgment call).
    """
    outcome = _CategoryDateOutcome()
    outcome.races_scheduled = serve_eval.fetch_races_scheduled(local_conn, category, date_str)
    source = serve_eval.resolve_source(category)
    raw_rows = serve_eval.fetch_fp_prediction_rows(neon_conn, source, date_str, date_str)
    rows = _fp_category_filtered_rows(category, serve_eval.filter_genuine_rows(raw_rows))
    if not rows:
        outcome.races_observed = 0
        outcome.coverage_pct = _coverage_pct(outcome.races_live, outcome.races_scheduled)
        return outcome
    outcome.races_observed = _distinct_race_count(rows)
    live_rows, backfill_rows = serve_eval.partition_live_backfill(rows)
    outcome.races_live = _distinct_race_count(live_rows)
    outcome.races_backfilled = _distinct_race_count(backfill_rows)
    outcome.coverage_pct = _coverage_pct(outcome.races_live, outcome.races_scheduled)
    champion_label = _resolve_champion_label(client, category, "finish-position")
    outcome.champion_model_version = champion_label

    served_model_versions_live: set[str] = set()
    for model_version, group_rows in serve_eval.group_by_model_version(rows).items():
        sync_key = f"{date_str}:{category}:{model_version}"
        run_id, created, existing_tags = _get_or_create_run_and_tags(
            client,
            experiment_id,
            sync_key=sync_key,
            task="finish-position",
            category=category,
            model_version=model_version,
            date_str=date_str,
        )
        if created:
            outcome.runs_created += 1
        else:
            outcome.runs_reused += 1

        if existing_tags.get(SYNC_BASE_LOGGED_TAG) != TRUE_STR:
            _log_base_tracking(
                client,
                run_id,
                task="finish-position",
                metric_prefix="fp",
                category=category,
                date_str=date_str,
                model_version=model_version,
                rows=group_rows,
                prediction_table=_fp_prediction_table(group_rows),
                races_scheduled=outcome.races_scheduled,
                coverage_pct=outcome.coverage_pct,
            )

        if existing_tags.get(SYNC_EVAL_LOGGED_TAG) != TRUE_STR:
            logged, trace_summary = _sync_fp_eval(
                client,
                run_id,
                tracing_client=tracing_client,
                experiment_id=experiment_id,
                category=category,
                date_str=date_str,
                model_version=model_version,
                rows=group_rows,
                races_scheduled=outcome.races_scheduled,
                coverage_pct=outcome.coverage_pct,
                local_conn=local_conn,
            )
            if logged:
                outcome.eval_logged += 1
            else:
                outcome.eval_skipped_no_results += 1
            outcome.traces_created += trace_summary.traces_created
            outcome.traces_already_existed += trace_summary.traces_already_existed
            outcome.trace_errors.extend(trace_summary.errors)

        group_live_rows, _group_backfill_rows = serve_eval.partition_live_backfill(group_rows)
        if group_live_rows:
            served_model_versions_live.add(model_version)
            # Widened (2026-07-10) to champion-OR-variant -- see
            # `_is_champion_or_variant`'s own docstring: a variant-only-
            # served day must not be miscounted as "champion did not serve".
            if _is_champion_or_variant(model_version, champion_label):
                outcome.champion_live_races += _distinct_race_count(group_live_rows)

        # Always leave this run FINISHED before moving to the next
        # model_version group -- see this function's own docstring. Runs
        # both just-created above and reused from a prior call/date hit this
        # same line; a status refresh on an already-FINISHED run is harmless.
        client.set_terminated(run_id, status="FINISHED")

    outcome.served_model_versions_live = frozenset(served_model_versions_live)
    return outcome


def _sync_rs_category_date(
    client: MlflowClient,
    tracing_client: TracingClient | None,
    experiment_id: str,
    neon_conn: db.ConnectionLike,
    local_conn: db.ConnectionLike,
    category: str,
    date_str: str,
) -> _CategoryDateOutcome:
    """Sync every genuinely-served RS model_version group for (category,
    date_str). Only ever called for `category in RS_CATEGORIES` by the
    caller -- Ban-ei has no running-style rows to sync.

    Sets `races_observed`/`races_live`/`races_backfilled`/
    `champion_model_version`/`champion_live_races`/`served_model_versions_live`
    and terminates every visited run FINISHED for exactly the same reasons as
    `_sync_fp_category_date`'s own docstring (this function is its RS-side
    twin) -- see that docstring for the full rationale, not repeated here.
    """
    outcome = _CategoryDateOutcome()
    source = serve_eval.resolve_source(category)
    raw_rows = serve_eval.fetch_rs_prediction_rows(neon_conn, source, date_str, date_str)
    rows = serve_eval.filter_genuine_rows(raw_rows)
    if not rows:
        outcome.races_observed = 0
        return outcome
    outcome.races_observed = _distinct_race_count(rows)
    live_rows, backfill_rows = serve_eval.partition_live_backfill(rows)
    outcome.races_live = _distinct_race_count(live_rows)
    outcome.races_backfilled = _distinct_race_count(backfill_rows)
    champion_label = _resolve_champion_label(client, category, "running-style")
    outcome.champion_model_version = champion_label

    served_model_versions_live: set[str] = set()
    for model_version, group_rows in serve_eval.group_by_model_version(rows).items():
        sync_key = f"{date_str}:{category}:{model_version}"
        run_id, created, existing_tags = _get_or_create_run_and_tags(
            client,
            experiment_id,
            sync_key=sync_key,
            task="running-style",
            category=category,
            model_version=model_version,
            date_str=date_str,
        )
        if created:
            outcome.runs_created += 1
        else:
            outcome.runs_reused += 1

        if existing_tags.get(SYNC_BASE_LOGGED_TAG) != TRUE_STR:
            _log_base_tracking(
                client,
                run_id,
                task="running-style",
                metric_prefix="rs",
                category=category,
                date_str=date_str,
                model_version=model_version,
                rows=group_rows,
                prediction_table=_rs_prediction_table(group_rows),
            )

        if existing_tags.get(SYNC_EVAL_LOGGED_TAG) != TRUE_STR:
            logged, trace_summary = _sync_rs_eval(
                client,
                run_id,
                tracing_client=tracing_client,
                experiment_id=experiment_id,
                category=category,
                date_str=date_str,
                model_version=model_version,
                rows=group_rows,
                local_conn=local_conn,
            )
            if logged:
                outcome.eval_logged += 1
            else:
                outcome.eval_skipped_no_results += 1
            outcome.traces_created += trace_summary.traces_created
            outcome.traces_already_existed += trace_summary.traces_already_existed
            outcome.trace_errors.extend(trace_summary.errors)

        group_live_rows, _group_backfill_rows = serve_eval.partition_live_backfill(group_rows)
        if group_live_rows:
            served_model_versions_live.add(model_version)
            # Widened (2026-07-10) to champion-OR-variant -- see
            # `_is_champion_or_variant`'s own docstring: a variant-only-
            # served day must not be miscounted as "champion did not serve".
            if _is_champion_or_variant(model_version, champion_label):
                outcome.champion_live_races += _distinct_race_count(group_live_rows)

        # See _sync_fp_category_date's matching comment: always FINISHED,
        # created or reused, before moving to the next model_version group.
        client.set_terminated(run_id, status="FINISHED")

    outcome.served_model_versions_live = frozenset(served_model_versions_live)
    return outcome


def _find_serving_gap_run(
    client: MlflowClient, experiment_id: str, serving_gap_key: str
) -> str | None:
    """Find the run tagged with `serving_gap_key`, mirroring
    `_find_sync_run`'s exact tag-search idiom for the serving-gap marker
    family (keyed by (date, category) alone) rather than the base sync run
    family (keyed by (date, category, model_version))."""
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{SERVING_GAP_KEY_TAG} = '{serving_gap_key}'",
        max_results=1,
    )
    return matches[0].info.run_id if matches else None


def _log_serving_gap(
    client: MlflowClient,
    experiment_id: str,
    *,
    date_str: str,
    category: str,
    gap_source: str,
    gap_type: str,
    expected_races: int,
    fp_races_observed: int,
    fp_races_live: int,
    fp_races_backfilled: int,
    fp_races_scheduled: int,
    fp_coverage_pct: float | None = None,
) -> None:
    """Find-or-create the serving-gap marker run for (date_str, category) in
    `experiment_id` (always `config.EXPERIMENT_FP_PRODUCTION_USAGE` -- the
    gap is about a MISSING finish-position side, so it belongs alongside the
    FP usage runs, not the RS ones that DID get served), log its metrics, and
    leave it FINISHED.

    Idempotent via a `serving_gap_key = "{date_str}:{category}"` tag search
    (mirroring `_find_sync_run`'s idiom exactly, with a different tag), so a
    daily cron re-running the same overlapping range every day while a gap
    persists across multiple days never creates a second marker run for the
    same (date_str, category) -- see `sync_production_range`'s own docstring
    on the serving-gap check. Metrics/tags are (re-)logged on every call
    regardless of whether the run was just created or found -- unlike
    `sync_base_logged`'s artifact-append-duplication concern, a plain metric
    value has no such hazard, and re-logging keeps `expected_races` current
    if the gap's shape changes across days (e.g. more rows land for the same
    date on a later call).

    Widened (2026-07-09) beyond its original jra/nar-only RS-vs-FP shape:
    `gap_source` (one of `GAP_SOURCE_RUNNING_STYLE`/`GAP_SOURCE_RACE_CALENDAR`,
    see `_resolve_expected_races`) names WHICH "races expected" oracle
    flagged this gap -- the original RS-vs-FP comparison for jra/nar
    (`expected_races` == that day's RS `races_observed`), or the local
    replica's own nvd_ra race calendar for banei (running-style has no
    Ban-ei model at all). `gap_type` (`GAP_TYPE_NO_ROWS`/
    `GAP_TYPE_BACKFILL_ONLY`) distinguishes a day with literally zero FP rows
    from one where FP rows exist but NONE of them are genuinely live serving
    (every row that landed was a delayed backfill re-prediction, see
    `serve_eval.partition_live_backfill`) -- both are real outages from a
    live-serving perspective, but a backfill-only day would previously make
    `fp_races_observed` nonzero and hide the gap entirely (see this
    function's caller in `sync_production_range`).

    `rs_races_observed` is additionally logged (with the same value as
    `expected_races`) whenever `gap_source == GAP_SOURCE_RUNNING_STYLE`,
    purely for backward compatibility with dashboards/queries built against
    this metric's original (pre-widening) name -- new callers should read
    `expected_races` instead, since it is the only metric name meaningful
    for BOTH sources.

    `run_name` is set at creation to `"gap {date_str} {category} {gap_type}"`
    -- matching the display name already carried by every pre-existing marker
    run in the real store, same rationale as `_get_or_create_run_and_tags`'s
    own `run_name` note.

    `fp_races_scheduled`/`fp_coverage_pct` (2026-07-11) are the
    `serve_eval.fetch_races_scheduled` race-calendar oracle count and the
    resulting coverage ratio for this (date_str, category) -- see
    `GAP_TYPE_PARTIAL_COVERAGE`'s own module-level comment. `fp_races_
    scheduled` is REQUIRED (never None): both callers of this function (the
    pre-existing no_rows/backfill_only check and the new partial_coverage
    check, see `sync_production_range`) always have a real `fp_outcome.
    races_scheduled` int in hand (computed unconditionally in
    `_sync_fp_category_date`) by the time they call this, so every marker run
    this function logs -- regardless of which `gap_type` fired -- carries the
    same enriched context. `fp_coverage_pct` stays optional/nullable, since
    it IS genuinely None whenever `fp_races_scheduled` is 0 (an undefined
    ratio, see `_coverage_pct`'s own docstring) -- skipped, never logged as a
    fabricated 0.0, in that case.
    """
    serving_gap_key = f"{date_str}:{category}"
    run_id = _find_serving_gap_run(client, experiment_id, serving_gap_key)
    if run_id is None:
        run = client.create_run(
            experiment_id,
            run_name=f"gap {date_str} {category} {gap_type}",
            tags={
                SERVING_GAP_KEY_TAG: serving_gap_key,
                SERVING_GAP_TAG: TRUE_STR,
                "gap_date": date_str,
                "category": category,
            },
        )
        run_id = run.info.run_id
    metrics = [
        Metric("fp_races_observed", float(fp_races_observed), 0, 0),
        Metric("fp_races_live", float(fp_races_live), 0, 0),
        Metric("fp_races_backfilled", float(fp_races_backfilled), 0, 0),
        Metric("expected_races", float(expected_races), 0, 0),
        Metric("fp_races_scheduled", float(fp_races_scheduled), 0, 0),
    ]
    if gap_source == GAP_SOURCE_RUNNING_STYLE:
        metrics.append(Metric("rs_races_observed", float(expected_races), 0, 0))
    if fp_coverage_pct is not None:
        metrics.append(Metric("fp_coverage_pct", float(fp_coverage_pct), 0, 0))
    log_batch_chunked(
        client,
        run_id,
        metrics=metrics,
        tags=[RunTag("gap_source", gap_source), RunTag("gap_type", gap_type)],
    )
    client.set_terminated(run_id, status="FINISHED")


def _resolve_expected_races(
    category: str,
    rs_outcome: _CategoryDateOutcome | None,
    local_conn: db.ConnectionLike,
    date_str: str,
    *,
    fp_races_scheduled: int,
) -> tuple[int, str] | None:
    """Return `(expected_races, gap_source)` for the serving-gap check on
    (date_str, category) this call, or None when there is nothing to compare
    FP against this call.

    jra/nar (`category in RS_CATEGORIES`), RS genuinely observed something
    (`rs_outcome.races_observed > 0`): `expected_races` is this call's RS
    sync `races_observed` for the same (date_str, category), tagged
    `GAP_SOURCE_RUNNING_STYLE`. `rs_outcome is None` (the RS sync itself
    failed/raised this call, see `sync_production_range`'s isolation) returns
    None outright -- an unknown RS race count must never be misread as "zero
    expected", which would incorrectly suppress a real gap.

    jra/nar, BOTH pipelines dark (2026-07-11 -- closes a residual blind
    spot): `rs_outcome.races_observed == 0` too, so the RS-vs-FP proxy has
    nothing of its own to compare FP against (real examples: JRA
    2026-06-13/14/20/21/28, `races_live == 0` for both finish-position AND
    running-style simultaneously that day). Rather than reporting an
    oracle of 0 from the RS proxy alone (which would make the caller's own
    `expected_races > 0` gate silently treat a genuine outage the same as an
    ordinary no-races day), this falls back to `fp_races_scheduled` -- the
    SAME race-calendar oracle banei's own branch below uses
    (`serve_eval.fetch_races_scheduled`, already computed once,
    unconditionally, by `_sync_fp_category_date` for this exact (date_str,
    category) and threaded down by the caller rather than re-queried here) --
    tagged the same `GAP_SOURCE_RACE_CALENDAR`. When the calendar itself is
    also empty (genuinely no races scheduled that day at all), this still
    returns that 0 rather than None -- mirroring banei's own convention of
    never special-casing a zero count -- since the caller's own
    `expected_races > 0` gate already treats that correctly as "not a gap"
    without this function needing to encode that rule itself.

    banei: running-style has no Ban-ei model at all (see `RS_CATEGORIES`), so
    there is no RS-served count to compare against -- `expected_races`
    instead comes from the local replica's own nvd_ra race calendar (see
    `serve_eval.fetch_banei_race_count`), tagged `GAP_SOURCE_RACE_CALENDAR`,
    independent of whether anything was ever served for those races.

    No fallback branch for an unrecognized category: this is only ever
    called with a non-None `fp_outcome` (see the caller), which itself is
    only non-None when `_sync_fp_category_date` -- and therefore
    `serve_eval.resolve_source(category)` -- already succeeded upstream this
    same call; that function accepts exactly "jra"/"nar"/"banei" and raises
    ValueError otherwise, so by this point `category` is guaranteed to be one
    of the two branches below.

    Otherwise UNCHANGED by the 2026-07-11 `GAP_TYPE_PARTIAL_COVERAGE`
    addition (see that constant's own module-level comment): that new check
    reads `fp_outcome.races_scheduled` directly off `fp_outcome` (never
    calling this function at all), so this function's own oracle selection
    keeps its pre-existing shape -- the only change here is the new
    both-dark branch above, which reuses the SAME value rather than
    introducing a second, independent query.
    """
    if category in RS_CATEGORIES:
        if rs_outcome is None:
            return None
        if rs_outcome.races_observed > 0:
            return rs_outcome.races_observed, GAP_SOURCE_RUNNING_STYLE
        return fp_races_scheduled, GAP_SOURCE_RACE_CALENDAR
    return serve_eval.fetch_banei_race_count(local_conn, date_str), GAP_SOURCE_RACE_CALENDAR


def _check_champion_gap(outcome: _CategoryDateOutcome) -> bool:
    """True when `outcome` had genuinely-served LIVE rows this call but NONE
    of them came from the currently-registered champion model_version OR a
    cell-routed VARIANT of it (`outcome.champion_live_races` is already
    accumulated champion-OR-variant-inclusive -- see `_is_champion_or_variant`
    and `_CategoryDateOutcome`'s own docstring for the 2026-07-10 widening).

    False both when there were no live rows at all this call (nothing to
    compare -- a plain no-service day, already covered by the serving-gap
    check) and when there is no resolvable champion alias at all for this
    (category, task) (comparing served rows against nothing is not a
    mismatch, it is simply "no champion set yet").
    """
    if outcome.races_live == 0:
        return False
    if outcome.champion_model_version is None:
        return False
    return outcome.champion_live_races == 0


def _find_champion_gap_run(
    client: MlflowClient, experiment_id: str, champion_gap_key: str
) -> str | None:
    """Find the run tagged with `champion_gap_key`, mirroring
    `_find_serving_gap_run`'s exact tag-search idiom for the champion-gap
    marker family (keyed by (date, category, task))."""
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{CHAMPION_GAP_KEY_TAG} = '{champion_gap_key}'",
        max_results=1,
    )
    return matches[0].info.run_id if matches else None


def _log_champion_gap(
    client: MlflowClient,
    experiment_id: str,
    *,
    date_str: str,
    category: str,
    task: str,
    champion_model_version: str | None,
    served_model_versions: frozenset[str],
) -> None:
    """Find-or-create the champion-mismatch marker run for (date_str,
    category, task) in `experiment_id` (the task's own production-usage
    experiment -- FP or RS -- since, unlike the serving-gap family, a
    champion mismatch is meaningful independently for each task).

    Idempotent via a `champion_gap_key = "{date_str}:{category}:{task}"` tag
    search, exactly mirroring `_log_serving_gap`'s idiom: a persistent
    mismatch (a production rollback silently stuck serving a superseded
    challenger variant for weeks, the real motivating incident -- see
    `_check_champion_gap`'s docstring) synced every day over an overlapping
    range never creates a second marker run for the same key. `served_model_
    versions` (re-logged every call, since which non-champion versions
    served can shift day to day) is the diagnostic payload naming what WAS
    actually served instead of the champion, sorted for a deterministic tag
    value.

    `CHAMPION_SERVED_TAG` is set to the static value FALSE_STR ("false") ONCE
    at creation time here, never re-logged -- this marker run's very reason
    to exist is that `_check_champion_gap` found ZERO champion-OR-variant
    live coverage that day (see the widened `champion_live_races`
    accumulation in `_sync_fp_category_date`/`_sync_rs_category_date`), so
    "false" is always the correct static fact for it. This is a DIFFERENT
    run family from the one `_champion_served_variant_tag` tags with
    CHAMPION_SERVED_VARIANT_VALUE ("variant") -- see this tag's own module-
    level comment for why the two never collide despite sharing a tag key.

    `run_name` is set at creation to `"champion-gap {date_str} {category}"`
    (deliberately without `task`, unlike the tag-based `champion_gap_key`) --
    matching the display name already carried by every pre-existing marker
    run in the real store, same rationale as `_get_or_create_run_and_tags`'s
    own `run_name` note.
    """
    champion_gap_key = f"{date_str}:{category}:{task}"
    run_id = _find_champion_gap_run(client, experiment_id, champion_gap_key)
    if run_id is None:
        run = client.create_run(
            experiment_id,
            run_name=f"champion-gap {date_str} {category}",
            tags={
                CHAMPION_GAP_KEY_TAG: champion_gap_key,
                CHAMPION_GAP_TAG: TRUE_STR,
                CHAMPION_SERVED_TAG: FALSE_STR,
                "gap_date": date_str,
                "category": category,
                "task": task,
            },
        )
        run_id = run.info.run_id
    log_batch_chunked(
        client,
        run_id,
        tags=[
            RunTag("champion_model_version", champion_model_version or "none"),
            RunTag("served_model_versions", ",".join(sorted(served_model_versions)) or "none"),
        ],
    )
    client.set_terminated(run_id, status="FINISHED")


# ── Self-heal: stale RUNNING runs from an interrupted invocation ───────────
#
# Real incident (2026-07-11): an interrupted `sync-production` process (killed
# mid-loop) left 18 runs in the two production-usage experiments tagged
# `sync_base_logged=true` but stuck `status=RUNNING` forever -- every run this
# module creates is left FINISHED at the end of its own model_version-group
# iteration (see `_sync_fp_category_date`'s own docstring), but a process
# killed BETWEEN `_log_base_tracking` setting that tag and this module's own
# `client.set_terminated(...)` call a few lines later never reaches that
# statement. This hid 54% of that experiment's volume from any
# `status=FINISHED` filter/dashboard. `sync_production_range` runs this sweep
# ONCE, at the very start of every call (see its own docstring), rather than
# fixing the specific 18 already-stuck runs itself (a separate, one-time
# manual repair, out of scope here) -- this is the CODE fix so the same
# failure mode never silently recurs.

_STALE_RUNNING_SEARCH_FILTER: Final[str] = (
    f"attributes.status = 'RUNNING' AND tags.{SYNC_BASE_LOGGED_TAG} = '{TRUE_STR}'"
)


def _stale_running_cutoff_ms(hours: float) -> int:
    """Return the epoch-millisecond cutoff for the self-heal sweep: a run
    with `run.info.start_time` older (smaller) than this value has been
    RUNNING for at least `hours` hours and is old enough to be considered
    abandoned rather than still genuinely in progress.

    Always computed against the REAL wall clock (`datetime.now(UTC)`) --
    unlike e.g. `timeline.upsert_timeline_point`'s historical-date metric
    timestamps, there is no "as-of a past date" notion for this sweep: it
    always answers "how stale is this run RIGHT NOW", at the moment
    `sync_production_range` happens to run.
    """
    cutoff = datetime.now(UTC) - timedelta(hours=hours)
    return int(cutoff.timestamp() * 1000)


def _heal_stale_running_runs(client: MlflowClient, experiment_id: str, cutoff_ms: int) -> int:
    """Force-terminate FINISHED every run in `experiment_id` that is BOTH
    still `status="RUNNING"` AND has `run.info.start_time < cutoff_ms`
    (searched via `_STALE_RUNNING_SEARCH_FILTER`, which additionally requires
    `sync_base_logged=true` -- see that constant's own comment for why this
    scopes the sweep to exactly the run family the real incident hit, never
    the marker-run families). Returns the count healed.

    Paginates via `page_token` until exhausted, mirroring
    `backfill_traces._find_evaluated_runs`'s exact idiom -- the real store
    could plausibly accumulate more stuck runs than fit in one page over a
    long-enough outage.

    The `run.info.status == "RUNNING"` check is deliberately re-verified
    here in code, not left to the search filter alone: this keeps the
    function's own contract self-evidently correct (a caller reading this
    function never has to trust an external filter string to know it will
    never touch an already-FINISHED run), and is exercised directly by this
    module's own test suite via a monkeypatched `search_runs` returning a
    mixed RUNNING/FINISHED result set.

    Never called with `cutoff_ms` computed from anything other than
    `_stale_running_cutoff_ms` in practice -- passed in rather than computed
    here so a caller (or a test) can supply a deterministic value without
    monkeypatching wall-clock time.
    """
    healed = 0
    page_token: str | None = None
    while True:
        page = client.search_runs(
            [experiment_id],
            filter_string=_STALE_RUNNING_SEARCH_FILTER,
            max_results=1000,
            page_token=page_token,
        )
        for run in page:
            if run.info.status == "RUNNING" and run.info.start_time < cutoff_ms:
                client.set_terminated(run.info.run_id, status="FINISHED")
                healed += 1
        page_token = page.token
        if not page_token:
            break
    return healed


def sync_production_range(
    client: MlflowClient,
    date_from: str,
    date_to: str,
    categories: Sequence[str] = FP_CATEGORIES,
    *,
    emit_traces: bool = True,
    partial_coverage_threshold: float = DEFAULT_PARTIAL_COVERAGE_THRESHOLD,
    repair_stale_running: bool = True,
    stale_running_hours: float = DEFAULT_STALE_RUNNING_HOURS,
    neon_connect: Callable[[], db.ConnectionLike] = db.connect_racing_neon,
    local_connect: Callable[[], db.ConnectionLike] = db.connect_local_replica,
) -> SyncProductionSummary:
    """Sync genuinely-served production predictions over [date_from, date_to]
    into MLflow, evaluating against finalized results where available.

    For each date in the inclusive range, for each requested category:
    finish-position is always synced (any of "jra"/"nar"/"banei"); running-
    style is additionally synced only when the category is also in
    `RS_CATEGORIES` (Ban-ei has no running-style rows at all). Each sync
    produces at most one run per (date, category, model_version) in
    `config.EXPERIMENT_FP_PRODUCTION_USAGE` / `config.EXPERIMENT_RS_PRODUCTION_USAGE`,
    identified by a deterministic `sync_key = "{date}:{category}:{model_version}"`
    tag, so repeated calls over overlapping ranges (the realistic daily-
    automation shape: "yesterday+today", every day) are cheap and correct:

    - `sync_base_logged` gates the base usage-tracking parts (tags, fp_races/
      fp_horses or rs_races/rs_horses metrics, and the predictions.json/
      .parquet artifact). Predictions are immutable once generated, so once
      this is "true" it is NEVER re-logged for that sync_key -- re-logging
      would silently duplicate rows in the prediction-table artifact, since
      `client.log_table` appends rather than overwrites.
    - `sync_eval_logged` gates the evaluation join (results/metadata query,
      fp_*/rs_* eval metrics, the eval.json/.parquet artifact, and the
      `timelines` upsert). While absent, EVERY call re-attempts the join --
      cheap, a single-date local-replica query -- so a prediction published
      a few days ahead of race day (this genuinely happens) gets its
      evaluation filled in on a LATER call once results become final,
      instead of being permanently stuck with none. Once "true", it is
      never re-attempted, for the same log_table append-duplication reason
      as `sync_base_logged`.

    `emit_traces` (default True) now GENUINELY gates trace emission
    (2026-07-10 -- see this module's own docstring's "★ MLflow traces"
    section and `trace_emit.py`'s module docstring for the full before/
    after story of why this used to be a documented no-op and no longer
    is). When True, a `trace_emit.TracingClient` is built once for this
    whole call (`trace_emit.build_tracing_client(client)` -- resolved from
    `client`'s OWN explicit `tracking_uri`, never ambient/global state) and
    threaded down to `_sync_fp_eval`/`_sync_rs_eval`, which each emit one
    trace (+ Feedback assessments) per race/horse the SAME pass they
    compute eval metrics (see those functions' own docstrings). When False,
    no `TracingClient` is even constructed, and no `trace_emit` call is
    ever made -- the `sync-production` CLI's `--no-traces` flag threads
    `emit_traces=False` here for exactly this reason. The per-race/
    per-horse `predictions.json`/`eval.json` table artifacts this module
    already logs remain the audit trail of record regardless of this flag
    (traces are an ADDITIONAL Usage/Quality/Tool-calls-page view onto the
    same underlying rows, not a replacement for the table artifacts).

    `neon_connect`/`local_connect` are each called exactly ONCE for the
    whole date range (not once per date), in a try/finally that always
    closes both connections. Tests always inject fakes here -- this
    function never opens a real network connection on its own initiative.

    Every individual (date, category, task) sync is isolated: a failure
    (`ValueError`/`KeyError`/`TypeError` from malformed data, `psycopg2.Error`
    from a live query, `MlflowException` from the tracking store) is caught,
    recorded as a descriptive string in `summary.errors`, and the loop
    continues -- one bad date/category/task never aborts the rest of the
    range, mirroring `backfill_serve_timeline.py`'s "isolate, don't abort"
    philosophy (that module uses tagged-outcome returns instead of
    exception isolation; this module's per-model_version-group control flow
    has more independent failure points, so exception isolation reads
    clearer here).

    After the FP sync for a given (date, category) completes, this function
    also checks for a SERVING GAP via `_resolve_expected_races`: for jra/nar,
    `rs_outcome.races_observed > 0 and fp_outcome.races_live == 0`, i.e.
    running-style was genuinely served for that exact (date, category) but
    finish-position was not LIVE -- a real incident observed on 2026-07-04
    (JRA had 12 races with RS predictions logged and ZERO FP rows the same
    day). For banei -- which has no running-style model at all, so there is
    no RS side to compare against -- the same check instead compares against
    the local replica's own nvd_ra race calendar (`serve_eval.
    fetch_banei_race_count`): a real, still-open incident (Ban-ei dark since
    2026-05-24) that the original RS-vs-FP-only check could never see, since
    it skipped banei entirely (banei is FP-eligible but not RS-eligible).
    Either way, neither side's absence is an error on its own -- production
    serving is sparse by design (see `_sync_fp_category_date`'s own
    docstring) -- only a genuine EXPECTED-but-not-LIVE mismatch is a gap.

    ★ Both-pipelines-dark fallback (2026-07-11, see `_resolve_expected_races`'s
    own docstring for the full three-way branch): the jra/nar RS-vs-FP
    comparison above assumes RS itself observed something to compare FP
    against. On a day where RS ALSO observed zero races (real examples: JRA
    2026-06-13, 06-14, 06-20, 06-21, 06-28 -- `races_live == 0` for BOTH
    finish-position and running-style simultaneously), that RS proxy is
    itself 0, so the comparison had nothing to compare against and never
    fired at all, even though races may genuinely have been scheduled that
    day. `_resolve_expected_races` now falls back, in exactly that case, to
    `fp_outcome.races_scheduled` -- the SAME race-calendar oracle already
    used for banei -- so a genuinely-abandoned day still gets checked
    against "were races actually scheduled" instead of silently passing.
    `gap_type` is still selected the same way as before (`GAP_TYPE_NO_ROWS`
    vs `GAP_TYPE_BACKFILL_ONLY`, off `fp_outcome.races_observed`); only the
    `gap_source` differs (`GAP_SOURCE_RACE_CALENDAR` instead of
    `GAP_SOURCE_RUNNING_STYLE`) on the days this fallback is what fired.

    The check reads `fp_outcome.races_live`, not `fp_outcome.races_observed`:
    a date with FP rows that are ALL backfill (delayed re-predictions, see
    `serve_eval.partition_live_backfill`) previously looked "served" even
    though nothing was live that day -- `summary` distinguishes the two via
    `gap_type` on the logged marker run (`GAP_TYPE_NO_ROWS` for literally zero
    FP rows, `GAP_TYPE_BACKFILL_ONLY` for rows that exist but are all
    backfill). A detected gap is surfaced two ways: a `warnings.warn` naming
    the date/category/counts (mirroring `champion_cell_eval.
    eval_champion_cells`'s own non-fatal-but-worth-surfacing convention), and
    an idempotent marker run in `config.EXPERIMENT_FP_PRODUCTION_USAGE`
    (find-or-create by a `serving_gap_key = "{date}:{category}"` tag, exactly
    like `sync_key`'s own idiom, so a daily cron re-running the same range
    every day while a gap persists never creates a second marker for the
    same pair) -- `summary.serving_gaps_detected` counts how many such pairs
    this call observed. This check (both `_resolve_expected_races`'s own
    query and `_log_serving_gap`) is isolated in its own try/except, using
    the same `_ISOLATED_EXCEPTIONS` tuple as the fp/rs syncs above: a
    transient failure (e.g. the banei race-calendar query, or a flaky
    tracking-store write while logging the marker) must never abort the rest
    of the date range, same as everywhere else in this function.

    ★ PARTIAL-COVERAGE check (2026-07-11, `GAP_TYPE_PARTIAL_COVERAGE`, see
    that constant's own module-level comment for the full incident): the
    two checks above are BOTH gated on `fp_outcome.races_live == 0` -- a day
    where SOME races serve live, no matter how few, is invisible to either
    one. This closes that blind spot with a genuinely INDEPENDENT oracle:
    `fp_outcome.races_scheduled` (`serve_eval.fetch_races_scheduled`'s direct
    jvd_ra/nvd_ra race-calendar count, computed for ALL THREE categories --
    not just banei -- unconditionally in `_sync_fp_category_date`, so it
    never depends on `_resolve_expected_races`/the RS sync succeeding this
    call the way the no_rows/backfill_only check above does). Whenever
    `fp_outcome.races_live > 0` (the territory the two checks above
    deliberately do NOT cover) and `fp_outcome.coverage_pct` (`races_live /
    races_scheduled * 100`) is not None and falls below
    `partial_coverage_threshold` (default `DEFAULT_PARTIAL_COVERAGE_THRESHOLD`
    = 80.0, configurable via the `sync-production` CLI's
    `--partial-coverage-threshold` flag), this logs a `GAP_TYPE_PARTIAL_
    COVERAGE` marker the same two ways as the checks above (a `warnings.warn`
    plus an idempotent `_log_serving_gap` marker run) and increments the
    SAME `summary.serving_gaps_detected` counter. Because this check
    requires `races_live > 0` while the no_rows/backfill_only check above
    requires `races_live == 0`, the two are MUTUALLY EXCLUSIVE per (date,
    category) per call -- both safely reuse `_log_serving_gap`'s one
    marker-run-per-(date, category) idempotency key without ever
    overwriting each other's `gap_type` tag. Isolated in its own try/except,
    same `_ISOLATED_EXCEPTIONS` tuple, same non-abort-the-range guarantee.

    Independently, after EACH of the FP sync and (when applicable) the RS
    sync for a (date, category), this function checks for a CHAMPION
    MISMATCH via `_check_champion_gap`: live rows were genuinely served that
    day for that task, but NONE of them came from the CURRENT champion
    model_version OR a CELL-ROUTED VARIANT of it (`_is_champion_or_variant`,
    widened 2026-07-10 -- see that function's own docstring; e.g. per-cell
    routing dispatching some races/horses to a build labeled
    `f"{champion_label}-jockey-pedigree269"` no longer, on its own, counts as
    a mismatch). A genuine mismatch is e.g. JRA silently served only a
    superseded CHALLENGER build (an entirely different, unrelated
    model_version, not a variant of the current champion) for weeks after a
    rollback. A detected mismatch is surfaced the same two ways as a serving
    gap -- a `warnings.warn`, and an idempotent marker run
    (`champion_gap_key = "{date}:{category}:{task}"`, in the task's own
    FP/RS production-usage experiment) -- with `summary.champion_gaps_
    detected` counting how many (date, category, task) triples this call
    observed, and isolated the same way.

    ★ SELF-HEAL sweep for interrupted invocations (2026-07-11, see
    `_heal_stale_running_runs`'s own docstring for the mechanics): BEFORE any
    date/category is visited, this function runs that sweep once against
    `config.EXPERIMENT_FP_PRODUCTION_USAGE` (always) and
    `config.EXPERIMENT_RS_PRODUCTION_USAGE` (only when `categories` includes
    an `RS_CATEGORIES` member -- preserving this function's own "never touch
    the RS experiment for an RS-ineligible-only call" invariant, same
    rationale as the lazy `rs_experiment_id` resolution below), gated by
    `repair_stale_running` (default True). The real incident this closes: an
    interrupted process left 18 runs tagged `sync_base_logged=true` stuck
    `status=RUNNING` forever, hiding 54% of one experiment's volume from any
    `status=FINISHED` filter. Any such run whose `start_time` is older than
    `stale_running_hours` (default `DEFAULT_STALE_RUNNING_HOURS` = 6.0,
    configurable via the `sync-production` CLI's `--stale-running-hours`
    flag) is force-terminated FINISHED via `client.set_terminated` -- old
    enough that it can only be genuinely abandoned, since this whole call's
    OWN runs do not exist yet at the moment the sweep runs, and a
    realistically-overlapping second invocation would be at most minutes
    old, nowhere near the threshold. `summary.stale_running_healed` counts
    how many runs this call healed (across both experiments); a sweep
    failure is isolated (same `_ISOLATED_EXCEPTIONS`, folded into
    `summary.errors`) exactly like every other failure point in this
    function, never aborting the date/category loop that follows. Pass
    `repair_stale_running=False` (the CLI's `--no-repair-stale-running`) to
    skip the sweep entirely for this call.

    Raises ValueError for an invalid `date_from`/`date_to` (see
    `timeline.validate_yyyymmdd`) or an inverted range (`date_to < date_from`).
    """
    timeline.validate_yyyymmdd(date_from)
    timeline.validate_yyyymmdd(date_to)
    if date_to < date_from:
        raise ValueError(f"date_to ({date_to!r}) must not be before date_from ({date_from!r})")

    dates = _date_range_yyyymmdd(date_from, date_to)
    fp_runs_created = 0
    fp_runs_reused = 0
    fp_eval_logged = 0
    fp_eval_skipped_no_results = 0
    rs_runs_created = 0
    rs_runs_reused = 0
    rs_eval_logged = 0
    rs_eval_skipped_no_results = 0
    serving_gaps_detected = 0
    champion_gaps_detected = 0
    traces_created = 0
    traces_already_existed = 0
    stale_running_healed = 0
    errors: list[str] = []

    # Both experiment ids are resolved lazily (on first actual use) rather
    # than eagerly up front, so requesting e.g. categories=("banei",) --
    # which is FP-eligible but never RS-eligible -- never creates the RS
    # production-usage experiment at all. The self-heal sweep just below is
    # the ONE exception on the FP side: it always needs the FP experiment
    # (every `categories` value this module ever syncs is FP-eligible), so
    # it is resolved right here rather than duplicating a second lazy path.
    fp_experiment_id: str | None = None
    rs_experiment_id: str | None = None

    # ★ Self-heal sweep for interrupted invocations (2026-07-11) -- see this
    # module's own docstring's "SELF-HEAL" section and
    # `_heal_stale_running_runs`'s own docstring for the mechanics. Runs
    # exactly ONCE, here, before any date/category is visited -- a stale run
    # left over from a PRIOR call could belong to any date that call
    # covered, so there is no natural "per (date, category)" home for this
    # sweep the way every other check below has one. Isolated per experiment
    # (same `_ISOLATED_EXCEPTIONS` as every other failure point in this
    # function), so a transient failure healing one side never blocks the
    # other side or the date/category loop that follows.
    if repair_stale_running:
        stale_running_cutoff_ms = _stale_running_cutoff_ms(stale_running_hours)
        try:
            fp_experiment_id = get_or_create_experiment(
                client, config.EXPERIMENT_FP_PRODUCTION_USAGE
            )
            stale_running_healed += _heal_stale_running_runs(
                client, fp_experiment_id, stale_running_cutoff_ms
            )
        except _ISOLATED_EXCEPTIONS as exc:
            errors.append(f"stale-running-heal:finish-position: {exc}")
        if any(category in RS_CATEGORIES for category in categories):
            try:
                rs_experiment_id = get_or_create_experiment(
                    client, config.EXPERIMENT_RS_PRODUCTION_USAGE
                )
                stale_running_healed += _heal_stale_running_runs(
                    client, rs_experiment_id, stale_running_cutoff_ms
                )
            except _ISOLATED_EXCEPTIONS as exc:
                errors.append(f"stale-running-heal:running-style: {exc}")

    # Built ONCE for this whole call (never per-date/per-category), from
    # `client`'s OWN explicit tracking_uri -- see `trace_emit.
    # build_tracing_client`'s own docstring for why this never touches
    # ambient/global tracking state. None entirely when `emit_traces=False`,
    # so `_sync_fp_eval`/`_sync_rs_eval` skip trace emission outright rather
    # than attempting-and-discarding it.
    tracing_client = trace_emit.build_tracing_client(client) if emit_traces else None

    # ★ Job-execution trace (2026-07-11) into `timelines` -- see this
    # module's own docstring's "★ MLflow traces" section for the full
    # rationale. DELIBERATELY UNCONDITIONAL (independent of `emit_traces`/
    # `--no-traces`, unlike `tracing_client` above): `--no-traces`'s own
    # docstring scopes it to the per-race/per-horse emission into the
    # production-usage experiments specifically (a potentially large trace
    # volume); this ONE lightweight, always-lands-in-`timelines` job trace
    # per `sync_production_range` call is a different, much cheaper
    # signal (one trace, one small Feedback), so it is not gated by that
    # flag. Reuses the SAME `TracingClient` when one was already built
    # above (`emit_traces=True`) rather than constructing a second one.
    job_tracing_client = (
        tracing_client if tracing_client is not None else trace_emit.build_tracing_client(client)
    )
    timelines_experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)

    neon_conn = neon_connect()
    local_conn = local_connect()
    with trace_emit.job_trace(
        job_tracing_client,
        timelines_experiment_id,
        "sync-production",
        attributes={
            "date_from": date_from,
            "date_to": date_to,
            "categories": ",".join(categories),
        },
    ) as t:
        try:
            for date_str in dates:
                for category in categories:
                    if fp_experiment_id is None:
                        fp_experiment_id = get_or_create_experiment(
                            client, config.EXPERIMENT_FP_PRODUCTION_USAGE
                        )
                    fp_outcome: _CategoryDateOutcome | None = None
                    try:
                        fp_outcome = _sync_fp_category_date(
                            client,
                            tracing_client,
                            fp_experiment_id,
                            neon_conn,
                            local_conn,
                            category,
                            date_str,
                        )
                    except _ISOLATED_EXCEPTIONS as exc:
                        errors.append(f"{date_str}:{category}:finish-position: {exc}")
                    else:
                        fp_runs_created += fp_outcome.runs_created
                        fp_runs_reused += fp_outcome.runs_reused
                        fp_eval_logged += fp_outcome.eval_logged
                        fp_eval_skipped_no_results += fp_outcome.eval_skipped_no_results
                        traces_created += fp_outcome.traces_created
                        traces_already_existed += fp_outcome.traces_already_existed
                        errors.extend(
                            f"{date_str}:{category}:trace-emit:finish-position: {msg}"
                            for msg in fp_outcome.trace_errors
                        )

                    rs_outcome: _CategoryDateOutcome | None = None
                    if category in RS_CATEGORIES:
                        if rs_experiment_id is None:
                            rs_experiment_id = get_or_create_experiment(
                                client, config.EXPERIMENT_RS_PRODUCTION_USAGE
                            )
                        try:
                            rs_outcome = _sync_rs_category_date(
                                client,
                                tracing_client,
                                rs_experiment_id,
                                neon_conn,
                                local_conn,
                                category,
                                date_str,
                            )
                        except _ISOLATED_EXCEPTIONS as exc:
                            errors.append(f"{date_str}:{category}:running-style: {exc}")
                        else:
                            rs_runs_created += rs_outcome.runs_created
                            rs_runs_reused += rs_outcome.runs_reused
                            rs_eval_logged += rs_outcome.eval_logged
                            rs_eval_skipped_no_results += rs_outcome.eval_skipped_no_results
                            traces_created += rs_outcome.traces_created
                            traces_already_existed += rs_outcome.traces_already_existed
                            errors.extend(
                                f"{date_str}:{category}:trace-emit:running-style: {msg}"
                                for msg in rs_outcome.trace_errors
                            )

                        # Champion-mismatch check, RS side (see this function's
                        # own docstring). Only reachable when the RS sync above
                        # completed without raising -- a failed sync's champion
                        # coverage is unknown, not meaningfully "no coverage".
                        if rs_outcome is not None and _check_champion_gap(rs_outcome):
                            warnings.warn(
                                f"champion gap: category={category!r} date={date_str!r} "
                                "task='running-style': champion "
                                f"{rs_outcome.champion_model_version!r} did not serve; "
                                f"served {sorted(rs_outcome.served_model_versions_live)}",
                                stacklevel=2,
                            )
                            try:
                                _log_champion_gap(
                                    client,
                                    rs_experiment_id,
                                    date_str=date_str,
                                    category=category,
                                    task="running-style",
                                    champion_model_version=rs_outcome.champion_model_version,
                                    served_model_versions=rs_outcome.served_model_versions_live,
                                )
                            except _ISOLATED_EXCEPTIONS as exc:
                                errors.append(
                                    f"{date_str}:{category}:champion-gap:running-style: {exc}"
                                )
                            else:
                                champion_gaps_detected += 1

                    # Champion-mismatch check, FP side. Attempted for every
                    # category (including banei, which has no RS side at all) --
                    # only reachable when the FP sync above completed without
                    # raising, same reasoning as the RS check above.
                    if fp_outcome is not None and _check_champion_gap(fp_outcome):
                        warnings.warn(
                            f"champion gap: category={category!r} date={date_str!r} "
                            "task='finish-position': champion "
                            f"{fp_outcome.champion_model_version!r} did not serve; "
                            f"served {sorted(fp_outcome.served_model_versions_live)}",
                            stacklevel=2,
                        )
                        try:
                            _log_champion_gap(
                                client,
                                fp_experiment_id,
                                date_str=date_str,
                                category=category,
                                task="finish-position",
                                champion_model_version=fp_outcome.champion_model_version,
                                served_model_versions=fp_outcome.served_model_versions_live,
                            )
                        except _ISOLATED_EXCEPTIONS as exc:
                            errors.append(
                                f"{date_str}:{category}:champion-gap:finish-position: {exc}"
                            )
                        else:
                            champion_gaps_detected += 1

                    # Serving-gap checks (see this function's own docstring).
                    # Only reachable when the FP sync above completed without
                    # raising -- a failed FP sync's race count is simply
                    # unknown, not meaningfully "zero", so it must not be
                    # misread as a gap.
                    if fp_outcome is None:
                        continue

                    # ★ Partial-coverage check FIRST (see this function's own
                    # docstring's "PARTIAL-COVERAGE check" section) --
                    # deliberately INDEPENDENT of `_resolve_expected_races`/
                    # `expected` below (a different, direct oracle: `fp_
                    # outcome.races_scheduled`, computed unconditionally in
                    # `_sync_fp_category_date` for every category), so a
                    # failed/skipped RS sync this call never suppresses it.
                    # Gated on `races_live > 0` specifically so this can
                    # never fire for the SAME (date, category) the
                    # no_rows/backfill_only check below might ALSO flag
                    # (that check requires `races_live == 0`) -- the two are
                    # therefore mutually exclusive per call, so both safely
                    # share `_log_serving_gap`'s one marker-run-per-(date,
                    # category) idempotency key without ever overwriting
                    # each other's `gap_type` tag.
                    if (
                        fp_outcome.races_live > 0
                        and fp_outcome.coverage_pct is not None
                        and fp_outcome.coverage_pct < partial_coverage_threshold
                    ):
                        warnings.warn(
                            f"serving gap: category={category!r} date={date_str!r}: partial "
                            f"finish-position coverage ({fp_outcome.coverage_pct:.2f}% < "
                            f"{partial_coverage_threshold}%) -- {fp_outcome.races_live}/"
                            f"{fp_outcome.races_scheduled} scheduled races served live "
                            f"(gap_type={GAP_TYPE_PARTIAL_COVERAGE!r})",
                            stacklevel=2,
                        )
                        try:
                            _log_serving_gap(
                                client,
                                fp_experiment_id,
                                date_str=date_str,
                                category=category,
                                gap_source=GAP_SOURCE_RACE_CALENDAR,
                                gap_type=GAP_TYPE_PARTIAL_COVERAGE,
                                expected_races=fp_outcome.races_scheduled,
                                fp_races_observed=fp_outcome.races_observed,
                                fp_races_live=fp_outcome.races_live,
                                fp_races_backfilled=fp_outcome.races_backfilled,
                                fp_races_scheduled=fp_outcome.races_scheduled,
                                fp_coverage_pct=fp_outcome.coverage_pct,
                            )
                        except _ISOLATED_EXCEPTIONS as exc:
                            errors.append(
                                f"{date_str}:{category}:serving-gap-partial-coverage: {exc}"
                            )
                        else:
                            serving_gaps_detected += 1

                    try:
                        expected = _resolve_expected_races(
                            category,
                            rs_outcome,
                            local_conn,
                            date_str,
                            fp_races_scheduled=fp_outcome.races_scheduled,
                        )
                    except _ISOLATED_EXCEPTIONS as exc:
                        errors.append(f"{date_str}:{category}:serving-gap-expected: {exc}")
                        continue
                    if expected is None:
                        continue
                    expected_races, gap_source = expected
                    if not (expected_races > 0 and fp_outcome.races_live == 0):
                        continue
                    gap_type = (
                        GAP_TYPE_BACKFILL_ONLY
                        if fp_outcome.races_observed > 0
                        else GAP_TYPE_NO_ROWS
                    )
                    warnings.warn(
                        f"serving gap: category={category!r} date={date_str!r}: "
                        f"{expected_races} expected race(s) ({gap_source}) but "
                        f"0 live finish-position races served (gap_type={gap_type!r})",
                        stacklevel=2,
                    )
                    try:
                        _log_serving_gap(
                            client,
                            fp_experiment_id,
                            date_str=date_str,
                            category=category,
                            gap_source=gap_source,
                            gap_type=gap_type,
                            expected_races=expected_races,
                            fp_races_observed=fp_outcome.races_observed,
                            fp_races_live=fp_outcome.races_live,
                            fp_races_backfilled=fp_outcome.races_backfilled,
                            fp_races_scheduled=fp_outcome.races_scheduled,
                            fp_coverage_pct=fp_outcome.coverage_pct,
                        )
                    except _ISOLATED_EXCEPTIONS as exc:
                        errors.append(f"{date_str}:{category}:serving-gap: {exc}")
                    else:
                        serving_gaps_detected += 1
        finally:
            neon_conn.close()
            local_conn.close()

        timeline_points_appended = fp_eval_logged + rs_eval_logged
        if timeline_points_appended == 0:
            t.discard()
        else:
            t.feedback("points_appended", float(timeline_points_appended))

    return SyncProductionSummary(
        dates_processed=len(dates),
        fp_runs_created=fp_runs_created,
        fp_runs_reused=fp_runs_reused,
        fp_eval_logged=fp_eval_logged,
        fp_eval_skipped_no_results=fp_eval_skipped_no_results,
        rs_runs_created=rs_runs_created,
        rs_runs_reused=rs_runs_reused,
        rs_eval_logged=rs_eval_logged,
        rs_eval_skipped_no_results=rs_eval_skipped_no_results,
        serving_gaps_detected=serving_gaps_detected,
        champion_gaps_detected=champion_gaps_detected,
        traces_created=traces_created,
        traces_already_existed=traces_already_existed,
        stale_running_healed=stale_running_healed,
        errors=errors,
    )
