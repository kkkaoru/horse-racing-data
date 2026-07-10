"""Metric time-series ("timeline") layer, so accuracy trends render as real
line charts in the MLflow UI instead of one flat point per run.

Every other run created by this package (backfill, ingest_eval, training_run)
represents a single point in time: one metadata.json, one serve-accuracy JSON
payload, one training-run manifest. MLflow's own line-chart view groups by
metric key WITHIN a run, so a metric family that is logged as a fresh
single-point run every day (e.g. finish-position/serve-accuracy) never forms
a trend line in the UI -- every run shows one dot. This module provides the
complementary primitive: exactly one PERSISTENT run per (task, category)
pair, in the dedicated `config.EXPERIMENT_TIMELINES` experiment, to which
callers append one point (step = a small monotonic day-index, see
`step_for_date` below) per day. That run's own chart then shows the real
trend.

★ Step scheme is v2 (`step_for_date`, days-since-2020-01-01), not the
original v1 scheme (`step = int(date_yyyymmdd)`, e.g. 20260516) -- see
`step_for_date`'s own docstring for the exact mapping and why it changed
(MLflow 3.14's GenAI/Eval run-detail chart-tile renderer spins forever on a
~20-million-magnitude step; confirmed empirically with 3 throwaway
diagnostic runs logged directly into this experiment, tagged
`junk=true`/`diagnostic=true` -- left in place as a record of that
investigation, never migrated or deleted). v1 and v2 runs are two entirely
disjoint families that happen to share this module's machinery: v1 runs were
tagged `TIMELINE_KEY_TAG` ("timeline_key"); v2 runs (everything
`upsert_timeline_point` creates today) are tagged `TIMELINE_KEY_TAG_V2`
("timeline_key_v2") instead -- a DIFFERENT tag KEY, not merely a different
value, so a v1 run's tag can never satisfy a v2 `_find_timeline_run` search
and vice versa. v2 run names also carry a `-v2` suffix (e.g.
`timeline-finish-position-jra-v2`) so old vs. new are visually
distinguishable in run-list views without inspecting tags. The one-time v1
-> v2 migration (`migrate_timeline_v2.py`) copies every v1 run's full metric
history into its new v2 counterpart (recomputing each point's step via
`step_for_date`; value and timestamp_ms carry over unchanged), then tags the
OLD run `deprecated=true` / `superseded_by=<new_run_id>` -- the v1 run itself
is never deleted or renamed.

This module is deliberately generic (task/category/metric-name-agnostic):
`upsert_timeline_point` doesn't know or care what "finish-position" or
"fp_top1_pct" mean. The `fp_metrics_for_timeline`/`rs_metrics_for_timeline`
extractors below ARE payload-aware -- they know the exact field names
`serve_accuracy_report.py`'s `metrics_to_dict()` (and the flattened
`aggregate_metrics` a training-run manifest derives from it) use -- but they
are still just callers of the generic primitive, not part of it.

Every function takes an explicit `MlflowClient` (see logging_api.py's module
docstring for why): this keeps every call hermetically testable against an
isolated sqlite tmp_path store, with no dependency on this package's own
tracking-URI resolution or any global/fluent mlflow state.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Final

from mlflow import MlflowClient
from mlflow.entities import Metric, RunTag

from mlflow_tracking import config
from mlflow_tracking.logging_api import get_or_create_experiment, log_batch_chunked

# Epoch for the v2 timeline `step` value -- see `step_for_date` below.
TIMELINE_STEP_EPOCH: Final[date] = date(2020, 1, 1)

# v1 (pre-migration) run-finding tag key: `upsert_timeline_point` no longer
# writes or searches this key (see TIMELINE_KEY_TAG_V2 below) -- it is kept
# as a named constant purely so `migrate_timeline_v2.py` (and any future
# reader) has a single, unambiguous name for "the legacy lookup key", rather
# than a bare string literal scattered across migration code.
TIMELINE_KEY_TAG: Final[str] = "timeline_key"

# v2 run-finding tag key: a DIFFERENT tag key from TIMELINE_KEY_TAG (not just
# a different value) so a v1 run can never be found by a v2 lookup, and vice
# versa -- see this module's own docstring for the full v1/v2 split
# rationale. Every run `upsert_timeline_point` creates from now on is tagged
# with this key.
TIMELINE_KEY_TAG_V2: Final[str] = "timeline_key_v2"

# 12:00 JST is used as the canonical time-of-day for every timeline point
# (see _timestamp_ms_for_date), so every day's point lands at the same,
# unambiguous position on the x-axis regardless of what time this function
# actually happens to run (same-day ingest vs. a backfill run months later).
_JST_NOON_AS_UTC_HOUR: Final[int] = 3  # 12:00 JST (UTC+9) == 03:00 UTC, same calendar date.

# Maps serve_accuracy_report.py's `metrics_to_dict()["finish_position"]` field
# names (also the flattened `aggregate_metrics` a training-run manifest
# derives from that same dict) onto their timeline metric key. The `fp_`
# prefix and `_pct` suffix intentionally mirror ingest_eval.py's EXISTING
# `_fp_serve_metrics` naming convention (e.g. `fp_top1_pct`) rather than the
# `serve_top1_pct` style sketched before the real payload shape was known, so
# a per-day run's chart and this timeline run's chart use identical metric
# key names for the same underlying value.
_FP_TIMELINE_METRIC_MAP: Final[dict[str, str]] = {
    "top1_pct": "fp_top1_pct",
    "place2_pct": "fp_place2_pct",
    "place3_pct": "fp_place3_pct",
    "place4_pct": "fp_place4_pct",
    "place5_pct": "fp_place5_pct",
    "place6_pct": "fp_place6_pct",
    "fukusho_2p_pct": "fp_fukusho_2p_pct",
    "top3_box_pct": "fp_top3_box_pct",
    "races": "fp_races",
}


def validate_yyyymmdd(date_str: str) -> None:
    """Raise ValueError unless `date_str` is an 8-digit numeric string naming
    a real calendar date (e.g. "20260614").

    Reuses `datetime.date(year, month, day)` construction to get calendar
    validity (rejects e.g. "20260231") for free, the same idiom
    `serve_accuracy_report.py`'s own `validate_date_arg` uses. Exposed as a
    standalone function (not inlined into `upsert_timeline_point`) so
    `backfill_serve_timeline.py` can validate a `--date-from`/`--date-to`
    range with the exact same rule before ever invoking a subprocess.
    """
    if len(date_str) != 8 or not date_str.isdigit():
        raise ValueError(f"date must be an 8-digit YYYYMMDD string, got: {date_str!r}")
    try:
        date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except ValueError as exc:
        raise ValueError(f"date {date_str!r} is not a real calendar date") from exc


def _timestamp_ms_for_date(date_yyyymmdd: str) -> int:
    """Return the epoch-ms timestamp for 12:00 JST on `date_yyyymmdd`."""
    year, month, day = int(date_yyyymmdd[:4]), int(date_yyyymmdd[4:6]), int(date_yyyymmdd[6:8])
    moment = datetime(year, month, day, _JST_NOON_AS_UTC_HOUR, 0, 0, tzinfo=UTC)
    return int(moment.timestamp() * 1000)


def step_for_date(date_yyyymmdd: str) -> int:
    """Return the v2 timeline metric `step` value for `date_yyyymmdd`.

    ``step = (parsed_date - TIMELINE_STEP_EPOCH).days`` -- i.e. step N means
    N calendar days AFTER 2020-01-01. To recover the date from a step value
    seen in `client.get_metric_history` or the MLflow UI, add N days to
    2020-01-01 (``TIMELINE_STEP_EPOCH + timedelta(days=N)``). Concretely:
    step 0 == 2020-01-01, step 1 == 2020-01-02, step 2343 == 2026-06-01.

    This formula is the ONLY record of what a step value means: tags are
    per-RUN, not per-POINT, so there is no per-point tag to fall back on --
    a future reader with nothing but a bare step integer must be able to
    derive the calendar date from it using exactly this mapping, with no
    other context.

    Replaces the original v1 scheme (`step = int(date_yyyymmdd)`, e.g.
    20260516) -- kept small and monotonic instead of a raw YYYYMMDD int
    specifically because MLflow 3.14's GenAI/Eval run-detail experience
    (where every timeline run gets auto-routed, since it carries no
    params/source tags and looks nothing like a classic training run) spins
    forever on a Model-metrics chart tile once step magnitude reaches
    ~20 million -- confirmed empirically with 3 throwaway diagnostic runs
    logged into the real `timelines` experiment (a copy of a real run at
    ~20,260,516-magnitude steps spun forever; the same shape at steps [1..8]
    and at days-since-2020-01-01 magnitude [~2327..2377] both rendered
    correctly). The classic (non-GenAI) chart renderer was never the
    problem -- only the GenAI/Eval renderer's handling of that specific
    magnitude was.

    Does not itself call `validate_yyyymmdd` -- every caller
    (`upsert_timeline_point`) already validates the date string before
    computing this.
    """
    year, month, day = int(date_yyyymmdd[:4]), int(date_yyyymmdd[4:6]), int(date_yyyymmdd[6:8])
    return (date(year, month, day) - TIMELINE_STEP_EPOCH).days


def _timeline_key(task: str, category: str) -> str:
    return f"{task}:{category}"


def _find_timeline_run(client: MlflowClient, experiment_id: str, timeline_key: str) -> str | None:
    """Find the persistent v2 timeline run for `timeline_key` by a tag search
    on `TIMELINE_KEY_TAG_V2`.

    Returns None both when the experiment has no matching run yet (first
    call for this task/category) and, transitively, when the experiment
    itself was just created (search_runs on a brand-new experiment_id simply
    returns no rows) -- callers treat both the same way: create on demand.

    Always searches `TIMELINE_KEY_TAG_V2`, never the legacy `TIMELINE_KEY_TAG`
    -- a v1 run's `TIMELINE_KEY_TAG` tag can therefore never satisfy this
    lookup (they are two different tag KEYS, not just two different tag
    values on the same key). The one-time v1 -> v2 migration
    (`migrate_timeline_v2.py`) identifies legacy v1 runs its own way (a bulk
    `search_runs` + Python-side tag-presence filter over the whole
    experiment, see `migrate_timeline_v2.find_v1_timeline_runs`), not through
    this function.
    """
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{TIMELINE_KEY_TAG_V2} = '{timeline_key}'",
        max_results=1,
    )
    return matches[0].info.run_id if matches else None


def upsert_timeline_point(
    client: MlflowClient,
    task: str,
    category: str,
    date_yyyymmdd: str,
    metrics: Mapping[str, float],
    *,
    tags: Mapping[str, str] | None = None,
) -> str:
    """Append one day's metrics to the (task, category) timeline run, creating
    it on first use, and return its run_id.

    Unlike every other run in this package (one run per eval/backfill
    invocation), a timeline run is PERSISTENT: exactly one run per
    (task, category) pair lives for the lifetime of the tracking store, in
    the `config.EXPERIMENT_TIMELINES` experiment, and every call for that
    pair appends another point to its metric series (step = `step_for_date
    (date_yyyymmdd)`, days-since-2020-01-01 -- see that function's docstring
    for the exact mapping and why it replaced the original `int(date_yyyymmdd)`
    scheme) rather than creating a new run. This is what makes MLflow's
    line-chart view show a real trend instead of one flat dot per run -- see
    this module's own docstring.

    Idempotent by construction:
    - The run is found (not recreated) via a `timeline_key = "{task}:{category}"`
      tag search on `TIMELINE_KEY_TAG_V2` rather than trusting a
      caller-passed run_id, so calling this twice for the same (task,
      category) always appends to the same run, even across process
      restarts / separate CLI invocations. The run is named
      `"timeline-{task}-{category}-v2"` and tagged with
      `TIMELINE_KEY_TAG_V2`/`task`/`category`/`eval_regime` at creation time
      only -- both the `-v2` run-name suffix and the distinct
      `TIMELINE_KEY_TAG_V2` tag KEY (not just a different tag value) keep
      these v2 runs visually and mechanically disjoint from the legacy v1
      runs a `TIMELINE_KEY_TAG` search would find (see this module's own
      docstring for the full v1/v2 split rationale). `eval_regime` is always
      `"serve"` because, unlike the per-day eval runs this timeline
      complements, a timeline run by design only ever aggregates
      genuinely-served (never backtested/WF) accuracy points, so the tag is
      a fixed fact about every timeline run rather than a caller-supplied,
      per-call value.
    - Each metric key is deduped by STEP, not by value: before logging a
      metric, `client.get_metric_history(run_id, key)` is consulted, and a
      point at the same step is skipped entirely (even if the value
      differs -- re-ingesting the same date is assumed to reproduce the same
      number; this is a presence check, not a reconciliation). This is an
      O(n) check per metric key (n = the number of points already logged for
      that key on this run) -- perfectly fine at the daily granularity this
      is designed for (n stays in the hundreds/low-thousands even after
      years of daily data), but would not scale to a sub-daily cadence
      without a smarter index. Dedup is per metric KEY, not per run: a
      metric key introduced for the first time on a later call for an
      already-used step still gets logged normally.

    Every metric point is timestamped at 12:00 JST on `date_yyyymmdd` (==
    03:00 UTC the same calendar date, JST = UTC+9), so the x-axis in the UI
    reflects the METRIC'S OWN historical date rather than whenever this
    function happened to actually run -- which matters for backfills run
    long after the fact (see backfill_serve_timeline.py).

    Status is set to FINISHED (via `client.set_terminated`) after every
    call, whether the run was just created or already existed. This is a
    deliberate choice, not an oversight: MLflow's metric/chart rendering
    does not depend on run status at all, every other run in this tracking
    store ends up FINISHED, and leaving a timeline run permanently RUNNING
    (arguably "correct" since it conceptually never finishes) would instead
    make it show a stuck/live indicator in run-list views that never
    resolves -- there is no natural "done" moment for a persistent series,
    so FINISHED-after-every-append is the least-surprising choice.
    `set_terminated` is deliberately called WITHOUT an explicit `end_time`,
    so mlflow defaults it to this call's actual wall-clock time: passing
    `timestamp_ms` (this metric point's own, potentially long-past,
    historical date) here would make `end_time < start_time` -- and a
    negative Duration in the UI -- for any backfill call whose
    `date_yyyymmdd` predates the run's real creation/last-touch time. Only
    the run-level start/end bookkeeping is wall-clock; every metric point
    still carries its own correct historical `timestamp_ms`/`step` (see the
    `Metric(...)` construction below), so the chart's x-axis is unaffected.

    `tags` (if given) are always applied via the same `log_batch_chunked`
    call used for the metrics, even when the run already existed: tags may
    legitimately need to be updated on every call (not just at creation).
    """
    validate_yyyymmdd(date_yyyymmdd)
    timeline_key = _timeline_key(task, category)
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_TIMELINES)
    run_id = _find_timeline_run(client, experiment_id, timeline_key)
    if run_id is None:
        run = client.create_run(
            experiment_id,
            run_name=f"timeline-{task}-{category}-v2",
            tags={
                TIMELINE_KEY_TAG_V2: timeline_key,
                "task": task,
                "category": category,
                "eval_regime": "serve",
            },
        )
        run_id = run.info.run_id

    step = step_for_date(date_yyyymmdd)
    timestamp_ms = _timestamp_ms_for_date(date_yyyymmdd)
    metric_items: list[Metric] = []
    for key, value in metrics.items():
        already_logged_steps = {point.step for point in client.get_metric_history(run_id, key)}
        if step in already_logged_steps:
            continue
        metric_items.append(Metric(key, float(value), timestamp_ms, step))

    tag_items = [RunTag(k, v) for k, v in (tags or {}).items()]
    log_batch_chunked(client, run_id, metrics=metric_items, tags=tag_items)
    client.set_terminated(run_id, status="FINISHED")
    return run_id


def fp_metrics_for_timeline(fp: Mapping[str, object]) -> dict[str, float]:
    """Extract the finish-position timeline metric subset from a dict shaped
    like `metrics_to_dict()["finish_position"]` (see serve_accuracy_report.py),
    or the flattened `aggregate_metrics` a training-run manifest derives from
    it -- both use the same unprefixed field names (`top1_pct`,
    `place2_pct`, ...), since `mlflow_hook.flatten_numeric_metrics` only
    strips non-numeric/non-top-level values, it never renames keys.

    Only includes a key when its source value is `isinstance(value, int |
    float)` -- matching (not excluding bool) the existing
    `ingest_eval._fp_serve_metrics` convention exactly, for consistency
    within this package rather than defending against a value shape that
    has never actually appeared in this field. This same `isinstance` guard
    is also what makes `place4_pct`/`place5_pct`/`place6_pct` compose
    correctly here without any extra handling: `isinstance(None, int |
    float)` is False, so a day/cell where one of those ranks had zero
    eligible races (see `serve_eval.aggregate_fp_day_metrics`'s own
    None-handling docstring) is silently skipped rather than logged as a
    fabricated 0.0.
    """
    result: dict[str, float] = {}
    for source_key, timeline_metric_key in _FP_TIMELINE_METRIC_MAP.items():
        value = fp.get(source_key)
        if isinstance(value, int | float):
            result[timeline_metric_key] = float(value)
    return result


def rs_metrics_for_timeline(rs: Mapping[str, object]) -> dict[str, float]:
    """Extract the running-style timeline metric subset from a dict shaped
    like `metrics_to_dict()["running_style"]`, or a flattened
    `aggregate_metrics` derived from it (which will be missing `per_class`,
    since `flatten_numeric_metrics` only keeps top-level int/float values --
    the per-class breakdown is only available via the full JSON payload path,
    i.e. `ingest_eval.ingest_serve_accuracy`, not `training_run.py`).
    """
    result: dict[str, float] = {}
    overall_accuracy = rs.get("overall_accuracy_pct")
    if isinstance(overall_accuracy, int | float):
        result["rs_overall_accuracy_pct"] = float(overall_accuracy)
    macro_f1 = rs.get("macro_f1_pct")
    if isinstance(macro_f1, int | float):
        result["rs_macro_f1_pct"] = float(macro_f1)

    per_class = rs.get("per_class")
    if not isinstance(per_class, list):
        return result
    for entry in per_class:
        if not isinstance(entry, dict):
            continue
        label = entry.get("label")
        f1_pct = entry.get("f1_pct")
        if isinstance(label, str) and isinstance(f1_pct, int | float):
            result[f"rs_f1_{label}_pct"] = float(f1_pct)
    return result


def timeline_dates_present(
    client: MlflowClient, task: str, category: str, metric_key: str
) -> set[int]:
    """Return the set of v2 `step` values (days-since-2020-01-01, see
    `step_for_date`) already logged for `metric_key` on the (task, category)
    v2 timeline run.

    Returns an empty set both when the `timelines` experiment does not exist
    yet at all, and when it exists but this (task, category) has no v2 run
    yet -- both cases mean "nothing to skip" for
    `backfill_serve_timeline.py`'s `--skip-existing` check.

    Only ever looks up the v2 run (`_find_timeline_run`'s default `tag_key`,
    `TIMELINE_KEY_TAG_V2`) -- a legacy v1 run for the same (task, category)
    is never matched here. A step value returned by this function is
    therefore NOT a YYYYMMDD integer: a caller that needs to check "does
    this YYYYMMDD date already have a point" must convert its own date via
    `step_for_date` first and compare THAT against this set (see
    `backfill_serve_timeline._timeline_point_exists_for_date`), not compare
    a raw `int(date_str)` the way pre-migration code did.
    """
    experiment = client.get_experiment_by_name(config.EXPERIMENT_TIMELINES)
    if experiment is None:
        return set()
    run_id = _find_timeline_run(client, experiment.experiment_id, _timeline_key(task, category))
    if run_id is None:
        return set()
    return {point.step for point in client.get_metric_history(run_id, metric_key)}
