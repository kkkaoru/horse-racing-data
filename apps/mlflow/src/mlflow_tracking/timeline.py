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
callers append one point (step = date) per day. That run's own chart then
shows the real trend.

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

TIMELINE_KEY_TAG: Final[str] = "timeline_key"

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


def _timeline_key(task: str, category: str) -> str:
    return f"{task}:{category}"


def _find_timeline_run(client: MlflowClient, experiment_id: str, timeline_key: str) -> str | None:
    """Find the persistent timeline run for `timeline_key` by tag search.

    Returns None both when the experiment has no matching run yet (first
    call for this task/category) and, transitively, when the experiment
    itself was just created (search_runs on a brand-new experiment_id simply
    returns no rows) -- callers treat both the same way: create on demand.
    """
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{TIMELINE_KEY_TAG} = '{timeline_key}'",
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
    pair appends another point to its metric series (step = date, as a
    YYYYMMDD int) rather than creating a new run. This is what makes
    MLflow's line-chart view show a real trend instead of one flat dot per
    run -- see this module's own docstring.

    Idempotent by construction:
    - The run is found (not recreated) via a `timeline_key = "{task}:{category}"`
      tag search rather than trusting a caller-passed run_id, so calling this
      twice for the same (task, category) always appends to the same run,
      even across process restarts / separate CLI invocations. The run is
      tagged with `timeline_key`/`task`/`category` at creation time only.
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
            run_name=f"timeline-{task}-{category}",
            tags={TIMELINE_KEY_TAG: timeline_key, "task": task, "category": category},
        )
        run_id = run.info.run_id

    step = int(date_yyyymmdd)
    timestamp_ms = _timestamp_ms_for_date(date_yyyymmdd)
    metric_items: list[Metric] = []
    for key, value in metrics.items():
        already_logged_steps = {point.step for point in client.get_metric_history(run_id, key)}
        if step in already_logged_steps:
            continue
        metric_items.append(Metric(key, float(value), timestamp_ms, step))

    tag_items = [RunTag(k, v) for k, v in (tags or {}).items()]
    log_batch_chunked(client, run_id, metrics=metric_items, tags=tag_items)
    client.set_terminated(run_id, status="FINISHED", end_time=timestamp_ms)
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
    has never actually appeared in this field.
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
    """Return the set of `step` (YYYYMMDD int) values already logged for
    `metric_key` on the (task, category) timeline run.

    Returns an empty set both when the `timelines` experiment does not exist
    yet at all, and when it exists but this (task, category) has no run yet
    -- both cases mean "nothing to skip" for
    `backfill_serve_timeline.py`'s `--skip-existing` check.
    """
    experiment = client.get_experiment_by_name(config.EXPERIMENT_TIMELINES)
    if experiment is None:
        return set()
    run_id = _find_timeline_run(client, experiment.experiment_id, _timeline_key(task, category))
    if run_id is None:
        return set()
    return {point.step for point in client.get_metric_history(run_id, metric_key)}
