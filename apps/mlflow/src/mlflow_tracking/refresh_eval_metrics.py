"""One-time metrics-only enrichment pass: adds `place4_pct`/`place5_pct`/
`place6_pct` (feedback_eval_rank_1_to_6) to already-evaluated finish-position
`production-usage` runs, without re-deriving or re-logging anything else.

★ Why this module exists at all: `serve_eval.aggregate_fp_day_metrics` grew
three new keys (see that function's own docstring), but every run this
package already logged BEFORE that change -- every (date, category,
model_version) sync run tagged `sync_eval_logged=true` in
`config.EXPERIMENT_FP_PRODUCTION_USAGE` -- was logged against the OLD
5-metric shape. Those runs are otherwise complete and correct; they simply
predate the 3 new metric keys. Re-running `sync-production` over the same
historical range would NOT fix this: `sync_production._sync_fp_category_date`
skips any (date, category, model_version) group whose run already has
`sync_eval_logged=true` (see that function's own docstring on the
idempotency state machine) -- by design, since re-deriving the FULL eval
(and therefore re-calling `client.log_table` for `eval.json`/`eval.parquet`,
which has APPEND semantics) would silently duplicate every row already in
that table artifact. This module is the surgical alternative: re-run ONLY
the join/aggregate (no new query shape, same `serve_eval` functions the
original sync used), extract ONLY the 3 new metric keys, and append them via
`log_batch_chunked` (metrics only, never a table/artifact call) plus a
`timeline.upsert_timeline_point` call (reusing that function's own
per-metric-key step-dedup, never reinventing it) to keep the v2
`finish-position` timeline run's charts in sync.

Never creates a new run, never touches `sync_base_logged`/`sync_eval_logged`/
any existing tag, never re-logs `predictions.json`/`predictions.parquet`/
`eval.json`/`eval.parquet`, and never re-logs an existing metric key
(`fp_top1_pct`, `fp_races_evaluated`, ...) -- purely additive.

Idempotent by construction: a run whose OWN `data.metrics` already contains
every one of `fp_place4_pct`/`fp_place5_pct`/`fp_place6_pct` (from a
previous call to this exact module) is skipped entirely, no re-query. A run
where every eligible-race population for ranks 4/5/6 is empty (all races
that day/model_version had too small a field, see
`serve_eval.aggregate_fp_day_metrics`'s own None-handling docstring) is
correctly left with NONE of the 3 keys ever added -- not an error, simply
"not applicable" -- and is therefore re-examined (cheaply -- it's a single
day's worth of rows) on every future call, since there is nothing to persist
as "done" for a run that has zero eligible metrics to add. `champion_cell_
eval.py`'s and `cell_eval_runs.py`'s ALREADY-LOGGED historical runs are
DELIBERATELY NOT touched by this module -- see `refresh_fp_place456_metrics`'s
own docstring for why: unlike this module's target (immutable, date-scoped
per-run join), those two modules' runs are naturally refreshed by their own
next regular `eval-champion-cells`/`eval-cells` invocation, which already
recomputes their whole cell table from a live trailing window every time
they run (no idempotency gate blocks re-deriving the new columns there).

Every DB-facing function here takes an injected connection (or a
zero-argument factory defaulting to `db.connect_racing_neon`/
`db.connect_local_replica`), matching this package's established hermetic
-testing rationale (see db.py's/serve_eval.py's own module docstrings).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

import psycopg2
from mlflow import MlflowClient
from mlflow.entities import Metric, Run
from mlflow.exceptions import MlflowException

from mlflow_tracking import config, db, serve_eval, timeline
from mlflow_tracking.logging_api import log_batch_chunked
from mlflow_tracking.sync_production import SYNC_EVAL_LOGGED_TAG, TRUE_STR

# The 3 new keys this module exists to backfill -- deliberately NOT the full
# set `serve_eval.aggregate_fp_day_metrics` returns: every other key
# (top1_pct, place2_pct, place3_pct, fukusho_2p_pct, top3_box_pct, races) was
# already logged by the original `sync-production` call for every one of
# these runs, and re-logging them here would be pointless (not harmful --
# `log_batch_chunked` doesn't append-duplicate a plain metric key the way
# `client.log_table` does for a whole artifact -- but this module's whole
# point is a MINIMAL, metrics-only, 3-key-only enrichment pass).
_NEW_METRIC_KEYS: Final[tuple[str, ...]] = ("place4_pct", "place5_pct", "place6_pct")

# Isolated per (date, category) query batch and per run, mirroring
# sync_production.py's own `_ISOLATED_EXCEPTIONS` tuple exactly and for the
# same reason: one bad batch/run must never abort the rest of the backfill.
_ISOLATED_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    ValueError,
    KeyError,
    TypeError,
    psycopg2.Error,
    MlflowException,
)


@dataclass
class RefreshEvalMetricsSummary:
    """Outcome counters for one `refresh_fp_place456_metrics` call.

    `runs_scanned` is every run found tagged `sync_eval_logged=true` in
    `config.EXPERIMENT_FP_PRODUCTION_USAGE`, regardless of outcome.
    `runs_updated` counts runs that got at least one of the 3 new metric
    keys appended THIS call. `runs_skipped_already_enriched` counts runs
    whose own metrics already carried all 3 keys (a previous call already
    backfilled them) -- never re-queried. `runs_skipped_not_applicable`
    counts runs that were queried but for which every rank-4/5/6 population
    was empty (every race that day/model_version had too small a field) --
    a normal, expected outcome (see this module's own docstring), not an
    error. `metric_points_appended` is the total count of
    `fp_place{4,5,6}_pct` metric points logged across every updated run
    (0-3 per run). `timeline_points_appended` is the total count of new
    points appended to any `finish-position` v2 timeline run.
    """

    runs_scanned: int = 0
    runs_updated: int = 0
    runs_skipped_already_enriched: int = 0
    runs_skipped_not_applicable: int = 0
    metric_points_appended: int = 0
    timeline_points_appended: int = 0
    errors: list[str] = field(default_factory=list)


def _category_filtered_rows(
    category: str, genuine_rows: list[dict[str, object]]
) -> list[dict[str, object]]:
    """Local duplicate of `sync_production._fp_category_filtered_rows` (same
    Ban-ei partition rule: jra passes through unchanged, nar keeps the
    non-Ban-ei partition, banei keeps the Ban-ei partition) -- not imported,
    per this package's established per-module-independence convention for
    small private helpers (see e.g. `champion_cell_eval.py`'s own docstring
    on why `resolve_champion_model_version`'s business-logic siblings are
    duplicated rather than shared).
    """
    if category == "jra":
        return genuine_rows
    non_banei_rows, banei_rows = serve_eval.partition_by_banei(genuine_rows)
    return banei_rows if category == "banei" else non_banei_rows


def _find_evaluated_fp_runs(client: MlflowClient, experiment_id: str) -> list[Run]:
    """Return EVERY run tagged `sync_eval_logged=true` in `experiment_id`,
    paginating via `page_token` until exhausted -- this package's real
    production history can exceed `search_runs`' default single-page
    `max_results`, unlike every other tag search in this package (which
    only ever expects at most one match, see e.g. `sync_production.
    _find_sync_run`)."""
    runs: list[Run] = []
    page_token: str | None = None
    while True:
        page = client.search_runs(
            [experiment_id],
            filter_string=f"tags.{SYNC_EVAL_LOGGED_TAG} = '{TRUE_STR}'",
            max_results=1000,
            page_token=page_token,
        )
        runs.extend(page)
        page_token = page.token
        if not page_token:
            break
    return runs


def _new_metrics_for_run(day_metrics: dict[str, float | None]) -> dict[str, float]:
    """Extract only the non-None keys among `_NEW_METRIC_KEYS` from
    `day_metrics` (see `serve_eval.aggregate_fp_day_metrics`'s own
    None-handling docstring) -- mirrors `timeline.fp_metrics_for_timeline`'s
    own `isinstance(value, int | float)` skip-None convention."""
    result: dict[str, float] = {}
    for key in _NEW_METRIC_KEYS:
        value = day_metrics.get(key)
        if isinstance(value, int | float):
            result[key] = float(value)
    return result


def refresh_fp_place456_metrics(
    client: MlflowClient,
    *,
    neon_connect: Callable[[], db.ConnectionLike] = db.connect_racing_neon,
    local_connect: Callable[[], db.ConnectionLike] = db.connect_local_replica,
) -> RefreshEvalMetricsSummary:
    """Backfill `fp_place4_pct`/`fp_place5_pct`/`fp_place6_pct` onto every
    already-evaluated finish-position production-usage run (and its
    corresponding v2 `finish-position` timeline run), for every (date,
    category, model_version) this package has EVER genuinely synced.

    Runs found are grouped by (date, category) so `serve_eval.
    fetch_fp_prediction_rows`/`fetch_race_results` are each queried ONCE per
    distinct (date, category) pair and shared across every model_version
    run sharing that pair -- mirrors `champion_cell_eval.py`'s/`cell_eval_
    runs.py`'s own "one query per distinct date" convention, rather than
    one query per run.

    Deliberately scoped to ONLY `config.EXPERIMENT_FP_PRODUCTION_USAGE`
    (the per-(date, category, model_version) sync runs `sync_production.py`
    creates) -- NOT `champion_cell_eval.py`'s per-(category, task)
    champion-cell runs, and NOT `cell_eval_runs.py`'s per-(category, cell,
    model_version) persistent runs. Both of those already recompute their
    ENTIRE cell table (via `serve_eval.aggregate_fp_day_metrics`/
    `champion_cell_eval._aggregate_fp_cells`, both of which the place4/5/6
    change already covers) from a live trailing window on every regular
    invocation -- their own idempotency gates only block re-CREATING a run
    for the same (category, task/cell/model_version, window) on the SAME
    day, they do not block picking up a new column the next time they
    genuinely run. This module's OWN target -- `sync_production.py`'s
    per-day sync runs -- is different: `sync_eval_logged=true` permanently
    blocks the FULL eval join from ever re-running for that (date, category,
    model_version) again (by design, to protect `eval.json`/`eval.parquet`
    from `client.log_table`'s append-duplication hazard), so there is no
    "next regular invocation" that would ever pick up the 3 new keys for
    those runs on its own -- this module is the one-time fix for exactly
    that gap.

    Never calls `client.log_table`/`_log_table_and_parquet` -- see this
    module's own docstring for the full APPEND-duplication rationale this
    is designed to avoid.
    """
    summary = RefreshEvalMetricsSummary()
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    if experiment is None:
        return summary

    runs = _find_evaluated_fp_runs(client, experiment.experiment_id)
    summary.runs_scanned = len(runs)
    if not runs:
        return summary

    runs_by_date_category: dict[tuple[str, str], list[Run]] = {}
    for run in runs:
        tags = run.data.tags
        date_str = tags.get("date")
        category = tags.get("category")
        if date_str is None or category is None:
            summary.errors.append(f"run {run.info.run_id}: missing date/category tag")
            continue
        runs_by_date_category.setdefault((date_str, category), []).append(run)

    neon_conn = neon_connect()
    local_conn = local_connect()
    try:
        for (date_str, category), date_category_runs in runs_by_date_category.items():
            # Runs whose own metrics already carry all 3 new keys are
            # skipped BEFORE any query -- a previous call already backfilled
            # them, and re-querying would be pure waste.
            pending_runs = [
                run
                for run in date_category_runs
                if not all(f"fp_{key}" in run.data.metrics for key in _NEW_METRIC_KEYS)
            ]
            summary.runs_skipped_already_enriched += len(date_category_runs) - len(pending_runs)
            if not pending_runs:
                continue

            try:
                source = serve_eval.resolve_source(category)
                raw_rows = serve_eval.fetch_fp_prediction_rows(
                    neon_conn, source, date_str, date_str
                )
                genuine_rows = _category_filtered_rows(
                    category, serve_eval.filter_genuine_rows(raw_rows)
                )
                rows_by_version = serve_eval.group_by_model_version(genuine_rows)
                results = serve_eval.fetch_race_results(local_conn, category, date_str)
            except _ISOLATED_EXCEPTIONS as exc:
                summary.errors.append(f"{date_str}:{category}: query failed: {exc}")
                continue

            for run in pending_runs:
                model_version = run.data.tags.get("model_version")
                if model_version is None:
                    summary.errors.append(f"run {run.info.run_id}: missing model_version tag")
                    continue
                group_rows = rows_by_version.get(model_version, [])
                if not group_rows:
                    summary.errors.append(
                        f"run {run.info.run_id}: no prediction rows found on re-fetch for "
                        f"model_version={model_version!r}"
                    )
                    continue

                try:
                    eval_rows = serve_eval.build_fp_race_eval_rows(
                        category, model_version, group_rows, results
                    )
                    day_metrics = serve_eval.aggregate_fp_day_metrics(eval_rows)
                except _ISOLATED_EXCEPTIONS as exc:
                    summary.errors.append(f"run {run.info.run_id}: eval join failed: {exc}")
                    continue
                if not eval_rows:
                    summary.errors.append(
                        f"run {run.info.run_id}: no matched races found on re-fetch"
                    )
                    continue

                new_metrics = _new_metrics_for_run(day_metrics)
                if not new_metrics:
                    summary.runs_skipped_not_applicable += 1
                    continue

                metric_items = [
                    Metric(f"fp_{key}", value, 0, 0) for key, value in new_metrics.items()
                ]
                timeline_metrics = {f"fp_{key}": value for key, value in new_metrics.items()}
                try:
                    log_batch_chunked(client, run.info.run_id, metrics=metric_items)
                    timeline.upsert_timeline_point(
                        client, "finish-position", category, date_str, timeline_metrics
                    )
                except _ISOLATED_EXCEPTIONS as exc:
                    summary.errors.append(f"run {run.info.run_id}: failed to log metrics: {exc}")
                    continue

                summary.runs_updated += 1
                summary.metric_points_appended += len(metric_items)
                summary.timeline_points_appended += len(timeline_metrics)
    finally:
        neon_conn.close()
        local_conn.close()

    return summary
