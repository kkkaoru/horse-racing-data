"""One-time (but safely re-runnable) historical backfill: emit MLflow traces
(+ Feedback assessments) for every already-evaluated (date, category,
model_version) combination in the REAL `finish-position/production-usage` /
`running-style/production-usage` experiments.

★ Why this module exists at all: `sync_production.py`'s daily pass only
ever emits a trace the FIRST time a (date, category, model_version) group's
eval join succeeds (see `_sync_fp_eval`/`_sync_rs_eval` -- gated by
`SYNC_EVAL_LOGGED_TAG`). Every run that ALREADY has `sync_eval_logged=true`
from BEFORE trace emission existed in this package will never revisit that
code path again (by design -- see that tag's own idempotency contract), so
this module is the one-time catch-up: re-derive the SAME `eval_rows` +
raw prediction `rows` those runs were originally built from (the exact same
`serve_eval` query/join functions `sync_production.py` itself uses), and
call the exact same `trace_emit.emit_fp_race_traces`/`emit_rs_horse_traces`
functions `sync_production.py` calls -- there is exactly ONE trace-emission
code path in this package, shared by the daily sync and this backfill.

Discovery approach mirrors `refresh_eval_metrics.py`'s own precedent
exactly (paginated `tags.sync_eval_logged = 'true'` search over
`config.EXPERIMENT_FP_PRODUCTION_USAGE`, generalized here to ALSO cover
`config.EXPERIMENT_RS_PRODUCTION_USAGE`), grouped by (date, category) so
`serve_eval.fetch_*_prediction_rows`/`fetch_race_results`/
`fetch_race_metadata` are each queried ONCE per distinct (date, category)
pair and shared across every model_version run sharing that pair, not once
per run.

Idempotent by construction, NOT by a run-level skip-cache: unlike
`refresh_eval_metrics.py` (which can cheaply skip a whole run by inspecting
its OWN already-logged metric keys before querying anything), there is no
run-level "traces already emitted" tag to check here -- trace existence
lives in the tracing store, keyed by `client_request_id`, not on the owning
run. So this module ALWAYS re-derives `eval_rows`/`rows` for every
(date, category) group and calls `trace_emit.emit_fp_race_traces`/
`emit_rs_horse_traces` for every run in it; those functions themselves are
what make a re-run of THIS module cheap and safe (a race/horse whose trace
already exists costs one `search_traces` round-trip and zero writes -- see
`trace_emit.py`'s own docstring). This is the deliberate, documented
trade-off: a full re-run after an interruption is trivially safe to resume
(just re-run this module -- already-emitted traces are skipped), at the
cost of one extra existence-check round-trip per already-covered
race/horse on every re-run. Given this package's own volume estimate
(~30-45 dates, a few thousand traces total), this is a fine trade.

Optional `date_from`/`date_to` (YYYYMMDD, inclusive) restrict which runs
are considered -- useful for chunking a very large backfill into smaller,
independently-resumable invocations (see this module's own module-level
docstring section on volume/duration expectations in README.md). Omitted
(the default): every run ever found, no date restriction.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

import psycopg2
from mlflow import MlflowClient
from mlflow.entities import Run
from mlflow.exceptions import MlflowException
from mlflow.tracing.client import TracingClient

from mlflow_tracking import config, db, serve_eval, trace_emit
from mlflow_tracking.sync_production import SYNC_EVAL_LOGGED_TAG, TRUE_STR

# Mirrors sync_production.py's own _ISOLATED_EXCEPTIONS tuple exactly and
# for the same reason: one bad (date, category) batch or one bad run must
# never abort the rest of the backfill.
_ISOLATED_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    ValueError,
    KeyError,
    TypeError,
    psycopg2.Error,
    MlflowException,
)


@dataclass
class BackfillTracesSummary:
    """Outcome counters for one `backfill_traces` call.

    `fp_runs_scanned`/`rs_runs_scanned` are every run found tagged
    `sync_eval_logged=true` in the respective experiment (after any
    `date_from`/`date_to` filter), regardless of outcome.
    `fp_traces_created`/`rs_traces_created` and `fp_traces_already_existed`/
    `rs_traces_already_existed` sum `trace_emit.TraceEmitSummary`'s own
    fields (see that dataclass's docstring) across every run this call
    processed -- `*_already_existed` is the NORMAL, expected count on any
    re-run of this same backfill, not an error. `errors` folds in both
    this module's own query/lookup failures (isolated per (date, category)
    group or per run) and any per-race/per-horse trace-emission failure
    `trace_emit` itself already isolated (see its own docstring).
    """

    fp_runs_scanned: int = 0
    fp_traces_created: int = 0
    fp_traces_already_existed: int = 0
    rs_runs_scanned: int = 0
    rs_traces_created: int = 0
    rs_traces_already_existed: int = 0
    errors: list[str] = field(default_factory=list)


def _category_filtered_rows(
    category: str, genuine_rows: list[dict[str, object]]
) -> list[dict[str, object]]:
    """Local duplicate of `sync_production._fp_category_filtered_rows` /
    `refresh_eval_metrics._category_filtered_rows` (same Ban-ei partition
    rule) -- not imported, per this package's established per-module-
    independence convention for small private helpers (see e.g.
    `refresh_eval_metrics.py`'s own identical duplicate, or
    `champion_cell_eval.py`'s module docstring for the same precedent)."""
    if category == "jra":
        return genuine_rows
    non_banei_rows, banei_rows = serve_eval.partition_by_banei(genuine_rows)
    return banei_rows if category == "banei" else non_banei_rows


def _in_date_range(date_str: str, date_from: str | None, date_to: str | None) -> bool:
    """True when `date_str` is within the optional inclusive [date_from,
    date_to] filter (either or both bounds may be None, meaning
    unbounded on that side)."""
    if date_from is not None and date_str < date_from:
        return False
    return not (date_to is not None and date_str > date_to)


def _find_evaluated_runs(client: MlflowClient, experiment_id: str) -> list[Run]:
    """Return EVERY run tagged `sync_eval_logged=true` in `experiment_id`,
    paginating via `page_token` until exhausted -- mirrors
    `refresh_eval_metrics._find_evaluated_fp_runs`'s exact idiom, generalized
    to either the FP or RS production-usage experiment (both share the same
    `SYNC_EVAL_LOGGED_TAG` tag)."""
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


def _group_runs_by_date_category(
    runs: list[Run], summary_errors: list[str], *, date_from: str | None, date_to: str | None
) -> dict[tuple[str, str], list[Run]]:
    """Group `runs` by their own `date`/`category` tags, applying the
    optional date filter and recording (never raising on) a run missing
    either tag -- mirrors `refresh_eval_metrics.refresh_fp_place456_metrics`'s
    own grouping loop exactly."""
    grouped: dict[tuple[str, str], list[Run]] = {}
    for run in runs:
        tags = run.data.tags
        date_str = tags.get("date")
        category = tags.get("category")
        if date_str is None or category is None:
            summary_errors.append(f"run {run.info.run_id}: missing date/category tag")
            continue
        if not _in_date_range(date_str, date_from, date_to):
            continue
        grouped.setdefault((date_str, category), []).append(run)
    return grouped


def _backfill_fp_traces(
    client: MlflowClient,
    tracing_client: TracingClient,
    experiment_id: str,
    neon_conn: db.ConnectionLike,
    local_conn: db.ConnectionLike,
    *,
    date_from: str | None,
    date_to: str | None,
    summary: BackfillTracesSummary,
) -> None:
    """Backfill traces for every FP production-usage run tagged
    `sync_eval_logged=true` (optionally date-filtered), mutating `summary`
    in place -- mirrors `refresh_eval_metrics.refresh_fp_place456_metrics`'s
    own overall structure (one query per (date, category) group, shared
    across every model_version run in it)."""
    runs = _find_evaluated_runs(client, experiment_id)
    summary.fp_runs_scanned = len(runs)
    if not runs:
        return
    runs_by_date_category = _group_runs_by_date_category(
        runs, summary.errors, date_from=date_from, date_to=date_to
    )

    for (date_str, category), date_category_runs in runs_by_date_category.items():
        try:
            source = serve_eval.resolve_source(category)
            raw_rows = serve_eval.fetch_fp_prediction_rows(neon_conn, source, date_str, date_str)
            genuine_rows = _category_filtered_rows(
                category, serve_eval.filter_genuine_rows(raw_rows)
            )
            rows_by_version = serve_eval.group_by_model_version(genuine_rows)
            results = serve_eval.fetch_race_results(local_conn, category, date_str)
        except _ISOLATED_EXCEPTIONS as exc:
            summary.errors.append(f"{date_str}:{category}:fp-backfill-query: {exc}")
            continue

        for run in date_category_runs:
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
            except _ISOLATED_EXCEPTIONS as exc:
                summary.errors.append(f"run {run.info.run_id}: eval join failed: {exc}")
                continue
            if not eval_rows:
                continue
            trace_summary = trace_emit.emit_fp_race_traces(
                tracing_client, experiment_id, eval_rows, group_rows
            )
            summary.fp_traces_created += trace_summary.traces_created
            summary.fp_traces_already_existed += trace_summary.traces_already_existed
            summary.errors.extend(f"run {run.info.run_id}: {msg}" for msg in trace_summary.errors)


def _backfill_rs_traces(
    client: MlflowClient,
    tracing_client: TracingClient,
    experiment_id: str,
    neon_conn: db.ConnectionLike,
    local_conn: db.ConnectionLike,
    *,
    date_from: str | None,
    date_to: str | None,
    summary: BackfillTracesSummary,
) -> None:
    """RS-side twin of `_backfill_fp_traces` -- one trace per HORSE (see
    `trace_emit.py`'s own RS design-decision docstring), no Ban-ei partition
    (running-style has no Ban-ei model at all)."""
    runs = _find_evaluated_runs(client, experiment_id)
    summary.rs_runs_scanned = len(runs)
    if not runs:
        return
    runs_by_date_category = _group_runs_by_date_category(
        runs, summary.errors, date_from=date_from, date_to=date_to
    )

    for (date_str, category), date_category_runs in runs_by_date_category.items():
        try:
            source = serve_eval.resolve_source(category)
            raw_rows = serve_eval.fetch_rs_prediction_rows(neon_conn, source, date_str, date_str)
            genuine_rows = serve_eval.filter_genuine_rows(raw_rows)
            rows_by_version = serve_eval.group_by_model_version(genuine_rows)
            results = serve_eval.fetch_race_results(local_conn, category, date_str)
            race_meta = serve_eval.fetch_race_metadata(local_conn, category, date_str)
        except _ISOLATED_EXCEPTIONS as exc:
            summary.errors.append(f"{date_str}:{category}:rs-backfill-query: {exc}")
            continue

        for run in date_category_runs:
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
                eval_rows = serve_eval.build_rs_horse_eval_rows(
                    category, model_version, group_rows, results, race_meta
                )
            except _ISOLATED_EXCEPTIONS as exc:
                summary.errors.append(f"run {run.info.run_id}: eval join failed: {exc}")
                continue
            if not eval_rows:
                continue
            trace_summary = trace_emit.emit_rs_horse_traces(
                tracing_client, experiment_id, eval_rows, group_rows
            )
            summary.rs_traces_created += trace_summary.traces_created
            summary.rs_traces_already_existed += trace_summary.traces_already_existed
            summary.errors.extend(f"run {run.info.run_id}: {msg}" for msg in trace_summary.errors)


def backfill_traces(
    client: MlflowClient,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    neon_connect: Callable[[], db.ConnectionLike] = db.connect_racing_neon,
    local_connect: Callable[[], db.ConnectionLike] = db.connect_local_replica,
) -> BackfillTracesSummary:
    """Backfill MLflow traces (+ assessments) for every already-evaluated
    (date, category, model_version) run in BOTH
    `config.EXPERIMENT_FP_PRODUCTION_USAGE` and
    `config.EXPERIMENT_RS_PRODUCTION_USAGE` -- see this module's own
    docstring for the full idempotency/design rationale.

    `neon_connect`/`local_connect` are each called exactly ONCE for this
    whole call (not once per date), in a try/finally that always closes
    both -- same convention as `sync_production.sync_production_range`.
    Either experiment being entirely absent (e.g. a from-scratch store) is
    a silent no-op for that side, not an error.
    """
    summary = BackfillTracesSummary()
    tracing_client = trace_emit.build_tracing_client(client)

    fp_experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    rs_experiment = client.get_experiment_by_name(config.EXPERIMENT_RS_PRODUCTION_USAGE)
    if fp_experiment is None and rs_experiment is None:
        return summary

    neon_conn = neon_connect()
    local_conn = local_connect()
    try:
        if fp_experiment is not None:
            _backfill_fp_traces(
                client,
                tracing_client,
                fp_experiment.experiment_id,
                neon_conn,
                local_conn,
                date_from=date_from,
                date_to=date_to,
                summary=summary,
            )
        if rs_experiment is not None:
            _backfill_rs_traces(
                client,
                tracing_client,
                rs_experiment.experiment_id,
                neon_conn,
                local_conn,
                date_from=date_from,
                date_to=date_to,
                summary=summary,
            )
    finally:
        neon_conn.close()
        local_conn.close()

    return summary
