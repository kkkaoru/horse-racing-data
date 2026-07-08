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
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Final, cast

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from mlflow import MlflowClient
from mlflow.entities import Metric, RunTag
from mlflow.exceptions import MlflowException

from mlflow_tracking import config, db, registry, serve_eval, timeline
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
    errors: list[str]


@dataclass
class _CategoryDateOutcome:
    """Per-(date, category) counters, summed by the caller into whichever of
    `SyncProductionSummary`'s fp_*/rs_* fields apply."""

    runs_created: int = 0
    runs_reused: int = 0
    eval_logged: int = 0
    eval_skipped_no_results: int = 0


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
    logged (never re-resolved later -- see `CHAMPION_AT_SYNC_TAG`'s name)."""
    champion_label = _resolve_champion_label(client, category, task)
    return TRUE_STR if champion_label == model_version else FALSE_STR


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
    """
    run_id = _find_sync_run(client, experiment_id, sync_key)
    if run_id is not None:
        return run_id, False, client.get_run(run_id).data.tags
    run = client.create_run(
        experiment_id,
        tags={
            SYNC_KEY_TAG: sync_key,
            "task": task,
            "category": category,
            "model_version": model_version,
            "date": date_str,
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
) -> None:
    """Log the base production-usage tracking for one (date, category,
    model_version) group and mark `sync_base_logged=true`.

    Called at most ONCE per sync_key (enforced by the caller checking
    `SYNC_BASE_LOGGED_TAG` first): predictions are immutable once generated,
    and `client.log_table`'s APPEND semantics would silently duplicate rows
    in the prediction-table artifact on a second call for the same run.
    `model_version`/`date`/`category` are already set as tags at run
    creation time (see `_get_or_create_run_and_tags`) and never change, so
    only `champion_at_sync` needs to be (re-)written here.
    """
    champion_value = _champion_at_sync_tag_value(client, category, task, model_version)
    metrics = [
        Metric(f"{metric_prefix}_races", float(_distinct_race_count(rows)), 0, 0),
        Metric(f"{metric_prefix}_horses", float(len(rows)), 0, 0),
    ]
    log_batch_chunked(
        client, run_id, metrics=metrics, tags=[RunTag(CHAMPION_AT_SYNC_TAG, champion_value)]
    )
    _log_table_and_parquet(
        client, run_id, prediction_table, _PREDICTIONS_JSON_ARTIFACT, _PREDICTIONS_PARQUET_ARTIFACT
    )
    log_batch_chunked(client, run_id, tags=[RunTag(SYNC_BASE_LOGGED_TAG, TRUE_STR)])


def _sync_fp_eval(
    client: MlflowClient,
    run_id: str,
    *,
    category: str,
    date_str: str,
    model_version: str,
    rows: list[dict[str, object]],
    local_conn: db.ConnectionLike,
) -> bool:
    """Attempt the FP evaluation join for one (date, category, model_version)
    group. Returns True (and logs metrics/table/timeline point, plus marks
    `sync_eval_logged=true`) only when at least one race matched a finalized
    result; returns False (logging nothing) otherwise, so the caller leaves
    the tag absent and a future call retries once results are final.
    """
    results = serve_eval.fetch_race_results(local_conn, category, date_str)
    eval_rows = serve_eval.build_fp_race_eval_rows(category, model_version, rows, results)
    if not eval_rows:
        return False

    day_metrics = serve_eval.aggregate_fp_day_metrics(eval_rows)
    metric_items = [
        Metric(f"fp_{key}", float(value), 0, 0)
        for key, value in day_metrics.items()
        if key != "races"
    ]
    # Named distinctly from the base-tracking `fp_races` metric (ALL served
    # races that day), which this is not: only races that matched at least
    # one finalized result count here.
    metric_items.append(Metric("fp_races_evaluated", float(day_metrics["races"]), 0, 0))
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
    return True


def _sync_rs_eval(
    client: MlflowClient,
    run_id: str,
    *,
    category: str,
    date_str: str,
    model_version: str,
    rows: list[dict[str, object]],
    local_conn: db.ConnectionLike,
) -> bool:
    """Attempt the RS evaluation join for one (date, category, model_version)
    group. Same "log only on a nonempty join, else leave retry-able" contract
    as `_sync_fp_eval`."""
    results = serve_eval.fetch_race_results(local_conn, category, date_str)
    race_meta = serve_eval.fetch_race_metadata(local_conn, category, date_str)
    eval_rows = serve_eval.build_rs_horse_eval_rows(
        category, model_version, rows, results, race_meta
    )
    if not eval_rows:
        return False

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
    return True


def _sync_fp_category_date(
    client: MlflowClient,
    experiment_id: str,
    neon_conn: db.ConnectionLike,
    local_conn: db.ConnectionLike,
    category: str,
    date_str: str,
) -> _CategoryDateOutcome:
    """Sync every genuinely-served FP model_version group for (category,
    date_str). A day with zero surviving rows for this category (the normal
    case -- production serving is sparse, not daily) is a silent no-op."""
    outcome = _CategoryDateOutcome()
    source = serve_eval.resolve_source(category)
    raw_rows = serve_eval.fetch_fp_prediction_rows(neon_conn, source, date_str, date_str)
    rows = _fp_category_filtered_rows(category, serve_eval.filter_genuine_rows(raw_rows))
    if not rows:
        return outcome

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
            )

        if existing_tags.get(SYNC_EVAL_LOGGED_TAG) != TRUE_STR:
            logged = _sync_fp_eval(
                client,
                run_id,
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

    return outcome


def _sync_rs_category_date(
    client: MlflowClient,
    experiment_id: str,
    neon_conn: db.ConnectionLike,
    local_conn: db.ConnectionLike,
    category: str,
    date_str: str,
) -> _CategoryDateOutcome:
    """Sync every genuinely-served RS model_version group for (category,
    date_str). Only ever called for `category in RS_CATEGORIES` by the
    caller -- Ban-ei has no running-style rows to sync."""
    outcome = _CategoryDateOutcome()
    source = serve_eval.resolve_source(category)
    raw_rows = serve_eval.fetch_rs_prediction_rows(neon_conn, source, date_str, date_str)
    rows = serve_eval.filter_genuine_rows(raw_rows)
    if not rows:
        return outcome

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
            logged = _sync_rs_eval(
                client,
                run_id,
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

    return outcome


def sync_production_range(
    client: MlflowClient,
    date_from: str,
    date_to: str,
    categories: Sequence[str] = FP_CATEGORIES,
    *,
    emit_traces: bool = True,
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

    `emit_traces` is accepted for API-contract completeness (so a future
    `--no-traces` CLI flag has somewhere to plug in) but has NO effect on
    behavior. This is a deliberate, investigated decision, not an
    unfinished feature: MLflow 3.14's `MlflowClient.start_trace`/`end_trace`
    were evaluated for this exact "per-race/per-horse audit trail" use case
    and found unsafe for this codebase. Empirically, they persist through a
    process-wide OpenTelemetry singleton that resolves its destination via
    the GLOBAL `mlflow.get_tracking_uri()` at first use -- completely
    bypassing the tracking_uri of the explicit `MlflowClient` instance
    passed into this very function. A trace logged through an isolated
    sqlite client silently failed ("No Experiment with id=1 exists") until
    `mlflow.set_tracking_uri()` was ALSO called globally, which would mean
    introducing global mutable tracking-URI state into a package built
    entirely around explicit-client, hermetically-testable design (see
    every other module's own docstring for why) -- exactly the failure
    class already responsible for a real production incident recorded in
    `tests/conftest.py` (the 2026-07-08 champion-alias-corruption incident
    from a leaked global env var / ambient tracking URI). The per-race/
    per-horse `predictions.json`/`eval.json` table artifacts logged by this
    module already give the same predicted-vs-actual audit visibility in
    the MLflow UI's Artifacts/Evaluation views, without any of that risk --
    so this module does not use MLflow traces at all, and `emit_traces`
    exists only so nobody mistakes its absence for an oversight, or a
    future `False` value for a real toggle that changes behavior today.

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
    errors: list[str] = []

    neon_conn = neon_connect()
    local_conn = local_connect()
    # Both experiment ids are resolved lazily (on first actual use) rather
    # than eagerly up front, so requesting e.g. categories=("banei",) --
    # which is FP-eligible but never RS-eligible -- never creates the RS
    # production-usage experiment at all.
    fp_experiment_id: str | None = None
    rs_experiment_id: str | None = None
    try:
        for date_str in dates:
            for category in categories:
                if fp_experiment_id is None:
                    fp_experiment_id = get_or_create_experiment(
                        client, config.EXPERIMENT_FP_PRODUCTION_USAGE
                    )
                try:
                    fp_outcome = _sync_fp_category_date(
                        client, fp_experiment_id, neon_conn, local_conn, category, date_str
                    )
                except _ISOLATED_EXCEPTIONS as exc:
                    errors.append(f"{date_str}:{category}:finish-position: {exc}")
                else:
                    fp_runs_created += fp_outcome.runs_created
                    fp_runs_reused += fp_outcome.runs_reused
                    fp_eval_logged += fp_outcome.eval_logged
                    fp_eval_skipped_no_results += fp_outcome.eval_skipped_no_results

                if category not in RS_CATEGORIES:
                    continue
                if rs_experiment_id is None:
                    rs_experiment_id = get_or_create_experiment(
                        client, config.EXPERIMENT_RS_PRODUCTION_USAGE
                    )
                try:
                    rs_outcome = _sync_rs_category_date(
                        client, rs_experiment_id, neon_conn, local_conn, category, date_str
                    )
                except _ISOLATED_EXCEPTIONS as exc:
                    errors.append(f"{date_str}:{category}:running-style: {exc}")
                else:
                    rs_runs_created += rs_outcome.runs_created
                    rs_runs_reused += rs_outcome.runs_reused
                    rs_eval_logged += rs_outcome.eval_logged
                    rs_eval_skipped_no_results += rs_outcome.eval_skipped_no_results
    finally:
        neon_conn.close()
        local_conn.close()

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
        errors=errors,
    )
