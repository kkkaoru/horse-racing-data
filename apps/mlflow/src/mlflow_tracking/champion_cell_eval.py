"""Per-CELL serve-accuracy evaluation for the CURRENT champion model version.

For each (category, task) pair, this module resolves the CURRENT "champion"
registered-model alias, gathers every genuinely-served prediction made BY
THAT EXACT model_version over a trailing window (default 90 days, see
`DEFAULT_WINDOW_DAYS`), joins those predictions against finalized race
results, and logs a per-CELL breakdown (venue x class_code x distance_band x
season_band x surface x field_size_band for finish-position; venue x
class_code x distance_band x surface for running-style, see
`FP_CELL_DIMENSIONS`/`RS_CELL_DIMENSIONS`) as one MLflow run per (category,
task) pair -- the champion's real-world accuracy broken down by racing-
condition slice, not just one overall number.

★ Empirical fact (2026-07-08, confirmed by direct read-only investigation):
3 of the 5 registered champion model versions (jra-finish-position,
nar-finish-position, banei-finish-position) have EITHER never been
genuinely served in the current window, or were served only on dates with
zero finalized results yet. Only jra-running-style and nar-running-style
currently have real, evaluable champion coverage. Every code path in this
module treats "zero champion rows found" as a normal, expected outcome --
not an error -- and still produces a valid run: an empty (but correctly
columned) cell table plus a `has_champion_coverage=false` tag/metric, so the
CURRENT REAL STATE of the other 3 categories is visible in MLflow rather
than silently producing no run at all.

This module is a sibling to (and shares no code with, other than the common
serve_eval.py/cells.py/logging_api.py library) `sync_production.py`, built
separately: that module pulls fresh rows from the racing Neon database and
stores/ingests them, looping per day; this module reads the SAME upstream
tables but issues one whole-window RANGE query per (category, task) pair
(via `serve_eval.fetch_fp_prediction_rows`/`fetch_rs_prediction_rows`) since
champion coverage is sparse and this is an evaluation, not a sync. The one
piece of logic both modules independently need -- resolving the current
champion's `model_version` label from the registry alias -- is deliberately
DUPLICATED (see `_resolve_champion_model_version` below) rather than shared,
so the two agents building these modules in parallel have zero file-
ownership overlap.

Every DB-facing function here takes an INJECTED connection (or a zero-
argument factory defaulting to `db.connect_racing_neon`/
`db.connect_local_replica`), matching db.py's/serve_eval.py's own module-
docstring rationale: this keeps `pytest` fully hermetic and keeps connection
lifecycle entirely the caller's responsibility.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Final, cast

import pandas as pd
import psycopg2
from mlflow import MlflowClient
from mlflow.entities import Metric, RunTag
from mlflow.exceptions import MlflowException

from mlflow_tracking import config, db, registry, serve_eval
from mlflow_tracking.logging_api import (
    get_or_create_experiment,
    log_batch_chunked,
    log_cell_table,
    select_headline_metrics,
)

DEFAULT_WINDOW_DAYS: Final[int] = 90
MIN_CELL_COUNT: Final[int] = 20

FP_CELL_DIMENSIONS: Final[tuple[str, ...]] = (
    "venue",
    "class_code",
    "distance_band",
    "season_band",
    "surface",
    "field_size_band",
)
RS_CELL_DIMENSIONS: Final[tuple[str, ...]] = ("venue", "class_code", "distance_band", "surface")

FP_CATEGORIES: Final[tuple[str, ...]] = ("jra", "nar", "banei")
# Running-style has no Ban-ei support (matches the repo-wide rule already
# enforced in training_run.py's own log_training_run validation).
RS_CATEGORIES: Final[tuple[str, ...]] = ("jra", "nar")

# The idempotency tag key: one run per (category, task, window_days,
# as_of_date) -- see eval_champion_cells_for_category's docstring for why a
# same-day re-run must be a cheap no-op rather than a duplicate/append.
CELL_EVAL_KEY_TAG: Final[str] = "cell_eval_key"

_FP_CELL_METRIC_COLUMNS: Final[tuple[str, ...]] = (
    "race_count",
    "top1_pct",
    "place2_pct",
    "place3_pct",
    "fukusho_2p_pct",
    "top3_box_pct",
    "low_n",
)
_FP_CELL_TABLE_COLUMNS: Final[tuple[str, ...]] = (
    "category",
    *FP_CELL_DIMENSIONS,
    *_FP_CELL_METRIC_COLUMNS,
)

_RS_CELL_METRIC_COLUMNS: Final[tuple[str, ...]] = ("horse_count", "accuracy_pct", "low_n")
_RS_CELL_TABLE_COLUMNS: Final[tuple[str, ...]] = (
    "category",
    *RS_CELL_DIMENSIONS,
    *_RS_CELL_METRIC_COLUMNS,
)


@dataclass(frozen=True)
class ChampionRef:
    """The resolved identity of a (category, task) pair's current champion.

    `model_version_label` is the trainer-assigned `model_version` tag value
    used to filter production-prediction rows (None when there is no
    registered model, no champion alias, or the aliased ModelVersion could
    not be fetched). `registered_name`/`version` are kept alongside it
    (rather than re-derived later) because the cross-navigation tag
    (`latest_cell_eval_run_id`, see eval_champion_cells_for_category step 9)
    needs both, and `version` can be non-None even when `model_version_label`
    is None (an aliased ModelVersion exists but happens to carry no
    `model_version` tag of its own).
    """

    model_version_label: str | None
    registered_name: str
    version: str | None


@dataclass(frozen=True)
class ChampionCellEvalResult:
    """Summary of one eval_champion_cells_for_category call."""

    run_id: str
    category: str
    task: str
    champion_model_version: str | None
    unit_count: int  # race_count for finish-position, horse_count for running-style
    cell_count: int
    low_n_cell_count: int
    has_champion_coverage: bool
    reused_existing_run: bool


def _resolve_champion_model_version(client: MlflowClient, category: str, task: str) -> ChampionRef:
    """Resolve the CURRENT champion for (category, task) via the registry alias.

    Every failure mode collapses to `ChampionRef(None, registered_name, None)`
    rather than raising: no registered model yet, a registered model with no
    `champion` alias set, or (defense in depth -- empirically unreachable
    against this package's own isolated-sqlite tracking store, since setting
    an alias validates the target version exists and deleting a version
    clears any alias pointing to it, but not guaranteed for every possible
    MLflow backend) an aliased version number that no longer resolves via
    `get_model_version`. A caller with no champion to evaluate against is a
    normal, expected state (see this module's docstring), not an error.
    """
    resolved_task: registry.Task = (
        "finish-position" if task == "finish-position" else "running-style"
    )
    normalized_category = registry.normalize_category(category)
    name = registry.registered_model_name(normalized_category, resolved_task)
    try:
        registered = client.get_registered_model(name)
    except MlflowException:
        return ChampionRef(model_version_label=None, registered_name=name, version=None)

    alias_version = registered.aliases.get(registry.CHAMPION_ALIAS)
    if alias_version is None:
        return ChampionRef(model_version_label=None, registered_name=name, version=None)

    version_str = str(alias_version)
    try:
        mv = client.get_model_version(name, version_str)
    except MlflowException:
        return ChampionRef(model_version_label=None, registered_name=name, version=None)

    return ChampionRef(
        model_version_label=mv.tags.get("model_version"),
        registered_name=name,
        version=version_str,
    )


def _find_existing_cell_eval_run(
    client: MlflowClient, experiment_id: str, cell_eval_key: str
) -> str | None:
    """Find an existing champion-cell-eval run for `cell_eval_key` by tag
    search (same idiom as timeline._find_timeline_run). Returns None when no
    matching run exists yet -- the whole point of the caller's idempotency
    check is to short-circuit BEFORE any DB work in that case, and to reuse
    the existing run's summary otherwise (see
    eval_champion_cells_for_category's docstring: mlflow.log_table has
    APPEND semantics, so a duplicate same-day run would silently double the
    logged cell rows)."""
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{CELL_EVAL_KEY_TAG} = '{cell_eval_key}'",
        max_results=1,
    )
    return matches[0].info.run_id if matches else None


def _result_from_existing_run(client: MlflowClient, run_id: str) -> ChampionCellEvalResult:
    """Rebuild a ChampionCellEvalResult purely from an already-logged run's
    own tags/metrics -- the idempotency short-circuit path never repeats any
    DB query or aggregation work."""
    run = client.get_run(run_id)
    tags = run.data.tags
    metrics = run.data.metrics
    raw_champion_model_version = tags.get("champion_model_version", "none")
    return ChampionCellEvalResult(
        run_id=run_id,
        category=tags.get("category", ""),
        task=tags.get("task", ""),
        champion_model_version=(
            None if raw_champion_model_version == "none" else raw_champion_model_version
        ),
        unit_count=int(metrics.get("unit_count", 0.0)),
        cell_count=int(metrics.get("cell_count", 0.0)),
        low_n_cell_count=int(metrics.get("low_n_cell_count", 0.0)),
        has_champion_coverage=tags.get("has_champion_coverage") == "true",
        reused_existing_run=True,
    )


def _collect_champion_prediction_rows(
    neon_conn: db.ConnectionLike,
    category: str,
    task: str,
    model_version_label: str,
    date_from: str,
    date_to: str,
) -> list[dict[str, object]]:
    """Fetch, genuine-filter, Ban-ei-partition, and champion-filter one
    task's raw production-prediction rows for `category` over the whole
    [date_from, date_to] window in a single range query (see this module's
    docstring for why a range query, unlike sync_production.py's per-day
    loop, is correct here).

    Ban-ei partitioning mirrors serve_eval's own convention exactly:
    category="nar" keeps the non-Ban-ei partition, category="banei" keeps
    the Ban-ei partition, category="jra" never partitions at all (JRA
    predictions are never mixed with NAR/Ban-ei rows in the first place,
    since they use a different `source` value).
    """
    source = serve_eval.resolve_source(category)
    raw_rows = (
        serve_eval.fetch_fp_prediction_rows(neon_conn, source, date_from, date_to)
        if task == "finish-position"
        else serve_eval.fetch_rs_prediction_rows(neon_conn, source, date_from, date_to)
    )
    genuine_rows = serve_eval.filter_genuine_rows(raw_rows)
    if category == "nar":
        genuine_rows, _ = serve_eval.partition_by_banei(genuine_rows)
    elif category == "banei":
        _, genuine_rows = serve_eval.partition_by_banei(genuine_rows)
    return [row for row in genuine_rows if row["model_version"] == model_version_label]


def _group_rows_by_race_date(
    rows: list[dict[str, object]],
) -> dict[str, list[dict[str, object]]]:
    """Group already champion-filtered prediction rows by race_date
    (kaisai_nen + kaisai_tsukihi), so callers issue exactly one
    `serve_eval.fetch_race_results`/`fetch_race_metadata` call (both
    per-date-only queries) per distinct date champion coverage actually
    touches -- expected to be a SMALL number of calls, since champion
    coverage is sparse (see this module's docstring)."""
    groups: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        race_date = str(row["kaisai_nen"]) + str(row["kaisai_tsukihi"])
        groups.setdefault(race_date, []).append(row)
    return groups


def _aggregate_fp_cells(
    rows: Sequence[serve_eval.FpRaceEvalRow], *, min_cell_count: int
) -> pd.DataFrame:
    """Group finish-position race-eval rows into one row per
    (category, *FP_CELL_DIMENSIONS) cell, computing race_count and the 5
    hit-rate percentages plus a `low_n` flag (race_count < min_cell_count).

    Returns a correctly-columned EMPTY DataFrame when `rows` is empty --
    the champion-cell-eval run must always produce a valid (if
    uninformative) cell-table artifact, even for the 3-of-5 champions with
    zero current coverage (see this module's docstring), never crash or
    silently omit the artifact.
    """
    if not rows:
        return pd.DataFrame(columns=list(_FP_CELL_TABLE_COLUMNS))

    groups: dict[tuple[object, ...], list[serve_eval.FpRaceEvalRow]] = {}
    for row in rows:
        key = tuple(getattr(row, dimension) for dimension in FP_CELL_DIMENSIONS)
        groups.setdefault(key, []).append(row)

    records: list[dict[str, object]] = []
    for key, group_rows in groups.items():
        race_count = len(group_rows)
        record: dict[str, object] = {"category": group_rows[0].category}
        record.update(dict(zip(FP_CELL_DIMENSIONS, key, strict=True)))
        record["race_count"] = race_count
        record["top1_pct"] = 100.0 * sum(r.top1_hit for r in group_rows) / race_count
        record["place2_pct"] = 100.0 * sum(r.place2_hit for r in group_rows) / race_count
        record["place3_pct"] = 100.0 * sum(r.place3_hit for r in group_rows) / race_count
        record["fukusho_2p_pct"] = 100.0 * sum(r.fukusho_2p_hit for r in group_rows) / race_count
        record["top3_box_pct"] = 100.0 * sum(r.top3_box_hit for r in group_rows) / race_count
        record["low_n"] = race_count < min_cell_count
        records.append(record)
    return pd.DataFrame(records, columns=list(_FP_CELL_TABLE_COLUMNS))


def _aggregate_rs_cells(
    rows: Sequence[serve_eval.RsHorseEvalRow], *, min_cell_count: int
) -> pd.DataFrame:
    """Group running-style horse-eval rows into one row per
    (category, *RS_CELL_DIMENSIONS) cell, computing horse_count,
    accuracy_pct, and a `low_n` flag. Same empty-input contract as
    `_aggregate_fp_cells` above."""
    if not rows:
        return pd.DataFrame(columns=list(_RS_CELL_TABLE_COLUMNS))

    groups: dict[tuple[object, ...], list[serve_eval.RsHorseEvalRow]] = {}
    for row in rows:
        key = tuple(getattr(row, dimension) for dimension in RS_CELL_DIMENSIONS)
        groups.setdefault(key, []).append(row)

    records: list[dict[str, object]] = []
    for key, group_rows in groups.items():
        horse_count = len(group_rows)
        record: dict[str, object] = {"category": group_rows[0].category}
        record.update(dict(zip(RS_CELL_DIMENSIONS, key, strict=True)))
        record["horse_count"] = horse_count
        record["accuracy_pct"] = 100.0 * sum(r.hit for r in group_rows) / horse_count
        record["low_n"] = horse_count < min_cell_count
        records.append(record)
    return pd.DataFrame(records, columns=list(_RS_CELL_TABLE_COLUMNS))


def _bool_column_true_count(cell_df: pd.DataFrame, column: str) -> int:
    """Count True values in a boolean cell-table column, 0 for an empty
    DataFrame. Pulled out as its own tiny helper (rather than inlining
    `int(cell_df[column].sum())`) purely for typing: a plain `DataFrame.
    __getitem__` call resolves to a `Series | DataFrame` union for
    basedpyright without a narrower cast, and `.sum()` on that union is not
    directly `int()`-able."""
    if cell_df.empty:
        return 0
    return int(cast(pd.Series, cell_df[column]).sum())


def _best_worst_cell(
    cell_df: pd.DataFrame, metric_column: str
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    """Return the (best, worst) cell row (as plain dicts) by `metric_column`
    ("top1_pct" for finish-position, "accuracy_pct" for running-style), or
    (None, None) when `cell_df` has no rows at all -- the "no champion
    coverage" case must never crash here, and must never log a fabricated
    best/worst metric."""
    if cell_df.empty:
        return None, None
    metric_values = cell_df[metric_column]
    best = cast(dict[str, object], cell_df.loc[metric_values.idxmax()].to_dict())
    worst = cast(dict[str, object], cell_df.loc[metric_values.idxmin()].to_dict())
    return best, worst


def _evaluate_fp_champion(
    neon_conn: db.ConnectionLike,
    local_conn: db.ConnectionLike,
    category: str,
    model_version_label: str | None,
    date_from: str,
    date_to: str,
    min_cell_count: int,
) -> tuple[pd.DataFrame, int]:
    """Fetch/join/aggregate one finish-position champion's serve-accuracy
    over [date_from, date_to]. Returns (cell_df, unit_count); unit_count is
    the number of races actually evaluated (len of the built eval rows, not
    the raw prediction-row count).

    Short-circuits to an empty cell table (no local_conn query at all) both
    when there is no champion to evaluate (`model_version_label is None`)
    and when the champion has zero genuinely-served rows in this window --
    both are real, expected states for most of today's registered champions
    (see this module's docstring), not error conditions.
    """
    if model_version_label is None:
        return _aggregate_fp_cells([], min_cell_count=min_cell_count), 0
    champion_rows = _collect_champion_prediction_rows(
        neon_conn, category, "finish-position", model_version_label, date_from, date_to
    )
    if not champion_rows:
        return _aggregate_fp_cells([], min_cell_count=min_cell_count), 0

    eval_rows: list[serve_eval.FpRaceEvalRow] = []
    for race_date, day_rows in _group_rows_by_race_date(champion_rows).items():
        results = serve_eval.fetch_race_results(local_conn, category, race_date)
        eval_rows.extend(
            serve_eval.build_fp_race_eval_rows(category, model_version_label, day_rows, results)
        )
    return _aggregate_fp_cells(eval_rows, min_cell_count=min_cell_count), len(eval_rows)


def _evaluate_rs_champion(
    neon_conn: db.ConnectionLike,
    local_conn: db.ConnectionLike,
    category: str,
    model_version_label: str | None,
    date_from: str,
    date_to: str,
    min_cell_count: int,
) -> tuple[pd.DataFrame, int]:
    """Running-style counterpart to `_evaluate_fp_champion`: unit_count is
    the number of horses actually evaluated. Also fetches race metadata per
    distinct date (needed to normalize corner_1 into a running-style class),
    unlike the finish-position path."""
    if model_version_label is None:
        return _aggregate_rs_cells([], min_cell_count=min_cell_count), 0
    champion_rows = _collect_champion_prediction_rows(
        neon_conn, category, "running-style", model_version_label, date_from, date_to
    )
    if not champion_rows:
        return _aggregate_rs_cells([], min_cell_count=min_cell_count), 0

    eval_rows: list[serve_eval.RsHorseEvalRow] = []
    for race_date, day_rows in _group_rows_by_race_date(champion_rows).items():
        results = serve_eval.fetch_race_results(local_conn, category, race_date)
        race_meta = serve_eval.fetch_race_metadata(local_conn, category, race_date)
        eval_rows.extend(
            serve_eval.build_rs_horse_eval_rows(
                category, model_version_label, day_rows, results, race_meta
            )
        )
    return _aggregate_rs_cells(eval_rows, min_cell_count=min_cell_count), len(eval_rows)


def eval_champion_cells_for_category(
    client: MlflowClient,
    category: str,
    task: str,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    as_of: date | None = None,
    min_cell_count: int = MIN_CELL_COUNT,
    neon_connect: Callable[[], db.ConnectionLike] = db.connect_racing_neon,
    local_connect: Callable[[], db.ConnectionLike] = db.connect_local_replica,
) -> ChampionCellEvalResult:
    """Score the current champion for one (category, task) pair per CELL and
    log the result as one MLflow run, returning a summary.

    At most one run is ever created per (category, task, window_days,
    as_of date): the idempotency check (tag `cell_eval_key`) runs FIRST,
    before any DB work, so calling this again the same day is a cheap no-op
    that reuses the existing run's summary rather than re-querying Neon/the
    local replica or re-logging the cell table -- `mlflow.log_table`
    (used by `log_cell_table`) has APPEND semantics, so a duplicate same-day
    run would otherwise silently double every logged cell row.

    Raises ValueError for an unsupported `task`, or for
    `task="running-style"` combined with a Ban-ei category (running-style
    has no Ban-ei support, matching the same rule already enforced in
    `training_run.log_training_run`) -- both checks run before any DB or
    MLflow work.
    """
    if task not in ("finish-position", "running-style"):
        raise ValueError(f"task must be 'finish-position' or 'running-style', got: {task!r}")
    normalized_category = registry.normalize_category(category)
    if task == "running-style" and normalized_category == "banei":
        raise ValueError("running-style does not support category=banei (Ban-ei is out of scope)")

    resolved_as_of = as_of if as_of is not None else date.today()
    as_of_str = resolved_as_of.strftime("%Y%m%d")
    date_from = (resolved_as_of - timedelta(days=window_days)).strftime("%Y%m%d")
    date_to = as_of_str

    experiment_name = (
        config.EXPERIMENT_FP_CHAMPION_EVAL
        if task == "finish-position"
        else config.EXPERIMENT_RS_CHAMPION_EVAL
    )
    experiment_id = get_or_create_experiment(client, experiment_name)
    cell_eval_key = f"{normalized_category}:{task}:{window_days}:{as_of_str}"
    existing_run_id = _find_existing_cell_eval_run(client, experiment_id, cell_eval_key)
    if existing_run_id is not None:
        return _result_from_existing_run(client, existing_run_id)

    champion_ref = _resolve_champion_model_version(client, normalized_category, task)

    neon_conn = neon_connect()
    local_conn = local_connect()
    try:
        if task == "finish-position":
            cell_df, unit_count = _evaluate_fp_champion(
                neon_conn,
                local_conn,
                normalized_category,
                champion_ref.model_version_label,
                date_from,
                date_to,
                min_cell_count,
            )
            weight_column, metric_column = "race_count", "top1_pct"
        else:
            cell_df, unit_count = _evaluate_rs_champion(
                neon_conn,
                local_conn,
                normalized_category,
                champion_ref.model_version_label,
                date_from,
                date_to,
                min_cell_count,
            )
            weight_column, metric_column = "horse_count", "accuracy_pct"
    finally:
        neon_conn.close()
        local_conn.close()

    has_coverage = unit_count > 0
    headline = select_headline_metrics(cell_df, weight_column=weight_column)
    best_cell, worst_cell = _best_worst_cell(cell_df, metric_column)
    low_n_cell_count = _bool_column_true_count(cell_df, "low_n")

    run = client.create_run(experiment_id, run_name=f"champion-cells-{normalized_category}")
    run_id = run.info.run_id

    tags = {
        CELL_EVAL_KEY_TAG: cell_eval_key,
        "category": normalized_category,
        "task": task,
        "champion_model_version": (
            champion_ref.model_version_label
            if champion_ref.model_version_label is not None
            else "none"
        ),
        "window_days": str(window_days),
        "as_of_date": as_of_str,
        "has_champion_coverage": "true" if has_coverage else "false",
    }
    metrics: dict[str, float] = dict(headline)
    metrics["unit_count"] = float(unit_count)
    metrics["cell_count"] = float(len(cell_df))
    metrics["low_n_cell_count"] = float(low_n_cell_count)
    if best_cell is not None and worst_cell is not None:
        metrics[f"best_cell_{metric_column}"] = float(cast(float, best_cell[metric_column]))
        metrics[f"worst_cell_{metric_column}"] = float(cast(float, worst_cell[metric_column]))

    log_batch_chunked(
        client,
        run_id,
        metrics=[Metric(k, v, 0, 0) for k, v in metrics.items()],
        tags=[RunTag(k, v) for k, v in tags.items()],
    )
    # Logged even when cell_df has zero rows: an empty-but-correctly-columned
    # table is still a valid, useful artifact -- it visibly documents "no
    # champion coverage" rather than an absent artifact looking like a bug.
    log_cell_table(client, run_id, cell_df)
    client.set_terminated(run_id, status="FINISHED")

    if champion_ref.version is not None:
        client.set_model_version_tag(
            champion_ref.registered_name, champion_ref.version, "latest_cell_eval_run_id", run_id
        )

    return ChampionCellEvalResult(
        run_id=run_id,
        category=normalized_category,
        task=task,
        champion_model_version=champion_ref.model_version_label,
        unit_count=unit_count,
        cell_count=len(cell_df),
        low_n_cell_count=low_n_cell_count,
        has_champion_coverage=has_coverage,
        reused_existing_run=False,
    )


def _tasks_for_category(category: str) -> tuple[str, ...]:
    """Return the tasks to evaluate for `category`: finish-position always,
    plus running-style only when `category` is in RS_CATEGORIES (running-
    style has no Ban-ei support)."""
    if category in RS_CATEGORIES:
        return ("finish-position", "running-style")
    return ("finish-position",)


def eval_champion_cells(
    client: MlflowClient,
    categories: Sequence[str] = FP_CATEGORIES,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    as_of: date | None = None,
    min_cell_count: int = MIN_CELL_COUNT,
    neon_connect: Callable[[], db.ConnectionLike] = db.connect_racing_neon,
    local_connect: Callable[[], db.ConnectionLike] = db.connect_local_replica,
) -> list[ChampionCellEvalResult]:
    """Run `eval_champion_cells_for_category` for every (category, task)
    pair implied by `categories` -- finish-position for every category, plus
    running-style for every category also in RS_CATEGORIES (see
    `_tasks_for_category`) -- isolating failures per pair.

    One pair failing (an unsupported category string, a transient Neon/local
    -replica connection error, an MLflow registry error, ...) must never
    prevent the OTHER pairs from being evaluated -- the same per-call
    isolation the sibling sync_production.py module applies, for the same
    reason. A failed pair is simply omitted from the returned list (with a
    `warnings.warn` for visibility): this function intentionally returns a
    plain `list[ChampionCellEvalResult]` of the pairs that succeeded rather
    than a richer "results + errors" shape; a caller that needs per-failure
    detail should call `eval_champion_cells_for_category` directly per pair
    instead.
    """
    results: list[ChampionCellEvalResult] = []
    for category in categories:
        for task in _tasks_for_category(category):
            try:
                results.append(
                    eval_champion_cells_for_category(
                        client,
                        category,
                        task,
                        window_days=window_days,
                        as_of=as_of,
                        min_cell_count=min_cell_count,
                        neon_connect=neon_connect,
                        local_connect=local_connect,
                    )
                )
            except (ValueError, KeyError, TypeError, psycopg2.Error, MlflowException) as exc:
                warnings.warn(
                    f"champion cell eval failed for category={category!r} task={task!r}: {exc}",
                    stacklevel=2,
                )
    return results
