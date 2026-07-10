"""Tests for mlflow_tracking.cell_eval_runs.

Entirely hermetic: every Neon/local-replica connection used below is a
hand-built `_RoutedFakeConnection` (never a real psycopg2 connection),
mirroring test_champion_cell_eval.py's own fake-connection/fake-cursor
pattern exactly (this module's DB-access shape -- one whole-window range
query on Neon, then per-distinct-date result/metadata queries on the local
replica -- is the same shape champion_cell_eval.py uses, unlike sync_
production.py's per-day loop). The real `client` fixture (isolated sqlite
MlflowClient, see conftest.py) is used for the MLflow side throughout.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import psycopg2
import pytest
from mlflow import MlflowClient
from mlflow.entities import Trace
from mlflow.entities.trace_state import TraceState

from mlflow_tracking import (
    cell_eval_runs,
    champion_cell_eval,
    config,
    registry,
    serve_eval,
    timeline,
    trace_emit,
)

# ── Hermetic fake DB connections (mirrors test_champion_cell_eval.py) ───────

_RouteFn = Callable[[str, "tuple[object, ...]"], "list[tuple[object, ...]]"]


class _RoutedFakeCursor:
    def __init__(self, route: _RouteFn) -> None:
        self._route: _RouteFn = route
        self._rows: list[tuple[object, ...]] = []

    def execute(self, query: str, params: object = None) -> None:
        bound_params = cast("tuple[object, ...]", params) if params is not None else ()
        self._rows = self._route(query, bound_params)

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows


class _RoutedFakeConnection:
    def __init__(self, route: _RouteFn) -> None:
        self._route: _RouteFn = route
        self.closed: bool = False

    def cursor(self) -> _RoutedFakeCursor:
        return _RoutedFakeCursor(self._route)

    def close(self) -> None:
        self.closed = True


def _make_neon_conn(prediction_rows: list[tuple[object, ...]]) -> _RoutedFakeConnection:
    return _RoutedFakeConnection(lambda _query, _params: prediction_rows)


def _make_local_conn(
    results_by_date: Mapping[str, Sequence[tuple[object, ...]]],
    meta_by_date: Mapping[str, Sequence[tuple[object, ...]]] | None = None,
) -> _RoutedFakeConnection:
    meta = meta_by_date or {}

    def route(query: str, params: tuple[object, ...]) -> list[tuple[object, ...]]:
        year, monthday = params
        date_str = f"{year}{monthday}"
        if "_ra" in query:
            return list(meta.get(date_str, []))
        return list(results_by_date.get(date_str, []))

    return _RoutedFakeConnection(route)


def _neon_factory(prediction_rows: list[tuple[object, ...]]) -> MagicMock:
    return MagicMock(side_effect=lambda: _make_neon_conn(prediction_rows))


def _local_factory(
    results_by_date: Mapping[str, Sequence[tuple[object, ...]]],
    meta_by_date: Mapping[str, Sequence[tuple[object, ...]]] | None = None,
) -> MagicMock:
    return MagicMock(side_effect=lambda: _make_local_conn(results_by_date, meta_by_date))


_AS_OF = date(2026, 7, 5)


# ── Validation ────────────────────────────────────────────────────────────
#
# Pure-logic units (champion-relation classification, model_version
# truncation, cell_key/tag/run-name encoding, the timestamp helper, the
# volume-guard floor algorithm, run find-or-create, metric-point dedup) are
# deliberately NOT unit-tested by calling this module's own private
# functions directly -- basedpyright's strict `reportPrivateUsage` rejects
# cross-module private access even from this module's OWN test file, and no
# test file in this package does that (verified against test_champion_cell_
# eval.py/test_timeline.py/test_sync_production.py, none of which call a
# private helper of their module under test directly either). Every branch
# of that logic is instead exercised through the PUBLIC `eval_cells_for_
# category`/`eval_cells` entry points below -- see each test's own docstring/
# comment for which private branch it targets.


def test_invalid_task_raises_value_error(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="task must be"):
        cell_eval_runs.eval_cells_for_category(client, "jra", "bogus-task")


def test_invalid_category_raises_value_error(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="unsupported category"):
        cell_eval_runs.eval_cells_for_category(client, "keirin", "finish-position")


def test_running_style_banei_raises_value_error_before_any_db_work(
    client: MlflowClient,
) -> None:
    neon_connect = _neon_factory([])
    local_connect = _local_factory({})
    with pytest.raises(ValueError, match="does not support category=banei"):
        cell_eval_runs.eval_cells_for_category(
            client,
            "banei",
            "running-style",
            neon_connect=neon_connect,
            local_connect=local_connect,
        )
    neon_connect.assert_not_called()
    local_connect.assert_not_called()


# ── Zero / filtered-out coverage ─────────────────────────────────────────


def test_zero_rows_produces_zero_runs_but_still_creates_the_experiment(
    client: MlflowClient,
) -> None:
    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )
    assert result.runs_created == 0
    assert result.runs_reused == 0
    assert result.cells_skipped_low_volume == 0
    assert result.champion_model_version is None
    assert result.volume_guard_triggered is False
    assert result.projected_run_count_at_requested_floor == 0
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    assert client.search_runs([experiment.experiment_id]) == []


def test_non_genuine_rows_are_filtered_out_before_grouping(client: MlflowClient) -> None:
    old_gen = datetime(2020, 1, 1, 3, 0, 0, tzinfo=UTC)  # decades before the race date
    prediction_rows: list[tuple[object, ...]] = [
        (
            "05", "01", "H1", "modelA", 1, old_gen, "sprint", "medium", "summer", "A", "turf",
            "2026", "0601",
        ),
    ]
    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory({}),
    )
    assert result.runs_created == 0
    assert result.cells_skipped_low_volume == 0


def _cell_eval_traces(client: MlflowClient, experiment_name: str) -> list[Trace]:
    tracing_client = trace_emit.build_tracing_client(client)
    experiment = client.get_experiment_by_name(experiment_name)
    assert experiment is not None
    return list(
        tracing_client.search_traces(experiment_ids=[experiment.experiment_id], max_results=50)
    )


def test_eval_cells_zero_rows_still_emits_job_trace_with_zero_feedback(
    client: MlflowClient,
) -> None:
    """job_trace wiring (2026-07-11): the zero-genuinely-served-rows early
    return still gets its own trace, with only `resolve-champion`/
    `collect-rows` steps (nothing to build/log yet) and zero-valued
    Feedback."""
    cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )

    traces = _cell_eval_traces(client, config.EXPERIMENT_FP_CELL_EVAL)
    assert len(traces) == 1
    trace = traces[0]
    assert trace.info.state == TraceState.OK
    span_names = {span.name for span in trace.data.spans}
    assert span_names == {"eval-cells", "resolve-champion", "collect-rows"}
    assessments_by_name = {a.name: a for a in trace.info.assessments}
    assert set(assessments_by_name) == {"runs_created", "runs_skipped_low_volume"}
    runs_created_feedback = assessments_by_name["runs_created"].feedback
    assert runs_created_feedback is not None
    assert runs_created_feedback.value == 0.0
    runs_skipped_feedback = assessments_by_name["runs_skipped_low_volume"].feedback
    assert runs_skipped_feedback is not None
    assert runs_skipped_feedback.value == 0.0


def test_eval_cells_fp_flow_emits_job_trace_with_all_4_steps_and_feedback(
    client: MlflowClient, tmp_path: Path
) -> None:
    """job_trace wiring (2026-07-11): a real, non-empty FP flow logs one
    trace into `finish-position/cell-eval` with all 4 documented steps and
    `runs_created`/`runs_skipped_low_volume` Feedback matching the returned
    summary -- reuses `test_fp_flow_creates_one_run_per_model_version_per_
    cell`'s exact fixture (3 runs created, 0 skipped)."""
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "iter14", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("05", "02", "H2", "iter14-jockeyA", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("10", "01", "H3", "other-model", 1, gen_at, "mile", "medium", "summer", "B", "dirt",
         "2026", "0601"),
    ]
    results_by_date = {
        "20260601": [
            ("05", "01", "H1", "1", "01"),
            ("05", "02", "H2", "1", "01"),
            ("10", "01", "H3", "2", "01"),
        ],
    }

    cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )

    traces = _cell_eval_traces(client, config.EXPERIMENT_FP_CELL_EVAL)
    assert len(traces) == 1
    trace = traces[0]
    assert trace.info.state == TraceState.OK
    span_names = {span.name for span in trace.data.spans}
    assert span_names == {
        "eval-cells",
        "resolve-champion",
        "collect-rows",
        "build-eval-rows",
        "log-runs",
    }
    assessments_by_name = {a.name: a for a in trace.info.assessments}
    runs_created_feedback = assessments_by_name["runs_created"].feedback
    assert runs_created_feedback is not None
    assert runs_created_feedback.value == 3.0
    runs_skipped_feedback = assessments_by_name["runs_skipped_low_volume"].feedback
    assert runs_skipped_feedback is not None
    assert runs_skipped_feedback.value == 0.0


def test_eval_cells_running_style_job_trace_lands_in_rs_cell_eval(
    client: MlflowClient,
) -> None:
    """RS payloads route their job trace into `running-style/cell-eval`,
    distinct from the FP experiment above."""
    cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "running-style",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )

    fp_experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert fp_experiment is None
    traces = _cell_eval_traces(client, config.EXPERIMENT_RS_CELL_EVAL)
    assert len(traces) == 1


# ── Full FP flow: champion / variant / other, run naming, tags, metrics ───


def test_fp_flow_creates_one_run_per_model_version_per_cell(
    client: MlflowClient, tmp_path: Path
) -> None:
    """3 races across 2 cells and 3 served model_versions:

    Cell A (venue=05, class=A, sprint/summer/turf/medium): champion "iter14"
    (race1, correct winner) and variant "iter14-jockeyA" (race2, correct
    winner) each get their OWN run despite sharing the same cell.
    Cell B (venue=10, class=B, mile/summer/dirt/medium): unrelated
    "other-model" (race3, WRONG winner -- predicted horse actually finished
    2nd) gets a third run.
    """
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "iter14", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("05", "02", "H2", "iter14-jockeyA", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("10", "01", "H3", "other-model", 1, gen_at, "mile", "medium", "summer", "B", "dirt",
         "2026", "0601"),
    ]
    results_by_date = {
        "20260601": [
            ("05", "01", "H1", "1", "01"),  # correct: actual rank 1
            ("05", "02", "H2", "1", "01"),  # correct: actual rank 1
            ("10", "01", "H3", "2", "01"),  # wrong: actual rank 2
        ],
    }

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )

    assert result.runs_created == 3
    assert result.runs_reused == 0
    assert result.cells_skipped_low_volume == 0
    assert result.champion_model_version == "iter14"
    assert result.min_races_used == 1
    assert result.volume_guard_triggered is False

    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    runs = client.search_runs([experiment.experiment_id])
    assert len(runs) == 3
    by_model_version = {r.data.tags["model_version"]: r for r in runs}

    champion_run = by_model_version["iter14"]
    assert champion_run.data.tags["champion_relation"] == "champion"
    assert champion_run.data.tags["category"] == "jra"
    assert champion_run.data.tags["venue"] == "05"
    assert champion_run.data.tags["class_code"] == "A"
    assert champion_run.data.tags["distance_band"] == "sprint"
    assert champion_run.data.tags["season_band"] == "summer"
    assert champion_run.data.tags["surface"] == "turf"
    assert champion_run.data.tags["field_size_band"] == "medium"
    assert champion_run.data.tags["window_days"] == str(cell_eval_runs.DEFAULT_WINDOW_DAYS)
    assert champion_run.data.tags["low_n"] == "true"
    assert champion_run.info.run_name == (
        "jra venue=05 class=A dist=sprint season=summer surf=turf fs=medium v=iter14"
    )
    assert champion_run.data.metrics["race_count"] == 1.0
    assert champion_run.data.metrics["top1_pct"] == pytest.approx(100.0)
    assert champion_run.data.metrics["place2_pct"] == pytest.approx(100.0)
    assert champion_run.data.metrics["place3_pct"] == pytest.approx(100.0)
    assert champion_run.data.metrics["fukusho_2p_pct"] == pytest.approx(100.0)
    assert champion_run.data.metrics["top3_box_pct"] == pytest.approx(0.0)
    # This race has only 1 finalized result (H1) -- total_starters=1, so
    # place4/5/6_pct are None (see serve_eval.aggregate_fp_day_metrics) and
    # must be entirely ABSENT from this run's metrics, never logged as a
    # fabricated 0.0 (this is the exact scenario the null-safety fix in
    # cell_eval_runs._fp_cell_metrics exists for -- see that function's own
    # docstring).
    assert "place4_pct" not in champion_run.data.metrics
    assert "place5_pct" not in champion_run.data.metrics
    assert "place6_pct" not in champion_run.data.metrics

    variant_run = by_model_version["iter14-jockeyA"]
    assert variant_run.data.tags["champion_relation"] == "variant"
    assert variant_run.data.tags["venue"] == "05"
    assert variant_run.info.run_id != champion_run.info.run_id

    other_run = by_model_version["other-model"]
    assert other_run.data.tags["champion_relation"] == "other"
    assert other_run.data.tags["venue"] == "10"
    assert other_run.data.metrics["top1_pct"] == pytest.approx(0.0)
    assert other_run.data.metrics["place2_pct"] == pytest.approx(100.0)
    assert other_run.data.metrics["place3_pct"] == pytest.approx(100.0)
    assert other_run.data.metrics["fukusho_2p_pct"] == pytest.approx(100.0)


def test_fp_place456_pct_logged_when_field_is_large_enough(
    client: MlflowClient, tmp_path: Path
) -> None:
    """A 6-starter field: place4_pct/place5_pct/place6_pct are all real,
    logged percentages -- proving the null-safety fix in
    `_fp_cell_metrics` only OMITS a None value, it does not somehow also
    suppress a genuinely-present one."""
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "iter14", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
    ]
    results_by_date = {
        "20260601": [
            ("05", "01", "H1", "1", "01"),
            ("05", "01", "H2", "2", "02"),
            ("05", "01", "H3", "3", "03"),
            ("05", "01", "H4", "4", "04"),
            ("05", "01", "H5", "5", "05"),
            ("05", "01", "H6", "6", "06"),
        ],
    }

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.runs_created == 1

    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    runs = client.search_runs([experiment.experiment_id])
    assert len(runs) == 1
    assert runs[0].data.metrics["place4_pct"] == pytest.approx(100.0)
    assert runs[0].data.metrics["place5_pct"] == pytest.approx(100.0)
    assert runs[0].data.metrics["place6_pct"] == pytest.approx(100.0)


def test_fp_near_miss_raw_prefix_without_dash_is_classified_other(
    client: MlflowClient, tmp_path: Path
) -> None:
    """A served model_version that shares the champion label as a raw
    string prefix WITHOUT the required "-" separator (`"iter140"` vs.
    champion `"iter14"`) must classify as "other", not "variant" -- the
    deliberately-tricky near-miss `_classify_champion_match` exists to
    reject (see cell_eval_runs.py's own module docstring)."""
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "iter140", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
    ]
    results_by_date = {"20260601": [("05", "01", "H1", "1", "01")]}

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.runs_created == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    run = client.search_runs([experiment.experiment_id])[0]
    assert run.data.tags["model_version"] == "iter140"
    assert run.data.tags["champion_relation"] == "other"


def test_fp_long_model_version_is_truncated_and_hashed_in_run_name(
    client: MlflowClient, tmp_path: Path
) -> None:
    """A model_version longer than `_VERSION_LABEL_MAX_LEN` (24) must be
    truncated with a hash suffix in the run NAME (cosmetic only -- the
    `model_version` TAG always carries the full, untruncated value, and
    idempotency is keyed by `cell_run_key`, never the name)."""
    long_version = "jra-cb-v9-sim-2013-clean-jockey-pedigree269"
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": long_version}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", long_version, 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
    ]
    results_by_date = {"20260601": [("05", "01", "H1", "1", "01")]}

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.runs_created == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    run = client.search_runs([experiment.experiment_id])[0]
    assert run.data.tags["model_version"] == long_version
    run_name = run.info.run_name
    assert run_name is not None
    assert long_version not in run_name
    assert run_name.startswith(
        "jra venue=05 class=A dist=sprint season=summer surf=turf fs=medium v="
    )


def test_fp_none_class_code_renders_as_literal_none_tag_and_run_name(
    client: MlflowClient,
) -> None:
    """A row whose `class_code` is None (a real, nullable source column)
    must render as the literal string "none" in both the `class_code` tag
    and the run name -- never a Python `None`/crash, and never silently
    dropped from the cell identity."""
    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "modelA", 1, gen_at, "sprint", "medium", "summer", None, "turf",
         "2026", "0601"),
    ]
    results_by_date = {"20260601": [("05", "01", "H1", "1", "01")]}

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.runs_created == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    run = client.search_runs([experiment.experiment_id])[0]
    assert run.data.tags["class_code"] == "none"
    run_name = run.info.run_name
    assert run_name is not None
    assert "class=none" in run_name


def test_fp_low_n_false_for_cell_with_at_least_min_cell_count_units(
    client: MlflowClient, tmp_path: Path
) -> None:
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "solo-model"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    unit_count = champion_cell_eval.MIN_CELL_COUNT
    prediction_rows: list[tuple[object, ...]] = []
    results: list[tuple[object, ...]] = []
    for i in range(unit_count):
        race_bango = f"{i + 1:02d}"
        horse = f"H{i}"
        prediction_rows.append(
            ("05", race_bango, horse, "solo-model", 1, gen_at, "sprint", "medium", "summer", "A",
             "turf", "2026", "0601")
        )
        results.append(("05", race_bango, horse, "1", "01"))
    results_by_date = {"20260601": results}

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.runs_created == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    run = client.search_runs([experiment.experiment_id])[0]
    assert run.data.tags["low_n"] == "false"
    assert run.data.metrics["race_count"] == float(unit_count)


def test_fp_low_volume_cell_skipped_from_run_creation_but_counted(
    client: MlflowClient,
) -> None:
    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "modelA", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("05", "02", "H2", "modelA", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
    ]
    results_by_date = {
        "20260601": [("05", "01", "H1", "1", "01"), ("05", "02", "H2", "1", "01")],
    }

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=3,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.runs_created == 0
    assert result.cells_skipped_low_volume == 1


def test_fp_multiple_dates_share_cached_results_within_one_group(
    client: MlflowClient, tmp_path: Path
) -> None:
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_0601 = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    gen_0602 = datetime(2026, 6, 2, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "iter14", 1, gen_0601, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("05", "01", "H2", "iter14", 1, gen_0602, "sprint", "medium", "summer", "A", "turf",
         "2026", "0602"),
    ]
    results_by_date = {
        "20260601": [("05", "01", "H1", "1", "01")],
        "20260602": [("05", "01", "H2", "1", "01")],
    }

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.runs_created == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    run = client.search_runs([experiment.experiment_id])[0]
    assert run.data.metrics["race_count"] == 2.0


# ── Ban-ei partitioning ──────────────────────────────────────────────────


def test_banei_partitioning_keeps_only_keibajo_83(client: MlflowClient) -> None:
    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("83", "01", "B1", "banei-v1", 1, gen_at, "sprint", "medium", "summer", "A", "dirt",
         "2026", "0601"),
        ("30", "01", "N1", "banei-v1", 1, gen_at, "sprint", "medium", "summer", "A", "dirt",
         "2026", "0601"),
    ]
    results_by_date = {
        "20260601": [("83", "01", "B1", "1", "00"), ("30", "01", "N1", "1", "00")],
    }

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "banei",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.runs_created == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    run = client.search_runs([experiment.experiment_id])[0]
    assert run.data.tags["venue"] == "83"


def test_nar_partitioning_excludes_banei_rows(client: MlflowClient) -> None:
    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("30", "01", "N1", "nar-v1", 1, gen_at, "sprint", "medium", "summer", "A", "dirt",
         "2026", "0601"),
        ("83", "01", "B1", "nar-v1", 1, gen_at, "sprint", "medium", "summer", "A", "dirt",
         "2026", "0601"),
    ]
    results_by_date = {
        "20260601": [("30", "01", "N1", "1", "00"), ("83", "01", "B1", "1", "00")],
    }

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "nar",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.runs_created == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    run = client.search_runs([experiment.experiment_id])[0]
    assert run.data.tags["venue"] == "30"


# ── Full RS flow: metric shape, per-class f1 omission ─────────────────────


def test_rs_flow_logs_horse_count_accuracy_and_defined_per_class_f1(
    client: MlflowClient, tmp_path: Path
) -> None:
    version = registry.register_version(
        client, "jra-running-style", f"file://{tmp_path}", tags={"model_version": "rsv3"}
    )
    registry.set_champion(client, "jra-running-style", version.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    # corner1_norm = (5-1)/(10-1) = 0.444 -> actual class = sashi; predicted
    # sashi too -> a hit, and (uniquely for this cell) the only class with a
    # defined precision/recall/F1 this call.
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "rsv3", serve_eval.RS_CLASS_SASHI, "sashi", gen_at, "2026", "0601"),
    ]
    results_by_date = {"20260601": [("05", "01", "H1", "5", "05")]}
    meta_by_date = {"20260601": [("05", "01", 1200, 10, "10", "A")]}

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "running-style",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date, meta_by_date),
    )
    assert result.runs_created == 1
    assert result.champion_model_version == "rsv3"

    experiment = client.get_experiment_by_name(config.EXPERIMENT_RS_CELL_EVAL)
    assert experiment is not None
    run = client.search_runs([experiment.experiment_id])[0]
    assert run.data.tags["champion_relation"] == "champion"
    assert run.data.tags["venue"] == "05"
    assert run.data.tags["class_code"] == "A"
    assert run.data.tags["distance_band"] == "sprint"
    assert run.data.tags["surface"] == "turf"
    assert "season_band" not in run.data.tags
    assert "field_size_band" not in run.data.tags
    assert run.data.metrics["horse_count"] == 1.0
    assert run.data.metrics["accuracy_pct"] == pytest.approx(100.0)
    assert run.data.metrics["macro_f1_pct"] == pytest.approx(100.0)
    assert run.data.metrics["f1_sashi_pct"] == pytest.approx(100.0)
    assert "f1_nige_pct" not in run.data.metrics
    assert "f1_senkou_pct" not in run.data.metrics
    assert "f1_oikomi_pct" not in run.data.metrics


def test_rs_flow_omits_macro_f1_and_per_class_when_entirely_undefined(
    client: MlflowClient,
) -> None:
    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    # corner1_norm = (2-1)/(10-1) = 0.111 -> actual class = senkou; predicted
    # nige -> a miss, and NO class this call has both a predicted AND an
    # actual member, so every class's precision-or-recall is undefined and
    # macro_f1/per-class f1 are entirely omitted (see serve_eval.aggregate_
    # rs_day_metrics's own None-propagation rules).
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "rsv3", serve_eval.RS_CLASS_NIGE, "nige", gen_at, "2026", "0601"),
    ]
    results_by_date = {"20260601": [("05", "01", "H1", "5", "02")]}
    meta_by_date = {"20260601": [("05", "01", 1200, 10, "10", "A")]}

    result = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "running-style",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date, meta_by_date),
    )
    assert result.runs_created == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_RS_CELL_EVAL)
    assert experiment is not None
    run = client.search_runs([experiment.experiment_id])[0]
    assert run.data.metrics["horse_count"] == 1.0
    assert run.data.metrics["accuracy_pct"] == pytest.approx(0.0)
    assert "macro_f1_pct" not in run.data.metrics
    assert "f1_nige_pct" not in run.data.metrics
    assert "f1_senkou_pct" not in run.data.metrics


# ── Idempotency ───────────────────────────────────────────────────────────


def test_idempotent_same_day_rerun_is_cheap_noop(
    client: MlflowClient, tmp_path: Path
) -> None:
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "iter14", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
    ]
    results_by_date = {"20260601": [("05", "01", "H1", "1", "01")]}
    neon_connect = _neon_factory(prediction_rows)
    local_connect = _local_factory(results_by_date)

    first = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    assert first.runs_created == 1
    assert first.runs_reused == 0

    second = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_races=1,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    assert second.runs_created == 0
    assert second.runs_reused == 1
    assert second.champion_model_version == "iter14"
    # eval-cells has NO whole-call idempotency short-circuit (unlike champion_
    # cell_eval's cell_eval_key) -- only the per-run metric point is deduped
    # by step -- so the DB is re-queried every call.
    assert neon_connect.call_count == 2

    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    runs = client.search_runs([experiment.experiment_id])
    assert len(runs) == 1
    history = client.get_metric_history(runs[0].info.run_id, "top1_pct")
    assert len(history) == 1


def test_new_as_of_date_appends_a_new_metric_point(
    client: MlflowClient, tmp_path: Path
) -> None:
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "iter14", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
    ]
    results_by_date = {"20260601": [("05", "01", "H1", "1", "01")]}
    neon_connect = _neon_factory(prediction_rows)
    local_connect = _local_factory(results_by_date)

    cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=date(2026, 7, 5),
        min_races=1,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    second = cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=date(2026, 7, 6),
        min_races=1,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    assert second.runs_created == 0
    assert second.runs_reused == 1

    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    runs = client.search_runs([experiment.experiment_id])
    assert len(runs) == 1
    history = client.get_metric_history(runs[0].info.run_id, "top1_pct")
    assert len(history) == 2
    steps = {point.step for point in history}
    assert steps == {timeline.step_for_date("20260705"), timeline.step_for_date("20260706")}
    timestamps_by_step = {point.step: point.timestamp for point in history}
    # Every metric point is timestamped at 12:00 JST on its OWN as_of date
    # (mirroring timeline._timestamp_ms_for_date's convention), not whenever
    # the call actually ran.
    expected_0705 = int(datetime(2026, 7, 5, 3, 0, 0, tzinfo=UTC).timestamp() * 1000)
    expected_0706 = int(datetime(2026, 7, 6, 3, 0, 0, tzinfo=UTC).timestamp() * 1000)
    assert timestamps_by_step[timeline.step_for_date("20260705")] == expected_0705
    assert timestamps_by_step[timeline.step_for_date("20260706")] == expected_0706


def test_champion_relation_tag_is_reapplied_when_champion_changes(
    client: MlflowClient, tmp_path: Path
) -> None:
    version1 = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version1.version)

    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "iter14", 1, gen_at, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
    ]
    results_by_date = {"20260601": [("05", "01", "H1", "1", "01")]}
    neon_connect = _neon_factory(prediction_rows)
    local_connect = _local_factory(results_by_date)

    cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=date(2026, 7, 5),
        min_races=1,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_CELL_EVAL)
    assert experiment is not None
    run_id = client.search_runs([experiment.experiment_id])[0].info.run_id
    assert client.get_run(run_id).data.tags["champion_relation"] == "champion"

    # Promote a DIFFERENT version to champion; "iter14" is now unrelated to
    # the new champion label.
    version2 = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter15"}
    )
    registry.set_champion(client, "jra-finish-position", version2.version)

    cell_eval_runs.eval_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=date(2026, 7, 6),
        min_races=1,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    assert client.get_run(run_id).data.tags["champion_relation"] == "other"


# ── Volume guard ──────────────────────────────────────────────────────────


def test_volume_guard_raises_floor_warns_and_reports_projected_count(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cell_eval_runs, "MAX_RUNS_PER_CATEGORY_TASK", 1)
    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)

    prediction_rows: list[tuple[object, ...]] = []
    results: list[tuple[object, ...]] = []
    race_counter = 0
    for class_code, count in (("A", 3), ("B", 2), ("C", 1)):
        for _ in range(count):
            race_counter += 1
            race_bango = f"{race_counter:02d}"
            horse = f"H{race_counter}"
            prediction_rows.append(
                ("05", race_bango, horse, "modelA", 1, gen_at, "sprint", "medium", "summer",
                 class_code, "turf", "2026", "0601")
            )
            results.append(("05", race_bango, horse, "1", "01"))
    results_by_date = {"20260601": results}

    with pytest.warns(UserWarning, match="volume guard triggered"):
        result = cell_eval_runs.eval_cells_for_category(
            client,
            "jra",
            "finish-position",
            as_of=_AS_OF,
            min_races=1,
            neon_connect=_neon_factory(prediction_rows),
            local_connect=_local_factory(results_by_date),
        )

    assert result.volume_guard_triggered is True
    assert result.projected_run_count_at_requested_floor == 3
    assert result.min_races_used == 3
    assert result.runs_created == 1
    assert result.cells_skipped_low_volume == 2


def test_volume_guard_tie_at_boundary_excludes_all_tied_groups(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """4 distinct cells, each with the SAME count (2 races) -- a tie at
    exactly the cap boundary. The documented tie-break (exclude ALL tied
    groups rather than risk exceeding the cap) means the new floor ends up
    higher than any of the tied counts, so every one of the 4 groups is
    skipped -- fewer than the cap, but never more."""
    monkeypatch.setattr(cell_eval_runs, "MAX_RUNS_PER_CATEGORY_TASK", 2)
    gen_at = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)

    prediction_rows: list[tuple[object, ...]] = []
    results: list[tuple[object, ...]] = []
    race_counter = 0
    for class_code in ("A", "B", "C", "D"):
        for _ in range(2):
            race_counter += 1
            race_bango = f"{race_counter:02d}"
            horse = f"H{race_counter}"
            prediction_rows.append(
                ("05", race_bango, horse, "modelA", 1, gen_at, "sprint", "medium", "summer",
                 class_code, "turf", "2026", "0601")
            )
            results.append(("05", race_bango, horse, "1", "01"))
    results_by_date = {"20260601": results}

    with pytest.warns(UserWarning, match="volume guard triggered"):
        result = cell_eval_runs.eval_cells_for_category(
            client,
            "jra",
            "finish-position",
            as_of=_AS_OF,
            min_races=1,
            neon_connect=_neon_factory(prediction_rows),
            local_connect=_local_factory(results_by_date),
        )

    assert result.volume_guard_triggered is True
    assert result.projected_run_count_at_requested_floor == 4
    assert result.min_races_used == 3  # (2 + 1): strictly above every tied count of 2
    assert result.runs_created == 0
    assert result.cells_skipped_low_volume == 4


# ── eval_cells wrapper: category/task fan-out + failure isolation ────────


def test_eval_cells_excludes_running_style_for_banei(client: MlflowClient) -> None:
    results = cell_eval_runs.eval_cells(
        client,
        categories=("banei",),
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )
    assert len(results) == 1
    assert results[0].category == "banei"
    assert results[0].task == "finish-position"
    assert client.get_experiment_by_name(config.EXPERIMENT_RS_CELL_EVAL) is None


def test_eval_cells_isolates_per_category_failures(client: MlflowClient) -> None:
    call_state = {"n": 0}

    def flaky_neon_connect() -> _RoutedFakeConnection:
        call_state["n"] += 1
        if call_state["n"] == 1:
            raise psycopg2.OperationalError("boom")
        return _make_neon_conn([])

    neon_connect = MagicMock(side_effect=flaky_neon_connect)
    local_connect = _local_factory({})

    with pytest.warns(UserWarning, match="jra"):
        results = cell_eval_runs.eval_cells(
            client,
            categories=("jra", "nar"),
            neon_connect=neon_connect,
            local_connect=local_connect,
        )

    # 4 pairs attempted (jra/nar x finish-position/running-style), 1 fails.
    assert len(results) == 3
    assert not any(r.category == "jra" and r.task == "finish-position" for r in results)
