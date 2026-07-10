"""Tests for mlflow_tracking.refresh_eval_metrics.

Entirely hermetic: `FakeNeonConnection`/`FakeLocalConnection` below are hand-
built fakes (never a real psycopg2 connection), mirroring test_sync_
production.py's own fake-connection dispatch-by-SQL-substring-and-date
convention exactly (this module's query shape -- one single-date
`fetch_fp_prediction_rows`/`fetch_race_results` call per distinct (date,
category) pair -- is identical to `sync_production.py`'s own per-day shape).
The `client` fixture (isolated sqlite MlflowClient, see conftest.py) is used
for the MLflow side throughout; "already-evaluated" production-usage runs
are built directly via `client.create_run`/`log_batch` (NOT via
`sync_production.sync_production_range`) specifically so they mimic a run
logged BEFORE `serve_eval.aggregate_fp_day_metrics` grew the
place4/5/6_pct keys -- calling `sync_production_range` today would already
include those keys from the start, which would never exercise this module's
actual backfill path.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

import psycopg2
import pytest
from mlflow import MlflowClient
from mlflow.entities import Metric, Run
from mlflow.store.entities.paged_list import PagedList

from mlflow_tracking import config, refresh_eval_metrics, serve_eval, timeline
from mlflow_tracking.logging_api import get_or_create_experiment, log_batch_chunked
from mlflow_tracking.sync_production import SYNC_EVAL_LOGGED_TAG, TRUE_STR

DATE_STR: Final[str] = "20260614"
GEN_AT: Final[datetime] = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)


# ── Fake connections (mirrors test_sync_production.py's own dispatch idiom) ─


class _FakeNeonCursor:
    _conn: FakeNeonConnection

    def __init__(self, conn: FakeNeonConnection) -> None:
        self._conn = conn
        self._pending: list[tuple[object, ...]] = []

    def execute(self, query: str, params: object = None) -> None:
        assert isinstance(params, tuple)
        source, date_from, date_to = params
        assert date_from == date_to
        date_str = str(date_from)
        if date_str in self._conn.raise_for:
            raise psycopg2.OperationalError(f"boom fp {date_str}")
        self._pending = self._conn.fp_rows.get((str(source), date_str), [])

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._pending


class FakeNeonConnection:
    fp_rows: dict[tuple[str, str], list[tuple[object, ...]]]
    raise_for: frozenset[str]
    closed: bool

    def __init__(
        self,
        fp_rows: dict[tuple[str, str], list[tuple[object, ...]]] | None = None,
        raise_for: frozenset[str] = frozenset(),
    ) -> None:
        self.fp_rows = fp_rows or {}
        self.raise_for = raise_for
        self.closed = False

    def cursor(self) -> _FakeNeonCursor:
        return _FakeNeonCursor(self)

    def close(self) -> None:
        self.closed = True


class _FakeLocalCursor:
    _conn: FakeLocalConnection

    def __init__(self, conn: FakeLocalConnection) -> None:
        self._conn = conn
        self._pending: list[tuple[object, ...]] = []

    def execute(self, query: str, params: object = None) -> None:
        assert isinstance(params, tuple)
        date_str = f"{params[0]}{params[1]}"
        if date_str in self._conn.raise_for:
            raise psycopg2.OperationalError(f"boom local {date_str}")
        self._pending = self._conn.result_rows.get(date_str, [])

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._pending


class FakeLocalConnection:
    result_rows: dict[str, list[tuple[object, ...]]]
    raise_for: frozenset[str]
    closed: bool

    def __init__(
        self,
        result_rows: dict[str, list[tuple[object, ...]]] | None = None,
        raise_for: frozenset[str] = frozenset(),
    ) -> None:
        self.result_rows = result_rows or {}
        self.raise_for = raise_for
        self.closed = False

    def cursor(self) -> _FakeLocalCursor:
        return _FakeLocalCursor(self)

    def close(self) -> None:
        self.closed = True


def _fp_row(
    keibajo_code: str,
    race_bango: str,
    ketto: str,
    model_version: str,
    predicted_rank: int,
    generated_at: datetime = GEN_AT,
    *,
    kaisai_nen: str = "2026",
    kaisai_tsukihi: str = "0614",
) -> tuple[object, ...]:
    return (
        keibajo_code,
        race_bango,
        ketto,
        model_version,
        predicted_rank,
        generated_at,
        "sprint",
        "medium",
        "summer",
        "A",
        "turf",
        kaisai_nen,
        kaisai_tsukihi,
    )


def _result_row(
    keibajo_code: str, race_bango: str, ketto: str, kakutei_chakujun: str, corner_1: str = "01"
) -> tuple[object, ...]:
    return (keibajo_code, race_bango, ketto, kakutei_chakujun, corner_1)


def _make_legacy_evaluated_run(
    client: MlflowClient,
    *,
    date_str: str = DATE_STR,
    category: str = "jra",
    model_version: str = "iter14",
    extra_metrics: dict[str, float] | None = None,
) -> str:
    """Build a run that mimics an already-evaluated `sync-production` FP run
    from BEFORE place4/5/6_pct existed: `sync_eval_logged=true`, the
    date/category/model_version tags `refresh_fp_place456_metrics` reads,
    and a `fp_top1_pct` metric but deliberately NO `fp_place{4,5,6}_pct`."""
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    run = client.create_run(
        experiment_id,
        tags={
            "sync_key": f"{date_str}:{category}:{model_version}",
            "task": "finish-position",
            "category": category,
            "model_version": model_version,
            "date": date_str,
            SYNC_EVAL_LOGGED_TAG: TRUE_STR,
            "sync_base_logged": TRUE_STR,
        },
    )
    metrics = {"fp_top1_pct": 100.0, "fp_races_evaluated": 1.0, **(extra_metrics or {})}
    log_batch_chunked(
        client, run.info.run_id, metrics=[Metric(k, v, 0, 0) for k, v in metrics.items()]
    )
    return run.info.run_id


# ── No-op / empty-state paths ────────────────────────────────────────────


def test_no_experiment_returns_empty_summary(client: MlflowClient) -> None:
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client,
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert summary.runs_scanned == 0
    assert summary.runs_updated == 0
    assert summary.errors == []


def test_experiment_exists_but_no_evaluated_runs(client: MlflowClient) -> None:
    get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client,
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert summary.runs_scanned == 0


# ── Happy path: backfill + timeline point ────────────────────────────────


def test_backfills_place456_onto_legacy_run_and_timeline(client: MlflowClient) -> None:
    run_id = _make_legacy_evaluated_run(client)
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
    )
    local = FakeLocalConnection(
        result_rows={
            DATE_STR: [
                _result_row("05", "01", "H1", "1"),
                _result_row("05", "01", "H2", "2"),
                _result_row("05", "01", "H3", "3"),
                _result_row("05", "01", "H4", "4"),
                _result_row("05", "01", "H5", "5"),
                _result_row("05", "01", "H6", "6"),
            ]
        }
    )
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )

    assert summary.runs_scanned == 1
    assert summary.runs_updated == 1
    assert summary.runs_skipped_already_enriched == 0
    assert summary.runs_skipped_not_applicable == 0
    assert summary.metric_points_appended == 3
    assert summary.timeline_points_appended == 3
    assert summary.errors == []
    assert neon.closed is True
    assert local.closed is True

    run = client.get_run(run_id)
    assert run.data.metrics["fp_place4_pct"] == 100.0
    assert run.data.metrics["fp_place5_pct"] == 100.0
    assert run.data.metrics["fp_place6_pct"] == 100.0
    # Pre-existing metrics/tags must be untouched.
    assert run.data.metrics["fp_top1_pct"] == 100.0
    assert run.data.tags[SYNC_EVAL_LOGGED_TAG] == TRUE_STR

    fp_dates = timeline.timeline_dates_present(client, "finish-position", "jra", "fp_place4_pct")
    assert timeline.step_for_date(DATE_STR) in fp_dates


def test_backfills_partial_ranks_when_field_is_exactly_5(client: MlflowClient) -> None:
    """A 5-starter field: place4/place5_pct backfill, place6_pct stays
    entirely absent (None -- not applicable, see serve_eval's own
    docstring)."""
    run_id = _make_legacy_evaluated_run(client)
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
    )
    local = FakeLocalConnection(
        result_rows={
            DATE_STR: [
                _result_row("05", "01", "H1", "1"),
                _result_row("05", "01", "H2", "2"),
                _result_row("05", "01", "H3", "3"),
                _result_row("05", "01", "H4", "4"),
                _result_row("05", "01", "H5", "5"),
            ]
        }
    )
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )

    assert summary.runs_updated == 1
    assert summary.metric_points_appended == 2
    run = client.get_run(run_id)
    assert run.data.metrics["fp_place4_pct"] == 100.0
    assert run.data.metrics["fp_place5_pct"] == 100.0
    assert "fp_place6_pct" not in run.data.metrics


# ── Idempotency / skip paths ──────────────────────────────────────────────


def test_skips_run_already_enriched_no_requery(client: MlflowClient) -> None:
    _make_legacy_evaluated_run(
        client,
        extra_metrics={"fp_place4_pct": 50.0, "fp_place5_pct": 50.0, "fp_place6_pct": 50.0},
    )
    # A connection that raises on ANY query -- proves this run's group was
    # never even queried, since it was fully pre-enriched.
    neon = FakeNeonConnection(raise_for=frozenset({DATE_STR}))
    local = FakeLocalConnection(raise_for=frozenset({DATE_STR}))

    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_scanned == 1
    assert summary.runs_skipped_already_enriched == 1
    assert summary.runs_updated == 0
    assert summary.errors == []


def test_not_applicable_run_recomputes_but_appends_nothing(client: MlflowClient) -> None:
    """Every race for this run's (date, category, model_version) has a
    too-small field for rank 4/5/6 -- the run is queried (unlike the
    already-enriched case) but nothing gets appended, and this is recorded
    as `runs_skipped_not_applicable`, not an error."""
    _make_legacy_evaluated_run(client)
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
    )
    local = FakeLocalConnection(
        result_rows={
            DATE_STR: [_result_row("05", "01", "H1", "1"), _result_row("05", "01", "H2", "2")]
        }
    )
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_updated == 0
    assert summary.runs_skipped_not_applicable == 1
    assert summary.metric_points_appended == 0
    assert summary.errors == []


# ── Defensive/error paths (isolated, never abort the whole call) ─────────


def test_missing_date_tag_records_error(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    client.create_run(
        experiment_id,
        tags={"category": "jra", "model_version": "iter14", SYNC_EVAL_LOGGED_TAG: TRUE_STR},
    )
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client,
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert summary.runs_scanned == 1
    assert len(summary.errors) == 1
    assert "missing date/category tag" in summary.errors[0]


def test_missing_model_version_tag_records_error(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    client.create_run(
        experiment_id,
        tags={"category": "jra", "date": DATE_STR, SYNC_EVAL_LOGGED_TAG: TRUE_STR},
    )
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(result_rows={DATE_STR: [_result_row("05", "01", "H1", "1")]})
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert len(summary.errors) == 1
    assert "missing model_version tag" in summary.errors[0]


def test_no_matching_prediction_rows_on_refetch_records_error(client: MlflowClient) -> None:
    """The run's model_version no longer appears among re-fetched genuine
    rows (defensive -- should not happen in practice against immutable
    data, but must not crash)."""
    _make_legacy_evaluated_run(client)
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): []})
    local = FakeLocalConnection(result_rows={})
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_updated == 0
    assert len(summary.errors) == 1
    assert "no prediction rows found on re-fetch" in summary.errors[0]


def test_no_matched_races_on_refetch_records_error(client: MlflowClient) -> None:
    """Predictions re-fetch fine, but the local replica has zero finalized
    results for this date -- the join yields zero eval rows."""
    _make_legacy_evaluated_run(client)
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(result_rows={})
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_updated == 0
    assert len(summary.errors) == 1
    assert "no matched races found on re-fetch" in summary.errors[0]


def test_query_failure_isolated_per_date_category_group(client: MlflowClient) -> None:
    """A query failure for one (date, category) group is recorded as an
    error and does not raise -- mirrors sync_production.py's own
    per-group isolation."""
    _make_legacy_evaluated_run(client)
    neon = FakeNeonConnection(raise_for=frozenset({DATE_STR}))
    local = FakeLocalConnection()
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_updated == 0
    assert len(summary.errors) == 1
    assert "query failed" in summary.errors[0]


def test_two_date_category_groups_one_fails_other_succeeds(client: MlflowClient) -> None:
    """Isolation across TWO distinct (date, category) groups sharing one
    call: a query failure for one must never prevent the other from being
    backfilled."""
    good_date = "20260615"
    _make_legacy_evaluated_run(client, date_str=DATE_STR)
    _make_legacy_evaluated_run(client, date_str=good_date, model_version="iter20")

    neon = FakeNeonConnection(
        fp_rows={
            ("jra", good_date): [_fp_row("05", "01", "H1", "iter20", 1, kaisai_tsukihi="0615")]
        },
        raise_for=frozenset({DATE_STR}),
    )
    local = FakeLocalConnection(
        result_rows={
            good_date: [
                _result_row("05", "01", "H1", "1"),
                _result_row("05", "01", "H2", "2"),
                _result_row("05", "01", "H3", "3"),
                _result_row("05", "01", "H4", "4"),
            ]
        }
    )
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_scanned == 2
    assert summary.runs_updated == 1
    assert len(summary.errors) == 1
    assert "query failed" in summary.errors[0]


# ── Ban-ei partitioning ────────────────────────────────────────────────────


def test_nar_category_excludes_banei_rows_on_refetch(client: MlflowClient) -> None:
    """category="nar" must keep only the non-Ban-ei partition on re-fetch --
    a Ban-ei row sharing the same date/source must never leak into a nar
    run's rebuilt group."""
    _make_legacy_evaluated_run(client, category="nar", model_version="nar-model")
    neon = FakeNeonConnection(
        fp_rows={
            ("nar", DATE_STR): [
                _fp_row("30", "01", "H1", "nar-model", 1),
                _fp_row("83", "02", "B1", "nar-model", 1),  # Ban-ei -- must be excluded
            ]
        }
    )
    local = FakeLocalConnection(
        result_rows={
            DATE_STR: [
                _result_row("30", "01", "H1", "1"),
                _result_row("30", "01", "H2", "2"),
                _result_row("30", "01", "H3", "3"),
                _result_row("30", "01", "H4", "4"),
            ]
        }
    )
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_updated == 1
    assert summary.errors == []


def test_banei_category_keeps_only_banei_rows_on_refetch(client: MlflowClient) -> None:
    _make_legacy_evaluated_run(client, category="banei", model_version="banei-model")
    neon = FakeNeonConnection(
        fp_rows={
            ("nar", DATE_STR): [
                _fp_row("30", "01", "H1", "banei-model", 1),  # non-Ban-ei -- excluded
                _fp_row("83", "02", "B1", "banei-model", 1),
            ]
        }
    )
    local = FakeLocalConnection(
        result_rows={
            DATE_STR: [
                _result_row("83", "02", "B1", "1"),
                _result_row("83", "02", "B2", "2"),
                _result_row("83", "02", "B3", "3"),
                _result_row("83", "02", "B4", "4"),
            ]
        }
    )
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_updated == 1
    assert summary.errors == []


# ── Defensive branches only reachable via monkeypatch (mirrors this ─────────
# package's established convention -- see e.g. test_champion_cell_eval.py's
# test_get_model_version_failure_after_alias_resolves_treated_as_no_champion
# -- for simulating an otherwise-unreachable-against-real-inputs failure) ──


def test_eval_join_failure_isolated_per_run(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`serve_eval.build_fp_race_eval_rows`/`aggregate_fp_day_metrics` are
    pure functions that essentially never raise on well-formed input in
    practice -- this monkeypatches `aggregate_fp_day_metrics` to simulate a
    defensive failure and confirm it is isolated per-run, never aborting
    the whole call."""
    _make_legacy_evaluated_run(client)
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(result_rows={DATE_STR: [_result_row("05", "01", "H1", "1")]})

    def boom(_rows: object) -> dict[str, float | None]:
        raise ValueError("simulated aggregation failure")

    monkeypatch.setattr(serve_eval, "aggregate_fp_day_metrics", boom)
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_updated == 0
    assert len(summary.errors) == 1
    assert "eval join failed" in summary.errors[0]


def test_metric_logging_failure_isolated_per_run(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A tracking-store failure while logging the new metrics (e.g. a
    transient sqlite lock) must be isolated per-run, not raised."""
    _make_legacy_evaluated_run(client)
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
    )
    local = FakeLocalConnection(
        result_rows={
            DATE_STR: [
                _result_row("05", "01", "H1", "1"),
                _result_row("05", "01", "H2", "2"),
                _result_row("05", "01", "H3", "3"),
                _result_row("05", "01", "H4", "4"),
            ]
        }
    )

    def boom(*_args: object, **_kwargs: object) -> None:
        raise ValueError("simulated logging failure")

    monkeypatch.setattr(refresh_eval_metrics, "log_batch_chunked", boom)
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.runs_updated == 0
    assert len(summary.errors) == 1
    assert "failed to log metrics" in summary.errors[0]


# ── Pagination ────────────────────────────────────────────────────────────


def test_pagination_across_multiple_search_runs_pages(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_find_evaluated_fp_runs` must follow `page_token` until exhausted --
    simulated here via 2 real runs split across 2 fake pages (both
    already-enriched, so no query is actually needed; this test targets
    ONLY the pagination merge, not the join logic)."""
    _make_legacy_evaluated_run(
        client,
        model_version="iter14",
        extra_metrics={"fp_place4_pct": 1.0, "fp_place5_pct": 1.0, "fp_place6_pct": 1.0},
    )
    _make_legacy_evaluated_run(
        client,
        model_version="iter20",
        extra_metrics={"fp_place4_pct": 1.0, "fp_place5_pct": 1.0, "fp_place6_pct": 1.0},
    )
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    all_runs = client.search_runs([experiment.experiment_id])
    assert len(all_runs) == 2
    run_a, run_b = all_runs[0], all_runs[1]

    call_tokens: list[str | None] = []
    original_search_runs = client.search_runs

    def fake_search_runs(
        experiment_ids: list[str],
        filter_string: str = "",
        run_view_type: int = 1,
        max_results: int = 1000,
        order_by: list[str] | None = None,
        page_token: str | None = None,
    ) -> PagedList[Run]:
        call_tokens.append(page_token)
        if page_token is None:
            return PagedList([run_a], "page-2-token")
        return PagedList([run_b], None)

    monkeypatch.setattr(client, "search_runs", fake_search_runs)
    summary = refresh_eval_metrics.refresh_fp_place456_metrics(
        client,
        neon_connect=lambda: FakeNeonConnection(raise_for=frozenset({DATE_STR})),
        local_connect=lambda: FakeLocalConnection(raise_for=frozenset({DATE_STR})),
    )
    monkeypatch.setattr(client, "search_runs", original_search_runs)

    assert call_tokens == [None, "page-2-token"]
    assert summary.runs_scanned == 2
    assert summary.runs_skipped_already_enriched == 2
