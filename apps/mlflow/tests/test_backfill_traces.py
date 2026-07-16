"""Tests for mlflow_tracking.backfill_traces.

Entirely hermetic: `FakeNeonConnection`/`FakeLocalConnection` mirror
test_sync_production.py's own fake-connection dispatch-by-SQL-substring-and-
date convention exactly (this module issues the SAME `serve_eval` queries).
"already-evaluated" production-usage runs are built directly via
`client.create_run` + tags (mirroring test_refresh_eval_metrics.py's own
`_make_legacy_evaluated_run` precedent) -- specifically WITHOUT ever calling
`sync_production.sync_production_range`, so they simulate a run that was
`sync_eval_logged=true` from BEFORE trace emission existed in this package
(exactly the state this module exists to catch up), with zero traces of its
own yet.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

import psycopg2
import pytest
from mlflow import MlflowClient
from mlflow.entities import Run
from mlflow.exceptions import MlflowException
from mlflow.store.entities.paged_list import PagedList

from mlflow_tracking import backfill_traces, config, serve_eval, trace_emit
from mlflow_tracking.logging_api import get_or_create_experiment
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
        if "race_finish_position_model_predictions" in query:
            if date_str in self._conn.raise_for_fp:
                raise psycopg2.OperationalError(f"boom fp {date_str}")
            self._pending = self._conn.fp_rows.get((str(source), date_str), [])
            return
        if date_str in self._conn.raise_for_rs:
            raise psycopg2.OperationalError(f"boom rs {date_str}")
        self._pending = self._conn.rs_rows.get((str(source), date_str), [])

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._pending


class FakeNeonConnection:
    fp_rows: dict[tuple[str, str], list[tuple[object, ...]]]
    rs_rows: dict[tuple[str, str], list[tuple[object, ...]]]
    raise_for_fp: frozenset[str]
    raise_for_rs: frozenset[str]
    closed: bool

    def __init__(
        self,
        fp_rows: dict[tuple[str, str], list[tuple[object, ...]]] | None = None,
        rs_rows: dict[tuple[str, str], list[tuple[object, ...]]] | None = None,
        raise_for_fp: frozenset[str] = frozenset(),
        raise_for_rs: frozenset[str] = frozenset(),
    ) -> None:
        self.fp_rows = fp_rows or {}
        self.rs_rows = rs_rows or {}
        self.raise_for_fp = raise_for_fp
        self.raise_for_rs = raise_for_rs
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
        if "_se" in query:
            self._pending = self._conn.result_rows.get(date_str, [])
        else:
            self._pending = self._conn.meta_rows.get(date_str, [])

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._pending


class FakeLocalConnection:
    result_rows: dict[str, list[tuple[object, ...]]]
    meta_rows: dict[str, list[tuple[object, ...]]]
    raise_for: frozenset[str]
    closed: bool

    def __init__(
        self,
        result_rows: dict[str, list[tuple[object, ...]]] | None = None,
        meta_rows: dict[str, list[tuple[object, ...]]] | None = None,
        raise_for: frozenset[str] = frozenset(),
    ) -> None:
        self.result_rows = result_rows or {}
        self.meta_rows = meta_rows or {}
        self.raise_for = raise_for
        self.closed = False

    def cursor(self) -> _FakeLocalCursor:
        return _FakeLocalCursor(self)

    def close(self) -> None:
        self.closed = True


# ── Row builders (column order matches serve_eval.py's SQL column tuples) ──


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


def _rs_row(
    keibajo_code: str,
    race_bango: str,
    ketto: str,
    model_version: str,
    predicted_class: int,
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
        predicted_class,
        serve_eval.RS_CLASS_LABELS[predicted_class],
        generated_at,
        kaisai_nen,
        kaisai_tsukihi,
    )


def _result_row(
    keibajo_code: str, race_bango: str, ketto: str, kakutei_chakujun: str, corner_1: str = "01"
) -> tuple[object, ...]:
    return (keibajo_code, race_bango, ketto, kakutei_chakujun, corner_1)


def _meta_row(
    keibajo_code: str,
    race_bango: str,
    kyori: int = 1200,
    shusso_tosu: int = 10,
    track_code: str = "10",
    kyoso_joken_code: str = "A",
) -> tuple[object, ...]:
    return (keibajo_code, race_bango, kyori, shusso_tosu, track_code, kyoso_joken_code)


def _make_legacy_fp_run(
    client: MlflowClient,
    *,
    date_str: str = DATE_STR,
    category: str = "jra",
    model_version: str = "iter14",
) -> str:
    """Build an FP production-usage run tagged `sync_eval_logged=true`
    WITHOUT ever calling `sync_production_range` -- simulates a run that
    predates trace emission in this package (see this module's own
    docstring)."""
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
        },
    )
    return run.info.run_id


def _make_legacy_rs_run(
    client: MlflowClient,
    *,
    date_str: str = DATE_STR,
    category: str = "jra",
    model_version: str = "rs-v3",
) -> str:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_RS_PRODUCTION_USAGE)
    run = client.create_run(
        experiment_id,
        tags={
            "sync_key": f"{date_str}:{category}:{model_version}",
            "task": "running-style",
            "category": category,
            "model_version": model_version,
            "date": date_str,
            SYNC_EVAL_LOGGED_TAG: TRUE_STR,
        },
    )
    return run.info.run_id


# ── No-op / empty-state paths ────────────────────────────────────────────


def test_no_experiments_at_all_returns_empty_summary_and_never_connects(
    client: MlflowClient,
) -> None:
    def _boom() -> FakeNeonConnection:
        raise AssertionError("must not connect when no experiment exists")

    summary = backfill_traces.backfill_traces(client, neon_connect=_boom, local_connect=_boom)
    assert summary.fp_runs_scanned == 0
    assert summary.rs_runs_scanned == 0
    assert summary.errors == []


def test_experiments_exist_but_no_evaluated_runs(client: MlflowClient) -> None:
    get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    get_or_create_experiment(client, config.EXPERIMENT_RS_PRODUCTION_USAGE)
    summary = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert summary.fp_runs_scanned == 0
    assert summary.rs_runs_scanned == 0


# ── FP happy path ────────────────────────────────────────────────────────


def test_backfills_traces_for_legacy_fp_run(client: MlflowClient) -> None:
    _make_legacy_fp_run(client)
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(result_rows={DATE_STR: [_result_row("05", "01", "H1", "1")]})

    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.fp_runs_scanned == 1
    assert summary.fp_traces_created == 1
    assert summary.fp_traces_already_existed == 0
    assert summary.errors == []
    assert neon.closed is True
    assert local.closed is True

    fp_experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert fp_experiment is not None
    tracing_client = trace_emit.build_tracing_client(client)
    traces = tracing_client.search_traces(
        experiment_ids=[fp_experiment.experiment_id], include_spans=False, max_results=10
    )
    assert len(traces) == 1
    assert traces[0].info.tags[trace_emit.TRACE_BUSINESS_KEY_TAG] == f"{DATE_STR}:jra:05:01:iter14"


def test_backfill_traces_is_idempotent_on_repeated_calls(client: MlflowClient) -> None:
    _make_legacy_fp_run(client)
    neon_rows = {("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
    local_rows = {DATE_STR: [_result_row("05", "01", "H1", "1")]}

    first = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(result_rows=local_rows),
    )
    assert first.fp_traces_created == 1
    assert first.fp_traces_already_existed == 0

    second = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(result_rows=local_rows),
    )
    assert second.fp_traces_created == 0
    assert second.fp_traces_already_existed == 1
    assert second.errors == []


# ── RS happy path ────────────────────────────────────────────────────────


def test_backfills_traces_for_legacy_rs_run(client: MlflowClient) -> None:
    _make_legacy_rs_run(client)
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "5", "05")]},
        meta_rows={DATE_STR: [_meta_row("05", "01")]},
    )

    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.rs_runs_scanned == 1
    assert summary.rs_traces_created == 1
    assert summary.rs_traces_already_existed == 0
    assert summary.errors == []

    rs_experiment = client.get_experiment_by_name(config.EXPERIMENT_RS_PRODUCTION_USAGE)
    assert rs_experiment is not None
    tracing_client = trace_emit.build_tracing_client(client)
    traces = tracing_client.search_traces(
        experiment_ids=[rs_experiment.experiment_id], include_spans=False, max_results=10
    )
    assert len(traces) == 1


def test_rs_backfill_idempotent_on_repeated_calls(client: MlflowClient) -> None:
    _make_legacy_rs_run(client)
    neon_rows = {("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    local_result_rows = {DATE_STR: [_result_row("05", "01", "H1", "5", "05")]}
    local_meta_rows = {DATE_STR: [_meta_row("05", "01")]}

    first = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(rs_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(
            result_rows=local_result_rows, meta_rows=local_meta_rows
        ),
    )
    assert first.rs_traces_created == 1

    second = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(rs_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(
            result_rows=local_result_rows, meta_rows=local_meta_rows
        ),
    )
    assert second.rs_traces_created == 0
    assert second.rs_traces_already_existed == 1


# ── Ban-ei partition (FP only, mirrors sync_production.py's own rule) ─────


def test_banei_partition_only_backfills_banei_rows(client: MlflowClient) -> None:
    _make_legacy_fp_run(client, category="banei", model_version="banei-model")
    mixed_rows = [
        _fp_row("30", "01", "H1", "nar-model", 1),
        _fp_row("83", "02", "H2", "banei-model", 1),
    ]
    neon = FakeNeonConnection(fp_rows={("nar", DATE_STR): mixed_rows})
    local = FakeLocalConnection(result_rows={DATE_STR: [_result_row("83", "02", "H2", "1")]})

    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.fp_traces_created == 1
    assert summary.errors == []


# ── date_from / date_to filtering ──────────────────────────────────────────


def test_date_range_filter_excludes_runs_outside_the_window(client: MlflowClient) -> None:
    _make_legacy_fp_run(client, date_str="20260601")
    _make_legacy_fp_run(client, date_str="20260614", model_version="iter20")
    old_generated_at = datetime(2026, 6, 1, tzinfo=UTC)
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", "20260601"): [_fp_row("05", "01", "H1", "iter14", 1, old_generated_at)],
            ("jra", "20260614"): [_fp_row("05", "01", "H1", "iter20", 1)],
        }
    )
    local = FakeLocalConnection(
        result_rows={
            "20260601": [_result_row("05", "01", "H1", "1")],
            "20260614": [_result_row("05", "01", "H1", "1")],
        }
    )

    summary = backfill_traces.backfill_traces(
        client,
        date_from="20260610",
        date_to="20260620",
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.fp_runs_scanned == 2
    assert summary.fp_traces_created == 1


# ── Defensive / error-isolation paths ────────────────────────────────────


def test_missing_date_or_category_tag_is_isolated(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    client.create_run(experiment_id, tags={SYNC_EVAL_LOGGED_TAG: TRUE_STR})
    summary = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert summary.fp_runs_scanned == 1
    assert len(summary.errors) == 1
    assert "missing date/category tag" in summary.errors[0]


def test_missing_model_version_tag_is_isolated(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    client.create_run(
        experiment_id,
        tags={SYNC_EVAL_LOGGED_TAG: TRUE_STR, "date": DATE_STR, "category": "jra"},
    )
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(result_rows={DATE_STR: [_result_row("05", "01", "H1", "1")]})
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert len(summary.errors) == 1
    assert "missing model_version tag" in summary.errors[0]


def test_no_matching_prediction_rows_on_refetch_is_isolated(client: MlflowClient) -> None:
    _make_legacy_fp_run(client, model_version="iter14")
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): []})
    local = FakeLocalConnection()
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.fp_traces_created == 0
    assert len(summary.errors) == 1
    assert "no prediction rows found on re-fetch" in summary.errors[0]


def test_no_eval_rows_from_join_is_silently_skipped_not_an_error(client: MlflowClient) -> None:
    """A run whose re-derived join produces zero eval_rows (e.g. the local
    replica no longer has finalized results for some reason) is simply
    skipped -- not an error, mirrors sync_production._sync_fp_eval's own
    "nothing to log yet" contract."""
    _make_legacy_fp_run(client, model_version="iter14")
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection()  # no result rows at all -> build_fp_race_eval_rows == []
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.fp_traces_created == 0
    assert summary.errors == []


def test_fp_query_failure_is_isolated_per_date_category(client: MlflowClient) -> None:
    _make_legacy_fp_run(client)
    neon = FakeNeonConnection(raise_for_fp=frozenset({DATE_STR}))
    local = FakeLocalConnection()
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.fp_traces_created == 0
    assert len(summary.errors) == 1
    assert "fp-backfill-query" in summary.errors[0]


def test_rs_query_failure_is_isolated_per_date_category(client: MlflowClient) -> None:
    _make_legacy_rs_run(client)
    neon = FakeNeonConnection(raise_for_rs=frozenset({DATE_STR}))
    local = FakeLocalConnection()
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.rs_traces_created == 0
    assert len(summary.errors) == 1
    assert "rs-backfill-query" in summary.errors[0]


def test_eval_join_exception_is_isolated_per_run(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _make_legacy_fp_run(client)
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(result_rows={DATE_STR: [_result_row("05", "01", "H1", "1")]})

    def _boom(*args: object, **kwargs: object) -> None:
        raise MlflowException("boom eval join")

    monkeypatch.setattr(serve_eval, "build_fp_race_eval_rows", _boom)
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert len(summary.errors) == 1
    assert "eval join failed" in summary.errors[0]


def test_rs_missing_model_version_tag_is_isolated(client: MlflowClient) -> None:
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_RS_PRODUCTION_USAGE)
    client.create_run(
        experiment_id,
        tags={SYNC_EVAL_LOGGED_TAG: TRUE_STR, "date": DATE_STR, "category": "jra"},
    )
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "5", "05")]},
        meta_rows={DATE_STR: [_meta_row("05", "01")]},
    )
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert len(summary.errors) == 1
    assert "missing model_version tag" in summary.errors[0]


def test_rs_no_matching_prediction_rows_on_refetch_is_isolated(client: MlflowClient) -> None:
    _make_legacy_rs_run(client, model_version="rs-v3")
    neon = FakeNeonConnection(rs_rows={("jra", DATE_STR): []})
    local = FakeLocalConnection()
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.rs_traces_created == 0
    assert len(summary.errors) == 1
    assert "no prediction rows found on re-fetch" in summary.errors[0]


def test_rs_eval_join_exception_is_isolated_per_run(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _make_legacy_rs_run(client)
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "5", "05")]},
        meta_rows={DATE_STR: [_meta_row("05", "01")]},
    )

    def _boom(*args: object, **kwargs: object) -> None:
        raise MlflowException("boom rs eval join")

    monkeypatch.setattr(serve_eval, "build_rs_horse_eval_rows", _boom)
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert len(summary.errors) == 1
    assert "eval join failed" in summary.errors[0]


def test_rs_no_eval_rows_from_join_is_silently_skipped_not_an_error(client: MlflowClient) -> None:
    _make_legacy_rs_run(client)
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection()  # no result/meta rows -> build_rs_horse_eval_rows == []
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.rs_traces_created == 0
    assert summary.errors == []


# ── Pagination ────────────────────────────────────────────────────────────


def test_pagination_across_multiple_search_runs_pages(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_find_evaluated_runs` must follow `page_token` until exhausted --
    simulated via 2 real runs split across 2 fake pages (mirrors
    test_refresh_eval_metrics.py's own precedent for the identical idiom)."""
    _make_legacy_fp_run(client, model_version="iter14")
    _make_legacy_fp_run(client, model_version="iter20")
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
    summary = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(raise_for_fp=frozenset({DATE_STR})),
        local_connect=lambda: FakeLocalConnection(),
    )
    monkeypatch.setattr(client, "search_runs", original_search_runs)

    assert call_tokens == [None, "page-2-token"]
    assert summary.fp_runs_scanned == 2


def test_trace_emission_errors_are_folded_into_summary(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _make_legacy_fp_run(client)
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(result_rows={DATE_STR: [_result_row("05", "01", "H1", "1")]})

    def _boom_emit(*args: object, **kwargs: object) -> trace_emit.TraceEmitSummary:
        return trace_emit.TraceEmitSummary(errors=["synthetic trace-emit failure"])

    monkeypatch.setattr(trace_emit, "emit_fp_race_traces", _boom_emit)
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.fp_traces_created == 0
    assert len(summary.errors) == 1
    assert "synthetic trace-emit failure" in summary.errors[0]


# ── Assessment-completeness healing (2026-07-11) ────────────────────────────
#
# Reproduces the real production incident this feature exists to fix: a
# trace whose spans were written successfully but whose assessments were
# left partial/empty by a transient store error between `log_spans` and one
# of the `log_assessment` calls that follow it. `tracing_client.
# delete_assessment` simulates that exact after-the-fact state.


def test_backfill_heals_a_previously_incomplete_fp_trace(client: MlflowClient) -> None:
    """Mirrors the real FP incident: top3_box_score (UNCONDITIONAL) missing
    from an otherwise-complete trace. A first backfill call creates the
    trace complete; simulate the defect by deleting one assessment
    afterwards, then confirm the NEXT backfill call (the normal re-run any
    cron/backfill invocation already performs) tops it back up without
    duplicating the still-present assessments or creating a new trace."""
    _make_legacy_fp_run(client)
    neon_rows = {("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
    # A full (>= 6 starter) field so place4_hit/place5_hit/place6_hit are all
    # populated (not small-field-excluded) -- this integration test wants the
    # FULL 7-assessment expected set; the small-field exclusion itself is
    # covered directly in test_trace_emit.py.
    local_rows = {
        DATE_STR: [
            _result_row("05", "01", "H1", "1"),
            _result_row("05", "01", "H2", "2"),
            _result_row("05", "01", "H3", "3"),
            _result_row("05", "01", "H4", "4"),
            _result_row("05", "01", "H5", "5"),
            _result_row("05", "01", "H6", "6"),
        ]
    }

    first = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(result_rows=local_rows),
    )
    assert first.fp_traces_created == 1
    assert first.fp_traces_healed == 0
    assert first.fp_assessments_healed == 0

    fp_experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert fp_experiment is not None
    tracing_client = trace_emit.build_tracing_client(client)
    traces = tracing_client.search_traces(
        experiment_ids=[fp_experiment.experiment_id], include_spans=False, max_results=10
    )
    assert len(traces) == 1
    trace_id = traces[0].info.trace_id
    top3_assessment = next(
        a
        for a in traces[0].info.assessments
        if a.name == trace_emit.TOP3_BOX_ASSESSMENT_NAME
    )
    assert top3_assessment.assessment_id is not None
    tracing_client.delete_assessment(trace_id, top3_assessment.assessment_id)
    broken = tracing_client.get_trace(trace_id)
    assert trace_emit.TOP3_BOX_ASSESSMENT_NAME not in {a.name for a in broken.info.assessments}

    second = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(result_rows=local_rows),
    )
    assert second.fp_traces_created == 0
    assert second.fp_traces_already_existed == 1
    assert second.fp_traces_healed == 1
    assert second.fp_assessments_healed == 1
    assert second.errors == []

    healed_traces = tracing_client.search_traces(
        experiment_ids=[fp_experiment.experiment_id], include_spans=False, max_results=10
    )
    assert len(healed_traces) == 1  # still exactly one trace -- never duplicated
    healed_names = [a.name for a in healed_traces[0].info.assessments]
    assert healed_names.count(trace_emit.TOP3_BOX_ASSESSMENT_NAME) == 1
    assert healed_names.count("place1_hit") == 1  # untouched assessment, not re-logged
    assert set(healed_names) == {
        "place1_hit",
        "place2_hit",
        "place3_hit",
        "place4_hit",
        "place5_hit",
        "place6_hit",
        trace_emit.TOP3_BOX_ASSESSMENT_NAME,
    }

    # A third call, once already complete, must be a true no-op for healing.
    third = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(result_rows=local_rows),
    )
    assert third.fp_traces_healed == 0
    assert third.fp_assessments_healed == 0


def test_backfill_heals_a_previously_incomplete_rs_trace(client: MlflowClient) -> None:
    """Mirrors the real RS incident directly: a trace with ZERO assessments
    logged at all."""
    _make_legacy_rs_run(client)
    neon_rows = {("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    local_result_rows = {DATE_STR: [_result_row("05", "01", "H1", "5", "05")]}
    local_meta_rows = {DATE_STR: [_meta_row("05", "01")]}

    first = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(rs_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(
            result_rows=local_result_rows, meta_rows=local_meta_rows
        ),
    )
    assert first.rs_traces_created == 1
    assert first.rs_traces_healed == 0

    rs_experiment = client.get_experiment_by_name(config.EXPERIMENT_RS_PRODUCTION_USAGE)
    assert rs_experiment is not None
    tracing_client = trace_emit.build_tracing_client(client)
    traces = tracing_client.search_traces(
        experiment_ids=[rs_experiment.experiment_id], include_spans=False, max_results=10
    )
    assert len(traces) == 1
    trace_id = traces[0].info.trace_id
    for assessment in list(traces[0].info.assessments):
        assert assessment.assessment_id is not None
        tracing_client.delete_assessment(trace_id, assessment.assessment_id)
    assert tracing_client.get_trace(trace_id).info.assessments == []

    second = backfill_traces.backfill_traces(
        client,
        neon_connect=lambda: FakeNeonConnection(rs_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(
            result_rows=local_result_rows, meta_rows=local_meta_rows
        ),
    )
    assert second.rs_traces_created == 0
    assert second.rs_traces_already_existed == 1
    assert second.rs_traces_healed == 1
    assert second.rs_assessments_healed == 1
    assert second.errors == []

    healed_names = {
        a.name for a in tracing_client.get_trace(trace_id).info.assessments
    }
    assert healed_names == {trace_emit.RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME}


def test_backfill_fp_heal_errors_are_folded_into_summary(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _make_legacy_fp_run(client)
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(result_rows={DATE_STR: [_result_row("05", "01", "H1", "1")]})

    def _boom_heal(*args: object, **kwargs: object) -> trace_emit.TraceHealSummary:
        return trace_emit.TraceHealSummary(errors=["synthetic heal failure"])

    monkeypatch.setattr(trace_emit, "heal_fp_race_traces", _boom_heal)
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.fp_traces_created == 1
    assert summary.fp_traces_healed == 0
    assert len(summary.errors) == 1
    assert "heal: synthetic heal failure" in summary.errors[0]


def test_backfill_rs_heal_errors_are_folded_into_summary(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _make_legacy_rs_run(client)
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "5", "05")]},
        meta_rows={DATE_STR: [_meta_row("05", "01")]},
    )

    def _boom_heal(*args: object, **kwargs: object) -> trace_emit.TraceHealSummary:
        return trace_emit.TraceHealSummary(errors=["synthetic rs heal failure"])

    monkeypatch.setattr(trace_emit, "heal_rs_horse_traces", _boom_heal)
    summary = backfill_traces.backfill_traces(
        client, neon_connect=lambda: neon, local_connect=lambda: local
    )
    assert summary.rs_traces_created == 1
    assert summary.rs_traces_healed == 0
    assert len(summary.errors) == 1
    assert "heal: synthetic rs heal failure" in summary.errors[0]
