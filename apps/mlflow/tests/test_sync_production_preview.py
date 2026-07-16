from __future__ import annotations

from datetime import UTC, datetime

import psycopg2
import pytest
from mlflow import MlflowClient
from mlflow.entities import Run

from mlflow_tracking import config, serve_eval, sync_production, sync_production_preview

DATE_STR = "20260717"
GEN_AT = datetime(2026, 7, 16, 11, 0, tzinfo=UTC)


def _fp_row(
    keibajo_code: str,
    race_bango: str,
    ketto: str,
    model_version: str,
) -> tuple[object, ...]:
    return (
        keibajo_code,
        race_bango,
        ketto,
        model_version,
        1,
        GEN_AT,
        "sprint",
        "medium",
        "summer",
        "A",
        "turf",
        "2026",
        "0717",
    )


def _rs_row(
    keibajo_code: str,
    race_bango: str,
    ketto: str,
    model_version: str,
) -> tuple[object, ...]:
    return (
        keibajo_code,
        race_bango,
        ketto,
        model_version,
        serve_eval.RS_CLASS_SASHI,
        serve_eval.RS_CLASS_LABELS[serve_eval.RS_CLASS_SASHI],
        GEN_AT,
        "2026",
        "0717",
    )


class _Cursor:
    def __init__(self, connection: _Connection) -> None:
        self.connection: _Connection = connection
        self.rows: list[tuple[object, ...]] = []

    def execute(self, query: str, params: object = None) -> None:
        assert isinstance(params, tuple)
        source, date_from, date_to = params
        assert date_from == date_to
        key = (str(source), str(date_from))
        if "race_finish_position_model_predictions" in query:
            if str(date_from) in self.connection.fail_fp:
                raise psycopg2.OperationalError("fp failed")
            self.rows = self.connection.fp_rows.get(key, [])
            return
        if str(date_from) in self.connection.fail_rs:
            raise psycopg2.OperationalError("rs failed")
        self.rows = self.connection.rs_rows.get(key, [])

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


class _Connection:
    def __init__(
        self,
        *,
        fp_rows: dict[tuple[str, str], list[tuple[object, ...]]] | None = None,
        rs_rows: dict[tuple[str, str], list[tuple[object, ...]]] | None = None,
        fail_fp: frozenset[str] = frozenset(),
        fail_rs: frozenset[str] = frozenset(),
    ) -> None:
        self.fp_rows: dict[tuple[str, str], list[tuple[object, ...]]] = fp_rows or {}
        self.rs_rows: dict[tuple[str, str], list[tuple[object, ...]]] = rs_rows or {}
        self.fail_fp: frozenset[str] = fail_fp
        self.fail_rs: frozenset[str] = fail_rs
        self.closed: bool = False

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def close(self) -> None:
        self.closed = True


def _run(client: MlflowClient, experiment_name: str, sync_key: str) -> Run:
    experiment = client.get_experiment_by_name(experiment_name)
    assert experiment is not None
    runs = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.{sync_production.SYNC_KEY_TAG} = '{sync_key}'",
    )
    assert len(runs) == 1
    return runs[0]


def test_preview_creates_and_updates_fp_and_rs_runs(client: MlflowClient) -> None:
    def connection() -> _Connection:
        return _Connection(
            fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "fp-v1")]},
            rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v1")]},
        )

    first = sync_production_preview.sync_production_preview_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=connection,
    )
    second = sync_production_preview.sync_production_preview_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=connection,
    )

    assert (first.fp_runs_created, first.rs_runs_created) == (1, 1)
    assert (second.fp_runs_updated, second.rs_runs_updated) == (1, 1)
    fp_run = _run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:fp-v1")
    rs_run = _run(client, config.EXPERIMENT_RS_PRODUCTION_USAGE, f"{DATE_STR}:jra:rs-v1")
    assert fp_run.data.metrics["fp_races"] == 1.0
    assert rs_run.data.metrics["rs_horses"] == 1.0
    assert sync_production_preview.PREVIEW_LOGGED_AT_TAG in fp_run.data.tags
    assert sync_production.SYNC_BASE_LOGGED_TAG not in fp_run.data.tags
    assert fp_run.info.status == "FINISHED"


def test_preview_filters_nar_and_banei_and_skips_rs_for_banei(client: MlflowClient) -> None:
    connection = _Connection(
        fp_rows={
            ("nar", DATE_STR): [
                _fp_row("50", "01", "H1", "nar-v1"),
                _fp_row("83", "01", "H2", "banei-v1"),
            ]
        }
    )
    summary = sync_production_preview.sync_production_preview_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("banei",),
        neon_connect=lambda: connection,
    )

    assert summary.fp_runs_created == 1
    assert summary.rs_runs_created == 0
    assert connection.closed is True
    assert client.get_experiment_by_name(config.EXPERIMENT_RS_PRODUCTION_USAGE) is None


def test_preview_isolates_fp_failure_and_continues_rs(client: MlflowClient) -> None:
    connection = _Connection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v1")]},
        fail_fp=frozenset({DATE_STR}),
    )
    summary = sync_production_preview.sync_production_preview_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: connection,
    )

    assert summary.rs_runs_created == 1
    assert len(summary.errors) == 1
    assert "finish-position" in summary.errors[0]
    assert connection.closed is True


def test_preview_isolates_rs_failure_and_continues_fp(client: MlflowClient) -> None:
    connection = _Connection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "fp-v1")]},
        fail_rs=frozenset({DATE_STR}),
    )
    summary = sync_production_preview.sync_production_preview_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: connection,
    )

    assert summary.fp_runs_created == 1
    assert len(summary.errors) == 1
    assert "running-style" in summary.errors[0]
    assert connection.closed is True


def test_preview_reuses_experiment_ids_across_categories(client: MlflowClient) -> None:
    """Two RS-eligible categories in one call: the second category's iteration
    must find both fp_experiment_id and rs_experiment_id already resolved from
    the first, rather than re-resolving (`get_or_create_experiment`) them --
    covers the `is None` guards' False branch, only reachable once at least
    one experiment id has already been cached earlier in the same call."""
    connection = _Connection(
        fp_rows={
            ("jra", DATE_STR): [_fp_row("05", "01", "H1", "fp-v1")],
            ("nar", DATE_STR): [_fp_row("50", "01", "H2", "fp-v1")],
        },
        rs_rows={
            ("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v1")],
            ("nar", DATE_STR): [_rs_row("50", "01", "H2", "rs-v1")],
        },
    )
    summary = sync_production_preview.sync_production_preview_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra", "nar"),
        neon_connect=lambda: connection,
    )

    assert summary.fp_runs_created == 2
    assert summary.rs_runs_created == 2
    assert summary.errors == []


def test_preview_rejects_inverted_range(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="must not be before"):
        sync_production_preview.sync_production_preview_range(
            client,
            "20260718",
            "20260717",
        )
