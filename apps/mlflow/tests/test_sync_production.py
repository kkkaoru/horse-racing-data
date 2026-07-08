"""Tests for mlflow_tracking.sync_production.

Entirely hermetic: `FakeNeonConnection`/`FakeLocalConnection` below are hand-
built fakes (never a real psycopg2 connection) injected via `neon_connect`/
`local_connect`, so these tests never touch Neon or the local PostgreSQL
replica. The `client` fixture (from conftest.py) is a REAL `MlflowClient`
backed by an isolated sqlite tracking store -- exercising real MLflow
run/experiment/registry/timeline code against a throwaway store is correct
and intended here.
"""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import psycopg2
import pytest
from mlflow import MlflowClient
from mlflow.entities import Run
from mlflow.exceptions import MlflowException

from mlflow_tracking import config, db, registry, serve_eval, sync_production, timeline

GEN_AT: datetime = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
DATE_STR: str = "20260614"


# ── Fake connections (hermetic, dispatch by SQL substring + date param) ─────


class _FakeNeonCursor:
    _conn: FakeNeonConnection

    def __init__(self, conn: FakeNeonConnection) -> None:
        self._conn = conn
        self._pending: list[tuple[object, ...]] = []

    def execute(self, query: str, params: object = None) -> None:
        assert isinstance(params, tuple)
        source, date_from, date_to = params
        assert date_from == date_to, "sync_production always issues single-day range queries"
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
    """Fake racing-Neon connection serving FP/RS prediction rows keyed by
    (source, date_str). Dispatch is by SQL substring, mirroring
    test_serve_eval.py's own `_make_mock_conn` substring-check convention."""

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
        year, month_day = params
        date_str = f"{year}{month_day}"
        if date_str in self._conn.raise_for:
            raise psycopg2.OperationalError(f"boom local {date_str}")
        if "_se" in query:
            self._pending = self._conn.result_rows.get(date_str, [])
        else:
            self._pending = self._conn.meta_rows.get(date_str, [])

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._pending


class FakeLocalConnection:
    """Fake local-PostgreSQL-replica connection serving finalized results/
    metadata rows keyed by date_str (jvd_se/nvd_se share the "_se" substring,
    jvd_ra/nvd_ra share "_ra", so category need not be tracked separately)."""

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
    distance_band: str = "sprint",
    field_size_band: str = "medium",
    season_band: str = "summer",
    class_code: str = "A",
    surface: str = "turf",
) -> tuple[object, ...]:
    return (
        keibajo_code,
        race_bango,
        ketto,
        model_version,
        predicted_rank,
        generated_at,
        distance_band,
        field_size_band,
        season_band,
        class_code,
        surface,
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
    keibajo_code: str, race_bango: str, ketto: str, kakutei_chakujun: str, corner_1: str
) -> tuple[object, ...]:
    return (keibajo_code, race_bango, ketto, kakutei_chakujun, corner_1)


def _meta_row(
    keibajo_code: str,
    race_bango: str,
    kyori: int,
    shusso_tosu: int,
    track_code: str,
    kyoso_joken_code: str,
) -> tuple[object, ...]:
    return (keibajo_code, race_bango, kyori, shusso_tosu, track_code, kyoso_joken_code)


def _get_run(client: MlflowClient, experiment_name: str, sync_key: str) -> Run:
    experiment = client.get_experiment_by_name(experiment_name)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.sync_key = '{sync_key}'"
    )
    assert len(matches) == 1
    return matches[0]


def _register_champion(
    client: MlflowClient, category: str, task: registry.Task, model_version_label: str
) -> None:
    name = registry.registered_model_name(registry.normalize_category(category), task)
    version = registry.register_version(
        client, name, "s3://bucket/dummy", tags={"model_version": model_version_label}
    )
    registry.set_champion(client, name, version.version)


# ── Basic FP happy path + champion tag ──────────────────────────────────────


def test_genuine_fp_row_creates_base_run_no_results_yet(client: MlflowClient) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.dates_processed == 1
    assert summary.fp_runs_created == 1
    assert summary.fp_runs_reused == 0
    assert summary.fp_eval_logged == 0
    assert summary.fp_eval_skipped_no_results == 1
    assert summary.errors == []
    assert neon.closed is True
    assert local.closed is True

    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.tags["sync_base_logged"] == "true"
    assert "sync_eval_logged" not in run.data.tags
    assert run.data.tags["model_version"] == "iter14"
    assert run.data.tags["date"] == DATE_STR
    assert run.data.tags["category"] == "jra"
    assert run.data.metrics["fp_races"] == 1.0
    assert run.data.metrics["fp_horses"] == 1.0
    artifact_paths = {a.path for a in client.list_artifacts(run.info.run_id)}
    assert "predictions.json" in artifact_paths
    assert "predictions.parquet" in artifact_paths


def test_champion_at_sync_true_when_matching(client: MlflowClient) -> None:
    _register_champion(client, "jra", "finish-position", "iter14")
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection()
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.tags["champion_at_sync"] == "true"


def test_champion_at_sync_false_when_mismatched(client: MlflowClient) -> None:
    _register_champion(client, "jra", "finish-position", "some-other-version")
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection()
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.tags["champion_at_sync"] == "false"


def test_champion_at_sync_false_when_registered_model_has_no_champion_alias(
    client: MlflowClient,
) -> None:
    """The registered model exists (so `get_registered_model` succeeds) but
    no champion alias was ever set -- `aliases.get(...)` cleanly returns
    None without raising, a distinct code path from the
    `except MlflowException` branch below."""
    registry.register_version(
        client, "jra-finish-position", "s3://bucket/dummy", tags={"model_version": "iter14"}
    )
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection()
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.tags["champion_at_sync"] == "false"


def test_champion_at_sync_false_when_no_registered_model_exists(client: MlflowClient) -> None:
    """No registered model at all for jra-finish-position -- hits the
    `except MlflowException` branch of `_resolve_champion_label`."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection()
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.tags["champion_at_sync"] == "false"


# ── Run status: every touched run ends FINISHED, never RUNNING ─────────────
#
# Regression coverage for a real production incident: this module never
# called `client.set_terminated` at all, so every run it ever created was
# observed stuck in RUNNING status for days. Mirrors
# test_timeline.py::test_upsert_timeline_point_run_finished_after_each_call's
# exact assertion style, applied to this module's own FP/RS sync runs.


def test_fp_run_is_finished_after_first_sync_call(client: MlflowClient) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection()
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.info.status == "FINISHED"


def test_fp_reused_run_is_still_finished_after_second_sync_call(client: MlflowClient) -> None:
    """A run reused on a LATER call (not just-created) must also end that
    call FINISHED -- proving the fix re-terminates on every visit, not only
    at creation time."""

    def _fresh_neon() -> FakeNeonConnection:
        return FakeNeonConnection(
            fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
        )

    def _fresh_local() -> FakeLocalConnection:
        return FakeLocalConnection(
            result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]}
        )

    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=_fresh_neon,
        local_connect=_fresh_local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    run_id = run.info.run_id

    # Force the run back to RUNNING so the second call's FINISHED status can
    # only be explained by that call re-terminating it, not by residual
    # status left over from the first call.
    client.update_run(run_id, status="RUNNING")
    assert client.get_run(run_id).info.status == "RUNNING"

    second = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=_fresh_neon,
        local_connect=_fresh_local,
    )
    assert second.fp_runs_reused == 1
    assert client.get_run(run_id).info.status == "FINISHED"


# ── eval_regime tag: every logged run represents genuinely-served usage ────


def test_fp_run_created_with_eval_regime_serve_tag(client: MlflowClient) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection()
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.tags["eval_regime"] == "serve"


# ── Idempotency: second call reuses run, never re-logs base ────────────────


def test_second_call_reuses_run_and_never_relogs_base_or_eval(client: MlflowClient) -> None:
    """Same range synced twice. Verification follows this module's own
    documented alternative to counting `log_table` calls: metric-history
    length and the artifact set must be byte-for-byte identical after the
    second call, proving neither the base tracking nor the eval join (both
    of which call `client.log_table`) were re-logged."""

    def _fresh_neon() -> FakeNeonConnection:
        return FakeNeonConnection(
            fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
        )

    def _fresh_local() -> FakeLocalConnection:
        return FakeLocalConnection(
            result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]}
        )

    first = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=_fresh_neon,
        local_connect=_fresh_local,
    )
    assert first.fp_runs_created == 1
    assert first.fp_eval_logged == 1

    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    run_id = run.info.run_id
    races_history_len = len(client.get_metric_history(run_id, "fp_races"))
    top1_history_len = len(client.get_metric_history(run_id, "fp_top1_pct"))
    artifacts_after_first = {a.path for a in client.list_artifacts(run_id)}

    second = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=_fresh_neon,
        local_connect=_fresh_local,
    )
    assert second.fp_runs_created == 0
    assert second.fp_runs_reused == 1
    assert second.fp_eval_logged == 0
    assert second.fp_eval_skipped_no_results == 0

    assert len(client.get_metric_history(run_id, "fp_races")) == races_history_len
    assert len(client.get_metric_history(run_id, "fp_top1_pct")) == top1_history_len
    assert {a.path for a in client.list_artifacts(run_id)} == artifacts_after_first


# ── Eval fills in later once results become final ──────────────────────────


def test_eval_fills_in_later_once_results_become_final(client: MlflowClient) -> None:
    neon_rows = {("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}

    first = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert first.fp_eval_skipped_no_results == 1
    assert first.fp_eval_logged == 0
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert "sync_eval_logged" not in run.data.tags

    second = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(
            result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]}
        ),
    )
    assert second.fp_runs_created == 0
    assert second.fp_runs_reused == 1
    assert second.fp_eval_logged == 1
    assert second.fp_eval_skipped_no_results == 0

    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.tags["sync_eval_logged"] == "true"
    assert "timeline_run_id:finish-position" in run.data.tags
    fp_dates = timeline.timeline_dates_present(client, "finish-position", "jra", "fp_top1_pct")
    assert int(DATE_STR) in fp_dates
    assert run.data.metrics["fp_top1_pct"] == 100.0
    assert run.data.metrics["fp_races_evaluated"] == 1.0


# ── Ban-ei partitioning (FP) ─────────────────────────────────────────────


def test_banei_partitioning_excludes_and_includes_correctly(client: MlflowClient) -> None:
    mixed_rows = [
        _fp_row("30", "01", "H1", "nar-model", 1),
        _fp_row("83", "02", "H2", "banei-model", 1),
    ]
    neon_rows = {("nar", DATE_STR): mixed_rows}

    nar_summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("nar",),
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert nar_summary.fp_runs_created == 1
    nar_run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:nar:nar-model")
    assert nar_run.data.tags["model_version"] == "nar-model"
    assert nar_run.data.metrics["fp_horses"] == 1.0

    banei_summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("banei",),
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert banei_summary.fp_runs_created == 1
    banei_run = _get_run(
        client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:banei:banei-model"
    )
    assert banei_run.data.tags["model_version"] == "banei-model"
    assert banei_run.data.metrics["fp_horses"] == 1.0

    # Neither run should exist under the other category's sync_key.
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    no_nar_as_banei = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.sync_key = '{DATE_STR}:banei:nar-model'",
    )
    assert no_nar_as_banei == []
    no_banei_as_nar = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.sync_key = '{DATE_STR}:nar:banei-model'",
    )
    assert no_banei_as_nar == []


# ── RS path: base + eval + timeline ─────────────────────────────────────


def test_rs_base_and_eval_and_timeline(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "5", "05")]},
        meta_rows={DATE_STR: [_meta_row("05", "01", 1200, 10, "10", "A")]},
    )
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.rs_runs_created == 1
    assert summary.rs_eval_logged == 1
    assert summary.rs_eval_skipped_no_results == 0

    run = _get_run(client, config.EXPERIMENT_RS_PRODUCTION_USAGE, f"{DATE_STR}:jra:rs-v3")
    assert run.data.tags["sync_base_logged"] == "true"
    assert run.data.tags["sync_eval_logged"] == "true"
    assert "timeline_run_id:running-style" in run.data.tags
    assert run.data.metrics["rs_races"] == 1.0
    assert run.data.metrics["rs_horses"] == 1.0
    # corner_1="05" over shusso_tosu=10 -> (5-1)/(10-1) == 0.444 -> sashi ==
    # predicted -> a full hit.
    assert run.data.metrics["rs_overall_accuracy_pct"] == 100.0
    assert run.data.metrics["rs_macro_f1_pct"] == pytest.approx(100.0)
    artifact_paths = {a.path for a in client.list_artifacts(run.info.run_id)}
    assert "predictions.json" in artifact_paths
    assert "eval.json" in artifact_paths

    rs_dates = timeline.timeline_dates_present(
        client, "running-style", "jra", "rs_overall_accuracy_pct"
    )
    assert int(DATE_STR) in rs_dates


def test_rs_eval_skipped_when_no_finalized_results_yet(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.rs_eval_logged == 0
    assert summary.rs_eval_skipped_no_results == 1
    run = _get_run(client, config.EXPERIMENT_RS_PRODUCTION_USAGE, f"{DATE_STR}:jra:rs-v3")
    assert "sync_eval_logged" not in run.data.tags


def test_rs_macro_f1_none_is_not_logged_as_a_metric(client: MlflowClient) -> None:
    """A single-class day (only nige ever predicted/actual) makes
    `macro_f1_pct` None (see serve_eval.aggregate_rs_day_metrics) -- must not
    attempt to log a None metric value."""
    neon = FakeNeonConnection(
        rs_rows={
            ("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SENKOU)]
        }
    )
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]},
        meta_rows={DATE_STR: [_meta_row("05", "01", 1200, 10, "10", "A")]},
    )
    # corner_1="01" -> corner1_norm == 0.0 -> actual_class == NIGE, predicted
    # senkou: NIGE has precision=None (never predicted) but recall=0.0;
    # SENKOU has precision=0.0 but recall=None -- both None-combinations
    # collapse macro_f1_pct to None (see test_serve_eval.py's own coverage
    # of this exact combination in aggregate_rs_day_metrics).
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_RS_PRODUCTION_USAGE, f"{DATE_STR}:jra:rs-v3")
    assert "rs_macro_f1_pct" not in run.data.metrics
    assert run.data.metrics["rs_overall_accuracy_pct"] == 0.0


def test_rs_second_call_reuses_run_and_never_relogs(client: MlflowClient) -> None:
    def _fresh_neon() -> FakeNeonConnection:
        return FakeNeonConnection(
            rs_rows={
                ("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]
            }
        )

    def _fresh_local() -> FakeLocalConnection:
        return FakeLocalConnection(
            result_rows={DATE_STR: [_result_row("05", "01", "H1", "5", "05")]},
            meta_rows={DATE_STR: [_meta_row("05", "01", 1200, 10, "10", "A")]},
        )

    first = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=_fresh_neon,
        local_connect=_fresh_local,
    )
    assert first.rs_runs_created == 1
    assert first.rs_eval_logged == 1

    second = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=_fresh_neon,
        local_connect=_fresh_local,
    )
    assert second.rs_runs_created == 0
    assert second.rs_runs_reused == 1
    assert second.rs_eval_logged == 0
    assert second.rs_eval_skipped_no_results == 0


def test_rs_never_attempted_for_banei(client: MlflowClient) -> None:
    """category="banei" is FP-eligible but not RS-eligible: requesting only
    banei must never create the RS production-usage experiment at all."""
    neon = FakeNeonConnection(fp_rows={("nar", DATE_STR): [_fp_row("83", "01", "H1", "v1", 1)]})
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("banei",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.rs_runs_created == 0
    assert summary.rs_runs_reused == 0
    assert summary.rs_eval_logged == 0
    assert summary.rs_eval_skipped_no_results == 0
    assert client.get_experiment_by_name(config.EXPERIMENT_RS_PRODUCTION_USAGE) is None


# ── Serving-gap detection: RS served, FP not, for the same (date, category) ─
#
# Regression coverage for a real incident: on 2026-07-04, JRA had 12 races
# with running-style predictions logged and ZERO finish-position rows the
# same day -- a genuine one-sided outage that otherwise goes unnoticed, since
# either side alone being empty is normal (production serving is sparse).


def test_serving_gap_detected_and_marker_run_logged(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection()

    with pytest.warns(UserWarning, match="jra"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )

    assert summary.serving_gaps_detected == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'",
    )
    assert len(matches) == 1
    gap_run = matches[0]
    assert gap_run.data.tags["serving_gap"] == "true"
    assert gap_run.data.tags["gap_date"] == DATE_STR
    assert gap_run.data.tags["category"] == "jra"
    assert gap_run.data.metrics["fp_races_observed"] == 0.0
    assert gap_run.data.metrics["rs_races_observed"] == 1.0
    assert gap_run.info.status == "FINISHED"


def test_no_serving_gap_when_both_fp_and_rs_empty(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    neon = FakeNeonConnection()
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.serving_gaps_detected == 0
    assert not any("serving gap" in str(w.message) for w in recwarn.list)
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id], filter_string="tags.serving_gap = 'true'"
    )
    assert matches == []


def test_no_serving_gap_when_both_fp_and_rs_nonempty(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    rs_row = _rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]},
        rs_rows={("jra", DATE_STR): [rs_row]},
    )
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.serving_gaps_detected == 0
    assert not any("serving gap" in str(w.message) for w in recwarn.list)
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id], filter_string="tags.serving_gap = 'true'"
    )
    assert matches == []


def test_serving_gap_marker_not_duplicated_on_repeated_calls(client: MlflowClient) -> None:
    """A persistent gap (the realistic multi-day-outage shape) synced twice
    over the same overlapping range must reuse the same marker run, not
    create a second one -- mirroring this module's other idempotency tests."""

    def _fresh_neon() -> FakeNeonConnection:
        return FakeNeonConnection(
            rs_rows={
                ("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]
            }
        )

    with pytest.warns(UserWarning, match="jra"):
        first = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=_fresh_neon,
            local_connect=lambda: FakeLocalConnection(),
        )
    assert first.serving_gaps_detected == 1

    with pytest.warns(UserWarning, match="jra"):
        second = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=_fresh_neon,
            local_connect=lambda: FakeLocalConnection(),
        )
    assert second.serving_gaps_detected == 1

    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'",
    )
    assert len(matches) == 1


def test_serving_gap_logging_failure_is_isolated_and_recorded(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient failure while logging the gap marker itself (e.g. a
    tracking-store write hiccup) must be isolated exactly like an fp/rs sync
    failure -- recorded in `summary.errors`, never raised, never counted in
    `serving_gaps_detected`, and must not prevent the fp/rs syncs (which
    already completed by the time this check runs) from being reported."""

    def _boom(*args: object, **kwargs: object) -> None:
        raise MlflowException("boom serving-gap log")

    monkeypatch.setattr(sync_production, "_log_serving_gap", _boom)

    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection()
    with pytest.warns(UserWarning, match="jra"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )
    assert summary.serving_gaps_detected == 0
    assert summary.rs_runs_created == 1
    assert len(summary.errors) == 1
    assert "serving-gap" in summary.errors[0]
    assert "boom serving-gap log" in summary.errors[0]


def test_serving_gap_never_checked_for_banei(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    """category="banei" is FP-eligible but not RS-eligible, so no RS query is
    ever issued for it -- the gap check must never fire even with FP-empty
    data, since there is nothing to compare against."""
    neon = FakeNeonConnection()
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("banei",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.serving_gaps_detected == 0
    assert not any("serving gap" in str(w.message) for w in recwarn.list)
    assert client.get_experiment_by_name(config.EXPERIMENT_RS_PRODUCTION_USAGE) is None


# ── Error isolation ──────────────────────────────────────────────────────


def test_error_on_one_date_does_not_abort_the_rest_of_the_range(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", "20260614"): [_fp_row("05", "01", "H1", "iter14", 1)],
            ("jra", "20260615"): [
                _fp_row(
                    "05",
                    "01",
                    "H2",
                    "iter14",
                    1,
                    datetime(2026, 6, 15, 3, 0, 0, tzinfo=UTC),
                    kaisai_tsukihi="0615",
                )
            ],
        },
        raise_for_fp=frozenset({"20260614"}),
    )
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        "20260614",
        "20260615",
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.dates_processed == 2
    assert summary.fp_runs_created == 1  # only 20260615 succeeded
    assert len(summary.errors) == 1
    assert "20260614" in summary.errors[0]
    assert "finish-position" in summary.errors[0]
    assert "boom fp 20260614" in summary.errors[0]


def test_rs_error_isolated_independently_from_fp(client: MlflowClient) -> None:
    """An RS-side failure must not roll back or skip the FP sync for the
    same (date, category), and must be tagged as a running-style error."""
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]},
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", 0)]},
        raise_for_rs=frozenset({DATE_STR}),
    )
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.fp_runs_created == 1
    assert summary.rs_runs_created == 0
    assert len(summary.errors) == 1
    assert "running-style" in summary.errors[0]


# ── Date-range validation ────────────────────────────────────────────────


def test_empty_range_with_no_production_data_is_a_clean_no_op(client: MlflowClient) -> None:
    neon = FakeNeonConnection()
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra", "nar", "banei"),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.dates_processed == 1
    assert summary.fp_runs_created == 0
    assert summary.rs_runs_created == 0
    assert summary.errors == []


def test_inverted_date_range_raises_value_error(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="must not be before"):
        sync_production.sync_production_range(
            client,
            "20260615",
            "20260614",
            neon_connect=lambda: FakeNeonConnection(),
            local_connect=lambda: FakeLocalConnection(),
        )


def test_invalid_date_from_raises_value_error(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="8-digit"):
        sync_production.sync_production_range(
            client,
            "not-a-date",
            DATE_STR,
            neon_connect=lambda: FakeNeonConnection(),
            local_connect=lambda: FakeLocalConnection(),
        )


def test_invalid_date_to_raises_value_error(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="8-digit"):
        sync_production.sync_production_range(
            client,
            DATE_STR,
            "not-a-date",
            neon_connect=lambda: FakeNeonConnection(),
            local_connect=lambda: FakeLocalConnection(),
        )


def test_emit_traces_parameter_has_no_effect_on_behavior(client: MlflowClient) -> None:
    """`emit_traces` is accepted for API-contract completeness only -- both
    True (default) and False must behave identically."""
    neon_rows = {("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
    with_traces_true = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        emit_traces=True,
        neon_connect=lambda: FakeNeonConnection(fp_rows=neon_rows),
        local_connect=lambda: FakeLocalConnection(),
    )
    with_traces_false = sync_production.sync_production_range(
        client,
        "20260620",
        "20260620",
        categories=("jra",),
        emit_traces=False,
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert with_traces_true.fp_runs_created == 1
    assert with_traces_false.fp_runs_created == 0
    assert with_traces_false.errors == []


# ── Default connection factories are wired to the real db module ───────────


def test_default_connect_factories_are_db_module_functions() -> None:
    sig = inspect.signature(sync_production.sync_production_range)
    assert sig.parameters["neon_connect"].default is db.connect_racing_neon
    assert sig.parameters["local_connect"].default is db.connect_local_replica


# ── predictions.parquet content: rank-1 lookup branch coverage ─────────────
#
# The rank-1 lookup loop's two non-"first row is rank 1" branches (continue
# scanning past a non-rank-1 row, and exhaust the loop with no rank-1 row at
# all) never occur with the single-horse-per-race fixtures used above, so
# they are exercised here with a two-horse race, reading back the logged
# predictions.parquet artifact through the public API (`client.
# download_artifacts` + `pd.read_parquet`) rather than reaching into
# sync_production's private table-builder directly.


def test_predictions_table_finds_rank1_horse_when_not_listed_first(
    client: MlflowClient, tmp_path: Path
) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 2),
                _fp_row("05", "01", "H2", "iter14", 1),
            ]
        }
    )
    local = FakeLocalConnection()
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    downloaded = client.download_artifacts(run.info.run_id, "predictions.parquet", str(tmp_path))
    df = pd.read_parquet(downloaded)
    assert len(df) == 1
    assert df.iloc[0]["predicted_top1_ketto"] == "H2"


def test_predictions_table_no_rank1_horse_leaves_none(client: MlflowClient, tmp_path: Path) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 2),
                _fp_row("05", "01", "H2", "iter14", 3),
            ]
        }
    )
    local = FakeLocalConnection()
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    downloaded = client.download_artifacts(run.info.run_id, "predictions.parquet", str(tmp_path))
    df = pd.read_parquet(downloaded)
    assert len(df) == 1
    assert df.iloc[0]["predicted_top1_ketto"] is None


# ── Multiple model_version groups on the same date ─────────────────────────


def test_multiple_model_version_groups_each_get_their_own_run(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 1),
                _fp_row("05", "02", "H2", "iter20", 1),
            ]
        }
    )
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.fp_runs_created == 2
    _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter20")


def test_non_genuine_backfill_rows_are_excluded(client: MlflowClient) -> None:
    """A row whose prediction_generated_at is decades away from the race
    date must never create a sync run at all."""
    stale_generated_at = datetime(2020, 1, 1, 3, 0, 0, tzinfo=UTC)
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1, stale_generated_at)]}
    )
    local = FakeLocalConnection()
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.fp_runs_created == 0
    assert client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE) is not None
