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
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import pytest
from mlflow import MlflowClient
from mlflow.entities import Run, Trace
from mlflow.exceptions import MlflowException
from mlflow.store.entities.paged_list import PagedList

from mlflow_tracking import config, db, registry, serve_eval, sync_production, timeline, trace_emit
from mlflow_tracking.logging_api import get_or_create_experiment

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
        date_str = f"{params[0]}{params[1]}"
        if date_str in self._conn.raise_for:
            raise psycopg2.OperationalError(f"boom local {date_str}")
        if "_se" in query:
            self._pending = self._conn.result_rows.get(date_str, [])
        elif "fetch_races_scheduled: jra" in query:
            # serve_eval.fetch_races_scheduled's jra-dedicated race-calendar
            # query (`_sync_fp_category_date` now calls this UNCONDITIONALLY
            # for every category, including jra/nar which never had a local
            # race-calendar query before 2026-07-11) -- tagged with a
            # distinguishing SQL comment so it never collides with
            # fetch_race_metadata's own jvd_ra SELECT (which lists kyori/
            # shusso_tosu/etc columns and carries no such comment).
            self._pending = self._conn.race_calendar_rows.get(("jra", date_str), [])
        elif "fetch_races_scheduled: nar" in query:
            # Same rationale as the jra branch above, checked BEFORE the
            # generic "race_bango FROM nvd_ra" substring below -- this
            # query's own text ALSO contains that substring (it queries the
            # same nvd_ra table), so order matters here.
            self._pending = self._conn.race_calendar_rows.get(("nar", date_str), [])
        elif "race_bango FROM nvd_ra" in query:
            # serve_eval.fetch_banei_race_count's dedicated race-calendar
            # query -- a distinct substring from fetch_race_metadata's own
            # nvd_ra SELECT (which lists keibajo_code first), so the two
            # never collide even though both read the same table. Also hit
            # by fetch_races_scheduled's own category="banei" branch (an
            # intentional exact re-issue of this same query text, see that
            # function's own docstring), so both call sites correctly read
            # from this SAME `banei_race_rows` fixture.
            self._pending = self._conn.banei_race_rows.get(date_str, [])
        else:
            self._pending = self._conn.meta_rows.get(date_str, [])

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._pending


class FakeLocalConnection:
    """Fake local-PostgreSQL-replica connection serving finalized results/
    metadata rows keyed by date_str (jvd_se/nvd_se share the "_se" substring,
    jvd_ra/nvd_ra share "_ra", so category need not be tracked separately).
    `banei_race_rows` backs `serve_eval.fetch_banei_race_count`'s dedicated
    race-calendar query (and `fetch_races_scheduled`'s category="banei"
    branch, which re-issues that exact query) -- rows are never inspected,
    only counted, so a caller may reuse `_meta_row(...)`'s tuple shape or any
    placeholder tuple of the right length. `race_calendar_rows` (2026-07-11)
    is the jra/nar counterpart, keyed by (category, date_str) since -- unlike
    every other dict here -- a single `sync_production_range` call over
    `categories=("jra", "nar")` can request BOTH categories' own scheduled
    count for the SAME date_str."""

    result_rows: dict[str, list[tuple[object, ...]]]
    meta_rows: dict[str, list[tuple[object, ...]]]
    banei_race_rows: dict[str, list[tuple[object, ...]]]
    race_calendar_rows: dict[tuple[str, str], list[tuple[object, ...]]]
    raise_for: frozenset[str]
    closed: bool

    def __init__(
        self,
        result_rows: dict[str, list[tuple[object, ...]]] | None = None,
        meta_rows: dict[str, list[tuple[object, ...]]] | None = None,
        banei_race_rows: dict[str, list[tuple[object, ...]]] | None = None,
        race_calendar_rows: dict[tuple[str, str], list[tuple[object, ...]]] | None = None,
        raise_for: frozenset[str] = frozenset(),
    ) -> None:
        self.result_rows = result_rows or {}
        self.meta_rows = meta_rows or {}
        self.banei_race_rows = banei_race_rows or {}
        self.race_calendar_rows = race_calendar_rows or {}
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


# ── run_name is set AT CREATION (never blank/default) ──────────────────────
#
# Regression coverage: none of this module's 3 `client.create_run(...)` call
# sites used to pass `run_name`, so a freshly created run displayed as a bare
# run_id in the MLflow UI until some later out-of-band rename. Every pattern
# below is verified to match exactly what pre-existing runs in the real store
# already display (confirmed via the REST API against the live store).


def test_sync_run_name_set_at_creation(client: MlflowClient) -> None:
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
    assert run.info.run_name == f"{DATE_STR} jra iter14"


def test_serving_gap_run_name_set_at_creation(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection()
    with pytest.warns(UserWarning, match="jra"):
        sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'"
    )
    assert len(matches) == 1
    assert matches[0].info.run_name == f"gap {DATE_STR} jra no_rows"


def test_champion_gap_run_name_set_at_creation(client: MlflowClient) -> None:
    _register_champion(client, "jra", "finish-position", "iter14")
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter20-challenger", 1)]}
    )
    local = FakeLocalConnection()
    with pytest.warns(UserWarning, match="champion gap"):
        sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.champion_gap_key = '{DATE_STR}:jra:finish-position'",
    )
    assert len(matches) == 1
    assert matches[0].info.run_name == f"champion-gap {DATE_STR} jra"


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
    assert timeline.step_for_date(DATE_STR) in fp_dates
    assert run.data.metrics["fp_top1_pct"] == 100.0
    assert run.data.metrics["fp_races_evaluated"] == 1.0
    # This race has only 1 finalized result row (H1) -- total_starters=1, so
    # place4/5/6_pct are all None (see serve_eval.aggregate_fp_day_metrics)
    # and must never be logged as a fabricated 0.0 metric.
    assert "fp_place4_pct" not in run.data.metrics
    assert "fp_place5_pct" not in run.data.metrics
    assert "fp_place6_pct" not in run.data.metrics


def test_fp_place456_pct_none_is_not_logged_as_a_metric(client: MlflowClient) -> None:
    """Mirrors test_rs_macro_f1_none_is_not_logged_as_a_metric for the FP
    side: a too-small field (3 finalized results, all below rank 4) makes
    place4_pct/place5_pct/place6_pct all None -- must not attempt to log a
    None metric value, while top1_pct (unaffected by the small-field guard)
    still logs normally."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        result_rows={
            DATE_STR: [
                _result_row("05", "01", "H1", "1", "01"),
                _result_row("05", "01", "H2", "2", "02"),
                _result_row("05", "01", "H3", "3", "03"),
            ]
        }
    )
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.metrics["fp_top1_pct"] == 100.0
    assert "fp_place4_pct" not in run.data.metrics
    assert "fp_place5_pct" not in run.data.metrics
    assert "fp_place6_pct" not in run.data.metrics


def test_fp_place456_pct_logged_for_a_large_enough_field(client: MlflowClient) -> None:
    """A 6-starter field: place4_pct/place5_pct/place6_pct are all real,
    logged percentages (100.0 -- the predicted winner H1 genuinely won)."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        result_rows={
            DATE_STR: [
                _result_row("05", "01", "H1", "1", "01"),
                _result_row("05", "01", "H2", "2", "02"),
                _result_row("05", "01", "H3", "3", "03"),
                _result_row("05", "01", "H4", "4", "04"),
                _result_row("05", "01", "H5", "5", "05"),
                _result_row("05", "01", "H6", "6", "06"),
            ]
        }
    )
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.metrics["fp_place4_pct"] == 100.0
    assert run.data.metrics["fp_place5_pct"] == 100.0
    assert run.data.metrics["fp_place6_pct"] == 100.0


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
    assert timeline.step_for_date(DATE_STR) in rs_dates


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


def test_no_rs_experiment_created_for_banei_only_categories(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    """category="banei" is FP-eligible but not RS-eligible, so no RS query is
    ever issued for it -- unlike the (now-removed) original behavior, the
    serving-gap check itself DOES still run for banei (via the local race
    calendar, see the `test_banei_*` tests below), but it must never touch
    the RS production-usage experiment at all."""
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


# ── Serving-gap detection: Ban-ei via the local nvd_ra race calendar ────────
#
# Regression coverage for a real, still-open incident: Ban-ei has no
# running-style model at all, so the original RS-vs-FP comparison could never
# even LOOK at banei (it early-`continue`d before the gap check for any
# non-RS-eligible category). Ban-ei has been dark (zero genuinely-served FP
# rows) since 2026-05-24 with nothing ever surfacing it. The local replica's
# own nvd_ra race calendar (keibajo_code == BANEI_KEIBAJO_CODE), independent
# of whether anything was ever served, is the only "did races even happen"
# oracle available for it.


def test_banei_serving_gap_detected_via_race_calendar(client: MlflowClient) -> None:
    neon = FakeNeonConnection()
    local = FakeLocalConnection(
        banei_race_rows={DATE_STR: [_meta_row("83", "01", 2000, 10, "00", "A")]}
    )

    with pytest.warns(UserWarning, match="banei"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("banei",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )

    assert summary.serving_gaps_detected == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:banei'"
    )
    assert len(matches) == 1
    gap_run = matches[0]
    assert gap_run.data.tags["gap_source"] == "race_calendar"
    assert gap_run.data.tags["gap_type"] == "no_rows"
    assert gap_run.data.metrics["expected_races"] == 1.0
    assert gap_run.data.metrics["fp_races_observed"] == 0.0
    assert gap_run.data.metrics["fp_races_live"] == 0.0
    assert "rs_races_observed" not in gap_run.data.metrics


def test_banei_no_gap_when_race_calendar_empty(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    """No races scheduled at all that day (calendar count 0) -- an ordinary
    dark day, not a gap."""
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


def test_banei_no_gap_when_fp_has_live_rows(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    neon = FakeNeonConnection(fp_rows={("nar", DATE_STR): [_fp_row("83", "01", "H1", "v1", 1)]})
    local = FakeLocalConnection(
        banei_race_rows={DATE_STR: [_meta_row("83", "01", 2000, 10, "00", "A")]}
    )
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


def test_banei_serving_gap_expected_query_failure_is_isolated(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient failure while resolving the banei race-calendar oracle
    itself (not while logging the marker) must be isolated exactly like every
    other failure point in this function."""

    def _boom(*args: object, **kwargs: object) -> int:
        raise psycopg2.OperationalError("boom banei calendar")

    monkeypatch.setattr(serve_eval, "fetch_banei_race_count", _boom)

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
    assert len(summary.errors) == 1
    assert "serving-gap-expected" in summary.errors[0]
    assert "boom banei calendar" in summary.errors[0]


def test_banei_serving_gap_marker_not_duplicated_on_repeated_calls(client: MlflowClient) -> None:
    def _fresh_local() -> FakeLocalConnection:
        return FakeLocalConnection(
            banei_race_rows={DATE_STR: [_meta_row("83", "01", 2000, 10, "00", "A")]}
        )

    with pytest.warns(UserWarning, match="banei"):
        first = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("banei",),
            neon_connect=lambda: FakeNeonConnection(),
            local_connect=_fresh_local,
        )
    assert first.serving_gaps_detected == 1

    with pytest.warns(UserWarning, match="banei"):
        second = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("banei",),
            neon_connect=lambda: FakeNeonConnection(),
            local_connect=_fresh_local,
        )
    assert second.serving_gaps_detected == 1

    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:banei'"
    )
    assert len(matches) == 1


# ── Both-pipelines-dark fallback (jra/nar, 2026-07-11) ──────────────────────
#
# Regression coverage for a residual blind spot: the pre-existing jra/nar
# no_rows/backfill_only check compares FP against running-style's OWN
# races_observed as its "expected races" proxy. On a day RS *itself* also
# observed zero races (real examples: JRA 2026-06-13, 06-14, 06-20, 06-21,
# 06-28), that proxy has nothing to compare FP against, so the check never
# fired even though races may genuinely have been scheduled that day.
# `_resolve_expected_races` now falls back, in exactly that case, to the same
# race-calendar oracle banei uses.


def test_both_dark_gap_detected_via_race_calendar_oracle(client: MlflowClient) -> None:
    """FP and RS both observed zero races this call, but the race calendar
    shows races WERE scheduled -- the fallback oracle must fire a `no_rows`
    gap tagged `gap_source=race_calendar` (not `running_style`, since RS
    itself had nothing to offer)."""
    neon = FakeNeonConnection()  # no fp_rows, no rs_rows -- both fully dark
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(3)]})

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
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'"
    )
    assert len(matches) == 1
    gap_run = matches[0]
    assert gap_run.data.tags["gap_type"] == "no_rows"
    assert gap_run.data.tags["gap_source"] == "race_calendar"
    assert gap_run.data.metrics["expected_races"] == 3.0
    assert gap_run.data.metrics["fp_races_scheduled"] == 3.0
    assert gap_run.data.metrics["fp_races_observed"] == 0.0
    assert gap_run.data.metrics["fp_races_live"] == 0.0
    # The RS-vs-FP backward-compat metric must NOT be logged for this
    # fallback path -- it is only meaningful when gap_source=running_style.
    assert "rs_races_observed" not in gap_run.data.metrics


def test_both_dark_no_gap_when_race_calendar_also_empty(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    """FP, RS, AND the race calendar are all empty -- genuinely no races that
    day, not a gap. Explicit regression test for the fallback's own "0 stays
    0, not a fabricated gap" behavior (distinct from
    test_no_serving_gap_when_both_fp_and_rs_empty, which never configures
    race_calendar_rows at all -- this makes the fallback's own zero-count
    path explicit)."""
    neon = FakeNeonConnection()
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): []})
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


def test_both_dark_backfill_only_gap_type_via_race_calendar_oracle(
    client: MlflowClient,
) -> None:
    """FP has rows, but every one is backfill-only (nothing genuinely LIVE),
    AND RS itself observed zero races -- the fallback must still correctly
    select `backfill_only` (not `no_rows`), off `fp_races_observed > 0`,
    exactly like the pre-existing RS-vs-FP path does."""
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1, _BACKFILL_GEN_AT)]}
    )
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(2)]})

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
    gap_run = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'"
    )[0]
    assert gap_run.data.tags["gap_type"] == "backfill_only"
    assert gap_run.data.tags["gap_source"] == "race_calendar"
    assert gap_run.data.metrics["fp_races_observed"] == 1.0
    assert gap_run.data.metrics["fp_races_backfilled"] == 1.0


def test_both_dark_fallback_reached_for_nar_too(client: MlflowClient) -> None:
    """The fallback applies uniformly to both RS_CATEGORIES members, not just
    jra -- nar reaches the exact same code path."""
    neon = FakeNeonConnection()
    local = FakeLocalConnection(race_calendar_rows={("nar", DATE_STR): [() for _ in range(1)]})

    with pytest.warns(UserWarning, match="nar"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("nar",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )

    assert summary.serving_gaps_detected == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    gap_run = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:nar'"
    )[0]
    assert gap_run.data.tags["gap_source"] == "race_calendar"


# ── Backfill-vs-live distinction ────────────────────────────────────────────
#
# Regression coverage: a date whose only FP rows are delayed backfill
# re-predictions (prediction_generated_at well after the race date, but still
# within the broader genuine-serving tolerance) previously looked "served"
# from the RS-vs-FP comparison's point of view even though nothing was
# genuinely LIVE that day.

_BACKFILL_GEN_AT: datetime = datetime(2026, 6, 17, 3, 0, 0, tzinfo=UTC)  # 3 days after DATE_STR


def test_fp_races_live_and_backfilled_metrics_split(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 1),
                _fp_row("05", "02", "H2", "iter14", 1, _BACKFILL_GEN_AT),
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
    assert run.data.metrics["fp_races"] == 2.0
    assert run.data.metrics["fp_races_live"] == 1.0
    assert run.data.metrics["fp_races_backfilled"] == 1.0


def test_backfill_only_gap_type_when_fp_rows_all_backfill(client: MlflowClient) -> None:
    rs_row = _rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1, _BACKFILL_GEN_AT)]},
        rs_rows={("jra", DATE_STR): [rs_row]},
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
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'"
    )
    assert len(matches) == 1
    gap_run = matches[0]
    assert gap_run.data.tags["gap_type"] == "backfill_only"
    assert gap_run.data.tags["gap_source"] == "running_style"
    assert gap_run.data.metrics["fp_races_observed"] == 1.0
    assert gap_run.data.metrics["fp_races_live"] == 0.0
    assert gap_run.data.metrics["fp_races_backfilled"] == 1.0
    assert gap_run.data.metrics["rs_races_observed"] == 1.0


# ── Coverage-ratio metrics + GAP_TYPE_PARTIAL_COVERAGE ──────────────────────
#
# Regression coverage for a real production blind spot found 2026-07-10: JRA's
# champion jra-cb-v9-sim-2013-clean has never written a single row since its
# 07-04 deploy -- only a cell-routed variant serves, for one narrow class-code
# slice, ~3% of scheduled races (real observed shapes: 11/485, 16/501,
# 16/476). Because that variant is champion-derived and races_live > 0, EVERY
# existing gap detector above (races_live == 0-gated) reads such a day as
# healthy. serve_eval.fetch_races_scheduled + GAP_TYPE_PARTIAL_COVERAGE close
# this blind spot with an independent coverage-ratio check.


def _n_fp_rows(count: int, *, model_version: str = "iter14") -> list[tuple[object, ...]]:
    """Build `count` distinct-race genuine live FP rows sharing one venue,
    race_bango "01".."count" -- enough for _distinct_race_count to report
    exactly `count` distinct races."""
    return [
        _fp_row("05", str(i).zfill(3), f"H{i}", model_version, 1) for i in range(1, count + 1)
    ]


def _n_banei_rows(count: int, *, model_version: str = "v1") -> list[tuple[object, ...]]:
    """Ban-ei counterpart of `_n_fp_rows`, using the Ban-ei keibajo_code
    (predictions stored under source="nar", see serve_eval.resolve_source)."""
    return [
        _fp_row(serve_eval.BANEI_KEIBAJO_CODE, str(i).zfill(3), f"H{i}", model_version, 1)
        for i in range(1, count + 1)
    ]


def _get_timeline_run(client: MlflowClient, task: str, category: str) -> Run:
    experiment = client.get_experiment_by_name(config.EXPERIMENT_TIMELINES)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.timeline_key_v2 = '{task}:{category}'",
    )
    assert len(matches) == 1
    return matches[0]


def test_fp_races_scheduled_and_coverage_pct_logged_on_model_version_run(
    client: MlflowClient,
) -> None:
    """Base-tracking AND eval passes both attach fp_races_scheduled/
    fp_coverage_pct to the SAME per-(date, category, model_version) run --
    duplicated day-level values, per this module's own documented judgment
    call (see _log_base_tracking's docstring)."""
    # race_calendar_rows == 1 (matching the single served race) keeps
    # coverage_pct at 100.0 -- comfortably above the default 80% threshold,
    # so this test's own data never incidentally triggers a partial_coverage
    # gap/warning (that behavior has its own dedicated tests below).
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): _n_fp_rows(1)})
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "001", "H1", "1", "01")]},
        race_calendar_rows={("jra", DATE_STR): [()]},
    )
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.metrics["fp_races_scheduled"] == 1.0
    assert run.data.metrics["fp_coverage_pct"] == pytest.approx(100.0)


def test_fp_races_scheduled_and_coverage_pct_logged_on_timeline(client: MlflowClient) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): _n_fp_rows(1)})
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "001", "H1", "1", "01")]},
        race_calendar_rows={("jra", DATE_STR): [()]},
    )
    sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    timeline_run = _get_timeline_run(client, "finish-position", "jra")
    assert timeline_run.data.metrics["fp_races_scheduled"] == 1.0
    assert timeline_run.data.metrics["fp_coverage_pct"] == pytest.approx(100.0)


def test_fp_coverage_pct_absent_and_races_scheduled_zero_when_no_calendar_data(
    client: MlflowClient,
) -> None:
    """A day where the race-calendar oracle itself has no rows (races_
    scheduled == 0) -- an undefined ratio, never a fabricated 0.0/100.0 --
    logs fp_races_scheduled=0.0 but omits fp_coverage_pct entirely, and never
    fires a partial_coverage gap (nothing to compute a ratio from)."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): _n_fp_rows(1)})
    local = FakeLocalConnection()  # no race_calendar_rows configured at all
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.serving_gaps_detected == 0
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14")
    assert run.data.metrics["fp_races_scheduled"] == 0.0
    assert "fp_coverage_pct" not in run.data.metrics


def test_partial_coverage_gap_detected_jra_real_incident_shape(client: MlflowClient) -> None:
    """The exact real incident shape (2026-07-10): 485 scheduled races, only
    11 served live -- coverage rounds to ~2.27%, well under the 80% default
    threshold, and races_live > 0 so no_rows/backfill_only never fire."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): _n_fp_rows(11)})
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(485)]})

    with pytest.warns(UserWarning, match="partial"):
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
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'"
    )
    assert len(matches) == 1
    gap_run = matches[0]
    assert gap_run.data.tags["gap_type"] == "partial_coverage"
    assert gap_run.data.tags["gap_source"] == "race_calendar"
    assert gap_run.data.metrics["fp_races_scheduled"] == 485.0
    assert gap_run.data.metrics["fp_races_live"] == 11.0
    assert gap_run.data.metrics["expected_races"] == 485.0
    assert round(gap_run.data.metrics["fp_coverage_pct"], 2) == pytest.approx(2.27)


def test_partial_coverage_no_gap_when_coverage_exactly_at_threshold(client: MlflowClient) -> None:
    """80/100 == exactly the default 80.0% threshold -- the check is a
    STRICT less-than, so exactly-at-threshold must NOT fire."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): _n_fp_rows(80)})
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(100)]})
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.serving_gaps_detected == 0


def test_partial_coverage_no_gap_when_coverage_above_threshold(client: MlflowClient) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): _n_fp_rows(95)})
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(100)]})
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.serving_gaps_detected == 0


def test_partial_coverage_configurable_threshold(client: MlflowClient) -> None:
    """The SAME 20/100 (20%) coverage shape is a gap under the default 80%
    threshold but not under a lowered 10% threshold -- confirms
    --partial-coverage-threshold genuinely changes the outcome."""

    def _fresh_neon() -> FakeNeonConnection:
        return FakeNeonConnection(fp_rows={("jra", DATE_STR): _n_fp_rows(20)})

    def _fresh_local() -> FakeLocalConnection:
        return FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(100)]})

    lenient = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        partial_coverage_threshold=10.0,
        neon_connect=_fresh_neon,
        local_connect=_fresh_local,
    )
    assert lenient.serving_gaps_detected == 0

    with pytest.warns(UserWarning, match="partial"):
        strict = sync_production.sync_production_range(
            client,
            "20260615",
            "20260615",
            categories=("jra",),
            partial_coverage_threshold=sync_production.DEFAULT_PARTIAL_COVERAGE_THRESHOLD,
            neon_connect=lambda: FakeNeonConnection(
                fp_rows={
                    ("jra", "20260615"): [
                        _fp_row("05", str(i).zfill(3), f"H{i}", "iter14", 1, kaisai_tsukihi="0615")
                        for i in range(1, 21)
                    ]
                }
            ),
            local_connect=lambda: FakeLocalConnection(
                race_calendar_rows={("jra", "20260615"): [() for _ in range(100)]}
            ),
        )
    assert strict.serving_gaps_detected == 1


def test_partial_coverage_mixed_backfill_and_partial_live_day(client: MlflowClient) -> None:
    """A day with BOTH some genuinely-live races AND some backfill-only races
    (reachable: they're just different race_bango values with different
    prediction_generated_at timestamps) -- races_live > 0 so no_rows/
    backfill_only never fire, but coverage is still well under threshold."""
    live_rows = _n_fp_rows(2)
    backfill_rows = [
        _fp_row("05", str(i).zfill(3), f"B{i}", "iter14", 1, _BACKFILL_GEN_AT)
        for i in range(101, 104)
    ]
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): live_rows + backfill_rows})
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(50)]})

    with pytest.warns(UserWarning, match="partial"):
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
    gap_run = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'"
    )[0]
    assert gap_run.data.tags["gap_type"] == "partial_coverage"
    assert gap_run.data.metrics["fp_races_live"] == 2.0
    assert gap_run.data.metrics["fp_races_backfilled"] == 3.0
    assert gap_run.data.metrics["fp_races_scheduled"] == 50.0


def test_partial_coverage_does_not_override_existing_no_rows_gap(
    client: MlflowClient,
) -> None:
    """A totally-empty FP day (races_live == 0) with race-calendar data ALSO
    present must still fire GAP_TYPE_NO_ROWS (existing behavior, unaffected),
    never GAP_TYPE_PARTIAL_COVERAGE -- the two checks are mutually exclusive
    on races_live, and the pre-existing no_rows/backfill_only logic must keep
    working exactly as before."""
    neon = FakeNeonConnection(
        rs_rows={("jra", DATE_STR): [_rs_row("05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI)]}
    )
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(50)]})

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
    gap_run = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'"
    )[0]
    assert gap_run.data.tags["gap_type"] == "no_rows"
    assert gap_run.data.tags["gap_source"] == "running_style"
    # Additive enrichment: the pre-existing no_rows marker run now ALSO
    # carries the new coverage-ratio metrics.
    assert gap_run.data.metrics["fp_races_scheduled"] == 50.0
    assert gap_run.data.metrics["fp_coverage_pct"] == 0.0


def test_partial_coverage_banei_uses_race_calendar_gap_source(client: MlflowClient) -> None:
    """category="banei" reaches GAP_TYPE_PARTIAL_COVERAGE the same way as
    jra/nar -- fetch_races_scheduled's category="banei" branch (re-issuing
    the exact banei_race_rows-backed query) makes this reachable without any
    RS side (Ban-ei has no running-style model at all)."""
    neon = FakeNeonConnection(fp_rows={("nar", DATE_STR): _n_banei_rows(1)})
    local = FakeLocalConnection(banei_race_rows={DATE_STR: [() for _ in range(20)]})

    with pytest.warns(UserWarning, match="partial"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("banei",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )

    assert summary.serving_gaps_detected == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    gap_run = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:banei'"
    )[0]
    assert gap_run.data.tags["gap_type"] == "partial_coverage"
    assert gap_run.data.tags["gap_source"] == "race_calendar"
    assert gap_run.data.metrics["fp_races_scheduled"] == 20.0
    assert gap_run.data.metrics["fp_races_live"] == 1.0


def test_serving_gap_partial_coverage_logging_failure_is_isolated_and_recorded(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _boom(*args: object, **kwargs: object) -> None:
        raise MlflowException("boom partial-coverage log")

    monkeypatch.setattr(sync_production, "_log_serving_gap", _boom)

    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): _n_fp_rows(2)})
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(100)]})

    with pytest.warns(UserWarning, match="partial"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )
    assert summary.serving_gaps_detected == 0
    assert len(summary.errors) == 1
    assert "serving-gap-partial-coverage" in summary.errors[0]
    assert "boom partial-coverage log" in summary.errors[0]


def test_partial_coverage_marker_not_duplicated_on_repeated_calls(client: MlflowClient) -> None:
    def _fresh_neon() -> FakeNeonConnection:
        return FakeNeonConnection(fp_rows={("jra", DATE_STR): _n_fp_rows(2)})

    def _fresh_local() -> FakeLocalConnection:
        return FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [() for _ in range(100)]})

    with pytest.warns(UserWarning, match="partial"):
        first = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=_fresh_neon,
            local_connect=_fresh_local,
        )
    assert first.serving_gaps_detected == 1

    with pytest.warns(UserWarning, match="partial"):
        second = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=_fresh_neon,
            local_connect=_fresh_local,
        )
    assert second.serving_gaps_detected == 1

    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id], filter_string=f"tags.serving_gap_key = '{DATE_STR}:jra'"
    )
    assert len(matches) == 1


# ── Champion-vs-served mismatch ─────────────────────────────────────────────
#
# Regression coverage for a real incident: JRA silently served only a
# superseded challenger variant (e.g. a "-jockey-pedigree269" build) for
# weeks after a rollback, while the registry's champion alias still pointed
# at a different model_version -- `champion_at_sync` already records this per
# RUN, but nothing rolled that up into a visible per-day signal.


def test_fp_champion_gap_detected_when_live_rows_not_from_champion(client: MlflowClient) -> None:
    _register_champion(client, "jra", "finish-position", "iter14")
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter20-challenger", 1)]}
    )
    local = FakeLocalConnection()

    with pytest.warns(UserWarning, match="champion gap"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )

    assert summary.champion_gaps_detected == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.champion_gap_key = '{DATE_STR}:jra:finish-position'",
    )
    assert len(matches) == 1
    gap_run = matches[0]
    assert gap_run.data.tags["champion_gap"] == "true"
    assert gap_run.data.tags["champion_served"] == "false"
    assert gap_run.data.tags["champion_model_version"] == "iter14"
    assert gap_run.data.tags["served_model_versions"] == "iter20-challenger"
    assert gap_run.info.status == "FINISHED"


def test_fp_champion_gap_not_detected_when_champion_served(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    _register_champion(client, "jra", "finish-position", "iter14")
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
    assert summary.champion_gaps_detected == 0
    assert not any("champion gap" in str(w.message) for w in recwarn.list)


def test_champion_gap_not_detected_when_no_champion_registered(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
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
    assert summary.champion_gaps_detected == 0
    assert not any("champion gap" in str(w.message) for w in recwarn.list)


def test_champion_gap_not_detected_when_no_live_rows(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    """Champion registered, but every row that landed is backfill -- there is
    nothing genuinely LIVE to compare against the champion at all, so this is
    purely a (possible) serving gap, never a champion mismatch."""
    _register_champion(client, "jra", "finish-position", "iter14")
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter20", 1, _BACKFILL_GEN_AT)]}
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
    assert summary.champion_gaps_detected == 0
    assert not any("champion gap" in str(w.message) for w in recwarn.list)


def test_rs_champion_gap_not_detected_when_champion_served(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    """RS-side twin of `test_fp_champion_gap_not_detected_when_champion_served`
    -- exercises the RS sync's own `model_version == champion_label` match
    branch (a distinct code path from the FP sync's identical-looking one)."""
    _register_champion(client, "jra", "running-style", "rs-v3")
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
    assert summary.champion_gaps_detected == 0
    assert not any("champion gap" in str(w.message) for w in recwarn.list)


def test_rs_all_backfill_rows_have_no_live_model_versions(client: MlflowClient) -> None:
    """RS-side twin of the FP backfill-only scenario -- exercises the RS
    sync's own `if group_live_rows:` False branch (a distinct code path from
    the FP sync's identical-looking one)."""
    neon = FakeNeonConnection(
        rs_rows={
            ("jra", DATE_STR): [
                _rs_row(
                    "05", "01", "H1", "rs-v3", serve_eval.RS_CLASS_SASHI, _BACKFILL_GEN_AT
                )
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
    assert summary.champion_gaps_detected == 0
    run = _get_run(client, config.EXPERIMENT_RS_PRODUCTION_USAGE, f"{DATE_STR}:jra:rs-v3")
    assert run.data.metrics["rs_races_live"] == 0.0
    assert run.data.metrics["rs_races_backfilled"] == 1.0


def test_rs_champion_gap_detected(client: MlflowClient) -> None:
    _register_champion(client, "jra", "running-style", "rs-v3")
    rs_row = _rs_row("05", "01", "H1", "rs-v4-challenger", serve_eval.RS_CLASS_NIGE)
    neon = FakeNeonConnection(rs_rows={("jra", DATE_STR): [rs_row]})
    local = FakeLocalConnection()

    with pytest.warns(UserWarning, match="running-style"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )

    assert summary.champion_gaps_detected == 1
    experiment = client.get_experiment_by_name(config.EXPERIMENT_RS_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.champion_gap_key = '{DATE_STR}:jra:running-style'",
    )
    assert len(matches) == 1
    assert matches[0].data.tags["champion_model_version"] == "rs-v3"
    assert matches[0].data.tags["served_model_versions"] == "rs-v4-challenger"


# ── Champion-OR-variant widening (fixes false "champion gap" alarms) ───────
#
# Production routes some races/horses to a CELL-ROUTED VARIANT of the
# champion, whose model_version is the champion label plus a "-<routing-
# scope>" suffix. The original exact-match-only `champion_live_races`
# accumulation made `_check_champion_gap` fire a false "champion did not
# serve" alarm on any day where only a variant (never the bare champion
# label) actually served. These tests cover: a variant match (NEW, no gap,
# `champion_served="variant"` tag set), a deliberately-tricky near-miss
# unrelated model_version that shares a raw string prefix but is NOT
# "-"-separated from the champion label (still a REAL gap), and that
# `champion_at_sync`'s existing true/false semantics are untouched.


def test_fp_champion_served_variant_tag_and_no_gap(client: MlflowClient) -> None:
    _register_champion(client, "jra", "finish-position", "iter14")
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14-jockey-pedigree269", 1)]}
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
    assert summary.champion_gaps_detected == 0

    run = _get_run(
        client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter14-jockey-pedigree269"
    )
    # champion_at_sync keeps its EXACT-MATCH-ONLY semantics: a variant is NOT
    # an exact match, so this stays "false" -- champion_served is the
    # additive signal that distinguishes "variant" from "unrelated".
    assert run.data.tags["champion_at_sync"] == "false"
    assert run.data.tags["champion_served"] == "variant"


def test_rs_champion_served_variant_tag_and_no_gap(
    client: MlflowClient, recwarn: pytest.WarningsRecorder
) -> None:
    """RS-side twin of the FP variant-widening test above -- exercises the RS
    sync's own `_is_champion_or_variant` call, a distinct code path from the
    FP sync's identical-looking one."""
    _register_champion(client, "jra", "running-style", "rs-v3")
    neon = FakeNeonConnection(
        rs_rows={
            ("jra", DATE_STR): [
                _rs_row("05", "01", "H1", "rs-v3-jockeyA", serve_eval.RS_CLASS_SASHI)
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
    assert summary.champion_gaps_detected == 0
    assert not any("champion gap" in str(w.message) for w in recwarn.list)

    run = _get_run(client, config.EXPERIMENT_RS_PRODUCTION_USAGE, f"{DATE_STR}:jra:rs-v3-jockeyA")
    assert run.data.tags["champion_at_sync"] == "false"
    assert run.data.tags["champion_served"] == "variant"


def test_fp_champion_gap_still_detected_for_unrelated_near_miss_model_version(
    client: MlflowClient,
) -> None:
    """"iter140" shares "iter14" as a raw string prefix but is NOT
    "-"-separated from it (`"iter140".startswith("iter14-")` is False) --
    this must still classify as an unrelated model_version, not a variant,
    so the champion gap fires exactly as it always did, and no
    `champion_served` tag is attached."""
    _register_champion(client, "jra", "finish-position", "iter14")
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter140", 1)]}
    )
    local = FakeLocalConnection()

    with pytest.warns(UserWarning, match="champion gap"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )

    assert summary.champion_gaps_detected == 1
    run = _get_run(client, config.EXPERIMENT_FP_PRODUCTION_USAGE, f"{DATE_STR}:jra:iter140")
    assert run.data.tags["champion_at_sync"] == "false"
    assert "champion_served" not in run.data.tags

    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.champion_gap_key = '{DATE_STR}:jra:finish-position'",
    )
    assert len(matches) == 1
    assert matches[0].data.tags["champion_served"] == "false"
    assert matches[0].data.tags["served_model_versions"] == "iter140"


def test_champion_served_tag_absent_for_exact_champion_match(client: MlflowClient) -> None:
    """An exact champion match gets no `champion_served` tag at all -- it is
    purely additive for the variant case, never a replacement for
    `champion_at_sync`."""
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
    assert "champion_served" not in run.data.tags


def test_champion_gap_marker_not_duplicated_on_repeated_calls(client: MlflowClient) -> None:
    _register_champion(client, "jra", "finish-position", "iter14")

    def _fresh_neon() -> FakeNeonConnection:
        return FakeNeonConnection(
            fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter20-challenger", 1)]}
        )

    with pytest.warns(UserWarning, match="champion gap"):
        first = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=_fresh_neon,
            local_connect=lambda: FakeLocalConnection(),
        )
    assert first.champion_gaps_detected == 1

    with pytest.warns(UserWarning, match="champion gap"):
        second = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=_fresh_neon,
            local_connect=lambda: FakeLocalConnection(),
        )
    assert second.champion_gaps_detected == 1

    experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert experiment is not None
    matches = client.search_runs(
        [experiment.experiment_id],
        filter_string=f"tags.champion_gap_key = '{DATE_STR}:jra:finish-position'",
    )
    assert len(matches) == 1


def test_champion_gap_logging_failure_is_isolated_and_recorded(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _boom(*args: object, **kwargs: object) -> None:
        raise MlflowException("boom champion-gap log")

    monkeypatch.setattr(sync_production, "_log_champion_gap", _boom)

    _register_champion(client, "jra", "finish-position", "iter14")
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter20-challenger", 1)]}
    )
    local = FakeLocalConnection()
    with pytest.warns(UserWarning, match="champion gap"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )
    assert summary.champion_gaps_detected == 0
    assert summary.fp_runs_created == 1
    assert len(summary.errors) == 1
    assert "champion-gap:finish-position" in summary.errors[0]
    assert "boom champion-gap log" in summary.errors[0]


def test_rs_champion_gap_logging_failure_is_isolated_and_recorded(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same isolation contract as the FP-side test above, exercised through
    the RS-side try/except block instead (a distinct code path in the main
    loop, even though both call the same `_log_champion_gap` helper)."""

    def _boom(*args: object, **kwargs: object) -> None:
        raise MlflowException("boom rs champion-gap log")

    monkeypatch.setattr(sync_production, "_log_champion_gap", _boom)

    _register_champion(client, "jra", "running-style", "rs-v3")
    neon = FakeNeonConnection(
        rs_rows={
            ("jra", DATE_STR): [
                _rs_row("05", "01", "H1", "rs-v4-challenger", serve_eval.RS_CLASS_NIGE)
            ]
        }
    )
    local = FakeLocalConnection()
    with pytest.warns(UserWarning, match="running-style"):
        summary = sync_production.sync_production_range(
            client,
            DATE_STR,
            DATE_STR,
            categories=("jra",),
            neon_connect=lambda: neon,
            local_connect=lambda: local,
        )
    assert summary.champion_gaps_detected == 0
    assert summary.rs_runs_created == 1
    assert len(summary.errors) == 1
    assert "champion-gap:running-style" in summary.errors[0]
    assert "boom rs champion-gap log" in summary.errors[0]


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


def test_emit_traces_true_creates_a_real_trace_with_assessments(client: MlflowClient) -> None:
    """`emit_traces=True` (the default): once the FP eval join succeeds, a
    real MLflow trace (+ Feedback assessments) is emitted for that race via
    `trace_emit.emit_fp_race_traces` -- verified against the REAL tracing
    store (never mocked), matching this module's own 2026-07-10 wiring."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]}
    )
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        emit_traces=True,
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.fp_eval_logged == 1
    assert summary.traces_created == 1
    assert summary.traces_already_existed == 0

    fp_experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert fp_experiment is not None
    tracing_client = trace_emit.build_tracing_client(client)
    traces = tracing_client.search_traces(
        experiment_ids=[fp_experiment.experiment_id], include_spans=False, max_results=10
    )
    assert len(traces) == 1
    trace = tracing_client.get_trace(traces[0].info.trace_id)
    assert trace.info.tags[trace_emit.TRACE_BUSINESS_KEY_TAG] == f"{DATE_STR}:jra:05:01:iter14"
    assessment_names = {a.name for a in trace.info.assessments}
    assert assessment_names >= {"place1_hit", trace_emit.TOP3_BOX_ASSESSMENT_NAME}


def test_emit_traces_false_skips_trace_emission_entirely(client: MlflowClient) -> None:
    """`emit_traces=False` (the `--no-traces` CLI flag's target): metrics/
    tables are logged as normal, but NO trace is ever built -- not even a
    `TracingClient` is constructed."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]}
    )
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        emit_traces=False,
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.fp_eval_logged == 1
    assert summary.traces_created == 0
    assert summary.traces_already_existed == 0
    assert summary.errors == []

    fp_experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert fp_experiment is not None
    tracing_client = trace_emit.build_tracing_client(client)
    traces = tracing_client.search_traces(
        experiment_ids=[fp_experiment.experiment_id], include_spans=False, max_results=10
    )
    assert traces == []


def test_emit_traces_true_rs_creates_a_real_trace_with_predicted_class_hit(
    client: MlflowClient,
) -> None:
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
        emit_traces=True,
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.rs_eval_logged == 1
    assert summary.traces_created == 1

    rs_experiment = client.get_experiment_by_name(config.EXPERIMENT_RS_PRODUCTION_USAGE)
    assert rs_experiment is not None
    tracing_client = trace_emit.build_tracing_client(client)
    traces = tracing_client.search_traces(
        experiment_ids=[rs_experiment.experiment_id], include_spans=False, max_results=10
    )
    assert len(traces) == 1
    trace = tracing_client.get_trace(traces[0].info.trace_id)
    assert {a.name for a in trace.info.assessments} == {
        trace_emit.RS_PREDICTED_CLASS_HIT_ASSESSMENT_NAME
    }


def test_second_sync_call_never_recreates_or_duplicates_traces(client: MlflowClient) -> None:
    """A second `sync_production_range` call over the SAME range never
    re-attempts the eval join at all (already gated by `sync_eval_logged`,
    see this module's own idempotency state machine) -- so trace emission
    is never even reattempted on a normal daily-cron re-run, and the real
    trace count in the store stays at exactly 1."""

    def _fresh_neon() -> FakeNeonConnection:
        return FakeNeonConnection(
            fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
        )

    def _fresh_local() -> FakeLocalConnection:
        return FakeLocalConnection(
            result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]}
        )

    first = sync_production.sync_production_range(
        client, DATE_STR, DATE_STR, categories=("jra",), neon_connect=_fresh_neon,
        local_connect=_fresh_local,
    )
    assert first.traces_created == 1

    second = sync_production.sync_production_range(
        client, DATE_STR, DATE_STR, categories=("jra",), neon_connect=_fresh_neon,
        local_connect=_fresh_local,
    )
    assert second.traces_created == 0
    assert second.traces_already_existed == 0

    fp_experiment = client.get_experiment_by_name(config.EXPERIMENT_FP_PRODUCTION_USAGE)
    assert fp_experiment is not None
    tracing_client = trace_emit.build_tracing_client(client)
    traces = tracing_client.search_traces(
        experiment_ids=[fp_experiment.experiment_id], include_spans=False, max_results=10
    )
    assert len(traces) == 1


def test_trace_emission_errors_are_folded_into_summary_with_prefix(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]}
    )

    def _boom_emit(*args: object, **kwargs: object) -> trace_emit.TraceEmitSummary:
        return trace_emit.TraceEmitSummary(errors=["synthetic failure"])

    monkeypatch.setattr(trace_emit, "emit_fp_race_traces", _boom_emit)
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert len(summary.errors) == 1
    assert "trace-emit:finish-position" in summary.errors[0]
    assert "synthetic failure" in summary.errors[0]


# ── job_trace wiring: one trace per call into `timelines` ──────────────────


def _timelines_traces(client: MlflowClient) -> list[Trace]:
    tracing_client = trace_emit.build_tracing_client(client)
    experiment = client.get_experiment_by_name(config.EXPERIMENT_TIMELINES)
    assert experiment is not None
    return list(
        tracing_client.search_traces(
            experiment_ids=[experiment.experiment_id], include_spans=False, max_results=10
        )
    )


def test_sync_production_range_emits_timelines_job_trace_with_points_appended(
    client: MlflowClient,
) -> None:
    """job_trace wiring (2026-07-11): once at least one timeline point was
    appended this call (`fp_eval_logged + rs_eval_logged > 0`), one trace
    lands in `timelines` (never in `finish-position/production-usage`)
    with a `points_appended` Feedback matching that count."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]}
    )
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.fp_eval_logged == 1

    traces = _timelines_traces(client)
    assert len(traces) == 1
    tracing_client = trace_emit.build_tracing_client(client)
    trace = tracing_client.get_trace(traces[0].info.trace_id)
    assert {span.name for span in trace.data.spans} == {"sync-production"}
    assessments_by_name = {a.name: a for a in trace.info.assessments}
    assert set(assessments_by_name) == {"points_appended"}
    feedback = assessments_by_name["points_appended"].feedback
    assert feedback is not None
    assert feedback.value == float(summary.fp_eval_logged + summary.rs_eval_logged)


def test_sync_production_range_discards_timelines_job_trace_when_nothing_appended(
    client: MlflowClient,
) -> None:
    """A call with zero served rows at all (nothing to sync, the common
    daily case) never appends a timeline point -- the `timelines` job trace
    must be discarded, not logged empty."""
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
    assert summary.fp_eval_logged == 0
    assert summary.rs_eval_logged == 0
    assert _timelines_traces(client) == []


def test_sync_production_range_timelines_job_trace_is_unaffected_by_no_traces_flag(
    client: MlflowClient,
) -> None:
    """DELIBERATE design choice (see this module's own docstring): the
    `timelines` job trace is independent of `emit_traces=False`
    (`--no-traces`) -- that flag scopes only the per-race/per-horse
    emission into the production-usage experiments."""
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        result_rows={DATE_STR: [_result_row("05", "01", "H1", "1", "01")]}
    )
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        emit_traces=False,
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert summary.fp_eval_logged == 1
    assert summary.traces_created == 0  # per-race trace emission stays off

    traces = _timelines_traces(client)
    assert len(traces) == 1  # the timelines job trace still fires


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


# ── Self-heal sweep for interrupted invocations (2026-07-11) ────────────────
#
# Real incident: an interrupted `sync-production` process left 18 runs
# tagged `sync_base_logged=true` stuck `status=RUNNING` forever. Tests below
# use `categories=()` for several cases specifically so `sync_production_
# range`'s date/category loop body never executes at all (nothing to sync),
# which keeps a monkeypatched `client.search_runs` scoped to EXACTLY the
# self-heal sweep's own calls -- every other `client.search_runs` call site
# in this module (`_find_sync_run`/`_find_serving_gap_run`/
# `_find_champion_gap_run`) only ever runs inside that per-(date, category)
# loop body.


def _stale_candidate_run(
    client: MlflowClient, experiment_id: str, *, start_time_ms: int, status: str = "RUNNING"
) -> Run:
    """Create a run tagged `sync_base_logged=true` at `start_time_ms` --
    exactly the run family `_heal_stale_running_runs` targets -- optionally
    terminated to `status` (default: left RUNNING)."""
    run = client.create_run(
        experiment_id,
        start_time=start_time_ms,
        tags={sync_production.SYNC_BASE_LOGGED_TAG: sync_production.TRUE_STR},
    )
    if status != "RUNNING":
        client.set_terminated(run.info.run_id, status=status)
    return client.get_run(run.info.run_id)


def _hours_ago_ms(hours: float) -> int:
    return int((datetime.now(UTC) - timedelta(hours=hours)).timestamp() * 1000)


def test_stale_running_sweep_terminates_only_running_and_old(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fake `search_runs` returns a MIX of RUNNING-and-old, RUNNING-and-
    recent, and FINISHED runs -- as if the real filter's own `status=RUNNING`
    clause were somehow bypassed -- proving `_heal_stale_running_runs`
    re-checks `run.info.status` itself (never touching the already-FINISHED
    run) AND `run.info.start_time` (never touching the still-fresh RUNNING
    run), not just trusting the search filter string alone."""
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    run_old = _stale_candidate_run(client, experiment_id, start_time_ms=_hours_ago_ms(10))
    run_recent = _stale_candidate_run(client, experiment_id, start_time_ms=_hours_ago_ms(1))
    run_finished = _stale_candidate_run(
        client, experiment_id, start_time_ms=_hours_ago_ms(10), status="FINISHED"
    )

    fixed_page = PagedList([run_old, run_recent, run_finished], None)
    monkeypatch.setattr(client, "search_runs", lambda *args, **kwargs: fixed_page)

    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=(),
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )

    assert summary.stale_running_healed == 1
    assert client.get_run(run_old.info.run_id).info.status == "FINISHED"
    assert client.get_run(run_recent.info.run_id).info.status == "RUNNING"
    assert client.get_run(run_finished.info.run_id).info.status == "FINISHED"


def test_stale_running_sweep_paginates_across_multiple_pages(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_heal_stale_running_runs` must follow `page_token` until exhausted --
    simulated via 2 real stale runs split across 2 fake pages (mirrors
    test_backfill_traces.py's own precedent for the identical idiom)."""
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    run_a = _stale_candidate_run(client, experiment_id, start_time_ms=_hours_ago_ms(10))
    run_b = _stale_candidate_run(client, experiment_id, start_time_ms=_hours_ago_ms(20))

    call_tokens: list[str | None] = []

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
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=(),
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )

    assert call_tokens == [None, "page-2-token"]
    assert summary.stale_running_healed == 2
    assert client.get_run(run_a.info.run_id).info.status == "FINISHED"
    assert client.get_run(run_b.info.run_id).info.status == "FINISHED"


def test_stale_running_sweep_real_filter_heals_both_experiments(client: MlflowClient) -> None:
    """End-to-end, via the REAL (unmonkeypatched) MLflow search filter: one
    abandoned run in EACH production-usage experiment is healed by a single
    `categories=("jra",)` call (jra is RS-eligible, so both experiments are
    swept)."""
    fp_experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    rs_experiment_id = get_or_create_experiment(client, config.EXPERIMENT_RS_PRODUCTION_USAGE)
    fp_abandoned = _stale_candidate_run(client, fp_experiment_id, start_time_ms=_hours_ago_ms(10))
    rs_abandoned = _stale_candidate_run(client, rs_experiment_id, start_time_ms=_hours_ago_ms(10))

    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )

    assert summary.stale_running_healed == 2
    assert client.get_run(fp_abandoned.info.run_id).info.status == "FINISHED"
    assert client.get_run(rs_abandoned.info.run_id).info.status == "FINISHED"


def test_stale_running_sweep_does_not_create_rs_experiment_for_banei_only(
    client: MlflowClient,
) -> None:
    """category="banei" is FP-eligible but not RS-eligible -- the self-heal
    sweep must never touch (let alone create) the RS production-usage
    experiment for a banei-only call, mirroring this module's other
    `RS_CATEGORIES`-gated invariants."""
    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("banei",),
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert summary.stale_running_healed == 0
    assert client.get_experiment_by_name(config.EXPERIMENT_RS_PRODUCTION_USAGE) is None


def test_stale_running_sweep_skipped_when_repair_disabled(client: MlflowClient) -> None:
    """`repair_stale_running=False` must skip the sweep entirely, leaving an
    otherwise-eligible abandoned run untouched."""
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    abandoned = _stale_candidate_run(client, experiment_id, start_time_ms=_hours_ago_ms(10))

    summary = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=("jra",),
        repair_stale_running=False,
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )

    assert summary.stale_running_healed == 0
    assert client.get_run(abandoned.info.run_id).info.status == "RUNNING"


def test_stale_running_hours_configurable_threshold(client: MlflowClient) -> None:
    """The SAME 3-hour-old abandoned run is untouched under the default 6.0h
    threshold but healed under a lowered 2.0h threshold -- confirms
    --stale-running-hours genuinely changes the outcome."""
    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_PRODUCTION_USAGE)
    borderline = _stale_candidate_run(client, experiment_id, start_time_ms=_hours_ago_ms(3))

    lenient = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=(),
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert lenient.stale_running_healed == 0
    assert client.get_run(borderline.info.run_id).info.status == "RUNNING"

    strict = sync_production.sync_production_range(
        client,
        DATE_STR,
        DATE_STR,
        categories=(),
        stale_running_hours=2.0,
        neon_connect=lambda: FakeNeonConnection(),
        local_connect=lambda: FakeLocalConnection(),
    )
    assert strict.stale_running_healed == 1
    assert client.get_run(borderline.info.run_id).info.status == "FINISHED"


def test_stale_running_sweep_failure_is_isolated_and_recorded(
    client: MlflowClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient failure while healing EITHER side (FP or RS) must be
    isolated exactly like every other failure point in this function --
    recorded in `summary.errors`, never raised, and must not prevent the
    rest of this call's (date, category) sync from proceeding."""

    def _boom(*args: object, **kwargs: object) -> int:
        raise MlflowException("boom stale-running heal")

    monkeypatch.setattr(sync_production, "_heal_stale_running_runs", _boom)

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

    assert summary.stale_running_healed == 0
    assert summary.fp_runs_created == 1
    assert len(summary.errors) == 2
    assert any("stale-running-heal:finish-position" in e for e in summary.errors)
    assert any("stale-running-heal:running-style" in e for e in summary.errors)
    assert all("boom stale-running heal" in e for e in summary.errors)
