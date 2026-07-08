"""Tests for mlflow_tracking.champion_cell_eval.

Entirely hermetic: every Neon/local-replica connection used below is a
hand-built `_RoutedFakeConnection` (never a real psycopg2 connection, never
even a MagicMock standing in for one -- `_RoutedFakeConnection` implements
`db.ConnectionLike`/`db.CursorLike` structurally). The real `client` fixture
(isolated sqlite `MlflowClient`, see conftest.py) is used for the MLflow
side throughout, matching this package's existing convention -- except in
`test_get_model_version_failure_after_alias_resolves_treated_as_no_champion`,
which monkeypatches exactly one method on that same real client to simulate
a registry backend inconsistency that is empirically UNREACHABLE against the
real isolated-sqlite store (see that test's docstring and
champion_cell_eval._resolve_champion_model_version's own docstring).

The zero-champion-coverage case is exercised heavily and deliberately: as of
2026-07-08, 3 of the 5 registered champion model versions in production
(jra/nar/banei finish-position) are in exactly this state (never genuinely
served, or served only on dates with no finalized results yet) -- it is the
CURRENT REAL STATE of most of this system, not a rare edge case.
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
from mlflow.exceptions import MlflowException

from mlflow_tracking import champion_cell_eval, config, db, logging_api, registry, serve_eval

# ── Hermetic fake DB connections ─────────────────────────────────────────────

_RouteFn = Callable[[str, "tuple[object, ...]"], "list[tuple[object, ...]]"]


class _RoutedFakeCursor:
    """A cursor stand-in whose fetchall() result depends on which query was
    just executed, driven by a caller-supplied `route(query, params)`
    function -- necessarily richer than test_serve_eval.py's single-query
    `_make_mock_conn`, since one eval_champion_cells_for_category call issues
    several distinct queries (one prediction-rows range query, plus one
    race_results/race_metadata call PER DISTINCT champion-attributed race
    date) against the SAME connection object.
    """

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
    """A Neon stand-in: exactly one query type (the whole-window prediction-
    rows range query) is ever issued against it per eval call, so the route
    ignores the query text/params entirely and always returns the same
    canned rows."""
    return _RoutedFakeConnection(lambda _query, _params: prediction_rows)


def _make_local_conn(
    results_by_date: Mapping[str, Sequence[tuple[object, ...]]],
    meta_by_date: Mapping[str, Sequence[tuple[object, ...]]] | None = None,
) -> _RoutedFakeConnection:
    """A local-replica stand-in: routes by date (from the bound params) and
    by table family (jvd_ra/nvd_ra metadata queries vs. jvd_se/nvd_se result
    queries, distinguished by the "_ra" substring, which the "_se" table
    names never contain)."""
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


# ── Zero-champion-coverage (the CURRENT REAL STATE for 3 of 5 champions) ────


def test_zero_champion_coverage_produces_valid_empty_run(
    client: MlflowClient, tmp_path: Path
) -> None:
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )

    assert result.champion_model_version == "iter14"
    assert result.has_champion_coverage is False
    assert result.unit_count == 0
    assert result.cell_count == 0
    assert result.low_n_cell_count == 0
    assert result.reused_existing_run is False

    run = client.get_run(result.run_id)
    assert run.data.tags["has_champion_coverage"] == "false"
    assert run.data.tags["champion_model_version"] == "iter14"
    # Zero champion coverage is still a serve-regime answer (no genuinely-
    # served rows found), never a different eval_regime.
    assert run.data.tags["eval_regime"] == "serve"
    assert "best_cell_top1_pct" not in run.data.metrics
    assert "worst_cell_top1_pct" not in run.data.metrics
    assert run.data.metrics["unit_count"] == 0.0
    assert run.data.metrics["cell_count"] == 0.0

    artifact_paths = {a.path for a in client.list_artifacts(result.run_id)}
    assert logging_api.CELL_METRICS_JSON_ARTIFACT in artifact_paths
    assert logging_api.CELL_METRICS_PARQUET_ARTIFACT in artifact_paths


def test_running_style_champion_resolved_zero_rows(client: MlflowClient, tmp_path: Path) -> None:
    version = registry.register_version(
        client, "jra-running-style", f"file://{tmp_path}", tags={"model_version": "rsv3"}
    )
    registry.set_champion(client, "jra-running-style", version.version)

    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "jra",
        "running-style",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )

    assert result.champion_model_version == "rsv3"
    assert result.has_champion_coverage is False
    assert result.cell_count == 0


# ── No champion alias at all ─────────────────────────────────────────────────


def test_no_registered_model_returns_none_champion(client: MlflowClient) -> None:
    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "nar",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )
    assert result.champion_model_version is None
    assert result.has_champion_coverage is False
    run = client.get_run(result.run_id)
    assert run.data.tags["champion_model_version"] == "none"


def test_registered_model_without_alias_returns_none_champion(
    client: MlflowClient, tmp_path: Path
) -> None:
    registry.register_version(
        client, "nar-finish-position", f"file://{tmp_path}", tags={"model_version": "iter5"}
    )

    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "nar",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )

    assert result.champion_model_version is None
    version_after = client.get_model_version("nar-finish-position", "1")
    assert version_after.tags.get("latest_cell_eval_run_id") is None


def test_running_style_no_champion_alias_returns_none(client: MlflowClient) -> None:
    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "jra",
        "running-style",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )
    assert result.champion_model_version is None
    assert result.has_champion_coverage is False
    assert result.cell_count == 0


def test_get_model_version_failure_after_alias_resolves_treated_as_no_champion(
    client: MlflowClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Defense-in-depth branch: if the registry's alias->version mapping and
    its version table were ever inconsistent, get_model_version failing
    AFTER a real alias resolves must be treated the same as "no champion",
    not raise.

    Empirically confirmed unreachable via the real isolated-sqlite store
    used everywhere else in this suite: set_registered_model_alias itself
    validates the target version exists, and delete_model_version clears any
    alias pointing at the deleted version -- so this exact combination
    (a resolvable alias whose target version then fails to fetch) cannot be
    constructed against the real backend. This test therefore monkeypatches
    exactly ONE method on the otherwise-real `client` fixture to simulate a
    backend that does not enforce that invariant as strictly (e.g. a
    concurrent delete racing this read against a different store
    implementation), rather than mocking MlflowClient wholesale.
    """
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    def flaky_get_model_version(_name: str, _version: str) -> object:
        raise MlflowException("simulated registry inconsistency")

    monkeypatch.setattr(client, "get_model_version", flaky_get_model_version)

    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )
    assert result.champion_model_version is None
    assert result.has_champion_coverage is False


# ── Idempotency ───────────────────────────────────────────────────────────


def test_idempotent_reuse_skips_db_work_and_preserves_summary(
    client: MlflowClient, tmp_path: Path
) -> None:
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)
    neon_connect = _neon_factory([])
    local_connect = _local_factory({})

    result1 = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    assert neon_connect.call_count == 1
    assert local_connect.call_count == 1
    assert result1.reused_existing_run is False

    result2 = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    assert neon_connect.call_count == 1
    assert local_connect.call_count == 1
    assert result2.reused_existing_run is True
    assert result2.run_id == result1.run_id
    assert result2.champion_model_version == "iter14"
    assert result2.has_champion_coverage is False
    assert result2.unit_count == 0
    assert result2.cell_count == 0


def test_idempotent_reuse_preserves_none_champion_tag(client: MlflowClient) -> None:
    neon_connect = _neon_factory([])
    local_connect = _local_factory({})

    champion_cell_eval.eval_champion_cells_for_category(
        client,
        "banei",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    result2 = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "banei",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=neon_connect,
        local_connect=local_connect,
    )
    assert neon_connect.call_count == 1
    assert result2.reused_existing_run is True
    assert result2.champion_model_version is None


# ── Real coverage: hand-verified per-cell/hit-rate math ─────────────────────


def test_real_coverage_finish_position_hand_verified_math(
    client: MlflowClient, tmp_path: Path
) -> None:
    """4 evaluated races across 2 dates and 2 cells (plus one noise row
    under a different model_version, which must be filtered out entirely).

    Cell A (venue=05, class=A, sprint/summer/turf/medium), 3 races:
      race1 pairs=[(1,1),(2,2)]      -> top1=1 place2=1 place3=1 fuku2p=1 top3box=0
      race2 pairs=[(1,3),(2,1)]      -> top1=0 place2=0 place3=1 fuku2p=1 top3box=0
      race3 pairs=[(1,1)]            -> top1=1 place2=1 place3=1 fuku2p=1 top3box=0
      => race_count=3, top1=66.667% place2=66.667% place3=100% fuku2p=100% top3box=0%
    Cell B (venue=10, class=B, mile/summer/dirt/medium), 1 race:
      race4 pairs=[(1,2)]            -> top1=0 place2=1 place3=1 fuku2p=1 top3box=0
      => race_count=1, top1=0% place2=100% place3=100% fuku2p=100% top3box=0%

    Overall (race-count-weighted, 4 races total): top1=50% place2=75%
    place3=100% fuku2p=100% top3box=0%. best_cell_top1_pct=200/3 (cell A),
    worst_cell_top1_pct=0 (cell B). min_cell_count=2 flags cell B (n=1) as
    low_n but not cell A (n=3).
    """
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    gen_0601 = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    gen_0602 = datetime(2026, 6, 2, 3, 0, 0, tzinfo=UTC)

    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1A", "iter14", 1, gen_0601, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("05", "01", "H1B", "iter14", 2, gen_0601, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("05", "02", "H2A", "iter14", 1, gen_0601, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("05", "02", "H2B", "iter14", 2, gen_0601, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
        ("05", "01", "H3A", "iter14", 1, gen_0602, "sprint", "medium", "summer", "A", "turf",
         "2026", "0602"),
        ("10", "01", "H4A", "iter14", 1, gen_0602, "mile", "medium", "summer", "B", "dirt",
         "2026", "0602"),
        # Noise: a different model_version -- must never be counted.
        ("05", "03", "NX", "iter99", 1, gen_0601, "sprint", "medium", "summer", "A", "turf",
         "2026", "0601"),
    ]
    results_by_date = {
        "20260601": [
            ("05", "01", "H1A", "1", "01"),
            ("05", "01", "H1B", "2", "02"),
            ("05", "02", "H2A", "3", "03"),
            ("05", "02", "H2B", "1", "01"),
            ("05", "03", "NX", "1", "01"),
        ],
        "20260602": [
            ("05", "01", "H3A", "1", "01"),
            ("10", "01", "H4A", "2", "02"),
        ],
    }

    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        min_cell_count=2,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )

    assert result.champion_model_version == "iter14"
    assert result.has_champion_coverage is True
    assert result.unit_count == 4
    assert result.cell_count == 2
    assert result.low_n_cell_count == 1

    run = client.get_run(result.run_id)
    assert run.data.tags["eval_regime"] == "serve"

    metrics = run.data.metrics
    assert metrics["overall_top1_pct"] == pytest.approx(50.0)
    assert metrics["overall_place2_pct"] == pytest.approx(75.0)
    assert metrics["overall_place3_pct"] == pytest.approx(100.0)
    assert metrics["overall_fukusho_2p_pct"] == pytest.approx(100.0)
    assert metrics["overall_top3_box_pct"] == pytest.approx(0.0)
    assert metrics["best_cell_top1_pct"] == pytest.approx(200.0 / 3.0)
    assert metrics["worst_cell_top1_pct"] == pytest.approx(0.0)
    assert metrics["unit_count"] == 4.0
    assert metrics["cell_count"] == 2.0
    assert metrics["low_n_cell_count"] == 1.0


def test_real_coverage_running_style_hand_verified_math(
    client: MlflowClient, tmp_path: Path
) -> None:
    """3 evaluated horses across 2 dates and 2 cells (plus one noise row
    under a different model_version).

    Cell (venue=05, class=A, sprint, turf), 2 horses:
      H1: corner1_norm=(5-1)/(10-1)=0.444 -> actual=sashi; predicted=sashi -> hit
      H2: corner1_norm=(1-1)/(10-1)=0.0   -> actual=nige;  predicted=senkou -> miss
      => horse_count=2, accuracy=50%
    Cell (venue=10, class=B, mile, dirt), 1 horse:
      H3: corner1_norm=(8-1)/(8-1)=1.0    -> actual=oikomi; predicted=oikomi -> hit
      => horse_count=1, accuracy=100%

    Overall (horse-count-weighted, 3 horses): accuracy=2/3=66.667%.
    best_cell=100% (venue10 cell), worst_cell=50% (venue05 cell).
    min_cell_count=2 flags the venue10 cell (n=1) as low_n.
    """
    version = registry.register_version(
        client, "jra-running-style", f"file://{tmp_path}", tags={"model_version": "rsv3"}
    )
    registry.set_champion(client, "jra-running-style", version.version)

    gen_0601 = datetime(2026, 6, 1, 3, 0, 0, tzinfo=UTC)
    gen_0602 = datetime(2026, 6, 2, 3, 0, 0, tzinfo=UTC)

    prediction_rows: list[tuple[object, ...]] = [
        ("05", "01", "H1", "rsv3", serve_eval.RS_CLASS_SASHI, "sashi", gen_0601, "2026", "0601"),
        ("05", "01", "H2", "rsv3", serve_eval.RS_CLASS_SENKOU, "senkou", gen_0602, "2026", "0602"),
        ("10", "01", "H3", "rsv3", serve_eval.RS_CLASS_OIKOMI, "oikomi", gen_0602, "2026", "0602"),
        ("05", "02", "NX", "other-v", serve_eval.RS_CLASS_NIGE, "nige", gen_0601, "2026", "0601"),
    ]
    results_by_date = {
        "20260601": [("05", "01", "H1", "5", "05"), ("05", "02", "NX", "1", "01")],
        "20260602": [("05", "01", "H2", "3", "01"), ("10", "01", "H3", "2", "08")],
    }
    meta_by_date = {
        "20260601": [("05", "01", 1200, 10, "10", "A")],
        "20260602": [("05", "01", 1200, 10, "10", "A"), ("10", "01", 1600, 8, "24", "B")],
    }

    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "jra",
        "running-style",
        as_of=_AS_OF,
        min_cell_count=2,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date, meta_by_date),
    )

    assert result.champion_model_version == "rsv3"
    assert result.has_champion_coverage is True
    assert result.unit_count == 3
    assert result.cell_count == 2
    assert result.low_n_cell_count == 1

    metrics = client.get_run(result.run_id).data.metrics
    assert metrics["overall_accuracy_pct"] == pytest.approx(200.0 / 3.0)
    assert metrics["best_cell_accuracy_pct"] == pytest.approx(100.0)
    assert metrics["worst_cell_accuracy_pct"] == pytest.approx(50.0)


# ── Ban-ei partitioning ──────────────────────────────────────────────────────


def test_banei_partitioning_keeps_only_keibajo_83(client: MlflowClient, tmp_path: Path) -> None:
    version = registry.register_version(
        client, "banei-finish-position", f"file://{tmp_path}", tags={"model_version": "banei-v1"}
    )
    registry.set_champion(client, "banei-finish-position", version.version)

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

    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "banei",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.has_champion_coverage is True
    assert result.unit_count == 1


def test_nar_partitioning_excludes_banei_rows(client: MlflowClient, tmp_path: Path) -> None:
    version = registry.register_version(
        client, "nar-finish-position", f"file://{tmp_path}", tags={"model_version": "nar-v1"}
    )
    registry.set_champion(client, "nar-finish-position", version.version)

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

    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "nar",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=_neon_factory(prediction_rows),
        local_connect=_local_factory(results_by_date),
    )
    assert result.has_champion_coverage is True
    assert result.unit_count == 1


# ── RS excludes Ban-ei ────────────────────────────────────────────────────


def test_eval_champion_cells_excludes_running_style_for_banei(client: MlflowClient) -> None:
    results = champion_cell_eval.eval_champion_cells(
        client,
        categories=("banei",),
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )
    assert len(results) == 1
    assert results[0].category == "banei"
    assert results[0].task == "finish-position"
    assert client.get_experiment_by_name(config.EXPERIMENT_RS_CHAMPION_EVAL) is None


# ── Validation ────────────────────────────────────────────────────────────


def test_running_style_banei_direct_call_raises_value_error(client: MlflowClient) -> None:
    neon_connect = _neon_factory([])
    local_connect = _local_factory({})
    with pytest.raises(ValueError, match="does not support category=banei"):
        champion_cell_eval.eval_champion_cells_for_category(
            client,
            "banei",
            "running-style",
            neon_connect=neon_connect,
            local_connect=local_connect,
        )
    neon_connect.assert_not_called()
    local_connect.assert_not_called()


def test_invalid_task_raises_value_error(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="task must be"):
        champion_cell_eval.eval_champion_cells_for_category(client, "jra", "bogus-task")


def test_invalid_category_raises_value_error(client: MlflowClient) -> None:
    with pytest.raises(ValueError, match="unsupported category"):
        champion_cell_eval.eval_champion_cells_for_category(client, "keirin", "finish-position")


# ── Cross-navigation tag ──────────────────────────────────────────────────


def test_cross_navigation_tag_set_on_real_champion_version(
    client: MlflowClient, tmp_path: Path
) -> None:
    version = registry.register_version(
        client, "jra-finish-position", f"file://{tmp_path}", tags={"model_version": "iter14"}
    )
    registry.set_champion(client, "jra-finish-position", version.version)

    result = champion_cell_eval.eval_champion_cells_for_category(
        client,
        "jra",
        "finish-position",
        as_of=_AS_OF,
        neon_connect=_neon_factory([]),
        local_connect=_local_factory({}),
    )

    updated = client.get_model_version("jra-finish-position", version.version)
    assert updated.tags.get("latest_cell_eval_run_id") == result.run_id


# ── Per-category failure isolation ───────────────────────────────────────


def test_eval_champion_cells_isolates_per_category_failures(client: MlflowClient) -> None:
    call_state = {"n": 0}

    def flaky_neon_connect() -> db.ConnectionLike:
        call_state["n"] += 1
        if call_state["n"] == 1:
            raise psycopg2.OperationalError("boom")
        return _make_neon_conn([])

    neon_connect = MagicMock(side_effect=flaky_neon_connect)
    local_connect = _local_factory({})

    with pytest.warns(UserWarning, match="jra"):
        results = champion_cell_eval.eval_champion_cells(
            client,
            categories=("jra", "nar"),
            neon_connect=neon_connect,
            local_connect=local_connect,
        )

    # 4 pairs attempted (jra/nar x finish-position/running-style), 1 fails.
    assert len(results) == 3
    assert not any(r.category == "jra" and r.task == "finish-position" for r in results)


