"""Tests for mlflow_tracking.cf_serving_recorder.

Entirely hermetic: `FakeNeonConnection`/`FakeLocalConnection` below are
hand-built fakes (never a real psycopg2 connection), mirroring test_sync_
production.py's own fake-connection convention exactly. The `client` fixture
(from conftest.py) is a REAL `MlflowClient` backed by an isolated sqlite
tracking store -- exercising real run/experiment/table/artifact/tracing code
against a throwaway store is correct and intended here.

Every test below drives the module ONLY through its PUBLIC surface
(`record_cf_serving_day`, `record_cf_serving_range`, `main`, and the public
`RACE_TABLE_PARQUET_ARTIFACT` constant) -- never a `_`-prefixed helper
directly, matching this package's established convention (see test_serve_
eval.py's own module docstring: "Private helpers ... are never called
directly from here ... they are exercised through [the public functions
that call them] instead"). Some scenarios below (e.g. a corrupt previous-
snapshot parquet, a run created but never logged a table) are engineered via
two/three sequential `record_cf_serving_day` calls plus `pytest.MonkeyPatch.
context()` scoped narrowly around `pandas.read_parquet`, rather than calling
the private read-back helper directly.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from types import ModuleType
from typing import Protocol

import pandas as pd
import psycopg2
import pytest
from mlflow import MlflowClient

from mlflow_tracking import cf_serving_recorder, config, registry, serve_eval
from mlflow_tracking.logging_api import get_or_create_experiment

DATE_STR: str = "20260614"
GEN_AT: datetime = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)


# ── Fake connections ─────────────────────────────────────────────────────────


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
        if "fetch_post_times: jra" in query:
            self._pending = self._conn.post_time_rows.get(("jra", date_str), [])
        elif "fetch_post_times: nar/banei" in query:
            self._pending = self._conn.post_time_rows.get(("nar_banei", date_str), [])
        elif "fetch_races_scheduled: jra" in query:
            self._pending = self._conn.race_calendar_rows.get(("jra", date_str), [])
        elif "fetch_races_scheduled: nar" in query:
            self._pending = self._conn.race_calendar_rows.get(("nar", date_str), [])
        elif "fetch_routing_race_conditions: jra" in query:
            self._pending = self._conn.routing_condition_rows.get(("jra", date_str), [])
        elif "fetch_routing_race_conditions: nar/banei" in query:
            self._pending = self._conn.routing_condition_rows.get(("nar_banei", date_str), [])
        elif "race_bango FROM nvd_ra" in query:
            self._pending = self._conn.race_calendar_rows.get(("banei", date_str), [])
        else:
            self._pending = []

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._pending


class FakeLocalConnection:
    race_calendar_rows: dict[tuple[str, str], list[tuple[object, ...]]]
    post_time_rows: dict[tuple[str, str], list[tuple[object, ...]]]
    routing_condition_rows: dict[tuple[str, str], list[tuple[object, ...]]]
    raise_for: frozenset[str]
    closed: bool

    def __init__(
        self,
        race_calendar_rows: dict[tuple[str, str], list[tuple[object, ...]]] | None = None,
        post_time_rows: dict[tuple[str, str], list[tuple[object, ...]]] | None = None,
        routing_condition_rows: dict[tuple[str, str], list[tuple[object, ...]]] | None = None,
        raise_for: frozenset[str] = frozenset(),
    ) -> None:
        self.race_calendar_rows = race_calendar_rows or {}
        self.post_time_rows = post_time_rows or {}
        self.routing_condition_rows = routing_condition_rows or {}
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


def _set_champion(client: MlflowClient, category: registry.Category, model_version: str) -> None:
    name = registry.registered_model_name(category, "finish-position")
    client.create_registered_model(name)
    mv = client.create_model_version(name, source="file:///tmp/model", run_id=None)
    client.set_model_version_tag(name, mv.version, "model_version", model_version)
    client.set_registered_model_alias(name, "champion", mv.version)


def _race_table(client: MlflowClient, run_id: str) -> pd.DataFrame:
    return pd.read_parquet(
        client.download_artifacts(run_id, cf_serving_recorder.RACE_TABLE_PARQUET_ARTIFACT)
    )


def _routing_row(
    keibajo_code: str,
    race_bango: str,
    *,
    kyori: str = "1000",
    shusso_tosu: str = "10",
    track_code: str = "24",
    kyoso_joken_code: str = "703",
    grade_code: str = " ",
) -> tuple[object, ...]:
    """One row matching cf_serving_recorder._ROUTING_RACE_CONDITION_COLUMNS
    exactly (keibajo_code, race_bango, kyori, shusso_tosu, track_code,
    kyoso_joken_code, grade_code)."""
    return (keibajo_code, race_bango, kyori, shusso_tosu, track_code, kyoso_joken_code, grade_code)


# Recomputed independently from config.REPO_ROOT (public) rather than
# reading cf_serving_recorder._CONTAINER_SRC (private) -- this file's own
# module docstring establishes "never touch a `_`-prefixed helper directly",
# and this small path constant is cheap enough to duplicate rather than
# reach past that boundary for.
_CONTAINER_SRC_FOR_TESTS: str = str(
    config.REPO_ROOT / "apps" / "finish-position-predict-container" / "src"
)


# Mirrors cf_serving_recorder._VariantSpecLike/_CategoryRoutingLike/
# _CellRouterLike EXACTLY, as OWN local copies (never importing those private
# names -- basedpyright's reportPrivateUsage forbids that regardless of
# whether the private name is only ever used for a type annotation).
# Declared via READ-ONLY `@property` rather than plain attributes: two
# independently-declared Protocols with a plain (read-write) attribute are
# NOT always mutually assignable under basedpyright's invariance rules for
# mutable Protocol members, even when the attribute's own type is
# structurally identical -- properties are covariant, which is all a
# read-only fixture like this one ever needs.
class _RouterVariantSpec(Protocol):
    @property
    def model_version(self) -> str: ...


class _RouterCategoryRouting(Protocol):
    @property
    def default_variant(self) -> str: ...
    @property
    def variants(self) -> Mapping[str, _RouterVariantSpec]: ...


class _RouterLike(Protocol):
    def has_routing(self, category: str) -> bool: ...

    def routing_for(self, category: str) -> _RouterCategoryRouting: ...

    def resolve_variant(
        self,
        category: str,
        entries: Sequence[Mapping[str, object]],
        card_max_race_bango: int | None = None,
    ) -> str: ...


def _real_cell_router_module() -> ModuleType:
    """Import the REAL predict_lib.cell_router module the same way
    cf_serving_recorder._load_cell_router does, so router fixtures below are
    built from the actual dataclasses (CellRouter/CategoryRouting/
    CellRouteRule/CellCondition/VariantSpec) rather than a hand-rolled copy
    that could silently drift from the real shape. Typed `ModuleType`
    (typeshed's own stub gives `ModuleType.__getattr__ -> Any`) rather than
    `object`, so every dynamic attribute access below type-checks as `Any`
    instead of failing `reportAttributeAccessIssue`."""
    if _CONTAINER_SRC_FOR_TESTS not in sys.path:
        sys.path.insert(0, _CONTAINER_SRC_FOR_TESTS)
    return importlib.import_module("predict_lib.cell_router")


def _make_router(
    routing: dict[str, tuple[str, dict[str, str], list[tuple[list[tuple[str, list[str]]], str]]]],
) -> _RouterLike:
    """Build a REAL predict_lib.cell_router.CellRouter from a compact
    `{category: (default_variant, {variant_name: model_version}, [([(dimension,
    values)], variant_name)])}` spec, for injection via record_cf_serving_day's
    `cell_router=` parameter (the same DI convention this file's own
    `neon_conn`/`local_conn` fakes already use, never monkeypatching this
    module's own private helper) -- keeps every new routing-honor test
    hermetic (a controlled routing table, never the ever-changing real
    cell_routing.json) while still exercising the REAL resolve_variant/
    all_conditions_match rule-matching logic, per this module's own mandate
    not to re-derive a parallel copy of it. The `# type: ignore`-free dynamic
    attribute access below relies on this package's own basedpyright
    settings (reportUnknownMemberType/reportUnknownVariableType = "none"),
    the same tolerance cf_serving_recorder.py's own `_load_cell_router`
    docstring documents -- no suppression comment is needed or added."""
    cr = _real_cell_router_module()
    category_routings: dict[str, object] = {}
    for category, (default_variant, variants, rules) in routing.items():
        variant_specs = {
            name: cr.VariantSpec(model_version=mv, feature_count=1, architecture="catboost")
            for name, mv in variants.items()
        }
        rule_objs = tuple(
            cr.CellRouteRule(
                conditions=tuple(
                    cr.CellCondition(dimension=dim, values=frozenset(values))
                    for dim, values in conditions
                ),
                variant=variant_name,
            )
            for conditions, variant_name in rules
        )
        category_routings[category] = cr.CategoryRouting(
            default_variant=default_variant, variants=variant_specs, rules=rule_objs
        )
    return cr.CellRouter(category_routings)


# ── record_cf_serving_day: basics ────────────────────────────────────────────


def test_record_cf_serving_day_unknown_category_raises_before_any_work(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection()
    local = FakeLocalConnection()
    with pytest.raises(ValueError, match="unknown category"):
        cf_serving_recorder.record_cf_serving_day(
            client, "bogus", DATE_STR, neon_conn=neon, local_conn=local
        )


def test_record_cf_serving_day_creates_run_computes_coverage_and_routing(
    client: MlflowClient,
) -> None:
    _set_champion(client, "jra", "iter14")
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 1),
                _fp_row("05", "01", "H2", "iter14", 2),
                _fp_row("05", "02", "H3", "iter14-variant", 1),
                _fp_row("05", "04", "H4", "iter20-unrelated", 1),
            ]
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",), ("02",), ("03",), ("04",)]},
        post_time_rows={("jra", DATE_STR): [("05", "01", "0900"), ("05", "02", "0930")]},
    )

    now = GEN_AT + timedelta(days=1)
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, now=now
    )

    assert result.run_created is True
    assert result.races_expected == 4
    assert result.races_covered == 3
    assert result.coverage_pct == pytest.approx(300.0 / 4.0)
    assert result.champion_races == 1
    assert result.variant_races == 1
    assert result.other_races == 1
    assert result.races_with_post_time == 2
    assert result.first_write == GEN_AT
    assert result.last_write == GEN_AT

    run = client.get_run(result.run_id)
    assert run.info.status == "FINISHED"
    assert run.data.metrics["cf_races_covered"] == 3.0

    table = _race_table(client, result.run_id)
    assert len(table) == 3
    assert set(table["champion_match"]) == {"champion", "variant", "other"}


def test_record_cf_serving_day_champion_prefix_without_dash_is_other(
    client: MlflowClient,
) -> None:
    """served="iter140" must not be misclassified as a variant of champion
    "iter14" -- the routing-scope suffix is always "-"-delimited."""
    _set_champion(client, "jra", "iter14")
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter140", 1)]}
    )
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [("01",)]})
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.champion_races == 0
    assert result.variant_races == 0
    assert result.other_races == 1


def test_record_cf_serving_day_no_champion_registered_all_other(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "some-model", 1)]}
    )
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [("01",)]})
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.champion_races == 0
    assert result.other_races == 1


def test_record_cf_serving_day_registered_model_no_champion_alias(
    client: MlflowClient,
) -> None:
    name = registry.registered_model_name("jra", "finish-position")
    client.create_registered_model(name)
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "some-model", 1)]}
    )
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [("01",)]})
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.other_races == 1


def test_record_cf_serving_day_no_scheduled_races_coverage_pct_none(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection()
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): []})
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.races_expected == 0
    assert result.races_covered == 0
    assert result.coverage_pct is None
    assert result.first_write is None
    assert result.last_write is None
    table = _race_table(client, result.run_id)
    assert len(table) == 0


def test_record_cf_serving_day_nar_excludes_banei_races(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("nar", DATE_STR): [
                _fp_row("30", "01", "H1", "nar-model", 1),
                _fp_row(serve_eval.BANEI_KEIBAJO_CODE, "02", "H2", "banei-model", 1),
            ]
        }
    )
    local = FakeLocalConnection(race_calendar_rows={("nar", DATE_STR): [("01",)]})
    result = cf_serving_recorder.record_cf_serving_day(
        client, "nar", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.races_covered == 1


def test_record_cf_serving_day_banei_keeps_only_banei_races(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("nar", DATE_STR): [
                _fp_row("30", "01", "H1", "nar-model", 1),
                _fp_row(serve_eval.BANEI_KEIBAJO_CODE, "02", "H2", "banei-model", 1),
            ]
        }
    )
    local = FakeLocalConnection(race_calendar_rows={("banei", DATE_STR): [("02",)]})
    result = cf_serving_recorder.record_cf_serving_day(
        client, "banei", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.races_covered == 1


# ── write_spread / partial writes ────────────────────────────────────────────


def test_record_cf_serving_day_write_spread_and_partial_write_flagged(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 1, GEN_AT),
                _fp_row("05", "01", "H2", "iter14", 2, GEN_AT + timedelta(seconds=30)),
            ]
        }
    )
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [("01",)]})
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.partial_write_race_count == 1
    table = _race_table(client, result.run_id)
    assert table.iloc[0]["horse_count"] == 2
    assert table.iloc[0]["write_spread_seconds"] == pytest.approx(30.0)


def test_record_cf_serving_day_small_write_spread_not_partial(client: MlflowClient) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 1, GEN_AT),
                _fp_row("05", "01", "H2", "iter14", 2, GEN_AT + timedelta(seconds=1)),
            ]
        }
    )
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [("01",)]})
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.partial_write_race_count == 0


# ── late-write / post-time ───────────────────────────────────────────────────


def test_record_cf_serving_day_write_after_post_time_is_late(client: MlflowClient) -> None:
    # post 10:15 JST == 01:15 UTC; prediction written at 01:30 UTC -> after post.
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 1, datetime(2026, 6, 14, 1, 30, 0, tzinfo=UTC))
            ]
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        post_time_rows={("jra", DATE_STR): [("05", "01", "1015")]},
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.late_race_count == 1
    table = _race_table(client, result.run_id)
    assert bool(table.iloc[0]["late_write"]) is True
    assert table.iloc[0]["lead_minutes"] < 0


def test_record_cf_serving_day_write_well_before_post_time_not_late(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 1, datetime(2026, 6, 13, 1, 0, 0, tzinfo=UTC))
            ]
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        post_time_rows={("jra", DATE_STR): [("05", "01", "1015")]},
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.late_race_count == 0
    table = _race_table(client, result.run_id)
    assert bool(table.iloc[0]["late_write"]) is False
    assert table.iloc[0]["lead_minutes"] > 60.0


def test_record_cf_serving_day_malformed_post_times_never_crash_and_are_not_late(
    client: MlflowClient,
) -> None:
    """Exercises every _parse_hhmm failure shape (wrong length, non-digit,
    hour out of range, minute out of range) via genuinely malformed
    hasso_jikoku values a real jvd_ra/nvd_ra row could contain -- all four
    must resolve to "no post time available" (lead_minutes None, never
    late), never raise."""
    malformed = {
        "01": "161",  # wrong length
        "02": "16ab",  # non-digit
        "03": "2401",  # hour out of range
        "04": "1660",  # minute out of range
    }
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", race_bango, f"H{race_bango}", "iter14", 1) for race_bango in malformed
            ]
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [(rb,) for rb in malformed]},
        post_time_rows={("jra", DATE_STR): [("05", rb, hhmm) for rb, hhmm in malformed.items()]},
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.late_race_count == 0
    table = _race_table(client, result.run_id)
    assert len(table) == 4
    assert bool(table["lead_minutes"].isna().all())
    assert not bool(table["late_write"].any())


def test_record_cf_serving_day_missing_before_post_skips_unparseable_post_time(
    client: MlflowClient,
) -> None:
    """An UNCOVERED race (no prediction row at all) whose own post time is
    unparseable must be skipped, not counted -- there is no deadline to
    compare against without a valid post time."""
    neon = FakeNeonConnection()
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        post_time_rows={("jra", DATE_STR): [("05", "01", "9999")]},
    )
    now = datetime(2026, 6, 14, 12, 0, 0, tzinfo=UTC)
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, now=now
    )
    assert result.missing_before_post_count == 0


def test_record_cf_serving_day_missing_before_post_when_deadline_passed(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection()
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        post_time_rows={("jra", DATE_STR): [("05", "01", "1015")]},
    )
    # post 10:15 JST == 01:15 UTC; now is well past the 60-minute deadline.
    now = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, now=now
    )
    assert result.missing_before_post_count == 1


def test_record_cf_serving_day_missing_before_post_not_yet_due(client: MlflowClient) -> None:
    neon = FakeNeonConnection()
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        post_time_rows={("jra", DATE_STR): [("05", "01", "1015")]},
    )
    now = datetime(2026, 6, 13, 12, 0, 0, tzinfo=UTC)
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, now=now
    )
    assert result.missing_before_post_count == 0


def test_record_cf_serving_day_missing_before_post_skips_covered_race(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        post_time_rows={("jra", DATE_STR): [("05", "01", "1015")]},
    )
    now = datetime(2026, 6, 14, 3, 0, 0, tzinfo=UTC)
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, now=now
    )
    assert result.missing_before_post_count == 0


# ── batch-burst signature ─────────────────────────────────────────────────────


def test_record_cf_serving_day_batch_burst_detected_above_threshold(
    client: MlflowClient,
) -> None:
    base = GEN_AT
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", str(i), f"H{i}", "iter14", 1, base + timedelta(seconds=i))
                for i in range(5)
            ]
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [(str(i),) for i in range(5)]}
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.batch_burst_minute_count == 1
    assert result.batch_burst_race_count == 5


def test_record_cf_serving_day_no_burst_at_exactly_four_races(client: MlflowClient) -> None:
    base = GEN_AT
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", str(i), f"H{i}", "iter14", 1, base + timedelta(seconds=i))
                for i in range(4)
            ]
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [(str(i),) for i in range(4)]}
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.batch_burst_minute_count == 0
    assert result.batch_burst_race_count == 0


def test_record_cf_serving_day_burst_bucketing_uses_max_write_across_groups(
    client: MlflowClient,
) -> None:
    """One race served under TWO model_version groups (e.g. champion + a
    cell-routed variant) where the EARLIER-encountered group has the LATER
    timestamp and the later-encountered group has an EARLIER one -- the
    per-race bucket must land on the genuinely later minute. Proven the same
    way as the burst-threshold tests above: 4 OTHER races share that later
    minute (exactly at the threshold on their own), and this race's later
    group is what tips the count into a genuine burst."""
    later = GEN_AT + timedelta(minutes=5)
    rows = [_fp_row("05", str(i), f"H{i}", "iter14", 1, later) for i in range(4)]
    rows.append(_fp_row("05", "99", "H99a", "iter14", 1, later))
    rows.append(_fp_row("05", "99", "H99b", "iter14-variant", 1, GEN_AT))
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): rows})
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [(str(i),) for i in [*range(4), 99]]}
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert result.batch_burst_minute_count == 1
    assert result.batch_burst_race_count == 5


# ── rewrite tracking across invocations ──────────────────────────────────────


def test_record_cf_serving_day_reuses_run_and_tracks_rewrites(client: MlflowClient) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [("01",)]})

    first = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert first.run_created is True
    table1 = _race_table(client, first.run_id)
    assert int(table1.iloc[0]["rewrite_count"]) == 0

    rewritten_at = GEN_AT + timedelta(minutes=10)
    neon2 = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1, rewritten_at)]}
    )
    second = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon2, local_conn=local
    )
    assert second.run_created is False
    assert second.run_id == first.run_id
    table2 = _race_table(client, second.run_id)
    assert int(table2.iloc[0]["rewrite_count"]) == 1

    # a third call with the SAME (unchanged) timestamp must not increment again.
    third = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon2, local_conn=local
    )
    table3 = _race_table(client, third.run_id)
    assert int(table3.iloc[0]["rewrite_count"]) == 1


def test_record_cf_serving_day_missing_previous_artifact_is_treated_as_fresh(
    client: MlflowClient,
) -> None:
    """A run can exist (found by tag) without ever having logged a race
    table -- e.g. a prior call that failed before reaching `_log_race_table`.
    The next call must not crash, and must treat every race as never-before-
    seen (rewrite_count starts at 0)."""
    failing_neon = FakeNeonConnection(raise_for=frozenset({DATE_STR}))
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [("01",)]})
    with pytest.raises(psycopg2.OperationalError):
        cf_serving_recorder.record_cf_serving_day(
            client, "jra", DATE_STR, neon_conn=failing_neon, local_conn=local
        )

    working_neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=working_neon, local_conn=local
    )
    assert result.run_created is False
    table = _race_table(client, result.run_id)
    assert int(table.iloc[0]["rewrite_count"]) == 0
    assert client.get_run(result.run_id).info.status == "FINISHED"


def test_record_cf_serving_day_corrupt_previous_snapshot_is_treated_as_fresh(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [("01",)]})
    first = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local
    )
    assert first.run_created is True

    def _raise_parquet(*_args: object, **_kwargs: object) -> pd.DataFrame:
        raise ValueError("corrupt parquet")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(pd, "read_parquet", _raise_parquet)
        rewritten_at = GEN_AT + timedelta(minutes=10)
        neon2 = FakeNeonConnection(
            fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1, rewritten_at)]}
        )
        second = cf_serving_recorder.record_cf_serving_day(
            client, "jra", DATE_STR, neon_conn=neon2, local_conn=local
        )

    table2 = _race_table(client, second.run_id)
    assert int(table2.iloc[0]["rewrite_count"]) == 0


# ── isolated failure ──────────────────────────────────────────────────────────


def test_record_cf_serving_day_isolated_failure_marks_run_failed_and_reraises(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection(raise_for=frozenset({DATE_STR}))
    local = FakeLocalConnection()
    with pytest.raises(psycopg2.OperationalError):
        cf_serving_recorder.record_cf_serving_day(
            client, "jra", DATE_STR, neon_conn=neon, local_conn=local
        )

    experiment_id = get_or_create_experiment(client, config.EXPERIMENT_FP_CF_SERVING)
    matches = client.search_runs(
        [experiment_id],
        filter_string=f"tags.{cf_serving_recorder.CF_SERVING_KEY_TAG} = '{DATE_STR}:jra'",
        max_results=1,
    )
    assert len(matches) == 1
    assert matches[0].info.status == "FAILED"


# ── record_cf_serving_range ───────────────────────────────────────────────────


def test_record_cf_serving_range_processes_all_pairs_and_closes_connections(
    client: MlflowClient,
) -> None:
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", "20260614"): [_fp_row("05", "01", "H1", "iter14", 1)],
            ("nar", "20260615"): [_fp_row("30", "01", "H1", "nar-model", 1, kaisai_tsukihi="0615")],
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={
            ("jra", "20260614"): [("01",)],
            ("nar", "20260615"): [("01",)],
        }
    )

    summary = cf_serving_recorder.record_cf_serving_range(
        client,
        ["jra", "nar"],
        "20260614",
        "20260615",
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )

    # 2 dates x 2 categories = 4 pairs. Every pair gets its own run, even the
    # two with zero rows that day (a dark (date, category) pair is exactly
    # what this recorder must be able to surface, not silently skip).
    assert summary.pairs_processed == 4
    assert summary.runs_created == 4
    assert summary.runs_reused == 0
    assert len(summary.results) == 4
    assert summary.errors == []
    assert neon.closed is True
    assert local.closed is True


def test_record_cf_serving_range_reuses_runs_on_second_call(client: MlflowClient) -> None:
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(race_calendar_rows={("jra", DATE_STR): [("01",)]})

    first = cf_serving_recorder.record_cf_serving_range(
        client,
        ["jra"],
        DATE_STR,
        DATE_STR,
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert first.runs_created == 1
    assert first.runs_reused == 0

    second = cf_serving_recorder.record_cf_serving_range(
        client,
        ["jra"],
        DATE_STR,
        DATE_STR,
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )
    assert second.runs_created == 0
    assert second.runs_reused == 1


def test_record_cf_serving_range_isolates_one_bad_pair(client: MlflowClient) -> None:
    neon = FakeNeonConnection(raise_for=frozenset({DATE_STR}))
    local = FakeLocalConnection()

    summary = cf_serving_recorder.record_cf_serving_range(
        client,
        ["jra"],
        DATE_STR,
        DATE_STR,
        neon_connect=lambda: neon,
        local_connect=lambda: local,
    )

    assert summary.pairs_processed == 1
    assert len(summary.errors) == 1
    assert "jra" in summary.errors[0]
    assert summary.results == []


# ── main() ────────────────────────────────────────────────────────────────────


def test_main_success_returns_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_range(
        client: object,
        categories: Sequence[str],
        date_from: str,
        date_to: str,
    ) -> cf_serving_recorder.CfServingRecordSummary:
        assert date_from == "20260614"
        assert date_to == "20260615"
        assert list(categories) == ["jra", "nar"]
        return cf_serving_recorder.CfServingRecordSummary(
            pairs_processed=2, runs_created=2, runs_reused=0
        )

    monkeypatch.setattr(cf_serving_recorder, "record_cf_serving_range", _fake_range)
    exit_code = cf_serving_recorder.main(
        ["--date-from", "20260614", "--date-to", "20260615", "--categories", "jra,nar"]
    )
    assert exit_code == 0


def test_main_reports_errors_returns_one(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_range(
        client: object, categories: Sequence[str], date_from: str, date_to: str
    ) -> cf_serving_recorder.CfServingRecordSummary:
        return cf_serving_recorder.CfServingRecordSummary(
            pairs_processed=1, errors=["20260614:jra: boom"]
        )

    monkeypatch.setattr(cf_serving_recorder, "record_cf_serving_range", _fake_range)
    exit_code = cf_serving_recorder.main(["--date-from", "20260614", "--date-to", "20260614"])
    assert exit_code == 1


def test_main_default_categories(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_range(
        client: object, categories: Sequence[str], date_from: str, date_to: str
    ) -> cf_serving_recorder.CfServingRecordSummary:
        captured["categories"] = list(categories)
        return cf_serving_recorder.CfServingRecordSummary()

    monkeypatch.setattr(cf_serving_recorder, "record_cf_serving_range", _fake_range)
    cf_serving_recorder.main(["--date-from", "20260614", "--date-to", "20260614"])
    assert captured["categories"] == ["jra", "nar", "banei"]


# ── routing-honor check (2026-07-12, team-lead assignment) ──────────────────


def test_record_cf_serving_day_routing_honor_all_matched(client: MlflowClient) -> None:
    router = _make_router(
        {
            "jra": (
                "sim",
                {"sim": "iter14", "variant703": "iter14-variant"},
                [([("kyoso_joken_code", ["703"])], "variant703")],
            )
        }
    )
    neon = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14-variant", 1)]}
    )
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        routing_condition_rows={
            ("jra", DATE_STR): [_routing_row("05", "01", kyoso_joken_code="703")]
        },
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 1
    assert result.routing_honored_races == 1
    assert result.routing_honor_rate == pytest.approx(100.0)
    assert result.routing_dishonored_race_keys == ()
    run = client.get_run(result.run_id)
    assert run.data.tags[cf_serving_recorder.ROUTING_HONOR_FLAG_TAG] == "true"
    assert run.data.tags[cf_serving_recorder.ROUTING_DISHONORED_RACES_TAG] == ""
    assert run.data.metrics["cf_routing_matched_races"] == 1.0
    assert run.data.metrics["cf_routing_honored_races"] == 1.0
    assert run.data.metrics["cf_routing_honor_rate"] == pytest.approx(100.0)
    table = _race_table(client, result.run_id)
    assert table.iloc[0]["expected_model_version"] == "iter14-variant"
    assert bool(table.iloc[0]["routing_honored"]) is True


def test_record_cf_serving_day_routing_honor_dishonored_flagged(client: MlflowClient) -> None:
    router = _make_router(
        {
            "jra": (
                "sim",
                {"sim": "iter14", "variant703": "iter14-variant"},
                [([("kyoso_joken_code", ["703"])], "variant703")],
            )
        }
    )
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        routing_condition_rows={
            ("jra", DATE_STR): [_routing_row("05", "01", kyoso_joken_code="703")]
        },
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 1
    assert result.routing_honored_races == 0
    assert result.routing_honor_rate == pytest.approx(0.0)
    assert result.routing_dishonored_race_keys == ("05:01",)
    run = client.get_run(result.run_id)
    assert run.data.tags[cf_serving_recorder.ROUTING_HONOR_FLAG_TAG] == "false"
    assert run.data.tags[cf_serving_recorder.ROUTING_DISHONORED_RACES_TAG] == "05:01"
    table = _race_table(client, result.run_id)
    assert table.iloc[0]["expected_model_version"] == "iter14-variant"
    assert bool(table.iloc[0]["routing_honored"]) is False


def test_record_cf_serving_day_routing_honor_field_size_uses_horse_count(
    client: MlflowClient,
) -> None:
    """A field_band-gated rule (dirt/f_le10/005) must resolve field_band from
    the REAL served horse_count (16 here), never from the number of (race,
    model_version) GROUPS (1 here, since all 16 horses share one
    model_version) -- the latter would wrongly resolve field_band=f_le10 and
    expect the small-field variant instead of the correct default."""
    router = _make_router(
        {
            "jra": (
                "sim",
                {"sim": "iter14", "smallfield": "iter14-smallfield"},
                [
                    (
                        [
                            ("surface", ["dirt"]),
                            ("field_band", ["f_le10"]),
                            ("kyoso_joken_code", ["005"]),
                        ],
                        "smallfield",
                    )
                ],
            )
        }
    )
    rows = [_fp_row("05", "01", f"H{i}", "iter14", i + 1) for i in range(16)]
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): rows})
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        routing_condition_rows={
            ("jra", DATE_STR): [_routing_row("05", "01", track_code="24", kyoso_joken_code="005")]
        },
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 1
    assert result.routing_honored_races == 1


def test_record_cf_serving_day_routing_honor_default_variant_matches(client: MlflowClient) -> None:
    router = _make_router({"jra": ("sim", {"sim": "iter14"}, [])})
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        routing_condition_rows={("jra", DATE_STR): [_routing_row("05", "01")]},
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 1
    assert result.routing_honored_races == 1


def test_record_cf_serving_day_routing_honor_no_routing_table_is_na(client: MlflowClient) -> None:
    router = _make_router({})
    neon = FakeNeonConnection(
        fp_rows={("nar", DATE_STR): [_fp_row("30", "01", "H1", "nar-model", 1)]}
    )
    local = FakeLocalConnection(
        race_calendar_rows={("nar", DATE_STR): [("01",)]},
        routing_condition_rows={("nar_banei", DATE_STR): [_routing_row("30", "01")]},
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "nar", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 0
    assert result.routing_honored_races == 0
    assert result.routing_honor_rate is None
    run = client.get_run(result.run_id)
    assert cf_serving_recorder.ROUTING_HONOR_FLAG_TAG not in run.data.tags


def test_record_cf_serving_day_routing_honor_missing_conditions_not_computable(
    client: MlflowClient,
) -> None:
    router = _make_router({"jra": ("sim", {"sim": "iter14"}, [])})
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        routing_condition_rows={},
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 0
    assert result.routing_honor_rate is None
    table = _race_table(client, result.run_id)
    assert bool(pd.isna(table.iloc[0]["expected_model_version"]))
    assert bool(pd.isna(table.iloc[0]["routing_honored"]))


def test_record_cf_serving_day_routing_honor_nar_excludes_banei_conditions(
    client: MlflowClient,
) -> None:
    """The shared nar/banei routing-conditions query returns BOTH nar and
    banei rows (mirrors fetch_post_times's own table-sharing convention) --
    category="nar" must filter OUT the Ban-ei row, leaving only the genuine
    nar race matched."""
    router = _make_router({"nar": ("sim", {"sim": "nar-model"}, [])})
    neon = FakeNeonConnection(
        fp_rows={
            ("nar", DATE_STR): [
                _fp_row("30", "01", "H1", "nar-model", 1),
                _fp_row(serve_eval.BANEI_KEIBAJO_CODE, "02", "H2", "banei-model", 1),
            ]
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("nar", DATE_STR): [("01",)]},
        routing_condition_rows={
            ("nar_banei", DATE_STR): [
                _routing_row("30", "01"),
                _routing_row(serve_eval.BANEI_KEIBAJO_CODE, "02"),
            ]
        },
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "nar", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 1
    assert result.routing_honored_races == 1


def test_record_cf_serving_day_routing_honor_unknown_variant_name_not_computable(
    client: MlflowClient,
) -> None:
    """A rule pointing at a variant name absent from `variants` (a malformed
    cell_routing.json) must degrade to "not computable", never raise."""
    router = _make_router(
        {
            "jra": (
                "sim",
                {"sim": "iter14"},
                [([("kyoso_joken_code", ["703"])], "does_not_exist")],
            )
        }
    )
    neon = FakeNeonConnection(fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]})
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        routing_condition_rows={
            ("jra", DATE_STR): [_routing_row("05", "01", kyoso_joken_code="703")]
        },
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 0
    assert result.routing_honor_rate is None


def test_record_cf_serving_day_routing_honor_banei_category_mapping(client: MlflowClient) -> None:
    """Exercises BOTH the raw `grade_code` dimension AND the "banei" ->
    "ban-ei" container-category mapping -- if the mapping were missing/wrong,
    `has_routing("banei")` against a router keyed by "ban-ei" would read
    False and this would wrongly read routing_matched_races == 0."""
    router = _make_router(
        {
            "ban-ei": (
                "sim",
                {"sim": "banei-v9", "base": "banei-v8"},
                [([("grade_code", ["E"])], "base")],
            )
        }
    )
    neon = FakeNeonConnection(
        fp_rows={
            ("nar", DATE_STR): [_fp_row(serve_eval.BANEI_KEIBAJO_CODE, "01", "H1", "banei-v8", 1)]
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("banei", DATE_STR): [("01",)]},
        routing_condition_rows={
            ("nar_banei", DATE_STR): [
                _routing_row(serve_eval.BANEI_KEIBAJO_CODE, "01", grade_code="E")
            ]
        },
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "banei", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 1
    assert result.routing_honored_races == 1


def test_record_cf_serving_day_routing_honor_flag_clears_on_rerun(client: MlflowClient) -> None:
    router = _make_router(
        {
            "jra": (
                "sim",
                {"sim": "iter14", "variant703": "iter14-variant"},
                [([("kyoso_joken_code", ["703"])], "variant703")],
            )
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        routing_condition_rows={
            ("jra", DATE_STR): [_routing_row("05", "01", kyoso_joken_code="703")]
        },
    )
    neon_bad = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14", 1)]}
    )
    first = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon_bad, local_conn=local, cell_router=router
    )
    assert (
        client.get_run(first.run_id).data.tags[cf_serving_recorder.ROUTING_HONOR_FLAG_TAG]
        == "false"
    )

    rewritten_at = GEN_AT + timedelta(minutes=10)
    neon_fixed = FakeNeonConnection(
        fp_rows={("jra", DATE_STR): [_fp_row("05", "01", "H1", "iter14-variant", 1, rewritten_at)]}
    )
    second = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon_fixed, local_conn=local, cell_router=router
    )
    assert second.run_id == first.run_id
    assert (
        client.get_run(second.run_id).data.tags[cf_serving_recorder.ROUTING_HONOR_FLAG_TAG]
        == "true"
    )
    assert (
        client.get_run(second.run_id).data.tags[cf_serving_recorder.ROUTING_DISHONORED_RACES_TAG]
        == ""
    )


def test_record_cf_serving_day_routing_honor_dual_row_honored_via_any_row(
    client: MlflowClient,
) -> None:
    router = _make_router(
        {
            "jra": (
                "sim",
                {"sim": "iter14", "variant703": "iter14-variant"},
                [([("kyoso_joken_code", ["703"])], "variant703")],
            )
        }
    )
    neon = FakeNeonConnection(
        fp_rows={
            ("jra", DATE_STR): [
                _fp_row("05", "01", "H1", "iter14", 1),
                _fp_row("05", "01", "H2", "iter14-variant", 1),
            ]
        }
    )
    local = FakeLocalConnection(
        race_calendar_rows={("jra", DATE_STR): [("01",)]},
        routing_condition_rows={
            ("jra", DATE_STR): [_routing_row("05", "01", kyoso_joken_code="703")]
        },
    )
    result = cf_serving_recorder.record_cf_serving_day(
        client, "jra", DATE_STR, neon_conn=neon, local_conn=local, cell_router=router
    )
    assert result.routing_matched_races == 1
    assert result.routing_honored_races == 1
    table = _race_table(client, result.run_id)
    assert len(table) == 2
    assert sorted(bool(v) for v in table["routing_honored"].tolist()) == [False, True]
