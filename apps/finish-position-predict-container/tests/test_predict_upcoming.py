"""Tests for the small pure helpers in ``predict_upcoming.py``.

``predict_upcoming.py`` itself is the I/O orchestration entrypoint (Neon TCP,
R2, DuckDB subprocess, native CatBoost / XGBoost load) and is excluded from
the ``--cov=predict_lib`` coverage gate per ``pyproject.toml``. The
per-category class-code extractor and the reconnect-on-write helpers are
structurally pure though — they are covered here without any real Neon I/O.

Tests here run alongside the predict_lib suite but do NOT count towards the
predict_lib coverage threshold; they are exclusively a correctness check for
the small helpers.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import final, override
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Import the cross-module helpers directly so the tests stay I/O-free.
from predict_lib.cell_router import build_base_model_r2_key
from predict_lib.model_meta import (
    METADATA_FILE_NAME,
    NAR_ETOP2_MODEL_VERSION,
    Architecture,
    Category,
    model_version_for,
)
from predict_lib.rescore import RaceScope
from predict_lib.scorer import BoosterLike
from predict_lib.serve import (
    ParquetPayloadFn,
    PerRaceParquetPayloadFn,
    PredictCategoryFn,
    PredictParams,
    iter_predict_chunks,
    parse_predict_params,
)
from predict_upcoming import (
    PredictWindow,
    VariantModel,
    execute,
    extract_race_class_code,
    flush_predictions,
    make_handler_class,
    predict_category,
    score_one_race_nar_etop2,
    score_races,
)

# ---------------------------------------------------------------------------
# Minimal stub connection
# ---------------------------------------------------------------------------


@final
class _StubCursor:
    """Minimal cursor stub that records the last execute call."""

    def __init__(self) -> None:
        self.last_sql: str = ""
        self.last_params: object = None

    def execute(self, query: str, params: object = None) -> object:
        self.last_sql = query
        self.last_params = params
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class _StubConnection:
    """Minimal connection stub that records commits, rollbacks, and closes."""

    committed: int
    rolledback: int
    closed: bool
    _raise_on_execute: Exception | None
    _cursor: _StubCursor

    def __init__(self, raise_on_execute: Exception | None = None) -> None:
        self.committed = 0
        self.rolledback = 0
        self.closed = False
        self._raise_on_execute = raise_on_execute
        self._cursor = _StubCursor()

    def cursor(self) -> _StubCursor:
        if self._raise_on_execute is not None:
            raise self._raise_on_execute
        return self._cursor

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolledback += 1

    def close(self) -> None:
        self.closed = True


def testextract_race_class_code_jra_returns_kyoso_joken_code() -> None:
    entries = [
        {"kyoso_joken_code": "005", "umaban": 1},
        {"kyoso_joken_code": "005", "umaban": 2},
    ]
    assert extract_race_class_code("jra", entries) == "005"


def testextract_race_class_code_jra_returns_none_when_missing() -> None:
    # JRA entry without the kyoso_joken_code field -> None.
    entries = [{"umaban": 1}]
    assert extract_race_class_code("jra", entries) is None


def testextract_race_class_code_jra_returns_none_for_empty_string() -> None:
    # PG returns the empty string for some legacy races; we collapse it to
    # None so the per-class router falls back to iter14.
    entries = [{"kyoso_joken_code": "  ", "umaban": 1}]
    assert extract_race_class_code("jra", entries) is None


def testextract_race_class_code_jra_strips_whitespace() -> None:
    entries = [{"kyoso_joken_code": " 703 ", "umaban": 1}]
    assert extract_race_class_code("jra", entries) == "703"


def testextract_race_class_code_nar_returns_nar_subclass() -> None:
    # Phase F: NAR reads ``nar_subclass`` rather than ``kyoso_joken_code``.
    entries = [
        {"nar_subclass": "NEW", "umaban": 1},
        {"nar_subclass": "NEW", "umaban": 2},
    ]
    assert extract_race_class_code("nar", entries) == "NEW"


def testextract_race_class_code_nar_returns_each_named_subclass() -> None:
    # All six NAR sub-classes pass through verbatim — the extractor reads from
    # the first entry only since all entries of a race share the same class.
    for subclass in ("NEW", "MUKATSU", "C", "B", "A", "OP"):
        entries = [{"nar_subclass": subclass, "umaban": 1}]
        assert extract_race_class_code("nar", entries) == subclass


def testextract_race_class_code_nar_returns_other_when_no_meisho_match() -> None:
    # The DuckDB build emits ``"other"`` literal when nothing matches the
    # nar_subclass_case_sql regex — pass it through verbatim so the resolver
    # routes to the NAR ``other`` ensemble.
    entries = [{"nar_subclass": "other", "umaban": 1}]
    assert extract_race_class_code("nar", entries) == "other"


def testextract_race_class_code_nar_returns_none_when_field_absent() -> None:
    # A NAR entry without the nar_subclass field collapses to None — the
    # router then falls back to the NAR iter 12 baseline.
    entries = [{"umaban": 1}]
    assert extract_race_class_code("nar", entries) is None


def testextract_race_class_code_nar_returns_none_when_field_null() -> None:
    entries = [{"nar_subclass": None, "umaban": 1}]
    assert extract_race_class_code("nar", entries) is None


def testextract_race_class_code_banei_returns_none_by_dispatch() -> None:
    # Ban-ei is NOT in the per-category dispatch map — the extractor returns
    # None regardless of whether the columns exist so the resolver
    # short-circuits to the Ban-ei category-global model.
    entries = [
        {"kyoso_joken_code": "BAN", "nar_subclass": "other", "umaban": 1},
    ]
    assert extract_race_class_code("ban-ei", entries) is None


def testextract_race_class_code_returns_none_for_empty_entries() -> None:
    # No entries -> nothing to read; safe None.
    assert extract_race_class_code("jra", []) is None
    assert extract_race_class_code("nar", []) is None


def testextract_race_class_code_coerces_non_string_value() -> None:
    # Defensive: numeric class code (DuckDB sometimes emits int for the JRA
    # numeric codes) is str-coerced before strip + return so the resolver
    # sees a clean string key.
    entries = [{"kyoso_joken_code": 703, "umaban": 1}]
    assert extract_race_class_code("jra", entries) == "703"


# ---------------------------------------------------------------------------
# execute — reconnect-on-write
# ---------------------------------------------------------------------------

_DB_URL = "postgresql://host/db"


def test_execute_succeeds_on_happy_path() -> None:
    # Normal path: execute+commit returns the same connection unchanged.
    conn = _StubConnection()
    result = execute(conn, "SELECT 1", [], _DB_URL)
    assert result is conn
    assert conn.committed == 1
    assert conn.rolledback == 0


def test_execute_non_transient_error_propagates_without_reconnect() -> None:
    # Non-transient errors (e.g. programming error) must NOT trigger a reconnect.
    auth_exc = Exception("password authentication failed")
    conn = _StubConnection(raise_on_execute=auth_exc)
    try:
        execute(conn, "SELECT 1", [], _DB_URL)
    except Exception as exc:
        assert exc is auth_exc
    else:
        raise AssertionError("should have raised")


def test_execute_reconnects_and_retries_on_admin_shutdown() -> None:
    # AdminShutdown mid-write: old connection should be rolled back + closed;
    # a fresh connection should be used for the retry and returned.
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    bad_conn = _StubConnection(raise_on_execute=admin_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn) as mock_connect:
        result = execute(bad_conn, "INSERT ...", ["p"], _DB_URL)

    assert result is fresh_conn
    assert fresh_conn.committed == 1
    mock_connect.assert_called_once_with(_DB_URL)
    # Old connection was asked to rollback + close (both may fail gracefully).


def test_execute_reconnects_on_connection_is_lost() -> None:
    lost_exc = Exception("the connection is lost")
    bad_conn = _StubConnection(raise_on_execute=lost_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn):
        result = execute(bad_conn, "INSERT ...", [], _DB_URL)

    assert result is fresh_conn
    assert fresh_conn.committed == 1


def test_execute_retry_failure_propagates() -> None:
    # If the reconnect attempt also raises, that error propagates to the caller.
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    bad_conn = _StubConnection(raise_on_execute=admin_exc)
    also_bad_conn = _StubConnection(raise_on_execute=RuntimeError("retry also failed"))

    with patch("predict_upcoming._connect", return_value=also_bad_conn):
        try:
            execute(bad_conn, "INSERT ...", [], _DB_URL)
        except RuntimeError as exc:
            assert "retry also failed" in str(exc)
        else:
            raise AssertionError("should have raised")


def test_execute_rollback_failure_is_swallowed() -> None:
    # If rollback also raises (connection already dead), the reconnect still
    # proceeds and the overall execute still succeeds on the fresh connection.
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")

    class _FailRollbackConn(_StubConnection):
        @override
        def rollback(self) -> None:
            raise RuntimeError("rollback also failed")

    bad_conn = _FailRollbackConn(raise_on_execute=admin_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn):
        result = execute(bad_conn, "INSERT ...", [], _DB_URL)

    assert result is fresh_conn
    assert fresh_conn.committed == 1


# ---------------------------------------------------------------------------
# flush_predictions — per-race dedup + reconnect propagation
# ---------------------------------------------------------------------------


def _make_pred_row(race_id: str, ketto: str) -> list[object]:
    # A prediction row whose structure satisfies _row_to_pk_map:
    #   index 0     — placeholder (not used by _row_to_pk_map)
    #   indices 1-5 — race_id parts joined by ":" to form the race_id key
    #   index 6     — ketto_toroku_bango
    # race_id is expected to be "p1:p2:p3:p4:p5" (5 colon-separated parts).
    parts = race_id.split(":")
    assert len(parts) == 5, f"race_id must have exactly 5 parts, got {parts}"
    return ["placeholder", parts[0], parts[1], parts[2], parts[3], parts[4], ketto]


def test_flush_predictions_empty_rows_returns_zero() -> None:
    conn = _StubConnection()
    written, returned_conn = flush_predictions(conn, [], _DB_URL)
    assert written == 0
    assert returned_conn is conn
    assert conn.committed == 0


def test_flush_predictions_writes_rows_and_returns_connection() -> None:
    conn = _StubConnection()
    rows = [_make_pred_row("20260619:05:11:01:01", "HORSE1")]
    written, returned_conn = flush_predictions(conn, rows, _DB_URL)
    assert written == 1
    assert returned_conn is conn
    assert conn.committed >= 1


def test_flush_predictions_returns_fresh_conn_after_reconnect() -> None:
    # Simulate AdminShutdown on first execute call; verify the returned
    # connection is the fresh one (not the original dead conn).
    admin_exc = type("AdminShutdown", (Exception,), {})("terminating connection")
    dead_conn = _StubConnection(raise_on_execute=admin_exc)
    fresh_conn = _StubConnection()

    with patch("predict_upcoming._connect", return_value=fresh_conn):
        rows = [_make_pred_row("20260619:05:11:01:01", "HORSE1")]
        written, returned_conn = flush_predictions(dead_conn, rows, _DB_URL)

    assert returned_conn is fresh_conn
    assert written == 1
    assert fresh_conn.committed >= 1


# ---------------------------------------------------------------------------
# make_handler_class — staticmethod binding (regression for 4-arg TypeError)
# ---------------------------------------------------------------------------
#
# Python's descriptor protocol makes plain function class attributes behave as
# bound methods when accessed on an instance, injecting ``self`` as the first
# argument.  This caused a ``TypeError`` in production:
#   _make_predict_fn.<locals>._predict() takes 3 positional arguments but 4
#   were given
# because ``self.predict_fn(category, run_date, days_ahead)`` was dispatched as
# ``predict_fn(self, category, run_date, days_ahead)``.
#
# The fix wraps the callables with ``staticmethod`` at class-definition time.
# These tests pin that contract: the class attributes must remain plain
# callables callable without any instance, i.e. NOT bound methods.


def _fake_predict(
    category: str,
    run_date: str,
    days_ahead: int,
    keibajo_code: str | None = None,
    race_bango: str | None = None,
) -> int:
    """Dummy predict_fn that returns the length of category as a sentinel."""
    return len(category)


def _fake_parquet_payload() -> tuple[str, str] | None:
    """Dummy parquet_payload_fn that returns None (no parquet available)."""
    return None


def _fake_per_race_parquet_payload() -> list[dict[str, str]] | None:
    """Dummy per_race_parquet_payload_fn that returns None (no per-race split)."""
    return None


def _fake_rescore(
    category: str,
    run_date: str,
    days_ahead: int,
    keibajo_code: str | None = None,
    race_bango: str | None = None,
) -> int:
    """Dummy rescore_fn that returns a fixed sentinel value."""
    return 99


def _fake_rescore_factory(
    scope: RaceScope,
) -> tuple[PredictCategoryFn, PerRaceParquetPayloadFn]:
    """Dummy rescore_factory that ignores the scope and returns a fixed fn + payload fn."""
    del scope

    def _per_race() -> list[dict[str, str]] | None:
        return None

    return _fake_rescore, _per_race


def test_make_handler_class_predict_fn_callable_without_instance() -> None:
    """predict_fn on the handler class must be callable as a plain 3-arg function."""
    handler_cls = make_handler_class(
        _fake_predict, _fake_parquet_payload, _fake_per_race_parquet_payload, _fake_rescore_factory
    )
    # Call directly on the class (no instance) — must NOT inject self.
    result = handler_cls.predict_fn("nar", "20260618", 0, None, None)
    assert result == len("nar")


def test_make_handler_class_parquet_payload_fn_callable_without_instance() -> None:
    """parquet_payload_fn stored as staticmethod must be callable without an instance."""
    handler_cls = make_handler_class(
        _fake_predict, _fake_parquet_payload, _fake_per_race_parquet_payload, _fake_rescore_factory
    )
    fn: ParquetPayloadFn = handler_cls.__dict__["parquet_payload_fn"].__func__
    result = fn()
    assert result is None


def test_make_handler_class_per_race_parquet_payload_fn_callable_without_instance() -> None:
    """per_race_parquet_payload_fn stored as staticmethod must be callable without an instance."""
    handler_cls = make_handler_class(
        _fake_predict, _fake_parquet_payload, _fake_per_race_parquet_payload, _fake_rescore_factory
    )
    fn: PerRaceParquetPayloadFn = handler_cls.__dict__["per_race_parquet_payload_fn"].__func__
    result = fn()
    assert result is None


def test_make_handler_class_rescore_factory_callable_without_instance() -> None:
    """rescore_factory on the handler class must be callable without an instance."""
    handler_cls = make_handler_class(
        _fake_predict, _fake_parquet_payload, _fake_per_race_parquet_payload, _fake_rescore_factory
    )
    factory = handler_cls.rescore_factory
    assert factory is not None
    rescore, per_race = factory(RaceScope())
    result = rescore("jra", "20260618", 1, None, None)
    assert result == 99
    assert per_race() is None


def test_make_handler_class_rescore_factory_none_when_not_provided() -> None:
    """When rescore_factory=None, the class attribute must also be None."""
    handler_cls = make_handler_class(
        _fake_predict, _fake_parquet_payload, _fake_per_race_parquet_payload, None
    )
    assert handler_cls.rescore_factory is None


def test_make_handler_class_predict_fn_not_bound_method() -> None:
    """Accessing predict_fn on the class must NOT produce a bound method."""
    handler_cls = make_handler_class(
        _fake_predict, _fake_parquet_payload, _fake_per_race_parquet_payload, _fake_rescore_factory
    )
    import inspect

    # A bound method has a __self__; a staticmethod result does not.
    assert not inspect.ismethod(handler_cls.predict_fn), (
        "predict_fn must not be a bound method — staticmethod wrapping is required"
    )


def test_make_handler_class_predict_fn_accepts_exactly_5_args() -> None:
    """Verify predict_fn exposes the 5-arg contract and no injected ``self``.

    The staticmethod wrapping must keep the signature at exactly five parameters
    (category, run_date, days_ahead, keibajo_code, race_bango) — if ``self`` were
    injected by the descriptor protocol the count would be six.
    """
    import inspect

    handler_cls = make_handler_class(
        _fake_predict, _fake_parquet_payload, _fake_per_race_parquet_payload, _fake_rescore_factory
    )
    sig = inspect.signature(handler_cls.predict_fn)
    params = list(sig.parameters.values())
    assert len(params) == 5, (
        f"predict_fn must have exactly 5 parameters "
        f"(category, run_date, days_ahead, keibajo_code, race_bango), "
        f"got {len(params)}: {[p.name for p in params]}"
    )


# ---------------------------------------------------------------------------
# NAR E-top2 per-class override wiring (iter23-nar-etop2)
# ---------------------------------------------------------------------------
#
# NAR production scores with XGBoost as the BASE and the CatBoost CB-2013 model
# supplies the override signal (mirror image of the JRA path). These tests pin
# the wiring of ``score_one_race_nar_etop2`` (the per-race override) and the
# ``score_races`` branch that loads the CB override booster only for NAR when
# NAR_ETOP2_ENABLED is True.


@final
class _ScoreByUmaban:
    """Fake booster that returns a per-row score keyed by the row's umaban.

    ``build_feature_matrix`` projects each entry onto the numeric feature order,
    dropping the umaban / ketto columns, so the fake instead consults a closure
    map indexed by the row's position in the race. The scores are supplied as a
    parallel list in entry order at construction time.
    """

    def __init__(self, scores: list[float]) -> None:
        self._scores = scores

    def predict(self, matrix: object) -> list[float]:
        del matrix  # scores are positional, not feature-derived
        return list(self._scores)


_NAR_RACE_ID: str = "nar:20260620:30:02:11"


def _nar_entries() -> list[dict[str, object]]:
    """Three NAR entries (umaban 1/2/3) in class ``A`` (an ADOPT class)."""
    return [
        {"ketto_toroku_bango": "H1", "umaban": 1, "nar_subclass": "A", "feat": 0.1},
        {"ketto_toroku_bango": "H2", "umaban": 2, "nar_subclass": "A", "feat": 0.2},
        {"ketto_toroku_bango": "H3", "umaban": 3, "nar_subclass": "A", "feat": 0.3},
    ]


def _run_nar_etop2(
    xgb: BoosterLike,
    cb: BoosterLike,
    entries: list[dict[str, object]],
) -> list[list[object]]:
    """Invoke ``score_one_race_nar_etop2`` for the shared NAR test race."""
    return score_one_race_nar_etop2(xgb, cb, _NAR_RACE_ID, "nar", entries, ["feat"])


def test_score_one_race_nar_etop2_override_promotes_xgb_rank2() -> None:
    """When CB#1 == XGB#2 in an ADOPT class, XGB#2 is promoted to rank-1."""
    # XGB base ranking: H1 (0.9) > H2 (0.5) > H3 (0.1) -> XGB#2 = H2 (umaban 2).
    xgb = _ScoreByUmaban([0.9, 0.5, 0.1])
    # CB rank-1 = H2 (umaban 2) -> equals XGB#2, so the override fires.
    cb = _ScoreByUmaban([0.2, 0.8, 0.1])

    rows = _run_nar_etop2(xgb, cb, _nar_entries())

    by_rank = {row[9]: row[7] for row in rows}  # predicted_rank -> umaban
    assert by_rank[1] == 2, "XGB#2 (umaban 2) must be promoted to rank-1"
    assert by_rank[2] == 1, "XGB#1 (umaban 1) must be demoted to rank-2"
    assert by_rank[3] == 3, "rank-3 (umaban 3) must be preserved"


def test_score_one_race_nar_etop2_writes_iter23_model_version() -> None:
    """Every emitted row is labelled with NAR_ETOP2_MODEL_VERSION."""
    xgb = _ScoreByUmaban([0.9, 0.5, 0.1])
    cb = _ScoreByUmaban([0.2, 0.8, 0.1])

    rows = _run_nar_etop2(xgb, cb, _nar_entries())

    assert rows, "at least one prediction row must be emitted"
    assert all(row[0] == NAR_ETOP2_MODEL_VERSION for row in rows)
    assert NAR_ETOP2_MODEL_VERSION == "iter23-nar-etop2"


def test_score_one_race_nar_etop2_no_override_for_reject_class() -> None:
    """A non-ADOPT class (``C``) keeps the pure XGB base ranking."""
    entries = [dict(entry, nar_subclass="C") for entry in _nar_entries()]
    xgb = _ScoreByUmaban([0.9, 0.5, 0.1])
    cb = _ScoreByUmaban([0.2, 0.8, 0.1])

    rows = _run_nar_etop2(xgb, cb, entries)

    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 1, "REJECT class must keep XGB#1 (umaban 1) at rank-1"
    assert by_rank[2] == 2
    assert by_rank[3] == 3


def test_score_races_loads_cb_override_booster_for_nar() -> None:
    """score_races loads the CB override booster for NAR and routes via the override."""
    entries = _nar_entries()
    races = {"nar:20260620:30:02:11": entries}
    xgb = _ScoreByUmaban([0.9, 0.5, 0.1])
    cb = _ScoreByUmaban([0.2, 0.8, 0.1])
    loaded: list[str] = []

    def _fake_load_cb(models_dir: Path) -> BoosterLike:
        del models_dir
        loaded.append("cb")
        return cb

    with (
        patch("predict_upcoming._load_booster", return_value=xgb),
        patch("predict_upcoming.init_member_pool", return_value=object()),
        patch("predict_upcoming._load_cb_nar_etop2_booster", side_effect=_fake_load_cb),
        patch("predict_upcoming.NAR_ETOP2_ENABLED", True),
    ):
        scored = score_races(races, "nar", Path("/models"), ["feat"])

    assert loaded == ["cb"], "the CB override booster must be loaded exactly once for NAR"
    rows = scored[0]
    assert all(row[0] == NAR_ETOP2_MODEL_VERSION for row in rows)
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 2, "override must promote XGB#2 (umaban 2) to rank-1"


# ---------------------------------------------------------------------------
# Cell-routing variant pool (score_races)
# ---------------------------------------------------------------------------
#
# score_races builds a dict[str, VariantModel] from the routing config's
# ``variants`` map: every non-default variant is loaded into the pool, the
# default variant is served by the already-loaded category-global fallback.
# A race whose resolved variant is in the pool scores against that variant's
# booster + feature order + architecture; otherwise the fallback scores it.
# These tests duck-type the routing config so they exercise the pool logic
# without depending on the concrete cell_router dataclasses.


@final
class _FakeVariantSpec:
    """Stand-in for ``cell_router.VariantSpec`` (model_version/feature_count/architecture)."""

    def __init__(self, model_version: str, feature_count: int, architecture: str) -> None:
        self.model_version = model_version
        self.feature_count = feature_count
        self.architecture = architecture


@final
class _FakeRouting:
    """Stand-in for ``CategoryRouting`` carrying a variants map + default variant."""

    def __init__(self, variants: dict[str, _FakeVariantSpec], default_variant: str) -> None:
        self.variants = variants
        self.default_variant = default_variant


@final
class _FakeRouter:
    """Stand-in for ``CellRouter`` that always routes a ban-ei race to ``resolved``."""

    def __init__(self, routing: _FakeRouting, resolved: str) -> None:
        self._routing = routing
        self._resolved = resolved

    def has_routing(self, category: str) -> bool:
        del category
        return True

    def routing_for(self, category: str) -> _FakeRouting:
        del category
        return self._routing

    def resolve_variant(self, category: str, entries: Sequence[Mapping[str, object]]) -> str:
        del category, entries
        return self._resolved


def _banei_entries() -> list[dict[str, object]]:
    """Three Ban-ei entries (umaban 1/2/3) carrying the single ``feat`` column."""
    return [
        {"ketto_toroku_bango": "B1", "umaban": 1, "feat": 0.1},
        {"ketto_toroku_bango": "B2", "umaban": 2, "feat": 0.2},
        {"ketto_toroku_bango": "B3", "umaban": 3, "feat": 0.3},
    ]


def _write_variant_metadata(
    models_dir: Path, category: str, model_version: str, feature_names: list[str]
) -> None:
    meta_path = models_dir / build_base_model_r2_key(category, model_version, METADATA_FILE_NAME)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps({"feature_names": feature_names}), encoding="utf-8")


def test_variant_model_holds_booster_and_feature_contract() -> None:
    """VariantModel is a frozen carrier for the booster + feature order + arch."""
    booster = _ScoreByUmaban([0.1])
    vm = VariantModel(booster=booster, feature_names=["a", "b"], architecture="catboost")
    assert vm.booster is booster
    assert list(vm.feature_names) == ["a", "b"]
    assert vm.architecture == "catboost"


def test_score_races_routes_to_pooled_variant_and_skips_default(tmp_path: Path) -> None:
    """A non-default variant is loaded into the pool and scores its routed race.

    The Ban-ei ``base`` variant (catboost) is loaded; the ``sim`` default is
    skipped (served by the fallback). The race routes to ``base`` so the variant
    booster — not the fallback — drives the ranking.
    """
    _write_variant_metadata(tmp_path, "ban-ei", "banei-base-v8", ["feat"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-sim-v9", 1, "catboost"),
            "base": _FakeVariantSpec("banei-base-v8", 1, "catboost"),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="base")
    fallback = _ScoreByUmaban([0.9, 0.1, 0.1])  # would rank umaban 1 first
    variant_booster = _ScoreByUmaban([0.1, 0.9, 0.3])  # ranks umaban 2 first
    loaded: list[str] = []

    def _fake_load_by_arch(model_path: Path, architecture: Architecture) -> BoosterLike:
        del architecture
        loaded.append(str(model_path))
        return variant_booster

    races = {"ban-ei:20260620:65:01:01": _banei_entries()}
    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=fallback),
        patch("predict_upcoming.init_member_pool", return_value=object()),
        patch("predict_upcoming._load_booster_by_arch", side_effect=_fake_load_by_arch),
    ):
        scored = score_races(races, "ban-ei", tmp_path, ["feat"])

    assert len(loaded) == 1, "only the non-default variant should be loaded"
    assert "banei-base-v8" in loaded[0], "the loaded variant must be the base model"
    assert "banei-sim-v9" not in loaded[0], "the default variant must be served by the fallback"
    rows = scored[0]
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 2, "the pooled variant booster must drive the ranking"
    assert all(row[0] == model_version_for("ban-ei") for row in rows)


def test_score_races_falls_back_when_resolved_variant_not_in_pool(tmp_path: Path) -> None:
    """When the resolved variant is the default (not pooled), the fallback scores."""
    _write_variant_metadata(tmp_path, "ban-ei", "banei-base-v8", ["feat"])
    routing = _FakeRouting(
        variants={
            "sim": _FakeVariantSpec("banei-sim-v9", 1, "catboost"),
            "base": _FakeVariantSpec("banei-base-v8", 1, "catboost"),
        },
        default_variant="sim",
    )
    router = _FakeRouter(routing, resolved="sim")  # default -> not in pool
    fallback = _ScoreByUmaban([0.9, 0.1, 0.1])  # ranks umaban 1 first
    variant_booster = _ScoreByUmaban([0.1, 0.9, 0.3])

    def _fake_load_by_arch(model_path: Path, architecture: Architecture) -> BoosterLike:
        del model_path, architecture
        return variant_booster

    races = {"ban-ei:20260620:65:01:01": _banei_entries()}
    with (
        patch("predict_upcoming.load_cell_router", return_value=router),
        patch("predict_upcoming._load_booster", return_value=fallback),
        patch("predict_upcoming.init_member_pool", return_value=object()),
        patch("predict_upcoming._load_booster_by_arch", side_effect=_fake_load_by_arch),
    ):
        scored = score_races(races, "ban-ei", tmp_path, ["feat"])

    rows = scored[0]
    by_rank = {row[9]: row[7] for row in rows}
    assert by_rank[1] == 1, "the fallback booster must drive the ranking for the default variant"


# ---------------------------------------------------------------------------
# Per-race mode=full feature generation (target_race wiring)
# ---------------------------------------------------------------------------
#
# When mode=full carries keibajoCode + raceBango, the Container builds features
# for a single race (DuckDB --target-race) instead of scanning the whole day.
# predict_category forwards a "keibajo:bango" target_race string straight to the
# pipeline; the HTTP handler parses the scope and the shared PredictCategoryFn
# contract carries it from iter_predict_chunks into the predict fn.


def _noop_sleep(_seconds: float) -> None:
    """No-op sleep injected so the keepalive loop never blocks the test."""


def testpredict_category_forwards_target_race_to_pipeline() -> None:
    """predict_category passes its target_race straight through to the pipeline."""
    import pipeline_runner

    captured: dict[str, object] = {}

    def _fake_build(
        category: Category,
        target_date: str,
        days_ahead: int,
        database_url: str,
        target_race: str | None = None,
    ) -> Mapping[str, list[Mapping[str, object]]]:
        captured["target_race"] = target_race
        return {}

    with (
        patch.object(pipeline_runner, "build_upcoming_feature_rows", side_effect=_fake_build),
        patch("predict_upcoming._score_and_flush_races", return_value=3),
    ):
        window = PredictWindow(target_date="20260628", days_ahead=0, database_url=_DB_URL)
        written = predict_category(_DB_URL, "jra", Path("/models"), window, target_race="01:05")

    assert captured["target_race"] == "01:05"
    assert written == 3


def testpredict_category_target_race_defaults_to_none() -> None:
    """The whole-window full path forwards target_race=None (no per-race filter)."""
    import pipeline_runner

    captured: dict[str, object] = {"target_race": "sentinel"}

    def _fake_build(
        category: Category,
        target_date: str,
        days_ahead: int,
        database_url: str,
        target_race: str | None = None,
    ) -> Mapping[str, list[Mapping[str, object]]]:
        captured["target_race"] = target_race
        return {}

    with (
        patch.object(pipeline_runner, "build_upcoming_feature_rows", side_effect=_fake_build),
        patch("predict_upcoming._score_and_flush_races", return_value=0),
    ):
        window = PredictWindow(target_date="20260628", days_ahead=2, database_url=_DB_URL)
        predict_category(_DB_URL, "nar", Path("/models"), window)

    assert captured["target_race"] is None


def test_parse_predict_params_full_mode_keeps_race_scope() -> None:
    """A full-mode request carries keibajoCode / raceBango as the per-race scope."""
    result = parse_predict_params(
        "category=jra&runDate=20260628&daysAhead=0&mode=full&keibajoCode=01&raceBango=05"
    )
    assert not isinstance(result, str)
    assert result.mode == "full"
    assert result.keibajo_code == "01"
    assert result.race_bango == "05"


def test_full_mode_handler_flow_passes_race_scope_to_predict_fn() -> None:
    """Mirror _PredictHandler.do_GET: parse a full-mode query, then drive the stream.

    Proves keibajoCode / raceBango parsed from a full-mode request reach the
    predict fn (the per-race feature-generation scope), exactly as the handler
    wires ``parse_predict_params`` -> ``iter_predict_chunks(result, predict_fn)``.
    """
    parsed = parse_predict_params(
        "category=jra&runDate=20260628&daysAhead=0&mode=full&keibajoCode=01&raceBango=05"
    )
    assert not isinstance(parsed, str)

    recorded: list[tuple[str, str, int, str | None, str | None]] = []

    def _recording_predict(
        category: str,
        run_date: str,
        days_ahead: int,
        keibajo_code: str | None = None,
        race_bango: str | None = None,
    ) -> int:
        recorded.append((category, run_date, days_ahead, keibajo_code, race_bango))
        return 1

    list(iter_predict_chunks(parsed, _recording_predict, sleep_fn=_noop_sleep))

    assert recorded == [("jra", "20260628", 0, "01", "05")]


def test_predict_params_default_full_mode_has_no_race_scope() -> None:
    """The whole-window full request leaves keibajoCode / raceBango unset (None)."""
    params = PredictParams(category="jra", run_date="20260628", days_ahead=0)
    assert params.mode == "full"
    assert params.keibajo_code is None
    assert params.race_bango is None
