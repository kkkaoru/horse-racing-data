"""Tests for the multi-booster pool used by ensemble routing (Phase B-2C + Phase F).

The pool's data structure (``BoosterPool`` + :class:`PoolBooster`) and pure
path helpers (``discover_member_models``, ``discover_baseline_member_model``,
``build_pool_from_paths``) are covered with ``tmp_path`` fakes + a stub
booster class implementing ``BoosterLike``. The ``load_booster_from_path``
side-effect helper imports ``catboost_adapter`` / ``xgboost_adapter`` lazily;
tests stub them via ``sys.modules`` so coverage can hit both branches without
needing the native CatBoost / XGBoost runtimes.

Phase F (2026-06-05) extends the pool with architecture-aware loading so NAR
ensembles can blend the iter 12 XGBoost baseline with iter 30 CatBoost
residuals — the pool entries now carry a :class:`PoolBooster` (booster +
architecture) record and the loader dispatches to the right adapter per arch.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from predict_lib.booster_pool import (
    BoosterPool,
    PoolBooster,
    build_pool_from_paths,
    discover_baseline_member_model,
    discover_member_models,
    load_booster_from_path,
)
from predict_lib.model_meta import Architecture
from predict_lib.scorer import BoosterLike

JRA_CATEGORY: str = "jra"
NAR_CATEGORY: str = "nar"
CLASS_703: str = "703"
CLASS_NEW: str = "NEW"
ITER20_BASE: str = "iter20-jra-cb-perclass-703-v8"
ITER20_HPO: str = "iter20-jra-cb-perclass-703-hpo-v8"
ITER21_CHAIN: str = "iter21-jra-cb-chain-703-v8"
ITER22_RESIDUAL: str = "iter22-jra-cb-residual-703-v8"
NAR_BASELINE: str = "iter12-nar-xgb-hpo-v8"
NAR_RESIDUAL_NEW: str = "iter30-nar-cb-residual-NEW-v8"


class _StubBooster:
    """``BoosterLike`` stub that returns a unique tag so tests can identify it."""

    _tag: str

    def __init__(self, tag: str) -> None:
        self._tag = tag

    def predict(self, matrix: Sequence[Sequence[float]]) -> Sequence[float]:
        return [float(len(matrix)) for _ in matrix]

    @property
    def tag(self) -> str:
        return self._tag


def _make_pool(records: dict[str, PoolBooster]) -> BoosterPool:
    return BoosterPool(boosters=records)


def _record(booster: BoosterLike, architecture: Architecture) -> PoolBooster:
    return PoolBooster(booster=booster, architecture=architecture)


def _write_fake_model_json(model_dir: Path) -> Path:
    model_dir.mkdir(parents=True, exist_ok=True)
    path = model_dir / "model.json"
    path.write_text("{}", encoding="utf-8")
    return path


class _FakeCatboostAdapter(ModuleType):
    """Typed stand-in for the ``catboost_adapter`` module.

    Declares ``load_catboost_booster`` so we can assign a stub without
    triggering basedpyright's "unknown attribute" error nor ruff B010
    (`setattr` with a constant). The lazy
    ``from catboost_adapter import load_catboost_booster`` inside
    ``load_booster_from_path`` resolves to this attribute when the instance is
    registered in ``sys.modules``.
    """

    load_catboost_booster: Callable[[str], BoosterLike]


class _FakeXgboostAdapter(ModuleType):
    """Typed stand-in for the ``xgboost_adapter`` module.

    Mirrors :class:`_FakeCatboostAdapter` but for the XGBoost loader used by
    the NAR baseline (iter 12 XGBoost). Declared as a class attribute so
    basedpyright + ruff stay quiet on the stub assignment.
    """

    load_xgboost_booster: Callable[[str], BoosterLike]


def _install_fake_catboost_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_load: Callable[[str], BoosterLike],
) -> None:
    """Inject a stub ``catboost_adapter`` module so ``load_booster_from_path``'s
    lazy ``from catboost_adapter import load_catboost_booster`` resolves to the
    test double without importing the native CatBoost runtime.
    """
    fake_module = _FakeCatboostAdapter("catboost_adapter")
    fake_module.load_catboost_booster = fake_load
    monkeypatch.setitem(sys.modules, "catboost_adapter", fake_module)


def _install_fake_xgboost_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_load: Callable[[str], BoosterLike],
) -> None:
    """Inject a stub ``xgboost_adapter`` module so ``load_booster_from_path``'s
    lazy ``from xgboost_adapter import load_xgboost_booster`` resolves to the
    test double without importing the native XGBoost runtime.
    """
    fake_module = _FakeXgboostAdapter("xgboost_adapter")
    fake_module.load_xgboost_booster = fake_load
    monkeypatch.setitem(sys.modules, "xgboost_adapter", fake_module)


# ---------------------------------------------------------------------------
# PoolBooster dataclass


def test_pool_booster_dataclass_signature() -> None:
    booster = _StubBooster("alpha")
    record = PoolBooster(booster=booster, architecture="catboost")
    assert record.booster is booster
    assert record.architecture == "catboost"


def test_pool_booster_dataclass_supports_xgboost() -> None:
    booster = _StubBooster("nar-baseline")
    record = PoolBooster(booster=booster, architecture="xgboost")
    assert record.architecture == "xgboost"


# ---------------------------------------------------------------------------
# BoosterPool


def test_booster_pool_get_returns_booster_when_present() -> None:
    stub = _StubBooster("alpha")
    pool = _make_pool({ITER20_BASE: _record(stub, "catboost")})
    result = pool.get(ITER20_BASE)
    assert result is stub


def test_booster_pool_get_returns_none_when_missing() -> None:
    pool = _make_pool({ITER20_BASE: _record(_StubBooster("alpha"), "catboost")})
    assert pool.get(ITER21_CHAIN) is None


def test_booster_pool_get_record_returns_pool_booster_when_present() -> None:
    stub = _StubBooster("alpha")
    pool = _make_pool({ITER20_BASE: _record(stub, "catboost")})
    record = pool.get_record(ITER20_BASE)
    assert record is not None
    assert record.booster is stub
    assert record.architecture == "catboost"


def test_booster_pool_get_record_returns_none_when_missing() -> None:
    pool = _make_pool({ITER20_BASE: _record(_StubBooster("alpha"), "catboost")})
    assert pool.get_record(ITER21_CHAIN) is None


def test_booster_pool_get_record_carries_xgboost_arch_for_nar_baseline() -> None:
    """NAR baseline boosters are XGBoost — the pool record preserves the arch
    so the scorer can build a float32-quantised matrix when scoring against
    them inside a mixed-arch ensemble."""
    stub = _StubBooster("nar-baseline")
    pool = _make_pool({NAR_BASELINE: _record(stub, "xgboost")})
    record = pool.get_record(NAR_BASELINE)
    assert record is not None
    assert record.architecture == "xgboost"


def test_booster_pool_has_true_when_present() -> None:
    pool = _make_pool({ITER22_RESIDUAL: _record(_StubBooster("beta"), "catboost")})
    assert pool.has(ITER22_RESIDUAL) is True


def test_booster_pool_has_false_when_absent() -> None:
    pool = _make_pool({ITER22_RESIDUAL: _record(_StubBooster("beta"), "catboost")})
    assert pool.has(ITER20_HPO) is False


def test_booster_pool_model_versions_returns_sorted_tuple() -> None:
    pool = _make_pool(
        {
            ITER22_RESIDUAL: _record(_StubBooster("d"), "catboost"),
            ITER20_BASE: _record(_StubBooster("a"), "catboost"),
            ITER21_CHAIN: _record(_StubBooster("c"), "catboost"),
            ITER20_HPO: _record(_StubBooster("b"), "catboost"),
        }
    )
    # ASCII sort: 'hpo-v8' < 'v8' because 'h' (104) < 'v' (118), so the iter20
    # HPO variant precedes the iter20 base in the sorted output.
    assert pool.model_versions() == (
        ITER20_HPO,
        ITER20_BASE,
        ITER21_CHAIN,
        ITER22_RESIDUAL,
    )


def test_booster_pool_model_versions_empty_pool() -> None:
    pool = _make_pool({})
    assert pool.model_versions() == ()


# ---------------------------------------------------------------------------
# discover_member_models


def test_discover_member_models_finds_existing_paths(tmp_path: Path) -> None:
    base = tmp_path / JRA_CATEGORY / "per-class" / CLASS_703
    expected_base = _write_fake_model_json(base / ITER20_BASE)
    expected_hpo = _write_fake_model_json(base / ITER20_HPO)

    found = discover_member_models(
        tmp_path,
        JRA_CATEGORY,
        CLASS_703,
        (ITER20_BASE, ITER20_HPO),
    )
    assert found == {ITER20_BASE: expected_base, ITER20_HPO: expected_hpo}


def test_discover_member_models_skips_missing(tmp_path: Path) -> None:
    base = tmp_path / JRA_CATEGORY / "per-class" / CLASS_703
    expected_chain = _write_fake_model_json(base / ITER21_CHAIN)

    found = discover_member_models(
        tmp_path,
        JRA_CATEGORY,
        CLASS_703,
        (ITER20_BASE, ITER21_CHAIN, ITER22_RESIDUAL),
    )
    assert found == {ITER21_CHAIN: expected_chain}


def test_discover_member_models_returns_empty_when_class_dir_missing(
    tmp_path: Path,
) -> None:
    found = discover_member_models(
        tmp_path,
        JRA_CATEGORY,
        CLASS_703,
        (ITER20_BASE, ITER21_CHAIN),
    )
    assert found == {}


def test_discover_member_models_returns_empty_when_no_members_requested(
    tmp_path: Path,
) -> None:
    found = discover_member_models(tmp_path, JRA_CATEGORY, CLASS_703, ())
    assert found == {}


def test_discover_member_models_walks_nar_subclass_layout(tmp_path: Path) -> None:
    """NAR per-class members live under ``per-class/NEW/<mv>/model.json`` — the
    same layout as JRA but with NAR sub-class names (NEW / MUKATSU / C / B /
    A / OP / other). The discoverer is layout-agnostic so the only assertion
    is the path resolution works with the new code values."""
    base = tmp_path / NAR_CATEGORY / "per-class" / CLASS_NEW
    expected_residual = _write_fake_model_json(base / NAR_RESIDUAL_NEW)

    found = discover_member_models(
        tmp_path,
        NAR_CATEGORY,
        CLASS_NEW,
        (NAR_RESIDUAL_NEW,),
    )
    assert found == {NAR_RESIDUAL_NEW: expected_residual}


# ---------------------------------------------------------------------------
# discover_baseline_member_model


def test_discover_baseline_member_model_returns_path_when_present(
    tmp_path: Path,
) -> None:
    """The category-global baseline lives at the category root (NOT under
    per-class/) — the discoverer mirrors the single-model loader's layout."""
    expected = _write_fake_model_json(tmp_path / NAR_CATEGORY / NAR_BASELINE)
    result = discover_baseline_member_model(tmp_path, NAR_CATEGORY, NAR_BASELINE)
    assert result == expected


def test_discover_baseline_member_model_returns_none_when_missing(
    tmp_path: Path,
) -> None:
    result = discover_baseline_member_model(tmp_path, NAR_CATEGORY, NAR_BASELINE)
    assert result is None


# ---------------------------------------------------------------------------
# load_booster_from_path


def test_load_booster_from_path_raises_when_file_missing(tmp_path: Path) -> None:
    missing = tmp_path / "nope" / "model.json"
    with pytest.raises(FileNotFoundError, match="booster missing"):
        load_booster_from_path(missing, "catboost")


def test_load_booster_from_path_xgboost_raises_when_file_missing(
    tmp_path: Path,
) -> None:
    """File-missing check must run before any arch dispatch — the XGBoost branch
    must surface the same ``FileNotFoundError`` as the CatBoost branch."""
    missing = tmp_path / "nope" / "model.json"
    with pytest.raises(FileNotFoundError, match="booster missing"):
        load_booster_from_path(missing, "xgboost")


def test_load_booster_from_path_delegates_to_catboost_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write_fake_model_json(tmp_path / ITER21_CHAIN)
    captured: dict[str, str] = {}
    stub_booster = _StubBooster("loaded")

    def fake_load(model_path: str) -> BoosterLike:
        captured["model_path"] = model_path
        return stub_booster

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    result = load_booster_from_path(path, "catboost")

    assert result is stub_booster
    assert captured["model_path"] == str(path)


def test_load_booster_from_path_delegates_to_xgboost_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The XGBoost dispatch is exercised by the NAR baseline; the same
    file-exists check + delegation pattern as the CatBoost branch."""
    path = _write_fake_model_json(tmp_path / NAR_BASELINE)
    captured: dict[str, str] = {}
    stub_booster = _StubBooster("nar-baseline")

    def fake_load(model_path: str) -> BoosterLike:
        captured["model_path"] = model_path
        return stub_booster

    _install_fake_xgboost_adapter(monkeypatch, fake_load)

    result = load_booster_from_path(path, "xgboost")

    assert result is stub_booster
    assert captured["model_path"] == str(path)


# ---------------------------------------------------------------------------
# build_pool_from_paths


def test_build_pool_from_paths_loads_all_catboost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_path = _write_fake_model_json(tmp_path / ITER20_BASE)
    hpo_path = _write_fake_model_json(tmp_path / ITER20_HPO)
    chain_path = _write_fake_model_json(tmp_path / ITER21_CHAIN)

    def fake_load(model_path: str) -> BoosterLike:
        return _StubBooster(model_path)

    _install_fake_catboost_adapter(monkeypatch, fake_load)

    pool = build_pool_from_paths(
        {ITER20_BASE: base_path, ITER20_HPO: hpo_path, ITER21_CHAIN: chain_path},
        {
            ITER20_BASE: "catboost",
            ITER20_HPO: "catboost",
            ITER21_CHAIN: "catboost",
        },
    )

    assert pool.model_versions() == (ITER20_HPO, ITER20_BASE, ITER21_CHAIN)
    base_record = pool.get_record(ITER20_BASE)
    hpo_record = pool.get_record(ITER20_HPO)
    chain_record = pool.get_record(ITER21_CHAIN)
    assert base_record is not None
    assert hpo_record is not None
    assert chain_record is not None
    assert base_record.architecture == "catboost"
    assert hpo_record.architecture == "catboost"
    assert chain_record.architecture == "catboost"
    base_loaded = base_record.booster
    hpo_loaded = hpo_record.booster
    chain_loaded = chain_record.booster
    assert isinstance(base_loaded, _StubBooster)
    assert isinstance(hpo_loaded, _StubBooster)
    assert isinstance(chain_loaded, _StubBooster)
    assert base_loaded.tag == str(base_path)
    assert hpo_loaded.tag == str(hpo_path)
    assert chain_loaded.tag == str(chain_path)


def test_build_pool_from_paths_mixed_arch_for_nar_ensemble(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A NAR ensemble member set spans XGBoost (iter 12 baseline) + CatBoost
    (iter 30 residual). The pool loads each member through the right adapter
    and the records preserve the per-member arch so the scorer can build a
    float32 matrix for the XGBoost member and a float64 matrix for the
    CatBoost member."""
    baseline_path = _write_fake_model_json(tmp_path / NAR_BASELINE)
    residual_path = _write_fake_model_json(tmp_path / NAR_RESIDUAL_NEW)

    def fake_catboost_load(model_path: str) -> BoosterLike:
        return _StubBooster(f"cb:{model_path}")

    def fake_xgboost_load(model_path: str) -> BoosterLike:
        return _StubBooster(f"xgb:{model_path}")

    _install_fake_catboost_adapter(monkeypatch, fake_catboost_load)
    _install_fake_xgboost_adapter(monkeypatch, fake_xgboost_load)

    pool = build_pool_from_paths(
        {NAR_BASELINE: baseline_path, NAR_RESIDUAL_NEW: residual_path},
        {NAR_BASELINE: "xgboost", NAR_RESIDUAL_NEW: "catboost"},
    )

    baseline_record = pool.get_record(NAR_BASELINE)
    residual_record = pool.get_record(NAR_RESIDUAL_NEW)
    assert baseline_record is not None
    assert residual_record is not None
    assert baseline_record.architecture == "xgboost"
    assert residual_record.architecture == "catboost"
    baseline_loaded = baseline_record.booster
    residual_loaded = residual_record.booster
    assert isinstance(baseline_loaded, _StubBooster)
    assert isinstance(residual_loaded, _StubBooster)
    assert baseline_loaded.tag == f"xgb:{baseline_path}"
    assert residual_loaded.tag == f"cb:{residual_path}"


def test_build_pool_from_paths_empty_input_returns_empty_pool() -> None:
    pool = build_pool_from_paths({}, {})
    assert pool.boosters == {}
    assert pool.model_versions() == ()


def test_build_pool_from_paths_propagates_file_missing(tmp_path: Path) -> None:
    missing = tmp_path / "absent" / "model.json"
    with pytest.raises(FileNotFoundError, match="booster missing"):
        build_pool_from_paths(
            {ITER22_RESIDUAL: missing},
            {ITER22_RESIDUAL: "catboost"},
        )
