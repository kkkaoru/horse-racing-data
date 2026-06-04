"""Tests for the multi-booster pool used by ensemble routing (Phase B-2C).

The pool's data structure (``BoosterPool``) and pure path helpers
(``discover_member_models``, ``build_pool_from_paths``) are covered with
``tmp_path`` fakes + a stub booster class implementing ``BoosterLike``. The
``load_booster_from_path`` side-effect helper imports ``catboost_adapter``
lazily; tests stub it via ``sys.modules`` so coverage can hit both branches
without needing the native CatBoost runtime.
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
    build_pool_from_paths,
    discover_member_models,
    load_booster_from_path,
)
from predict_lib.scorer import BoosterLike

JRA_CATEGORY: str = "jra"
CLASS_703: str = "703"
ITER20_BASE: str = "iter20-jra-cb-perclass-703-v8"
ITER20_HPO: str = "iter20-jra-cb-perclass-703-hpo-v8"
ITER21_CHAIN: str = "iter21-jra-cb-chain-703-v8"
ITER22_RESIDUAL: str = "iter22-jra-cb-residual-703-v8"


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


def _make_pool(boosters: dict[str, BoosterLike]) -> BoosterPool:
    return BoosterPool(boosters=boosters)


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


# ---------------------------------------------------------------------------
# BoosterPool


def test_booster_pool_get_returns_booster_when_present() -> None:
    stub = _StubBooster("alpha")
    pool = _make_pool({ITER20_BASE: stub})
    result = pool.get(ITER20_BASE)
    assert result is stub


def test_booster_pool_get_returns_none_when_missing() -> None:
    pool = _make_pool({ITER20_BASE: _StubBooster("alpha")})
    assert pool.get(ITER21_CHAIN) is None


def test_booster_pool_has_true_when_present() -> None:
    pool = _make_pool({ITER22_RESIDUAL: _StubBooster("beta")})
    assert pool.has(ITER22_RESIDUAL) is True


def test_booster_pool_has_false_when_absent() -> None:
    pool = _make_pool({ITER22_RESIDUAL: _StubBooster("beta")})
    assert pool.has(ITER20_HPO) is False


def test_booster_pool_model_versions_returns_sorted_tuple() -> None:
    pool = _make_pool(
        {
            ITER22_RESIDUAL: _StubBooster("d"),
            ITER20_BASE: _StubBooster("a"),
            ITER21_CHAIN: _StubBooster("c"),
            ITER20_HPO: _StubBooster("b"),
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


# ---------------------------------------------------------------------------
# load_booster_from_path


def test_load_booster_from_path_raises_when_file_missing(tmp_path: Path) -> None:
    missing = tmp_path / "nope" / "model.json"
    with pytest.raises(FileNotFoundError, match="booster missing"):
        load_booster_from_path(missing)


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

    result = load_booster_from_path(path)

    assert result is stub_booster
    assert captured["model_path"] == str(path)


# ---------------------------------------------------------------------------
# build_pool_from_paths


def test_build_pool_from_paths_loads_all(
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
        {ITER20_BASE: base_path, ITER20_HPO: hpo_path, ITER21_CHAIN: chain_path}
    )

    assert pool.model_versions() == (ITER20_HPO, ITER20_BASE, ITER21_CHAIN)
    base_loaded = pool.get(ITER20_BASE)
    hpo_loaded = pool.get(ITER20_HPO)
    chain_loaded = pool.get(ITER21_CHAIN)
    assert isinstance(base_loaded, _StubBooster)
    assert isinstance(hpo_loaded, _StubBooster)
    assert isinstance(chain_loaded, _StubBooster)
    assert base_loaded.tag == str(base_path)
    assert hpo_loaded.tag == str(hpo_path)
    assert chain_loaded.tag == str(chain_path)


def test_build_pool_from_paths_empty_input_returns_empty_pool() -> None:
    pool = build_pool_from_paths({})
    assert pool.boosters == {}
    assert pool.model_versions() == ()


def test_build_pool_from_paths_propagates_file_missing(tmp_path: Path) -> None:
    missing = tmp_path / "absent" / "model.json"
    with pytest.raises(FileNotFoundError, match="booster missing"):
        build_pool_from_paths({ITER22_RESIDUAL: missing})
